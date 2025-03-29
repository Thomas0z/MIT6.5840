package raft

import (
	"fmt"
	"slices"
	"sort"
	"time"
)

type LogEntry struct {
	CommandValid bool
	Command      any
	Term         int
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	// used to update the follower's commitIndex
	LeaderCommitIndex int

	// used to probe the match point
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d", args.LeaderId, args.Term)
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictTerm  int
	ConflictIndex int
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Sucess: %v", reply.Term, reply.Success)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Append, Args=%v", args.LeaderId, args.String())

	// align the term
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Reject append, Higher term, T%d>T%d",
			args.LeaderId, rf.currentTerm, args.Term)
		return
	}
	rf.becomeFollowerLocked(args.Term)

	// After we align the term, we accept the args.LeaderId as
	// Leader, then we must reset election timer whether we
	// accept the log or not
	defer rf.resetElectionTimerLocked()

	l := rf.log.size()
	if args.PrevLogIndex >= l {
		LOG(rf.me, rf.currentTerm, DReceiveLog, "<- S%d, Reject log, Follower log too short, "+
			"Len:%d < Prev:%d", args.LeaderId, l, args.PrevLogIndex)
		reply.ConflictTerm = InvalidTerm
		reply.ConflictIndex = l
		return
	}
	if args.PrevLogIndex < rf.log.snapLastIdx {
		reply.ConflictIndex = rf.log.snapLastIdx
		reply.ConflictTerm = rf.log.snapLastTerm
		LOG(rf.me, rf.currentTerm, DReceiveLog, "<- S%d, Reject log, Follower log truncated in %d", args.LeaderId, rf.log.snapLastIdx)
		return
	}
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		LOG(rf.me, rf.currentTerm, DReceiveLog, "<- S%d, Reject log, Prev log not match, [%d]: T%d != T%d",
			args.LeaderId, args.PrevLogIndex, rf.log.at(args.PrevLogIndex).Term, args.PrevLogTerm)
		reply.ConflictTerm = rf.log.at(args.PrevLogIndex).Term
		reply.ConflictIndex = rf.log.firstFor(reply.ConflictTerm)
		return
	}

	// log override rule
	rf.log.appendFrom(args.PrevLogIndex, args.Entries)
	rf.persistLocked()
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DReceiveLog, "Follower accept logs: (%d, %d]",
		args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	if rf.commitIndex < args.LeaderCommitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d",
			rf.commitIndex, args.LeaderCommitIndex)
		rf.commitIndex = min(args.LeaderCommitIndex, rf.log.size()-1)
		rf.applyCond.Signal()
	}
}

func (rf *Raft) getMajorityCommitIndexLocked() int {
	tmpIndexes := make([]int, len(rf.peers))
	copy(tmpIndexes, rf.matchIndex)
	sort.Ints(tmpIndexes)
	// For n (0-indexed), find the position that makes the number of subsequent elements is (n+1)/2.
	// majority: (n+1)/2
	// position: n-(n+1)/2=(n-1)/2
	majorityIdx := (len(rf.peers) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tmpIndexes,
		majorityIdx, tmpIndexes[majorityIdx])
	return tmpIndexes[majorityIdx]
}

func (rf *Raft) startReplication(term int) bool {
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DSendLog, "-> S%d, Lost or crashed", peer)
			return
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Reply=%v", peer, reply.String())
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DSendLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer,
				term, rf.currentTerm, rf.role)
			return
		}

		// try to find the lower matched log index if the prevLog not matched
		if !reply.Success {
			prevIndex := rf.nextIndex[peer]
			if reply.ConflictTerm == InvalidTerm {
				rf.nextIndex[peer] = reply.ConflictIndex
			} else {
				firstIndex := rf.log.firstFor(reply.ConflictTerm)
				if firstIndex != InvalidIndex {
					rf.nextIndex[peer] = firstIndex
				} else {
					rf.nextIndex[peer] = reply.ConflictIndex
				}
			}
			if rf.nextIndex[peer] > prevIndex {
				rf.nextIndex[peer] = prevIndex
			}

			nextPrevIndex := rf.nextIndex[peer] - 1
			nextPrevTerm := InvalidTerm
			if nextPrevIndex >= rf.log.snapLastIdx {
				nextPrevTerm = rf.log.at(nextPrevIndex).Term
			}
			LOG(rf.me, rf.currentTerm, DSendLog, "-> S%d, Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d",
				peer, args.PrevLogIndex, args.PrevLogTerm, nextPrevIndex, nextPrevTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Leader log=%v", peer, rf.log.String())
			return
		}

		// update match/next index if log appended successfully
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		majorityMatched := rf.getMajorityCommitIndexLocked()
		// paper 5.4.2 Raft never commits log entries from previous terms by counting replicas.
		// Only log entries from the leader's current term are committed by counting replicas.
		if majorityMatched > rf.commitIndex && rf.log.at(majorityMatched).Term == term {
			LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d",
				rf.commitIndex, majorityMatched)
			rf.commitIndex = majorityMatched
			rf.applyCond.Signal()
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DSendLog, "Lost Leader[%d] to %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			rf.matchIndex[peer] = rf.log.size() - 1
			rf.nextIndex[peer] = rf.log.size()
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1
		if prevIdx < rf.log.snapLastIdx {
			args := &InstallSnapshotArgs{
				Term:             rf.currentTerm,
				LeaderId:         rf.me,
				LastIncludedIdx:  rf.log.snapLastIdx,
				LastIncludedTerm: rf.log.snapLastTerm,
				Snapshot:         rf.log.snapshot,
			}
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Send snapshot, Args=%v", peer, args.String())
			go rf.installSnapshotToPeer(peer, term, args)
			continue
		}

		prevTerm := rf.log.at(prevIdx).Term
		args := &AppendEntriesArgs{
			Term:              term,
			LeaderId:          rf.me,
			PrevLogIndex:      prevIdx,
			PrevLogTerm:       prevTerm,
			Entries:           slices.Clone(rf.log.tail(prevIdx + 1)),
			LeaderCommitIndex: rf.commitIndex,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Args=%v", peer, args.String())
		go replicateToPeer(peer, args)
	}

	return true
}

func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			break
		}

		time.Sleep(replicateInterval)
	}
}
