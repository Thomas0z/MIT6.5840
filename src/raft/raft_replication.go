package raft

import (
	"fmt"
	"sort"
	"time"
)

type LogEntry struct {
	CommandValid bool
	Command      any
	Term         int
}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	LeaderCommitIndex int
	Entries           []LogEntry
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d", args.LeaderId, args.Term)
}

type AppendEntriesReply struct {
	Term    int
	Success bool
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

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Reject append, Higher term, T%d>T%d",
			args.LeaderId, rf.currentTerm, args.Term)
		return
	}

	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	if args.PrevLogIndex >= len(rf.log) {
		LOG(rf.me, rf.currentTerm, DReceiveLog, "<- S%d, Reject log, Follower log too short, "+
			"Len:%d < Prev:%d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		LOG(rf.me, rf.currentTerm, DReceiveLog, "<- S%d, Reject log, Prev log not match, [%d]: T%d != T%d",
			args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}

	// log override rule
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DReceiveLog, "Follower accept logs: (%d, %d]",
		args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	if rf.commitIndex < args.LeaderCommitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d",
			rf.commitIndex, args.LeaderCommitIndex)
		rf.commitIndex = args.LeaderCommitIndex
		rf.applyCond.Signal()
	}

	rf.resetElectionTimerLocked()
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
			// go back a term
			idx, term := args.PrevLogIndex, args.PrevLogTerm
			for idx > 0 && rf.log[idx].Term == term {
				idx--
			}
			rf.nextIndex[peer] = idx + 1
			LOG(rf.me, rf.currentTerm, DSendLog, "-> S%d, Not matched at %d, try next=%d", peer,
				args.PrevLogIndex, rf.nextIndex[peer])
			return
		}

		// update match/next index if log appended successfully
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		majorityMatched := rf.getMajorityCommitIndexLocked()
		if majorityMatched > rf.commitIndex {
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
			rf.matchIndex[peer] = len(rf.log) - 1
			rf.nextIndex[peer] = len(rf.log)
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1
		prevTerm := rf.log[prevIdx].Term
		args := &AppendEntriesArgs{
			Term:              term,
			LeaderId:          rf.me,
			PrevLogIndex:      prevIdx,
			PrevLogTerm:       prevTerm,
			Entries:           rf.log[prevIdx+1:],
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
