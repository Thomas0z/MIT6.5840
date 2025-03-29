package raft

import "6.5840/raftapi"

func (rf *Raft) applicationTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait()

		if !rf.snapPending {
			entries := make([]LogEntry, 0)
			if rf.lastApplied < rf.log.snapLastIdx {
				rf.lastApplied = rf.log.snapLastIdx
			}

			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				entries = append(entries, rf.log.at(i))
			}
			rf.mu.Unlock()

			for i, entry := range entries {
				rf.applyCh <- raftapi.ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied + 1 + i,
				}
			}

			rf.mu.Lock()
			LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1,
				rf.lastApplied+len(entries))
			rf.lastApplied += len(entries)
			rf.mu.Unlock()
			continue
		}

		rf.mu.Unlock()
		rf.applyCh <- raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.log.snapshot,
			SnapshotTerm:  rf.log.snapLastTerm,
			SnapshotIndex: rf.log.snapLastIdx,
		}

		rf.mu.Lock()
		LOG(rf.me, rf.currentTerm, DApply, "Install Snapshot for [0, %d]", rf.log.snapLastIdx)
		rf.lastApplied = rf.log.snapLastIdx
		if rf.commitIndex < rf.lastApplied {
			rf.commitIndex = rf.lastApplied
		}
		rf.snapPending = false
		rf.mu.Unlock()
	}
}
