package raft

import "6.5840/raftapi"

func (rf *Raft) applicationTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait()

		if !rf.snapPending {
			if rf.lastApplied < rf.log.snapLastIdx {
				rf.lastApplied = rf.log.snapLastIdx
			}

			startIdx := rf.lastApplied + 1
			endIdx := rf.commitIndex
			entries := make([]LogEntry, 0, endIdx-startIdx+1)
			for i := startIdx; i <= endIdx; i++ {
				entries = append(entries, rf.log.at(i))
			}
			rf.mu.Unlock()

			for i, entry := range entries {
				rf.applyCh <- raftapi.ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: startIdx + i,
				}
			}

			rf.mu.Lock()
			LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", startIdx, endIdx)
			rf.lastApplied = endIdx
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
