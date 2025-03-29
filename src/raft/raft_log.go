package raft

import (
	"fmt"

	"6.5840/labgob"
)

type RaftLog struct {
	snapLastIdx  int
	snapLastTerm int
	// contains [1, snapLastIdx]
	snapshot []byte
	// the fisrt log entry is dummy log entry, which serves purposes:
	// 1. align the physical and logical index(tailLog[i] corresponds to log entry at snapLastIdx+1)
	// the entries between (snapLastIdx, snapLastIdx+len(tailLog)-1] have real data
	tailLog []LogEntry
}

func NewLog(snapLastIdx, snapLastTerm int, snapshot []byte, entries []LogEntry) *RaftLog {
	rl := &RaftLog{
		snapLastIdx:  snapLastIdx,
		snapLastTerm: snapLastTerm,
		snapshot:     snapshot,
	}

	rl.tailLog = make([]LogEntry, 0, len(entries)+1)
	rl.tailLog = append(rl.tailLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	rl.tailLog = append(rl.tailLog, entries...)

	return rl
}

// all the functions below should be called under the protection of rf.mute
func (rl *RaftLog) readPersist(d *labgob.LabDecoder) error {
	var lastIdx int
	if err := d.Decode(&lastIdx); err != nil {
		return fmt.Errorf("decode last include index failed")
	}
	rl.snapLastIdx = lastIdx

	var lastTerm int
	if err := d.Decode(&lastTerm); err != nil {
		return fmt.Errorf("decode last include term failed")
	}
	rl.snapLastTerm = lastTerm

	var log []LogEntry
	if err := d.Decode(&log); err != nil {
		return fmt.Errorf("decode tail log failed")
	}
	rl.tailLog = log

	return nil
}

func (rl *RaftLog) persist(e *labgob.LabEncoder) {
	e.Encode(rl.snapLastIdx)
	e.Encode(rl.snapLastTerm)
	e.Encode(rl.tailLog)
}

func (rl *RaftLog) size() int {
	return rl.snapLastIdx + len(rl.tailLog)
}

// access the index `rl.snapLastIdx` is allowed, although it's not exist actually.
func (rl *RaftLog) idx(logicIdx int) int {
	if logicIdx < rl.snapLastIdx || logicIdx >= rl.size() {
		panic(fmt.Sprintf("%d is out of [%d, %d]", logicIdx, rl.snapLastIdx+1, rl.size()-1))
	}
	return logicIdx - rl.snapLastIdx
}

func (rl *RaftLog) at(logicIdx int) LogEntry {
	return rl.tailLog[rl.idx(logicIdx)]
}

func (rl *RaftLog) last() (idx, term int) {
	return rl.size() - 1, rl.tailLog[len(rl.tailLog)-1].Term
}

func (rl *RaftLog) tail(startIdx int) []LogEntry {
	if startIdx >= rl.size() {
		return nil
	}
	return rl.tailLog[rl.idx(startIdx):]
}

func (rl *RaftLog) firstFor(term int) int {
	for idx, entry := range rl.tailLog {
		if entry.Term == term {
			return rl.snapLastIdx + idx
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

func (rl *RaftLog) append(e LogEntry) {
	rl.tailLog = append(rl.tailLog, e)
}

func (rl *RaftLog) appendFrom(prevIdx int, entries []LogEntry) {
	rl.tailLog = append(rl.tailLog[:rl.idx(prevIdx)+1], entries...)
}

// string methods for debug
func (rl *RaftLog) String() string {
	var terms string
	prevTerm := rl.snapLastTerm
	prevStart := rl.snapLastIdx
	for i := 0; i < len(rl.tailLog); i++ {
		if rl.tailLog[i].Term != prevTerm {
			terms += fmt.Sprintf(" [%d, %d]T%d", prevStart, rl.snapLastIdx+i-1, prevTerm)
			prevTerm = rl.tailLog[i].Term
			prevStart = i
		}
	}
	terms += fmt.Sprintf("[%d, %d]T%d", prevStart, rl.snapLastIdx+len(rl.tailLog)-1, prevTerm)
	return terms
}

func (rl *RaftLog) doSnapshotLocked(index int, snapshot []byte) {
	// transfer the index to the index in tailLog
	idx := rl.idx(index)

	rl.snapLastIdx = index
	rl.snapLastTerm = rl.tailLog[idx].Term
	rl.snapshot = snapshot

	newTailLog := make([]LogEntry, 0, rl.size()-rl.snapLastIdx)
	newTailLog = append(newTailLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	newTailLog = append(newTailLog, rl.tailLog[idx+1:]...)

	rl.tailLog = newTailLog
}

func (rl *RaftLog) installSnapshot(index, term int, snapshot []byte) {
	rl.snapLastIdx = index
	rl.snapLastTerm = term
	rl.snapshot = snapshot

	newTailLog := make([]LogEntry, 0, 1)
	newTailLog = append(newTailLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	rl.tailLog = newTailLog
}
