package raft

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	firstIndex := rf.firstLogIndex()
	lastIndex := rf.lastLogIndex()
	if index <= firstIndex || index > lastIndex {
		return
	}

	offset := rf.toLogOffset(index)
	lastIncludedTerm := rf.PersistState.Log[offset].Term
	tail := append([]LogEntry(nil), rf.PersistState.Log[offset+1:]...)

	newLog := make([]LogEntry, 1, len(tail)+1)
	newLog[0] = LogEntry{Term: lastIncludedTerm, Index: index}
	newLog = append(newLog, tail...)
	rf.PersistState.Log = newLog

	if rf.VolatileState.CommitIndex < index {
		rf.VolatileState.CommitIndex = index
	}
	if rf.VolatileState.LastApplied < index {
		rf.VolatileState.LastApplied = index
	}

	rf.persistWithSnapshot(snapshot)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	shouldApplySnapshot := false

	reply.Term = rf.PersistState.CurrentTerm
	if args.Term < rf.PersistState.CurrentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.PersistState.CurrentTerm {
		rf.PersistState.CurrentTerm = args.Term
		rf.PersistState.VotedFor = -1
	}

	rf.CurrentState = Follower
	rf.hasHeartBeat = true
	reply.Term = rf.PersistState.CurrentTerm

	oldFirst := rf.firstLogIndex()
	if args.LastIncludedIndex > oldFirst {
		newLog := []LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}
		if args.LastIncludedIndex <= rf.lastLogIndex() {
			offset := rf.toLogOffset(args.LastIncludedIndex)
			if offset >= 0 && offset < len(rf.PersistState.Log) && rf.PersistState.Log[offset].Term == args.LastIncludedTerm {
				suffix := append([]LogEntry(nil), rf.PersistState.Log[offset+1:]...)
				newLog = append(newLog, suffix...)
			}
		}
		rf.PersistState.Log = newLog

		if rf.VolatileState.CommitIndex < args.LastIncludedIndex {
			rf.VolatileState.CommitIndex = args.LastIncludedIndex
		}
		if rf.VolatileState.LastApplied < args.LastIncludedIndex {
			shouldApplySnapshot = true
			rf.VolatileState.LastApplied = args.LastIncludedIndex
		}
	}

	rf.persistWithSnapshot(args.Data)
	rf.mu.Unlock()

	if shouldApplySnapshot {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}
}