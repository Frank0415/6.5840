package raft

// currently only used for heartbeats, so we can ignore the log replication part for now.
// TODO: Implement log replication in part 3B.

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.PersistState.CurrentTerm
	reply.ConflictTerm = -1
	reply.ConflictIdx = -1 // keep your current field name if already used elsewhere

	if args.Term < rf.PersistState.CurrentTerm {
		return
	}

	if args.Term > rf.PersistState.CurrentTerm {
		rf.PersistState.CurrentTerm = args.Term
		rf.PersistState.VotedFor = -1
		rf.persist()
	}
	rf.CurrentState = Follower
	rf.hasHeartBeat = true
	reply.Term = rf.PersistState.CurrentTerm
	firstLogIndex := rf.firstLogIndex()
	lastLogIndex := rf.lastLogIndex()

	if args.PrevLogIndex < firstLogIndex {
		reply.ConflictIdx = firstLogIndex + 1
		return
	}

	// 1) missing prevLogIndex
	if args.PrevLogIndex > lastLogIndex {
		reply.ConflictIdx = lastLogIndex + 1
		return
	}

	// 2) term mismatch at prevLogIndex
	if rf.logTermAt(args.PrevLogIndex) != args.PrevLogTerm {
		ct := rf.logTermAt(args.PrevLogIndex)
		reply.ConflictTerm = ct
		i := args.PrevLogIndex
		for i > firstLogIndex && rf.logTermAt(i-1) == ct {
			i--
		}
		reply.ConflictIdx = i
		return
	}

	// 3) prefix matches; now merge
	for i := 0; i < len(args.Entries); i++ {
		index := args.PrevLogIndex + 1 + i
		if index <= rf.lastLogIndex() {
			if rf.logTermAt(index) != args.Entries[i].Term {
				offset := rf.toLogOffset(index)
				rf.PersistState.Log = append(rf.PersistState.Log[:offset], args.Entries[i:]...)
				rf.persist()
				break
			}
		} else {
			rf.PersistState.Log = append(rf.PersistState.Log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	if args.LeaderCommit > rf.VolatileState.CommitIndex {
		last := rf.lastLogIndex()
		rf.VolatileState.CommitIndex = min(args.LeaderCommit, last)
	}

	reply.Success = true
}

/*
For a leader to send appendEntries RPC, we need to:
1. Get the current term and log info (PrevLogIndex, PrevLogTerm, Entries, LeaderCommit), should be the last one if no error.
2. Send the RPC to all followers.
3. Receive the reply
4. If the reply is false:
	Either the term is outdated: the leader should step down and update its term. This also needs to cancel all
	Or the log is inconsistent: the leader should decrement nextIndex and retry until it matches.
5. If the reply is true:
	Update matchIndex and nextIndex for that follower.
6. If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, and log[N].term == currentTerm:
	Update commitIndex = N.
*/

func (rf *Raft) replicateToPeer(peer int) {
	rf.mu.Lock()
	if rf.CurrentState != Leader {
		rf.mu.Unlock()
		return
	}
	firstIndex := rf.firstLogIndex()
	nextIndex := rf.VolatileLeaderState.NextIndex[peer]
	if nextIndex <= firstIndex {
		args := InstallSnapshotArgs{
			Term:              rf.PersistState.CurrentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: firstIndex,
			LastIncludedTerm:  rf.PersistState.Log[0].Term,
			Offset:            0,
			Data:              rf.persister.ReadSnapshot(),
			Done:              true,
		}
		rf.mu.Unlock()

		reply := InstallSnapshotReply{}
		if ok := rf.sendInstallSnapshot(peer, &args, &reply); ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.PersistState.CurrentTerm {
				rf.PersistState.CurrentTerm = reply.Term
				rf.PersistState.VotedFor = -1
				rf.CurrentState = Follower
				rf.persist()
				return
			}

			if rf.CurrentState != Leader || rf.PersistState.CurrentTerm != args.Term {
				return
			}

			rf.VolatileLeaderState.MatchIndex[peer] = max(rf.VolatileLeaderState.MatchIndex[peer], args.LastIncludedIndex)
			rf.VolatileLeaderState.NextIndex[peer] = rf.VolatileLeaderState.MatchIndex[peer] + 1
		}
		return
	}

	prevLogIndex := nextIndex - 1
	prevOffset := rf.toLogOffset(prevLogIndex)
	entries := append([]LogEntry(nil), rf.PersistState.Log[prevOffset+1:]...)

	args := AppendEntriesArgs{
		Term:         rf.PersistState.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.PersistState.Log[prevOffset].Term,
		Entries:      entries,
		LeaderCommit: rf.VolatileState.CommitIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	if ok := rf.sendAppendEntries(peer, &args, &reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.PersistState.CurrentTerm {
			rf.PersistState.CurrentTerm = reply.Term
			rf.PersistState.VotedFor = -1
			rf.CurrentState = Follower
			rf.persist()
			return
		}

		if rf.CurrentState != Leader || rf.PersistState.CurrentTerm != args.Term {
			return
		}

		if reply.Success {
			rf.VolatileLeaderState.MatchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			rf.VolatileLeaderState.NextIndex[peer] = rf.VolatileLeaderState.MatchIndex[peer] + 1
			rf.updateCommitIndex()
		} else {
			next := reply.ConflictIdx
			if reply.ConflictTerm != -1 {
				leaderLastIdxForTerm := -1
				for idx := rf.lastLogIndex(); idx >= rf.firstLogIndex(); idx-- {
					if rf.logTermAt(idx) == reply.ConflictTerm {
						leaderLastIdxForTerm = idx
						break
					}
				}
				if leaderLastIdxForTerm != -1 {
					next = leaderLastIdxForTerm + 1
				}
			}
			rf.VolatileLeaderState.NextIndex[peer] = max(next, 1)
			go rf.replicateToPeer(peer)
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	for n := rf.lastLogIndex(); n > rf.VolatileState.CommitIndex; n-- {
		if rf.logTermAt(n) != rf.PersistState.CurrentTerm {
			continue
		}
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.VolatileLeaderState.MatchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.VolatileState.CommitIndex = n
			break
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	for i := range rf.peers {
		if i != rf.me {
			go rf.replicateToPeer(i)
		}
	}
	rf.VolatileLeaderState.SentIndirectHeartbeat = true
}

// func (rf *Raft) broadcastNoOp() {
// 	rf.mu.Lock()
// 	if rf.CurrentState != Leader {
// 		rf.mu.Unlock()
// 		return
// 	}
// 	rf.PersistState.Log = append(rf.PersistState.Log, LogEntry{
// 		Term:  rf.PersistState.CurrentTerm,
// 		Index: len(rf.PersistState.Log),
// 	})
// 	rf.persist()
// 	rf.mu.Unlock()
// 	rf.broadcastAppendEntries()
// }
