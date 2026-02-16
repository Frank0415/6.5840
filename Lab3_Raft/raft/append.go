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

	// 1) missing prevLogIndex
	if args.PrevLogIndex >= len(rf.PersistState.Log) {
		reply.ConflictIdx = rf.PersistState.Log[len(rf.PersistState.Log)-1].Index + 1
		return
	}

	// 2) term mismatch at prevLogIndex
	if rf.PersistState.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		ct := rf.PersistState.Log[args.PrevLogIndex].Term
		reply.ConflictTerm = ct
		i := args.PrevLogIndex
		for i > 0 && rf.PersistState.Log[i-1].Term == ct {
			i--
		}
		reply.ConflictIdx = i
		return
	}

	// 3) prefix matches; now merge
	for i := 0; i < len(args.Entries); i++ {
		pos := args.PrevLogIndex + 1 + i
		if pos < len(rf.PersistState.Log) {
			if rf.PersistState.Log[pos].Term != args.Entries[i].Term {
				rf.PersistState.Log = append(rf.PersistState.Log[:pos], args.Entries[i:]...)
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
		last := len(rf.PersistState.Log) - 1
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

	prevLogIndex := rf.VolatileLeaderState.NextIndex[peer] - 1
	entries := make([]LogEntry, len(rf.PersistState.Log)-(prevLogIndex+1))
	copy(entries, rf.PersistState.Log[prevLogIndex+1:])

	args := AppendEntriesArgs{
		Term:         rf.PersistState.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.PersistState.Log[prevLogIndex].Term,
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
			rf.VolatileLeaderState.NextIndex[peer] = max(reply.ConflictIdx, 1)
			go rf.replicateToPeer(peer)
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	for n := len(rf.PersistState.Log) - 1; n > rf.VolatileState.CommitIndex; n-- {
		if rf.PersistState.Log[n].Term != rf.PersistState.CurrentTerm {
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
