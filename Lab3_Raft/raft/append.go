package raft

// currently only used for heartbeats, so we can ignore the log replication part for now.
// TODO: Implement log replication in part 3B.

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.PersistState.CurrentTerm {
		rf.CurrentState = Follower
		rf.PersistState.CurrentTerm = reply.Term
	}
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.PersistState.CurrentTerm {
		reply.Term = rf.PersistState.CurrentTerm
		reply.Success = false
		return
	} else {	
		rf.CurrentState = Follower
		rf.hasHeartBeat = true
		reply.Term = rf.PersistState.CurrentTerm
		reply.Success = true
		return
	}
}

// TODO: If later we need to send AppendEntries for log replication and send loop if the two does not match, we should modify this function.
func (rf *Raft) sendAll(args *AppendEntriesArgs) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		reply := AppendEntriesReply{}
		rf.sendAppendEntries(i, args, &reply)

		if reply.Term > rf.PersistState.CurrentTerm {
			rf.CurrentState = Follower
			rf.PersistState.CurrentTerm = reply.Term
			rf.hasHeartBeat = false
			return
		}
	}
}
