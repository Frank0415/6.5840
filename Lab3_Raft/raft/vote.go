package raft

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if args.LastLogTerm < rf.PersistState.CurrentTerm || args.Term < rf.PersistState.CurrentTerm || args.LastLogIndex < rf.PersistState.Log[len(rf.PersistState.Log)-1].Index {
		reply.VoteGranted = false
		reply.Term = rf.PersistState.CurrentTerm
		return
	}
	reply.VoteGranted = true
	reply.Term = rf.PersistState.CurrentTerm
}

// The exact voting logic
func (rf *Raft) voteProcess() {
	rf.mu.Lock()
	if rf.hasHeartBeat || rf.CurrentState == Leader {
		rf.hasHeartBeat = false // reset for next ticker cycle
		rf.mu.Unlock()
		return
	}
	// Transition to Candidate and increment term
	rf.CurrentState = Candidate
	rf.PersistState.CurrentTerm++
	rf.hasHeartBeat = false
	rf.PersistState.HasVoted = false

	electionTerm := rf.PersistState.CurrentTerm
	me := rf.me
	lastPos := len(rf.PersistState.Log) - 1
	reqArgs := RequestVoteArgs{
		Term:         electionTerm,
		CandidateId:  me,
		LastLogIndex: rf.PersistState.Log[lastPos].Index,
		LastLogTerm:  rf.PersistState.Log[lastPos].Term,
	}
	rf.mu.Unlock()

	ch := make(chan bool, len(rf.peers))
	for i := range rf.peers {
		if i == me {
			continue
		}
		go func(peer int) {
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(peer, &reqArgs, &reply); ok {
				ch <- reply.VoteGranted
			} else {
				ch <- false
			}
		}(i)
	}

	rf.mu.Lock()
	rf.PersistState.HasVoted = true
	rf.PersistState.VotedFor = me
	rf.mu.Unlock()

	totalVotes := 1
	for {
		vote := <-ch

		rf.mu.Lock()
		// Termination logic:
		// 1. Term changed (new election started)
		// 2. Role changed (received heartbeat or already won)
		if rf.PersistState.CurrentTerm != electionTerm || rf.CurrentState != Candidate {
			rf.mu.Unlock()
			return
		}

		if vote {
			totalVotes++
		}

		if totalVotes > len(rf.peers)/2 {
			rf.CurrentState = Leader
			rf.mu.Unlock()
			go rf.sendAll(&AppendEntriesArgs{
				Term:     rf.PersistState.CurrentTerm,
				LeaderId: rf.me,
			})
			return
		}
		rf.mu.Unlock()

		// Break if all weight/replies accounted for
		if totalVotes+(len(rf.peers)-1-totalVotes) <= len(rf.peers)/2 {
			return
		}
	}
}
