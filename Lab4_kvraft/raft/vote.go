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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLog := rf.PersistState.Log[len(rf.PersistState.Log)-1]
	reply.Term = rf.PersistState.CurrentTerm
	reply.VoteGranted = false

	// 1. Reply false if term < currentTerm
	if args.Term < rf.PersistState.CurrentTerm {
		return
	}

	// If term is greater, update currentTerm and reset votedFor
	if args.Term > rf.PersistState.CurrentTerm {
		rf.PersistState.CurrentTerm = args.Term
		rf.PersistState.VotedFor = -1
		rf.CurrentState = Follower
		rf.persist()
		reply.Term = rf.PersistState.CurrentTerm
	}

	// 2. If votedFor is null or candidateId, and candidate's log is at least
	// as up-to-date as receiver's log, grant vote
	upToDate := (args.LastLogTerm > lastLog.Term) ||
		(args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)

	if (rf.PersistState.VotedFor == -1 || rf.PersistState.VotedFor == args.CandidateId) && upToDate {
		rf.PersistState.VotedFor = args.CandidateId
		rf.CurrentState = Follower
		rf.hasHeartBeat = true // reset election timer
		rf.persist()
		reply.VoteGranted = true
	}
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
	rf.PersistState.VotedFor = rf.me
	rf.hasHeartBeat = false // reset election timer for self
	rf.persist()

	electionTerm := rf.PersistState.CurrentTerm
	me := rf.me
	lastLog := rf.PersistState.Log[len(rf.PersistState.Log)-1]
	reqArgs := RequestVoteArgs{
		Term:         electionTerm,
		CandidateId:  me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
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
			rf.VolatileLeaderState.NextIndex = make([]int, len(rf.peers))
			for i := range rf.peers {
				rf.VolatileLeaderState.NextIndex[i] = rf.PersistState.Log[len(rf.PersistState.Log)-1].Index + 1
			}
			rf.VolatileLeaderState.MatchIndex = make([]int, len(rf.peers))
			rf.mu.Unlock()
			// go rf.broadcastNoOp() // send initial heartbeat
			go rf.broadcastAppendEntries() // send initial heartbeat
			return
		}
		rf.mu.Unlock()

		// Break if all weight/replies accounted for
		if totalVotes+(len(rf.peers)-1-totalVotes) <= len(rf.peers)/2 {
			return
		}
	}
}
