package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync/atomic"
	"time"

	"Lab3_Raft/labgob"
	"Lab3_Raft/labrpc"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.PersistState.CurrentTerm
	isleader := (rf.CurrentState == Leader)
	return term, isleader
}

func (rf *Raft) firstLogIndex() int {
	return rf.PersistState.Log[0].Index
}

func (rf *Raft) lastLogIndex() int {
	return rf.PersistState.Log[len(rf.PersistState.Log)-1].Index
}

func (rf *Raft) toLogOffset(index int) int {
	return index - rf.firstLogIndex()
}

func (rf *Raft) logTermAt(index int) int {
	offset := rf.toLogOffset(index)
	if offset < 0 || offset >= len(rf.PersistState.Log) {
		return -1
	}
	return rf.PersistState.Log[offset].Term
}

func (rf *Raft) logEntryAt(index int) LogEntry {
	return rf.PersistState.Log[rf.toLogOffset(index)]
}

func (rf *Raft) encodePersistentState() ([]byte, bool) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.PersistState.CurrentTerm) != nil ||
		e.Encode(rf.PersistState.VotedFor) != nil ||
		e.Encode(rf.PersistState.Log) != nil {
		return nil, false
	}
	return w.Bytes(), true
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	if raftState, ok := rf.encodePersistentState(); ok {
		rf.persister.Save(raftState, snapshot)
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	rf.persistWithSnapshot(rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		return
	}

	rf.PersistState.CurrentTerm = currentTerm
	rf.PersistState.VotedFor = votedFor
	if len(log) == 0 {
		rf.PersistState.Log = []LogEntry{{Term: 0, Index: 0}}
	} else {
		rf.PersistState.Log = log
	}
}



// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command any) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurrentState != Leader {
		return -1, rf.PersistState.CurrentTerm, false
	}

	index := rf.lastLogIndex() + 1
	rf.PersistState.Log = append(rf.PersistState.Log, LogEntry{
		Term:    rf.PersistState.CurrentTerm,
		Index:   index,
		Command: command,
	})
	rf.persist()
	go rf.broadcastAppendEntries()
	return index, rf.PersistState.CurrentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		ms := 250 + (rand.Int63() % 250)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		go rf.voteProcess()
	}
}

func (rf *Raft) sendHeartbeatsTicker() {
	for rf.killed() == false {
		time.Sleep(110 * time.Millisecond)
		rf.mu.Lock()
		if rf.CurrentState == Leader {
			if rf.VolatileLeaderState.SentIndirectHeartbeat {
				rf.VolatileLeaderState.SentIndirectHeartbeat = false
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
				rf.broadcastAppendEntries()
			}
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		var msgs []ApplyMsg

		rf.mu.Lock()
		for rf.VolatileState.LastApplied < rf.VolatileState.CommitIndex {
			rf.VolatileState.LastApplied++
			i := rf.VolatileState.LastApplied
			cmd := rf.logEntryAt(i).Command
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      cmd,
				CommandIndex: i,
			})
		}
		rf.mu.Unlock()

		for _, m := range msgs {
			rf.applyCh <- m
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.PersistState.Log = []LogEntry{{Term: 0, Index: 0}}
	rf.CurrentState = Follower
	rf.PersistState.CurrentTerm = 0
	rf.PersistState.VotedFor = -1
	rf.VolatileState.CommitIndex = 0
	rf.VolatileState.LastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.VolatileState.CommitIndex = rf.firstLogIndex()
	rf.VolatileState.LastApplied = rf.firstLogIndex()

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.sendHeartbeatsTicker()
	go rf.applier()

	return rf
}
