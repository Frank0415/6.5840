package raft

import (
	"Lab5_shard/labrpc"
	"sync"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Index   int
	Command any
}

type Persist struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

type Volatile struct {
	CommitIndex int
	LastApplied int
}

type VolatileLeader struct {
	NextIndex             []int
	MatchIndex            []int
	SentIndirectHeartbeat bool
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan ApplyMsg

	CurrentState State
	hasHeartBeat bool

	// Persistent state on all servers:
	PersistState Persist

	// Volatile state on all servers:
	VolatileState Volatile

	// Volatile state on leaders:
	VolatileLeaderState VolatileLeader
}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// ToDo: Your data here (3A).
	VoteGranted bool
	Term        int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// If the conflict occurs we immediately clean up an entire term and expects the leader to send data to us again
	ConflictIdx  int
	ConflictTerm int
}

type InstallSnapshotArgs struct {
	Term     int
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm  int

	Offset int
	Data   []byte
	Done   bool
}

type InstallSnapshotReply struct {
	Term int
}
