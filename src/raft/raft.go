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
	//	"bytes"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int
	Command interface{}
}

type State int

const (
	leader    State = 0
	follower  State = 1
	candidate State = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// non Volatile state
	currentTerm   int
	votedFor      int
	logs          []Entry
	snapshotIndex int
	// Volatile state
	state       State
	commitIndex int
	lastApplied int
	// server volatile state
	nextIndex  []int
	matchIndex []int
	// state for synchronize
	replicateCond *sync.Cond
	replicateCh   []chan struct{}
	applyChan     chan ApplyMsg
	applyCond     *sync.Cond
	electionTimer *time.Timer
	hrtBtTimer    *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	rf.persister.SaveRaftState(rf.getPersistData())
}

func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.logs)
	return w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		currentTerm, votedFor      int
		logs                       []Entry
		commitIndex, snapshotIndex int
	)
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&commitIndex) != nil ||
		d.Decode(&snapshotIndex) != nil || d.Decode(&logs) != nil {
		log.Fatal("rf read persist err!")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.commitIndex = commitIndex
		rf.snapshotIndex = snapshotIndex
		rf.logs = logs
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		return -1, -1, false
	}
	newLog := Entry{Command: command, Term: rf.currentTerm}
	rf.logs = append(rf.logs, newLog)
	index := len(rf.logs) - 1 + rf.snapshotIndex
	DPrintf("{Node %v} receives a new command[%+v] to replicate in term %v\n", rf.me, newLog, rf.currentTerm)
	rf.replicateCond.Signal()
	return index, newLog.Term, true
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

func (rf *Raft) ticker() {
	go func() {
		for !rf.killed() {
			select {
			case <-rf.hrtBtTimer.C:
				rf.doHeartBeat()
			case <-rf.electionTimer.C:
				rf.startElection()
			}
		}
	}()

	go func() {
		rf.replicateCond.L.Lock()
		defer rf.replicateCond.L.Unlock()
		for !rf.killed() {
			for rf.state != leader {
				rf.replicateCond.Wait()
			}
			rf.doHeartBeat()
		}
	}()
	go rf.applyCommand()
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
	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		applyChan:     applyCh,
		dead:          0,
		state:         follower,
		currentTerm:   1,
		votedFor:      -1,
		logs:          make([]Entry, 1),
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		hrtBtTimer:    time.NewTimer(heartBeatDuration()),
		electionTimer: time.NewTimer(electionDuration()),
		replicateCond: sync.NewCond(&sync.Mutex{}),
		replicateCh:   make([]chan struct{}, len(peers)),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	for i := 0; i < len(peers); i++ {
		rf.replicateCh[i] = make(chan struct{})
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("initialization me:%d term:%d,isLeader:%t\n", rf.me, rf.currentTerm, rf.state == leader)
	// start ticker goroutine to start elections
	rf.ticker()

	return rf
}
