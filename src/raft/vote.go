package raft

import (
	"math/rand"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func electionDuration() time.Duration {
	return time.Duration(400+(rand.Int()&255)) * time.Millisecond
}

func (rf *Raft) logIsUpdated(lastTerm, lastIndex int) bool {
	return lastTerm > rf.logs[len(rf.logs)-1].Term ||
		(lastTerm == rf.logs[len(rf.logs)-1].Term && lastIndex >= len(rf.logs)-1+rf.snapshotIndex)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// DPrintf("me:%d receive requestvote need get lock\n", rf.me)
	rf.mu.Lock()
	// DPrintf("me:%d receive requestvote already get lock\n", rf.me)
	defer rf.mu.Unlock()
	defer func() {
		DPrintf("after requestvote with args:%+v me:%d term:%d,voteFor:%d,state:%d\n", args, rf.me, rf.currentTerm, rf.votedFor, rf.state)
	}()
	defer rf.persist()
	reply.VoteGranted = true
	if rf.currentTerm > args.Term ||
		(rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateID) {
		reply.VoteGranted = false
	}
	if args.Term > rf.currentTerm {
		rf.state, rf.currentTerm, rf.votedFor = follower, args.Term, -1
		// rf.hrtBtTimer.Stop()
		// rf.electionTimer.Reset(electionDuration())
		rf.resetElection()
	}
	if !rf.logIsUpdated(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	if reply.VoteGranted {
		rf.state, rf.votedFor = follower, args.CandidateID
		// rf.hrtBtTimer.Stop()
		// rf.electionTimer.Reset(electionDuration())
		rf.resetElection()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("me:%d send requestvote to peer:%d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// defer rf.electionTimer.Reset(electionDuration())
	defer rf.resetElection()
	if rf.state == leader {
		return
	}
	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.logs) - 1 + rf.snapshotIndex,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
	DPrintf("me:%d term:%d lastLogIndex:%d start Election\n", rf.me, rf.currentTerm, args.LastLogIndex)
	vote := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(peer, args, reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			defer rf.persist()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = follower
				rf.resetElection()
			}
			if reply.Term != rf.currentTerm || rf.state != candidate {
				return
			}
			if reply.VoteGranted {
				vote++
			}
			if vote*2 > len(rf.peers) {
				rf.state = leader
				lastLogIndex := len(rf.logs) - 1 + rf.snapshotIndex
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = lastLogIndex + 1
					rf.matchIndex[i] = 0
				}
				DPrintf("me:%d become Leader term:%d lastLogIndex:%d\n", rf.me, rf.currentTerm, lastLogIndex)
				rf.doHeartBeatWithLock(true)
			}
		}(peer)
	}
}

func (rf *Raft) resetHeartbeart() {
	rf.nextHrtBtTime = time.Now().Add(HeartBeartTimeout)
}

func (rf *Raft) resetElection() {
	duration := electionDuration()
	rf.nextElectionTime = time.Now().Add(duration)
}

func (rf *Raft) doHeartBeatWithLock(heartBeart bool) {
	defer rf.resetHeartbeart()
	DPrintf("me:%d term:%d do heartBeart [%v]\n", rf.me, rf.currentTerm, heartBeart)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			rf.replicate(peer)
		}(peer)
	}
	rf.lastHrtBtTime = time.Now()
}
