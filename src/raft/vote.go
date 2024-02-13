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
	ms := 200 + (rand.Int63() % 200)
	return time.Duration(ms) * time.Millisecond
}

func heartBeatDuration() time.Duration {
	return time.Duration(100) * time.Microsecond
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
	}
	if !rf.logIsUpdated(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	if reply.VoteGranted {
		rf.state, rf.votedFor = follower, args.CandidateID
		rf.electionTimer.Reset(electionDuration())
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("me: %d send requestvote to peer:%d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.electionTimer.Reset(electionDuration())
	if rf.state == leader {
		return
	}
	DPrintf("me:%d start Election\n", rf.me)
	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.logs) - 1,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
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
				rf.electionTimer.Reset(electionDuration())
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
					rf.matchIndex[i] = lastLogIndex
				}
				DPrintf("me:%d become Leader term:%d lastLogIndex:%d\n", rf.me, rf.currentTerm, lastLogIndex)
				rf.hrtBtTimer.Reset(0)
			}
		}(peer)
	}
}

func (rf *Raft) doHeartBeat() {
	if rf.state != leader {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.doHeartBeatWithLock()
}

func (rf *Raft) doHeartBeatWithLock() {
	defer rf.hrtBtTimer.Reset(heartBeatDuration())
	DPrintf("me:%d term: %d do heartBeart\n", rf.me, rf.currentTerm)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			rf.replicate(peer)
		}(peer)
		// rf.replicateCh[peer] <- struct{}{}
	}
}
