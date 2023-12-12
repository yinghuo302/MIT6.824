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
	ms := 50 + (rand.Int63() % 300)
	return time.Duration(ms) * time.Millisecond
}

func heartBeatDuration() time.Duration {
	return time.Duration(30) * time.Microsecond
}

func (rf *Raft) logIsUpdated(lastTerm, lastIndex int) bool {
	return lastTerm > rf.logs[len(rf.logs)-1].Term ||
		(lastTerm == rf.logs[len(rf.logs)-1].Term && lastIndex >= len(rf.logs)-1+rf.snapshotIndex)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 调用前已加锁
func (rf *Raft) startElection() {
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
				rf.doHeartBeat()
			}
		}(peer)
	}
}

// TODO: check lock
func (rf *Raft) doHeartBeat() {
	if rf.state == leader {
		return
	}
	args := &AppendEntriesArgs{
		LeaderId: rf.me,
		Term:     rf.currentTerm,
		Entries:  make([]Entry, 0),
	}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := &AppendEntriesReply{}
			rf.sendAppendEntries(peer, args, reply)
		}(peer)
	}
	rf.hrtBtTimer.Reset(heartBeatDuration())
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.hrtBtTimer.C:
			rf.mu.Lock()
			rf.doHeartBeat()
			rf.mu.Unlock()
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.startElection()
			rf.mu.Unlock()
		}
	}
}
