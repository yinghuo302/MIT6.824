package raft

// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int     // leader's term
	LeaderId     int     // so follower canredirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of preLogIndex entry
	Entries      []Entry // log entries to store (empty for heart beat)
	LeaderCommit int     // leader's commitIndex
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term         int  // currentTerm, for leader to update itself
	Success      bool // true if follower contained entry matching preLogIndex and prevLogTerm
	NextLogTerm  int
	NextLogIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		reply.Success, reply.Term = false, rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	rf.state = follower
	rf.electionTimer.Reset(electionDuration())

	lastLogIndex := len(rf.logs) - 1 + rf.snapshotIndex
	if args.PrevLogIndex < rf.snapshotIndex {
		reply.Success, reply.NextLogIndex = false, rf.snapshotIndex
	} else if args.PrevLogIndex > lastLogIndex {
		reply.Success, reply.NextLogIndex = false, lastLogIndex+1
	} else if args.PrevLogTerm == rf.logs[args.PrevLogIndex-rf.snapshotIndex].Term {
		rf.logs = append(rf.logs[:args.PrevLogIndex-rf.snapshotIndex+1], args.Entries...)
		reply.Success, reply.NextLogIndex = true, len(rf.logs)-1+rf.snapshotIndex
	} else {
		index, term := args.PrevLogIndex, rf.logs[args.PrevLogIndex-rf.snapshotIndex].Term
		for index > rf.snapshotIndex && rf.logs[index-rf.snapshotIndex].Term == term {
			index--
		}
		reply.Success, reply.NextLogIndex = false, index+1
	}

	if reply.Success {
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) replicate(peer int) {
	rf.mu.Lock()
	if rf.state != leader {
		rf.mu.Unlock()
		return
	}
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[peer] - 1,
		PrevLogTerm:  rf.logs[rf.nextIndex[peer]-1].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      rf.logs[rf.nextIndex[peer]-rf.snapshotIndex:],
	}
	reply := &AppendEntriesReply{}
	if !rf.sendAppendEntries(peer, args, reply) {
		return
	}
	rf.nextIndex[peer] = reply.NextLogIndex
	if reply.Success {
		rf.matchIndex[peer] = reply.NextLogIndex - 1
	}
}

func (rf *Raft) applyCommand() {
	for !rf.killed() {
		rf.applyChan <- ApplyMsg{}
	}
}
