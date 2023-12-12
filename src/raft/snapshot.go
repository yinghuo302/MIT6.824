package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	//Offset            int
	Data []byte
	//Done bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}

	if args.Term > rf.currentTerm || rf.state != follower {
		// rf.changeRole(Role_Follower)
		rf.votedFor = -1
		rf.currentTerm = args.Term
		// rf.resetElectionTimer()
		rf.persist()
	}

	if rf.snapshotIndex >= args.LastIncludedIndex {
		return
	}

	lastIncludedIndex := args.LastIncludedIndex
	lastIncludedTerm := args.LastIncludedTerm
	lastIndex := len(rf.logs) - 1 + rf.snapshotIndex
	if lastIncludedIndex > lastIndex {
		rf.logs = make([]Entry, 1)
	} else {
		installLen := lastIncludedIndex - rf.snapshotIndex
		rf.logs = rf.logs[installLen:]
		rf.logs[0].Command = nil
	}
	rf.logs[0].Term = lastIncludedTerm

	// rf.lastSnapshotIndex, rf.lastSnapshotTerm = lastIncludedIndex, lastIncludedTerm
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex

	// rf.persister.SaveStateAndSnapshot(rf.getPersistData(), args.Data)

	rf.applyChan <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

}

// 向指定节点发送快照
func (rf *Raft) sendInstallSnapshotToPeer(server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotIndex,
		// LastIncludedTerm:  rf.currentTerm,
		Data: rf.persister.ReadSnapshot(),
	}
	reply := &InstallSnapshotReply{}
	rf.peers[server].Call("Raft.InstallSnapshot", &args, reply)
	if rf.state != leader || args.Term != rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		// rf.changeRole(Role_Follower)
		rf.currentTerm = reply.Term
		// rf.resetElectionTimer()
		rf.persist()
		return
	}

	if args.LastIncludedIndex > rf.matchIndex[server] {
		rf.matchIndex[server] = args.LastIncludedIndex
	}
	if args.LastIncludedIndex+1 > rf.nextIndex[server] {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
	}
}
