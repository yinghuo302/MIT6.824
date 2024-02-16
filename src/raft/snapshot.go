package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer func() {
		DPrintf("me:%d term:%d receive snapshot from %d index:%d term:%d reply:%+v snapshotindex:%d\n", rf.me, rf.currentTerm, args.LeaderId, args.LastIncludedIndex, args.Term, reply, rf.snapshotIndex)
	}()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}

	if args.Term > rf.currentTerm || rf.state != follower {
		rf.state, rf.votedFor, rf.currentTerm = follower, -1, reply.Term
		// rf.hrtBtTimer.Stop()
		rf.resetElection()
		// rf.electionTimer.Reset(electionDuration())
	}

	if rf.snapshotIndex >= args.LastIncludedIndex {
		return
	}

	go func() {
		rf.snapshotMu.Lock()
		rf.applyChan <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.snapshotMu.Unlock()
	}()

}

func (rf *Raft) installSnapshotToPeer(server int) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.logs[0].Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := &InstallSnapshotReply{}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, reply)
	rf.mu.Lock()
	if !ok || rf.state != leader || args.Term != rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.state, rf.votedFor, rf.currentTerm = follower, -1, reply.Term
		// rf.hrtBtTimer.Stop()
		// rf.electionTimer.Reset(electionDuration())
		rf.resetElection()
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.snapshotIndex >= index {
		DPrintf("me:%v rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v\n", rf.me, index, rf.snapshotIndex, rf.currentTerm)
		return
	}
	term := rf.logs[index-rf.snapshotIndex].Term
	rf.logs = append([]Entry{}, rf.logs[index-rf.snapshotIndex:]...)
	oldIndex := rf.snapshotIndex
	rf.snapshotIndex = index
	rf.logs[0].Term = term
	rf.logs[0].Command = nil
	rf.persister.Save(rf.getPersistData(), snapshot)
	DPrintf("me:%v state:%v term:%v commitIndex:%v lastApplied:%v newSnapshotIndex:%v oldSnapshotIndex:%v\n", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.snapshotIndex, oldIndex)
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("me:%d CondInstallSnapshot term:%d,index:%d\n", rf.me, lastIncludedTerm, lastIncludedIndex)
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}
	lastIndex := len(rf.logs) - 1 + rf.snapshotIndex
	if lastIncludedIndex > lastIndex {
		rf.logs = make([]Entry, 1)
	} else {
		rf.logs = append([]Entry{}, rf.logs[lastIncludedIndex-rf.snapshotIndex:]...)
		rf.logs[0].Command = nil
	}
	rf.logs[0].Term = lastIncludedTerm

	rf.snapshotIndex = lastIncludedIndex
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
	rf.persister.Save(rf.getPersistData(), snapshot)
	return true
}
