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
		rf.hrtBtTimer.Stop()
		rf.electionTimer.Reset(electionDuration())
	}

	if rf.snapshotIndex >= args.LastIncludedIndex {
		return
	}

	rf.applyChan <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

}

func (rf *Raft) installSnapshotToPeer(server int) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.currentTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := &InstallSnapshotReply{}
	rf.peers[server].Call("Raft.InstallSnapshot", &args, reply)
	if rf.state != leader || args.Term != rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.state, rf.votedFor, rf.currentTerm = follower, -1, reply.Term
		rf.hrtBtTimer.Stop()
		rf.electionTimer.Reset(electionDuration())
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

// type InstallSnapshotArgs struct {
// 	Term              int
// 	LeaderId          int
// 	LastIncludedIndex int
// 	LastIncludedTerm  int
// 	//Offset            int
// 	Data []byte
// 	//Done bool
// }

// type InstallSnapshotReply struct {
// 	Term int
// }

// func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	defer func() {
// 		DPrintf("me:%d term:%d receive snapshot from %d index:%d term:%d reply:%+v snapshotindex:%d", rf.me, rf.currentTerm, args.LeaderId, args.LastIncludedIndex, args.Term, reply, rf.snapshotIndex)
// 	}()
// 	reply.Term = rf.currentTerm
// 	if rf.currentTerm > args.Term {
// 		return
// 	}

// 	if args.Term > rf.currentTerm || rf.state != follower {
// 		rf.state = follower
// 		rf.votedFor = -1
// 		rf.currentTerm = args.Term
// 		rf.electionTimer.Reset(electionDuration())
// 		rf.persist()
// 	}

// 	//如果自身快照包含的最后一个日志>=leader快照包含的最后一个日志，就没必要接受了
// 	if rf.snapshotIndex >= args.LastIncludedIndex {
// 		return
// 	}

// 	/********以下内容和CondInstallSnapshot的操作是相同的，因为不知道为什么在lab4B中只要调用CondInstallSnapshot函数就会陷入阻塞，因此将操作逻辑复制到这里一份，lab4中就没有调用CondInstallSnapshot函数了***********/

// 	/***********************************/

// 	//接收发来的快照，并提交一个命令处理
// 	rf.applyChan <- ApplyMsg{
// 		SnapshotValid: true,
// 		Snapshot:      args.Data,
// 		SnapshotTerm:  args.LastIncludedTerm,
// 		SnapshotIndex: args.LastIncludedIndex,
// 	}

// }

// // 向指定节点发送快照
// func (rf *Raft) installSnapshotToPeer(server int) {
// 	args := InstallSnapshotArgs{
// 		Term:              rf.currentTerm,
// 		LeaderId:          rf.me,
// 		LastIncludedIndex: rf.snapshotIndex,
// 		LastIncludedTerm:  rf.lastSnapshotTerm,
// 		Data:              rf.persister.ReadSnapshot(),
// 	}
// 	rf.mu.Unlock()
// 	reply := &InstallSnapshotReply{}
// 	rf.peers[server].Call("Raft.InstallSnapshot", &args, reply)
// 	rf.mu.Lock()
// 	// defer func() {
// 	// 	DPrintf("me:%d term:%d receive snapshot from %d index:%d term:%d reply:%+v snapshotindex:%d", rf.me, rf.currentTerm, args.LeaderId, args.LastIncludedIndex, args.Term, reply, rf.snapshotIndex)
// 	// }()
// 	if rf.state != follower || args.Term != rf.currentTerm {
// 		return
// 	}
// 	if reply.Term > rf.currentTerm {
// 		rf.state = follower
// 		rf.currentTerm = reply.Term
// 		rf.electionTimer.Reset(electionDuration())
// 		rf.persist()
// 		return
// 	}

// 	if args.LastIncludedIndex > rf.matchIndex[server] {
// 		rf.matchIndex[server] = args.LastIncludedIndex
// 	}
// 	if args.LastIncludedIndex+1 > rf.nextIndex[server] {
// 		rf.nextIndex[server] = args.LastIncludedIndex + 1
// 	}
// }

// func (rf *Raft) Snapshot(index int, snapshot []byte) {
// 	// Your code here (2D).
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	snapshotIndex := rf.snapshotIndex
// 	if snapshotIndex >= index {
// 		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
// 		return
// 	}
// 	oldLastSnapshotIndex := rf.snapshotIndex
// 	rf.lastSnapshotTerm = rf.logs[index-rf.snapshotIndex].Term
// 	rf.snapshotIndex = index
// 	//删掉index前的所有日志
// 	rf.logs = rf.logs[index-oldLastSnapshotIndex:]
// 	//0位置就是快照命令
// 	rf.logs[0].Term = rf.lastSnapshotTerm
// 	rf.logs[0].Command = nil
// 	rf.persister.Save(rf.getPersistData(), snapshot)
// 	// DPrintf("{Node %v}'s state is {role %v,term %v,commitIndex %v,lastApplied %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller", rf.me, rf.role, rf.currentTerm, rf.commitIndex, rf.lastApplied, index, snapshotIndex)
// }

// // A service wants to switch to snapshot.  Only do so if Raft hasn't
// // have more recent info since it communicate the snapshot on applyCh.
// //
// // 其实CondInstallSnapshot中的逻辑可以直接在InstallSnapshot中来完成，让CondInstallSnapshot成为一个空函数，这样可以减少锁的获取和释放
// func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	// Your code here (2D).
// 	//installLen := lastIncludedIndex - rf.lastSnapshotIndex
// 	//if installLen >= len(rf.logs)-1 {
// 	//	rf.logs = make([]LogEntry, 1)
// 	//	rf.logs[0].Term = lastIncludedTerm
// 	//} else {
// 	//	rf.logs = rf.logs[installLen:]
// 	//}
// 	lastIndex := len(rf.logs) - 1 + rf.snapshotIndex
// 	if lastIncludedIndex > lastIndex {
// 		rf.logs = make([]Entry, 1)
// 	} else {
// 		installLen := lastIncludedIndex - rf.snapshotIndex
// 		rf.logs = rf.logs[installLen:]
// 		rf.logs[0].Command = nil
// 	}
// 	//0处是空日志，代表了快照日志的标记
// 	rf.logs[0].Term = lastIncludedTerm

// 	rf.snapshotIndex, rf.lastSnapshotTerm = lastIncludedIndex, lastIncludedTerm
// 	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
// 	//保存快照和状态
// 	rf.persister.Save(rf.getPersistData(), snapshot)
// 	return true
// }
