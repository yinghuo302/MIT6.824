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
	// DPrintf("me:%d receive entries need get lock\n", rf.me)
	rf.mu.Lock()
	// DPrintf("me:%d receive entries already get lock\n", rf.me)
	defer rf.mu.Unlock()
	defer rf.persist()
	defer func() {
		// DPrintf("me:%d term:%d receive %d entries val:%+v prev:%d,then have %d entries,reply:%+v\n", rf.me, rf.currentTerm, len(args.Entries), args.Entries, args.PrevLogIndex, len(rf.logs), reply)
		DPrintf("me:%d term:%d receive %d entries prev:%d,then have %d entries,reply:%+v\n", rf.me, rf.currentTerm, len(args.Entries), args.PrevLogIndex, len(rf.logs), reply)
	}()
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
		reply.Success, reply.NextLogIndex = true, len(rf.logs)+rf.snapshotIndex
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
			rf.applyCond.Signal()
		}
	}
}

func (rf *Raft) sendAppendEntries(peer int) {
	prevIndex := rf.nextIndex[peer] - 1
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  rf.logs[prevIndex-rf.snapshotIndex].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      rf.logs[rf.nextIndex[peer]-rf.snapshotIndex:],
	}
	reply := &AppendEntriesReply{}
	ok := sendRPCWithTimeout(rf.peers[peer], "Raft.AppendEntries", args, reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.state = follower
			rf.currentTerm = reply.Term
		} else {
			rf.nextIndex[peer] = reply.NextLogIndex
			if reply.Success {
				rf.matchIndex[peer] = reply.NextLogIndex - 1
				idx := rf.commitIndex + 1
				for ; idx < len(rf.logs); idx++ {
					consensus := 1
					for i := 0; i < len(rf.peers); i++ {
						if rf.matchIndex[i] >= idx {
							consensus++
						}
					}
					// DPrintf("[term %d] [me %d] log entry %d with consenus %d\n", rf.currentTerm, rf.me, idx, consensus)
					if consensus*2 <= len(rf.peers) || rf.logs[idx].Term != rf.currentTerm {
						break
					}
				}
				if rf.commitIndex < idx-1 {
					rf.commitIndex = idx - 1
					rf.applyCond.Signal()
				}
			}
		}
		DPrintf("me:%d after seed entry to peer:%d match:%d next:%d\n", rf.me, peer, rf.matchIndex[peer], rf.nextIndex[peer])
	}
	// if !ok || !reply.Success {
	// 	go func() {
	// 		time.Sleep(time.Duration(30) * time.Second)
	// 		rf.replicate(peer)
	// 	}()
	// }
}

func (rf *Raft) replicate(peer int) {
	rf.mu.Lock()
	// DPrintf("me:%d replicate already get lock\n", rf.me)
	defer rf.mu.Unlock()
	if rf.state != leader {
		return
	}
	prevIndex := rf.nextIndex[peer] - 1
	if prevIndex < rf.snapshotIndex {
		rf.installSnapshotToPeer(peer)
	} else {
		rf.sendAppendEntries(peer)
	}
	// DPrintf("me:%d replicate release lock\n", rf.me)
}

func (rf *Raft) applyCommand() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		msgs := make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for idx := rf.lastApplied + 1; idx <= rf.commitIndex; idx++ {
			msgs = append(msgs, ApplyMsg{
				Command:      rf.logs[idx-rf.snapshotIndex].Command,
				CommandIndex: idx,
				CommandValid: true,
			})
		}
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
		for _, msg := range msgs {
			rf.applyChan <- msg
		}
	}
}
