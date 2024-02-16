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
	defer func() {
		DPrintf("me:%d term:%d receive %d entries from [%d,%d] val:%+v prev:%d,then have %d entries,reply:%+v\n", rf.me, rf.currentTerm, len(args.Entries), args.LeaderId, args.Term, args.Entries, args.PrevLogIndex, len(rf.logs)+rf.snapshotIndex, reply)
	}()
	if args.Term < rf.currentTerm {
		reply.Success, reply.Term = false, rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	rf.state = follower
	rf.resetElection()
	lastLogIndex := len(rf.logs) - 1 + rf.snapshotIndex
	if args.PrevLogIndex < rf.snapshotIndex {
		DPrintf("me:%d PrevLogIndex:%d snapshotIndex:%d\n", rf.me, args.PrevLogIndex, rf.snapshotIndex)
		reply.Success, reply.NextLogIndex = false, rf.snapshotIndex+1
	} else if args.PrevLogIndex > lastLogIndex {
		DPrintf("me:%d PrevLogIndex:%d lastLogIndex:%d\n", rf.me, args.PrevLogIndex, lastLogIndex)
		reply.Success, reply.NextLogIndex = false, lastLogIndex+1
	} else if args.PrevLogTerm == rf.logs[args.PrevLogIndex-rf.snapshotIndex].Term {
		index := args.PrevLogIndex - rf.snapshotIndex + 1
		for i, entry := range args.Entries {
			if index >= len(rf.logs) || rf.logs[index].Term != entry.Term {
				rf.logs = append(append([]Entry{}, rf.logs[:index]...), args.Entries[i:]...)
				break
			}
			index++
		}
		reply.Success, reply.NextLogIndex = true, args.PrevLogIndex+1+len(args.Entries)
	} else {
		index, term := args.PrevLogIndex, rf.logs[args.PrevLogIndex-rf.snapshotIndex].Term
		for index > rf.snapshotIndex && rf.logs[index-rf.snapshotIndex].Term == term {
			index--
		}
		reply.Success, reply.NextLogIndex = false, index+1
		DPrintf("me:%d argsIndex:%d argsTerm:%d meTerm:%d back to %d\n", rf.me, args.PrevLogIndex, args.Term, term, reply.NextLogIndex)
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
		Entries:      append([]Entry{}, rf.logs[rf.nextIndex[peer]-rf.snapshotIndex:]...),
	}
	reply := &AppendEntriesReply{}
	rf.mu.Unlock()
	ok := sendRPCWithTimeout(rf.peers[peer], "Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	if !ok || rf.state != leader {
		return
	}
	defer rf.persist()
	defer DPrintf("me:%d after seed entry to peer:%d match:%d next:%d\n", rf.me, peer, rf.matchIndex[peer], rf.nextIndex[peer])
	if !reply.Success || reply.NextLogIndex > rf.nextIndex[peer] {
		rf.nextIndex[peer] = reply.NextLogIndex
	}
	if reply.Success {
		rf.matchIndex[peer] = reply.NextLogIndex - 1
		idx, lastLogIndex := rf.commitIndex+1, len(rf.logs)-1+rf.snapshotIndex
		for ; idx <= lastLogIndex; idx++ {
			consensus := 1
			for _, match := range rf.matchIndex {
				if match >= idx {
					consensus++
				}
			}
			if consensus*2 <= len(rf.peers) {
				break
			}
		}
		if rf.commitIndex < idx-1 && rf.logs[idx-1-rf.snapshotIndex].Term == rf.currentTerm {
			DPrintf("me:%d term:%d commitIndex:%d\n", rf.me, rf.currentTerm, rf.commitIndex)
			rf.commitIndex = idx - 1
			rf.applyCond.Signal()
		}
	}
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
		lastApplied, commitIdx := rf.lastApplied, rf.commitIndex
		entries := append([]Entry{}, rf.logs[lastApplied+1-rf.snapshotIndex:commitIdx+1-rf.snapshotIndex]...)
		DPrintf("me:%d copy log from %d to %d try to apply\n", rf.me, lastApplied, commitIdx)
		rf.mu.Unlock()

		rf.snapshotMu.Lock() // apply时不允许snapshot
		for idx, entry := range entries {
			DPrintf("me:%d apply Command index:%d %+v\n", rf.me, idx+1+lastApplied, entry.Command)
			rf.applyChan <- ApplyMsg{
				Command:      entry.Command,
				CommandIndex: idx + 1 + lastApplied,
				CommandValid: true,
				CommandTerm:  entry.Term,
			}
		}
		rf.snapshotMu.Unlock()

		rf.mu.Lock()
		if commitIdx > rf.lastApplied {
			rf.lastApplied = commitIdx
		}
		rf.mu.Unlock()
	}
}
