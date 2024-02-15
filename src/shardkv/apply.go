package shardkv

import (
	"time"

	"6.5840/raft"
)

func (kv *ShardKV) applyCommand() {
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.mu.Lock()
		kv.getHandler(&msg)(&msg)
		kv.saveSnapshot(msg.CommandIndex)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) getHandler(msg *raft.ApplyMsg) func(*raft.ApplyMsg) {
	if msg.SnapshotValid {
		return kv.applySnapShot
	}
	switch msg.Command.(Command).Op {
	case Operation:
		return kv.applyOperation
	case Configuration:
		return kv.applyConfiguration
	case InsertShards:
		return kv.applyPullShards
	case DeleteShards:
		return kv.applyDeleteShards
	case EmptyEntry:
		return kv.applyEmptyEntry
	default:
		panic("Unkown Command Type")
	}
}

func (kv *ShardKV) applyEmptyEntry(msg *raft.ApplyMsg) {

}

func (kv *ShardKV) applyOperation(msg *raft.ApplyMsg) {
	args := msg.Command.(Command).Cmd.(CommonArgs)
	reply := kv.shards[key2shard(args.Key)].Exec(args.ClientId, args.RequestId, &args)
	currentTerm, isLeader := kv.rf.GetState()
	ch, ok := kv.notifier[msg.CommandIndex]
	if isLeader && msg.CommandTerm == currentTerm && ok {
		ch <- reply
	}
}

func (kv *ShardKV) canServe(shardID int) bool {
	return kv.config.Shards[shardID] == kv.gid &&
		(kv.shards[shardID].Status == Serving || kv.shards[shardID].Status == Waiting)
}

func (kv *ShardKV) HandleRequest(args *CommonArgs, reply *CommonReply) {
	kv.mu.RLock()
	shard := key2shard(args.Key)
	if !kv.canServe(shard) {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	} else if lastRep := kv.shards[shard].GetLastReq(args.ClientId, args.RequestId); lastRep != nil {
		reply.Value, reply.Err = lastRep.Value, lastRep.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	kv.commitToRaft(args, reply)
}

func (kv *ShardKV) commitToRaft(command interface{}, reply *CommonReply) {
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err, reply.Value = ErrWrongLeader, ""
	}
	ch := make(chan *CommonReply)
	kv.mu.Lock()
	kv.notifier[index] = ch
	kv.mu.Lock()
	select {
	case r := <-ch:
		reply.Err, reply.Value = r.Err, r.Value
	case <-time.After(ExecTimeOut):
		reply.Err, reply.Value = ErrTimeout, ""
	}
	kv.mu.Lock()
	delete(kv.notifier, index)
	kv.mu.Lock()
}
