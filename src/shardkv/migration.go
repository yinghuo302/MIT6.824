package shardkv

import (
	"sync"

	"6.5840/raft"
)

func (kv *ShardKV) pullShards() {
	kv.mu.RLock()
	shardMp := kv.getShardIDsByStatus(Pulling)
	var wg sync.WaitGroup
	for gid, shardIDs := range shardMp {
		wg.Add(1)
		go func(servers []string, configNum int, shardIDs []int) {
			DPrintf("me:%d gid:%d pull shards %v", kv.me, kv.gid, shardIDs)
			defer wg.Done()
			args, reply := &PullShardArgs{configNum, shardIDs}, &PullShardReply{}
			for _, server := range servers {
				if sendRPC(kv.make_end(server), "ShardKV.PullShards", args, reply) && reply.Err == OK {
					break
				}
			}
			kv.rf.Start(Command{PullShards, *reply})
		}(kv.oldConfig.Groups[gid], kv.config.Num, shardIDs)
	}
	kv.mu.RUnlock()
	wg.Wait()
}

func (kv *ShardKV) getShardIDsByStatus(state ShardStatus) map[int][]int {
	mp := make(map[int][]int)
	for sid, shard := range kv.shards {
		if shard.Status == state {
			gid := kv.oldConfig.Shards[sid]
			mp[gid] = append(mp[gid], sid)
		}
	}
	return mp
}

func (kv *ShardKV) PullShards(args *PullShardArgs, reply *PullShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if kv.config.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}
	reply.Shards = make(map[int]*Shard)
	for _, shardID := range args.ShardIds {
		reply.Shards[shardID] = kv.shards[shardID].Copy()
	}
	reply.ConfigNum, reply.Err = args.ConfigNum, OK
}

func (kv *ShardKV) applyPullShards(msg *raft.ApplyMsg) {
	shardsInfo := msg.Command.(Command).Cmd.(PullShardReply)
	if shardsInfo.ConfigNum != kv.config.Num {
		return
	}
	if shardsInfo.ConfigNum == kv.config.Num {
		for shardId, newShard := range shardsInfo.Shards {
			oldShard := kv.shards[shardId]
			if oldShard.Status == Pulling {
				newShard.Status = Waiting
				kv.shards[shardId] = newShard.Copy()
			}
		}
	}
}

func (kv *ShardKV) deleteShards() {
	kv.mu.RLock()
	groupToShards := kv.getShardIDsByStatus(Waiting)
	currConfNum := kv.config.Num
	wg := sync.WaitGroup{}
	for gid, shards := range groupToShards {
		wg.Add(1)
		DPrintf("me:%d gid:%d request to delete shards %v to %d", kv.me, kv.gid, shards, gid)
		servers := kv.oldConfig.Groups[gid]
		go func(servers []string, shards []int, confNum int) {
			defer wg.Done()
			args := &DelShardsArgs{
				ConfNum:  confNum,
				ShardIDs: shards,
			}
			reply := &DelShardsReply{}
			for _, server := range servers {
				shardOwner := kv.make_end(server)
				if shardOwner.Call("ShardKV.DeleteShards", args, reply) && reply.Err == OK {
					kv.rf.Start(Command{DeleteShards, *args})
					break
				}
			}
		}(servers, shards, currConfNum)
	}

	kv.mu.RUnlock()
	wg.Wait()
}

func (kv *ShardKV) DeleteShards(args *DelShardsArgs, reply *CommonReply) {
	kv.commitToRaft(Command{DeleteShards, *args}, reply)
}

func (kv *ShardKV) applyDeleteShards(msg *raft.ApplyMsg) {
	args, reply := msg.Command.(Command).Cmd.(DelShardsArgs), &CommonReply{Err: OK}
	if args.ConfNum == kv.config.Num {
		for _, sid := range args.ShardIDs {
			shard := kv.shards[sid]
			if shard == nil {
				continue
			}
			if shard.Status == Waiting { // 当前 raft 组在当前 config 下负责管理此分片
				shard.Status = Serving
			} else if shard.Status == Expire { // 当前 raft 组在当前 config 下不负责管理此分片，在oldConfig中负责
				delete(kv.shards, sid)
			}
		}
	}
	currentTerm, isLeader := kv.rf.GetState()
	ch, ok := kv.notifier[msg.CommandIndex]
	if isLeader && msg.CommandTerm == currentTerm && ok {
		ch <- reply
	}
}
