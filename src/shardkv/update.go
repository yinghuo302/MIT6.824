package shardkv

import (
	"6.5840/raft"
	"6.5840/shardctrler"
)

func (kv *ShardKV) fetchConfig() {
	canPerformNextConfig := true
	kv.mu.RLock()
	for _, shard := range kv.shards {
		if shard.Status != Serving {
			canPerformNextConfig = false
			break
		}
	}
	currentConfigNum := kv.config.Num
	kv.mu.RUnlock()
	if canPerformNextConfig {
		nextConfig := kv.sc.Query(currentConfigNum + 1)
		if nextConfig.Num == currentConfigNum+1 {
			kv.rf.Start(Command{Configuration, nextConfig})
		}
	}
}

func (kv *ShardKV) applyConfiguration(msg *raft.ApplyMsg) {
	nextConfig := msg.Command.(Command).Cmd.(shardctrler.Config)
	if nextConfig.Num == kv.config.Num+1 {
		for sid, gid := range nextConfig.Shards {
			if gid == kv.gid && kv.config.Shards[sid] != gid {
				kv.shards[sid] = NewShard(Pulling)
			} else if gid != kv.gid && kv.config.Shards[sid] == gid {
				kv.shards[sid].Status = Expire
			}
		}
		kv.oldConfig = kv.config
		kv.config = nextConfig

	}
}
