package shardkv

import (
	"bytes"
	"log"

	"6.5840/labgob"
	"6.5840/raft"
)

func (kv *ShardKV) restoreSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	shards := make(map[int]*Shard)

	if d.Decode(&shards) != nil {
		log.Fatal("kv read persist err!")
	}
	kv.shards = shards
}

func (kv *ShardKV) saveSnapshot(index int) {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.shards); err != nil {
		panic(err)
	}
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

func (kv *ShardKV) applySnapShot(msg *raft.ApplyMsg) {
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.restoreSnapshot(msg.Snapshot)
	}
}
