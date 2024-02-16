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

	if d.Decode(&kv.shards) != nil || d.Decode(&kv.config) != nil || d.Decode(&kv.oldConfig) != nil {
		log.Fatal("kv read persist err!")
	}
	return
}

func (kv *ShardKV) saveSnapshot(index int) {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.shards) != nil || e.Encode(kv.config) != nil || e.Encode(kv.oldConfig) != nil {
		panic("save snapshot error")
	}
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

func (kv *ShardKV) applySnapShot(msg *raft.ApplyMsg) {
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.restoreSnapshot(msg.Snapshot)
	}
}
