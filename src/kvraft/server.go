package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const (
	Debug       = false
	ExecTimeOut = time.Millisecond * 500
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type RequestHistory struct {
	reply RequestReply
	reqId int64
}

type KVServer struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	kvCache      KVCache
	maxraftstate int // snapshot if log grows this big
	notifier     map[int]chan *RequestReply
	lastReq      map[int64]RequestHistory
	persister    *raft.Persister
}

func (kv *KVServer) restoreSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvCache KVMemory
	var lastReq map[int64]RequestHistory

	if d.Decode(&kvCache) != nil ||
		d.Decode(&lastReq) != nil {
		log.Fatal("kv read persist err!")
	}
	kv.lastReq = lastReq
	kv.kvCache = &kvCache
}

func (kv *KVServer) saveSnapshot(index int) {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.kvCache); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.lastReq); err != nil {
		panic(err)
	}
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

func (kv *KVServer) HandleRequest(args *RequestArgs, reply *RequestReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(args)
	if !isLeader {
		reply.Err, reply.Value = ErrWrongLeader, ""
	}
	ch := make(chan *RequestReply)
	kv.notifier[index] = ch
	select {
	case r := <-ch:
		reply.Err, reply.Value = r.Err, r.Value
	case <-time.After(ExecTimeOut):
		reply.Err, reply.Value = ErrTimeout, ""
	}
	delete(kv.notifier, index)
}

func (kv *KVServer) execCmd(args *RequestArgs) (reply RequestReply) {
	if args.Op == GET_OP {
		reply.Value, reply.Err = kv.kvCache.Get(args.Key)
	} else if args.Op == APPEND_OP {
		reply.Value, reply.Err = "", kv.kvCache.Append(args.Key, args.Value)
	} else if args.Op == PUT_OP {
		reply.Value, reply.Err = "", kv.kvCache.Put(args.Key, args.Value)
	}
	return
}

func (kv *KVServer) handleCommand() {
	for !kv.killed() {
		msg := <-kv.applyCh
		func(msg *raft.ApplyMsg) {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			if msg.SnapshotValid {
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.restoreSnapshot(msg.Snapshot)
				}
			} else if msg.CommandValid {
				req := msg.Command.(RequestArgs)
				lastReq, ok := kv.lastReq[req.ClientId]
				if req.Op != GET_OP && ok && lastReq.reqId != req.RequestId {
					kv.notifier[msg.CommandIndex] <- &lastReq.reply
					return
				}
				reply := kv.execCmd(&req)
				kv.notifier[msg.CommandIndex] <- &reply
				if req.Op != GET_OP {
					kv.lastReq[req.ClientId] = RequestHistory{reqId: req.RequestId, reply: reply}
				}
				kv.saveSnapshot(msg.CommandIndex)
			}
		}(&msg)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(RequestArgs{})
	labgob.Register(RequestReply{})
	labgob.Register(RequestHistory{})
	labgob.Register(KVMemory{})
	ch := make(chan raft.ApplyMsg)
	kv := &KVServer{
		me:           me,
		dead:         0,
		maxraftstate: maxraftstate,
		applyCh:      ch,
		rf:           raft.Make(servers, me, persister, ch),
		kvCache:      NewKVCache(),
		persister:    persister,
	}
	go kv.handleCommand()
	// You may need initialization code here.

	return kv
}
