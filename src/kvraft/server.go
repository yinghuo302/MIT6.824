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
	ExecTimeOut = time.Millisecond * 800
)

type RequestHistory struct {
	Reply RequestReply
	ReqId int64
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
	if len(data) == 0 {
		return
	}
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
	kv.mu.RLock()
	lastReq, ok := kv.lastReq[args.ClientId]
	if args.Op != GET_OP && ok && lastReq.ReqId == args.RequestId {
		reply.Value, reply.Err = lastReq.Reply.Value, lastReq.Reply.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	index, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		reply.Err, reply.Value = ErrWrongLeader, ""
		return
	}
	DPrintf("me:%d start %+v\n", kv.me, args)
	ch := make(chan *RequestReply)
	kv.mu.Lock()
	DPrintf("me:%d add channel Lock\n", kv.me)
	kv.notifier[index] = ch
	kv.mu.Unlock()
	DPrintf("me:%d add channel UnLock\n", kv.me)
	select {
	case r := <-ch:
		reply.Err, reply.Value = r.Err, r.Value
	case <-time.After(ExecTimeOut):
		reply.Err, reply.Value = ErrTimeout, ""
	}
	kv.mu.Lock()
	DPrintf("me:%d delete channel Lock\n", kv.me)
	delete(kv.notifier, index)
	kv.mu.Unlock()
	DPrintf("me:%d delete channel UnLock\n", kv.me)
}

func (kv *KVServer) execCmd(args *RequestArgs) *RequestReply {
	lastReq, ok := kv.lastReq[args.ClientId]
	if args.Op != GET_OP && ok && lastReq.ReqId == args.RequestId {
		return &lastReq.Reply
	}
	reply := &RequestReply{}
	if args.Op == GET_OP {
		reply.Value, reply.Err = kv.kvCache.Get(args.Key)
	} else if args.Op == APPEND_OP {
		reply.Value, reply.Err = "", kv.kvCache.Append(args.Key, args.Value)
	} else if args.Op == PUT_OP {
		reply.Value, reply.Err = "", kv.kvCache.Put(args.Key, args.Value)
	}
	if args.Op != GET_OP {
		kv.lastReq[args.ClientId] = RequestHistory{ReqId: args.RequestId, Reply: *reply}
	}
	term, isLeader := kv.rf.GetState()
	DPrintf("me:%d state:%d,%v  apply %+v result: %+v\n", kv.me, term, isLeader, args, reply)
	return reply
}

func (kv *KVServer) handleCommand() {
	for !kv.killed() {
		msg := <-kv.applyCh
		func(msg *raft.ApplyMsg) {
			kv.mu.Lock()
			DPrintf("me:%d handleCommnad Lock\n", kv.me)
			defer kv.mu.Unlock()
			defer DPrintf("me:%d handleCommnad UnLock\n", kv.me)
			if msg.SnapshotValid {
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.restoreSnapshot(msg.Snapshot)
				}
			} else if msg.CommandValid {
				args := msg.Command.(RequestArgs)
				reply := kv.execCmd(&args)
				currentTerm, isLeader := kv.rf.GetState()
				ch, ok := kv.notifier[msg.CommandIndex]
				if isLeader && msg.CommandTerm == currentTerm && ok {
					ch <- reply
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
		notifier:     make(map[int]chan *RequestReply),
		lastReq:      map[int64]RequestHistory{},
	}
	kv.restoreSnapshot(kv.persister.ReadSnapshot())
	go kv.handleCommand()
	// You may need initialization code here.

	return kv
}
