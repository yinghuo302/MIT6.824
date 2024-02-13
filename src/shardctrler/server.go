package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type RequestHistory struct {
	ReqId int64
	Reply CommandReply
}

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32
	// Your data here.
	notifier map[int]chan *CommandReply
	configs  []Config // indexed by config num
	lastReq  map[int64]RequestHistory
}

const (
	ExecTimeOut = time.Millisecond * 500
)

func copyConfig(cfg *Config) Config {
	config := Config{
		Num:    cfg.Num,
		Shards: cfg.Shards,
		Groups: make(map[int][]string),
	}
	for gid, s := range cfg.Groups {
		config.Groups[gid] = append([]string{}, s...)
	}
	return config
}

func (sc *ShardCtrler) HandleRequest(args *CommandArgs, reply *CommandReply) {
	sc.mu.RLock()
	if args.Op == Query_OP && args.Num >= 0 && args.Num < len(sc.configs) {
		reply.Config, reply.Err = sc.configs[args.Num], OK
		sc.mu.RUnlock()
		return
	}
	lastReq, ok := sc.lastReq[args.ClientId]
	if args.Op != Query_OP && ok && lastReq.ReqId == args.CommandId {
		reply.Config, reply.Err = lastReq.Reply.Config, lastReq.Reply.Err
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()
	index, _, isLeader := sc.rf.Start(*args)
	if !isLeader {
		reply.Err, reply.Config = ErrWrongLeader, Config{}
		return
	}
	DPrintf("me:%d start %+v\n", sc.me, args)
	ch := make(chan *CommandReply)
	sc.mu.Lock()
	sc.notifier[index] = ch
	sc.mu.Unlock()
	select {
	case r := <-ch:
		reply.Err, reply.Config = r.Err, r.Config
	case <-time.After(ExecTimeOut):
		reply.Err, reply.Config = ErrTimeout, Config{}
	}
	sc.mu.Lock()
	delete(sc.notifier, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) execCmd(args *CommandArgs) *CommandReply {
	lastReq, ok := sc.lastReq[args.ClientId]
	if args.Op != Query_OP && ok && lastReq.ReqId == args.CommandId {
		return &lastReq.Reply
	}
	reply := &CommandReply{Err: OK}
	switch args.Op {
	case Query_OP:
		if args.Num >= 0 && args.Num < len(sc.configs) {
			reply.Config = sc.configs[args.Num]
		} else {
			reply.Config = sc.configs[len(sc.configs)-1]
		}
	case Join_OP:
		conf := copyConfig(&sc.configs[len(sc.configs)-1])
		conf.Num += 1
		for k, v := range args.Servers {
			conf.Groups[k] = v
		}
		redistributeShards(&conf)
		sc.configs = append(sc.configs, conf)
	case Leave_OP:
		conf := copyConfig(&sc.configs[len(sc.configs)-1])
		conf.Num += 1
		for _, gid := range args.GIDs {
			delete(conf.Groups, gid)
		}
		redistributeShards(&conf)
		sc.configs = append(sc.configs, conf)
	case Move_OP:
		conf := copyConfig(&sc.configs[len(sc.configs)-1])
		conf.Num += 1
		conf.Shards[args.Shard] = args.GID
		sc.configs = append(sc.configs, conf)
	}
	if args.Op != Query_OP {
		sc.lastReq[args.ClientId] = RequestHistory{args.CommandId, *reply}
	}
	term, isLeader := sc.rf.GetState()
	DPrintf("me:%d state:%d,%v  apply %+v result: %+v\n", sc.me, term, isLeader, args, reply)
	return reply
}

func (sc *ShardCtrler) applyCommand() {
	for !sc.killed() {
		msg := <-sc.applyCh
		func(msg *raft.ApplyMsg) {
			sc.mu.Lock()
			defer sc.mu.Unlock()
			if msg.CommandValid {
				args := msg.Command.(CommandArgs)
				reply := sc.execCmd(&args)
				currentTerm, isLeader := sc.rf.GetState()
				ch, ok := sc.notifier[msg.CommandIndex]
				if isLeader && msg.CommandTerm == currentTerm && ok {
					ch <- reply
				}
			}
		}(&msg)
	}
}

func redistributeShards(conf *Config) {
	if len(conf.Groups) == 0 {
		conf.Shards = [NShards]int{}
		return
	}
	groups := make([]int, 0, len(conf.Groups))
	for group, _ := range conf.Groups {
		groups = append(groups, group)
	}
	groupCounts := make(map[int]int)
	for _, groupID := range conf.Shards {
		groupCounts[groupID]++

	}
	// groups 按包含的shard数量从大到小排列
	sort.Slice(groups, func(i, j int) bool {
		if groupCounts[groups[i]] == groupCounts[groups[j]] {
			return groups[i] > groups[j]
		}
		return groupCounts[groups[i]] > groupCounts[groups[j]]
	})

	targetShardCount := NShards / len(groups)
	remainingShards := NShards % len(groups)
	targetCounts := make(map[int]int)
	for i, group := range groups {
		if i < remainingShards {
			targetCounts[group] = targetShardCount + 1
		} else {
			targetCounts[group] = targetShardCount
		}
	}

	newShards := [NShards]int{}
	groupIndex, newGroupID := len(groups)-1, groups[len(groups)-1]
	for shardID, groupID := range conf.Shards {
		if groupCounts[groupID] > targetCounts[groupID] {
			groupCounts[groupID]--
			groupCounts[newGroupID]++
			newShards[shardID] = newGroupID
		} else {
			newShards[shardID] = groupID
		}
		if groupCounts[newGroupID] >= targetCounts[newGroupID] && groupIndex != 0 {
			groupIndex, newGroupID = groupIndex-1, groups[groupIndex-1]
		}
	}
	conf.Shards = newShards
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(CommandArgs{})
	labgob.Register(CommandReply{})
	labgob.Register(Config{})
	applyCh := make(chan raft.ApplyMsg)
	sc := &ShardCtrler{
		me:       me,
		configs:  make([]Config, 1),
		applyCh:  applyCh,
		rf:       raft.Make(servers, me, persister, applyCh),
		dead:     0,
		notifier: make(map[int]chan *CommandReply),
		lastReq:  make(map[int64]RequestHistory),
	}

	sc.configs[0].Groups = map[int][]string{}
	go sc.applyCommand()
	// Your code here.

	return sc
}

func (sc *ShardCtrler) Join_OP(args *JoinArgs, reply *JoinReply) {
	// Your code here.
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}
