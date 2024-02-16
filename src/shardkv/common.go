package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrNotReady    = "ErrNotReady"
	ErrOutDated    = "ErrOutDated"
)

type OpType int

const (
	GET_OP    OpType = iota
	PUT_OP    OpType = iota
	APPEND_OP OpType = iota
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type CommonArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Op        OpType
	ClientId  int64
	RequestId int64
}

type CommonReply struct {
	Err   Err
	Value string
}

type PullShardArgs struct {
	ConfigNum int
	ShardIds  []int
}

type PullShardReply struct {
	Err       Err
	ConfigNum int
	Shards    map[int]*Shard
}

type DelShardsArgs struct {
	ConfNum  int
	ShardIDs []int
}

type DelShardsReply struct {
	Err Err
}

type CommandType int

const (
	Operation CommandType = iota
	Configuration
	PullShards
	DeleteShards
	EmptyEntry
)

type Command struct {
	Op  CommandType
	Cmd interface{}
}
