package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
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
	Key   string
	Value string
	Op    OpType // "Put" or "Append"
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

type RequestArgs struct {
	ClientId  int64
	RequestId int64
	Key       string
	Value     string
	Op        OpType
}

type RequestReply struct {
	Err   Err
	Value string
}

type RaftCmdType int

const (
	Operation RaftCmdType = iota
	Configuration
	InsertShards
	DeleteShards
	EmptyEntry
)

type RaftCmd struct {
	Op  RaftCmdType
	Cmd interface{}
}
