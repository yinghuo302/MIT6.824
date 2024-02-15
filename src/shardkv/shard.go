package shardkv

type ShardStatus int

const (
	Empty ShardStatus = iota
	Serving
	Pulling
	Expire
	Waiting
)

type RequestHistory struct {
	ReqId int64
	Reply CommonReply
}

type Shard struct {
	KV      map[string]string
	LastReq map[int64]RequestHistory
	Status  ShardStatus
}

func NewShard(state ShardStatus) *Shard {
	return &Shard{make(map[string]string), make(map[int64]RequestHistory), state}
}

func (shard *Shard) Get(key string) (string, Err) {
	if value, ok := shard.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (shard *Shard) Put(key, value string) Err {
	shard.KV[key] = value
	return OK
}

func (shard *Shard) Append(key, value string) Err {
	shard.KV[key] += value
	return OK
}

func (shard *Shard) PutAppend(method OpType, key, value string) Err {
	if method == PUT_OP {
		shard.KV[key] = value
	} else if method == APPEND_OP {
		shard.KV[key] += value
	}
	return OK
}

func (shard *Shard) Copy() *Shard {
	newShard := &Shard{make(map[string]string), make(map[int64]RequestHistory), shard.Status}
	for k, v := range shard.KV {
		newShard.KV[k] = v
	}
	for clkId, history := range shard.LastReq {
		newShard.LastReq[clkId] = history
	}
	return newShard
}

func (shard *Shard) Exec(clkId, cmdId int64, args *CommonArgs) *CommonReply {
	if args.Op == GET_OP {
		value, err := shard.Get(args.Key)
		return &CommonReply{Value: value, Err: err}
	}
	if shard.GetLastReq(clkId, cmdId) == nil {
		history := RequestHistory{ReqId: cmdId}
		history.Reply.Err = shard.PutAppend(args.Op, args.Key, args.Value)
		shard.LastReq[clkId] = history
	}
	return shard.GetLastReq(clkId, cmdId)
}

func (shard *Shard) GetLastReq(clkId, cmdId int64) *CommonReply {
	last, ok := shard.LastReq[clkId]
	if !ok || last.ReqId != cmdId {
		return nil
	}
	return &last.Reply
}
