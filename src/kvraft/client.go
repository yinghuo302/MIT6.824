package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	leaderId  int
	commandId int64
	clientId  int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:   servers,
		leaderId:  0,
		commandId: 0,
		clientId:  nrand(),
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	return ck.SendRequest(&RequestArgs{Key: key, Op: GET_OP})
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op OpType) {
	// You will have to modify this function.
	ck.SendRequest(&RequestArgs{Key: key, Value: value, Op: op})
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT_OP)
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND_OP)
}

func (ck *Clerk) SendRequest(args *RequestArgs) string {
	var reply RequestReply
	args.ClientId, args.RequestId = ck.clientId, ck.commandId
	for {
		if !ck.servers[ck.leaderId].Call("KVServer.HandleRequest", args, &reply) ||
			reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.commandId++
		return reply.Value
	}
}
