package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	clientId  int64
	commandId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &CommandArgs{}
	args.Num = num
	args.Op = Query_OP
	reply := &CommandReply{}
	ck.sendRequest(args, reply)
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &CommandArgs{}
	args.Servers = servers
	args.Op = Join_OP
	reply := &CommandReply{}
	ck.sendRequest(args, reply)
}

func (ck *Clerk) Leave(gids []int) {
	args := &CommandArgs{}
	args.GIDs = gids
	args.Op = Leave_OP
	reply := &CommandReply{}
	ck.sendRequest(args, reply)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &CommandArgs{}
	args.Shard = shard
	args.Op = Move_OP
	args.GID = gid
	reply := &CommandReply{}
	ck.sendRequest(args, reply)
}

func (ck *Clerk) sendRequest(args *CommandArgs, reply *CommandReply) {
	args.ClientId = ck.clientId
	args.CommandId = ck.commandId
	ck.commandId++
	for {
		// try each known server.
		for idx, srv := range ck.servers {
			DPrintf("send request to %d: %+v\n", idx, args)
			ok := srv.Call("ShardCtrler.HandleRequest", args, reply)
			DPrintf("%+v receive reply from %d: %v,%+v\n", args, idx, ok, reply)
			if ok && reply.Err != ErrWrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
