package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm        *shardctrler.Clerk
	config    shardctrler.Config
	make_end  func(string) *labrpc.ClientEnd
	clkId     int64
	requestId int64

	// You will have to modify this struct.
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	return &Clerk{
		sm:       shardctrler.MakeClerk(ctrlers),
		make_end: make_end,
		clkId:    nrand(),
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := &CommonArgs{
		Key: key,
		Op:  GET_OP,
	}
	return ck.SendRequest(args)
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op OpType) {
	args := &CommonArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	ck.SendRequest(args)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT_OP)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND_OP)
}

func (ck *Clerk) SendRequest(args *CommonArgs) string {
	args.ClientId = ck.clkId
	args.RequestId = ck.requestId
	ck.requestId++
	reply := &CommonReply{}
	for {
		shard := key2shard(args.Key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				DPrintf("shardkv client send request to gid:%d server:%s", gid, servers[si])
				srv := ck.make_end(servers[si])
				ok := srv.Call("ShardKV.HandleRequest", args, reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		DPrintf("client update config num:%d", ck.config.Num)
	}
	return ""
}
