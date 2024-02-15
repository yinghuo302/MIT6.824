package raft

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"6.5840/labrpc"
)

const Debug = true

var file *os.File

func init() {
	if !Debug {
		return
	}
	f, err := os.Create("./log-" + strconv.Itoa(int(time.Now().Unix())) + ".txt")
	if err != nil {
		panic("log create file fail!")
	}
	file = f
}

// debug下打印日志
func DPrintf(format string, value ...interface{}) {
	if Debug {
		currMs := (time.Now().UnixMilli()) & 0xfffff
		info := fmt.Sprintf("[%vms] ", currMs) + fmt.Sprintf(format, value...)
		file.WriteString(info)
	}
}

const RPCTimeout = 30 * time.Millisecond

func sendRPCWithTimeout(server *labrpc.ClientEnd, svcMeth string, args interface{}, reply interface{}) bool {
	// ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
	// defer cancel()

	ch := make(chan struct{})
	go func() {
		for i := 0; i < 10; i++ {
			ok := server.Call(svcMeth, args, reply)
			if ok {
				ch <- struct{}{}
				return
			}
		}
	}()
	// go func() {
	// 	for i := 0; i < 3; i++ {
	// 		select {
	// 		case <-ctx.Done():
	// 			return
	// 		default:
	// 			ok := server.Call(svcMeth, args, reply)
	// 			if ok {
	// 				ch <- struct{}{}
	// 				return
	// 			}
	// 		}

	// 	}
	// }()
	select {
	case <-ch:
		return true
	case <-time.After(RPCTimeout):
		return false
	}
	// return server.Call(svcMeth, args, reply)
}
