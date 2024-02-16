package raft

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"6.5840/labrpc"
)

const Debug = false

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

const RPCTimeout = 50 * time.Millisecond

func sendRPCWithTimeout(server *labrpc.ClientEnd, svcMeth string, args interface{}, reply interface{}) bool {
	ch := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
	defer cancel()
	go func() {
		for i := 0; i < 3; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				ok := server.Call(svcMeth, args, reply)
				if ok {
					ch <- struct{}{}
					return
				}
			}

		}
	}()
	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	}
}

// func sendRPCWithTimeout(server *labrpc.ClientEnd, svcMeth string, args interface{}, reply interface{}) bool {
// 	ch := make(chan struct{})
// 	go func() {
// 		for i := 0; i < 10; i++ {
// 			ok := server.Call(svcMeth, args, reply)
// 			if ok {
// 				select {
// 				case ch <- struct{}{}:
// 				default:
// 				}
// 				return
// 			}
// 		}
// 	}()
// 	select {
// 	case <-ch:
// 		return true
// 	case <-time.After(RPCTimeout):
// 		return false
// 	}
// }

// func sendRPCWithTimeout(server *labrpc.ClientEnd, svcMeth string, args interface{}, reply interface{}) bool {
// 	return server.Call(svcMeth, args, reply)
// }
