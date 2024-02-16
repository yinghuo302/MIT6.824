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
	f, err := os.Create("./raft-" + strconv.Itoa(int(time.Now().Unix())) + ".log")
	if err != nil {
		panic("log create file fail!")
	}
	file = f
}

// debug下打印日志
func DPrintf(format string, value ...interface{}) {
	if Debug {
		currMs := (time.Now().UnixMilli()) & 0xfffff
		fmt.Fprintf(file, "[%vms] %s", currMs, fmt.Sprintf(format, value...))
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
					select {
					case ch <- struct{}{}:
					default:
					}
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
