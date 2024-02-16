package shardkv

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"6.5840/labrpc"
)

// Debugging
const Debug = true

var file *os.File

func init() {
	if !Debug {
		return
	}
	f, err := os.Create("./shardkv-" + strconv.Itoa(int(time.Now().Unix())) + ".log")
	if err != nil {
		panic("log create file fail!")
	}
	file = f
}

// debug下打印日志
func DPrintf(format string, value ...interface{}) {
	if Debug {
		currMs := (time.Now().UnixMilli()) & 0xfffff
		fmt.Fprintf(file, "[%vms] %s\n", currMs, fmt.Sprintf(format, value...))
	}
}

func sendRPC(server *labrpc.ClientEnd, svcMeth string, args interface{}, reply interface{}) bool {
	return server.Call(svcMeth, args, reply)
}

const RPCTimeout = 50 * time.Microsecond

func sendRPCWithTimeout(server *labrpc.ClientEnd, svcMeth string, args interface{}, reply interface{}) bool {
	ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
	defer cancel()

	ch := make(chan struct{})
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
