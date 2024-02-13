package raft

import (
	"context"
	"fmt"
	"os"
	"time"

	"6.5840/labrpc"
)

// Debugging
const Debug = true

var file *os.File = nil

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		if file == nil {
			var fileErr error
			file, fileErr = os.OpenFile("tem.log", os.O_WRONLY|os.O_CREATE, 0666)
			if fileErr != nil {
				panic("open file error")
			}
		}
		fmt.Fprintf(file, format, a...)
	}
	return
}

const RPCTimeout = 100 * time.Microsecond

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
