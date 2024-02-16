package shardctrler

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Debugging
const Debug = false

var file *os.File

func init() {
	if !Debug {
		return
	}
	f, err := os.Create("./shardctrler-" + strconv.Itoa(int(time.Now().Unix())) + ".log")
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
