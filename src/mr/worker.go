package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply := doHeartBeat(-1)
		switch reply.JobType {
		case MapJob:
			doMap(mapf, reply)
		case ReduceJob:
			doReduce(reducef, reply)
		case WaitJob:
			time.Sleep(time.Second)
		case EndJob:
			return
		default:
			log.Fatalln("Worker: Unknown Task Type")
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func doHeartBeat(state int) *HeartBeatReply {
	args := HeartBeatArgs{State: state}
	reply := HeartBeatReply{}
	call("Coordinator.HeartBeat", &args, &reply)
	// fmt.Printf("state:%d,Id:%d,JobType:%d,filename:%s,nWorker:%d\n", args.State, reply.Id, reply.JobType, reply.FileName, reply.NWorker)
	return &reply
}

func doMap(mapf func(string, string) []KeyValue, reply *HeartBeatReply) {
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	file.Close()
	intermediate := mapf(reply.FileName, string(content))
	buckets := make([][]KeyValue, reply.NWorker)
	for _, kv := range intermediate {
		idx := ihash(kv.Key) % reply.NWorker
		buckets[idx] = append(buckets[idx], kv)
	}
	for i, kv := range buckets {
		tmpFile, err := os.CreateTemp("", "mr-map-tmp-*")
		if err != nil {
			log.Fatalf("Worker: cann't create Temp File")
		}
		enc := json.NewEncoder(tmpFile)
		if err := enc.Encode(kv); err != nil {
			log.Fatalf("Worker: cann't write intermediate result %v", err)
		}
		fileName := fmt.Sprintf("mr-mapId:%d-reduceId:%d", reply.Id, i)
		os.Rename(tmpFile.Name(), fileName)
		tmpFile.Close()
	}
	doHeartBeat(reply.Id)
}

func doReduce(reducef func(string, []string) string, reply *HeartBeatReply) {
	intermediate := make([]KeyValue, 0)
	for i := 0; i < reply.NWorker; i++ {
		file, err := os.Open(fmt.Sprintf("mr-mapId:%d-reduceId:%d", i, reply.Id))
		dec := json.NewDecoder(file)
		if err != nil {
			log.Fatalf("Worker: can't open intermediate result file %v\n", err)
		}
		kvs := make([]KeyValue, 0)
		if err := dec.Decode(&kvs); err != nil {
			log.Fatalf("Worker: can't read intermediate result file %v\n", err)
		}
		intermediate = append(intermediate, kvs...)
		file.Close()
	}
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})
	n := len(intermediate)
	tmpFile, err := os.CreateTemp("", "mr-reduce-tmp-*")
	if err != nil {
		log.Fatalln("Worker: cann't create Temp File")
	}
	for i := 0; i < n; {
		values, j := make([]string, 0), i
		for ; j < n && intermediate[i].Key == intermediate[j].Key; j++ {
			values = append(values, intermediate[j].Value)
		}
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, reducef(intermediate[i].Key, values))
		i = j
	}
	os.Rename(tmpFile.Name(), fmt.Sprintf("mr-out-%d", reply.Id))
	for i := 0; i < reply.NWorker; i++ {
		os.Remove(fmt.Sprintf("mr-mapId:%d-reduceId:%d", i, reply.Id))
	}
	doHeartBeat(reply.Id)
}
