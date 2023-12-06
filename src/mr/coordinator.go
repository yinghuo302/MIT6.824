package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const (
	mapPhase    = 0
	reducePhase = 1
	finishPhase = 2
)

type JobState int

const (
	JobUnalloc JobState = 0
	JobStart   JobState = 1
	JobFinish  JobState = 2
)

type Coordinator struct {
	// Your definitions here.
	phase   int
	nWorker [3]int
	nStart  [2]int
	nFinish [2]int
	jobType [2]JobType
	files   []string
	tasks   []JobState
	msg     chan Msg
	timeout chan int
}

type Msg struct {
	args   *HeartBeatArgs
	reply  *HeartBeatReply
	finish chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.phase == finishPhase
}

func (c *Coordinator) findTask(reply *HeartBeatReply) {
	if c.phase == finishPhase || c.nStart[c.phase] >= c.nWorker[c.phase] {
		reply.JobType = EndJob
		if c.phase != finishPhase {
			reply.JobType = WaitJob
		}
		return
	}

	for i := 0; i < c.nWorker[c.phase]; i++ {
		if c.tasks[i] == 0 {
			reply.Id = i
			break
		}
	}
	if c.phase == mapPhase {
		reply.FileName = c.files[reply.Id]
	}
	c.nStart[c.phase]++
	c.tasks[reply.Id] = JobStart
	reply.JobType = c.jobType[c.phase]
	reply.NWorker = c.nWorker[c.phase^1]
	go func(id int) {
		time.Sleep(10 * time.Second)
		c.timeout <- id
	}(reply.Id*2 + c.phase)
}

func (c *Coordinator) finishTask(id int) {
	if c.tasks[id] == JobStart {
		c.tasks[id] = JobFinish
		c.nFinish[c.phase]++
		if c.nFinish[c.phase] == c.nWorker[c.phase] {
			c.phase++
			c.tasks = make([]JobState, c.nWorker[c.phase])
		}
	}
}

func (c *Coordinator) taskTimeout(taskId int) {
	// 可能存在上一个phase的timeout在下一个phase处理
	phase, id := taskId&1, taskId/2
	if phase == c.phase && c.tasks[id] == JobStart {
		c.tasks[id] = JobUnalloc
		c.nStart[c.phase]--
	}
}

func (c *Coordinator) Schedule() {
	for {
		select {
		case msg := <-c.msg:
			args, reply := msg.args, msg.reply
			if args.State == -1 {
				c.findTask(reply)
			} else {
				c.finishTask(args.State)
			}
			msg.finish <- struct{}{}
		case id := <-c.timeout:
			c.taskTimeout(id)
		}
		// if c.phase != finishPhase {
		// 	fmt.Printf("phase:%d,total job count:%d,start job count:%d,finish job count%d,job state array:%v\n", c.phase, c.nWorker[c.phase], c.nStart[c.phase], c.nFinish[c.phase], c.tasks)
		// }
	}
}

func (c *Coordinator) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
	// Your code here.
	msg := Msg{args: args, reply: reply, finish: make(chan struct{})}
	c.msg <- msg
	<-msg.finish
	// fmt.Printf("state:%d,Id:%d,JobType:%d,filename:%s,nWorker:%d\n", args.State, reply.Id, reply.JobType, reply.FileName, reply.NWorker)
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		phase:   mapPhase,
		files:   files,
		nWorker: [3]int{len(files), nReduce, 0},
		nStart:  [2]int{0, 0},
		nFinish: [2]int{0, 0},
		timeout: make(chan int, 8),
		msg:     make(chan Msg, 8),
		tasks:   make([]JobState, len(files)),
		jobType: [2]JobType{MapJob, ReduceJob},
	}
	go c.Schedule()
	c.server()
	return &c
}
