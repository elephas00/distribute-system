package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	IdleTasks chan Task
	ProcessingTasks chan Task
	CompletedTasks chan Task
	NextAutoIncreasedWorkerId int32
	NextAutoIncreasedTaskId int32
	HeartBeats map[int]time.Time
	HeartBeatGapThresholdSeconds int
}

const (
	IDLE = iota
	PROCESSING 
	COMPLETED	
)

const (
	MAP = iota
	REDUCE
)

type Task struct {
	TaskId int32
	PreviousTaskId int32
	WorkerId int32
	TaskType int
	State int
	InputLocation string
	InputSize int
	OutputLocation string
	OutputSize int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	println("coordinator called")
	println("X %v\n", args.X)
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) WorkerDiscover(args *WorkerDiscoverArgs, reply *WorkerDiscoverReply) error {
	workerId := atomic.AddInt32(&c.NextAutoIncreasedWorkerId, 1)
	reply.WorkerId = workerId
	return nil
}

func (c *Coordinator) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
	if(!isActiveWorker(args.WorkerId, c)){
		reply.Success = false
	}else{
		c.UpdateHeartBeat(args.WorkerId)
		reply.Success = true
	}
	return nil
}

func isActiveWorker(workerId int32, c *Coordinator) bool {
	prevousHeartBeat := c.HeartBeats[int(workerId)]
	if(prevousHeartBeat.IsZero()){
		return false;
	}
	return prevousHeartBeat.Add(time.Duration(c.HeartBeatGapThresholdSeconds) * time.Second).Before(time.Now())
}

func (c *Coordinator) UpdateHeartBeat(WorkerId int32){
	c.HeartBeats[int(WorkerId)] = time.Now()
}


func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// lock
	if(len(c.IdleTasks) == 0 && len(c.ProcessingTasks) == 0){
		return nil
	}
	if(len(c.IdleTasks) == 0 && len(c.ProcessingTasks) > 0){
		// 遍历 ProcessingTasks 通道并将未完成的任务加入 Idle 通道
		for task := range c.ProcessingTasks {
			if(!isActiveWorker(task.WorkerId, c)){
				c.IdleTasks <- task
				task.WorkerId = -1
				task.State = IDLE
			}
		}
	}
	// unlock
	task := <- c.IdleTasks
	task.WorkerId = args.WorkerId
	task.State = PROCESSING
	reply.Task = task
	c.IdleTasks <- task
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply * TaskDoneRelpy) error{
	if(!isActiveWorker(args.Task.WorkerId, c)){
		reply.Success = false
		return nil
	}
	
	var task Task 
	for t := range c.ProcessingTasks {
		if(task.WorkerId == args.Task.TaskId){
			task = t
		}
	}
	
	task.OutputLocation = args.Task.OutputLocation
	task.State = COMPLETED
	c.CompletedTasks <- task
	if(task.TaskType == MAP){
		newTask := Task{}
		newTask.TaskId = atomic.AddInt32(&c.NextAutoIncreasedTaskId, 1)
		newTask.OutputLocation = "reduce" + string(task.TaskId)
		newTask.PreviousTaskId = task.WorkerId
		newTask.InputLocation = task.OutputLocation
		newTask.State = IDLE
		newTask.TaskType = REDUCE
		c.IdleTasks <- newTask
	}
	return nil
}


// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	
	// Your code here.
	return len(c.IdleTasks) == 0 && len(c.ProcessingTasks) == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.NextAutoIncreasedWorkerId = 0
	c.IdleTasks =  make(chan Task)
	c.ProcessingTasks =  make(chan Task)
	c.CompletedTasks =  make(chan Task)

	// Your code here.
	for _, filename := range os.Args[2:] {
		task := Task{}
		task.TaskId = c.NextAutoIncreasedTaskId
		c.NextAutoIncreasedTaskId++;
		task.InputLocation = filename
		task.OutputLocation = "map" + string(task.TaskId) 
		task.State = IDLE
		task.TaskType = MAP
		task.PreviousTaskId = -1
		c.IdleTasks <- task
	}

	c.server()
	return &c
}
