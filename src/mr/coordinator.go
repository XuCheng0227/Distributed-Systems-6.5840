package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"reflect"
	"sync"
	"time"
)

// const (
// 	idle = 1
// 	assigned = 2
// 	completed = 3
// )

// Coordinator manages the map and reduce tasks.
// To do that, it maintains an array storing all the input files.
// Meanwhile, it maintains four task stacks: two for map tasks, two for reduce tasks.
// The idle one is used to store tasks ready to be scheduled,
// the assigned one is used to store tasks being processed while not completed.
type Coordinator struct {
	// Your definitions here.
	fineName []string // The input file list

	idleMapTask        taskStack
	assignedMapTask    taskStack
	idleReduceTask     taskStack
	assignedReduceTask taskStack

	completed bool
	nReduce   int
}

type Task struct {
	beginTime time.Time
	fileName  string // The name of the input file. Used for map to red the file.

	nReduce int // The # of reduce tasks to use.
	nMap    int // The # of files to process.

	indexMapWorker    int // The index of map worker
	indexReduceWorker int // The index of reduce worker
}

type taskQueue struct {
	taskList []Task
	mutex    sync.Mutex
}

func (t *Task) SetTime() {
	t.beginTime = time.Now()
}

func (t *Task) GetTime() time.Time {
	return t.beginTime
}

func (t *Task) TimeInterval() time.Duration {
	return time.Now().Sub(t.GetTime())
}

func (t *Task) TimeExceeded() bool {
	return t.TimeInterval() > time.Second*10
}

func (tq *taskQueue) taskQueueSize() int {
	return len(tq.taskList)
}

func (tq *taskQueue) PushTask(t Task) {
	tq.mutex.Lock()
	defer tq.mutex.Unlock()
	if !reflect.DeepEqual(t, Task{}) {
		tq.taskList = append(tq.taskList, t)
	}
	return
}

func (tq *taskQueue) PopTask() (Task, error) {
	tq.mutex.Lock()
	defer tq.mutex.Unlock()

	queueSize := taskQueueSize(tq)
	if queueSize == 0 {
		return Task{}, errors.New("Task stack is empty")
	}

	task := tq.taskList[0]
	tq.taskList = tq.taskList[1:]
	return task, nil
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
