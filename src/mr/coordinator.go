package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)


type State int

const (
	StateInit State = iota
	StateMap 
	StateReduce
	StateDone
)

type WorkingTask struct{
	working_task_type TaskType 
	index int
	filename string
	start_working_time time.Time
}

type Coordinator struct {
	reduce_num int
	now_state   State
	map_files   []string
	working_queue []WorkingTask
	done_queue []WorkingTask
	idle_chan chan WorkingTask
	working_chan chan WorkingTask
	done_chan chan WorkingTask
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error{
	reply.DispatchedTaskList = []int{}
	reply.DispatchedTaskFile = []string{}
	reply.ReduceNum = c.reduce_num
	// to avoid read and write at the same time, we use a copy of now_state
	// the read of now_state is atomic, and we only need to achieve eventual consistency
	// todo: use a mutex or channel here?
	temp_nowstate := c.now_state
	if temp_nowstate == StateInit{
		reply.DispatchedTaskType = TaskNo
		return nil
	}else if temp_nowstate == StateMap{
		c.ProcessFinishedTask(args, temp_nowstate)
		c.DispatchTask(reply, temp_nowstate)
		return nil
	}else if temp_nowstate == StateReduce{
		c.ProcessFinishedTask(args, temp_nowstate)
		c.DispatchTask(reply, temp_nowstate)
		return nil
	}else if temp_nowstate == StateDone{
		reply.DispatchedTaskType = TaskFinish
		return nil
	}else {
		log.Panic("Unknown state")
		return nil
	}
}

func (c *Coordinator) ProcessFinishedTask(args *AskForTaskArgs, temp_nowstate State){
	if temp_nowstate == StateMap && args.FinishedTaskType == TaskMap{
		for _, task_idx := range args.FinishTaskList{
			task := WorkingTask{working_task_type: TaskMap, index: task_idx, filename: c.map_files[task_idx], start_working_time: time.Time{}}
			c.done_chan <- task
		}
	}else if temp_nowstate == StateReduce && args.FinishedTaskType == TaskReduce{
		for _, task_idx := range args.FinishTaskList{
			task := WorkingTask{working_task_type: TaskReduce, index: task_idx, filename: "", start_working_time: time.Time{}}
			c.done_chan <- task
		}
	}
}


func (c *Coordinator) DispatchTask(reply *AskForTaskReply, temp_nowstate State){
		var task WorkingTask
		select{
			case task = <- c.idle_chan:
				reply.DispatchedTaskList = append(reply.DispatchedTaskList, task.index)
				reply.DispatchedTaskFile = append(reply.DispatchedTaskFile, task.filename)
				reply.ReduceNum = c.reduce_num
				task.start_working_time = time.Now()
				if temp_nowstate == StateMap{
					reply.DispatchedTaskType = TaskMap
				} else if temp_nowstate == StateReduce{
					reply.DispatchedTaskType = TaskReduce
				}
				c.working_chan <- task
				return
			default:
				reply.DispatchedTaskType = TaskNo
				return
		}
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = c.now_state == StateDone

	return ret
}

func (c *Coordinator) _handle_done_task(task WorkingTask) {
	for _, done_task := range c.done_queue{
		if done_task.index == task.index{
			return 
		}
	}
	c.done_queue = append(c.done_queue, task)
}

func (c *Coordinator) handle_done_task(done_task WorkingTask) {
	if done_task.working_task_type == TaskMap && c.now_state == StateMap{
		c._handle_done_task(done_task)
		if len(c.done_queue) == len(c.map_files){
			c.done_queue = []WorkingTask{}
			c.working_queue = []WorkingTask{}
			c.now_state = StateReduce
			for i := 0; i < c.reduce_num; i++{
				c.idle_chan <- WorkingTask{working_task_type: TaskReduce, index: i, filename: "", start_working_time: time.Time{}}
			}
		}
	}else if done_task.working_task_type == TaskReduce && c.now_state == StateReduce{
		c._handle_done_task(done_task)
		if len(c.done_queue) == c.reduce_num{
			c.now_state = StateDone
		}
	}
}

// func (c *Coordinator) remov_from_working_queue(index int){
// 	c.working_queue[index] = c.working_queue[len(c.working_queue)-1]
// 	// c.working_queue = c.working_queue[:len(c.working_queue)-1]
// }

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		reduce_num: nReduce,
		now_state:   StateInit,
		map_files:   files,
		working_queue: []WorkingTask{},
		done_queue: []WorkingTask{},
	}

	// Your code here.


	c.server()

	// each file correspond to one map task
	c.idle_chan = make(chan WorkingTask, len(files))
	c.working_chan = make(chan WorkingTask, len(files))
	c.done_chan = make(chan WorkingTask, len(files) * 2) // avoid a task is completed more than once
	for idx, filename := range files{
		c.idle_chan <- WorkingTask{working_task_type: TaskMap, index: idx, filename: filename, start_working_time: time.Time{}}
	}
	
	c.now_state = StateMap

	for !c.Done(){
		// var task WorkingTask
		// priority select to handle done task before working task
		select{
			case done_task := <- c.done_chan:
				c.handle_done_task(done_task)
			case working_task := <- c.working_chan:
				priority:
					for{
						select {
							case done_task := <- c.done_chan:
								c.handle_done_task(done_task)
							default:
								break priority
						}
					}
				if working_task.working_task_type == TaskMap && c.now_state == StateMap{
					c.working_queue = append(c.working_queue, working_task)
				} else if working_task.working_task_type == TaskReduce && c.now_state == StateReduce{
					c.working_queue = append(c.working_queue, working_task)
				}
			default:
				temp_list := []WorkingTask{}
				for _, task := range c.working_queue{
					if (task.working_task_type == TaskMap && c.now_state == StateMap) || (task.working_task_type == TaskReduce && c.now_state == StateReduce){
						this_task_done := false
						for _, done_task := range c.done_queue{
							if done_task.index == task.index && done_task.working_task_type == task.working_task_type{
								this_task_done = true
								break
							}
						}
						if this_task_done{
							continue
						} else if time.Since(task.start_working_time) > 10 * time.Second{
							c.idle_chan <- task
						} else {
							temp_list = append(temp_list, task)
						}
					}
				}
				c.working_queue = temp_list
		}
	}
	time.Sleep(5 * time.Second)
	close(c.idle_chan)
	close(c.working_chan)
	close(c.done_chan)

	return &c
}
