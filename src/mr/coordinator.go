package mr

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	TaskKind         int
	TaskStatus       []bool
	ReduceNum        int
	RemainTasksCount int

	WaitingTasks chan Task
	KeyValueSet  map[string][]string
}

func WorkFlow() {
	// 得有个同步所有线程的操作，因为要知道所有 map 或者 reduce 任务都完成了，
	// 然后再分配下一步的任务下去或者结束
	// coordinator 这边一直有个线程与 worker 同步
	// when each worker goes online, register on server
	// assign map task to worker
	// receive map task result from worker
	// combine this task result then distribute to n reduce task
	// assign these reduce tasks to worker
	// combine reduce tasks result
	// send shutdown task to all alive workers
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		ReduceNum: nReduce,
	}

	c.setupMapTask(files)
	c.server()
	return &c
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.TaskKind == ReduceTaskFlag && c.RemainTasksCount == 0
}

func (c *Coordinator) RequestTask(_, task *Task) error {
	if c.Done() {
		task.TaskKind = ShutdownFlag
		task.File = ""
		task.TaskId = 0
		return nil
	}

	t := <-c.WaitingTasks
	task.File = t.File
	task.TaskId = t.TaskId
	task.TaskKind = t.TaskKind

	defer c.assignToNewWorkerIfTimeOut(t, 10)
	return nil
}

func (c *Coordinator) UploadMapResult(result MapResult, _ *struct{}) error {
	if c.RemainTasksCount == 0 || c.TaskStatus[result.TaskId] == true {
		// 表示迟来的结果，已经有 worker 做完了，这个才发过来
		return nil
	}

	// 这里要测试下下面这段代码的作用
	for _, item := range result.Items {
		k := item.Key
		c.KeyValueSet[k] = append(c.KeyValueSet[k], item.Values...)
	}

	c.TaskStatus[result.TaskId] = true
	c.RemainTasksCount--
	if c.RemainTasksCount == 0 {
		c.setupReduceTask()
	}

	return nil
}

func (c *Coordinator) UploadReduceResult(result ReduceResult, _ *struct{}) error {
	if c.RemainTasksCount == 0 || c.TaskStatus[result.TaskId] == true {
		// 表示迟来的结果，已经有 worker 做完了，这个才发过来
		return nil
	}
	c.TaskStatus[result.TaskId] = true
	c.RemainTasksCount--

	return nil
}

// func fileIfExist(filename string) bool {
// 	_, err := os.Stat(filename)
// 	if err != nil {
// 		fmt.Println(filename, "is not exist")
// 		return false
// 	}

// 	return !os.IsNotExist(err)
// }

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}

	// 下面这个是干嘛
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	// if e != nil {
	// 	log.Fatal("listen error:", e)
	// }

	// go http.Serve(l, nil)

	go func() {
		for {
			conn, err := l.Accept()
			// 保存这个连接
			if err != nil {
				log.Fatal("accept error:", err)
			}
			// rpc 主要用来获得 map 和 reduce 的结果会比较方便
			go rpc.ServeConn(conn)
		}
	}()
}

func (c *Coordinator) setupMapTask(files []string) {
	n := len(files)
	c.TaskKind = MapTaskFlag
	c.RemainTasksCount = n
	c.WaitingTasks = make(chan Task, len(files))
	c.TaskStatus = make([]bool, n)

	for i := 0; i < n; i++ {
		c.TaskStatus[i] = false
		t := Task{
			File:     files[i],
			TaskId:   i,
			TaskKind: MapTaskFlag,
		}
		c.WaitingTasks <- t
	}
}

func getReduceArgFilenameOf(id int) string {
	return fmt.Sprint("reduce-arg-", id)
}

func (c *Coordinator) setupReduceTask() {
	// 这时候如果有 worker 来访问怎么办，要处理下 TODO
	n := c.ReduceNum
	c.TaskKind = ReduceTaskFlag
	c.RemainTasksCount = n
	c.WaitingTasks = make(chan Task, n)
	c.TaskStatus = make([]bool, n)

	var reduceArgFileWriters []*os.File
	for i := 0; i < n; i++ {
		filename := getReduceArgFilenameOf(i)
		f, _ := os.Create(filename)
		reduceArgFileWriters = append(reduceArgFileWriters, f)
	}

	for k, vs := range c.KeyValueSet {
		i := ihash(k) % c.ReduceNum
		appendTo(reduceArgFileWriters[i], k, vs)
	}

	// 想一下，当某个任务失败时，这些文件怎么处理
	for i := 0; i < n; i++ {
		c.TaskStatus[i] = false
		filename := getReduceArgFilenameOf(i)
		t := Task{
			File:     filename,
			TaskId:   i,
			TaskKind: ReduceTaskFlag,
		}

		c.WaitingTasks <- t
	}
}

func appendTo(writer io.Writer, key string, values []string) {
	write := func(s string) {
		n, e := io.WriteString(writer, s)
		if n != len(s) || e != nil {
			panic(e)
		}
		divider := " "
		io.WriteString(writer, divider)
	}
	write(key)
	for _, v := range values {
		write(v)
	}
	write("\n")
}

func (c *Coordinator) assignToNewWorkerIfTimeOut(task Task, seconds int) {
	time.Sleep(time.Duration(seconds) * time.Second)
	done := c.TaskStatus[task.TaskId]
	if !done {
		c.WaitingTasks <- task
		go c.assignToNewWorkerIfTimeOut(task, seconds)
	}
}

// 使用互斥量互斥访问一些共享变量以及一些可能共同调用的过程，比如 setupReduceTask()
