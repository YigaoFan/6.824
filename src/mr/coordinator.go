package mr

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TODO 读写锁优化
// 下列涉及数据竞争
// TaskKind 刷新 kind 的时候，在 map 完成的时候，可能有线程想查询，这里是不是要有优先级
// TaskStatus 查询、刷新 status 的时候，在 map 或者 reduce 任务完成的时候
// RemainTasksCount 更新，别的线程会查询
// KeyValueSet 更新
type Coordinator struct {
	TaskKind             int
	TaskKindLock         sync.Mutex
	TaskStatus           []bool
	TaskStatusLock       sync.Mutex
	TaskStatusItemLock   []sync.Mutex
	ReduceNum            int
	RemainTasksCount     int
	RemainTasksCountLock sync.Mutex
	KeyValueSet          map[string][]string
	KeyValueSetLock      sync.Mutex

	WaitingTasks chan Task
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		ReduceNum:   nReduce,
		KeyValueSet: make(map[string][]string),
	}

	c.setupMapTask(files)
	c.server()
	return &c
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done(withLog bool) bool {
	c.TaskKindLock.Lock()
	if withLog {
		Log("Obtain TaskKindLock")
	}
	defer c.TaskKindLock.Unlock()
	c.RemainTasksCountLock.Lock()
	if withLog {
		Log("Obtain RemainTasksCountLock")
	}
	defer c.RemainTasksCountLock.Unlock()
	return c.TaskKind == ReduceTaskFlag && c.RemainTasksCount == 0
}

func (c *Coordinator) RequestTask(_, task *Task) error {
	// fmt.Println("Query work done")
	if c.Done(true) {
		task.TaskKind = ShutdownFlag
		task.File = ""
		task.TaskId = strconv.Itoa(0)
		return nil
	}

	// fmt.Println("Waiting count", len(c.WaitingTasks))s
	t, ok := <-c.WaitingTasks
	if !ok {
		task.TaskKind = ShutdownFlag
		task.File = ""
		task.TaskId = strconv.Itoa(0)
		return nil
	}

	*task = Task{
		TaskKind: t.TaskKind,
		TaskId:   t.TaskId,
		File:     t.File,
	}
	// fmt.Println("Assign task", task.TaskId)

	// fmt.Println("Monitor task status")
	go c.assignToNewWorkerIfTimeOut(t, 10)
	return nil
}

func (c *Coordinator) UploadMapResult(result MapResult, _ *struct{}) error {
	// 多线程查询变量然后依据不同去更改这个变量
	// 下面这里查询，后面位置又更新了这个为 true
	// 可能别的线程在这个查询后，将其更新为 true
	// 所以要保证这个 TaskStatus 项的访问唯一

	c.TaskKindLock.Lock()
	defer c.TaskKindLock.Unlock()

	if c.TaskKind != MapTaskFlag {
		return nil
	}
	c.RemainTasksCountLock.Lock()
	defer c.RemainTasksCountLock.Unlock()
	if c.RemainTasksCount == 0 { // RemainTasksCount 和 TaskStatusLock 有可能造成死锁？
		return nil
	}
	id, _ := strconv.Atoi(result.TaskId)
	c.TaskStatusLock.Lock()
	defer c.TaskStatusLock.Unlock()
	c.TaskStatusItemLock[id].Lock()
	defer c.TaskStatusItemLock[id].Unlock()

	if c.TaskStatus[id] == true {
		// 表示迟来的结果，已经有 worker 做完了，这个才发过来
		return nil
	}
	c.TaskStatus[id] = true
	c.RemainTasksCount--
	c.KeyValueSetLock.Lock()
	for _, item := range result.Items {
		k := item.Key
		if ele, ok := c.KeyValueSet[k]; ok {
			c.KeyValueSet[k] = append(ele, item.Values...)
		} else {
			c.KeyValueSet[k] = item.Values
		}

		if k == "A" {
			fmt.Println("Add A :", len(item.Values))
			fmt.Println("After add A count :", len(c.KeyValueSet[k]))
		}
	}
	c.KeyValueSetLock.Unlock()

	if c.RemainTasksCount == 0 {
		// 要等 KeyValueSet 的插入操作完了，才能开始下面的 setup
		c.setupReduceTask()
		close(c.WaitingTasks)
	}

	return nil
}

func (c *Coordinator) UploadReduceResult(result ReduceResult, _ *struct{}) error {
	Log("Try to get TaskKindLock")
	c.TaskKindLock.Lock()
	if c.TaskKind != ReduceTaskFlag {
		return nil
	}
	c.TaskKindLock.Unlock()
	Log("Try to get RemainTasksCountLock")
	c.RemainTasksCountLock.Lock()
	if c.RemainTasksCount == 0 {
		return nil
	}
	id, _ := strconv.Atoi(result.TaskId)
	Log("Try to get TaskStatusLock")
	c.TaskStatusLock.Lock()
	c.TaskStatusItemLock[id].Lock()
	if c.TaskStatus[id] == true {
		return nil
	}
	c.TaskStatus[id] = true
	c.RemainTasksCount--
	c.TaskStatusItemLock[id].Unlock()
	c.TaskStatusLock.Unlock()
	c.RemainTasksCountLock.Unlock()

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
// no race
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go http.Serve(l, nil)

	// go func() {
	// 	for {
	// 		conn, err := l.Accept()
	// 		// 保存这个连接
	// 		if err != nil {
	// 			log.Fatal("accept error:", err)
	// 		}
	// 		// rpc 主要用来获得 map 和 reduce 的结果会比较方便
	// 		go rpc.ServeConn(conn)
	// 	}
	// }()
}

// no race
func (c *Coordinator) setupMapTask(files []string) {
	n := len(files)
	c.TaskKind = MapTaskFlag
	c.RemainTasksCount = n
	c.WaitingTasks = make(chan Task, len(files))
	c.TaskStatus = make([]bool, n)
	c.TaskStatusItemLock = make([]sync.Mutex, n)

	for i := 0; i < n; i++ {
		c.TaskStatus[i] = false
		t := Task{
			File:     files[i],
			TaskId:   strconv.Itoa(i),
			TaskKind: MapTaskFlag,
		}
		c.WaitingTasks <- t
	}
}

// no race
func getReduceArgFilenameOf(id int) string {
	return fmt.Sprint("reduce-arg-", id)
}

func (c *Coordinator) setupReduceTask() {
	n := c.ReduceNum
	c.TaskKind = ReduceTaskFlag
	c.RemainTasksCount = n
	// c.WaitingTasks = make(chan Task, n*2) // more space for duplicate task(need?)
	c.TaskStatus = make([]bool, n)
	c.TaskStatusItemLock = make([]sync.Mutex, n)

	// Prepare arg files
	{
		var argFiles []*os.File
		for i := 0; i < n; i++ {
			filename := getReduceArgFilenameOf(i)
			f, _ := os.Create(filename)
			argFiles = append(argFiles, f)
		}

		for k, values := range c.KeyValueSet {
			i := ihash(k) % c.ReduceNum
			appendLineTo(argFiles[i], k, values)
		}

		for _, f := range argFiles {
			f.Close()
		}
	}

	// 想一下，当某个任务失败时，这些文件怎么处理
	for i := 0; i < n; i++ {
		// fmt.Println("prepare reduce task ", i)
		c.TaskStatus[i] = false
		filename := getReduceArgFilenameOf(i)
		// below chan will ref this i, so copy to j
		t := Task{
			File:     filename,
			TaskId:   strconv.Itoa(i),
			TaskKind: ReduceTaskFlag,
		}

		c.WaitingTasks <- t
	}
}

func appendLineTo(writer io.Writer, key string, values []string) {
	write := func(s string) {
		n, e := io.WriteString(writer, s)
		if n != len(s) || e != nil {
			panic(e)
		}
	}

	write(key + " ")
	if key == "A" {
		fmt.Println("print A values count:", len(values))
	}
	write(strings.Join(values, " "))
	write("\n")
}

func (c *Coordinator) assignToNewWorkerIfTimeOut(task Task, seconds int) {
	time.Sleep(time.Duration(seconds) * time.Second)
	c.TaskKindLock.Lock()
	if c.TaskKind != task.TaskKind {
		return
	}

	id, _ := strconv.Atoi(task.TaskId)
	c.TaskStatusLock.Lock()
	c.TaskStatusItemLock[id].Lock()
	done := c.TaskStatus[id]
	c.TaskStatusItemLock[id].Unlock()
	c.TaskStatusLock.Unlock()
	c.TaskKindLock.Unlock()

	if !done {
		c.WaitingTasks <- task
		go c.assignToNewWorkerIfTimeOut(task, seconds)
	}
}

// 使用互斥量互斥访问一些共享变量以及一些可能共同调用的过程，比如 setupReduceTask()

func Log(message string) {
	// fmt.Println(message)
}
