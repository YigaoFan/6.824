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
	defer c.TaskKindLock.Unlock()
	if withLog {
		Log("Obtain TaskKindLock")
	}
	c.RemainTasksCountLock.Lock()
	defer c.RemainTasksCountLock.Unlock()
	if withLog {
		Log("Obtain RemainTasksCountLock")
	}
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

	waited := false
	for {
		select {
		case t := <-c.WaitingTasks:
			*task = Task{
				TaskKind: t.TaskKind,
				TaskId:   t.TaskId,
				File:     t.File,
			}
			go c.assignToNewWorkerIfTimeOut(t, 10)
			return nil

		default:
			if waited {
				task.TaskKind = ShutdownFlag
				task.File = ""
				task.TaskId = strconv.Itoa(0)
				return nil
			} else {
				seconds := 2
				time.Sleep(time.Duration(seconds) * time.Second)
				waited = true
			}
		}
	}
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
	Log("upload map task", result.TaskId)
	for _, item := range result.Items {
		k := item.Key
		if ele, ok := c.KeyValueSet[k]; ok {
			c.KeyValueSet[k] = append(ele, item.Values...)
		} else {
			c.KeyValueSet[k] = item.Values
		}
	}
	Log("upload map task", result.TaskId, "done")

	c.KeyValueSetLock.Unlock()

	if c.RemainTasksCount == 0 {
		// 要等 KeyValueSet 的插入操作完了，才能开始下面的 setup
		c.setupReduceTask()
		// close(c.WaitingTasks)
	}

	return nil
}

func (c *Coordinator) UploadReduceResult(result ReduceResult, _ *struct{}) error {
	// 是不是这里也有问题？所有的地方都因为 TaskKindLock 给 block 住了？
	Log("Try to get TaskKindLock")
	c.TaskKindLock.Lock()
	defer c.TaskKindLock.Unlock()
	if c.TaskKind != ReduceTaskFlag {
		return nil
	}
	Log("Try to get RemainTasksCountLock")
	c.RemainTasksCountLock.Lock()
	defer c.RemainTasksCountLock.Unlock()
	if c.RemainTasksCount == 0 {
		return nil
	}
	id, _ := strconv.Atoi(result.TaskId)
	Log("Try to get TaskStatusLock")
	c.TaskStatusLock.Lock()
	defer c.TaskStatusLock.Unlock()
	c.TaskStatusItemLock[id].Lock()
	defer c.TaskStatusItemLock[id].Unlock()
	if c.TaskStatus[id] == true {
		return nil
	}
	c.TaskStatus[id] = true
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
	c.WaitingTasks = make(chan Task, len(files)*2) // 这里也有可能阻塞。。。真是好多地方可以卡住，再想想这个问题及程序
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
		// Log("prepare reduce task ", i)
		c.TaskStatus[i] = false
		filename := getReduceArgFilenameOf(i)
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
	write(strings.Join(values, " "))
	write("\n")
}

func (c *Coordinator) assignToNewWorkerIfTimeOut(task Task, seconds int) {
	time.Sleep(time.Duration(seconds) * time.Second)
	// 这里的锁可能有点问题
	c.TaskKindLock.Lock()
	defer c.TaskKindLock.Unlock()
	if c.TaskKind != task.TaskKind {
		return
	}

	id, _ := strconv.Atoi(task.TaskId)
	c.TaskStatusLock.Lock()
	c.TaskStatusItemLock[id].Lock()
	done := c.TaskStatus[id]
	if !done {
		// Log("assign new: require closed lock")
		// c.WaitingTaskClosedLock.Lock()
		// if !c.WaitingTaskClosed {
		c.WaitingTasks <- task
		go c.assignToNewWorkerIfTimeOut(task, seconds)
		// }
		// c.WaitingTaskClosedLock.Unlock()
		// Log("assign new: unlock closed lock")
	}
	c.TaskStatusItemLock[id].Unlock()
	c.TaskStatusLock.Unlock()

}

// 使用互斥量互斥访问一些共享变量以及一些可能共同调用的过程，比如 setupReduceTask()
