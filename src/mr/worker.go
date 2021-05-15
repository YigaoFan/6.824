package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strings"
)

// 凡是个类型都要像下面这样声明一套可以排序的接口，很麻烦吧？
// for sorting by key.
type ByKey []Pair

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of Pair.
//
type Pair struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func divideIntoItems(pairs []Pair) []Set {
	first := Set{Key: pairs[0].Key, Values: []string{pairs[0].Value}}
	items := []Set{first}
	for _, p := range pairs[1:] {
		last := &items[len(items)-1]
		if p.Key == last.Key {
			last.Values = append(last.Values, p.Value)
		} else {
			i := Set{Key: p.Key, Values: []string{p.Value}}
			items = append(items, i)
		}
	}
	return items
}

func readFrom(reader *bufio.Reader) (bool, string, []string) {
	line, e := reader.ReadString('\n')
	// fmt.Println("read line ", line)
	words := strings.Fields(line) // 这里会在 words 里保留换行符吗？
	end := e == io.EOF
	if !end {
		return end, words[0], words[1:]
	} else {
		return end, "", nil
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []Pair, reducef func(string, []string) string) {
	client := MakeRpcClient()
	defer client.Close()
	for {
		// 对端的 server 如果退出了，下面这个会有什么反应
		task := Task{TaskKind: ReduceTaskFlag, TaskId: "10"}

		// fmt.Println("request task")
		status := client.Call("Coordinator.RequestTask", struct{}{}, &task)
		// fmt.Println("Get response", task)
		if status == false {
			break
		}

		switch task.TaskKind {
		case MapTaskFlag:
			fmt.Println("get map task ", task.TaskId)
			intermediate := mapf(task.File, readFileToString(task.File))
			fmt.Println("map task done")
			sort.Sort(ByKey(intermediate))
			r := MapResult{TaskId: task.TaskId, Items: divideIntoItems(intermediate)}
			client.Call("Coordinator.UploadMapResult", r, nil)
			fmt.Println("map result upload")

		case ReduceTaskFlag:
			fmt.Println("get reduce task ", task.TaskId)
			filename := fmt.Sprint("mr-out-", task.TaskId)
			f, _ := os.Create(filename)
			defer f.Close()
			argFile, _ := os.Open(task.File)
			reader := bufio.NewReader(argFile)

			for {
				end, k, vs := readFrom(reader)
				if end {
					break
				}
				// fmt.Println("key: ", k, "values: ", vs)

				v := reducef(k, vs)
				fmt.Fprintf(f, "%v %v\n", k, v)
			}

			result := ReduceResult{TaskId: task.TaskId, Filename: filename}
			client.Call("Coordinator.UploadReduceResult", result, nil)
			fmt.Println("reduce result upload")

		case ShutdownFlag:
			fallthrough
		default:
			return
		}
	}
}

func readFileToString(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return string(content)
}
