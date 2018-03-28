package mapreduce

import (
	"fmt"
	"sync"
	"errors"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	var wg sync.WaitGroup

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		println("111111:",mapFiles[i])

		var doTask DoTaskArgs
		doTask.File = mapFiles[i]
		doTask.JobName = jobName
		doTask.Phase = phase
		doTask.TaskNumber = i
		doTask.NumOtherPhase = n_other

		//一定要传值！！！不然go并行运行，下面的值会引起混乱
		go func(doTask DoTaskArgs) {
			defer wg.Done()

			for{
				err := sendWork(doTask, registerChan)
				fmt.Printf("doTask:%v.%v.%v",doTask.File, doTask.Phase, doTask.TaskNumber)
				if err == nil{
					break
				}
			}

		}(doTask)
		fmt.Printf("schedule...i:%v, mapFileName:%v\n", i, mapFiles[i])
	}

	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}

func sendWork(doTask DoTaskArgs, registerChan chan string) error {
	println("111111:",doTask.File, doTask.Phase,doTask.TaskNumber)

	ch_worker := <- registerChan
	println("222222:",doTask.File, doTask.Phase,doTask.TaskNumber)
	ok := call(ch_worker, "Worker.DoTask", doTask, nil)
	//worker工作完成，再次放在注册列表等待分配任务
	println("3333333:",doTask.File, doTask.Phase,doTask.TaskNumber, ok)
	if ok{
	//用go执行，不会阻塞进程。
	//最后几个registerChan不会有人读，所以这里不会go会卡死
	//主进程结束，这里也会停止
		go func() {
			registerChan <- ch_worker
		}()
		return nil
	}
	println("444444:",doTask.File, doTask.Phase,doTask.TaskNumber, ok)
	var err error = errors.New("worker 调用失败")
	return err
}