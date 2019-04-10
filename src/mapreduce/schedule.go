package mapreduce

import "fmt"
import "sync"
import "container/list"
// import "time"
//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
type WorkerStatus struct {
	taskId int
	exitSuccessfully bool
	rpcName string
	reschedule bool
}

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

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	finished := make(chan bool)
	var waitAllTasksComplete = sync.WaitGroup{}
	waitAllTasksComplete.Add(ntasks)

	tasks := list.New()
	for i := 0; i < ntasks; i++ {
		tasks.PushBack(i)
	}
	var taskMux sync.Mutex

	taskRet := make(chan WorkerStatus)
	var call_worker_with_next_task = func(rpc string, taskExit *chan WorkerStatus) bool {
		taskMux.Lock()
		defer taskMux.Unlock()
		if tasks.Len() == 0 {
			fmt.Printf("No task to Schedule!!\n")
			return false
		}
		var taskId = tasks.Remove(tasks.Front()).(int)
		
		taskArgs := DoTaskArgs {
			JobName : jobName,
			File : mapFiles[taskId],
			Phase: phase,
			TaskNumber : taskId,
			NumOtherPhase : n_other,
		}
		go func() {
			ret := call(rpc, "Worker.DoTask", &taskArgs, nil)
			taskState := WorkerStatus {
				taskId: taskId,
				exitSuccessfully: ret,
				rpcName: rpc,
				reschedule: false,
			}
			*taskExit <- taskState
		} ()
		return true
	}

	go func() {
		for {
			select {
			case <- finished:
				return
			case tR := <- taskRet:
				if tR.exitSuccessfully && !tR.reschedule{
					fmt.Printf("task %d done\n", tR.taskId)
					waitAllTasksComplete.Done()
				} 
				call_worker_with_next_task(tR.rpcName, &taskRet)
				if !tR.exitSuccessfully && !tR.reschedule {
					func() {
						taskMux.Lock()
						defer taskMux.Unlock()
						// fmt.Printf("task %d failed, need to reschedule\n", tR.taskId)
						tasks.PushBack(tR.taskId)
					}()
				}
			case rpc := <-registerChan:
				call_worker_with_next_task(rpc, &taskRet)
			}
		}
	} ()

	waitAllTasksComplete.Wait()
	finished <- true
	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
