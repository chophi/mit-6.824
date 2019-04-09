package mapreduce

import "fmt"
import "sync"
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

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	finished := make(chan bool)
	var waitAllTasksComplete = sync.WaitGroup{}
	waitAllTasksComplete.Add(ntasks)

	go func() {
		taskId := 0
		taskRet := make(chan [2]int)
		for {
			select {
			case tR := <- taskRet:
				if tR[0] == 1 {
					waitAllTasksComplete.Done()
				}
			case <- finished:
				return
			case rpc := <-registerChan:
				taskArgs := DoTaskArgs {
					JobName : jobName,
					File : mapFiles[taskId],
					Phase: phase,
					TaskNumber : taskId,
					NumOtherPhase : n_other,
				}
				go func() {
					ret := call(rpc, "Worker.DoTask", &taskArgs, nil)
					success := 0
					if ret {
						success = 1
					}
					taskState := [2]int {success, taskArgs.TaskNumber}
					taskRet <- taskState
				} ()
				taskId++
			}
		}
	} ()

	go func() {
		waitAllTasksComplete.Wait()
		finished <- true
	} ()
	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
