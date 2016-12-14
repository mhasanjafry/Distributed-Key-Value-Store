package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  // You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func send_map(mr *MapReduce, worker string, index int) bool {
    var jobReply DoJobReply
    jobArgs:= DoJobArgs{File: mr.file, Operation: Map, JobNumber: index, NumOtherPhase: mr.nReduce}
    return call(worker, "Worker.DoJob", jobArgs, &jobReply)
}

func send_reduce(mr *MapReduce, worker string, index int) bool {
    var jobReply DoJobReply
    jobArgs:= DoJobArgs{File: mr.file, Operation: Reduce, JobNumber: index, NumOtherPhase: mr.nMap}
    return call(worker, "Worker.DoJob", jobArgs, &jobReply)
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  var mapChan, reduceChan = make(chan int, mr.nMap), make(chan int, mr.nReduce)

  for k := 0; k < mr.nMap; k++ {
    go func(ind int) {
      for {
        var worker string
        var done bool
        select {
          case worker = <-mr.Chanlidle:
            done = send_map(mr, worker, ind)
          case worker = <-mr.registerChannel:
            done = send_map(mr, worker, ind)
        }
        if done {
          mapChan <- ind
          mr.Chanlidle <- worker
          return
        }
      }
    }(k)
  }

  for k := 0; k < mr.nMap; k++ {
    <-mapChan
  }

  for k := 0; k < mr.nReduce; k++ {
    go func(ind int) {
      for {
        var worker string
        var done bool
        select {
          case worker = <-mr.Chanlidle:
            done = send_reduce(mr, worker, ind)
          case worker = <-mr.registerChannel:
            done = send_reduce(mr, worker, ind)
        }
        if done {
          reduceChan <- ind
          mr.Chanlidle <- worker
          return
        }
      }
    }(k)
  }

  for k := 0; k < mr.nReduce; k++ {
    <-reduceChan
  }

  return mr.KillWorkers()
}
