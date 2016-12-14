package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  CommandNum int
  UnappliedCMap map[int]chan int
  HighestExecuted int
}


type Op struct {
  // Your data here.
  Origin int
  CommandNum int
  CommandType string
  JoinArguments JoinArgs
  LeaveArguments LeaveArgs
  MoveArguments MoveArgs
  Noop bool 

}

func bounds(dist map[int64]int)(int64, int64, bool){
  var min int = NShards+1
  var minGroup, maxGroup int64
  var max int = 0
  if (len(dist) == 0){
    return minGroup, maxGroup, false
  }
  for group, num_replicas := range dist {
    if (num_replicas < min){
      min = num_replicas
      minGroup = group
    }
    if (num_replicas >= max){
      max = num_replicas
      maxGroup = group
    }
  }
  var k bool = false
  if (min<max-1){
    k = true
  }
  return minGroup, maxGroup, k
}

func (sm *ShardMaster) execute(op Op){

  currconfig := sm.configs[len(sm.configs)-1]
  curr_dist := make(map[int64]int)
  if (op.CommandType=="join"){
    curr_dist[op.JoinArguments.GID] = 0
    for index, group := range currconfig.Shards {
      if group == 0 {
        currconfig.Shards[index] = op.JoinArguments.GID
        curr_dist[op.JoinArguments.GID] += 1
      } else {
        _, ok := curr_dist[group];
        if (ok){
          curr_dist[group] += 1
        }else{
          curr_dist[group] = 1
        }
      }
    }
  }else if (op.CommandType=="leave"){
    for k, _ := range currconfig.Groups {
      if (k != op.LeaveArguments.GID){
        curr_dist[k] = 0
      }
    }
    var temp int64 = 0
    for _, group := range currconfig.Shards {
      if (group !=  op.LeaveArguments.GID){
        temp = group
        break
      }
    }
    if (temp != 0){
    for index, group := range currconfig.Shards {
      if (group ==  op.LeaveArguments.GID){
        currconfig.Shards[index] = temp
        curr_dist[temp] += 1
      }else{
        curr_dist[group] += 1
      }
    }
    }
  }else if (op.CommandType=="move"){
    currconfig.Shards[op.MoveArguments.Shard] = op.MoveArguments.GID
  }

  if ((op.CommandType=="join") || (op.CommandType=="leave")){ //rebalance
    // log.Printf("Before: Server [%d] curr_dist [%v]",sm.me, curr_dist)
    minGroup, maxGroup, redistribute := bounds(curr_dist)
    for (redistribute){
      for index, group := range currconfig.Shards {
        if (group==maxGroup){
            currconfig.Shards[index] = minGroup
            break
        }
      }
      curr_dist[maxGroup]--
      curr_dist[minGroup]++
      minGroup, maxGroup, redistribute = bounds(curr_dist)
    }
    // log.Printf("After: Server [%d] curr_dist [%v]",sm.me, curr_dist)
  }

  newGroups := make(map[int64][]string)
  for k,v := range currconfig.Groups{ //copy to new map
    newGroups[k] = v
  }
  if (op.CommandType=="join"){
    newGroups[op.JoinArguments.GID] = op.JoinArguments.Servers
  }else if (op.CommandType=="leave"){
    delete(newGroups, op.LeaveArguments.GID)
  }
  

  newconfig := Config{Num: len(sm.configs), Shards: currconfig.Shards, Groups: newGroups}
  

  sm.configs = append(sm.configs, newconfig)
}

func (sm *ShardMaster) tick(){
  replay := false
  seq := 0
  to := 10 * time.Millisecond
  for {
    decided, retOp := sm.px.Status(seq)
    if decided {
      replay = false
      Operation, isOperation := retOp.(Op)
      if (isOperation==true){
        sm.mu.Lock()
        sm.HighestExecuted = seq
        if (Operation.Noop == false){ //not no op
            wait,found := sm.UnappliedCMap[seq]
            temp := sm.UnappliedCMap
            // log.Printf("server [%d] agreed seq [%d] CommandType [%s]",sm.me, seq, Operation.CommandType)
            if (Operation.CommandType!="query"){
              sm.execute(Operation)
            }
            sm.mu.Unlock()
            if found{//if that command did exist in my database 
              if (Operation.Origin != sm.me){//if the sequence existed in my database, but I saw something else
                  wait <- -1// log.Printf("server [%d] recieved seq [%d]. NOT SEEN before. But restarted seq Val1 [%s], Prevval1 [%s], ErrorVal [%s]",sm.me, seq, Val1, Prevval1, ErrorVal)
              }else{
                  wait <- Operation.CommandNum
              }
              <- wait
            }
            for seq_p, wait_m := range temp {
            // log.Printf("server [%d] seq_p [%d], seq [%d] ",sm.me, seq_p, seq)
              if seq_p < seq{
                // log.Printf("Cancelling seq [%d] ",seq_p)
                wait_m <- -1
                <- wait_m
              }
            }
        }else{
          sm.mu.Unlock()
          // log.Printf("Noop proposed for seq [%d]", seq)
        }
        sm.px.Done(seq)
      }
      seq++
      to = 10 * time.Millisecond
    }else{
      time.Sleep(to)
      // log.Printf("Server [%d] slept for [%d] seq [%d]", sm.me, to/1000000, seq)
      if to < time.Second {
        to *= 2
      }else{//restart
        if !replay {
          replay = true
          sm.px.Start(seq, Op{Noop: true})
          // log.Printf("Server [%d] restarted for seq [%d]", sm.me, seq)
          time.Sleep(time.Millisecond)
          to = 10 * time.Millisecond
        }
      }
    }
  }
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
  var repeat bool = true
  for (repeat==true){
    sm.mu.Lock()
    seq := sm.px.Max() + 1
    _,found := sm.UnappliedCMap[seq]
    for found || sm.HighestExecuted >= seq { 
      seq++
      _,found = sm.UnappliedCMap[seq]
    }
    sm.CommandNum++
    // log.Printf("Server [%d] SEQ [%d] to join GID [%d] Servers [%v]",sm.me, seq, args.GID, args.Servers)

    // log.Printf("Server [%d] SEQ [%d] highest[%d] to join for GI [%d]",sm.me, seq, sm.HighestExecuted, args.GID)
    Operation := Op{CommandType: "join", JoinArguments: *args, Origin: sm.me, CommandNum: sm.CommandNum}
    wait := make(chan int)
    sm.UnappliedCMap[seq] = wait
    sm.mu.Unlock()
    sm.px.Start(seq, Operation)
    acceptedCommandNum := <- wait
    repeat = false
    if acceptedCommandNum==-1 || acceptedCommandNum!=Operation.CommandNum{ 
      repeat = true
      // log.Printf("Server [%d] recieved ErrorRetry for get request",sm.me)
    }
    // if (repeat==false){
    //   log.Printf("Accepted: Server [%d] SEQ [%d] to join GID [%d] Servers [%v]",sm.me, seq, args.GID, args.Servers)
    // }
    sm.mu.Lock()
    delete(sm.UnappliedCMap, seq)
    wait <- -1
    sm.mu.Unlock()
  }
  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
  var repeat bool = true
  for (repeat==true){
    sm.mu.Lock()
    seq := sm.px.Max() + 1
    _,found := sm.UnappliedCMap[seq]
    for found || sm.HighestExecuted >= seq { 
      seq++
      _,found = sm.UnappliedCMap[seq]
    }
    sm.CommandNum++
    // log.Printf("Server [%d] SEQ [%d] highest[%d] to join for GI [%d]",sm.me, seq, sm.HighestExecuted, args.GID)
    // log.Printf("Server [%d] SEQ [%d] to leave GID [%d]",sm.me, seq, args.GID)

    Operation := Op{CommandType: "leave", LeaveArguments: *args, Origin: sm.me, CommandNum: sm.CommandNum}
    wait := make(chan int)
    sm.UnappliedCMap[seq] = wait
    sm.mu.Unlock()
    sm.px.Start(seq, Operation)
    acceptedCommandNum := <- wait
    repeat = false
    if acceptedCommandNum==-1 || acceptedCommandNum!=Operation.CommandNum{ 
      repeat = true
      // log.Printf("Server [%d] recieved ErrorRetry for get request",sm.me)
    }
    // if (repeat==false){
    //   log.Printf("Accepted: Server [%d] SEQ [%d] to leave GID [%d]",sm.me, seq, args.GID)
    // }
    sm.mu.Lock()
    delete(sm.UnappliedCMap, seq)
    wait <- -1
    sm.mu.Unlock()
  }
  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  // Your code here.
  var repeat bool = true
  for (repeat==true){
    sm.mu.Lock()
    seq := sm.px.Max() + 1
    _,found := sm.UnappliedCMap[seq]
    for found || sm.HighestExecuted >= seq { 
      seq++
      _,found = sm.UnappliedCMap[seq]
    }
    sm.CommandNum++
    // log.Printf("Server [%d] SEQ [%d] highest[%d] to join for GI [%d]",sm.me, seq, sm.HighestExecuted, args.GID)
    // log.Printf("Server [%d] SEQ [%d] to move GID [%d]",sm.me, seq, args.GID)

    Operation := Op{CommandType: "move", MoveArguments: *args, Origin: sm.me, CommandNum: sm.CommandNum}
    wait := make(chan int)
    sm.UnappliedCMap[seq] = wait
    sm.mu.Unlock()
    sm.px.Start(seq, Operation)
    acceptedCommandNum := <- wait
    repeat = false
    if acceptedCommandNum==-1 || acceptedCommandNum!=Operation.CommandNum{ 
      repeat = true
      // log.Printf("Server [%d] recieved ErrorRetry for get request",sm.me)
    }
    // if (repeat==false){
    //   log.Printf("Accepted: Server [%d] SEQ [%d] to move GID [%d]",sm.me, seq, args.GID)
    // }
    sm.mu.Lock()
    delete(sm.UnappliedCMap, seq)
    wait <- -1
    sm.mu.Unlock()
  }
  return nil
}



func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.
  var repeat bool = true
  for (repeat==true){
    sm.mu.Lock()
    seq := sm.px.Max() + 1
    _,found := sm.UnappliedCMap[seq]
    for found || sm.HighestExecuted >= seq { 
      seq++
      _,found = sm.UnappliedCMap[seq]
    }
    sm.CommandNum++
    // log.Printf("Server [%d] SEQ [%d] highest[%d] to query for Num [%d]",sm.me, seq, sm.HighestExecuted, args.Num)
    // log.Printf("Server [%d] calling from client [%d] SEQ [%d] highest[%d] for get for args.Key [%s]",sm.me, args.ClientId, seq, sm.HighestExecuted, args.Key)
    Operation := Op{CommandType: "query", Origin: sm.me, CommandNum: sm.CommandNum}
    wait := make(chan int)
    sm.UnappliedCMap[seq] = wait
    sm.mu.Unlock()
    sm.px.Start(seq, Operation)
    acceptedCommandNum := <- wait
    repeat = false
    if acceptedCommandNum==-1 || acceptedCommandNum!=Operation.CommandNum{ 
      repeat = true
      // log.Printf("Server [%d] recieved ErrorRetry for get request",sm.me)
    }else{
      if (args.Num >=0 && args.Num < len(sm.configs)){
        // log.Printf("Server [%d] SEQ [%d] recieved reply to query for Num [%s]",sm.me, seq, sm.configs[args.Num])
        reply.Config = sm.configs[args.Num]
      }else{
        // log.Printf("Server [%d] SEQ [%d] recieved reply to query Config Num [%d] Shards [%v] Groups [%v]",sm.me, seq, sm.configs[len(sm.configs)-1].Num, sm.configs[len(sm.configs)-1].Shards, sm.configs[len(sm.configs)-1].Groups)
        reply.Config = sm.configs[len(sm.configs)-1]
      }
    }
    sm.mu.Lock()
    delete(sm.UnappliedCMap, seq)
    wait <- -1
    sm.mu.Unlock()
  }
  return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})
  gob.Register(JoinArgs{})
  gob.Register(LeaveArgs{})
  gob.Register(MoveArgs{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  //my variables
  sm.UnappliedCMap = make(map[int]chan int)
  sm.HighestExecuted = -1

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()
  go sm.tick()
  return sm
}
