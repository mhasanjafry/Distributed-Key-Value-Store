package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"

type Op struct {
  // Your definitions here.
  CommandType string
  ClientID string
  CommandNum int
  Key string
  Val string
  PrevVal string
  Err Err
  ReconfArgs *ReconfArgs
}

type shardstruct struct {
  shardsdata[] map[int]map[string]string
  latestconfig int
}

type responses struct {
  data[] map[int]map[string]*Op
  latestdata int
}

type CommandReply struct{
  Val string
  PrevVal string
  Err Err

  ClientID string
  CommandNum int
  ReconfArgs *ReconfArgs
}

type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  // HighestExecuted int
  randomnumber int
  client *responses
  shards *shardstruct
  config *shardmaster.Config
  UnappliedCMap map[int]chan CommandReply

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
// log.Printf("Get called")
  kv.mu.Lock()
  if kv.config.Num == 0 {
    reply.Err = Err(ErrWrongGroup)
    kv.mu.Unlock()
    return nil
  }
  seq := kv.px.Max() + 1
  _,found := kv.UnappliedCMap[seq]
  for found { 
    seq++
    _,found = kv.UnappliedCMap[seq]
  }
  wait := make(chan CommandReply)
  kv.UnappliedCMap[seq] = wait
  kv.mu.Unlock()

  ID := strconv.FormatInt(args.ClientID, 10)
  Operation := Op{CommandType: "get", ClientID: ID, CommandNum: args.CommandNum, Key: args.Key}

  kv.px.Start(seq, Operation)
  Creply := <- wait

  if (Operation.ClientID == Creply.ClientID && Operation.CommandNum == Creply.CommandNum){
    // if Creply.SeqFilled{ //think about it more, of what to do.
    //   reply.Err = Err(SeqFilled)
    // }else{
      reply.Err = Creply.Err
      reply.Value = Creply.Val

      kv.mu.Lock()
      delete(kv.UnappliedCMap, seq)
      kv.mu.Unlock()
      wait <- CommandReply{}
      return nil
    // }
  }else{
    kv.mu.Lock();
    delete(kv.UnappliedCMap, seq)
    kv.mu.Unlock()
    wait <- CommandReply{}
    kv.Get(args, reply)
    return nil
  }

}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
// log.Printf("Put called")
  kv.mu.Lock()
  if kv.config.Num == 0 {
    reply.Err = Err(ErrWrongGroup)
    kv.mu.Unlock()
    return nil
  }
  seq := kv.px.Max() + 1
  _,found := kv.UnappliedCMap[seq]
  for found{ 
    seq++
    _,found = kv.UnappliedCMap[seq]
  }
  wait := make(chan CommandReply)
  kv.UnappliedCMap[seq] = wait

  kv.mu.Unlock()

  ID := strconv.FormatInt(args.ClientID, 10)
  var Operation Op
  if args.DoHash{
    Operation = Op{CommandType: "puthash", ClientID: ID, CommandNum: args.CommandNum, Key: args.Key, Val: args.Value}
  }else{
    Operation = Op{CommandType: "put", ClientID: ID, CommandNum: args.CommandNum, Key: args.Key, Val: args.Value}    
  }

  kv.px.Start(seq, Operation)
  Creply := <- wait
  if (Operation.ClientID == Creply.ClientID && Operation.CommandNum == Creply.CommandNum){
    // if Creply.SeqFilled{ //think about it more, of what to do.
    //   reply.Err = Err(SeqFilled)
    // }else{
      reply.Err = Creply.Err
      if Operation.CommandType == "puthash"{
        reply.PreviousValue = Creply.PrevVal
      }
      // reply.Err = Err(OK)
      kv.mu.Lock()
      delete(kv.UnappliedCMap, seq)
      kv.mu.Unlock()
      wait <- CommandReply{}
      return nil
    // }
  }else{
    kv.mu.Lock()
    delete(kv.UnappliedCMap, seq)
    kv.mu.Unlock()
    wait <- CommandReply{}
    kv.Put(args, reply)
    return nil
  }

}

func (kv *ShardKV) Reconf(args *ReconfArgs) error {
// log.Printf("recon called")

  kv.mu.Lock()
  seq := kv.px.Max() + 1
  _,found := kv.UnappliedCMap[seq]
  for found{ 
    seq++
    _,found = kv.UnappliedCMap[seq]
  }
  wait := make(chan CommandReply)
  kv.UnappliedCMap[seq] = wait
  kv.mu.Unlock()


  ID := "cronf" + strconv.FormatInt(4136792947444620313, 10)
  Operation := Op{CommandType: "reconf", ClientID: ID, CommandNum: args.Config.Num, ReconfArgs: args}


  kv.px.Start(seq, Operation)
  Creply := <- wait

  kv.mu.Lock()
  delete(kv.UnappliedCMap, seq)
  kv.mu.Unlock()
  wait <- CommandReply{}

  if (Operation.ClientID != Creply.ClientID || Operation.CommandNum != Creply.CommandNum){
    kv.Reconf(args)
  }
  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
// log.Printf("tick called")

  kv.mu.Lock()
  defer kv.mu.Unlock()
  latest := kv.sm.Query(-1)
  var next shardmaster.Config
  for kv.config.Num < latest.Num {
    next = kv.sm.Query(kv.config.Num + 1)
    kv.mu.Unlock()
    kv.Reconf(&ReconfArgs{Config: next})
    kv.mu.Lock()
  }
}

func (kv *ShardKV) execute(tempOp *Op){
// log.Printf("execute called")

  if (tempOp.CommandType == "reconf" && kv.config.Num+1 == tempOp.ReconfArgs.Config.Num) {
        // log.Printf("recon execute called %d %d", kv.config.Num, tempOp.ReconfArgs.Config.Num)
        kv.shards.latestconfig = kv.config.Num+1
        kv.client.latestdata = kv.config.Num+1      
        // log.Printf("recon execute 2 called %d %d", kv.config.Num, tempOp.ReconfArgs.Config.Num)
        if kv.config.Num == 0 {
          // log.Printf("recon execute 3 called %d %d", kv.config.Num, tempOp.ReconfArgs.Config.Num)
            for n := 0; n < shardmaster.NShards; n++ {
              kv.shards.shardsdata[n][1] = make(map[string]string)
            }  
            for n := 0; n < shardmaster.NShards; n++ {
              kv.client.data[n][1] = make(map[string]*Op)
            }
        }else{
            clientdata := make([]map[string]*Op, shardmaster.NShards)
            sharddata := make([]map[string]string, shardmaster.NShards)
            for s, g := range tempOp.ReconfArgs.Config.Shards {
                oldGID := kv.config.Shards[s]
                if g == kv.gid && oldGID != kv.gid {
                    args := &GetShardArgs{Config: tempOp.ReconfArgs.Config.Num, Shard: s}
                    var reply *GetShardReply
                    servers := kv.config.Groups[oldGID]
                    for {
                        server_num := kv.randomnumber % len(servers)
                        reply = &GetShardReply{}
                        kv.mu.Unlock()
                        ok := call(servers[server_num], "ShardKV.GetShard", args, &reply)
                        if ok && reply.Err == ""{
                          kv.mu.Lock()
                          break
                        } else {
                          time.Sleep(250 * time.Millisecond)
                        }
                        kv.mu.Lock()
                        kv.randomnumber++
                    }
                    clientdata[s] = reply.ClientData
                    sharddata[s] = reply.Shard
                }
            }
            for n, s := range sharddata {
                if s != nil {
                  kv.shards.shardsdata[n][tempOp.ReconfArgs.Config.Num] = s
                } else {
                  kv.shards.shardsdata[n][tempOp.ReconfArgs.Config.Num] = kv.shards.shardsdata[n][tempOp.ReconfArgs.Config.Num-1]
                }
            }
            for n, s := range clientdata {
                if s != nil {
                  kv.client.data[n][tempOp.ReconfArgs.Config.Num] = s
                } else {//copy the old one
                  kv.client.data[n][tempOp.ReconfArgs.Config.Num] = kv.client.data[n][tempOp.ReconfArgs.Config.Num-1]
                }
            }
        }
        // log.Printf("recon execute 6 called %d %d", kv.config.Num, tempOp.ReconfArgs.Config.Num)
        kv.config = &tempOp.ReconfArgs.Config        
        // log.Printf("recon execute 7 called %d %d", kv.config.Num, tempOp.ReconfArgs.Config.Num)
     
  }else if (tempOp.CommandType != "noop"){
    shard := key2shard(tempOp.Key)
    if kv.config.Shards[shard] == kv.gid {
      s := kv.shards.shardsdata[shard][kv.shards.latestconfig]
      sclient := kv.client.data[shard][kv.client.latestdata]
      if (tempOp.CommandType == "get"){
        // log.Printf("get execute called")
        val, founds := s[tempOp.Key]
        if (founds == false){
          tempOp.Err = Err(ErrNoKey)
          tempOp.Val = ""
        }else{
          tempOp.Val = val
          sclient[tempOp.ClientID] = tempOp
        }
      }else if (tempOp.CommandType == "put"){
        // log.Printf("put execute called")
        s[tempOp.Key] = tempOp.Val
        sclient[tempOp.ClientID] = tempOp
      }else if (tempOp.CommandType == "puthash"){
        // log.Printf("hash execute called")
        val := s[tempOp.Key]
        newval := strconv.Itoa(int(hash(val + tempOp.Val)))
        s[tempOp.Key] = newval
        tempOp.PrevVal = val
        sclient[tempOp.ClientID] = tempOp
      }
    }else{
      tempOp.Err = Err(ErrWrongGroup)
    }
  }
            // log.Printf("recon execute 8 called")

}

func (kv *ShardKV) checkPaxosConsensus(){
// log.Printf("paxos consensus called")

  restarted := false
  seq := 0
  to := 10 * time.Millisecond
  for !kv.dead{
    decided, retOp := kv.px.Status(seq)
    if decided {
      restarted = false
      Operation, isOperation := retOp.(Op)
      if (isOperation==true){
          kv.mu.Lock()
          // kv.HighestExecuted = seq
          if (Operation.CommandType != "noop"){ //not no op
              if kv.config.Num == 0 {
                  kv.execute(&Operation)
              }else{
                  if (Operation.CommandType == "reconf"){
                      if Operation.ReconfArgs.Config.Num <= kv.config.Num {
                          // var w = Operation.ClientID
                          // var w2 = Operation.CommandNum
                          // Operation = Op{ClientID: w, CommandNum: w2}
                      } else {
                          kv.execute(&Operation)
                      }                
                  }else if (Operation.CommandType == "get" || Operation.CommandType == "put" || Operation.CommandType == "puthash"){
                      shard := key2shard(Operation.Key)
                      s := kv.client.data[shard][kv.client.latestdata]
                      v,ok := s[Operation.ClientID]
                      if (ok){
                          if Operation.CommandNum == v.CommandNum {
                              Operation = *v
                          } else if Operation.CommandNum < v.CommandNum {
                              Operation = Op{}
                          }else{
                              kv.execute(&Operation)
                          }
                      }else{
                          kv.execute(&Operation)
                      }
                  }
              }
              if wait,found := kv.UnappliedCMap[seq]; found {
                  kv.mu.Unlock()
                  wait <- CommandReply{Val: Operation.Val, PrevVal: Operation.PrevVal, Err: Operation.Err, ReconfArgs: Operation.ReconfArgs, ClientID: Operation.ClientID, CommandNum: Operation.CommandNum}
                  <- wait
              } else {
                  kv.mu.Unlock()
              }
          }else{
              kv.mu.Unlock()
          }
      }
      // kv.px.Done(seq)
      seq++
      // to = 10 * time.Millisecond
    }else{
        time.Sleep(to)
        if to < time.Second {
            to *= 2
        }else{//restart
            if !restarted {
              restarted = true
              time.Sleep(time.Millisecond)
              to = 10 * time.Millisecond
              kv.px.Start(seq, Op{CommandType: "noop"})
            }
        }
    }
  }
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {
  kv.mu.Lock()
  if kv.config.Num < args.Config {
    reply.Err = ErrNoKey
  }else if args.Shard >=0 && shardmaster.NShards >= args.Shard {
    reply.ClientData = kv.client.data[args.Shard][args.Config]
    reply.Shard = kv.shards.shardsdata[args.Shard][args.Config]
  } else {
    log.Fatalf("Invalid shard number. Quit")
  }
  kv.mu.Unlock()
  return nil


}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {

  

  gob.Register(Op{})
  // log.Printf("Server starting")
  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)
  kv.client = &responses{data: make([]map[int]map[string]*Op, shardmaster.NShards), latestdata: 0}
  // Your initialization code here.
  // Don't call Join().
  
  kv.shards = &shardstruct{shardsdata: make([]map[int]map[string]string, shardmaster.NShards), latestconfig: 0}
  kv.UnappliedCMap = make(map[int]chan CommandReply)
  kv.config = &shardmaster.Config{}
  kv.randomnumber = rand.Int()
  
  for i := 0; i < shardmaster.NShards; i++ {
    kv.shards.shardsdata[i] = make(map[int]map[string]string)
  }
  for i := 0; i < shardmaster.NShards; i++  {
    kv.client.data[i] = make(map[int]map[string]*Op)
  }
  // kv.HighestExecuted = -1

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)


  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && kv.dead == false {
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()
  go kv.checkPaxosConsensus()
  return kv
}
