package kvpaxos

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
import "strconv"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  CommandType string
  CommandNum int
  Key string
  Val string
  ClientID int64
  DestID int
  CommandID int64
  Error string
  PrevVal string
  Noop bool
}

type CommandReply struct{
  Val string
  PrevVal string
  ErrorValue string
  SeqFilled bool
  ClientID int64
  DestID int
  CommandNum int
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  HighestExecuted int
  UnappliedCMap map[int]chan CommandReply
  ClientCommandNum map[int64]int
  data map[string]string
  CMap map[int64]Op
  LastReply map[int64]Op
}


func (kv *KVPaxos) execute(tempOp *Op) (string,string,string){
  // kv.mu.Lock()
  // defer kv.mu.Unlock()

  if (tempOp.CommandType == "get"){
    //tempOp.Error = OK
    // kv.mu.Lock()
    // kv.LastReply[tempOp.ClientID] = tempOp
    val, founds := kv.data[tempOp.Key]
    if (founds == false){
      tempOp.Error = ErrNoKey
      return "","","ErrNoKey"
    }else{
      // tempOp.Val = val
      tempOp.Error = OK
      tempOp.Val = val
      return val,"","OK"
    }
    // tempOp.Val = val
//    log.Printf("Server [%d] replying for get [%s]",kv.me, tempOp.Val)
    // fmt.Println("Updatedmap:", kv.data)
    // kv.mu.Unlock()
  }else if (tempOp.CommandType == "put"){
    tempOp.Error = OK
    // kv.mu.Lock()
    kv.data[tempOp.Key] = tempOp.Val
    // kv.mu.Unlock()
    // fmt.Println("Updatedmap:", kv.data)
    // kv.LastReply[tempOp.ClientID] = tempOp
    return "","","OK"
  }else if (tempOp.CommandType == "puthash"){
    tempOp.Error = OK
    //   prev := kv.db[op.Key]
    //   value := strconv.Itoa(int(hash(prev + op.Value)))
    //   op.PreviousValue = prev
    //   kv.db[op.Key] = value

    // var f int
    // kv.mu.Lock()
    val := kv.data[tempOp.Key]
    newval := strconv.Itoa(int(hash(val + tempOp.Val)))
    kv.data[tempOp.Key] = newval
    // kv.LastReply[tempOp.ClientID] = tempOp

    // tt := val
    // if (founds == false){
    //     f = int(hash(tempOp.Val))
    //     // val = ""
    // }else{
    //     f = int(hash(val+tempOp.Val))     
    // }               
    // // f = int(hash(val+tempOp.Val))
    // kv.data[tempOp.Key] = strconv.Itoa(f)
    // fmt.Println("Updatedmap:", kv.data)
    tempOp.PrevVal = val
    return "",val,"OK"
    // kv.mu.Unlock()
  }
  // fmt.Println("Updatedmap:", kv.data)
  return "","","OK"
}

func (kv* KVPaxos) removeExtra(seq int){

}
// log.Printf("TICK: Client [%d] server [%d] seq [%d] Key [%s] value [%s]",Operation.ClientID, Operation.DestID, seq, Operation.Key)
            // log.Printf("server [%d] recieved first seq [%d] ",kv.me, seq)
// func (kv *KVPaxos) tick(){
//   restarted := false
//   seq := 0
//   to := 10 * time.Millisecond
//   for {
//     decided, retOp := kv.px.Status(seq)
//     if decided {
//       restarted = false
//       Operation, isOperation := retOp.(Op)
//       if (isOperation==true){
//         kv.mu.Lock()
//         kv.HighestExecuted = seq
//         if (Operation.Noop == false){ //not no op
//             tempOp,found := kv.CMap[Operation.CommandID]
//             wait,found2 := kv.UnappliedCMap[seq]

//             lastOp,found5 := kv.LastReply[Operation.ClientID]
//             if (found5){
//               if (found2 && Operation.CommandNum <= lastOp.CommandNum){
//               }
//             }else{
//               kv.LastReply[Operation.ClientID] = tempOp
//             }
//             temp := kv.UnappliedCMap
//             if found==false{ //if that command didnt exist in my database
//               kv.execute(&tempOp)
//               kv.CMap[Operation.CommandID] = tempOp
//               kv.mu.Unlock()
//               if found2{//if the sequence existed in my database, but I saw something else
//                   wait <- CommandReply{SeqFilled: true}
//                   <- wait
//       // log.Printf("server [%d] recieved seq [%d]. NOT SEEN before. But restarted seq Val1 [%s], Prevval1 [%s], ErrorVal [%s]",kv.me, seq, Val1, Prevval1, ErrorVal)                
//               }
//             }else if found2{//if that command did exist in my database
//               Val1, Prevval1, ErrorVal := kv.execute(&tempOp)
//               kv.mu.Unlock()
//               wait <- CommandReply{Val: Val1, PrevVal: Prevval1, ErrorValue: ErrorVal, ClientID: tempOp.ClientID, DestID: tempOp.DestID, CommandNum: tempOp.CommandNum}
//               <- wait
//               // log.Printf("server [%d] recieved seq [%d]. My command so executed ",kv.me, seq)
//             }
//               for seq_p, wait_m := range temp {
//               // log.Printf("server [%d] seq_p [%d], seq [%d] ",kv.me, seq_p, seq)
//                 if seq_p < seq{
//                   // log.Printf("Cancelling seq [%d] ",seq_p)
//                   wait_m <- CommandReply{SeqFilled: true}
//                   <- wait_m
//                 }
//               }
//         }else{
//           kv.mu.Unlock()
//           // log.Printf("Noop proposed for seq [%d]", seq)
//         }
//         kv.px.Done(seq)
//       }
//       seq++
//       to = 10 * time.Millisecond
//     }else{
//       time.Sleep(to)
//       // log.Printf("Server [%d] slept for [%d] seq [%d]", kv.me, to/1000000, seq)
//       if to < time.Second {
//         to *= 2
//       }else{//restart
//         if !restarted {
//           // log.Printf("Server [%d] restarted for seq [%d]", kv.me, seq)
//           to = 10 * time.Millisecond
//           kv.px.Start(seq, Op{Noop: true})
//           time.Sleep(time.Millisecond)
//           restarted = true
//         }
//       }
//     }
//   }
// }

func (kv *KVPaxos) tick(){
  restarted := false
  seq := 0
  to := 10 * time.Millisecond
  for {
    decided, retOp := kv.px.Status(seq)
    if decided {
      restarted = false
      Operation, isOperation := retOp.(Op)
      if (isOperation==true){
        kv.mu.Lock()
        kv.HighestExecuted = seq
        if (Operation.Noop == false){ //not no op
            _,found := kv.CMap[Operation.CommandID]
            wait,found2 := kv.UnappliedCMap[seq]
            temp := kv.UnappliedCMap
            lastOp,found5 := kv.LastReply[Operation.ClientID]
            var k bool
            k = true
            if (found5){
              if (Operation.CommandNum == lastOp.CommandNum){
                Operation = lastOp
                k = false
              }else if (Operation.CommandNum < lastOp.CommandNum){
                  k = false
                  Operation.Noop = true
              }
            }

            if found==false{ //if that command didnt exist in my database
              if (k==true){
                kv.execute(&Operation)
                kv.LastReply[Operation.ClientID] = Operation
              }
              kv.CMap[Operation.CommandID] = Operation
              kv.mu.Unlock()
              if found2{//if the sequence existed in my database, but I saw something else
                  wait <- CommandReply{SeqFilled: true}
                  <- wait
      // log.Printf("server [%d] recieved seq [%d]. NOT SEEN before. But restarted seq Val1 [%s], Prevval1 [%s], ErrorVal [%s]",kv.me, seq, Val1, Prevval1, ErrorVal)                
              }
            }else if found2{//if that command did exist in my database
              if (k==true){
                kv.execute(&Operation)
                kv.LastReply[Operation.ClientID] = Operation
              }
              kv.mu.Unlock()
              if (Operation.Noop){
                  wait <- CommandReply{SeqFilled: true}
              }else{
                wait <- CommandReply{Val: Operation.Val, PrevVal: Operation.PrevVal, ErrorValue: Operation.Error, ClientID: Operation.ClientID, DestID: Operation.DestID, CommandNum: Operation.CommandNum}
              }
              <- wait
              // log.Printf("server [%d] recieved seq [%d]. My command so executed ",kv.me, seq)
            }
              for seq_p, wait_m := range temp {
              // log.Printf("server [%d] seq_p [%d], seq [%d] ",kv.me, seq_p, seq)
                if seq_p < seq{
                  // log.Printf("Cancelling seq [%d] ",seq_p)
                  wait_m <- CommandReply{SeqFilled: true}
                  <- wait_m
                }
              }
        }else{
          kv.mu.Unlock()
          // log.Printf("Noop proposed for seq [%d]", seq)
        }
        kv.px.Done(seq)
      }
      seq++
      to = 10 * time.Millisecond
    }else{
      time.Sleep(to)
      // log.Printf("Server [%d] slept for [%d] seq [%d]", kv.me, to/1000000, seq)
      if to < time.Second {
        to *= 2
      }else{//restart
        if !restarted {
          // log.Printf("Server [%d] restarted for seq [%d]", kv.me, seq)
          to = 10 * time.Millisecond
          kv.px.Start(seq, Op{Noop: true})
          time.Sleep(time.Millisecond)
          restarted = true
        }
      }
    }
  }
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  _,found := kv.CMap[args.CommandId]
  if (found == false){
    seq := kv.px.Max() + 1
    _,found = kv.UnappliedCMap[seq]
    // kv.mu.Unlock()
    for found || kv.HighestExecuted >= seq { 
      seq++
      // kv.mu.Lock()
      _,found = kv.UnappliedCMap[seq]
      // kv.mu.Unlock()
    }
    // log.Printf("Server [%d] calling from client [%d] SEQ [%d] highest[%d] for get for args.Key [%s]",kv.me, args.ClientId, seq, kv.HighestExecuted, args.Key)
    Operation := Op{CommandType: "get", Key: args.Key, CommandID: args.CommandId, ClientID: args.ClientId, DestID: kv.me, CommandNum: args.CommandNum}
    wait := make(chan CommandReply)
    // kv.mu.Lock()
    kv.UnappliedCMap[seq] = wait
    kv.CMap[args.CommandId] = Operation
    kv.mu.Unlock()
    kv.px.Start(seq, Operation)
    Creply := <- wait
    // tempOp2 := kv.CMap[args.CommandId]
    if Creply.SeqFilled || Creply.CommandNum!=args.CommandNum || Creply.ClientID!=args.ClientId{
      reply.Err = Err(SeqFilled)
      // log.Printf("Server [%d] recieved ErrorRetry for get request",kv.me)
    }else{
      if (Creply.ErrorValue=="ErrNoKey"){
          reply.Err = Err(ErrNoKey)
      }else{
        // log.Printf("Server [%d] recieved from server [%d] reply to get [%s]",kv.me,  Creply.DestID, Creply.Val)
        reply.Value = Creply.Val
        reply.Err = Err(OK)
      }
    }
    kv.mu.Lock()
    // LastReply map[int64]Op
    //kv.CMap[args.CommandId] = Op{}
    delete(kv.UnappliedCMap, seq)
    kv.mu.Unlock()
    wait <- CommandReply{}
    // kv.mu.Unlock()
  }else{
      kv.mu.Unlock()
      reply.Err = Err(SeqFilled)
      return nil //not sure
  }
  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  // lastcommand,found5 := kv.ClientCommandNum[args.ClientId]
  // if (found5){
  //     if (args.CommandNum <= lastcommand){
  //         kv.mu.Unlock()
  //         reply.Err = Err(SeqFilled)
  //         return nil
  //     }else{
  //         kv.ClientCommandNum[args.ClientId] = args.CommandNum
  //     }
  // }else{
  //     kv.ClientCommandNum[args.ClientId] = args.CommandNum
  // }
  _,found := kv.CMap[args.CommandId]
  if (found == false){
    seq := kv.px.Max() + 1
    _,found = kv.UnappliedCMap[seq]
    // kv.mu.Unlock()
    for found || kv.HighestExecuted >= seq { 
      seq++
      // kv.mu.Lock()
      _,found = kv.UnappliedCMap[seq]
      // kv.mu.Unlock()
    }
    var Operation Op
    if args.DoHash{
      Operation = Op{CommandType: "puthash", Key: args.Key, Val: args.Value, CommandID: args.CommandId, ClientID: args.ClientId, DestID: kv.me, CommandNum: args.CommandNum}
      // log.Printf("Server [%d] calling from client [%d] SEQ [%d] highest[%d] for puthash for args.Key [%s] for value",kv.me, args.ClientId, seq, kv.HighestExecuted, args.Key, args.Value)
    }else{
      Operation = Op{CommandType: "put", Key: args.Key, Val: args.Value, CommandID: args.CommandId, ClientID: args.ClientId, DestID: kv.me, CommandNum: args.CommandNum}    
      // log.Printf("Server [%d] calling from client [%d] SEQ [%d] highest [%d] for put for args.Key [%s] for value",kv.me, args.ClientId, seq, kv.HighestExecuted, args.Key, args.Value)
    }
    wait := make(chan CommandReply)
    // kv.mu.Lock()
    kv.UnappliedCMap[seq] = wait
    kv.CMap[args.CommandId] = Operation
    kv.mu.Unlock()
    kv.px.Start(seq, Operation)
    Creply := <- wait
    if Creply.SeqFilled || Creply.CommandNum!=args.CommandNum || Creply.ClientID!=args.ClientId{
      reply.Err = Err(SeqFilled)
      // log.Printf("Server [%d] recieved ErrorRetry for put request",kv.me)
    }else{
      if args.DoHash{
        reply.PreviousValue = Creply.PrevVal
        // log.Printf("Server [%d] recieved reply from server [%d] to puthash for args.Key [%s], Prev Val [%s]",kv.me, Creply.DestID, args.Key,Creply.PrevVal)
      }else{
        // log.Printf("Server [%d] recieved reply from server [%d] to put for args.Key [%s]",kv.me, Creply.DestID, args.Key)
      }
      // kv.mu.Lock()
      //if (Operation.Error == OK){
      reply.Err = Err(OK)
      //}
    }
    kv.mu.Lock()
    //kv.CMap[args.CommandId] = Op{}
    delete(kv.UnappliedCMap, seq)
    kv.mu.Unlock()
    wait <- CommandReply{}
    
  }else{
    reply.Err = Err(SeqFilled)
    kv.mu.Unlock()
  }
  return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me
  kv.UnappliedCMap = make(map[int]chan CommandReply)
  kv.CMap = make(map[int64]Op)
  kv.data = make(map[string]string)
  kv.ClientCommandNum = make(map[int64]int)
  kv.LastReply = make(map[int64]Op)
  kv.HighestExecuted = -1

  // Your initialization code here.

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
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go kv.tick()
  return kv
}

