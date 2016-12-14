package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
  data map[string]string
  amPrimary bool
  amBackup bool
  haveBackup bool
  Backup string
  mu sync.Mutex
  set map[int64]string
}

func (pb *PBServer) ForwardPut(args *PutArgs, reply *PutReply) error {
  // Your code here
//  fmt.Println("Updated map:", pb.data)
  pb.tick()
  pb.mu.Lock()
  var backup = pb.amBackup
  pb.mu.Unlock()

  pb.mu.Lock()

  if (backup){
          var founds bool
          _,founds = pb.set[args.CommandId]
          // pb.mu.Unlock()
          if (founds == false){
  //          pb.mu.Lock()
            pb.data[args.Key] = args.Value
            pb.set[args.CommandId] = args.Key
  //          pb.mu.Unlock()
          }
  //  pb.mu.Lock()
    pb.data[args.Key] = args.Value
    // fmt.Println("Updated map at Backup:", pb.data)
  //  pb.mu.Unlock()

    reply.Err = OK
  }else{
    reply.Err = ErrWrongServer
  }
  pb.mu.Unlock()

  return nil
}

func (pb *PBServer) ForwardGet(args *GetArgs, reply *GetReply) error {
  // Your code here.
  pb.tick()
  //log.Printf("found key at Backup: [%s]", reply.Value)
  pb.mu.Lock()
  var backup = pb.amBackup
  pb.mu.Unlock()

  //pb.mu.Lock()
  if (backup){
    var val string
    var found bool

    pb.mu.Lock()
    val, found = pb.data[args.Key]
    // pb.mu.Unlock()

    if (found == false){
      reply.Err = ErrNoKey
      // log.Printf("args.Key: [%s] not found at backup server: [%s].", args.Key, pb.me)
    }else{
      reply.Err = OK
      reply.Value = val
      // fmt.Println("found key at Backup: [%s]", reply.Value)
    }
    pb.mu.Unlock()
  }else{
    reply.Err = ErrWrongServer
  }
//  pb.mu.Unlock()

//      fmt.Println("found key at Backup: [%s]", reply.Value)
  return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here
  var found bool
  pb.mu.Lock()
  _,found = pb.set[args.CommandId]
  pb.mu.Unlock()
  
  if (found == false){

    pb.tick()
  
    pb.mu.Lock()
    var primary = pb.amPrimary
    var hasbackup = pb.haveBackup
    pb.mu.Unlock()

    pb.mu.Lock()
//    pb.set[args.CommandId] = args.Key
    if (primary){
      if (hasbackup){
//      log.Printf("ForwardPut calling backup server: [%s]",pb.Backup)
      for ii := 0; ii < viewservice.DeadPings * 25; ii++ {
//        pb.mu.Lock()
        var backup = pb.Backup
//        pb.mu.Unlock()

        call(backup, "PBServer.ForwardPut", &args, &reply)
        if (reply.Err == OK){
          var founds bool
//          pb.mu.Lock()
          _,founds = pb.set[args.CommandId]
//          pb.mu.Unlock()
          if (founds == false){
//            pb.mu.Lock()
            pb.data[args.Key] = args.Value
            pb.set[args.CommandId] = args.Key
//            pb.mu.Unlock()
          }
          break
        }
        pb.mu.Unlock()
        time.Sleep(viewservice.PingInterval)
        pb.mu.Lock()
      }
    }else{
          var foundz bool
//          pb.mu.Lock()
          _,foundz = pb.set[args.CommandId]
//          pb.mu.Unlock()
          if (foundz == false){
//            pb.mu.Lock()
            pb.data[args.Key] = args.Value
            pb.set[args.CommandId] = args.Key
//            pb.mu.Unlock()
          }
          reply.Err = OK
    }
  }else{
    reply.Err = ErrWrongServer
  }
  pb.mu.Unlock()
  }else{
    reply.Err = OK
  }
  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here
    pb.tick()
//  fmt.Println("Get request at Primary: [%s]", pb.me)
    pb.mu.Lock()
    var primary = pb.amPrimary
    var hasbackup = pb.haveBackup
    pb.mu.Unlock()

  //pb.mu.Lock()  
  if (primary){
    if (hasbackup){
//      fmt.Println("Forwarded Get Request")
//      var check bool = true
      for kk := 0; kk < viewservice.DeadPings * 25; kk++ {
        pb.mu.Lock()
        var backup = pb.Backup
        pb.mu.Unlock()

        call(backup, "PBServer.ForwardGet", &args, &reply)
        if reply.Err==OK{
            break
          }
          // pb.tick()
          // fmt.Println("GET: ErrWrongServer recieved at Primary")          
        time.Sleep(viewservice.PingInterval)
      }
      if reply.Err != OK{
//        pb.mu.Unlock()
        return nil
      }
    }
    
    var val string
    var found bool
      
    pb.mu.Lock()
    val, found = pb.data[args.Key]
//    pb.mu.Unlock()
    
    reply.Err = OK  
    if (found == false){
      reply.Err = ErrNoKey
      // log.Printf("args.Key: [%s] not found at Primary server: [%s].", args.Key, pb.me)
    }else{
//      pb.mu.Lock()
      reply.Value = val
      pb.set[args.CommandId] = args.Key
//      pb.mu.Unlock()
    }
    pb.mu.Unlock()
  }else{
    reply.Err = ErrWrongServer
  }
//  pb.mu.Unlock()
  return nil
}

func (pb *PBServer) UpdateData(args *TransferArgs, reply *TransferReply) error{
  pb.mu.Lock()
  for k := range pb.data {
    delete(pb.data, k)
  }
  for k, v := range args.DataMap {
    pb.data[k] = v
  }
  // fmt.Println("Recieved map:", pb.data)
  for k := range pb.set {
    delete(pb.set, k)
  }
  for k, v := range args.SetMap {
    pb.set[k] = v
  }
  // fmt.Println("Recieved map:", pb.set)
  pb.mu.Unlock()

  // log.Printf("Updated MAP at Backup [%s] : [%t].", pb.me, pb.amBackup)
  return nil
}

func (pb *PBServer) sendData(source string, dest string){
  //transfer until done, or for certain time loop
  // fmt.Println("sending map:", pb.data)
  // log.Printf("Primary: [%s] sending data to Backup: [%s].", source, dest)
  var reply TransferReply
  pb.mu.Lock()
  args := &TransferArgs{DataMap: pb.data, SetMap: pb.set}
  pb.mu.Unlock()
  
  for {
    ok := call(dest, "PBServer.UpdateData", args, &reply)
    if ok{
      break
    }
  }
  pb.mu.Lock()
  pb.haveBackup = true
  pb.Backup = dest
  pb.mu.Unlock()
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
  // Your code here.
  vx,_ := pb.vs.Get()
  //log.Printf("Me [%s] tick: Viewnum: [%d], Primary [%s], Backup[%s]",pb.me, vx.Viewnum, vx.Primary, vx.Backup)

  pb.mu.Lock()
  var isbackup = pb.amBackup
  var hasbackup = pb.haveBackup
//  pb.mu.Unlock()

  if (vx.Primary == pb.me){
      if isbackup{ // if I have been promoted
          if (vx.Backup != ""){ // if I have a backup
            pb.mu.Unlock()
            pb.sendData(vx.Primary, vx.Backup)
            pb.mu.Lock()
          }else{
  //          pb.mu.Lock()
            pb.haveBackup = false
            pb.Backup = ""
  //          pb.mu.Unlock()
          }
      }else if (hasbackup == false && vx.Backup != ""){ // if backup was just added
          pb.mu.Unlock()
          pb.sendData(vx.Primary, vx.Backup)
          pb.mu.Lock()
      }else if (hasbackup == true && vx.Backup == ""){ // if backup was just removed
//          pb.mu.Lock()
          pb.haveBackup = false
          pb.Backup = ""
//          pb.mu.Unlock()
      }
//      pb.mu.Lock()
      pb.amPrimary, pb.amBackup = true, false
//      pb.mu.Unlock()

      pb.vs.Ping(vx.Viewnum)
  }else if (vx.Backup == pb.me){
//      pb.mu.Lock()
      pb.amPrimary, pb.amBackup, pb.haveBackup = false, true, false
//      pb.mu.Unlock()
      
      pb.vs.Ping(vx.Viewnum)
  }else{
    pb.vs.Ping(0)
  }
  pb.mu.Unlock()

  // log.Printf("PIng sent: [%s]", pb.vs.me)
}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.

  pb.data = make(map[string]string)
  pb.set = make(map[int64]string)
//  pb.Backup = ""
  pb.amPrimary, pb.amBackup, pb.haveBackup = false, false, false
  pb.vs.Ping(0)
  vx,_ := pb.vs.Get()
  if (vx.Primary == pb.me){
    pb.amPrimary = true
    if (vx.Backup != ""){
      pb.haveBackup = true  
    }
  }else if (vx.Backup == pb.me){
    pb.amBackup = true
  }


  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait() 
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
