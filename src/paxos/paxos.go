package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on Values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"

type Request struct {
  Seq int
  Id int
  Val interface{}
  Op string
  MinDone int
  Identity int
}

type PrevAccepted struct{
  Id int
  Val interface{}
}

type PrevDecided struct{
  Decided bool
  Val interface{}
  Id int
}

type RequestReply struct {
  Agreed bool
  AlreadyAccepted PrevAccepted
  AlreadyDecided PrevDecided
}

type instance struct {
    maxId int
    maxIdAgreed int
    Val interface{}
    decided bool
    seq int
    waitingChanel chan interface{} 
    maxPropIdSeen int
}

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]

  // Your data here.
  highest_seq int
  setInstances map[int]instance
  doneseq int
  min_peer []int
  lastseen map[int]time.Time
}

const MaxInt = int((^uint(0)) >> 1)
// clients should send a Ping RPC this often,
// to tell the viewservice that the client is alive.
const PingInterval = time.Millisecond * 100

// the viewserver will declare a client dead if it misses
// this many Ping RPCs in a row.
const DeadPings = 5
// func nrand() int64 {
//   max := big.NewInt(int64(1) << 62)
//   bigx, _ := rand.Int(rand.Reader, max)
//   x := bigx.Int64()
//   return x
//  }

func random(min int) int {
    rand.Seed(time.Now().UnixNano())
    return rand.Intn(MaxInt - min) + min
}

func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
    
  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

func max(a, b int) int {
    if a > b {
        return a
    }
    return b
}

func (px *Paxos) Acceptor(args *Request, reply *RequestReply) error{
  var found bool
  var insta instance
  px.mu.Lock()
  defer px.mu.Unlock()
  px.lastseen[args.Seq] = time.Now()
  px.min_peer[args.Identity] = max(args.MinDone, px.min_peer[args.Identity])
  // px.mu.Unlock()
  min := px.Min()
  // px.mu.Lock()
  for k := range px.setInstances {
    if (k < min){
      delete(px.setInstances, k)
      delete(px.lastseen, k)
    }
  }
  insta,found = px.setInstances[args.Seq]
  // px.mu.Unlock()
  // log.Printf("Acceptor: [%d] received: Seq: [%d], Id: [%d], Op: [%s], found[%t]", px.me, args.Seq, args.Id, args.Op, found)
  if (found && insta.decided){
      reply.Agreed = false
      reply.AlreadyAccepted.Id = -1 //check 
      reply.AlreadyDecided.Val = insta.Val
      reply.AlreadyDecided.Decided = true
      // log.Printf("Acceptor: [%d] has Seq: [%d] already committed.", px.me, args.Seq)
      // px.mu.Unlock()
      return nil
  }
  reply.AlreadyDecided.Decided = false 
  px.highest_seq = max(px.highest_seq, args.Seq)

  if (args.Op == "propose"){
    if (found == false){//havent seen seq before
      // inst := new(instance)
      // px.mu.Lock()
      done := make(chan interface{}, 2)
      inst := instance{Val: args.Val, decided: false, seq: args.Seq, waitingChanel: done, maxPropIdSeen: args.Id}
      px.setInstances[args.Seq] = inst
//      px.mu.Unlock()
      reply.Agreed = true
      reply.AlreadyAccepted.Id = -1
      // px.mu.Unlock()
      // log.Printf("Acceptor: [%d] has recieved new seq [%d] maxPropIdSeen: [%d].", px.me, args.Seq, args.Id)
      return nil
    }else{
      if (args.Id > insta.maxPropIdSeen){         
          reply.Agreed = true
          reply.AlreadyAccepted.Id = insta.maxPropIdSeen
          reply.AlreadyAccepted.Val = insta.Val
          insta.maxPropIdSeen = args.Id
          // px.mu.Lock()
          px.setInstances[args.Seq] = insta
          // px.mu.Unlock()
          // log.Printf("Acceptor: [%d] has incremented maxPropIdSeen [%d] for seq: [%d].", px.me, insta.maxPropIdSeen, args.Seq)
      }else{
          reply.Agreed = false
          reply.AlreadyAccepted.Id = insta.maxPropIdSeen
          reply.AlreadyAccepted.Val = insta.Val
          // log.Printf("Acceptor: [%d] has rejected id [%d] maxPropIdSeen: [%d].", px.me, args.Id, insta.maxPropIdSeen)
      }
      return nil
    }
  }else if (args.Op == "accept"){
    if (found){//havent seen seq before
        if (args.Id >= insta.maxPropIdSeen){
              reply.Agreed = true
              reply.AlreadyAccepted.Id = insta.maxPropIdSeen
              reply.AlreadyAccepted.Val = insta.Val
              insta.maxPropIdSeen = args.Id
              insta.Val = args.Val
              // px.mu.Lock()
              px.setInstances[args.Seq] = insta
              // px.mu.Unlock()
          // log.Printf("Acceptor-P: [%d] has incremented maxPropIdSeen [%d] for seq: [%d].", px.me, insta.maxPropIdSeen, args.Seq)
              return nil
        }else{
          reply.AlreadyAccepted.Id = insta.maxPropIdSeen
          reply.AlreadyAccepted.Val = insta.Val
          // log.Printf("Acceptor-P: [%d] has rejected id [%d] maxPropIdSeen: [%d].", px.me, args.Id, insta.maxPropIdSeen)
        }
    }
    reply.Agreed = false
  }else if (args.Op == "commit"){
    if (found){//havent seen seq before
        px.mu.Unlock()
        insta.waitingChanel <- args.Val
        px.mu.Lock()
        // log.Printf("Acceptor: [%d] commits for seq [%d]", px.me, args.Seq)
        return nil
    }else{
      done := make(chan interface{}, 2)
      // px.mu.Lock()
      inst := instance{Val: args.Val, decided: false, seq: args.Seq, waitingChanel: done, maxPropIdSeen: args.Id}
      px.setInstances[args.Seq] = inst
      px.mu.Unlock()
      done <- args.Val
      px.mu.Lock()
      // log.Printf("Recieved commit for unseen sequence but STILL committed [%d]", args.Seq)      
    }
  }
  return nil
}

func (px *Paxos) Execute(insta instance, s string){
  px.mu.Lock()
  args := Request{Seq: insta.seq, Id: insta.maxId, Val: insta.Val, Op: "commit", MinDone: px.min_peer[px.me], Identity: px.me}
  defer px.mu.Unlock()
  for k := 0; k < len(px.peers); k++ {
    if (k==px.me){
      continue
    }
    go func(dest string, args Request, f int){
      var reply RequestReply
      var i = 0
      for{
        i++
        // log.Printf("Proposer [%d] sending commit to: [%d], seq: [%d]", px.me, f, insta.seq)
        ok := call(dest, "Paxos.Acceptor", &args, &reply) //send prepare(n) to all servers including self
        if ok || i>3{
          break
        }
      }
    }(px.peers[k], args, k)
  }
}

    func (px *Paxos) ProposeStart(insta instance) {
        done := insta.waitingChanel
        for { //while not decided:
            if (px.dead){
              return
            }
            px.mu.Lock()   
            var propChan, compChan = make(chan PrevAccepted, len(px.peers)), make(chan PrevDecided, len(px.peers))
            args := Request{Seq: insta.seq, Id: insta.maxId, Val: insta.Val, Op: "propose", MinDone: px.min_peer[px.me], Identity: px.me}
            // px.mu.Unlock()
            for k := 0; k < len(px.peers); k++ {
                go func(dest string, args Request, f int){
                    var reply RequestReply
                    var ok bool
                    if (f==px.me){
                      px.Acceptor(&args, &reply)
                      ok = true
                    }else{
                      var g = 0
                      for{
                        g++
                        ok = call(dest, "Paxos.Acceptor", &args, &reply) //send accept(n) to all servers including self
                        if ok || g>3{
                          break
                        }
                      }
                    }
                    // log.Printf("Proposer 2")
                    if ok && reply.Agreed==true{
                        // log.Printf("Proposer [%d] got acceptance for: [%d], seq: [%d]", px.me, f, insta.seq)
                        propChan <- reply.AlreadyAccepted
                    }
                    if ok && reply.AlreadyDecided.Decided{
                        compChan <- reply.AlreadyDecided
                    }else{
                        compChan <- PrevDecided{Id:reply.AlreadyAccepted.Id, Decided: false}
                    }
                }(px.peers[k], args, k)
            }
            var accepted int
            comp := 0
            var prev PrevAccepted
            var prevd PrevDecided
            bestprop := PrevAccepted{Id: -1}
            var propFail bool
            highest_id := insta.maxId
            px.mu.Unlock()
            for (accepted<(len(px.peers)/2 + 1)){
                select {
                    case prevd = <-compChan:
                      if prevd.Decided{
                          done <- prevd.Val
                          return    
                      }
                      highest_id = max(prevd.Id, highest_id)
                      comp++
                      if comp==len(px.peers){
                          propFail = true
                          break                  
                      }
                    case prev = <-propChan:
                      if (prev.Id > bestprop.Id){
                          bestprop = prev
                      }
                      accepted++
                      // log.Printf("Acceptor [%d] Acceptance count: [%d], seq: [%d]", px.me, accepted, insta.seq)                      
                }
            }
            if (propFail){
                // log.Printf("Proposer: [%d] didnt get proposed for Seq: [%d], Id: [%d].", px.me, insta.seq, insta.maxId)
                // Execute(insta, "abort")
                insta.maxId = (px.me+1)*(highest_id + 1)
                continue
            }
            if (bestprop.Id != -1){
                insta.Val = bestprop.Val
                // log.Printf("Proposer: [%d] got majority, but will try for [%s].", px.me, insta.Val)
            }else{
                // log.Printf("Proposer: [%d] got majority for Val [%s].", px.me, insta.Val)
            }
            var acceptChan, compChan2 = make(chan PrevAccepted, len(px.peers)), make(chan PrevDecided, len(px.peers))
            px.mu.Lock()
            args2 := Request{Seq: insta.seq, Id: insta.maxId, Val: insta.Val, Op: "accept", MinDone: px.min_peer[px.me], Identity: px.me}
            // px.mu.Unlock()            
            for k := 0; k < len(px.peers); k++ {
                go func(dest string, args2 Request, f int){
                    var reply2 RequestReply
                    var ok bool
                    if (f==px.me){
                      px.Acceptor(&args2, &reply2)
                      ok = true
                    }else{
                      var g = 0
                      for{
                        g++
                        ok = call(dest, "Paxos.Acceptor", &args2, &reply2) //send accept(n) to all servers including self
                        if ok || g>3{
                          break
                        }
                      }
                    }
                    // log.Printf("Acceptor replied: [%t].", reply2.Agreed)
                    if ok && reply2.Agreed{
                        acceptChan <- reply2.AlreadyAccepted
                    }
                    if ok && reply2.AlreadyDecided.Decided{
                        compChan2 <- reply2.AlreadyDecided
                    }else{
                        compChan2 <- PrevDecided{Decided: false}
                    }
                }(px.peers[k], args2, k)
            }
            var comp2, accepted2 int
            var prev2 PrevAccepted
            var prevd2 PrevDecided
            bestprop2 := PrevAccepted{Id: -1}
            var propFail2 bool
            highest_id = insta.maxId
            px.mu.Unlock() 
            for (accepted2<(len(px.peers)/2 + 1)){
                select {
                    case prevd2 = <-compChan2:
                      if (prevd2.Decided){
                          done <- prevd2.Val
                          return    
                      }                      
                      highest_id = max(prevd.Id, highest_id)
                      comp2++
                      if comp2==len(px.peers){
                          propFail2 = true
                          break                  
                      }
                    case prev2 = <-acceptChan:
                      if (prev2.Id > bestprop2.Id){
                          bestprop2 = prev
                      }
                      accepted2++
                      // log.Printf("Acceptor-P [%d] Acceptance count: [%d], seq: [%d]", px.me, accepted2, insta.seq)
                }
            }
            if (propFail2){
                // log.Printf("Acceptor-P: [%d] didnt get accepted for Seq: [%d], Id: [%d].", px.me, insta.seq, insta.maxId)
                insta.maxId = (px.me+1)*(highest_id + 1)
                continue
            }
            // log.Printf("Acceptor-P: [%d] got majority for Seq: [%d], Id: [%d].", px.me, insta.seq, insta.maxId)
            done <- insta.Val
            // log.Printf("Propose: [%d] commits for seq [%d].", px.me, insta.seq)
            px.Execute(insta, "commit")
            return
        }
      }

func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
  var found bool


  px.mu.Lock()
  _,found = px.setInstances[seq]
  if (found == false){
    px.highest_seq = max(px.highest_seq, seq)
    // px.mu.Unlock()
    // log.Printf("Sequence: [%d] arrived at [%d] for value [%s]", seq, px.me, v)
    done := make(chan interface{}, 2)
    inst := instance{Val: v, decided: false, maxId: px.me+1, seq: seq, waitingChanel: done, maxPropIdSeen: -1}
    // px.mu.Lock()
    px.lastseen[seq] = time.Now()
    px.setInstances[seq] = inst
    px.mu.Unlock()
    go px.ProposeStart(inst)
  }else{
    px.mu.Unlock()
  }
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  // log.Printf("Done Check")
  px.mu.Lock()
  if (seq <= px.highest_seq){
    px.min_peer[px.me] = seq+1
  }
  // px.mu.Unlock()
  min := px.Min()
  // px.mu.Lock()
  for k := range px.setInstances {
    // inst,_ = px.setInstances[k]
    // inst = instance{}
    if (k < min){
      delete(px.setInstances, k)
      delete(px.lastseen, k)
    }
  }
  px.mu.Unlock()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  return px.highest_seq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  // You code here.
  // px.mu.Lock()
  min := px.min_peer[0]
  for _, val := range px.min_peer {
    if val < min {
        min = val
    }
  }
  // px.mu.Unlock()
  return min
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed Value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  var found bool
  var inst instance
  px.mu.Lock()
  inst,found = px.setInstances[seq]
  defer px.mu.Unlock()
  if (found){
    if (inst.decided==false){
      select {
        case x, ok := <-inst.waitingChanel:
          if ok {
            inst.decided = true
            inst.Val = x
            // px.mu.Lock()
            px.setInstances[seq] = inst
            // px.mu.Unlock()
            // log.Printf("Sequence: [%d] decided at [%d] for value [%s]", seq, px.me, inst.Val)
            return inst.decided, inst.Val
          }
        default:
          if (time.Now().After(px.lastseen[seq].Add(PingInterval*DeadPings))){
            done := make(chan interface{}, 2)
            inst3 := instance{Val: inst.Val, decided: false, maxId: (px.me+1), seq: inst.seq, waitingChanel: done, maxPropIdSeen: -1}
            delete(px.setInstances, seq)
            px.lastseen[seq] = time.Now()
            px.setInstances[seq] = inst3
            go px.ProposeStart(inst3)
          }
      }
    }else{
      return inst.decided, inst.Val
    }
  }
  return false, nil
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me
  px.setInstances = make(map[int]instance)

  // Your initialization code here.
  px.highest_seq = -1
  px.doneseq = 0
  px.min_peer = make([]int, len(px.peers))
  px.lastseen = make(map[int]time.Time)
  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
