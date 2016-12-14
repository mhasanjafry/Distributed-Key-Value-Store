
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string //name of server; can literally be anything
  start bool

  // Your declarations here.
  lastseen map[string]time.Time// map[KeyType]ValueType -> map from server names to time.Time
  UnappliedView View
  currView View
  unacked bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  // Your code here.
//  log.Printf("Ping: Server: [%s], Viewnum: [%d], currView: [%d], Primary [%s], Backup[%s], unacked[%t]",args.Me, args.Viewnum, vs.currView.Viewnum, vs.currView.Primary, vs.currView.Backup, vs.unacked)
  vs.mu.Lock()
  reply.View = vs.currView
  vs.mu.Unlock()
  
  if args.Viewnum == 0 { // new or restarted  
    // log.Printf("Hello Ping: NEW server joined/ restarted. currView ID [%d].",vs.currView.Viewnum)
    vs.mu.Lock()
    if (vs.currView.Primary == args.Me && vs.currView.Backup != "") {
      if (vs.unacked){
        vs.UnappliedView = View{Primary: vs.currView.Backup, Backup: "", Viewnum: vs.currView.Viewnum+1}
      }else{
        vs.currView = View{Primary: vs.currView.Backup, Backup: "", Viewnum: vs.currView.Viewnum+1}
        vs.UnappliedView = vs.currView
        vs.unacked = true;
      }

    }
    if (vs.currView.Primary == "" && vs.start==false) {
      vs.currView.Primary = args.Me
      vs.currView.Viewnum++
      vs.UnappliedView = View{Primary: vs.currView.Primary, Backup: "", Viewnum: vs.currView.Viewnum}
      vs.start = true;
    } else if (vs.currView.Backup == "") {
      if (vs.unacked){
        vs.UnappliedView = View{Primary: vs.currView.Primary, Backup: args.Me, Viewnum:vs.currView.Viewnum+1}
      }else{
        vs.currView = View{Primary: vs.currView.Primary, Backup: args.Me, Viewnum:vs.currView.Viewnum+1}
        vs.UnappliedView = vs.currView
        vs.unacked = true;
      }
    }
    vs.mu.Unlock()
  }else{
      vs.mu.Lock()
      if (vs.unacked && args.Me == vs.currView.Primary && args.Viewnum == vs.currView.Viewnum) {
//          reply.View = View{Primary: vs.UnappliedView.Primary, Backup: vs.UnappliedView.Backup, Viewnum: vs.UnappliedView.Viewnum}
//          log.Printf("Hello Ping: Primary ACKED: currView ID [%d].",vs.currView.Viewnum)
          vs.currView = vs.UnappliedView
          vs.unacked = false;

          if (vs.currView.Primary != args.Me && vs.currView.Backup != args.Me){
            if (vs.currView.Primary==""){
              vs.currView = View{Primary: args.Me, Backup: "", Viewnum:vs.currView.Viewnum+1}
              vs.UnappliedView = vs.currView
              vs.unacked = true;
            }else if (vs.currView.Backup==""){
              vs.currView = View{Primary: vs.currView.Primary, Backup: args.Me, Viewnum:vs.currView.Viewnum+1}
              vs.UnappliedView = vs.currView
              vs.unacked = true;
            }
          }

      }
      vs.mu.Unlock()
  }
  vs.mu.Lock()
  reply.View = vs.currView
  vs.lastseen[args.Me] = time.Now()
  vs.mu.Unlock()
  // log.Printf("Replied to Server: Primary[%s], Backup[%s], Viewnum: [%d]",reply.View.Primary, reply.View.Backup,reply.View.Viewnum)
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // log.Printf("Hello Get: Primary[%s], Backup[%s]",vs.currView.Primary, vs.currView.Backup)

  // Your code here.
  // Get(): fetch the current view, without volunteering
// to be a server. mostly for clients of the p/b service,
// and for testing.
  // if vs.currView != nil{
  vs.mu.Lock()
  reply.View = vs.currView
  vs.mu.Unlock()
  // }

  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  //log.Printf("Hello tick")

  // Your code here.
  vs.mu.Lock()
  for k, v := range vs.lastseen {  // copied from http://stackoverflow.com/questions/1841443/iterating-over-all-the-keys-of-a-golang-map
//    if v-time.Now() > DeadPings*PingInterval {
      //log.Printf("tick: server[%s] time[%d]\n", k, v.Second())
//*1000*1000*DeadPings
      if (time.Now().After(v.Add(PingInterval*DeadPings))){
//        log.Printf("Server [%s] Dead isPrimary[%t], isBackup[%t]",k, k==vs.currView.Primary, k==vs.currView.Backup)
        if k==vs.currView.Primary {
          if (vs.currView.Backup != ""){
            if (vs.unacked){ //postpone change until acked
              //vs.currView = View{Primary: "", Backup: vs.currView.Backup, Viewnum:vs.currView.Viewnum}
              vs.UnappliedView = View{Primary: vs.currView.Backup, Backup: "", Viewnum:vs.currView.Viewnum+1}
            }else{ //commit change if acked uptill last change
              vs.currView = View{Primary: vs.currView.Backup, Backup: "", Viewnum:vs.currView.Viewnum+1}
              vs.UnappliedView = vs.currView
              vs.unacked = true;
            }
          }else{
            if (vs.unacked){ //postpone change until acked
              vs.UnappliedView = View{Primary: "", Backup: "", Viewnum:vs.currView.Viewnum+1}
            }else{ //commit change if acked uptill last change
              vs.currView = View{Primary: "", Backup: "", Viewnum:vs.currView.Viewnum+1}
              vs.UnappliedView = vs.currView
              vs.unacked = true;
            }
          }  
        }else if k==vs.currView.Backup {
          if (vs.unacked){ //postpone change until acked
            vs.UnappliedView = View{Primary: vs.currView.Primary, Backup: "", Viewnum:vs.currView.Viewnum+1}
          }else{ //commit change if acked uptill last change
            vs.currView = View{Primary: vs.currView.Primary, Backup: "", Viewnum:vs.currView.Viewnum+1}
            vs.UnappliedView = vs.currView
            vs.unacked = true;
          }
        }
        delete(vs.lastseen, k)
      }
    //fmt.Printf("key[%s] value[%s]\n", k, v)
  }
  vs.mu.Unlock()

}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // log.Printf("Hello StartServer")

  // Your vs.* initializations here.
  vs.lastseen = make(map[string]time.Time)
  vs.UnappliedView = View{}
  vs.start = false
  //vs.lastseen = last
  vs.currView = View{}

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
