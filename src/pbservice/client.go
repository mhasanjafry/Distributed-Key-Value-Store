package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

//import "log"

// You'll probably need to uncomment these:
// import "time"
import "crypto/rand"
import "math/big"
import "strconv"



type Clerk struct {
  vs *viewservice.Clerk
  // Your declarations here
}


func MakeClerk(vshost string, me string) *Clerk {
  ck := new(Clerk)
  ck.vs = viewservice.MakeClerk(me, vshost)
  // Your ck.* initializations here

  // fmt.Printf("MakeClerk called ...\n")

  return ck
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
 }

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

  // Your code here.
  var reply GetReply
    args := &GetArgs{Key: key, CommandId: nrand()}
  for{
    ok := call(ck.vs.Primary(), "PBServer.Get", args, &reply)
      if ok {
        if (reply.Err!=ErrWrongServer){
        break
        }
      }
  }
    if (reply.Err!=OK){
      return ""
    }
    // if (reply.Err==ErrWrongServer){
    //  return reply.Value + ErrWrongServer
    // }
    return reply.Value
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {

  // Your code here.
  //var v string
  var reply PutReply
  var tt string

  if (dohash){
    v := ck.Get(key)
//    log.Printf("Get returned: [%s].", v)
    tt = v
    var f = int(hash(v + value))
//    log.Printf("hash(v + key): [%d].", f)
    value = strconv.Itoa(f)
//    log.Printf("Adding to data map: [%s].", value)
//    log.Printf("Returning value for comparison: [%s]", v)
  }

  args := &PutArgs{Key: key, Value: value, DoHash: dohash, CommandId: nrand()}
 
  // send an RPC request, wait for the reply.
  for{
   ok := call(ck.vs.Primary(), "PBServer.Put", args, &reply)
    if ok{
      if (reply.Err!=ErrWrongServer){
        break
      }
  }
  }
//  log.Printf("Returning value for 234comparison: [%s]", v) 
  return tt
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
//  log.Printf("Returning value for PutHash: [%s]", v)
  return v
}
