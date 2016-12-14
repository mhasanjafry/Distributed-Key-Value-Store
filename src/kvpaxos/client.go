package kvpaxos

import "net/rpc"
import "fmt"
// import "log"
// You'll probably need to uncomment these:
// import "time"
import "crypto/rand"
import "math/big"

// import "strconv"
import mrand "math/rand"
import "time"
// import "math"

type Clerk struct {
  servers []string
  // You will have to modify this struct.
  Name string
  Command int
  ClientName int64
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}

// func (ck *Clerk) randomx() int {
//     mrand.Seed(time.Now().UnixNano())
//     return mrand.Intn(50)
// }

func MakeClerk(servers []string) *Clerk {
  ck := new(Clerk)
  ck.servers = servers
  // You'll have to add code here.
  ck.Command = 0
  ck.ClientName = nrand()
  // ck.Name = strconv.Itoa(nrand())

  return ck
}

func (ck *Clerk) random() int {
    mrand.Seed(time.Now().UnixNano())
    return mrand.Intn(len(ck.servers))
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
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  // You will have to modify this function.
    ck.Command++
    //server_num := int(math.Mod(float64(ck.Command), float64(len(ck.servers))))//ck.random()
    server_num := ck.random()

    var reply GetReply
    for{
      // log.Printf("Client [%d] calling server_num [%d] for get for args.Key [%s]",ck.ClientName, server_num, key)
      args := &GetArgs{Key: key, CommandId: nrand(), ClientId: ck.ClientName, CommandNum: ck.Command}
      ok := call(ck.servers[server_num], "KVPaxos.Get", args, &reply)
      if (reply.Err==SeqFilled){
        // log.Printf("Client [%d] recieved ErrorRetry for get for Key [%s]",ck.ClientName, key)
      }
      if ok && reply.Err==OK{
          break
      }
      if reply.Err==ErrNoKey{
        // log.Printf("Client [%d] request answered: ErrNoKey",ck.ClientName)
        return ""
      }
      // if reply.Err==RetryCommand{
      //     ck.Command++
      // }
      reply.Err=""
      //server_num = int(math.Mod(float64(server_num+1), float64(len(ck.servers))))      
      server_num = ck.random()
    }
    // log.Printf("Client [%d] request answered for put for Key [%s] for value [%s]",ck.ClientName, key, reply.Value)
    return reply.Value
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  // You will have to modify this function.
  server_num := ck.random()
  var reply PutReply
  ck.Command++
  //server_num := int(math.Mod(float64(ck.Command), float64(len(ck.servers))))
  // send an RPC request, wait for the reply.
  for{
    if dohash{
      // log.Printf("Client [%d] server_num [%d] for PutHash for Key [%s] for value [%s]",ck.ClientName,server_num, key, value)
    }else{
      // log.Printf("Client [%d] server_num [%d] for Put for Key [%s] for value [%s]",ck.ClientName, server_num,key, value)      
    }
    args := &PutArgs{Key: key, Value: value, DoHash: dohash, CommandId: nrand(),ClientId: ck.ClientName, CommandNum: ck.Command}
    ok := call(ck.servers[server_num], "KVPaxos.Put", args, &reply)
    // if (reply.Err==SeqFilled){
    //   log.Printf("Client [%d] recieved ErrorRetry from server [%d] for puthash for Key [%s] for value [%s]",ck.ClientName, server_num, key, value)
    // }
    if ok && reply.Err==OK{
      break
    }
      // if reply.Err==RetryCommand{
      //     ck.Command++
      // }
    reply.Err=""
    //server_num = int(math.Mod(float64(server_num+1), float64(len(ck.servers))))      
  server_num = ck.random()
  }
  if dohash{
    // log.Printf("Client [%d] request answered for PutHash for Key [%s] for prevvalue [%s]",ck.ClientName, key, reply.PreviousValue)
    return reply.PreviousValue
  }else{
    // log.Printf("Client [%d] request answered for put for Key [%s] for value [%s]",ck.ClientName, key, value)
    return ""
  }
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
