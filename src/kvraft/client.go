package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id            int64
	lastRequestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	if 1 == 2 {
		log.Println()
	}
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	success := false
	var value string
	reqId := atomic.AddInt64(&ck.lastRequestId, 1)

	for {
		var waitGroup sync.WaitGroup
		waitGroup.Add(len(ck.servers))
		if success {
			break
		}
		args := GetArgs{
			Key:       key,
			ClientId:  int(ck.id),
			RequestId: int(reqId),
		}
		// log.Printf(" get req begin %+v \n", args)
		for i := range ck.servers {
			ck.makeGetCall(i, &success, &args, &waitGroup, &value)
		}
		// waitGroup.Wait()
		if success {
			break
		} else {
			time.Sleep(time.Millisecond * 10)
		}
	}

	// log.Printf("get request %d success 2 \n", reqId)
	return value
}

func (ck *Clerk) makeGetCall(server int, success *bool, args *GetArgs, wg *sync.WaitGroup, value *string) {
	defer wg.Done()
	reply := GetReply{}
	// log.Printf("client %d, send get request to server %d, args:%+v\n", ck.id, server, args)
	ok := ck.servers[server].Call("KVServer.Get", args, &reply)
	// log.Printf("client %d, send get request to server %d, args:%+v, receive %+v\n", ck.id, server, args, reply)
	if ok && reply.Err == OK {
		*success = true
		*value = reply.Value
		//	log.Printf("args %+v, resp %+v\n", args, reply)
	} else if ok && reply.Err == ErrNoKey {
		*success = true
		*value = ""
		//	log.Printf("args %+v, resp %+v\n", args, reply)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	success := false
	reqId := atomic.AddInt64(&ck.lastRequestId, 1)
	// log.Printf("put append request %d begin 2 \n", reqId)

	for {
		var waitGroup sync.WaitGroup
		waitGroup.Add(len(ck.servers))
		if success {
			break
		}
		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			ClientId:  int(ck.id),
			RequestId: int(reqId),
		}
		// log.Printf(" put append req begin %+v \n", args)
		for i := range ck.servers {
			ck.makePutAppendCall(i, &success, &args, &waitGroup)
		}
		// waitGroup.Wait()
		if success {
			break
		} else {
			time.Sleep(time.Millisecond * 10)
		}
	}
	// log.Printf("put append request %d begin 2 \n", reqId)
	// log.Printf("request %d success 2 \n", reqId)
}

func (ck *Clerk) makePutAppendCall(server int, success *bool, args *PutAppendArgs, wg *sync.WaitGroup) {
	// defer wg.Done()
	reply := PutAppendReply{}
	// log.Printf("client %d, send put append request to server %d, args:%+v\n", ck.id, server, args)
	ok := ck.servers[server].Call("KVServer.PutAppend", args, &reply)
	// log.Printf("client %d, send put append request to server %d, args:%+v, receive %+v\n", ck.id, server, args, reply)
	// log.Printf("reply %+v \n", reply)
	if ok && reply.Err == OK {
		*success = true
		// log.Printf("request %d success 1 \n", args.RequestId)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
