package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	client_id int64
	next_seq_id int
	last_leader int
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
	ck.client_id = nrand()
	ck.next_seq_id = 0
	ck.last_leader = 0
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
	// You will have to modify this function.
	success_rpc := false
	args := GetArgs{Key: key, RPCId: UniqueId{ClientId: ck.client_id, RequestId: ck.next_seq_id}}
	now_server:= ck.last_leader
	for (!success_rpc) {
		reply := GetReply{}
		now_server = now_server % len(ck.servers);
		ok := ck.servers[now_server].Call("KVServer.Get", &args, &reply)
		if (ok) {
			if (reply.Err == OK || reply.Err == ErrNoKey) {
				// todo: we guarantee when errnokey, the value is ""
				success_rpc = true
				ck.last_leader = now_server
				ck.next_seq_id++
				return reply.Value
			} else if (reply.Err == ErrWrongLeader || reply.Err == ErrWrongTerm || reply.Err == ErrTimeOut) {
				now_server++
				continue
			}
		} else {
			// rpc failed
			now_server++
			continue
		}
	}
	return ""
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
	success_rpc := false
	args := PutAppendArgs{Key: key, Value: value, Op: op, RPCId: UniqueId{ClientId: ck.client_id, RequestId: ck.next_seq_id}}
	now_server:= ck.last_leader
	for (!success_rpc) {
		reply := PutAppendReply{}
		now_server = now_server % len(ck.servers);
		ok := ck.servers[now_server].Call("KVServer.PutAppend", &args, &reply)
		if (ok) {
			if (reply.Err == OK || reply.Err == ErrNoKey) {
				success_rpc = true
				ck.last_leader = now_server
				ck.next_seq_id++
				return
			} else if (reply.Err == ErrWrongLeader || reply.Err == ErrWrongTerm || reply.Err == ErrTimeOut) {
				now_server++
				continue
			}
		} else {
			// rpc failed
			now_server++
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
