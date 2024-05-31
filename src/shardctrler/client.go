package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.client_id = nrand()
	ck.next_seq_id = 0
	ck.last_leader = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.RPCId = UniqueId{ClientId: ck.client_id, RequestId: ck.next_seq_id}
	now_server:= ck.last_leader
	for {
		var reply QueryReply
		ok := ck.servers[now_server].Call("ShardCtrler.Query", args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			ck.last_leader = now_server
			ck.next_seq_id++
			return reply.Config
		}
		now_server = (now_server + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.RPCId = UniqueId{ClientId: ck.client_id, RequestId: ck.next_seq_id}
	now_server:= ck.last_leader
	
	for {
		var reply JoinReply
		ok := ck.servers[now_server].Call("ShardCtrler.Join", args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			ck.last_leader = now_server
			ck.next_seq_id++
			return
		}
		now_server = (now_server + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.RPCId = UniqueId{ClientId: ck.client_id, RequestId: ck.next_seq_id}
	now_server:= ck.last_leader
	for {
		// try each known server.
		var reply LeaveReply
		ok := ck.servers[now_server].Call("ShardCtrler.Leave", args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK{
			ck.last_leader = now_server
			ck.next_seq_id++
			return
		}
		now_server = (now_server + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.RPCId = UniqueId{ClientId: ck.client_id, RequestId: ck.next_seq_id}
	now_server:= ck.last_leader

	for {
		// try each known server.
		var reply MoveReply
		ok := ck.servers[now_server].Call("ShardCtrler.Move", args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			ck.last_leader = now_server
			ck.next_seq_id++
			return
		}
		now_server = (now_server + 1) % len(ck.servers)

		time.Sleep(100 * time.Millisecond)
	}
}
