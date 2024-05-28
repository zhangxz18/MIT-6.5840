package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const ApplyTimeOut = 1000 * time.Millisecond
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType int
const (
	GET OpType = iota
	PUT
	APPEND
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type OpType
	Key string
	Value string
	RPCId UniqueId
}

type OPTIMESTATE int
const (
	STALEOP OPTIMESTATE = iota
	LASTOP
	NEWOP
)

type OpResult struct {
	Err Err
	Value string
	RPCId UniqueId
	Term int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kv_map map[string]string
	lastOpReuslt map[int64]OpResult
	chan_map map[int]chan OpResult // chan between rpc handler and applyCh reader
}

func (kv *KVServer) IsLastOp(uid UniqueId) OPTIMESTATE{
	if last_result, ok := kv.lastOpReuslt[uid.ClientId]; ok {
		if last_result.RPCId.RequestId < uid.RequestId{
			return NEWOP
		} else if last_result.RPCId.RequestId == uid.RequestId{
			return LASTOP
		} else {
			return STALEOP
		}
	}
	return NEWOP
}

func (kv *KVServer) ApplyChReader() {
	for m := range kv.applyCh {
		if kv.killed() {
            break
        }
		op_result := OpResult{Err: OK, Value: "", RPCId: UniqueId{0, 0}}
		kv.mu.Lock()
		if m.CommandValid{
			if chan_op, ok := m.Command.(Op); ok{
				is_last_op := kv.IsLastOp(chan_op.RPCId)
				if is_last_op == STALEOP{
					// do nothing
				} else if is_last_op == LASTOP{
					op_result = kv.lastOpReuslt[chan_op.RPCId.ClientId]
				} else {
					// new op
					op_result.RPCId = chan_op.RPCId
					op_result.Err = OK
					if chan_op.Type == GET{
						value, key_exist := kv.kv_map[chan_op.Key]
						if key_exist{			
							op_result.Value = value
						} else {
							op_result.Value = ""
						}
					} else if chan_op.Type == PUT{
						kv.kv_map[chan_op.Key] = chan_op.Value
					} else if chan_op.Type == APPEND{
						value, key_exist := kv.kv_map[chan_op.Key]
						if key_exist{
							kv.kv_map[chan_op.Key] = value + chan_op.Value
						} else{
							kv.kv_map[chan_op.Key] = chan_op.Value
						}
					}
					kv.lastOpReuslt[chan_op.RPCId.ClientId] = op_result
				}
				if ch, ok := kv.chan_map[m.CommandIndex]; ok{
					ch <- op_result
				}
			}
		}
		kv.mu.Unlock()
	}
	

}

func (kv *KVServer) HandleOp(op Op) OpResult{
	op_result := OpResult{Err: OK, Value: "", RPCId: op.RPCId}
	kv.mu.Lock()
	is_last_op := kv.IsLastOp(op.RPCId)
	if is_last_op == STALEOP{
		// staleop 说明client其实已经往下走了，随便回复都可以
		kv.mu.Unlock()
		return op_result
	} else if is_last_op == LASTOP{
		op_result = kv.lastOpReuslt[op.RPCId.ClientId]
		kv.mu.Unlock()
		return op_result
	}
	// new op
	kv.mu.Unlock()
	
	
	// _, rf_is_leader := kv.rf.GetState()
	// if	!rf_is_leader{
	// 	op_result.Err = ErrWrongLeader
	// 	return op_result
	// }
	
	idx, start_term, is_leader := kv.rf.Start(op)
	if !is_leader{
		op_result.Err = ErrWrongLeader
		return op_result
	}
	// will there be a exsiting chan?
	kv.chan_map[idx] = make(chan OpResult)
	select {
		case msg := <-kv.chan_map[idx]:
			now_term, is_leader := kv.rf.GetState()
			if !is_leader || start_term != now_term{
				// todo:should we check the term?
				// 实际上，只需要check start_term和apply的term是否一致即可（保证是同一个op），但是原来的代码不太好实现。。。
				// 不过这里逻辑也是对的，只有在整个处理期间leader和term没改变，才能认为这是一个正常的操作，虽然可能影响效率
				op_result.Err = ErrWrongLeader
			} else{
				op_result = msg
			}
		case <-time.After(1000 * time.Millisecond):
			op_result.Err = ErrTimeOut
		// default:
	}
	kv.mu.Lock()
	close(kv.chan_map[idx])
	delete(kv.chan_map, idx)
	kv.mu.Unlock()
	return op_result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	operations := Op{Type: GET, Key: args.Key, Value: "", RPCId: args.RPCId}
	op_result := kv.HandleOp(operations)
	reply.Err = op_result.Err
	reply.RPCId = op_result.RPCId
	reply.Value = op_result.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	operations := Op{Type: PUT, Key: args.Key, Value: args.Value, RPCId: args.RPCId}
	if args.Op == "Append"{
		operations.Type = APPEND
	}
	op_result := kv.HandleOp(operations)
	reply.Err = op_result.Err
	reply.RPCId = op_result.RPCId
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kv_map = make(map[string]string)
	kv.lastOpReuslt = make(map[int64]OpResult)
	kv.chan_map = make(map[int]chan OpResult)
	go kv.ApplyChReader()

	return kv
}
