package shardctrler

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const ApplyTimeOut = 500 * time.Millisecond
const INVALID_GID = 0
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 

	maxraftstate int // snapshot if log grows this big
	// Your data here.
	lastOpReuslt map[int64]OpResult
	chan_map map[int]*chan OpResult
	last_executed_index int

	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
	Type string
	Args interface{}
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
	Config  Config // only used for Query
	RPCId UniqueId
}

func (sc *ShardCtrler) IsLastOp(uid UniqueId) OPTIMESTATE{
	if last_result, ok := sc.lastOpReuslt[uid.ClientId]; ok {
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

func CopyConfig(src_config Config, dst_config *Config){
	dst_config.Num = src_config.Num
	dst_config.Shards = src_config.Shards
	dst_config.Groups = make(map[int][]string)
	for key, value := range src_config.Groups{
		dst_config.Groups[key] = value
	}
}

func (sc *ShardCtrler)Rebalance (op_type string, changed_gid_list []int) [NShards]int{
	now_config := sc.configs[len(sc.configs) - 1]
	new_shards2_gid := [NShards]int{}

	// initialize
	gid2shards := make(map[int][]int)
	gid2shards[INVALID_GID] = []int{}
	// old gid2shards
	for i := 0; i < NShards; i++{
		gid := now_config.Shards[i]
		if gidlist, ok := gid2shards[gid]; ok{
			gid2shards[gid] = append(gidlist, i)
		} else{
			gid2shards[gid] = []int{i}
		}
	}
	// remove or add gid
	if op_type == "Join"{
		for i := 0; i < len(changed_gid_list); i++{
			gid2shards[changed_gid_list[i]] = []int{}
		}
	} else if op_type == "Leave"{
		for i := 0; i < len(changed_gid_list); i++{
			gid2shards[INVALID_GID] = append(gid2shards[INVALID_GID], gid2shards[changed_gid_list[i]]...)
			delete(gid2shards, changed_gid_list[i])
		}
	}

	// rebalance: find max and min. First, move shard from gid 0 to min_gid; then move shard from max_gid to min_gid. until max_gid - min_gid <= 1
	// 比较大小：优先比较shard数量，shard数量相同的情况下，比较gid，gid大就大
	for {
		min_gid := -1
		max_gid := -1
		min_gid_shrads := NShards + 1
		max_gid_shards := -1
		for gid := range gid2shards{
			if gid == INVALID_GID{
				continue
			}
			if len(gid2shards[gid]) < min_gid_shrads || (len(gid2shards[gid]) == min_gid_shrads && gid < min_gid){
				min_gid_shrads = len(gid2shards[gid])
				min_gid = gid
			}
			if len(gid2shards[gid]) > max_gid_shards || (len(gid2shards[gid]) == max_gid_shards && gid > max_gid){
				max_gid_shards = len(gid2shards[gid])
				max_gid = gid
			}
		}
		if min_gid == -1 || max_gid == -1{
			fmt.Printf("Rebalance error\n")
		}
		// todo: should we check there are no valid gid? but it means the system is broken...
		if len(gid2shards[INVALID_GID]) > 0{
			shard_to_move := gid2shards[INVALID_GID][0]
			gid2shards[min_gid] = append(gid2shards[min_gid], shard_to_move)
			gid2shards[INVALID_GID] = gid2shards[INVALID_GID][1:]
		} else if max_gid - min_gid > 1{
			shard_to_move := gid2shards[max_gid][0]
			gid2shards[min_gid] = append(gid2shards[min_gid], shard_to_move)
			gid2shards[max_gid] = gid2shards[max_gid][1:]
		} else {
			break
		}
	}
	// update config
	for gid := range gid2shards{
		for i := 0; i < len(gid2shards[gid]); i++{
			new_shards2_gid[gid2shards[gid][i]] = gid
		}
	}
	return new_shards2_gid

}

func (sc *ShardCtrler) ExecuteOp(op Op, result *OpResult){
	switch op.Type{
	case "Join":
		args := op.Args.(JoinArgs)
		add_list := []int{}
		for gid := range args.Servers{
			add_list = append(add_list, gid)
		}
		new_shards2gid := sc.Rebalance("Join", add_list)
		new_config := Config{}
		CopyConfig(sc.configs[len(sc.configs) - 1], &new_config)
		new_config.Num = len(sc.configs)
		new_config.Shards = new_shards2gid
		for gid := range args.Servers{
			new_config.Groups[gid] = args.Servers[gid]
		}
		sc.configs = append(sc.configs, new_config)
		result.Err = OK
	case "Leave":
		args := op.Args.(LeaveArgs)
		new_shards2gid := sc.Rebalance("Leave", args.GIDs)
		new_config := Config{}
		CopyConfig(sc.configs[len(sc.configs) - 1], &new_config)
		new_config.Num = len(sc.configs)
		new_config.Shards = new_shards2gid
		for i := 0; i < len(args.GIDs); i++{
			delete(new_config.Groups, args.GIDs[i])
		}
		result.Err = OK
	case "Move":
		args := op.Args.(MoveArgs)
		old_config := sc.configs[len(sc.configs) - 1]
		new_config := Config{}
		CopyConfig(old_config, &new_config)
		new_config.Num = len(sc.configs)
		new_config.Shards[args.Shard] = args.GID
		result.Err = OK
	case "Query":
		args := op.Args.(QueryArgs)
		if args.Num == -1 || args.Num >= len(sc.configs){
			CopyConfig(sc.configs[len(sc.configs) - 1], &result.Config)
		} else if args.Num >= 0 && args.Num < len(sc.configs){
			CopyConfig(sc.configs[args.Num], &result.Config)
		} else {
			fmt.Printf("Query config index %d out of bound\n", args.Num)
		}
		result.Err = OK
	}
}

func (sc *ShardCtrler) ApplyChReader() {
	for m := range sc.applyCh {
		if sc.killed() {
            break
        }
		var op_result OpResult
		sc.mu.Lock()
		if m.CommandValid{
			if m.CommandIndex <= sc.last_executed_index{
				sc.mu.Unlock()
				continue
			}
			sc.last_executed_index = m.CommandIndex
			if chan_op, ok := m.Command.(Op); ok{
				is_last_op := sc.IsLastOp(chan_op.RPCId)
				if is_last_op == STALEOP{
					// do nothing
				} else if is_last_op == LASTOP{
					op_result = sc.lastOpReuslt[chan_op.RPCId.ClientId]
				} else {
					// new op
					op_result.RPCId = chan_op.RPCId
					op_result.Err = OK
					sc.ExecuteOp(chan_op, &op_result)
					sc.lastOpReuslt[chan_op.RPCId.ClientId] = op_result
				}
				if ch_ptr, ok := sc.chan_map[m.CommandIndex]; ok{
					*ch_ptr <- op_result
				}
				if sc.maxraftstate != -1 && float64(sc.rf.GetPersistSize()) > 0.8 * float64(sc.maxraftstate){
					snapshot_data := sc.SCMakePersistfunc()
					sc.rf.Snapshot(m.CommandIndex, snapshot_data)
				}
			}
		} else if m.SnapshotValid{
			if sc.last_executed_index >= m.SnapshotIndex{
				sc.mu.Unlock()
				continue
			}
			// don't need to update last_executed_index here, because raft doesn't apply a log with smaller index
			snapshot_data := m.Snapshot
			sc.SCReadPersist(snapshot_data)
			sc.last_executed_index = m.SnapshotIndex
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) HandleOp(op Op) OpResult{
	var op_result OpResult
	_, is_leader  := sc.rf.GetState()

	if !is_leader{
		op_result.Err = ErrWrongLeader
		return op_result
	}

	sc.mu.Lock()
	is_last_op := sc.IsLastOp(op.RPCId)

	if is_last_op == STALEOP{
		// staleop 说明client其实已经往下走了，随便回复都可以
		sc.mu.Unlock()
		return op_result
	} else if is_last_op == LASTOP{
		op_result = sc.lastOpReuslt[op.RPCId.ClientId]	
		sc.mu.Unlock()
		return op_result
	}
	sc.mu.Unlock()

	// new op
	idx, start_term, is_leader := sc.rf.Start(op)
	if !is_leader{
		op_result.Err = ErrWrongLeader
		return op_result
	}
	// should make a buffered chan to avoid deadlock
	mychan := make(chan OpResult, 1)
	sc.mu.Lock()
	sc.chan_map[idx] = &mychan
	sc.mu.Unlock()
	select {
		case msg := <-mychan:
			now_term, is_leader := sc.rf.GetState()
			if !is_leader || start_term != now_term{
				op_result.Err = ErrWrongLeader
			} else{
				op_result = msg
			}
		case <-time.After(ApplyTimeOut):
			// op_result.Err = ErrTimeOut
			op_result.Err = ErrWrongLeader
		// default:
	}
	sc.mu.Lock()
	if chan_ptr, ok := sc.chan_map[idx]; ok{
		if chan_ptr == &mychan{
			delete(sc.chan_map, idx)
		}
	}
	close(mychan)
	sc.mu.Unlock()
	return op_result
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{Type: "Join", Args: *args, RPCId: args.RPCId}
	op_result := sc.HandleOp(op)
	reply.Err = op_result.Err
	reply.WrongLeader = reply.Err == ErrWrongLeader
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{Type: "Leave", Args: *args, RPCId: args.RPCId}
	op_result := sc.HandleOp(op)
	reply.Err = op_result.Err
	reply.WrongLeader = reply.Err == ErrWrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{Type: "Move", Args: *args, RPCId: args.RPCId}
	op_result := sc.HandleOp(op)
	reply.Err = op_result.Err
	reply.WrongLeader = reply.Err == ErrWrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{Type: "Query", Args: *args, RPCId: args.RPCId}
	op_result := sc.HandleOp(op)
	reply.Err = op_result.Err
	reply.Config = op_result.Config
	reply.WrongLeader = reply.Err == ErrWrongLeader
}

func (sc *ShardCtrler) SCMakePersistfunc () []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.configs)
	e.Encode(sc.lastOpReuslt)
	e.Encode(sc.last_executed_index)
	scsnapshot := w.Bytes()
	return scsnapshot
}


func (sc *ShardCtrler) SCReadPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// // Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvm []Config
	var lor map[int64]OpResult
	var lei int
	if d.Decode(&kvm) != nil || d.Decode(&lor) != nil || d.Decode(&lei) != nil{
		fmt.Printf("Decode error\n")
		return
	} else {
		sc.configs = kvm
		sc.lastOpReuslt = lor
		sc.last_executed_index = lei
	}
}


// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.configs[0].Shards = [NShards]int{}
	for i := 0; i < NShards; i++{
		sc.configs[0].Shards[i] = 0
	}
	sc.dead = 0
	sc.maxraftstate = -1
	sc.lastOpReuslt = make(map[int64]OpResult)
	sc.chan_map = make(map[int]*chan OpResult)
	sc.last_executed_index = -1

	go sc.ApplyChReader()

	return sc
}
