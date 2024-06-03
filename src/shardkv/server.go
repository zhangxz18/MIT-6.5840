package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)


const ApplyTimeOut = 500 * time.Millisecond
const GetConfInterval = 90 * time.Millisecond // should be less than 100 ms
const RPCRetryInterval = 100 * time.Millisecond
const EmptyOpWrieInterval = 100 * time.Millisecond
const INVALIDGID = 0
type ShardState int
const (
	OWNED ShardState = iota
	NOTOWNED
	TRANSFERING_OUT
	TRANSFERING_IN
)

const (
	GET = "Get"
	PUT = "Put"
	APPEND = "Append"
	UPDATECONFIG = "UpdateConfig"
	ADDSHARD = "AddShard"
	RemoveSHARD = "RemoveShard"
)

const Debug = false
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string
	Key string
	Value string
	RPCId UniqueId
	NewConfig shardctrler.Config // for update config
	RemoveShardList []int // for removeshard
	NewShardData []ShardData // for addshard
}

type OPTIMESTATE int
const (
	STALEOP OPTIMESTATE = iota
	LASTOP
	NEWOP
)

type CONFIGTIMESTATE int
const (
	CONFIGFINISHED CONFIGTIMESTATE = iota
	CONFIGRIGHT
	CONFIGTOONEW
)

type OpResult struct {
	Err Err
	Value string
	RPCId UniqueId
}

type ShardData struct {
	ShardIndex int
	ShardState ShardState
	KVMap map[string]string
	LastOpReuslt map[int64]OpResult
}

type AddShardArgs struct {
	ShardDataToAdd []ShardData
	RPCId UniqueId
}

type AddShardReply struct {
	Err Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead    int32
	chan_map map[int]*chan OpResult // chan between rpc handler and applyCh reader
	mck *shardctrler.Clerk
	// snapshot
	prev_config shardctrler.Config
	cur_config shardctrler.Config
	shard_data []ShardData 
	last_executed_index int 
	// next_seq_id int // last sequence id for now state, need snapshot
	// is_transfering bool
}

func (kv *ShardKV) IsShardInGroup(shard int) bool{
	return kv.shard_data[shard].ShardState == OWNED
}

func (kv *ShardKV) IsLastOp(uid UniqueId, key string) OPTIMESTATE{
	keys_shard := key2shard(key) 
	if last_result, ok := kv.shard_data[keys_shard].LastOpReuslt[uid.ClientId]; ok {
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

// func (kv *ShardKV) IsRightConfigOp()

func CloneShardData(src_data *ShardData, dst_data *ShardData){
	dst_data.ShardIndex = src_data.ShardIndex
	dst_data.ShardState = src_data.ShardState
	dst_data.KVMap = make(map[string]string)
	dst_data.LastOpReuslt = make(map[int64]OpResult)
	for key, value := range src_data.KVMap{
		dst_data.KVMap[key] = value
	}
	for key, value := range src_data.LastOpReuslt{
		dst_data.LastOpReuslt[key] = value
	}
}

func (kv *ShardKV) ExecuteOp(op Op, result *OpResult){
	shard := key2shard(op.Key)
	switch op.Type{
	case "Get":
		value, key_exist := kv.shard_data[shard].KVMap[op.Key]
		if key_exist{			
			result.Value = value
		} else {
			result.Value = ""
		}
	case "Put":
		kv.shard_data[shard].KVMap[op.Key] = op.Value
	case "Append":
		value, key_exist := kv.shard_data[shard].KVMap[op.Key]
		if key_exist{
			kv.shard_data[shard].KVMap[op.Key] = value + op.Value
		} else{
			kv.shard_data[shard].KVMap[op.Key] = op.Value
		}

	// default:
	// 	fmt.Printf("Unknown op type %s\n", op.Type)
	}
}

func CloneConfig(src_config *shardctrler.Config, dst_config *shardctrler.Config){
	dst_config.Num = src_config.Num
	dst_config.Shards = src_config.Shards
	dst_config.Groups = make(map[int][]string)
	for key, value := range src_config.Groups{
		dst_config.Groups[key] = value
	}
}

func (kv *ShardKV) ExecuteUpdateConfig(op Op, result *OpResult){
	CloneConfig(&kv.cur_config, &kv.prev_config)
	CloneConfig(&op.NewConfig, &kv.cur_config)
	for i := 0; i < shardctrler.NShards; i++{
		if kv.cur_config.Shards[i] == kv.gid && kv.prev_config.Shards[i] != kv.gid{
			if kv.prev_config.Shards[i] == INVALIDGID{
				kv.shard_data[i].KVMap = make(map[string]string)
				kv.shard_data[i].ShardState = OWNED
			} else {
				kv.shard_data[i].ShardState = TRANSFERING_IN
			}
		} else if kv.cur_config.Shards[i] != kv.gid && kv.prev_config.Shards[i] == kv.gid{
			kv.shard_data[i].ShardState = TRANSFERING_OUT
		}
	}
	result.Err = OK
}


func (kv *ShardKV) ApplyChReader() {
	for m := range kv.applyCh {
		if kv.killed() {
            break
        }
		op_result := OpResult{Err: OK, Value: "", RPCId: UniqueId{0, 0}}
		kv.mu.Lock()
		if m.CommandValid{
			if m.CommandIndex <= kv.last_executed_index{
				DPrintf("[group %v server %v]:ApplyChReader: command %v <= %v, has been executed\n", kv.gid, kv.me, m.CommandIndex, kv.last_executed_index)
				kv.mu.Unlock()
				continue
			}
			kv.last_executed_index = m.CommandIndex
			if chan_op, ok := m.Command.(Op); ok{
				if chan_op.Type == GET || chan_op.Type == PUT || chan_op.Type == APPEND{
					keyshard := key2shard(chan_op.Key)
					if !kv.IsShardInGroup(keyshard){
						op_result.Err = ErrWrongGroup
					}  else{
						is_last_op := kv.IsLastOp(chan_op.RPCId, chan_op.Key)
						if is_last_op == STALEOP{
							// do nothing
						} else if is_last_op == LASTOP{
							op_result = kv.shard_data[keyshard].LastOpReuslt[chan_op.RPCId.ClientId]
						} else {
							// new op
							op_result.RPCId = chan_op.RPCId
							op_result.Err = OK
							kv.ExecuteOp(chan_op, &op_result)
							kv.shard_data[keyshard].LastOpReuslt[chan_op.RPCId.ClientId] = op_result
						}	
					}
				} else if chan_op.Type == UPDATECONFIG{
					if chan_op.NewConfig.Num <= kv.cur_config.Num {
						op_result.Err = ErrConfigFinished
					} else {
						kv.ExecuteUpdateConfig(chan_op, &op_result)
						op_result.Err = OK
						// kv.lastOpReuslt[chan_op.RPCId.ClientId] = op_result
					}
				} else if chan_op.Type == ADDSHARD{
					DPrintf("[group %v server %v]:ApplyChReader handle AddShard for idx%v\n", kv.gid, kv.me, m.CommandIndex)
					if chan_op.RPCId.RequestId < int(kv.cur_config.Num){
						op_result.Err = ErrConfigFinished
					} else if chan_op.RPCId.RequestId > int(kv.cur_config.Num){
						op_result.Err = ErrNewConfigTooNew
					} else {
						op_result.Err = OK
						for i := 0; i < len(chan_op.NewShardData); i++{
							shard_idx := chan_op.NewShardData[i].ShardIndex
							if (kv.shard_data[shard_idx].ShardState != TRANSFERING_IN){
								if i != 0 {
									fmt.Printf("ApplyChReader: AddShard error, what happend????\n")
								}
								op_result.Err = ErrConfigFinished
								break
							}
							CloneShardData(&chan_op.NewShardData[i], &kv.shard_data[shard_idx])
							kv.shard_data[shard_idx].ShardState = OWNED
							DPrintf("[group %v server %v]:ApplyChReader handle AddShard %v finished\n", kv.gid, kv.me, shard_idx)
						}
					}
				} else if chan_op.Type == RemoveSHARD{
					// 这个如果在同一个ConfigNum内是幂等的 其实也可以不检查。。。
					if chan_op.RPCId.RequestId < int(kv.cur_config.Num){
						op_result.Err = ErrConfigFinished
					} else if chan_op.RPCId.RequestId > int(kv.cur_config.Num){
						fmt.Printf("ApplyChReader: RemoveShard error because too new, what happens??\n")
						op_result.Err = ErrNewConfigTooNew
					} else {
						op_result.Err = OK
						for i := 0; i < len(chan_op.RemoveShardList); i++{
							shard_idx := chan_op.RemoveShardList[i]
							if (kv.shard_data[shard_idx].ShardState != TRANSFERING_OUT){
								if i != 0{
									fmt.Printf("ApplyChReader: RemoveShard error because shards states different, what happend????\n")
								}
								op_result.Err = ErrConfigFinished
								break
							}
							// todo: garbage collection
							kv.shard_data[shard_idx].ShardState = NOTOWNED
							kv.shard_data[shard_idx].KVMap = make(map[string]string)
							kv.shard_data[shard_idx].LastOpReuslt = make(map[int64]OpResult)
						}
						DPrintf("[group %v server %v]:ApplyChReader handle RemoveShard %v finished, result is %v\n", kv.gid, kv.me, chan_op.RemoveShardList, op_result.Err)
					}
				}
				if ch_ptr, ok := kv.chan_map[m.CommandIndex]; ok{
					*ch_ptr <- op_result
				}
				if kv.maxraftstate != -1 && float64(kv.rf.GetPersistSize()) > 0.8 * float64(kv.maxraftstate){
					snapshot_data := kv.KVMakePersistfunc()
					kv.rf.Snapshot(m.CommandIndex, snapshot_data)
				}
			}
		} else if m.SnapshotValid{
			if kv.last_executed_index >= m.SnapshotIndex{
				kv.mu.Unlock()
				continue
			}
			// don't need to update last_executed_index here, because raft doesn't apply a log with smaller index
			snapshot_data := m.Snapshot
			kv.KVReadPersist(snapshot_data)
			kv.last_executed_index = m.SnapshotIndex
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) HandleOp(op Op) OpResult{
	op_result := OpResult{Err: OK, Value: "", RPCId: op.RPCId}
	kv.mu.Lock()
	_, is_leader  := kv.rf.GetState()
	if !is_leader{
		op_result.Err = ErrWrongLeader
		kv.mu.Unlock()
		return op_result
	}

	key_shard := key2shard(op.Key)
	if !kv.IsShardInGroup(key_shard){
		op_result.Err = ErrWrongGroup
		kv.mu.Unlock()
		return op_result
	}
	
	is_last_op := kv.IsLastOp(op.RPCId, op.Key)

	if is_last_op == STALEOP{
		// staleop 说明client其实已经往下走了，随便回复都可以
		kv.mu.Unlock()
		return op_result
	} else if is_last_op == LASTOP{
		op_result = kv.shard_data[key_shard].LastOpReuslt[op.RPCId.ClientId]	
		kv.mu.Unlock()
		return op_result
	}
	// new op
	// we cannot stop lock here like in raftkv, because the state may change? maybe we can still do it
	idx, start_term, is_leader := kv.rf.Start(op)
	if !is_leader{
		op_result.Err = ErrWrongLeader
		kv.mu.Unlock()
		return op_result
	}
	// should make a buffered chan to avoid deadlock
	mychan := make(chan OpResult, 1)
	kv.chan_map[idx] = &mychan
	kv.mu.Unlock()

	select {
		case msg := <-mychan:
			now_term, is_leader := kv.rf.GetState()
			if !is_leader || start_term != now_term{
				op_result.Err = ErrWrongLeader
			} else{
				op_result = msg
			}
		case <-time.After(ApplyTimeOut):
			op_result.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	if chan_ptr, ok := kv.chan_map[idx]; ok{
		if chan_ptr == &mychan{
			delete(kv.chan_map, idx)
		}
	}
	close(mychan)
	kv.mu.Unlock()
	return op_result
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	operations := Op{Type: "Get", Key: args.Key, Value: "", RPCId: args.RPCId}
	op_result := kv.HandleOp(operations)
	reply.Err = op_result.Err
	reply.Value = op_result.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	operations := Op{Type: args.Op, Key: args.Key, Value: args.Value, RPCId: args.RPCId}
	op_result := kv.HandleOp(operations)
	reply.Err = op_result.Err
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) KVMakePersistfunc () []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shard_data)
	e.Encode(kv.last_executed_index)
	e.Encode(kv.prev_config)
	e.Encode(kv.cur_config)
	kvsnapshot := w.Bytes()
	return kvsnapshot
}


func (kv *ShardKV) KVReadPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// // Your code here (2C).
	DPrintf("[group %v server %v]:KVReadPersist\n", kv.gid, kv.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var sd []ShardData
	var lei int
	var pc shardctrler.Config
	var cc shardctrler.Config
	if d.Decode(&sd) != nil  || d.Decode(&lei) != nil || d.Decode(&pc) != nil || d.Decode(&cc) != nil{
		fmt.Printf("Decode error\n")
		return
	} else {
		kv.shard_data = sd
		kv.last_executed_index = lei
		kv.prev_config = pc
		kv.cur_config = cc
	}
	DPrintf("[group %v server %v]:KVReadPersist finished, last_executed_index: %v, prev_config:%v, now_config:%v\n", kv.gid, kv.me, kv.last_executed_index, kv.prev_config, kv.cur_config)
}

func (kv *ShardKV) HandleConfigurationChange(op Op) OpResult{
	var op_result OpResult
	op_result.Err = OK
	op_result.RPCId = op.RPCId
	// if config_num != kv.cur_config.Num{
	// 	fmt.Printf("config_num %d not equal to cur_config_num %d\n", config_num, kv.cur_config.Num)
	// 	return
	// }
	// todo: just start it, let applyCh to check the config_num?
	kv.mu.Lock()
	idx, _, is_leader := kv.rf.Start(op)
	if !is_leader{
		kv.mu.Unlock()
		op_result.Err = ErrWrongLeader
		return op_result
	}
	mychan := make(chan OpResult, 1)
	kv.chan_map[idx] = &mychan
	kv.mu.Unlock()
	select {
		case msg := <-mychan:
			op_result = msg
		case <-time.After(ApplyTimeOut):
			op_result.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	if chan_ptr, ok := kv.chan_map[idx]; ok{
		if chan_ptr == &mychan{
			delete(kv.chan_map, idx)
		}
	}
	close(mychan)
	kv.mu.Unlock()
	return op_result	
}

func (kv *ShardKV) RemoveShardTicker(shards []int, config_num int){
	var op Op
	op.Type = RemoveSHARD
	op.RemoveShardList = make([]int, len(shards))
	copy(op.RemoveShardList, shards)
	op.RPCId = UniqueId{int64(kv.gid), config_num}
	DPrintf("[group %v server %v]:RemoveShardTicker %v starts for config %v\n", kv.gid, kv.me, shards, config_num)
	for !kv.killed(){
		result := kv.HandleConfigurationChange(op)
		DPrintf("[group %v server %v]:RemoveShardTicker %v for %v end, result %v\n", kv.gid, kv.me, shards, config_num,result.Err)
		if result.Err == OK || result.Err == ErrConfigFinished{
			return
		}
		time.Sleep(RPCRetryInterval)		
	}
}

func (kv *ShardKV) sendAddShard(gid int, shards []int, config_num int){
	kv.mu.Lock()
	var args  AddShardArgs
	args.ShardDataToAdd = make([]ShardData, len(shards))
	for i := 0; i < len(shards); i++{
		CloneShardData(&kv.shard_data[shards[i]], &args.ShardDataToAdd[i])
	}
	args.RPCId = UniqueId{int64(kv.gid), config_num}
	kv.mu.Unlock()
	DPrintf("[group %v server %v]:sendAddShard %v to group %v\n", kv.gid, kv.me, shards, gid)
	// todo:如果接收方一直没收到这个shard，就永远卡在这了？好像也合理
	for !kv.killed(){
		kv.mu.Lock()
		servers, ok := kv.cur_config.Groups[gid]
		kv.mu.Unlock()
		if  ok{
			for si := 0; si < len(servers); si++{
				srv := kv.make_end(servers[si])
				var reply AddShardReply
				ok := srv.Call("ShardKV.AddShard", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrConfigFinished){
					DPrintf("[group %v server %v]:sendAddShard %v to group %v success\n", kv.gid, kv.me, shards, gid)
					go kv.RemoveShardTicker(shards, config_num)
					return
				} 
				time.Sleep(RPCRetryInterval)
			}
		}
	}
}

func (kv *ShardKV) PrintAllShardState(){
	for i := 0; i < shardctrler.NShards; i++{
		DPrintf("[group %v server %v]:PrintAllShardState shard %d state %d", kv.gid, kv.me, i, kv.shard_data[i].ShardState)
	}
}

func (kv *ShardKV) AddShard(args *AddShardArgs, reply *AddShardReply){
	DPrintf("[group %v server %v]:AddShard from group%v start\n", kv.gid, kv.me, args.RPCId.ClientId)
	operations := Op{Type: "AddShard", Key: "", Value: "", RPCId: args.RPCId}
	operations.NewShardData = args.ShardDataToAdd
	op_result := kv.HandleConfigurationChange(operations)
	DPrintf("[group %v server %v]:AddShard from group%v end, reply %v\n", kv.gid, kv.me, args.RPCId.ClientId, op_result.Err)
	reply.Err = op_result.Err
}

func (kv *ShardKV) ConfigurationReader(){
	for !kv.killed(){
		if _, is_leader := kv.rf.GetState(); !is_leader{
			time.Sleep(GetConfInterval)
			continue
		}
		kv.mu.Lock()
		// check whether updating finished
		finished := true
		gid2need_send_shard := make(map[int][]int)
		for i := 0; i < shardctrler.NShards; i++{
			if kv.shard_data[i].ShardState == TRANSFERING_OUT {
				finished = false
				gid2need_send_shard[kv.cur_config.Shards[i]] = append(gid2need_send_shard[kv.cur_config.Shards[i]], i)
			}else if kv.shard_data[i].ShardState == TRANSFERING_IN{
				DPrintf("[group %v server %v]:ConfigurationReader: shard %d wait for transfering in\n", kv.gid, kv.me, i)
				finished = false
			} 
		}
		if !finished{
			DPrintf("[group %v server %v]:ConfigurationReader: unfinished transfering %v\n", kv.gid, kv.me, gid2need_send_shard)
			kv.mu.Unlock()
			for gid, shard_list := range gid2need_send_shard{
				go kv.sendAddShard(gid, shard_list, kv.cur_config.Num)
			}
			time.Sleep(GetConfInterval)
			continue
		}
		kv.mu.Unlock()
		
		new_config := kv.mck.Query(kv.cur_config.Num + 1)
		kv.mu.Lock()
		if new_config.Num != kv.cur_config.Num + 1{
			// 需要一个一个完成
			kv.mu.Unlock()
			time.Sleep(GetConfInterval)
			continue
		} else {
			DPrintf("[group %v server %v]:ConfigurationReader: cur config%v, new config %v\n", kv.gid, kv.me, kv.cur_config, new_config)
			op := Op{Type: UPDATECONFIG, Key: "", Value: "", RPCId: UniqueId{int64(kv.gid), new_config.Num}, NewConfig: new_config}
			kv.mu.Unlock()
			kv.HandleConfigurationChange(op)
			time.Sleep(GetConfInterval)
			continue
		}
	}
}

func (kv *ShardKV) EmptyOpWriter(){
	// a goroutine write empty op periods, to avoid the deadlock due to no new op mentioned in the readme
	for !kv.killed(){
		kv.rf.Start(nil)
		time.Sleep(EmptyOpWrieInterval)
	} 
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.dead = 0
	kv.chan_map = make(map[int]*chan OpResult)
	kv.shard_data = make([]ShardData, shardctrler.NShards)
	for i := 0; i < shardctrler.NShards; i++{
		kv.shard_data[i] = ShardData{ShardIndex: i,ShardState: NOTOWNED, KVMap: make(map[string]string)}
		kv.shard_data[i].LastOpReuslt = make(map[int64]OpResult)
	}
	kv.cur_config = shardctrler.Config{Num: 0, Shards: [shardctrler.NShards]int{}, Groups: make(map[int][]string)}
	kv.prev_config = shardctrler.Config{Num: 0, Shards: [shardctrler.NShards]int{}, Groups: make(map[int][]string)}
	for i := 0; i < shardctrler.NShards; i++{
		kv.cur_config.Shards[i] = INVALIDGID
		kv.prev_config.Shards[i] = INVALIDGID
	}
	kv.last_executed_index = -1

	snapshot := persister.ReadSnapshot()
	kv.mu.Lock()
	kv.KVReadPersist(snapshot)
	kv.mu.Unlock()


	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	
	go kv.ApplyChReader()
	go kv.ConfigurationReader()
	go kv.EmptyOpWriter()

	return kv
}
