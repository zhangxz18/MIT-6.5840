package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongTerm   = "ErrWrongTerm"
	ErrTimeOut     = "ErrTimeOut"
)

type Err string

type UniqueId struct {
	ClientId  int64
	RequestId int
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RPCId UniqueId
}

type PutAppendReply struct {
	Err Err
	RPCId UniqueId
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RPCId UniqueId
}

type GetReply struct {
	Err   Err
	Value string
	RPCId UniqueId
}
