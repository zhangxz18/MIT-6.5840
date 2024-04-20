package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RaftServerState int
const (
	Follower RaftServerState = iota
	Candidate
	Leader
)

type ReceivedTermState int
const (
	LargerTerm ReceivedTermState = iota
	SameTerm
	SmallerTerm
)

const ELECTION_TIMEOUT_MAX int = 600
const ELECTION_TIMEOUT_MIN int = 300
const HEARTBEAT_INTERVAL int = 100
const TICKER_INTERVAL int = 20
const RESEND_WHEN_LOG_CONFLICT_INTERVAL int = 10
const RESEND_WHEN_REQLOST_INTERVAL int = 20

type LogEntry struct {
	Term int
	Index int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	timer_mu sync.Mutex

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent
	CurrentTerm int
	VotedFor int
	Log []LogEntry
	LastSnapshotLogIndex int
	LastSnapshotLogTerm int
	SnapshotData []byte

	// Volatile states on all servers
	CommitIndex int
	LastApplied int

	// volatile state on leaders
	NextIndex []int
	MatchIndex []int

	// my own state
	now_state RaftServerState
	election_timeout_start_time time.Time
	heartbeat_start_time time.Time
	random_election_timeout int
	random_heartbeat_timeout int
	got_vote_num int 
	msg_chan chan ApplyMsg
	installing_snapshot bool
	// Lock of currentterm
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.CurrentTerm
	isleader = rf.now_state == Leader
	return term, isleader
}

// equal to last log index + 1
func (rf *Raft) GetLogLength() int {
	return rf.LastSnapshotLogIndex + len(rf.Log)
}

func (rf *Raft) GetLastLogIndex() int {
	return rf.LastSnapshotLogIndex + len(rf.Log) - 1
}

func (rf *Raft) GetLogByIndex(idx int) LogEntry{
	if (idx < rf.LastSnapshotLogIndex){
		fmt.Printf("Server %v GetLogByIndex: idx %v < LastSnapshotLogIndex %v\n", rf.me, idx, rf.LastSnapshotLogIndex)
	}
	return rf.Log[idx - rf.LastSnapshotLogIndex]
}

func (rf *Raft) SetLogByIndex(idx int, entry LogEntry){
	rf.Log[idx - rf.LastSnapshotLogIndex] = entry
}

// only return a slices from the current log
func (rf *Raft) GetLogSlices(start int, end int) []LogEntry{
	if (end == -1){
		return rf.Log[start - rf.LastSnapshotLogIndex:]
	} else if(start == -1){
		// only used for truncate log
		return rf.Log[:end - rf.LastSnapshotLogIndex]
	}
	return rf.Log[start - rf.LastSnapshotLogIndex: end - rf.LastSnapshotLogIndex]
}
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.LastSnapshotLogIndex)
	e.Encode(rf.LastSnapshotLogTerm)
	// e.Encode(rf.SnapshotData)
	raftstate := w.Bytes()

	rf.persister.Save(raftstate, rf.SnapshotData)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// // Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var saved_term int
	var saved_votefor int
	var saved_logs []LogEntry
	var saved_last_snapshot_log_index int
	var saved_last_snapshot_log_term int
	if d.Decode(&saved_term) != nil ||
	   d.Decode(&saved_votefor) != nil ||
	   d.Decode(&saved_logs) != nil ||
	   d.Decode(&saved_last_snapshot_log_index) != nil ||
	   d.Decode(&saved_last_snapshot_log_term) != nil {
		fmt.Printf("Decode error\n")
		return
	} else {
		rf.CurrentTerm = saved_term
		rf.VotedFor = saved_votefor
		rf.Log = saved_logs
		rf.LastSnapshotLogIndex = saved_last_snapshot_log_index
		rf.LastSnapshotLogTerm = saved_last_snapshot_log_term
		rf.CommitIndex = rf.LastSnapshotLogIndex
		rf.LastApplied = rf.LastSnapshotLogIndex
		rf.SnapshotData = rf.persister.ReadSnapshot()
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// DPrintf("server %d start snapshot at index %v", rf.me, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.LastSnapshotLogIndex {
		return
	}
	if index > rf.GetLastLogIndex(){
		fmt.Printf("Snapshot: index %v > last_log_index%v\n", index, rf.GetLastLogIndex())
		return
	}
	// trim the log
	// to avoid the index out of range, we need to add 1(because Raft think the log is start from 1)
	last_term := rf.GetLogByIndex(index).Term
	rf.CutLogWithSnapshot(index, last_term)
	rf.LastSnapshotLogTerm = last_term
	rf.LastSnapshotLogIndex = index
	rf.SnapshotData = clone(snapshot)
	// todo: check need it?
	if (rf.CommitIndex < rf.LastSnapshotLogIndex){
		rf.CommitIndex = rf.LastSnapshotLogIndex
	}
	if (rf.LastApplied < rf.LastSnapshotLogIndex){
		rf.LastApplied = rf.LastSnapshotLogIndex
	}
	// DPrintf("server %d finish snapshot at index %v, now log is %v", rf.me, index, rf.Log)
	rf.persist()
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.LastSnapshotLogIndex)
	// e.Encode(rf.GetLogByIndex(index))
	// snapshot_data := w.Bytes()
	// rf.LastSnapshotLogIndex = index
	// rf.LastSnapshotLogTerm = rf.GetLogByIndex(index).Term
	// rf.SnapshotData = clone(snapshot_data)
}

type InstallSnapshotArgs struct{
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	// Offset int // always be zero
	// Done bool // always be true
	Data []byte
}

type InstallSnapshotReply struct{
	Term int
	NewSnapLogIndex int
}

func (rf *Raft) CutLogWithSnapshot(LastIncludedIndex int, LastIncludedTerm int){
	// set idx 0 to {Term: LastIncludedTerm, Index: LastIncludedIndex, Command: nil} to avoid special judge in the appendentries
	if LastIncludedIndex > rf.GetLastLogIndex(){
		rf.Log = []LogEntry{{Term: LastIncludedTerm, Index: LastIncludedIndex, Command: nil}}	
	} else {
		if (LastIncludedTerm != rf.GetLogByIndex(LastIncludedIndex).Term){
			rf.Log = []LogEntry{{Term: LastIncludedTerm, Index: LastIncludedIndex, Command: nil}}
		} else {
			temp_log := rf.GetLogSlices(LastIncludedIndex + 1, -1)
			rf.Log = []LogEntry{{Term: LastIncludedTerm, Index: LastIncludedIndex, Command: nil}}
			rf.Log = append(rf.Log, temp_log...)
		}
	}
}

func (rf *Raft) write_data_to_applychan(msg ApplyMsg){
	rf.msg_chan <- msg
	rf.mu.Lock()
	if msg.SnapshotIndex > rf.LastApplied{
		rf.LastApplied = msg.SnapshotIndex
	}
	rf.installing_snapshot = false
	rf.mu.Unlock()
}

func (rf *Raft) SendInstallSnapshot(server int){
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term: rf.CurrentTerm,
		LeaderId: rf.me,
		LastIncludedIndex: rf.LastSnapshotLogIndex,
		LastIncludedTerm: rf.LastSnapshotLogTerm,
		Data: clone(rf.SnapshotData),
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	// DPrintf("server %d send InstallSnapshot to %d, args: %v", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	if ok{
		rf.mu.Lock()
		if rf.UpdateTerm(reply.Term) == LargerTerm{
			rf.now_state = Follower
		} else if reply.NewSnapLogIndex > args.LastIncludedIndex {
			// only the commit data can be snapshoted, so previous data is same
			// only to prevent packet loss
			// do nothing
			// rf.NextIndex[server] = reply.NewSnapLogIndex + 1 
		} else{
			rf.MatchIndex[server] = args.LastIncludedIndex
			rf.NextIndex[server] = args.LastIncludedIndex + 1
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply){
	// DPrintf("server %d receive InstallSnapshot from %d, args: %v", rf.me, args.LeaderId, args)
	rf.mu.Lock()
	if rf.UpdateTerm(args.Term) == SmallerTerm{
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	}
	reply.Term = rf.CurrentTerm
	// don't need to reply in fact? but cannot implement...
	if args.LastIncludedIndex <= rf.LastSnapshotLogIndex{
		reply.NewSnapLogIndex = rf.LastSnapshotLogIndex
		rf.mu.Unlock()
		return
	}
	msg := ApplyMsg{
		CommandValid: false,
		SnapshotValid: true,
		Snapshot: args.Data,
		SnapshotTerm: args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.installing_snapshot = true
	rf.CutLogWithSnapshot(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.SnapshotData = clone(args.Data)
	rf.LastSnapshotLogTerm = args.LastIncludedTerm
	rf.LastSnapshotLogIndex = args.LastIncludedIndex
	if args.LastIncludedIndex > rf.CommitIndex{
		rf.CommitIndex = args.LastIncludedIndex
	}
	reply.NewSnapLogIndex = rf.LastSnapshotLogIndex
	rf.persist()
	rf.mu.Unlock()
	go rf.write_data_to_applychan(msg)
	// DPrintf("server %d finish InstallSnapshot from %d, now log is %v", rf.me, args.LeaderId, rf.Log)
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply){
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// // // DPrintf("server %d receive RequestVote from %d, args: %v", rf.me, args.CandidateId, args)
	// // // DPrintf("server %d now term is %v", rf.me, rf.currentTerm)
	if rf.UpdateTerm(args.Term) == SmallerTerm{
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	last_log_idx := rf.GetLastLogIndex()
	log_ok := (args.LastLogTerm > rf.GetLogByIndex(last_log_idx).Term) || 
	(args.LastLogTerm == rf.GetLogByIndex(last_log_idx).Term && args.LastLogIndex >= last_log_idx)
	grant_ok := args.Term == rf.CurrentTerm && log_ok && (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId)
	// the tla+ code of ongardie writes args.term <= rf.currentTerm here, because there are other operations that can change the term? but if the rf.current term is larger, we dont nned to do it?

	if grant_ok{
		rf.VotedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		rf.reset_election_timer()
	}
	// // // DPrintf("server %d reply RequestVote to %d, reply.term:%v, reply.votegranted:%v", rf.me, args.CandidateId, reply.Term, reply.VoteGranted)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// // // DPrintf("server %d send RequestVote to %d, args: %v, now_state: %v", rf.me, server, args, rf.now_state)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// only retry when the term is the same and still in candidate state
	for !ok {
		rf.mu.Lock()
		if args.Term != rf.CurrentTerm || rf.now_state != Candidate{
			rf.mu.Unlock()
			return ok
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(RESEND_WHEN_REQLOST_INTERVAL) * time.Millisecond)
		reply.Term = 0
		reply.VoteGranted = false
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	// // // DPrintf("server %d receive RequestVoteReply from %d, reply: %v", rf.me, server, reply)
	rf.mu.Lock()
	if rf.UpdateTerm(reply.Term) == SameTerm{
		if args.Term == rf.CurrentTerm && rf.now_state == Candidate && reply.VoteGranted{
			rf.got_vote_num += 1
			// // // DPrintf("server %d got_vote_num: %d after processing reply from %v", rf.me, rf.got_vote_num, server)
			if rf.got_vote_num * 2 > len(rf.peers) {
				rf.become_leader()
			}
		}
	}
	rf.mu.Unlock()
	return ok
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	ConflictIndex int
	ConflictTerm int
	// IndexIsInsideSnapshot bool
}

func init_reply(reply *AppendEntriesReply){
	reply.Term = 0
	reply.Success = false
	reply.ConflictIndex = 0
	reply.ConflictTerm = 0
	// reply.IndexIsInsideSnapshot = false
}

func (rf *Raft) check_need_send_snapshot(server int) bool{
	return rf.NextIndex[server] <= rf.LastSnapshotLogIndex
}

func (rf *Raft) sendAppendEntries(server int){
	for{
		rf.mu.Lock()
		if rf.now_state != Leader{
			rf.mu.Unlock()
			return
		}
		need_send_snapshot := rf.check_need_send_snapshot(server)
		if (need_send_snapshot){
			rf.mu.Unlock()
			rf.SendInstallSnapshot(server)
		} else {
			args := AppendEntriesArgs{}
			reply := AppendEntriesReply{}
			init_reply(&reply)
			args.Term = rf.CurrentTerm
			args.LeaderId = rf.me
			rf.set_prev_log_index_term(server, &args)
			args.Entries = rf.GetLogSlices(args.PrevLogIndex + 1, -1)
			args.LeaderCommit = rf.CommitIndex
			rf.mu.Unlock()
			if rf.__sendAppendEntries(server, &args, &reply){
				break
			}
		}
	}
}

// return true if don't need to retry
func (rf *Raft) __sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	// if len(args.Entries) > 0 {
	// 	// // DPrintf("server %d sendAppendEntries to %d, args: %v", rf.me, server, args)
	// }
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// DPrintf("server %d send AppendEntries to %d, args: %v", rf.me, server, args)
	// only retry when the term is the same and still in leader state
	for !ok{
		cterm, isleader := rf.GetState()
		if args.Term != cterm || !isleader{
			return true // don't retry
		}
		time.Sleep(time.Duration(RESEND_WHEN_REQLOST_INTERVAL) * time.Millisecond)
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		// DPrintf("server %d send AppendEntries to %d inretry, args: %v", rf.me, server, args)
	}
	return rf.handleAppendEntriesReply(server, args, reply)
}

// return false if need to retry
func (rf *Raft)handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	// DPrintf("server %d receive AppendEntriesReply from %d, args:%v, reply: %v", rf.me, server, args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// stop retry if become a follower
	// stop retry if the msg is stale
	if rf.UpdateTerm(reply.Term) != SameTerm || rf.now_state != Leader || args.Term != rf.CurrentTerm{
		return true
	}
	if reply.Success{
		// a stale msg, don't need to handle
		if args.PrevLogIndex + len(args.Entries) > rf.MatchIndex[server] {
			rf.NextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.MatchIndex[server] = args.PrevLogIndex + len(args.Entries)
		}
	} else{
		// if reply.IndexIsInsideSnapshot{
		// 	rf.NextIndex[server] = reply.ConflictIndex + 1
		// 	return false
		// }
		if (reply.ConflictTerm == 0){
			if(reply.ConflictIndex == 0){
				// rf.NextIndex[server] -= 1 // todo: check need it?
				return true
			} else {
				rf.NextIndex[server] = reply.ConflictIndex
				return false
			}
		} else {
			same_term_idx := -1
			for idx := args.PrevLogIndex; idx >= rf.LastSnapshotLogIndex; idx --{
				if rf.GetLogByIndex(idx).Term == reply.ConflictTerm{
					same_term_idx = idx
					break
				}
			}
			if same_term_idx == -1{
				rf.NextIndex[server] = reply.ConflictIndex
				return false
			} else {
				rf.NextIndex[server] = same_term_idx + 1
				return false
			}
		}

	}

	// commitidx update
	prev_commit_idx := rf.CommitIndex
	// new_commit_idx := rf.commitIndex
	finish_find_new_commit := false
	for log_idx := rf.GetLastLogIndex(); log_idx > prev_commit_idx; log_idx--{
		count := 1
		// only commit the entry in the same term
		if rf.GetLogByIndex(log_idx).Term != rf.CurrentTerm{
			break
		}
		for peer_idx, matchidx := range rf.MatchIndex{
			if peer_idx != rf.me && matchidx >= log_idx{
				count += 1
				if count * 2 > len(rf.peers){
					// // DPrintf("server %d chang commit index:%v -> %v", rf.me, prev_commit_idx, log_idx)
					rf.CommitIndex = log_idx
					finish_find_new_commit = true
					break
				}
			}
		}
		if finish_find_new_commit{
			break
		}
	} 
	// if finish_find_new_commit{
	// 	rf.ApplyMsg2StateMachine()
	// }
	return true
}

func (rf *Raft) ApplyMsg2StateMachine(){
	// // DPrintf("server %d start apply msg to state machine, lastApplied: %d, commitIndex: %d", rf.me, rf.LastApplied, rf.CommitIndex)
	for idx := rf.LastApplied + 1; idx <= rf.CommitIndex; idx++ {
		msg := ApplyMsg{
			CommandValid: true,
			Command: rf.GetLogByIndex(idx).Command,
			CommandIndex: idx,
		}
		// // DPrintf("server %d apply msg to state machine, command:%v, idx:%d", rf.me, msg.Command, idx)
		rf.msg_chan <- msg
		rf.LastApplied = idx
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("server %d receive AppendEntries from %d, args: %v", rf.me, args.LeaderId, args)
	// DPrintf("server %d now Log is %v", rf.me, rf.Log)
	msg_term_state := rf.UpdateTerm(args.Term)
	if msg_term_state == SmallerTerm{
		reply.Term = rf.CurrentTerm
		reply.Success = false
		reply.ConflictIndex = 0
		reply.ConflictTerm = 0
		return
	// set to follower to stop the election
	} else if msg_term_state == SameTerm && rf.now_state == Candidate{
		rf.now_state = Follower
	}
	rf.reset_election_timer()
	// If the previous log index is less than the last snapshot index, then just let the leader start from the lastidx of this server
	if args.PrevLogIndex < rf.LastSnapshotLogIndex{
		reply.Term = rf.CurrentTerm
		reply.Success = false
		reply.ConflictIndex = rf.GetLastLogIndex() + 1
		reply.ConflictTerm = 0
		// reply.IndexIsInsideSnapshot = true
		return
	}
	if args.PrevLogIndex != 0 {
		if args.PrevLogIndex > rf.GetLastLogIndex(){
			reply.Term = rf.CurrentTerm
			reply.Success = false
			reply.ConflictIndex = rf.GetLastLogIndex() + 1
			reply.ConflictTerm = 0
			// DPrintf("server %d reject AppendEntries from %d, because PrevLogIndex > LastLogIndex, reply: %v", rf.me, args.LeaderId, reply)
			return
		} else if rf.GetLogByIndex(args.PrevLogIndex).Term != args.PrevLogTerm {
			reply.Term = rf.CurrentTerm
			reply.Success = false
			reply.ConflictIndex = args.PrevLogIndex
			reply.ConflictTerm = rf.GetLogByIndex(args.PrevLogIndex).Term
			for idx := args.PrevLogIndex - 1; idx >= rf.LastSnapshotLogIndex; idx--{
				if (rf.GetLogByIndex(idx).Term == reply.ConflictTerm){
					reply.ConflictIndex = idx
				} else {
					break
				}
			}
			// DPrintf("server %d reject AppendEntries from %d, because PrevLogTerm not match, reply: %v", rf.me, args.LeaderId, reply)
			return
		}
	}

	// 3. write conflict entry(if a entry is conflict, truncate the log following it)
	start_idx := args.PrevLogIndex + 1
	idx := 0
	write_idx := start_idx + idx
	// // // DPrintf("server %d start append entry from %d, start_idx: %d", rf.me, args.LeaderId, start_idx)
	for _, entry := range args.Entries {
		if write_idx <= rf.GetLastLogIndex(){ 
			if rf.GetLogByIndex(write_idx).Term != entry.Term{
				rf.Log = rf.GetLogSlices(-1, write_idx)
				break
			}
			idx += 1
			write_idx += 1
		} else {
			break
		}
	}
	// 4. write the remain entry
	rf.Log = append(rf.Log, args.Entries[idx:]...)
	rf.persist()
	// 5. update commit index
	if args.LeaderCommit > rf.CommitIndex{
		newest_index := start_idx + len(args.Entries) - 1
		if args.LeaderCommit < newest_index {
			rf.CommitIndex = args.LeaderCommit
		} else {
			rf.CommitIndex = newest_index
		}
		// // DPrintf("server %d chang commit index: to %v", rf.me, rf.CommitIndex)
		// rf.ApplyMsg2StateMachine()
	}
	reply.Term = rf.CurrentTerm
	reply.Success = true
}

// return true if the term is updated
func (rf *Raft) UpdateTerm(term int) ReceivedTermState{
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	if term > rf.CurrentTerm {
		// // // DPrintf("server %d update term from %d to %d", rf.me, rf.currentTerm, term)
		rf.CurrentTerm = term
		rf.now_state = Follower
		rf.VotedFor = -1
		rf.got_vote_num = 0
		rf.persist()
		return LargerTerm
	} else if term < rf.CurrentTerm {
		return SmallerTerm
	} else {
		return SameTerm
	}
}

func (rf *Raft) set_prev_log_index_term(server int, args *AppendEntriesArgs){
	args.PrevLogIndex = rf.NextIndex[server] - 1
	if args.PrevLogIndex < 0{
		args.PrevLogTerm = 0
	} else {
		args.PrevLogTerm = rf.GetLogByIndex(args.PrevLogIndex).Term
	}
}

func (rf *Raft) send_appendentries_to_all(){
	for idx := range rf.peers{
		if idx != rf.me{
			go rf.sendAppendEntries(idx)
		}
	}
}

func (rf *Raft) send_heartbeat() {
	rf.reset_heartbeat_timer()
	rf.send_appendentries_to_all()
}

func (rf *Raft) send_real_appendentry(){
	rf.reset_heartbeat_timer()
	rf.send_appendentries_to_all()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()

	if rf.now_state != Leader{
		isLeader = false
	} else {
		// need lock? yes, to avoid write in different places
		index = rf.GetLastLogIndex() + 1
		term = rf.CurrentTerm
		isLeader = true
		logs := []LogEntry{{Term: term, Index: index, Command: command}}
		rf.Log = append(rf.Log, logs...)
		rf.persist()
		go rf.send_real_appendentry()
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
 func (rf *Raft) reset_election_timer() {
	rf.timer_mu.Lock()
	defer rf.timer_mu.Unlock()
	rf.election_timeout_start_time = time.Now()
	rf.random_election_timeout = ELECTION_TIMEOUT_MIN + rand.Intn(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN)
 }

 func (rf *Raft) reset_heartbeat_timer() {
	rf.timer_mu.Lock()
	defer rf.timer_mu.Unlock()
	rf.heartbeat_start_time = time.Now()
	rf.random_heartbeat_timeout = HEARTBEAT_INTERVAL
 }

func (rf *Raft) become_leader() {
	// // DPrintf("server %d become leader of term%v\n", rf.me, rf.CurrentTerm)
	rf.now_state = Leader
	rf.got_vote_num = 0 // to avoid other rpc goroutine make it leader again
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))
	for idx := range rf.peers {
		rf.NextIndex[idx] = rf.GetLastLogIndex() + 1
		rf.MatchIndex[idx] = 0
	}
	rf.send_heartbeat()
}

func (rf *Raft) start_election() {
	rf.mu.Lock()
	// // // DPrintf("server %d start election at %v", rf.me, time.Now())
	
	rf.now_state = Candidate
	rf.CurrentTerm += 1
	rf.VotedFor = rf.me
	rf.got_vote_num = 1
	rf.persist()
	rf.reset_election_timer()
	replys := make([]RequestVoteReply, len(rf.peers))
	args := RequestVoteArgs{
		Term: rf.CurrentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.GetLastLogIndex(),
		LastLogTerm: rf.GetLogByIndex(rf.GetLastLogIndex()).Term,
	}
	rf.mu.Unlock()
	for idx := range rf.peers {
		if idx != rf.me {
			go rf.sendRequestVote(idx, &args, &replys[idx])
		}
	} 
}

func (rf *Raft) commit_ticker(){
	for !rf.killed(){
		rf.mu.Lock()
		if( rf.CommitIndex > rf.LastApplied && !rf.installing_snapshot){
			// DPrintf("server %d commit_ticker, commitIndex: %d, lastApplied: %d", rf.me, rf.CommitIndex, rf.LastApplied)
			new_commit_idx := rf.CommitIndex
			commit_msgs := []ApplyMsg{}
			for idx := rf.LastApplied + 1; idx <= rf.CommitIndex; idx++ {
				commit_msgs = append(commit_msgs, ApplyMsg{
					CommandValid: true,
					Command: rf.GetLogByIndex(idx).Command,
					CommandIndex: idx,
				})
			}
			rf.mu.Unlock()
			for _, msg := range commit_msgs{
				rf.msg_chan <- msg
			}
			rf.mu.Lock()
			if (new_commit_idx > rf.LastApplied){
				rf.LastApplied = new_commit_idx
			}
			rf.mu.Unlock()
		} else{
			rf.mu.Unlock()
		}
		time.Sleep(time.Duration(TICKER_INTERVAL) * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	rf.reset_election_timer()
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.
		_, isleader := rf.GetState()
		if !isleader {
			rf.timer_mu.Lock()
			if time.Since(rf.election_timeout_start_time) > time.Duration(rf.random_election_timeout) * time.Millisecond {
				rf.timer_mu.Unlock()
				rf.start_election()	
			} else {
				rf.timer_mu.Unlock()
			}
		} else {
			rf.timer_mu.Lock()
			if time.Since(rf.heartbeat_start_time) > time.Duration(rf.random_heartbeat_timeout) * time.Millisecond {
				rf.timer_mu.Unlock()
				rf.send_heartbeat()
			} else {
				rf.timer_mu.Unlock()
			}
		}

		time.Sleep(time.Duration(TICKER_INTERVAL) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu = sync.Mutex{}
	rf.timer_mu = sync.Mutex{}
	rf.dead = 0

	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = []LogEntry{{Term: 0, Index: 0, Command: nil}}
	rf.LastSnapshotLogIndex = 0
	rf.LastSnapshotLogTerm = 0
	rf.SnapshotData = nil
	rf.CommitIndex = 0
	rf.LastApplied = 0
	// will initial when become leader
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))

	rf.now_state = Follower
	rf.reset_election_timer()
	rf.reset_heartbeat_timer()
	rf.msg_chan = applyCh
	rf.installing_snapshot = false


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commit_ticker()


	return rf
}
