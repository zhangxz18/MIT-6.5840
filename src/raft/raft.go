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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent
	currentTerm int
	votedFor int
	log []LogEntry

	// Volatile states on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex []int
	matchIndex []int

	// my own state
	now_state RaftServerState
	election_timeout_start_time time.Time
	heartbeat_start_time time.Time
	random_election_timeout int
	random_heartbeat_timeout int
	got_vote_num int 
	msg_chan chan ApplyMsg
	// Lock of currentterm
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.now_state == Leader
	return term, isleader
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	// DPrintf("server %d receive RequestVote from %d, args: %v", rf.me, args.CandidateId, args)
	// DPrintf("server %d now term is %v", rf.me, rf.currentTerm)
	if rf.UpdateTerm(args.Term) == SmallerTerm{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	last_log_idx := len(rf.log) - 1
	log_ok := (args.LastLogTerm > rf.log[last_log_idx].Term) || 
	(args.LastLogTerm == rf.log[last_log_idx].Term && args.LastLogIndex >= last_log_idx)
	grant_ok := args.Term == rf.currentTerm && log_ok && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)
	// the tla+ code of ongardie writes args.term <= rf.currentTerm here, because there are other operations that can change the term? but if the rf.current term is larger, we dont nned to do it?

	if grant_ok{
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.reset_election_timer()
	}
	// DPrintf("server %d reply RequestVote to %d, reply.term:%v, reply.votegranted:%v", rf.me, args.CandidateId, reply.Term, reply.VoteGranted)
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
	// DPrintf("server %d send RequestVote to %d, args: %v, now_state: %v", rf.me, server, args, rf.now_state)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		time.Sleep(time.Duration(TICKER_INTERVAL) * time.Millisecond)
		reply.Term = 0
		reply.VoteGranted = false
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	// DPrintf("server %d receive RequestVoteReply from %d, reply: %v", rf.me, server, reply)
	rf.mu.Lock()
	if rf.UpdateTerm(reply.Term) == SameTerm{
		if args.Term == rf.currentTerm && rf.now_state == Candidate && reply.VoteGranted{
			rf.got_vote_num += 1
			// DPrintf("server %d got_vote_num: %d after processing reply from %v", rf.me, rf.got_vote_num, server)
			if rf.got_vote_num * 2 > len(rf.peers) {
				rf.become_leader()
			}
		}
	}
	rf.mu.Unlock()
	// cannot handle reply here because of the parallelism, do it in the start_election
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
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply){
	for !rf.__sendAppendEntries(server, args, reply){
		// todo: check whether it's need to sleep
		// time.Sleep(time.Duration(TICKER_INTERVAL) * time.Millisecond)
		reply.Success = false
		reply.Term = 0
		rf.set_prev_log_index_term(server, args)
		args.Entries = rf.log[args.PrevLogIndex + 1:]
		args.LeaderCommit = rf.commitIndex
	}
}

func (rf *Raft) __sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		time.Sleep(time.Duration(TICKER_INTERVAL) * time.Millisecond)
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
	return rf.handleAppendEntriesReply(server, args, reply)
}

// return false if need to retry
func (rf *Raft)handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// stop retry if become a follower
	// stop retry if the msg is stale
	if rf.UpdateTerm(reply.Term) != SameTerm || rf.now_state != Leader || args.Term != rf.currentTerm{
		return true
	}
	if reply.Success{
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	} else{
		rf.nextIndex[server] -= 1
		// retry
		return false
	}

	// commitidx update
	prev_commit_idx := rf.commitIndex
	// new_commit_idx := rf.commitIndex
	finish_find_new_commit := false
	for log_idx := len(rf.log) - 1; log_idx > prev_commit_idx; log_idx--{
		count := 1
		// only commit the entry in the same term
		if rf.log[log_idx].Term != rf.currentTerm{
			break
		}
		for peer_idx, matchidx := range rf.matchIndex{
			if peer_idx != rf.me && matchidx >= log_idx{
				count += 1
				if count * 2 > len(rf.peers){
					rf.commitIndex = log_idx
					//todo: send commit to follower
					finish_find_new_commit = true
					break
				}
			}
		}
		if finish_find_new_commit{
			break
		}
	} 
	if finish_find_new_commit{
		rf.ApplyMsg2StateMachine()
	}
	return true
}

func (rf *Raft) ApplyMsg2StateMachine(){
	DPrintf("server %d start apply msg to state machine, lastApplied: %d, commitIndex: %d", rf.me, rf.lastApplied, rf.commitIndex)
	for idx := rf.lastApplied + 1; idx <= rf.commitIndex; idx++ {
		msg := ApplyMsg{
			CommandValid: true,
			Command: rf.log[idx].Command,
			CommandIndex: idx,
		}
		DPrintf("server %d apply msg to state machine, command:%v, idx:%d", rf.me, msg.Command, idx)
		rf.msg_chan <- msg
		rf.lastApplied += 1
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (len(args.Entries) > 0 || args.LeaderCommit > rf.commitIndex){
		DPrintf("server %d receive AppendEntries from %d, args: %v", rf.me, args.LeaderId, args)
	}
	msg_term_state := rf.UpdateTerm(args.Term)
	if msg_term_state == SmallerTerm{
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	// set to follower to stop the election
	} else if msg_term_state == SameTerm && rf.now_state == Candidate{
		rf.now_state = Follower
	}
	rf.reset_election_timer()
	if args.PrevLogIndex != 0 && (len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 3. write conflict entry
	start_idx := args.PrevLogIndex + 1
	idx := 0
	write_idx := start_idx + idx
	for _, entry := range args.Entries {
		if write_idx < len(rf.log){ 
			if rf.log[write_idx].Term != entry.Term{
				rf.log[write_idx] = entry
			}
			idx += 1
			write_idx += 1
		} else {
			break
		}
	}
	// 4. write the remain entry
	rf.log = append(rf.log, args.Entries[idx:]...)
	// 5. update commit index
	if args.LeaderCommit > rf.commitIndex{
		newest_index := start_idx + len(args.Entries) - 1
		if args.LeaderCommit < newest_index {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = newest_index
		}
		rf.ApplyMsg2StateMachine()
	}
	reply.Term = rf.currentTerm
	reply.Success = true
}

// return true if the term is updated
func (rf *Raft) UpdateTerm(term int) ReceivedTermState{
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.now_state = Follower
		rf.votedFor = -1
		rf.got_vote_num = 0
		return LargerTerm
	} else if term < rf.currentTerm {
		return SmallerTerm
	} else {
		return SameTerm
	}
}

func (rf *Raft) set_prev_log_index_term(server int, args *AppendEntriesArgs){
	args.PrevLogIndex = rf.nextIndex[server] - 1
	if args.PrevLogIndex < 0{
		args.PrevLogTerm = 0
	} else {
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	}
}

func (rf *Raft) send_appendentries_to_all(){
	args := make([]AppendEntriesArgs, len(rf.peers))
	replys := make([]AppendEntriesReply, len(rf.peers))
	for idx := range rf.peers{
		if idx != rf.me{
			args[idx] = AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				LeaderCommit: rf.commitIndex,
			}
			rf.set_prev_log_index_term(idx, &args[idx])
			args[idx].Entries = rf.log[rf.nextIndex[idx]:]
			go rf.sendAppendEntries(idx, &args[idx], &replys[idx])
		}
	}
}

func (rf *Raft) send_heartbeat() {
	rf.reset_heartbeat_timer()
	rf.send_appendentries_to_all()
}

func (rf *Raft) send_real_appendentry(){
	rf.reset_heartbeat_timer()
	// rf.log = append(rf.log, logs...) // todo: maybe it should be move to start() because of the lock
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
	if rf.now_state != Leader{
		isLeader = false
	} else {
		// need lock? yes, to avoid write in different places
		rf.mu.Lock()
		index = len(rf.log)
		term = rf.currentTerm
		isLeader = true
		logs := []LogEntry{{Term: term, Index: index, Command: command}}
		rf.log = append(rf.log, logs...)
		go rf.send_real_appendentry()
		rf.mu.Unlock()
	}
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
	rf.election_timeout_start_time = time.Now()
	rf.random_election_timeout = ELECTION_TIMEOUT_MIN + rand.Intn(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN)
 }

 func (rf *Raft) reset_heartbeat_timer() {
	rf.heartbeat_start_time = time.Now()
	rf.random_election_timeout = HEARTBEAT_INTERVAL
 }

func (rf *Raft) become_leader() {
	rf.now_state = Leader
	rf.got_vote_num = 0 // to avoid other rpc goroutine make it leader again
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for idx := range rf.peers {
		rf.nextIndex[idx] = len(rf.log)
		rf.matchIndex[idx] = 0
	}
	rf.send_heartbeat()
}

func (rf *Raft) start_election() {
	rf.mu.Lock()
	// DPrintf("server %d start election at %v", rf.me, time.Now())
	
	rf.now_state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.got_vote_num = 1
	rf.mu.Unlock()
	rf.reset_election_timer()
	replys := make([]RequestVoteReply, len(rf.peers))
	args := make([]RequestVoteArgs, len(rf.peers))

	for idx := range rf.peers {
		if idx != rf.me {
			args[idx] = RequestVoteArgs{
				Term: rf.currentTerm,
				CandidateId: rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm: rf.log[len(rf.log) - 1].Term,
			}
			go rf.sendRequestVote(idx, &args[idx], &replys[idx])
		}
	} 
}

func (rf *Raft) ticker() {
	rf.reset_election_timer()
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.
		// todo: lock?
		if rf.now_state != Leader {
			if time.Since(rf.election_timeout_start_time) > time.Duration(rf.random_election_timeout) * time.Millisecond {
				rf.start_election()	
			}
		} else {
			if time.Since(rf.heartbeat_start_time) > time.Duration(rf.random_heartbeat_timeout) * time.Millisecond {
				rf.send_heartbeat()
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
	rf.dead = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0, Index: 0, Command: nil}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	// will initial when become leader
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.now_state = Follower
	rf.reset_election_timer()
	rf.reset_heartbeat_timer()
	rf.msg_chan = applyCh


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
