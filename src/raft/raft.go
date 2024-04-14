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

const ELECTION_TIMEOUT_MAX int = 500
const ELECTION_TIMEOUT_MIN int = 300
const HEARTBEAT_INTERVAL int = 100
const ELECTION_TICKER_INTERVAL int = 50

type LogEntry struct {
	term int
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
	// todo: when to start heartbeat_start_time?
	election_timeout_start_time time.Time
	heartbeat_start_time time.Time

	got_vote_num int 

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
	term int
	candidateId int
	lastLogIndex int
	lastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	term int
	voteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error{
	// Your code here (2A, 2B).
	rf.UpdateTerm(args.term)
	reply.term = rf.currentTerm
	reply.voteGranted = false
	log_ok := (args.lastLogTerm > rf.log[rf.lastApplied].term) || 
	(args.lastLogTerm == rf.log[rf.lastApplied].term && args.lastLogIndex >= rf.lastApplied)
	grant_ok := args.term == rf.currentTerm && log_ok && (rf.votedFor == -1 || rf.votedFor == args.candidateId)
	// the tla+ code of ongardie writes args.term <= rf.currentTerm here, because there are other operations that can change the term? but if the rf.current term is larger, we dont nned to do it?

	if args.term <= rf.currentTerm{
		if grant_ok{
			reply.voteGranted = true
			rf.votedFor = args.candidateId
			// restart timer
			rf.election_timeout_start_time = time.Now()
		}
	}
	return nil
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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, finish_vote chan int) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	// just to avoid the finish_vote channel is closed
	select {
		case finish_vote <- server:
		default:
	}
	// cannot handle reply here because of the parallelism, do it in the start_election
	return ok
}

type AppendEntriesArgs struct {
	term int
	leaderId int
	prevLogIndex int
	prevLogTerm int
	entries []LogEntry
	leaderCommit int
}

type AppendEntriesReply struct {
	term int
	success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply){
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error{
	rf.UpdateTerm(args.term)
	reply.term = rf.currentTerm
	reply.success = false
	if args.term == rf.currentTerm{	
	}
	return nil
}

// return true if the term is updated
func (rf *Raft) UpdateTerm(term int) bool{
	result := false
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.now_state = Follower
		rf.votedFor = -1
		rf.got_vote_num = 0
		// rf.election_timeout_start_time = time.Now()
		result = true
	}
	return result
}

func (rf *Raft) send_heartbeat() {
	args := AppendEntriesArgs{
		term: rf.currentTerm,
		leaderId: rf.me,
		prevLogIndex: rf.lastApplied,
		prevLogTerm: rf.log[rf.lastApplied].term,
		entries: []LogEntry{},
		leaderCommit: rf.commitIndex,
	}
	replys := make([]AppendEntriesReply, len(rf.peers))
	for idx := range rf.peers{
		if idx != rf.me{
			go rf.sendAppendEntries(idx, &args, &replys[idx])
		}
	}
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


func (rf *Raft) start_election() {
	rf.now_state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.got_vote_num = 1
	rf.election_timeout_start_time = time.Now()
	got_replied_num := 1
	got_replied_map := make(map[int]bool, len(rf.peers)) // use to avoid duplicate reply
	got_replied_map[rf.me] = true
	replys := make([]RequestVoteReply, len(rf.peers))
	args := RequestVoteArgs{
		term: rf.currentTerm,
		candidateId: rf.me,
		lastLogIndex: rf.lastApplied,
		lastLogTerm: rf.log[rf.lastApplied].term,
	}
	finish_vote_reply := make(chan int, len(rf.peers) * 2)
	defer close(finish_vote_reply)

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go rf.sendRequestVote(idx, &args, &replys[idx], finish_vote_reply)
	} 

	// deal with the replys
	has_finish := false
	for  !has_finish && !rf.killed(){
		replied_server := -1
		select {
			case replied_server = <-finish_vote_reply:
				reply := replys[replied_server]
				// if reply.term > rf.currentterm, become the follower and still deal with others?
				if !rf.UpdateTerm(reply.term) {
					if got_replied_map[replied_server] {
						continue
					} else{
						got_replied_map[replied_server] = true
						got_replied_num += 1
						if got_replied_num == len(rf.peers){
							has_finish = true
						}
						if reply.voteGranted{
							rf.got_vote_num += 1
							if rf.got_vote_num * 2 > len(rf.peers) {
								rf.now_state = Leader
								has_finish = true
								// todo: send a heartbeat
							}
						}
					}	
				}
			default:
				// do nothing
		}
	}
}

func (rf *Raft) ticker() {
	random_election_timeout := ELECTION_TIMEOUT_MIN + rand.Intn(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN)
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.
		_, is_leader := rf.GetState()

		if !is_leader {
			if time.Since(rf.election_timeout_start_time) > time.Duration(random_election_timeout) * time.Millisecond {
				go rf.start_election()
			}
		} else {
			if time.Since()
			rf.send_heartbeat()
		}
		random_election_timeout =  ELECTION_TIMEOUT_MIN + rand.Intn(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN)
		time.Sleep(time.Duration(ELECTION_TICKER_INTERVAL) * time.Millisecond)
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
