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
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	SERVER_STATE_FOLLOWER  = 0
	SERVER_STATE_CANDIDATE = 1
	SERVER_STATE_LEADER    = 2
)

const (
	RPC_FAIL_MAX_RETRY = 1
)

const (
	FOR_LOOP_FIXED_DELAY_MILLISECONDS = 20
	LEADER_HEARTBEATS_FIXED_DELAY_MILLISECONDS = 100
	FOLLOWER_TIME_OUT_HALF_AVERAGE_DELAY_MILLISECONDS = 150
)

const (
	DEFAULT_TERM_TIMES_OUT_MILLISECONDS            = 3000
	DEFAULT_LEADER_ELECTION_TIMES_OUT_MILLISECONDS = 500
	DEFAULT_CLUSTER_SERVER_NUMBERS                 = 5
)

var serverStates = map[int]string{
	SERVER_STATE_FOLLOWER:  "FOLLOWER",
	SERVER_STATE_CANDIDATE: "CANDIDATE",
	SERVER_STATE_LEADER:    "LEADER",
}

const (
	NO_LEADER = -1
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex         // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	leader                int
	serverState           int
	persistState          PersistentState
	volatileState         VolatileState
	volatileLeaderState   VolatileLeaderState
	volatileFollowerState VolatileFollowerState
	applyCh               chan ApplyMsg
}

type VolatileFollowerState struct {
	previosuHeartBeat time.Time
}

type VolatileLeaderState struct {
	nextIndex  []int
	matchIndex []int
}

type VolatileState struct {
	commitIndex int
	lastApplied int
}

type PersistentState struct {
	term     int
	votedFor int
	logs     []Log
}

type Term struct {
}

type Log struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int   // leader’s term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat;may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term         int  // currentTerm, for leader to update itself
	Success      bool // true if follower contained entry matching
	PrevLogIndex int  // and prevLogTerm
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.persistState.term
	isleader = rf.serverState == SERVER_STATE_LEADER
	rf.mu.Unlock()
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
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	term := rf.persistState.term
	reply.Term = term
	// if there are server with higher term, convert current server to follower state
	if args.Term > term {
		rf.serverState = SERVER_STATE_FOLLOWER
		rf.persistState.term = args.Term
		rf.persistState.votedFor = NO_LEADER
	}

	if args.Term < term {
		reply.VoteGranted = false
		// log.Printf("server %d (term %d, %s) reject to vote candidate %d (term %d), because the candidate's term not large than current server", rf.me, rf.persistState.term, serverStates[rf.serverState], args.CandidateId, args.Term)
	} else if rf.persistState.term == args.Term && rf.persistState.votedFor != NO_LEADER {
		reply.VoteGranted = false
	} else if rf.persistState.logs[len(rf.persistState.logs)-1].Term > args.LastLogTerm || (rf.persistState.logs[len(rf.persistState.logs)-1].Term == args.LastLogTerm && len(rf.persistState.logs)-1 > args.LastLogIndex) {
		// candidate is not at least-up-to-date
		// log.Printf("server %d (term %d, %s) reject to vote candidate %d (term %d), because the candidate is not up to date", rf.me, rf.persistState.term, serverStates[rf.serverState], args.CandidateId, args.Term)
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		rf.serverState = SERVER_STATE_FOLLOWER
		rf.volatileFollowerState.previosuHeartBeat = time.Now()
		rf.persistState.term = args.Term
		rf.persistState.votedFor = args.CandidateId
		// log.Printf("server %d (term %d, %s) vote candidate %d (term %d)", rf.me, rf.persistState.term, serverStates[rf.serverState],args.CandidateId, args.Term)
	}

	rf.mu.Unlock()

}

func (rf *Raft) AppendEntires(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("server %d (term %d) receive leader %d (term %d) heartbeat \n", rf.me, rf.persistState.term, args.LeaderId, args.Term)

	term := rf.persistState.term
	logSize := len(rf.persistState.logs)
	// if discover a server with higher term, convert current server to follower state
	if args.Term > term {
		rf.serverState = SERVER_STATE_FOLLOWER
		rf.persistState.term = args.Term
		rf.persistState.votedFor = NO_LEADER
		rf.volatileFollowerState.previosuHeartBeat = time.Now()
	}
	if args.Term < term {
		// log.Printf("server %d (term %d) reject leader %d (term %d) heartbeat,  \n", rf.me, rf.persistState.term, args.LeaderId, args.Term)
		reply.Success = false
	} else if logSize < args.PrevLogIndex + 1 || rf.persistState.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// the index of log entries are not match current follower, just receive heart beat, but reject log
		reply.Success = false
		rf.volatileFollowerState.previosuHeartBeat = time.Now()
		rf.leader = args.LeaderId
		rf.serverState = SERVER_STATE_FOLLOWER
		// log.Printf("server %d (term %d) logs: %v, reject heart beat %+v  \n", rf.me, rf.persistState.term, rf.persistState.logs, args)
	} else {
		reply.Success = true
		// update state
		rf.volatileFollowerState.previosuHeartBeat = time.Now()
		rf.leader = args.LeaderId
		rf.serverState = SERVER_STATE_FOLLOWER

		// there are new logs to receive
		if len(args.Entries) > 0 {
			// update log
			// override
			rf.receiveLogs(args)
		}

		// newLogSize := len(rf.persistState.logs)
		oldCommitIndex := rf.volatileState.commitIndex
		lastestEntryIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit > oldCommitIndex {
			// apply committed logs to state machine
			var min int
			if args.LeaderCommit > lastestEntryIndex {
				min = lastestEntryIndex
			} else {
				min = args.LeaderCommit
			}
			if min > oldCommitIndex {
				rf.volatileState.commitIndex = min
				rf.printCommitLogs()
				go rf.applyMsg()
			}
			 	
		}
	}
}


// deprecated, some bugs here.
func (rf *Raft) receiveEntries(args *AppendEntriesArgs) {
	lastLogIndex := len(rf.persistState.logs) - 1
	newEntriesSize := len(args.Entries)
	lastLogIndexAfterMerge := args.PrevLogIndex + newEntriesSize 
	// roll back applied logs
	if rf.volatileState.lastApplied > args.LeaderCommit {
		for i := args.LeaderCommit + 1; i <= rf.volatileState.lastApplied && i <= lastLogIndex; i++ {
			msg := ApplyMsg{
				CommandValid: false,
				CommandIndex: i,
				Command:      rf.persistState.logs[i],
			}
			rf.applyCh <- msg
		}
		rf.volatileState.lastApplied = args.LeaderCommit
		rf.volatileState.commitIndex = args.LeaderCommit
	}

	if lastLogIndex >= lastLogIndexAfterMerge {
		firstNotMatchIndex := -1
		for i, j := lastLogIndexAfterMerge, newEntriesSize - 1; i > args.PrevLogIndex;  {
			
			if rf.persistState.logs[i].Term != args.Entries[j].Term {
				firstNotMatchIndex = i
				
			}			
			i--
			j--
		}
		if firstNotMatchIndex != -1 && firstNotMatchIndex <= rf.volatileState.commitIndex {
			rf.volatileState.commitIndex = firstNotMatchIndex - 1
		}
		if firstNotMatchIndex != -1 && firstNotMatchIndex <= rf.volatileState.lastApplied {
			rf.volatileState.lastApplied = firstNotMatchIndex - 1
		}
		for i, j := args.PrevLogIndex + 1, 0; j < newEntriesSize;  {
			rf.persistState.logs[i] = args.Entries[j]
			i++
			j++
		}
	} else {
		firstNotMatchIndex := -1
		for i, j := lastLogIndex, lastLogIndex - args.PrevLogIndex - 1; j >= 0; {
			if rf.persistState.logs[i].Term != args.Entries[j].Term {
				firstNotMatchIndex = i
				msg := ApplyMsg{
					CommandValid: false,
					CommandIndex: i,
					Command:      rf.persistState.logs[i],
				}
				rf.applyCh <- msg
			}			
			i--
			j--
		} 	
		if firstNotMatchIndex != -1 && firstNotMatchIndex <= rf.volatileState.commitIndex {
			rf.volatileState.commitIndex = firstNotMatchIndex - 1
		}
		if firstNotMatchIndex != -1 && firstNotMatchIndex <= rf.volatileState.lastApplied {
			rf.volatileState.lastApplied = firstNotMatchIndex - 1
		}
		rf.persistState.logs = rf.persistState.logs[0 : args.PrevLogIndex + 1]
		rf.persistState.logs = append(rf.persistState.logs, args.Entries...)
	}
	
	
	if lastLogIndex > args.PrevLogIndex {
		cnt := 0
		for i := args.PrevLogIndex + 1 ; i < lastLogIndex && cnt < newEntriesSize ; i++{
			if rf.persistState.logs[i].Term != args.Entries[cnt].Term {
				oldMsg := ApplyMsg{
					CommandValid: false,
					CommandIndex: i,
					Command:      rf.persistState.logs[i],
				}
				rf.applyCh <- oldMsg
				rf.persistState.logs[i] = args.Entries[cnt]

				newMsg := ApplyMsg{
					CommandValid: true,
					CommandIndex: i,
					Command:      rf.persistState.logs[i],
				}
				rf.applyCh <- newMsg
			}
			cnt++
		}
		if cnt < newEntriesSize {
			rf.persistState.logs = append(rf.persistState.logs, args.Entries[cnt:]...)
		}
	}else{
		rf.persistState.logs = append(rf.persistState.logs, args.Entries...)
	}
}

func (rf *Raft) receiveLogs(args *AppendEntriesArgs) {
	// newLatestIndex := len(args.Entries) + args.PrevLogIndex
	// if rf.volatileState.commitIndex >= newLatestIndex {
	// 	return
	// }

	logSize := len(rf.persistState.logs)
	if logSize > args.PrevLogIndex + 1 {
		for pos := args.PrevLogIndex + 1; pos <= rf.volatileState.lastApplied; pos++ {
			msg := ApplyMsg{
				CommandValid: false,
				CommandIndex: pos,
				Command:      rf.persistState.logs[pos],
			}
			rf.applyCh <- msg
		}

		var min = rf.volatileState.lastApplied
		if min > args.LeaderCommit {
			min = args.LeaderCommit
		}
		if min > args.PrevLogIndex {
			min = args.PrevLogIndex
		}
		if min != rf.volatileState.lastApplied {
			lastestLogIndexAfterReceive := args.PrevLogIndex + len(args.Entries)
			message := "%s %d term %d, roll back triggerd, oldCommit %d, now commit %d, after receive log latest log index %d \n"
			log.Printf(message, serverStates[rf.serverState], rf.me, rf.persistState.term, rf.volatileState.commitIndex, min, lastestLogIndexAfterReceive)
			rf.volatileState.lastApplied = min
			rf.volatileState.commitIndex = min
		}
		rf.persistState.logs = rf.persistState.logs[0 : args.PrevLogIndex + 1]
	}
	// rf.printCommitLogs()
	// log.Printf("%s %d, term %d, commit %d, args: %+v \n", serverStates[rf.serverState], rf.me, rf.persistState.term, rf.volatileState.commitIndex, args)
	rf.persistState.logs = append(rf.persistState.logs, args.Entries...)	
	// rf.printCommitLogs()

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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	rf.mu.Lock()
	// Your code here (2B).
	if rf.serverState == SERVER_STATE_LEADER {
		term = rf.persistState.term
		index = len(rf.persistState.logs)
		newLog := Log{
			Term:    term,
			Command: command,
		}
		rf.persistState.logs = append(rf.persistState.logs, newLog)
		// log.Printf("%s %d (term %d) receive a new Log (index %d), detail: %v \n", serverStates[rf.serverState], rf.me, rf.persistState.term, len(rf.persistState.logs) - 1, newLog)
		go rf.sendAppendEntriesToFollowers()
		rf.printCommitLogs()	
	} else {
		isLeader = false
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

func (rf *Raft) callSendRequestVote() {
	rf.mu.Lock()
	rf.persistState.term++
	term := rf.persistState.term
	rf.mu.Unlock()

	// vote the server it self.
	getVotes := 1
	heartBeatSended := false
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.doRequestVote(i, &getVotes, &heartBeatSended, term)
	}

}

func (rf *Raft) doRequestVote(server int, getVotes *int, heartBeatSended *bool, term int) {
	rf.mu.Lock()
	if rf.persistState.term != term {
		rf.mu.Unlock()
		return
	}
	peersNum := len(rf.peers)
	lastLogIndex := len(rf.persistState.logs) - 1
	lastLogTerm := rf.persistState.logs[lastLogIndex].Term
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	reply := RequestVoteReply{}
	rf.mu.Unlock()

	ok := false
	for i := 0; !ok && i < RPC_FAIL_MAX_RETRY; i++ {
		ok = rf.sendRequestVote(server, &args, &reply)
		if !ok {
			time.Sleep(time.Millisecond * FOR_LOOP_FIXED_DELAY_MILLISECONDS)
		}
	}
	if !ok {
		return
	}
	// log.Printf("server %d (term %d, %s) received vote from server %d (term %d), result is %v", rf.me, rf.persistState.term, serverStates[rf.serverState], server, reply.Term, reply.VoteGranted)
	rf.mu.Lock()

	if reply.Term > rf.persistState.term {
		rf.persistState.term = reply.Term
		rf.serverState = SERVER_STATE_FOLLOWER
		rf.persistState.votedFor = NO_LEADER
		rf.mu.Unlock()
		return
	}

	if term == rf.persistState.term && reply.VoteGranted && rf.serverState == SERVER_STATE_CANDIDATE {
		*getVotes = (*getVotes + 1)
		if *getVotes > peersNum/2 && !*heartBeatSended {
			// log.Printf("server %d get %d votes and win the election on term %d , now begin send heartbeat\n", rf.me, *getVotes, rf.persistState.term)
			rf.serverState = SERVER_STATE_LEADER
			*heartBeatSended = true
			logsNum := lastLogIndex + 1
			// initialize volatile leader info
			rf.volatileLeaderState.nextIndex = make([]int, peersNum)
			rf.volatileLeaderState.matchIndex = make([]int, peersNum)
			for idx := 0; idx < peersNum; idx++ {
				rf.volatileLeaderState.nextIndex[idx] = logsNum
				rf.volatileLeaderState.matchIndex[idx] = 0
			}
			// send heart beat to followers
			go func() {
				time.Sleep(time.Millisecond * FOR_LOOP_FIXED_DELAY_MILLISECONDS)
				rf.sendAppendEntriesToFollowers()
				
				// the ticker may sleeping a little long, so the heart beat will not send timely as considered.
				time.Sleep(time.Millisecond * LEADER_HEARTBEATS_FIXED_DELAY_MILLISECONDS)
				rf.sendAppendEntriesToFollowers()
			}()
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) processAppendEntriesSuccessReply(args *AppendEntriesArgs, reply *AppendEntriesReply, server int) {
	prevLogIndex := args.PrevLogIndex
	peersNum := len(rf.peers)

	matchIndex := prevLogIndex + len(args.Entries)
	if matchIndex > rf.volatileLeaderState.matchIndex[server] {
		rf.volatileLeaderState.nextIndex[server] = matchIndex + 1
		rf.volatileLeaderState.matchIndex[server] = matchIndex
	}
	// update commits
	oldCommitIndex := rf.volatileState.commitIndex
	for commitIndex := matchIndex; commitIndex > oldCommitIndex; commitIndex-- {
		// a leader can only commit logs in its term
		if rf.persistState.logs[commitIndex].Term != rf.persistState.term {
			break
		}
		// the log at least exists in the leader
		cnt := 1
		for _, matchIdx := range rf.volatileLeaderState.matchIndex {
			if matchIdx >= commitIndex {
				cnt++
			}
		}
		if cnt > peersNum / 2 {
			rf.volatileState.commitIndex = commitIndex
			// log.Printf("%s %d, term %d, commit %d \n", serverStates[rf.serverState], rf.me, rf.persistState.term, rf.volatileState.commitIndex)
			// rf.printCommitLogs()
			// apply commited logs to state machine
			go rf.applyMsg()
		}
	}
}

func (rf *Raft) applyMsg() {

	rf.mu.Lock()
	lastApplied := rf.volatileState.lastApplied
	commitIndex := rf.volatileState.commitIndex
	for pos := lastApplied + 1; pos <= commitIndex; pos++ {
		msg := ApplyMsg{
			Command:      rf.persistState.logs[pos].Command,
			CommandIndex: pos,
			CommandValid: true,
		}
		rf.applyCh <- msg
		rf.volatileState.lastApplied = pos
		// log.Printf("server %d (term %d) commit {index:%d}, commitIndex: %v\n", rf.me, rf.persistState.term, msg.CommandIndex, rf.volatileState.commitIndex)
	}
	rf.mu.Unlock()
}

func (rf *Raft) prepareAppendEntriesArgs(args *AppendEntriesArgs, server int) {
	lastLogIdx := len(rf.persistState.logs) - 1
	nextIdx := rf.volatileLeaderState.nextIndex[server]

	args.LeaderId = rf.me
	args.LeaderCommit = rf.volatileState.commitIndex
	args.Term = rf.persistState.term

	if nextIdx > lastLogIdx {
		// heart beat, send empty log entry
		args.PrevLogIndex = lastLogIdx
		args.PrevLogTerm = rf.persistState.logs[lastLogIdx].Term
		args.Entries = []Log{}

		// log.Printf("branch1, next index %d, last log index %d, leader logs: %v, args: %+v\n",nextIdx, lastLogIdx, rf.persistState.logs, args)
	} else {
		// there are new logs send to followers
		prevLogIndex := nextIdx - 1
		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = rf.persistState.logs[prevLogIndex].Term
		entries := rf.persistState.logs[prevLogIndex+1:]
		args.Entries = entries
		// log.Printf("branch2 next index %d, last log index %d, leader logs: %v, args: %+v\n",nextIdx, lastLogIdx, rf.persistState.logs, args)
	}
}

func (rf *Raft) sendAppendEntriesToFollowers() {
	rf.mu.Lock()
	if rf.serverState != SERVER_STATE_LEADER {
		rf.mu.Unlock()
		return
	}
	term := rf.persistState.term
	peersNum := len(rf.peers)
	me := rf.me
	rf.mu.Unlock()
	// respNum := 0
	for i := 0; i < peersNum; i++ {
		// send heart beats.
		if i == me {
			continue
		}
		go rf.doAppendEntries(i, term)
	}
	// log.Printf("server %d (term %d) heart beat, receive %d responds\n", rf.me, rf.persistState.term, respNum)

}

func (rf *Raft) doAppendEntries(server int, term int) {
	// if last log index >= nextIndex, do below
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}

	rf.mu.Lock()
	if rf.persistState.term != term {
		rf.mu.Unlock()
		return
	}
	rf.prepareAppendEntriesArgs(&args, server)
	rf.mu.Unlock()
	// log.Printf("leader %d (term %d, commit %d) send append entries to server %d \n", args.LeaderId, args.Term, args.LeaderCommit, server)
	ok := false
	for i := 0; !ok && i < RPC_FAIL_MAX_RETRY; i++ {

		ok = rf.sendAppendEntries(server, &args, &reply)
		if !ok {
			time.Sleep(time.Millisecond * FOR_LOOP_FIXED_DELAY_MILLISECONDS)
		}
	}
	if !ok {
		// log.Printf("APPEND ENTRIES RPC FAILED, server: %d, args:%+v, \n", server, args)
		return
	}
	// process response
	rf.mu.Lock()
	if reply.Term > rf.persistState.term {
		rf.persistState.term = reply.Term
		rf.serverState = SERVER_STATE_FOLLOWER
		rf.persistState.votedFor = NO_LEADER
	}
	if rf.persistState.term != term {
		rf.mu.Unlock()
		return
	}
	if rf.persistState.term == args.Term && reply.Success {
		rf.processAppendEntriesSuccessReply(&args, &reply, server)
	} else {
		// log.Printf("leader %d (term %d, commit %d) send append entries to server %d failed, logs: %v, args:%+v \n", args.LeaderId, args.Term, args.LeaderCommit, server, rf.persistState.logs, args)
		diff := rf.volatileLeaderState.nextIndex[server] - rf.volatileLeaderState.matchIndex[server]
		if diff > 10 {
			delta := diff / 5
			rf.volatileLeaderState.nextIndex[server] -= delta
		} else if rf.volatileLeaderState.nextIndex[server] > 1 {
			rf.volatileLeaderState.nextIndex[server]--
		}
	}
	rf.mu.Unlock()
	// log.Printf("retry\n")
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		// log.Printf("node %d ticker triggered\n", rf.me)

		// pause for a random amount of time between 50 and 350
		// milliseconds.

		ms := FOLLOWER_TIME_OUT_HALF_AVERAGE_DELAY_MILLISECONDS + (rand.Int63() % FOLLOWER_TIME_OUT_HALF_AVERAGE_DELAY_MILLISECONDS)
		// TODO: ms *= 10 is for debug, remove it
		sleepTime := time.Duration(ms) * time.Millisecond
		beforeSleep := time.Now()
		rf.mu.Lock()
		if rf.serverState == SERVER_STATE_LEADER {
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * LEADER_HEARTBEATS_FIXED_DELAY_MILLISECONDS)
		} else if rf.serverState == SERVER_STATE_CANDIDATE {
			rf.mu.Unlock()
			time.Sleep(sleepTime)
		} else {
			rf.mu.Unlock()
			time.Sleep(sleepTime)
		}
		rf.mu.Lock()
		if rf.serverState == SERVER_STATE_LEADER {
			// rf.printCommitLogs()
			go func() {
				rf.sendAppendEntriesToFollowers()
			}()
		} else {
			if rf.serverState == SERVER_STATE_CANDIDATE {
				go func() {
					rf.callSendRequestVote()
				}()
			} else if rf.serverState == SERVER_STATE_FOLLOWER {
				if rf.volatileFollowerState.previosuHeartBeat.After(beforeSleep) {

				} else {
					rf.serverState = SERVER_STATE_CANDIDATE
					go func() {
						rf.callSendRequestVote()
					}()
				}
			}
		}
		rf.mu.Unlock()
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
	rf.applyCh = applyCh
	rf.leader = NO_LEADER
	rf.serverState = SERVER_STATE_FOLLOWER
	followerState := VolatileFollowerState{}
	rf.volatileFollowerState = followerState
	persistentState := PersistentState{
		term: 0,
	}
	rf.persistState = persistentState
	leaderState := VolatileLeaderState{}
	rf.volatileLeaderState = leaderState
	// initialize logs
	sentienl := Log{Term: 0}
	rf.persistState.logs = []Log{sentienl}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// log.Printf("make node %d \n ", me)
	// start ticker goroutine to start elections
	go func() {
		for {
			rf.ticker()
		}
	}()

	if 1 == 2 {
		log.Println()
	}

	return rf
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	// n := len(rf.peers)
	// log.Printf("send append entries called server:%d, total server num: %d \n", server, n)
	ok := rf.peers[server].Call("Raft.AppendEntires", args, reply)
	return ok
}

func (rf *Raft) printCommitLogs() {
	p := "%s %d (term: %d), commitIndex %d, logs: ["
	message := fmt.Sprintf(p, serverStates[rf.serverState], rf.me, rf.persistState.term, rf.volatileState.commitIndex)
	var builder strings.Builder
	builder.WriteString(message)
	logPattern := "%d:%d"
	for i := 1; i < len(rf.persistState.logs); i++ {
		log := rf.persistState.logs[i]
		if i != 1 {
			builder.WriteString(", ")
		}
		builder.WriteString(fmt.Sprintf(logPattern, i, log.Term))
	}
	builder.WriteString("]")
	log.Println(builder.String())
}
