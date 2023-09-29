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
	"log"
	"math/rand"
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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
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
	lastApplid  int
}

type PersistentState struct {
	term     int
	votedFor int
	logs     []Log
}

type Term struct {
}

type Log struct {
	Term int
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
	reply.Term = rf.persistState.term
	if rf.persistState.term >= args.Term {
		reply.VoteGranted = false
		reply.Term = rf.persistState.term
		// log.Printf("server %d (term %d, %s) reject to vote candidate %d (term %d)", rf.me, rf.persistState.term, serverStates[rf.serverState], args.CandidateId, args.Term)
	} else if rf.persistState.logs[len(rf.persistState.logs) - 1].Term > args.LastLogTerm || (rf.persistState.logs[len(rf.persistState.logs) - 1].Term == args.LastLogTerm && len(rf.persistState.logs) > args.LastLogIndex) {
		// candidate is not at least-up-to-date
		reply.VoteGranted = false
		reply.Term = rf.persistState.term
	} else {
		rf.serverState = SERVER_STATE_FOLLOWER
		rf.volatileFollowerState.previosuHeartBeat = time.Now()
		rf.persistState.term = args.Term
		rf.persistState.votedFor = args.CandidateId
		reply.VoteGranted = true
		// log.Printf("server %d (term %d, %s) vote candidate %d (term %d)", rf.me, rf.persistState.term, serverStates[rf.serverState],args.CandidateId, args.Term)

	}
	rf.mu.Unlock()

}

func (rf *Raft) AppendEntires(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	logSize := len(rf.persistState.logs)
	if args.Term < rf.persistState.term {
		// log.Printf("server %d (term %d) reject leader %d (term %d) heartbeat,  \n", rf.me, rf.persistState.term, args.LeaderId, args.Term)
		reply.Success = false
	} else if logSize < args.PrevLogIndex + 1 || rf.persistState.logs[args.PrevLogIndex].Term != args.Term {
		reply.Success = false
	} else {
		// log.Printf("server %d (term %d) receive leader %d (term %d) heartbeat \n", rf.me, rf.persistState.term, args.LeaderId, args.Term)
		
		reply.Success = true
		// update state
		rf.persistState.term = args.Term
		rf.volatileFollowerState.previosuHeartBeat = time.Now()
		rf.leader = args.LeaderId
		rf.serverState = SERVER_STATE_FOLLOWER
		// update log
		if logSize == args.PrevLogIndex + 1 {
			rf.persistState.logs = append(rf.persistState.logs, args.Entries...)
		}else {
			newLogsNum := len(args.Entries)
			appendLogsNum := args.PrevLogIndex + 1 + newLogsNum - logSize
			overrideLogsNum := newLogsNum - appendLogsNum
			insertCount := 0
			// override logs
			for i := 0; i < overrideLogsNum; i++ {
				rf.persistState.logs[args.PrevLogIndex + 1 + insertCount] = args.Entries[insertCount]
				insertCount++
			}
			for i := 0; i < appendLogsNum; i++ {
				rf.persistState.logs = append(rf.persistState.logs, args.Entries[insertCount])
				insertCount++
			}
			if args.LeaderCommit > rf.volatileState.commitIndex {
				rf.volatileState.commitIndex = args.LeaderCommit
			}
		} 
	}
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
	defer rf.mu.Unlock()
	// Your code here (2B).
	if rf.serverState != SERVER_STATE_LEADER {
		isLeader = false
	}else {
		term = rf.persistState.term
		index = len(rf.persistState.logs)
		newLog := Log{
			Term: term,
		}
		rf.persistState.logs = append(rf.persistState.logs, newLog)
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

func (rf *Raft) callSendRequestVote() {
	rf.mu.Lock()
	rf.persistState.term++
	rf.volatileFollowerState.previosuHeartBeat = time.Now()
	peerNum := len(rf.peers)
	rf.mu.Unlock()

	// vote the server it self.
	getVotes := 1
	heartBeatSended := false
	for i := 0; i < peerNum; i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := RequestVoteArgs{
				Term:        rf.persistState.term,
				CandidateId: rf.me,
			}
			reply := RequestVoteReply{}
			rf.sendRequestVote(server, &args, &reply)
			// log.Printf("server %d (term %d, %s) received vote from server %d (term %d), result is %v", rf.me, rf.persistState.term, serverStates[rf.serverState], server, reply.Term, reply.VoteGranted)
			rf.mu.Lock()
			if reply.VoteGranted && rf.serverState == SERVER_STATE_CANDIDATE {
				getVotes++
				if getVotes > peerNum/2 && !heartBeatSended {
					// log.Printf("server %d get %d votes and win the election on term %d , now begin send heartbeat\n", rf.me, getVotes, rf.persistState.term)
					if rf.serverState == SERVER_STATE_CANDIDATE {
						rf.serverState = SERVER_STATE_LEADER
						rf.sendAppendEntriesToFollowers()
						heartBeatSended = true
					}
				}
			}
			rf.mu.Unlock()
		}(i)
	}

}

func (rf *Raft) sendAppendEntriesToFollowers() {
	peerNum := len(rf.peers)
	// respNum := 0
	for i := 0; i < peerNum; i++ {
		// send heart beats.
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:     rf.persistState.term,
				LeaderId: rf.me,
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(server, &args, &reply)
			// if reply.Success {
			// 	respNum++
			// }
		}(i)
	}
	// log.Printf("server %d (term %d) heart beat, receive %d responds\n", rf.me, rf.persistState.term, respNum)

}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		// log.Printf("node %d ticker triggered\n", rf.me)

		// pause for a random amount of time between 50 and 350
		// milliseconds.

		ms := 150 + (rand.Int63() % 150)
		// TODO: ms *= 10 is for debug, remove it
		sleepTime := time.Duration(ms) * time.Millisecond
		beforeSleep := time.Now()
		rf.mu.Lock()
		if rf.serverState == SERVER_STATE_LEADER {
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * time.Duration(100))
		} else if rf.serverState == SERVER_STATE_CANDIDATE {
			rf.mu.Unlock()
			time.Sleep(sleepTime)
		} else {
			rf.mu.Unlock()
			time.Sleep(sleepTime)
		}
		rf.mu.Lock()
		if rf.serverState == SERVER_STATE_LEADER {
			rf.sendAppendEntriesToFollowers()
		} else {
			if rf.serverState == SERVER_STATE_CANDIDATE {
				// log.Printf("server %d (term %d, %s) time out and found no leader in this term, new vote begin", rf.me, rf.persistState.term, serverStates[rf.serverState])
				go func() {
					rf.callSendRequestVote()
				}()
			} else if rf.serverState == SERVER_STATE_FOLLOWER {
				if rf.volatileFollowerState.previosuHeartBeat.After(beforeSleep) {

				} else {
					rf.serverState = SERVER_STATE_CANDIDATE
					// log.Printf("server %d (term %d, %s) time out, vote begin, previous heatbeat at %v  \n", rf.me, rf.persistState.term, serverStates[rf.serverState], rf.volatileFollowerState.previosuHeartBeat)
					go func() {
						rf.callSendRequestVote()
					}()
				}
			}
		}
		rf.mu.Unlock()

		// TODO: what will do if current server is leader?

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
