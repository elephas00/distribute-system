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
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	FOR_LOOP_FIXED_DELAY_MILLISECONDS                           = 20
	LEADER_HEARTBEATS_FIXED_DELAY_MILLISECONDS                  = 100
	FOLLOWER_TIME_OUT_HALF_AVERAGE_DELAY_MILLISECONDS           = 150
	CANDIDATE_ELECTION_TIME_OUT_HALF_AVERAGE_DELAY_MILLISECONDS = 120
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
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
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
	commitIndex         int
	lastApplied         int
	appliedMsgCacheChan chan ApplyMsg
}

type PersistentState struct {
	term              int
	votedFor          int
	logs              []Log
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
}

type Log struct {
	Index   int
	Term    int
	Command interface{}
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

type AppendEntriesArgs struct {
	Term         int   // leader’s term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat;may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term              int  // currentTerm, for leader to update itself
	Success           bool // true if follower contained entry matching
	MostProbableIndex int
	MostProbableTerm  int
}

// for 2D
type InstallSnapshotArgs struct {
	Term              int // leader's term
	LeaderId          int // so follower can redirect clients
	LastIncludedIndex int // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int
	Data              []byte
	Entries           []Log
}

type InstallSnapshotReply struct {
	Term int
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// log.Printf("server %d made\n", me)
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
	sentinel := Log{Term: 0}
	rf.persistState.logs = []Log{sentinel}
	rf.volatileState.appliedMsgCacheChan = make(chan ApplyMsg, 10000)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.persistState.lastIncludedIndex > 0 {
		snapshot := rf.persister.ReadSnapshot()
		rf.persistState.snapshot = snapshot
		msg := ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      snapshot,
			SnapshotIndex: rf.persistState.lastIncludedIndex,
			SnapshotTerm:  rf.persistState.lastIncludedTerm,
		}
		rf.volatileState.appliedMsgCacheChan <- msg
	}

	// rf.printServerStartInfo()

	// start ticker goroutine to start elections
	go func() {
		rf.electionTimeoutTicker()
	}()

	go func() {
		rf.leaderAppendEntriesTicker()
	}()

	go func() {
		rf.msgApplier()
	}()
	return rf
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
	// ok := rf.mu.TryLock()
	// if !ok {
	// 	return 0, 0, false
	// }
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.persistState.term
	isLeader := rf.serverState == SERVER_STATE_LEADER
	index := rf.getLastLogIndex() + 1
	if isLeader {
		newLog := Log{
			Index:   index,
			Term:    term,
			Command: command,
		}
		rf.persistState.logs = append(rf.persistState.logs, newLog)
		rf.persist()
		go rf.sendAppendEntriesToFollowers()
		// log.Printf("%s %d (term %d) receive a new Log (index %d), detail: %v \n", serverStates[rf.serverState], rf.me, rf.persistState.term, index, newLog)
	}
	return index, term, isLeader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term := rf.persistState.term
	isleader := rf.serverState == SERVER_STATE_LEADER
	return term, isleader
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.persistState.term
	reply.Term = term
	// if there are server with higher term, convert current server to follower state
	if args.Term > term {
		rf.persistState.term = args.Term
		rf.convertToFollower()
		rf.persist()
	}

	if args.Term < term {
		reply.VoteGranted = false
		// log.Printf("server %d (term %d, %s) reject to vote candidate %d (term %d), because the candidate's term not large than current server", rf.me, rf.persistState.term, serverStates[rf.serverState], args.CandidateId, args.Term)
	} else if rf.serverState == SERVER_STATE_LEADER || rf.serverState == SERVER_STATE_CANDIDATE || rf.persistState.votedFor != NO_LEADER {
		reply.VoteGranted = false
	} else if rf.getLog(rf.getLastLogIndex()).Term > args.LastLogTerm || (rf.getLog(rf.getLastLogIndex()).Term == args.LastLogTerm && rf.getLastLogIndex() > args.LastLogIndex) {
		// candidate is not at least-up-to-date
		// log.Printf("server %d (term %d, %s) reject to vote candidate %d (term %d), because the candidate is not up to date", rf.me, rf.persistState.term, serverStates[rf.serverState], args.CandidateId, args.Term)
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		rf.volatileFollowerState.previosuHeartBeat = time.Now()
		rf.persistState.votedFor = args.CandidateId
		rf.persist()
		// log.Printf("server %d (term %d, %s) vote candidate %d (term %d)", rf.me, rf.persistState.term, serverStates[rf.serverState],args.CandidateId, args.Term)
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("%s %d (term %d) receive leader %d (term %d) heartbeat,\t args: %+v\n", serverStates[rf.serverState], rf.me, rf.persistState.term, args.LeaderId, args.Term, args)

	term := rf.persistState.term
	reply.Term = term
	// if discover a server with higher term, convert current server to follower state]
	// figure 2, append entries 1
	// Reply false if term < currentTerm
	if args.Term < term {
		reply.Success = false
		return
	}
	// if discover a server with higher term, convert to follower immediately
	if args.Term > term {
		rf.persistState.term = args.Term
		rf.convertToFollower()
		rf.persist()
	}
	// do some thing after receive heart beat.
	rf.volatileFollowerState.previosuHeartBeat = time.Now()
	if rf.leader != args.LeaderId {
		rf.leader = args.LeaderId
	}
	if rf.serverState != SERVER_STATE_FOLLOWER {
		rf.serverState = SERVER_STATE_FOLLOWER
	}
	// figure 2, append entries 2
	// reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if rf.notContainsMatchPrevLog(args) {
		reply.Success = false
		rf.volatileFollowerState.previosuHeartBeat = time.Now()
		rf.leader = args.LeaderId
		rf.serverState = SERVER_STATE_FOLLOWER
		rf.writeMostProbableInfo(args, reply)
		return
	}
	reply.Success = true
	// just a heart beat
	if len(args.Entries) > 0 {
		// figure 2, append entries 3
		// delete conflict logs if exists
		rf.deleteConflictEntriesIfExist(args)
		// figure 2, append entries 4
		// receive not already exists log
		rf.receiveNotExistEntries(args)
	}
	// figure 2, append entries 5
	// if leader commit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.volatileState.commitIndex {
		rf.updateCommit(args)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	if rf.killed() {
		return
	}
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("%s %d, term %d, called snapshot(), index %d, oldlastIndex %d \n", serverStates[rf.serverState], rf.me, rf.persistState.term, index, rf.persistState.lastIncludedIndex)
	if index < rf.persistState.lastIncludedIndex || index > rf.getLastLogIndex() {
		message := "warning: %s:%d, term:%d, trying to access index:%d, lastIncludedInex:%d, commit:%d\n"
		log.Printf(message, serverStates[rf.serverState], rf.me, rf.persistState.term, index, rf.persistState.lastIncludedIndex, rf.volatileState.commitIndex)
		return
	}
	lastIndexInLog := rf.toRelativeIndex(index)
	rf.persistState.logs = rf.persistState.logs[lastIndexInLog:]
	newLogs := make([]Log, len(rf.persistState.logs))
	copy(newLogs, rf.persistState.logs)
	rf.persistState.logs = newLogs
	rf.persistState.lastIncludedIndex = index
	rf.persistState.lastIncludedTerm = rf.persistState.logs[0].Term
	rf.persistState.snapshot = snapshot
	if rf.volatileState.commitIndex < index {
		rf.volatileState.commitIndex = index
	}
	if rf.volatileState.lastApplied < index {
		rf.volatileState.lastApplied = index
	}
	if rf.serverState == SERVER_STATE_LEADER {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.volatileLeaderState.matchIndex[i] < index {
				rf.volatileLeaderState.matchIndex[i] = index
			}
			if rf.volatileLeaderState.nextIndex[i] <= index {
				rf.volatileLeaderState.nextIndex[i] = index + 1
			}
		}
	}
	// msg := ApplyMsg{
	// 	SnapshotValid: true,
	// 	Snapshot:      snapshot,
	// 	SnapshotIndex: rf.persistState.lastIncludedIndex,
	// 	SnapshotTerm:  rf.persistState.lastIncludedTerm,
	// }
	// rf.volatileState.appliedMsgCacheChan <- msg

	rf.persist()
	// log.Printf("%s %d, term %d, after snapshot() \n", serverStates[rf.serverState], rf.me, rf.persistState.term)
	// rf.printCommitLogs()

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("%s %d, term %d, called install snapshot(), leader is %d, term is %d \n", serverStates[rf.serverState], rf.me, rf.persistState.term, args.LeaderId, args.Term)
	// 1. Reply immediately if term < currentTerm
	currentTerm := rf.persistState.term
	reply.Term = currentTerm
	if args.Term < currentTerm {
		return
	}

	if args.Term > rf.persistState.term {
		rf.persistState.term = args.Term
	}
	rf.persistState.logs = args.Entries
	rf.persistState.lastIncludedIndex = args.LastIncludedIndex
	rf.persistState.lastIncludedTerm = args.LastIncludedTerm
	rf.persistState.snapshot = args.Data
	msg := ApplyMsg{
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
		Snapshot:      args.Data,
	}
	rf.volatileState.appliedMsgCacheChan <- msg
	rf.volatileState.lastApplied = args.LastIncludedIndex
	rf.volatileState.commitIndex = args.LastIncludedIndex
	rf.persist()
	log.Printf("%s %d, term %d, after called install snapshot(), leader is %d, term is %d \n", serverStates[rf.serverState], rf.me, rf.persistState.term, args.LeaderId, args.Term)
	rf.printCommitLogs()
}

func (rf *Raft) updateCommit(args *AppendEntriesArgs) {
	lastestEntryIndex := args.PrevLogIndex + len(args.Entries)
	min := lastestEntryIndex
	if min > args.LeaderCommit {
		min = args.LeaderCommit
	}
	if min > rf.volatileState.commitIndex {
		rf.volatileState.commitIndex = min
		go rf.applyMsg()
	}
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.persistState.term)
	e.Encode(rf.persistState.votedFor)
	e.Encode(rf.persistState.logs)
	e.Encode(rf.persistState.lastIncludedIndex)
	e.Encode(rf.persistState.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persistState.snapshot)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term, votedFor, lastLogIncludedIndex, lastLogIncludedTerm int
	var logs []Log
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastLogIncludedIndex) != nil ||
		d.Decode(&lastLogIncludedTerm) != nil {
		log.Fatal("read persist failed")
	} else {
		rf.persistState.term = term
		rf.persistState.votedFor = votedFor
		rf.persistState.logs = logs
		rf.persistState.lastIncludedIndex = lastLogIncludedIndex
		rf.persistState.lastIncludedTerm = lastLogIncludedTerm
		rf.volatileState.commitIndex = rf.persistState.lastIncludedIndex
		rf.volatileState.lastApplied = rf.persistState.lastIncludedIndex
	}
	// log.Printf("MARK 9")
}

func (rf *Raft) toAbsoluteIndex(index int) int {
	return rf.persistState.lastIncludedIndex + index
}

func (rf *Raft) getLog(absoluteIndex int) Log {
	if absoluteIndex < rf.persistState.lastIncludedIndex {
		msg := "%s %d, term %d, failed to access global log index %d, local log index is %d, lastIncludedIndex %d"
		fail := fmt.Sprintf(msg, serverStates[rf.serverState], rf.me, rf.persistState.term, absoluteIndex, absoluteIndex, rf.persistState.lastIncludedIndex)
		log.Println(fail)
	}
	localIndex := rf.toRelativeIndex(absoluteIndex)
	return rf.persistState.logs[localIndex]
}

func (rf *Raft) toRelativeIndex(index int) int {
	relativeIndex := index - rf.persistState.lastIncludedIndex
	if relativeIndex < 0 {
		message := "%s:%d, term:%d, failed to access index:%d, lastIncludedInex:%d\n"
		log.Printf(message, serverStates[rf.serverState], rf.me, index, rf.persistState.term, rf.persistState.lastIncludedIndex)
	}
	return relativeIndex
}

func (rf *Raft) convertToFollower() {
	rf.serverState = SERVER_STATE_FOLLOWER
	rf.persistState.votedFor = NO_LEADER
}

func (rf *Raft) convertToCandidate() {
	rf.serverState = SERVER_STATE_CANDIDATE
}

func (rf *Raft) convertToLeader() {
	// initialize volatile leader info
	rf.serverState = SERVER_STATE_LEADER
	lastLogIndex := rf.getLastLogIndex()
	peersNum := len(rf.peers)
	rf.volatileLeaderState.nextIndex = make([]int, peersNum)
	rf.volatileLeaderState.matchIndex = make([]int, peersNum)
	for idx := 0; idx < peersNum; idx++ {
		rf.volatileLeaderState.nextIndex[idx] = lastLogIndex + 1
		rf.volatileLeaderState.matchIndex[idx] = rf.persistState.lastIncludedIndex
	}
}

func (rf *Raft) writeMostProbableInfo(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	min := rf.getLastLogIndex()
	if min > args.PrevLogIndex-1 {
		min = args.PrevLogIndex - 1
	}
	for i := min; i >= rf.persistState.lastIncludedIndex; i-- {
		if rf.getLog(i).Term <= args.PrevLogTerm {
			reply.MostProbableIndex = i
			for j := i - 1; j >= rf.persistState.lastIncludedIndex; j-- {
				if rf.getLog(j).Term == rf.getLog(i).Term {
					reply.MostProbableIndex = j
				}
			}
			break
		}
	}
	if rf.persistState.lastIncludedIndex > reply.MostProbableIndex {
		reply.MostProbableIndex = rf.persistState.lastIncludedIndex
		reply.MostProbableTerm = rf.persistState.lastIncludedTerm
	} else {
		reply.MostProbableTerm = rf.getLog(reply.MostProbableIndex).Term
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
	rf.persistState.votedFor = rf.me
	rf.volatileFollowerState.previosuHeartBeat = time.Now()
	rf.persist()
	rf.mu.Unlock()

	// vote the server it self.
	getVotes := 1
	heartBeatSended := false
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.doRequestVote(i, &getVotes, &heartBeatSended, term, RPC_FAIL_MAX_RETRY)
	}

}

func (rf *Raft) doRequestVote(server int, getVotes *int, heartBeatSended *bool, term int, retry int) {
	rf.mu.RLock()
	if rf.persistState.term != term {
		rf.mu.RUnlock()
		return
	}
	peersNum := len(rf.peers)
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLog(lastLogIndex).Term
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	reply := RequestVoteReply{}
	rf.mu.RUnlock()

	ok := rf.sendRequestVote(server, &args, &reply)

	if !ok {
		if retry > 1 {
			time.Sleep(time.Millisecond * FOR_LOOP_FIXED_DELAY_MILLISECONDS)
			rf.doRequestVote(server, getVotes, heartBeatSended, term, retry-1)
		}
		return
	}
	// log.Printf("server %d (term %d, %s) received vote from server %d (term %d), result is %v", rf.me, rf.persistState.term, serverStates[rf.serverState], server, reply.Term, reply.VoteGranted)
	rf.mu.Lock()
	if reply.Term > rf.persistState.term {
		rf.persistState.term = reply.Term
		rf.convertToFollower()
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if term == rf.persistState.term && reply.VoteGranted && rf.serverState == SERVER_STATE_CANDIDATE {
		*getVotes = (*getVotes + 1)
		if *getVotes > peersNum/2 && !*heartBeatSended {
			// log.Printf("server %d get %d votes and win the election on term %d , now begin send heartbeat\n", rf.me, *getVotes, rf.persistState.term)
			*heartBeatSended = true
			rf.convertToLeader()
			go rf.sendAppendEntriesToFollowers()
			// noOp := Log{
			// 	Term:    rf.persistState.term,
			// 	Index:   rf.getLastLogIndex() + 1,
			// 	Command: nil,
			// }
			// rf.persistState.logs = append(rf.persistState.logs, noOp)
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
		if rf.getLog(commitIndex).Term != rf.persistState.term {
			break
		}
		// the log at least exists in the leader
		cnt := 1
		for _, matchIdx := range rf.volatileLeaderState.matchIndex {
			if matchIdx >= commitIndex {
				cnt++
			}
		}
		if cnt > peersNum/2 {
			rf.volatileState.commitIndex = commitIndex
			// log.Printf("%s %d, term %d, commit %d \n", serverStates[rf.serverState], rf.me, rf.persistState.term, rf.volatileState.commitIndex)
			// apply commited logs to state machine
			go rf.applyMsg()
			break
		}
	}
}

func (rf *Raft) applyMsg() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// log.Printf("%s %d, term %d, MARK 7-1", serverStates[rf.serverState], rf.me, rf.persistState.term)
	lastApplied := rf.volatileState.lastApplied
	commitIndex := rf.volatileState.commitIndex
	lastLogIndex := rf.getLastLogIndex()

	for pos := lastApplied + 1; pos <= commitIndex && pos <= lastLogIndex; pos++ {
		msg := ApplyMsg{
			Command:      rf.getLog(pos).Command,
			CommandIndex: pos,
			CommandValid: true,
		}
		rf.volatileState.appliedMsgCacheChan <- msg
		rf.volatileState.lastApplied = pos
		// log.Printf("server %d (term %d) commit {index:%d}, commitIndex: %v\n", rf.me, rf.persistState.term, msg.CommandIndex, rf.volatileState.commitIndex)
	}
}

func (rf *Raft) prepareAppendEntriesArgs(args *AppendEntriesArgs, server int) {
	lastLogIndex := rf.getLastLogIndex()
	nextIdx := rf.volatileLeaderState.nextIndex[server]
	args.LeaderId = rf.me
	args.LeaderCommit = rf.volatileState.commitIndex
	args.Term = rf.persistState.term

	if nextIdx > lastLogIndex {
		// heart beat, nothing to send
		args.PrevLogIndex = lastLogIndex
		args.PrevLogTerm = rf.getLog(lastLogIndex).Term
		args.Entries = []Log{}

		// log.Printf("branch1, next index %d, last log index %d, leader logs: %v, args: %+v\n",nextIdx, lastLogIdx, rf.persistState.logs, args)
	} else if nextIdx > rf.persistState.lastIncludedIndex {
		// there are new logs send to followers
		prevLogIndex := nextIdx - 1
		args.PrevLogIndex = prevLogIndex
		localPrevLogIndex := rf.toRelativeIndex(prevLogIndex)
		args.PrevLogTerm = rf.getLog(args.PrevLogIndex).Term
		entries := rf.persistState.logs[localPrevLogIndex+1:]
		args.Entries = entries
		// log.Printf("branch2 next index %d, last log index %d, leader logs: %v, args: %+v\n",nextIdx, lastLogIdx, rf.persistState.logs, args)
	} else {
		// in this situation, leader should send install snapshot request to follower, so just send all and wait for failure handle
		// try to send something current leader contains
		args.PrevLogIndex = rf.persistState.lastIncludedIndex
		args.PrevLogTerm = rf.getLog(args.PrevLogIndex).Term
		args.Entries = rf.persistState.logs[1:]
		// log.Printf("leader call append entries, args: %+v", args)
	}
}

func (rf *Raft) sendAppendEntriesToFollowers() {
	rf.mu.RLock()
	if rf.serverState != SERVER_STATE_LEADER {
		rf.mu.RUnlock()
		return
	}
	term := rf.persistState.term
	peersNum := len(rf.peers)
	me := rf.me
	rf.mu.RUnlock()
	// respNum := 0
	for i := 0; i < peersNum; i++ {
		// send heart beats.
		if i == me {
			continue
		}
		go rf.doAppendEntries(i, term, RPC_FAIL_MAX_RETRY)
	}
	// log.Printf("server %d (term %d) heart beat, receive %d responds\n", rf.me, rf.persistState.term, respNum)

}

func (rf *Raft) doAppendEntries(server int, term int, retry int) {
	// if last log index >= nextIndex, do below
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}

	rf.mu.RLock()
	if rf.persistState.term != term {
		rf.mu.RUnlock()
		return
	}
	rf.prepareAppendEntriesArgs(&args, server)
	rf.mu.RUnlock()

	// log.Printf("leader %d (term %d, commit %d) send append entries to server %d \n", args.LeaderId, args.Term, args.LeaderCommit, server)
	ok := rf.sendAppendEntries(server, &args, &reply)

	if !ok {
		// log.Printf("APPEND ENTRIES RPC FAILED, server: %d, args:%+v, \n", server, args)
		if retry > 1 {
			time.Sleep(time.Millisecond * FOR_LOOP_FIXED_DELAY_MILLISECONDS)
			rf.doAppendEntries(server, term, retry-1)
		}
		return
	}
	// process response
	rf.mu.Lock()
	if reply.Term > rf.persistState.term {
		rf.persistState.term = reply.Term
		rf.convertToFollower()
		rf.persist()
	}
	if rf.persistState.term != term {
		rf.mu.Unlock()
		return
	}
	if rf.persistState.term == args.Term && reply.Success {
		rf.processAppendEntriesSuccessReply(&args, &reply, server)
	} else {
		// log.Printf("leader %d (term %d, commit %d) send append entries to server %d failed, logs: %v, args:%+v \n", args.LeaderId, args.Term, args.LeaderCommit, server, rf.persistState.logs, args)
		rf.handleAppendEntriesFailure(&reply, server)
		if retry > 1 {
			go func(server int, term int, retry int) {
				time.Sleep(time.Millisecond * FOR_LOOP_FIXED_DELAY_MILLISECONDS)
				rf.doAppendEntries(server, term, retry-1)
			}(server, term, retry)
		}
	}
	rf.mu.Unlock()
	// log.Printf("retry\n")
}

func (rf *Raft) msgApplier() {
	for {
		msg := <-rf.volatileState.appliedMsgCacheChan
		if msg.CommandValid && msg.CommandIndex < rf.persistState.lastIncludedIndex || msg.CommandIndex > rf.getLastLogIndex() {
			message := "warning: %s %d, term:%d, try to send a illegal message %+v\n"
			log.Printf(message, serverStates[rf.serverState], rf.me, rf.persistState.term, msg)
		}
		// if msg.SnapshotValid {
		// 	rf.mu.Lock()
		// 	rf.volatileState.appliedMsgCacheChan = make(chan ApplyMsg, 20)
		// 	for _, log := range rf.persistState.logs {
		// 		if log.Index > msg.SnapshotIndex && log.Index <= rf.volatileState.commitIndex {
		// 			rf.volatileState.appliedMsgCacheChan <- ApplyMsg{
		// 				CommandIndex: log.Index,
		// 				Command:      log.Command,
		// 				CommandValid: true,
		// 			}
		// 		} else {
		// 			break
		// 		}
		// 	}
		// 	rf.mu.Unlock()
		// }
		rf.applyCh <- msg

	}
}

func (rf *Raft) electionTimeoutTicker() {
	for !rf.killed() {

		beforeSleep := time.Now()
		rf.mu.RLock()
		// log.Printf("%s %d, term %d ELECTION 1\n", serverStates[rf.serverState], rf.me, rf.persistState.term)
		if rf.serverState == SERVER_STATE_LEADER {
			rf.mu.RUnlock()
			time.Sleep(time.Millisecond * LEADER_HEARTBEATS_FIXED_DELAY_MILLISECONDS)
		} else if rf.serverState == SERVER_STATE_CANDIDATE {
			ms := CANDIDATE_ELECTION_TIME_OUT_HALF_AVERAGE_DELAY_MILLISECONDS + (rand.Int63() % CANDIDATE_ELECTION_TIME_OUT_HALF_AVERAGE_DELAY_MILLISECONDS)
			sleepTime := time.Duration(ms) * time.Millisecond

			rf.mu.RUnlock()
			time.Sleep(sleepTime)
		} else {
			ms := FOLLOWER_TIME_OUT_HALF_AVERAGE_DELAY_MILLISECONDS + (rand.Int63() % FOLLOWER_TIME_OUT_HALF_AVERAGE_DELAY_MILLISECONDS)
			sleepTime := time.Duration(ms) * time.Millisecond
			rf.mu.RUnlock()
			time.Sleep(sleepTime)
		}

		rf.mu.Lock()
		// log.Printf("%s %d, term %d ELECTION 2\n", serverStates[rf.serverState], rf.me, rf.persistState.term)

		if rf.serverState == SERVER_STATE_CANDIDATE {
			go func() {
				rf.callSendRequestVote()
			}()
		}

		if rf.serverState == SERVER_STATE_FOLLOWER && rf.volatileFollowerState.previosuHeartBeat.Before(beforeSleep) {
			rf.convertToCandidate()
			go func() {
				rf.callSendRequestVote()
			}()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) leaderAppendEntriesTicker() {
	for !rf.killed() {
		rf.mu.RLock()
		if rf.serverState == SERVER_STATE_LEADER {
			go func() {
				rf.sendAppendEntriesToFollowers()
			}()
		}
		// log.Printf("%s %d, term %d LEADER APPEND ENTRIES\n", serverStates[rf.serverState], rf.me, rf.persistState.term)
		rf.mu.RUnlock()

		time.Sleep(time.Millisecond * LEADER_HEARTBEATS_FIXED_DELAY_MILLISECONDS)
	}
}

func (rf *Raft) doInstallSnapshot(server int, term int, retry int) {
	rf.mu.Lock()
	if term != rf.persistState.term {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.persistState.lastIncludedIndex,
		LastIncludedTerm:  rf.persistState.lastIncludedTerm,
		Data:              rf.persistState.snapshot,
		Entries:           rf.persistState.logs,
	}
	lastIndex := rf.getLastLogIndex()
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	ok := rf.sendInstallSnapshot(server, &args, &reply)
	if !ok {
		// log.Printf("APPEND ENTRIES RPC FAILED, server: %d, args:%+v, \n", server, args)
		if retry > 1 {
			time.Sleep(time.Millisecond * FOR_LOOP_FIXED_DELAY_MILLISECONDS)
			rf.doInstallSnapshot(server, term, retry-1)
		}
		return
	}

	rf.mu.Lock()
	if reply.Term > rf.persistState.term {
		rf.persistState.term = reply.Term
		rf.convertToFollower()
		rf.persist()
	} else {
		rf.volatileLeaderState.nextIndex[server] = lastIndex + 1
		rf.volatileLeaderState.matchIndex[server] = lastIndex
	}
	rf.mu.Unlock()
}

func (rf *Raft) handleAppendEntriesFailure(reply *AppendEntriesReply, server int) {

	min := rf.getLastLogIndex()
	if reply.MostProbableIndex < min {
		min = reply.MostProbableIndex
	}
	if rf.volatileLeaderState.nextIndex[server]-2 < min {
		min = rf.volatileLeaderState.nextIndex[server] - 2
	}
	if min <= 0 {
		rf.volatileLeaderState.nextIndex[server] = 1
		rf.volatileLeaderState.matchIndex[server] = 0
		return
	}
	if min < rf.persistState.lastIncludedIndex {
		go rf.doInstallSnapshot(server, rf.persistState.term, RPC_FAIL_MAX_RETRY)
		return
	}
	if min <= rf.volatileLeaderState.matchIndex[server] {
		rf.volatileLeaderState.nextIndex[server] = rf.volatileLeaderState.matchIndex[server] + 1
		return
	}

	for i := min; i >= rf.volatileLeaderState.matchIndex[server]; i-- {
		if rf.getLog(i).Term <= reply.MostProbableTerm {
			rf.volatileLeaderState.nextIndex[server] = i + 1
			for j := min - 1; j >= rf.volatileLeaderState.matchIndex[server]; j-- {
				if rf.getLog(j).Term == rf.getLog(i).Term {
					rf.volatileLeaderState.nextIndex[server] = j + 1
				}
			}
			return
		}
	}
	go rf.doInstallSnapshot(server, rf.persistState.term, RPC_FAIL_MAX_RETRY)
}

func (rf *Raft) getLastLogIndex() int {
	return rf.persistState.lastIncludedIndex + len(rf.persistState.logs) - 1
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	// n := len(rf.peers)
	// log.Printf("send append entries called server:%d, total server num: %d \n", server, n)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) printServerStartInfo() {
	log.Println()
	log.Printf("------------------------------")
	log.Printf("server %d start, state: {term: %d, voteFor: %d}", rf.me, rf.persistState.term, rf.persistState.votedFor)
	rf.printCommitLogs()
	log.Printf("-------------------------------")
	log.Println()
}

func (rf *Raft) deleteConflictEntriesIfExist(args *AppendEntriesArgs) {
	if args.PrevLogIndex < rf.persistState.lastIncludedIndex {
		i := rf.persistState.lastIncludedIndex + 1
		j := rf.persistState.lastIncludedIndex - args.PrevLogIndex
		lastEntriesIndex := len(args.Entries) - 1
		for i <= rf.getLastLogIndex() && j <= lastEntriesIndex {
			if rf.getLog(i).Term != args.Entries[j].Term {
				rf.persistState.logs = rf.persistState.logs[:rf.toRelativeIndex(i)]
				break
			}
			i++
			j++
		}
	} else {
		i := args.PrevLogIndex + 1
		j := 0
		lastEntriesIndex := len(args.Entries) - 1
		for i <= rf.getLastLogIndex() && j <= lastEntriesIndex {
			if rf.getLog(i).Term != args.Entries[j].Term {
				rf.persistState.logs = rf.persistState.logs[:rf.toRelativeIndex(i)]
				break
			}
			i++
			j++
		}
	}
}

func (rf *Raft) receiveNotExistEntries(args *AppendEntriesArgs) {
	newLastLogIndex := args.PrevLogIndex + len(args.Entries)
	if newLastLogIndex <= rf.getLastLogIndex() {
		return
	}
	appendEntries := args.Entries[rf.getLastLogIndex()-args.PrevLogIndex:]
	clone := make([]Log, len(appendEntries))
	copy(clone, appendEntries)
	rf.persistState.logs = append(rf.persistState.logs, clone...)
	rf.persist()
}

func (rf *Raft) notContainsMatchPrevLog(args *AppendEntriesArgs) bool {
	if rf.getLastLogIndex() < args.PrevLogIndex {
		return true
	}
	if args.PrevLogIndex >= rf.persistState.lastIncludedIndex && rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm {
		return true
	}
	return false
}

func (rf *Raft) printCommitLogs() {
	p := "%s %d (term: %d), commitIndex %d, lastIncluded %d, logs: ["
	message := fmt.Sprintf(p, serverStates[rf.serverState], rf.me, rf.persistState.term, rf.volatileState.commitIndex, rf.persistState.lastIncludedIndex)
	var builder strings.Builder
	builder.WriteString(message)
	logPattern := "%d:%d"
	for i := rf.persistState.lastIncludedIndex + 1; i <= rf.getLastLogIndex(); i++ {
		log := rf.getLog(i)
		if log.Index != i {
			fmt.Println("index mismatch")
		}
		if i != rf.persistState.lastIncludedIndex+1 {
			builder.WriteString(", ")
		}
		builder.WriteString(fmt.Sprintf(logPattern, log.Index, log.Term))
	}
	builder.WriteString("]")
	log.Println(builder.String())
}
