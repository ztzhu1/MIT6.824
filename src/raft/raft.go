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
	"fmt"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"

	"6.824/assert"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

const (
	ElecTimeoutLowerBound = 320
	ElecTimeoutUpperBound = 470
	HeartBeat             = 100 * time.Millisecond
)

type PeerState int32

const (
	Follower PeerState = iota
	Candidate
	Leader
)

type Entry struct {
	Cmd      interface{}
	RecvTerm int32
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	entryMus  []sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int32
	state         PeerState
	votedFor      int32
	recvHeartBeat int32 // zero: false; non-zero: true
	log           map[int]Entry
	commitIndex   int
	lastApplied   int32
	nextIndex     []int
	matchIndex    []int
	applyCh       chan ApplyMsg
}

// ------ impl Raft ------

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = int(rf.getCurrentTerm())
	isleader = rf.isLeader()
	return term, isleader
}

func (rf *Raft) setPeerState(state PeerState) {
	atomic.StoreInt32((*int32)(&rf.state), int32(state))
}

func (rf *Raft) setVotedFor(votedFor int32) {
	atomic.StoreInt32(&rf.votedFor, votedFor)
}

func (rf *Raft) setCurrentTerm(term int32) {
	atomic.StoreInt32(&rf.currentTerm, term)
}

func (rf *Raft) setHeartBeat(received int32) {
	atomic.StoreInt32(&rf.recvHeartBeat, received)
}

func (rf *Raft) voteForSelf() {
	rf.setVotedFor(int32(rf.me))
}

func (rf *Raft) unvote() {
	rf.setVotedFor(-1)
}

func (rf *Raft) incCurrentTerm() {
	atomic.AddInt32(&rf.currentTerm, 1)
	rf.voteForSelf()
}

func (rf *Raft) updateTerm(term int32) {
	currTerm := rf.getCurrentTerm()
	// assert.Assert(term >= currTerm)
	if term > currTerm {
		rf.unvote()
	}
	rf.setCurrentTerm(term)
}

func (rf *Raft) setAll(term int32, state PeerState) {
	rf.updateTerm(term)
	rf.setPeerState(state)
	rf.setHeartBeat(1)
}

func (rf *Raft) resetAll(term int32) {
	rf.setAll(term, Follower)
}

func (rf *Raft) getHeartBeat() int32 {
	recvHeartBeat := atomic.LoadInt32(&rf.recvHeartBeat)
	rf.setHeartBeat(0)
	return recvHeartBeat
}

func (rf *Raft) getCurrentTerm() int32 {
	return atomic.LoadInt32(&rf.currentTerm)
}

func (rf *Raft) getPeerState() PeerState {
	return PeerState(atomic.LoadInt32((*int32)(&rf.state)))
}

func (rf *Raft) isLeader() bool {
	return rf.getPeerState() == Leader
}

func (rf *Raft) isFollower() bool {
	return rf.getPeerState() == Follower
}

func (rf *Raft) sendHeartBeat() {
	if !rf.isLeader() {
		return
	}

	args := AppendEntriesArgs{
		Term:        rf.getCurrentTerm(),
		LearderId:   int32(rf.me),
		IsHeartBeat: true,
	}
	// fmt.Printf("%v(term %v) notifies all\n", rf.me, rf.currentTerm)
	args.LeaderCommit = rf.commitIndex
	replies := make([]AppendEntriesReply, len(rf.peers))
	for i := 0; i < len(replies); i++ {
		if i == rf.me {
			continue
		}
		go func(i int, reply *AppendEntriesReply) {
			if rf.isLeader() {
				ok := rf.sendAppendEntries(i, &args, reply)
				if ok && reply.Term > rf.currentTerm {
					rf.resetAll(reply.Term)
				}
			}
		}(i, &replies[i])
	}
}

func (rf *Raft) termOutOfDate(othersTerm int32) bool {
	currTerm := rf.getCurrentTerm()
	outOfDate := currTerm < othersTerm
	if outOfDate {
		// fmt.Printf("%v term(%v) out of date\n", rf.me, currTerm)
		rf.resetAll(othersTerm)
	}
	return outOfDate
}

func (rf *Raft) initIndicesInLeader() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) elect() {
	fmt.Printf("%v (term %v) begins election\n", rf.me, rf.getCurrentTerm())

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.recvHeartBeat == 1 {
		fmt.Printf("%v (term %v) heart beat == 1\n", rf.me, rf.getCurrentTerm())
		return
	}

	rf.setPeerState(Candidate)
	rf.incCurrentTerm()
	rf.setHeartBeat(1)
	currTerm := rf.getCurrentTerm()
	args := &RequestVoteArgs{
		Term:        currTerm,
		CandidateId: int32(rf.me),
		// LastLogIndex: len(rf.log),
		LastLogIndex: rf.commitIndex,
		LastLogTerm:  currTerm,
	}
	replies := make([]RequestVoteReply, len(rf.peers))
	oks := make([]bool, len(replies))
	var wg sync.WaitGroup

	// wait for replies
	for i := 0; i < len(rf.peers); i++ {
		oks[i] = false
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int, reply *RequestVoteReply) {
			defer wg.Done()
			oks[i] = rf.sendRequestVote(i, args, reply)
		}(i, &replies[i])
	}
	waitTimeout(&wg, randSleepTime())

	// check result
	vote := 1
	crash := true
	for i, reply := range replies {
		if !oks[i] {
			continue
		} else {
			crash = false
		}
		if rf.termOutOfDate(reply.Term) {
			// fmt.Printf("%v's term > %v' term\n", i, rf.me)
			break
		} else if reply.VoteGranted {
			vote++
		}
	}
	// not sure should I do this or not
	if crash {
		rf.resetAll(rf.getCurrentTerm() - 1)
	}
	// if any other one becomes leader
	if rf.isFollower() {
		// fmt.Printf("%v (term %v) fail, vote: %v\n", rf.me, rf.getCurrentTerm(), vote)
		assert.Assert(rf.recvHeartBeat > 0)
	} else if vote > len(rf.peers)/2 { // if win
		rf.setPeerState(Leader)
		rf.setHeartBeat(1)
		fmt.Printf("%v (term %v) is leader now\n", rf.me, rf.currentTerm)
		go rf.sendHeartBeat()
		rf.initIndicesInLeader()
	} else { // votes are split, no one wins
		rf.setHeartBeat(1)
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// -------- RPC --------

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32
	CandidateId  int32
	LastLogIndex int
	LastLogTerm  int32
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("%v(term %v) requests %v(term %v) to vote\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
	currTerm := rf.getCurrentTerm()
	reply.Term = currTerm
	if args.Term < currTerm || rf.upToDateThan(args.LastLogIndex, args.LastLogTerm) {
		fmt.Printf("%v(term %v) refuses to vote for %v(term %v)\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.VoteGranted = false
		return
	} else if args.Term > currTerm {
		reply.VoteGranted = true
		rf.setVotedFor(args.CandidateId)
		rf.setPeerState(Follower)
		rf.setHeartBeat(1)
		rf.setCurrentTerm(args.Term)
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// follower is in the same term with requester and hasn't voted
		reply.VoteGranted = true
		rf.setVotedFor(args.CandidateId)
		rf.setPeerState(Follower)
		rf.setHeartBeat(1)
	}
}

func (rf *Raft) upToDateThan(lastLogIndex int, lastLogTerm int32) bool {
	if len(rf.log) == 0 {
		return false
	}

	// if len(rf.log) < lastLogIndex {
	// 	return false
	// }
	// if len(rf.log) > lastLogIndex {
	// 	return true
	// }
	// // len(rf.log) = lastLogIndex
	// if rf.log[lastLogIndex].RecvTerm < lastLogTerm {
	// 	return false
	// }
	// if rf.log[lastLogIndex].RecvTerm > lastLogTerm {
	// 	return true
	// }
	// // rf.log[lastLogIndex].RecvTerm = lastLogTerm

	if rf.commitIndex < lastLogIndex {
		return false
	}
	if rf.commitIndex > lastLogIndex {
		return true
	}
	// len(rf.log) = lastLogIndex
	if rf.log[lastLogIndex].RecvTerm < lastLogTerm {
		return false
	}
	if rf.log[lastLogIndex].RecvTerm > lastLogTerm {
		return true
	}
	// rf.log[lastLogIndex].RecvTerm = lastLogTerm
	panic("Not handled")
	return false
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int32
	LearderId    int32
	PrevLogIndex int
	PrevLogTerm  int32
	Entries      map[int]Entry
	LeaderCommit int
	IsHeartBeat  bool
}

type AppendEntriesReply struct {
	Term    int32
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	currTerm := rf.getCurrentTerm()
	reply.Term = currTerm
	reply.Success = false

	if args.Term < currTerm {
		return
	}

	// heart beat
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetAll(args.Term)
	if rf.commitIndex < args.LeaderCommit {
		rf.commitTo(args.LeaderCommit)
	}
	if args.IsHeartBeat {
		return
	}
	// replicate log
	logLen := len(rf.log)
	if logLen < args.PrevLogIndex {
		return
	} else if logLen != 0 && args.PrevLogIndex != 0 && rf.log[args.PrevLogIndex].RecvTerm != args.PrevLogTerm {
		return
	}
	// now leader and follower reach an agreement
	// clear entries that leader doesn't record
	for i := args.PrevLogIndex + 1; i <= logLen; i++ {
		delete(rf.log, i)
	}
	assert.Assert(len(rf.log) == args.PrevLogIndex)
	// ready to insert
	for k, v := range args.Entries {
		// fmt.Printf("%v(term %v, leader: %v): %v, %v\n", rf.me, rf.currentTerm, rf.isLeader(), k, *v)
		rf.log[k] = Entry{
			Cmd:      v.Cmd,
			RecvTerm: v.RecvTerm,
		}
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	if !rf.isLeader() {
		return -1, -1, false
	}

	rf.mu.Lock()

	index := len(rf.log) + 1
	term := rf.getCurrentTerm()
	isLeader := true

	// append entry to leader's log
	rf.log[index] = Entry{
		Cmd:      command,
		RecvTerm: int32(term),
	}
	defer rf.mu.Unlock()
	// send entry to every peer
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.appendEntryToOnePeer(i)
	}

	return index, int(term), isLeader
}

func (rf *Raft) appendEntryToOnePeer(server int) {
	rf.entryMus[server].Lock()
	defer rf.entryMus[server].Unlock()

	logLen := len(rf.log)

	if logLen < rf.nextIndex[server] {
		return
	}
	term := rf.getCurrentTerm()
	args := AppendEntriesArgs{
		Term:         term,
		LearderId:    int32(rf.me),
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  0,
		Entries:      map[int]Entry{},
		LeaderCommit: -1,
		IsHeartBeat:  false,
	}
	if args.PrevLogIndex > 0 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex].RecvTerm
	}
	for i := rf.nextIndex[server]; i <= logLen; i++ {
		args.Entries[i] = Entry{
			Cmd:      rf.log[i].Cmd,
			RecvTerm: rf.log[i].RecvTerm,
		}
	}
	for !rf.killed() && rf.isLeader() {
		// if logLen < rf.nextIndex[server] {
		// 	return
		// }
		assert.Assert(logLen >= rf.nextIndex[server])

		args.LeaderCommit = rf.commitIndex
		reply := AppendEntriesReply{}
		// send indefinitely until success
		for !rf.sendAppendEntries(server, &args, &reply) {
			time.Sleep(HeartBeat)
			if !rf.isLeader() {
				return
			}
		}

		if !reply.Success {
			// fmt.Println(rf.me, rf.isLeader(), rf.currentTerm)
			if reply.Term > rf.currentTerm {
				rf.resetAll(reply.Term)
				break
			}
			rf.nextIndex[server]--
			nextIndex := rf.nextIndex[server]
			args.PrevLogIndex--

			assert.Assert(nextIndex > 0)

			args.Entries[nextIndex] = Entry{
				Cmd:      rf.log[nextIndex].Cmd,
				RecvTerm: rf.log[nextIndex].RecvTerm,
			}
			if args.PrevLogIndex > 0 {
				args.PrevLogTerm = rf.log[args.PrevLogIndex].RecvTerm
			}
		} else {
			rf.matchIndex[server] = logLen
			rf.nextIndex[server] = logLen + 1
			rf.checkReadyToCommit(logLen)
			break
		}
	}
}

func (rf *Raft) checkReadyToCommit(index int) {
	if rf.commitIndex >= index {
		return
	}

	replicatedNum := 1 // myself
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.matchIndex[i] >= index {
			replicatedNum++
		}
		// the majority of peers have replicated the log,
		// ready to commit
		if replicatedNum > len(rf.peers)/2 {
			rf.mu.Lock()
			rf.commitTo(index)
			defer rf.mu.Unlock()
			break
		}
	}
}

func (rf *Raft) commitTo(index int) {
	index = min(index, len(rf.log))
	for j := rf.commitIndex + 1; j <= index; j++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[j].Cmd,
			CommandIndex: j,
		}
		rf.commitIndex++
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// fmt.Printf("%v(term %v), log: %v, commit idx: %v\n", rf.me, rf.currentTerm, rf.log,rf.commitIndex)
		fmt.Printf("%v(term %v)\n", rf.me, rf.currentTerm)
		if rf.isLeader() {
			time.Sleep(HeartBeat)
			rf.sendHeartBeat()
			continue
		}
		if rf.getHeartBeat() > 0 {
			randSleep()
			continue
		}

		rf.elect()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.entryMus = make([]sync.Mutex, len(rf.peers))
	rf.currentTerm = 0
	rf.state = Follower
	rf.votedFor = -1 // nil
	rf.recvHeartBeat = 1
	rf.log = map[int]Entry{}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// ------- helper functions ------
func randSleepTime() time.Duration {
	value := rand.Intn(ElecTimeoutUpperBound-ElecTimeoutLowerBound) + ElecTimeoutLowerBound
	return time.Duration(value) * time.Millisecond
}

func randSleep() {
	time.Sleep(randSleepTime())
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}
