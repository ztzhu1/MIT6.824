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

	. "6.824/assert"
	"6.824/labgob"
	"6.824/labrpc"
)

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) randTimeout(baseTimeout int64) int64 {
	r.mu.Lock()
	v := r.rand.Int63n(baseTimeout)
	r.mu.Unlock()
	return baseTimeout + v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	id        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	commitCh chan ApplyMsg
	applyCh  chan ApplyMsg

	state     RaftState
	term      int
	votedFor  int
	votedTerm int

	entries          map[int]Entry
	commitIndex      int
	discardCount     int
	lastdiscarDEntry Entry
	nextIndex        []int
	matchIndex       []int

	electionTimeout  int64
	heartBeatTimeout int64

	electionElapsed  int64
	heartBeatElapsed int64

	tick func()
}

type RaftPersistent struct {
	Term             int
	VotedFor         int
	VotedTerm        int
	DiscardCount     int
	LastdiscarDEntry Entry
	Entries          map[int]Entry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.state == StateLeader
}

// -------- RPC --------

// -------- RequestVote --------

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	From         int
	CampType     CampaignType
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	reply.VoteGranted = false
	if !rf.canVoteFor(args.From, args.Term, args.LastLogIndex, args.LastLogTerm) {
		return
	}
	if args.CampType == CampaignCandidate {
		rf.recordVoteFor(args.From, args.Term, args.LastLogIndex, args.LastLogTerm)
		DPrintf("%v(term %v) voted for %v(term %v)", rf.id, rf.term, args.From, args.Term)
		rf.term = args.Term
		rf.persist()
	}
	reply.VoteGranted = true
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

// -------- AppendEntries --------

type AppendEntriesArgs struct {
	Type         MsgType
	LeaderTerm   int
	LeaderId     int
	LeaderCommit int
	PrevLogIdx   int
	PrevLogTerm  int
	Entries      map[int]Entry
}

type AppendEntriesReply struct {
	Term                         int
	Success                      bool
	FollowerEntrySize            int
	FirstConflictOfThisTerm      int
	FirstConflictOfThisTermValid bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	reply.Success = false
	reply.FollowerEntrySize = rf.logSize()
	reply.FirstConflictOfThisTermValid = false
	if args.LeaderTerm < rf.term {
		// old leader
		return
	}

	Assert(rf.term < args.LeaderTerm || rf.state != StateLeader)
	rf.resetElectionElapsed()
	rf.setState(StateFollower)
	rf.tick = rf.tickFollower
	rf.term = args.LeaderTerm
	rf.persist()
	switch {
	case args.Type == MsgHeartBeat:
		reply.Success = true
		rf.followerTryToCommitUntil(args.LeaderCommit, args.LeaderTerm)
	case args.Type == MsgAppend:
		size := rf.logSize()
		prev := args.PrevLogIdx
		if prev >= rf.discardCount {
			if prev == 0 || (size >= prev && rf.entry(prev).Term == args.PrevLogTerm) {
				// append entries to follower
				i := prev + 1
				for ; i <= prev+len(args.Entries); i++ {
					if rf.commitIndex >= i {
						continue
					}
					entry, exist := rf.entries[i]
					if exist && entry.AppendTerm >= args.LeaderTerm {
						// The entry is appended by a up-to-date leader, may be
						// the caller itself, so we can ignore it.
						continue
					}
					rf.entries[i] = args.Entries[i]
					rf.persist()
				}
				for ; i <= size; i++ {
					if rf.commitIndex >= i {
						continue
					}
					entry, exist := rf.entries[i]
					if exist && entry.AppendTerm >= args.LeaderTerm {
						// the latter entries must be at least as
						// up-to-date as leader's entries
						break
					}
					delete(rf.entries, i)
					rf.persist()
				}
				rf.followerTryToCommitUntil(args.LeaderCommit, args.LeaderTerm)
				reply.FollowerEntrySize = rf.logSize()
				reply.Success = true
			} else if size >= prev && rf.entry(prev).Term != args.PrevLogTerm {
				// invariant: prev > 0, size > 0
				term := rf.entry(prev).Term
				i := prev
				for ; i > 0; i-- {
					if rf.entry(i).Term != term {
						break
					}
				}
				reply.FirstConflictOfThisTerm = i + 1
				reply.FirstConflictOfThisTermValid = true
			} else {
				// DInfo("%v(term %v, leader %v), size: %v, prev: %v, prev term:%v, leader prev term: %v", rf.id, rf.term, args.LeaderId, size, prev, rf.entries[prev].Term, args.PrevLogTerm)
			}
		} else {
			// reply.NeedSnapshot = true
		}
	default:
		panic("unreachable")
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapShotArgs struct {
	LeaderTerm int
	LeaderId   int
	Snapshot   []byte
}

type InstallSnapShotReply struct {
	Term       int
	MatchIndex int
}

func (rf *Raft) InstallSnap(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	reply.MatchIndex = rf.commitIndex
	if args.LeaderTerm < rf.term {
		// old leader
		return
	}
	// DInfo("%v(term %v) discnt: %v, len: %v, logsize: %v, commit index: %v", rf.id, rf.term, rf.discardCount, len(rf.entries), rf.logSize(), rf.commitIndex)
	commandIndex, cmds := rf.decode(args.Snapshot)

	ssLastIndex := commandIndex
	// ssLastTerm:=args.Entries[ssLastIndex].Term
	if rf.commitIndex >= ssLastIndex {
		// we don't need this snapshot
		return
	}
	Assert(rf.minEntryIndex() < ssLastIndex)
	if rf.maxEntryIndex() <= ssLastIndex {
		// this snapshot includes all the information we should know
		for i := rf.commitIndex + 1; i <= ssLastIndex; i++ {
			rf.entries[i] = Entry{cmds[i-1].(int), 0, 0}
			rf.commit(i)
		}
		Assert(rf.commitIndex == ssLastIndex)
		rf.lastdiscarDEntry = rf.entry(ssLastIndex)
		rf.entries = make(map[int]Entry)
		rf.discardCount = ssLastIndex
		Assert(ssLastIndex != 0)
		Assert(len(args.Snapshot) > 0)
		rf.persistStateAndSnapshot(args.Snapshot)
		reply.MatchIndex = ssLastIndex
		return
	}
	for i := rf.commitIndex + 1; i <= ssLastIndex; i++ {
		rf.entries[i] = Entry{cmds[i-1].(int), 0, 0}
		rf.commit(i)
	}

	rf.lastdiscarDEntry = rf.entry(ssLastIndex)
	for i := rf.minEntryIndex(); i <= ssLastIndex; i++ {
		delete(rf.entries, i)
		rf.discardCount++
		Assert(len(args.Snapshot) > 0)
		rf.persistStateAndSnapshot(args.Snapshot)
	}
	reply.MatchIndex = ssLastIndex
}

func (rf *Raft) sendInstallSnap(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnap", args, reply)
	return ok
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
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	var rp RaftPersistent
	rp.Term = rf.term
	rp.VotedFor = rf.votedFor
	rp.VotedTerm = rf.votedTerm
	rp.DiscardCount = rf.discardCount
	rp.LastdiscarDEntry = rf.lastdiscarDEntry
	rp.Entries = rf.entries
	encoder.Encode(rp)
	data := buf.Bytes()
	rf.persister.SaveRaftState(data)
	// DInfo("%v(term %v, commit %v) %v",rf.id,rf.term ,rf.commitIndex,rp)
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	Assert(len(snapshot) > 0)
	stateBuf := new(bytes.Buffer)
	stateEncoder := labgob.NewEncoder(stateBuf)
	var rp RaftPersistent
	rp.Term = rf.term
	rp.VotedFor = rf.votedFor
	rp.VotedTerm = rf.votedTerm
	rp.DiscardCount = rf.discardCount
	rp.LastdiscarDEntry = rf.lastdiscarDEntry
	rp.Entries = rf.entries
	stateEncoder.Encode(rp)
	data := stateBuf.Bytes()

	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
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
	buf := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buf)
	var rp RaftPersistent
	if decoder.Decode(&rp) != nil {
		panic("decode error")
	}
	rf.term = rp.Term
	rf.votedFor = rp.VotedFor
	rf.votedTerm = rp.VotedTerm
	rf.discardCount = rp.DiscardCount
	rf.lastdiscarDEntry = rp.LastdiscarDEntry
	rf.entries = rp.Entries

	if len(snapshot) == 0 {
		return
	}
	commandIndex, _ := rf.decode(snapshot)
	Assert(rf.discardCount == commandIndex)
	rf.commitIndex = commandIndex

	DInfo("%v recovered", rf)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Assert(index > 0)
	Assert(index <= rf.commitIndex)
	if index <= rf.discardCount {
		return
	}
	for i := rf.minEntryIndex(); i <= index; i++ {
		if i > rf.commitIndex {
			rf.commit(i)
		}
		delete(rf.entries, i)
		rf.discardCount++
	}
	Assert(index <= rf.commitIndex)
	Assert(rf.discardCount <= rf.commitIndex)
	Assert(len(snapshot) > 0)
	rf.persistStateAndSnapshot(snapshot)
	DInfo("%v(term %v) created a snapshot, %v", rf.id, rf.term, rf)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.logSize() + 1
	Assert(rf.commitIndex < index)
	term := rf.term

	if !rf.isLeader() || rf.killed() {
		return -1, -1, false
	}

	rf.entries[index] = Entry{command, rf.term, rf.term}
	rf.persist()
	rf.bcastAppendEntries(term, rf.commitIndex)

	return index, term, true
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
	close(rf.commitCh)
	DInfo("kill %v", rf)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// -------- time --------

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	if rf.killed() {
		return
	}
	ticker_ := time.NewTicker(TickInterval)
	defer ticker_.Stop()
	for range ticker_.C {
		if rf.killed() {
			DSysInfo("%v(term %v) killed", rf.id, rf.term)
			return
		}
		rf.tick()
	}
	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().
}

func (rf *Raft) tickFollower() {
	if !rf.isFollower() {
		return
	}
	// If the node is follower, there is no another goroutine
	// to prompt it to pre-candidate, so we can assume it's
	// always a follower until `rf.campaign`.

	if rf.incElectionElapsed(1) < rf.electionTimeout {
		return
	}
	rf.resetElectionElapsed()

	rf.campaign(CampaignPreCandidate)
}

func (rf *Raft) tickLeader() {
	rf.mu.Lock()
	if !rf.isLeader() {
		rf.mu.Unlock()
		return
	}

	if rf.incHBElapsed(1) < rf.heartBeatTimeout {
		rf.mu.Unlock()
		return
	}
	rf.resetHBElapsed()
	leaderTerm := rf.term
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()
	rf.bcastHeartBeat(leaderTerm, leaderCommit)
	// rf.bcastAppendEntries(leaderTerm, leaderCommit)
}

// -------- state --------

func (rf *Raft) becomeFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Assert(rf.state != StateFollower)
	if rf.state == StateFollower {
		return
	}
	rf.state = StateFollower
	rf.resetVote()

	rf.resetTimeout()

	rf.tick = rf.tickFollower
	DPrintf("%v(term %v) becomes follower", rf.id, rf.term)
}

func (rf *Raft) becomePreCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.checkFollower()
	rf.state = StatePreCandidate
	DPrintf("%v(term %v) becomes pre-candidate", rf.id, rf.term)
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.isPreCandidate() {
		Assert(rf.isFollower())
		return
	}
	rf.state = StateCandidate
	rf.term++
	rf.persist()
	DPrintf("%v(term %v) becomes candidate", rf.id, rf.term)
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Assert(rf.state == StateCandidate)
	rf.setState(StateLeader)
	DPrintf("%v(term %v) becomes leader", rf.id, rf.term)
	rf.resetHBElapsed()
	rf.tick = rf.tickLeader
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.id {
			continue
		}
		rf.nextIndex[i] = rf.logSize() + 1
		rf.matchIndex[i] = 0
	}
	rf.bcastHeartBeat(rf.term, rf.commitIndex)
	rf.bcastAppendEntries(rf.term, rf.commitIndex)
}

func (rf *Raft) resetTimeout() {
	rf.electionTimeout = globalRand.randTimeout(ElectionTimeoutNorm)
	rf.heartBeatTimeout = HeartBeatTimeoutNorm

	rf.resetElectionElapsed()
	rf.resetHBElapsed()
}

// caller should have acquired the lock
func (rf *Raft) resetVote() {
	rf.votedFor = rf.id
	rf.votedTerm = rf.term
	rf.persist()
}

// caller should have acquired the lock
func (rf *Raft) recordVoteFor(cand int, termOfCand int, lastLogindexOfCand int, lastLogTermOfCand int) {
	Assert(rf.canVoteFor(cand, termOfCand, lastLogindexOfCand, lastLogTermOfCand))
	rf.votedFor = cand
	rf.votedTerm = termOfCand
	rf.persist()
}

// caller should have acquired the lock
func (rf *Raft) canVoteFor(cand int, termOfCand int, lastLogindexOfCand int, lastLogTermOfCand int) bool {
	if rf.state == StateLeader || (rf.state != StateFollower && cand != rf.id) {
		// only followers can vote, except when voting for self
		return false
	}
	if rf.upToDateThan(lastLogindexOfCand, lastLogTermOfCand) {
		return false
	}
	if rf.votedTerm > termOfCand {
		// `which` is out-of-date
		return false
	}
	if rf.votedTerm < termOfCand {
		// `which` is newer
		return true
	}
	// `rf.votedTerm` == `termOfCand`, only can vote for self
	if rf.votedFor == cand {
		return true
	}
	return false
}

// caller should have acquired the lock
func (rf *Raft) upToDateThan(lastLogindexOfCand int, lastLogTermOfCand int) bool {
	last := rf.logSize()
	if last == 0 {
		return false
	}
	if lastLogindexOfCand == 0 {
		return true
	}
	if rf.entry(last).Term > lastLogTermOfCand {
		return true
	}
	if rf.entry(last).Term < lastLogTermOfCand {
		return false
	}
	// rf.entries[last].Term == lastLogTermOfCand
	return last > lastLogindexOfCand
}

// -------- actions --------

func (rf *Raft) campaign(t CampaignType) {
	replyCh := make(chan RequestVoteReply, len(rf.peers))

	switch {
	case t == CampaignPreCandidate:
		rf.becomePreCandidate()

		rf.mu.Lock()
		candTerm := rf.term + 1
		lastLogindexOfCand := rf.logSize()
		lastLogTermOfCand := 0
		if lastLogindexOfCand > 0 {
			lastLogTermOfCand = rf.entries[lastLogindexOfCand].Term
		}
		rf.mu.Unlock()

		rf.bcastRequestVote(replyCh, candTerm, CampaignPreCandidate, lastLogindexOfCand, lastLogTermOfCand)
		success, _ := rf.campaignSuccess(replyCh)
		if rf.isFollower() {
			// State transition may happen at any
			// unprotected time due to `AppendEntries`.
			return
		}
		if success {
			rf.becomeCandidate()
			rf.campaign(CampaignCandidate)
		} else if rf.isPreCandidate() {
			rf.becomeFollower()
		} else {
			Assert(rf.isFollower())
		}
	case t == CampaignCandidate:
		DPrintf("%v(term %v) campaigning", rf.id, rf.term)
		rf.mu.Lock()
		Assert(!rf.isPreCandidate())
		Assert(!rf.isLeader())
		if rf.isFollower() {
			DInfo("%v(term %v) steps down to follower, stop campaigning", rf.id, rf.term)
			rf.mu.Unlock()
			return
		}
		candTerm := rf.term
		lastLogindexOfCand := rf.logSize()
		lastLogTermOfCand := rf.entries[lastLogindexOfCand].Term
		rf.mu.Unlock()

		rf.bcastRequestVote(replyCh, candTerm, CampaignCandidate, lastLogindexOfCand, lastLogTermOfCand)
		success, timeout := rf.campaignSuccess(replyCh)
		if rf.isFollower() {
			// State transition may happen at any
			// unprotected time due to `AppendEntries`.
			DInfo("%v(term %v) steps down to follower, stop campaigning", rf.id, rf.term)
			return
		}
		if success {
			rf.becomeLeader()
		} else if timeout {
			// re-elect
			DInfo("%v(term %v) timeout, re-campaign", rf.id, rf.term)
			rf.term++
			rf.persist()
			rf.campaign(CampaignCandidate)
		}else{
			// unsuccess
			rf.becomeFollower()
		}
	}
}

func (rf *Raft) campaignSuccess(replyCh chan RequestVoteReply) (success bool, timeout bool) {
	half := len(rf.peers) / 2
	var reply RequestVoteReply
	grantedNum := 0
	rejectedNum := 0
	for grantedNum <= half && rejectedNum <= half {
		select {
		case reply = <-replyCh:
		case <-time.After(CandidateTimeout):
			return false, true
		}
		if reply.VoteGranted {
			grantedNum++
		} else {
			rejectedNum++
		}
		// TODO (ztzhu): use `reply.Term` to do more things
	}
	return grantedNum > half, false
}

func (rf *Raft) bcastRequestVote(replyCh chan RequestVoteReply, candTerm int, campType CampaignType, lastLogindexOfCand int, lastLogTermOfCand int) {
	for id := 0; id < len(rf.peers); id++ {
		go rf.requestVote(id, replyCh, candTerm, campType, lastLogindexOfCand, lastLogTermOfCand)
	}
}

func (rf *Raft) requestVote(to int, replyCh chan RequestVoteReply, candTerm int, campType CampaignType, lastLogindexOfCand int, lastLogTermOfCand int) {
	rf.mu.Lock()
	if rf.isFollower() {
		rf.mu.Unlock()
		return
	}
	// it can vote for self directly
	if rf.id == to {
		if campType == CampaignCandidate {
			rf.recordVoteFor(rf.id, candTerm, lastLogindexOfCand, lastLogTermOfCand)
		}
		replyCh <- RequestVoteReply{candTerm, true}
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	args := &RequestVoteArgs{
		Term:         candTerm,
		From:         rf.id,
		CampType:     campType,
		LastLogIndex: lastLogindexOfCand,
		LastLogTerm:  lastLogTermOfCand,
	}
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(to, args, reply)
	if ok {
		replyCh <- *reply
	}
}

func (rf *Raft) bcastHeartBeat(leaderTerm int, leaderCommit int) {
	for id := 0; id < len(rf.peers); id++ {
		if id != rf.id {
			go rf.sendHeartBeat(id, leaderTerm, leaderCommit)
		}
	}
}

func (rf *Raft) sendHeartBeat(to int, leaderTerm int, leaderCommit int) {
	if !rf.isLeader() {
		return
	}
	args := &AppendEntriesArgs{
		Type:         MsgHeartBeat,
		LeaderTerm:   leaderTerm,
		LeaderId:     rf.id,
		LeaderCommit: leaderCommit,
	}
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(to, args, reply)
	if ok && reply.Term > leaderTerm && rf.isLeader() {
		rf.becomeFollower()
	}
}

func (rf *Raft) bcastAppendEntries(leaderTerm int, leaderCommitIndex int) {
	for id := 0; id < len(rf.peers); id++ {
		if id != rf.id {
			go rf.appendEntries(id, leaderTerm, leaderCommitIndex)
		}
	}
}

func (rf *Raft) appendEntries(to int, leaderTerm int, leaderCommitIndex int) {
	if !rf.isLeader() {
		return
	}

	entries := make(map[int]Entry)

	rf.mu.Lock()
	beginIdx := rf.nextIndex[to]
	Assert(beginIdx > 0)
	leastIdx := rf.logSize() + 1
	rf.mu.Unlock()
	var ok bool
	// DInfo("%v(term %v) appends to %v (begin: %v)", rf.id, rf.term, to, beginIdx)
	for rf.isLeader() {
		rf.mu.Lock()

		if beginIdx <= rf.discardCount {
			installArgs := &InstallSnapShotArgs{
				LeaderTerm: leaderTerm,
				LeaderId:   rf.id,
				Snapshot:   rf.persister.ReadSnapshot(),
			}
			installReply := &InstallSnapShotReply{}
			rf.mu.Unlock()
			if !rf.isLeader() {
				return
			}
			// _, e := rf.decode(installArgs.Snapshot)
			// DInfo("%v(term %v) send snapshot(len %v) to %v, begin:%v, dis:%v", rf.id, rf.term, len(e), to, beginIdx, rf.discardCount)

			ok := rf.sendInstallSnap(to, installArgs, installReply)
			for !ok {
				time.Sleep(AppendWaitTime)
				if !rf.isLeader() || rf.killed() {
					return
				}
				ok = rf.sendInstallSnap(to, installArgs, installReply)
			}
			if installReply.MatchIndex > rf.matchIndex[to] {
				rf.matchIndex[to] = installReply.MatchIndex
				rf.nextIndex[to] = rf.matchIndex[to] + 1
			}
			rf.appendEntries(to, leaderTerm, leaderCommitIndex)
			return
		}

		for i := leastIdx - 1; i >= beginIdx; i-- {
			entry := rf.entries[i]
			entry.AppendTerm = leaderTerm
			entries[i] = entry
		}
		leastIdx = beginIdx
		args := &AppendEntriesArgs{
			Type:         MsgAppend,
			LeaderTerm:   leaderTerm,
			LeaderId:     rf.id,
			LeaderCommit: leaderCommitIndex,
			Entries:      entries,
			PrevLogIdx:   beginIdx - 1,
		}
		reply := &AppendEntriesReply{}
		if beginIdx > 1 {
			args.PrevLogTerm = rf.entry(beginIdx - 1).Term
		}
		rf.mu.Unlock()

		if !rf.isLeader() {
			return
		}
		ok = rf.sendAppendEntries(to, args, reply)
		for !ok {
			time.Sleep(AppendWaitTime)
			if !rf.isLeader() || rf.killed() {
				return
			}
			ok = rf.sendAppendEntries(to, args, reply)
		}

		if reply.Term > leaderTerm {
			return
		}
		if reply.Success {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.matchIndex[to] < reply.FollowerEntrySize {
				rf.matchIndex[to] = reply.FollowerEntrySize
				rf.nextIndex[to] = reply.FollowerEntrySize + 1
				rf.updateLeaderCommit()
			}
			return
		}
		if reply.FirstConflictOfThisTermValid {
			beginIdx = reply.FirstConflictOfThisTerm
		} else if reply.FollowerEntrySize < beginIdx {
			beginIdx = reply.FollowerEntrySize
			if beginIdx == 0 {
				beginIdx = 1
			}
		} else {
			beginIdx--
		}
		rf.mu.Lock()
		if beginIdx <= rf.matchIndex[to] {
			rf.mu.Unlock()
			return
		}
		Assert(beginIdx > 0)
		rf.nextIndex[to] = beginIdx
		rf.mu.Unlock()
	}
}

// Assuming rf has acquired lock
func (rf *Raft) updateLeaderCommit() {
	if !rf.isLeader() {
		return
	}
	var count int
	for i := rf.logSize(); i > rf.commitIndex; i-- {
		count = 0
		for j, index := range rf.matchIndex {
			if j == rf.id {
				count++
			} else if index >= i {
				count++
			}
			if count > len(rf.peers)/2 && rf.entries[i].Term == rf.term {
				rf.commitUntil(i)
				rf.commitIndex = i
				return
			}
		}
	}
}

func (rf *Raft) followerTryToCommitUntil(leaderCommitIndex int, leaderTerm int) {
	newCommitIndex := min(rf.logSize(), leaderCommitIndex)
	for newCommitIndex > rf.commitIndex {
		Assert(rf.logSize() != 0 && (rf.minEntryIndex() <= rf.commitIndex+1 || rf.commitIndex == 0))
		if rf.entries[newCommitIndex].Term == leaderTerm {
			rf.commitUntil(newCommitIndex)
			break
		}
		newCommitIndex--
	}
}

func (rf *Raft) commitUntil(commitIndex int) {
	if rf.killed() {
		return
	}
	commitIndex = min(commitIndex, rf.logSize())
	Assert(rf.logSize() >= commitIndex)
	Assert(rf.commitIndex < commitIndex)
	for i := rf.commitIndex + 1; i <= commitIndex; i++ {
		rf.commit(i)
	}
}

func (rf *Raft) commit(commitIndex int) {
	Assert(rf.logSize() >= commitIndex && rf.commitIndex < commitIndex)
	Assert(commitIndex > rf.discardCount)
	if rf.killed() {
		return
	}
	rf.commitCh <- ApplyMsg{
		CommandValid: true,
		Command:      rf.entries[commitIndex].Cmd,
		CommandIndex: commitIndex,
	}
	rf.commitIndex++
}

func (rf *Raft) decode(snapshot []byte) (commandIndex int, cmds []interface{}) {
	Assert(len(snapshot) > 0) // if snapshot==nil, len(snapshot)==0
	spbuf := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(spbuf)
	if decoder.Decode(&commandIndex) != nil || decoder.Decode(&cmds) != nil {
		panic("decode error")
	}
	Assert(len(cmds) > 1)
	cmds = cmds[1:] // the first entry is nil
	Assert(commandIndex == len(cmds))
	return
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
	rf.id = me

	// Your initialization code here (2A, 2B, 2C).
	rf.dead = 0
	rf.commitCh = make(chan ApplyMsg, 400)
	rf.applyCh = applyCh
	rf.entries = make(map[int]Entry)
	rf.commitIndex = 0
	rf.discardCount = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.state = StateFollower
	rf.term = 0
	rf.votedFor = rf.id
	rf.votedTerm = 0

	rf.resetTimeout()

	rf.tick = rf.tickFollower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()
	go func() {
		for msg := range rf.commitCh {
			applyCh <- msg
		}
	}()

	return rf
}

// -------- safety check --------

// Caller should ensure that rf already holds mu.
func (rf *Raft) checkFollower() {
	Assert(!rf.killed())
	Assert(rf.isFollower())
}

// Caller should ensure that rf already holds mu.
func (rf *Raft) checkPreCandidate() {
	Assert(!rf.killed())
	Assert(rf.isPreCandidate())
}

// Caller should ensure that rf already holds mu.
func (rf *Raft) checkCandidate() {
	Assert(!rf.killed())
	Assert(rf.isCandidate())
	Assert(rf.term > 0)
}

func (rf *Raft) checkPreCandOrCand() {
	switch {
	case rf.state == StatePreCandidate:
		rf.checkPreCandidate()
	case rf.state == StateCandidate:
		rf.checkCandidate()
	default:
		panic(fmt.Sprintf("expected state: %v or %v, actual: %v", StatePreCandidate, StateCandidate, rf.state))
	}
}

// Caller should ensure that rf already holds mu.
func (rf *Raft) checkLeader() {
	Assert(!rf.killed())
	Assert(rf.isLeader())
	Assert(rf.term > 0)
}

// Caller should ensure that rf already holds mu.
func (rf *Raft) preCandOrCand() bool {
	return rf.state == StatePreCandidate || rf.state == StateCandidate
}

// -------- atomic operation --------

func (rf *Raft) incElectionElapsed(delta int64) (new int64) {
	return atomic.AddInt64(&rf.electionElapsed, delta)
}

func (rf *Raft) incHBElapsed(delta int64) (new int64) {
	return atomic.AddInt64(&rf.heartBeatElapsed, delta)
}

func (rf *Raft) resetElectionElapsed() {
	atomic.StoreInt64(&rf.electionElapsed, 0)
}

func (rf *Raft) resetHBElapsed() {
	atomic.StoreInt64(&rf.heartBeatElapsed, 0)
}

func (rf *Raft) getElectionElapsed() int64 {
	return atomic.LoadInt64(&rf.electionElapsed)
}

func (rf *Raft) getHBElapsed() int64 {
	return atomic.LoadInt64(&rf.heartBeatElapsed)
}

func (rf *Raft) entry(index int) Entry {
	Assert(index > 0)
	Assert(index >= rf.discardCount)
	if index == rf.discardCount {
		return rf.lastdiscarDEntry
	}
	// index > rf.discardCount
	return rf.entries[index]
}

// assume rf has acquired the lock and the log is not empty
func (rf *Raft) maxEntryIndex() int {
	return rf.logSize()
}

// assume rf has acquired the lock and the log is not empty
func (rf *Raft) minEntryIndex() int {
	return rf.discardCount + 1
}

// assume rf has acquired the lock
func (rf *Raft) logSize() int {
	return rf.discardCount + len(rf.entries)
}

// assume rf has acquired the lock
func (rf *Raft) undiscard(index int) bool {
	return index > rf.discardCount
}

func (rf *Raft) String() string {
	return fmt.Sprintf("%v(term %v) commit: %v, disc cnt: %v, log size: %v", rf.id, rf.term, rf.commitIndex, rf.discardCount, rf.logSize())
}

func (rf *Raft) setState(state RaftState) {
	atomic.StoreInt32((*int32)(&rf.state), int32(state))
}

func (rf *Raft) isFollower() bool {
	return atomic.LoadInt32((*int32)(&rf.state)) == int32(StateFollower)
}

func (rf *Raft) isPreCandidate() bool {
	return atomic.LoadInt32((*int32)(&rf.state)) == int32(StatePreCandidate)
}

func (rf *Raft) isCandidate() bool {
	return atomic.LoadInt32((*int32)(&rf.state)) == int32(StateCandidate)
}

func (rf *Raft) isLeader() bool {
	return atomic.LoadInt32((*int32)(&rf.state)) == int32(StateLeader)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
