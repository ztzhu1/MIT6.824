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
	ElecTimeoutLowerBound = 200
	ElecTimeoutUpperBound = 500
	HeartBeat             = 100 * time.Millisecond
)

type PeerState int32

const (
	Follower PeerState = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	state         PeerState
	votedFor      int
	recvHeartBeat int32 // zero: false; non-zero: true

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

func (rf *Raft) changeState(state PeerState) {
	atomic.StoreInt32((*int32)(&rf.state), int32(state))
}

func (rf *Raft) setHeartBeat(received int32) {
	atomic.StoreInt32(&rf.recvHeartBeat, received)
}

func (rf *Raft) getHeartBeat() int32 {
	recvHeartBeat := atomic.LoadInt32(&rf.recvHeartBeat)
	rf.setHeartBeat(0)
	return recvHeartBeat
}

func (rf *Raft) isLeader() bool {
	state := atomic.LoadInt32((*int32)(&rf.state))
	return state == int32(Leader)
}

func (rf *Raft) leaderNotify() {
	assert.Assert(rf.isLeader())

	args := AppendEntriesArgs{Term: rf.currentTerm, LearderId: rf.me}
	for rf.isLeader() && !rf.killed() {
		replies := make([]AppendEntriesReply, len(rf.peers))
		for i := 0; i < len(replies); i++ {
			if i == rf.me {
				continue
			}
			go func(i int, reply *AppendEntriesReply) {
				rf.sendAppendEntries(i, &args, reply)
			}(i, &replies[i])
		}
		time.Sleep(HeartBeat)
	}
}

func (rf *Raft) setState(term int, state PeerState) {
	rf.currentTerm = term
	rf.state = state
	rf.recvHeartBeat = 1
	rf.votedFor = -1
}

func (rf *Raft) resetState(term int) {
	rf.setState(term, Follower)
}

func (rf *Raft) termOutOfDate(othersTerm int) bool {
	outOfDate := rf.currentTerm < othersTerm
	if outOfDate {
		rf.resetState(othersTerm)
	}
	return outOfDate
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
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

	fmt.Printf("a: %v(term %v) request %v(term %v) to vote\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || (rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		fmt.Printf("b: %v(term %v) request %v(term %v) to vote\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
		reply.VoteGranted = false
		return
	} else {
		fmt.Printf("c: %v(term %v) request %v(term %v) to vote\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.state = Follower
		// rf.recvHeartBeat = 1
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
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
	Term      int
	LearderId int
}

type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term >= rf.currentTerm {
		rf.resetState(args.Term)
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

func randSleepTime() time.Duration {
	value := rand.Intn(ElecTimeoutUpperBound-ElecTimeoutLowerBound) + ElecTimeoutLowerBound
	return time.Duration(value) * time.Millisecond
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if rf.isLeader() {
			rf.leaderNotify()
			continue
		}
		if rf.getHeartBeat() > 0 {
			time.Sleep(randSleepTime())
			continue
		}

		time.Sleep(randSleepTime())
		if rf.getHeartBeat() > 0 {
			time.Sleep(randSleepTime())
			continue
		} else if rf.votedFor != -1 {
			assert.Assert(rf.state == Follower)
			continue
		} else { // ask other peers to vote for me
			fmt.Printf("%v(term %v), state: %v, votedFor: %v\n", rf.me, rf.currentTerm, rf.state, rf.votedFor)
			rf.changeState(Candidate)
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			args := &RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			rf.mu.Unlock()
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
			wg.Wait()

			// check result
			vote := 1
			for i, reply := range replies {
				if !oks[i] {
					continue
				}
				if rf.termOutOfDate(reply.Term) {
					break
				} else if reply.VoteGranted {
					vote++
				}
			}
			// if win
			if rf.state == Candidate && vote > len(rf.peers)/2 {
				rf.setState(rf.currentTerm, Leader)
				fmt.Println(rf.me, "is leader")
				aeArgs := &AppendEntriesArgs{Term: rf.currentTerm, LearderId: rf.me}
				aeReplies := make([]AppendEntriesReply, len(rf.peers))
				oks := make([]bool, len(rf.peers))
				// tell others I'm leader
				for i := 0; i < len(rf.peers); i++ {
					oks[i] = false
					if i == rf.me {
						continue
					}
					wg.Add(1)
					go func(i int, aeReply *AppendEntriesReply) {
						oks[i] = rf.sendAppendEntries(i, aeArgs, aeReply)
					}(i, &aeReplies[i])
				}
				wg.Wait()
				for i, aeReply := range aeReplies {
					if !oks[i] {
						continue
					}
					rf.termOutOfDate(aeReply.Term)
				}
			} else { // lose for some reason
				rf.resetState(rf.currentTerm)
			}
		}
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
	rf.currentTerm = 0
	rf.state = Follower
	rf.votedFor = -1 // nil
	rf.recvHeartBeat = 1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
