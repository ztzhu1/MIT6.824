package raft

// ------ RaftState ------
type RaftState int32

const (
	StateFollower RaftState = iota
	StatePreCandidate
	StateCandidate
	StateLeader
)

// ------ MsgType ------
type MsgType int

const (
	MsgVote MsgType = iota
	MsgHeartBeat
	MsgAppend
	MsgCommit
)

// ------ CampaignType ------
type CampaignType int

const (
	CampaignPreCandidate CampaignType = iota
	CampaignCandidate
)

// ------ Entry ------
type Entry struct {
	Cmd interface{}
	// leader term in `Start`
	Term int
	// leader term in `AppendEntries`
	// Add this field so that if an
	// entry has been replicated by
	// the leader, later re-replicate
	// request caused by latency will
	// be ignored
	AppendTerm int
}
