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
)

// ------ CampaignType ------
type CampaignType int

const (
	CampaignPreCandidate CampaignType = iota
	CampaignCandidate
)