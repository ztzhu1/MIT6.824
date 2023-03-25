package raft

import (
	"time"
)

const TickInterval = 20 * time.Millisecond

const CandidateTimeout = 100 * time.Millisecond

const ElectionTimeout = 240 * time.Millisecond
const HeartBeatTimeout = 100 * time.Millisecond

const ElectionTimeoutNorm = int64(ElectionTimeout / TickInterval)
const HeartBeatTimeoutNorm = int64(HeartBeatTimeout / TickInterval)
