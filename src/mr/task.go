package mr

import "time"

type TaskType uint8

const (
	MAP    TaskType = 0
	REDUCE TaskType = 1
	REREQ  TaskType = 2 // re-request
	QUIT   TaskType = 3
)

type Task struct {
	Type       TaskType
	ID         int
	InputName  string
	OutputName string
	processing bool
	procStart  time.Time
}
