package mr

type TaskType uint8

const (
	MAP    TaskType = 0
	REDUCE TaskType = 1
	FAKE   TaskType = 2
)

type Task struct {
	Type       TaskType
	ID         int
	InputName  string
	OutputName string
	processing bool
	procTime   int64 // processing time
}
