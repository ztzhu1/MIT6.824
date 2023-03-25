package raft

import "log"

// Debugging
var Debug bool = false

func InitLog(enable bool) {
	if enable {
		Debug = true
		log.SetFlags(log.Ltime | log.Lmicroseconds)
	}
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DWarning(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf("\x1b[1;33m"+format+"\x1b[0m", a...)
	}
	return
}
