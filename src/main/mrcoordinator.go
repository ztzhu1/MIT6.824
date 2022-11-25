package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import "6.824/mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	var elapsed_ms int64 = 50

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for !m.Done() {
		time.Sleep(time.Duration(elapsed_ms) * time.Millisecond)
		m.Tick(elapsed_ms)
	}

}
