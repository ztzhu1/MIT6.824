package assert

import "os"
import "fmt"
import "runtime"

func Assert(condition bool) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[1;31m")
		fmt.Printf("Assertion Failed: %v(%v)\n", file, line)
		fmt.Printf("\033[0m")
		os.Exit(1)
	}
}