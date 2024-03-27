package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"6.824/mr"
	"fmt"
	"os"
	"time"
)

func main() {
	//if len(os.Args) < 2 {
	//	fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
	//	os.Exit(1)
	//}

	fmt.Println("make a coordinator of reduce 1")
	m := mr.MakeCoordinator(os.Args[1:], 1)
	//m := mr.MakeCoordinator([]string{"pg-being_ernest.txt"}, 10)
	for m.Done() == false {
		fmt.Println("check m.done...not done yet")
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
	fmt.Println("all work done...")
}
