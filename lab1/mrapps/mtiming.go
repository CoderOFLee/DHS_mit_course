package mrapps

//
// a MapReduce pseudo-application to test that workers
// execute map tasks in parallel.
//
// go build -buildmode=plugin mtiming.go
//

import (
	"6.824/mr"
	"os/exec"
	"strconv"
)
import "strings"
import "fmt"
import "os"
import "time"
import "sort"
import "io/ioutil"

func nparallel1(phase string) int {
	// create a file so that other workers will see that
	// we're running at the same time as them.
	pid := os.Getpid()
	myfilename := fmt.Sprintf("mr-worker-%s-%d", phase, pid)
	err := ioutil.WriteFile(myfilename, []byte("x"), 0666)
	if err != nil {
		panic(err)
	}

	// are any other workers running?
	// find their PIDs by scanning directory for mr-worker-XXX files.
	dd, err := os.Open(".")
	if err != nil {
		panic(err)
	}
	names, err := dd.Readdirnames(1000000)
	if err != nil {
		panic(err)
	}
	ret := 0
	for _, name := range names {
		var xpid int
		pat := fmt.Sprintf("mr-worker-%s-%%d", phase)
		n, err := fmt.Sscanf(name, pat, &xpid)
		if n == 1 && err == nil {
			cmd := exec.Command("kill", "-9", strconv.Itoa(xpid))
			err := cmd.Run()
			if err != nil {
				fmt.Println("Error killing thread:", err)
				os.Exit(-1)
			}
			if err == nil {
				// if err == nil, xpid is alive.
				ret += 1
			}
		}
	}
	dd.Close()

	time.Sleep(1 * time.Second)

	err = os.Remove(myfilename)
	if err != nil {
		panic(err)
	}

	return ret
}

func MapMTiming(filename string, contents string) []mr.KeyValue {
	t0 := time.Now()
	ts := float64(t0.Unix()) + (float64(t0.Nanosecond()) / 1000000000.0)
	pid := os.Getpid()

	n := nparallel1("map")

	kva := []mr.KeyValue{}
	kva = append(kva, mr.KeyValue{
		fmt.Sprintf("times-%v", pid),
		fmt.Sprintf("%.1f", ts)})
	kva = append(kva, mr.KeyValue{
		fmt.Sprintf("parallel-%v", pid),
		fmt.Sprintf("%d", n)})
	return kva
}

func ReduceMTiming(key string, values []string) string {
	//n := nparallel("reduce")

	// sort values to ensure deterministic output.
	vv := make([]string, len(values))
	copy(vv, values)
	sort.Strings(vv)

	val := strings.Join(vv, " ")
	return val
}
