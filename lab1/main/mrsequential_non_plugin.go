package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import (
	"fmt"
)
import "6.824/mr"
import "6.824/mrapps"
import "os"
import "log"
import "io/ioutil"
import "sort"

// for sorting by key.
type NByKey []mr.KeyValue

// for sorting by key.
func (a NByKey) Len() int           { return len(a) }
func (a NByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a NByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	//if len(os.Args) < 3 {
	//	// go run -race mrsequential.go wc.so pg*.txt
	//	fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
	//	os.Exit(1)
	//}

	//mapf, reducef := loadPlugin(os.Args[1])
	mapF := mrapps.Map_wc
	reduceF := mrapps.Reduce_wc
	// go run -race mrsequential_non_plugin.go pg*.txt

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[1:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		err = file.Close()
		if err != nil {
			fmt.Println(err.Error())
		}
		kva := mapF(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(NByKey(intermediate))

	oname := "seq"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to seq.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reduceF(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			fmt.Println(err.Error())
		}

		i = j
	}

	err := ofile.Close()
	if err != nil {
		fmt.Println(err.Error())
	}
}

//// load the application Map and Reduce functions
//// from a plugin file, e.g. ../mrapps/wc.so
//func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
//	p, err := plugin.Open(filename)
//	if err != nil {
//		log.Fatalf("cannot load plugin %v", filename)
//	}
//	xmapf, err := p.Lookup("Map")
//	if err != nil {
//		log.Fatalf("cannot find Map in %v", filename)
//	}
//	mapf := xmapf.(func(string, string) []mr.KeyValue)
//	xreducef, err := p.Lookup("Reduce")
//	if err != nil {
//		log.Fatalf("cannot find Reduce in %v", filename)
//	}
//	reducef := xreducef.(func(string, []string) string)
//
//	return mapf, reducef
//}
