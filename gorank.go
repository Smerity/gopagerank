package main

import "bufio"
import "compress/gzip"
import "fmt"
import "os"
import "runtime"
import "sync"

type Edge struct {
	From uint32
	To   uint32
}

var workers = 32

// Necessary as all other atoi converters require allocating a string
// Allocating a string on each line substantially slows down reading
func bytesToUint32(s []byte) uint32 {
	n := uint32(0)
	p := uint32(1)
	for i := range s {
		n += uint32(s[len(s)-1-i]-'0') * p
		p *= 10
	}
	return n
}

func sendEdges(filename string, chans [](chan Edge)) {
	// TODO: Convert once to a varint binary format for smaller size + faster reading
	f, _ := os.Open(filename)
	gunzip, _ := gzip.NewReader(f)
	scanner := bufio.NewScanner(gunzip)
	scanner.Split(bufio.ScanWords)
	i := 0
	for scanner.Scan() {
		eFrom := bytesToUint32(scanner.Bytes())
		scanner.Scan()
		eTo := bytesToUint32(scanner.Bytes())
		chans[eFrom%uint32(len(chans))] <- Edge{uint32(eFrom), uint32(eTo)}
		//
		i++
		if i%1000000 == 0 {
			fmt.Println("Edge ", i)
		}
	}
	for _, c := range chans {
		close(c)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	// We can either have total nodes supplied by the user or perform a full traversal of the data
	var total uint32 = 42889799
	//
	/*
		var src []uint32
		var dest []uint32
		src = make([]uint32, total, total)
		dest = make([]uint32, total, total)
	*/
	var degree []uint32
	degree = make([]uint32, total, total)
	//
	// The work for each of the workers is deposited onto their chansnnel
	// A given node will only ever be mapped to a single worker
	// theirhis allows preventing concurrency issues without locking
	var chans = make([]chan Edge, workers, workers)
	for i := range chans {
		chans[i] = make(chan Edge, 256)
	}
	//
	go sendEdges("pld-arc.gz", chans)
	var wg sync.WaitGroup
	for _, c := range chans {
		wg.Add(1)
		go func(c chan Edge) {
			for edge := range c {
				degree[edge.From] += 1
			}
			wg.Done()
		}(c)
	}
	wg.Wait()
	//
	fmt.Println(degree[0])
}
