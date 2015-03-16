package main

import "bufio"
import "compress/gzip"
import "encoding/binary"
import "fmt"
import "io"
import "os"
import "runtime"
import "sync"

type Edge struct {
	From uint32
	To   uint32
}

var workers = 32

func sendEdges(filename string, chans [](chan Edge)) {
	f, _ := os.Open(filename)
	defer f.Close()
	gunzip, _ := gzip.NewReader(f)
	// Adds the ReadByte method requird by io.ByteReader interface
	wrappedByteReader := bufio.NewReader(gunzip)
	i := 0
	prevEdge := uint64(0)
	for {
		rawEdge, err := binary.ReadUvarint(wrappedByteReader)
		// Undo the delta encoding
		edge := rawEdge + prevEdge
		prevEdge = edge
		if err != nil {
			if err == io.EOF {
				break
			} else {
				panic(err)
			}
		}
		eFrom := uint32(edge >> 32)
		eTo := uint32(edge & 0xFFFFFFFF)
		chans[eFrom%uint32(len(chans))] <- Edge{eFrom, eTo}
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
	go sendEdges("pld-arc.bin.gz", chans)
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
