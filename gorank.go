package main

import "bufio"
import "compress/gzip"
import "encoding/binary"
import "io"
import "log"
import "os"
import "runtime"
import "sync"

type Edge struct {
	From uint32
	To   uint32
}

func sendEdges(filename string, chans [](chan Edge), hashOnSource bool) {
	f, _ := os.Open(filename)
	defer f.Close()
	gunzip, _ := gzip.NewReader(f)
	// Adds the ReadByte method requird by io.ByteReader interface
	wrappedByteReader := bufio.NewReader(gunzip)
	edge := uint64(0)
	chanLen := uint32(len(chans))
	for {
		// Read the variable integer and undo the delta encoding by adding the previous edge
		rawEdge, err := binary.ReadUvarint(wrappedByteReader)
		edge += rawEdge
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		// Seperate the two 32 bit nodes from the 64 bit edge
		eFrom := uint32(edge >> 32)
		// Converting uint64 to uint32 drops the top 32 bits
		// Had (edge & 0xFFFFFFFF) for clarity but Go compiler doesn't optimize it away ...
		eTo := uint32(edge)
		// Edges are distributed across workers according to either source or destination node
		if hashOnSource {
			chans[eFrom%chanLen] <- Edge{eFrom, eTo}
		} else {
			chans[eTo%chanLen] <- Edge{eFrom, eTo}
		}
	}
	for _, c := range chans {
		close(c)
	}
}

func applyFunctionToEdges(f func(c chan Edge), workers int, hashOnSource bool) {
	// The work for each of the workers is deposited onto their channel
	chans := make([]chan Edge, workers, workers)
	for i := range chans {
		chans[i] = make(chan Edge, 1024)
	}
	//
	go sendEdges("pld-arc.bin.gz", chans, hashOnSource)
	var wg sync.WaitGroup
	for _, c := range chans {
		wg.Add(1)
		go func(c chan Edge) {
			f(c)
			wg.Done()
		}(c)
	}
	wg.Wait()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	// We can either have total nodes supplied by the user or perform a full traversal of the data
	total := uint32(42889799 + 1)
	alpha := float32(0.85)
	//
	src := make([]float32, total, total)
	dest := make([]float32, total, total)
	degree := make([]float32, total, total)
	//
	log.Printf("Calculating degree of each source node\n")
	applyFunctionToEdges(func(c chan Edge) {
		for edge := range c {
			degree[edge.From] += 1
		}
	}, 31, true)
	//
	for iter := 0; iter < 20; iter++ {
		log.Printf("PageRank Iteration: %d\n", iter)
		log.Printf("Calculating the source and destination vectors\n")
		for i := range dest {
			src[i] = alpha * dest[i] / degree[i]
			dest[i] = 1 - alpha
		}
		log.Printf("Calculating the probability mass gifted by incoming edges\n")
		applyFunctionToEdges(func(c chan Edge) {
			for edge := range c {
				dest[edge.To] += src[edge.From]
			}
		}, 31, false)
	}
}
