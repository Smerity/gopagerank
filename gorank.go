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

var workers = 31

func sendEdges(filename string, chans [](chan Edge)) {
	f, _ := os.Open(filename)
	defer f.Close()
	gunzip, _ := gzip.NewReader(f)
	// Adds the ReadByte method requird by io.ByteReader interface
	wrappedByteReader := bufio.NewReader(gunzip)
	i := 0
	prevEdge := uint64(0)
	for {
		// Read the variable integer and undo the delta encoding by adding the previous edge
		rawEdge, err := binary.ReadUvarint(wrappedByteReader)
		edge := rawEdge + prevEdge
		prevEdge = edge
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		// Seperate the two 32 bit nodes from the 64 bit edge
		eFrom := uint32(edge >> 32)
		eTo := uint32(edge & 0xFFFFFFFF)
		// Edges are distributed across workers according to naive hash over source node
		chans[eFrom%uint32(len(chans))] <- Edge{eFrom, eTo}
		//
		i++
		if i%1e6 == 0 {
			fmt.Println("Edge ", i)
		}
	}
	for _, c := range chans {
		close(c)
	}
}

func applyFunctionToEdges(f func(c chan Edge)) {
	// The work for each of the workers is deposited onto their chansnnel
	// A given node will only ever be mapped to a single worker
	// theirhis allows preventing concurrency issues without locking
	chans := make([]chan Edge, workers, workers)
	for i := range chans {
		chans[i] = make(chan Edge, 1024)
	}
	//
	go sendEdges("pld-arc.bin.gz", chans)
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
	applyFunctionToEdges(func(c chan Edge) {
		for edge := range c {
			degree[edge.From] += 1
		}
	})
	//
	for iter := 0; iter < 20; iter++ {
		fmt.Println("PageRank Iteration:", iter)
		fmt.Println("Calculating the source and destination vectors")
		for i := range dest {
			src[i] = alpha * dest[i] / degree[i]
			dest[i] = 1 - alpha
		}
		fmt.Println("Calculating the probability mass gifted by incoming edges")
		applyFunctionToEdges(func(c chan Edge) {
			for edge := range c {
				dest[edge.To] += src[edge.From]
			}
		})
	}
}
