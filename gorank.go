package main

import "bufio"
import "encoding/binary"
import "fmt"
import "io"
import "log"
import "os"
import "runtime"
import "sync"

type Edge struct {
	From uint32
	To   uint32
}

func processEdgeStore(edgeStore []uint64, f func(uint32, uint32)) {
	for e := range edgeStore {
		// Seperate the two 32 bit nodes from the 64 bit edge
		eFrom := uint32(e >> 32)
		// Converting uint64 to uint32 drops the top 32 bits
		// Had (edge & 0xFFFFFFFF) for clarity but Go compiler doesn't optimize it away ...
		eTo := uint32(e)
		// Edges are distributed across workers according to either source or destination node
		f(eFrom, eTo)
	}
}

func sendEdges(filename string, f func(uint32, uint32), senderGroup *sync.WaitGroup) {
	defer senderGroup.Done()
	file, _ := os.Open(filename)
	defer file.Close()
	// Adds the ReadByte method requird by io.ByteReader interface
	wrappedByteReader := bufio.NewReader(file)
	edge := uint64(0)
	edgeStore := make([]uint64, 16384, 16384)
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
		if len(edgeStore) == cap(edgeStore) {
			processEdgeStore(edgeStore, f)
			// Empty the edge store
			edgeStore = edgeStore[0:0]
		}
		edgeStore = append(edgeStore, edge)
	}
	processEdgeStore(edgeStore, f)
}

func applyFunctionToEdges(f func(uint32, uint32), workers int) {
	var senderGroup sync.WaitGroup
	for i := 0; i < 8; i++ {
		senderGroup.Add(1)
		go sendEdges(fmt.Sprintf("pld-arc.%d.bin", i), f, &senderGroup)
	}
	//
	senderGroup.Wait()
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
	applyFunctionToEdges(func(from uint32, to uint32) {
		degree[from] += 1
	}, 8)
	//
	for iter := 0; iter < 20; iter++ {
		log.Printf("PageRank Iteration: %d\n", iter+1)
		log.Printf("Calculating the source and destination vectors\n")
		for i := range dest {
			src[i] = alpha * dest[i] / degree[i]
			dest[i] = 1 - alpha
		}
		log.Printf("Calculating the probability mass gifted by incoming edges\n")
		applyFunctionToEdges(func(from uint32, to uint32) {
			dest[to] += src[from]
		}, 8)
	}
}
