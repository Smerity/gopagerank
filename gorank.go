package main

import "bufio"
import "encoding/binary"
import "fmt"
import "io"
import "log"
import "os"
import "runtime"
import "sync"
import "sync/atomic"

type Edge struct {
	From uint32
	To   uint32
}

func processEdgeStore(edgeStore []uint64, f func(uint32, uint32)) {
	for _, e := range edgeStore {
		// Seperate the two 32 bit nodes from the 64 bit edge
		eFrom := uint32(e >> 32)
		// Converting uint64 to uint32 drops the top 32 bits
		// Had (edge & 0xFFFFFFFF) for clarity but Go compiler doesn't optimize it away ...
		eTo := uint32(e)
		// Edges are distributed across workers according to either source or destination node
		f(eFrom, eTo)
	}
}

func sendEdges(filename string, f func(uint32, uint32)) {
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
	totalParts := 4
	for i := 0; i < totalParts; i++ {
		senderGroup.Add(1)
		go func(i int) {
			sendEdges(fmt.Sprintf("pld-arc.%d.bin", i), f)
			log.Printf("Completed processing part %d of %d\n", i, totalParts)
			senderGroup.Done()
		}(i)
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
	degree := make([]uint32, total, total)
	//
	log.Printf("Calculating degree of each source node\n")
	applyFunctionToEdges(func(from uint32, to uint32) {
		// Atomic is necessary here as each file is partitioned on the Edge(from, to)'s "to"
		// Different workers may try to update the same "from" in a non-atomic fashion
		atomic.AddUint32(&degree[from], 1)
	}, 8)
	//
	// The first loop should have alpha set, else it's a noop as all src is equal to zero
	for i := range dest {
		src[i] = 1 - alpha
	}
	//
	for iter := 0; iter < 20; iter++ {
		log.Printf("PageRank Iteration: %d\n", iter+1)
		log.Printf("Calculating the source and destination vectors\n")
		for i := range dest {
			// If the node is dangling, src will equal +Inf due to degree being zero
			// As the result is not used elsewhere, this isn't so much a problem
			src[i] = alpha * (dest[i] / float32(degree[i]))
			dest[i] = 1 - alpha
		}
		log.Printf("Calculating the probability mass gifted by incoming edges\n")
		applyFunctionToEdges(func(from uint32, to uint32) {
			dest[to] += src[from]
		}, 8)
	}
	// Write result
	log.Printf("Saving results\n")
	outf, _ := os.Create("result.txt")
	defer outf.Close()
	w := bufio.NewWriter(outf)
	defer w.Flush()
	for i, v := range dest {
		w.WriteString(fmt.Sprintf("%d\t%f\n", i, v))
	}
	log.Printf("Saved results\n")
}
