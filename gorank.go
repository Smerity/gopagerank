package main

import "bufio"
import "encoding/binary"
import "flag"
import "fmt"
import "io"
import "log"
import "net/http"
import "os"
import "path/filepath"
import "runtime"
import "sync"
import "sync/atomic"
import _ "net/http/pprof"

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
	edgeStore := make([]uint64, 0, 524288)
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

func applyFunctionToEdges(filePrefix string, f func(uint32, uint32)) {
	var senderGroup sync.WaitGroup
	edgeFiles, _ := filepath.Glob(filePrefix + ".*.bin")
	for i, fn := range edgeFiles {
		senderGroup.Add(1)
		go func(fn string, i int) {
			sendEdges(fn, f)
			log.Printf("Completed processing part %d of %d -- (%s)\n", i, len(edgeFiles), fn)
			senderGroup.Done()
		}(fn, i)
	}
	//
	senderGroup.Wait()
}

func main() {
	// Start a server for pprof so we can get CPU profiles whilst running
	go func() {
		log.Println("Starting pprof server at localhost:6060...")
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	//
	filePrefix := flag.String("prefix", "pld-arc", "File prefix for the edge files")
	totalNodes := flag.Int("nodes", 42889799, "Total number of nodes in the graph")
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
	// We can either have total nodes supplied by the user or perform a full traversal of the data
	total := uint32(*totalNodes + 1)
	alpha := float64(0.85)
	saveResults := false
	//
	// floats must be 64 bit else we lose probability mass to floating point errors
	// This is annoying as it substantially increases our in-memory requirements ...
	src := make([]float64, total, total)
	dest := make([]float64, total, total)
	degree := make([]uint32, total, total)
	//
	log.Printf("Calculating degree of each source node\n")
	applyFunctionToEdges(*filePrefix, func(from uint32, to uint32) {
		// Atomic is necessary here as each file is partitioned on the Edge(from, to)'s "to"
		// Different workers may try to update the same "from" in a non-atomic fashion
		atomic.AddUint32(&degree[from], 1)
	})
	//
	// The first loop should have alpha set, else it's a noop as all src is equal to zero
	// We also distribute the starting probability mass s.t. it totals one
	for i := range dest {
		dest[i] = 1 / float64(total)
	}
	//
	for iter := 0; iter < 20; iter++ {
		log.Printf("PageRank Iteration: %d\n", iter+1)
		log.Printf("Calculating the source and destination vectors\n")
		// Calculate the probability mass that will be lost via dangling nodes
		missingProb := float64(0)
		for i := range degree {
			if degree[i] == 0 {
				missingProb += dest[i]
			}
		}
		// Calculate the starting values
		for i := range dest {
			// If the node is dangling, src will equal +Inf due to degree being zero
			// As the result is not used elsewhere, this isn't so much a problem
			src[i] = alpha * (dest[i] / float64(degree[i]))
			dest[i] = ((1 - alpha) / float64(total))
		}
		// Distribute the probability mass according to the edges
		log.Printf("Calculating the probability mass gifted by incoming edges\n")
		applyFunctionToEdges(*filePrefix, func(from uint32, to uint32) {
			dest[to] += src[from]
		})
		// Replace missing probability mass from dangling nodes
		// (assumption is that they were equally distributed to all nodes)
		for i := range dest {
			dest[i] += alpha * (missingProb / float64(total))
		}
	}
	// Write result
	if saveResults {
		log.Printf("Saving results\n")
		outf, _ := os.Create("result.txt")
		defer outf.Close()
		w := bufio.NewWriter(outf)
		defer w.Flush()
		for i, v := range dest {
			w.WriteString(fmt.Sprintf("%d\t%.12f\n", i, v))
		}
		log.Printf("Saved results\n")
	}
	//
	totalProb := float64(0)
	for _, v := range dest {
		totalProb += v
	}
	log.Printf("Total probability mass: %f\n", totalProb)
}
