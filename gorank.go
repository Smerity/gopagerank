package main

import "bufio"
import "compress/gzip"
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

func ReadU(r io.ByteReader) (uint64, error) {
	var x uint64
	var count int
	//
	b, err := r.ReadByte()
	if err != nil {
		return x, err
	}
	for b == 0 {
		b, err = r.ReadByte()
		if err != nil {
			return x, err
		}
		count += 1
	}
	//
	for c := 0; c < count; c++ {
		x = (x << 8) + uint64(b)
		b, err = r.ReadByte()
		if err != nil {
			return x, err
		}
	}
	x = (x << 8) + uint64(b)
	return x, nil
}

func sendEdges(filename string, hashOnSource bool, chans [](chan Edge), senderGroup *sync.WaitGroup) {
	defer senderGroup.Done()
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
		//rawEdge, err := ReadU(wrappedByteReader)
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
}

func applyFunctionToEdges(f func(c chan Edge), workers int, hashOnSource bool) {
	// The work for each of the workers is deposited onto their channel
	chans := make([]chan Edge, workers, workers)
	for i := range chans {
		chans[i] = make(chan Edge, 1024)
	}
	//
	var senderGroup sync.WaitGroup
	for i := 0; i < 8; i++ {
		senderGroup.Add(1)
		go sendEdges(fmt.Sprintf("pld-arc.%d.bin.gz", i), hashOnSource, chans, &senderGroup)
	}
	//
	var readerGroup sync.WaitGroup
	for _, c := range chans {
		readerGroup.Add(1)
		go func(c chan Edge) {
			defer readerGroup.Done()
			f(c)
		}(c)
	}
	//
	senderGroup.Wait()
	for _, c := range chans {
		close(c)
	}
	readerGroup.Wait()
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
	}, 8, true)
	//
	for iter := 0; iter < 20; iter++ {
		log.Printf("PageRank Iteration: %d\n", iter+1)
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
		}, 8, false)
	}
}
