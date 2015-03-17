package main

import "bufio"
import "compress/gzip"
import "encoding/binary"
import "fmt"
import "os"
import "runtime"

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

func PutU(b []byte, x uint64) int {
	i := 0
	shifts := []uint8{56, 48, 40, 32, 24, 16, 8}
	for _, shift := range shifts {
		if (x >> uint8(shift)) != 0 {
			b[i] = 0
			i += 1
		}
	}
	for _, shift := range shifts {
		if (x >> uint8(shift)) != 0 {
			b[i] = byte(x >> uint8(shift))
			i += 1
		}
	}
	b[i] = byte(x)
	i += 1
	return i
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	//
	f, _ := os.Open("pld-arc.gz")
	defer f.Close()
	gunzip, _ := gzip.NewReader(f)
	defer gunzip.Close()
	scanner := bufio.NewScanner(gunzip)
	// Split on any whitespace (which includes field separator \t and newline \n)
	scanner.Split(bufio.ScanWords)
	//
	outTotal := 8
	outFiles := make([]*os.File, outTotal, outTotal)
	outBufs := make([]*gzip.Writer, outTotal, outTotal)
	prevEdges := make([]uint64, outTotal, outTotal)
	for i := 0; i < outTotal; i++ {
		outFiles[i], _ = os.Create(fmt.Sprintf("./pld-arc.%d.bin.gz", i))
		outBufs[i], _ = gzip.NewWriterLevel(outFiles[i], gzip.BestCompression)
	}
	//
	// This is the buffer where encoding variable integers are placed
	//var b = make([]byte, binary.MaxVarintLen64, binary.MaxVarintLen64)
	var b = make([]byte, 16)
	i := 0
	// We perform delta encoding (i.e. store the difference between previous and current value)
	// so we must keep track of what the previous edge value was
	for scanner.Scan() {
		eFrom := bytesToUint32(scanner.Bytes())
		scanner.Scan()
		eTo := bytesToUint32(scanner.Bytes())
		// Store an edge as a 64 bit unsigned integer - first 32 bits are from, second 32 are destination
		edge := uint64(eFrom)<<32 + uint64(eTo)
		/*
			// Sanity check to ensure we can recover the original edge data
				if uint32(edge&0xFFFFFFFF) != eTo {
					fmt.Println("uh oh eTo")
				}
				if uint32(edge>>32) != eFrom {
					fmt.Println("uh oh eFrom")
				}
		*/
		// Store only the difference (delta encoding)
		bucket := eFrom % uint32(outTotal)
		bytesWritten := binary.PutUvarint(b, edge-prevEdges[bucket])
		//bytesWritten := PutU(b, edge-prevEdges[bucket])
		binary.Write(outBufs[bucket], binary.LittleEndian, b[:bytesWritten])
		prevEdges[bucket] = edge
		//
		i++
		if i%1e6 == 0 {
			fmt.Println("Edge ", i)
			fmt.Println("Bytes written:", bytesWritten)
		}
	}
	//
	for i := 0; i < outTotal; i++ {
		outBufs[i].Close()
		outFiles[i].Close()
	}
}
