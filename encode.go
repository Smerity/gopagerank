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
	outFile, _ := os.Create("./pld-arc.bin.gz")
	defer outFile.Close()
	outBuf := gzip.NewWriter(outFile)
	defer outBuf.Close()
	//
	// This is the buffer where encoding variable integers are placed
	var b = make([]byte, binary.MaxVarintLen64, binary.MaxVarintLen64)
	i := 0
	// We perform delta encoding (i.e. store the difference between previous and current value)
	// so we must keep track of what the previous edge value was
	prevEdge := uint64(0)
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
		bytesWritten := binary.PutUvarint(b, edge-prevEdge)
		binary.Write(outBuf, binary.LittleEndian, b[:bytesWritten])
		prevEdge = edge
		//
		i++
		if i%1000000 == 0 {
			fmt.Println("Edge ", i)
			fmt.Println("Bytes written:", bytesWritten)
		}
	}
	//
}
