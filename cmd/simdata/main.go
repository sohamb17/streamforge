// Command simdata cuts a slice of the TLC month into the compact file the
// in-browser simulation replays (internal/simdemo/trips.bin.gz).
//
// Format (then gzip): int64 base time (ms, little endian), then per trip
// uvarint seconds since previous trip, zone, fare cents, distance in
// hundredths of a mile.
package main

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/sohamb17/streamforge/internal/tlc"
)

func main() {
	file := flag.String("tlc-file", "data/yellow_tripdata_2026-03.parquet", "TLC file")
	from := flag.String("from", "2026-03-06T16:00:00Z", "start (RFC3339)")
	to := flag.String("to", "2026-03-06T21:00:00Z", "end (RFC3339)")
	out := flag.String("out", "internal/simdemo/trips.bin.gz", "output")
	flag.Parse()
	f, _ := time.Parse(time.RFC3339, *from)
	t, _ := time.Parse(time.RFC3339, *to)
	trips, _, err := tlc.Load(*file, time.Date(f.Year(), f.Month(), 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		panic(err)
	}
	var raw bytes.Buffer
	binary.Write(&raw, binary.LittleEndian, f.UnixMilli())
	prev := f.UnixMilli() / 1000
	n := 0
	buf := make([]byte, binary.MaxVarintLen64)
	for _, tr := range trips {
		if tr.EventTimeMs < f.UnixMilli() || tr.EventTimeMs >= t.UnixMilli() {
			continue
		}
		s := tr.EventTimeMs / 1000
		for _, v := range []uint64{uint64(s - prev), uint64(tr.Zone), uint64(tr.FareCents), uint64(tr.DistanceMilli / 10)} {
			raw.Write(buf[:binary.PutUvarint(buf, v)])
		}
		prev = s
		n++
	}
	var gz bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&gz, gzip.BestCompression)
	zw.Write(raw.Bytes())
	zw.Close()
	if err := os.WriteFile(*out, gz.Bytes(), 0o644); err != nil {
		panic(err)
	}
	fmt.Printf("%d trips from %s to %s: %d bytes raw, %d gzipped\n", n, *from, *to, raw.Len(), gz.Len())
}
