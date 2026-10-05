// Package tlc loads NYC TLC yellow-taxi trip records (monthly Parquet files)
// and turns them into a cleaned, time-ordered event stream.
//
// Cleaning rules (documented in data/MANIFEST.md, and reported per load):
//   - pickup time must fall inside the file's month,
//   - pickup zone must be a real zone (1..263; 264/265 are "unknown"),
//   - fare must be in [0, 500] dollars, distance in [0, 100] miles.
package tlc

import (
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/sohamb17/streamforge/internal/event"
)

type row struct {
	Pickup   int64   `parquet:"tpep_pickup_datetime,timestamp(microsecond)"`
	Zone     int32   `parquet:"PULocationID"`
	Fare     float64 `parquet:"fare_amount"`
	Distance float64 `parquet:"trip_distance"`
}

// Stats describes what a load kept and dropped.
type Stats struct {
	Rows, Kept, BadTime, BadZone, BadFare, BadDistance int
}

// Load reads one monthly file. month is the first instant of the month (UTC,
// matching the TLC's naive local timestamps, which are treated as UTC
// consistently everywhere in StreamForge).
func Load(path string, month time.Time) ([]event.Trip, Stats, error) {
	var st Stats
	f, err := os.Open(path)
	if err != nil {
		return nil, st, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, st, err
	}
	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		return nil, st, err
	}
	r := parquet.NewGenericReader[row](pf)
	defer r.Close()
	start := month.UnixMilli()
	end := month.AddDate(0, 1, 0).UnixMilli()
	out := make([]event.Trip, 0, pf.NumRows())
	buf := make([]row, 8192)
	for {
		n, err := r.Read(buf)
		for _, x := range buf[:n] {
			st.Rows++
			ms := x.Pickup / 1000
			switch {
			case ms < start || ms >= end:
				st.BadTime++
			case x.Zone < 1 || x.Zone > 263:
				st.BadZone++
			case x.Fare < 0 || x.Fare > 500:
				st.BadFare++
			case x.Distance < 0 || x.Distance > 100:
				st.BadDistance++
			default:
				out = append(out, event.Trip{
					EventTimeMs:   ms,
					Zone:          x.Zone,
					FareCents:     int64(x.Fare*100 + 0.5),
					DistanceMilli: int64(x.Distance*1000 + 0.5),
				})
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, st, fmt.Errorf("tlc: read %s: %w", path, err)
		}
	}
	st.Kept = len(out)
	// Stable sort by event time keeps the file's order among equal times,
	// so the stream is identical on every run.
	sort.SliceStable(out, func(i, j int) bool { return out[i].EventTimeMs < out[j].EventTimeMs })
	return out, st, nil
}
