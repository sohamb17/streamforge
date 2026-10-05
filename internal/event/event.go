// Package event defines the trip event written to Kafka.
//
// Wire format (little endian, 37 bytes):
//
//	[0]      version = 1
//	[1:9]    event time, ms since epoch (pickup time, shifted per replay loop)
//	[9:13]   pickup zone id
//	[13:21]  fare, cents
//	[21:29]  trip distance, miles * 1000
//	[29:37]  publish wall time, ms since epoch (freshness metrics only)
//
// The Kafka record key is the zone id, so every event of a zone lands in
// the same partition and per-zone windows can be computed per partition.
package event

import (
	"encoding/binary"
	"errors"
	"strconv"
)

// Trip is one trip-start event.
type Trip struct {
	EventTimeMs   int64
	Zone          int32
	FareCents     int64
	DistanceMilli int64
	PublishMs     int64
}

const size = 37

// ErrBadEvent is returned for malformed payloads.
var ErrBadEvent = errors.New("event: malformed payload")

// Encode serializes a trip.
func Encode(t Trip) []byte {
	b := make([]byte, size)
	b[0] = 1
	binary.LittleEndian.PutUint64(b[1:], uint64(t.EventTimeMs))
	binary.LittleEndian.PutUint32(b[9:], uint32(t.Zone))
	binary.LittleEndian.PutUint64(b[13:], uint64(t.FareCents))
	binary.LittleEndian.PutUint64(b[21:], uint64(t.DistanceMilli))
	binary.LittleEndian.PutUint64(b[29:], uint64(t.PublishMs))
	return b
}

// Decode parses a trip.
func Decode(b []byte) (Trip, error) {
	if len(b) != size || b[0] != 1 {
		return Trip{}, ErrBadEvent
	}
	return Trip{
		EventTimeMs:   int64(binary.LittleEndian.Uint64(b[1:])),
		Zone:          int32(binary.LittleEndian.Uint32(b[9:])),
		FareCents:     int64(binary.LittleEndian.Uint64(b[13:])),
		DistanceMilli: int64(binary.LittleEndian.Uint64(b[21:])),
		PublishMs:     int64(binary.LittleEndian.Uint64(b[29:])),
	}, nil
}

// Key returns the Kafka record key for a zone.
func Key(zone int32) []byte { return []byte(strconv.Itoa(int(zone))) }
