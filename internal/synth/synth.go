// Package synth generates a seeded synthetic trip stream with configurable
// key skew. It exists for throughput tests and for when the TLC files are
// unavailable; results from it say nothing about real-world features.
package synth

import (
	"math"
	"math/rand"

	"github.com/sohamb17/streamforge/internal/event"
)

// Config for the generator.
type Config struct {
	Seed  int64
	Zones int     // number of zones (ids 1..Zones)
	Zipf  float64 // skew exponent s > 1 for Zipf; <= 1 means uniform
	// EventsPerMinute of event time (mean).
	EventsPerMinute float64
	StartMs         int64
}

// Generator yields events in event-time order.
type Generator struct {
	cfg  Config
	rng  *rand.Rand
	zipf *rand.Zipf
	t    float64
}

// New creates a generator.
func New(cfg Config) *Generator {
	if cfg.Zones <= 0 {
		cfg.Zones = 263
	}
	if cfg.EventsPerMinute <= 0 {
		cfg.EventsPerMinute = 6000
	}
	g := &Generator{cfg: cfg, rng: rand.New(rand.NewSource(cfg.Seed)), t: float64(cfg.StartMs)}
	if cfg.Zipf > 1 {
		g.zipf = rand.NewZipf(g.rng, cfg.Zipf, 1, uint64(cfg.Zones-1))
	}
	return g
}

// Next returns the next event.
func (g *Generator) Next() event.Trip {
	// Exponential inter-arrival times (a Poisson process in event time).
	g.t += g.rng.ExpFloat64() * 60000 / g.cfg.EventsPerMinute
	var zone int32
	if g.zipf != nil {
		zone = int32(g.zipf.Uint64()) + 1
	} else {
		zone = int32(g.rng.Intn(g.cfg.Zones)) + 1
	}
	dist := math.Exp(g.rng.NormFloat64()*0.8 + 0.6) // miles, log-normal
	fare := 3 + 2.8*dist + g.rng.Float64()*3
	return event.Trip{
		EventTimeMs:   int64(g.t),
		Zone:          zone,
		FareCents:     int64(fare * 100),
		DistanceMilli: int64(dist * 1000),
	}
}
