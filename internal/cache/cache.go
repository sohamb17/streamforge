// Package cache is the Redis read-through cache in front of the online
// store. Redis is never a source of truth: entries expire after a short TTL
// and are deleted when a newer feature row commits. A reader can therefore
// see a value at most TTL older than the committed state (and that is the
// staleness bound the serving API reports for cache hits).
package cache

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	sfv1 "github.com/sohamb17/streamforge/gen/streamforge/v1"
)

// Cache wraps a Redis client.
type Cache struct {
	R   *redis.Client
	TTL time.Duration
}

func key(zone int32) string { return "fv:zone_demand_v1:" + strconv.Itoa(int(zone)) }

// Get returns cached rows for zones; misses are absent from the map. A
// Redis error is treated as all misses (the cache is optional).
func (c *Cache) Get(ctx context.Context, zones []int32) (map[int32]*sfv1.ZoneFeatures, error) {
	keys := make([]string, len(zones))
	for i, z := range zones {
		keys[i] = key(z)
	}
	vals, err := c.R.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[int32]*sfv1.ZoneFeatures, len(zones))
	for i, v := range vals {
		s, ok := v.(string)
		if !ok {
			continue
		}
		f := &sfv1.ZoneFeatures{}
		if proto.Unmarshal([]byte(s), f) == nil {
			out[zones[i]] = f
		}
	}
	return out, nil
}

// Set stores rows with the TTL.
func (c *Cache) Set(ctx context.Context, rows []*sfv1.ZoneFeatures) error {
	pipe := c.R.Pipeline()
	for _, f := range rows {
		b, err := proto.Marshal(f)
		if err != nil {
			return err
		}
		pipe.Set(ctx, key(f.Zone), b, c.TTL)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Invalidate deletes entries; implements worker.Invalidator. Errors are
// ignored on purpose: the TTL bounds staleness if a delete is lost.
func (c *Cache) Invalidate(ctx context.Context, zones []int32) {
	keys := make([]string, len(zones))
	for i, z := range zones {
		keys[i] = key(z)
	}
	c.R.Del(ctx, keys...)
}
