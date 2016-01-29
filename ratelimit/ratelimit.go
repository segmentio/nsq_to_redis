package ratelimit

import (
	"github.com/hashicorp/golang-lru"
	"github.com/juju/ratelimit"
)

// Ratelimiter implements a simple rate limiter.
type Ratelimiter struct {
	keys *lru.Cache
	rate int64
}

// New initializes a new Ratelimiter
// with rate per second and lru key cache size.
func New(rate, size int) *Ratelimiter {
	c, _ := lru.New(size)
	return &Ratelimiter{
		keys: c,
		rate: int64(rate),
	}
}

// Exceeded returns true if the given `key`
// has exceeded rate per second.
func (rl *Ratelimiter) Exceeded(key string) bool {
	if b, ok := rl.keys.Get(key); ok {
		b := b.(*ratelimit.Bucket)
		_, took := b.TakeMaxDuration(1, 0)
		return !took
	}

	rate := float64(rl.rate)
	cap := rl.rate
	b := ratelimit.NewBucketWithRate(rate, cap)
	rl.keys.Add(key, b)
	b.Take(1)
	return false
}
