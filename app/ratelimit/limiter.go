// Package ratelimit provides per-user bandwidth rate limiting for XrayCore.
//
// It implements a token bucket algorithm that wraps transport.Link reader/writer
// to enforce ingress and egress bandwidth limits keyed by user email.
package ratelimit

import (
	"sync"
	"sync/atomic"
	"time"
)

// TokenBucket implements a classic token bucket rate limiter.
// It is safe for concurrent use.
type TokenBucket struct {
	rate       int64 // tokens (bytes) added per second
	capacity   int64 // max burst size
	tokens     int64 // current available tokens
	lastRefill int64 // unix nanoseconds of last refill
	mu         sync.Mutex
}

// NewTokenBucket creates a token bucket with the given rate (bytes/sec).
// Burst capacity is set to rate (1 second of buffering).
func NewTokenBucket(bytesPerSec int64) *TokenBucket {
	if bytesPerSec <= 0 {
		return nil // no limit
	}
	return &TokenBucket{
		rate:       bytesPerSec,
		capacity:   bytesPerSec, // 1s burst
		tokens:     bytesPerSec, // start full
		lastRefill: time.Now().UnixNano(),
	}
}

// Wait blocks until n tokens (bytes) are available, consuming them.
// For large transfers, it may wait in chunks to avoid long blocking.
func (tb *TokenBucket) Wait(n int64) {
	if tb == nil || n <= 0 {
		return
	}

	for n > 0 {
		tb.mu.Lock()
		tb.refillLocked()
		consume := minInt64(n, tb.tokens)
		if consume > 0 {
			tb.tokens -= consume
			n -= consume
		}
		rate := tb.rate
		tb.mu.Unlock()

		if n > 0 {
			waitNs := (n * int64(time.Second)) / rate
			if waitNs < int64(time.Millisecond) {
				waitNs = int64(time.Millisecond)
			}
			if waitNs > int64(100*time.Millisecond) {
				waitNs = int64(100 * time.Millisecond)
			}
			time.Sleep(time.Duration(waitNs))
		}
	}
}

func (tb *TokenBucket) refillLocked() {
	now := time.Now().UnixNano()
	elapsed := now - tb.lastRefill
	if elapsed <= 0 {
		return
	}

	newTokens := (elapsed * tb.rate) / int64(time.Second)
	if newTokens <= 0 {
		return
	}

	tb.tokens += newTokens
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
	tb.lastRefill = now
}

// UpdateRate changes the rate limit dynamically.
func (tb *TokenBucket) UpdateRate(bytesPerSec int64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.rate = bytesPerSec
	tb.capacity = bytesPerSec
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
}

// Rate returns the current rate in bytes/sec.
func (tb *TokenBucket) Rate() int64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.rate
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// UserLimiter holds the ingress and egress limiters for a single user.
type UserLimiter struct {
	Email   string
	Egress  *TokenBucket // upload from user perspective (download from proxy)
	Ingress *TokenBucket // download from user perspective (upload from proxy)

	// Real-time speed tracking (rolling 1-second window)
	egressBytes  atomic.Int64
	ingressBytes atomic.Int64
	lastReset    atomic.Int64
}

// NewUserLimiter creates a rate limiter pair for one user.
func NewUserLimiter(email string, egressBps, ingressBps int64) *UserLimiter {
	ul := &UserLimiter{
		Email: email,
	}
	if egressBps > 0 {
		ul.Egress = NewTokenBucket(egressBps)
	}
	if ingressBps > 0 {
		ul.Ingress = NewTokenBucket(ingressBps)
	}
	ul.lastReset.Store(time.Now().UnixNano())
	return ul
}

// TrackEgress records bytes for real-time speed measurement.
func (ul *UserLimiter) TrackEgress(n int64) {
	ul.maybeReset()
	ul.egressBytes.Add(n)
}

// TrackIngress records bytes for real-time speed measurement.
func (ul *UserLimiter) TrackIngress(n int64) {
	ul.maybeReset()
	ul.ingressBytes.Add(n)
}

// Speed returns the approximate current speed in bytes/sec for both directions.
func (ul *UserLimiter) Speed() (egressBps, ingressBps int64) {
	now := time.Now().UnixNano()
	last := ul.lastReset.Load()
	elapsed := now - last
	if elapsed <= 0 {
		return 0, 0
	}
	secs := float64(elapsed) / float64(time.Second)
	if secs < 0.1 {
		secs = 0.1
	}
	return int64(float64(ul.egressBytes.Load()) / secs),
		int64(float64(ul.ingressBytes.Load()) / secs)
}

func (ul *UserLimiter) maybeReset() {
	now := time.Now().UnixNano()
	last := ul.lastReset.Load()
	if now-last > int64(time.Second) {
		if ul.lastReset.CompareAndSwap(last, now) {
			ul.egressBytes.Store(0)
			ul.ingressBytes.Store(0)
		}
	}
}
