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

const (
	// Keep only a very small scheduling tolerance so throughput stays smooth.
	defaultBurstWindow = 50 * time.Millisecond
	// Pace traffic in coarse slices to reduce wakeups and lock contention.
	defaultPacingWindow = 100 * time.Millisecond
	minChunkBytes       = 4 * 1024
)

// TokenBucket implements a classic token bucket rate limiter.
// It is safe for concurrent use.
type TokenBucket struct {
	rate          int64 // bytes added per second
	capacity      int64 // max scheduling tolerance in bytes
	quantum       int64 // preferred paced chunk size in bytes
	nextAvailable int64 // unix nanoseconds when the next chunk may be sent
	mu            sync.Mutex
}

// NewTokenBucket creates a token bucket with the given rate (bytes/sec).
// Internally it behaves as a paced leaky bucket with a tiny burst tolerance.
func NewTokenBucket(bytesPerSec int64) *TokenBucket {
	if bytesPerSec <= 0 {
		return nil // no limit
	}
	return &TokenBucket{
		rate:          bytesPerSec,
		capacity:      burstBytes(bytesPerSec),
		quantum:       pacingQuantum(bytesPerSec),
		nextAvailable: time.Now().UnixNano(),
	}
}

// Wait blocks until n bytes may be sent. Most calls sleep at most once; only
// unusually large buffers are split into paced chunks.
func (tb *TokenBucket) Wait(n int64) {
	if tb == nil || n <= 0 {
		return
	}

	for n > 0 {
		chunk := n
		if tb.quantum > 0 && chunk > tb.quantum {
			chunk = tb.quantum
		}
		sleepFor := tb.reserve(chunk)
		if sleepFor > 0 {
			time.Sleep(sleepFor)
		}
		n -= chunk
	}
}

func (tb *TokenBucket) reserve(chunk int64) time.Duration {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now().UnixNano()
	if tb.nextAvailable < now-int64(defaultBurstWindow) {
		tb.nextAvailable = now - int64(defaultBurstWindow)
	}

	startAt := tb.nextAvailable
	if startAt < now {
		startAt = now
	}
	serviceNs := chunkDurationNs(chunk, tb.rate)
	tb.nextAvailable = startAt + serviceNs

	if startAt <= now {
		return 0
	}
	return time.Duration(startAt - now)
}

func chunkDurationNs(chunk, rate int64) int64 {
	if chunk <= 0 || rate <= 0 {
		return 0
	}
	ns := (chunk * int64(time.Second)) / rate
	if ns <= 0 {
		return 1
	}
	return ns
}

func burstBytes(rate int64) int64 {
	burst := (rate * int64(defaultBurstWindow)) / int64(time.Second)
	if burst < minChunkBytes {
		burst = minChunkBytes
	}
	if burst > rate {
		burst = rate
	}
	if burst <= 0 {
		return 1
	}
	return burst
}

func pacingQuantum(rate int64) int64 {
	quantum := (rate * int64(defaultPacingWindow)) / int64(time.Second)
	if quantum < minChunkBytes {
		quantum = minChunkBytes
	}
	if quantum <= 0 {
		quantum = 1
	}
	return quantum
}

func (tb *TokenBucket) refillLocked() {
	// No-op: pacing is managed via nextAvailable scheduling.
}

// UpdateRate changes the rate limit dynamically.
func (tb *TokenBucket) UpdateRate(bytesPerSec int64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.rate = bytesPerSec
	tb.capacity = burstBytes(bytesPerSec)
	tb.quantum = pacingQuantum(bytesPerSec)
	now := time.Now().UnixNano()
	if tb.nextAvailable < now-int64(defaultBurstWindow) {
		tb.nextAvailable = now - int64(defaultBurstWindow)
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
