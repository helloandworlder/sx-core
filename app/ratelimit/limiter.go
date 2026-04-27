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
	rate             int64 // base bytes added per second
	burstRate        int64 // temporary burst bytes per second
	burstDuration    time.Duration
	burstCooldown    time.Duration
	burstActiveUntil int64 // unix nanoseconds
	nextBurstAt      int64 // unix nanoseconds
	capacity         int64 // max scheduling tolerance in bytes
	quantum          int64 // preferred paced chunk size in bytes
	nextAvailable    int64 // unix nanoseconds when the next chunk may be sent
	mu               sync.Mutex
}

// NewTokenBucket creates a token bucket with the given rate (bytes/sec).
// Internally it behaves as a paced leaky bucket with a tiny burst tolerance.
func NewTokenBucket(bytesPerSec int64) *TokenBucket {
	return NewTokenBucketWithBurst(bytesPerSec, 0, 0, 0)
}

// NewTokenBucketWithBurst creates a token bucket with an optional temporary
// burst rate. The burst window starts on first activity after cooldown and is
// shared by all connections using this bucket.
func NewTokenBucketWithBurst(
	bytesPerSec int64,
	burstBytesPerSec int64,
	burstDuration time.Duration,
	burstCooldown time.Duration,
) *TokenBucket {
	if bytesPerSec <= 0 {
		return nil // no limit
	}
	if burstBytesPerSec <= bytesPerSec || burstDuration <= 0 {
		burstBytesPerSec = 0
		burstDuration = 0
		burstCooldown = 0
	}
	return &TokenBucket{
		rate:          bytesPerSec,
		burstRate:     burstBytesPerSec,
		burstDuration: burstDuration,
		burstCooldown: burstCooldown,
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
	rate := tb.effectiveRateLocked(now)

	startAt := tb.nextAvailable
	if startAt < now {
		startAt = now
	}
	serviceNs := chunkDurationNs(chunk, rate)
	tb.nextAvailable = startAt + serviceNs

	if startAt <= now {
		return 0
	}
	return time.Duration(startAt - now)
}

func (tb *TokenBucket) effectiveRateLocked(now int64) int64 {
	if tb.burstRate <= tb.rate || tb.burstDuration <= 0 {
		return tb.rate
	}
	if tb.burstActiveUntil > now {
		return tb.burstRate
	}
	if tb.nextBurstAt <= now {
		tb.burstActiveUntil = now + int64(tb.burstDuration)
		tb.nextBurstAt = tb.burstActiveUntil + int64(tb.burstCooldown)
		return tb.burstRate
	}
	return tb.rate
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
	tb.UpdateConfig(bytesPerSec, 0, 0, 0)
}

// UpdateConfig changes the base and burst rate limits dynamically.
func (tb *TokenBucket) UpdateConfig(
	bytesPerSec int64,
	burstBytesPerSec int64,
	burstDuration time.Duration,
	burstCooldown time.Duration,
) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.rate = bytesPerSec
	if burstBytesPerSec <= bytesPerSec || burstDuration <= 0 {
		burstBytesPerSec = 0
		burstDuration = 0
		burstCooldown = 0
		tb.burstActiveUntil = 0
		tb.nextBurstAt = 0
	}
	tb.burstRate = burstBytesPerSec
	tb.burstDuration = burstDuration
	tb.burstCooldown = burstCooldown
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
	return tb.effectiveRateLocked(time.Now().UnixNano())
}

// BaseRate returns the configured non-burst rate in bytes/sec.
func (tb *TokenBucket) BaseRate() int64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.rate
}

// BurstConfig returns the configured burst rate and window.
func (tb *TokenBucket) BurstConfig() (burstBytesPerSec int64, duration time.Duration, cooldown time.Duration) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.burstRate, tb.burstDuration, tb.burstCooldown
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
	return NewUserLimiterWithBurst(email, egressBps, ingressBps, 0, 0, 0, 0)
}

// NewUserLimiterWithBurst creates a limiter pair keyed by XrayCore client email.
func NewUserLimiterWithBurst(
	email string,
	egressBps int64,
	ingressBps int64,
	burstEgressBps int64,
	burstIngressBps int64,
	burstDuration time.Duration,
	burstCooldown time.Duration,
) *UserLimiter {
	ul := &UserLimiter{
		Email: email,
	}
	if egressBps > 0 {
		ul.Egress = NewTokenBucketWithBurst(egressBps, burstEgressBps, burstDuration, burstCooldown)
	}
	if ingressBps > 0 {
		ul.Ingress = NewTokenBucketWithBurst(ingressBps, burstIngressBps, burstDuration, burstCooldown)
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
