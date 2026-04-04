package ratelimit

import (
	"sync"
	"testing"
	"time"
)

func TestTokenBucket_BasicRateLimit(t *testing.T) {
	// 1MB/s rate limit
	tb := NewTokenBucket(1_000_000)
	if tb == nil {
		t.Fatal("expected non-nil bucket")
	}

	start := time.Now()
	// Consume 500KB — should be instant (within burst capacity)
	tb.Wait(500_000)
	elapsed := time.Since(start)
	if elapsed > 100*time.Millisecond {
		t.Errorf("500KB within burst should be instant, took %v", elapsed)
	}
}

func TestTokenBucket_WaitBlocksWhenEmpty(t *testing.T) {
	// 100KB/s rate
	tb := NewTokenBucket(100_000)

	// Drain the bucket
	tb.Wait(100_000)

	// Next 100KB should take ~1 second
	start := time.Now()
	tb.Wait(100_000)
	elapsed := time.Since(start)

	if elapsed < 500*time.Millisecond {
		t.Errorf("expected wait >=500ms after draining, got %v", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Errorf("expected wait <3s, got %v (too slow)", elapsed)
	}
}

func TestTokenBucket_NilBucketNoOp(t *testing.T) {
	// Zero/negative rate returns nil bucket
	tb := NewTokenBucket(0)
	if tb != nil {
		t.Error("expected nil bucket for 0 rate")
	}
	tb = NewTokenBucket(-1)
	if tb != nil {
		t.Error("expected nil bucket for negative rate")
	}

	// Wait on nil bucket should not panic
	var nilBucket *TokenBucket
	nilBucket.Wait(1000) // should not panic
}

func TestTokenBucket_UpdateRate(t *testing.T) {
	tb := NewTokenBucket(1_000_000)

	// Update to 500B/s
	tb.UpdateRate(500)
	if tb.Rate() != 500 {
		t.Errorf("expected rate 500, got %d", tb.Rate())
	}
}

func TestTokenBucket_ConcurrentAccess(t *testing.T) {
	tb := NewTokenBucket(10_000_000) // 10MB/s

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tb.Wait(1000)
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Wait timed out — possible deadlock")
	}
}

func TestUserLimiter_SpeedTracking(t *testing.T) {
	ul := NewUserLimiter("speed-test@user", 1_000_000, 1_000_000)

	// Simulate traffic over a short window
	ul.TrackEgress(50_000)
	ul.TrackIngress(100_000)
	time.Sleep(150 * time.Millisecond) // let some time pass for meaningful speed calc

	eBps, iBps := ul.Speed()
	// Speed should be non-zero since we tracked bytes within the window
	if eBps <= 0 {
		t.Errorf("expected positive egress speed, got %d", eBps)
	}
	if iBps <= 0 {
		t.Errorf("expected positive ingress speed, got %d", iBps)
	}
}

func TestUserLimiter_NoLimitWhenZero(t *testing.T) {
	ul := NewUserLimiter("nolimit@user", 0, 0)
	if ul.Egress != nil {
		t.Error("expected nil egress limiter for 0 rate")
	}
	if ul.Ingress != nil {
		t.Error("expected nil ingress limiter for 0 rate")
	}
}
