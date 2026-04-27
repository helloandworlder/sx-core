package ratelimit

import (
	"sync"
	"time"
)

// Manager is the global singleton that holds all per-user rate limiters.
// It is safe for concurrent use.
var Manager = &RateLimitManager{
	users: sync.Map{},
}

// RateLimitManager maps user emails to their rate limiters.
type RateLimitManager struct {
	users sync.Map // map[string]*UserLimiter
}

// Set creates or updates the rate limit for a user.
func (m *RateLimitManager) Set(email string, egressBps, ingressBps int64) *UserLimiter {
	return m.SetWithBurst(email, egressBps, ingressBps, 0, 0, 0, 0)
}

// SetWithBurst creates or updates the rate limit for one XrayCore client email.
func (m *RateLimitManager) SetWithBurst(
	email string,
	egressBps int64,
	ingressBps int64,
	burstEgressBps int64,
	burstIngressBps int64,
	burstDurationSeconds int64,
	burstCooldownSeconds int64,
) *UserLimiter {
	if egressBps <= 0 && ingressBps <= 0 {
		m.users.Delete(email)
		return nil
	}
	if burstDurationSeconds < 0 {
		burstDurationSeconds = 0
	}
	if burstCooldownSeconds < 0 {
		burstCooldownSeconds = 0
	}
	burstDuration := time.Duration(burstDurationSeconds) * time.Second
	burstCooldown := time.Duration(burstCooldownSeconds) * time.Second

	existing, ok := m.users.Load(email)
	if ok {
		ul := existing.(*UserLimiter)
		if ul.Egress != nil && egressBps > 0 {
			ul.Egress.UpdateConfig(egressBps, burstEgressBps, burstDuration, burstCooldown)
		} else if egressBps > 0 {
			ul.Egress = NewTokenBucketWithBurst(egressBps, burstEgressBps, burstDuration, burstCooldown)
		} else {
			ul.Egress = nil
		}
		if ul.Ingress != nil && ingressBps > 0 {
			ul.Ingress.UpdateConfig(ingressBps, burstIngressBps, burstDuration, burstCooldown)
		} else if ingressBps > 0 {
			ul.Ingress = NewTokenBucketWithBurst(ingressBps, burstIngressBps, burstDuration, burstCooldown)
		} else {
			ul.Ingress = nil
		}
		return ul
	}

	ul := NewUserLimiterWithBurst(
		email,
		egressBps,
		ingressBps,
		burstEgressBps,
		burstIngressBps,
		burstDuration,
		burstCooldown,
	)
	m.users.Store(email, ul)
	return ul
}

// Get returns the limiter for the given email, or nil if not set.
func (m *RateLimitManager) Get(email string) *UserLimiter {
	v, ok := m.users.Load(email)
	if !ok {
		return nil
	}
	return v.(*UserLimiter)
}

// Remove deletes the rate limit for a user.
func (m *RateLimitManager) Remove(email string) {
	m.users.Delete(email)
}

// ListAll returns all user limiters with their current settings and speeds.
func (m *RateLimitManager) ListAll() []UserSpeedInfo {
	var result []UserSpeedInfo
	m.users.Range(func(key, value any) bool {
		email := key.(string)
		ul := value.(*UserLimiter)
		eSpeed, iSpeed := ul.Speed()
		info := UserSpeedInfo{
			Email:      email,
			EgressBps:  eSpeed,
			IngressBps: iSpeed,
		}
		if ul.Egress != nil {
			info.EgressLimitBps = ul.Egress.BaseRate()
			info.BurstEgressLimitBps, info.BurstDuration, info.BurstCooldown = ul.Egress.BurstConfig()
		}
		if ul.Ingress != nil {
			info.IngressLimitBps = ul.Ingress.BaseRate()
			info.BurstIngressLimitBps, info.BurstDuration, info.BurstCooldown = ul.Ingress.BurstConfig()
		}
		info.BurstDurationSeconds = int64(info.BurstDuration / time.Second)
		info.BurstCooldownSeconds = int64(info.BurstCooldown / time.Second)
		result = append(result, info)
		return true
	})
	return result
}

// UserSpeedInfo holds rate limit config and real-time speed for one user.
type UserSpeedInfo struct {
	Email                string        `json:"email"`
	EgressLimitBps       int64         `json:"egressLimitBps"`
	IngressLimitBps      int64         `json:"ingressLimitBps"`
	BurstEgressLimitBps  int64         `json:"burstEgressLimitBps"`
	BurstIngressLimitBps int64         `json:"burstIngressLimitBps"`
	BurstDuration        time.Duration `json:"-"`
	BurstCooldown        time.Duration `json:"-"`
	BurstDurationSeconds int64         `json:"burstDurationSeconds"`
	BurstCooldownSeconds int64         `json:"burstCooldownSeconds"`
	EgressBps            int64         `json:"egressBps"`  // current speed
	IngressBps           int64         `json:"ingressBps"` // current speed
}
