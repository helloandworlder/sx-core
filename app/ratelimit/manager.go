package ratelimit

import (
	"sync"
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
	if egressBps <= 0 && ingressBps <= 0 {
		m.users.Delete(email)
		return nil
	}

	existing, ok := m.users.Load(email)
	if ok {
		ul := existing.(*UserLimiter)
		if ul.Egress != nil && egressBps > 0 {
			ul.Egress.UpdateRate(egressBps)
		} else if egressBps > 0 {
			ul.Egress = NewTokenBucket(egressBps)
		} else {
			ul.Egress = nil
		}
		if ul.Ingress != nil && ingressBps > 0 {
			ul.Ingress.UpdateRate(ingressBps)
		} else if ingressBps > 0 {
			ul.Ingress = NewTokenBucket(ingressBps)
		} else {
			ul.Ingress = nil
		}
		return ul
	}

	ul := NewUserLimiter(email, egressBps, ingressBps)
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
			info.EgressLimitBps = ul.Egress.Rate()
		}
		if ul.Ingress != nil {
			info.IngressLimitBps = ul.Ingress.Rate()
		}
		result = append(result, info)
		return true
	})
	return result
}

// UserSpeedInfo holds rate limit config and real-time speed for one user.
type UserSpeedInfo struct {
	Email           string `json:"email"`
	EgressLimitBps  int64  `json:"egressLimitBps"`
	IngressLimitBps int64  `json:"ingressLimitBps"`
	EgressBps       int64  `json:"egressBps"`  // current speed
	IngressBps      int64  `json:"ingressBps"` // current speed
}
