package ratelimit

// This file exposes helper functions around the global Manager.
// The actual gRPC API is implemented in app/ratelimit/command so Commander can
// register it into Xray's API server.

// SetUserRateLimit sets or updates the rate limit for a user.
func SetUserRateLimit(email string, egressBps, ingressBps int64) {
	Manager.Set(email, egressBps, ingressBps)
}

// SetUserRateLimitWithBurst sets or updates the rate limit and burst window for a user.
func SetUserRateLimitWithBurst(
	email string,
	egressBps int64,
	ingressBps int64,
	burstEgressBps int64,
	burstIngressBps int64,
	burstDurationSeconds int64,
	burstCooldownSeconds int64,
) {
	Manager.SetWithBurst(
		email,
		egressBps,
		ingressBps,
		burstEgressBps,
		burstIngressBps,
		burstDurationSeconds,
		burstCooldownSeconds,
	)
}

// GetUserRateLimit returns the current rate limit and speed for a user.
// Returns nil if no limit is set.
func GetUserRateLimit(email string) *UserSpeedInfo {
	ul := Manager.Get(email)
	if ul == nil {
		return nil
	}
	eSpeed, iSpeed := ul.Speed()
	info := &UserSpeedInfo{
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
	info.BurstDurationSeconds = int64(info.BurstDuration.Seconds())
	info.BurstCooldownSeconds = int64(info.BurstCooldown.Seconds())
	return info
}

// RemoveUserRateLimit removes the rate limit for a user.
func RemoveUserRateLimit(email string) {
	Manager.Remove(email)
}

// GetUserSpeed returns the current real-time speed for a user.
func GetUserSpeed(email string) (egressBps, ingressBps int64) {
	ul := Manager.Get(email)
	if ul == nil {
		return 0, 0
	}
	return ul.Speed()
}

// ListUserSpeeds returns all users' speeds and rate limit settings.
func ListUserSpeeds() []UserSpeedInfo {
	return Manager.ListAll()
}
