package ratelimit

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// LoadFromConfigFile reads rate limit entries from the Xray config JSON.
// sx-ui injects a top-level "rateLimits" array into the config:
//
//	{
//	  "rateLimits": [
//	    {"email": "em-vm", "egressBps": 125000, "ingressBps": 125000},
//	    ...
//	  ],
//	  "inbounds": [...],
//	  ...
//	}
//
// This is called during Xray startup to pre-populate the Manager.
func LoadFromConfigFile(configPath string) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return
	}
	LoadFromJSON(data)
}

// LoadFromJSON parses the Xray config JSON and loads rate limits into Manager.
func LoadFromJSON(configJSON []byte) {
	var cfg struct {
		RateLimits []struct {
			Email                string `json:"email"`
			EgressBps            int64  `json:"egressBps"`
			IngressBps           int64  `json:"ingressBps"`
			BurstEgressBps       int64  `json:"burstEgressBps"`
			BurstIngressBps      int64  `json:"burstIngressBps"`
			BurstDurationSeconds int64  `json:"burstDurationSeconds"`
			BurstCooldownSeconds int64  `json:"burstCooldownSeconds"`
		} `json:"rateLimits"`
	}
	if err := json.Unmarshal(configJSON, &cfg); err != nil {
		return
	}

	for _, rl := range cfg.RateLimits {
		email := strings.TrimSpace(rl.Email)
		if email != "" && (rl.EgressBps > 0 || rl.IngressBps > 0) {
			Manager.SetWithBurst(
				email,
				rl.EgressBps,
				rl.IngressBps,
				rl.BurstEgressBps,
				rl.BurstIngressBps,
				rl.BurstDurationSeconds,
				rl.BurstCooldownSeconds,
			)
			fmt.Fprintf(os.Stderr, "[sx-core] ratelimit loaded: %s egress=%d ingress=%d burstEgress=%d burstIngress=%d bps\n", email, rl.EgressBps, rl.IngressBps, rl.BurstEgressBps, rl.BurstIngressBps)
		}
	}
	if len(cfg.RateLimits) > 0 {
		fmt.Fprintf(os.Stderr, "[sx-core] ratelimit: loaded %d entries from config\n", len(cfg.RateLimits))
	}
}
