package ratelimit

import "testing"

func TestBandwidthConstants(t *testing.T) {
	tests := []struct {
		name     string
		value    int64
		expected int64
	}{
		{"1 Kbps", 1 * Kbps, 125},
		{"100 Kbps", 100 * Kbps, 12_500},
		{"1 Mbps", 1 * Mbps, 125_000},
		{"100 Mbps", 100 * Mbps, 12_500_000},
		{"1 Gbps", 1 * Gbps, 125_000_000},
		{"10 Gbps", 10 * Gbps, 1_250_000_000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.value != tt.expected {
				t.Errorf("got %d bytes/sec, want %d", tt.value, tt.expected)
			}
		})
	}
}

func TestParseBandwidth(t *testing.T) {
	tests := []struct {
		value    int64
		unit     string
		expected int64
	}{
		{100, "mbps", 12_500_000},
		{100, "Mbps", 12_500_000},
		{512, "kbps", 64_000},
		{1, "gbps", 125_000_000},
		{1024, "bps", 1024},
		{10, "MBps", 10 * 1024 * 1024},
		{42, "unknown", 42}, // fallback to bps
	}
	for _, tt := range tests {
		got := ParseBandwidth(tt.value, tt.unit)
		if got != tt.expected {
			t.Errorf("ParseBandwidth(%d, %q) = %d, want %d", tt.value, tt.unit, got, tt.expected)
		}
	}
}

func TestFormatBandwidth(t *testing.T) {
	tests := []struct {
		bps      int64
		expected string
	}{
		{125_000_000, "1 Gbps"},
		{12_500_000, "100 Mbps"},
		{125_000, "1 Mbps"},
		{12_500, "100 Kbps"},
		{125, "1 Kbps"},
		{50, "50 bps"},
		{0, "0 bps"},
	}
	for _, tt := range tests {
		got := FormatBandwidth(tt.bps)
		if got != tt.expected {
			t.Errorf("FormatBandwidth(%d) = %q, want %q", tt.bps, got, tt.expected)
		}
	}
}

func TestManagerWithUnits(t *testing.T) {
	m := Manager

	// Set rate limit using Mbps constant
	m.Set("bandwidth-test@user", 100*Mbps, 50*Mbps)
	ul := m.Get("bandwidth-test@user")
	if ul == nil {
		t.Fatal("expected limiter")
	}
	if ul.Egress.Rate() != 12_500_000 {
		t.Errorf("egress: got %d, want 12500000 (100 Mbps)", ul.Egress.Rate())
	}
	if ul.Ingress.Rate() != 6_250_000 {
		t.Errorf("ingress: got %d, want 6250000 (50 Mbps)", ul.Ingress.Rate())
	}
	m.Remove("bandwidth-test@user")
}
