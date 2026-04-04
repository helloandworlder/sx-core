package ratelimit

// Bandwidth unit constants for converting human-readable rates to bytes/sec.
//
// Usage:
//   ratelimit.Manager.Set(email, 100*Mbps, 100*Mbps)  // 100 Mbps
//   ratelimit.Manager.Set(email, 512*Kbps, 512*Kbps)  // 512 Kbps
//   ratelimit.Manager.Set(email, 1*Gbps, 1*Gbps)      // 1 Gbps
const (
	Bps  int64 = 1                // bytes per second
	Kbps int64 = 1_000 / 8       // kilobits per second → bytes/sec (125)
	Mbps int64 = 1_000_000 / 8   // megabits per second → bytes/sec (125,000)
	Gbps int64 = 1_000_000_000 / 8 // gigabits per second → bytes/sec (125,000,000)

	// Alternative: binary units (KiB/s, MiB/s)
	KBps int64 = 1024        // kilobytes per second
	MBps int64 = 1024 * 1024 // megabytes per second
)

// ParseBandwidth converts a rate in the given unit to bytes/sec.
// Supported unit strings: "bps", "kbps", "mbps", "gbps", "KBps", "MBps"
func ParseBandwidth(value int64, unit string) int64 {
	switch unit {
	case "bps":
		return value
	case "kbps", "Kbps":
		return value * Kbps
	case "mbps", "Mbps":
		return value * Mbps
	case "gbps", "Gbps":
		return value * Gbps
	case "KBps":
		return value * KBps
	case "MBps":
		return value * MBps
	default:
		return value // assume bytes/sec
	}
}

// FormatBandwidth returns a human-readable string for a bytes/sec value.
func FormatBandwidth(bytesPerSec int64) string {
	switch {
	case bytesPerSec >= Gbps:
		return formatFloat(float64(bytesPerSec)/float64(Gbps)) + " Gbps"
	case bytesPerSec >= Mbps:
		return formatFloat(float64(bytesPerSec)/float64(Mbps)) + " Mbps"
	case bytesPerSec >= Kbps:
		return formatFloat(float64(bytesPerSec)/float64(Kbps)) + " Kbps"
	default:
		return formatFloat(float64(bytesPerSec)) + " bps"
	}
}

func formatFloat(v float64) string {
	if v == float64(int64(v)) {
		return itoa(int64(v))
	}
	// Simple 1 decimal place
	whole := int64(v)
	frac := int64((v - float64(whole)) * 10)
	if frac < 0 {
		frac = -frac
	}
	return itoa(whole) + "." + itoa(frac)
}

func itoa(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	buf := [20]byte{}
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
