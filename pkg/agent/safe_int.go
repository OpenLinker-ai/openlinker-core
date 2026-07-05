package agent

import (
	"math"
	"time"
)

func clampIntToInt32(value int) int32 {
	if value < 0 {
		return 0
	}
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	// #nosec G115 -- value is clamped into int32 range above.
	return int32(value)
}

func clampDurationSecondsToInt32(d time.Duration) int32 {
	if d <= 0 {
		return 0
	}
	seconds := d / time.Second
	if seconds > time.Duration(math.MaxInt32) {
		return math.MaxInt32
	}
	// #nosec G115 -- seconds is clamped into int32 range above.
	return int32(seconds)
}
