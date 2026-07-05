package runtime

import (
	"fmt"
	"math"
	"time"
)

func clampDurationMillisToInt32(d time.Duration) int32 {
	return clampInt64ToInt32(d.Milliseconds())
}

func clampInt64ToInt32(value int64) int32 {
	if value < 0 {
		return 0
	}
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	// #nosec G115 -- value is clamped into int32 range above.
	return int32(value)
}

func clampLenToInt32(length int) int32 {
	if length < 0 {
		return 0
	}
	if length > math.MaxInt32 {
		return math.MaxInt32
	}
	// #nosec G115 -- length is clamped into int32 range above.
	return int32(length)
}

func checkedInt64ToInt32(value int64) (int32, error) {
	if value < math.MinInt32 || value > math.MaxInt32 {
		return 0, fmt.Errorf("value out of int32 range")
	}
	// #nosec G115 -- value is checked against int32 bounds above.
	return int32(value), nil
}
