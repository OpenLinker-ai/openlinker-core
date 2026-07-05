package skill

import "math"

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

func checkedInt64ToInt32(value int64) (int32, bool) {
	if value < math.MinInt32 || value > math.MaxInt32 {
		return 0, false
	}
	// #nosec G115 -- value is checked against int32 bounds above.
	return int32(value), true
}

func clampFloat64ToInt32(value float64) int32 {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return 0
	}
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	// #nosec G115 -- value is clamped into int32 range above.
	return int32(value)
}
