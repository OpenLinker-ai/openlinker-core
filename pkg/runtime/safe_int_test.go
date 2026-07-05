package runtime

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRuntimeSafeIntHelpers(t *testing.T) {
	require.Equal(t, int32(0), clampInt64ToInt32(-1))
	require.Equal(t, int32(42), clampInt64ToInt32(42))
	require.Equal(t, int32(math.MaxInt32), clampInt64ToInt32(int64(math.MaxInt32)+1))

	require.Equal(t, int32(0), clampLenToInt32(-1))
	require.Equal(t, int32(42), clampLenToInt32(42))
	if strconv.IntSize > 32 {
		largeLen := int64(math.MaxInt32)
		largeLen++
		require.Equal(t, int32(math.MaxInt32), clampLenToInt32(int(largeLen)))
	}

	require.Equal(t, int32(1500), clampDurationMillisToInt32(1500*time.Millisecond))
	require.Equal(t, int32(0), clampDurationMillisToInt32(-time.Second))
	require.Equal(t, int32(math.MaxInt32), clampDurationMillisToInt32(time.Duration(math.MaxInt64)))
}
