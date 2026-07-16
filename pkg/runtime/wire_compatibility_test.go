package runtime

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRuntimeSupportedWireGenerationRingIsBounded(t *testing.T) {
	t.Parallel()

	require.Len(t, runtimeSupportedWireGenerations, 2)
	require.True(t, runtimeWireContractSupported(RuntimeContractDigest))
	require.True(t, runtimeWireContractSupported(runtimePreviousContractDigest))
	require.False(t, runtimeWireContractSupported("857598f6e8f07d87d1f7240e34d98f0911bf23e5204a865d282a6bcb7f52865f"))
	require.False(t, runtimeWireContractSupported(strings.Repeat("f", 64)))
	require.False(t, runtimeWireContractAllowsMissingAttachment(RuntimeContractDigest))
	require.False(t, runtimeWireContractAllowsMissingAttachment(runtimePreviousContractDigest))
}

func TestCurrentRuntimeWireCompatibilityReturnsImmutableTwoGenerationSnapshot(t *testing.T) {
	t.Parallel()

	snapshot := CurrentRuntimeWireCompatibility()
	require.Equal(t, RuntimeContractDigest, snapshot.CurrentContractDigest)
	require.Equal(t, []string{RuntimeContractDigest, runtimePreviousContractDigest}, snapshot.SupportedContractDigests)
	require.Equal(t, runtimePreviousSupportedUntil, snapshot.PreviousSupportedUntilRFC)
	require.NotContains(t, snapshot.SupportedContractDigests, "857598f6e8f07d87d1f7240e34d98f0911bf23e5204a865d282a6bcb7f52865f")

	snapshot.SupportedContractDigests[0] = "mutated"
	require.Equal(t, RuntimeContractDigest, CurrentRuntimeWireCompatibility().SupportedContractDigests[0])
}
