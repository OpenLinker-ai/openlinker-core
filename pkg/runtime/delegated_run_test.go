package runtime

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDelegatedResultsRequireExplicitSessionAudience(t *testing.T) {
	signer, err := NewRuntimeInvocationSigner(runtimeInvocationTestSecret)
	require.NoError(t, err)
	capability := runtimeInvocationCapabilityFixture()
	oldEnvelope, oldToken, err := signer.Issue(capability)
	require.NoError(t, err)
	require.Empty(t, runtimeSessionInvocationAudience(RuntimeRequiredFeatures()))
	capability.Audience = runtimeSessionInvocationAudience(append(RuntimeRequiredFeatures(), RuntimeDelegatedRunReadFeature))
	require.Equal(t, runtimeDelegationAudience, capability.Audience)
	newEnvelope, newToken, err := signer.Issue(capability)
	require.NoError(t, err)
	require.NotEqual(t, oldToken, newToken)
	verified, err := verifyRuntimeDelegationCapabilityPair(signer, newEnvelope, newToken, capability.IssuedAt)
	require.NoError(t, err)
	require.Equal(t, runtimeDelegationAudience, runtimeCapabilityAudience(verified))
	_, err = verifyRuntimeDelegationCapabilityPair(signer, oldEnvelope, newToken, capability.IssuedAt)
	require.Error(t, err)
	_, err = verifyRuntimeDelegationCapabilityPair(signer, newEnvelope, oldToken, capability.IssuedAt)
	require.Error(t, err)
	capability.Audience = "arbitrary-owner-api"
	_, _, err = signer.Issue(capability)
	require.Error(t, err)
}

func TestDelegatedReadRequestRejectsAuthorityAndTrailingInput(t *testing.T) {
	for _, raw := range []string{
		`{"run_id":"11111111-1111-4111-8111-111111111111","parent_run_id":"22222222-2222-4222-8222-222222222222"}`,
		`{"run_id":"11111111-1111-4111-8111-111111111111"} {}`,
	} {
		var request DelegatedRunReadRequest
		require.Error(t, decodeRuntimeJSON([]byte(raw), &request))
	}
}
