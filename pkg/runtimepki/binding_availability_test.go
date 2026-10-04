package runtimepki

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestBindingVerifierDatabaseFailureDoesNotBecomeCredentialRejection(t *testing.T) {
	databaseErr := errors.New("private database failure")
	missing := runtimeBindingRowFunc(func(...any) error { return pgx.ErrNoRows })
	failed := runtimeBindingRowFunc(func(...any) error { return databaseErr })
	for _, tc := range []struct {
		name      string
		rows      []pgx.Row
		tokenOnly bool
	}{
		{"mtls_binding", []pgx.Row{failed}, false},
		{"mtls_historical", []pgx.Row{missing, failed}, false},
		{"token_binding", []pgx.Row{failed}, true},
		{"token_historical", []pgx.Row{missing, failed}, true},
		{"token_occupancy", []pgx.Row{missing, missing, failed}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queries := &runtimeBindingQueryFake{rows: tc.rows}
			verifier := &BindingVerifier{pool: queries}
			credentialID, nodeID := uuid.New(), uuid.New()
			var err error
			if tc.tokenOnly {
				_, err = verifier.ResolveTokenOnlyRuntimeDeviceIdentity(context.Background(), credentialID, nodeID)
			} else {
				err = verifier.VerifyRuntimePrincipalBinding(context.Background(), credentialID, coreruntime.RuntimeDeviceIdentity{NodeID: nodeID})
			}
			var transportErr *coreruntime.RuntimeTransportError
			require.ErrorAs(t, err, &transportErr)
			require.ErrorIs(t, err, databaseErr)
			require.Equal(t, coreruntime.RuntimeErrorServiceUnavailable, transportErr.Body.Code)
			require.True(t, transportErr.Body.Retryable)
			require.Len(t, queries.queries, len(tc.rows), "failed query must never trigger a legacy fallback")
		})
	}
}
