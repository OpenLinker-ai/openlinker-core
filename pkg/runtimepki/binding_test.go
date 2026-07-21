package runtimepki

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

func TestBindingVerifierRejectsUnrelatedUnboundCredential(t *testing.T) {
	credentialID, nodeID := uuid.New(), uuid.New()
	device := coreruntime.RuntimeDeviceIdentity{
		NodeID:                       nodeID,
		CertificateSerial:            "abc",
		CertificateFingerprintSHA256: strings.Repeat("a", 64),
		PublicKeyThumbprintSHA256:    strings.Repeat("b", 64),
	}
	queries := &runtimeBindingQueryFake{rows: []pgx.Row{
		runtimeBindingRowFunc(func(...any) error { return pgx.ErrNoRows }),
		runtimeBindingRowFunc(func(dest ...any) error {
			*(dest[0].(*bool)) = false
			return nil
		}),
	}}
	verifier := &BindingVerifier{pool: queries}

	if err := verifier.VerifyRuntimePrincipalBinding(context.Background(), credentialID, device); err == nil {
		t.Fatal("unrelated unbound credential was accepted")
	}
	if len(queries.queries) != 2 ||
		!strings.Contains(queries.queries[1], "session.credential_id = $1") ||
		!strings.Contains(queries.queries[1], "session.device_certificate_serial = $3") ||
		!strings.Contains(queries.queries[1], "node.device_public_key_thumbprint = $4") {
		t.Fatalf("legacy relationship query = %#v", queries.queries)
	}
}

func TestBindingVerifierAcceptsOnlyExactHistoricalPrincipal(t *testing.T) {
	credentialID, nodeID := uuid.New(), uuid.New()
	device := coreruntime.RuntimeDeviceIdentity{
		NodeID:                       nodeID,
		CertificateSerial:            "abc",
		CertificateFingerprintSHA256: strings.Repeat("a", 64),
		PublicKeyThumbprintSHA256:    strings.Repeat("b", 64),
	}
	queries := &runtimeBindingQueryFake{rows: []pgx.Row{
		runtimeBindingRowFunc(func(...any) error { return pgx.ErrNoRows }),
		runtimeBindingRowFunc(func(dest ...any) error {
			*(dest[0].(*bool)) = true
			return nil
		}),
	}}

	if err := (&BindingVerifier{pool: queries}).VerifyRuntimePrincipalBinding(
		context.Background(), credentialID, device,
	); err != nil {
		t.Fatalf("exact historical principal was rejected: %v", err)
	}
	wantArgs := []any{credentialID, nodeID, "abc", strings.Repeat("b", 64)}
	if len(queries.args) != 2 || len(queries.args[1]) != len(wantArgs) {
		t.Fatalf("legacy relationship args = %#v", queries.args)
	}
	for index := range wantArgs {
		if queries.args[1][index] != wantArgs[index] {
			t.Fatalf("legacy relationship arg %d = %#v, want %#v", index, queries.args[1][index], wantArgs[index])
		}
	}
}

func TestBindingVerifierDoesNotFallbackForMismatchedDurableBinding(t *testing.T) {
	credentialID, selectedNodeID := uuid.New(), uuid.New()
	queries := &runtimeBindingQueryFake{rows: []pgx.Row{
		boundMTLSRuntimeIdentityRow(uuid.New()),
	}}
	device := coreruntime.RuntimeDeviceIdentity{
		NodeID:                       selectedNodeID,
		CertificateSerial:            "abc",
		CertificateFingerprintSHA256: strings.Repeat("a", 64),
		PublicKeyThumbprintSHA256:    strings.Repeat("b", 64),
	}

	if err := (&BindingVerifier{pool: queries}).VerifyRuntimePrincipalBinding(
		context.Background(), credentialID, device,
	); err == nil {
		t.Fatal("mismatched durable binding was accepted")
	}
	if len(queries.queries) != 1 {
		t.Fatalf("mismatched durable binding used legacy fallback: %#v", queries.queries)
	}
}

func TestBindingVerifierResolvesTokenOnlyHistoricalNode(t *testing.T) {
	credentialID, nodeID := uuid.New(), uuid.New()
	thumbprint := strings.Repeat("b", 64)
	queries := &runtimeBindingQueryFake{rows: []pgx.Row{
		runtimeBindingRowFunc(func(...any) error { return pgx.ErrNoRows }),
		runtimeBindingRowFunc(func(dest ...any) error {
			*(dest[0].(*uuid.UUID)) = nodeID
			*(dest[1].(*string)) = "abc"
			*(dest[2].(*string)) = thumbprint
			*(dest[3].(*string)) = "active"
			return nil
		}),
	}}

	identity, err := (&BindingVerifier{pool: queries}).ResolveTokenOnlyRuntimeDeviceIdentity(
		context.Background(), credentialID, nodeID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if identity.NodeID != nodeID || identity.CertificateSerial != "abc" ||
		identity.AuthenticationMode != coreruntime.RuntimeAuthenticationTokenOnly ||
		identity.PublicKeyThumbprintSHA256 != thumbprint || identity.CertificateFingerprintSHA256 != thumbprint {
		t.Fatalf("token-only identity = %#v", identity)
	}
	if len(queries.queries) != 2 || !strings.Contains(queries.queries[1], "session.credential_id = $1") {
		t.Fatalf("token-only history query = %#v", queries.queries)
	}
}

func TestBindingVerifierRejectsTokenOnlyNodeDifferentFromDurableBinding(t *testing.T) {
	queries := &runtimeBindingQueryFake{rows: []pgx.Row{boundTokenOnlyRuntimeIdentityRow(uuid.New(), "token_only")}}
	if _, err := (&BindingVerifier{pool: queries}).ResolveTokenOnlyRuntimeDeviceIdentity(
		context.Background(), uuid.New(), uuid.New(),
	); err == nil {
		t.Fatal("token-only selector escaped the durable binding")
	}
	if len(queries.queries) != 1 {
		t.Fatalf("mismatched token-only binding used history fallback: %#v", queries.queries)
	}
}

func TestBindingVerifierResolvesExistingTokenOnlyBinding(t *testing.T) {
	credentialID, nodeID := uuid.New(), uuid.New()
	queries := &runtimeBindingQueryFake{rows: []pgx.Row{boundTokenOnlyRuntimeIdentityRow(nodeID, "token_only")}}

	identity, err := (&BindingVerifier{pool: queries}).ResolveTokenOnlyRuntimeDeviceIdentity(
		context.Background(), credentialID, nodeID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if identity.NodeID != nodeID || identity.AuthenticationMode != coreruntime.RuntimeAuthenticationTokenOnly ||
		identity.CertificateFingerprintSHA256 != identity.PublicKeyThumbprintSHA256 {
		t.Fatalf("token-only identity = %#v", identity)
	}
}

func TestBindingVerifierCreatesOnlyPendingIdentityForUnusedNode(t *testing.T) {
	credentialID, nodeID := uuid.New(), uuid.New()
	queries := &runtimeBindingQueryFake{rows: []pgx.Row{
		runtimeBindingRowFunc(func(...any) error { return pgx.ErrNoRows }),
		runtimeBindingRowFunc(func(...any) error { return pgx.ErrNoRows }),
		runtimeBindingRowFunc(func(dest ...any) error {
			*(dest[0].(*bool)) = false
			return nil
		}),
	}}

	identity, err := (&BindingVerifier{pool: queries}).ResolveTokenOnlyRuntimeDeviceIdentity(
		context.Background(), credentialID, nodeID,
	)
	if err != nil {
		t.Fatal(err)
	}
	want := pendingTokenOnlyRuntimeDeviceIdentity(credentialID, nodeID)
	if identity != want || identity.AuthenticationMode != coreruntime.RuntimeAuthenticationTokenOnly {
		t.Fatalf("pending identity = %#v, want %#v", identity, want)
	}
	if len(queries.queries) != 3 || !strings.Contains(queries.queries[2], "SELECT EXISTS") {
		t.Fatalf("pending enrollment queries = %#v", queries.queries)
	}
}

func TestBindingVerifierRejectsPendingIdentityForOccupiedNode(t *testing.T) {
	queries := &runtimeBindingQueryFake{rows: []pgx.Row{
		runtimeBindingRowFunc(func(...any) error { return pgx.ErrNoRows }),
		runtimeBindingRowFunc(func(...any) error { return pgx.ErrNoRows }),
		runtimeBindingRowFunc(func(dest ...any) error {
			*(dest[0].(*bool)) = true
			return nil
		}),
	}}
	if _, err := (&BindingVerifier{pool: queries}).ResolveTokenOnlyRuntimeDeviceIdentity(
		context.Background(), uuid.New(), uuid.New(),
	); err == nil {
		t.Fatal("occupied Node received a pending identity")
	}
}

func boundMTLSRuntimeIdentityRow(nodeID uuid.UUID) pgx.Row {
	return runtimeBindingRowFunc(func(dest ...any) error {
		*(dest[0].(*uuid.UUID)) = nodeID
		*(dest[1].(*string)) = "abc"
		*(dest[2].(*string)) = strings.Repeat("a", 64)
		*(dest[3].(*string)) = strings.Repeat("b", 64)
		*(dest[4].(*string)) = "active"
		*(dest[5].(*string)) = "mtls"
		return nil
	})
}

func boundTokenOnlyRuntimeIdentityRow(nodeID uuid.UUID, mode string) pgx.Row {
	return runtimeBindingRowFunc(func(dest ...any) error {
		*(dest[0].(*uuid.UUID)) = nodeID
		*(dest[1].(*string)) = "abc"
		*(dest[2].(*string)) = strings.Repeat("b", 64)
		*(dest[3].(*string)) = "active"
		*(dest[4].(*string)) = mode
		return nil
	})
}

type runtimeBindingQueryFake struct {
	rows    []pgx.Row
	queries []string
	args    [][]any
}

func (f *runtimeBindingQueryFake) QueryRow(_ context.Context, query string, args ...any) pgx.Row {
	f.queries = append(f.queries, query)
	f.args = append(f.args, append([]any(nil), args...))
	if len(f.rows) == 0 {
		return runtimeBindingRowFunc(func(...any) error { return pgx.ErrNoRows })
	}
	row := f.rows[0]
	f.rows = f.rows[1:]
	return row
}

type runtimeBindingRowFunc func(...any) error

func (f runtimeBindingRowFunc) Scan(dest ...any) error {
	return f(dest...)
}
