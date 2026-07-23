package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// This exercises the operational enrollment path and the real PostgreSQL
// Session service. It proves a Node provisioned with the current Server
// identity can negotiate the previous SDK adapter over the same canonical
// HTTP and WebSocket URLs; no hand-inserted previous Node is involved.
func TestRuntimeOperationalProvisioningNegotiatesPreviousHTTPAndWebSocket(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	defer pool.Close()

	var schemaVersion int32
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT schema_version FROM runtime_schema_contracts WHERE is_current`).Scan(&schemaVersion))
	if schemaVersion != coreruntime.RuntimeSchemaVersion {
		t.Skipf("runtime schema = %d, need %d", schemaVersion, coreruntime.RuntimeSchemaVersion)
	}

	var previousMode string
	require.NoError(t, pool.QueryRow(context.Background(), `
SELECT mode FROM runtime_cluster_control WHERE singleton_id = 1`).Scan(&previousMode))
	_, err = pool.Exec(context.Background(), `
UPDATE runtime_cluster_control SET mode = 'normal', updated_at = clock_timestamp()
WHERE singleton_id = 1`)
	require.NoError(t, err)
	defer func() {
		_, _ = pool.Exec(context.Background(), `
UPDATE runtime_cluster_control SET mode = $1, updated_at = clock_timestamp()
WHERE singleton_id = 1`, previousMode)
	}()

	supported := coreruntime.CurrentRuntimeWireCompatibility().SupportedContractDigests
	require.Len(t, supported, 2)
	previousDigest := supported[1]
	previousFeatures := make([]string, 0, len(coreruntime.RuntimeRequiredFeatures()))
	for _, feature := range coreruntime.RuntimeRequiredFeatures() {
		if feature != "session_drain" {
			previousFeatures = append(previousFeatures, feature)
		}
	}

	for _, transport := range []string{"http", "websocket"} {
		t.Run(transport, func(t *testing.T) {
			nodeID, agentID, tokenID, userID, coreID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
			serial := strings.ReplaceAll(nodeID.String(), "-", "")
			thumbprintBytes := sha256.Sum256([]byte("thumbprint:" + nodeID.String()))
			thumbprint := hex.EncodeToString(thumbprintBytes[:])
			record := runtimeNodeRecord{
				NodeID:                    nodeID,
				DisplayName:               "compat " + transport,
				DeviceCertificateSerial:   serial,
				DevicePublicKeyThumbprint: thumbprint,
				NodeVersion:               "2.0.0",
				Features:                  coreruntime.RuntimeRequiredFeatures(),
				Capacity:                  2,
			}
			require.NoError(t, registerRuntimeNode(context.Background(), pool, record, func() error { return nil }))
			seedRuntimeCompatibilityAgent(t, pool, userID, agentID, tokenID, transport)

			device := coreruntime.RuntimeDeviceIdentity{
				NodeID:                       nodeID,
				CertificateSerial:            serial,
				CertificateFingerprintSHA256: strings.Repeat("a", 64),
				PublicKeyThumbprintSHA256:    thumbprint,
			}
			token := db.AgentRuntimeToken{ID: tokenID, AgentID: agentID, Scopes: []string{"agent:pull"}}
			controller := coreruntime.NewRuntimeHTTPController(coreruntime.RuntimeHTTPDependencies{
				TokenValidator:      runtimeCompatibilityTokenValidator{token: token},
				DeviceAuthenticator: runtimeCompatibilityDeviceAuthenticator{device: device},
				Sessions:            coreruntime.NewRuntimeSessionService(pool, coreID),
				Leases:              runtimeCompatibilityLeaseService{},
				EventProjector:      runtimeCompatibilityEventProjector{store: coreruntime.NewEventStore(pool)},
				Finalizer:           coreruntime.NewResultFinalizer(pool, nil, nil),
				Cancellations:       coreruntime.NewRuntimeCancellationCoordinator(pool),
				CoreInstanceID:      coreID,
			})
			e := echo.New()
			controller.Register(e.Group("/api/v1"))
			server := httptest.NewServer(e)
			defer server.Close()

			hello := coreruntime.RuntimeHelloPayload{
				NodeID:           nodeID,
				AgentID:          agentID,
				WorkerID:         "worker-" + transport,
				RuntimeSessionID: uuid.New(),
				SessionEpoch:     1,
				NodeVersion:      "2.0.0",
				Capacity:         2,
				Features:         previousFeatures,
				ContractDigest:   previousDigest,
			}

			var ready map[string]json.RawMessage
			switch transport {
			case "http":
				body, marshalErr := json.Marshal(hello)
				require.NoError(t, marshalErr)
				req, requestErr := http.NewRequest(http.MethodPost, server.URL+"/api/v1/agent-runtime/sessions", bytes.NewReader(body))
				require.NoError(t, requestErr)
				req.Header.Set(echo.HeaderAuthorization, "Bearer previous-test-token")
				req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
				response, requestErr := http.DefaultClient.Do(req)
				require.NoError(t, requestErr)
				defer response.Body.Close()
				require.Equal(t, http.StatusOK, response.StatusCode)
				require.NoError(t, json.NewDecoder(response.Body).Decode(&ready))
			case "websocket":
				target := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/v1/agent-runtime/ws"
				conn, response, dialErr := websocket.DefaultDialer.Dial(target, http.Header{
					echo.HeaderAuthorization: []string{"Bearer previous-test-token"},
				})
				if response != nil && response.Body != nil {
					defer response.Body.Close()
				}
				require.NoError(t, dialErr)
				defer conn.Close()
				message, messageErr := coreruntime.NewRuntimeTypedMessage(coreruntime.RuntimeMessageHello, nil, hello)
				require.NoError(t, messageErr)
				require.NoError(t, conn.WriteJSON(message))
				_, frame, readErr := conn.ReadMessage()
				require.NoError(t, readErr)
				envelope, parseErr := coreruntime.ParseRuntimeEnvelope(frame)
				require.NoError(t, parseErr)
				require.Equal(t, coreruntime.RuntimeMessageReady, envelope.Type)
				require.NoError(t, json.Unmarshal(envelope.Payload, &ready))
			}

			require.Contains(t, ready, "attachment_id")
			require.Len(t, ready, 6)
			var nodeDigest, sessionDigest string
			require.NoError(t, pool.QueryRow(context.Background(), `
SELECT n.runtime_contract_digest, s.runtime_contract_digest
FROM runtime_nodes n JOIN runtime_sessions s ON s.node_id = n.node_id
WHERE n.node_id = $1 AND s.runtime_session_id = $2`, nodeID, hello.RuntimeSessionID).Scan(&nodeDigest, &sessionDigest))
			require.Equal(t, previousDigest, nodeDigest)
			require.Equal(t, previousDigest, sessionDigest)
		})
	}
}

func seedRuntimeCompatibilityAgent(t *testing.T, pool *pgxpool.Pool, userID, agentID, tokenID uuid.UUID, suffix string) {
	t.Helper()
	slug := "compat-" + strings.ReplaceAll(agentID.String(), "-", "")[:16]
	prefix := "ol_agent_" + strings.ReplaceAll(tokenID.String(), "-", "")[:16]
	tx, err := pool.Begin(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.Background()) }()
	_, err = tx.Exec(context.Background(), `
INSERT INTO users (id, email, password_hash, display_name, is_creator, creator_verified)
VALUES ($1, $2, 'x', $3, true, true)`,
		userID, "compat-"+userID.String()+"@example.test", "compat "+suffix,
	)
	require.NoError(t, err)
	_, err = tx.Exec(context.Background(), `
INSERT INTO agents (id, creator_id, slug, name, description, endpoint_url,
                    price_per_call_cents, connection_mode)
VALUES ($1, $2, $3, $4, 'compatibility integration test',
        'openlinker-runtime://' || $3, 0, 'runtime')`,
		agentID, userID, slug, "compat "+suffix,
	)
	require.NoError(t, err)
	_, err = tx.Exec(context.Background(), `
INSERT INTO agent_tokens (id, agent_id, creator_user_id, name, prefix, token_hash,
                          scopes, status, redeemed_at)
VALUES ($1, $2, $3, $4, $5, 'integration-hash', ARRAY['agent:pull'],
        'active_runtime', clock_timestamp())`,
		tokenID, agentID, userID, "compat "+suffix, prefix,
	)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(context.Background()))
}

type runtimeCompatibilityTokenValidator struct{ token db.AgentRuntimeToken }

func (v runtimeCompatibilityTokenValidator) ValidateRuntimeToken(context.Context, string, ...string) (db.AgentRuntimeToken, error) {
	return v.token, nil
}

type runtimeCompatibilityDeviceAuthenticator struct {
	device coreruntime.RuntimeDeviceIdentity
}

func (a runtimeCompatibilityDeviceAuthenticator) AuthenticateHTTP(context.Context, *http.Request) (coreruntime.RuntimeDeviceIdentity, error) {
	return a.device, nil
}

type runtimeCompatibilityLeaseService struct{}

func (runtimeCompatibilityLeaseService) ClaimOffer(context.Context, coreruntime.RuntimeSessionPrincipal) (*coreruntime.RunAssignedPayload, error) {
	return nil, nil
}
func (runtimeCompatibilityLeaseService) AckAssignment(context.Context, coreruntime.RuntimeSessionPrincipal, coreruntime.RunAssignmentAckPayload) (coreruntime.RunAssignmentConfirmedPayload, error) {
	return coreruntime.RunAssignmentConfirmedPayload{}, nil
}
func (runtimeCompatibilityLeaseService) RejectAssignment(context.Context, coreruntime.RuntimeSessionPrincipal, coreruntime.RunAssignmentRejectPayload) (coreruntime.RunAssignmentRejectedPayload, error) {
	return coreruntime.RunAssignmentRejectedPayload{}, nil
}
func (runtimeCompatibilityLeaseService) RenewLease(context.Context, coreruntime.RuntimeSessionPrincipal, coreruntime.RunLeaseRenewPayload) (coreruntime.RunLeaseRenewedPayload, error) {
	return coreruntime.RunLeaseRenewedPayload{}, nil
}
func (runtimeCompatibilityLeaseService) ReleaseUnackedOffer(context.Context, coreruntime.RuntimeSessionPrincipal, ...string) error {
	return nil
}

type runtimeCompatibilityEventProjector struct{ store *coreruntime.EventStore }

func (p runtimeCompatibilityEventProjector) AppendRuntimeEvent(
	ctx context.Context,
	principal coreruntime.RuntimeEventPrincipal,
	identity coreruntime.RuntimeAttemptIdentity,
	request coreruntime.RuntimeEventRequest,
) (coreruntime.RuntimeEventAck, error) {
	return p.store.Append(ctx, principal, identity, request)
}
