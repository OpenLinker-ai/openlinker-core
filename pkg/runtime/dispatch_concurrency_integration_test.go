package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// Run creation, the signal outbox, Session/Node capacity, claim, ACK and result
// all use production code and PostgreSQL. The clients only speak the WS wire
// protocol: they never poll Claim or send synthetic wake hints. In particular,
// no RuntimeDispatchWakeReconciler (including its startup pass) is started.
func TestConcurrentRuntimeDispatchWithoutCompensation(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		agents, runs, capacity int
		holdReference          bool
	}{
		{"same_agent_capacity_two", 1, 4, 2, false},
		{"different_agents_shared_node", 3, 3, 3, false},
		{"shared_node_capacity_release", 3, 3, 1, false},
		{"foreign_key_reference_during_dispatch", 3, 3, 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := setupTestDB(t)
			resetRuntimeNodeAdminTables(t, pool)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			coreID, nodeID := uuid.New(), uuid.New()
			ownerID := insertCreator(t, pool)
			features := runtime.RuntimeRequiredFeatures()
			device := runtime.RuntimeDeviceIdentity{NodeID: nodeID, CertificateSerial: strings.ReplaceAll(nodeID.String(), "-", ""), CertificateFingerprintSHA256: strings.Repeat("a", 64), PublicKeyThumbprintSHA256: strings.Repeat("b", 64)}
			_, err := pool.Exec(ctx, `INSERT INTO runtime_nodes
    (node_id,display_name,device_certificate_serial,device_public_key_thumbprint,node_version,protocol_version,runtime_contract_id,runtime_contract_digest,features,capacity,inflight,status,last_seen_at)
    VALUES ($1,'dispatch integration',$2,$3,'dispatch-test-v1',2,$4,$5,$6,$7,0,'active',clock_timestamp())`, nodeID, device.CertificateSerial, device.PublicKeyThumbprintSHA256, runtime.RuntimeContractID, runtime.RuntimeContractDigest, features, tc.capacity)
			require.NoError(t, err)
			tokens := dispatchIntegrationTokens{}
			agentIDs := make([]uuid.UUID, tc.agents)
			for i := range agentIDs {
				agentIDs[i] = insertAgent(t, pool, ownerID, "openlinker-runtime://dispatch-test", 0, "approved")
				_, err = pool.Exec(ctx, `UPDATE agents SET connection_mode='runtime' WHERE id=$1`, agentIDs[i])
				require.NoError(t, err)
				tokenID := uuid.New()
				plaintext := tokenID.String()
				_, err = pool.Exec(ctx, `INSERT INTO agent_tokens (id,agent_id,creator_user_id,name,prefix,token_hash,scopes,status,redeemed_at)
     VALUES ($1,$2,$3,'dispatch integration',$4,$4,ARRAY['agent:pull']::text[],'active_runtime',clock_timestamp())`, tokenID, agentIDs[i], ownerID, "ol_agent_"+plaintext[:8])
				require.NoError(t, err)
				tokens[plaintext] = db.AgentRuntimeToken{ID: tokenID, AgentID: agentIDs[i], Scopes: []string{"agent:pull"}}
			}
			cfg := newTestConfig()
			cfg.RunTimeoutSeconds = 60
			service := runtime.NewService(pool, cfg)
			service.ConfigureCoreRuntime(coreID)
			signer, err := runtime.NewRuntimeInvocationSigner(strings.Repeat("integration-only-", 4))
			require.NoError(t, err)
			hub := runtime.NewRuntimeWakeHub()
			controller := runtime.NewRuntimeHTTPController(runtime.RuntimeHTTPDependencies{
				TokenValidator: tokens, DeviceAuthenticator: dispatchIntegrationDevice{device},
				Sessions:       runtime.NewRuntimeSessionService(pool, coreID),
				Leases:         runtime.NewRuntimeLeaseService(pool, coreID, signer, runtime.RuntimeLeaseConfig{}),
				EventProjector: service, Finalizer: runtime.NewResultFinalizer(pool, nil, nil),
				Cancellations: runtime.NewRuntimeCancellationCoordinator(pool), WakeHub: hub, CoreInstanceID: coreID,
			})
			e := echo.New()
			controller.Register(e.Group("/api/v1"))
			server := httptest.NewServer(e)
			defer server.Close()
			defer func() {
				shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer shutdownCancel()
				require.NoError(t, controller.Shutdown(shutdownCtx))
			}()
			redisServer := miniredis.RunT(t)
			redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
			defer redisClient.Close()
			channel := "test:dispatch:" + nodeID.String()
			bus, err := runtime.NewRedisSignalBus(redisClient, runtime.RedisSignalBusConfig{Channel: channel, InstanceID: coreID})
			require.NoError(t, err)
			defer bus.Close()
			subscriberDone := make(chan struct{})
			go func() {
				defer close(subscriberDone)
				runtime.StartRuntimeSignalSubscriber(ctx, bus, coreID, hub, service)
			}()
			defer func() { cancel(); <-subscriberDone }()
			require.Eventually(t, func() bool { return redisServer.PubSubNumSub(channel)[channel] == 1 }, time.Second, time.Millisecond)
			arrived := make(chan uuid.UUID, tc.runs*2)
			clientErrors := make(chan error, tc.agents)
			var clients []*websocket.Conn
			var clientWG sync.WaitGroup
			defer func() {
				for _, conn := range clients {
					_ = conn.Close()
				}
				clientWG.Wait()
			}()
			for plaintext, token := range tokens {
				conn, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/api/v1/agent-runtime/ws", http.Header{"Authorization": []string{"Bearer " + plaintext}})
				if response != nil && response.Body != nil {
					_ = response.Body.Close()
				}
				require.NoError(t, err)
				clients = append(clients, conn)
				hello := runtime.RuntimeHelloPayload{NodeID: nodeID, AgentID: token.AgentID, WorkerID: "dispatch-" + token.AgentID.String(), RuntimeSessionID: uuid.New(), SessionEpoch: 1, NodeVersion: "dispatch-test-v1", Capacity: int64(tc.capacity), Features: features, ContractDigest: runtime.RuntimeContractDigest}
				require.NoError(t, sendDispatchIntegrationMessage(conn, runtime.RuntimeMessageHello, hello))
				require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
				var ready runtime.RuntimeEnvelope
				require.NoError(t, conn.ReadJSON(&ready))
				require.Equal(t, runtime.RuntimeMessageReady, ready.Type, string(ready.Payload))
				clientWG.Add(1)
				go func() {
					defer clientWG.Done()
					if err := dispatchIntegrationClient(conn, arrived); err != nil {
						clientErrors <- err
					}
				}()
			}
			runIDs := make([]uuid.UUID, tc.runs)
			start := make(chan struct{})
			createErrors := make(chan error, tc.runs)
			var creates sync.WaitGroup
			for i := range runIDs {
				creates.Add(1)
				go func() {
					defer creates.Done()
					<-start
					run, err := service.StartRun(ctx, ownerID, &runtime.RunRequest{AgentID: agentIDs[i%tc.agents].String(), Input: map[string]any{"text": "concurrent dispatch"}, IdempotencyKey: uuid.NewString()}, "api")
					if err != nil {
						createErrors <- err
						return
					}
					runIDs[i] = uuid.MustParse(run.RunID)
				}()
			}
			close(start)
			creates.Wait()
			close(createErrors)
			for err := range createErrors {
				require.NoError(t, err)
			}
			var reference pgx.Tx
			if tc.holdReference {
				reference, err = pool.Begin(ctx)
				require.NoError(t, err)
				defer reference.Rollback(context.Background())
				// An actual FK insert holds KEY SHARE on runs, like attaching a newly
				// created Workflow child to workflow_run_steps. Hold it across delivery
				// of run.available so this does not depend on scheduler luck.
				_, err = reference.Exec(ctx, `INSERT INTO run_messages(run_id,role,content) VALUES($1,'platform','reference lock barrier')`, runIDs[0])
				require.NoError(t, err)
			}
			outboxDone := make(chan struct{})
			go func() {
				defer close(outboxDone)
				runtime.StartRuntimeSignalOutboxWorker(ctx, runtime.NewRuntimeSignalOutboxWorker(db.New(pool), bus), runtime.RuntimeSignalOutboxWorkerConfig{Interval: 10 * time.Millisecond})
			}()
			defer func() { cancel(); <-outboxDone }()
			seen := map[uuid.UUID]bool{}
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()
			for len(seen) < len(runIDs) {
				select {
				case id := <-arrived:
					require.Contains(t, runIDs, id)
					require.False(t, seen[id], "assignment must not be duplicated")
					seen[id] = true
					if reference != nil && id == runIDs[0] {
						require.NoError(t, reference.Rollback(ctx))
						reference = nil
					}
				case err := <-clientErrors:
					require.NoError(t, err)
				case <-deadline.C:
					t.Fatalf("only %d/%d Runs dispatched without compensation; missing=%v", len(seen), len(runIDs), missingDispatchRunIDs(runIDs, seen))
				}
			}
			require.Eventually(t, func() bool {
				var complete int
				err := pool.QueryRow(ctx, `SELECT count(*) FROM runs WHERE id=ANY($1::uuid[]) AND status='success'`, runIDs).Scan(&complete)
				return err == nil && complete == len(runIDs)
			}, 5*time.Second, 10*time.Millisecond, "all results must settle through the real finalizer")
		})
	}
}

type dispatchIntegrationTokens map[string]db.AgentRuntimeToken

func (tokens dispatchIntegrationTokens) ValidateRuntimeToken(_ context.Context, plaintext string, _ ...string) (db.AgentRuntimeToken, error) {
	if token, ok := tokens[plaintext]; ok {
		return token, nil
	}
	return db.AgentRuntimeToken{}, errors.New("unknown integration token")
}

type dispatchIntegrationDevice struct{ identity runtime.RuntimeDeviceIdentity }

func (d dispatchIntegrationDevice) AuthenticateHTTP(context.Context, *http.Request) (runtime.RuntimeDeviceIdentity, error) {
	return d.identity, nil
}
func sendDispatchIntegrationMessage[P any](conn *websocket.Conn, kind runtime.RuntimeMessageType, payload P, reply ...*uuid.UUID) error {
	var replyID *uuid.UUID
	if len(reply) > 0 {
		replyID = reply[0]
	}
	message, err := runtime.NewRuntimeTypedMessage(kind, replyID, payload)
	if err != nil {
		return err
	}
	return conn.WriteJSON(message)
}
func dispatchIntegrationClient(conn *websocket.Conn, arrived chan<- uuid.UUID) error {
	for {
		if err := conn.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
			return err
		}
		var message runtime.RuntimeEnvelope
		if err := conn.ReadJSON(&message); err != nil {
			return err
		}
		switch message.Type {
		case runtime.RuntimeMessageRunAssigned:
			var assigned runtime.RunAssignedPayload
			if err := json.Unmarshal(message.Payload, &assigned); err != nil {
				return err
			}
			arrived <- assigned.AttemptIdentity.RunID
			if err := sendDispatchIntegrationMessage(conn, runtime.RuntimeMessageAssignmentAck, runtime.RunAssignmentAckPayload{AttemptIdentity: assigned.AttemptIdentity}, &message.MessageID); err != nil {
				return err
			}
		case runtime.RuntimeMessageAssignmentConfirmed:
			var confirmed runtime.RunAssignmentConfirmedPayload
			if err := json.Unmarshal(message.Payload, &confirmed); err != nil {
				return err
			}
			if err := sendDispatchIntegrationMessage(conn, runtime.RuntimeMessageRunResult, runtime.RunResultPayload{AttemptIdentity: confirmed.AttemptIdentity, ResultID: uuid.New(), Status: "success", Output: map[string]any{"summary": "dispatch regression"}, DurationMS: 2, FinalClientEventSeq: 0}); err != nil {
				return err
			}
		case runtime.RuntimeMessageRunResultAck:
		default:
			return fmt.Errorf("unexpected runtime message %s", message.Type)
		}
	}
}
func missingDispatchRunIDs(ids []uuid.UUID, seen map[uuid.UUID]bool) []uuid.UUID {
	var missing []uuid.UUID
	for _, id := range ids {
		if !seen[id] {
			missing = append(missing, id)
		}
	}
	return missing
}

func TestRuntimeClaimReferenceLockCompatibilityAndExclusion(t *testing.T) {
	pool := setupTestDB(t)
	resetRuntimeNodeAdminTables(t, pool)
	fixture := insertRuntimeNodeAdminFixture(t, pool)
	ctx := context.Background()
	service := newTestService(t, pool)
	owner := insertRuntimeUser(t, pool)
	run, err := service.StartRun(ctx, owner, &runtime.RunRequest{AgentID: fixture.agentID.String(), Input: map[string]any{"text": "lock exclusion"}, IdempotencyKey: uuid.NewString()}, "api")
	require.NoError(t, err)
	reference, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer reference.Rollback(ctx)
	_, err = reference.Exec(ctx, `INSERT INTO run_messages(run_id,role,content) VALUES($1,'platform','foreign key reader')`, uuid.MustParse(run.RunID))
	require.NoError(t, err)
	first, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer first.Rollback(ctx)
	args := db.LockNextClaimableRuntimeRunForAgentParams{AgentID: fixture.agentID}
	candidate, err := db.New(first).LockNextClaimableRuntimeRunForAgent(ctx, args)
	require.NoError(t, err, "a foreign-key reader cannot hide pending work")
	require.Equal(t, run.RunID, candidate.ID.String())
	second, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer second.Rollback(ctx)
	_, err = db.New(second).LockNextClaimableRuntimeRunForAgent(ctx, args)
	require.ErrorIs(t, err, pgx.ErrNoRows, "a concurrent claimant must still skip the reserved Run")
	require.NoError(t, first.Rollback(ctx))
	candidate, err = db.New(second).LockNextClaimableRuntimeRunForAgent(ctx, args)
	require.NoError(t, err, "rollback releases the claim lock even while the FK reference remains")
	require.Equal(t, run.RunID, candidate.ID.String())
}
