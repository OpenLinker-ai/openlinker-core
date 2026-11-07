// Package runtime - 测试钩子（仅在测试二进制中编译）。
//
// 通过 internal-package 文件给 _test.go 暴露一个安全的 SetHTTPClient 入口，
// 让测试可以注入 httptest.NewTLSServer 自带的 *http.Client（含其自签 CA 信任）。
//
// 不在 service.go 主文件里加 setter，避免污染生产 API。

package runtime

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

// WithRuntimeBackgroundClaimForTest marks ctx the way the WebSocket transport
// marks a claim that no new dispatch signal asked for.
func WithRuntimeBackgroundClaimForTest(ctx context.Context) context.Context {
	return withRuntimeBackgroundClaim(ctx)
}

// NewRuntimeLeaseServiceWithCandidateHookForTest builds the PostgreSQL lease
// service and calls afterCandidateLock while a claim transaction holds its
// freshly locked candidate Run, so tests can force concurrent interleavings.
func NewRuntimeLeaseServiceWithCandidateHookForTest(
	pool *pgxpool.Pool,
	coreInstanceID uuid.UUID,
	issuer RuntimeInvocationCapabilityIssuer,
	config RuntimeLeaseConfig,
	afterCandidateLock func(),
) *RuntimeLeaseService {
	return newRuntimeLeaseService(
		candidateHookLeaseRepository{inner: &postgresRuntimeLeaseRepository{pool: pool}, hook: afterCandidateLock},
		coreInstanceID, issuer, config,
	)
}

type candidateHookLeaseRepository struct {
	inner runtimeLeaseRepository
	hook  func()
}

func (r candidateHookLeaseRepository) WithTransaction(
	ctx context.Context,
	fn func(runtimeLeaseTransaction) error,
) error {
	return r.inner.WithTransaction(ctx, func(tx runtimeLeaseTransaction) error {
		return fn(candidateHookLeaseTransaction{runtimeLeaseTransaction: tx, hook: r.hook})
	})
}

type candidateHookLeaseTransaction struct {
	runtimeLeaseTransaction
	hook func()
}

func (t candidateHookLeaseTransaction) LockNextClaimableRuntimeRunForAgent(
	ctx context.Context,
	params db.LockNextClaimableRuntimeRunForAgentParams,
) (db.LockNextClaimableRuntimeRunForAgentRow, error) {
	row, err := t.runtimeLeaseTransaction.LockNextClaimableRuntimeRunForAgent(ctx, params)
	if err == nil && t.hook != nil {
		t.hook()
	}
	return row, err
}

// SetHTTPClient 仅供 _test.go 注入 client（如 httptest.NewTLSServer().Client()）。
// 生产代码不应调用。
func (s *Service) SetHTTPClient(c *http.Client) {
	s.httpClient = c
}

// DropLocalFramesForTest removes this process's frame buffer for a Run without
// touching the audit, standing in for a Core that restarted while an
// observation was still recorded as active.
func (observation *BrowserObservation) DropLocalFramesForTest(runID uuid.UUID) {
	observation.frames.close(runID)
}

// AgeLastPollForTest backdates when a Run's observation was last polled, so the
// abandonment grace can be exercised without waiting it out.
func (observation *BrowserObservation) AgeLastPollForTest(
	runID uuid.UUID,
	age time.Duration,
) {
	observation.frames.mu.Lock()
	defer observation.frames.mu.Unlock()
	if live := observation.frames.live[runID]; live != nil {
		live.lastPolledAt = live.lastPolledAt.Add(-age)
	}
}

// ConfigureObservationStartHandshakeForTest shortens the otherwise
// user-facing handshake so retry behavior can be proven without sleeping for
// production-scale intervals.
func (observation *BrowserObservation) ConfigureObservationStartHandshakeForTest(
	retryInterval time.Duration,
	timeout time.Duration,
) {
	if observation == nil {
		return
	}
	observation.startRetryInterval = retryInterval
	observation.startHandshakeTimeout = timeout
}

// RetainFinalFrameForTest seeds a retained final frame for a Run and Attempt by
// the ordinary path -- open, and one frame the Worker marks as final -- so a test
// can start from a round that has already ended and kept its last picture.
func (observation *BrowserObservation) RetainFinalFrameForTest(
	runID, attemptID uuid.UUID,
) {
	identity := BrowserObserverIdentity{
		RunID:        runID,
		AttemptID:    attemptID,
		SessionEpoch: 1,
	}
	leaseID, commandID := uuid.New(), uuid.New()
	if !observation.frames.open(runID, leaseID, commandID, identity) {
		return
	}
	defer observation.frames.close(runID)
	if !observation.frames.admit(runID, leaseID, commandID, identity, 1) {
		return
	}
	_ = observation.frames.publish(
		runID,
		leaseID,
		commandID,
		identity,
		BrowserObservationFrame{
			FrameSeq:   1,
			CapturedAt: observation.now().UTC(),
			MIMEType:   "image/jpeg",
			Data:       []byte{0xff, 0xd8, 0xff, 0xd9},
			Width:      1280,
			Height:     720,
		},
		true,
	)
}

// HoldsFinalFrameForTest reports whether this instance holds a retained final
// frame for the Run.
func (observation *BrowserObservation) HoldsFinalFrameForTest(runID uuid.UUID) bool {
	return observation.frames.finalFrame(runID).frame != nil
}
