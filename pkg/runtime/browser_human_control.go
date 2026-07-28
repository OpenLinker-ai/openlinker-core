package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	browserPauseTTL         = 10 * time.Minute
	browserHumanTTL         = 10 * time.Minute
	browserHumanTotalTTL    = 30 * time.Minute
	browserViewerInputLimit = 10_000
	browserViewerFrameLimit = 1_200
	browserViewerBytesLimit = 256 << 20
)

type BrowserHumanControlState struct {
	RunID            uuid.UUID  `json:"run_id"`
	AttemptID        uuid.UUID  `json:"attempt_id"`
	RuntimeSessionID uuid.UUID  `json:"runtime_session_id"`
	BrowserSessionID uuid.UUID  `json:"browser_session_id"`
	SessionEpoch     uint64     `json:"session_epoch"`
	AttachmentID     uuid.UUID  `json:"attachment_id"`
	ControlEpoch     uint64     `json:"control_epoch"`
	Controller       string     `json:"controller"`
	State            string     `json:"state"`
	PauseReason      string     `json:"pause_reason"`
	PauseExpiresAt   time.Time  `json:"pause_expires_at"`
	HumanExpiresAt   *time.Time `json:"human_expires_at,omitempty"`
	ClaimedAt        *time.Time `json:"claimed_at,omitempty"`
	HumanDurationMS  int64      `json:"human_duration_ms"`
	UpdatedAt        time.Time  `json:"updated_at"`

	UserID       uuid.UUID `json:"-"`
	AgentID      uuid.UUID `json:"-"`
	LeaseID      uuid.UUID `json:"-"`
	FencingToken int64     `json:"-"`
	NodeID       uuid.UUID `json:"-"`
	WorkerID     string    `json:"-"`
	RunDeadline  time.Time `json:"-"`
}

type browserViewerLiveState struct {
	state      BrowserHumanControlState
	inputCount int64
	frameCount int64
	frameBytes int64
	frame      *BrowserViewerFramePayload
	notify     chan struct{}
}

type browserViewerCommandSender interface {
	SendBrowserViewerCommand(
		uuid.UUID,
		BrowserViewerCommandPayload,
	) error
}

// BrowserHumanControl persists only low-frequency ownership transitions.
// Frames, input and their wakeups remain bounded process memory.
type BrowserHumanControl struct {
	pool *pgxpool.Pool
	now  func() time.Time

	mu     sync.Mutex
	live   map[uuid.UUID]*browserViewerLiveState
	sender browserViewerCommandSender
}

func NewBrowserHumanControl(pool *pgxpool.Pool) *BrowserHumanControl {
	return &BrowserHumanControl{
		pool: pool,
		now:  time.Now,
		live: make(map[uuid.UUID]*browserViewerLiveState),
	}
}

func (control *BrowserHumanControl) BindCommandSender(
	sender browserViewerCommandSender,
) {
	if control == nil {
		return
	}
	control.mu.Lock()
	control.sender = sender
	control.mu.Unlock()
}

func (control *BrowserHumanControl) RunGC(ctx context.Context) {
	if control == nil || control.pool == nil {
		return
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			control.reapExpired(ctx)
		}
	}
}

func (control *BrowserHumanControl) reapExpired(ctx context.Context) {
	rows, err := control.pool.Query(ctx, `
SELECT run_id, user_id, state
FROM browser_run_controls
WHERE (state = 'human' AND human_expires_at <= clock_timestamp())
   OR (state IN ('paused', 'released') AND pause_expires_at <= clock_timestamp())
ORDER BY updated_at
LIMIT 100
`)
	if err != nil {
		return
	}
	type expiredControl struct {
		runID  uuid.UUID
		userID uuid.UUID
		state  string
	}
	expired := make([]expiredControl, 0, 16)
	for rows.Next() {
		var item expiredControl
		if rows.Scan(&item.runID, &item.userID, &item.state) == nil {
			expired = append(expired, item)
		}
	}
	rows.Close()
	for _, item := range expired {
		if item.state == "human" {
			released, err := control.transition(
				ctx,
				item.userID,
				item.runID,
				[]string{"human"},
				"released",
				"none",
				BrowserViewerActionRelease,
				false,
			)
			if err != nil {
				continue
			}
			item.state = released.State
		}
		_, _ = control.transition(
			ctx,
			item.userID,
			item.runID,
			[]string{"paused", "released"},
			"closed",
			"agent",
			BrowserViewerActionTerminate,
			false,
		)
	}
}

func (control *BrowserHumanControl) PauseFromEvent(
	ctx context.Context,
	identity RuntimeAttemptIdentity,
	payload map[string]any,
) error {
	if control == nil || control.pool == nil {
		return errors.New("browser human control is unavailable")
	}
	if phase, _ := payload["phase"].(string); phase != "paused" {
		return nil
	}
	browserSessionID, err := browserPayloadUUID(payload, "browser_session_id")
	if err != nil {
		return err
	}
	attachmentID, err := browserPayloadUUID(payload, "attachment_id")
	if err != nil {
		return err
	}
	sessionEpoch, err := browserPayloadUint64(payload, "session_epoch")
	if err != nil {
		return err
	}
	controlEpoch, err := browserPayloadUint64(payload, "control_epoch")
	if err != nil {
		return err
	}
	reason, _ := payload["reason"].(string)
	if reason == "" || len(reason) > 120 {
		reason = "user_action_required"
	}
	if identity.NodeID == nil ||
		identity.WorkerID == nil ||
		identity.RuntimeSessionID == nil {
		return errors.New("browser pause lacks Runtime authority")
	}
	now := control.now().UTC()
	row := control.pool.QueryRow(ctx, `
INSERT INTO browser_run_controls (
    run_id, user_id, agent_id, attempt_id, lease_id, fencing_token,
    node_id, worker_id, runtime_session_id, browser_session_id,
    session_epoch, attachment_id, control_epoch, controller, state,
    pause_reason, run_deadline_at, pause_expires_at, updated_at
)
SELECT r.id, r.user_id, r.agent_id, $2, $3, $4, $5, $6, $7, $8,
       $9, $10, $11, 'none', 'paused', $12,
       COALESCE(r.run_deadline_at, $13::timestamptz),
       LEAST($13::timestamptz, COALESCE(r.run_deadline_at, $13::timestamptz)),
       $14
FROM runs r
WHERE r.id = $1
ON CONFLICT (run_id) DO UPDATE SET
    attempt_id = EXCLUDED.attempt_id,
    lease_id = EXCLUDED.lease_id,
    fencing_token = EXCLUDED.fencing_token,
    node_id = EXCLUDED.node_id,
    worker_id = EXCLUDED.worker_id,
    runtime_session_id = EXCLUDED.runtime_session_id,
    browser_session_id = EXCLUDED.browser_session_id,
    session_epoch = EXCLUDED.session_epoch,
    attachment_id = EXCLUDED.attachment_id,
    control_epoch = EXCLUDED.control_epoch,
    controller = 'none',
    state = 'paused',
    pause_reason = EXCLUDED.pause_reason,
    run_deadline_at = EXCLUDED.run_deadline_at,
    pause_expires_at = EXCLUDED.pause_expires_at,
    human_expires_at = NULL,
    claimed_by_user_id = NULL,
    claimed_at = NULL,
    released_at = NULL,
    resumed_at = NULL,
    input_count = 0,
    frame_count = 0,
    updated_at = EXCLUDED.updated_at
WHERE browser_run_controls.attempt_id = EXCLUDED.attempt_id
  AND browser_run_controls.control_epoch <= EXCLUDED.control_epoch
RETURNING run_id, user_id, agent_id, attempt_id, lease_id, fencing_token,
          node_id, worker_id, runtime_session_id, browser_session_id,
          session_epoch, attachment_id, control_epoch, controller, state,
          pause_reason, run_deadline_at, pause_expires_at, human_expires_at,
          claimed_at, human_duration_ms, updated_at
`,
		identity.RunID,
		identity.AttemptID,
		identity.LeaseID,
		identity.FencingToken,
		*identity.NodeID,
		*identity.WorkerID,
		*identity.RuntimeSessionID,
		browserSessionID,
		sessionEpoch,
		attachmentID,
		controlEpoch,
		reason,
		now.Add(browserPauseTTL),
		now,
	)
	state, err := scanBrowserHumanControlState(row)
	if err != nil {
		return err
	}
	control.setLive(state)
	return nil
}

func (control *BrowserHumanControl) State(
	ctx context.Context,
	userID uuid.UUID,
	runID uuid.UUID,
) (BrowserHumanControlState, error) {
	if control == nil || control.pool == nil {
		return BrowserHumanControlState{}, errors.New("browser human control is unavailable")
	}
	row := control.pool.QueryRow(ctx, browserControlSelectSQL+
		` WHERE run_id = $1 AND user_id = $2`, runID, userID)
	state, err := scanBrowserHumanControlState(row)
	if err != nil {
		return BrowserHumanControlState{}, err
	}
	control.setLive(state)
	return state, nil
}

func (control *BrowserHumanControl) Claim(
	ctx context.Context,
	userID uuid.UUID,
	runID uuid.UUID,
) (BrowserHumanControlState, error) {
	return control.transition(
		ctx,
		userID,
		runID,
		[]string{"paused", "released"},
		"human",
		"human",
		BrowserViewerActionClaim,
		true,
	)
}

func (control *BrowserHumanControl) Release(
	ctx context.Context,
	userID uuid.UUID,
	runID uuid.UUID,
) (BrowserHumanControlState, error) {
	return control.transition(
		ctx,
		userID,
		runID,
		[]string{"human"},
		"released",
		"none",
		BrowserViewerActionRelease,
		true,
	)
}

func (control *BrowserHumanControl) Resume(
	ctx context.Context,
	userID uuid.UUID,
	runID uuid.UUID,
) (BrowserHumanControlState, error) {
	return control.transition(
		ctx,
		userID,
		runID,
		[]string{"paused", "released"},
		"resumed",
		"agent",
		BrowserViewerActionResume,
		true,
	)
}

func (control *BrowserHumanControl) transition(
	ctx context.Context,
	userID uuid.UUID,
	runID uuid.UUID,
	from []string,
	stateName string,
	controllerName string,
	action BrowserViewerAction,
	enforceExpiry bool,
) (BrowserHumanControlState, error) {
	if control == nil || control.pool == nil {
		return BrowserHumanControlState{}, errors.New("browser human control is unavailable")
	}
	now := control.now().UTC()
	tx, err := control.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return BrowserHumanControlState{}, err
	}
	defer tx.Rollback(ctx)
	before, err := scanBrowserHumanControlState(tx.QueryRow(
		ctx,
		browserControlSelectSQL+
			` WHERE run_id = $1 AND user_id = $2 FOR UPDATE`,
		runID,
		userID,
	))
	if err != nil {
		return BrowserHumanControlState{}, err
	}
	if !containsBrowserControlState(from, before.State) {
		return BrowserHumanControlState{}, fmt.Errorf(
			"browser control transition from %s is not allowed",
			before.State,
		)
	}
	if enforceExpiry &&
		((before.State == "paused" && !now.Before(before.PauseExpiresAt)) ||
			(before.State == "human" &&
				(before.HumanExpiresAt == nil ||
					!now.Before(*before.HumanExpiresAt)))) {
		return BrowserHumanControlState{}, errors.New("browser control lease has expired")
	}
	live := control.liveSnapshot(runID)
	inputCount, frameCount := live.inputCount, live.frameCount
	humanDurationMS := before.HumanDurationMS
	if before.State == "human" && before.ClaimedAt != nil {
		humanDurationMS += now.Sub(*before.ClaimedAt).Milliseconds()
	}
	var humanExpiresAt *time.Time
	pauseExpiresAt := before.PauseExpiresAt
	if stateName == "human" {
		remaining := browserHumanTotalTTL -
			time.Duration(before.HumanDurationMS)*time.Millisecond
		if remaining <= 0 {
			return BrowserHumanControlState{}, errors.New(
				"browser human-control total duration is exhausted",
			)
		}
		expires := now.Add(browserHumanTTL)
		if remaining < browserHumanTTL {
			expires = now.Add(remaining)
		}
		if before.RunDeadline.Before(expires) {
			expires = before.RunDeadline
		}
		humanExpiresAt = &expires
	}
	if stateName == "released" {
		pauseExpiresAt = now.Add(browserPauseTTL)
		if before.RunDeadline.Before(pauseExpiresAt) {
			pauseExpiresAt = before.RunDeadline
		}
	}
	if before.State == "human" {
		if _, err := tx.Exec(ctx, `
INSERT INTO browser_human_control_audits (
    run_id, user_id, attempt_id, browser_session_id, session_epoch,
    attachment_id, control_epoch, controller, pause_reason, claimed_at,
    ended_at, duration_ms, end_reason, input_count, frame_count
) VALUES ($1, $2, $3, $4, $5, $6, $7, 'human', $8, $9, $10, $11, $12, $13, $14)
`, before.RunID, userID, before.AttemptID,
			before.BrowserSessionID, before.SessionEpoch, before.AttachmentID,
			before.ControlEpoch,
			before.PauseReason, *before.ClaimedAt, now,
			now.Sub(*before.ClaimedAt).Milliseconds(), string(action),
			inputCount, frameCount,
		); err != nil {
			return BrowserHumanControlState{}, err
		}
	}
	_, err = tx.Exec(ctx, `
UPDATE browser_run_controls
SET control_epoch = control_epoch + 1,
    controller = $3,
    state = $4,
    human_expires_at = $5,
    claimed_by_user_id = CASE WHEN $4 = 'human' THEN $2 ELSE NULL END,
    claimed_at = CASE WHEN $4 = 'human' THEN $6 ELSE claimed_at END,
    released_at = CASE WHEN $4 = 'released' THEN $6 ELSE released_at END,
    resumed_at = CASE WHEN $4 = 'resumed' THEN $6 ELSE resumed_at END,
    input_count = $7,
    frame_count = $8,
    human_duration_ms = $9,
    pause_expires_at = $10,
    updated_at = $6
WHERE run_id = $1 AND user_id = $2
`, runID, userID, controllerName, stateName, humanExpiresAt, now,
		inputCount, frameCount, humanDurationMS, pauseExpiresAt)
	if err != nil {
		return BrowserHumanControlState{}, err
	}
	after, err := scanBrowserHumanControlState(tx.QueryRow(
		ctx,
		browserControlSelectSQL+` WHERE run_id = $1 AND user_id = $2`,
		runID,
		userID,
	))
	if err != nil {
		return BrowserHumanControlState{}, err
	}
	command := browserViewerCommand(after, action, before.ControlEpoch, nil, now)
	if err := control.send(command); err != nil {
		return BrowserHumanControlState{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return BrowserHumanControlState{}, err
	}
	control.setLive(after)
	return after, nil
}

func (control *BrowserHumanControl) Input(
	ctx context.Context,
	userID uuid.UUID,
	runID uuid.UUID,
	input BrowserViewerInputPayload,
) error {
	state, err := control.liveState(ctx, userID, runID)
	if err != nil {
		return err
	}
	now := control.now().UTC()
	if state.State != "human" ||
		state.Controller != "human" ||
		state.HumanExpiresAt == nil ||
		!now.Before(*state.HumanExpiresAt) ||
		!validBrowserViewerInput(input) {
		return errors.New("browser human control is not active")
	}
	live := control.liveSnapshot(runID)
	if live.state.ControlEpoch != state.ControlEpoch ||
		live.inputCount >= browserViewerInputLimit {
		return errors.New("browser human-control input limit is exhausted")
	}
	command := browserViewerCommand(
		state,
		BrowserViewerActionInput,
		state.ControlEpoch,
		&input,
		now,
	)
	if err := control.send(command); err != nil {
		return err
	}
	control.mu.Lock()
	if live := control.live[runID]; live != nil &&
		live.state.ControlEpoch == state.ControlEpoch {
		live.inputCount++
	}
	control.mu.Unlock()
	return nil
}

func (control *BrowserHumanControl) PublishFrame(
	frame BrowserViewerFramePayload,
) error {
	if err := validateBrowserViewerFrame(frame); err != nil {
		return err
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	live := control.live[frame.AttemptIdentity.RunID]
	if live == nil ||
		live.state.State != "human" ||
		live.state.HumanExpiresAt == nil ||
		!control.now().UTC().Before(*live.state.HumanExpiresAt) ||
		live.state.ControlEpoch != frame.ControlEpoch ||
		live.state.AttemptID != frame.AttemptIdentity.AttemptID ||
		live.state.RuntimeSessionID != frame.AttemptIdentity.RuntimeSessionID ||
		live.state.BrowserSessionID != frame.BrowserSessionID ||
		live.state.AttachmentID != frame.AttachmentID ||
		(live.frame != nil && frame.FrameSeq <= live.frame.FrameSeq) ||
		live.frameCount >= browserViewerFrameLimit ||
		live.frameBytes+int64(len(frame.Data)) > browserViewerBytesLimit {
		return errors.New("browser Viewer frame authority is stale")
	}
	copied := frame
	copied.Data = append([]byte(nil), frame.Data...)
	live.frame = &copied
	live.frameCount++
	live.frameBytes += int64(len(frame.Data))
	close(live.notify)
	live.notify = make(chan struct{})
	return nil
}

func (control *BrowserHumanControl) WaitFrame(
	ctx context.Context,
	userID uuid.UUID,
	runID uuid.UUID,
	after uint64,
) (*BrowserViewerFramePayload, error) {
	state, err := control.liveState(ctx, userID, runID)
	if err != nil {
		return nil, err
	}
	if state.State != "human" ||
		state.HumanExpiresAt == nil ||
		!control.now().UTC().Before(*state.HumanExpiresAt) {
		return nil, errors.New("browser human control is not active")
	}
	for {
		control.mu.Lock()
		live := control.live[runID]
		if live == nil || live.state.ControlEpoch != state.ControlEpoch {
			control.mu.Unlock()
			return nil, errors.New("browser human control changed")
		}
		if live.frame != nil && live.frame.FrameSeq > after {
			copied := *live.frame
			copied.Data = append([]byte(nil), live.frame.Data...)
			control.mu.Unlock()
			return &copied, nil
		}
		notify := live.notify
		control.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-notify:
		}
	}
}

func (control *BrowserHumanControl) send(
	command BrowserViewerCommandPayload,
) error {
	control.mu.Lock()
	sender := control.sender
	control.mu.Unlock()
	if sender == nil {
		return errors.New("browser Viewer Runtime connection is unavailable")
	}
	return sender.SendBrowserViewerCommand(
		command.AttemptIdentity.RuntimeSessionID,
		command,
	)
}

func (control *BrowserHumanControl) setLive(
	state BrowserHumanControlState,
) {
	control.mu.Lock()
	defer control.mu.Unlock()
	existing := control.live[state.RunID]
	if existing != nil && existing.state.ControlEpoch == state.ControlEpoch {
		existing.state = state
		return
	}
	control.live[state.RunID] = &browserViewerLiveState{
		state:  state,
		notify: make(chan struct{}),
	}
}

func (control *BrowserHumanControl) liveSnapshot(
	runID uuid.UUID,
) browserViewerLiveState {
	control.mu.Lock()
	defer control.mu.Unlock()
	if live := control.live[runID]; live != nil {
		return *live
	}
	return browserViewerLiveState{}
}

func (control *BrowserHumanControl) liveState(
	ctx context.Context,
	userID uuid.UUID,
	runID uuid.UUID,
) (BrowserHumanControlState, error) {
	control.mu.Lock()
	live := control.live[runID]
	if live != nil && live.state.UserID == userID {
		state := live.state
		control.mu.Unlock()
		return state, nil
	}
	control.mu.Unlock()
	return control.State(ctx, userID, runID)
}

func browserViewerCommand(
	state BrowserHumanControlState,
	action BrowserViewerAction,
	previous uint64,
	input *BrowserViewerInputPayload,
	now time.Time,
) BrowserViewerCommandPayload {
	deadline := now.Add(15 * time.Second)
	if state.HumanExpiresAt != nil && state.HumanExpiresAt.Before(deadline) {
		deadline = *state.HumanExpiresAt
	}
	return BrowserViewerCommandPayload{
		AttemptIdentity: AttemptIdentity{
			RunID:            state.RunID,
			AttemptID:        state.AttemptID,
			LeaseID:          state.LeaseID,
			FencingToken:     state.FencingToken,
			NodeID:           state.NodeID,
			AgentID:          state.AgentID,
			WorkerID:         state.WorkerID,
			RuntimeSessionID: state.RuntimeSessionID,
		},
		Action:               action,
		BrowserSessionID:     state.BrowserSessionID,
		SessionEpoch:         state.SessionEpoch,
		AttachmentID:         state.AttachmentID,
		PreviousControlEpoch: previous,
		ControlEpoch:         state.ControlEpoch,
		Input:                input,
		DeadlineAt:           deadline,
	}
}

func containsBrowserControlState(states []string, current string) bool {
	for _, state := range states {
		if state == current {
			return true
		}
	}
	return false
}

func browserPayloadUUID(
	payload map[string]any,
	key string,
) (uuid.UUID, error) {
	value, _ := payload[key].(string)
	parsed, err := uuid.Parse(value)
	if err != nil || parsed == uuid.Nil {
		return uuid.Nil, fmt.Errorf("invalid browser lifecycle %s", key)
	}
	return parsed, nil
}

func browserPayloadUint64(
	payload map[string]any,
	key string,
) (uint64, error) {
	var value uint64
	switch raw := payload[key].(type) {
	case float64:
		if raw < 1 || raw != float64(uint64(raw)) {
			return 0, fmt.Errorf("invalid browser lifecycle %s", key)
		}
		value = uint64(raw)
	case int:
		if raw < 1 {
			return 0, fmt.Errorf("invalid browser lifecycle %s", key)
		}
		value = uint64(raw)
	case int64:
		if raw < 1 {
			return 0, fmt.Errorf("invalid browser lifecycle %s", key)
		}
		value = uint64(raw)
	case uint64:
		value = raw
	case json.Number:
		parsed, err := strconv.ParseUint(string(raw), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid browser lifecycle %s", key)
		}
		value = parsed
	default:
		return 0, fmt.Errorf("invalid browser lifecycle %s", key)
	}
	if value < 1 {
		return 0, fmt.Errorf("invalid browser lifecycle %s", key)
	}
	return value, nil
}

const browserControlSelectSQL = `
SELECT run_id, user_id, agent_id, attempt_id, lease_id, fencing_token,
       node_id, worker_id, runtime_session_id, browser_session_id,
       session_epoch, attachment_id, control_epoch, controller, state,
       pause_reason, run_deadline_at, pause_expires_at, human_expires_at, claimed_at,
       human_duration_ms, updated_at
FROM browser_run_controls`

type browserControlRow interface {
	Scan(...any) error
}

func scanBrowserHumanControlState(
	row browserControlRow,
) (BrowserHumanControlState, error) {
	var state BrowserHumanControlState
	var sessionEpoch, controlEpoch int64
	err := row.Scan(
		&state.RunID,
		&state.UserID,
		&state.AgentID,
		&state.AttemptID,
		&state.LeaseID,
		&state.FencingToken,
		&state.NodeID,
		&state.WorkerID,
		&state.RuntimeSessionID,
		&state.BrowserSessionID,
		&sessionEpoch,
		&state.AttachmentID,
		&controlEpoch,
		&state.Controller,
		&state.State,
		&state.PauseReason,
		&state.RunDeadline,
		&state.PauseExpiresAt,
		&state.HumanExpiresAt,
		&state.ClaimedAt,
		&state.HumanDurationMS,
		&state.UpdatedAt,
	)
	if err != nil {
		return BrowserHumanControlState{}, err
	}
	if sessionEpoch < 1 || controlEpoch < 1 {
		return BrowserHumanControlState{}, errors.New("browser control epoch is invalid")
	}
	state.SessionEpoch = uint64(sessionEpoch)
	state.ControlEpoch = uint64(controlEpoch)
	return state, nil
}
