package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	runtimeWSWriteWait          = 10 * time.Second
	runtimeWSPongWait           = 75 * time.Second
	runtimeWSPingInterval       = 30 * time.Second
	runtimeWSSendBuffer         = 16
	runtimeWSRecvBuffer         = 64
	runtimeWSDispatchRetryDelay = 250 * time.Millisecond
	runtimeWSAssignmentAckWait  = 30 * time.Second
	runtimeWSHeartbeatTimeout   = 5 * time.Second
)

var runtimeWSUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return r.Header.Get("Origin") == "" },
}

type runtimeWSHub struct {
	mu    sync.Mutex
	conns map[uuid.UUID]map[*runtimeWSConn]struct{}
}

func newRuntimeWSHub() *runtimeWSHub {
	return &runtimeWSHub{conns: map[uuid.UUID]map[*runtimeWSConn]struct{}{}}
}

func (h *runtimeWSHub) register(conn *runtimeWSConn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conns[conn.token.AgentID] == nil {
		h.conns[conn.token.AgentID] = map[*runtimeWSConn]struct{}{}
	}
	h.conns[conn.token.AgentID][conn] = struct{}{}
}

func (h *runtimeWSHub) unregister(conn *runtimeWSConn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	agentConns := h.conns[conn.token.AgentID]
	if agentConns == nil {
		return
	}
	delete(agentConns, conn)
	if len(agentConns) == 0 {
		delete(h.conns, conn.token.AgentID)
	}
}

func (h *runtimeWSHub) connections(agentID uuid.UUID) []*runtimeWSConn {
	h.mu.Lock()
	defer h.mu.Unlock()
	agentConns := h.conns[agentID]
	out := make([]*runtimeWSConn, 0, len(agentConns))
	for conn := range agentConns {
		out = append(out, conn)
	}
	return out
}

type runtimeWSConn struct {
	service        *Service
	ws             *websocket.Conn
	token          db.AgentRuntimeToken
	agent          db.Agent
	plaintext      string
	connectionMode string
	send           chan runtimeWSSendRequest
	recv           chan RuntimeWSClientMessage
	closed         chan struct{}
	closeOnce      sync.Once
	mu             sync.Mutex
	inFlightRuns   map[uuid.UUID]*runtimeWSInFlight
}

type runtimeWSSendRequest struct {
	msg    RuntimeWSServerMessage
	result chan error
}

type runtimeWSInFlight struct {
	acked bool
}

// ServeRuntimeWebSocket accepts an Agent-owned outbound WebSocket and uses it
// as a low-latency assignment channel. Runtime state remains DB-backed; the
// existing long-poll claim/result endpoints remain the fallback path.
func (s *Service) ServeRuntimeWebSocket(w http.ResponseWriter, r *http.Request, plaintextToken string) error {
	ctx := r.Context()
	token, err := s.verifyRuntimeToken(ctx, plaintextToken, "agent:pull")
	if err != nil {
		return err
	}
	agent, err := s.queries.GetAgentByID(ctx, token.AgentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return httpx.NotFound("Agent 不存在")
	}
	if err != nil {
		return httpx.Internal("查询 Agent 失败")
	}
	if agent.LifecycleStatus != "active" {
		return httpx.Forbidden("Agent 未启用")
	}
	if !isQueuedRuntimeMode(agent.ConnectionMode) {
		return httpx.Conflict("Agent 不是队列型 runtime 接入模式")
	}

	upgrader := runtimeWSUpgrader
	upgrader.CheckOrigin = s.checkRuntimeWSOrigin
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return err
	}
	conn := &runtimeWSConn{
		service:        s,
		ws:             ws,
		token:          token,
		agent:          agent,
		plaintext:      plaintextToken,
		connectionMode: agent.ConnectionMode,
		send:           make(chan runtimeWSSendRequest, runtimeWSSendBuffer),
		recv:           make(chan RuntimeWSClientMessage, runtimeWSRecvBuffer),
		closed:         make(chan struct{}),
	}
	s.wsHub.register(conn)
	defer func() {
		s.wsHub.unregister(conn)
		conn.close()
	}()

	go conn.writeLoop()
	go conn.messageLoop(ctx)
	if heartbeat, heartbeatErr := s.heartbeatRuntimeAgentForToken(ctx, token, &agent); heartbeatErr == nil {
		_ = conn.sendServerMessage(ctx, RuntimeWSServerMessage{
			Type:      "runtime.ready",
			AgentID:   token.AgentID.String(),
			Heartbeat: heartbeat,
		})
		if runtimeWSHeartbeatShouldDispatch(heartbeat) {
			s.dispatchRuntimeWSRunBestEffort(ctx, token.AgentID)
		}
	} else {
		log.Warn().Err(heartbeatErr).Str("agent_id", token.AgentID.String()).Msg("runtime.ws: heartbeat on connect failed")
	}

	conn.readLoop(ctx)
	return nil
}

func (s *Service) checkRuntimeWSOrigin(r *http.Request) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	cfg := s.cfg
	if cfg == nil {
		return false
	}
	if originMatchesConfiguredURL(origin, cfg.FrontendURL) || originMatchesConfiguredURL(origin, cfg.APIURL) {
		return true
	}
	if !cfg.IsProduction() {
		parsed, err := url.Parse(origin)
		if err == nil && isLocalRuntimeWSOriginHost(parsed.Hostname()) {
			return true
		}
	}
	return false
}

func originMatchesConfiguredURL(origin, configured string) bool {
	originURL, err := url.Parse(strings.TrimSpace(origin))
	if err != nil || originURL.Scheme == "" || originURL.Host == "" {
		return false
	}
	configuredURL, err := url.Parse(strings.TrimSpace(configured))
	if err != nil || configuredURL.Scheme == "" || configuredURL.Host == "" {
		return false
	}
	return strings.EqualFold(originURL.Scheme, configuredURL.Scheme) &&
		strings.EqualFold(originURL.Host, configuredURL.Host)
}

func isLocalRuntimeWSOriginHost(host string) bool {
	switch strings.ToLower(strings.Trim(host, "[]")) {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
}

func (c *runtimeWSConn) writeLoop() {
	ticker := time.NewTicker(runtimeWSPingInterval)
	defer ticker.Stop()
	defer c.ws.Close()
	defer c.close()
	for {
		select {
		case req, ok := <-c.send:
			_ = c.ws.SetWriteDeadline(time.Now().Add(runtimeWSWriteWait))
			if !ok {
				_ = c.ws.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.ws.WriteJSON(req.msg); err != nil {
				req.result <- err
				return
			}
			req.result <- nil
		case <-ticker.C:
			_ = c.ws.SetWriteDeadline(time.Now().Add(runtimeWSWriteWait))
			if err := c.ws.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(runtimeWSWriteWait)); err != nil {
				return
			}
		case <-c.closed:
			_ = c.ws.SetWriteDeadline(time.Now().Add(runtimeWSWriteWait))
			_ = c.ws.WriteMessage(websocket.CloseMessage, []byte{})
			return
		}
	}
}

func (c *runtimeWSConn) readLoop(ctx context.Context) {
	c.ws.SetReadLimit(1 << 20)
	_ = c.ws.SetReadDeadline(time.Now().Add(runtimeWSPongWait))
	c.ws.SetPongHandler(func(string) error {
		return c.ws.SetReadDeadline(time.Now().Add(runtimeWSPongWait))
	})
	for {
		var msg RuntimeWSClientMessage
		if err := c.ws.ReadJSON(&msg); err != nil {
			return
		}
		if runtimeWSMessageIsHeartbeat(msg.Type) {
			c.service.handleRuntimeWSHeartbeatMessageAsync(ctx, c, msg)
			continue
		}
		c.enqueueClientMessage(ctx, msg)
	}
}

func (c *runtimeWSConn) messageLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closed:
			return
		case msg := <-c.recv:
			c.service.handleRuntimeWSClientMessage(ctx, c, &msg)
		}
	}
}

func (c *runtimeWSConn) enqueueClientMessage(ctx context.Context, msg RuntimeWSClientMessage) {
	select {
	case <-ctx.Done():
		return
	case <-c.closed:
		return
	case c.recv <- msg:
		return
	default:
		_ = c.sendServerMessage(ctx, runtimeWSErrorMessage(msg.ID, httpx.RateLimited("runtime websocket message backlog full")))
	}
}

func (c *runtimeWSConn) sendServerMessage(ctx context.Context, msg RuntimeWSServerMessage) error {
	select {
	case <-c.closed:
		return errors.New("runtime websocket closed")
	default:
	}
	req := runtimeWSSendRequest{
		msg:    msg,
		result: make(chan error, 1),
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return errors.New("runtime websocket closed")
	case c.send <- req:
	default:
		return errors.New("runtime websocket send buffer full")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return errors.New("runtime websocket closed")
	case err := <-req.result:
		return err
	}
}

func (c *runtimeWSConn) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

func (c *runtimeWSConn) close() {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.releaseAllRunClaims(context.Background(), "close")
	})
}

func (c *runtimeWSConn) tryReserveRunSlot(runID uuid.UUID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed() {
		return false
	}
	if c.inFlightRuns == nil {
		c.inFlightRuns = map[uuid.UUID]*runtimeWSInFlight{}
	}
	if len(c.inFlightRuns) > 0 {
		return false
	}
	c.inFlightRuns[runID] = &runtimeWSInFlight{}
	return true
}

func (c *runtimeWSConn) releaseRunSlot(runID uuid.UUID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inFlightRuns, runID)
}

func (c *runtimeWSConn) markRunAssignmentAcked(runID uuid.UUID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	inFlight := c.inFlightRuns[runID]
	if inFlight == nil {
		return false
	}
	inFlight.acked = true
	return true
}

func (c *runtimeWSConn) runAssignmentState(runID uuid.UUID) (bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	inFlight := c.inFlightRuns[runID]
	if inFlight == nil {
		return false, false
	}
	return true, inFlight.acked
}

func (c *runtimeWSConn) startAssignmentAckTimer(runID uuid.UUID) {
	go func() {
		timer := time.NewTimer(runtimeWSAssignmentAckWait)
		defer timer.Stop()
		select {
		case <-c.closed:
			return
		case <-timer.C:
		}
		present, acked := c.runAssignmentState(runID)
		if !present || acked {
			return
		}
		c.releaseRunSlot(runID)
		c.releaseRunClaim(context.Background(), runID, "assignment_ack_timeout")
		if c.service != nil {
			c.service.dispatchRuntimeWSRunBestEffort(context.Background(), c.token.AgentID)
		}
	}()
}

func (c *runtimeWSConn) releaseAllRunClaims(ctx context.Context, reason string) {
	c.mu.Lock()
	runIDs := make([]uuid.UUID, 0, len(c.inFlightRuns))
	for runID := range c.inFlightRuns {
		if runID != uuid.Nil {
			runIDs = append(runIDs, runID)
		}
	}
	c.inFlightRuns = nil
	c.mu.Unlock()

	if c.service == nil || c.service.queries == nil || len(runIDs) == 0 {
		return
	}
	releaseCtx, cancel := context.WithTimeout(ctx, runtimeBestEffortWriteTimeout)
	defer cancel()
	for _, runID := range runIDs {
		c.releaseRunClaim(releaseCtx, runID, reason)
	}
	c.service.dispatchRuntimeWSRunBestEffort(releaseCtx, c.token.AgentID)
}

func (c *runtimeWSConn) releaseRunClaim(ctx context.Context, runID uuid.UUID, reason string) {
	if c.service == nil || c.service.queries == nil || runID == uuid.Nil {
		return
	}
	if err := c.service.queries.ReleaseRuntimePullRunClaim(ctx, db.ReleaseRuntimePullRunClaimParams{
		ID:             runID,
		RuntimeTokenID: c.token.ID,
	}); err != nil {
		log.Warn().Err(err).Str("run_id", runID.String()).Str("reason", reason).
			Msg("runtime.ws: release in-flight claim")
	}
}

func (c *runtimeWSConn) replaceReservedRunSlot(oldRunID, newRunID uuid.UUID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed() {
		delete(c.inFlightRuns, oldRunID)
		return false
	}
	if c.inFlightRuns == nil {
		c.inFlightRuns = map[uuid.UUID]*runtimeWSInFlight{}
	}
	inFlight := c.inFlightRuns[oldRunID]
	if inFlight == nil {
		inFlight = &runtimeWSInFlight{}
	}
	delete(c.inFlightRuns, oldRunID)
	c.inFlightRuns[newRunID] = inFlight
	return true
}

func (s *Service) handleRuntimeWSClientMessage(ctx context.Context, conn *runtimeWSConn, msg *RuntimeWSClientMessage) {
	switch msg.Type {
	case "heartbeat", "ping":
		s.handleRuntimeWSHeartbeatMessage(ctx, conn, msg)
	case "claim", "runtime.claim":
		if assigned, err := s.assignRuntimeWSRun(ctx, conn); err != nil {
			_ = conn.sendServerMessage(ctx, runtimeWSErrorMessage(msg.ID, err))
		} else if !assigned {
			_ = conn.sendServerMessage(ctx, RuntimeWSServerMessage{
				Type:              "run.empty",
				ID:                msg.ID,
				AgentID:           conn.token.AgentID.String(),
				RetryAfterSeconds: int32(runtimePullEmptyClaimRetryAfter.Seconds()),
			})
		}
	case "run.assignment.accepted", "run.assignment.ack":
		runID, err := uuid.Parse(msg.RunID)
		if err != nil {
			_ = conn.sendServerMessage(ctx, runtimeWSErrorMessage(msg.ID, httpx.BadRequest("run_id 不是合法 uuid")))
			return
		}
		conn.markRunAssignmentAcked(runID)
		_ = conn.sendServerMessage(ctx, RuntimeWSServerMessage{
			Type:  "run.assignment.accepted",
			ID:    msg.ID,
			RunID: runID.String(),
		})
	case "run.event":
		runID, err := uuid.Parse(msg.RunID)
		if err != nil {
			_ = conn.sendServerMessage(ctx, runtimeWSErrorMessage(msg.ID, httpx.BadRequest("run_id 不是合法 uuid")))
			return
		}
		conn.markRunAssignmentAcked(runID)
		event, err := s.reportRuntimeTokenRunEventForToken(ctx, conn.token, runID, &ReportRunEventRequest{
			EventType: msg.EventType,
			Payload:   msg.Payload,
		})
		if err != nil {
			_ = conn.sendServerMessage(ctx, runtimeWSErrorMessage(msg.ID, err))
			return
		}
		_ = conn.sendServerMessage(ctx, RuntimeWSServerMessage{
			Type:  "run.event.accepted",
			ID:    msg.ID,
			RunID: runID.String(),
			Event: event,
		})
	case "run.result":
		runID, err := uuid.Parse(msg.RunID)
		if err != nil {
			_ = conn.sendServerMessage(ctx, runtimeWSErrorMessage(msg.ID, httpx.BadRequest("run_id 不是合法 uuid")))
			return
		}
		conn.markRunAssignmentAcked(runID)
		result, err := s.completeRuntimePullRunForToken(ctx, conn.token, &conn.agent, runID, &RuntimePullResultRequest{
			Status:     msg.Status,
			Output:     msg.Output,
			Events:     msg.Events,
			Error:      msg.Error,
			DurationMs: msg.DurationMs,
		})
		if err != nil {
			_ = conn.sendServerMessage(ctx, runtimeWSErrorMessage(msg.ID, err))
			conn.releaseRunSlot(runID)
			return
		}
		conn.releaseRunSlot(runID)
		_ = conn.sendServerMessage(ctx, RuntimeWSServerMessage{
			Type:   "run.result.accepted",
			ID:     msg.ID,
			RunID:  runID.String(),
			Status: result.Status,
			Result: result,
		})
		s.dispatchRuntimeWSRunBestEffort(ctx, conn.token.AgentID)
	default:
		_ = conn.sendServerMessage(ctx, RuntimeWSServerMessage{
			Type:  "error",
			ID:    msg.ID,
			Error: &AgentError{Code: "UNKNOWN_WS_MESSAGE", Message: "runtime websocket message type 不支持"},
		})
	}
}

func runtimeWSMessageIsHeartbeat(messageType string) bool {
	switch messageType {
	case "heartbeat", "ping":
		return true
	default:
		return false
	}
}

func runtimeWSHeartbeatShouldDispatch(heartbeat *AgentHeartbeatResponse) bool {
	return heartbeat != nil && heartbeat.ClaimNow && heartbeat.PendingRunCount > 0
}

func (s *Service) handleRuntimeWSHeartbeatMessageAsync(ctx context.Context, conn *runtimeWSConn, msg RuntimeWSClientMessage) {
	if ctx == nil {
		ctx = context.Background()
	}
	go func() {
		heartbeatCtx, cancel := context.WithTimeout(ctx, runtimeWSHeartbeatTimeout)
		defer cancel()
		s.handleRuntimeWSHeartbeatMessage(heartbeatCtx, conn, &msg)
	}()
}

func (s *Service) handleRuntimeWSHeartbeatMessage(ctx context.Context, conn *runtimeWSConn, msg *RuntimeWSClientMessage) {
	started := time.Now()
	heartbeat, err := s.heartbeatRuntimeAgentForTokenWithOptions(ctx, conn.token, &conn.agent, runtimeHeartbeatOptions{
		asyncTokenTouch: true,
	})
	if err != nil {
		_ = conn.sendServerMessage(ctx, runtimeWSErrorMessage(msg.ID, err))
		logRuntimeWSSlowHeartbeat(started, conn, heartbeat, err)
		return
	}
	_ = conn.sendServerMessage(ctx, RuntimeWSServerMessage{
		Type:      "runtime.heartbeat",
		ID:        msg.ID,
		AgentID:   conn.token.AgentID.String(),
		Heartbeat: heartbeat,
	})
	logRuntimeWSSlowHeartbeat(started, conn, heartbeat, nil)
	if runtimeWSHeartbeatShouldDispatch(heartbeat) {
		s.dispatchRuntimeWSRunBestEffort(ctx, conn.token.AgentID)
	}
}

func logRuntimeWSSlowHeartbeat(started time.Time, conn *runtimeWSConn, heartbeat *AgentHeartbeatResponse, err error) {
	elapsed := time.Since(started)
	if elapsed < time.Second && err == nil {
		return
	}
	event := log.Warn().
		Dur("elapsed", elapsed).
		Str("agent_id", conn.token.AgentID.String()).
		Str("runtime_token_id", conn.token.ID.String())
	if heartbeat != nil {
		event = event.
			Bool("claim_now", heartbeat.ClaimNow).
			Int32("pending_run_count", heartbeat.PendingRunCount)
	}
	if err != nil {
		event = event.Err(err)
	}
	event.Msg("runtime.ws: heartbeat slow")
}

func runtimeWSErrorMessage(id string, err error) RuntimeWSServerMessage {
	code := "RUNTIME_WS_ERROR"
	message := err.Error()
	var httpErr *httpx.HTTPError
	if errors.As(err, &httpErr) {
		code = string(httpErr.Code)
		message = httpErr.Message
	}
	return RuntimeWSServerMessage{
		Type:  "error",
		ID:    id,
		Error: &AgentError{Code: code, Message: message},
	}
}

func (s *Service) dispatchRuntimeWSRunBestEffort(ctx context.Context, agentID uuid.UUID) bool {
	if s.wsHub == nil {
		return false
	}
	for _, conn := range s.wsHub.connections(agentID) {
		if conn.isClosed() {
			continue
		}
		if err := ctx.Err(); err != nil {
			return false
		}
		assigned, err := s.assignRuntimeWSRun(ctx, conn)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return false
			}
			log.Warn().Err(err).Str("agent_id", agentID.String()).Msg("runtime.ws: assign failed")
			continue
		}
		if assigned {
			return true
		}
	}
	return false
}

func (s *Service) dispatchRuntimeWSRunAsync(ctx context.Context, agent db.Agent) {
	if strings.TrimSpace(agent.ConnectionMode) != connectionModeRuntimeWS || s == nil || s.wsHub == nil {
		return
	}
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	go func() {
		dispatchCtx, cancel := context.WithTimeout(base, 10*time.Second)
		defer cancel()
		for {
			if s.dispatchRuntimeWSRunBestEffort(dispatchCtx, agent.ID) {
				return
			}
			timer := time.NewTimer(runtimeWSDispatchRetryDelay)
			select {
			case <-dispatchCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}

func (s *Service) assignRuntimeWSRun(ctx context.Context, conn *runtimeWSConn) (bool, error) {
	if !conn.tryReserveRunSlot(uuid.Nil) {
		return false, nil
	}
	run, err := s.claimRuntimePullRunOnce(ctx, conn.token, conn.connectionMode)
	if err != nil {
		conn.releaseRunSlot(uuid.Nil)
		return false, err
	}
	if run == nil {
		conn.releaseRunSlot(uuid.Nil)
		return false, nil
	}
	runID, err := uuid.Parse(run.RunID)
	if err != nil {
		conn.releaseRunSlot(uuid.Nil)
		return false, err
	}
	if !conn.replaceReservedRunSlot(uuid.Nil, runID) {
		releaseCtx, cancel := context.WithTimeout(context.Background(), runtimeBestEffortWriteTimeout)
		defer cancel()
		conn.releaseRunClaim(releaseCtx, runID, "connection_closed")
		return false, nil
	}
	if err := conn.sendServerMessage(ctx, runtimeWSAssignmentMessage(run)); err != nil {
		conn.releaseRunSlot(runID)
		releaseCtx, cancel := context.WithTimeout(context.Background(), runtimeBestEffortWriteTimeout)
		defer cancel()
		conn.releaseRunClaim(releaseCtx, runID, "send_failure")
		return true, err
	}
	conn.startAssignmentAckTimer(runID)
	return true, nil
}

func runtimeWSAssignmentMessage(run *RuntimePullRunResponse) RuntimeWSServerMessage {
	return RuntimeWSServerMessage{
		Type:           "run.assigned",
		ID:             uuid.NewString(),
		RunID:          run.RunID,
		AgentID:        run.AgentID,
		Input:          run.Input,
		Metadata:       run.Metadata,
		Source:         run.Source,
		ResultEndpoint: run.ResultEndpoint,
		ResultMethod:   run.ResultMethod,
		ResultRequired: run.ResultRequired,
		A2A:            run.A2A,
		Conversation:   run.Conversation,
	}
}

// ReportRuntimeTokenRunEvent lets runtime_ws workers stream progress over the
// same authenticated channel used for assignment and result delivery.
func (s *Service) ReportRuntimeTokenRunEvent(ctx context.Context, plaintextToken string, runID uuid.UUID, req *ReportRunEventRequest) (*RunEventResponse, error) {
	if req == nil {
		return nil, httpx.BadRequest("请求体不能为空")
	}
	token, err := s.verifyRuntimeToken(ctx, plaintextToken, "agent:pull")
	if err != nil {
		return nil, err
	}
	return s.reportRuntimeTokenRunEventForToken(ctx, token, runID, req)
}

func (s *Service) reportRuntimeTokenRunEventForToken(ctx context.Context, token db.AgentRuntimeToken, runID uuid.UUID, req *ReportRunEventRequest) (*RunEventResponse, error) {
	if req == nil {
		return nil, httpx.BadRequest("请求体不能为空")
	}
	state, err := s.queries.GetRuntimePullRunState(ctx, runID)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && state.AgentID != token.AgentID) {
		return nil, httpx.NotFound("调用记录不存在")
	}
	if err != nil {
		return nil, httpx.Internal("查询调用记录失败")
	}
	if state.Status != "running" {
		return nil, httpx.Conflict("run 已结束，不能继续上报事件")
	}
	if state.ClaimedByRuntimeTokenID == nil || *state.ClaimedByRuntimeTokenID != token.ID {
		return nil, httpx.Conflict("run 未被当前访问令牌领取")
	}
	eventType := strings.TrimSpace(req.EventType)
	if _, ok := allowedAgentResponseEventTypes[eventType]; !ok {
		return nil, httpx.Unprocessable("event_type 不支持")
	}
	payload := req.Payload
	if payload == nil {
		payload = map[string]interface{}{}
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, httpx.BadRequest("payload 不是合法 JSON")
	}
	event, err := s.queries.CreateRunEvent(ctx, db.CreateRunEventParams{
		RunID:       runID,
		ParentRunID: nil,
		EventType:   eventType,
		Payload:     payloadJSON,
	})
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Str("event_type", eventType).
			Msg("runtime.ReportRuntimeTokenRunEvent: CreateRunEvent")
		return nil, httpx.Internal("记录运行事件失败")
	}
	s.triggerTaskCallbackEvent(&event)
	resp := runEventToResponse(event)
	if eventType == "run.message.delta" {
		s.recordRunMessageBestEffort(ctx, runID, &resp.Sequence, "agent", messageContentFromMap(payload), payload)
	}
	if eventType == "run.artifact.delta" {
		s.recordArtifactDeltaBestEffort(ctx, runID, &resp.Sequence, payload)
	}
	return &resp, nil
}
