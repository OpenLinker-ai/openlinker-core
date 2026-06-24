package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
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
	runtimeWSWriteWait    = 10 * time.Second
	runtimeWSPongWait     = 75 * time.Second
	runtimeWSPingInterval = 30 * time.Second
	runtimeWSSendBuffer   = 16
)

var runtimeWSUpgrader = websocket.Upgrader{
	CheckOrigin: func(_ *http.Request) bool {
		return true
	},
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
	service   *Service
	ws        *websocket.Conn
	token     db.AgentRuntimeToken
	plaintext string
	send      chan RuntimeWSServerMessage
	closed    chan struct{}
	closeOnce sync.Once
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

	ws, err := runtimeWSUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return err
	}
	conn := &runtimeWSConn{
		service:   s,
		ws:        ws,
		token:     token,
		plaintext: plaintextToken,
		send:      make(chan RuntimeWSServerMessage, runtimeWSSendBuffer),
		closed:    make(chan struct{}),
	}
	s.wsHub.register(conn)
	defer func() {
		s.wsHub.unregister(conn)
		conn.close()
	}()

	go conn.writeLoop()
	if heartbeat, heartbeatErr := s.HeartbeatAgent(ctx, plaintextToken); heartbeatErr == nil {
		_ = conn.sendServerMessage(ctx, RuntimeWSServerMessage{
			Type:      "runtime.ready",
			AgentID:   token.AgentID.String(),
			Heartbeat: heartbeat,
		})
	} else {
		log.Warn().Err(heartbeatErr).Str("agent_id", token.AgentID.String()).Msg("runtime.ws: heartbeat on connect failed")
	}
	s.dispatchRuntimeWSRunBestEffort(ctx, token.AgentID)

	conn.readLoop(ctx)
	return nil
}

func (c *runtimeWSConn) writeLoop() {
	ticker := time.NewTicker(runtimeWSPingInterval)
	defer ticker.Stop()
	defer c.ws.Close()
	for {
		select {
		case msg, ok := <-c.send:
			_ = c.ws.SetWriteDeadline(time.Now().Add(runtimeWSWriteWait))
			if !ok {
				_ = c.ws.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.ws.WriteJSON(msg); err != nil {
				return
			}
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
		c.service.handleRuntimeWSClientMessage(ctx, c, &msg)
	}
}

func (c *runtimeWSConn) sendServerMessage(ctx context.Context, msg RuntimeWSServerMessage) error {
	select {
	case <-c.closed:
		return errors.New("runtime websocket closed")
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return errors.New("runtime websocket closed")
	case c.send <- msg:
		return nil
	default:
		return errors.New("runtime websocket send buffer full")
	}
}

func (c *runtimeWSConn) close() {
	c.closeOnce.Do(func() {
		close(c.closed)
	})
}

func (s *Service) handleRuntimeWSClientMessage(ctx context.Context, conn *runtimeWSConn, msg *RuntimeWSClientMessage) {
	switch msg.Type {
	case "heartbeat", "ping":
		heartbeat, err := s.HeartbeatAgent(ctx, conn.plaintext)
		if err != nil {
			_ = conn.sendServerMessage(ctx, runtimeWSErrorMessage(msg.ID, err))
			return
		}
		_ = conn.sendServerMessage(ctx, RuntimeWSServerMessage{
			Type:      "runtime.heartbeat",
			ID:        msg.ID,
			AgentID:   conn.token.AgentID.String(),
			Heartbeat: heartbeat,
		})
		s.dispatchRuntimeWSRunBestEffort(ctx, conn.token.AgentID)
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
	case "run.event":
		runID, err := uuid.Parse(msg.RunID)
		if err != nil {
			_ = conn.sendServerMessage(ctx, runtimeWSErrorMessage(msg.ID, httpx.BadRequest("run_id 不是合法 uuid")))
			return
		}
		event, err := s.ReportRuntimeTokenRunEvent(ctx, conn.plaintext, runID, &ReportRunEventRequest{
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
		result, err := s.CompleteRuntimePullRun(ctx, conn.plaintext, runID, &RuntimePullResultRequest{
			Status:     msg.Status,
			Output:     msg.Output,
			Events:     msg.Events,
			Error:      msg.Error,
			DurationMs: msg.DurationMs,
		})
		if err != nil {
			_ = conn.sendServerMessage(ctx, runtimeWSErrorMessage(msg.ID, err))
			return
		}
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

func (s *Service) dispatchRuntimeWSRunBestEffort(ctx context.Context, agentID uuid.UUID) {
	if s.wsHub == nil {
		return
	}
	for _, conn := range s.wsHub.connections(agentID) {
		assigned, err := s.assignRuntimeWSRun(ctx, conn)
		if err != nil {
			log.Warn().Err(err).Str("agent_id", agentID.String()).Msg("runtime.ws: assign failed")
			continue
		}
		if assigned {
			return
		}
	}
}

func (s *Service) assignRuntimeWSRun(ctx context.Context, conn *runtimeWSConn) (bool, error) {
	run, err := s.claimRuntimePullRunOnce(ctx, conn.token)
	if err != nil {
		return false, err
	}
	if run == nil {
		return false, nil
	}
	return true, conn.sendServerMessage(ctx, runtimeWSAssignmentMessage(run))
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
