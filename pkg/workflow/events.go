package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	workflowEventsPollWithListener    = 15 * time.Second
	workflowEventsPollWithoutListener = 2 * time.Second
	workflowEventsHeartbeat           = 15 * time.Second
)

// workflowRunListener fans PostgreSQL workflow_run.changed notifications out
// to subscribers of each run. Notifications are only wake-ups: subscribers
// always re-read a snapshot and compare revisions.
type workflowRunListener struct {
	mu          sync.Mutex
	subscribers map[uuid.UUID]map[chan struct{}]struct{}
	connected   bool
}

// StartWorkflowRunEventListener listens for run change notifications until
// ctx ends, reconnecting after failures. Without it, event streams poll.
func (s *Service) StartWorkflowRunEventListener(ctx context.Context) {
	if s == nil || s.pool == nil {
		return
	}
	listener := s.runListener()
	go func() {
		backoff := time.Second
		for ctx.Err() == nil {
			err := listener.listen(ctx, s)
			if ctx.Err() != nil {
				return
			}
			log.Warn().Err(err).Msg("workflow: run event listener disconnected")
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
		}
	}()
}

func (s *Service) runListener() *workflowRunListener {
	s.listenerOnce.Do(func() {
		s.listener = &workflowRunListener{subscribers: map[uuid.UUID]map[chan struct{}]struct{}{}}
	})
	return s.listener
}

func (l *workflowRunListener) listen(ctx context.Context, s *Service) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `LISTEN "`+workflowRunChangedChannel+`"`); err != nil {
		return err
	}
	l.setConnected(true)
	defer l.setConnected(false)
	l.wakeAll() // anything missed while disconnected
	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if id, parseErr := uuid.Parse(notification.Payload); parseErr == nil {
			l.wake(id)
		}
	}
}

func (l *workflowRunListener) setConnected(connected bool) {
	l.mu.Lock()
	l.connected = connected
	l.mu.Unlock()
}

func (l *workflowRunListener) isConnected() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.connected
}

func (l *workflowRunListener) subscribe(runID uuid.UUID) (chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	l.mu.Lock()
	if l.subscribers[runID] == nil {
		l.subscribers[runID] = map[chan struct{}]struct{}{}
	}
	l.subscribers[runID][ch] = struct{}{}
	l.mu.Unlock()
	return ch, func() {
		l.mu.Lock()
		delete(l.subscribers[runID], ch)
		if len(l.subscribers[runID]) == 0 {
			delete(l.subscribers, runID)
		}
		l.mu.Unlock()
	}
}

func (l *workflowRunListener) wake(runID uuid.UUID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for ch := range l.subscribers[runID] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (l *workflowRunListener) wakeAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, subscribers := range l.subscribers {
		for ch := range subscribers {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
}

func workflowRunTerminal(status string) bool {
	switch status {
	case workflowRunStatusSuccess, workflowRunStatusFailed, workflowRunStatusCanceled:
		return true
	}
	return false
}

// RunEvents streams a workflow run as server-sent events: a full snapshot
// (event "snapshot", id = revision) on connect and whenever the revision
// advances, until the run is terminal. Last-Event-ID resumes: a snapshot is
// sent only when the revision moved past it. Notifications wake the stream;
// a periodic re-read covers lost notifications.
func (h *Handler) RunEvents(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	id, err := pathUUID(c)
	if err != nil {
		return err
	}
	if _, err := h.requireWorkflowRunPermission(c, uid, id, "workflows:read"); err != nil {
		return err
	}
	svc, ok := h.svc.(*Service)
	if !ok {
		return httpx.NewError(http.StatusNotImplemented, "NOT_IMPLEMENTED", "workflow run events are not available")
	}
	var lastSent int64 = -1
	if raw := strings.TrimSpace(c.Request().Header.Get("Last-Event-ID")); raw != "" {
		if parsed, parseErr := strconv.ParseInt(raw, 10, 64); parseErr == nil && parsed >= 0 {
			lastSent = parsed
		}
	}

	res := c.Response()
	if _, ok := res.Writer.(http.Flusher); !ok {
		return httpx.Internal("当前响应不支持 streaming")
	}
	res.Writer = httpx.NewStreamingResponseWriter(res.Writer, httpx.StreamWriteWindow)
	flusher := res.Writer.(http.Flusher)
	res.Header().Set(echo.HeaderContentType, "text/event-stream")
	res.Header().Set(echo.HeaderCacheControl, "no-cache")
	res.Header().Set(echo.HeaderConnection, "keep-alive")
	res.WriteHeader(http.StatusOK)

	ctx := c.Request().Context()
	listener := svc.runListener()
	wake, unsubscribe := listener.subscribe(id)
	defer unsubscribe()
	pollEvery := workflowEventsPollWithoutListener
	if listener.isConnected() {
		pollEvery = workflowEventsPollWithListener
	}
	poll := time.NewTicker(pollEvery)
	defer poll.Stop()
	heartbeat := time.NewTicker(workflowEventsHeartbeat)
	defer heartbeat.Stop()

	for {
		snapshot, err := svc.GetWorkflowRun(ctx, uid, id)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			payload, _ := json.Marshal(map[string]string{"message": "workflow run snapshot unavailable"})
			_, _ = fmt.Fprintf(res.Writer, "event: error\ndata: %s\n\n", payload)
			flusher.Flush()
			return nil
		}
		if snapshot.Revision > lastSent {
			payload, err := json.Marshal(snapshot)
			if err != nil {
				return nil
			}
			if _, err := fmt.Fprintf(res.Writer, "id: %d\nevent: snapshot\ndata: %s\n\n", snapshot.Revision, payload); err != nil {
				return nil
			}
			flusher.Flush()
			lastSent = snapshot.Revision
		}
		if workflowRunTerminal(snapshot.Status) {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-wake:
		case <-poll.C:
		case <-heartbeat.C:
			if _, err := fmt.Fprint(res.Writer, ": heartbeat\n\n"); err != nil {
				return nil
			}
			flusher.Flush()
		}
	}
}
