package workflow_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
)

// Every change advances the run revision; the snapshot carries logical
// nodes.
func TestWorkflowRunRevisionAndSnapshot(t *testing.T) {
	f := okAgentFixture(t)
	ctx := context.Background()
	queued, err := f.svc.StartWorkflowRun(ctx, f.ownerID, f.workflowID, &workflow.RunWorkflowRequest{})
	require.NoError(t, err)
	runID := uuid.MustParse(queued.ID)
	before, err := f.svc.GetWorkflowRun(ctx, f.ownerID, runID)
	require.NoError(t, err)

	claimed, err := f.svc.ClaimAndRunPendingWorkflow(ctx)
	require.NoError(t, err)
	require.True(t, claimed)
	after, err := f.svc.GetWorkflowRun(ctx, f.ownerID, runID)
	require.NoError(t, err)
	require.Equal(t, "success", after.Status)
	require.Greater(t, after.Revision, before.Revision+3, "claim, node init, start, settle and finish each advance it")
	require.Len(t, after.Nodes, 1)
	require.Equal(t, "success", after.Nodes[0].State)
	require.EqualValues(t, 1, after.Nodes[0].EffectiveAttempt)
}

type sseEvent struct {
	id    int64
	event string
	data  string
}

func readSSE(t *testing.T, body *bufio.Reader, deadline time.Time) (sseEvent, bool) {
	t.Helper()
	var event sseEvent
	for time.Now().Before(deadline) {
		line, err := body.ReadString('\n')
		if err != nil {
			return event, false
		}
		line = strings.TrimRight(line, "\n")
		switch {
		case line == "":
			if event.event != "" {
				return event, true
			}
		case strings.HasPrefix(line, "id: "):
			event.id, _ = strconv.ParseInt(strings.TrimPrefix(line, "id: "), 10, 64)
		case strings.HasPrefix(line, "event: "):
			event.event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			event.data = strings.TrimPrefix(line, "data: ")
		}
	}
	return event, false
}

// The event stream sends a snapshot on connect, a new snapshot whenever the
// revision advances (woken by notifications), and closes after a terminal
// snapshot. Reconnecting with the last id sends nothing new.
func TestWorkflowRunEventStream(t *testing.T) {
	f := newAdmissionFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.svc.StartWorkflowRunEventListener(ctx)

	e := echo.New()
	api := e.Group("/api/v1")
	setUser := func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set(string(httpx.CtxKeyUserID), f.ownerID.String())
			return next(c)
		}
	}
	workflow.NewHandler(f.svc).RegisterProtected(api, setUser)
	server := httptest.NewServer(e)
	defer server.Close()

	runID, done := f.startBlockedRun(t, 3)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/v1/workflow-runs/"+runID.String()+"/events", nil)
	require.NoError(t, err)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Contains(t, response.Header.Get("Content-Type"), "text/event-stream")
	reader := bufio.NewReader(response.Body)

	first, ok := readSSE(t, reader, time.Now().Add(5*time.Second))
	require.True(t, ok)
	require.Equal(t, "snapshot", first.event)
	var snapshot workflow.WorkflowRunResponse
	require.NoError(t, json.Unmarshal([]byte(first.data), &snapshot))
	require.Equal(t, "running", snapshot.Status)
	require.Equal(t, first.id, snapshot.Revision)

	f.release()
	waitWorker(t, done)
	last := first
	for {
		event, ok := readSSE(t, reader, time.Now().Add(10*time.Second))
		if !ok {
			break // the server closed the stream after the terminal snapshot
		}
		require.Greater(t, event.id, last.id, "revisions only move forward")
		last = event
	}
	var final workflow.WorkflowRunResponse
	require.NoError(t, json.Unmarshal([]byte(last.data), &final))
	require.Equal(t, "success", final.Status)

	// Resuming at the latest revision of a finished run yields no snapshot.
	resume, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/v1/workflow-runs/"+runID.String()+"/events", nil)
	require.NoError(t, err)
	resume.Header.Set("Last-Event-ID", strconv.FormatInt(last.id, 10))
	resumed, err := http.DefaultClient.Do(resume)
	require.NoError(t, err)
	defer resumed.Body.Close()
	_, ok = readSSE(t, bufio.NewReader(resumed.Body), time.Now().Add(3*time.Second))
	require.False(t, ok)
}

func TestWorkflowDefinitionLimitsAreConfigurable(t *testing.T) {
	f := okAgentFixture(t)
	ctx := context.Background()
	nodes := make([]workflow.WorkflowNodeRequest, 0, 12)
	for i := 0; i < 12; i++ {
		nodes = append(nodes, workflow.WorkflowNodeRequest{Key: "n" + strconv.Itoa(i), AgentID: f.agentID})
	}
	_, err := f.svc.CreateWorkflow(ctx, f.ownerID, &workflow.CreateWorkflowRequest{Name: "twelve", Nodes: nodes})
	require.NoError(t, err, "12 nodes fit the default limit of 30")
	f.svc.SetWorkflowLimits(10, 60)
	_, err = f.svc.CreateWorkflow(ctx, f.ownerID, &workflow.CreateWorkflowRequest{Name: "twelve", Nodes: nodes})
	require.ErrorContains(t, err, "10")
}
