// Command a2a-demo verifies a real local Agent-to-Agent completion path through a running API.
//
// Start the API with ALLOW_LOCAL_HTTP_ENDPOINTS=true before running this command. It creates a
// throwaway user, self-registers three loopback demo Agents with a registration-purpose access token, publishes
// a task recommendation, invokes the parent Agent, and fails unless both delegated child runs
// reach success.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type client struct {
	baseURL string
	token   string
	http    *http.Client
}

type authResponse struct {
	JWT string `json:"jwt"`
}

type bootstrapResponse struct {
	PlaintextToken string `json:"plaintext_token"`
}

type agentResponse struct {
	ID   string `json:"id"`
	Slug string `json:"slug"`
}

type registrationResponse struct {
	Agent        agentResponse `json:"agent"`
	RuntimeToken struct {
		PlaintextToken string `json:"plaintext_token"`
	} `json:"runtime_token"`
}

type runResponse struct {
	RunID       string         `json:"run_id"`
	Status      string         `json:"status"`
	Output      map[string]any `json:"output,omitempty"`
	ParentRunID string         `json:"parent_run_id,omitempty"`
}

type childRun struct {
	ChildRunID string `json:"child_run_id"`
	Status     string `json:"status"`
}

type childrenResponse struct {
	Items []childRun `json:"items"`
}

type eventsResponse struct {
	Events []struct {
		EventType string `json:"event_type"`
	} `json:"events"`
}

type taskRecommendation struct {
	Agent agentResponse `json:"agent"`
}

type taskResponse struct {
	TaskID          string               `json:"task_id"`
	ParsedSkills    []string             `json:"parsed_skills"`
	MCPTools        []string             `json:"mcp_tools"`
	Recommendations []taskRecommendation `json:"recommendations"`
}

type taskDemoResult struct {
	TaskID        string
	PlannerChosen bool
}

type demoResult struct {
	Email       string
	ParentAgent agentResponse
	WorkerAgent agentResponse
	ReviewAgent agentResponse
	TaskID      string
	ParentRunID string
	ChildRunIDs []string
}

type endpointConfig struct {
	mu           sync.RWMutex
	apiURL       string
	runtimeToken string
	workerID     string
	reviewerID   string
}

func main() {
	os.Exit(runMain(os.Args[1:], os.Stdout, os.Stderr, waitForSignal))
}

func runMain(args []string, stdout, stderr io.Writer, waitForStop func()) int {
	fs := flag.NewFlagSet("a2a-demo", flag.ContinueOnError)
	fs.SetOutput(stderr)
	apiURL := fs.String("api", "http://localhost:8080", "OpenLinker API base URL")
	serve := fs.Bool("serve", false, "Keep demo endpoints online for repeated Playground calls")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfg := &endpointConfig{apiURL: strings.TrimRight(*apiURL, "/")}
	worker := httptest.NewServer(http.HandlerFunc(workerEndpoint))
	defer worker.Close()
	reviewer := httptest.NewServer(http.HandlerFunc(reviewerEndpoint))
	defer reviewer.Close()
	caller := httptest.NewServer(http.HandlerFunc(cfg.callerEndpoint))
	defer caller.Close()

	api := &client{baseURL: cfg.apiURL, http: &http.Client{Timeout: 10 * time.Second}}
	result, err := run(api, cfg, caller.URL, worker.URL, reviewer.URL)
	if err != nil {
		fmt.Fprintf(stderr, "A2A demo failed: %v\n", err)
		return 1
	}
	if *serve {
		printServeInstructions(stdout, result)
		waitForStop()
		fmt.Fprintln(stdout, "Stopping local demo endpoints.")
	}
	return 0
}

func waitForSignal() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
}

func printServeInstructions(w io.Writer, result *demoResult) {
	fmt.Fprintln(w, "Local demo endpoints are online until interrupted.")
	fmt.Fprintf(w, "playground: /playground/%s\n", result.ParentAgent.Slug)
	fmt.Fprintf(w, "task: /tasks/%s\n", result.TaskID)
	fmt.Fprintf(w, "a2a trace: /a2a?run_id=%s\n", result.ParentRunID)
}

func run(api *client, cfg *endpointConfig, callerURL, workerURL, reviewerURL string) (*demoResult, error) {
	email := fmt.Sprintf("a2a-demo-%d@example.local", time.Now().UnixNano())
	var signedUp authResponse
	if err := api.do(http.MethodPost, "/api/v1/auth/register", map[string]any{
		"email": email, "password": "local-demo-pass-123", "display_name": "Local A2A Demo",
	}, &signedUp); err != nil {
		return nil, fmt.Errorf("register user: %w", err)
	}
	api.token = signedUp.JWT

	if err := api.do(http.MethodPost, "/api/v1/me/become-creator", map[string]any{}, nil); err != nil {
		return nil, fmt.Errorf("become creator: %w", err)
	}

	var bootstrap bootstrapResponse
	if err := api.do(http.MethodPost, "/api/v1/creator/agent-registration-tokens", map[string]any{
		"label": "local a2a demo", "expires_in_minutes": 30, "max_agents": 3,
	}, &bootstrap); err != nil {
		return nil, fmt.Errorf("mint bootstrap token: %w", err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	workerAgent, err := registerAgent(api, bootstrap.PlaintextToken, "demo-worker-"+suffix, "Local Worker Agent", workerURL, []string{"ops/document-generate"})
	if err != nil {
		return nil, fmt.Errorf("self-register worker: %w", err)
	}
	reviewerAgent, err := registerAgent(api, bootstrap.PlaintextToken, "demo-reviewer-"+suffix, "Local Reviewer Agent", reviewerURL, []string{"content/summarization"})
	if err != nil {
		return nil, fmt.Errorf("self-register reviewer: %w", err)
	}
	callerAgent, err := registerAgent(api, bootstrap.PlaintextToken, "demo-planner-"+suffix, "Local Planner Agent", callerURL, []string{
		"ai/agent-orchestration",
		"ai/prompt-engineering",
		"ops/document-generate",
		"content/summarization",
		"data/analysis",
	})
	if err != nil {
		return nil, fmt.Errorf("self-register planner: %w", err)
	}

	cfg.mu.Lock()
	cfg.runtimeToken = callerAgent.RuntimeToken.PlaintextToken
	cfg.workerID = workerAgent.Agent.ID
	cfg.reviewerID = reviewerAgent.Agent.ID
	cfg.mu.Unlock()

	task, err := publishTask(api, callerAgent.Agent.ID)
	if err != nil {
		return nil, err
	}

	var parent runResponse
	if err := api.do(http.MethodPost, "/api/v1/run", map[string]any{
		"agent_id": callerAgent.Agent.ID,
		"input":    map[string]any{"task": "plan then delegate this task"},
	}, &parent); err != nil {
		return nil, fmt.Errorf("invoke parent: %w", err)
	}
	if parent.Status != "success" {
		return nil, fmt.Errorf("parent run ended with status %q", parent.Status)
	}

	var children childrenResponse
	if err := api.do(http.MethodGet, "/api/v1/runs/"+parent.RunID+"/children", nil, &children); err != nil {
		return nil, fmt.Errorf("read child runs: %w", err)
	}
	if len(children.Items) != 2 {
		return nil, fmt.Errorf("expected two child runs, got %+v", children.Items)
	}
	childIDs := make([]string, 0, len(children.Items))
	for _, child := range children.Items {
		if child.Status != "success" {
			return nil, fmt.Errorf("expected successful child runs, got %+v", children.Items)
		}
		childIDs = append(childIDs, child.ChildRunID)
	}

	var events eventsResponse
	if err := api.do(http.MethodGet, "/api/v1/runs/"+parent.RunID+"/events", nil, &events); err != nil {
		return nil, fmt.Errorf("read parent events: %w", err)
	}
	if countEvent(events, "run.child.created") != 2 || countEvent(events, "run.child.completed") != 2 {
		return nil, errors.New("parent trace is missing delegation lifecycle events")
	}

	fmt.Println("A2A local completion verified")
	fmt.Printf("user: %s\n", email)
	taskStatus := "published"
	if task.PlannerChosen {
		taskStatus = "published and planner chosen"
	}
	fmt.Printf("task: %s [%s]\n", task.TaskID, taskStatus)
	fmt.Printf("parent agent: %s (%s)\n", callerAgent.Agent.Slug, callerAgent.Agent.ID)
	fmt.Printf("worker agent: %s (%s)\n", workerAgent.Agent.Slug, workerAgent.Agent.ID)
	fmt.Printf("reviewer agent: %s (%s)\n", reviewerAgent.Agent.Slug, reviewerAgent.Agent.ID)
	fmt.Printf("parent run: %s [success]\n", parent.RunID)
	for i, childID := range childIDs {
		fmt.Printf("child run %d: %s [success]\n", i+1, childID)
	}
	return &demoResult{
		Email:       email,
		ParentAgent: callerAgent.Agent,
		WorkerAgent: workerAgent.Agent,
		ReviewAgent: reviewerAgent.Agent,
		TaskID:      task.TaskID,
		ParentRunID: parent.RunID,
		ChildRunIDs: childIDs,
	}, nil
}

func publishTask(api *client, plannerID string) (*taskDemoResult, error) {
	var task taskResponse
	err := api.do(http.MethodPost, "/api/v1/tasks/recommend", map[string]any{
		"query": "请用 Agent 编排完成报告文档生成，并让另一个 Agent 做摘要复核",
		"skill_ids": []string{
			"ai/agent-orchestration",
			"ai/prompt-engineering",
			"ops/document-generate",
			"content/summarization",
			"data/analysis",
		},
		"mcp_tools": []string{"create_task", "run_agent", "get_run"},
	}, &task)
	if err != nil {
		return nil, fmt.Errorf("publish task: %w", err)
	}
	if task.TaskID == "" {
		return nil, errors.New("published task did not return task_id")
	}
	plannerRecommended := false
	for _, rec := range task.Recommendations {
		if rec.Agent.ID == plannerID {
			plannerRecommended = true
			break
		}
	}
	if !plannerRecommended {
		fmt.Fprintf(os.Stderr, "warning: published task recommendations missing planner %s; continuing with direct A2A run\n", plannerID)
		return &taskDemoResult{TaskID: task.TaskID}, nil
	}
	if err := api.do(http.MethodPost, "/api/v1/tasks/"+task.TaskID+"/choose", map[string]any{
		"agent_id": plannerID,
	}, nil); err != nil {
		return nil, fmt.Errorf("choose planner for task: %w", err)
	}
	return &taskDemoResult{TaskID: task.TaskID, PlannerChosen: true}, nil
}

func registerAgent(api *client, bootstrap, slug, name, endpoint string, skillIDs []string) (*registrationResponse, error) {
	var registered registrationResponse
	err := api.do(http.MethodPost, "/api/v1/agent-registration/agents", map[string]any{
		"bootstrap_token":      bootstrap,
		"slug":                 slug,
		"name":                 name,
		"description":          "Local endpoint used to verify a real Agent-to-Agent completion flow.",
		"endpoint_url":         endpoint,
		"price_per_call_cents": 0,
		"tags":                 []string{"local-demo", "a2a"},
		"skill_ids":            skillIDs,
		"visibility":           "public",
	}, &registered)
	return &registered, err
}

func workerEndpoint(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"output": map[string]any{"result": "worker generated the report draft"},
	})
}

func reviewerEndpoint(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"output": map[string]any{"result": "reviewer summarized and checked the draft"},
	})
}

func (cfg *endpointConfig) callerEndpoint(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var incoming struct {
		RunID string         `json:"run_id"`
		Input map[string]any `json:"input"`
	}
	if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cfg.mu.RLock()
	token := cfg.runtimeToken
	workerID := cfg.workerID
	reviewerID := cfg.reviewerID
	cfg.mu.RUnlock()

	api := &client{baseURL: cfg.apiURL, token: token, http: &http.Client{Timeout: 10 * time.Second}}
	var workerChild runResponse
	err := api.do(http.MethodPost, "/api/v1/agent-runtime/call-agent", map[string]any{
		"parent_run_id":   incoming.RunID,
		"target_agent_id": workerID,
		"reason":          "planner delegates report generation to worker",
		"input":           map[string]any{"task": incoming.Input["task"]},
	}, &workerChild)
	if err == nil {
		var reviewerChild runResponse
		err = api.do(http.MethodPost, "/api/v1/agent-runtime/call-agent", map[string]any{
			"parent_run_id":   incoming.RunID,
			"target_agent_id": reviewerID,
			"reason":          "planner delegates summary review to reviewer",
			"input": map[string]any{
				"task":          incoming.Input["task"],
				"worker_run_id": workerChild.RunID,
			},
		}, &reviewerChild)
		if err == nil {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"output": map[string]any{
					"result":          "planner completed after two delegations",
					"worker_run_id":   workerChild.RunID,
					"worker_status":   workerChild.Status,
					"reviewer_run_id": reviewerChild.RunID,
					"reviewer_status": reviewerChild.Status,
				},
			})
			return
		}
	}
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{"code": "DELEGATION_FAILED", "message": err.Error()},
		})
		return
	}
}

func (c *client) do(method, path string, body any, output any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s returned %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if output == nil || len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, output)
}

func countEvent(payload eventsResponse, expected string) int {
	count := 0
	for _, event := range payload.Events {
		if event.EventType == expected {
			count++
		}
	}
	return count
}
