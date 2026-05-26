// Command a2a-demo verifies a real local Agent-to-Agent completion path through a running API.
//
// Start the API with ALLOW_LOCAL_HTTP_ENDPOINTS=true before running this command. It creates a
// throwaway user, self-registers two loopback demo Agents with a Bootstrap Token, invokes the
// parent Agent, and fails unless both parent and delegated child runs reach success.
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

type demoResult struct {
	Email       string
	ParentAgent agentResponse
	ChildAgent  agentResponse
	ParentRunID string
	ChildRunID  string
}

type endpointConfig struct {
	mu           sync.RWMutex
	apiURL       string
	runtimeToken string
	targetID     string
}

func main() {
	apiURL := flag.String("api", "http://localhost:8080", "OpenLinker API base URL")
	serve := flag.Bool("serve", false, "Keep demo endpoints online for repeated Playground calls")
	flag.Parse()

	cfg := &endpointConfig{apiURL: strings.TrimRight(*apiURL, "/")}
	worker := httptest.NewServer(http.HandlerFunc(workerEndpoint))
	defer worker.Close()
	caller := httptest.NewServer(http.HandlerFunc(cfg.callerEndpoint))
	defer caller.Close()

	api := &client{baseURL: cfg.apiURL, http: &http.Client{Timeout: 10 * time.Second}}
	result, err := run(api, cfg, caller.URL, worker.URL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "A2A demo failed: %v\n", err)
		os.Exit(1)
	}
	if *serve {
		fmt.Println("Local demo endpoints are online until interrupted.")
		fmt.Printf("playground: /playground/%s\n", result.ParentAgent.Slug)
		fmt.Printf("a2a trace: /a2a?run_id=%s\n", result.ParentRunID)
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
		<-stop
		fmt.Println("Stopping local demo endpoints.")
	}
}

func run(api *client, cfg *endpointConfig, callerURL, workerURL string) (*demoResult, error) {
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
		"label": "local a2a demo", "expires_in_minutes": 30, "max_agents": 2,
	}, &bootstrap); err != nil {
		return nil, fmt.Errorf("mint bootstrap token: %w", err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	workerAgent, err := registerAgent(api, bootstrap.PlaintextToken, "demo-worker-"+suffix, "Local Worker Agent", workerURL)
	if err != nil {
		return nil, fmt.Errorf("self-register worker: %w", err)
	}
	callerAgent, err := registerAgent(api, bootstrap.PlaintextToken, "demo-planner-"+suffix, "Local Planner Agent", callerURL)
	if err != nil {
		return nil, fmt.Errorf("self-register planner: %w", err)
	}

	cfg.mu.Lock()
	cfg.runtimeToken = callerAgent.RuntimeToken.PlaintextToken
	cfg.targetID = workerAgent.Agent.ID
	cfg.mu.Unlock()

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
	if len(children.Items) != 1 || children.Items[0].Status != "success" {
		return nil, fmt.Errorf("expected one successful child run, got %+v", children.Items)
	}

	var events eventsResponse
	if err := api.do(http.MethodGet, "/api/v1/runs/"+parent.RunID+"/events", nil, &events); err != nil {
		return nil, fmt.Errorf("read parent events: %w", err)
	}
	if !hasEvent(events, "run.child.created") || !hasEvent(events, "run.child.completed") {
		return nil, errors.New("parent trace is missing delegation lifecycle events")
	}

	fmt.Println("A2A local completion verified")
	fmt.Printf("user: %s\n", email)
	fmt.Printf("parent agent: %s (%s)\n", callerAgent.Agent.Slug, callerAgent.Agent.ID)
	fmt.Printf("child agent: %s (%s)\n", workerAgent.Agent.Slug, workerAgent.Agent.ID)
	fmt.Printf("parent run: %s [success]\n", parent.RunID)
	fmt.Printf("child run: %s [success]\n", children.Items[0].ChildRunID)
	return &demoResult{
		Email:       email,
		ParentAgent: callerAgent.Agent,
		ChildAgent:  workerAgent.Agent,
		ParentRunID: parent.RunID,
		ChildRunID:  children.Items[0].ChildRunID,
	}, nil
}

func registerAgent(api *client, bootstrap, slug, name, endpoint string) (*registrationResponse, error) {
	var registered registrationResponse
	err := api.do(http.MethodPost, "/api/v1/agent-registration/agents", map[string]any{
		"bootstrap_token":      bootstrap,
		"slug":                 slug,
		"name":                 name,
		"description":          "Local endpoint used to verify a real Agent-to-Agent completion flow.",
		"endpoint_url":         endpoint,
		"price_per_call_cents": 0,
		"tags":                 []string{"local-demo", "a2a"},
		"visibility":           "public",
	}, &registered)
	return &registered, err
}

func workerEndpoint(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"output": map[string]any{"result": "worker completed delegated task"},
	})
}

func (cfg *endpointConfig) callerEndpoint(w http.ResponseWriter, r *http.Request) {
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
	targetID := cfg.targetID
	cfg.mu.RUnlock()

	api := &client{baseURL: cfg.apiURL, token: token, http: &http.Client{Timeout: 10 * time.Second}}
	var child runResponse
	err := api.do(http.MethodPost, "/api/v1/agent-runtime/call-agent", map[string]any{
		"parent_run_id":   incoming.RunID,
		"target_agent_id": targetID,
		"reason":          "planner delegates execution to worker",
		"input":           map[string]any{"task": incoming.Input["task"]},
	}, &child)
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{"code": "DELEGATION_FAILED", "message": err.Error()},
		})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"output": map[string]any{
			"result":       "planner completed after delegation",
			"child_run_id": child.RunID,
			"child_status": child.Status,
		},
	})
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

func hasEvent(payload eventsResponse, expected string) bool {
	for _, event := range payload.Events {
		if event.EventType == expected {
			return true
		}
	}
	return false
}
