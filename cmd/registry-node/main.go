package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	defaultAPIBase      = "http://127.0.0.1:8080"
	defaultPollInterval = 5 * time.Second
	defaultHTTPTimeout  = 60 * time.Second
	maxResponseBody     = 1 << 20
)

type endpointMap map[string]string

func (m endpointMap) String() string {
	if len(m) == 0 {
		return ""
	}
	parts := make([]string, 0, len(m))
	for k, v := range m {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ",")
}

func (m endpointMap) Set(raw string) error {
	return m.add(raw)
}

func (m endpointMap) add(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.SplitN(raw, "=", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return fmt.Errorf("agent endpoint must be local_agent_id=url")
	}
	agentID := strings.TrimSpace(parts[0])
	endpoint := strings.TrimSpace(parts[1])
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("agent endpoint url is invalid: %s", endpoint)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("agent endpoint only supports http/https: %s", endpoint)
	}
	m[agentID] = endpoint
	return nil
}

func (m endpointMap) addList(raw string) error {
	for _, item := range strings.Split(raw, ",") {
		if err := m.add(item); err != nil {
			return err
		}
	}
	return nil
}

type config struct {
	APIBase      string
	NodeSecret   string
	Endpoints    endpointMap
	Once         bool
	Interval     time.Duration
	HTTPTimeout  time.Duration
	SyncMetadata bool
}

func parseConfig(args []string, env map[string]string) (config, error) {
	cfg := config{
		APIBase:      firstNonEmpty(env["OPENLINKER_API_ROOT"], env["OPENLINKER_API_URL"], defaultAPIBase),
		NodeSecret:   env["OPENLINKER_NODE_SECRET"],
		Endpoints:    endpointMap{},
		Interval:     defaultPollInterval,
		HTTPTimeout:  defaultHTTPTimeout,
		SyncMetadata: true,
	}
	if raw := env["OPENLINKER_AGENT_ENDPOINTS"]; raw != "" {
		if err := cfg.Endpoints.addList(raw); err != nil {
			return cfg, err
		}
	}

	fs := flag.NewFlagSet("registry-node", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.APIBase, "api", cfg.APIBase, "OpenLinker API root, with or without /api/v1")
	fs.StringVar(&cfg.NodeSecret, "secret", cfg.NodeSecret, "Registry Node secret, or OPENLINKER_NODE_SECRET")
	fs.Var(cfg.Endpoints, "agent-endpoint", "Repeatable local_agent_id=url mapping for claimed proxy runs")
	fs.BoolVar(&cfg.Once, "once", false, "Run one heartbeat/claim cycle and exit")
	fs.DurationVar(&cfg.Interval, "interval", cfg.Interval, "Polling interval")
	fs.DurationVar(&cfg.HTTPTimeout, "timeout", cfg.HTTPTimeout, "HTTP timeout for cloud and local agent calls")
	fs.BoolVar(&cfg.SyncMetadata, "sync-metadata", cfg.SyncMetadata, "Sync cloud listing metadata each cycle")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	cfg.APIBase = normalizeAPIBase(cfg.APIBase)
	cfg.NodeSecret = strings.TrimSpace(cfg.NodeSecret)
	if cfg.NodeSecret == "" {
		return cfg, errors.New("missing -secret or OPENLINKER_NODE_SECRET")
	}
	if len(cfg.Endpoints) == 0 {
		return cfg, errors.New("missing -agent-endpoint local_agent_id=url mapping")
	}
	if cfg.Interval <= 0 {
		return cfg, errors.New("-interval must be positive")
	}
	if cfg.HTTPTimeout <= 0 {
		return cfg, errors.New("-timeout must be positive")
	}
	return cfg, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func normalizeAPIBase(raw string) string {
	raw = strings.TrimRight(strings.TrimSpace(raw), "/")
	if strings.HasSuffix(raw, "/api/v1") {
		return raw
	}
	return raw + "/api/v1"
}

type cloudClient struct {
	base       string
	secret     string
	httpClient *http.Client
}

func newCloudClient(cfg config) *cloudClient {
	return &cloudClient{
		base:   cfg.APIBase,
		secret: cfg.NodeSecret,
		httpClient: &http.Client{
			Timeout: cfg.HTTPTimeout,
		},
	}
}

type heartbeatResponse struct {
	NodeID             string `json:"node_id"`
	HeartbeatStatus    string `json:"heartbeat_status"`
	LinkedListingCount int32  `json:"linked_listing_count"`
	PendingRunCount    int32  `json:"pending_run_count"`
}

type nodeMetadataSyncResponse struct {
	RegistryNodeID     string `json:"registry_node_id"`
	SyncedListingCount int32  `json:"synced_listing_count"`
}

type proxyRun struct {
	ID               string         `json:"id"`
	CloudRunID       string         `json:"cloud_run_id"`
	CloudListingID   string         `json:"cloud_listing_id"`
	RegistryNodeID   string         `json:"registry_node_id"`
	LocalAgentID     string         `json:"local_agent_id"`
	RequestingUserID string         `json:"requesting_user_id"`
	Status           string         `json:"status"`
	PayloadPolicy    string         `json:"payload_policy"`
	Input            map[string]any `json:"input,omitempty"`
	InputSummary     string         `json:"input_summary,omitempty"`
	Output           map[string]any `json:"output,omitempty"`
	OutputSummary    string         `json:"output_summary,omitempty"`
	ErrorCode        string         `json:"error_code,omitempty"`
	ErrorMessage     string         `json:"error_message,omitempty"`
	AttemptCount     int32          `json:"attempt_count,omitempty"`
	MaxAttempts      int32          `json:"max_attempts,omitempty"`
	NextRetryAt      string         `json:"next_retry_at,omitempty"`
}

type completeProxyRunRequest struct {
	Status        string         `json:"status"`
	Output        map[string]any `json:"output,omitempty"`
	OutputSummary string         `json:"output_summary,omitempty"`
	ErrorCode     string         `json:"error_code,omitempty"`
	ErrorMessage  string         `json:"error_message,omitempty"`
	Retryable     bool           `json:"retryable,omitempty"`
}

func (c *cloudClient) post(ctx context.Context, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.secret)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.doJSON(req, out, http.StatusOK, http.StatusCreated, http.StatusAccepted)
}

func (c *cloudClient) get(ctx context.Context, path string, out any, okStatuses ...int) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.secret)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if statusAllowed(resp.StatusCode, okStatuses...) {
		if out != nil && resp.StatusCode != http.StatusNoContent {
			defer resp.Body.Close()
			if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBody)).Decode(out); err != nil {
				return resp, err
			}
		} else {
			_ = resp.Body.Close()
		}
		return resp, nil
	}
	defer resp.Body.Close()
	return resp, fmt.Errorf("cloud API %s returned %d: %s", path, resp.StatusCode, readSmallBody(resp.Body))
}

func (c *cloudClient) doJSON(req *http.Request, out any, okStatuses ...int) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if !statusAllowed(resp.StatusCode, okStatuses...) {
		return fmt.Errorf("cloud API %s returned %d: %s", req.URL.Path, resp.StatusCode, readSmallBody(resp.Body))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(io.LimitReader(resp.Body, maxResponseBody)).Decode(out)
}

func statusAllowed(status int, okStatuses ...int) bool {
	for _, ok := range okStatuses {
		if status == ok {
			return true
		}
	}
	return false
}

func readSmallBody(r io.Reader) string {
	raw, _ := io.ReadAll(io.LimitReader(r, 4096))
	return strings.TrimSpace(string(raw))
}

func (c *cloudClient) heartbeat(ctx context.Context) (*heartbeatResponse, error) {
	var resp heartbeatResponse
	if err := c.post(ctx, "/registry-node/heartbeat", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *cloudClient) syncMetadata(ctx context.Context) (*nodeMetadataSyncResponse, error) {
	var resp nodeMetadataSyncResponse
	if err := c.post(ctx, "/registry-node/metadata-sync", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *cloudClient) claim(ctx context.Context) (*proxyRun, bool, error) {
	var run proxyRun
	resp, err := c.get(ctx, "/proxy/runs/claim", &run, http.StatusOK, http.StatusNoContent)
	if err != nil {
		return nil, false, err
	}
	if resp.StatusCode == http.StatusNoContent {
		return nil, false, nil
	}
	return &run, true, nil
}

func (c *cloudClient) complete(ctx context.Context, runID string, req completeProxyRunRequest) (*proxyRun, error) {
	var resp proxyRun
	if err := c.post(ctx, "/proxy/runs/"+url.PathEscape(runID)+"/result", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

type agentRequest struct {
	Input       map[string]any `json:"input"`
	Metadata    map[string]any `json:"metadata"`
	RunID       string         `json:"run_id"`
	ParentRunID string         `json:"parent_run_id,omitempty"`
}

type agentResponse struct {
	Output *map[string]any `json:"output,omitempty"`
	Error  *agentError     `json:"error,omitempty"`
	Events []any           `json:"events,omitempty"`
}

type agentError struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type daemon struct {
	cloud      *cloudClient
	endpoints  endpointMap
	httpClient *http.Client
	logger     *log.Logger
}

func newDaemon(cfg config, logger *log.Logger) *daemon {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &daemon{
		cloud:      newCloudClient(cfg),
		endpoints:  cfg.Endpoints,
		httpClient: &http.Client{Timeout: cfg.HTTPTimeout},
		logger:     logger,
	}
}

func (d *daemon) runCycle(ctx context.Context, syncMetadata bool) error {
	hb, err := d.cloud.heartbeat(ctx)
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	d.logger.Printf("heartbeat node=%s status=%s linked=%d pending=%d", hb.NodeID, hb.HeartbeatStatus, hb.LinkedListingCount, hb.PendingRunCount)

	if syncMetadata {
		synced, err := d.cloud.syncMetadata(ctx)
		if err != nil {
			return fmt.Errorf("metadata sync: %w", err)
		}
		d.logger.Printf("metadata-sync node=%s listings=%d", synced.RegistryNodeID, synced.SyncedListingCount)
	}

	for {
		run, ok, err := d.cloud.claim(ctx)
		if err != nil {
			return fmt.Errorf("claim proxy run: %w", err)
		}
		if !ok {
			d.logger.Printf("claim no pending runs")
			return nil
		}
		d.logger.Printf("claimed proxy_run=%s cloud_run=%s local_agent=%s", run.ID, run.CloudRunID, run.LocalAgentID)
		result := d.invokeLocalAgent(ctx, run)
		completed, err := d.cloud.complete(ctx, run.ID, result)
		if err != nil {
			return fmt.Errorf("complete proxy run %s: %w", run.ID, err)
		}
		d.logger.Printf("completed proxy_run=%s status=%s", completed.ID, completed.Status)
	}
}

func (d *daemon) invokeLocalAgent(ctx context.Context, run *proxyRun) completeProxyRunRequest {
	endpoint := d.endpoints[run.LocalAgentID]
	if strings.TrimSpace(endpoint) == "" {
		return failedResult("LOCAL_AGENT_ENDPOINT_NOT_CONFIGURED", "No -agent-endpoint mapping for local_agent_id "+run.LocalAgentID)
	}

	input := run.Input
	if input == nil {
		input = map[string]any{}
	}
	payload := agentRequest{
		Input: input,
		RunID: run.CloudRunID,
		Metadata: map[string]any{
			"platform":         "openlinker",
			"proxy_run_id":     run.ID,
			"cloud_run_id":     run.CloudRunID,
			"cloud_listing_id": run.CloudListingID,
			"registry_node_id": run.RegistryNodeID,
			"local_agent_id":   run.LocalAgentID,
			"payload_policy":   run.PayloadPolicy,
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return failedResult("INVALID_PROXY_RUN_PAYLOAD", err.Error())
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return failedResult("LOCAL_AGENT_REQUEST_ERROR", err.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-OpenLinker-Run-Id", run.CloudRunID)
	req.Header.Set("X-OpenLinker-Proxy-Run-Id", run.ID)
	req.Header.Set("X-OpenLinker-Registry-Node-Id", run.RegistryNodeID)

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return retryableFailedResult("LOCAL_AGENT_UNREACHABLE", err.Error())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if err != nil {
		return retryableFailedResult("LOCAL_AGENT_RESPONSE_READ_ERROR", err.Error())
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode >= 500 {
			return retryableFailedResultFromBody(fmt.Sprintf("LOCAL_AGENT_HTTP_%d", resp.StatusCode), body)
		}
		return failedResultFromBody(fmt.Sprintf("LOCAL_AGENT_HTTP_%d", resp.StatusCode), body)
	}
	var agentResp agentResponse
	if err := json.Unmarshal(body, &agentResp); err != nil {
		return failedResult("INVALID_LOCAL_AGENT_RESPONSE", "local Agent returned non-JSON response: "+truncate(string(body), 300))
	}
	if agentResp.Error != nil {
		code := strings.TrimSpace(agentResp.Error.Code)
		if code == "" {
			code = "LOCAL_AGENT_ERROR"
		}
		return failedResult(code, firstNonEmpty(agentResp.Error.Message, "local Agent returned an error"))
	}
	if agentResp.Output == nil {
		return failedResult("LOCAL_AGENT_OUTPUT_MISSING", "local Agent response must include an output object")
	}
	output := *agentResp.Output
	if output == nil {
		output = map[string]any{}
	}
	return completeProxyRunRequest{
		Status:        "success",
		Output:        output,
		OutputSummary: summarizeOutput(output),
	}
}

func failedResult(code, message string) completeProxyRunRequest {
	code = strings.TrimSpace(code)
	if code == "" {
		code = "LOCAL_AGENT_ERROR"
	}
	message = strings.TrimSpace(message)
	if message == "" {
		message = code
	}
	return completeProxyRunRequest{
		Status:       "failed",
		ErrorCode:    truncate(code, 80),
		ErrorMessage: truncate(message, 1000),
	}
}

func retryableFailedResult(code, message string) completeProxyRunRequest {
	result := failedResult(code, message)
	result.Retryable = true
	return result
}

func failedResultFromBody(code string, body []byte) completeProxyRunRequest {
	var parsed struct {
		Error agentError `json:"error"`
	}
	if json.Unmarshal(body, &parsed) == nil && (parsed.Error.Code != "" || parsed.Error.Message != "") {
		if parsed.Error.Code != "" {
			code = parsed.Error.Code
		}
		return failedResult(code, parsed.Error.Message)
	}
	msg := strings.TrimSpace(string(body))
	if msg == "" {
		msg = code
	}
	return failedResult(code, msg)
}

func retryableFailedResultFromBody(code string, body []byte) completeProxyRunRequest {
	result := failedResultFromBody(code, body)
	result.Retryable = true
	return result
}

func summarizeOutput(output map[string]any) string {
	for _, key := range []string{"summary", "answer", "text", "message", "result"} {
		if value, ok := output[key].(string); ok && strings.TrimSpace(value) != "" {
			return truncate(strings.TrimSpace(value), 1000)
		}
	}
	raw, err := json.Marshal(output)
	if err != nil {
		return ""
	}
	return truncate(string(raw), 1000)
}

func truncate(s string, max int) string {
	if max <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	if max <= 3 {
		return string(runes[:max])
	}
	return string(runes[:max-3]) + "..."
}

func run(ctx context.Context, cfg config, logger *log.Logger) error {
	d := newDaemon(cfg, logger)
	if cfg.Once {
		return d.runCycle(ctx, cfg.SyncMetadata)
	}
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	for {
		if err := d.runCycle(ctx, cfg.SyncMetadata); err != nil {
			logger.Printf("cycle failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func main() {
	env := map[string]string{
		"OPENLINKER_API_ROOT":        os.Getenv("OPENLINKER_API_ROOT"),
		"OPENLINKER_API_URL":         os.Getenv("OPENLINKER_API_URL"),
		"OPENLINKER_NODE_SECRET":     os.Getenv("OPENLINKER_NODE_SECRET"),
		"OPENLINKER_AGENT_ENDPOINTS": os.Getenv("OPENLINKER_AGENT_ENDPOINTS"),
	}
	cfg, err := parseConfig(os.Args[1:], env)
	if err != nil {
		fmt.Fprintf(os.Stderr, "registry-node: %v\n\n", err)
		fmt.Fprintln(os.Stderr, "usage: registry-node -api http://127.0.0.1:8080 -secret rn_live_xxx -agent-endpoint local_agent_id=http://127.0.0.1:18081/run")
		os.Exit(2)
	}
	logger := log.New(os.Stdout, "registry-node ", log.LstdFlags)
	if err := run(context.Background(), cfg, logger); err != nil && !errors.Is(err, context.Canceled) {
		logger.Printf("stopped: %v", err)
		os.Exit(1)
	}
}
