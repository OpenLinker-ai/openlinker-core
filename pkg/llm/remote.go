package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const defaultRemoteTimeout = 15 * time.Second

// RemoteClient calls an internal LLM completion service. Core intentionally
// keeps provider-specific clients, keys, and model selection outside this repo.
type RemoteClient struct {
	endpoint      string
	internalToken string
	http          *http.Client
}

type remoteCompleteRequest struct {
	System string `json:"system,omitempty"`
	User   string `json:"user"`
}

type remoteCompleteResponse struct {
	Text string `json:"text"`
}

// NewRemoteClient returns nil when endpoint is empty so callers can keep the
// rule-based fallback path.
func NewRemoteClient(endpoint, internalToken string) *RemoteClient {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil
	}
	return &RemoteClient{
		endpoint:      endpoint,
		internalToken: strings.TrimSpace(internalToken),
		http:          &http.Client{Timeout: defaultRemoteTimeout},
	}
}

func (c *RemoteClient) Complete(ctx context.Context, system, user string) (string, error) {
	if c == nil || strings.TrimSpace(c.endpoint) == "" {
		return "", errors.New("llm: remote endpoint is empty")
	}
	body, err := json.Marshal(remoteCompleteRequest{System: system, User: user})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("content-type", "application/json")
	if c.internalToken != "" {
		req.Header.Set("X-OpenLinker-Internal-Token", c.internalToken)
	}

	client := c.http
	if client == nil {
		client = &http.Client{Timeout: defaultRemoteTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("llm: remote status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var parsed remoteCompleteResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return "", fmt.Errorf("llm: decode remote response: %w", err)
	}
	if strings.TrimSpace(parsed.Text) == "" {
		return "", errors.New("llm: empty remote response")
	}
	return parsed.Text, nil
}
