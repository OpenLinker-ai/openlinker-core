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

const (
	defaultAnthropicModel    = "claude-haiku-4-5-20251001"
	defaultAnthropicEndpoint = "https://api.anthropic.com/v1/messages"
	anthropicAPIVersion      = "2023-06-01"
	defaultAnthropicTimeout  = 15 * time.Second
)

// AnthropicClient implements Client with the Anthropic Messages API.
type AnthropicClient struct {
	apiKey   string
	endpoint string
	model    string
	http     *http.Client
}

// NewAnthropicClient returns nil when apiKey is empty so callers can keep the
// existing rule-based fallback path.
func NewAnthropicClient(apiKey string) *AnthropicClient {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return nil
	}
	return newAnthropicClient(apiKey, defaultAnthropicEndpoint, defaultAnthropicModel, nil)
}

func newAnthropicClient(apiKey, endpoint, model string, httpClient *http.Client) *AnthropicClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultAnthropicTimeout}
	}
	return &AnthropicClient{
		apiKey:   apiKey,
		endpoint: endpoint,
		model:    model,
		http:     httpClient,
	}
}

type anthropicMessageRequest struct {
	Model     string             `json:"model"`
	MaxTokens int                `json:"max_tokens"`
	System    string             `json:"system,omitempty"`
	Messages  []anthropicMessage `json:"messages"`
}

type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type anthropicMessageResponse struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	Error *struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func (c *AnthropicClient) Complete(ctx context.Context, system, user string) (string, error) {
	if c == nil || strings.TrimSpace(c.apiKey) == "" {
		return "", errors.New("llm: anthropic api key is empty")
	}
	body, err := json.Marshal(anthropicMessageRequest{
		Model:     c.model,
		MaxTokens: 512,
		System:    system,
		Messages:  []anthropicMessage{{Role: "user", Content: user}},
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-api-key", c.apiKey)
	req.Header.Set("anthropic-version", anthropicAPIVersion)

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var parsed anthropicMessageResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return "", fmt.Errorf("llm: decode response: %w", err)
	}
	if parsed.Error != nil {
		return "", fmt.Errorf("llm: %s: %s", parsed.Error.Type, parsed.Error.Message)
	}
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("llm: status %d", resp.StatusCode)
	}
	if len(parsed.Content) == 0 {
		return "", errors.New("llm: empty content")
	}
	return parsed.Content[0].Text, nil
}
