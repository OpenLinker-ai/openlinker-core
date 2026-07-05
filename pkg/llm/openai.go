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

const defaultOpenAITimeout = 30 * time.Second

// OpenAIClient 调用任意 OpenAI 兼容接口（openai.com / Azure / 本地代理均可）。
//
// 自托管用户可通过 LLM_OPENAI_URL + LLM_OPENAI_API_KEY + LLM_OPENAI_MODEL
// 直接配置，无需私有代理服务。
type OpenAIClient struct {
	baseURL string
	apiKey  string
	model   string
	http    *http.Client
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIRequest struct {
	Model       string          `json:"model"`
	Messages    []openAIMessage `json:"messages"`
	MaxTokens   int             `json:"max_tokens,omitempty"`
	Temperature float64         `json:"temperature,omitempty"`
}

type openAIChoice struct {
	Message openAIMessage `json:"message"`
}

type openAIResponse struct {
	Choices []openAIChoice `json:"choices"`
	Error   *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// NewOpenAIClient 返回 OpenAI 兼容 client。baseURL 为空时返回 nil（调用方走规则 fallback）。
//
// baseURL 示例：
//   - https://api.openai.com/v1
//   - https://your-azure-endpoint.openai.azure.com/openai/deployments/gpt-4o-mini
//   - http://localhost:11434/v1  （Ollama）
func NewOpenAIClient(baseURL, apiKey, model string) *OpenAIClient {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return nil
	}
	if model == "" {
		model = "gpt-4o-mini"
	}
	return &OpenAIClient{
		baseURL: baseURL,
		apiKey:  strings.TrimSpace(apiKey),
		model:   model,
		http:    &http.Client{Timeout: defaultOpenAITimeout},
	}
}

func (c *OpenAIClient) Complete(ctx context.Context, system, user string) (string, error) {
	if c == nil {
		return "", errors.New("llm: openai client is nil")
	}
	messages := make([]openAIMessage, 0, 2)
	if strings.TrimSpace(system) != "" {
		messages = append(messages, openAIMessage{Role: "system", Content: system})
	}
	messages = append(messages, openAIMessage{Role: "user", Content: user})

	body, err := json.Marshal(openAIRequest{
		Model:     c.model,
		Messages:  messages,
		MaxTokens: 512,
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("llm: openai request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}

	var parsed openAIResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return "", fmt.Errorf("llm: openai decode: %w (status=%d)", err, resp.StatusCode)
	}
	if parsed.Error != nil {
		return "", fmt.Errorf("llm: openai api error: %s", parsed.Error.Message)
	}
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("llm: openai status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if len(parsed.Choices) == 0 || strings.TrimSpace(parsed.Choices[0].Message.Content) == "" {
		return "", errors.New("llm: openai returned empty content")
	}
	return parsed.Choices[0].Message.Content, nil
}
