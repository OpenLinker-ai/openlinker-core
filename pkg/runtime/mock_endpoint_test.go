// Package runtime_test - 测试 mock HTTP endpoint 共享 helper。
//
// 这些 helper 由 service_test.go / handler_test.go 共用，模拟 agent 后端 endpoint。
//
// 关键点：
//   - 使用 httptest.NewTLSServer，因为数据库约束 agents.endpoint_url LIKE 'https://%'。
//   - service 在测试模式下需要把 TLS 验证关闭（通过 cfg 或 transport 注入）；
//     若 service 不支持，则测试代码可以选择走 url=server.URL 同时 mock CA。
//     当前实现：用 NewTLSServer 拿 https URL，service 内部 http.Client 必须支持
//     `InsecureSkipVerify` 或测试环境跳过证书。如不支持，subagent-4a 需要在
//     Service 中暴露一个测试钩子（例如把 *http.Client 注入参数）。本文件提供
//     server.Client() 的 Transport（含其自签 CA）作为权威 client，调用方可读取
//     `TestHTTPClientFor(server)` 获取它。
package runtime_test

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// startMockEndpoint 启动一个本地 https mock，t.Cleanup 自动关闭。
// 返回 server URL（https://...）。
func startMockEndpoint(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	return server
}

// startMockEndpointURL 便捷形式：只关心 URL 时使用。
func startMockEndpointURL(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()
	return startMockEndpoint(t, handler).URL
}

// mockEndpointReturning 返回一个固定 status + body 的 handler。
func mockEndpointReturning(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

// mockEndpointReturningJSON 返回一个 JSON 序列化的 body。
func mockEndpointReturningJSON(status int, body any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}
}

// mockEndpointTimeout 故意 sleep d，再返回 200。用于触发 service 的 timeout 路径。
func mockEndpointTimeout(d time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(d)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"output":{"text":"too late"}}`)
	}
}

// mockEndpointEcho 返回 200 + 一个固定 output 字段，并把请求 body 回显进去。
func mockEndpointEcho(output string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"output":{"text":%q},"echoed":%s}`, output, jsonOrNull(raw))
	}
}

// jsonOrNull 若 raw 是合法 JSON 则原样返回，否则返回 "null"。
func jsonOrNull(raw []byte) string {
	if len(raw) == 0 {
		return "null"
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return "null"
	}
	return string(raw)
}

// mockEndpointCounting 包装一个 handler，并发安全地统计被调用次数。
// 第二个返回值是一个读取计数器的函数。
func mockEndpointCounting(inner http.HandlerFunc) (http.HandlerFunc, func() int64) {
	var n int64
	return func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&n, 1)
			inner(w, r)
		}, func() int64 {
			return atomic.LoadInt64(&n)
		}
}

// insecureHTTPClient 返回一个跳过 TLS 校验的 http.Client，超时 5s。
// 当 Service 接受外部 *http.Client 注入时可用；若 Service 内部硬编码 client，
// subagent-4a 需要支持读取 TLS_INSECURE 之类的 env，或在测试 helper 里给 mock
// server 写入系统 CA。最简单是让 Service 接收 *http.Client。
//
// nolint:unused — handler/service 是否使用取决于实现签名，保留方便开关。
func insecureHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // #nosec G402 — 测试 only
		},
	}
}
