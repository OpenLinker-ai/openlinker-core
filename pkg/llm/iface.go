// Package llm contains Core's LLM abstraction. Provider-specific model clients
// and API keys belong outside Core; Core can either receive an injected Client
// or call a generic internal completion endpoint through RemoteClient.
package llm

import "context"

// Client 单轮 system+user → 文本完成。max_tokens 由实现侧决定。
//
// 调用方在持有 nil interface 时应走规则 fallback(详见 task.llmParse / skill.Benchmark)。
// 注意区分 typed nil:wire 处应用真 nil interface,不要把 (*concrete)(nil) 赋给本接口。
type Client interface {
	Complete(ctx context.Context, system, user string) (string, error)
}
