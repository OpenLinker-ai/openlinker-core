// Package llm 是 LLM 客户端的 core 侧抽象。
//
// 具体实现(目前是 Anthropic Messages API 客户端)归 cloud:
// openlinker-cloud/internal/llm/anthropic.go。core 自身只定义接口,
// 让 task / skill.Benchmark 等模块在依赖方向上不引入云服务的商业凭据
// (ANTHROPIC_API_KEY)。
//
// 接口由 cloud 实现结构性满足,无需 import 本包。
package llm

import "context"

// Client 单轮 system+user → 文本完成。max_tokens 由实现侧决定。
//
// 调用方在持有 nil interface 时应走规则 fallback(详见 task.llmParse / skill.Benchmark)。
// 注意区分 typed nil:wire 处应用真 nil interface,不要把 (*concrete)(nil) 赋给本接口。
type Client interface {
	Complete(ctx context.Context, system, user string) (string, error)
}
