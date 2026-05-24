package task

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/llm"
)

// llmParse 调用 Anthropic 把用户描述映射到 1-3 个 skill_id。
//
// 调用方应在 client == nil 时改走 ruleParse；本函数不再判空。
// 任何错误（网络 / 解析 / 全部 ID 非法）→ 返回 nil + err，调用方据此 fallback。
func llmParse(ctx context.Context, client llm.Client, query string, skills []db.Skill) ([]string, error) {
	system := buildLLMSystem(skills)
	user := "用户任务：" + query

	raw, err := client.Complete(ctx, system, user)
	if err != nil {
		return nil, fmt.Errorf("llm.Complete: %w", err)
	}

	ids, err := parseLLMResp(raw)
	if err != nil {
		return nil, err
	}

	// 校验：丢弃不在候选 catalog 里的 id（防 LLM 幻觉）
	valid := make(map[string]struct{}, len(skills))
	for i := range skills {
		valid[skills[i].ID] = struct{}{}
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := valid[id]; ok {
			out = append(out, id)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("llm: no valid skill returned")
	}
	if len(out) > 3 {
		out = out[:3]
	}
	return out, nil
}

// buildLLMSystem 拼 system prompt：要求严格 JSON 输出 + 列出全部候选 skill。
func buildLLMSystem(skills []db.Skill) string {
	var b strings.Builder
	b.WriteString("你是 OpenLinker 的任务路由器。用户描述一个任务，你输出最匹配的 1-3 个 skill_id（来自下方候选）。\n")
	b.WriteString("严格只输出 JSON：{\"skills\": [\"id1\",\"id2\"]}，不要任何解释。\n\n")
	b.WriteString("候选 skills:\n")
	for i := range skills {
		s := &skills[i]
		b.WriteString("- ")
		b.WriteString(s.ID)
		b.WriteString(": ")
		b.WriteString(s.Name)
		b.WriteString(" — ")
		b.WriteString(s.Description)
		b.WriteString("\n")
	}
	return b.String()
}

// llmRespBody 期望的 JSON 响应结构。
type llmRespBody struct {
	Skills []string `json:"skills"`
}

// parseLLMResp 兼容 LLM 偶尔在 JSON 前后包 ```json ... ``` 或额外文字的情况。
func parseLLMResp(raw string) ([]string, error) {
	s := strings.TrimSpace(raw)
	// 去掉可能的 markdown code fence
	if strings.HasPrefix(s, "```") {
		s = strings.TrimPrefix(s, "```json")
		s = strings.TrimPrefix(s, "```")
		if i := strings.LastIndex(s, "```"); i >= 0 {
			s = s[:i]
		}
		s = strings.TrimSpace(s)
	}
	// 截取第一个 { ... 最后一个 }
	start := strings.Index(s, "{")
	end := strings.LastIndex(s, "}")
	if start < 0 || end <= start {
		return nil, fmt.Errorf("llm: not json: %q", raw)
	}
	s = s[start : end+1]

	var body llmRespBody
	if err := json.Unmarshal([]byte(s), &body); err != nil {
		return nil, fmt.Errorf("llm: unmarshal: %w (raw=%q)", err, raw)
	}
	return body.Skills, nil
}
