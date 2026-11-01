package runoutput

import (
	"encoding/json"
	"strings"

	"github.com/google/uuid"
)

const maxCompletionSummaryLength = 2000

// CompletionSummary 从 Run 输出生成任务的完成摘要，长度超限时按 rune 截断。
func CompletionSummary(output map[string]any) string {
	for _, key := range []string{"summary", "text", "message", "answer"} {
		if value, ok := output[key].(string); ok {
			if summary := compactCompletionSummary(value); summary != "" {
				return summary
			}
		}
	}
	if encoded, err := json.Marshal(output); err == nil {
		if summary := compactCompletionSummary(string(encoded)); summary != "" && summary != "{}" {
			return summary
		}
	}
	return "运行成功"
}

func compactCompletionSummary(value string) string {
	value = strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
	runes := []rune(value)
	if len(runes) > maxCompletionSummaryLength {
		value = string(runes[:maxCompletionSummaryLength])
	}
	return value
}

// CompletionSource 是已存摘要对应的 Run 和 Agent。
type CompletionSource struct {
	Output        []byte
	CreatorID     *uuid.UUID
	EndpointToken *string
}

// ProjectCompletionSummary 返回 viewerID 能读到的摘要和内容是否有变化。source 为 nil 表示 Run 已不存在。
//
// 已存摘要可能截断了令牌或 IP，规则无法命中残段，所以优先从 Run 原始输出过滤后重算。
func ProjectCompletionSummary(viewerID uuid.UUID, stored string, source *CompletionSource) (string, bool) {
	if source == nil {
		return ForNonCreator().Text(stored)
	}
	var creatorID uuid.UUID
	if source.CreatorID != nil {
		creatorID = *source.CreatorID
	}
	projection := ForViewer(viewerID, creatorID, source.EndpointToken)
	if !projection.Active() {
		return stored, false
	}
	var output map[string]any
	if json.Unmarshal(source.Output, &output) == nil && output != nil {
		if projected, changed := projection.Object(output); changed {
			return CompletionSummary(projected), true
		}
	}
	return projection.Text(stored)
}
