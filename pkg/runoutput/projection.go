package runoutput

import (
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/google/uuid"
)

// 太短的令牌按原值替换会误伤业务文本，这种令牌只靠请求头规则处理。
const minMaskedSecretLength = 8

var echoedOriginInText = regexp.MustCompile(`(?i)["']origin\\*["']\s*:\s*\\*["'][0-9a-f:.,\s]+\\*["']`)

// Projection 决定一次读取要如何过滤。零值不过滤。
type Projection struct {
	active  bool
	secrets []string
}

// ForViewer 只对 Agent 创建者保留原文：创建者本来就掌握自己的端点和令牌，其他人都要过滤。
func ForViewer(viewerID, creatorID uuid.UUID, endpointToken *string) Projection {
	if viewerID != uuid.Nil && viewerID == creatorID {
		return Projection{}
	}
	if endpointToken == nil {
		return ForNonCreator()
	}
	return ForNonCreator(*endpointToken)
}

// ForNonCreator 用于已知查看者不是创建者的读取。tokens 是涉及 Agent 当前的端点令牌。
func ForNonCreator(tokens ...string) Projection {
	projection := Projection{active: true}
	seen := map[string]struct{}{}
	add := func(secret string) {
		if len(secret) < minMaskedSecretLength {
			return
		}
		if _, ok := seen[secret]; ok {
			return
		}
		seen[secret] = struct{}{}
		projection.secrets = append(projection.secrets, secret)
	}
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		add(token)
		// MCP 端点的令牌可能以 Authorization: Bearer xxx 发出，回显时只剩后半段也要命中。
		if len(token) > 7 && strings.EqualFold(token[:7], "bearer ") {
			add(strings.TrimSpace(token[7:]))
		}
	}
	// 先替换长的，避免短令牌是长令牌子串时留下残段。
	sort.Slice(projection.secrets, func(i, j int) bool {
		return len(projection.secrets[i]) > len(projection.secrets[j])
	})
	return projection
}

// Active 表示这次读取需要过滤。
func (p Projection) Active() bool {
	return p.active
}

// Object 返回过滤后的副本和内容是否有变化，不修改入参。
func (p Projection) Object(object map[string]any) (map[string]any, bool) {
	if !p.active || object == nil {
		return object, false
	}
	out, _ := p.maskValue(Redact(object)).(map[string]any)
	return out, !reflect.DeepEqual(out, object)
}

// Text 处理不一定是 JSON 的文本，如错误信息。对象内的字符串走 Object 的规则；
// 这里额外识别夹在说明文字中的回显，命中后整段替换，因为无法安全地只删其中一部分。
func (p Projection) Text(text string) (string, bool) {
	if !p.active || text == "" {
		return text, false
	}
	out := redactEmbeddedJSON(text, 0)
	if out == text {
		out = redactUnparsedJSON(text)
		if out == text && echoedOriginInText.MatchString(decodeUnicodeEscapes(text)) {
			out = redactedText
		}
	}
	out = p.maskText(out)
	return out, out != text
}

// EventPayload 过滤事件负载。事件的 id、序号和类型由调用方保留，断点续传不受影响。
func (p Projection) EventPayload(eventType string, payload map[string]any) (map[string]any, bool) {
	if !p.active {
		return payload, false
	}
	out, changed := p.Object(payload)
	if message, ok := out["error_message"].(string); ok {
		if redacted, textChanged := p.Text(message); textChanged {
			out["error_message"] = redacted
			changed = true
		}
	}
	// 卖家的上游主机不属于输出，不算 output_redacted。
	if eventType == "run.started" {
		delete(out, "endpoint_host")
	}
	return out, changed
}

func (p Projection) maskValue(value any) any {
	if len(p.secrets) == 0 {
		return value
	}
	switch typed := value.(type) {
	case string:
		return p.maskText(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = p.maskValue(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = p.maskValue(item)
		}
		return out
	default:
		return value
	}
}

func (p Projection) maskText(text string) string {
	for _, secret := range p.secrets {
		text = strings.ReplaceAll(text, secret, redactedText)
	}
	return text
}
