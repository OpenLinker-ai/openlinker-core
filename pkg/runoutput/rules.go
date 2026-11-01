// Package runoutput 定义 Agent 输出对外展示时的过滤规则。
//
// Run 的库内原文是执行事实源，不在这里改写；调用方在读取时按查看者投影。
package runoutput

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"
)

// echo 型 Agent 会回显它收到的请求：平台注入的他方身份、内部服务标识，以及传输层来源 IP 与凭据头。
// 规则与 Cloud 的服务订单快照和 Hosted Web 的展示过滤一致，
// testdata/output-redaction.json 是同一份用例的副本，改规则时要同时改三处。
var (
	internalIdentityKeys = []string{
		"seller_user_id",
		"caller_service_id",
		"principal_scope_id",
		"_openlinker_runtime_authority",
		"x-openlinker-user-id",
	}
	// 这些名字也可能是业务字段或工作流节点 key，所以只在 headers 容器内剔除。
	headerContainerKeys = []string{"headers", "request_headers"}
	transportHeaderKeys = []string{
		"x-forwarded-for",
		"x-real-ip",
		"forwarded",
		"cf-connecting-ip",
		"true-client-ip",
		"x-envoy-external-address",
		"authorization",
		"proxy-authorization",
		"cookie",
		"set-cookie",
		"x-api-key",
		// Core 把卖家登记的 endpoint_auth_header 放在这个头里发给上游。
		"x-openlinker-token",
	}
)

const (
	maxIPListLength = 512
	// 字符串里还能再嵌字符串 JSON；超过这个层数不再解析，按无法安全处理对待。
	maxEmbeddedJSONDepth = 4
	// 单个 JSON 文本的结构嵌套上限，超过时同样按无法安全处理对待。
	maxEmbeddedJSONNesting = 1000
	redactedText           = "[redacted]"
)

var (
	ipv4Pattern          = regexp.MustCompile(`^\d{1,3}(?:\.\d{1,3}){3}$`)
	ipv6GroupPattern     = regexp.MustCompile(`^[0-9a-fA-F]{1,4}$`)
	identityKeyPattern   = regexp.MustCompile(`(?i)` + strings.Join(internalIdentityKeys, "|"))
	headerContainerInRaw = jsonKeyPattern(headerContainerKeys)
	transportKeyInRaw    = jsonKeyPattern(transportHeaderKeys)
	backslashRunPattern  = regexp.MustCompile(`\\+`)
	unicodeEscapePattern = regexp.MustCompile(`\\u([0-9a-fA-F]{4})`)
	errMalformedEmbedded = errors.New("malformed embedded JSON")
)

// 传输层字段名也是普通英文词或业务字段，只有处在 JSON 键的位置才算命中。
func jsonKeyPattern(keys []string) *regexp.Regexp {
	return regexp.MustCompile(`(?i)["'](?:` + strings.Join(keys, "|") + `)\\*["']\s*:`)
}

// Redact 返回剔除后的副本，不修改入参；nil 保持 nil。
func Redact(output map[string]any) map[string]any {
	if output == nil {
		return nil
	}
	return redactOutputObject(output)
}

func redactOutputObject(object map[string]any) map[string]any {
	out := make(map[string]any, len(object))
	for key, value := range object {
		if isRedactedMember(key, value) {
			continue
		}
		if headers, ok := value.(map[string]any); ok && isHeaderContainer(key) {
			kept := make(map[string]any, len(headers))
			for name, header := range headers {
				if !isTransportHeader(name) {
					kept[name] = header
				}
			}
			value = kept
		}
		out[key] = redactOutputValue(value)
	}
	return out
}

func redactOutputValue(value any) any {
	switch typed := value.(type) {
	case string:
		return redactEmbeddedJSON(typed, 0)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = redactOutputValue(item)
		}
		return out
	case map[string]any:
		return redactOutputObject(typed)
	default:
		return value
	}
}

func isHeaderContainer(key string) bool {
	return slices.Contains(headerContainerKeys, strings.ToLower(key))
}

func isTransportHeader(name string) bool {
	return slices.Contains(transportHeaderKeys, strings.ToLower(name))
}

// httpbin 系的 origin 是请求来源 IP；业务输出也可能有 origin（如出发地），只在值确为 IP 时剔除。
func isRedactedMember(key string, value any) bool {
	name := strings.ToLower(key)
	if slices.Contains(internalIdentityKeys, name) {
		return true
	}
	text, ok := value.(string)
	return ok && name == "origin" && isIPList(text)
}

func isIPList(value string) bool {
	if len(value) > maxIPListLength {
		return false
	}
	for _, part := range strings.Split(value, ",") {
		address := strings.TrimFunc(part, isTextSpace)
		if !isIPv4(address) && !isIPv6(address) {
			return false
		}
	}
	return true
}

func isIPv4(address string) bool {
	if !ipv4Pattern.MatchString(address) {
		return false
	}
	for _, octet := range strings.Split(address, ".") {
		if parsed, err := strconv.Atoi(octet); err != nil || parsed > 255 {
			return false
		}
	}
	return true
}

func isIPv6(address string) bool {
	halves := strings.Split(address, "::")
	if len(halves) > 2 {
		return false
	}
	var groups []string
	for _, half := range halves {
		if half != "" {
			groups = append(groups, strings.Split(half, ":")...)
		}
	}
	tailIPv4 := len(groups) > 0 && isIPv4(groups[len(groups)-1])
	hexGroups := groups
	size := len(groups)
	if tailIPv4 {
		hexGroups = groups[:len(groups)-1]
		size++
	}
	for _, group := range hexGroups {
		if !ipv6GroupPattern.MatchString(group) {
			return false
		}
	}
	if len(halves) == 2 {
		return size < 8
	}
	return size == 8
}

func isTextSpace(r rune) bool {
	return unicode.IsSpace(r) || r == 0xFEFF
}

// echo 型 Agent（如 httpbin 的 data 字段）常把请求体作为 JSON 字符串原样返回，这里解析后同样剔除。
//   - 未命中时返回原字符串，格式不变；
//   - 命中时只删除对应成员，其余成员保持顺序，数字保持源文本；
//   - 无法安全处理（解析失败、嵌套过深）时，只要文本里出现相应字段名就整体隐藏。
func redactEmbeddedJSON(text string, depth int) string {
	trimmed := strings.TrimLeftFunc(text, isTextSpace)
	if trimmed == "" || (trimmed[0] != '[' && trimmed[0] != '{') {
		return text
	}
	if depth >= maxEmbeddedJSONDepth {
		return redactUnparsedJSON(text)
	}
	parsed, err := parseEmbeddedJSON(text)
	if err != nil {
		return redactUnparsedJSON(text)
	}
	redacted, changed := redactEmbeddedValue(parsed, depth)
	if !changed {
		return text
	}
	var buffer bytes.Buffer
	writeEmbeddedValue(&buffer, redacted)
	return buffer.String()
}

func redactUnparsedJSON(text string) string {
	decoded := decodeUnicodeEscapes(text)
	if identityKeyPattern.MatchString(decoded) ||
		(headerContainerInRaw.MatchString(decoded) && transportKeyInRaw.MatchString(decoded)) {
		return redactedText
	}
	return text
}

// 结果只用于识别，不用于展示：键名可能写成 principal_scope_id，
// 嵌套 JSON 字符串里还会叠加多层反斜杠，所以先折叠反斜杠再展开 \uXXXX。
func decodeUnicodeEscapes(text string) string {
	current := backslashRunPattern.ReplaceAllString(text, `\`)
	for pass := 0; pass < maxEmbeddedJSONDepth+1; pass++ {
		next := unicodeEscapePattern.ReplaceAllStringFunc(current, func(escape string) string {
			code, err := strconv.ParseUint(escape[2:], 16, 32)
			if err != nil {
				return escape
			}
			return string(rune(code))
		})
		next = backslashRunPattern.ReplaceAllString(next, `\`)
		if next == current {
			break
		}
		current = next
	}
	return current
}

// embeddedObject 保留成员顺序和重复键，改写后的文本除被删成员外与原文语义一致。
type embeddedObject []embeddedMember

type embeddedMember struct {
	key   string
	value any
}

func parseEmbeddedJSON(text string) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	value, err := parseEmbeddedValue(decoder, 0)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errMalformedEmbedded
	}
	return value, nil
}

func parseEmbeddedValue(decoder *json.Decoder, nesting int) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		return token, nil
	}
	if nesting >= maxEmbeddedJSONNesting {
		return nil, errMalformedEmbedded
	}
	if delim == '[' {
		items := []any{}
		for decoder.More() {
			item, err := parseEmbeddedValue(decoder, nesting+1)
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		_, err := decoder.Token()
		return items, err
	}
	object := embeddedObject{}
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, errMalformedEmbedded
		}
		value, err := parseEmbeddedValue(decoder, nesting+1)
		if err != nil {
			return nil, err
		}
		object = append(object, embeddedMember{key: key, value: value})
	}
	_, err = decoder.Token()
	return object, err
}

func redactEmbeddedValue(value any, depth int) (any, bool) {
	switch typed := value.(type) {
	case string:
		redacted := redactEmbeddedJSON(typed, depth+1)
		return redacted, redacted != typed
	case []any:
		changed := false
		out := make([]any, len(typed))
		for i, item := range typed {
			redacted, itemChanged := redactEmbeddedValue(item, depth)
			out[i] = redacted
			changed = changed || itemChanged
		}
		return out, changed
	case embeddedObject:
		changed := false
		out := make(embeddedObject, 0, len(typed))
		for _, member := range typed {
			if isRedactedMember(member.key, member.value) {
				changed = true
				continue
			}
			memberValue := member.value
			if headers, ok := memberValue.(embeddedObject); ok && isHeaderContainer(member.key) {
				kept := make(embeddedObject, 0, len(headers))
				for _, header := range headers {
					if isTransportHeader(header.key) {
						changed = true
						continue
					}
					kept = append(kept, header)
				}
				memberValue = kept
			}
			redacted, memberChanged := redactEmbeddedValue(memberValue, depth)
			changed = changed || memberChanged
			out = append(out, embeddedMember{key: member.key, value: redacted})
		}
		return out, changed
	default:
		return value, false
	}
}

func writeEmbeddedValue(buffer *bytes.Buffer, value any) {
	switch typed := value.(type) {
	case embeddedObject:
		buffer.WriteByte('{')
		for i, member := range typed {
			if i > 0 {
				buffer.WriteByte(',')
			}
			writeEmbeddedString(buffer, member.key)
			buffer.WriteByte(':')
			writeEmbeddedValue(buffer, member.value)
		}
		buffer.WriteByte('}')
	case []any:
		buffer.WriteByte('[')
		for i, item := range typed {
			if i > 0 {
				buffer.WriteByte(',')
			}
			writeEmbeddedValue(buffer, item)
		}
		buffer.WriteByte(']')
	case string:
		writeEmbeddedString(buffer, typed)
	case json.Number:
		buffer.WriteString(typed.String())
	case bool:
		buffer.WriteString(strconv.FormatBool(typed))
	default:
		buffer.WriteString("null")
	}
}

func writeEmbeddedString(buffer *bytes.Buffer, text string) {
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	// 字符串编码不会失败；Encode 末尾带换行，写完去掉。
	_ = encoder.Encode(text)
	buffer.Truncate(buffer.Len() - 1)
}
