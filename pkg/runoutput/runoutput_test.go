package runoutput

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 用例是 openlinker-cloud/pkg/marketplace/testdata/service-order-output-redaction.json 的副本。
func TestRedactMatchesSharedCases(t *testing.T) {
	raw, err := os.ReadFile("testdata/output-redaction.json")
	require.NoError(t, err)
	var cases []struct {
		Name     string         `json:"name"`
		Input    map[string]any `json:"input"`
		Expected map[string]any `json:"expected"`
	}
	require.NoError(t, json.Unmarshal(raw, &cases))
	require.NotEmpty(t, cases)
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			assert.Equal(t, tc.Expected, Redact(tc.Input))
			projected, _ := ForNonCreator().Object(tc.Input)
			assert.Equal(t, tc.Expected, projected)
		})
	}
}

func echoOutput() map[string]any {
	return map[string]any{
		"origin": "203.0.113.7",
		"url":    "https://echo.example/anything",
		"headers": map[string]any{
			"X-Openlinker-Token": []any{"seller-endpoint-token"},
			"X-Forwarded-For":    []any{"203.0.113.7"},
			"Accept":             []any{"application/json"},
		},
		"note": "fetched with seller-endpoint-token",
	}
}

func TestForViewerKeepsOriginalForCreatorOnly(t *testing.T) {
	creator, buyer := uuid.New(), uuid.New()
	token := "seller-endpoint-token"

	own := ForViewer(creator, creator, &token)
	assert.False(t, own.Active())
	output, changed := own.Object(echoOutput())
	assert.False(t, changed)
	assert.Equal(t, echoOutput(), output)

	assert.True(t, ForViewer(buyer, creator, &token).Active())
	assert.True(t, ForViewer(buyer, creator, nil).Active())
	assert.True(t, ForViewer(uuid.Nil, uuid.Nil, nil).Active(), "没有身份时不能当作创建者")
}

func TestObjectMasksTokenAnywhereWithoutMutatingInput(t *testing.T) {
	token := "seller-endpoint-token"
	input := echoOutput()
	output, changed := ForViewer(uuid.New(), uuid.New(), &token).Object(input)
	require.True(t, changed)
	assert.Equal(t, echoOutput(), input)
	assert.Equal(t, "fetched with [redacted]", output["note"])
	assert.Equal(t, map[string]any{"Accept": []any{"application/json"}}, output["headers"])
	assert.NotContains(t, output, "origin")
	assert.Equal(t, "https://echo.example/anything", output["url"])

	clean := map[string]any{"summary": "完成", "cookie": "oatmeal"}
	output, changed = ForNonCreator(token).Object(clean)
	assert.False(t, changed)
	assert.Equal(t, clean, output)
}

func TestForNonCreatorTokenForms(t *testing.T) {
	output, _ := ForNonCreator("Bearer seller-secret-1").Object(map[string]any{"a": "x seller-secret-1 y"})
	assert.Equal(t, "x [redacted] y", output["a"], "回显只带 Bearer 后半段时也要命中")

	output, changed := ForNonCreator("abc").Object(map[string]any{"a": "abcdef"})
	assert.False(t, changed, "过短的令牌不做原值替换")
	assert.Equal(t, "abcdef", output["a"])
}

func TestTextHandlesEchoInsideProse(t *testing.T) {
	projection := ForNonCreator("seller-endpoint-token")
	for name, tc := range map[string]struct {
		input, expected string
	}{
		"headers after prose": {`upstream rejected: {"headers":{"X-Real-Ip":["203.0.113.7"]}}`, "[redacted]"},
		"origin after prose":  {`upstream rejected: {"origin":"203.0.113.7"}`, "[redacted]"},
		"identity in prose":   {`bad seller_user_id=1`, "[redacted]"},
		"token in prose":      {`auth seller-endpoint-token refused`, "auth [redacted] refused"},
		"plain error":         {`upstream timed out`, `upstream timed out`},
		"business origin":     {`invalid {"origin":"Shanghai"}`, `invalid {"origin":"Shanghai"}`},
		"pure json":           {`{"origin":"203.0.113.7","code":1}`, `{"code":1}`},
	} {
		t.Run(name, func(t *testing.T) {
			output, changed := projection.Text(tc.input)
			assert.Equal(t, tc.expected, output)
			assert.Equal(t, tc.expected != tc.input, changed)
		})
	}
	output, changed := Projection{}.Text(`{"origin":"203.0.113.7"}`)
	assert.False(t, changed)
	assert.Equal(t, `{"origin":"203.0.113.7"}`, output)
}

func TestEventPayload(t *testing.T) {
	projection := ForNonCreator("seller-endpoint-token")

	started, changed := projection.EventPayload("run.started", map[string]any{"endpoint_host": "seller.example", "status": "running"})
	assert.False(t, changed, "去掉 endpoint_host 不算输出被过滤")
	assert.Equal(t, map[string]any{"status": "running"}, started)

	failed, changed := projection.EventPayload("run.failed", map[string]any{"error_message": `no: {"origin":"203.0.113.7"}`})
	assert.True(t, changed)
	assert.Equal(t, "[redacted]", failed["error_message"])

	raw := map[string]any{"endpoint_host": "seller.example"}
	kept, changed := Projection{}.EventPayload("run.started", raw)
	assert.False(t, changed)
	assert.Equal(t, raw, kept)
}
