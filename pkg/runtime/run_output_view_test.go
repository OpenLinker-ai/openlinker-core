package runtime

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OpenLinker-ai/openlinker-core/pkg/runoutput"
)

func TestProjectDelegatedRun(t *testing.T) {
	filter := runoutput.ForNonCreator("child-endpoint-token")

	t.Run("filtered output keeps number precision", func(t *testing.T) {
		view := DelegatedRunView{Output: json.RawMessage(`{"amount":12345678901234567890,"origin":"203.0.113.7"}`)}
		redacted, err := projectDelegatedRun(&view, filter)
		require.NoError(t, err)
		assert.True(t, redacted)
		assert.Equal(t, `{"amount":12345678901234567890}`, string(view.Output))
	})
	t.Run("unchanged output keeps its bytes", func(t *testing.T) {
		raw := `{ "total": 1.50, "origin": "shanghai" }`
		view := DelegatedRunView{Output: json.RawMessage(raw), ErrorMessage: "ordinary failure"}
		redacted, err := projectDelegatedRun(&view, filter)
		require.NoError(t, err)
		assert.False(t, redacted)
		assert.Equal(t, raw, string(view.Output))
		assert.Equal(t, "ordinary failure", view.ErrorMessage)
	})
	t.Run("no output", func(t *testing.T) {
		view := DelegatedRunView{ErrorMessage: "rejected child-endpoint-token"}
		redacted, err := projectDelegatedRun(&view, filter)
		require.NoError(t, err)
		assert.True(t, redacted)
		assert.Empty(t, view.Output)
		assert.Equal(t, "rejected [redacted]", view.ErrorMessage)
	})
	t.Run("output that is not an object is refused rather than passed on", func(t *testing.T) {
		view := DelegatedRunView{Output: json.RawMessage(`["child-endpoint-token"]`)}
		_, err := projectDelegatedRun(&view, filter)
		require.Error(t, err)
	})
	t.Run("creator projection leaves the view alone", func(t *testing.T) {
		raw := `{"origin":"203.0.113.7"}`
		view := DelegatedRunView{Output: json.RawMessage(raw)}
		redacted, err := projectDelegatedRun(&view, runoutput.Projection{})
		require.NoError(t, err)
		assert.False(t, redacted)
		assert.Equal(t, raw, string(view.Output))
	})
}

func TestDelegatedRunProjection(t *testing.T) {
	creator, other := uuid.New(), uuid.New()
	assert.False(t, delegatedRunProjection(&creator, &creator, nil).Active())
	assert.True(t, delegatedRunProjection(&other, &creator, nil).Active())
	assert.True(t, delegatedRunProjection(nil, &creator, nil).Active(), "caller Agent is gone")
	assert.True(t, delegatedRunProjection(&creator, nil, nil).Active(), "child Agent is gone")
	assert.True(t, delegatedRunProjection(nil, nil, nil).Active(), "both Agents are gone")
}
