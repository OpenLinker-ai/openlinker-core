package skill

import (
	"context"
	"testing"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

type benchmarkStatusFakeRunner struct{}

func (benchmarkStatusFakeRunner) DryRun(context.Context, *db.Agent, map[string]interface{}) (map[string]interface{}, string) {
	return map[string]interface{}{"ok": true}, ""
}

type benchmarkStatusFakeLLM struct{}

func (benchmarkStatusFakeLLM) Complete(context.Context, string, string) (string, error) {
	return "{}", nil
}

func TestBenchmarkServiceRuntimeStatus(t *testing.T) {
	t.Run("missing dependencies", func(t *testing.T) {
		status := (&BenchmarkService{}).RuntimeStatus()
		if status.CanRun {
			t.Fatalf("expected benchmark runtime to be unavailable")
		}
		if len(status.Reasons) != 2 {
			t.Fatalf("expected 2 readiness reasons, got %d: %#v", len(status.Reasons), status.Reasons)
		}
	})

	t.Run("ready", func(t *testing.T) {
		status := (&BenchmarkService{
			runner: benchmarkStatusFakeRunner{},
			llm:    benchmarkStatusFakeLLM{},
		}).RuntimeStatus()
		if !status.CanRun {
			t.Fatalf("expected benchmark runtime to be available: %#v", status)
		}
		if len(status.Reasons) != 0 {
			t.Fatalf("expected no readiness reasons, got %#v", status.Reasons)
		}
	})
}
