package log

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
)

func TestParseLevel(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want zerolog.Level
	}{
		{raw: "debug", want: zerolog.DebugLevel},
		{raw: "INFO", want: zerolog.InfoLevel},
		{raw: "warn", want: zerolog.WarnLevel},
		{raw: "warning", want: zerolog.WarnLevel},
		{raw: "error", want: zerolog.ErrorLevel},
		{raw: "unknown", want: zerolog.InfoLevel},
		{raw: "", want: zerolog.InfoLevel},
	} {
		if got := parseLevel(tc.raw); got != tc.want {
			t.Fatalf("parseLevel(%q) = %s, want %s", tc.raw, got, tc.want)
		}
	}
}

func TestInitConfiguresGlobalLogger(t *testing.T) {
	originalLogger := zerologlog.Logger
	originalLevel := zerolog.GlobalLevel()
	originalTimeFormat := zerolog.TimeFieldFormat
	t.Cleanup(func() {
		zerologlog.Logger = originalLogger
		zerolog.SetGlobalLevel(originalLevel)
		zerolog.TimeFieldFormat = originalTimeFormat
	})

	Init("debug", true)
	if got := zerolog.GlobalLevel(); got != zerolog.DebugLevel {
		t.Fatalf("production global level = %s", got)
	}
	if zerolog.TimeFieldFormat != time.RFC3339Nano {
		t.Fatalf("unexpected time field format = %q", zerolog.TimeFieldFormat)
	}

	Init("warning", false)
	if got := zerolog.GlobalLevel(); got != zerolog.WarnLevel {
		t.Fatalf("development global level = %s", got)
	}
}
