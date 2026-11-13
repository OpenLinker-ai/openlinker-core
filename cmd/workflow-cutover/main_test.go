package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunRejectsUsageErrorsBeforeConnecting(t *testing.T) {
	env := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want int
		out  string
	}{
		{name: "no command", args: nil, want: exitUsage, out: "usage"},
		{name: "unknown command", args: []string{"undrain"}, want: exitUsage, out: "usage"},
		{name: "drain needs a database", args: []string{"drain"}, want: exitUsage, out: "DATABASE_URL is required"},
		{name: "extra argument", args: []string{"verify", "now"}, env: map[string]string{"DATABASE_URL": "postgres://db"}, want: exitUsage},
		{name: "missing database", args: []string{"preflight"}, want: exitUsage, out: "DATABASE_URL is required"},
		{name: "help", args: []string{"help"}, want: exitOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			got := run(context.Background(), tc.args, &stdout, &stderr, env(tc.env))
			if got != tc.want {
				t.Fatalf("exit = %d, want %d (stderr %q)", got, tc.want, stderr.String())
			}
			if tc.out != "" && !strings.Contains(stderr.String()+stdout.String(), tc.out) {
				t.Fatalf("output %q does not mention %q", stderr.String()+stdout.String(), tc.out)
			}
		})
	}
}
