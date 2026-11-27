package main

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestParseRegistryMigrateArgs(t *testing.T) {
	subject := uuid.New()
	version := uuid.New()
	for _, test := range []struct {
		name string
		args []string
		ok   bool
		want registryMigrateCommand
	}{
		{"no command", nil, false, registryMigrateCommand{}},
		{"unknown command", []string{"down"}, false, registryMigrateCommand{}},
		{"inventory", []string{"inventory"}, true, registryMigrateCommand{Name: "inventory", Timeout: 10 * time.Minute}},
		{"status", []string{"status"}, true, registryMigrateCommand{Name: "status", Timeout: 10 * time.Minute}},
		{"rollback timeout", []string{"rollback", "--timeout", "30s"}, true, registryMigrateCommand{Name: "rollback", Timeout: 30 * time.Second}},
		{"extra argument", []string{"cutover", "now"}, false, registryMigrateCommand{}},
		{"exclude add", []string{"exclude", "--add", "--kind", "skill", "--subject", subject.String(), "--version", version.String(), "--reason", "bad label"}, true,
			registryMigrateCommand{Name: "exclude", Add: true, Kind: "skill", Subject: subject, Version: &version, Reason: "bad label", Timeout: 10 * time.Minute}},
		{"exclude remove", []string{"exclude", "--remove", "--kind", "mcp", "--subject", subject.String()}, true,
			registryMigrateCommand{Name: "exclude", Remove: true, Kind: "mcp", Subject: subject, Timeout: 10 * time.Minute}},
		{"exclude needs a direction", []string{"exclude", "--kind", "mcp", "--subject", subject.String()}, false, registryMigrateCommand{}},
		{"exclude both directions", []string{"exclude", "--add", "--remove", "--kind", "mcp", "--subject", subject.String(), "--reason", "x"}, false, registryMigrateCommand{}},
		{"exclude add needs a reason", []string{"exclude", "--add", "--kind", "mcp", "--subject", subject.String()}, false, registryMigrateCommand{}},
		{"exclude bad kind", []string{"exclude", "--add", "--kind", "agent", "--subject", subject.String(), "--reason", "x"}, false, registryMigrateCommand{}},
		{"exclude bad subject", []string{"exclude", "--add", "--kind", "skill", "--subject", "nope", "--reason", "x"}, false, registryMigrateCommand{}},
		{"mcp version", []string{"exclude", "--add", "--kind", "mcp", "--subject", subject.String(), "--version", version.String(), "--reason", "x"}, false, registryMigrateCommand{}},
		{"timeout elsewhere", []string{"exclude", "--add", "--kind", "skill", "--subject", subject.String(), "--reason", "x", "--timeout", "1s"}, false, registryMigrateCommand{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseRegistryMigrateArgs(test.args)
			if test.ok != (err == nil) {
				t.Fatalf("parse error = %v, want ok=%v", err, test.ok)
			}
			if !test.ok {
				return
			}
			if got.Name != test.want.Name || got.Add != test.want.Add || got.Remove != test.want.Remove || got.Kind != test.want.Kind ||
				got.Subject != test.want.Subject || got.Reason != test.want.Reason || got.Timeout != test.want.Timeout ||
				(got.Version == nil) != (test.want.Version == nil) || (got.Version != nil && *got.Version != *test.want.Version) {
				t.Fatalf("parse = %+v, want %+v", got, test.want)
			}
		})
	}
}
