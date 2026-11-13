// Command workflow-cutover checks and finishes a workflow release drain.
//
//	mode         print whether upgrading this database needs a workflow drain
//	drain        switch admission to draining; ready once every run finished
//	abort-drain  reopen admission for a release abandoned before migrating
//	preflight  read-only, before the workflow attempt migration
//	verify     read-only, after the migration while admission still drains
//	reopen     verifies, then reopens workflow admission
//
// It prints one JSON report. Exit codes: 0 ready, 1 failure, 2 usage,
// 3 blocked (the report lists the blockers).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/OpenLinker-ai/openlinker-core/pkg/workflow"
)

const (
	exitOK      = 0
	exitFailure = 1
	exitUsage   = 2
	exitBlocked = 3
)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr, os.Getenv))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	if len(args) == 0 {
		usage(stderr)
		return exitUsage
	}
	command := args[0]
	if command == "help" || command == "-h" || command == "--help" {
		usage(stdout)
		return exitOK
	}
	switch command {
	case "mode", "drain", "abort-drain", "preflight", "verify", "reopen":
	default:
		usage(stderr)
		return exitUsage
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	timeout := flags.Duration("timeout", 30*time.Second, "command timeout")
	reason := flags.String("reason", "", "reason recorded on workflow admission (drain, abort-drain, reopen)")
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
		return exitUsage
	}
	databaseURL := getenv("DATABASE_URL")
	if databaseURL == "" {
		fmt.Fprintln(stderr, "workflow-cutover: DATABASE_URL is required")
		return exitUsage
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		fmt.Fprintf(stderr, "workflow-cutover: connect: %v\n", err)
		return exitFailure
	}
	defer pool.Close()

	if command == "mode" {
		mode, modeErr := workflow.DecideWorkflowCutoverMode(ctx, pool)
		if modeErr != nil && !errors.Is(modeErr, workflow.ErrWorkflowCutoverUnsupportedSource) {
			fmt.Fprintf(stderr, "workflow-cutover mode: %v\n", modeErr)
			return exitFailure
		}
		_ = json.NewEncoder(stdout).Encode(mode)
		if modeErr != nil {
			fmt.Fprintf(stderr, "workflow-cutover mode: %v\n", modeErr)
			return exitFailure
		}
		return exitOK
	}
	var report workflow.CutoverReport
	switch command {
	case "drain":
		report, err = workflow.DrainWorkflowAdmission(ctx, pool, *reason)
	case "abort-drain":
		report, err = workflow.AbortWorkflowDrain(ctx, pool, *reason)
	case "preflight":
		report, err = workflow.PreflightWorkflowCutover(ctx, pool)
	case "verify":
		report, err = workflow.VerifyWorkflowCutover(ctx, pool)
	case "reopen":
		report, err = workflow.ReopenWorkflowAdmission(ctx, pool, *reason)
	}
	if err != nil && !errors.Is(err, workflow.ErrWorkflowCutoverBlocked) {
		fmt.Fprintf(stderr, "workflow-cutover %s: %v\n", command, err)
		return exitFailure
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if encodeErr := encoder.Encode(report); encodeErr != nil {
		fmt.Fprintf(stderr, "workflow-cutover: %v\n", encodeErr)
		return exitFailure
	}
	if errors.Is(err, workflow.ErrWorkflowCutoverBlocked) {
		return exitBlocked
	}
	return exitOK
}

func usage(w io.Writer) {
	fmt.Fprintln(w, `usage: workflow-cutover <mode|drain|abort-drain|preflight|verify|reopen> [-timeout 30s] [-reason text]
  mode         print {"mode":"cutover"|"none"} for upgrading this database
  drain        switch admission to draining; ready once every run finished
  abort-drain  reopen admission for a release abandoned before migrating
  preflight  read-only drain check before the workflow attempt migration
  verify     read-only check after the migration, admission still draining
  reopen     verify, then reopen workflow admission
exit codes: 0 ready, 1 failure, 2 usage, 3 blocked`)
}
