package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/OpenLinker-ai/openlinker-core/pkg/catalogregistry"
	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/registryprojection"
)

const registryMigrateUsage = "usage: api registry-migrate <inventory|preflight|cutover|reconcile|rollback|status|exclude> [flags]"

type registryMigrateCommand struct {
	Name    string
	Add     bool
	Remove  bool
	Kind    string
	Subject uuid.UUID
	Version *uuid.UUID
	Reason  string
	Timeout time.Duration
}

func parseRegistryMigrateArgs(args []string) (registryMigrateCommand, error) {
	cmd := registryMigrateCommand{Timeout: 10 * time.Minute}
	if len(args) == 0 {
		return cmd, errors.New(registryMigrateUsage)
	}
	cmd.Name = args[0]
	fs := flag.NewFlagSet("registry-migrate "+cmd.Name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var subject, version string
	switch cmd.Name {
	case "inventory", "preflight", "reconcile", "status":
	case "cutover", "rollback":
		fs.Func("timeout", "how long to drive delivery", func(v string) error {
			d, err := time.ParseDuration(v)
			if err != nil || d <= 0 {
				return errors.New("timeout must be a positive duration")
			}
			cmd.Timeout = d
			return nil
		})
	case "exclude":
		fs.BoolVar(&cmd.Add, "add", false, "add an exclusion")
		fs.BoolVar(&cmd.Remove, "remove", false, "remove an exclusion")
		fs.StringVar(&cmd.Kind, "kind", "", "skill or mcp")
		fs.StringVar(&subject, "subject", "", "skill package or MCP Agent id")
		fs.StringVar(&version, "version", "", "skill package version id")
		fs.StringVar(&cmd.Reason, "reason", "", "why the subject is excluded")
	default:
		return registryMigrateCommand{}, errors.New(registryMigrateUsage)
	}
	if err := fs.Parse(args[1:]); err != nil {
		return registryMigrateCommand{}, err
	}
	if fs.NArg() > 0 {
		return registryMigrateCommand{}, errors.New(registryMigrateUsage)
	}
	if cmd.Name != "exclude" {
		return cmd, nil
	}
	if cmd.Add == cmd.Remove {
		return registryMigrateCommand{}, errors.New("exclude needs exactly one of --add or --remove")
	}
	if cmd.Kind != registryprojection.KindSkill && cmd.Kind != registryprojection.KindMCP {
		return registryMigrateCommand{}, errors.New("--kind must be skill or mcp")
	}
	id, err := uuid.Parse(subject)
	if err != nil {
		return registryMigrateCommand{}, errors.New("--subject must be a UUID")
	}
	cmd.Subject = id
	if version != "" {
		if cmd.Kind != registryprojection.KindSkill {
			return registryMigrateCommand{}, errors.New("--version applies only to skill packages")
		}
		v, err := uuid.Parse(version)
		if err != nil {
			return registryMigrateCommand{}, errors.New("--version must be a UUID")
		}
		cmd.Version = &v
	}
	if cmd.Add && cmd.Reason == "" {
		return registryMigrateCommand{}, errors.New("--add needs --reason")
	}
	return cmd, nil
}

func runRegistryMigrate(args []string) {
	if code := runRegistryMigrateWith(args, os.Stdout, os.Stderr); code != 0 {
		os.Exit(code)
	}
}

func runRegistryMigrateWith(args []string, stdout, stderr io.Writer) int {
	cmd, err := parseRegistryMigrateArgs(args)
	if err != nil {
		fmt.Fprintln(stderr, "registry-migrate:", err)
		return 2
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(stderr, "registry-migrate:", err)
		return 1
	}
	registries, err := catalogregistry.Parse(cfg.CatalogRegistries)
	if err != nil {
		fmt.Fprintln(stderr, "registry-migrate:", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		fmt.Fprintln(stderr, "registry-migrate: connect database failed")
		return 1
	}
	defer pool.Close()
	proj, err := registryprojection.New(pool, catalogregistry.NewClient(registries, catalogregistry.Options{}), cfg.CatalogProjectionRegistry, cfg.CatalogProjectionPublicURL)
	if err != nil {
		fmt.Fprintln(stderr, "registry-migrate:", err)
		return 1
	}
	out, err := executeRegistryMigrate(ctx, proj, cmd)
	if out != nil {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(out)
	}
	if err != nil {
		fmt.Fprintln(stderr, "registry-migrate:", err)
		return 1
	}
	return 0
}

func executeRegistryMigrate(ctx context.Context, proj *registryprojection.Projector, cmd registryMigrateCommand) (any, error) {
	switch cmd.Name {
	case "inventory":
		return proj.Inventory(ctx)
	case "preflight":
		pre, err := proj.Preflight(ctx)
		if err == nil && len(pre.Blockers) > 0 {
			return pre, fmt.Errorf("%d preflight blockers", len(pre.Blockers))
		}
		return pre, err
	case "cutover":
		rep, err := proj.Cutover(ctx, cmd.Timeout)
		if err == nil && !rep.Complete {
			err = errors.New("cutover is not complete")
		}
		return rep, err
	case "reconcile":
		return proj.Reconcile(ctx)
	case "rollback":
		return proj.Rollback(ctx, cmd.Timeout)
	case "status":
		return proj.Status(ctx)
	case "exclude":
		if cmd.Add {
			return nil, proj.Exclude(ctx, cmd.Kind, cmd.Subject, cmd.Version, cmd.Reason)
		}
		return nil, proj.Unexclude(ctx, cmd.Kind, cmd.Subject, cmd.Version)
	}
	return nil, errors.New(registryMigrateUsage)
}
