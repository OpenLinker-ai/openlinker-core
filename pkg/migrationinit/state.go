// Package migrationinit protects the current-schema initialization boundary.
// It deliberately supports only a fresh database or the exact current schema;
// historical upgrade orchestration does not belong in this phase.
package migrationinit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

const (
	CoreVersion       int64 = 86
	CloudVersion      int64 = 55
	CoreSchemaDigest        = "6c22808a8cd658cf827a5828a92d3343f040d7d6ff3302f9fdab691fe90aec5b"
	CloudSchemaDigest       = "0cf21f9a518d9875e62e66e1b490148e45b67eaaeddf9cab118efd778575abd5"
)

var coreTables = []string{
	"a2a_context_mappings",
	"agent_action_approval_requests",
	"agent_availability_alerts",
	"agent_availability_snapshots",
	"agent_call_policies",
	"agent_capabilities",
	"agent_examples",
	"agent_metric_snapshots",
	"agent_onboarding_status",
	"agent_skill_benchmark_runs",
	"agent_skill_scores",
	"agent_skills",
	"agent_tokens",
	"agents",
	"core_instance_identity",
	"delivery_targets",
	"external_execution_cancellations",
	"external_execution_keys",
	"external_executions",
	"oauth_login_codes",
	"proxy_run_artifacts",
	"proxy_runs",
	"registry_federation_invites",
	"registry_listing_links",
	"registry_nodes",
	"registry_peers",
	"run_accounting_ledger",
	"run_artifact_chunks",
	"run_artifacts",
	"run_attempts",
	"run_cancellations",
	"run_dead_letters",
	"run_delegations",
	"run_deliveries",
	"run_effect_outbox",
	"run_effect_replays",
	"run_event_retention_watermarks",
	"run_events",
	"run_messages",
	"run_requirement_evidence",
	"runs",
	"runtime_cluster_control",
	"runtime_cluster_members",
	"runtime_node_bindings",
	"runtime_node_certificates",
	"runtime_nodes",
	"runtime_pki_authorities",
	"runtime_resume_grants",
	"runtime_schema_contracts",
	"runtime_session_attachments",
	"runtime_sessions",
	"runtime_signal_outbox",
	"runtime_wire_contracts",
	"skill_proposals",
	"skill_test_cases",
	"skills",
	"task_callback_deliveries",
	"task_callback_subscriptions",
	"task_queries",
	"user_token_core_grants",
	"user_tokens",
	"users",
	"webhook_deliveries",
	"workflow_nodes",
	"workflow_run_cancellations",
	"workflow_run_steps",
	"workflow_runs",
	"workflow_step_launches",
	"workflows",
}

var cloudTables = []string{
	"auth_verification_codes",
	"charges",
	"service_listings",
	"service_orders",
	"user_token_cloud_grants",
	"wallets",
	"withdrawals",
}

var obsoleteCloudTables = []string{
	"user_token_credentials",
}

var builtInSkillIDs = []string{
	"content/translation", "content/summarization", "content/copywriting",
	"content/proofreading", "content/structured-data", "dev/code-review",
	"dev/code-generation", "dev/code-explanation", "dev/test-generation",
	"dev/devops-ci", "data/sql-query", "data/data-cleaning", "data/analysis",
	"data/visualization", "data/forecasting", "media/image-generate",
	"media/image-edit", "media/audio-transcribe", "media/audio-generate",
	"media/video-process", "ops/document-generate", "ops/email-process",
	"ops/scheduling", "ops/web-scraping", "ops/notification", "ai/rag",
	"ai/agent-orchestration", "ai/finetune", "ai/prompt-engineering",
	"ai/safety-eval",
}

// MigrationTableState is the complete state of one golang-migrate version
// table. Rows must be exactly one whenever the table exists.
type MigrationTableState struct {
	Exists  bool
	Rows    int64
	Version int64
	Dirty   bool
}

// SchemaShape is a closed current-schema fingerprint for one owner. The
// object counts are scoped to the owner's canonical tables.
type SchemaShape struct {
	Digest            string
	Tables            int64
	Constraints       int64
	Indexes           int64
	Triggers          int64
	GuardFunctions    int64
	CoreIdentities    int64
	RuntimeControls   int64
	RuntimeSchemas    int64
	CurrentRuntime    int64
	RuntimeWires      int64
	CurrentWire       int64
	PreviousWire      int64
	BuiltInSkills     int64
	BuiltInSkillCases int64
}

// Snapshot contains migration bookkeeping plus immutable catalog evidence.
type Snapshot struct {
	Core                  MigrationTableState
	Cloud                 MigrationTableState
	NonBookkeepingObjects int64
	CoreShape             SchemaShape
	CloudShape            SchemaShape
	ObsoleteCloudObjects  int64
}

// Inspect reads initialization evidence without creating migration tables.
func Inspect(ctx context.Context, databaseURL string) (Snapshot, error) {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return Snapshot{}, fmt.Errorf("connect migration inspection database: %w", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	coreState, err := readMigrationTable(ctx, conn, "schema_migrations")
	if err != nil {
		return Snapshot{}, err
	}
	cloudState, err := readMigrationTable(ctx, conn, "schema_migrations_cloud")
	if err != nil {
		return Snapshot{}, err
	}

	var snapshot Snapshot
	snapshot.Core = coreState
	snapshot.Cloud = cloudState
	if err := conn.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public'
		  AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
		  AND c.relname NOT IN ('schema_migrations', 'schema_migrations_cloud')
	`).Scan(&snapshot.NonBookkeepingObjects); err != nil {
		return Snapshot{}, fmt.Errorf("inspect public relations: %w", err)
	}

	snapshot.CoreShape, err = inspectShape(ctx, conn, coreTables, false)
	if err != nil {
		return Snapshot{}, fmt.Errorf("inspect Core schema: %w", err)
	}
	snapshot.CloudShape, err = inspectShape(ctx, conn, cloudTables, true)
	if err != nil {
		return Snapshot{}, fmt.Errorf("inspect Cloud schema: %w", err)
	}
	if err := conn.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public'
		  AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
		  AND c.relname = ANY($1::text[])
	`, obsoleteCloudTables).Scan(&snapshot.ObsoleteCloudObjects); err != nil {
		return Snapshot{}, fmt.Errorf("inspect obsolete Cloud relations: %w", err)
	}

	if snapshot.CoreShape.Tables == int64(len(coreTables)) {
		if err := inspectCoreSeeds(ctx, conn, &snapshot.CoreShape); err != nil {
			return Snapshot{}, err
		}
	}
	return snapshot, nil
}

func readMigrationTable(ctx context.Context, conn *pgx.Conn, table string) (MigrationTableState, error) {
	qualified := "public." + pgx.Identifier{table}.Sanitize()
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, qualified).Scan(&exists); err != nil {
		return MigrationTableState{}, fmt.Errorf("inspect %s existence: %w", table, err)
	}
	state := MigrationTableState{Exists: exists}
	if !exists {
		return state, nil
	}

	rows, err := conn.Query(ctx, `SELECT version::bigint, dirty FROM `+qualified)
	if err != nil {
		return MigrationTableState{}, fmt.Errorf("inspect %s rows: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		state.Rows++
		if state.Rows == 1 {
			if err := rows.Scan(&state.Version, &state.Dirty); err != nil {
				return MigrationTableState{}, fmt.Errorf("scan %s row: %w", table, err)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return MigrationTableState{}, fmt.Errorf("read %s rows: %w", table, err)
	}
	return state, nil
}

func inspectShape(ctx context.Context, conn *pgx.Conn, tables []string, cloud bool) (SchemaShape, error) {
	var shape SchemaShape
	if err := conn.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public'
		  AND c.relkind IN ('r', 'p')
		  AND c.relname = ANY($1::text[])
	`, tables).Scan(&shape.Tables); err != nil {
		return SchemaShape{}, err
	}
	if shape.Tables == 0 {
		return shape, nil
	}
	if err := conn.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_constraint con
		JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relname = ANY($1::text[])
	`, tables).Scan(&shape.Constraints); err != nil {
		return SchemaShape{}, err
	}
	if err := conn.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_indexes
		WHERE schemaname = 'public' AND tablename = ANY($1::text[])
	`, tables).Scan(&shape.Indexes); err != nil {
		return SchemaShape{}, err
	}
	if err := conn.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_trigger t
		JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public'
		  AND c.relname = ANY($1::text[])
		  AND NOT t.tgisinternal
	`, tables).Scan(&shape.Triggers); err != nil {
		return SchemaShape{}, err
	}
	if cloud {
		if err := conn.QueryRow(ctx, `
			SELECT count(*)
			FROM pg_catalog.pg_proc p
			JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
			WHERE n.nspname = 'public' AND p.proname = 'service_listings_guard_slug'
		`).Scan(&shape.GuardFunctions); err != nil {
			return SchemaShape{}, err
		}
	}
	digest, err := inspectSchemaDigest(ctx, conn, tables, cloud)
	if err != nil {
		return SchemaShape{}, err
	}
	shape.Digest = digest
	return shape, nil
}

func inspectSchemaDigest(ctx context.Context, conn *pgx.Conn, tables []string, cloud bool) (string, error) {
	rows, err := conn.Query(ctx, `
		WITH scoped_tables AS (
			SELECT c.oid, c.relname, c.relkind, c.relpersistence
			FROM pg_catalog.pg_class c
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') AND c.relname = ANY($1::text[])
		), objects AS (
			SELECT 'table'::text AS kind, t.relname AS identity,
			       concat_ws('|', t.relkind::text, t.relpersistence::text) AS definition
			FROM scoped_tables t
			UNION ALL
			SELECT 'column', t.relname || '/' || lpad(a.attnum::text, 5, '0'),
			       concat_ws('|', a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod),
			         a.attnotnull::text, a.attidentity::text, a.attgenerated::text,
			         coalesce(pg_catalog.pg_get_expr(d.adbin, d.adrelid, true), ''))
			FROM scoped_tables t
			JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid
			LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
			WHERE a.attnum > 0 AND NOT a.attisdropped
			UNION ALL
			SELECT 'constraint', t.relname || '/' || c.conname,
			       concat_ws('|', c.contype::text, c.convalidated::text, c.condeferrable::text,
			         c.condeferred::text, pg_catalog.pg_get_constraintdef(c.oid, true))
			FROM scoped_tables t
			JOIN pg_catalog.pg_constraint c ON c.conrelid = t.oid
			UNION ALL
			SELECT 'index', t.relname || '/' || i.relname, pg_catalog.pg_get_indexdef(i.oid, 0, true)
			FROM scoped_tables t
			JOIN pg_catalog.pg_index x ON x.indrelid = t.oid
			JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
			UNION ALL
			SELECT 'trigger', t.relname || '/' || g.tgname, pg_catalog.pg_get_triggerdef(g.oid, true)
			FROM scoped_tables t
			JOIN pg_catalog.pg_trigger g ON g.tgrelid = t.oid
			WHERE NOT g.tgisinternal
			UNION ALL
			SELECT 'function', p.proname || '(' || pg_catalog.pg_get_function_identity_arguments(p.oid) || ')',
			       pg_catalog.pg_get_functiondef(p.oid)
			FROM pg_catalog.pg_proc p
			JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
			WHERE n.nspname = 'public'
			  AND (($2::boolean AND p.proname = 'service_listings_guard_slug')
			       OR (NOT $2::boolean AND p.proname <> 'service_listings_guard_slug'))
		)
		SELECT kind, identity, definition
		FROM objects
		ORDER BY kind, identity, definition
	`, tables, cloud)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	hash := sha256.New()
	for rows.Next() {
		var kind, identity, definition string
		if err := rows.Scan(&kind, &identity, &definition); err != nil {
			return "", err
		}
		for _, value := range []string{kind, identity, definition} {
			_, _ = fmt.Fprintf(hash, "%d:", len(value))
			_, _ = hash.Write([]byte(value))
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func inspectCoreSeeds(ctx context.Context, conn *pgx.Conn, shape *SchemaShape) error {
	queries := []struct {
		label string
		query string
		args  []any
		dest  *int64
	}{
		{"Core instance identity", `SELECT count(*) FROM public.core_instance_identity WHERE singleton`, nil, &shape.CoreIdentities},
		{"Runtime cluster control", `SELECT count(*) FROM public.runtime_cluster_control WHERE singleton_id = 1`, nil, &shape.RuntimeControls},
		{"Runtime schema contracts", `SELECT count(*) FROM public.runtime_schema_contracts`, nil, &shape.RuntimeSchemas},
		{"current Runtime schema", `SELECT count(*) FROM public.runtime_schema_contracts WHERE schema_version = 80 AND migration_name = '080_runtime_attempt_transport_evidence' AND runtime_contract_id = 'openlinker.runtime.v2' AND runtime_contract_digest = '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481' AND is_current`, nil, &shape.CurrentRuntime},
		{"Runtime wire contracts", `SELECT count(*) FROM public.runtime_wire_contracts`, nil, &shape.RuntimeWires},
		{"current Runtime wire", `SELECT count(*) FROM public.runtime_wire_contracts WHERE runtime_contract_id = 'openlinker.runtime.v2' AND runtime_contract_digest = '4be9b2fe09eeedf0e37119075134064be88f93b301c502cdfa21a6cb978c6481' AND support_tier = 'current'`, nil, &shape.CurrentWire},
		{"previous Runtime wire", `SELECT count(*) FROM public.runtime_wire_contracts WHERE runtime_contract_id = 'openlinker.runtime.v2' AND runtime_contract_digest = '3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9' AND support_tier = 'previous'`, nil, &shape.PreviousWire},
		{"built-in skills", `SELECT count(*) FROM public.skills WHERE id = ANY($1::text[])`, []any{builtInSkillIDs}, &shape.BuiltInSkills},
		{"built-in skill cases", `SELECT count(*) FROM public.skill_test_cases WHERE skill_id = ANY($1::text[])`, []any{[]string{"content/translation", "content/summarization", "dev/code-review", "data/sql-query", "ops/email-process"}}, &shape.BuiltInSkillCases},
	}
	for _, item := range queries {
		if err := conn.QueryRow(ctx, item.query, item.args...).Scan(item.dest); err != nil {
			return fmt.Errorf("inspect %s: %w", item.label, err)
		}
	}
	return nil
}

// ValidateCoreUp returns true when the current schema is already installed.
func (s Snapshot) ValidateCoreUp() (bool, error) {
	if !s.Core.Exists {
		if s.NonBookkeepingObjects != 0 {
			return false, fmt.Errorf("Core initialization requires an empty database; found %d public relations", s.NonBookkeepingObjects)
		}
		if s.Cloud.Exists {
			return false, errors.New("Core initialization found Cloud migration bookkeeping without Core")
		}
		return false, nil
	}
	if err := validateMigrationTable("Core", s.Core, CoreVersion); err != nil {
		return false, err
	}
	if err := validateCoreShape(s.CoreShape); err != nil {
		return false, err
	}
	if err := s.validateKnownRelations(); err != nil {
		return false, err
	}
	return true, nil
}

// ValidateCloudUp returns true when the current Cloud schema is already installed.
func (s Snapshot) ValidateCloudUp() (bool, error) {
	if err := validateMigrationTable("Core", s.Core, CoreVersion); err != nil {
		return false, fmt.Errorf("Cloud initialization requires current Core: %w", err)
	}
	if err := validateCoreShape(s.CoreShape); err != nil {
		return false, fmt.Errorf("Cloud initialization requires current Core: %w", err)
	}
	if !s.Cloud.Exists {
		if s.CloudShape.Tables != 0 || s.CloudShape.GuardFunctions != 0 || s.ObsoleteCloudObjects != 0 {
			return false, errors.New("Cloud initialization found a partial or legacy Cloud schema")
		}
		if err := s.validateKnownRelations(); err != nil {
			return false, err
		}
		return false, nil
	}
	if err := validateMigrationTable("Cloud", s.Cloud, CloudVersion); err != nil {
		return false, err
	}
	if err := validateCloudShape(s.CloudShape, s.ObsoleteCloudObjects); err != nil {
		return false, err
	}
	if err := s.validateKnownRelations(); err != nil {
		return false, err
	}
	return true, nil
}

func (s Snapshot) validateKnownRelations() error {
	want := s.CoreShape.Tables + s.CloudShape.Tables + s.ObsoleteCloudObjects
	if s.NonBookkeepingObjects != want {
		return fmt.Errorf("public relation ownership mismatch: found %d relations, recognized %d", s.NonBookkeepingObjects, want)
	}
	return nil
}

func validateMigrationTable(owner string, state MigrationTableState, current int64) error {
	if !state.Exists {
		return fmt.Errorf("%s migration table is missing", owner)
	}
	if state.Rows != 1 {
		return fmt.Errorf("%s migration table has %d rows; expected exactly one", owner, state.Rows)
	}
	if state.Dirty {
		return fmt.Errorf("%s migration %d is dirty", owner, state.Version)
	}
	if state.Version != current {
		return fmt.Errorf("%s migration %d is unsupported; rebuild an empty database at version %d", owner, state.Version, current)
	}
	return nil
}

func validateCoreShape(shape SchemaShape) error {
	want := SchemaShape{
		Digest:            CoreSchemaDigest,
		Tables:            69,
		Constraints:       587,
		Indexes:           259,
		Triggers:          70,
		CoreIdentities:    1,
		RuntimeControls:   1,
		RuntimeSchemas:    10,
		CurrentRuntime:    1,
		RuntimeWires:      5,
		CurrentWire:       1,
		PreviousWire:      1,
		BuiltInSkills:     30,
		BuiltInSkillCases: 15,
	}
	if shape.Digest != want.Digest || shape.Tables != want.Tables || shape.Constraints != want.Constraints ||
		shape.Indexes != want.Indexes || shape.Triggers != want.Triggers ||
		shape.CoreIdentities != want.CoreIdentities || shape.RuntimeControls != want.RuntimeControls ||
		shape.RuntimeSchemas != want.RuntimeSchemas || shape.CurrentRuntime != want.CurrentRuntime ||
		shape.RuntimeWires != want.RuntimeWires || shape.CurrentWire != want.CurrentWire ||
		shape.PreviousWire != want.PreviousWire || shape.BuiltInSkills != want.BuiltInSkills ||
		shape.BuiltInSkillCases < want.BuiltInSkillCases {
		return fmt.Errorf("Core schema fingerprint mismatch: %s", formatShape(shape))
	}
	return nil
}

func validateCloudShape(shape SchemaShape, obsolete int64) error {
	if shape.Digest != CloudSchemaDigest || shape.Tables != 7 || shape.Constraints != 72 || shape.Indexes != 30 ||
		shape.Triggers != 5 || shape.GuardFunctions != 1 || obsolete != 0 {
		return fmt.Errorf("Cloud schema fingerprint mismatch: %s obsolete=%d", formatShape(shape), obsolete)
	}
	return nil
}

func formatShape(shape SchemaShape) string {
	parts := []string{
		fmt.Sprintf("digest=%s", shape.Digest),
		fmt.Sprintf("tables=%d", shape.Tables),
		fmt.Sprintf("constraints=%d", shape.Constraints),
		fmt.Sprintf("indexes=%d", shape.Indexes),
		fmt.Sprintf("triggers=%d", shape.Triggers),
		fmt.Sprintf("guard_functions=%d", shape.GuardFunctions),
		fmt.Sprintf("runtime_schemas=%d", shape.RuntimeSchemas),
		fmt.Sprintf("runtime_wires=%d", shape.RuntimeWires),
	}
	return strings.Join(parts, ",")
}
