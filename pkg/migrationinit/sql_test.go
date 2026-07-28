package migrationinit

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCoreMigrationDirectoryContainsCurrentInitializerAndSupportedForwardMigration(t *testing.T) {
	paths, err := filepath.Glob("../../migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"086_current_schema_init.up.sql":             true,
		"086_current_schema_init_verify.sql":         true,
		"087_browser_agent_execution_profile.up.sql": true,
		"088_browser_human_control.up.sql":           true,
	}
	if len(paths) != len(want) {
		t.Fatalf("migration SQL files = %v, want only current initializer and verifier", paths)
	}
	for _, path := range paths {
		if !want[filepath.Base(path)] {
			t.Fatalf("unexpected executable migration file %s", path)
		}
	}
}

func TestBrowserHumanControlForwardMigrationIsSingleAuditableTransition(t *testing.T) {
	up := readInitializer(
		t,
		"../../migrations/088_browser_human_control.up.sql",
	)
	for _, fragment := range []string{
		"CREATE TABLE public.browser_run_controls",
		"browser_run_controls_human_consistent",
		"browser_run_controls_expiry_idx",
		"CREATE TABLE public.browser_human_control_audits",
		"browser_human_control_audits_counts_nonnegative",
		"browser_human_control_audits_run_created_idx",
	} {
		if !strings.Contains(up, fragment) {
			t.Fatalf("Browser human-control migration missing %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"IF NOT EXISTS",
		"DO $$",
		"EXECUTE ",
	} {
		if strings.Contains(up, forbidden) {
			t.Fatalf(
				"Browser human-control migration hides or duplicates its transition with %q",
				forbidden,
			)
		}
	}
}

func TestBrowserAgentExecutionProfileForwardMigrationIsSingleAuditableTransition(t *testing.T) {
	up := readInitializer(
		t,
		"../../migrations/087_browser_agent_execution_profile.up.sql",
	)
	for _, fragment := range []string{
		"ADD COLUMN browser_execution_profile",
		"agents_browser_execution_profile_private",
		"CREATE TABLE public.runtime_agent_execution_profiles",
		"runtime_agent_execution_profiles_reset_consistent",
		"runtime_agent_execution_profiles_credential_id_fkey",
		"runtime_agent_execution_profiles_reset_by_user_id_fkey",
		"CREATE INDEX runtime_agent_execution_profiles_browser_idx",
	} {
		if !strings.Contains(up, fragment) {
			t.Fatalf("Browser Agent profile migration missing %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"IF NOT EXISTS",
		"DO $$",
		"EXECUTE ",
	} {
		if strings.Contains(up, forbidden) {
			t.Fatalf(
				"Browser Agent profile migration hides or duplicates its transition with %q",
				forbidden,
			)
		}
	}
}

func TestCoreFoundationalInitializerContainsPredecessorContracts(t *testing.T) {
	up := readInitializer(t, "../../migrations/086_current_schema_init.up.sql")
	for _, fragment := range []string{
		"CREATE TABLE public.runtime_node_bindings",
		"binding_mode text DEFAULT 'mtls'::text NOT NULL",
		"CREATE TABLE public.runtime_node_certificates",
		"certificate_chain_pem text",
		"CREATE INDEX idx_runtime_node_certificates_retention",
		"CREATE INDEX idx_run_events_metric_cursor",
		"CREATE INDEX idx_runtime_sessions_credential_lifecycle",
		"CREATE FUNCTION public.emit_event_wake_notification()",
		"CREATE FUNCTION public.emit_external_execution_wake_notification()",
		"CREATE FUNCTION public.enforce_run_attempt_runtime_attachment_evidence()",
		"CREATE TABLE public.external_execution_keys",
		"CREATE TABLE public.external_execution_cancellations",
		"CREATE TABLE public.workflow_run_cancellations",
		"CREATE TABLE public.workflow_step_launches",
		"expected_contract_hash text",
		"input_schema_fingerprint bytea",
		"CREATE TABLE public.oauth_login_codes",
		"jwt text,",
		"scopes text[] DEFAULT ARRAY['agents:run'::text] NOT NULL",
		"ADD CONSTRAINT api_keys_pkey PRIMARY KEY (id)",
		"ADD CONSTRAINT api_keys_user_id_fkey FOREIGN KEY (user_id)",
		"INSERT INTO public.runtime_wire_contracts",
		"INSERT INTO public.runtime_schema_contracts",
		"(79, '079_runtime_attempt_transport_evidence'",
	} {
		if !strings.Contains(up, fragment) {
			t.Fatalf("Core initializer missing %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"\nLOCK TABLE ",
		"\nSELECT pg_advisory_xact_lock",
		"CREATE INDEX CONCURRENTLY",
		"ALTER COLUMN jwt SET NOT NULL",
	} {
		if strings.Contains(up, forbidden) {
			t.Fatalf("Core initializer contains transition-only statement %q", forbidden)
		}
	}
	for _, futureCatalog := range []string{
		"runtime_agent_execution_profiles",
		"browser_execution_profile",
		"browser_run_controls",
		"browser_human_control_audits",
	} {
		if strings.Contains(up, futureCatalog) {
			t.Fatalf(
				"Core version 086 predecessor contains later Browser catalog %q",
				futureCatalog,
			)
		}
	}
}

func TestCoreInitializerVerifierCoversCatalogAndSeedState(t *testing.T) {
	verify := readInitializer(t, "../../migrations/086_current_schema_init_verify.sql")
	for _, fragment := range []string{
		"public_tables <> 72",
		"public_constraints <> 615",
		"public_indexes <> 265",
		"public_triggers <> 70",
		"public_functions <> 65",
		"NOT IN ('schema_migrations', 'schema_migrations_cloud')",
		"Core initializer built-in skills are incomplete",
		"Core Runtime schema contract initialization is inconsistent",
		"count(*) FROM runtime_schema_contracts) <> 10",
		"Core Runtime wire contract initialization is inconsistent",
		"idx_runtime_node_certificates_retention",
		"idx_runtime_sessions_credential_lifecycle",
		"runtime_agent_execution_profiles_browser_idx",
		"browser_run_controls_expiry_idx",
		"browser_human_control_audits_run_created_idx",
		"agents_browser_execution_profile_private",
	} {
		if !strings.Contains(verify, fragment) {
			t.Fatalf("Core initializer verifier missing %q", fragment)
		}
	}
}

func readInitializer(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}
