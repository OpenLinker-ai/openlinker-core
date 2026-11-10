package mcpbinding

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// The HMAC framing is pinned to the value the pre-transport code produced
// (cross-checked with an independent HMAC-SHA256 implementation).
func TestConfigDigestGolden(t *testing.T) {
	target := uuid.MustParse("6a1f2a7e-2f43-4b2e-9a36-0d8c8b7f0a11")
	const want = "6136175d98778a12b8400acdce44fc37f1d6793bd1b8afa90acd09194bacc393"
	if got := configDigest("golden-master-secret", target, []byte(`["x"]`)); got != want {
		t.Fatalf("configDigest = %s, want %s", got, want)
	}
}

// Default Agents must authenticate exactly the pre-transport configuration,
// so saved grants keep their digest; non-default transports and header names
// must change it.
func TestResolveDigestKeepsDefaultConfigAndCoversTransport(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	owner, target := uuid.New(), uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO users(id,email,password_hash,display_name,is_creator) VALUES($1,$2,'synthetic','Synthetic',true)`, owner, owner.String()+"@example.invalid"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO agents(id,creator_id,slug,name,description,endpoint_url,endpoint_auth_header,connection_mode,mcp_tool_name,price_per_call_cents)
VALUES($1,$2,$3,'Digest MCP','Synthetic','https://mcp.example/mcp','Bearer secret','mcp_server','lookup',0)`, target, owner, "digest-"+target.String()[:8]); err != nil {
		t.Fatal(err)
	}
	const secret = "digest-test-master-secret"
	legacyDigest := func() string {
		var config []byte
		if err := tx.QueryRow(ctx, `SELECT `+legacyConfigSQL+` FROM agents a LEFT JOIN agent_capabilities c ON c.agent_id=a.id LEFT JOIN agent_call_policies p ON p.agent_id=a.id WHERE a.id=$1`, target).Scan(&config); err != nil {
			t.Fatal(err)
		}
		return configDigest(secret, target, config)
	}
	resolve := func() string {
		tool, err := Resolve(ctx, tx, owner, target, secret)
		if err != nil {
			t.Fatal(err)
		}
		return tool.Digest
	}
	defaultDigest := resolve()
	if want := legacyDigest(); defaultDigest != want {
		t.Fatalf("default Agent digest = %s, pre-transport digest = %s", defaultDigest, want)
	}
	seen := map[string]string{defaultDigest: "default"}
	for _, change := range []string{
		`UPDATE agents SET mcp_transport='streamable_http' WHERE id=$1`,
		`UPDATE agents SET mcp_transport='legacy_sse' WHERE id=$1`,
		`UPDATE agents SET mcp_transport='legacy_json', endpoint_auth_header_name='X-Api-Key' WHERE id=$1`,
		`UPDATE agents SET endpoint_auth_header_name='X-Other-Key' WHERE id=$1`,
	} {
		if _, err := tx.Exec(ctx, change, target); err != nil {
			t.Fatal(err)
		}
		digest := resolve()
		if previous, ok := seen[digest]; ok {
			t.Fatalf("%s kept the digest of %s", change, previous)
		}
		seen[digest] = change
	}
	if _, err := tx.Exec(ctx, `UPDATE agents SET mcp_transport='legacy_json', endpoint_auth_header_name=NULL WHERE id=$1`, target); err != nil {
		t.Fatal(err)
	}
	if got := resolve(); got != defaultDigest {
		t.Fatalf("restoring defaults gave %s, want %s", got, defaultDigest)
	}
}
