// Package mcpbinding owns private Agent tool grants and immutable Run snapshots.
// Endpoint credentials stay in Core. Consumers receive tool schemas and IDs only.
package mcpbinding

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const MetadataKey = "_openlinker_mcp_bindings"
const CallMetadataKey = "_openlinker_mcp_binding"
const Feature = "mcp_bindings.v1"
const MaxBindings = 16

type DB interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}
type Tool struct {
	BindingID   uuid.UUID       `json:"binding_id"`
	TargetID    uuid.UUID       `json:"target_agent_id"`
	Digest      string          `json:"digest"`
	Name        string          `json:"name"`
	ToolName    string          `json:"tool_name"`
	Title       string          `json:"title"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}
type Reference struct {
	BindingID uuid.UUID `json:"binding_id"`
	Digest    string    `json:"digest"`
}
type SnapshotData struct {
	Schema int    `json:"schema_version"`
	Tools  []Tool `json:"tools"`
}

func denied() error {
	return httpx.Forbidden("Tool authorization is unavailable; refresh the Agent configuration")
}

// Resolve checks the selecting owner's authority and authenticates every execution-
// relevant service field. Changing a URL, credential, tool, schema or policy
// invalidates the grant until the owner explicitly saves it again. The stable Core
// master secret prevents the public digest from becoming an offline credential oracle.
func Resolve(ctx context.Context, q DB, owner, target uuid.UUID, secret string) (Tool, error) {
	if strings.TrimSpace(secret) == "" {
		return Tool{}, httpx.ServiceUnavailable("MCP authorization key is unavailable")
	}
	var t Tool
	var config []byte
	err := q.QueryRow(ctx, `SELECT a.name,a.description,a.mcp_tool_name,COALESCE(c.input_schema,'{"type":"object"}'::jsonb),
 jsonb_build_array(a.creator_id,a.connection_mode,a.lifecycle_status,a.visibility,a.endpoint_url,a.endpoint_auth_header,a.mcp_tool_name,COALESCE(c.input_schema,'{"type":"object"}'::jsonb),COALESCE(p.callable_by,'public'))
 FROM agents a LEFT JOIN agent_capabilities c ON c.agent_id=a.id LEFT JOIN agent_call_policies p ON p.agent_id=a.id
 WHERE a.id=$1 AND a.connection_mode='mcp_server' AND a.lifecycle_status='active'
 AND (a.creator_id=$2 OR (a.visibility IN ('public','unlisted') AND COALESCE(p.callable_by,'public')='public')) FOR SHARE OF a`, target, owner).Scan(&t.Title, &t.Description, &t.ToolName, &t.InputSchema, &config)
	if errors.Is(err, pgx.ErrNoRows) {
		return t, denied()
	}
	if err != nil {
		return t, err
	}
	if len(t.InputSchema) > 16*1024 {
		return t, httpx.NewError(400, "MCP_SCHEMA_TOO_LARGE", "MCP tool schema exceeds 16 KiB")
	}
	var schema map[string]any
	if json.Unmarshal(t.InputSchema, &schema) != nil || schema["type"] != "object" {
		return t, httpx.NewError(400, "MCP_SCHEMA_INVALID", "MCP tool requires an object input schema")
	}
	t.TargetID = target
	t.Name = "mcp_" + strings.ReplaceAll(target.String(), "-", "")
	if len([]rune(t.Description)) > 500 {
		t.Description = string([]rune(t.Description)[:500])
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("openlinker/mcp-binding/config/v1\x00"))
	_, _ = mac.Write(target[:])
	_, _ = mac.Write(config)
	t.Digest = hex.EncodeToString(mac.Sum(nil))
	return t, nil
}

func ReferenceFrom(value any) (*Reference, error) {
	if value == nil {
		return nil, nil
	}
	raw, err := json.Marshal(value)
	var r Reference
	if err != nil || json.Unmarshal(raw, &r) != nil || r.BindingID == uuid.Nil || len(r.Digest) != 64 {
		return nil, denied()
	}
	return &r, nil
}

// Authorize is called both before admission and under the parent's active
// Attempt locks. A removed/replaced grant cannot be revived by an old snapshot.
func Authorize(ctx context.Context, q DB, parentRun, parentAgent, target uuid.UUID, ref Reference, secret string) error {
	var owner uuid.UUID
	err := q.QueryRow(ctx, `SELECT s.owner_user_id FROM run_mcp_binding_snapshots s
 JOIN agent_mcp_bindings b ON b.binding_id=s.binding_id AND b.agent_id=s.agent_id AND b.target_agent_id=s.target_agent_id AND b.digest=s.digest AND b.owner_user_id=s.owner_user_id
 JOIN agents a ON a.id=b.agent_id AND a.creator_id=b.owner_user_id
 WHERE s.run_id=$1 AND s.agent_id=$2 AND s.target_agent_id=$3 AND s.binding_id=$4 AND s.digest=$5 FOR SHARE OF b,a`, parentRun, parentAgent, target, ref.BindingID, ref.Digest).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return denied()
	}
	if err != nil {
		return err
	}
	t, err := Resolve(ctx, q, owner, target, secret)
	if err != nil {
		return err
	}
	if t.Digest != ref.Digest {
		return denied()
	}
	return nil
}

func Snapshot(ctx context.Context, tx pgx.Tx, run, agent uuid.UUID, secret string) error {
	// Serialize with owner edits so a Run receives one coherent selection.
	if _, err := tx.Exec(ctx, `SELECT id FROM agents WHERE id=$1 FOR SHARE`, agent); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO run_mcp_binding_snapshots(run_id,binding_id,agent_id,target_agent_id,owner_user_id,digest,tool)
 SELECT $1,binding_id,agent_id,target_agent_id,owner_user_id,digest,tool FROM agent_mcp_bindings WHERE agent_id=$2`, run, agent)
	if err != nil {
		return err
	}
	var count int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM run_mcp_binding_snapshots WHERE run_id=$1`, run).Scan(&count); err != nil || count == 0 {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT binding_id,target_agent_id,digest FROM run_mcp_binding_snapshots WHERE run_id=$1`, run)
	if err != nil {
		return err
	}
	type grant struct {
		BindingID, TargetID uuid.UUID
		Digest              string
	}
	grants, err := pgx.CollectRows(rows, pgx.RowToStructByPos[grant])
	if err != nil {
		return err
	}
	for _, g := range grants {
		if err = Authorize(ctx, tx, run, agent, g.TargetID, Reference{BindingID: g.BindingID, Digest: g.Digest}, secret); err != nil {
			return httpx.NewError(http.StatusServiceUnavailable, httpx.CodeServiceUnavailable, "Agent execution environment is temporarily unavailable")
		}
	}
	host, err := CheckHost(ctx, tx, agent)
	if err != nil {
		return err
	}
	if host.Known && !host.Compatible {
		return httpx.NewError(http.StatusServiceUnavailable, httpx.CodeServiceUnavailable, "Agent execution environment is temporarily unavailable")
	}
	return nil
}

func ValidateAssignment(value any, features []string) error {
	if value == nil {
		return nil
	}
	raw, err := json.Marshal(value)
	var s SnapshotData
	if err != nil || json.Unmarshal(raw, &s) != nil || s.Schema != 1 || len(s.Tools) > MaxBindings {
		return errors.New("invalid MCP tool snapshot")
	}
	if len(s.Tools) > 0 && !slices.Contains(features, Feature) {
		return errors.New("runtime does not support MCP bindings")
	}
	return nil
}

var ErrInvalidReceipt = errors.New("invalid MCP binding receipt")

func RecordReceipt(ctx context.Context, tx pgx.Tx, run, agent uuid.UUID, event string, payload []byte) error {
	status := ""
	switch event {
	case "run.mcp_bindings.ready":
		status = "ready"
	case "run.mcp_bindings.used":
		status = "used"
	case "run.mcp_bindings.failed":
		status = "failed"
	default:
		return nil
	}
	var r struct {
		Bindings []Reference `json:"bindings"`
	}
	if json.Unmarshal(payload, &r) != nil || len(r.Bindings) == 0 || len(r.Bindings) > MaxBindings {
		return ErrInvalidReceipt
	}
	seen := map[uuid.UUID]bool{}
	for _, ref := range r.Bindings {
		if seen[ref.BindingID] {
			return ErrInvalidReceipt
		}
		seen[ref.BindingID] = true
		var matches bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM run_mcp_binding_snapshots WHERE run_id=$1 AND agent_id=$2 AND binding_id=$3 AND digest=$4)`, run, agent, ref.BindingID, ref.Digest).Scan(&matches); err != nil {
			return err
		}
		if !matches {
			return ErrInvalidReceipt
		}
	}
	for _, ref := range r.Bindings {
		_, err := tx.Exec(ctx, `UPDATE agent_mcp_bindings b SET status=CASE WHEN last_run_id=$3 AND status='used' AND $4='ready' THEN status ELSE $4 END,last_run_id=$3
 WHERE agent_id=$1 AND binding_id=$2 AND digest=$5 AND (last_run_id IS NULL OR last_run_id=$3 OR
 (SELECT started_at FROM runs WHERE id=last_run_id)<=(SELECT started_at FROM runs WHERE id=$3))`, agent, ref.BindingID, run, status, ref.Digest)
		if err != nil {
			return err
		}
	}
	return nil
}
