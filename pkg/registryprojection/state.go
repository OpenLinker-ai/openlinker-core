package registryprojection

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	ModeLocal    = "local"
	ModeRegistry = "registry"
	ModeDraining = "draining"
	KindSkill    = "skill"
	KindMCP      = "mcp"
)

type Settings struct {
	Mode         string
	Instance     uuid.UUID
	RegistryURL  string
	Consumer     string
	ReconciledAt *time.Time
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func loadSettings(ctx context.Context, q querier) (Settings, error) {
	var s Settings
	var url, consumer *string
	err := q.QueryRow(ctx, `SELECT mode, core_instance, registry_url, consumer, reconciled_at FROM registry_projection_settings WHERE singleton`).
		Scan(&s.Mode, &s.Instance, &url, &consumer, &s.ReconciledAt)
	if url != nil {
		s.RegistryURL = *url
	}
	if consumer != nil {
		s.Consumer = *consumer
	}
	return s, err
}

func entryName(prefix string, instance, subject uuid.UUID) string {
	return prefix + "-" + strings.ReplaceAll(instance.String(), "-", "")[:8] + "-" + strings.ReplaceAll(subject.String(), "-", "")
}

type skillVersion struct {
	ID          uuid.UUID
	Label       string
	Digest      string
	Payload     string
	License     string
	Name        string
	Description string
}

type skillDesired struct {
	Exists   bool
	Owner    uuid.UUID
	Visible  bool
	Listed   bool
	Versions []skillVersion
}

func desiredSkill(ctx context.Context, q querier, id uuid.UUID) (skillDesired, error) {
	var d skillDesired
	var visibility string
	var active, excluded bool
	err := q.QueryRow(ctx, `SELECT p.owner_user_id, p.visibility,
			EXISTS (SELECT 1 FROM users u WHERE u.id = p.owner_user_id AND u.deleted_at IS NULL AND u.disabled_at IS NULL),
			EXISTS (SELECT 1 FROM registry_projection_exclusions x WHERE x.kind = 'skill' AND x.subject_id = p.id AND x.version_id IS NULL)
		FROM skill_packages p WHERE p.id = $1`, id).Scan(&d.Owner, &visibility, &active, &excluded)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, nil
	}
	if err != nil {
		return d, err
	}
	d.Exists = true
	d.Listed = visibility == "public"
	if (visibility != "public" && visibility != "unlisted") || !active || excluded {
		return d, nil
	}
	rows, err := q.Query(ctx, `SELECT v.id, v.version, v.digest, v.payload, COALESCE(v.publication_metadata->>'license', '') FROM skill_package_versions v
		WHERE v.package_id = $1 AND v.published_at IS NOT NULL
		AND NOT EXISTS (SELECT 1 FROM registry_projection_exclusions x WHERE x.kind = 'skill' AND x.subject_id = v.package_id AND x.version_id = v.id)
		ORDER BY v.created_at, v.id`, id)
	if err != nil {
		return d, err
	}
	defer rows.Close()
	for rows.Next() {
		var v skillVersion
		if err := rows.Scan(&v.ID, &v.Label, &v.Digest, &v.Payload, &v.License); err != nil {
			return d, err
		}
		var meta struct {
			Name        string `json:"name"`
			Description string `json:"description"`
		}
		_ = json.Unmarshal([]byte(v.Payload), &meta)
		v.Name, v.Description = meta.Name, meta.Description
		d.Versions = append(d.Versions, v)
	}
	if err := rows.Err(); err != nil {
		return d, err
	}
	d.Visible = len(d.Versions) > 0
	return d, nil
}

type mcpDesired struct {
	Exists           bool
	Owner            uuid.UUID
	Visible          bool
	Name             string
	Description      string
	Slug             string
	ToolName         string
	Metadata         json.RawMessage
	MetadataRevision int64
}

const serviceEligible = `a.visibility = 'public' AND a.lifecycle_status = 'active' AND a.connection_mode = 'mcp_server'
	AND NOT EXISTS (SELECT 1 FROM unnest(a.tags) AS tag WHERE lower(tag) IN ('internal','test','testing','validation') OR tag IN ('内部','测试','验收'))
	AND EXISTS (SELECT 1 FROM users u WHERE u.id = a.creator_id AND u.deleted_at IS NULL AND u.disabled_at IS NULL)
	AND NOT EXISTS (SELECT 1 FROM registry_projection_exclusions x WHERE x.kind = 'mcp' AND x.subject_id = a.id)`

func desiredMCP(ctx context.Context, q querier, id uuid.UUID) (mcpDesired, error) {
	var d mcpDesired
	var metadata []byte
	var revision *int64
	err := q.QueryRow(ctx, `SELECT a.creator_id, (`+serviceEligible+`), a.name, a.description, a.slug, COALESCE(a.mcp_tool_name, ''),
			m.metadata, m.revision
		FROM agents a LEFT JOIN mcp_service_metadata m ON m.agent_id = a.id WHERE a.id = $1`, id).
		Scan(&d.Owner, &d.Visible, &d.Name, &d.Description, &d.Slug, &d.ToolName, &metadata, &revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, nil
	}
	if err != nil {
		return d, err
	}
	d.Exists = true
	if len(metadata) > 0 {
		d.Metadata = metadata
	}
	if revision != nil {
		d.MetadataRevision = *revision
	}
	return d, nil
}

type link struct {
	Found       bool
	Entry       string
	Owner       uuid.UUID
	Attempted   bool
	Revision    int64
	Phase       int
	Hidden      bool
	MCPVersion  string
	MCPDigest   string
	RegistryURL string
}

func loadLink(ctx context.Context, q querier, kind string, id uuid.UUID) (link, error) {
	var l link
	err := q.QueryRow(ctx, `SELECT registry_entry, owner_id, attempted, applied_revision, applied_phase, applied_hidden, mcp_version, mcp_digest, registry_url
		FROM registry_projection_links WHERE kind = $1 AND subject_id = $2`, kind, id).
		Scan(&l.Entry, &l.Owner, &l.Attempted, &l.Revision, &l.Phase, &l.Hidden, &l.MCPVersion, &l.MCPDigest, &l.RegistryURL)
	if errors.Is(err, pgx.ErrNoRows) {
		return l, nil
	}
	l.Found = err == nil
	return l, err
}

func ingestedVersions(ctx context.Context, q querier, pkg uuid.UUID, registryURL string) (map[uuid.UUID]string, error) {
	rows, err := q.Query(ctx, `SELECT version_id, registry_version FROM skill_package_registry_links WHERE package_id = $1 AND registry_url = $2`, pkg, registryURL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[uuid.UUID]string{}
	for rows.Next() {
		var id uuid.UUID
		var label string
		if err := rows.Scan(&id, &label); err != nil {
			return nil, err
		}
		out[id] = label
	}
	return out, rows.Err()
}

func versionErrors(ctx context.Context, q querier, pkg uuid.UUID) (map[uuid.UUID]bool, error) {
	rows, err := q.Query(ctx, `SELECT version_id FROM registry_projection_version_errors WHERE package_id = $1`, pkg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[uuid.UUID]bool{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}
