package registryprojection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const skillCandidates = `SELECT p.id FROM skill_packages p WHERE p.visibility IN ('public','unlisted')
	AND EXISTS (SELECT 1 FROM users u WHERE u.id = p.owner_user_id AND u.deleted_at IS NULL AND u.disabled_at IS NULL)
	AND EXISTS (SELECT 1 FROM skill_package_versions v WHERE v.package_id = p.id AND v.published_at IS NOT NULL)`

const mcpCandidates = `SELECT a.id FROM agents a WHERE ` + serviceEligible

type Inventory struct {
	SkillPackages          int            `json:"skill_packages"`
	SkillVersions          int            `json:"skill_versions"`
	PackagesByVisibility   map[string]int `json:"packages_by_visibility"`
	UnpublishedVersions    int            `json:"unpublished_versions"`
	DisabledOwnerPackages  int            `json:"disabled_owner_packages"`
	BoundVersions          int            `json:"bound_versions"`
	SnapshotVersions       int            `json:"snapshot_versions"`
	CopiedVersions         int            `json:"copied_from_versions"`
	Owners                 int            `json:"owners"`
	OwnersWithoutHandle    int            `json:"owners_without_handle"`
	SPDXDeclared           int            `json:"spdx_declared"`
	MCPServices            int            `json:"mcp_services"`
	MCPMetadata            int            `json:"mcp_metadata"`
	NonPublicMCPAgents     int            `json:"non_public_mcp_agents"`
	ExcludedSubjects       int            `json:"excluded_subjects"`
	ExcludedVersions       int            `json:"excluded_versions"`
	UnknownHistoryVersions int            `json:"unknown_history_versions"`
}

func (p *Projector) Inventory(ctx context.Context) (Inventory, error) {
	inv := Inventory{PackagesByVisibility: map[string]int{}}
	counts := []struct {
		dst *int
		sql string
	}{
		{&inv.SkillPackages, `SELECT count(*) FROM (` + skillCandidates + `) c WHERE NOT EXISTS (SELECT 1 FROM registry_projection_exclusions x WHERE x.kind='skill' AND x.subject_id=c.id AND x.version_id IS NULL)`},
		{&inv.SkillVersions, `SELECT count(*) FROM skill_package_versions v JOIN (` + skillCandidates + `) c ON c.id = v.package_id WHERE v.published_at IS NOT NULL
			AND NOT EXISTS (SELECT 1 FROM registry_projection_exclusions x WHERE x.kind='skill' AND x.subject_id=v.package_id AND (x.version_id IS NULL OR x.version_id = v.id))`},
		{&inv.UnpublishedVersions, `SELECT count(*) FROM skill_package_versions WHERE published_at IS NULL`},
		{&inv.DisabledOwnerPackages, `SELECT count(*) FROM skill_packages p JOIN users u ON u.id = p.owner_user_id WHERE p.visibility <> 'private' AND (u.disabled_at IS NOT NULL OR u.deleted_at IS NOT NULL)`},
		{&inv.BoundVersions, `SELECT count(DISTINCT version_id) FROM agent_skill_package_bindings`},
		{&inv.SnapshotVersions, `SELECT count(DISTINCT version_id) FROM run_skill_package_snapshots`},
		{&inv.CopiedVersions, `SELECT count(*) FROM skill_packages WHERE source_package_id IS NOT NULL`},
		{&inv.Owners, `SELECT count(DISTINCT p.owner_user_id) FROM (` + skillCandidates + `) c JOIN skill_packages p ON p.id = c.id`},
		{&inv.SPDXDeclared, `SELECT count(*) FROM skill_package_versions WHERE published_at IS NOT NULL AND publication_metadata ? 'license'`},
		{&inv.MCPServices, `SELECT count(*) FROM (` + mcpCandidates + `) c`},
		{&inv.MCPMetadata, `SELECT count(*) FROM mcp_service_metadata m JOIN (` + mcpCandidates + `) c ON c.id = m.agent_id`},
		{&inv.NonPublicMCPAgents, `SELECT count(*) FROM agents a WHERE a.connection_mode = 'mcp_server' AND NOT (` + serviceEligible + `)`},
		{&inv.ExcludedSubjects, `SELECT count(*) FROM registry_projection_exclusions WHERE version_id IS NULL`},
		{&inv.ExcludedVersions, `SELECT count(*) FROM registry_projection_exclusions WHERE version_id IS NOT NULL`},
		{&inv.UnknownHistoryVersions, `SELECT count(*) FROM skill_package_versions v
			WHERE v.published_at IS NULL AND (EXISTS (SELECT 1 FROM run_skill_package_snapshots s WHERE s.version_id = v.id)
			OR EXISTS (SELECT 1 FROM agent_skill_package_bindings b WHERE b.version_id = v.id)
			OR EXISTS (SELECT 1 FROM skill_package_versions c WHERE c.source_version_id = v.id))`},
	}
	for _, c := range counts {
		if err := p.pool.QueryRow(ctx, c.sql).Scan(c.dst); err != nil {
			return inv, err
		}
	}
	rows, err := p.pool.Query(ctx, `SELECT visibility, count(*) FROM skill_packages GROUP BY visibility`)
	if err != nil {
		return inv, err
	}
	defer rows.Close()
	for rows.Next() {
		var v string
		var n int
		if err := rows.Scan(&v, &n); err != nil {
			return inv, err
		}
		inv.PackagesByVisibility[v] = n
	}
	inv.OwnersWithoutHandle = inv.Owners
	return inv, rows.Err()
}

type Blocker struct {
	Kind    string `json:"kind"`
	Subject string `json:"subject"`
	Version string `json:"version,omitempty"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Preflight struct {
	Inventory Inventory `json:"inventory"`
	Blockers  []Blocker `json:"blockers"`
}

func (p *Projector) Preflight(ctx context.Context) (Preflight, error) {
	inv, err := p.Inventory(ctx)
	if err != nil {
		return Preflight{}, err
	}
	out := Preflight{Inventory: inv, Blockers: []Blocker{}}
	if _, err := p.call(ctx, http.MethodGet, "/v1/projections/skills/p-preflight-probe", "", nil, uuid.Nil); err != nil {
		var re *remoteError
		if !errors.As(err, &re) || re.Status != http.StatusNotFound {
			out.Blockers = append(out.Blockers, Blocker{Kind: "registry", Subject: p.registry.Name, Code: "target_unusable", Message: err.Error()})
			return out, nil
		}
	}
	rows, err := p.pool.Query(ctx, `SELECT v.package_id, v.id, v.version, v.payload, p.owner_user_id FROM skill_package_versions v
		JOIN (`+skillCandidates+`) c ON c.id = v.package_id JOIN skill_packages p ON p.id = v.package_id
		WHERE v.published_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM registry_projection_exclusions x
			WHERE x.kind='skill' AND x.subject_id=v.package_id AND (x.version_id IS NULL OR x.version_id = v.id))
		ORDER BY v.package_id, v.created_at`)
	if err != nil {
		return out, err
	}
	type candidate struct {
		pkg, version, owner uuid.UUID
		label, payload      string
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.pkg, &c.version, &c.label, &c.payload, &c.owner); err != nil {
			rows.Close()
			return out, err
		}
		candidates = append(candidates, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, err
	}
	owners := map[uuid.UUID]bool{}
	for _, c := range candidates {
		owners[c.owner] = true
		if !validRegistryVersion(c.label) {
			out.Blockers = append(out.Blockers, Blocker{Kind: KindSkill, Subject: c.pkg.String(), Version: c.label, Code: "version_invalid",
				Message: "the version label is not a valid Registry version"})
			continue
		}
		doc, err := p.call(ctx, http.MethodPost, "/v1/projections/validate", bundleType, []byte(c.payload), c.owner)
		if err != nil {
			return out, err
		}
		if ok, _ := doc["ok"].(bool); !ok {
			code, _ := doc["code"].(string)
			msg, _ := doc["message"].(string)
			out.Blockers = append(out.Blockers, Blocker{Kind: KindSkill, Subject: c.pkg.String(), Version: c.label, Code: code, Message: msg})
		}
	}
	mrows, err := p.pool.Query(ctx, mcpCandidates)
	if err != nil {
		return out, err
	}
	for mrows.Next() {
		var id uuid.UUID
		if err := mrows.Scan(&id); err != nil {
			mrows.Close()
			return out, err
		}
		var owner uuid.UUID
		if err := p.pool.QueryRow(ctx, `SELECT creator_id FROM agents WHERE id = $1`, id).Scan(&owner); err != nil {
			mrows.Close()
			return out, err
		}
		owners[owner] = true
	}
	mrows.Close()
	subjects := []string{}
	for o := range owners {
		subjects = append(subjects, o.String())
	}
	slices.Sort(subjects)
	var list []any
	for start := 0; start < len(subjects); start += preflightBatch {
		batch := subjects[start:min(start+preflightBatch, len(subjects))]
		body, _ := json.Marshal(map[string]any{"subjects": batch})
		first, _ := uuid.Parse(batch[0])
		doc, err := p.call(ctx, http.MethodPost, "/v1/projections/namespaces/preflight", "application/json", body, first)
		if err != nil {
			return out, err
		}
		page, _ := doc["namespaces"].([]any)
		list = append(list, page...)
	}
	if len(list) > 0 {
		withHandle := 0
		for _, item := range list {
			m, _ := item.(map[string]any)
			if conflict, _ := m["conflict"].(string); conflict != "" {
				sub, _ := m["sub"].(string)
				out.Blockers = append(out.Blockers, Blocker{Kind: "owner", Subject: sub, Code: conflict, Message: "no Registry namespace can be provisioned for this owner"})
				continue
			}
			if ns, _ := m["namespace"].(string); ns != "" && !generatedNamespace(ns) {
				withHandle++
			}
		}
		out.Inventory.OwnersWithoutHandle = len(list) - withHandle
	}
	return out, nil
}

const preflightBatch = 1000

func generatedNamespace(ns string) bool {
	handle, ok := strings.CutPrefix(ns, "ai.openlinker.u")
	if !ok || (len(handle) != 12 && len(handle) != 16) {
		return false
	}
	for _, r := range handle {
		if !strings.ContainsRune("0123456789abcdef", r) {
			return false
		}
	}
	return true
}

func validRegistryVersion(v string) bool {
	if len(v) == 0 || len(v) > 64 {
		return false
	}
	for i, r := range v {
		alnum := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		if i == 0 && !alnum {
			return false
		}
		if !alnum && r != '.' && r != '_' && r != '-' {
			return false
		}
	}
	return true
}

type Report struct {
	Mode            string   `json:"mode"`
	Complete        bool     `json:"complete"`
	Pending         int      `json:"pending"`
	SubjectErrors   []string `json:"subject_errors"`
	VersionErrors   int      `json:"version_errors"`
	ReconcileMarked int      `json:"reconcile_marked"`
}

func (p *Projector) Cutover(ctx context.Context, limit time.Duration) (Report, error) {
	pre, err := p.Preflight(ctx)
	if err != nil {
		return Report{}, err
	}
	if len(pre.Blockers) > 0 {
		return Report{}, fmt.Errorf("%d preflight blockers remain; exclude or fix them first", len(pre.Blockers))
	}
	err = pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT 1 FROM registry_projection_settings WHERE singleton FOR UPDATE`); err != nil {
			return err
		}
		s, err := loadSettings(ctx, tx)
		if err != nil {
			return err
		}
		if s.Mode != ModeLocal && !p.matches(s) {
			return ErrNotConfigured
		}
		if _, err := tx.Exec(ctx, `UPDATE registry_projection_settings SET mode = 'registry', registry_url = $1, consumer = $2, changed_at = now() WHERE singleton`,
			p.registry.URL, p.registry.Consumer); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT registry_projection_mark(l.kind, l.subject_id, 0::smallint) FROM registry_projection_links l
			WHERE l.attempted AND l.registry_url = $1`, p.registry.URL); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT registry_projection_mark('skill', c.id, 1::smallint) FROM (`+skillCandidates+`) c`); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `SELECT registry_projection_mark('mcp', c.id, 1::smallint) FROM (`+mcpCandidates+`) c`)
		return err
	})
	if err != nil {
		return Report{}, err
	}
	derr := p.Drain(ctx, limit)
	rep, err := p.report(ctx)
	if err != nil {
		return rep, err
	}
	if derr != nil {
		return rep, derr
	}
	return rep, nil
}

func (p *Projector) report(ctx context.Context) (Report, error) {
	s, err := loadSettings(ctx, p.pool)
	if err != nil {
		return Report{}, err
	}
	rep := Report{Mode: s.Mode, SubjectErrors: []string{}}
	if err := p.pool.QueryRow(ctx, `SELECT count(*) FROM registry_projection_dirty`).Scan(&rep.Pending); err != nil {
		return rep, err
	}
	if err := p.pool.QueryRow(ctx, `SELECT count(*) FROM registry_projection_version_errors e WHERE NOT EXISTS (SELECT 1 FROM registry_projection_exclusions x
		WHERE x.kind = 'skill' AND x.subject_id = e.package_id AND (x.version_id IS NULL OR x.version_id = e.version_id))`).Scan(&rep.VersionErrors); err != nil {
		return rep, err
	}
	rows, err := p.pool.Query(ctx, `SELECT kind || ':' || subject_id || ' ' || last_error FROM registry_projection_dirty WHERE last_error <> '' ORDER BY marked_at`)
	if err != nil {
		return rep, err
	}
	defer rows.Close()
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return rep, err
		}
		rep.SubjectErrors = append(rep.SubjectErrors, line)
	}
	rep.Complete = rep.Pending == 0 && rep.VersionErrors == 0
	return rep, rows.Err()
}

type ReconcileReport struct {
	Checked int `json:"checked"`
	Marked  int `json:"marked"`
}

func (p *Projector) Reconcile(ctx context.Context) (ReconcileReport, error) {
	var rep ReconcileReport
	s, err := loadSettings(ctx, p.pool)
	if err != nil {
		return rep, err
	}
	if s.Mode == ModeLocal {
		return rep, nil
	}
	if !p.matches(s) {
		return rep, ErrNotConfigured
	}
	started := time.Now()
	type subject struct {
		kind string
		id   uuid.UUID
	}
	var subjects []subject
	rows, err := p.pool.Query(ctx, `SELECT kind, subject_id FROM registry_projection_links WHERE attempted
		UNION SELECT 'skill', c.id FROM (`+skillCandidates+`) c UNION SELECT 'mcp', c.id FROM (`+mcpCandidates+`) c`)
	if err != nil {
		return rep, err
	}
	for rows.Next() {
		var sj subject
		if err := rows.Scan(&sj.kind, &sj.id); err != nil {
			rows.Close()
			return rep, err
		}
		subjects = append(subjects, sj)
	}
	rows.Close()
	for _, sj := range subjects {
		rep.Checked++
		drift, err := p.drifted(ctx, s, sj.kind, sj.id)
		if err != nil {
			return rep, err
		}
		if drift {
			if _, err := p.pool.Exec(ctx, `SELECT registry_projection_mark($1, $2, 1::smallint)`, sj.kind, sj.id); err != nil {
				return rep, err
			}
			rep.Marked++
		}
	}
	_, err = p.pool.Exec(ctx, `UPDATE registry_projection_settings SET reconciled_at = $1 WHERE singleton`, started)
	return rep, err
}

func (p *Projector) drifted(ctx context.Context, s Settings, kind string, id uuid.UUID) (bool, error) {
	l, err := loadLink(ctx, p.pool, kind, id)
	if err != nil {
		return false, err
	}
	if kind == KindSkill {
		d, err := desiredSkill(ctx, p.pool, id)
		if err != nil {
			return false, err
		}
		if !l.Found || !l.Attempted {
			return d.Visible, nil
		}
		doc, err := p.call(ctx, http.MethodGet, "/v1/projections/skills/"+l.Entry, "", nil, l.Owner)
		var re *remoteError
		if errors.As(err, &re) && re.Status == http.StatusNotFound {
			return d.Visible, nil
		}
		if err != nil {
			return false, err
		}
		failed, err := versionErrors(ctx, p.pool, id)
		if err != nil {
			return false, err
		}
		want := []string{}
		wantDigests := map[string]string{}
		current := ""
		for _, v := range d.Versions {
			if !failed[v.ID] {
				want = append(want, v.Label)
				wantDigests[v.Label] = "sha256:" + v.Digest
				current = v.Label
			}
		}
		got := []string{}
		if list, ok := doc["versions"].([]any); ok {
			for _, x := range list {
				got = append(got, fmt.Sprint(x))
			}
		}
		hidden, _ := doc["hidden"].(bool)
		listed, _ := doc["listed"].(bool)
		wantHidden := !d.Visible || len(want) == 0
		if wantHidden {
			return !hidden, nil
		}
		digests, _ := doc["digests"].(map[string]any)
		for label, digest := range wantDigests {
			if digests[label] != digest {
				return true, nil
			}
		}
		gotCurrent, _ := doc["current_version"].(string)
		slices.Sort(want)
		slices.Sort(got)
		return hidden || listed != d.Listed || !slices.Equal(want, got) || gotCurrent != current || d.Owner != l.Owner, nil
	}
	d, err := desiredMCP(ctx, p.pool, id)
	if err != nil {
		return false, err
	}
	if !l.Found || !l.Attempted {
		return d.Visible, nil
	}
	doc, err := p.call(ctx, http.MethodGet, "/v1/projections/mcp/"+l.Entry, "", nil, l.Owner)
	var re *remoteError
	if errors.As(err, &re) && re.Status == http.StatusNotFound {
		return d.Visible, nil
	}
	if err != nil {
		return false, err
	}
	hidden, _ := doc["hidden"].(bool)
	if hidden == d.Visible || d.Owner != l.Owner {
		return true, nil
	}
	if !d.Visible {
		return false, nil
	}
	_, digest := mcpDocument(p.publicURL, id, d)
	version, _ := doc["version"].(string)
	return digest != l.MCPDigest || version != l.MCPVersion, nil
}

func (p *Projector) Rollback(ctx context.Context, limit time.Duration) (Report, error) {
	s, err := loadSettings(ctx, p.pool)
	if err != nil {
		return Report{}, err
	}
	if s.Mode == ModeLocal {
		return p.report(ctx)
	}
	if !p.matches(s) {
		return Report{}, ErrNotConfigured
	}
	if err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if err := p.lockDestination(ctx, tx, ModeRegistry, ModeDraining); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE registry_projection_settings SET mode = 'draining', changed_at = now() WHERE singleton`)
		return err
	}); err != nil {
		return Report{}, err
	}
	if err := p.Drain(ctx, limit); err != nil {
		rep, _ := p.report(ctx)
		return rep, fmt.Errorf("rollback stays draining: %w", err)
	}
	if _, err := p.Reconcile(ctx); err != nil {
		rep, _ := p.report(ctx)
		return rep, fmt.Errorf("rollback stays draining: %w", err)
	}
	if err := p.Drain(ctx, limit); err != nil {
		rep, _ := p.report(ctx)
		return rep, fmt.Errorf("rollback stays draining: %w", err)
	}
	err = pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if err := p.lockDestination(ctx, tx, ModeDraining); err != nil {
			return err
		}
		var reconciled *time.Time
		if err := tx.QueryRow(ctx, `SELECT reconciled_at FROM registry_projection_settings WHERE singleton`).Scan(&reconciled); err != nil {
			return err
		}
		var pending, claimed, versionErrors int
		var lastMark *time.Time
		if err := tx.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE claim_token IS NOT NULL), max(marked_at) FROM registry_projection_dirty`).
			Scan(&pending, &claimed, &lastMark); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM registry_projection_version_errors e WHERE NOT EXISTS (SELECT 1 FROM registry_projection_exclusions x
			WHERE x.kind = 'skill' AND x.subject_id = e.package_id AND (x.version_id IS NULL OR x.version_id = e.version_id))`).Scan(&versionErrors); err != nil {
			return err
		}
		if pending > 0 || claimed > 0 || versionErrors > 0 || reconciled == nil {
			return fmt.Errorf("rollback stays draining: %d pending, %d claimed, %d version errors", pending, claimed, versionErrors)
		}
		_, err := tx.Exec(ctx, `UPDATE registry_projection_settings SET mode = 'local', changed_at = now() WHERE singleton`)
		return err
	})
	rep, rerr := p.report(ctx)
	if err != nil {
		return rep, err
	}
	return rep, rerr
}

func (p *Projector) lockDestination(ctx context.Context, tx pgx.Tx, modes ...string) error {
	if _, err := tx.Exec(ctx, `SELECT 1 FROM registry_projection_settings WHERE singleton FOR UPDATE`); err != nil {
		return err
	}
	s, err := loadSettings(ctx, tx)
	if err != nil {
		return err
	}
	if !slices.Contains(modes, s.Mode) || !p.matches(s) {
		return fmt.Errorf("the projection mode changed to %s meanwhile: %w", s.Mode, ErrNotConfigured)
	}
	return nil
}

type Status struct {
	Report
	Instance     string     `json:"core_instance"`
	RegistryURL  string     `json:"registry_url"`
	Consumer     string     `json:"consumer"`
	ReconciledAt *time.Time `json:"reconciled_at"`
	Links        int        `json:"links"`
	VersionLinks int        `json:"version_links"`
}

func (p *Projector) Status(ctx context.Context) (Status, error) {
	s, err := loadSettings(ctx, p.pool)
	if err != nil {
		return Status{}, err
	}
	rep, err := p.report(ctx)
	if err != nil {
		return Status{}, err
	}
	st := Status{Report: rep, Instance: s.Instance.String(), RegistryURL: s.RegistryURL, Consumer: s.Consumer, ReconciledAt: s.ReconciledAt}
	if err := p.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM registry_projection_links WHERE attempted), (SELECT count(*) FROM skill_package_registry_links)`).
		Scan(&st.Links, &st.VersionLinks); err != nil {
		return st, err
	}
	return st, nil
}

func (p *Projector) Exclude(ctx context.Context, kind string, subject uuid.UUID, version *uuid.UUID, reason string) error {
	if kind != KindSkill && kind != KindMCP {
		return errors.New("kind must be skill or mcp")
	}
	if strings.TrimSpace(reason) == "" {
		return errors.New("a reason is required")
	}
	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO registry_projection_exclusions (kind, subject_id, version_id, reason) VALUES ($1, $2, $3, $4)
			ON CONFLICT (kind, subject_id, version_id) DO UPDATE SET reason = EXCLUDED.reason`, kind, subject, version, reason); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `SELECT registry_projection_mark($1, $2, 0::smallint)`, kind, subject)
		return err
	})
}

func (p *Projector) Unexclude(ctx context.Context, kind string, subject uuid.UUID, version *uuid.UUID) error {
	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM registry_projection_exclusions WHERE kind = $1 AND subject_id = $2 AND version_id IS NOT DISTINCT FROM $3`,
			kind, subject, version); err != nil {
			return err
		}
		if kind == KindSkill {
			if _, err := tx.Exec(ctx, `DELETE FROM registry_projection_version_errors WHERE package_id = $1 AND ($2::uuid IS NULL OR version_id = $2)`, subject, version); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, `SELECT registry_projection_mark($1, $2, 1::smallint)`, kind, subject)
		return err
	})
}
