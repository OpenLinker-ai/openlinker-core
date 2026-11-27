package registryprojection

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/OpenLinker-ai/openlinker-core/pkg/catalogconfig"
	"github.com/OpenLinker-ai/openlinker-core/pkg/catalogregistry"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	claimLease      = 5 * time.Minute
	bundleType      = "application/vnd.openlinker.skill-bundle.v1+json"
	platformLicense = "LicenseRef-openlinker-platform-terms"
)

var ErrNotConfigured = errors.New("registry projection destination does not match this process")

type Projector struct {
	pool      *pgxpool.Pool
	client    *catalogregistry.Client
	registry  catalogregistry.Registry
	publicURL string
	log       *slog.Logger
}

func New(pool *pgxpool.Pool, client *catalogregistry.Client, registryName, publicURL string) (*Projector, error) {
	if registryName == "" {
		return nil, errors.New("CATALOG_PROJECTION_REGISTRY is not configured")
	}
	if _, err := catalogconfig.Projection(client.Registries(), registryName, publicURL); err != nil {
		return nil, err
	}
	reg, _ := client.Registry(registryName)
	return &Projector{pool: pool, client: client, registry: reg, publicURL: strings.TrimRight(publicURL, "/"), log: slog.Default()}, nil
}

func (p *Projector) Client() *catalogregistry.Client { return p.client }

func (p *Projector) Instance(ctx context.Context) (uuid.UUID, error) {
	s, err := loadSettings(ctx, p.pool)
	return s.Instance, err
}

func (p *Projector) matches(s Settings) bool {
	return s.RegistryURL == p.registry.URL && s.Consumer == p.registry.Consumer
}

type claim struct {
	Kind     string
	Subject  uuid.UUID
	Revision int64
	Token    uuid.UUID
}

const destinationMatches = `EXISTS (SELECT 1 FROM registry_projection_settings st WHERE st.mode <> 'local' AND st.registry_url = $%d AND st.consumer = $%d)`

func (p *Projector) claim(ctx context.Context) (*claim, error) {
	token := uuid.New()
	var c claim
	err := p.pool.QueryRow(ctx, `UPDATE registry_projection_dirty d SET claim_token = $1, claimed_at = clock_timestamp(), attempts = d.attempts + 1
		WHERE (d.kind, d.subject_id) = (SELECT kind, subject_id FROM registry_projection_dirty
			WHERE NOT suspended AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp())
			AND (claim_token IS NULL OR claimed_at < clock_timestamp() - $2::interval)
			ORDER BY priority, marked_at LIMIT 1 FOR UPDATE SKIP LOCKED)
		AND `+fmt.Sprintf(destinationMatches, 3, 4)+`
		RETURNING d.kind, d.subject_id, d.revision`, token, claimLease.String(), p.registry.URL, p.registry.Consumer).Scan(&c.Kind, &c.Subject, &c.Revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	c.Token = token
	return &c, nil
}

func (p *Projector) registerAttempt(ctx context.Context, c *claim, entry string, owner uuid.UUID) (bool, error) {
	ok := false
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		var mode, url, consumer string
		if err := tx.QueryRow(ctx, `SELECT mode, COALESCE(registry_url, ''), COALESCE(consumer, '') FROM registry_projection_settings WHERE singleton FOR SHARE`).
			Scan(&mode, &url, &consumer); err != nil {
			return err
		}
		if mode == ModeLocal || url != p.registry.URL || consumer != p.registry.Consumer {
			return nil
		}
		tag, err := tx.Exec(ctx, `UPDATE registry_projection_dirty SET claimed_at = clock_timestamp()
			WHERE kind = $1 AND subject_id = $2 AND claim_token = $3 AND revision = $4 AND claimed_at > clock_timestamp() - $5::interval`,
			c.Kind, c.Subject, c.Token, c.Revision, claimLease.String())
		if err != nil || tag.RowsAffected() != 1 {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO registry_projection_links (kind, subject_id, registry_url, registry_entry, owner_id, attempted)
			VALUES ($1, $2, $3, $4, $5, true)
			ON CONFLICT (kind, subject_id) DO UPDATE SET attempted = true, registry_url = EXCLUDED.registry_url,
				registry_entry = EXCLUDED.registry_entry, owner_id = EXCLUDED.owner_id`, c.Kind, c.Subject, p.registry.URL, entry, owner); err != nil {
			return err
		}
		ok = true
		return nil
	})
	return ok, err
}

func (p *Projector) ack(ctx context.Context, c *claim) error {
	tag, err := p.pool.Exec(ctx, `DELETE FROM registry_projection_dirty WHERE kind = $1 AND subject_id = $2 AND claim_token = $3 AND revision = $4
		AND `+fmt.Sprintf(destinationMatches, 5, 6), c.Kind, c.Subject, c.Token, c.Revision, p.registry.URL, p.registry.Consumer)
	if err != nil || tag.RowsAffected() == 1 {
		return err
	}
	_, err = p.pool.Exec(ctx, `UPDATE registry_projection_dirty SET claim_token = NULL, claimed_at = NULL WHERE claim_token = $1`, c.Token)
	return err
}

var permanentCodes = map[string]bool{"managed_by_core": true, "name_conflict": true, "projection_namespace": true, "owner_mismatch": true,
	"projection_public_only": true, "projection_not_allowed": true, "name_invalid": true}

func permanent(err error) bool {
	var re *remoteError
	return errors.As(err, &re) && permanentCodes[re.Code]
}

func (p *Projector) release(ctx context.Context, c *claim, cause error) {
	msg := cause.Error()
	if len(msg) > 500 {
		msg = msg[:500]
	}
	ctx = context.WithoutCancel(ctx)
	tag, _ := p.pool.Exec(ctx, `UPDATE registry_projection_dirty SET claim_token = NULL, claimed_at = NULL, last_error = $3, suspended = $4,
		next_attempt_at = clock_timestamp() + LEAST(interval '1 second' * power(2, LEAST(attempts, 12)), interval '1 hour')
		WHERE claim_token = $1 AND revision = $2`, c.Token, c.Revision, msg, permanent(cause))
	if tag.RowsAffected() == 1 && permanent(cause) {
		_, _ = p.pool.Exec(ctx, `UPDATE registry_projection_links SET attempted = false
			WHERE kind = $1 AND subject_id = $2 AND applied_revision = 0 AND registry_namespace = ''`, c.Kind, c.Subject)
	}
	if tag.RowsAffected() == 0 {
		_, _ = p.pool.Exec(ctx, `UPDATE registry_projection_dirty SET claim_token = NULL, claimed_at = NULL WHERE claim_token = $1`, c.Token)
	}
}

func (p *Projector) RunOnce(ctx context.Context) (bool, error) {
	s, err := loadSettings(ctx, p.pool)
	if err != nil {
		return false, err
	}
	if s.Mode == ModeLocal {
		return false, nil
	}
	if !p.matches(s) {
		return false, ErrNotConfigured
	}
	c, err := p.claim(ctx)
	if err != nil || c == nil {
		return false, err
	}
	switch c.Kind {
	case KindSkill:
		err = p.projectSkill(ctx, s, c)
	default:
		err = p.projectMCP(ctx, s, c)
	}
	if err != nil {
		p.release(ctx, c, err)
		return false, err
	}
	return true, p.ack(ctx, c)
}

func (p *Projector) Drain(ctx context.Context, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for {
		processed, err := p.RunOnce(ctx)
		var pending, suspended int
		if qerr := p.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE NOT suspended), count(*) FILTER (WHERE suspended) FROM registry_projection_dirty`).
			Scan(&pending, &suspended); qerr != nil {
			return qerr
		}
		if pending == 0 && suspended > 0 {
			return fmt.Errorf("%d projection subjects failed permanently; see status", suspended)
		}
		if pending == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return err
			}
			return fmt.Errorf("%d projection subjects are still pending", pending)
		}
		if !processed {
			if _, rerr := p.pool.Exec(ctx, `UPDATE registry_projection_dirty SET next_attempt_at = NULL WHERE claim_token IS NULL AND next_attempt_at > clock_timestamp() + interval '2 seconds'`); rerr != nil {
				return rerr
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func (p *Projector) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		reconcile := time.NewTicker(time.Hour)
		defer ticker.Stop()
		defer reconcile.Stop()
		for {
			for {
				processed, err := p.RunOnce(ctx)
				if err != nil && !errors.Is(err, context.Canceled) {
					p.log.Warn("registry projection", "err", err.Error())
				}
				if !processed {
					break
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-reconcile.C:
				if _, err := p.Reconcile(ctx); err != nil && !errors.Is(err, context.Canceled) {
					p.log.Warn("registry reconciliation", "err", err.Error())
				}
			case <-ticker.C:
			}
		}
	}()
}

type remoteError struct {
	Status int
	Code   string
}

func (e *remoteError) Error() string { return fmt.Sprintf("registry answered %d %s", e.Status, e.Code) }

func (p *Projector) call(ctx context.Context, method, target, contentType string, body []byte, owner uuid.UUID) (map[string]any, error) {
	res, err := p.client.Project(ctx, p.registry.Name, method, target, contentType, body, &catalogregistry.User{ID: owner})
	if err != nil {
		return nil, err
	}
	var doc map[string]any
	_ = json.Unmarshal(res.Body, &doc)
	if res.Status >= 200 && res.Status < 300 {
		return doc, nil
	}
	code := ""
	if e, ok := doc["error"].(map[string]any); ok {
		code, _ = e["code"].(string)
	}
	return nil, &remoteError{Status: res.Status, Code: code}
}

func versionRefused(err error) (*remoteError, bool) {
	var re *remoteError
	if errors.As(err, &re) && (re.Status == http.StatusUnprocessableEntity || re.Status == http.StatusConflict ||
		re.Status == http.StatusRequestEntityTooLarge || re.Status == http.StatusUnsupportedMediaType || re.Status == http.StatusBadRequest) {
		return re, true
	}
	return nil, false
}

func (p *Projector) sendSkillState(ctx context.Context, instance uuid.UUID, entry string, owner uuid.UUID, c *claim, phase int, hidden, listed bool, versions []skillVersion) error {
	labels := []string{}
	current, name, description := "", "", ""
	for _, v := range versions {
		labels = append(labels, v.Label)
		current, name, description = v.Label, v.Name, v.Description
	}
	state := "visible"
	if hidden || len(labels) == 0 {
		state, labels, current, name, description = "hidden", []string{}, "", "", ""
	}
	legacy := map[string]string{"core_instance": instance.String(), "package_id": c.Subject.String()}
	if len(versions) > 0 && state == "visible" {
		legacy["version_id"] = versions[len(versions)-1].ID.String()
	}
	body, _ := json.Marshal(map[string]any{"op": uuid.NewString(), "revision": c.Revision, "phase": phase, "owner": map[string]string{"sub": owner.String()},
		"state": state, "listed": listed, "versions": labels, "current_version": current, "display": map[string]string{"name": name, "description": description},
		"legacy": legacy})
	doc, err := p.call(ctx, http.MethodPut, "/v1/projections/skills/"+entry, "application/json", body, owner)
	if err != nil {
		return err
	}
	namespace, _ := doc["namespace"].(string)
	_, err = p.pool.Exec(ctx, `UPDATE registry_projection_links SET applied_revision = $3, applied_phase = $4, applied_hidden = $5,
		registry_namespace = CASE WHEN $6 = '' THEN registry_namespace ELSE $6 END
		WHERE kind = $1 AND subject_id = $2 AND (applied_revision, applied_phase) < ($3, $4)`, KindSkill, c.Subject, c.Revision, phase, state == "hidden", namespace)
	return err
}

func (p *Projector) projectSkill(ctx context.Context, s Settings, c *claim) error {
	d, err := desiredSkill(ctx, p.pool, c.Subject)
	if err != nil {
		return err
	}
	l, err := loadLink(ctx, p.pool, KindSkill, c.Subject)
	if err != nil {
		return err
	}
	if l.Found && l.Attempted && d.Exists && l.Owner != d.Owner {
		ok, err := p.registerAttempt(ctx, c, l.Entry, l.Owner)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("claim lost before the first request")
		}
		if err := p.sendSkillState(ctx, s.Instance, l.Entry, l.Owner, c, 0, true, false, nil); err != nil {
			return err
		}
		if err := p.forgetOwner(ctx, c, l.Owner); err != nil {
			return err
		}
		l = link{}
	}
	owner := d.Owner
	if !d.Exists {
		owner = l.Owner
	}
	if !d.Visible && !(l.Found && l.Attempted) {
		return nil
	}
	entry := entryName("p", s.Instance, c.Subject)
	ok, err := p.registerAttempt(ctx, c, entry, owner)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("claim lost before the first request")
	}
	ingested, err := ingestedVersions(ctx, p.pool, c.Subject, p.registry.URL)
	if err != nil {
		return err
	}
	var retained []skillVersion
	for _, v := range d.Versions {
		if _, ok := ingested[v.ID]; ok {
			retained = append(retained, v)
		}
	}
	if err := p.sendSkillState(ctx, s.Instance, entry, owner, c, 0, !d.Visible, d.Listed, retained); err != nil {
		return err
	}
	if !d.Visible {
		return nil
	}
	failed, err := versionErrors(ctx, p.pool, c.Subject)
	if err != nil {
		return err
	}
	var available []skillVersion
	for _, v := range d.Versions {
		if _, ok := ingested[v.ID]; ok {
			available = append(available, v)
			continue
		}
		if failed[v.ID] {
			continue
		}
		doc, err := p.ingest(ctx, entry, owner, v)
		if re, refused := versionRefused(err); refused {
			if _, err := p.pool.Exec(ctx, `INSERT INTO registry_projection_version_errors (package_id, version_id, code, message) VALUES ($1, $2, $3, $4)
				ON CONFLICT (package_id, version_id) DO UPDATE SET code = EXCLUDED.code, message = EXCLUDED.message, at = now()`,
				c.Subject, v.ID, strings.TrimSpace("registry_"+re.Code), re.Error()); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		digest, _ := doc["digest"].(string)
		if digest != "sha256:"+v.Digest {
			if _, err := p.pool.Exec(ctx, `INSERT INTO registry_projection_version_errors (package_id, version_id, code, message) VALUES ($1, $2, 'digest_mismatch', $3)
				ON CONFLICT (package_id, version_id) DO UPDATE SET code = EXCLUDED.code, message = EXCLUDED.message, at = now()`,
				c.Subject, v.ID, "registry digest "+digest); err != nil {
				return err
			}
			continue
		}
		namespace, _ := doc["namespace"].(string)
		if _, err := p.pool.Exec(ctx, `INSERT INTO skill_package_registry_links (package_id, version_id, registry_url, registry_entry, registry_version, registry_digest)
			VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (package_id, version_id) DO UPDATE SET registry_url = EXCLUDED.registry_url,
				registry_entry = EXCLUDED.registry_entry, registry_version = EXCLUDED.registry_version, registry_digest = EXCLUDED.registry_digest, linked_at = now()`,
			c.Subject, v.ID, p.registry.URL, namespace+"/"+entry, v.Label, digest); err != nil {
			return err
		}
		available = append(available, v)
	}
	return p.sendSkillState(ctx, s.Instance, entry, owner, c, 1, false, d.Listed, available)
}

func (p *Projector) projectMCP(ctx context.Context, s Settings, c *claim) error {
	d, err := desiredMCP(ctx, p.pool, c.Subject)
	if err != nil {
		return err
	}
	l, err := loadLink(ctx, p.pool, KindMCP, c.Subject)
	if err != nil {
		return err
	}
	owner := d.Owner
	if !d.Exists {
		owner = l.Owner
	}
	if l.Found && l.Attempted && d.Exists && l.Owner != d.Owner {
		ok, err := p.registerAttempt(ctx, c, l.Entry, l.Owner)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("claim lost before the first request")
		}
		hide, _ := json.Marshal(map[string]any{"op": uuid.NewString(), "revision": c.Revision, "phase": 0, "owner": map[string]string{"sub": l.Owner.String()},
			"state": "hidden", "listed": false})
		if _, err := p.call(ctx, http.MethodPut, "/v1/projections/mcp/"+l.Entry, "application/json", hide, l.Owner); err != nil {
			return err
		}
		if err := p.forgetOwner(ctx, c, l.Owner); err != nil {
			return err
		}
		l, owner = link{}, d.Owner
	}
	serverJSON, digest := mcpDocument(p.publicURL, c.Subject, d)
	if !d.Visible && !(l.Found && l.Attempted) {
		return nil
	}
	entry := entryName("m", s.Instance, c.Subject)
	ok, err := p.registerAttempt(ctx, c, entry, owner)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("claim lost before the first request")
	}
	version := l.MCPVersion
	if version == "" || digest != l.MCPDigest {
		version = fmt.Sprintf("0.%d.%d", d.MetadataRevision, c.Revision)
	}
	state, phase := "visible", 1
	if !d.Visible {
		state, phase = "hidden", 0
	}
	doc := map[string]any{"op": uuid.NewString(), "revision": c.Revision, "phase": phase, "owner": map[string]string{"sub": owner.String()},
		"state": state, "listed": true, "version": version, "server": json.RawMessage(serverJSON),
		"metadata_revision": d.MetadataRevision, "legacy": map[string]string{"agent_id": c.Subject.String(), "slug": d.Slug}}
	if len(d.Metadata) > 0 {
		doc["metadata"] = d.Metadata
	}
	body, _ := json.Marshal(doc)
	res, err := p.call(ctx, http.MethodPut, "/v1/projections/mcp/"+entry, "application/json", body, owner)
	if err != nil {
		return err
	}
	if applied, _ := res["applied"].(bool); !applied {
		remote, err := p.call(ctx, http.MethodGet, "/v1/projections/mcp/"+entry, "", nil, owner)
		if err != nil {
			return err
		}
		revision, _ := remote["revision"].(float64)
		remotePhase, _ := remote["phase"].(float64)
		remoteVersion, _ := remote["version"].(string)
		if int64(revision) != c.Revision || int(remotePhase) != phase || (state == "visible" && remoteVersion != version) {
			return nil
		}
		res = remote
	}
	namespace, _ := res["namespace"].(string)
	_, err = p.pool.Exec(ctx, `UPDATE registry_projection_links SET applied_revision = $3, applied_phase = $4, applied_hidden = $5,
		mcp_version = CASE WHEN $5 THEN mcp_version ELSE $6 END, mcp_digest = CASE WHEN $5 THEN mcp_digest ELSE $7 END,
		registry_namespace = CASE WHEN $8 = '' THEN registry_namespace ELSE $8 END
		WHERE kind = $1 AND subject_id = $2 AND (applied_revision, applied_phase) < ($3, $4)`, KindMCP, c.Subject, c.Revision, phase, state == "hidden", version, digest, namespace)
	return err
}

func mcpDocument(publicURL string, subject uuid.UUID, d mcpDesired) ([]byte, string) {
	server := map[string]any{"title": d.Name, "description": d.Description,
		"remotes": []map[string]string{{"type": "streamable-http", "url": publicURL + "/mcp/agents/" + subject.String()}}}
	serverJSON, _ := json.Marshal(server)
	sum := sha256.Sum256(append(append([]byte{}, serverJSON...), d.Metadata...))
	return serverJSON, hex.EncodeToString(sum[:])
}

var spdxID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.+-]{0,63}$`)

func (p *Projector) ingest(ctx context.Context, entry string, owner uuid.UUID, v skillVersion) (map[string]any, error) {
	target := "/v1/projections/skills/" + entry + "/versions/" + url.PathEscape(v.Label) + "?license="
	if spdxID.MatchString(v.License) && v.License != "NOASSERTION" && v.License != "NONE" {
		doc, err := p.call(ctx, http.MethodPut, target+url.QueryEscape(v.License), bundleType, []byte(v.Payload), owner)
		var re *remoteError
		if !(errors.As(err, &re) && strings.HasPrefix(re.Code, "license")) {
			return doc, err
		}
	}
	return p.call(ctx, http.MethodPut, target+platformLicense, bundleType, []byte(v.Payload), owner)
}

func (p *Projector) forgetOwner(ctx context.Context, c *claim, previous uuid.UUID) error {
	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `SELECT 1 FROM registry_projection_dirty WHERE kind = $1 AND subject_id = $2 AND claim_token = $3 AND revision = $4
			AND claimed_at > clock_timestamp() - $5::interval FOR UPDATE`, c.Kind, c.Subject, c.Token, c.Revision, claimLease.String())
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return errors.New("claim lost before the ownership change was recorded")
		}
		tag, err = tx.Exec(ctx, `DELETE FROM registry_projection_links WHERE kind = $1 AND subject_id = $2 AND owner_id = $3`, c.Kind, c.Subject, previous)
		if err != nil || tag.RowsAffected() == 0 || c.Kind != KindSkill {
			return err
		}
		_, err = tx.Exec(ctx, `DELETE FROM skill_package_registry_links WHERE package_id = $1`, c.Subject)
		return err
	})
}
