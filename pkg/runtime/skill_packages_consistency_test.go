package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/skillpackage"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

type skillConsistencyFixture struct {
	pool                  *pgxpool.Pool
	agent, owner, session uuid.UUID
	e                     *echo.Echo
}

func newSkillConsistencyFixture(t *testing.T) skillConsistencyFixture {
	t.Helper()
	pool := setupTestDB(t)
	executing := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute, skillpackage.Feature, "skill_packages.codex.v1")
	f := skillConsistencyFixture{pool: pool, agent: executing.identity.AgentID, session: *executing.identity.RuntimeSessionID, e: echo.New()}
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT creator_id FROM agents WHERE id=$1`, f.agent).Scan(&f.owner))
	_, err := pool.Exec(context.Background(), `UPDATE agents SET connection_mode='runtime',endpoint_url='openlinker-runtime://' || id::text WHERE id=$1`, f.agent)
	require.NoError(t, err)
	f.e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	skillpackage.NewHandler(pool).Register(f.e.Group("/api/v1"), func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error { c.Set(string(httpx.CtxKeyUserID), f.owner.String()); return next(c) }
	})
	return f
}

func (f skillConsistencyFixture) request(method, path string, body any) *httptest.ResponseRecorder {
	var raw []byte
	if body != nil {
		raw, _ = json.Marshal(body)
	}
	req := httptest.NewRequest(method, "/api/v1/creator"+path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	f.e.ServeHTTP(rec, req)
	return rec
}

func (f skillConsistencyFixture) call(t *testing.T, method, path string, body any, status int) *httptest.ResponseRecorder {
	t.Helper()
	rec := f.request(method, path, body)
	require.Equal(t, status, rec.Code, rec.Body.String())
	return rec
}

func (f skillConsistencyFixture) version(t *testing.T, packageID uuid.UUID, label string, providers ...string) (uuid.UUID, uuid.UUID) {
	t.Helper()
	path := "/skill-packages"
	if packageID != uuid.Nil {
		path += "/" + packageID.String() + "/versions"
	}
	body := skillpackage.ImportRequest{Version: label, Providers: providers, Files: map[string]string{"SKILL.md": "---\nname: fixture\ndescription: Consistency fixture\n---\nSynthetic instructions"}}
	var result struct {
		ID, VersionID uuid.UUID
	}
	var response map[string]string
	require.NoError(t, json.Unmarshal(f.call(t, http.MethodPost, path, body, 201).Body.Bytes(), &response))
	result.ID, result.VersionID = uuid.MustParse(response["id"]), uuid.MustParse(response["version_id"])
	return result.ID, result.VersionID
}

func (f skillConsistencyFixture) path(packageID uuid.UUID) string {
	path := "/agents/" + f.agent.String() + "/skill-packages"
	if packageID != uuid.Nil {
		path += "/" + packageID.String()
	}
	return path
}

func (f skillConsistencyFixture) binding(t *testing.T, packageID uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	require.NoError(t, f.pool.QueryRow(context.Background(), `SELECT binding_id FROM agent_skill_package_bindings WHERE agent_id=$1 AND package_id=$2`, f.agent, packageID).Scan(&id))
	return id
}

func (f skillConsistencyFixture) addSession(t *testing.T, providers ...string) uuid.UUID {
	t.Helper()
	features := []string{}
	if len(providers) > 0 {
		features = append(features, skillpackage.Feature)
		for _, provider := range providers {
			features = append(features, "skill_packages."+provider+".v1")
		}
	}
	var id uuid.UUID
	err := f.pool.QueryRow(context.Background(), `WITH source AS (SELECT * FROM runtime_sessions WHERE runtime_session_id=$1),
 new_node AS (INSERT INTO runtime_nodes SELECT (jsonb_populate_record(NULL::runtime_nodes,to_jsonb(n)||jsonb_build_object(
 'node_id',gen_random_uuid(),'device_certificate_serial',gen_random_uuid()::text,'device_public_key_thumbprint',repeat(replace(gen_random_uuid()::text,'-',''),2),
 'features',ARRAY(SELECT x FROM unnest(n.features) x WHERE x NOT LIKE 'skill_packages.%') || $2::text[],'inflight',0,'created_at',clock_timestamp()))).* FROM runtime_nodes n JOIN source s USING(node_id) RETURNING *),
 new_session AS (INSERT INTO runtime_sessions SELECT (jsonb_populate_record(NULL::runtime_sessions,to_jsonb(s)||jsonb_build_object(
 'runtime_session_id',gen_random_uuid(),'node_id',n.node_id,'device_certificate_serial',n.device_certificate_serial,'features',n.features,'inflight',0,'created_at',clock_timestamp()))).* FROM source s CROSS JOIN new_node n RETURNING *),
 attachment AS (INSERT INTO runtime_session_attachments(id,runtime_session_id,core_instance_id,attachment_kind)
 SELECT gen_random_uuid(),runtime_session_id,attached_core_instance_id,'connected' FROM new_session)
 SELECT runtime_session_id FROM new_session`, f.session, features).Scan(&id)
	require.NoError(t, err)
	return id
}

func (f skillConsistencyFixture) disconnect(t *testing.T, id uuid.UUID, status string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, pgx.BeginFunc(ctx, f.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE runtime_session_attachments SET detached_at=clock_timestamp(),disconnect_reason='skill_consistency_test' WHERE runtime_session_id=$1 AND detached_at IS NULL`, id); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE runtime_sessions SET status=$2,attached_core_instance_id=NULL,disconnected_at=clock_timestamp() WHERE runtime_session_id=$1`, id, status)
		return err
	}))
}

func TestSkillPackageBindingConcurrency(t *testing.T) {
	f := newSkillConsistencyFixture(t)
	pkg, v1 := f.version(t, uuid.Nil, "1", "codex")
	_, v2 := f.version(t, pkg, "2", "codex")
	_, v3 := f.version(t, pkg, "3", "codex")
	path := f.path(pkg)
	put := func(version uuid.UUID, expected string) map[string]any {
		return map[string]any{"version_id": version, "expected_binding_id": expected}
	}
	f.call(t, http.MethodPut, path, put(v1, ""), 200)
	first := f.binding(t, pkg)
	require.Contains(t, f.call(t, http.MethodPut, path, put(v2, ""), 409).Body.String(), "SKILL_PACKAGE_BINDING_CONFLICT")
	f.call(t, http.MethodPut, path, put(v2, first.String()), 200)
	second := f.binding(t, pkg)
	require.NotEqual(t, first, second)
	f.call(t, http.MethodPut, path, put(v1, first.String()), 409)
	f.call(t, http.MethodDelete, path, map[string]string{"expected_binding_id": first.String()}, 409)
	f.call(t, http.MethodPut, path, put(v1, "invalid"), 400)
	f.call(t, http.MethodPut, path, put(v2, second.String()), 200)
	require.Equal(t, second, f.binding(t, pkg), "same-version save preserves generation")
	f.call(t, http.MethodDelete, path, map[string]string{"expected_binding_id": second.String()}, 204)
	f.call(t, http.MethodPut, path, put(v1, second.String()), 409)
	f.call(t, http.MethodPut, path, put(v1, ""), 200)
	third := f.binding(t, pkg)
	f.call(t, http.MethodDelete, path, map[string]string{"expected_binding_id": second.String()}, 409)

	// Two clients reviewed the same generation: exactly one can replace it.
	results := make(chan int, 2)
	for _, version := range []uuid.UUID{v2, v3} {
		go func() { results <- f.request(http.MethodPut, path, put(version, third.String())).Code }()
	}
	statuses := []int{<-results, <-results}
	slices.Sort(statuses)
	require.Equal(t, []int{200, 409}, statuses)
	require.NotEqual(t, third, f.binding(t, pkg))
}

func TestSkillPackageSessionCompatibility(t *testing.T) {
	t.Run("older compatible session is not hidden by newest incompatible", func(t *testing.T) {
		f := newSkillConsistencyFixture(t)
		f.addSession(t)
		pkg, version := f.version(t, uuid.Nil, "1", "codex")
		f.call(t, http.MethodPut, f.path(pkg), map[string]any{"version_id": version}, 200)
		require.Contains(t, f.call(t, http.MethodGet, f.path(uuid.Nil), nil, 200).Body.String(), `"bindings_compatible":true`)
		_, err := newTestService(t, f.pool).StartRun(context.Background(), f.owner, makeRunReq(f.agent, map[string]any{"text": "compatible"}), "api")
		require.NoError(t, err)
	})
	t.Run("one session must support all packages", func(t *testing.T) {
		f := newSkillConsistencyFixture(t)
		f.addSession(t, "claude")
		codex, codexVersion := f.version(t, uuid.Nil, "1", "codex")
		claude, claudeVersion := f.version(t, uuid.Nil, "1", "claude")
		f.call(t, http.MethodPut, f.path(codex), map[string]any{"version_id": codexVersion}, 200)
		require.Contains(t, f.call(t, http.MethodPut, f.path(claude), map[string]any{"version_id": claudeVersion}, 400).Body.String(), "SKILL_PACKAGE_COMBINATION_INCOMPATIBLE")
		// Simulate a previously saved incompatible combination; admission and
		// the actual dispatch query must also reject the union of two hosts.
		_, err := f.pool.Exec(context.Background(), `INSERT INTO agent_skill_package_bindings(agent_id,package_id,version_id,binding_id) VALUES($1,$2,$3,$4)`, f.agent, claude, claudeVersion, uuid.New())
		require.NoError(t, err)
		state := f.call(t, http.MethodGet, f.path(uuid.Nil), nil, 200)
		require.Contains(t, state.Body.String(), `"bindings_compatible":false`)
		require.Contains(t, state.Body.String(), `"provider_sets":[["claude"],["codex"]]`)
		_, err = newTestService(t, f.pool).StartRun(context.Background(), f.owner, makeRunReq(f.agent, map[string]any{"text": "mixed"}), "api")
		var unavailable *httpx.HTTPError
		require.ErrorAs(t, err, &unavailable)
		require.Equal(t, 503, unavailable.Status)
		require.NotContains(t, unavailable.Message, "skill")
		// A real session advertising both providers makes the whole set eligible.
		f.addSession(t, "codex", "claude")
		run, err := newTestService(t, f.pool).StartRun(context.Background(), f.owner, makeRunReq(f.agent, map[string]any{"text": "whole set"}), "api")
		require.NoError(t, err)
		q := db.New(f.pool)
		for _, provider := range []string{"codex", "claude"} {
			_, err = q.LockNextClaimableRuntimeRunForAgent(context.Background(), db.LockNextClaimableRuntimeRunForAgentParams{AgentID: f.agent, SkillPackageProviders: []string{provider}})
			require.ErrorIs(t, err, pgx.ErrNoRows)
		}
		candidate, err := q.LockNextClaimableRuntimeRunForAgent(context.Background(), db.LockNextClaimableRuntimeRunForAgentParams{AgentID: f.agent, SkillPackageProviders: []string{"codex", "claude"}})
		require.NoError(t, err)
		require.Equal(t, run.RunID, candidate.ID.String())
	})
	t.Run("revoked latest session is excluded", func(t *testing.T) {
		f := newSkillConsistencyFixture(t)
		f.disconnect(t, f.addSession(t), "revoked")
		pkg, version := f.version(t, uuid.Nil, "1", "codex")
		f.call(t, http.MethodPut, f.path(pkg), map[string]any{"version_id": version}, 200)
		_, err := newTestService(t, f.pool).StartRun(context.Background(), f.owner, makeRunReq(f.agent, map[string]any{"text": "valid session"}), "api")
		require.NoError(t, err)
	})
	t.Run("closed compatible history cannot override current downgrade", func(t *testing.T) {
		f := newSkillConsistencyFixture(t)
		f.addSession(t)
		f.disconnect(t, f.session, "closed")
		pkg, version := f.version(t, uuid.Nil, "1", "codex")
		f.call(t, http.MethodPut, f.path(pkg), map[string]any{"version_id": version}, 400)
	})
	t.Run("offline compatible sessions are explicit and preserve queuing", func(t *testing.T) {
		f := newSkillConsistencyFixture(t)
		f.disconnect(t, f.session, "offline")
		pkg, version := f.version(t, uuid.Nil, "1", "codex")
		state := f.call(t, http.MethodPut, f.path(pkg), map[string]any{"version_id": version}, 200)
		require.Contains(t, state.Body.String(), `"compatible_session_online":false`)
		require.Contains(t, state.Body.String(), `"bindings_compatible":true`)
		_, err := newTestService(t, f.pool).StartRun(context.Background(), f.owner, makeRunReq(f.agent, map[string]any{"text": "queue"}), "api")
		require.NoError(t, err)
	})
}
