package runtime_test

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/OpenLinker-ai/openlinker-core/pkg/skillpackage"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestSkillPackageLifecycle(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute, skillpackage.Feature, "skill_packages.codex.v1")
	agentID := fixture.identity.AgentID
	var creatorID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT creator_id FROM agents WHERE id=$1`, agentID).Scan(&creatorID))
	_, err := pool.Exec(ctx, `UPDATE agents SET connection_mode='runtime',endpoint_url='openlinker-runtime://' || id::text WHERE id=$1`, agentID)
	require.NoError(t, err)
	actor := creatorID
	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	skillpackage.NewHandler(pool).Register(e.Group("/api/v1"), func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error { c.Set(string(httpx.CtxKeyUserID), actor.String()); return next(c) }
	})
	call := func(method, path string, body any, want int) *httptest.ResponseRecorder {
		t.Helper()
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest(method, "/api/v1/creator"+path, bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		require.Equal(t, want, rec.Code, rec.Body.String())
		return rec
	}
	request := skillpackage.ImportRequest{Version: "1.0.0", Providers: []string{"codex"}, CapabilityIDs: []string{"data/analysis"}, Files: map[string]string{"SKILL.md": "---\nname: report\ndescription: Private report instructions\n---\nPRIVATE-INSTRUCTION-CONTENT\n", "references/example.txt": "example"}}
	var imported struct {
		ID        uuid.UUID `json:"id"`
		VersionID uuid.UUID `json:"version_id"`
		Digest    string    `json:"digest"`
	}
	require.NoError(t, json.Unmarshal(call(http.MethodPost, "/skill-packages", request, 201).Body.Bytes(), &imported))
	packagePath := "/skill-packages/" + imported.ID.String()
	call(http.MethodPost, packagePath+"/versions", request, 409)
	bindPath := "/agents/" + agentID.String() + "/skill-packages/" + imported.ID.String()
	body := map[string]any{"version_id": imported.VersionID}
	actor = insertRuntimeUser(t, pool)
	call(http.MethodGet, packagePath, nil, 404)
	call(http.MethodGet, packagePath+"/versions/"+imported.VersionID.String(), nil, 404)
	call(http.MethodPost, packagePath+"/versions", request, 404)
	call(http.MethodPut, bindPath, body, 404)
	call(http.MethodGet, "/agents/"+agentID.String()+"/skill-packages", nil, 404)
	actor = creatorID
	require.NotContains(t, call(http.MethodGet, packagePath, nil, 200).Body.String(), "PRIVATE-INSTRUCTION-CONTENT")
	require.Contains(t, call(http.MethodGet, packagePath+"/versions/"+imported.VersionID.String(), nil, 200).Body.String(), "PRIVATE-INSTRUCTION-CONTENT")
	invalidRequest := request
	invalidRequest.CapabilityIDs = []string{"not/existing"}
	require.Contains(t, call(http.MethodPost, "/skill-packages", invalidRequest, 400).Body.String(), "SKILL_PACKAGE_CAPABILITY_UNKNOWN")
	invalidRequest = request
	invalidRequest.Files = map[string]string{"SKILL.md": "missing frontmatter"}
	require.Contains(t, call(http.MethodPost, "/skill-packages", invalidRequest, 400).Body.String(), "SKILL_PACKAGE_FRONTMATTER_REQUIRED")
	invalidRequest = request
	invalidRequest.Files = map[string]string{"SKILL.md": request.Files["SKILL.md"], "escaped.txt": strings.Repeat("<", 12000)}
	require.Contains(t, call(http.MethodPost, "/skill-packages", invalidRequest, 400).Body.String(), "SKILL_PACKAGE_PAYLOAD_TOO_LARGE")
	bound := call(http.MethodPut, bindPath, body, 200)
	require.Contains(t, bound.Body.String(), `"lifecycle_status":"active"`)
	require.Contains(t, bound.Body.String(), `"max_bindings":5`)
	require.Contains(t, bound.Body.String(), `"host_status":"compatible"`)
	require.Equal(t, "private, no-store", bound.Header().Get("Cache-Control"))
	// A sixth binding returns a stable actionable error and leaves the original intact.
	extras := []string{}
	for i := 0; i < 5; i++ {
		var extra struct{ ID, VersionID uuid.UUID }
		var response map[string]string
		require.NoError(t, json.Unmarshal(call(http.MethodPost, "/skill-packages", request, 201).Body.Bytes(), &response))
		extra.ID = uuid.MustParse(response["id"])
		extra.VersionID = uuid.MustParse(response["version_id"])
		path := "/agents/" + agentID.String() + "/skill-packages/" + extra.ID.String()
		if i < 4 {
			call(http.MethodPut, path, map[string]any{"version_id": extra.VersionID}, 200)
			extras = append(extras, path)
		} else {
			require.Contains(t, call(http.MethodPut, path, map[string]any{"version_id": extra.VersionID}, 400).Body.String(), "SKILL_PACKAGE_BINDING_LIMIT")
		}
	}
	// Re-saving a package at capacity remains permitted.
	call(http.MethodPut, bindPath, body, 200)
	for _, path := range extras {
		call(http.MethodDelete, path, nil, 204)
	}
	var bindingID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT binding_id FROM agent_skill_package_bindings WHERE agent_id=$1`, agentID).Scan(&bindingID))
	// The production creation entry snapshots bindings and drops caller-spoofed contents.
	svc := newTestService(t, pool)
	req := makeRunReq(agentID, map[string]any{"text": "test"})
	req.Metadata = map[string]any{skillpackage.MetadataKey: map[string]any{"payload": "CALLER-SPOOF"}}
	caller := insertRuntimeUser(t, pool)
	run, err := svc.Run(ctx, caller, req, "api")
	require.NoError(t, err)
	var snapshot, metadata []byte
	require.NoError(t, pool.QueryRow(ctx, `SELECT to_jsonb(s),r.request_metadata FROM run_skill_package_snapshots s JOIN runs r ON r.id=s.run_id WHERE s.run_id=$1`, run.RunID).Scan(&snapshot, &metadata))
	require.NotContains(t, string(snapshot), "PRIVATE-INSTRUCTION-CONTENT")
	require.Contains(t, string(snapshot), imported.VersionID.String())
	require.Contains(t, string(snapshot), imported.Digest)
	require.NotContains(t, string(snapshot), "CALLER-SPOOF")
	require.NotContains(t, string(metadata), "PRIVATE-INSTRUCTION-CONTENT")
	require.NotContains(t, string(metadata), "CALLER-SPOOF")
	// The existing Run detail policy allows an Agent owner to read another caller's Run.
	_, err = svc.GetRun(ctx, creatorID, uuid.MustParse(run.RunID))
	require.NoError(t, err)
	_, err = svc.GetRun(ctx, caller, uuid.MustParse(run.RunID))
	require.NoError(t, err)
	_, err = svc.GetRun(ctx, insertRuntimeUser(t, pool), uuid.MustParse(run.RunID))
	var denied *httpx.HTTPError
	require.ErrorAs(t, err, &denied)
	require.Equal(t, 404, denied.Status)
	// Actual assignment SQL delivers private files only to compatible Workers.
	q := db.New(pool)
	_, err = q.LockNextClaimableRuntimeRunForAgent(ctx, db.LockNextClaimableRuntimeRunForAgentParams{AgentID: agentID})
	require.ErrorIs(t, err, pgx.ErrNoRows)
	candidate, err := q.LockNextClaimableRuntimeRunForAgent(ctx, db.LockNextClaimableRuntimeRunForAgentParams{AgentID: agentID, SkillPackageProviders: []string{"codex"}})
	require.NoError(t, err)
	require.Contains(t, string(candidate.RequestMetadata), "PRIVATE-INSTRUCTION-CONTENT")
	// The executing fixture gets an explicit snapshot, then the real fenced event store consumes the receipt.
	require.NoError(t, pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error { return skillpackage.Snapshot(ctx, tx, fixture.identity.RunID, agentID) }))
	receipt := map[string]any{"bindings": []map[string]any{{"binding_id": bindingID.String(), "version_id": imported.VersionID.String(), "digest": imported.Digest}}}
	event := runtime.RuntimeEventRequest{ClientEventID: uuid.New(), ClientEventSeq: 1, EventType: "run.skill_packages.loaded", Payload: receipt}
	store := runtime.NewEventStore(pool)
	_, err = store.Append(ctx, fixture.principal, fixture.identity, event)
	require.NoError(t, err)
	var status string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM agent_skill_package_bindings WHERE agent_id=$1`, agentID).Scan(&status))
	require.Equal(t, "loaded", status)
	ack, err := store.Append(ctx, fixture.principal, fixture.identity, event)
	require.NoError(t, err)
	require.True(t, ack.Replayed)
	// Upgrade creates a new generation; an old Run's valid receipt cannot mark it loaded.
	request.Version = "2.0.0"
	request.Files["SKILL.md"] = strings.Replace(request.Files["SKILL.md"], "name: report", "name: new-report", 1)
	request.Files["references/example.txt"] = "updated"
	var updated struct {
		VersionID uuid.UUID `json:"version_id"`
	}
	require.NoError(t, json.Unmarshal(call(http.MethodPost, packagePath+"/versions", request, 201).Body.Bytes(), &updated))
	var beforeUpgrade struct {
		Items []struct {
			Name            string    `json:"name"`
			LatestVersionID uuid.UUID `json:"latest_version_id"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal(call(http.MethodGet, "/agents/"+agentID.String()+"/skill-packages", nil, 200).Body.Bytes(), &beforeUpgrade))
	require.Len(t, beforeUpgrade.Items, 1)
	require.Equal(t, "report", beforeUpgrade.Items[0].Name)
	require.Equal(t, updated.VersionID, beforeUpgrade.Items[0].LatestVersionID)
	call(http.MethodPut, bindPath, map[string]any{"version_id": updated.VersionID}, 200)
	event.ClientEventID = uuid.New()
	event.ClientEventSeq = 2
	_, err = store.Append(ctx, fixture.principal, fixture.identity, event)
	require.NoError(t, err)
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM agent_skill_package_bindings WHERE agent_id=$1`, agentID).Scan(&status))
	require.Equal(t, "pending", status)
	var preserved []byte
	require.NoError(t, pool.QueryRow(ctx, `SELECT to_jsonb(s) FROM run_skill_package_snapshots s WHERE run_id=$1`, run.RunID).Scan(&preserved))
	require.JSONEq(t, string(snapshot), string(preserved))
	// A forged receipt cannot alter state, even with a valid Runtime identity.
	event.ClientEventID = uuid.New()
	event.ClientEventSeq = 3
	event.Payload = map[string]any{"bindings": []map[string]any{{"binding_id": bindingID.String(), "version_id": updated.VersionID.String(), "digest": imported.Digest}}}
	_, err = store.Append(ctx, fixture.principal, fixture.identity, event)
	require.NoError(t, err) // Invalid optional evidence is ACKed to preserve Worker spool progress.
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM agent_skill_package_bindings WHERE agent_id=$1`, agentID).Scan(&status))
	require.Equal(t, "pending", status)
	event.ClientEventID = uuid.New()
	event.ClientEventSeq = 4
	event.EventType = "run.progress"
	event.Payload = map[string]any{"text": "after invalid receipt"}
	_, err = store.Append(ctx, fixture.principal, fixture.identity, event)
	require.NoError(t, err)
	// A downgraded current host is visible and rejects new Runs immediately.
	_, err = pool.Exec(ctx, `WITH source AS (SELECT * FROM runtime_sessions WHERE agent_id=$1 ORDER BY created_at DESC LIMIT 1),
 new_node AS (INSERT INTO runtime_nodes SELECT (jsonb_populate_record(NULL::runtime_nodes,to_jsonb(n)||jsonb_build_object(
 'node_id',gen_random_uuid(),'device_certificate_serial',gen_random_uuid()::text,'device_public_key_thumbprint',repeat(replace(gen_random_uuid()::text,'-',''),2),
 'features',array_remove(array_remove(n.features,'skill_packages.v1'),'skill_packages.codex.v1'),'inflight',0,'created_at',clock_timestamp()))).* FROM runtime_nodes n JOIN source s USING(node_id) RETURNING *),
 new_session AS (INSERT INTO runtime_sessions SELECT (jsonb_populate_record(NULL::runtime_sessions,to_jsonb(s)||jsonb_build_object(
 'runtime_session_id',gen_random_uuid(),'node_id',n.node_id,'device_certificate_serial',n.device_certificate_serial,'features',n.features,'inflight',0,'created_at',clock_timestamp()))).* FROM source s CROSS JOIN new_node n RETURNING *)
 INSERT INTO runtime_session_attachments(id,runtime_session_id,core_instance_id,attachment_kind)
 SELECT gen_random_uuid(),runtime_session_id,attached_core_instance_id,'connected' FROM new_session`, agentID)
	require.NoError(t, err)
	incompatibleBindings := call(http.MethodGet, "/agents/"+agentID.String()+"/skill-packages", nil, 200)
	require.Contains(t, incompatibleBindings.Body.String(), `"supported":false`)
	require.Contains(t, incompatibleBindings.Body.String(), `"host_status":"incompatible"`)
	_, err = svc.Run(ctx, creatorID, makeRunReq(agentID, map[string]any{"text": "downgraded"}), "api")
	var incompatible *httpx.HTTPError
	require.ErrorAs(t, err, &incompatible)
	require.Equal(t, httpx.CodeServiceUnavailable, incompatible.Code)
	require.Equal(t, http.StatusServiceUnavailable, incompatible.Status)
	require.NotContains(t, strings.ToLower(incompatible.Message), "skill")
	require.Contains(t, call(http.MethodPut, bindPath, map[string]any{"version_id": updated.VersionID}, 400).Body.String(), "SKILL_PACKAGE_HOST_INCOMPATIBLE")
	require.NoError(t, pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE runtime_session_attachments SET detached_at=clock_timestamp(),disconnect_reason='skill_read_test' WHERE runtime_session_id IN (SELECT runtime_session_id FROM runtime_sessions WHERE agent_id=$1) AND detached_at IS NULL`, agentID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE runtime_sessions SET status='closed',disconnected_at=clock_timestamp(),attached_core_instance_id=NULL WHERE agent_id=$1`, agentID)
		return err
	}))
	require.Contains(t, call(http.MethodGet, "/agents/"+agentID.String()+"/skill-packages", nil, 200).Body.String(), `"host_status":"none"`)
	// Disabled Agents retain read/remove access, while new associations stay forbidden.
	_, err = pool.Exec(ctx, `UPDATE agents SET lifecycle_status='disabled' WHERE id=$1`, agentID)
	require.NoError(t, err)
	require.Contains(t, call(http.MethodGet, "/agents/"+agentID.String()+"/skill-packages", nil, 200).Body.String(), `"lifecycle_status":"disabled"`)
	require.Contains(t, call(http.MethodPut, bindPath, map[string]any{"version_id": updated.VersionID}, 400).Body.String(), "SKILL_PACKAGE_AGENT_DISABLED")
	call(http.MethodDelete, bindPath, nil, 204)
	require.NoError(t, pool.QueryRow(ctx, `SELECT to_jsonb(s) FROM run_skill_package_snapshots s WHERE run_id=$1`, run.RunID).Scan(&preserved))
	require.JSONEq(t, string(snapshot), string(preserved))
	// Removing the binding still cannot delete a version pinned by an existing Run.
	_, err = pool.Exec(ctx, `DELETE FROM skill_package_versions WHERE id=$1`, imported.VersionID)
	var foreignKey *pgconn.PgError
	require.ErrorAs(t, err, &foreignKey)
	require.Equal(t, "23503", foreignKey.Code)
}

// StartRun is the asynchronous admission entry that explicitly permits offline queues.
func TestSkillPackageOfflineQueue(t *testing.T) {
	for _, tc := range []struct {
		name, status        string
		compatible, history bool
	}{
		{"no history", "", false, false},
		{"compatible offline", "offline", true, true},
		{"compatible closed", "closed", true, true},
		{"incompatible offline", "offline", false, true},
		{"incompatible closed", "closed", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := setupTestDB(t)
			ctx := context.Background()
			var agentID, creatorID uuid.UUID
			if tc.history {
				features := []string{}
				if tc.compatible {
					features = []string{skillpackage.Feature, "skill_packages.codex.v1"}
				}
				fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute, features...)
				agentID = fixture.identity.AgentID
				require.NoError(t, pool.QueryRow(ctx, `SELECT creator_id FROM agents WHERE id=$1`, agentID).Scan(&creatorID))
				require.NoError(t, pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
					if _, err := tx.Exec(ctx, `UPDATE runtime_session_attachments SET detached_at=clock_timestamp(),disconnect_reason='test_shutdown' WHERE runtime_session_id=$1 AND detached_at IS NULL`, *fixture.identity.RuntimeSessionID); err != nil {
						return err
					}
					_, err := tx.Exec(ctx, `UPDATE runtime_sessions SET status=$2,attached_core_instance_id=NULL,disconnected_at=clock_timestamp() WHERE runtime_session_id=$1`, *fixture.identity.RuntimeSessionID, tc.status)
					return err
				}))
			} else {
				creatorID = insertCreator(t, pool)
				agentID = insertAgent(t, pool, creatorID, "https://example.test/offline", 0, "approved")
			}
			_, err := pool.Exec(ctx, `UPDATE agents SET connection_mode='runtime',endpoint_url='openlinker-runtime://' || id::text WHERE id=$1`, agentID)
			require.NoError(t, err)
			packageID, versionID := uuid.New(), uuid.New()
			bundle, payload, digest, err := skillpackage.ValidateImport(skillpackage.ImportRequest{Version: "1", Providers: []string{"codex"}, Files: map[string]string{"SKILL.md": "---\nname: private\ndescription: private instructions\n---\nPRIVATE CONTENT"}})
			require.NoError(t, err)
			_, err = pool.Exec(ctx, `INSERT INTO skill_packages(id,owner_user_id,name,description) VALUES($1,$2,$3,$4)`, packageID, creatorID, bundle.Name, bundle.Description)
			require.NoError(t, err)
			_, err = pool.Exec(ctx, `INSERT INTO skill_package_versions(id,package_id,version,digest,payload,providers) VALUES($1,$2,'1',$3,$4,ARRAY['codex'])`, versionID, packageID, digest, payload)
			require.NoError(t, err)
			_, err = pool.Exec(ctx, `INSERT INTO agent_skill_package_bindings(agent_id,package_id,version_id,binding_id) VALUES($1,$2,$3,$4)`, agentID, packageID, versionID, uuid.New())
			require.NoError(t, err)
			caller := insertRuntimeUser(t, pool)
			run, err := newTestService(t, pool).StartRun(ctx, caller, makeRunReq(agentID, map[string]any{"text": "queued"}), "api")
			if tc.history && !tc.compatible {
				var unavailable *httpx.HTTPError
				require.ErrorAs(t, err, &unavailable)
				require.Equal(t, 503, unavailable.Status)
				require.Equal(t, httpx.CodeServiceUnavailable, unavailable.Code)
				rec := httptest.NewRecorder()
				require.NoError(t, httpx.SendError(echo.New().NewContext(httptest.NewRequest("GET", "/", nil), rec), err))
				require.NotContains(t, strings.ToLower(rec.Body.String()), "skill")
				require.NotContains(t, rec.Body.String(), packageID.String())
				var count int
				require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM runs WHERE user_id=$1`, caller).Scan(&count))
				require.Zero(t, count)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, run.NextAction)
			var count int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM run_events WHERE run_id=$1 AND event_type='run.dispatch.waiting_runtime' AND payload->>'reason'='runtime_offline'`, run.RunID).Scan(&count))
			require.Equal(t, 1, count)
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM run_skill_package_snapshots WHERE run_id=$1 AND version_id=$2`, run.RunID, versionID).Scan(&count))
			require.Equal(t, 1, count)
			_, err = db.New(pool).LockNextClaimableRuntimeRunForAgent(ctx, db.LockNextClaimableRuntimeRunForAgentParams{AgentID: agentID})
			require.ErrorIs(t, err, pgx.ErrNoRows)
			candidate, err := db.New(pool).LockNextClaimableRuntimeRunForAgent(ctx, db.LockNextClaimableRuntimeRunForAgentParams{AgentID: agentID, SkillPackageProviders: []string{"codex"}})
			require.NoError(t, err)
			require.Equal(t, run.RunID, candidate.ID.String())
		})
	}
}

// Publication, public reads and private-copy import through the real handlers,
// followed by the production Run -> assignment -> receipt path on the copy.
func TestSkillPackagePublicationAndImport(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	fixture := insertEventStoreExecutingAttempt(t, pool, 5*time.Minute, skillpackage.Feature, "skill_packages.codex.v1")
	agentID := fixture.identity.AgentID
	var importer uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT creator_id FROM agents WHERE id=$1`, agentID).Scan(&importer))
	_, err := pool.Exec(ctx, `UPDATE agents SET connection_mode='runtime',endpoint_url='openlinker-runtime://' || id::text WHERE id=$1`, agentID)
	require.NoError(t, err)
	publisher, outsider := insertRuntimeUser(t, pool), insertRuntimeUser(t, pool)
	actor := publisher
	e := echo.New()
	e.HTTPErrorHandler = func(err error, c echo.Context) { _ = httpx.SendError(c, err) }
	handler := skillpackage.NewHandler(pool)
	handler.Register(e.Group("/api/v1"), func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error { c.Set(string(httpx.CtxKeyUserID), actor.String()); return next(c) }
	})
	handler.RegisterPublic(e.Group("/api/v1"))
	serve := func(method, path string, body any, want int) *httptest.ResponseRecorder {
		t.Helper()
		var reader *bytes.Reader
		if body == nil {
			reader = bytes.NewReader(nil)
		} else {
			raw, _ := json.Marshal(body)
			reader = bytes.NewReader(raw)
		}
		req := httptest.NewRequest(method, "/api/v1"+path, reader)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		require.Equal(t, want, rec.Code, "%s %s: %s", method, path, rec.Body.String())
		return rec
	}
	public := func(path string, want int) *httptest.ResponseRecorder {
		t.Helper()
		rec := serve(http.MethodGet, "/skill-packages"+path, nil, want)
		require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
		return rec
	}
	request := skillpackage.ImportRequest{Version: "1.0.0", Providers: []string{"codex"}, CapabilityIDs: []string{"data/analysis"}, RequiredCommands: []string{"git"}, Files: map[string]string{
		"SKILL.md":            "---\nname: release-notes\ndescription: Prepare release notes from changes.\n---\nPUBLISHED-INSTRUCTION-CONTENT\n",
		"references/style.md": "Group entries by feature.\n",
	}}
	var source struct {
		ID        uuid.UUID `json:"id"`
		VersionID uuid.UUID `json:"version_id"`
		Digest    string    `json:"digest"`
	}
	require.NoError(t, json.Unmarshal(serve(http.MethodPost, "/creator/skill-packages", request, 201).Body.Bytes(), &source))
	pkgPath, versionPath := "/"+source.ID.String(), "/"+source.ID.String()+"/versions/"+source.VersionID.String()
	ownerPath := "/creator/skill-packages/" + source.ID.String()
	publication := ownerPath + "/versions/" + source.VersionID.String() + "/publication"
	var owned struct {
		Visibility      string     `json:"visibility"`
		SourcePackageID *uuid.UUID `json:"source_package_id"`
		Versions        []struct {
			ID              uuid.UUID  `json:"id"`
			PublishedAt     *time.Time `json:"published_at"`
			SourceVersionID *uuid.UUID `json:"source_version_id"`
		} `json:"versions"`
	}
	require.NoError(t, json.Unmarshal(serve(http.MethodGet, ownerPath, nil, 200).Body.Bytes(), &owned))
	require.Equal(t, "private", owned.Visibility)
	require.Nil(t, owned.SourcePackageID)
	require.Nil(t, owned.Versions[0].PublishedAt)

	// Private by default: no public read and no import, even of a published version.
	public(pkgPath, 404)
	public(versionPath, 404)
	public(versionPath+"/bundle.json", 404)
	serve(http.MethodPut, publication, map[string]any{}, 200)
	for _, path := range []string{pkgPath, versionPath, versionPath + "/bundle.json", versionPath + "/archive.zip", versionPath + "/files/SKILL.md"} {
		public(path, 404)
	}
	require.Contains(t, serve(http.MethodPatch, ownerPath, map[string]any{"visibility": "listed"}, 400).Body.String(), "SKILL_PACKAGE_VISIBILITY_INVALID")
	actor = outsider
	serve(http.MethodPatch, ownerPath, map[string]any{"visibility": "public"}, 404)
	serve(http.MethodDelete, publication, nil, 404)
	actor = publisher

	// Unlisted: readable by link, absent from the public list.
	serve(http.MethodPatch, ownerPath, map[string]any{"visibility": "unlisted"}, 200)
	var listed struct {
		Items []struct {
			ID   uuid.UUID `json:"id"`
			Name string    `json:"name"`
		} `json:"items"`
		Total int `json:"total"`
		Page  int `json:"page"`
		Size  int `json:"size"`
	}
	require.NoError(t, json.Unmarshal(public("", 200).Body.Bytes(), &listed))
	require.Zero(t, listed.Total)
	require.Contains(t, public(pkgPath, 200).Body.String(), `"visibility": "unlisted"`)
	var version struct {
		ID          uuid.UUID           `json:"id"`
		PackageID   uuid.UUID           `json:"package_id"`
		Digest      string              `json:"digest"`
		PublishedAt *time.Time          `json:"published_at"`
		Visibility  string              `json:"visibility"`
		Contents    skillpackage.Bundle `json:"contents"`
	}
	require.NoError(t, json.Unmarshal(public(versionPath, 200).Body.Bytes(), &version))
	require.Equal(t, source.Digest, version.Digest)
	require.NotNil(t, version.PublishedAt)
	require.Equal(t, "release-notes", version.Contents.Name)
	require.Equal(t, request.Files, version.Contents.Files)

	// bundle.json is the exact stored payload, so its SHA-256 is the digest.
	bundle := public(versionPath+"/bundle.json", 200)
	sum := sha256.Sum256(bundle.Body.Bytes())
	require.Equal(t, source.Digest, hex.EncodeToString(sum[:]))
	require.Contains(t, bundle.Header().Get("Content-Disposition"), "attachment;")
	var stored string
	require.NoError(t, pool.QueryRow(ctx, `SELECT payload FROM skill_package_versions WHERE id=$1`, source.VersionID).Scan(&stored))
	require.Equal(t, stored, bundle.Body.String())
	file := public(versionPath+"/files/references/style.md", 200)
	require.Equal(t, "text/plain; charset=utf-8", file.Header().Get("Content-Type"))
	require.Equal(t, "nosniff", file.Header().Get("X-Content-Type-Options"))
	require.Equal(t, request.Files["references/style.md"], file.Body.String())
	require.Equal(t, request.Files["SKILL.md"], public(versionPath+"/files/SKILL.md", 200).Body.String())
	public(versionPath+"/files/references%2Fstyle.md", 200)
	for _, missing := range []string{"/files/missing.md", "/files/references", "/files/references/../SKILL.md", "/files/./SKILL.md", "/files/"} {
		public(versionPath+missing, 404)
	}

	// Deterministic archive: identical bytes, package files only, under the skill name.
	first, second := public(versionPath+"/archive.zip", 200), public(versionPath+"/archive.zip", 200)
	require.Equal(t, first.Body.Bytes(), second.Body.Bytes())
	require.Equal(t, "application/zip", first.Header().Get("Content-Type"))
	require.Contains(t, first.Header().Get("Content-Disposition"), `filename="release-notes-1.0.0.zip"`)
	archive, err := zip.NewReader(bytes.NewReader(first.Body.Bytes()), int64(first.Body.Len()))
	require.NoError(t, err)
	entries := map[string]string{}
	for _, entry := range archive.File {
		reader, err := entry.Open()
		require.NoError(t, err)
		raw, err := io.ReadAll(reader)
		require.NoError(t, err)
		entries[entry.Name] = string(raw)
	}
	require.Equal(t, map[string]string{"release-notes/SKILL.md": request.Files["SKILL.md"], "release-notes/references/style.md": request.Files["references/style.md"]}, entries)

	// A private draft renames the package row but never changes the public view.
	draft := request
	draft.Version = "2.0.0"
	draft.Files = map[string]string{"SKILL.md": "---\nname: PRIVATE-DRAFT-NAME\ndescription: PRIVATE-DRAFT-DESCRIPTION\n---\nPRIVATE-DRAFT-CONTENT\n"}
	var drafted struct {
		VersionID uuid.UUID `json:"version_id"`
	}
	require.NoError(t, json.Unmarshal(serve(http.MethodPost, ownerPath+"/versions", draft, 201).Body.Bytes(), &drafted))
	draftPath := pkgPath + "/versions/" + drafted.VersionID.String()
	serve(http.MethodPatch, ownerPath, map[string]any{"visibility": "public"}, 200)
	detail := public(pkgPath, 200).Body.String()
	require.Contains(t, detail, `"name": "release-notes"`)
	require.NotContains(t, detail, "PRIVATE-DRAFT")
	require.NotContains(t, detail, drafted.VersionID.String())
	for _, path := range []string{draftPath, draftPath + "/bundle.json", draftPath + "/archive.zip", draftPath + "/files/SKILL.md"} {
		public(path, 404)
	}
	require.NoError(t, json.Unmarshal(public("?q=release&size=1", 200).Body.Bytes(), &listed))
	require.Equal(t, 1, listed.Total)
	require.Equal(t, 1, listed.Size)
	require.Equal(t, "release-notes", listed.Items[0].Name)
	require.NoError(t, json.Unmarshal(public("?q=PRIVATE-DRAFT", 200).Body.Bytes(), &listed))
	require.Zero(t, listed.Total)
	require.NoError(t, json.Unmarshal(public("?q=100%25", 200).Body.Bytes(), &listed))
	require.Zero(t, listed.Total)

	// Import creates the caller's private copy of the exact published bytes.
	actor = importer
	importBody := map[string]any{"source_package_id": source.ID, "source_version_id": source.VersionID, "expected_digest": strings.Repeat("0", 64)}
	require.Contains(t, serve(http.MethodPost, "/creator/skill-packages/imports", importBody, 409).Body.String(), "SKILL_PACKAGE_DIGEST_MISMATCH")
	serve(http.MethodPost, "/creator/skill-packages/imports", map[string]any{"source_package_id": source.ID, "source_version_id": drafted.VersionID, "expected_digest": source.Digest}, 404)
	importBody["expected_digest"] = source.Digest
	var copied struct {
		ID        uuid.UUID `json:"id"`
		VersionID uuid.UUID `json:"version_id"`
		Digest    string    `json:"digest"`
	}
	require.NoError(t, json.Unmarshal(serve(http.MethodPost, "/creator/skill-packages/imports", importBody, 201).Body.Bytes(), &copied))
	require.Equal(t, source.Digest, copied.Digest)
	require.NotEqual(t, source.ID, copied.ID)
	retry := serve(http.MethodPost, "/creator/skill-packages/imports", importBody, 200)
	require.JSONEq(t, `{"id":"`+copied.ID.String()+`","version_id":"`+copied.VersionID.String()+`","digest":"`+source.Digest+`"}`, retry.Body.String())
	var copiedPayload string
	require.NoError(t, pool.QueryRow(ctx, `SELECT payload FROM skill_package_versions WHERE id=$1`, copied.VersionID).Scan(&copiedPayload))
	require.Equal(t, stored, copiedPayload)
	require.NoError(t, json.Unmarshal(serve(http.MethodGet, "/creator/skill-packages/"+copied.ID.String(), nil, 200).Body.Bytes(), &owned))
	require.Equal(t, "private", owned.Visibility)
	require.Equal(t, source.ID, *owned.SourcePackageID)
	require.Equal(t, source.VersionID, *owned.Versions[0].SourceVersionID)
	require.Nil(t, owned.Versions[0].PublishedAt)
	public("/"+copied.ID.String(), 404)
	// Publication does not open cross-owner bindings: only the copy can be bound.
	serve(http.MethodPut, "/creator/agents/"+agentID.String()+"/skill-packages/"+source.ID.String(), map[string]any{"version_id": source.VersionID}, 404)
	serve(http.MethodPut, "/creator/agents/"+agentID.String()+"/skill-packages/"+copied.ID.String(), map[string]any{"version_id": copied.VersionID}, 200)

	// Withdrawal blocks new imports but leaves existing copies usable.
	actor = publisher
	serve(http.MethodDelete, publication, nil, 200)
	public(versionPath, 404)
	public(pkgPath, 404)
	actor = outsider
	serve(http.MethodPost, "/creator/skill-packages/imports", importBody, 404)
	actor = importer
	serve(http.MethodGet, "/creator/skill-packages/"+copied.ID.String()+"/versions/"+copied.VersionID.String(), nil, 200)

	svc := newTestService(t, pool)
	run, err := svc.Run(ctx, insertRuntimeUser(t, pool), makeRunReq(agentID, map[string]any{"text": "copy"}), "api")
	require.NoError(t, err)
	var snapshotVersion uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT version_id FROM run_skill_package_snapshots WHERE run_id=$1`, run.RunID).Scan(&snapshotVersion))
	require.Equal(t, copied.VersionID, snapshotVersion)
	candidate, err := db.New(pool).LockNextClaimableRuntimeRunForAgent(ctx, db.LockNextClaimableRuntimeRunForAgentParams{AgentID: agentID, SkillPackageProviders: []string{"codex"}})
	require.NoError(t, err)
	require.Contains(t, string(candidate.RequestMetadata), "PUBLISHED-INSTRUCTION-CONTENT")
	require.Contains(t, string(candidate.RequestMetadata), source.Digest)
	var bindingID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT binding_id FROM agent_skill_package_bindings WHERE agent_id=$1 AND package_id=$2`, agentID, copied.ID).Scan(&bindingID))
	require.NoError(t, pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error { return skillpackage.Snapshot(ctx, tx, fixture.identity.RunID, agentID) }))
	_, err = runtime.NewEventStore(pool).Append(ctx, fixture.principal, fixture.identity, runtime.RuntimeEventRequest{ClientEventID: uuid.New(), ClientEventSeq: 1, EventType: "run.skill_packages.loaded",
		Payload: map[string]any{"bindings": []map[string]any{{"binding_id": bindingID.String(), "version_id": copied.VersionID.String(), "digest": copied.Digest}}}})
	require.NoError(t, err)
	var status string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM agent_skill_package_bindings WHERE binding_id=$1`, bindingID).Scan(&status))
	require.Equal(t, "loaded", status)

	// Republishing restores reads; a disabled owner hides everything again.
	actor = publisher
	serve(http.MethodPut, publication, nil, 200)
	public(versionPath, 200)
	_, err = pool.Exec(ctx, `UPDATE users SET disabled_at=now() WHERE id=$1`, publisher)
	require.NoError(t, err)
	public(pkgPath, 404)
	public(versionPath+"/bundle.json", 404)
	require.NoError(t, json.Unmarshal(public("", 200).Body.Bytes(), &listed))
	require.Zero(t, listed.Total)
	_, err = pool.Exec(ctx, `UPDATE users SET disabled_at=NULL WHERE id=$1`, publisher)
	require.NoError(t, err)

	// A withdrawal holding the version lock linearizes with a concurrent import.
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `UPDATE skill_package_versions SET published_at=NULL WHERE id=$1`, source.VersionID)
	require.NoError(t, err)
	actor = outsider
	done := make(chan int, 1)
	go func() {
		raw, _ := json.Marshal(importBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/creator/skill-packages/imports", bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		done <- rec.Code
	}()
	select {
	case code := <-done:
		t.Fatalf("import did not wait for the withdrawal lock: %d", code)
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, tx.Commit(ctx))
	require.Equal(t, http.StatusNotFound, <-done)
	var outsiderCopies int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM skill_packages WHERE owner_user_id=$1`, outsider).Scan(&outsiderCopies))
	require.Zero(t, outsiderCopies)
}
