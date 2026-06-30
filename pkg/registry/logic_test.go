package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/labstack/echo/v4"
	"golang.org/x/crypto/bcrypt"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestRegistryURLScopeAndCredentialHelpers(t *testing.T) {
	if got, err := normalizeBaseURL("  http://127.0.0.1:3000/node  "); err != nil || got == nil || *got != "http://127.0.0.1:3000/node" {
		t.Fatalf("normalizeBaseURL valid = %#v, %v", got, err)
	}
	if got, err := normalizeBaseURL(" "); err != nil || got != nil {
		t.Fatalf("normalizeBaseURL empty = %#v, %v", got, err)
	}
	for _, raw := range []string{"not-url", "ftp://example.com/node", "http://" + strings.Repeat("a", 501) + ".com"} {
		if _, err := normalizeBaseURL(raw); err == nil {
			t.Fatalf("normalizeBaseURL(%q) should fail", raw)
		}
	}

	if got, err := normalizeRemoteAPIBaseURL("https://peer.example/root/?x=1#frag"); err != nil || got != "https://peer.example/root/api/v1" {
		t.Fatalf("normalizeRemoteAPIBaseURL root = %q, %v", got, err)
	}
	if got, err := normalizeRemoteAPIBaseURL("https://peer.example/api/v1/"); err != nil || got != "https://peer.example/api/v1" {
		t.Fatalf("normalizeRemoteAPIBaseURL api = %q, %v", got, err)
	}
	if got, err := normalizeFederationExchangeURL("https://peer.example/api/v1/registry-peers/federation-invitations/exchange?token=secret#frag"); err != nil || got != "https://peer.example/api/v1/registry-peers/federation-invitations/exchange" {
		t.Fatalf("normalizeFederationExchangeURL = %q, %v", got, err)
	}
	if _, err := normalizeRemoteAPIBaseURL("https://peer.example/" + strings.Repeat("a", 501)); err == nil {
		t.Fatalf("too long remote api base URL should fail")
	}
	if _, err := normalizeFederationExchangeURL("https://peer.example/" + strings.Repeat("a", 601)); err == nil {
		t.Fatalf("too long federation exchange URL should fail")
	}
	for _, raw := range []string{"", "mailto:test@example.com", "://bad"} {
		if _, err := normalizeRemoteAPIBaseURL(raw); err == nil {
			t.Fatalf("normalizeRemoteAPIBaseURL(%q) should fail", raw)
		}
		if _, err := normalizeFederationExchangeURL(raw); err == nil {
			t.Fatalf("normalizeFederationExchangeURL(%q) should fail", raw)
		}
	}

	scopes, err := normalizeScopes(nil)
	if err != nil || !reflect.DeepEqual(scopes, defaultNodeScopes) {
		t.Fatalf("default scopes = %#v, %v", scopes, err)
	}
	scopes, err = normalizeScopes([]string{"proxy:pull", " proxy:pull ", "listing:sync"})
	if err != nil || !reflect.DeepEqual(scopes, []string{"heartbeat", "proxy:pull", "listing:sync"}) {
		t.Fatalf("dedup scopes = %#v, %v", scopes, err)
	}
	if _, err := normalizeScopes([]string{"unknown"}); err == nil {
		t.Fatalf("unknown scope should fail")
	}
	if !hasScope(scopes, "proxy:pull") || hasScope(scopes, "proxy:result") {
		t.Fatalf("hasScope failed for %#v", scopes)
	}

	for _, policy := range []string{payloadPolicyMetadataOnly, payloadPolicyStoreRunSummary, payloadPolicyStoreFullPayload} {
		if !validPayloadPolicy(policy) {
			t.Fatalf("expected valid policy %q", policy)
		}
	}
	if validPayloadPolicy("store_everything") {
		t.Fatalf("unexpected valid payload policy")
	}

	token := " bearer-token "
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	if got := registryCredentialHint(token); got != "sha256:"+hex.EncodeToString(sum[:])[:12] {
		t.Fatalf("registryCredentialHint = %q", got)
	}
	nodeSecret, nodePrefix, err := generateNodeSecret()
	if err != nil || !strings.HasPrefix(nodeSecret, nodeSecretPrefix) || len(nodeSecret) != len(nodeSecretPrefix)+nodeSecretRandomSize*2 || nodePrefix != nodeSecret[:nodeSecretPrefixLen] {
		t.Fatalf("generateNodeSecret = %q %q %v", nodeSecret, nodePrefix, err)
	}
	fedToken, fedPrefix, err := generateFederationToken()
	if err != nil || !strings.HasPrefix(fedToken, federationTokenPrefix) || len(fedToken) != len(federationTokenPrefix)+federationTokenRandomSize*2 || fedPrefix != fedToken[:federationTokenPrefixLen] {
		t.Fatalf("generateFederationToken = %q %q %v", fedToken, fedPrefix, err)
	}

	svc := &Service{}
	if _, err := svc.verifyNodeSecret(context.Background(), "bad", "heartbeat"); err == nil {
		t.Fatalf("invalid node secret should fail before database lookup")
	} else {
		requireRegistryHTTPStatus(t, err, http.StatusUnauthorized)
	}
	remoteRoot, token, peerID, routeMode, err := svc.resolveRemoteRegistryCredentials(context.Background(), uuid.New(), &CreateRemoteProxyRunRequest{
		RemoteAPIBaseURL:  " https://remote.example/root?x=1#frag ",
		RemoteBearerToken: "  bearer-token-123  ",
	})
	if err != nil || remoteRoot != "https://remote.example/root/api/v1" || token != "bearer-token-123" || peerID != "" || routeMode != "explicit" {
		t.Fatalf("resolve explicit credentials = %q %q %q %q %v", remoteRoot, token, peerID, routeMode, err)
	}
	if _, _, _, _, err := svc.resolveRemoteRegistryCredentials(context.Background(), uuid.New(), &CreateRemoteProxyRunRequest{
		RemoteAPIBaseURL:  "https://remote.example",
		RemoteBearerToken: "short",
	}); err == nil {
		t.Fatalf("short remote bearer token should fail")
	} else {
		requireRegistryHTTPStatus(t, err, http.StatusUnprocessableEntity)
	}
	if _, _, _, _, err := svc.resolveRemoteRegistryCredentials(context.Background(), uuid.New(), &CreateRemoteProxyRunRequest{
		RegistryPeerID:    uuid.NewString(),
		RemoteAPIBaseURL:  "https://remote.example",
		RemoteBearerToken: "bearer-token-123",
	}); err == nil {
		t.Fatalf("mixed registry peer and explicit credentials should fail")
	} else {
		requireRegistryHTTPStatus(t, err, http.StatusUnprocessableEntity)
	}
	if _, _, _, _, err := svc.resolveRemoteRegistryCredentials(context.Background(), uuid.New(), &CreateRemoteProxyRunRequest{}); err == nil {
		t.Fatalf("auto registry peer without storage should fail")
	} else {
		requireRegistryHTTPStatus(t, err, http.StatusInternalServerError)
	}
	if _, _, _, _, err := svc.resolveRemoteRegistryCredentials(context.Background(), uuid.New(), &CreateRemoteProxyRunRequest{RegistryPeerID: uuid.NewString()}); err == nil {
		t.Fatalf("registry peer lookup without storage should fail")
	} else {
		requireRegistryHTTPStatus(t, err, http.StatusInternalServerError)
	}
}

func TestRegistryPayloadPolicyAndArtifactHelpers(t *testing.T) {
	keys, err := normalizePayloadRedactionKeys([]string{" Secret ", "secret", "token"})
	if err != nil || !reflect.DeepEqual(keys, []string{"Secret", "token"}) {
		t.Fatalf("normalizePayloadRedactionKeys = %#v, %v", keys, err)
	}
	if _, err := normalizePayloadRedactionKeys([]string{""}); err == nil {
		t.Fatalf("empty redaction key should fail")
	}
	if _, err := normalizePayloadRedactionKeys(make([]string, 21)); err == nil {
		t.Fatalf("too many redaction keys should fail")
	}
	if _, err := normalizePayloadRedactionKeys([]string{strings.Repeat("x", 81)}); err == nil {
		t.Fatalf("too long redaction key should fail")
	}

	raw := []byte(`{"secret":"top","nested":{"token":"abc"},"items":[{"secret":"inner"}],"safe":1}`)
	redacted := redactPayload(raw, []string{"secret", "TOKEN"})
	var body map[string]interface{}
	if err := json.Unmarshal(redacted, &body); err != nil {
		t.Fatalf("redacted JSON invalid: %v", err)
	}
	if body["secret"] != "[redacted]" || body["nested"].(map[string]interface{})["token"] != "[redacted]" || body["items"].([]interface{})[0].(map[string]interface{})["secret"] != "[redacted]" {
		t.Fatalf("payload was not redacted recursively: %s", redacted)
	}
	if got := string(redactPayload([]byte(`not-json`), []string{"secret"})); got != "not-json" {
		t.Fatalf("invalid JSON payload should be preserved, got %q", got)
	}
	if got := string(redactPayload(raw, nil)); got != string(raw) {
		t.Fatalf("payload without redaction keys should be preserved, got %q", got)
	}

	summary := "summary"
	stored, storedSummary := applyInputPayloadPolicy(payloadPolicyStoreFullPayload, raw, &summary, []string{"secret"})
	if !strings.Contains(string(stored), "[redacted]") || storedSummary == nil || *storedSummary != "summary" {
		t.Fatalf("full input policy = %s %#v", stored, storedSummary)
	}
	stored, storedSummary = applyInputPayloadPolicy(payloadPolicyStoreRunSummary, raw, &summary, nil)
	if string(stored) != "{}" || storedSummary == nil || *storedSummary != "summary" {
		t.Fatalf("summary input policy = %s %#v", stored, storedSummary)
	}
	stored, storedSummary = applyInputPayloadPolicy(payloadPolicyMetadataOnly, raw, &summary, nil)
	if string(stored) != "{}" || storedSummary != nil {
		t.Fatalf("metadata input policy = %s %#v", stored, storedSummary)
	}
	stored, storedSummary = applyOutputPayloadPolicy(payloadPolicyStoreFullPayload, raw, &summary, "success", []string{"token"})
	if !strings.Contains(string(stored), "[redacted]") || storedSummary == nil || *storedSummary != "summary" {
		t.Fatalf("full output policy = %s %#v", stored, storedSummary)
	}
	stored, storedSummary = applyOutputPayloadPolicy(payloadPolicyStoreRunSummary, raw, &summary, "success", nil)
	if string(stored) != "{}" || storedSummary == nil || *storedSummary != "summary" {
		t.Fatalf("summary output policy = %s %#v", stored, storedSummary)
	}
	stored, storedSummary = applyOutputPayloadPolicy(payloadPolicyMetadataOnly, raw, &summary, "failed", nil)
	if string(stored) != "{}" || storedSummary == nil {
		t.Fatalf("failed metadata output should preserve summary: %s %#v", stored, storedSummary)
	}
	stored, storedSummary = applyOutputPayloadPolicy(payloadPolicyMetadataOnly, raw, &summary, "timeout", nil)
	if string(stored) != "{}" || storedSummary == nil {
		t.Fatalf("timeout metadata output should preserve summary: %s %#v", stored, storedSummary)
	}
	stored, storedSummary = applyOutputPayloadPolicy(payloadPolicyMetadataOnly, raw, &summary, "success", nil)
	if string(stored) != "{}" || storedSummary != nil {
		t.Fatalf("success metadata output should drop summary: %s %#v", stored, storedSummary)
	}

	output := []byte(`{
		"artifacts": [
			{"id":"orders","title":"Orders","artifact_type":"file","content":{"rows":2},"file":{"uri":"https://files.example/orders.csv","file_name":"orders.csv","mime_type":"text/csv","sha256":"` + strings.Repeat("a", 64) + `","size":42}},
			{"id":"orders","name":"Duplicate","data":{"ok":true}}
		],
		"artifact": {"name":"Inline","data":{"value":1}}
	}`)
	items := extractProxyRunArtifacts(output, payloadPolicyStoreFullPayload)
	if len(items) != 3 {
		t.Fatalf("extractProxyRunArtifacts len = %d, %#v", len(items), items)
	}
	if items[0].SourceArtifactID != "orders" || items[0].FileURI == nil || *items[0].FileURI != "https://files.example/orders.csv" || items[0].FileSizeBytes == nil || *items[0].FileSizeBytes != 42 {
		t.Fatalf("file artifact draft = %#v", items[0])
	}
	if items[1].SourceArtifactID == "orders" {
		t.Fatalf("duplicate artifact source id was not uniquified: %#v", items[1])
	}
	metadataOnly := extractProxyRunArtifacts(output, payloadPolicyMetadataOnly)
	if len(metadataOnly) != 3 || string(metadataOnly[0].Content) != "{}" {
		t.Fatalf("metadata-only artifacts = %#v", metadataOnly)
	}
	if got := artifactValuesFromOutput(map[string]interface{}{"artifact": map[string]interface{}{"id": "one"}}); len(got) != 1 {
		t.Fatalf("artifactValuesFromOutput single = %#v", got)
	}
	fallbackItems := extractProxyRunArtifacts([]byte(`{"artifacts":[42,{"content":{"ok":true}}]}`), payloadPolicyStoreFullPayload)
	if len(fallbackItems) != 1 || fallbackItems[0].SourceArtifactID != "artifact-2" || fallbackItems[0].Title != "Artifact 2" || fallbackItems[0].ArtifactType != "data" || string(fallbackItems[0].Content) != `{"ok":true}` {
		t.Fatalf("fallback artifacts = %#v", fallbackItems)
	}
	if got := extractProxyRunArtifacts(nil, payloadPolicyStoreFullPayload); got != nil {
		t.Fatalf("empty artifact output = %#v", got)
	}
	if got := extractProxyRunArtifacts([]byte(`not-json`), payloadPolicyStoreFullPayload); got != nil {
		t.Fatalf("invalid artifact output = %#v", got)
	}
	if got := string(artifactContent(map[string]interface{}{"data": map[string]interface{}{"ok": true}})); got != `{"ok":true}` {
		t.Fatalf("artifactContent data = %q", got)
	}
	if got := string(artifactContent(map[string]interface{}{"content": map[string]interface{}{"ok": true}})); got != `{"ok":true}` {
		t.Fatalf("artifactContent content = %q", got)
	}
	if got := string(artifactContent(map[string]interface{}{"content": "raw"})); got != "{}" {
		t.Fatalf("artifactContent fallback = %q", got)
	}

	meta := artifactFileMetadataFromMap(map[string]interface{}{
		"mimeType": "text/plain",
		"file": map[string]interface{}{
			"url":      "https://files.example/a.txt",
			"filename": "a.txt",
			"checksum": strings.Repeat("b", 64),
			"size":     float64(12),
		},
	})
	if meta.MimeType != "text/plain" || meta.FileURI != "https://files.example/a.txt" || meta.FileName != "a.txt" || meta.FileSHA256 != strings.Repeat("b", 64) || meta.FileSizeBytes == nil || *meta.FileSizeBytes != 12 {
		t.Fatalf("artifact metadata = %#v", meta)
	}
	for _, tc := range []struct {
		name string
		raw  map[string]interface{}
		want int64
		ok   bool
	}{
		{name: "int64", raw: map[string]interface{}{"size": int64(7)}, want: 7, ok: true},
		{name: "int", raw: map[string]interface{}{"size": int(8)}, want: 8, ok: true},
		{name: "int32 after negative", raw: map[string]interface{}{"bad": -1, "size": int32(5)}, want: 5, ok: true},
		{name: "float64", raw: map[string]interface{}{"size": float64(12.9)}, want: 12, ok: true},
		{name: "float32", raw: map[string]interface{}{"size": float32(13.1)}, want: 13, ok: true},
		{name: "negative only", raw: map[string]interface{}{"size": int64(-1)}, ok: false},
		{name: "unsupported", raw: map[string]interface{}{"size": "14"}, ok: false},
	} {
		t.Run("firstInt64 "+tc.name, func(t *testing.T) {
			got, ok := firstInt64(tc.raw, "bad", "size")
			if ok != tc.ok || got != tc.want {
				t.Fatalf("firstInt64 = %d %v, want %d %v", got, ok, tc.want, tc.ok)
			}
		})
	}
	if firstString(map[string]interface{}{"name": "  file.txt  "}, "name") != "  file.txt  " {
		t.Fatalf("firstString should return original string")
	}
	if len([]rune(normalizeArtifactText(strings.Repeat("界", 5), 3))) != 3 {
		t.Fatalf("normalizeArtifactText did not truncate by rune")
	}
	if normalizeSHA256(strings.Repeat("C", 64)) != strings.Repeat("c", 64) || normalizeSHA256("not-sha") != "" {
		t.Fatalf("normalizeSHA256 failed")
	}
	if normalizeSHA256(strings.Repeat("g", 64)) != "" {
		t.Fatalf("normalizeSHA256 should reject non-hex strings")
	}
	if stringPtr("") != nil || stringPtr("x") == nil {
		t.Fatalf("stringPtr failed")
	}
}

func TestRegistryDTOAndJSONHelpers(t *testing.T) {
	now := time.Date(2026, 6, 20, 1, 2, 3, 0, time.UTC)
	later := now.Add(time.Minute)
	baseURL := "https://node.example"
	nodeResp := registryNodeToResponse(db.RegistryNode{
		ID:              uuid.New(),
		NodeName:        "Node",
		NodeType:        "bridge_proxy",
		BaseURL:         &baseURL,
		SecretPrefix:    "rn_live_abcd",
		Scopes:          []string{"heartbeat"},
		HeartbeatStatus: "healthy",
		LastHeartbeatAt: &later,
		CreatedAt:       now,
		UpdatedAt:       later,
	})
	if nodeResp.BaseURL != baseURL || nodeResp.LastHeartbeatAt != "2026-06-20T01:03:03Z" || nodeResp.Scopes[0] != "heartbeat" {
		t.Fatalf("registryNodeToResponse = %#v", nodeResp)
	}

	peerResp := registryPeerToResponse(registryPeerRow{
		ID:             uuid.New(),
		Name:           "Peer",
		APIBaseURL:     "https://peer.example/api/v1",
		CredentialHint: "sha256:abc",
		Status:         "active",
		LastUsedAt:     &later,
		CreatedAt:      now,
		UpdatedAt:      later,
	})
	if peerResp.LastUsedAt != "2026-06-20T01:03:03Z" || peerResp.CredentialHint != "sha256:abc" {
		t.Fatalf("registryPeerToResponse = %#v", peerResp)
	}
	inviteResp := registryFederationInviteToResponse(registryFederationInviteRow{
		ID:             uuid.New(),
		Name:           "Invite",
		APIBaseURL:     "https://peer.example/api/v1",
		CredentialHint: "sha256:def",
		Status:         "active",
		TokenPrefix:    "rf_live_abcd",
		ExpiresAt:      later,
		CreatedAt:      now,
		UpdatedAt:      later,
	}, true)
	if inviteResp.ExchangeURL != "https://peer.example/api/v1/registry-peers/federation-invitations/exchange" || inviteResp.TokenPrefix != "rf_live_abcd" {
		t.Fatalf("registryFederationInviteToResponse = %#v", inviteResp)
	}

	link := db.CloudListingLink{
		ID:                       uuid.New(),
		CloudListingID:           uuid.New(),
		RegistryNodeID:           uuid.New(),
		LocalAgentID:             uuid.New(),
		RoutingMode:              "pull_proxy",
		PayloadPolicy:            payloadPolicyMetadataOnly,
		PayloadRedactionKeys:     []string{"secret"},
		SyncStatus:               "linked",
		SyncedAgentSlug:          "synced-slug",
		SyncedAgentName:          "Synced Agent",
		SyncedAgentDescription:   "desc",
		SyncedAgentTags:          []string{"bridge"},
		SyncedAvailabilityStatus: "healthy",
		MetadataSyncedAt:         &now,
		LastSyncAt:               now,
		CreatedAt:                now,
		UpdatedAt:                later,
	}
	linkResp := cloudListingLinkToResponse(link, "Node", "fallback", "Fallback")
	if linkResp.AgentSlug != "synced-slug" || linkResp.AgentName != "Synced Agent" || linkResp.MetadataSyncedAt != "2026-06-20T01:02:03Z" {
		t.Fatalf("cloudListingLinkToResponse = %#v", linkResp)
	}
	metadataErr := "sync failed"
	rowResp := cloudListingRowToResponse(db.ListCloudListingLinksByOwnerRow{
		ID:                   uuid.New(),
		CloudListingID:       uuid.New(),
		RegistryNodeID:       uuid.New(),
		NodeName:             "Row Node",
		LocalAgentID:         uuid.New(),
		AgentSlug:            "row-agent",
		AgentName:            "Row Agent",
		RoutingMode:          "pull_proxy",
		PayloadPolicy:        payloadPolicyStoreRunSummary,
		PayloadRedactionKeys: []string{"token"},
		SyncStatus:           "paused",
		AgentDescription:     "row desc",
		AgentTags:            []string{"row", "bridge"},
		AvailabilityStatus:   "degraded",
		MetadataSyncedAt:     &now,
		MetadataSyncError:    &metadataErr,
		LastSyncAt:           now,
		CreatedAt:            now,
		UpdatedAt:            later,
	})
	if rowResp.NodeName != "Row Node" || rowResp.AgentSlug != "row-agent" || rowResp.MetadataSyncError != metadataErr || rowResp.PayloadRedactionKeys[0] != "token" || rowResp.UpdatedAt != "2026-06-20T01:03:03Z" {
		t.Fatalf("cloudListingRowToResponse = %#v", rowResp)
	}

	inputSummary := "input summary"
	outputSummary := "output summary"
	errorCode := "ERR"
	claimedAt := now.Add(2 * time.Minute)
	proxyResp := proxyRunToResponse(db.ProxyRun{
		ID:                 uuid.New(),
		CloudRunID:         uuid.New(),
		CloudListingLinkID: uuid.New(),
		CloudListingID:     uuid.New(),
		RegistryNodeID:     uuid.New(),
		LocalAgentID:       uuid.New(),
		RequestingUserID:   uuid.New(),
		IdempotencyKey:     "idem",
		Status:             "failed",
		PayloadPolicy:      payloadPolicyStoreFullPayload,
		Input:              []byte(`{"q":"hi"}`),
		InputSummary:       &inputSummary,
		Output:             []byte(`{"ok":false}`),
		OutputSummary:      &outputSummary,
		ErrorCode:          &errorCode,
		ClaimedAt:          &claimedAt,
		AttemptCount:       2,
		MaxAttempts:        3,
		CreatedAt:          now,
		UpdatedAt:          later,
	})
	if proxyResp.Input["q"] != "hi" || proxyResp.Output["ok"] != false || proxyResp.InputSummary != inputSummary || proxyResp.ErrorCode != errorCode || proxyResp.ClaimedAt != "2026-06-20T01:04:03Z" {
		t.Fatalf("proxyRunToResponse = %#v", proxyResp)
	}

	mime := "text/plain"
	fileURI := "https://files.example/a.txt"
	fileName := "a.txt"
	sha := strings.Repeat("a", 64)
	size := int64(12)
	artifactResp := proxyRunArtifactToResponse(db.ProxyRunArtifact{
		ID:               uuid.New(),
		ProxyRunID:       uuid.New(),
		CloudRunID:       uuid.New(),
		SourceArtifactID: "source",
		ArtifactType:     "file",
		Title:            "A",
		Content:          []byte(`{"meta":true}`),
		MimeType:         &mime,
		FileURI:          &fileURI,
		FileName:         &fileName,
		FileSHA256:       &sha,
		FileSizeBytes:    &size,
		CreatedAt:        now,
	})
	if artifactResp.Content["meta"] != true || artifactResp.FileURI != fileURI || artifactResp.FileSizeBytes == nil || *artifactResp.FileSizeBytes != size {
		t.Fatalf("proxyRunArtifactToResponse = %#v", artifactResp)
	}

	raw, err := marshalJSONObj(nil)
	if err != nil || string(raw) != "{}" {
		t.Fatalf("marshalJSONObj nil = %s %v", raw, err)
	}
	if got := jsonObjFromBytes([]byte(`{"x":1}`)); got["x"] != float64(1) {
		t.Fatalf("jsonObjFromBytes valid = %#v", got)
	}
	if got := jsonObjFromBytes(nil); got != nil {
		t.Fatalf("jsonObjFromBytes nil = %#v", got)
	}
	if got := jsonObjFromBytes([]byte(`{}`)); got != nil {
		t.Fatalf("jsonObjFromBytes empty = %#v", got)
	}
	if got := jsonObjFromBytes([]byte(`[]`)); got != nil {
		t.Fatalf("jsonObjFromBytes array = %#v", got)
	}
	if got, err := optionalText("  hello  ", 10, "field"); err != nil || got == nil || *got != "hello" {
		t.Fatalf("optionalText = %#v %v", got, err)
	}
	if got, err := optionalText("   ", 10, "field"); err != nil || got != nil {
		t.Fatalf("optionalText blank = %#v %v", got, err)
	}
	if _, err := optionalText(strings.Repeat("x", 11), 10, "field"); err == nil {
		t.Fatalf("optionalText too long should fail")
	}
	if timePtrString(nil) != "" || stringPtrValue(nil) != "" || stringPtrValue(&inputSummary) != inputSummary {
		t.Fatalf("time/string ptr helpers failed")
	}
}

func TestRegistryHandlersValidateBeforeServiceDispatch(t *testing.T) {
	h := NewHandler(nil)
	userID := uuid.NewString()
	id := uuid.NewString()
	artifactID := uuid.NewString()

	for _, tc := range []struct {
		name   string
		method func(echo.Context) error
		req    *registryHandlerRequest
		want   int
	}{
		{name: "create node missing user", method: h.CreateNode, req: &registryHandlerRequest{method: http.MethodPost, target: "/"}, want: http.StatusUnauthorized},
		{name: "create node invalid json", method: h.CreateNode, req: &registryHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: "{"}, want: http.StatusBadRequest},
		{name: "create node validation", method: h.CreateNode, req: &registryHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: `{}`}, want: http.StatusUnprocessableEntity},
		{name: "revoke invalid id", method: h.RevokeNode, req: &registryHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "rotate invalid id", method: h.RotateNodeSecret, req: &registryHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "heartbeat missing bearer", method: h.Heartbeat, req: &registryHandlerRequest{method: http.MethodPost, target: "/"}, want: http.StatusUnauthorized},
		{name: "sync metadata missing bearer", method: h.SyncNodeMetadata, req: &registryHandlerRequest{method: http.MethodPost, target: "/"}, want: http.StatusUnauthorized},
		{name: "create peer validation", method: h.CreateRegistryPeer, req: &registryHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: `{}`}, want: http.StatusUnprocessableEntity},
		{name: "delete peer invalid id", method: h.DeleteRegistryPeer, req: &registryHandlerRequest{method: http.MethodDelete, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "create invite bearer missing", method: h.CreateRegistryFederationInvite, req: &registryHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: `{"name":"Peer","api_base_url":"https://peer.example/api/v1"}`}, want: http.StatusBadRequest},
		{name: "consume invite validation", method: h.ConsumeRegistryFederationInvite, req: &registryHandlerRequest{method: http.MethodPost, target: "/", body: `{}`}, want: http.StatusUnprocessableEntity},
		{name: "exchange invite validation", method: h.ExchangeRegistryFederationInvite, req: &registryHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: `{}`}, want: http.StatusUnprocessableEntity},
		{name: "create listing validation", method: h.CreateCloudListing, req: &registryHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: `{}`}, want: http.StatusUnprocessableEntity},
		{name: "update listing invalid id", method: h.UpdateCloudListingStatus, req: &registryHandlerRequest{method: http.MethodPatch, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "update listing validation", method: h.UpdateCloudListingStatus, req: &registryHandlerRequest{method: http.MethodPatch, target: "/", userID: userID, body: `{}`, params: map[string]string{"id": id}}, want: http.StatusUnprocessableEntity},
		{name: "sync listing invalid id", method: h.SyncCloudListingMetadata, req: &registryHandlerRequest{method: http.MethodPost, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "create proxy validation", method: h.CreateProxyRun, req: &registryHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: `{}`}, want: http.StatusUnprocessableEntity},
		{name: "create remote proxy validation", method: h.CreateRemoteProxyRun, req: &registryHandlerRequest{method: http.MethodPost, target: "/", userID: userID, body: `{}`}, want: http.StatusUnprocessableEntity},
		{name: "get proxy invalid id", method: h.GetProxyRun, req: &registryHandlerRequest{method: http.MethodGet, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "list artifacts invalid id", method: h.ListProxyRunArtifacts, req: &registryHandlerRequest{method: http.MethodGet, target: "/", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "download artifact invalid artifact", method: h.DownloadProxyRunArtifact, req: &registryHandlerRequest{method: http.MethodGet, target: "/", userID: userID, params: map[string]string{"id": id, "artifactID": "bad"}}, want: http.StatusBadRequest},
		{name: "claim missing bearer", method: h.ClaimProxyRun, req: &registryHandlerRequest{method: http.MethodGet, target: "/"}, want: http.StatusUnauthorized},
		{name: "complete invalid id", method: h.CompleteProxyRun, req: &registryHandlerRequest{method: http.MethodPost, target: "/", headers: map[string]string{echo.HeaderAuthorization: "Bearer secret"}, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "complete invalid json", method: h.CompleteProxyRun, req: &registryHandlerRequest{method: http.MethodPost, target: "/", headers: map[string]string{echo.HeaderAuthorization: "Bearer secret"}, body: "{", params: map[string]string{"id": id}}, want: http.StatusBadRequest},
		{name: "complete validation", method: h.CompleteProxyRun, req: &registryHandlerRequest{method: http.MethodPost, target: "/", headers: map[string]string{echo.HeaderAuthorization: "Bearer secret"}, body: `{}`, params: map[string]string{"id": id}}, want: http.StatusUnprocessableEntity},
		{name: "download run invalid id first", method: h.DownloadProxyRunArtifact, req: &registryHandlerRequest{method: http.MethodGet, target: "/", userID: userID, params: map[string]string{"id": "bad", "artifactID": artifactID}}, want: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newRegistryTestContext(tc.req)
			requireRegistryHTTPStatus(t, tc.method(c), tc.want)
		})
	}

	if got, err := bearerToken(" Bearer rn_live_secret "); err != nil || got != "rn_live_secret" {
		t.Fatalf("bearerToken valid = %q %v", got, err)
	}
	if _, err := bearerToken("Basic nope"); err == nil {
		t.Fatalf("bearerToken invalid should fail")
	}
	if safeDownloadFilename("") != "artifact.bin" || safeDownloadFilename(`bad/"name`+"\n.csv") != "bad__name_.csv" {
		t.Fatalf("safeDownloadFilename failed")
	}
	c := newRegistryTestContext(&registryHandlerRequest{method: http.MethodGet, target: "/", userID: userID})
	if got, err := userIDFromCtx(c); err != nil || got.String() != userID {
		t.Fatalf("userIDFromCtx valid = %s %v", got, err)
	}
	c = newRegistryTestContext(&registryHandlerRequest{method: http.MethodGet, target: "/", userID: "bad"})
	requireRegistryHTTPStatus(t, userIDFromCtxOnly(c), http.StatusUnauthorized)

	e := echo.New()
	api := e.Group("/api/v1")
	noop := func(next echo.HandlerFunc) echo.HandlerFunc { return next }
	h.RegisterProtected(api, noop)
	routes := map[string]bool{}
	for _, route := range e.Routes() {
		routes[route.Method+" "+route.Path] = true
	}
	for _, route := range []string{
		"POST /api/v1/registry-node/link",
		"POST /api/v1/registry-node/heartbeat",
		"POST /api/v1/registry-peers/federation-invitations",
		"POST /api/v1/registry-peers/federation-invitations/exchange",
		"POST /api/v1/registry/listings",
		"POST /api/v1/cloud/listings",
		"GET /api/v1/proxy/runs/claim",
		"GET /api/v1/proxy/runs/:id/artifacts/:artifactID/download",
	} {
		if !routes[route] {
			t.Fatalf("missing route %s", route)
		}
	}
}

func TestRegistryNodeServiceErrorEdges(t *testing.T) {
	ownerID := uuid.New()
	nodeID := uuid.New()
	secret, hashedSecret := registryFakeNodeSecret(t)
	node := registryFakeNodeValues(nodeID, ownerID, hashedSecret, nil)

	createErrSvc := &Service{q: db.New(&registryFakeDBTX{queryRowRows: []registryFakeRow{{err: errors.New("create failed")}}})}
	_, err := createErrSvc.CreateNode(context.Background(), ownerID, &CreateNodeRequest{
		NodeName: "Bridge Node",
		NodeType: "bridge_proxy",
		BaseURL:  "https://node.example",
	})
	requireRegistryHTTPStatus(t, err, http.StatusInternalServerError)

	listErrSvc := &Service{q: db.New(&registryFakeDBTX{queryResults: []registryFakeQueryResult{{err: errors.New("list failed")}}})}
	_, err = listErrSvc.ListNodes(context.Background(), ownerID)
	requireRegistryHTTPStatus(t, err, http.StatusInternalServerError)

	listScanErrSvc := &Service{q: db.New(&registryFakeDBTX{queryResults: []registryFakeQueryResult{{rows: []registryFakeRow{{err: errors.New("scan failed")}}}}})}
	_, err = listScanErrSvc.ListNodes(context.Background(), ownerID)
	requireRegistryHTTPStatus(t, err, http.StatusInternalServerError)

	for _, tc := range []struct {
		name string
		call func(*Service) (*RegistryNodeResponse, error)
		row  registryFakeRow
		want int
	}{
		{
			name: "revoke missing",
			call: func(s *Service) (*RegistryNodeResponse, error) {
				return s.RevokeNode(context.Background(), ownerID, nodeID)
			},
			row:  registryFakeRow{err: pgx.ErrNoRows},
			want: http.StatusNotFound,
		},
		{
			name: "revoke db error",
			call: func(s *Service) (*RegistryNodeResponse, error) {
				return s.RevokeNode(context.Background(), ownerID, nodeID)
			},
			row:  registryFakeRow{err: errors.New("revoke failed")},
			want: http.StatusInternalServerError,
		},
		{
			name: "rotate missing",
			call: func(s *Service) (*RegistryNodeResponse, error) {
				return s.RotateNodeSecret(context.Background(), ownerID, nodeID)
			},
			row:  registryFakeRow{err: pgx.ErrNoRows},
			want: http.StatusNotFound,
		},
		{
			name: "rotate db error",
			call: func(s *Service) (*RegistryNodeResponse, error) {
				return s.RotateNodeSecret(context.Background(), ownerID, nodeID)
			},
			row:  registryFakeRow{err: errors.New("rotate failed")},
			want: http.StatusInternalServerError,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &Service{q: db.New(&registryFakeDBTX{queryRowRows: []registryFakeRow{tc.row}})}
			_, err := tc.call(svc)
			requireRegistryHTTPStatus(t, err, tc.want)
		})
	}

	heartbeatMissingSvc := &Service{q: db.New(&registryFakeDBTX{
		queryResults: []registryFakeQueryResult{{rows: []registryFakeRow{{values: node}}}},
		queryRowRows: []registryFakeRow{{err: pgx.ErrNoRows}},
	})}
	_, err = heartbeatMissingSvc.Heartbeat(context.Background(), secret)
	requireRegistryHTTPStatus(t, err, http.StatusUnauthorized)

	heartbeatErrSvc := &Service{q: db.New(&registryFakeDBTX{
		queryResults: []registryFakeQueryResult{{rows: []registryFakeRow{{values: node}}}},
		queryRowRows: []registryFakeRow{{err: errors.New("heartbeat failed")}},
	})}
	_, err = heartbeatErrSvc.Heartbeat(context.Background(), secret)
	requireRegistryHTTPStatus(t, err, http.StatusInternalServerError)

	heartbeatCountErrSvc := &Service{q: db.New(&registryFakeDBTX{
		queryResults: []registryFakeQueryResult{{rows: []registryFakeRow{{values: node}}}},
		queryRowRows: []registryFakeRow{
			{values: node},
			{err: errors.New("listing count failed")},
			{err: errors.New("pending count failed")},
		},
	})}
	got, err := heartbeatCountErrSvc.Heartbeat(context.Background(), secret)
	if err != nil || got.LinkedListingCount != 0 || got.PendingRunCount != 0 {
		t.Fatalf("Heartbeat count error fallback = %#v, %v", got, err)
	}

	syncErrSvc := &Service{q: db.New(&registryFakeDBTX{
		queryResults: []registryFakeQueryResult{{rows: []registryFakeRow{{values: node}}}},
		queryRowRows: []registryFakeRow{{err: errors.New("sync failed")}},
	})}
	_, err = syncErrSvc.SyncNodeMetadata(context.Background(), secret)
	requireRegistryHTTPStatus(t, err, http.StatusInternalServerError)
}

func TestRegistryListingServiceErrorEdges(t *testing.T) {
	ownerID := uuid.New()
	nodeID := uuid.New()
	agentID := uuid.New()
	listingID := uuid.New()
	linkID := uuid.New()
	revokedAt := time.Date(2026, 6, 22, 9, 45, 0, 0, time.UTC)
	node := registryFakeNodeValues(nodeID, ownerID, "hash", nil)
	revokedNode := registryFakeNodeValues(nodeID, ownerID, "hash", &revokedAt)
	agent := registryFakeAgentValues(agentID, ownerID, "active")

	for _, tc := range []struct {
		name string
		req  *CreateCloudListingRequest
		rows []registryFakeRow
		want int
	}{
		{
			name: "existing listing not found",
			req: &CreateCloudListingRequest{
				RegistryNodeID: nodeID.String(),
				AgentID:        agentID.String(),
				CloudListingID: listingID.String(),
			},
			rows: []registryFakeRow{{err: pgx.ErrNoRows}},
			want: http.StatusNotFound,
		},
		{
			name: "existing listing db error",
			req: &CreateCloudListingRequest{
				RegistryNodeID: nodeID.String(),
				AgentID:        agentID.String(),
				CloudListingID: listingID.String(),
			},
			rows: []registryFakeRow{{err: errors.New("listing lookup failed")}},
			want: http.StatusInternalServerError,
		},
		{
			name: "existing listing belongs to other agent",
			req: &CreateCloudListingRequest{
				RegistryNodeID: nodeID.String(),
				AgentID:        agentID.String(),
				CloudListingID: listingID.String(),
			},
			rows: []registryFakeRow{{values: registryFakeCloudListingValues(linkID, listingID, nodeID, uuid.New())}},
			want: http.StatusConflict,
		},
		{
			name: "node missing",
			req:  &CreateCloudListingRequest{RegistryNodeID: nodeID.String(), AgentID: agentID.String()},
			rows: []registryFakeRow{{err: pgx.ErrNoRows}},
			want: http.StatusNotFound,
		},
		{
			name: "node db error",
			req:  &CreateCloudListingRequest{RegistryNodeID: nodeID.String(), AgentID: agentID.String()},
			rows: []registryFakeRow{{err: errors.New("node lookup failed")}},
			want: http.StatusInternalServerError,
		},
		{
			name: "node revoked",
			req:  &CreateCloudListingRequest{RegistryNodeID: nodeID.String(), AgentID: agentID.String()},
			rows: []registryFakeRow{{values: revokedNode}},
			want: http.StatusConflict,
		},
		{
			name: "agent missing",
			req:  &CreateCloudListingRequest{RegistryNodeID: nodeID.String(), AgentID: agentID.String()},
			rows: []registryFakeRow{{values: node}, {err: pgx.ErrNoRows}},
			want: http.StatusNotFound,
		},
		{
			name: "agent db error",
			req:  &CreateCloudListingRequest{RegistryNodeID: nodeID.String(), AgentID: agentID.String()},
			rows: []registryFakeRow{{values: node}, {err: errors.New("agent lookup failed")}},
			want: http.StatusInternalServerError,
		},
		{
			name: "agent inactive",
			req:  &CreateCloudListingRequest{RegistryNodeID: nodeID.String(), AgentID: agentID.String()},
			rows: []registryFakeRow{{values: node}, {values: registryFakeAgentValues(agentID, ownerID, "disabled")}},
			want: http.StatusConflict,
		},
		{
			name: "upsert fails",
			req:  &CreateCloudListingRequest{RegistryNodeID: nodeID.String(), AgentID: agentID.String()},
			rows: []registryFakeRow{{values: node}, {values: agent}, {err: errors.New("upsert failed")}},
			want: http.StatusInternalServerError,
		},
		{
			name: "metadata sync fails",
			req:  &CreateCloudListingRequest{RegistryNodeID: nodeID.String(), AgentID: agentID.String()},
			rows: []registryFakeRow{
				{values: node},
				{values: agent},
				{values: registryFakeCloudListingValues(linkID, listingID, nodeID, agentID)},
				{err: errors.New("sync failed")},
			},
			want: http.StatusInternalServerError,
		},
		{
			name: "get row fails",
			req:  &CreateCloudListingRequest{RegistryNodeID: nodeID.String(), AgentID: agentID.String()},
			rows: []registryFakeRow{
				{values: node},
				{values: agent},
				{values: registryFakeCloudListingValues(linkID, listingID, nodeID, agentID)},
				{values: registryFakeCloudListingRowValues(linkID, listingID, nodeID, agentID)},
				{err: errors.New("get row failed")},
			},
			want: http.StatusInternalServerError,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &Service{q: db.New(&registryFakeDBTX{queryRowRows: tc.rows})}
			_, err := svc.CreateCloudListing(context.Background(), ownerID, tc.req)
			requireRegistryHTTPStatus(t, err, tc.want)
		})
	}

	listErrSvc := &Service{q: db.New(&registryFakeDBTX{queryResults: []registryFakeQueryResult{{err: errors.New("list failed")}}})}
	_, err := listErrSvc.ListCloudListings(context.Background(), ownerID)
	requireRegistryHTTPStatus(t, err, http.StatusInternalServerError)

	for _, tc := range []struct {
		name string
		call func(*Service) (*CloudListingLinkResponse, error)
		row  registryFakeRow
		want int
	}{
		{
			name: "update missing",
			call: func(s *Service) (*CloudListingLinkResponse, error) {
				return s.UpdateCloudListingStatus(context.Background(), ownerID, listingID, &UpdateCloudListingStatusRequest{SyncStatus: "linked"})
			},
			row:  registryFakeRow{err: pgx.ErrNoRows},
			want: http.StatusNotFound,
		},
		{
			name: "update db error",
			call: func(s *Service) (*CloudListingLinkResponse, error) {
				return s.UpdateCloudListingStatus(context.Background(), ownerID, listingID, &UpdateCloudListingStatusRequest{SyncStatus: "paused"})
			},
			row:  registryFakeRow{err: errors.New("update failed")},
			want: http.StatusInternalServerError,
		},
		{
			name: "sync missing",
			call: func(s *Service) (*CloudListingLinkResponse, error) {
				return s.SyncCloudListingMetadata(context.Background(), ownerID, listingID)
			},
			row:  registryFakeRow{err: pgx.ErrNoRows},
			want: http.StatusNotFound,
		},
		{
			name: "sync db error",
			call: func(s *Service) (*CloudListingLinkResponse, error) {
				return s.SyncCloudListingMetadata(context.Background(), ownerID, listingID)
			},
			row:  registryFakeRow{err: errors.New("sync failed")},
			want: http.StatusInternalServerError,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &Service{q: db.New(&registryFakeDBTX{queryRowRows: []registryFakeRow{tc.row}})}
			_, err := tc.call(svc)
			requireRegistryHTTPStatus(t, err, tc.want)
		})
	}
}

func TestRegistryProxyRunServiceErrorEdges(t *testing.T) {
	ownerID := uuid.New()
	nodeID := uuid.New()
	agentID := uuid.New()
	listingID := uuid.New()
	linkID := uuid.New()
	runID := uuid.New()
	artifactID := uuid.New()
	link := registryFakeCloudListingValues(linkID, listingID, nodeID, agentID)
	run := registryFakeProxyRunValues(runID, linkID, listingID, nodeID, agentID, ownerID)

	proxyRunCases := []struct {
		name string
		req  *CreateProxyRunRequest
		rows []registryFakeRow
		want int
	}{
		{name: "listing required", req: &CreateProxyRunRequest{}, want: http.StatusBadRequest},
		{name: "listing invalid", req: &CreateProxyRunRequest{RegistryListingID: "bad"}, want: http.StatusBadRequest},
		{name: "idempotency too short", req: &CreateProxyRunRequest{RegistryListingID: listingID.String(), IdempotencyKey: "short"}, want: http.StatusUnprocessableEntity},
		{name: "listing missing", req: &CreateProxyRunRequest{RegistryListingID: listingID.String()}, rows: []registryFakeRow{{err: pgx.ErrNoRows}}, want: http.StatusNotFound},
		{name: "listing db error", req: &CreateProxyRunRequest{RegistryListingID: listingID.String()}, rows: []registryFakeRow{{err: errors.New("listing lookup failed")}}, want: http.StatusInternalServerError},
		{name: "create no rows", req: &CreateProxyRunRequest{RegistryListingID: listingID.String()}, rows: []registryFakeRow{{values: link}, {err: pgx.ErrNoRows}}, want: http.StatusNotFound},
		{name: "create db error", req: &CreateProxyRunRequest{RegistryListingID: listingID.String()}, rows: []registryFakeRow{{values: link}, {err: errors.New("create failed")}}, want: http.StatusInternalServerError},
	}
	for _, tc := range proxyRunCases {
		t.Run(tc.name, func(t *testing.T) {
			svc := &Service{q: db.New(&registryFakeDBTX{queryRowRows: tc.rows})}
			_, err := svc.CreateProxyRun(context.Background(), ownerID, tc.req)
			requireRegistryHTTPStatus(t, err, tc.want)
		})
	}

	for _, tc := range []struct {
		name string
		call func(*Service) error
		row  registryFakeRow
		want int
	}{
		{
			name: "get run missing",
			call: func(s *Service) error {
				_, err := s.GetProxyRun(context.Background(), ownerID, runID)
				return err
			},
			row:  registryFakeRow{err: pgx.ErrNoRows},
			want: http.StatusNotFound,
		},
		{
			name: "get run db error",
			call: func(s *Service) error {
				_, err := s.GetProxyRun(context.Background(), ownerID, runID)
				return err
			},
			row:  registryFakeRow{err: errors.New("get failed")},
			want: http.StatusInternalServerError,
		},
		{
			name: "download missing artifact",
			call: func(s *Service) error {
				_, err := s.DownloadProxyRunArtifact(context.Background(), ownerID, runID, artifactID)
				return err
			},
			row:  registryFakeRow{err: pgx.ErrNoRows},
			want: http.StatusNotFound,
		},
		{
			name: "download artifact db error",
			call: func(s *Service) error {
				_, err := s.DownloadProxyRunArtifact(context.Background(), ownerID, runID, artifactID)
				return err
			},
			row:  registryFakeRow{err: errors.New("artifact failed")},
			want: http.StatusInternalServerError,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &Service{q: db.New(&registryFakeDBTX{queryRowRows: []registryFakeRow{tc.row}})}
			requireRegistryHTTPStatus(t, tc.call(svc), tc.want)
		})
	}

	expireSvc := &Service{q: db.New(&registryFakeDBTX{queryRowRows: []registryFakeRow{{values: []any{int32(3)}}}})}
	expired, err := expireSvc.ExpireStaleProxyRuns(context.Background(), -time.Second)
	if err != nil || expired != 3 {
		t.Fatalf("ExpireStaleProxyRuns = %d, %v", expired, err)
	}
	expireErrSvc := &Service{q: db.New(&registryFakeDBTX{queryRowRows: []registryFakeRow{{err: errors.New("timeout failed")}}})}
	_, err = expireErrSvc.ExpireStaleProxyRuns(context.Background(), time.Minute)
	requireRegistryHTTPStatus(t, err, http.StatusInternalServerError)

	artifactsRunMissingSvc := &Service{q: db.New(&registryFakeDBTX{queryRowRows: []registryFakeRow{{err: pgx.ErrNoRows}}})}
	_, err = artifactsRunMissingSvc.ListProxyRunArtifacts(context.Background(), ownerID, runID)
	requireRegistryHTTPStatus(t, err, http.StatusNotFound)
	artifactsRunErrSvc := &Service{q: db.New(&registryFakeDBTX{queryRowRows: []registryFakeRow{{err: errors.New("run failed")}}})}
	_, err = artifactsRunErrSvc.ListProxyRunArtifacts(context.Background(), ownerID, runID)
	requireRegistryHTTPStatus(t, err, http.StatusInternalServerError)
	artifactsListErrSvc := &Service{q: db.New(&registryFakeDBTX{
		queryRowRows: []registryFakeRow{{values: run}},
		queryResults: []registryFakeQueryResult{{err: errors.New("artifact list failed")}},
	})}
	_, err = artifactsListErrSvc.ListProxyRunArtifacts(context.Background(), ownerID, runID)
	requireRegistryHTTPStatus(t, err, http.StatusInternalServerError)

	noURIArtifact := registryFakeArtifactValues(artifactID, runID, uuid.New(), nil, nil)
	downloadNoURISvc := &Service{q: db.New(&registryFakeDBTX{queryRowRows: []registryFakeRow{{values: noURIArtifact}}})}
	_, err = downloadNoURISvc.DownloadProxyRunArtifact(context.Background(), ownerID, runID, artifactID)
	requireRegistryHTTPStatus(t, err, http.StatusUnprocessableEntity)

	ftpURI := "ftp://files.example/out.txt"
	downloadBadSchemeSvc := &Service{q: db.New(&registryFakeDBTX{queryRowRows: []registryFakeRow{{values: registryFakeArtifactValues(artifactID, runID, uuid.New(), &ftpURI, nil)}}})}
	_, err = downloadBadSchemeSvc.DownloadProxyRunArtifact(context.Background(), ownerID, runID, artifactID)
	requireRegistryHTTPStatus(t, err, http.StatusUnprocessableEntity)

	oversize := maxProxyArtifactDownloadBytes + 1
	httpURI := "https://files.example/out.txt"
	downloadOversizeSvc := &Service{q: db.New(&registryFakeDBTX{queryRowRows: []registryFakeRow{{values: registryFakeArtifactValues(artifactID, runID, uuid.New(), &httpURI, &oversize)}}})}
	_, err = downloadOversizeSvc.DownloadProxyRunArtifact(context.Background(), ownerID, runID, artifactID)
	requireRegistryHTTPStatus(t, err, http.StatusRequestEntityTooLarge)
}

func TestRegistryProxyRunValidationEdges(t *testing.T) {
	nodeID := uuid.New()
	ownerID := uuid.New()
	runID := uuid.New()
	secret, hashedSecret := registryFakeNodeSecret(t)
	node := registryFakeNodeValues(nodeID, ownerID, hashedSecret, nil)
	newSvc := func(queryRowRows ...registryFakeRow) *Service {
		return &Service{q: db.New(&registryFakeDBTX{
			queryResults: []registryFakeQueryResult{{rows: []registryFakeRow{{values: node}}}},
			queryRowRows: queryRowRows,
		})}
	}

	for _, tc := range []struct {
		name string
		req  *CompleteProxyRunRequest
		want int
	}{
		{name: "invalid status", req: &CompleteProxyRunRequest{Status: "done"}, want: http.StatusUnprocessableEntity},
		{name: "invalid output", req: &CompleteProxyRunRequest{Status: "failed", Output: map[string]any{"bad": func() {}}}, want: http.StatusBadRequest},
		{name: "output summary too long", req: &CompleteProxyRunRequest{Status: "failed", OutputSummary: strings.Repeat("x", 1001)}, want: http.StatusUnprocessableEntity},
		{name: "error code too long", req: &CompleteProxyRunRequest{Status: "failed", ErrorCode: strings.Repeat("x", 81)}, want: http.StatusUnprocessableEntity},
		{name: "error message too long", req: &CompleteProxyRunRequest{Status: "failed", ErrorMessage: strings.Repeat("x", 1001)}, want: http.StatusUnprocessableEntity},
		{name: "retry after negative", req: &CompleteProxyRunRequest{Status: "failed", RetryAfterSec: -1}, want: http.StatusUnprocessableEntity},
		{name: "retry after too large", req: &CompleteProxyRunRequest{Status: "failed", RetryAfterSec: 3601}, want: http.StatusUnprocessableEntity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newSvc().CompleteProxyRun(context.Background(), secret, runID, tc.req)
			requireRegistryHTTPStatus(t, err, tc.want)
		})
	}

	noRun, err := newSvc(registryFakeRow{err: pgx.ErrNoRows}).ClaimProxyRun(context.Background(), secret)
	if err != nil || noRun != nil {
		t.Fatalf("ClaimProxyRun no rows = %#v, %v", noRun, err)
	}
	_, err = newSvc(registryFakeRow{err: errors.New("claim failed")}).ClaimProxyRun(context.Background(), secret)
	requireRegistryHTTPStatus(t, err, http.StatusInternalServerError)
}

func TestRegistryRemoteHTTPServiceEdges(t *testing.T) {
	oldRemoteClient := remoteRegistryHTTPClient
	defer func() { remoteRegistryHTTPClient = oldRemoteClient }()

	ownerID := uuid.New()
	listingID := uuid.New()
	baseReq := func() *CreateRemoteProxyRunRequest {
		return &CreateRemoteProxyRunRequest{
			RemoteAPIBaseURL:        "https://remote.example/root",
			RemoteBearerToken:       "bearer-token-123",
			RemoteRegistryListingID: listingID.String(),
			IdempotencyKey:          "idem-key-123",
			Input:                   map[string]any{"ok": true},
		}
	}
	svc := &Service{}

	_, err := svc.CreateRemoteProxyRun(context.Background(), ownerID, &CreateRemoteProxyRunRequest{
		RemoteAPIBaseURL:        "https://remote.example",
		RemoteBearerToken:       "bearer-token-123",
		RemoteRegistryListingID: "bad",
	})
	requireRegistryHTTPStatus(t, err, http.StatusBadRequest)
	invalidSummaryReq := baseReq()
	invalidSummaryReq.InputSummary = strings.Repeat("x", 501)
	_, err = svc.CreateRemoteProxyRun(context.Background(), ownerID, invalidSummaryReq)
	requireRegistryHTTPStatus(t, err, http.StatusUnprocessableEntity)

	remoteRegistryHTTPClient = registryHTTPClient(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial failed")
	})
	_, err = svc.CreateRemoteProxyRun(context.Background(), ownerID, baseReq())
	requireRegistryHTTPStatus(t, err, http.StatusServiceUnavailable)

	remoteRegistryHTTPClient = registryHTTPClient(func(*http.Request) (*http.Response, error) {
		return registryHTTPResponse(http.StatusBadGateway, strings.Repeat("x", 400)), nil
	})
	_, err = svc.CreateRemoteProxyRun(context.Background(), ownerID, baseReq())
	requireRegistryHTTPStatus(t, err, http.StatusBadGateway)

	remoteRegistryHTTPClient = registryHTTPClient(func(*http.Request) (*http.Response, error) {
		return registryHTTPResponse(http.StatusOK, `not-json`), nil
	})
	_, err = svc.CreateRemoteProxyRun(context.Background(), ownerID, baseReq())
	requireRegistryHTTPStatus(t, err, http.StatusServiceUnavailable)

	remoteRegistryHTTPClient = registryHTTPClient(func(*http.Request) (*http.Response, error) {
		return registryHTTPResponse(http.StatusOK, `{"id":"`+uuid.NewString()+`"}`), nil
	})
	_, err = svc.CreateRemoteProxyRun(context.Background(), ownerID, baseReq())
	requireRegistryHTTPStatus(t, err, http.StatusServiceUnavailable)

	var capturedAuth string
	remoteRunID := uuid.NewString()
	remoteRegistryHTTPClient = registryHTTPClient(func(req *http.Request) (*http.Response, error) {
		capturedAuth = req.Header.Get("Authorization")
		body := `{"id":"` + remoteRunID + `","registry_listing_id":"` + listingID.String() + `","cloud_listing_id":"` + listingID.String() + `"}`
		return registryHTTPResponse(http.StatusCreated, body), nil
	})
	got, err := svc.CreateRemoteProxyRun(context.Background(), ownerID, baseReq())
	if err != nil || got == nil || got.RouteMode != "explicit" || got.RemoteRun.ID != remoteRunID || capturedAuth != "Bearer bearer-token-123" {
		t.Fatalf("CreateRemoteProxyRun success = %#v auth=%q err=%v", got, capturedAuth, err)
	}

	exchangeURL := "https://remote.example/api/v1/registry-peers/federation-invitations/exchange"
	remoteRegistryHTTPClient = registryHTTPClient(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("exchange failed")
	})
	_, err = svc.ExchangeRegistryFederationInvite(context.Background(), ownerID, &ExchangeRegistryFederationInviteRequest{
		ExchangeURL:     exchangeURL,
		FederationToken: "federation-token-123",
		InitialStatus:   "active",
	})
	requireRegistryHTTPStatus(t, err, http.StatusServiceUnavailable)

	remoteRegistryHTTPClient = registryHTTPClient(func(*http.Request) (*http.Response, error) {
		return registryHTTPResponse(http.StatusInternalServerError, `failed`), nil
	})
	_, err = svc.ExchangeRegistryFederationInvite(context.Background(), ownerID, &ExchangeRegistryFederationInviteRequest{
		ExchangeURL:     exchangeURL,
		FederationToken: "federation-token-123",
	})
	requireRegistryHTTPStatus(t, err, http.StatusServiceUnavailable)

	remoteRegistryHTTPClient = registryHTTPClient(func(*http.Request) (*http.Response, error) {
		return registryHTTPResponse(http.StatusOK, `not-json`), nil
	})
	_, err = svc.ExchangeRegistryFederationInvite(context.Background(), ownerID, &ExchangeRegistryFederationInviteRequest{
		ExchangeURL:     exchangeURL,
		FederationToken: "federation-token-123",
	})
	requireRegistryHTTPStatus(t, err, http.StatusServiceUnavailable)

	remoteRegistryHTTPClient = registryHTTPClient(func(*http.Request) (*http.Response, error) {
		return registryHTTPResponse(http.StatusOK, `{"name":"Peer","api_base_url":"https://peer.example/api/v1","bearer_token":"bearer-token-123","credential_hint":"sha256:abc"}`), nil
	})
	_, err = svc.ExchangeRegistryFederationInvite(context.Background(), ownerID, &ExchangeRegistryFederationInviteRequest{
		ExchangeURL:     exchangeURL,
		FederationToken: "federation-token-123",
	})
	requireRegistryHTTPStatus(t, err, http.StatusInternalServerError)
}

func TestRegistryArtifactDownloadHTTPEdges(t *testing.T) {
	oldArtifactClient := proxyArtifactHTTPClient
	defer func() { proxyArtifactHTTPClient = oldArtifactClient }()

	ownerID := uuid.New()
	runID := uuid.New()
	artifactID := uuid.New()
	cloudRunID := uuid.New()
	fileURI := "https://files.example/out.txt"
	newSvc := func(values []any) *Service {
		return &Service{q: db.New(&registryFakeDBTX{queryRowRows: []registryFakeRow{{values: values}}})}
	}

	for _, tc := range []struct {
		name      string
		artifact  []any
		transport func(*http.Request) (*http.Response, error)
		want      int
	}{
		{
			name:     "fetch fails",
			artifact: registryFakeArtifactValues(artifactID, runID, cloudRunID, &fileURI, nil),
			transport: func(*http.Request) (*http.Response, error) {
				return nil, errors.New("fetch failed")
			},
			want: http.StatusServiceUnavailable,
		},
		{
			name:     "upstream non success",
			artifact: registryFakeArtifactValues(artifactID, runID, cloudRunID, &fileURI, nil),
			transport: func(*http.Request) (*http.Response, error) {
				return registryHTTPResponse(http.StatusNotFound, "missing"), nil
			},
			want: http.StatusServiceUnavailable,
		},
		{
			name:     "declared content length too large",
			artifact: registryFakeArtifactValues(artifactID, runID, cloudRunID, &fileURI, nil),
			transport: func(*http.Request) (*http.Response, error) {
				resp := registryHTTPResponse(http.StatusOK, "")
				resp.ContentLength = maxProxyArtifactDownloadBytes + 1
				return resp, nil
			},
			want: http.StatusRequestEntityTooLarge,
		},
		{
			name:     "read fails",
			artifact: registryFakeArtifactValues(artifactID, runID, cloudRunID, &fileURI, nil),
			transport: func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode:    http.StatusOK,
					Header:        make(http.Header),
					Body:          registryErrorReadCloser{},
					ContentLength: -1,
				}, nil
			},
			want: http.StatusServiceUnavailable,
		},
		{
			name:     "size mismatch",
			artifact: registryFakeArtifactValues(artifactID, runID, cloudRunID, &fileURI, registryInt64Ptr(4)),
			transport: func(*http.Request) (*http.Response, error) {
				return registryHTTPResponse(http.StatusOK, "abc"), nil
			},
			want: http.StatusServiceUnavailable,
		},
		{
			name:     "sha mismatch",
			artifact: registryFakeArtifactValuesWithMetadata(artifactID, runID, cloudRunID, &fileURI, registryStringPtr("text/plain"), registryStringPtr("out.txt"), registryStringPtr(strings.Repeat("b", 64)), registryInt64Ptr(3)),
			transport: func(*http.Request) (*http.Response, error) {
				return registryHTTPResponse(http.StatusOK, "abc"), nil
			},
			want: http.StatusServiceUnavailable,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proxyArtifactHTTPClient = registryHTTPClient(tc.transport)
			_, err := newSvc(tc.artifact).DownloadProxyRunArtifact(context.Background(), ownerID, runID, artifactID)
			requireRegistryHTTPStatus(t, err, tc.want)
		})
	}

	proxyArtifactHTTPClient = registryHTTPClient(func(*http.Request) (*http.Response, error) {
		resp := registryHTTPResponse(http.StatusOK, "abc")
		resp.Header.Set("Content-Type", "text/custom")
		return resp, nil
	})
	got, err := newSvc(registryFakeArtifactValuesWithMetadata(artifactID, runID, cloudRunID, &fileURI, nil, nil, nil, registryInt64Ptr(3))).
		DownloadProxyRunArtifact(context.Background(), ownerID, runID, artifactID)
	if err != nil || got.FileName != "artifact-1" || got.ContentType != "text/custom" || got.SizeBytes != 3 || string(got.Body) != "abc" {
		t.Fatalf("DownloadProxyRunArtifact success = %#v, %v", got, err)
	}
}

type registryHandlerRequest struct {
	method  string
	target  string
	body    string
	userID  string
	params  map[string]string
	headers map[string]string
}

func newRegistryTestContext(spec *registryHandlerRequest) echo.Context {
	method := spec.method
	if method == "" {
		method = http.MethodGet
	}
	req := httptest.NewRequest(method, spec.target, strings.NewReader(spec.body))
	if spec.body != "" {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}
	for key, value := range spec.headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)
	if spec.userID != "" {
		c.Set(string(httpx.CtxKeyUserID), spec.userID)
	}
	if len(spec.params) > 0 {
		names := make([]string, 0, len(spec.params))
		values := make([]string, 0, len(spec.params))
		for name, value := range spec.params {
			names = append(names, name)
			values = append(values, value)
		}
		c.SetParamNames(names...)
		c.SetParamValues(values...)
	}
	return c
}

type registryRoundTripper func(*http.Request) (*http.Response, error)

func (f registryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func registryHTTPClient(fn func(*http.Request) (*http.Response, error)) *http.Client {
	return &http.Client{Transport: registryRoundTripper(fn)}
}

func registryHTTPResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode:    status,
		Header:        make(http.Header),
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

type registryErrorReadCloser struct{}

func (registryErrorReadCloser) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

func (registryErrorReadCloser) Close() error {
	return nil
}

type registryFakeDBTX struct {
	queryRowRows     []registryFakeRow
	queryRowCalls    int
	queryResults     []registryFakeQueryResult
	queryCalls       int
	execErr          error
	execRowsAffected int64
	queryRowSQL      string
	queryRowArgs     []any
	querySQL         string
	queryArgs        []any
	execSQL          string
	execArgs         []any
}

func (f *registryFakeDBTX) Exec(_ context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	f.execSQL = sql
	f.execArgs = append([]any(nil), args...)
	if f.execErr != nil {
		return pgconn.CommandTag{}, f.execErr
	}
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", f.execRowsAffected)), nil
}

func (f *registryFakeDBTX) Query(_ context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	f.querySQL = sql
	f.queryArgs = append([]any(nil), args...)
	result := registryFakeQueryResult{err: errors.New("registry fake query is not implemented")}
	if f.queryCalls < len(f.queryResults) {
		result = f.queryResults[f.queryCalls]
	}
	f.queryCalls++
	if result.err != nil {
		return nil, result.err
	}
	return &registryFakeRows{rows: result.rows, err: result.rowsErr}, nil
}

func (f *registryFakeDBTX) QueryRow(_ context.Context, sql string, args ...interface{}) pgx.Row {
	f.queryRowSQL = sql
	f.queryRowArgs = append([]any(nil), args...)
	if f.queryRowCalls < len(f.queryRowRows) {
		row := f.queryRowRows[f.queryRowCalls]
		f.queryRowCalls++
		return row
	}
	f.queryRowCalls++
	return registryFakeRow{err: errors.New("registry fake query row is not implemented")}
}

type registryFakeQueryResult struct {
	rows    []registryFakeRow
	err     error
	rowsErr error
}

type registryFakeRow struct {
	values []any
	err    error
}

func (r registryFakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return fmt.Errorf("registry fake row scan destination mismatch: got %d want %d", len(dest), len(r.values))
	}
	for i, value := range r.values {
		target := reflect.ValueOf(dest[i])
		if target.Kind() != reflect.Ptr || target.IsNil() {
			return errors.New("registry fake row scan target must be a non-nil pointer")
		}
		slot := target.Elem()
		if value == nil {
			slot.Set(reflect.Zero(slot.Type()))
			continue
		}
		source := reflect.ValueOf(value)
		if source.Type().AssignableTo(slot.Type()) {
			slot.Set(source)
			continue
		}
		if source.Type().ConvertibleTo(slot.Type()) {
			slot.Set(source.Convert(slot.Type()))
			continue
		}
		return fmt.Errorf("registry fake row scan value type mismatch at %d: %s into %s", i, source.Type(), slot.Type())
	}
	return nil
}

type registryFakeRows struct {
	rows    []registryFakeRow
	current int
	closed  bool
	err     error
}

func (r *registryFakeRows) Close() {
	r.closed = true
}

func (r *registryFakeRows) Err() error {
	if !r.closed {
		return nil
	}
	return r.err
}

func (r *registryFakeRows) CommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{}
}

func (r *registryFakeRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *registryFakeRows) Next() bool {
	if r.current >= len(r.rows) {
		r.Close()
		return false
	}
	r.current++
	return true
}

func (r *registryFakeRows) Scan(dest ...any) error {
	if r.current == 0 || r.current > len(r.rows) {
		return errors.New("registry fake rows scan without current row")
	}
	if err := r.rows[r.current-1].Scan(dest...); err != nil {
		r.Close()
		return err
	}
	return nil
}

func (r *registryFakeRows) Values() ([]any, error) {
	if r.current == 0 || r.current > len(r.rows) {
		return nil, errors.New("registry fake rows values without current row")
	}
	return append([]any(nil), r.rows[r.current-1].values...), nil
}

func (r *registryFakeRows) RawValues() [][]byte {
	return nil
}

func (r *registryFakeRows) Conn() *pgx.Conn {
	return nil
}

func registryFakeNodeSecret(t *testing.T) (string, string) {
	t.Helper()
	secret := nodeSecretPrefix + strings.Repeat("a", nodeSecretRandomSize*2)
	hash, err := bcrypt.GenerateFromPassword([]byte(secret), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("bcrypt fake node secret: %v", err)
	}
	return secret, string(hash)
}

func registryFakeNodeValues(nodeID, ownerID uuid.UUID, secretHash string, revokedAt *time.Time) []any {
	now := time.Date(2026, 6, 22, 10, 0, 0, 0, time.UTC)
	baseURL := "https://node.example"
	return []any{
		nodeID,
		ownerID,
		"Bridge Node",
		"bridge_proxy",
		&baseURL,
		nodeSecretPrefix + "aaaa",
		secretHash,
		defaultNodeScopes,
		"healthy",
		&now,
		revokedAt,
		now,
		now,
	}
}

func registryFakeCloudListingValues(linkID, listingID, nodeID, agentID uuid.UUID) []any {
	now := time.Date(2026, 6, 22, 10, 0, 0, 0, time.UTC)
	return []any{
		linkID,
		listingID,
		nodeID,
		agentID,
		"pull_proxy",
		payloadPolicyMetadataOnly,
		[]string{"secret"},
		"linked",
		"agent-one",
		"Agent One",
		"does work",
		[]string{"a2a"},
		"healthy",
		&now,
		nil,
		now,
		now,
		now,
	}
}

func registryFakeCloudListingRowValues(linkID, listingID, nodeID, agentID uuid.UUID) []any {
	now := time.Date(2026, 6, 22, 10, 0, 0, 0, time.UTC)
	return []any{
		linkID,
		listingID,
		nodeID,
		"Bridge Node",
		agentID,
		"agent-one",
		"Agent One",
		"pull_proxy",
		payloadPolicyMetadataOnly,
		[]string{"secret"},
		"linked",
		"does work",
		[]string{"a2a"},
		"healthy",
		&now,
		nil,
		now,
		now,
		now,
	}
}

func registryFakeAgentValues(agentID, ownerID uuid.UUID, lifecycleStatus string) []any {
	now := time.Date(2026, 6, 22, 10, 0, 0, 0, time.UTC)
	return []any{
		agentID,
		ownerID,
		"agent-one",
		"Agent One",
		"does work",
		"https://agent.example/run",
		nil,
		int32(0),
		[]string{"a2a"},
		lifecycleStatus,
		"public",
		"unreviewed",
		nil,
		nil,
		int64(0),
		int64(0),
		nil,
		"direct_http",
		nil,
		now,
		now,
	}
}

func registryFakeProxyRunValues(runID, linkID, listingID, nodeID, agentID, requesterID uuid.UUID) []any {
	now := time.Date(2026, 6, 22, 10, 0, 0, 0, time.UTC)
	return []any{
		runID,
		uuid.New(),
		linkID,
		listingID,
		nodeID,
		agentID,
		requesterID,
		"idem-key-123",
		"pending",
		payloadPolicyMetadataOnly,
		[]string{"secret"},
		[]byte(`{}`),
		nil,
		[]byte(`{}`),
		nil,
		nil,
		nil,
		int32(0),
		int32(3),
		nil,
		nil,
		nil,
		now,
		now,
	}
}

func registryFakeArtifactValues(artifactID, runID, cloudRunID uuid.UUID, fileURI *string, fileSize *int64) []any {
	return registryFakeArtifactValuesWithMetadata(
		artifactID,
		runID,
		cloudRunID,
		fileURI,
		registryStringPtr("text/plain"),
		registryStringPtr("out.txt"),
		nil,
		fileSize,
	)
}

func registryFakeArtifactValuesWithMetadata(
	artifactID,
	runID,
	cloudRunID uuid.UUID,
	fileURI,
	mimeType,
	fileName,
	fileSHA *string,
	fileSize *int64,
) []any {
	now := time.Date(2026, 6, 22, 10, 0, 0, 0, time.UTC)
	return []any{
		artifactID,
		runID,
		cloudRunID,
		"artifact-1",
		"file",
		"Output",
		[]byte(`{}`),
		mimeType,
		fileURI,
		fileName,
		fileSHA,
		fileSize,
		now,
	}
}

func registryStringPtr(value string) *string {
	return &value
}

func registryInt64Ptr(value int64) *int64 {
	return &value
}

func requireRegistryHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected HTTP error %d, got nil", want)
	}
	var he *httpx.HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected *httpx.HTTPError, got %T (%v)", err, err)
	}
	if he.Status != want {
		t.Fatalf("HTTP status = %d (%s), want %d", he.Status, he.Message, want)
	}
}

func userIDFromCtxOnly(c echo.Context) error {
	_, err := userIDFromCtx(c)
	return err
}
