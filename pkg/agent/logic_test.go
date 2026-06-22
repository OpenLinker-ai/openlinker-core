package agent

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/kinzhi/openlinker-core/pkg/config"
	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

func TestAgentSlugTagsStatusAndSQLStateHelpers(t *testing.T) {
	validSlug80 := strings.Repeat("a", 80)
	for _, tc := range []struct {
		name string
		slug string
		want bool
	}{
		{name: "simple", slug: "agent-123", want: true},
		{name: "min length", slug: "a1b", want: true},
		{name: "too short", slug: "ab", want: false},
		{name: "too long", slug: validSlug80 + "a", want: false},
		{name: "valid max", slug: validSlug80, want: true},
		{name: "leading dash", slug: "-agent", want: false},
		{name: "trailing dash", slug: "agent-", want: false},
		{name: "uppercase", slug: "Agent", want: false},
		{name: "underscore", slug: "agent_one", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isValidSlug(tc.slug); got != tc.want {
				t.Fatalf("isValidSlug(%q) = %v, want %v", tc.slug, got, tc.want)
			}
		})
	}

	if got := normalizeTagsForInsert([]string{" AI ", "", "Data", " mixed-Case "}); !reflect.DeepEqual(got, []string{"ai", "data", "mixed-case"}) {
		t.Fatalf("normalizeTagsForInsert returned %#v", got)
	}
	if got := normalizeTags(nil); got == nil || len(got) != 0 {
		t.Fatalf("normalizeTags(nil) = %#v, want empty non-nil slice", got)
	}
	if got := normalizeTags([]string{"ai"}); !reflect.DeepEqual(got, []string{"ai"}) {
		t.Fatalf("normalizeTags(non-nil) = %#v", got)
	}
	if got := normalizeAuthHeader("  Bearer test  "); got == nil || *got != "Bearer test" {
		t.Fatalf("normalizeAuthHeader did not trim non-empty header: %#v", got)
	}
	if got := normalizeAuthHeader(" \t "); got != nil {
		t.Fatalf("normalizeAuthHeader(empty) = %#v, want nil", got)
	}

	for _, tc := range []struct {
		name string
		a    db.Agent
		want string
	}{
		{name: "disabled wins", a: db.Agent{LifecycleStatus: "disabled", CertificationStatus: "certified"}, want: "disabled"},
		{name: "pending", a: db.Agent{LifecycleStatus: "active", CertificationStatus: "pending"}, want: "pending"},
		{name: "rejected", a: db.Agent{LifecycleStatus: "active", CertificationStatus: "rejected"}, want: "rejected"},
		{name: "default approved", a: db.Agent{LifecycleStatus: "active", CertificationStatus: "unreviewed"}, want: "approved"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := deriveLegacyStatus(&tc.a); got != tc.want {
				t.Fatalf("deriveLegacyStatus() = %q, want %q", got, tc.want)
			}
		})
	}

	if !isUniqueViolation(sqlStateErr{state: "23505"}) || isUniqueViolation(sqlStateErr{state: "23514"}) || isUniqueViolation(nil) {
		t.Fatalf("isUniqueViolation did not classify SQLSTATE 23505 only")
	}
	if !isCheckViolation(sqlStateErr{state: "23514"}) || isCheckViolation(sqlStateErr{state: "23505"}) || isCheckViolation(nil) {
		t.Fatalf("isCheckViolation did not classify SQLSTATE 23514 only")
	}
	if !isUndefinedTable(sqlStateErr{state: "42P01"}) || isUndefinedTable(sqlStateErr{state: "23505"}) || isUndefinedTable(nil) {
		t.Fatalf("isUndefinedTable did not classify SQLSTATE 42P01 only")
	}
}

func TestNormalizeConnectionSettings(t *testing.T) {
	for _, tc := range []struct {
		name        string
		slug        string
		endpoint    string
		mode        string
		tool        string
		allowLocal  bool
		wantMode    string
		wantURL     string
		wantTool    string
		wantHTTP    int
		wantToolNil bool
	}{
		{
			name:        "default direct http trims endpoint",
			slug:        "direct-agent",
			endpoint:    " https://example.com/agent ",
			wantMode:    ConnectionModeDirectHTTP,
			wantURL:     "https://example.com/agent",
			wantToolNil: true,
		},
		{
			name:        "direct requires endpoint",
			slug:        "direct-agent",
			mode:        ConnectionModeDirectHTTP,
			wantHTTP:    http.StatusUnprocessableEntity,
			wantToolNil: true,
		},
		{
			name:        "direct allows local endpoint when enabled",
			slug:        "local-agent",
			endpoint:    "http://localhost:3000/agent",
			allowLocal:  true,
			wantMode:    ConnectionModeDirectHTTP,
			wantURL:     "http://localhost:3000/agent",
			wantToolNil: true,
		},
		{
			name:        "direct rejects local endpoint by default",
			slug:        "local-agent",
			endpoint:    "http://localhost:3000/agent",
			wantHTTP:    http.StatusUnprocessableEntity,
			wantToolNil: true,
		},
		{
			name:        "mcp server requires tool",
			slug:        "mcp-agent",
			endpoint:    "https://example.com/mcp",
			mode:        ConnectionModeMCPServer,
			wantHTTP:    http.StatusUnprocessableEntity,
			wantToolNil: true,
		},
		{
			name:     "mcp server trims tool",
			slug:     "mcp-agent",
			endpoint: "https://example.com/mcp",
			mode:     ConnectionModeMCPServer,
			tool:     " search ",
			wantMode: ConnectionModeMCPServer,
			wantURL:  "https://example.com/mcp",
			wantTool: "search",
		},
		{
			name:        "runtime pull fills endpoint",
			slug:        "pull-agent",
			mode:        ConnectionModeRuntimePull,
			wantMode:    ConnectionModeRuntimePull,
			wantURL:     runtimePullEndpointPrefix + "pull-agent",
			wantToolNil: true,
		},
		{
			name:        "runtime pull replaces non canonical endpoint",
			slug:        "pull-agent",
			endpoint:    "https://example.com/ignored",
			mode:        ConnectionModeRuntimePull,
			wantMode:    ConnectionModeRuntimePull,
			wantURL:     runtimePullEndpointPrefix + "pull-agent",
			wantToolNil: true,
		},
		{
			name:        "runtime pull preserves canonical endpoint",
			slug:        "pull-agent",
			endpoint:    runtimePullEndpointPrefix + "custom",
			mode:        ConnectionModeRuntimePull,
			wantMode:    ConnectionModeRuntimePull,
			wantURL:     runtimePullEndpointPrefix + "custom",
			wantToolNil: true,
		},
		{
			name:        "runtime ws fills endpoint",
			slug:        "ws-agent",
			mode:        ConnectionModeRuntimeWS,
			wantMode:    ConnectionModeRuntimeWS,
			wantURL:     runtimeWSEndpointPrefix + "ws-agent",
			wantToolNil: true,
		},
		{
			name:        "runtime ws replaces non canonical endpoint",
			slug:        "ws-agent",
			endpoint:    "https://example.com/ignored",
			mode:        ConnectionModeRuntimeWS,
			wantMode:    ConnectionModeRuntimeWS,
			wantURL:     runtimeWSEndpointPrefix + "ws-agent",
			wantToolNil: true,
		},
		{
			name:        "unsupported mode",
			slug:        "bad-agent",
			endpoint:    "https://example.com/agent",
			mode:        "grpc",
			wantHTTP:    http.StatusUnprocessableEntity,
			wantToolNil: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeConnectionSettings(tc.slug, tc.endpoint, tc.mode, tc.tool, tc.allowLocal)
			if tc.wantHTTP != 0 {
				requireHTTPStatus(t, err, tc.wantHTTP)
				return
			}
			if err != nil {
				t.Fatalf("normalizeConnectionSettings() error = %v", err)
			}
			if got.Mode != tc.wantMode || got.EndpointURL != tc.wantURL {
				t.Fatalf("settings = %#v, want mode=%q url=%q", got, tc.wantMode, tc.wantURL)
			}
			if tc.wantToolNil {
				if got.MCPToolName != nil {
					t.Fatalf("MCPToolName = %q, want nil", *got.MCPToolName)
				}
				return
			}
			if got.MCPToolName == nil || *got.MCPToolName != tc.wantTool {
				t.Fatalf("MCPToolName = %#v, want %q", got.MCPToolName, tc.wantTool)
			}
		})
	}
}

func TestAgentDTOAvailabilityAndAlertHelpers(t *testing.T) {
	agentID := uuid.New()
	creatorID := uuid.New()
	certifiedAt := time.Date(2026, 6, 20, 3, 4, 5, 0, time.FixedZone("CST", 8*3600))
	createdAt := time.Date(2026, 6, 20, 4, 4, 5, 0, time.FixedZone("CST", 8*3600))
	reason := "needs proof"
	webhook := "https://example.com/hook"
	tool := "lookup"
	resp := toAgentResponse(&db.Agent{
		ID:                  agentID,
		CreatorID:           creatorID,
		Slug:                "dto-agent",
		Name:                "DTO Agent",
		Description:         "desc",
		EndpointURL:         "https://example.com/agent",
		PricePerCallCents:   12,
		Tags:                nil,
		LifecycleStatus:     "active",
		Visibility:          "unlisted",
		CertificationStatus: "rejected",
		RejectionReason:     &reason,
		CertifiedAt:         &certifiedAt,
		TotalCalls:          7,
		TotalRevenueCents:   99,
		WebhookURL:          &webhook,
		ConnectionMode:      ConnectionModeMCPServer,
		MCPToolName:         &tool,
		CreatedAt:           createdAt,
	})
	if resp.ID != agentID.String() || resp.Status != "rejected" || resp.CreatedAt != "2026-06-19T20:04:05Z" {
		t.Fatalf("unexpected AgentResponse: %#v", resp)
	}
	if resp.CertifiedAt == nil || *resp.CertifiedAt != "2026-06-19T19:04:05Z" {
		t.Fatalf("CertifiedAt = %#v", resp.CertifiedAt)
	}
	if resp.Tags == nil || len(resp.Tags) != 0 || resp.WebhookURL == nil || *resp.WebhookURL != webhook || resp.MCPToolName == nil || *resp.MCPToolName != tool {
		t.Fatalf("unexpected optional fields in AgentResponse: %#v", resp)
	}

	capability := toCapabilityResponse(&db.AgentCapability{
		ID:           uuid.New(),
		AgentID:      agentID,
		InputSchema:  []byte(`{"type":"object","properties":{"q":{"type":"string"}}}`),
		OutputSchema: []byte(`{"type":"object"}`),
		Summary:      "summary",
		Version:      3,
		PublishedAt:  certifiedAt,
		UpdatedAt:    createdAt,
	})
	if capability.InputSchema["type"] != "object" || capability.Version != 3 || capability.PublishedAt != "2026-06-19T19:04:05Z" {
		t.Fatalf("unexpected CapabilityResponse: %#v", capability)
	}
	if got := decodeJSONMap([]byte(`not-json`)); len(got) != 0 {
		t.Fatalf("decodeJSONMap invalid JSON = %#v, want empty map", got)
	}

	example := toExampleResponse(&db.AgentExample{
		ID:                 uuid.New(),
		AgentID:            agentID,
		Title:              "sample",
		InputJSON:          []byte(`{"q":"hi"}`),
		ExpectedOutputJSON: []byte(`{"answer":"ok"}`),
		SortOrder:          4,
		CreatedAt:          certifiedAt,
		UpdatedAt:          createdAt,
	})
	if example.InputJSON["q"] != "hi" || example.ExpectedOutputJSON["answer"] != "ok" || example.SortOrder != 4 {
		t.Fatalf("unexpected ExampleResponse: %#v", example)
	}
	if emptyExpected := toExampleResponse(&db.AgentExample{ID: uuid.New(), AgentID: agentID}); emptyExpected.ExpectedOutputJSON != nil {
		t.Fatalf("expected empty expected output to remain omitted: %#v", emptyExpected)
	}

	dryErr := "failed"
	status := toOnboardingStatusResponse(&db.AgentOnboardingStatus{
		AgentID:          agentID,
		EndpointSet:      true,
		CapabilitiesSet:  true,
		ExamplesSet:      true,
		DryRunPassed:     false,
		DryRunLastResult: "fail",
		DryRunError:      &dryErr,
		DryRunAt:         &certifiedAt,
		UpdatedAt:        createdAt,
	})
	if status.DryRunAt == nil || *status.DryRunAt != "2026-06-19T19:04:05Z" || status.UpdatedAt != "2026-06-19T20:04:05Z" {
		t.Fatalf("unexpected OnboardingStatusResponse: %#v", status)
	}

	healthy := availabilityResponse("healthy", &certifiedAt, nil, &createdAt, 0)
	if healthy.Status != "healthy" || healthy.LastSuccessfulRunAt == nil || healthy.LastCheckedAt == nil {
		t.Fatalf("unexpected healthy availability: %#v", healthy)
	}
	unknown := availabilityResponse("not-a-status", nil, nil, nil, 0)
	if unknown.Status != "unknown" || unknown.LastSuccessfulRunAt != nil {
		t.Fatalf("unexpected fallback availability: %#v", unknown)
	}
	snapshot := availabilityFromSnapshot(db.AgentAvailabilitySnapshot{
		AvailabilityStatus:  "unreachable",
		LastFailedRunAt:     &createdAt,
		ConsecutiveFailures: 3,
	})
	if snapshot.Status != "unreachable" || snapshot.LastFailedRunAt == nil || snapshot.ConsecutiveFailures != 3 {
		t.Fatalf("unexpected snapshot availability: %#v", snapshot)
	}

	alertErr := "timeout"
	readAt := certifiedAt
	alert := availabilityAlertToResponse(&db.AgentAvailabilityAlert{
		ID:                  uuid.New(),
		AgentID:             agentID,
		AlertType:           "availability_failed",
		Severity:            "critical",
		AvailabilityStatus:  "unreachable",
		ConsecutiveFailures: 4,
		Title:               "down",
		Message:             "failed",
		LastError:           &alertErr,
		RepairHints:         []string{"check endpoint"},
		ReadAt:              &readAt,
		CreatedAt:           certifiedAt,
		UpdatedAt:           createdAt,
	}, "dto-agent", "DTO Agent")
	if alert.AgentSlug != "dto-agent" || alert.ReadAt == nil || *alert.ReadAt != "2026-06-19T19:04:05Z" || alert.LastError == nil {
		t.Fatalf("unexpected alert response: %#v", alert)
	}
	rowAlert := availabilityAlertRowToResponse(&db.ListAgentAvailabilityAlertsByCreatorRow{
		AgentAvailabilityAlert: db.AgentAvailabilityAlert{
			ID:                  uuid.New(),
			AgentID:             agentID,
			AlertType:           "availability_recovered",
			Severity:            "info",
			AvailabilityStatus:  "healthy",
			ConsecutiveFailures: 0,
			Title:               "recovered",
			Message:             "ok",
			CreatedAt:           certifiedAt,
			UpdatedAt:           createdAt,
		},
		AgentSlug: "row-agent",
		AgentName: "Row Agent",
	})
	if rowAlert.AgentSlug != "row-agent" || rowAlert.AgentName != "Row Agent" || rowAlert.Type != "availability_recovered" {
		t.Fatalf("unexpected row alert response: %#v", rowAlert)
	}
	if got := stringPtrOrNil(""); got != nil {
		t.Fatalf("stringPtrOrNil(empty) = %#v, want nil", got)
	}
	if got := stringPtrOrNil("x"); got == nil || *got != "x" {
		t.Fatalf("stringPtrOrNil(non-empty) = %#v", got)
	}
	if got := formatOptionalTime(nil); got != nil {
		t.Fatalf("formatOptionalTime(nil) = %#v, want nil", got)
	}
}

func TestReadinessRuntimeRepairHintsAndAgentCardSigning(t *testing.T) {
	runAt := time.Date(2026, 6, 20, 1, 2, 3, 0, time.UTC)
	batchID := "bench-1"
	ready := readinessForAgent("ready-agent", "active", "public", "certified", availabilityResponse("healthy", &runAt, nil, &runAt, 0), 2, &batchID)
	if !ready.Listed || !ready.Discoverable || !ready.Callable || !ready.Verified || !ready.Certified || ready.PaidEnabled {
		t.Fatalf("unexpected ready flags: %#v", ready)
	}
	if ready.AgentCardURL != "/api/v1/agents/ready-agent/agent-card.json" || ready.A2AEndpoint != "/api/v1/a2a/agents/ready-agent" {
		t.Fatalf("unexpected ready endpoints: %#v", ready)
	}
	if ready.LatestBenchmarkBatchID == nil || *ready.LatestBenchmarkBatchID != batchID {
		t.Fatalf("unexpected benchmark id: %#v", ready.LatestBenchmarkBatchID)
	}

	private := readinessForAgent("", "active", "private", "unreviewed", availabilityResponse("degraded", &runAt, nil, nil, 1), 0, nil)
	if private.Listed || private.Discoverable || !private.Callable || private.AgentCardURL != "" || private.A2AEndpoint != "" {
		t.Fatalf("unexpected private readiness: %#v", private)
	}
	unlisted := readinessForAgent("quiet-agent", "active", "unlisted", "certified", availabilityResponse("unknown", nil, nil, nil, 0), 0, nil)
	if unlisted.Listed || !unlisted.Discoverable || unlisted.Callable || !unlisted.Certified {
		t.Fatalf("unexpected unlisted readiness: %#v", unlisted)
	}
	if unlisted.AgentCardURL != "/api/v1/agents/quiet-agent/agent-card.json" || unlisted.A2AEndpoint != "/api/v1/a2a/agents/quiet-agent" {
		t.Fatalf("unexpected unlisted machine endpoints: %#v", unlisted)
	}
	unreachable := readinessForAgent("down", "active", "public", "unreviewed", availabilityResponse("unreachable", &runAt, nil, nil, 3), 0, nil)
	if unreachable.Callable {
		t.Fatalf("unreachable agent with old success should not be callable: %#v", unreachable)
	}
	degraded := availabilityResponse("degraded", nil, &runAt, &runAt, 2)
	if degraded.Status != "degraded" || degraded.Label != "不稳定" || degraded.LastFailedRunAt == nil || degraded.LastCheckedAt == nil || degraded.ConsecutiveFailures != 2 {
		t.Fatalf("unexpected degraded availability: %#v", degraded)
	}
	unreachableAvailability := availabilityResponse("unreachable", nil, &runAt, nil, 5)
	if unreachableAvailability.Status != "unreachable" || unreachableAvailability.Label != "不可达" || unreachableAvailability.LastSuccessfulRunAt != nil {
		t.Fatalf("unexpected unreachable availability: %#v", unreachableAvailability)
	}
	if !isQueuedRuntimeConnectionMode(ConnectionModeRuntimePull) || !isQueuedRuntimeConnectionMode(ConnectionModeRuntimeWS) || isQueuedRuntimeConnectionMode(ConnectionModeDirectHTTP) {
		t.Fatalf("queued runtime connection mode detection failed")
	}

	for _, tc := range []struct {
		mode       string
		wantSignal string
	}{
		{mode: ConnectionModeDirectHTTP, wantSignal: "direct_endpoint_probe_and_run_result"},
		{mode: ConnectionModeRuntimePull, wantSignal: "runtime_pull_heartbeat_claim_result"},
		{mode: ConnectionModeRuntimeWS, wantSignal: "runtime_ws_socket_heartbeat_assignment_result"},
		{mode: ConnectionModeMCPServer, wantSignal: "mcp_tool_call_and_run_result"},
	} {
		if got := agentCardRuntimeExt(tc.mode); got.Adapter != "openlinker_a2a_proxy" || got.ConnectionMode != tc.mode || got.OnlineSignal != tc.wantSignal {
			t.Fatalf("agentCardRuntimeExt(%q) = %#v", tc.mode, got)
		}
	}

	if hints := repairHintsForDryRun(&db.Agent{ConnectionMode: ConnectionModeDirectHTTP}, ""); hints != nil {
		t.Fatalf("empty dry-run error should not produce hints: %#v", hints)
	}
	pullHints := repairHintsForDryRun(&db.Agent{ConnectionMode: ConnectionModeRuntimePull}, "schema timeout")
	if len(pullHints) < 4 || !strings.Contains(strings.Join(pullHints, " "), "schema") {
		t.Fatalf("runtime pull hints did not include mode/schema/timeout help: %#v", pullHints)
	}
	mcpHints := repairHintsForDryRun(&db.Agent{ConnectionMode: ConnectionModeMCPServer}, "bad")
	if len(mcpHints) == 0 || !strings.Contains(mcpHints[0], "MCP") {
		t.Fatalf("mcp hints = %#v", mcpHints)
	}

	seed := bytes.Repeat([]byte{7}, ed25519.SeedSize)
	t.Setenv("AGENT_CARD_SIGNING_SEED", hex.EncodeToString(seed))
	if got := agentCardSigningSeed(); !bytes.Equal(got, seed) {
		t.Fatalf("hex signing seed = %x, want %x", got, seed)
	}
	card := &AgentCardResponse{
		Name:        "Signed Agent",
		Description: "signed",
		URL:         "/api/v1/a2a/agents/signed",
		Version:     "v1",
		Provider:    AgentCardProvider{Organization: "OpenLinker"},
		OpenLinker:  AgentCardOpenLinkerExt{Slug: "signed"},
	}
	signAgentCard(card)
	if card.Signature == nil || card.Signature.Algorithm != "Ed25519" {
		t.Fatalf("signature missing: %#v", card.Signature)
	}
	sig := card.Signature
	unsigned := *card
	unsigned.Signature = nil
	payload, err := json.Marshal(&unsigned)
	if err != nil {
		t.Fatalf("marshal unsigned card: %v", err)
	}
	pub, err := base64.RawURLEncoding.DecodeString(sig.PublicKey)
	if err != nil {
		t.Fatalf("decode public key: %v", err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(sig.Signature)
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	if !ed25519.Verify(ed25519.PublicKey(pub), payload, signature) {
		t.Fatalf("agent card signature does not verify")
	}

	t.Setenv("AGENT_CARD_SIGNING_SEED", "")
	t.Setenv("OPENLINKER_AGENT_CARD_SIGNING_SEED", base64.RawURLEncoding.EncodeToString(seed))
	if got := agentCardSigningSeed(); !bytes.Equal(got, seed) {
		t.Fatalf("raw-url base64 seed = %x, want %x", got, seed)
	}
	t.Setenv("OPENLINKER_AGENT_CARD_SIGNING_SEED", base64.StdEncoding.EncodeToString(seed))
	if got := agentCardSigningSeed(); !bytes.Equal(got, seed) {
		t.Fatalf("standard base64 seed = %x, want %x", got, seed)
	}
	t.Setenv("OPENLINKER_AGENT_CARD_SIGNING_SEED", "")
	t.Setenv("JWT_SECRET", "fallback-secret")
	if got := agentCardSigningSeed(); len(got) != ed25519.SeedSize {
		t.Fatalf("fallback hashed seed length = %d", len(got))
	}
	t.Setenv("JWT_SECRET", "")
	if got := agentCardSigningSeed(); got != nil {
		t.Fatalf("empty seed = %#v, want nil", got)
	}
}

func TestRegistrationApprovalAndMetricHelpers(t *testing.T) {
	slug := deriveSlug(" My Agent!! ")
	if !strings.HasPrefix(slug, "my-agent-") || len(slug) != len("my-agent-")+6 || !isValidSlug(slug) {
		t.Fatalf("deriveSlug ASCII = %q", slug)
	}
	fallbackSlug := deriveSlug("中文")
	if !strings.HasPrefix(fallbackSlug, "agent-") || len(fallbackSlug) != len("agent-")+6 || !isValidSlug(fallbackSlug) {
		t.Fatalf("deriveSlug fallback = %q", fallbackSlug)
	}
	longSlug := deriveSlug(strings.Repeat("a", 120))
	if len(longSlug) > maxSlugLen || !isValidSlug(longSlug) {
		t.Fatalf("deriveSlug long = %q", longSlug)
	}
	randomSuffix := randomHex6()
	if len(randomSuffix) != 6 {
		t.Fatalf("randomHex6 length = %d", len(randomSuffix))
	}
	if _, err := hex.DecodeString(randomSuffix); err != nil {
		t.Fatalf("randomHex6 returned non-hex %q: %v", randomSuffix, err)
	}

	regSvc := &RegistrationService{}
	if _, err := regSvc.verifyBootstrapToken(context.Background(), "bad-token"); err == nil {
		t.Fatalf("verifyBootstrapToken should reject malformed token before database lookup")
	} else {
		requireHTTPStatus(t, err, http.StatusUnauthorized)
	}
	if got, err := regSvc.normalizeRegistrationSkillIDs(context.Background(), nil); err != nil || len(got) != 0 {
		t.Fatalf("normalizeRegistrationSkillIDs empty = %#v, %v", got, err)
	}
	if got, err := regSvc.normalizeRegistrationSkillIDs(context.Background(), []string{" ", ""}); err != nil || len(got) != 0 {
		t.Fatalf("normalizeRegistrationSkillIDs blank = %#v, %v", got, err)
	}
	tooManySkills := []string{"skill/1", "skill/2", "skill/3", "skill/4", "skill/5", "skill/6"}
	if _, err := regSvc.normalizeRegistrationSkillIDs(context.Background(), tooManySkills); err == nil {
		t.Fatalf("normalizeRegistrationSkillIDs should reject too many skills before database lookup")
	} else {
		requireHTTPStatus(t, err, http.StatusBadRequest)
	}

	now := time.Date(2026, 6, 20, 8, 9, 10, 0, time.UTC)
	revokedAt := now.Add(time.Minute)
	lastUsedAt := now.Add(2 * time.Minute)
	tokenResp := bootstrapTokenResponse(db.AgentRegistrationToken{
		ID:         uuid.New(),
		Label:      "bootstrap",
		Prefix:     "sk_live_test",
		MaxAgents:  2,
		UsedCount:  1,
		ExpiresAt:  now,
		RevokedAt:  &revokedAt,
		LastUsedAt: &lastUsedAt,
		CreatedAt:  now.Add(-time.Hour),
	})
	if tokenResp.RevokedAt == nil || tokenResp.LastUsedAt == nil || tokenResp.ExpiresAt != "2026-06-20T08:09:10Z" {
		t.Fatalf("unexpected bootstrapTokenResponse: %#v", tokenResp)
	}

	if got := normalizeNote("  approved  "); got == nil || *got != "approved" {
		t.Fatalf("normalizeNote(non-empty) = %#v", got)
	}
	if got := normalizeNote(" \n "); got != nil {
		t.Fatalf("normalizeNote(empty) = %#v, want nil", got)
	}
	approvalSvc := &ApprovalService{cfg: &config.Config{FrontendURL: "https://ui.example/"}}
	if got := approvalSvc.approvalURL("abc"); got != "https://ui.example/hub/approvals/abc" {
		t.Fatalf("approvalURL configured = %q", got)
	}
	if got := (&ApprovalService{}).approvalURL("abc"); got != "https://openlinker.ai/hub/approvals/abc" {
		t.Fatalf("approvalURL default = %q", got)
	}
	approvalID := uuid.New()
	requester := uuid.New()
	tokenID := uuid.New()
	decider := uuid.New()
	decisionNote := "looks good"
	decisionAt := now.Add(3 * time.Minute)
	approvalResp := approvalSvc.toApprovalResponse(&db.AgentActionApprovalRequest{
		ID:                 approvalID,
		AgentID:            uuid.New(),
		RequestedByUserID:  &requester,
		RequestedByTokenID: &tokenID,
		Action:             "set-visibility-public",
		PayloadJSON:        []byte(`{"visibility":"public"}`),
		Status:             "confirmed",
		ApprovalURLSlug:    "abc",
		ExpiresAt:          now,
		DecidedAt:          &decisionAt,
		DecidedByUserID:    &decider,
		DecisionNote:       &decisionNote,
		CreatedAt:          now.Add(-time.Minute),
	})
	if approvalResp.ID != approvalID.String() || approvalResp.ApprovalURL != "https://ui.example/hub/approvals/abc" || approvalResp.Payload["visibility"] != "public" {
		t.Fatalf("unexpected approval response: %#v", approvalResp)
	}
	if approvalResp.DecidedAt == nil || approvalResp.DecisionNote == nil || approvalResp.RequestedByTokenID == nil {
		t.Fatalf("approval optional fields missing: %#v", approvalResp)
	}
	slugPart, err := generateApprovalSlug()
	if err != nil {
		t.Fatalf("generateApprovalSlug error: %v", err)
	}
	if len(slugPart) != approvalSlugRandomLen*2 {
		t.Fatalf("generateApprovalSlug length = %d", len(slugPart))
	}
	if _, err := hex.DecodeString(slugPart); err != nil {
		t.Fatalf("generateApprovalSlug non-hex %q: %v", slugPart, err)
	}

	if len(metricWindows) != 3 || metricWindows[0].label != "24h" || metricWindows[2].interval != "30 days" {
		t.Fatalf("unexpected metric windows: %#v", metricWindows)
	}
	runMetricTick(context.Background(), nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	StartMetricWorker(ctx, nil, nil)
	StartAvailabilityMonitor(ctx, nil, AvailabilityMonitorConfig{})
}

func TestMarketQueryParsingAndRouteRegistration(t *testing.T) {
	if got := parseTagsParam(" ai, data,ai, ,ops "); !reflect.DeepEqual(got, []string{"ai", "data", "ops"}) {
		t.Fatalf("parseTagsParam = %#v", got)
	}
	if got := parseTagsParam(" , "); got != nil {
		t.Fatalf("parseTagsParam(empty) = %#v, want nil", got)
	}
	if got := parseInt32QueryDefault("12", 3); got != 12 {
		t.Fatalf("parseInt32QueryDefault valid = %d", got)
	}
	if got := parseInt32QueryDefault("", 3); got != 3 {
		t.Fatalf("parseInt32QueryDefault empty = %d", got)
	}
	if got := parseInt32QueryDefault("-1", 3); got != 3 {
		t.Fatalf("parseInt32QueryDefault invalid = %d", got)
	}
	if got := parseInt32QueryDefault("999999999999", 3); got != 3 {
		t.Fatalf("parseInt32QueryDefault overflow = %d", got)
	}
	for _, raw := range []string{"1", "true", "YES", "y", "on"} {
		if !parseBoolQuery(raw) {
			t.Fatalf("parseBoolQuery(%q) = false", raw)
		}
	}
	if parseBoolQuery("no") {
		t.Fatalf("parseBoolQuery(no) = true")
	}
	if got := firstHeaderValue(" https , http "); got != "https" {
		t.Fatalf("firstHeaderValue = %q", got)
	}
	if got := inferSkillDocWebBase("https://api.example.com:8080"); got != "https://example.com" {
		t.Fatalf("inferSkillDocWebBase api host = %q", got)
	}
	if got := inferSkillDocWebBase("http://localhost:8080"); got != "http://localhost:3000" {
		t.Fatalf("inferSkillDocWebBase localhost = %q", got)
	}
	eOrigin := echo.New()
	reqOrigin := httptest.NewRequest(http.MethodGet, "http://internal.local/skill/publish-agent", nil)
	reqOrigin.Header.Set("X-Forwarded-Proto", "https, http")
	reqOrigin.Header.Set("X-Forwarded-Host", "api.example.com, internal.local")
	if got := requestOrigin(eOrigin.NewContext(reqOrigin, httptest.NewRecorder())); got != "https://api.example.com" {
		t.Fatalf("requestOrigin forwarded = %q", got)
	}
	t.Setenv("API_URL", "https://api.stage.example/")
	t.Setenv("FRONTEND_URL", "")
	apiBase, webBase := skillDocBaseURLs(eOrigin.NewContext(reqOrigin, httptest.NewRecorder()))
	if apiBase != "https://api.stage.example" || webBase != "https://stage.example" {
		t.Fatalf("skillDocBaseURLs inferred = %q %q", apiBase, webBase)
	}

	e := echo.New()
	api := e.Group("/api/v1")
	noop := func(next echo.HandlerFunc) echo.HandlerFunc { return next }
	NewHandler(nil).Register(api)
	NewHandler(nil).RegisterProtected(api, noop)
	NewHandler(nil).RegisterAdmin(api, noop, noop)
	NewRegistrationHandler(nil).RegisterPublic(api)
	NewRegistrationHandler(nil).RegisterProtected(api, noop)
	NewApprovalHandler(nil).RegisterProtected(api, noop)
	NewMarketHandler(nil).Register(api)
	NewMarketHandler(nil).RegisterProtected(api, noop)
	NewMetricHandler(nil).Register(api)

	routes := map[string]bool{}
	for _, route := range e.Routes() {
		routes[route.Method+" "+route.Path] = true
	}
	for _, route := range []string{
		"GET /api/v1/agents/check-slug",
		"POST /api/v1/me/become-creator",
		"POST /api/v1/creator/agents",
		"PATCH /api/v1/creator/agents/:id/visibility",
		"POST /api/v1/creator/agents/:id/dry-run",
		"POST /api/v1/admin/agents/:id/certify",
		"POST /api/v1/agent-registration/agents",
		"DELETE /api/v1/creator/agent-registration-tokens/:id",
		"POST /api/v1/creator/approvals/:id/confirm",
		"GET /api/v1/agents/:slug/agent-card.json",
		"GET /api/v1/agents/:slug/agent-card.extended.json",
		"GET /api/v1/creator/agents/by-slug/:slug",
		"GET /api/v1/agents/:id/metrics",
	} {
		if !routes[route] {
			t.Fatalf("missing registered route %s; got %#v", route, routes)
		}
	}
}

func TestAgentHandlersValidateBeforeServiceDispatch(t *testing.T) {
	h := NewHandler(nil)
	userID := uuid.NewString()
	agentID := uuid.NewString()
	exampleID := uuid.NewString()
	alertID := uuid.NewString()

	for _, tc := range []struct {
		name   string
		method func(echo.Context) error
		req    *handlerRequest
		want   int
	}{
		{name: "become creator missing user", method: h.BecomeCreator, req: &handlerRequest{method: http.MethodPost, target: "/"}, want: http.StatusUnauthorized},
		{name: "create invalid json", method: h.CreateAgent, req: &handlerRequest{method: http.MethodPost, target: "/", body: "{", userID: userID}, want: http.StatusBadRequest},
		{name: "create validation", method: h.CreateAgent, req: &handlerRequest{method: http.MethodPost, target: "/", body: `{}`, userID: userID}, want: http.StatusUnprocessableEntity},
		{name: "update invalid id", method: h.UpdateAgent, req: &handlerRequest{method: http.MethodPatch, target: "/bad", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "update invalid json", method: h.UpdateAgent, req: &handlerRequest{method: http.MethodPatch, target: "/" + agentID, body: "{", userID: userID, params: map[string]string{"id": agentID}}, want: http.StatusBadRequest},
		{name: "visibility validation", method: h.UpdateVisibility, req: &handlerRequest{method: http.MethodPatch, target: "/" + agentID, body: `{}`, userID: userID, params: map[string]string{"id": agentID}}, want: http.StatusUnprocessableEntity},
		{name: "disable invalid id", method: h.DisableAgent, req: &handlerRequest{method: http.MethodDelete, target: "/bad", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "onboarding invalid id", method: h.GetAgentOnboarding, req: &handlerRequest{method: http.MethodGet, target: "/bad", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "capability invalid json", method: h.UpsertCapability, req: &handlerRequest{method: http.MethodPut, target: "/" + agentID, body: "{", userID: userID, params: map[string]string{"id": agentID}}, want: http.StatusBadRequest},
		{name: "capability validation", method: h.UpsertCapability, req: &handlerRequest{method: http.MethodPut, target: "/" + agentID, body: `{}`, userID: userID, params: map[string]string{"id": agentID}}, want: http.StatusUnprocessableEntity},
		{name: "example validation", method: h.CreateExample, req: &handlerRequest{method: http.MethodPost, target: "/" + agentID + "/examples", body: `{}`, userID: userID, params: map[string]string{"id": agentID}}, want: http.StatusUnprocessableEntity},
		{name: "delete example invalid example id", method: h.DeleteExample, req: &handlerRequest{method: http.MethodDelete, target: "/" + agentID + "/examples/bad", userID: userID, params: map[string]string{"id": agentID, "exampleID": "bad"}}, want: http.StatusBadRequest},
		{name: "dry run invalid id", method: h.RunDryRun, req: &handlerRequest{method: http.MethodPost, target: "/bad/dry-run", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "health check invalid id", method: h.RunHealthCheck, req: &handlerRequest{method: http.MethodPost, target: "/bad/health-check", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "availability alerts bad limit", method: h.ListAvailabilityAlerts, req: &handlerRequest{method: http.MethodGet, target: "/alerts?limit=bad", userID: userID}, want: http.StatusBadRequest},
		{name: "mark alert invalid id", method: h.MarkAvailabilityAlertRead, req: &handlerRequest{method: http.MethodPost, target: "/alerts/bad/read", userID: userID, params: map[string]string{"alertID": "bad"}}, want: http.StatusBadRequest},
		{name: "request certification invalid id", method: h.RequestCertification, req: &handlerRequest{method: http.MethodPost, target: "/bad/request-certification", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "certify invalid id", method: h.CertifyAgent, req: &handlerRequest{method: http.MethodPost, target: "/bad/certify", params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "reject invalid json", method: h.RejectCertification, req: &handlerRequest{method: http.MethodPost, target: "/" + agentID + "/reject", body: "{", params: map[string]string{"id": agentID}}, want: http.StatusBadRequest},
		{name: "reject validation", method: h.RejectCertification, req: &handlerRequest{method: http.MethodPost, target: "/" + agentID + "/reject", body: `{}`, params: map[string]string{"id": agentID}}, want: http.StatusUnprocessableEntity},
		{name: "mark alert valid user before bad uuid", method: h.MarkAvailabilityAlertRead, req: &handlerRequest{method: http.MethodPost, target: "/alerts/" + alertID + "/read", userID: "not-a-uuid", params: map[string]string{"alertID": alertID}}, want: http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newHandlerContext(tc.req)
			requireHTTPStatus(t, tc.method(c), tc.want)
		})
	}

	c := newHandlerContext(&handlerRequest{method: http.MethodGet, target: "/", userID: userID})
	gotUser, err := userIDFromCtx(c)
	if err != nil || gotUser.String() != userID {
		t.Fatalf("userIDFromCtx valid = %s, %v", gotUser, err)
	}
	c = newHandlerContext(&handlerRequest{method: http.MethodGet, target: "/" + exampleID, params: map[string]string{"exampleID": exampleID}})
	gotID, err := pathUUID(c, "exampleID")
	if err != nil || gotID.String() != exampleID {
		t.Fatalf("pathUUID valid = %s, %v", gotID, err)
	}
}

func TestRegistrationApprovalAndMetricHandlersValidateBeforeServiceDispatch(t *testing.T) {
	reg := NewRegistrationHandler(nil)
	approval := NewApprovalHandler(nil)
	metric := NewMetricHandler(nil)
	userID := uuid.NewString()
	id := uuid.NewString()

	for _, tc := range []struct {
		name   string
		method func(echo.Context) error
		req    *handlerRequest
		want   int
	}{
		{name: "mint missing user", method: reg.MintBootstrapToken, req: &handlerRequest{method: http.MethodPost, target: "/"}, want: http.StatusUnauthorized},
		{name: "mint invalid json", method: reg.MintBootstrapToken, req: &handlerRequest{method: http.MethodPost, target: "/", body: "{", userID: userID}, want: http.StatusBadRequest},
		{name: "mint validation", method: reg.MintBootstrapToken, req: &handlerRequest{method: http.MethodPost, target: "/", body: `{}`, userID: userID}, want: http.StatusUnprocessableEntity},
		{name: "revoke invalid id", method: reg.RevokeBootstrapToken, req: &handlerRequest{method: http.MethodDelete, target: "/bad", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "register invalid json", method: reg.RegisterAgentViaBootstrap, req: &handlerRequest{method: http.MethodPost, target: "/", body: "{"}, want: http.StatusBadRequest},
		{name: "register validation", method: reg.RegisterAgentViaBootstrap, req: &handlerRequest{method: http.MethodPost, target: "/", body: `{"name":"Agent Name","ability_tags":["ai"]}`}, want: http.StatusUnprocessableEntity},
		{name: "create approval missing user", method: approval.CreateApproval, req: &handlerRequest{method: http.MethodPost, target: "/"}, want: http.StatusUnauthorized},
		{name: "create approval invalid json", method: approval.CreateApproval, req: &handlerRequest{method: http.MethodPost, target: "/", body: "{", userID: userID}, want: http.StatusBadRequest},
		{name: "create approval validation", method: approval.CreateApproval, req: &handlerRequest{method: http.MethodPost, target: "/", body: `{}`, userID: userID}, want: http.StatusUnprocessableEntity},
		{name: "get approval invalid id", method: approval.GetApproval, req: &handlerRequest{method: http.MethodGet, target: "/bad", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "confirm approval invalid id", method: approval.ConfirmApproval, req: &handlerRequest{method: http.MethodPost, target: "/bad/confirm", userID: userID, params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
		{name: "confirm approval invalid json", method: approval.ConfirmApproval, req: &handlerRequest{method: http.MethodPost, target: "/" + id + "/confirm", body: "{", userID: userID, params: map[string]string{"id": id}}, want: http.StatusBadRequest},
		{name: "confirm approval note validation", method: approval.ConfirmApproval, req: &handlerRequest{method: http.MethodPost, target: "/" + id + "/confirm", body: `{"note":"` + strings.Repeat("x", 501) + `"}`, userID: userID, params: map[string]string{"id": id}}, want: http.StatusUnprocessableEntity},
		{name: "reject approval note validation", method: approval.RejectApproval, req: &handlerRequest{method: http.MethodPost, target: "/" + id + "/reject", body: `{"note":"` + strings.Repeat("x", 501) + `"}`, userID: userID, params: map[string]string{"id": id}}, want: http.StatusUnprocessableEntity},
		{name: "metric invalid id", method: metric.GetMetrics, req: &handlerRequest{method: http.MethodGet, target: "/bad/metrics", params: map[string]string{"id": "bad"}}, want: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newHandlerContext(tc.req)
			requireHTTPStatus(t, tc.method(c), tc.want)
		})
	}
}

type handlerRequest struct {
	method string
	target string
	body   string
	userID string
	params map[string]string
}

func newHandlerContext(reqSpec *handlerRequest) echo.Context {
	method := reqSpec.method
	if method == "" {
		method = http.MethodGet
	}
	req := httptest.NewRequest(method, reqSpec.target, strings.NewReader(reqSpec.body))
	if reqSpec.body != "" {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)
	if reqSpec.userID != "" {
		c.Set(string(httpx.CtxKeyUserID), reqSpec.userID)
	}
	if len(reqSpec.params) > 0 {
		names := make([]string, 0, len(reqSpec.params))
		values := make([]string, 0, len(reqSpec.params))
		for name, value := range reqSpec.params {
			names = append(names, name)
			values = append(values, value)
		}
		c.SetParamNames(names...)
		c.SetParamValues(values...)
	}
	return c
}

func requireHTTPStatus(t *testing.T, err error, want int) {
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

type sqlStateErr struct {
	state string
}

func (e sqlStateErr) Error() string {
	return e.state
}

func (e sqlStateErr) SQLState() string {
	return e.state
}
