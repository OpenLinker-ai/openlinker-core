package runtime

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	corecontracts "github.com/OpenLinker-ai/openlinker-core/contracts"
	"github.com/stretchr/testify/require"
)

type runtimeContractSchemaRef struct {
	Ref string `json:"$ref"`
}

type runtimeContractManifest struct {
	Schema            string   `json:"$schema"`
	Name              string   `json:"name"`
	Scope             string   `json:"scope"`
	Version           string   `json:"version"`
	RuntimeContractID string   `json:"runtime_contract_id"`
	ProtocolVersion   int      `json:"protocol_version"`
	WireFormat        string   `json:"wire_format"`
	RequiredFeatures  []string `json:"required_features"`
	Limits            struct {
		MaxNonArtifactMessageBytes int `json:"max_non_artifact_message_bytes"`
		OfferTTLSeconds            int `json:"offer_ttl_seconds"`
		LeaseTTLSeconds            int `json:"lease_ttl_seconds"`
		LeaseRenewIntervalSeconds  int `json:"lease_renew_interval_seconds"`
		HelloTimeoutSeconds        int `json:"hello_timeout_seconds"`
		MaxPullWaitSeconds         int `json:"max_pull_wait_seconds"`
	} `json:"limits"`
	WebSocket struct {
		Path           string                   `json:"path"`
		Auth           string                   `json:"auth"`
		EnvelopeSchema runtimeContractSchemaRef `json:"envelope_schema"`
		CloseCodes     map[string]string        `json:"close_codes"`
		Messages       []struct {
			Type            string                   `json:"type"`
			Direction       string                   `json:"direction"`
			ExpectsReply    *bool                    `json:"expects_reply,omitempty"`
			ReplyToRequired *bool                    `json:"reply_to_required,omitempty"`
			Schema          runtimeContractSchemaRef `json:"schema"`
		} `json:"messages"`
	} `json:"websocket"`
	Endpoints []struct {
		ClientMethod          string                     `json:"client_method"`
		HTTPMethod            string                     `json:"http_method"`
		Path                  string                     `json:"path"`
		Query                 map[string]json.RawMessage `json:"query,omitempty"`
		RequiredHeaders       []string                   `json:"required_headers,omitempty"`
		RequestBodySchema     *runtimeContractSchemaRef  `json:"request_body_schema,omitempty"`
		SuccessResponseSchema runtimeContractSchemaRef   `json:"success_response_schema"`
		EmptyResponseStatus   *int                       `json:"empty_response_status,omitempty"`
		ErrorResponseSchema   runtimeContractSchemaRef   `json:"error_response_schema"`
	} `json:"endpoints"`
	LegacyRoutes []struct {
		HTTPMethod     string `json:"http_method"`
		Path           string `json:"path"`
		ResponseStatus int    `json:"response_status"`
		ErrorCode      string `json:"error_code"`
	} `json:"legacy_routes"`
	StableErrorCodes []string                   `json:"stable_error_codes"`
	Definitions      map[string]json.RawMessage `json:"$defs"`
}

func TestRuntimeV2ContractMatchesExportedConstants(t *testing.T) {
	contract := decodeRuntimeContract(t)

	require.Equal(t, "https://json-schema.org/draft/2020-12/schema", contract.Schema)
	require.Equal(t, "openlinker-runtime", contract.Name)
	require.Equal(t, "core-runtime", contract.Scope)
	require.Equal(t, "v2", contract.Version)
	require.Equal(t, RuntimeContractID, contract.RuntimeContractID)
	require.Equal(t, RuntimeProtocolVersion, contract.ProtocolVersion)
	require.Equal(t, "application/json", contract.WireFormat)
	require.Equal(t, RuntimeRequiredFeatures(), contract.RequiredFeatures)
	require.Equal(t, 4<<20, contract.Limits.MaxNonArtifactMessageBytes)
	require.Equal(t, 30, contract.Limits.OfferTTLSeconds)
	require.Equal(t, 60, contract.Limits.LeaseTTLSeconds)
	require.Equal(t, 20, contract.Limits.LeaseRenewIntervalSeconds)
	require.Equal(t, 5, contract.Limits.HelloTimeoutSeconds)
	require.Equal(t, 30, contract.Limits.MaxPullWaitSeconds)

	digest := sha256.Sum256(corecontracts.RuntimeV2)
	require.Equal(t, RuntimeContractDigest, fmt.Sprintf("%x", digest))
}

func TestRuntimeV2ContractCoversWireProtocol(t *testing.T) {
	contract := decodeRuntimeContract(t)

	require.Equal(t, "/api/v1/agent-runtime/v2/ws", contract.WebSocket.Path)
	require.Equal(t, "agent_principal_and_node_device", contract.WebSocket.Auth)
	require.Equal(t, "#/$defs/RuntimeMessage", contract.WebSocket.EnvelopeSchema.Ref)
	require.Equal(t, map[string]string{
		"4401": "AUTHENTICATION_FAILED",
		"4406": "RUNTIME_CLIENT_UPGRADE_REQUIRED",
		"4409": "RUNTIME_SESSION_CONFLICT",
		"4412": "RUNTIME_REQUIRED_FEATURE_MISSING",
	}, contract.WebSocket.CloseCodes)

	messageTypes := make([]string, 0, len(contract.WebSocket.Messages))
	for _, message := range contract.WebSocket.Messages {
		require.NotEmpty(t, message.Direction, message.Type)
		requireDefinitionRef(t, contract.Definitions, message.Schema.Ref)
		messageTypes = append(messageTypes, message.Type)
	}
	requireUniqueStrings(t, "message type", messageTypes)
	require.ElementsMatch(t, []string{
		"runtime.hello",
		"runtime.ready",
		"run.assigned",
		"run.assignment.ack",
		"run.assignment.confirmed",
		"run.assignment.reject",
		"run.assignment.rejected",
		"run.lease.renew",
		"run.lease.renewed",
		"run.event",
		"run.event.ack",
		"run.result",
		"run.result.ack",
		"run.cancel",
		"run.cancel.ack",
		"runtime.resume",
		"run.resume.accepted",
		"run.lease.revoked",
		"runtime.drain",
		"runtime.error",
	}, messageTypes)

	endpointKeys := make([]string, 0, len(contract.Endpoints))
	for _, endpoint := range contract.Endpoints {
		require.NotEmpty(t, endpoint.ClientMethod)
		require.True(t, strings.HasPrefix(endpoint.Path, "/api/v1/agent-runtime/v2/"), endpoint.Path)
		requireDefinitionRef(t, contract.Definitions, endpoint.SuccessResponseSchema.Ref)
		requireDefinitionRef(t, contract.Definitions, endpoint.ErrorResponseSchema.Ref)
		if endpoint.RequestBodySchema != nil {
			requireDefinitionRef(t, contract.Definitions, endpoint.RequestBodySchema.Ref)
		}
		endpointKeys = append(endpointKeys, endpoint.HTTPMethod+" "+endpoint.Path)
	}
	requireUniqueStrings(t, "endpoint", endpointKeys)
	require.ElementsMatch(t, []string{
		"POST /api/v1/agent-runtime/v2/sessions",
		"POST /api/v1/agent-runtime/v2/runs/claim",
		"POST /api/v1/agent-runtime/v2/runs/{id}/assignment-ack",
		"POST /api/v1/agent-runtime/v2/runs/{id}/assignment-reject",
		"POST /api/v1/agent-runtime/v2/runs/{id}/lease-renew",
		"POST /api/v1/agent-runtime/v2/runs/{id}/events",
		"POST /api/v1/agent-runtime/v2/runs/{id}/result",
		"POST /api/v1/agent-runtime/v2/runs/resume",
		"POST /api/v1/agent-runtime/v2/runs/{id}/cancel-ack",
		"GET /api/v1/agent-runtime/v2/commands",
		"POST /api/v1/agent-runtime/v2/call-agent",
	}, endpointKeys)
}

func TestRuntimeV2ContractDefinesRecoveryAndCancellation(t *testing.T) {
	contract := decodeRuntimeContract(t)
	for _, definition := range []string{
		"AttemptIdentity",
		"EventRange",
		"RuntimeError",
		"RunResultPayload",
		"RunResultAckPayload",
		"RunCancelPayload",
		"RunCancelAckPayload",
		"RunCancellationState",
		"ResumeAttempt",
		"RuntimeResumePayload",
		"RunResumeAcceptedPayload",
		"PendingCommand",
		"RuntimeCommandsResponse",
	} {
		require.Contains(t, contract.Definitions, definition)
	}

	var cancelState struct {
		Enum []string `json:"enum"`
	}
	require.NoError(t, json.Unmarshal(contract.Definitions["CancelState"], &cancelState))
	require.Equal(t, []string{
		"requested",
		"delivered",
		"stopping",
		"stopped",
		"unsupported",
		"failed",
		"unconfirmed",
	}, cancelState.Enum)

	var resumeDecision struct {
		Enum []string `json:"enum"`
	}
	require.NoError(t, json.Unmarshal(contract.Definitions["ResumeDecision"], &resumeDecision))
	require.Equal(t, []string{
		"continue_execution",
		"upload_spool_only",
		"result_already_acked",
		"lease_revoked",
	}, resumeDecision.Enum)

	var runtimeError struct {
		Properties struct {
			Code struct {
				Enum []string `json:"enum"`
			} `json:"code"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(contract.Definitions["RuntimeErrorBody"], &runtimeError))
	require.Equal(t, contract.StableErrorCodes, runtimeError.Properties.Code.Enum)
	requireUniqueStrings(t, "stable error code", contract.StableErrorCodes)

	var sessionQuery struct {
		Type     string `json:"type"`
		Format   string `json:"format"`
		Required bool   `json:"required"`
	}
	for _, endpoint := range contract.Endpoints {
		if endpoint.Path != "/api/v1/agent-runtime/v2/commands" {
			continue
		}
		raw, ok := endpoint.Query["runtime_session_id"]
		require.True(t, ok)
		require.NoError(t, json.Unmarshal(raw, &sessionQuery))
	}
	require.Equal(t, "string", sessionQuery.Type)
	require.Equal(t, "uuid", sessionQuery.Format)
	require.True(t, sessionQuery.Required)
}

func TestRuntimeV2ContractRejectsLegacyRoutes(t *testing.T) {
	contract := decodeRuntimeContract(t)
	keys := make([]string, 0, len(contract.LegacyRoutes))
	for _, route := range contract.LegacyRoutes {
		require.Equal(t, 426, route.ResponseStatus)
		require.Equal(t, "RUNTIME_CLIENT_UPGRADE_REQUIRED", route.ErrorCode)
		keys = append(keys, route.HTTPMethod+" "+route.Path)
	}
	requireUniqueStrings(t, "legacy route", keys)
	require.ElementsMatch(t, []string{
		"POST /api/v1/agent-runtime/heartbeat",
		"GET /api/v1/agent-runtime/runs/claim",
		"POST /api/v1/agent-runtime/runs/{id}/result",
		"GET /api/v1/agent-runtime/ws",
		"POST /api/v1/agent-runtime/call-agent",
	}, keys)
}

func TestRuntimeV2ContractReferencesExistingDefinitions(t *testing.T) {
	contract := decodeRuntimeContract(t)
	var document any
	require.NoError(t, json.Unmarshal(corecontracts.RuntimeV2, &document))
	var refs []string
	collectContractRefs(document, &refs)
	require.NotEmpty(t, refs)
	for _, ref := range refs {
		requireDefinitionRef(t, contract.Definitions, ref)
	}
}

func TestRuntimeRequiredFeaturesReturnsCopy(t *testing.T) {
	features := RuntimeRequiredFeatures()
	features[0] = "mutated"
	require.Equal(t, "lease_fence", RuntimeRequiredFeatures()[0])
}

func decodeRuntimeContract(t *testing.T) runtimeContractManifest {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(corecontracts.RuntimeV2))
	decoder.DisallowUnknownFields()
	var contract runtimeContractManifest
	require.NoError(t, decoder.Decode(&contract))
	require.NotEmpty(t, contract.Definitions)
	return contract
}

func collectContractRefs(value any, refs *[]string) {
	switch typed := value.(type) {
	case map[string]any:
		if ref, ok := typed["$ref"].(string); ok {
			*refs = append(*refs, ref)
		}
		for _, item := range typed {
			collectContractRefs(item, refs)
		}
	case []any:
		for _, item := range typed {
			collectContractRefs(item, refs)
		}
	}
}

func requireDefinitionRef(t *testing.T, definitions map[string]json.RawMessage, ref string) {
	t.Helper()
	require.True(t, strings.HasPrefix(ref, "#/$defs/"), ref)
	_, ok := definitions[strings.TrimPrefix(ref, "#/$defs/")]
	require.True(t, ok, ref)
}

func requireUniqueStrings(t *testing.T, label string, values []string) {
	t.Helper()
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	for index := 1; index < len(sorted); index++ {
		require.NotEqual(t, sorted[index-1], sorted[index], "%s %q appears more than once", label, sorted[index])
	}
}
