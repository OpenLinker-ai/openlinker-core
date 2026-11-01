package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/endpointurl"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

type normalizedRunCreation struct {
	request                *RunRequest
	agentID                uuid.UUID
	idempotencyKeyHash     []byte
	idempotencyFingerprint []byte
}

// LookupRunByCreationRequest performs the read-only half of Run creation idempotency.
// It deliberately resolves the immutable user/key/fingerprint identity before
// any mutable Agent eligibility lookup, so retiring an Agent cannot invalidate
// a committed Run. A missing identity is reported without creating a Run or
// consulting the Agent registry.
func (s *Service) LookupRunByCreationRequest(
	ctx context.Context,
	userID uuid.UUID,
	req *RunRequest,
	source string,
) (*RunResponse, bool, error) {
	if source == "" {
		source = "web"
	}
	switch source {
	case "web", "mcp", "api":
	default:
		return nil, false, httpx.BadRequest("source 取值非法")
	}

	normalized, err := s.normalizeRunCreation(req, source, createRunOptions{})
	if err != nil {
		return nil, false, err
	}
	runID, found, err := s.findExistingRunByIdentity(
		ctx,
		userID,
		normalized.idempotencyKeyHash,
		normalized.idempotencyFingerprint,
	)
	if err != nil {
		var httpErr *httpx.HTTPError
		if errors.As(err, &httpErr) {
			return nil, false, err
		}
		log.Error().Err(err).Str("user_id", userID.String()).Msg("runtime.LookupRunByCreationRequest: idempotency lookup")
		return nil, false, httpx.Internal("查询幂等调用记录失败")
	}
	if !found {
		return nil, false, nil
	}

	resp, err := s.idempotencyReplayResponse(ctx, userID, runID)
	if err != nil {
		return nil, true, err
	}
	return resp, true, nil
}

func (s *Service) findExistingRunByIdentity(
	ctx context.Context,
	userID uuid.UUID,
	keyHash, fingerprint []byte,
) (uuid.UUID, bool, error) {
	record, err := s.queries.GetRunIdempotencyRecord(ctx, db.GetRunIdempotencyRecordParams{
		UserID:             userID,
		IdempotencyKeyHash: keyHash,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, false, nil
		}
		return uuid.Nil, false, err
	}
	if len(record.IdempotencyFingerprint) != sha256.Size ||
		subtle.ConstantTimeCompare(record.IdempotencyFingerprint, fingerprint) != 1 {
		return uuid.Nil, false, idempotencyHTTPError(&IdempotencyError{Class: IdempotencyErrorKeyReused})
	}
	return record.ID, true, nil
}

func (s *Service) idempotencyReplayResponse(ctx context.Context, userID, runID uuid.UUID) (*RunResponse, error) {
	resp, err := s.GetRunUnprojected(ctx, userID, runID)
	if err != nil {
		return nil, err
	}
	resp.Replayed = true
	return resp, nil
}

func (s *Service) normalizeRunCreation(req *RunRequest, source string, opts createRunOptions) (*normalizedRunCreation, error) {
	if req == nil {
		return nil, httpx.Unprocessable("请求体不能为空")
	}

	agentID, err := uuid.Parse(strings.TrimSpace(req.AgentID))
	if err != nil {
		return nil, httpx.BadRequest("agent_id 不是合法 UUID")
	}

	keyHash, err := HashIdempotencyKey(req.IdempotencyKey)
	if err != nil {
		return nil, idempotencyHTTPError(err)
	}

	input, err := normalizeIJSONObject(req.Input, true)
	if err != nil {
		return nil, idempotencyHTTPError(err)
	}
	metadata, err := normalizeIJSONObject(req.Metadata, false)
	if err != nil {
		return nil, idempotencyHTTPError(err)
	}
	creationOptions, err := normalizeIJSONObject(req.CreationOptions, false)
	if err != nil {
		return nil, idempotencyHTTPError(err)
	}

	a2aContext, err := normalizeRunA2AContextRequest(req.A2AContext, opts.delegation, agentID)
	if err != nil {
		return nil, err
	}
	callback, err := s.taskCallbackFingerprintValue(taskCallbackConfigFromRunRequest(req))
	if err != nil {
		return nil, err
	}
	creationSource, err := normalizeRunCreationSource(req.CreationProtocol, req.CreationMethod, source)
	if err != nil {
		return nil, err
	}

	var delegation *RunFingerprintDelegation
	parentRunID := ""
	callerAgentID := ""
	if opts.delegation != nil {
		parentRunID = opts.delegation.ParentRunID.String()
		callerAgentID = opts.delegation.CallerAgentID.String()
		delegation = &RunFingerprintDelegation{
			Reason:  strings.TrimSpace(opts.delegation.Reason),
			Mode:    "free_delegation",
			Options: map[string]any{},
		}
	}

	fingerprint, err := FingerprintRunCreation(RunFingerprintInput{
		Target: RunFingerprintTarget{
			AgentID: agentID.String(),
		},
		Input:         input,
		Metadata:      metadata,
		Callback:      callback,
		Source:        creationSource,
		ParentRunID:   parentRunID,
		CallerAgentID: callerAgentID,
		Delegation:    delegation,
		A2A:           runFingerprintA2AFromRequest(a2aContext),
		Visibility:    a2aVisibility(a2aContext),
		Options:       creationOptions,
	})
	if err != nil {
		return nil, idempotencyHTTPError(err)
	}

	normalizedReq := *req
	normalizedReq.AgentID = agentID.String()
	normalizedReq.Input = input
	normalizedReq.Metadata = metadata
	normalizedReq.A2AContext = a2aContext
	normalizedReq.CreationProtocol = creationSource.Protocol
	normalizedReq.CreationMethod = creationSource.Method
	normalizedReq.CreationOptions = creationOptions

	return &normalizedRunCreation{
		request:                &normalizedReq,
		agentID:                agentID,
		idempotencyKeyHash:     append([]byte(nil), keyHash[:]...),
		idempotencyFingerprint: append([]byte(nil), fingerprint[:]...),
	}, nil
}

func normalizeRunCreationSource(protocol, method, source string) (RunFingerprintSource, error) {
	protocol = strings.ToLower(strings.TrimSpace(protocol))
	method = strings.ToLower(strings.TrimSpace(method))
	if protocol == "" && method == "" {
		switch source {
		case "", "web":
			protocol, method = "rest", "runs.create"
		case "mcp":
			protocol, method = "mcp", "run_agent"
		case "api":
			protocol, method = "api", "runs.create"
		default:
			return RunFingerprintSource{}, httpx.BadRequest("source 取值非法")
		}
	}
	if protocol == "" || method == "" || len(protocol) > 80 || len(method) > 120 {
		return RunFingerprintSource{}, httpx.BadRequest("调用来源标识不完整")
	}
	return RunFingerprintSource{Protocol: protocol, Method: method}, nil
}

func normalizeIJSONObject(value map[string]interface{}, required bool) (map[string]interface{}, error) {
	if value == nil {
		if required {
			return nil, httpx.Unprocessable("input 不能为空")
		}
		return map[string]interface{}{}, nil
	}
	canonical, err := CanonicalizeRFC8785(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.UseNumber()
	var normalized map[string]interface{}
	if err := decoder.Decode(&normalized); err != nil {
		return nil, &IdempotencyError{Class: IdempotencyErrorInputInvalid, cause: err}
	}
	return normalized, nil
}

func (s *Service) taskCallbackFingerprintValue(cfg *TaskCallbackConfig) (any, error) {
	if cfg == nil {
		return nil, nil
	}
	targetURL := strings.TrimSpace(cfg.URL)
	if targetURL == "" {
		return nil, httpx.BadRequest("task_callback.url 不能为空")
	}
	allowLocalHTTP := s.cfg != nil && s.cfg.AllowLocalHTTPEndpoints
	if err := endpointurl.Validate(targetURL, allowLocalHTTP); err != nil {
		return nil, httpx.BadRequest("task_callback.url 必须是 HTTPS；本地开发需开启 ALLOW_LOCAL_HTTP_ENDPOINTS 后才允许 loopback HTTP")
	}
	eventTypes, err := normalizeRunTaskCallbackEventTypes(taskCallbackEventTypesFromRunConfig(cfg))
	if err != nil {
		return nil, err
	}
	sort.Strings(eventTypes)
	metadata, err := normalizeIJSONObject(cfg.Metadata, false)
	if err != nil {
		return nil, idempotencyHTTPError(err)
	}

	secret := strings.TrimSpace(cfg.Secret)
	secretMode := "generated"
	secretHash := ""
	if secret != "" {
		secretMode = "caller_supplied"
		secretHash = hashSensitiveFingerprintField(secret)
	}
	authScheme, authCredentials := callbackAuthFromRunConfig(cfg)
	return map[string]any{
		"auth": map[string]any{
			"credentials_sha256": hashOptionalSensitiveFingerprintField(authCredentials),
			"scheme":             strings.ToLower(strings.TrimSpace(authScheme)),
		},
		"event_types": eventTypes,
		"metadata":    metadata,
		"secret": map[string]any{
			"mode":   secretMode,
			"sha256": secretHash,
		},
		"url": targetURL,
	}, nil
}

func hashSensitiveFingerprintField(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func hashOptionalSensitiveFingerprintField(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return hashSensitiveFingerprintField(value)
}

func runFingerprintA2AFromRequest(ctx *RunA2AContextRequest) *RunFingerprintA2A {
	if ctx == nil {
		return nil
	}
	return &RunFingerprintA2A{
		MessageID:           ctx.MessageID,
		ProtocolContextID:   ctx.ProtocolContextID,
		ProtocolTaskID:      ctx.ProtocolTaskID,
		RootContextID:       ctx.RootContextID,
		ParentContextID:     ctx.ParentContextID,
		ParentTaskID:        ctx.ParentTaskID,
		ReferenceTaskIDs:    append([]string{}, ctx.ReferenceTaskIDs...),
		Source:              ctx.Source,
		Visibility:          ctx.Visibility,
		AcceptedOutputModes: append([]string{}, ctx.AcceptedOutputModes...),
		Extensions:          append([]string{}, ctx.Extensions...),
		Options:             ctx.Options,
		TraceID:             ctx.TraceID,
	}
}

func a2aVisibility(ctx *RunA2AContextRequest) string {
	if ctx == nil {
		return ""
	}
	return ctx.Visibility
}

func idempotencyHTTPError(err error) error {
	class, ok := IdempotencyErrorClassOf(err)
	if !ok {
		return err
	}
	status := http.StatusUnprocessableEntity
	if class == IdempotencyErrorKeyReused {
		status = http.StatusConflict
	}
	return httpx.NewError(status, httpx.ErrorCode(class), err.Error())
}
