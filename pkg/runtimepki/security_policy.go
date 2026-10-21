package runtimepki

import (
	"errors"
	"net/http"
	"strings"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"
)

// This authenticated policy is scoped to one existing credential. Public
// discovery remains compatible with installations that have not migrated.
func (s *CredentialService) RegisterSecurityPolicy(api *echo.Group, cfg *config.Config) {
	if s != nil && cfg != nil {
		s.transitionOnly = cfg.RuntimeMTLSTransitionEnabled && !cfg.RuntimeMTLSEnabled
		s.authenticationPolicyID = cfg.RuntimeAuthenticationPolicyID()
	}
	if api == nil || cfg == nil || !cfg.RuntimeMTLSTransitionEnabled {
		return
	}
	api.POST("/runtime-security-policy", func(c echo.Context) error {
		c.Response().Header().Set("Cache-Control", "no-store")
		if s == nil || s.pool == nil || s.authority == nil || s.tokens == nil {
			return httpx.ServiceUnavailable("Runtime PKI 不可用")
		}
		raw, err := bearerToken(c.Request().Header.Get("Authorization"))
		if err != nil {
			return err
		}
		token, err := s.tokens.ValidateRuntimeToken(c.Request().Context(), raw, runtimeCredentialScope)
		if err != nil {
			return writeCredentialServiceError(c, err)
		}
		var request struct {
			NodeID string `json:"node_id"`
		}
		if err = c.Bind(&request); err != nil {
			return httpx.BadRequest("请求体不是合法 JSON")
		}
		nodeID, err := uuid.Parse(request.NodeID)
		if err != nil || nodeID == uuid.Nil || nodeID.String() != request.NodeID {
			return httpx.BadRequest("node_id 无效")
		}
		var boundID uuid.UUID
		var mode, operationID, key string
		var fenced bool
		err = s.pool.QueryRow(c.Request().Context(), `SELECT b.node_id,b.binding_mode,
 COALESCE(CASE WHEN n.status='draining' AND op.kind IN ('token_to_mtls','certificate_rotation') AND auth.credential_id=b.credential_id
 THEN op.operation_id::text END,''),b.public_key_thumbprint,
 EXISTS(SELECT 1 FROM runtime_node_upgrade_operations history WHERE history.node_id=n.node_id AND history.kind='token_to_mtls')
 FROM runtime_node_bindings b JOIN runtime_nodes n USING(node_id)
 LEFT JOIN runtime_node_upgrade_state st ON st.node_id=n.node_id
 LEFT JOIN runtime_node_upgrade_operations op ON op.operation_id=st.current_operation_id AND op.revision=st.revision
 LEFT JOIN runtime_node_credential_authorizations auth ON auth.operation_id=op.operation_id
 WHERE b.credential_id=$1 AND b.agent_id=$2 AND n.status IN ('active','draining') AND n.revoked_at IS NULL`, token.ID, token.AgentID).Scan(&boundID, &mode, &operationID, &key, &fenced)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return httpx.ServiceUnavailable("Runtime 认证策略暂不可用")
		}
		if errors.Is(err, pgx.ErrNoRows) {
			// No binding is allowed only for a genuinely unregistered credential/Node.
			// A revoked/mismatched binding must never be presented as a new token-only enrollment.
			var occupied bool
			if err = s.pool.QueryRow(c.Request().Context(), `SELECT EXISTS(SELECT 1 FROM runtime_node_bindings WHERE credential_id=$1) OR EXISTS(SELECT 1 FROM runtime_nodes WHERE node_id=$2)`, token.ID, nodeID).Scan(&occupied); err != nil {
				return httpx.ServiceUnavailable("Runtime 认证策略暂不可用")
			}
			if occupied {
				return httpx.Unauthorized("Runtime 身份不匹配")
			}
			mode = "token_only"
		} else if boundID != nodeID {
			return httpx.Unauthorized("Runtime 身份不匹配")
		}
		bindingMode := mode
		if fenced || cfg.RuntimeMTLSEnabled {
			mode = "mtls"
		}
		if fenced && operationID == "" {
			// A migrated binding can operate without a pending ticket. An interrupted
			// migration cannot silently recover by reverting to its old auth mode.
			if bindingMode != "mtls" {
				return httpx.NewError(http.StatusConflict, "RUNTIME_CREDENTIAL_MIGRATION_REQUIRED", "需要重新授权认证迁移")
			}
		}
		result := map[string]any{"node_id": nodeID.String(), "agent_id": token.AgentID.String(), "authentication_mode": mode}
		if mode == "mtls" {
			origin, originErr := config.NormalizeRuntimePublicOrigin(cfg.RuntimeMTLSAPIURL)
			if originErr != nil {
				return httpx.ServiceUnavailable("Runtime mTLS 地址不可用")
			}
			result["runtime_url"] = origin
			result["credential_endpoint"] = strings.TrimRight(cfg.APIURL, "/") + "/api/v1/runtime-credentials"
			if operationID != "" {
				result["operation_id"] = operationID
			}
		}
		return c.JSON(http.StatusOK, result)
	})
}
