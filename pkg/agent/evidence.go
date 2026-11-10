package agent

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/mcpclient"
)

// errEvidenceTargetChanged marks a probe result that was not stored because
// the Agent stopped pointing at the probed connection target before the write.
var errEvidenceTargetChanged = errors.New("agent connection target changed while collecting evidence")

// connectionTarget is what readiness evidence is collected against: the
// connection mode, the normalized endpoint and, for MCP servers, the tool. A
// Runtime Agent's target is its own Runtime queue: its endpoint_url is only a
// display value, so the mode alone identifies it.
type connectionTarget struct {
	Mode        string
	EndpointURL string
	MCPToolName string
}

func storedConnectionTarget(a *db.Agent) connectionTarget {
	target := connectionTarget{
		Mode:        strings.TrimSpace(a.ConnectionMode),
		EndpointURL: strings.TrimSpace(a.EndpointURL),
	}
	if target.Mode == "" {
		target.Mode = ConnectionModeDirectHTTP
	}
	if target.Mode == ConnectionModeMCPServer && a.MCPToolName != nil {
		target.MCPToolName = strings.TrimSpace(*a.MCPToolName)
	}
	if target.Mode == ConnectionModeRuntime {
		target.EndpointURL = ""
	}
	return target
}

func (c connectionSettings) target() connectionTarget {
	target := connectionTarget{Mode: c.Mode, EndpointURL: c.EndpointURL}
	if c.Mode == ConnectionModeMCPServer && c.MCPToolName != nil {
		target.MCPToolName = strings.TrimSpace(*c.MCPToolName)
	}
	if c.Mode == ConnectionModeRuntime {
		target.EndpointURL = ""
	}
	return target
}

// usesEndpointCredential reports whether Core presents endpoint_auth_header
// when calling the Agent. Runtime Workers pull work with their own token.
func usesEndpointCredential(mode string) bool {
	return mode == ConnectionModeDirectHTTP || mode == ConnectionModeMCPServer
}

func authHeaderValue(header *string) string {
	if header == nil {
		return ""
	}
	return strings.TrimSpace(*header)
}

// evidenceInvalidation lists which stored evidence an Agent edit supersedes.
type evidenceInvalidation struct {
	// Reachability covers the callable signal: the availability snapshot the
	// monitor and readiness read, and the dry-run / health-check result.
	Reachability bool
	// Trust covers evidence about the service behind the target:
	// certification and Skill Benchmark scores.
	Trust bool
}

func (e evidenceInvalidation) any() bool {
	return e.Reachability || e.Trust
}

// evidenceInvalidationForUpdate compares the stored Agent with the connection
// it is about to be saved with. A new connection target invalidates all
// evidence. A changed endpoint credential on the same target only invalidates
// reachability: calls now present different credentials, but certification
// and benchmarks still describe the same service, so routine secret rotation
// keeps them. A changed MCP transport or credential header name changes how
// the same service is called, so it is treated like a credential change.
func evidenceInvalidationForUpdate(existing *db.Agent, next connectionSettings, nextAuthHeader *string) evidenceInvalidation {
	targetChanged := storedConnectionTarget(existing) != next.target()
	credentialChanged := usesEndpointCredential(next.Mode) &&
		(authHeaderValue(existing.EndpointAuthHeader) != authHeaderValue(nextAuthHeader) ||
			mcpclient.NormalizeTransport(existing.MCPTransport) != mcpclient.NormalizeTransport(next.MCPTransport) ||
			authHeaderValue(existing.EndpointAuthHeaderName) != authHeaderValue(next.EndpointAuthHeaderName))
	return evidenceInvalidation{
		Reachability: targetChanged || credentialChanged,
		Trust:        targetChanged,
	}
}

// invalidateAgentEvidence resets the derived evidence rows inside the caller's
// transaction. Run history, benchmark case results and alerts are kept.
func invalidateAgentEvidence(ctx context.Context, q *db.Queries, agentID uuid.UUID, scope evidenceInvalidation) error {
	if scope.Reachability {
		if _, err := q.ResetAgentAvailabilityEvidence(ctx, agentID); err != nil {
			return err
		}
		if _, err := q.ResetAgentDryRunEvidence(ctx, agentID); err != nil {
			return err
		}
	}
	if scope.Trust {
		if _, err := q.ReopenAgentCertificationAfterTargetChange(ctx, agentID); err != nil {
			return err
		}
		if _, err := q.ResetAgentSkillScoresAfterTargetChange(ctx, agentID); err != nil {
			return err
		}
	}
	return nil
}

// lockReachabilityEvidenceTarget share-locks the Agent row for a reachability
// write only while it still has the connection target and endpoint credential
// that were probed. It returns errEvidenceTargetChanged otherwise. Run
// finalization applies the same rule (db.LockAgentEvidenceTargetUnchanged).
func lockReachabilityEvidenceTarget(ctx context.Context, q *db.Queries, probed *db.Agent) error {
	err := q.LockAgentEvidenceTargetUnchanged(ctx, probed.ID, db.AgentEvidenceTargetOf(*probed), true)
	if errors.Is(err, db.ErrAgentEvidenceTargetChanged) {
		return errEvidenceTargetChanged
	}
	return err
}
