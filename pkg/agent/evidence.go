package agent

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

// errEvidenceTargetChanged marks a probe result that was not stored because
// the Agent stopped pointing at the probed connection target before the write.
var errEvidenceTargetChanged = errors.New("agent connection target changed while collecting evidence")

// connectionTarget is what readiness evidence is collected against: the
// connection mode, the normalized endpoint and, for MCP servers, the tool.
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
	return target
}

func (c connectionSettings) target() connectionTarget {
	target := connectionTarget{Mode: c.Mode, EndpointURL: c.EndpointURL}
	if c.Mode == ConnectionModeMCPServer && c.MCPToolName != nil {
		target.MCPToolName = strings.TrimSpace(*c.MCPToolName)
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
// keeps them.
func evidenceInvalidationForUpdate(existing *db.Agent, next connectionSettings, nextAuthHeader *string) evidenceInvalidation {
	targetChanged := storedConnectionTarget(existing) != next.target()
	credentialChanged := usesEndpointCredential(next.Mode) &&
		authHeaderValue(existing.EndpointAuthHeader) != authHeaderValue(nextAuthHeader)
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
// that were probed. It returns errEvidenceTargetChanged otherwise.
func lockReachabilityEvidenceTarget(ctx context.Context, q *db.Queries, probed *db.Agent) error {
	_, err := q.LockAgentEvidenceTarget(ctx, db.LockAgentEvidenceTargetParams{
		ID:                 probed.ID,
		ConnectionMode:     probed.ConnectionMode,
		EndpointURL:        probed.EndpointURL,
		MCPToolName:        probed.MCPToolName,
		CompareAuthHeader:  usesEndpointCredential(probed.ConnectionMode),
		EndpointAuthHeader: probed.EndpointAuthHeader,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return errEvidenceTargetChanged
	}
	return err
}
