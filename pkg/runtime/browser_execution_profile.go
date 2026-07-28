package runtime

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const RuntimeBrowserExecutionProfileFeature = "browser_execution_profile.v1"

const (
	runtimeExecutionProfileStandard = "standard"
	runtimeExecutionProfileBrowser  = "browser"
)

func runtimeSessionUsesBrowserProfile(features []string) bool {
	return containsRuntimeFeature(features, RuntimeBrowserExecutionProfileFeature)
}

func validateAgentRuntimeProfile(
	active []db.RuntimeSession,
	requestFeatures []string,
) error {
	requestBrowser := runtimeSessionUsesBrowserProfile(requestFeatures)
	for _, session := range active {
		if runtimeSessionUsesBrowserProfile(session.Features) != requestBrowser {
			return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
		}
	}
	return nil
}

type runtimeAgentExecutionProfile struct {
	Known   bool
	Browser bool
}

func validateRuntimeAgentExecutionProfile(
	profile runtimeAgentExecutionProfile,
	visibility string,
	creatorID, authenticatedUserID uuid.UUID,
) error {
	if profile.Browser &&
		(visibility != "private" || creatorID != authenticatedUserID) {
		return httpx.Forbidden("Browser Agent 仅允许所有者私有调用")
	}
	return nil
}

func runtimeAgentAllowsOfflineQueue(
	profile runtimeAgentExecutionProfile,
	requestAllowsOffline bool,
) bool {
	return requestAllowsOffline && !profile.Browser
}

func runtimeExecutionProfileName(profile runtimeAgentExecutionProfile) string {
	if profile.Browser {
		return runtimeExecutionProfileBrowser
	}
	return runtimeExecutionProfileStandard
}

func (s *Service) inspectRuntimeAgentExecutionProfile(
	ctx context.Context,
	agentID uuid.UUID,
) (runtimeAgentExecutionProfile, error) {
	if s == nil || s.pool == nil || agentID == uuid.Nil {
		return runtimeAgentExecutionProfile{}, errors.New("Runtime Agent profile store is unavailable")
	}
	stored, err := db.New(s.pool).GetRuntimeAgentExecutionProfile(ctx, agentID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return runtimeAgentExecutionProfile{}, nil
		}
		return runtimeAgentExecutionProfile{}, err
	}
	return runtimeAgentExecutionProfile{
		Known:   true,
		Browser: stored.ExecutionProfile == runtimeExecutionProfileBrowser,
	}, nil
}

func ensureRuntimeAgentExecutionProfile(
	ctx context.Context,
	tx runtimeSessionTransaction,
	principal AuthenticatedRuntimePrincipal,
	request RuntimeSessionRequest,
) error {
	if runtimeSessionUsesBrowserProfile(request.Features) {
		profile, err := tx.ClassifyRuntimeAgentBrowserExecutionProfile(
			ctx,
			db.ClassifyRuntimeAgentBrowserExecutionProfileParams{
				AgentID:          request.AgentID,
				CredentialID:     principal.CredentialID,
				RuntimeSessionID: request.RuntimeSessionID,
			},
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// Public/unlisted Agents and credentials created by anybody
				// other than the Agent owner are rejected before the Session
				// becomes usable.
				return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, err)
			}
			return err
		}
		if profile.ExecutionProfile != runtimeExecutionProfileBrowser {
			return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
		}
		return nil
	}

	profile, err := tx.GetRuntimeAgentExecutionProfileForUpdate(ctx, request.AgentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if profile.ExecutionProfile == runtimeExecutionProfileBrowser {
		return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
	}
	return nil
}
