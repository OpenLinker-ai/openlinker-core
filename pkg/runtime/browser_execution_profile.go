package runtime

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/OpenLinker-ai/openlinker-core/pkg/browserpolicy"
	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const RuntimeBrowserExecutionProfileFeature = "browser_execution_profile.v1"
const RuntimeBrowserFullInteractionFeature = "browser_full_interaction.v1"

const (
	runtimeExecutionProfileStandard = "standard"
	runtimeExecutionProfileBrowser  = "browser"
)

func runtimeSessionUsesBrowserProfile(features []string) bool {
	return containsRuntimeFeature(features, RuntimeBrowserExecutionProfileFeature)
}

func runtimeSessionUsesFullBrowserInteraction(features []string) bool {
	return containsRuntimeFeature(features, RuntimeBrowserFullInteractionFeature)
}

func validateAgentRuntimeProfile(
	active []db.RuntimeSession,
	requestFeatures []string,
) error {
	requestBrowser := runtimeSessionUsesBrowserProfile(requestFeatures)
	requestFull := runtimeSessionUsesFullBrowserInteraction(requestFeatures)
	if requestFull && !requestBrowser {
		return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
	}
	for _, session := range active {
		if runtimeSessionUsesBrowserProfile(session.Features) != requestBrowser ||
			runtimeSessionUsesFullBrowserInteraction(session.Features) != requestFull {
			return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
		}
	}
	return nil
}

type runtimeAgentExecutionProfile struct {
	Known                        bool
	Browser                      bool
	InteractionPolicy            string
	BrowserMutationOrigins       []string
	InteractionPolicyGeneration  int64
	BrowserMutationOriginsSHA256 string
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

func runtimeAgentProfileFromStored(
	stored db.RuntimeAgentExecutionProfile,
) (runtimeAgentExecutionProfile, error) {
	profile := runtimeAgentExecutionProfile{
		Known:                       true,
		Browser:                     stored.ExecutionProfile == runtimeExecutionProfileBrowser,
		InteractionPolicy:           stored.InteractionPolicy,
		BrowserMutationOrigins:      append([]string{}, stored.BrowserMutationOrigins...),
		InteractionPolicyGeneration: stored.InteractionPolicyGeneration,
	}
	if !profile.Browser {
		return profile, nil
	}
	digest, err := browserpolicy.ValidateCanonical(
		profile.InteractionPolicy,
		profile.BrowserMutationOrigins,
	)
	if err != nil || profile.InteractionPolicyGeneration < 1 {
		return runtimeAgentExecutionProfile{}, errors.New("stored Browser interaction policy is invalid")
	}
	profile.BrowserMutationOriginsSHA256 = digest
	return profile, nil
}

func runtimeAgentProfileMatchesFeatures(
	profile runtimeAgentExecutionProfile,
	features []string,
) bool {
	if !profile.Browser || !runtimeSessionUsesBrowserProfile(features) {
		return !profile.Browser && !runtimeSessionUsesBrowserProfile(features) &&
			!runtimeSessionUsesFullBrowserInteraction(features)
	}
	full := runtimeSessionUsesFullBrowserInteraction(features)
	return (profile.InteractionPolicy == browserpolicy.Full && full) ||
		(profile.InteractionPolicy == browserpolicy.Restricted && !full)
}

func sameRuntimeBrowserAuthority(
	left,
	right runtimeAgentExecutionProfile,
) bool {
	if !left.Browser || !right.Browser ||
		left.InteractionPolicy != right.InteractionPolicy ||
		left.InteractionPolicyGeneration != right.InteractionPolicyGeneration ||
		left.BrowserMutationOriginsSHA256 != right.BrowserMutationOriginsSHA256 ||
		len(left.BrowserMutationOrigins) != len(right.BrowserMutationOrigins) {
		return false
	}
	for index := range left.BrowserMutationOrigins {
		if left.BrowserMutationOrigins[index] != right.BrowserMutationOrigins[index] {
			return false
		}
	}
	return true
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
	return runtimeAgentProfileFromStored(stored)
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
				AgentID:                request.AgentID,
				CredentialID:           principal.CredentialID,
				FullBrowserInteraction: runtimeSessionUsesFullBrowserInteraction(request.Features),
				RuntimeSessionID:       request.RuntimeSessionID,
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
		normalized, err := runtimeAgentProfileFromStored(profile)
		if err != nil || !runtimeAgentProfileMatchesFeatures(normalized, request.Features) {
			return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, err)
		}
		return nil
	}
	if runtimeSessionUsesFullBrowserInteraction(request.Features) {
		return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
	}
	declaredBrowser, err := tx.LockRuntimeAgentBrowserDeclaration(ctx, request.AgentID)
	if err != nil {
		return err
	}
	if declaredBrowser {
		return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
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
