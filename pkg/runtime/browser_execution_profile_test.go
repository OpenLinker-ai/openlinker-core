package runtime

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestBrowserAgentExecutionProfileRequiresPrivateOwner(t *testing.T) {
	owner, other := uuid.New(), uuid.New()
	for _, test := range []struct {
		name       string
		profile    runtimeAgentExecutionProfile
		visibility string
		creator    uuid.UUID
		caller     uuid.UUID
		wantError  bool
	}{
		{
			name:       "private owner",
			profile:    runtimeAgentExecutionProfile{Known: true, Browser: true},
			visibility: "private",
			creator:    owner,
			caller:     owner,
		},
		{
			name:       "public owner",
			profile:    runtimeAgentExecutionProfile{Known: true, Browser: true},
			visibility: "public",
			creator:    owner,
			caller:     owner,
			wantError:  true,
		},
		{
			name:       "unlisted owner",
			profile:    runtimeAgentExecutionProfile{Known: true, Browser: true},
			visibility: "unlisted",
			creator:    owner,
			caller:     owner,
			wantError:  true,
		},
		{
			name:       "private other caller",
			profile:    runtimeAgentExecutionProfile{Known: true, Browser: true},
			visibility: "private",
			creator:    owner,
			caller:     other,
			wantError:  true,
		},
		{
			name:       "ordinary runtime unchanged",
			profile:    runtimeAgentExecutionProfile{Known: true},
			visibility: "public",
			creator:    owner,
			caller:     other,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateRuntimeAgentExecutionProfile(
				test.profile,
				test.visibility,
				test.creator,
				test.caller,
			)
			if (err != nil) != test.wantError {
				t.Fatalf("eligibility error = %v, wantError %t", err, test.wantError)
			}
		})
	}
}

func TestEnsureRuntimeAgentExecutionProfileIsStickyAndOwnerBound(t *testing.T) {
	fixture := newSessionFixture()

	t.Run("ordinary Session cannot downgrade Browser Agent", func(t *testing.T) {
		tx := newSessionTransactionFake(fixture)
		tx.executionProfile = db.RuntimeAgentExecutionProfile{
			AgentID:          fixture.request.AgentID,
			ExecutionProfile: runtimeExecutionProfileBrowser,
		}
		err := ensureRuntimeAgentExecutionProfile(
			context.Background(),
			tx,
			fixture.principal,
			fixture.request,
		)
		if !IsRuntimeSessionError(err, RuntimeSessionErrorSessionConflict) {
			t.Fatalf("error = %v, want SESSION_CONFLICT", err)
		}
	})

	t.Run("ordinary Session cannot race an Owner staged Browser declaration", func(t *testing.T) {
		tx := newSessionTransactionFake(fixture)
		tx.browserDeclared = true
		err := ensureRuntimeAgentExecutionProfile(
			context.Background(),
			tx,
			fixture.principal,
			fixture.request,
		)
		if !IsRuntimeSessionError(err, RuntimeSessionErrorSessionConflict) {
			t.Fatalf("error = %v, want SESSION_CONFLICT", err)
		}
	})

	t.Run("Browser Session durably classifies exact principal", func(t *testing.T) {
		tx := newSessionTransactionFake(fixture)
		request := fixture.request
		request.Features = append(request.Features, RuntimeBrowserExecutionProfileFeature)
		if err := ensureRuntimeAgentExecutionProfile(
			context.Background(),
			tx,
			fixture.principal,
			request,
		); err != nil {
			t.Fatalf("ensure profile: %v", err)
		}
		if tx.classifyProfileParams.AgentID != request.AgentID ||
			tx.classifyProfileParams.CredentialID != fixture.principal.CredentialID ||
			tx.classifyProfileParams.RuntimeSessionID != request.RuntimeSessionID {
			t.Fatalf("classification params = %#v", tx.classifyProfileParams)
		}
	})

	t.Run("first full Browser Session consumes only matching staged authority", func(t *testing.T) {
		tx := newSessionTransactionFake(fixture)
		tx.classifyProfile = db.RuntimeAgentExecutionProfile{
			AgentID:                     fixture.request.AgentID,
			ExecutionProfile:            runtimeExecutionProfileBrowser,
			InteractionPolicy:           "full",
			BrowserMutationOrigins:      []string{"https://example.com"},
			InteractionPolicyGeneration: 1,
		}
		request := fixture.request
		request.Features = append(
			request.Features,
			RuntimeBrowserExecutionProfileFeature,
			RuntimeBrowserFullInteractionFeature,
		)
		if err := ensureRuntimeAgentExecutionProfile(
			context.Background(),
			tx,
			fixture.principal,
			request,
		); err != nil {
			t.Fatalf("ensure initial full profile: %v", err)
		}
		if !tx.classifyProfileParams.FullBrowserInteraction {
			t.Fatalf("classification params = %#v", tx.classifyProfileParams)
		}
	})

	t.Run("ineligible Browser declaration fails closed", func(t *testing.T) {
		tx := newSessionTransactionFake(fixture)
		tx.classifyProfileErr = pgx.ErrNoRows
		request := fixture.request
		request.Features = append(request.Features, RuntimeBrowserExecutionProfileFeature)
		err := ensureRuntimeAgentExecutionProfile(
			context.Background(),
			tx,
			fixture.principal,
			request,
		)
		if !IsRuntimeSessionError(err, RuntimeSessionErrorSessionConflict) {
			t.Fatalf("error = %v, want SESSION_CONFLICT", err)
		}
	})
}

func TestBrowserAgentExecutionProfileNeverQueuesOffline(t *testing.T) {
	if runtimeAgentAllowsOfflineQueue(
		runtimeAgentExecutionProfile{Known: true, Browser: true},
		true,
	) {
		t.Fatal("Browser Runtime accepted an offline queued Run")
	}
	if !runtimeAgentAllowsOfflineQueue(
		runtimeAgentExecutionProfile{Known: true},
		true,
	) {
		t.Fatal("ordinary Runtime offline queue behavior changed")
	}
	if runtimeAgentAllowsOfflineQueue(runtimeAgentExecutionProfile{}, false) {
		t.Fatal("request-level offline queue denial was ignored")
	}
}

func TestBrowserInteractionPolicyMustMatchEverySessionFeatureSet(t *testing.T) {
	restricted := runtimeAgentExecutionProfile{
		Known:                       true,
		Browser:                     true,
		InteractionPolicy:           "restricted",
		BrowserMutationOrigins:      []string{},
		InteractionPolicyGeneration: 1,
	}
	full := runtimeAgentExecutionProfile{
		Known:                       true,
		Browser:                     true,
		InteractionPolicy:           "full",
		BrowserMutationOrigins:      []string{"https://fixture.example"},
		InteractionPolicyGeneration: 2,
	}
	restrictedFeatures := []string{RuntimeBrowserExecutionProfileFeature}
	fullFeatures := []string{
		RuntimeBrowserExecutionProfileFeature,
		RuntimeBrowserFullInteractionFeature,
	}
	if !runtimeAgentProfileMatchesFeatures(restricted, restrictedFeatures) ||
		runtimeAgentProfileMatchesFeatures(restricted, fullFeatures) ||
		!runtimeAgentProfileMatchesFeatures(full, fullFeatures) ||
		runtimeAgentProfileMatchesFeatures(full, restrictedFeatures) ||
		runtimeAgentProfileMatchesFeatures(full, []string{RuntimeBrowserFullInteractionFeature}) {
		t.Fatal("Browser policy/feature matching did not fail closed")
	}
	active := []db.RuntimeSession{
		{Features: append([]string{}, fullFeatures...)},
		{Features: append([]string{}, fullFeatures...)},
	}
	if err := validateAgentRuntimeProfile(active, fullFeatures); err != nil {
		t.Fatalf("same full-policy Sessions conflict: %v", err)
	}
	active[1].Features = restrictedFeatures
	if err := validateAgentRuntimeProfile(active, fullFeatures); !IsRuntimeSessionError(err, RuntimeSessionErrorSessionConflict) {
		t.Fatalf("mixed Browser policies error = %v, want SESSION_CONFLICT", err)
	}
}

func TestRuntimeBrowserAuthorityComparisonIncludesGenerationAndExactOrigins(t *testing.T) {
	base := runtimeAgentExecutionProfile{
		Known:                        true,
		Browser:                      true,
		InteractionPolicy:            "full",
		BrowserMutationOrigins:       []string{"https://a.example", "https://b.example"},
		InteractionPolicyGeneration:  4,
		BrowserMutationOriginsSHA256: "digest",
	}
	if !sameRuntimeBrowserAuthority(base, base) {
		t.Fatal("equal Browser authority did not match")
	}
	for name, mutate := range map[string]func(*runtimeAgentExecutionProfile){
		"generation": func(profile *runtimeAgentExecutionProfile) {
			profile.InteractionPolicyGeneration++
		},
		"policy": func(profile *runtimeAgentExecutionProfile) {
			profile.InteractionPolicy = "restricted"
		},
		"digest": func(profile *runtimeAgentExecutionProfile) {
			profile.BrowserMutationOriginsSHA256 = "different"
		},
		"origins": func(profile *runtimeAgentExecutionProfile) {
			profile.BrowserMutationOrigins = []string{"https://b.example", "https://a.example"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			if sameRuntimeBrowserAuthority(base, changed) {
				t.Fatalf("changed %s Browser authority matched", name)
			}
		})
	}
}
