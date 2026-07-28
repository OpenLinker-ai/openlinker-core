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
