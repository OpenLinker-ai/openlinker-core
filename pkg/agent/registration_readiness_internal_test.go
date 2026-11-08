package agent

import (
	"testing"

	"github.com/google/uuid"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestRegistrationNextActionsAreBoundedAndStateDriven(t *testing.T) {
	agentRow := &db.Agent{
		ID:              uuid.New(),
		LifecycleStatus: "active",
		Visibility:      "private",
		ConnectionMode:  ConnectionModeDirectHTTP,
	}
	unknown := availabilityResponse("unknown", nil, nil, nil, 0)
	notCallable := &Readiness{}

	all := registrationNextActions(agentRow, &unknown, notCallable, registrationProgress{})
	if len(all) != 4 || len(all) > maxRegistrationNextActions {
		t.Fatalf("direct_http steps = %#v", all)
	}
	want := []string{
		RegistrationNextActionDeclareCapability,
		RegistrationNextActionAddExample,
		RegistrationNextActionRunHealthCheck,
		RegistrationNextActionMakePublic,
	}
	for i, action := range all {
		if action.Type != want[i] {
			t.Fatalf("step %d = %q, want %q", i, action.Type, want[i])
		}
	}

	done := registrationNextActions(agentRow, &unknown, &Readiness{Callable: true}, registrationProgress{
		hasCapability:  true,
		exampleCount:   1,
		browserProfile: true,
	})
	if len(done) != 0 {
		t.Fatalf("callable Browser Agent must not be asked to publish: %#v", done)
	}

	disabled := *agentRow
	disabled.LifecycleStatus = "disabled"
	if got := registrationNextActions(&disabled, &unknown, notCallable, registrationProgress{}); len(got) != 0 {
		t.Fatalf("disabled Agent steps = %#v", got)
	}

	runtimeRow := *agentRow
	runtimeRow.ConnectionMode = ConnectionModeRuntime
	runtimeRow.Visibility = "public"
	healthy := availabilityForRuntimeSession(unknown, true)
	if got := registrationNextActions(&runtimeRow, &healthy, &Readiness{Callable: true}, registrationProgress{
		hasCapability: true, exampleCount: 1,
	}); len(got) != 0 {
		t.Fatalf("online public runtime Agent steps = %#v", got)
	}
}

func TestEvidenceInvalidationForUpdate(t *testing.T) {
	header := func(v string) *string { return &v }
	tool := "search"
	cases := []struct {
		name     string
		existing db.Agent
		next     connectionSettings
		auth     *string
		want     evidenceInvalidation
	}{
		{
			name:     "same target and credential",
			existing: db.Agent{ConnectionMode: ConnectionModeDirectHTTP, EndpointURL: "https://a.example/x", EndpointAuthHeader: header("Bearer a")},
			next:     connectionSettings{Mode: ConnectionModeDirectHTTP, EndpointURL: "https://a.example/x"},
			auth:     header("Bearer a"),
		},
		{
			name:     "endpoint change",
			existing: db.Agent{ConnectionMode: ConnectionModeDirectHTTP, EndpointURL: "https://a.example/x"},
			next:     connectionSettings{Mode: ConnectionModeDirectHTTP, EndpointURL: "https://b.example/x"},
			want:     evidenceInvalidation{Reachability: true, Trust: true},
		},
		{
			name:     "credential rotation",
			existing: db.Agent{ConnectionMode: ConnectionModeMCPServer, EndpointURL: "https://a.example/rpc", MCPToolName: &tool, EndpointAuthHeader: header("Bearer a")},
			next:     connectionSettings{Mode: ConnectionModeMCPServer, EndpointURL: "https://a.example/rpc", MCPToolName: &tool},
			auth:     header("Bearer b"),
			want:     evidenceInvalidation{Reachability: true},
		},
		{
			name:     "runtime ignores endpoint credential",
			existing: db.Agent{ConnectionMode: ConnectionModeRuntime, EndpointURL: "openlinker-runtime://a"},
			next:     connectionSettings{Mode: ConnectionModeRuntime, EndpointURL: "openlinker-runtime://a"},
			auth:     header("Bearer new"),
		},
		{
			name:     "runtime display endpoint is not part of the target",
			existing: db.Agent{ConnectionMode: ConnectionModeRuntime, EndpointURL: "openlinker-runtime://a"},
			next:     connectionSettings{Mode: ConnectionModeRuntime, EndpointURL: "openlinker-runtime://renamed"},
		},
		{
			name:     "runtime to direct_http",
			existing: db.Agent{ConnectionMode: ConnectionModeRuntime, EndpointURL: "openlinker-runtime://a"},
			next:     connectionSettings{Mode: ConnectionModeDirectHTTP, EndpointURL: "https://a.example/x"},
			want:     evidenceInvalidation{Reachability: true, Trust: true},
		},
		{
			name:     "legacy tool name on direct_http is not part of the target",
			existing: db.Agent{ConnectionMode: ConnectionModeDirectHTTP, EndpointURL: "https://a.example/x", MCPToolName: &tool},
			next:     connectionSettings{Mode: ConnectionModeDirectHTTP, EndpointURL: "https://a.example/x"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := evidenceInvalidationForUpdate(&tc.existing, tc.next, tc.auth); got != tc.want {
				t.Fatalf("evidenceInvalidationForUpdate = %#v, want %#v", got, tc.want)
			}
		})
	}
}
