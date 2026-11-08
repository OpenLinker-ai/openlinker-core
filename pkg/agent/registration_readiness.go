package agent

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	coreruntime "github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
)

// Registration next actions reuse the Run next-action shape
// (coreruntime.RunNextAction) and the Hub onboarding page it links to. Type is
// the stable machine-readable step; AdditionalProps carries the owner API call.
const (
	RegistrationNextActionDeclareCapability  = "declare_capability"
	RegistrationNextActionAddExample         = "add_example"
	RegistrationNextActionStartRuntimeWorker = "start_runtime_worker"
	RegistrationNextActionRunHealthCheck     = "run_health_check"
	RegistrationNextActionMakePublic         = "make_public"

	maxRegistrationNextActions = 5
)

// registrationProgress is the stored onboarding state a registration response
// reports. It is read after the registration committed (or, on replay, from
// the Agent as it is now).
type registrationProgress struct {
	hasCapability bool
	exampleCount  int32
	onboarding    *OnboardingStatusResponse
	// browserProfile Agents must stay private, so publishing is not a step.
	browserProfile bool
}

// registrationResponse builds the success body shared by first registration
// and replay. It only reads. A failed progress read omits onboarding and
// next_actions instead of reporting an empty, misleading step list.
func (s *RegistrationService) registrationResponse(
	ctx context.Context,
	agentRow *db.Agent,
	token db.AgentToken,
) *RegisterAgentViaTokenResponse {
	resp := &RegisterAgentViaTokenResponse{
		Agent:      agentResponseWithReadiness(ctx, s.queries, agentRow),
		AgentToken: agentTokenResponse(token),
	}
	progress, err := readRegistrationProgress(ctx, s.queries, agentRow)
	if err != nil {
		log.Warn().Err(err).Str("agent_id", agentRow.ID.String()).
			Msg("registration.RegisterAgentViaToken: read onboarding progress")
		return resp
	}
	resp.Onboarding = progress.onboarding
	actions := registrationNextActions(agentRow, resp.Agent.Availability, resp.Agent.Readiness, progress)
	resp.NextActions = &actions
	return resp
}

func readRegistrationProgress(ctx context.Context, q *db.Queries, agentRow *db.Agent) (registrationProgress, error) {
	var progress registrationProgress
	if _, err := q.GetAgentCapabilityByAgentID(ctx, agentRow.ID); err == nil {
		progress.hasCapability = true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return registrationProgress{}, err
	}
	count, err := q.CountAgentExamplesByAgentID(ctx, agentRow.ID)
	if err != nil {
		return registrationProgress{}, err
	}
	progress.exampleCount = count
	status, err := q.GetOnboardingStatusForOwner(ctx, db.GetOnboardingStatusForOwnerParams{
		AgentID:   agentRow.ID,
		CreatorID: agentRow.CreatorID,
	})
	switch {
	case err == nil:
		resp := toOnboardingStatusResponse(&status)
		progress.onboarding = &resp
	case errors.Is(err, pgx.ErrNoRows):
		// Agents registered before onboarding rows were created at
		// registration have none until the owner opens onboarding; replay
		// must not create it.
	default:
		return registrationProgress{}, err
	}
	profile, err := q.GetRuntimeAgentExecutionProfile(ctx, agentRow.ID)
	switch {
	case err == nil:
		progress.browserProfile = profile.ExecutionProfile == "browser"
	case !errors.Is(err, pgx.ErrNoRows):
		return registrationProgress{}, err
	}
	return progress, nil
}

// registrationNextActions lists, in order, what still stands between the
// Agent and being callable and public. The list is bounded and derived only
// from stored state.
func registrationNextActions(
	agentRow *db.Agent,
	availability *Availability,
	readiness *Readiness,
	progress registrationProgress,
) []coreruntime.RunNextAction {
	actions := make([]coreruntime.RunNextAction, 0, maxRegistrationNextActions)
	if agentRow.LifecycleStatus != "active" {
		return actions
	}
	agentID := agentRow.ID.String()
	onboardingHref := "/hub/agents/" + agentID + "/onboarding"
	apiBase := "/api/v1/creator/agents/" + agentID
	ownerStep := func(actionType, label, hint, href, method, path, registrationField string) coreruntime.RunNextAction {
		props := map[string]interface{}{
			"agent_id":   agentID,
			"api_method": method,
			"api_path":   path,
		}
		if registrationField != "" {
			props["registration_field"] = registrationField
		}
		return coreruntime.RunNextAction{
			Type:            actionType,
			Label:           label,
			Hint:            hint,
			Href:            href,
			RequiresHuman:   true,
			ResourceType:    "agent",
			ResourceID:      agentID,
			Source:          "platform",
			AdditionalProps: props,
		}
	}

	if !progress.hasCapability {
		actions = append(actions, ownerStep(
			RegistrationNextActionDeclareCapability,
			"声明能力",
			"保存 input_schema / output_schema 后才能执行健康检查和巡检。可在注册请求的 capability 字段中声明，或由创作者在接入页保存。",
			onboardingHref, "PUT", apiBase+"/capabilities", "capability",
		))
	}
	if progress.exampleCount == 0 {
		actions = append(actions, ownerStep(
			RegistrationNextActionAddExample,
			"添加示例",
			"至少需要 1 条输入示例；健康检查和巡检使用首条示例调用 Agent。可在注册请求的 examples 字段中提供，或由创作者在接入页添加。",
			onboardingHref, "POST", apiBase+"/examples", "examples",
		))
	}
	callable := readiness != nil && readiness.Callable
	switch agentRow.ConnectionMode {
	case ConnectionModeRuntime:
		if availability == nil || availability.Status != "healthy" {
			actions = append(actions, coreruntime.RunNextAction{
				Type:          RegistrationNextActionStartRuntimeWorker,
				Label:         "启动 Agent runtime",
				Hint:          "使用本次注册的 Agent Token 和 Node 设备证书启动 Runtime Worker；Worker 建立 Runtime Session 后 Agent 即可接收运行。",
				Href:          onboardingHref,
				RequiresHuman: false,
				ResourceType:  "agent",
				ResourceID:    agentID,
				Source:        "agent_runtime",
				AdditionalProps: map[string]interface{}{
					"agent_id": agentID,
				},
			})
		}
	default:
		if !callable {
			actions = append(actions, ownerStep(
				RegistrationNextActionRunHealthCheck,
				"执行健康检查",
				"能力声明和示例就绪后，用首条示例调用 endpoint 验证可用性；平台巡检也会在数分钟内自动检查。",
				onboardingHref, "POST", apiBase+"/health-check", "",
			))
		}
	}
	if agentRow.Visibility != "public" && !progress.browserProfile {
		step := ownerStep(
			RegistrationNextActionMakePublic,
			"公开到市场",
			"Agent 当前不在公开市场中。验证可调用后，由创作者把可见性改为 public。",
			"/hub/agents/"+agentID+"/settings", "PATCH", apiBase+"/visibility", "",
		)
		step.AdditionalProps["api_body"] = map[string]interface{}{"visibility": "public"}
		actions = append(actions, step)
	}
	if len(actions) > maxRegistrationNextActions {
		actions = actions[:maxRegistrationNextActions]
	}
	return actions
}
