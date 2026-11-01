package runtime

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runoutput"
)

type agentLookup interface {
	GetAgentByID(context.Context, uuid.UUID) (db.Agent, error)
}

// RunOutputProjection 给出 viewerID 读取该 Agent 的 Run 时要用的过滤方式。
// Agent 已不存在时无法确认创作者，按非创作者处理；查询失败时不返回原文。
func (s *Service) RunOutputProjection(ctx context.Context, viewerID, agentID uuid.UUID) (runoutput.Projection, error) {
	return s.runOutputProjection(ctx, s.queries, viewerID, agentID)
}

func (s *Service) runOutputProjection(
	ctx context.Context,
	queries agentLookup,
	viewerID, agentID uuid.UUID,
) (runoutput.Projection, error) {
	agent, err := queries.GetAgentByID(ctx, agentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return runoutput.ForNonCreator(), nil
	}
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("runtime.runOutputProjection")
		return runoutput.Projection{}, httpx.Internal("查询调用记录失败")
	}
	return runoutput.ForViewer(viewerID, agent.CreatorID, agent.EndpointAuthHeader), nil
}

// ProjectRunForViewer 过滤要返回给 viewerID 的 Run，所有把 Run 交给查看者的入口都要经过这里。
func (s *Service) ProjectRunForViewer(ctx context.Context, viewerID uuid.UUID, resp *RunResponse) (*RunResponse, error) {
	if resp == nil {
		return nil, nil
	}
	agentID, err := uuid.Parse(resp.AgentID)
	if err != nil {
		// 不知道是哪个 Agent 就没有创作者可以比对。
		return ProjectRunResponse(runoutput.ForNonCreator(), resp), nil
	}
	projection, err := s.runOutputProjection(ctx, s.queries, viewerID, agentID)
	if err != nil {
		return nil, err
	}
	return ProjectRunResponse(projection, resp), nil
}

// ProjectRunResponse 返回过滤后的副本。next_action 取自输出或错误信息，要一起过滤。
func ProjectRunResponse(projection runoutput.Projection, resp *RunResponse) *RunResponse {
	if resp == nil || !projection.Active() {
		return resp
	}
	out := *resp
	var outputChanged, errorChanged bool
	out.Output, outputChanged = projection.Object(resp.Output)
	out.ErrorMsg, errorChanged = projection.Text(resp.ErrorMsg)
	out.OutputRedacted = outputChanged || errorChanged
	if resp.NextAction != nil {
		action := *resp.NextAction
		action.Label, _ = projection.Text(action.Label)
		action.Hint, _ = projection.Text(action.Hint)
		action.Href, _ = projection.Text(action.Href)
		action.AdditionalProps, _ = projection.Object(action.AdditionalProps)
		if message, ok := action.AdditionalProps["error_message"].(string); ok {
			action.AdditionalProps["error_message"], _ = projection.Text(message)
		}
		out.NextAction = &action
	}
	return &out
}

func projectRunEvent(projection runoutput.Projection, event RunEventResponse) RunEventResponse {
	event.Payload, event.OutputRedacted = projection.EventPayload(event.EventType, event.Payload)
	return event
}

func projectRunArtifact(projection runoutput.Projection, artifact RunArtifactResponse) RunArtifactResponse {
	var contentChanged, titleChanged bool
	artifact.Content, contentChanged = projection.Object(artifact.Content)
	artifact.Title, titleChanged = projection.Text(artifact.Title)
	artifact.OutputRedacted = contentChanged || titleChanged
	return artifact
}

func projectRunMessage(projection runoutput.Projection, message RunMessageResponse) RunMessageResponse {
	var payloadChanged, contentChanged bool
	message.Payload, payloadChanged = projection.Object(message.Payload)
	message.Content, contentChanged = projection.Text(message.Content)
	message.OutputRedacted = payloadChanged || contentChanged
	return message
}
