package registry

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runoutput"
)

// proxyRunProjection 给出请求者读取 local_agent_id 的 Proxy Run 时要用的过滤方式。
// Agent 已不存在时按非创建者处理；查询失败时不返回原文。
func (s *Service) proxyRunProjection(ctx context.Context, requestingUserID uuid.UUID, localAgentID string) (runoutput.Projection, error) {
	agentID, err := uuid.Parse(localAgentID)
	if err != nil {
		return runoutput.ForNonCreator(), nil
	}
	agent, err := s.q.GetAgentByID(ctx, agentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return runoutput.ForNonCreator(), nil
	}
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("registry.proxyRunProjection")
		return runoutput.Projection{}, httpx.Internal("查询 Proxy Run 失败")
	}
	return runoutput.ForViewer(requestingUserID, agent.CreatorID, agent.EndpointAuthHeader), nil
}

func (s *Service) projectProxyRunForRequester(ctx context.Context, requestingUserID uuid.UUID, resp ProxyRunResponse) (*ProxyRunResponse, error) {
	projection, err := s.proxyRunProjection(ctx, requestingUserID, resp.LocalAgentID)
	if err != nil {
		return nil, err
	}
	projected := projectProxyRun(projection, resp)
	return &projected, nil
}

func projectProxyRun(projection runoutput.Projection, resp ProxyRunResponse) ProxyRunResponse {
	var outputChanged, summaryChanged, errorChanged bool
	resp.Output, outputChanged = projection.Object(resp.Output)
	resp.OutputSummary, summaryChanged = projection.Text(resp.OutputSummary)
	resp.ErrorMessage, errorChanged = projection.Text(resp.ErrorMessage)
	// 远端 Registry 可能已经过滤过。
	resp.OutputRedacted = resp.OutputRedacted || outputChanged || summaryChanged || errorChanged
	return resp
}

func projectProxyRunArtifact(projection runoutput.Projection, artifact ProxyRunArtifactResponse) ProxyRunArtifactResponse {
	var titleChanged, contentChanged bool
	artifact.Title, titleChanged = projection.Text(artifact.Title)
	artifact.Content, contentChanged = projection.Object(artifact.Content)
	artifact.OutputRedacted = titleChanged || contentChanged
	return artifact
}
