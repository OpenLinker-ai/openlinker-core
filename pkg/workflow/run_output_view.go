package workflow

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runoutput"
)

// projectWorkflowRun 就地过滤 resp，只用于返回给查看者的副本。
// 节点之间传递的是库内原值，这里不影响执行。步骤输入里含上游节点的输出，要一起过滤。
// 汇总输出来自所有节点，只要有一个节点的 Agent 不属于查看者就要过滤。
func (s *Service) projectWorkflowRun(ctx context.Context, viewerID uuid.UUID, resp *WorkflowRunResponse) error {
	if resp == nil {
		return nil
	}
	foreignTokens := []string{}
	ownsAllSteps := len(resp.Steps) > 0
	stepProjections := map[string]runoutput.Projection{}
	for _, step := range resp.Steps {
		if _, seen := stepProjections[step.AgentID]; seen {
			continue
		}
		projection := runoutput.ForNonCreator()
		token := ""
		if agentID, err := uuid.Parse(step.AgentID); err == nil {
			agent, err := s.queries.GetAgentByID(ctx, agentID)
			switch {
			case err == nil:
				projection = runoutput.ForViewer(viewerID, agent.CreatorID, agent.EndpointAuthHeader)
				if agent.EndpointAuthHeader != nil {
					token = *agent.EndpointAuthHeader
				}
			case !errors.Is(err, pgx.ErrNoRows):
				log.Error().Err(err).Str("agent_id", step.AgentID).Msg("workflow.projectWorkflowRun: GetAgentByID")
				return httpx.Internal("查询 workflow_run 失败")
			}
		}
		stepProjections[step.AgentID] = projection
		if projection.Active() {
			ownsAllSteps = false
			foreignTokens = append(foreignTokens, token)
		}
	}
	// 上游节点的令牌也可能出现在下游节点的输入输出里，所以步骤和汇总都用全部外部令牌。
	combined := runoutput.ForNonCreator(foreignTokens...)
	for i := range resp.Steps {
		step := &resp.Steps[i]
		if ownsAllSteps {
			continue
		}
		var inputChanged, outputChanged, errorChanged bool
		step.Input, inputChanged = combined.Object(step.Input)
		step.Output, outputChanged = combined.Object(step.Output)
		step.Error, errorChanged = combined.Text(step.Error)
		step.OutputRedacted = inputChanged || outputChanged || errorChanged
	}
	if ownsAllSteps {
		return nil
	}
	var outputChanged, errorChanged, workerErrorChanged bool
	resp.Output, outputChanged = combined.Object(resp.Output)
	resp.Error, errorChanged = combined.Text(resp.Error)
	resp.LastWorkerError, workerErrorChanged = combined.Text(resp.LastWorkerError)
	resp.OutputRedacted = outputChanged || errorChanged || workerErrorChanged
	return nil
}
