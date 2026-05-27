package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

const runRequirementsSnapshottedEvent = "run.requirements.snapshotted"

type runRequirementSnapshot struct {
	TaskID           uuid.UUID
	AgentID          uuid.UUID
	UserID           uuid.UUID
	RequiredSkills   []string
	RequiredMCPTools []string
	AgentSkills      []string
	MatchedSkills    []string
	MissingSkills    []string
	UsedMCPTools     []string
	MissingMCPTools  []string
	CoverageStatus   string
	EvidenceSource   string
}

func (s *Service) buildRunRequirementSnapshot(
	ctx context.Context,
	userID uuid.UUID,
	agentID uuid.UUID,
	req *RunRequest,
	source string,
) (*runRequirementSnapshot, error) {
	taskID, ok, err := taskIDFromRunMetadata(req.Metadata)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	task, err := s.queries.GetTaskQuery(ctx, taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, httpx.NotFound("任务不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("task_id", taskID.String()).Msg("runtime.requirements: GetTaskQuery")
		return nil, httpx.Internal("查询任务失败")
	}
	if err := requireTaskRunAssociation(task, userID, agentID); err != nil {
		return nil, err
	}

	declared, err := s.queries.ListAgentSkills(ctx, agentID)
	if err != nil {
		log.Error().Err(err).Str("agent_id", agentID.String()).Msg("runtime.requirements: ListAgentSkills")
		return nil, httpx.Internal("查询 Agent Skill 失败")
	}
	agentSkills := make([]string, 0, len(declared))
	for _, skill := range declared {
		agentSkills = append(agentSkills, skill.ID)
	}

	requiredSkills := uniqueStrings(task.ParsedSkills)
	requiredTools := uniqueStrings(task.MCPTools)
	usedTools := normalizeUsedMCPTools(req.Metadata, source)
	matchedSkills, missingSkills := splitCoverage(requiredSkills, agentSkills)
	matchedTools, missingTools := splitCoverage(requiredTools, usedTools)

	return &runRequirementSnapshot{
		TaskID:           taskID,
		AgentID:          agentID,
		UserID:           userID,
		RequiredSkills:   requiredSkills,
		RequiredMCPTools: requiredTools,
		AgentSkills:      uniqueStrings(agentSkills),
		MatchedSkills:    matchedSkills,
		MissingSkills:    missingSkills,
		UsedMCPTools:     usedTools,
		MissingMCPTools:  missingTools,
		CoverageStatus:   coverageStatus(requiredSkills, requiredTools, matchedSkills, missingSkills, matchedTools, missingTools),
		EvidenceSource:   source,
	}, nil
}

func taskIDFromRunMetadata(metadata map[string]interface{}) (uuid.UUID, bool, error) {
	if metadata == nil {
		return uuid.Nil, false, nil
	}
	raw, ok := metadata["task_id"]
	if !ok || raw == nil {
		return uuid.Nil, false, nil
	}
	text := strings.TrimSpace(fmt.Sprint(raw))
	if text == "" {
		return uuid.Nil, false, nil
	}
	id, err := uuid.Parse(text)
	if err != nil {
		return uuid.Nil, false, httpx.BadRequest("metadata.task_id 不是合法 uuid")
	}
	return id, true, nil
}

func requireTaskRunAssociation(task db.TaskQuery, userID, agentID uuid.UUID) error {
	isOwner := task.UserID == userID
	isClaimant := task.ClaimedByUserID != nil && *task.ClaimedByUserID == userID
	if !isOwner && !isClaimant {
		return httpx.NotFound("任务不存在")
	}
	if task.ClaimedAgentID != nil {
		if *task.ClaimedAgentID != agentID {
			return httpx.Conflict("run 的 Agent 与接入任务的 Agent 不一致")
		}
		return nil
	}
	if task.ChosenAgentID != nil {
		if *task.ChosenAgentID != agentID {
			return httpx.Conflict("run 的 Agent 与任务选择的 Agent 不一致")
		}
		return nil
	}
	if isOwner && uuidInList(agentID, task.RecommendedAgentIDs) {
		return nil
	}
	return httpx.Conflict("run 的 Agent 与任务未关联")
}

func uuidInList(target uuid.UUID, values []uuid.UUID) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func normalizeUsedMCPTools(metadata map[string]interface{}, source string) []string {
	var tools []string
	if metadata != nil {
		tools = append(tools, stringListFromMetadata(metadata["used_mcp_tools"])...)
	}
	if source == "mcp" {
		tools = append(tools, "run_agent")
	}
	return uniqueStrings(tools)
}

func stringListFromMetadata(raw interface{}) []string {
	switch v := raw.(type) {
	case nil:
		return nil
	case []string:
		return v
	case []interface{}:
		out := make([]string, 0, len(v))
		for _, item := range v {
			out = append(out, fmt.Sprint(item))
		}
		return out
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		return []string{v}
	default:
		return []string{fmt.Sprint(v)}
	}
}

func uniqueStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func splitCoverage(required, actual []string) ([]string, []string) {
	actualSet := make(map[string]struct{}, len(actual))
	for _, item := range actual {
		actualSet[item] = struct{}{}
	}
	matched := make([]string, 0, len(required))
	missing := make([]string, 0, len(required))
	for _, item := range required {
		if _, ok := actualSet[item]; ok {
			matched = append(matched, item)
		} else {
			missing = append(missing, item)
		}
	}
	return matched, missing
}

func coverageStatus(requiredSkills, requiredTools, matchedSkills, missingSkills, matchedTools, missingTools []string) string {
	totalRequired := len(requiredSkills) + len(requiredTools)
	if totalRequired == 0 {
		return "no_requirements"
	}
	if len(missingSkills)+len(missingTools) == 0 {
		return "covered"
	}
	if len(matchedSkills)+len(matchedTools) > 0 {
		return "partial"
	}
	return "missing_requirements"
}

func (snapshot *runRequirementSnapshot) createParams(runID uuid.UUID) db.CreateRunRequirementEvidenceParams {
	return db.CreateRunRequirementEvidenceParams{
		RunID:            runID,
		TaskID:           snapshot.TaskID,
		AgentID:          snapshot.AgentID,
		UserID:           snapshot.UserID,
		RequiredSkillIDs: snapshot.RequiredSkills,
		RequiredMCPTools: snapshot.RequiredMCPTools,
		AgentSkillIDs:    snapshot.AgentSkills,
		MatchedSkillIDs:  snapshot.MatchedSkills,
		MissingSkillIDs:  snapshot.MissingSkills,
		UsedMCPTools:     snapshot.UsedMCPTools,
		MissingMCPTools:  snapshot.MissingMCPTools,
		CoverageStatus:   snapshot.CoverageStatus,
		EvidenceSource:   snapshot.EvidenceSource,
	}
}

func runRequirementEvidencePayload(e db.RunRequirementEvidence) map[string]interface{} {
	return map[string]interface{}{
		"task_id":             e.TaskID.String(),
		"agent_id":            e.AgentID.String(),
		"required_skill_ids":  e.RequiredSkillIDs,
		"required_mcp_tools":  e.RequiredMCPTools,
		"agent_skill_ids":     e.AgentSkillIDs,
		"matched_skill_ids":   e.MatchedSkillIDs,
		"missing_skill_ids":   e.MissingSkillIDs,
		"used_mcp_tools":      e.UsedMCPTools,
		"missing_mcp_tools":   e.MissingMCPTools,
		"coverage_status":     e.CoverageStatus,
		"evidence_source":     e.EvidenceSource,
		"evidence_created_at": e.CreatedAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
	}
}

func runRequirementEvidenceToResponse(e db.RunRequirementEvidence) *RunRequirementEvidenceResponse {
	return &RunRequirementEvidenceResponse{
		RunID:            e.RunID.String(),
		TaskID:           e.TaskID.String(),
		AgentID:          e.AgentID.String(),
		RequiredSkillIDs: append([]string{}, e.RequiredSkillIDs...),
		RequiredMCPTools: append([]string{}, e.RequiredMCPTools...),
		AgentSkillIDs:    append([]string{}, e.AgentSkillIDs...),
		MatchedSkillIDs:  append([]string{}, e.MatchedSkillIDs...),
		MissingSkillIDs:  append([]string{}, e.MissingSkillIDs...),
		UsedMCPTools:     append([]string{}, e.UsedMCPTools...),
		MissingMCPTools:  append([]string{}, e.MissingMCPTools...),
		CoverageStatus:   e.CoverageStatus,
		EvidenceSource:   e.EvidenceSource,
		CreatedAt:        e.CreatedAt,
	}
}

func (s *Service) attachRunRequirementEvidence(ctx context.Context, runID uuid.UUID, resp *RunResponse) {
	if resp == nil {
		return
	}
	evidence, err := s.queries.GetRunRequirementEvidenceByRun(ctx, runID)
	if errors.Is(err, pgx.ErrNoRows) {
		return
	}
	if err != nil {
		log.Error().Err(err).Str("run_id", runID.String()).Msg("runtime.requirements: GetRunRequirementEvidenceByRun")
		return
	}
	resp.RequirementEvidence = runRequirementEvidenceToResponse(evidence)
}
