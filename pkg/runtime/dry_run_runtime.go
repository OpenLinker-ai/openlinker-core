package runtime

import (
	"context"
	"time"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// Runtime diagnostics use the same authorization, input validation, durable
// dispatch and result finalization as normal invocations. A Worker cannot be
// called through the legacy endpoint adapter.
func (s *Service) dryRunRuntime(ctx context.Context, agent *db.Agent, input map[string]interface{}) (map[string]interface{}, string) {
	timeout := 60 * time.Second
	if s.cfg != nil && s.cfg.RunTimeoutSeconds > 0 {
		timeout = time.Duration(s.cfg.RunTimeoutSeconds) * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp, err := s.Run(ctx, agent.CreatorID, &RunRequest{
		AgentID: agent.ID.String(), Input: input,
		IdempotencyKey: "dry-run:" + uuid.NewString(),
		Metadata:       map[string]interface{}{"purpose": "dry_run"},
	}, "api")
	if err != nil {
		return nil, "Runtime 试运行失败: " + truncate(err.Error(), errMsgMaxLen)
	}
	runID, err := uuid.Parse(resp.RunID)
	if err != nil {
		return nil, "Runtime 试运行未返回有效 Run ID"
	}
	finished := false
	defer func() {
		if finished {
			return
		}
		// The request context may already be canceled. Do not leave a diagnostic
		// queued for execution after the health check or benchmark has ended.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cleanupCancel()
		if _, err := s.CancelRun(cleanupCtx, agent.CreatorID, runID); err != nil {
			log.Warn().Err(err).Str("run_id", runID.String()).Msg("runtime.DryRun: cancel unfinished diagnostic")
		}
	}()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	status := resp.Status
	for status == "running" {
		select {
		case <-ctx.Done():
			return nil, "Runtime 试运行已取消或超时: " + ctx.Err().Error()
		case <-ticker.C:
			status, err = s.GetRunWaitStatus(ctx, agent.CreatorID, runID)
			if err != nil {
				return nil, "读取 Runtime 试运行状态失败: " + truncate(err.Error(), errMsgMaxLen)
			}
		}
	}
	resp, err = s.GetRun(ctx, agent.CreatorID, runID)
	if err != nil {
		return nil, "读取 Runtime 试运行结果失败: " + truncate(err.Error(), errMsgMaxLen)
	}
	finished = resp.Status != "running"
	if resp.Status == "success" {
		return resp.Output, ""
	}
	message := resp.ErrorCode + ": " + resp.ErrorMsg
	if resp.ErrorCode == "" && resp.ErrorMsg == "" {
		message = "Runtime 试运行结束，状态: " + resp.Status
	}
	return nil, truncate(message, errMsgMaxLen)
}
