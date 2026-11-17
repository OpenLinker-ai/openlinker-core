package runtime

import (
	"errors"
	"net/http"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/OpenLinker-ai/openlinker-core/pkg/attachment"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

// RunAttachments validates attachment links in a Run input and binds them to
// the created Run. Nil disables attachment handling (links are then passed to
// the Agent like any other input value).
type RunAttachments interface {
	CheckRunInput(caller uuid.UUID, input any) ([]string, error)
	BindRun(run uuid.UUID, ids []string) error
}

// SetRunAttachments wires the attachment store used at Run creation.
func (s *Service) SetRunAttachments(attachments RunAttachments) { s.attachments = attachments }

func (s *Service) checkRunAttachments(caller uuid.UUID, input any) ([]string, error) {
	if s.attachments == nil {
		return nil, nil
	}
	ids, err := s.attachments.CheckRunInput(caller, input)
	if err == nil {
		return ids, nil
	}
	var field *attachment.FieldError
	if errors.As(err, &field) {
		httpErr := httpx.NewError(http.StatusUnprocessableEntity, httpx.ErrorCode(field.Code), field.Error())
		httpErr.Details = map[string]any{"field": field.Field, "reason": field.Code}
		return nil, httpErr
	}
	log.Error().Err(err).Msg("runtime.Run: check attachments")
	return nil, httpx.Internal("检查附件失败")
}

// bindRunAttachments records the attachments on the committed Run. A failure
// only delays release to the bound lifetime; the Run itself is unaffected.
func (s *Service) bindRunAttachments(run uuid.UUID, ids []string) {
	if s.attachments == nil || len(ids) == 0 {
		return
	}
	if err := s.attachments.BindRun(run, ids); err != nil {
		log.Warn().Err(err).Str("run_id", run.String()).Msg("runtime.Run: bind attachments")
	}
}

// IsTerminalRunStatus reports whether a Run can no longer execute, so its
// attachments may be released.
func IsTerminalRunStatus(status string) bool { return isTerminalRunStatus(status) }
