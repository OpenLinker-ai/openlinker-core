package agent

import (
	"context"

	"github.com/google/uuid"
)

// SetAvailabilityResultBeforeCommitHook installs a hook that runs inside the
// monitor's guarded result transaction right before it commits.
func (s *Service) SetAvailabilityResultBeforeCommitHook(hook func(ctx context.Context, agentID uuid.UUID)) {
	s.availabilityResultBeforeCommit = hook
}
