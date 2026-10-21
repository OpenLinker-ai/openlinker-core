package mcpbinding

import (
	"context"
	"github.com/google/uuid"
)

// HostAvailability is shared by the owner view and Run admission. Offline
// queuing is allowed for an unknown or last-known compatible Host. An offline
// known incompatible Host must not be presented as merely waiting to reconnect.
type HostAvailability struct {
	Connected  bool
	Known      bool
	Compatible bool
}

func CheckHost(ctx context.Context, q DB, agent uuid.UUID) (HostAvailability, error) {
	var h HostAvailability
	err := q.QueryRow(ctx, `WITH current AS (SELECT features FROM runtime_sessions WHERE agent_id=$1 AND status NOT IN ('closed','revoked')),
 hosts AS (SELECT features FROM current UNION ALL SELECT features FROM (SELECT features FROM runtime_sessions WHERE agent_id=$1 AND status='closed' ORDER BY created_at DESC,runtime_session_id DESC LIMIT 1) h WHERE NOT EXISTS(SELECT 1 FROM current))
 SELECT EXISTS(SELECT 1 FROM runtime_sessions WHERE agent_id=$1 AND status IN ('active','draining')),
 count(*)>0,COALESCE(bool_or($2=ANY(features)),false) FROM hosts`, agent, Feature).Scan(&h.Connected, &h.Known, &h.Compatible)
	return h, err
}

func (h HostAvailability) Status() string {
	if h.Known && !h.Compatible {
		return "incompatible"
	}
	if h.Connected && h.Compatible {
		return "compatible"
	}
	return "offline"
}
