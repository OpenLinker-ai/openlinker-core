package skillpackage

import (
	"context"
	"slices"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type queryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

type packageHost struct {
	providers []string
	status    string
}

// Current sessions are candidates individually, never a union of providers.
// Only Run admission may fall back to the most recent closed session when all
// current sessions are gone. Revoked sessions are not compatibility evidence.
func packageHosts(ctx context.Context, db queryer, agentID uuid.UUID, history bool) ([]packageHost, error) {
	rows, err := db.Query(ctx, `WITH current_hosts AS (
 SELECT status,features FROM runtime_sessions WHERE agent_id=$1 AND status NOT IN ('closed','revoked'))
 SELECT status,features FROM current_hosts
 UNION ALL
 SELECT status,features FROM (
 SELECT status,features FROM runtime_sessions WHERE agent_id=$1 AND status='closed'
 ORDER BY created_at DESC,runtime_session_id DESC LIMIT 1) h
 WHERE $2 AND NOT EXISTS(SELECT 1 FROM current_hosts)`, agentID, history)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var hosts []packageHost
	for rows.Next() {
		var status string
		var features []string
		if err := rows.Scan(&status, &features); err != nil {
			return nil, err
		}
		hosts = append(hosts, packageHost{providers: CompatibleProviders(features), status: status})
	}
	return hosts, rows.Err()
}

func (host packageHost) supports(requirements [][]string) bool {
	if len(host.providers) == 0 {
		return false
	}
	for _, allowed := range requirements {
		if !slices.ContainsFunc(allowed, func(provider string) bool { return slices.Contains(host.providers, provider) }) {
			return false
		}
	}
	return true
}

func anyPackageHost(hosts []packageHost, requirements [][]string) bool {
	return slices.ContainsFunc(hosts, func(host packageHost) bool { return host.supports(requirements) })
}

func packageRequirements(ctx context.Context, db queryer, agentID, replacedPackage uuid.UUID) ([][]string, error) {
	rows, err := db.Query(ctx, `SELECT v.providers FROM agent_skill_package_bindings b
 JOIN skill_package_versions v ON v.id=b.version_id WHERE b.agent_id=$1 AND b.package_id<>$2`, agentID, replacedPackage)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var requirements [][]string
	for rows.Next() {
		var providers []string
		if err := rows.Scan(&providers); err != nil {
			return nil, err
		}
		requirements = append(requirements, providers)
	}
	return requirements, rows.Err()
}
