// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（基于 pkg/db/queries/skills.sql）。
//
// 子轮 2.3：Skill 注册表 + Agent ↔ Skill 关联。

package db

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// scanSkill 把一行扫描成 Skill 结构（按声明列顺序，给所有 SELECT 共用）。
func scanSkill(row interface {
	Scan(dest ...any) error
}, s *Skill) error {
	return row.Scan(
		&s.ID,
		&s.Category,
		&s.Name,
		&s.Description,
		&s.SortOrder,
		&s.CreatedAt,
	)
}

const listSkills = `-- name: ListSkills :many
SELECT id, category, name, description, sort_order, created_at
FROM skills
ORDER BY category, sort_order`

// ListSkills 列出全部内置 skill。
func (q *Queries) ListSkills(ctx context.Context) ([]Skill, error) {
	rows, err := q.db.Query(ctx, listSkills)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Skill
	for rows.Next() {
		var s Skill
		if err := scanSkill(rows, &s); err != nil {
			return nil, err
		}
		items = append(items, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getSkill = `-- name: GetSkill :one
SELECT id, category, name, description, sort_order, created_at
FROM skills
WHERE id = $1`

// GetSkill 按 id 取单条 skill；用于校验 Agent 声明的 skill 是否存在。
func (q *Queries) GetSkill(ctx context.Context, id string) (Skill, error) {
	row := q.db.QueryRow(ctx, getSkill, id)
	var s Skill
	err := scanSkill(row, &s)
	return s, err
}

const listAgentSkills = `-- name: ListAgentSkills :many
SELECT s.id, s.category, s.name, s.description, s.sort_order, s.created_at
FROM agent_skills ag
JOIN skills s ON s.id = ag.skill_id
WHERE ag.agent_id = $1
ORDER BY s.category, s.sort_order`

// ListAgentSkills 列出某 Agent 已声明的全部 skill 详情行。
func (q *Queries) ListAgentSkills(ctx context.Context, agentID uuid.UUID) ([]Skill, error) {
	rows, err := q.db.Query(ctx, listAgentSkills, agentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Skill
	for rows.Next() {
		var s Skill
		if err := scanSkill(rows, &s); err != nil {
			return nil, err
		}
		items = append(items, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// 下面两条不是单条 sqlc 语句，由调用方在事务里 / 用数组传入：
//
//   ReplaceAgentSkills    DELETE + 批量 INSERT（事务内）
//   ListAgentsBySkills    UNNEST 数组 + GROUP BY，返回 (agent_id, match_count)

const deleteAgentSkills = `-- name: DeleteAgentSkills :exec
DELETE FROM agent_skills WHERE agent_id = $1`

const insertAgentSkill = `-- name: InsertAgentSkill :exec
INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, $2)`

// ReplaceAgentSkills 在事务内用新 skillIDs 覆盖某 Agent 的关联。
//
// 调用方负责 BeginTx + Commit/Rollback；本函数只负责在传入 tx 上执行
// DELETE + 批量 INSERT。skillIDs 为空时仅清空。
//
// 调用约束：
//   - skillIDs 必须已被调用方校验（存在性 + 上限）
//   - 由于 PRIMARY KEY (agent_id, skill_id)，重复元素会让 INSERT 失败；
//     调用方应保证去重。
func ReplaceAgentSkills(ctx context.Context, tx pgx.Tx, agentID uuid.UUID, skillIDs []string) error {
	if _, err := tx.Exec(ctx, deleteAgentSkills, agentID); err != nil {
		return err
	}
	for _, sid := range skillIDs {
		if _, err := tx.Exec(ctx, insertAgentSkill, agentID, sid); err != nil {
			return err
		}
	}
	return nil
}

const listAgentsBySkills = `-- name: ListAgentsBySkills :many
SELECT a.id AS agent_id,
       COUNT(*)::int AS match_count,
       a.total_calls
FROM agent_skills ag
JOIN agents a ON a.id = ag.agent_id
WHERE ag.skill_id = ANY($1::text[])
  AND a.status = 'approved'
GROUP BY a.id, a.total_calls
ORDER BY match_count DESC, a.total_calls DESC, a.id`

// AgentSkillMatch 推荐结果行：命中了多少个输入 skill + 累计调用次数。
type AgentSkillMatch struct {
	AgentID    uuid.UUID `db:"agent_id" json:"agent_id"`
	MatchCount int32     `db:"match_count" json:"match_count"`
	TotalCalls int32     `db:"total_calls" json:"total_calls"`
}

// ListAgentsBySkills 任务驱动推荐：返回每个 approved Agent 命中了多少个输入 skill。
//
// 排序：match_count desc, total_calls desc, id（稳定 tie-break）。
// 上层（任务模块 2.4）通常会再按 match_count >= 阈值过滤并截 top-N。
func (q *Queries) ListAgentsBySkills(ctx context.Context, skillIDs []string) ([]AgentSkillMatch, error) {
	rows, err := q.db.Query(ctx, listAgentsBySkills, skillIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AgentSkillMatch
	for rows.Next() {
		var m AgentSkillMatch
		if err := rows.Scan(&m.AgentID, &m.MatchCount, &m.TotalCalls); err != nil {
			return nil, err
		}
		items = append(items, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
