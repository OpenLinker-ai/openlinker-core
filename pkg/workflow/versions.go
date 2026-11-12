package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog/log"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

const (
	workflowSemanticsV1 = "v1"
	workflowSemanticsV2 = "v2"

	workflowStatusActive   = "active"
	workflowStatusArchived = "archived"

	workflowVersionConflictCode = httpx.ErrorCode("WORKFLOW_VERSION_CONFLICT")
	workflowArchivedCode        = httpx.ErrorCode("WORKFLOW_ARCHIVED")
)

// workflowVersionDefinition is one immutable workflow version: the header
// (with the version's name, description and edges) and its nodes, shaped as
// the generated workflow types the engine already uses.
type workflowVersionDefinition struct {
	workflow   db.Workflow
	nodes      []db.WorkflowNode
	id         uuid.UUID
	number     int32
	semantics  string
	digest     string
	createdAt  time.Time
	createdBy  *uuid.UUID
	outputJSON []byte
}

type workflowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

// loadWorkflowVersion reads one version of a workflow. versionID nil means
// the workflow's current version, read from the same header row, so a
// concurrent edit can never mix one version's edges with another's nodes.
func loadWorkflowVersion(ctx context.Context, q workflowQuerier, workflowID uuid.UUID, versionID *uuid.UUID) (workflowVersionDefinition, error) {
	var def workflowVersionDefinition
	var current *uuid.UUID
	w := &def.workflow
	if err := q.QueryRow(ctx, `
		SELECT id, user_id, status, created_at, updated_at, current_version_id
		FROM workflows WHERE id = $1
	`, workflowID).Scan(&w.ID, &w.UserID, &w.Status, &w.CreatedAt, &w.UpdatedAt, &current); err != nil {
		return def, err
	}
	if versionID == nil {
		versionID = current
	}
	if versionID == nil {
		return def, errors.New("workflow has no current version")
	}
	if err := q.QueryRow(ctx, `
		SELECT id, version, semantics, name, description, edges, output_mapping, definition_digest, created_by, created_at
		FROM workflow_versions WHERE id = $1 AND workflow_id = $2
	`, *versionID, workflowID).Scan(&def.id, &def.number, &def.semantics, &w.Name, &w.Description, &w.Edges,
		&def.outputJSON, &def.digest, &def.createdBy, &def.createdAt); err != nil {
		return def, err
	}
	rows, err := q.Query(ctx, `
		SELECT id, node_key, node_type, agent_id, title, config, "position", created_at
		FROM workflow_version_nodes WHERE workflow_version_id = $1
		ORDER BY "position", created_at, id
	`, def.id)
	if err != nil {
		return def, err
	}
	defer rows.Close()
	for rows.Next() {
		node := db.WorkflowNode{WorkflowID: workflowID}
		if err := rows.Scan(&node.ID, &node.NodeKey, &node.NodeType, &node.AgentID, &node.Title,
			&node.Config, &node.Position, &node.CreatedAt); err != nil {
			return def, err
		}
		def.nodes = append(def.nodes, node)
	}
	return def, rows.Err()
}

func (s *Service) workflowQuerier() workflowQuerier {
	if s.pool != nil {
		return s.pool
	}
	return nil
}

// loadOwnedWorkflowVersion loads a version of a workflow the user owns.
func (s *Service) loadOwnedWorkflowVersion(ctx context.Context, userID, workflowID uuid.UUID, versionID *uuid.UUID) (workflowVersionDefinition, error) {
	q := s.workflowQuerier()
	if q == nil {
		return workflowVersionDefinition{}, httpx.Internal("workflow 存储未配置")
	}
	def, err := loadWorkflowVersion(ctx, q, workflowID, versionID)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && def.workflow.UserID != userID) {
		return workflowVersionDefinition{}, httpx.NotFound("workflow 不存在")
	}
	if err != nil {
		log.Error().Err(err).Str("workflow_id", workflowID.String()).Msg("workflow.loadOwnedWorkflowVersion")
		return workflowVersionDefinition{}, httpx.Internal("查询 workflow 失败")
	}
	return def, nil
}

// workflowRunVersionID returns the version a run executes.
func (s *Service) workflowRunVersionID(ctx context.Context, runID uuid.UUID) (uuid.UUID, error) {
	var versionID uuid.UUID
	err := s.pool.QueryRow(ctx, `SELECT workflow_version_id FROM workflow_runs WHERE id = $1`, runID).Scan(&versionID)
	return versionID, err
}

// workflowDefinitionInput is a validated definition ready to be written as
// a new version.
type workflowDefinitionInput struct {
	name        string
	description string
	edgesJSON   []byte
	nodes       []WorkflowNodeRequest
	semantics   string
	outputJSON  []byte
}

// prepareWorkflowDefinition validates a definition request exactly as
// workflow creation always did.
func (s *Service) prepareWorkflowDefinition(ctx context.Context, userID uuid.UUID, req *CreateWorkflowRequest) (workflowDefinitionInput, error) {
	name, description, nodes, rawEdges := req.Name, req.Description, req.Nodes, req.Edges
	semantics := strings.TrimSpace(req.Semantics)
	if semantics == "" {
		semantics = workflowSemanticsV1
	}
	if semantics != workflowSemanticsV1 && semantics != workflowSemanticsV2 {
		return workflowDefinitionInput{}, httpx.BadRequest("semantics 只接受 v1 或 v2")
	}
	if semantics == workflowSemanticsV1 && req.OutputMapping != nil {
		return workflowDefinitionInput{}, httpx.BadRequest("output_mapping 需要 semantics v2")
	}
	if len(nodes) == 0 {
		return workflowDefinitionInput{}, httpx.BadRequest("workflow 至少需要一个 Agent 节点")
	}
	maxNodes, maxEdges := s.workflowLimits()
	if len(nodes) > maxNodes {
		return workflowDefinitionInput{}, httpx.BadRequest(fmt.Sprintf("workflow 节点不能超过 %d 个", maxNodes))
	}
	if len(rawEdges) > maxEdges {
		return workflowDefinitionInput{}, httpx.BadRequest(fmt.Sprintf("workflow 依赖不能超过 %d 条", maxEdges))
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return workflowDefinitionInput{}, httpx.BadRequest("workflow 名称不能为空")
	}
	edges, err := normalizeWorkflowEdgesFromRequest(nodes, rawEdges)
	if err != nil {
		return workflowDefinitionInput{}, err
	}
	if err := validateWorkflowGraphFromRequest(nodes, edges); err != nil {
		return workflowDefinitionInput{}, err
	}
	if err := s.validateWorkflowRequestAgentsAvailable(ctx, userID, nodes, false); err != nil {
		return workflowDefinitionInput{}, err
	}
	var outputJSON []byte
	if semantics == workflowSemanticsV2 {
		v2Nodes := make([]workflowV2NodeInput, 0, len(nodes))
		for _, node := range nodes {
			config, err := json.Marshal(node.Config)
			if err != nil {
				return workflowDefinitionInput{}, httpx.BadRequest("node config 不是合法 JSON")
			}
			v2Nodes = append(v2Nodes, workflowV2NodeInput{key: strings.TrimSpace(node.Key), config: config})
		}
		var output interface{}
		if req.OutputMapping != nil {
			output = req.OutputMapping
		}
		if _, err := parseWorkflowV2Definition(v2Nodes, edges, output); err != nil {
			return workflowDefinitionInput{}, httpx.NewError(422, workflowInputMappingInvalidCode, "workflow v2 定义无效: "+err.Error())
		}
		if req.OutputMapping != nil {
			if outputJSON, err = json.Marshal(req.OutputMapping); err != nil {
				return workflowDefinitionInput{}, httpx.BadRequest("output_mapping 不是合法 JSON")
			}
		}
	} else if err := s.validateWorkflowRequestInputMappings(ctx, nodes, edges); err != nil {
		return workflowDefinitionInput{}, err
	}
	edgesJSON, err := json.Marshal(edges)
	if err != nil {
		return workflowDefinitionInput{}, httpx.BadRequest("edges 不是合法 JSON")
	}
	seen := map[string]struct{}{}
	for _, node := range nodes {
		key := strings.TrimSpace(node.Key)
		if key == "" {
			return workflowDefinitionInput{}, httpx.BadRequest("workflow node key 不能为空")
		}
		if _, ok := seen[key]; ok {
			return workflowDefinitionInput{}, httpx.BadRequest("workflow node key 不能重复: " + key)
		}
		if node.AgentID == uuid.Nil {
			return workflowDefinitionInput{}, httpx.BadRequest("workflow node agent_id 不能为空")
		}
		seen[key] = struct{}{}
	}
	return workflowDefinitionInput{
		name: name, description: strings.TrimSpace(description), edgesJSON: edgesJSON, nodes: nodes,
		semantics: semantics, outputJSON: outputJSON,
	}, nil
}

// writeWorkflowVersion is the single version creation transaction step used
// by create, update and duplicate. It locks the workflow row, numbers the
// version, writes the immutable version and its nodes (new node UUIDs),
// replaces the workflow_nodes projection with the same UUIDs, and moves the
// header and current_version_id to it.
func writeWorkflowVersion(ctx context.Context, tx pgx.Tx, workflowID, userID uuid.UUID, def workflowDefinitionInput) error {
	var locked int
	if err := tx.QueryRow(ctx, `SELECT 1 FROM workflows WHERE id = $1 FOR UPDATE`, workflowID).Scan(&locked); err != nil {
		return err
	}
	var versionID uuid.UUID
	if err := tx.QueryRow(ctx, `
		INSERT INTO workflow_versions (workflow_id, version, semantics, name, description, edges, output_mapping, definition_digest, created_by)
		SELECT $1, COALESCE(max(version), 0) + 1, $6, $2, $3, $4, $7::jsonb, repeat('0', 64), $5
		FROM workflow_versions WHERE workflow_id = $1
		RETURNING id
	`, workflowID, def.name, def.description, def.edgesJSON, userID, def.semantics, nullableJSON(def.outputJSON)).Scan(&versionID); err != nil {
		return err
	}
	for i, node := range def.nodes {
		key := strings.TrimSpace(node.Key)
		title := strings.TrimSpace(node.Title)
		if title == "" {
			title = "Agent " + key
		}
		config := node.Config
		if config == nil {
			config = map[string]interface{}{}
		}
		configJSON, err := json.Marshal(config)
		if err != nil {
			return httpx.BadRequest("node config 不是合法 JSON")
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO workflow_version_nodes (workflow_version_id, node_key, node_type, agent_id, title, config, "position")
			VALUES ($1, $2, 'agent', $3, $4, $5, $6)
		`, versionID, key, node.AgentID, title, configJSON, i); err != nil {
			return err
		}
	}
	// Same digest statement as migration 105.
	if _, err := tx.Exec(ctx, `
		UPDATE workflow_versions v
		SET definition_digest = encode(sha256(convert_to(jsonb_build_object(
		        'semantics', v.semantics,
		        'edges', v.edges,
		        'output_mapping', v.output_mapping,
		        'nodes', COALESCE((
		            SELECT jsonb_agg(jsonb_build_object(
		                'node_key', vn.node_key, 'node_type', vn.node_type, 'agent_id', vn.agent_id,
		                'title', vn.title, 'config', vn.config, 'position', vn."position"
		            ) ORDER BY vn.node_key)
		            FROM workflow_version_nodes vn WHERE vn.workflow_version_id = v.id
		        ), '[]'::jsonb)
		    )::text, 'UTF8')), 'hex')
		WHERE v.id = $1
	`, versionID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM workflow_nodes WHERE workflow_id = $1`, workflowID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO workflow_nodes (id, workflow_id, node_key, node_type, agent_id, title, config, "position", created_at)
		SELECT id, $2, node_key, node_type, agent_id, title, config, "position", created_at
		FROM workflow_version_nodes WHERE workflow_version_id = $1
	`, versionID, workflowID); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		UPDATE workflows
		SET name = $2, description = $3, edges = $4, current_version_id = $5, updated_at = NOW()
		WHERE id = $1
	`, workflowID, def.name, def.description, def.edgesJSON, versionID)
	return err
}

func workflowWriteError(err error, op string, workflowID uuid.UUID) error {
	var he *httpx.HTTPError
	if errors.As(err, &he) {
		return err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return httpx.NotFound("workflow 不存在")
	}
	log.Error().Err(err).Str("workflow_id", workflowID.String()).Msg(op)
	return httpx.Internal("保存 workflow 失败")
}

func (s *Service) workflowResponseForVersion(def workflowVersionDefinition) *WorkflowResponse {
	resp := workflowToResponse(def.workflow, def.nodes)
	resp.Version = def.number
	resp.VersionID = def.id.String()
	resp.Semantics = def.semantics
	if len(def.outputJSON) > 0 {
		_ = json.Unmarshal(def.outputJSON, &resp.OutputMapping)
	}
	return &resp
}

// UpdateWorkflow writes a new version. expectedVersion must be the current
// version number (optimistic concurrency, If-Match); archived workflows
// cannot get new versions.
func (s *Service) UpdateWorkflow(ctx context.Context, userID, workflowID uuid.UUID, expectedVersion int32, req *UpdateWorkflowRequest) (*WorkflowResponse, error) {
	if req == nil {
		return nil, httpx.BadRequest("请求体不能为空")
	}
	if expectedVersion < 1 {
		return nil, httpx.NewError(428, workflowVersionConflictCode, "更新 workflow 需要 If-Match 当前版本号")
	}
	if _, err := s.loadOwnedWorkflowVersion(ctx, userID, workflowID, nil); err != nil {
		return nil, err
	}
	def, err := s.prepareWorkflowDefinition(ctx, userID, req)
	if err != nil {
		return nil, err
	}
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var owner uuid.UUID
		var status string
		var current int32
		if err := tx.QueryRow(ctx, `
			SELECT w.user_id, w.status, v.version
			FROM workflows w JOIN workflow_versions v ON v.id = w.current_version_id
			WHERE w.id = $1 FOR UPDATE OF w
		`, workflowID).Scan(&owner, &status, &current); err != nil {
			return err
		}
		if owner != userID {
			return httpx.NotFound("workflow 不存在")
		}
		if status != workflowStatusActive {
			return httpx.NewError(409, workflowArchivedCode, "workflow 已归档，不能修改")
		}
		if current != expectedVersion {
			e := httpx.NewError(409, workflowVersionConflictCode, "workflow 已被修改，请基于最新版本重新编辑")
			e.Details = map[string]any{"current_version": current}
			return e
		}
		return writeWorkflowVersion(ctx, tx, workflowID, userID, def)
	})
	if err != nil {
		return nil, workflowWriteError(err, "workflow.UpdateWorkflow", workflowID)
	}
	return s.GetWorkflow(ctx, userID, workflowID)
}

// DuplicateWorkflow creates a new workflow whose version 1 copies the
// source's current version.
func (s *Service) DuplicateWorkflow(ctx context.Context, userID, workflowID uuid.UUID, req *DuplicateWorkflowRequest) (*WorkflowResponse, error) {
	source, err := s.loadOwnedWorkflowVersion(ctx, userID, workflowID, nil)
	if err != nil {
		return nil, err
	}
	name := source.workflow.Name + " (副本)"
	if req != nil && strings.TrimSpace(req.Name) != "" {
		name = req.Name
	}
	var edges []map[string]interface{}
	if len(source.workflow.Edges) > 0 {
		if err := json.Unmarshal(source.workflow.Edges, &edges); err != nil {
			return nil, httpx.Internal("workflow edges 不是合法 JSON")
		}
	}
	nodes := make([]WorkflowNodeRequest, 0, len(source.nodes))
	for _, node := range source.nodes {
		config := map[string]interface{}{}
		if len(node.Config) > 0 {
			_ = json.Unmarshal(node.Config, &config)
		}
		nodes = append(nodes, WorkflowNodeRequest{Key: node.NodeKey, Title: node.Title, AgentID: node.AgentID, Config: config})
	}
	var output map[string]interface{}
	if len(source.outputJSON) > 0 {
		_ = json.Unmarshal(source.outputJSON, &output)
	}
	return s.CreateWorkflow(ctx, userID, &CreateWorkflowRequest{
		Name: name, Description: source.workflow.Description, Nodes: nodes, Edges: edges,
		Semantics: source.semantics, OutputMapping: output,
	})
}

// SetWorkflowArchived archives or restores a workflow. An archived workflow
// accepts no new runs or versions; existing runs are unaffected.
func (s *Service) SetWorkflowArchived(ctx context.Context, userID, workflowID uuid.UUID, archived bool) (*WorkflowResponse, error) {
	status := workflowStatusActive
	if archived {
		status = workflowStatusArchived
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE workflows SET status = $3, updated_at = NOW()
		WHERE id = $1 AND user_id = $2
	`, workflowID, userID, status)
	if err != nil {
		return nil, workflowWriteError(err, "workflow.SetWorkflowArchived", workflowID)
	}
	if tag.RowsAffected() == 0 {
		return nil, httpx.NotFound("workflow 不存在")
	}
	return s.GetWorkflow(ctx, userID, workflowID)
}

// ListWorkflowVersions lists a workflow's versions, newest first.
func (s *Service) ListWorkflowVersions(ctx context.Context, userID, workflowID uuid.UUID) (*WorkflowVersionListResponse, error) {
	current, err := s.loadOwnedWorkflowVersion(ctx, userID, workflowID, nil)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT v.id, v.version, v.semantics, v.name, v.definition_digest, v.created_at,
		       (SELECT count(*) FROM workflow_version_nodes n WHERE n.workflow_version_id = v.id)
		FROM workflow_versions v WHERE v.workflow_id = $1
		ORDER BY v.version DESC
	`, workflowID)
	if err != nil {
		return nil, workflowWriteError(err, "workflow.ListWorkflowVersions", workflowID)
	}
	defer rows.Close()
	resp := &WorkflowVersionListResponse{Items: []WorkflowVersionSummary{}, CurrentVersion: current.number}
	for rows.Next() {
		var item WorkflowVersionSummary
		var id uuid.UUID
		var createdAt time.Time
		if err := rows.Scan(&id, &item.Version, &item.Semantics, &item.Name, &item.DefinitionDigest, &createdAt, &item.NodeCount); err != nil {
			return nil, workflowWriteError(err, "workflow.ListWorkflowVersions", workflowID)
		}
		item.ID = id.String()
		item.CreatedAt = createdAt.UTC().Format(time.RFC3339)
		item.Current = item.Version == current.number
		resp.Items = append(resp.Items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, workflowWriteError(err, "workflow.ListWorkflowVersions", workflowID)
	}
	return resp, nil
}

// GetWorkflowVersion returns one version's full definition.
func (s *Service) GetWorkflowVersion(ctx context.Context, userID, workflowID uuid.UUID, version int32) (*WorkflowResponse, error) {
	versionID, err := s.resolveWorkflowVersionNumber(ctx, workflowID, version)
	if err != nil {
		return nil, err
	}
	def, err := s.loadOwnedWorkflowVersion(ctx, userID, workflowID, &versionID)
	if err != nil {
		return nil, err
	}
	return s.workflowResponseForVersion(def), nil
}

func (s *Service) resolveWorkflowVersionNumber(ctx context.Context, workflowID uuid.UUID, version int32) (uuid.UUID, error) {
	var versionID uuid.UUID
	err := s.pool.QueryRow(ctx, `
		SELECT id FROM workflow_versions WHERE workflow_id = $1 AND version = $2
	`, workflowID, version).Scan(&versionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, httpx.NotFound("workflow 版本不存在")
	}
	return versionID, err
}

var errWorkflowArchived = httpx.NewError(409, workflowArchivedCode, "workflow 已归档，不能创建新的运行")

// requestedWorkflowVersionID resolves an explicit run version; nil means
// the current version (resolved atomically when the run is created).
func (s *Service) requestedWorkflowVersionID(ctx context.Context, workflowID uuid.UUID, req *RunWorkflowRequest) (*uuid.UUID, error) {
	if req == nil || req.Version <= 0 || s.pool == nil {
		return nil, nil
	}
	id, err := s.resolveWorkflowVersionNumber(ctx, workflowID, req.Version)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// workflowRunCreateError maps run creation results: no row while draining,
// a NULL version (the not-null violation) when the workflow is archived.
func workflowRunCreateError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return errWorkflowAdmissionDraining
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23502" && pgErr.ColumnName == "workflow_version_id" {
		return errWorkflowArchived
	}
	return err
}

func isWorkflowHTTPError(err error) bool {
	var he *httpx.HTTPError
	return errors.As(err, &he)
}

func nullableJSON(raw []byte) interface{} {
	if len(raw) == 0 {
		return nil
	}
	return string(raw)
}

const (
	defaultWorkflowMaxNodes = 30
	defaultWorkflowMaxEdges = 60
)

// SetWorkflowLimits sets the definition size limits (Q4 defaults 30/60).
func (s *Service) SetWorkflowLimits(maxNodes, maxEdges int) {
	if maxNodes > 0 && maxNodes <= 100 {
		s.maxNodes = maxNodes
	}
	if maxEdges > 0 && maxEdges <= 200 {
		s.maxEdges = maxEdges
	}
}

func (s *Service) workflowLimits() (int, int) {
	nodes, edges := defaultWorkflowMaxNodes, defaultWorkflowMaxEdges
	if s.maxNodes > 0 {
		nodes = s.maxNodes
	}
	if s.maxEdges > 0 {
		edges = s.maxEdges
	}
	return nodes, edges
}
