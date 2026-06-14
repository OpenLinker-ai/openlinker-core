package task

import (
	"context"
	"strings"

	db "github.com/kinzhi/openlinker-core/pkg/db/generated"
	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

type taskTemplate struct {
	ID                    string
	Slug                  string
	Title                 string
	Category              string
	Summary               string
	RequiredSkillIDs      []string
	RequiredMCPTools      []string
	ExampleQuery          string
	ExpectedArtifactTypes []string
}

var taskTemplateCatalog = []taskTemplate{
	{
		ID:               "support-review",
		Slug:             "support-review",
		Title:            "客服工单复盘",
		Category:         "support",
		Summary:          "把客服对话整理成问题分类、情绪、根因和下一步动作。",
		RequiredSkillIDs: []string{"content/summarization", "content/structured-data"},
		ExampleQuery:     "请复盘这段客服对话，输出问题分类、客户情绪、根因和下一步动作。",
		ExpectedArtifactTypes: []string{
			"json", "text",
		},
	},
	{
		ID:               "code-review",
		Slug:             "code-review",
		Title:            "代码审查摘要",
		Category:         "engineering",
		Summary:          "审查 diff、PR 描述或代码片段，输出风险、阻断项和测试建议。",
		RequiredSkillIDs: []string{"dev/code-review"},
		ExampleQuery:     "请审查这段 diff，指出潜在 bug、安全风险和需要补的测试。",
		ExpectedArtifactTypes: []string{
			"text", "json",
		},
	},
	{
		ID:               "data-summary",
		Slug:             "data-summary",
		Title:            "数据表摘要",
		Category:         "data",
		Summary:          "从指标 JSON、表格摘要或 CSV 片段中提取趋势、异常和业务结论。",
		RequiredSkillIDs: []string{"data/analysis"},
		ExampleQuery:     "请分析这组周度指标，输出趋势、异常点和建议继续追问的问题。",
		ExpectedArtifactTypes: []string{
			"json", "text",
		},
	},
	{
		ID:               "competitor-pricing",
		Slug:             "competitor-pricing",
		Title:            "竞品定价研究",
		Category:         "market",
		Summary:          "整理竞品、差异化能力、定价区间和 OpenLinker 可学习的产品动作。",
		RequiredSkillIDs: []string{"ops/web-scraping", "data/analysis"},
		ExampleQuery:     "请分析 4 家竞品的定位、定价逻辑、优势劣势，并给出我们应该学习的点。",
		ExpectedArtifactTypes: []string{
			"text", "json",
		},
	},
	{
		ID:               "contract-risk",
		Slug:             "contract-risk",
		Title:            "合同风险清单",
		Category:         "legal",
		Summary:          "从合同或条款摘要中抽取风险点、责任边界和需要人工复核的条款。",
		RequiredSkillIDs: []string{"content/structured-data", "ops/document-generate"},
		ExampleQuery:     "请从这份合同摘要中抽取高风险条款、责任边界和需要人工复核的问题。",
		ExpectedArtifactTypes: []string{
			"json", "text",
		},
	},
}

func (s *Service) ListTaskTemplates(ctx context.Context) ([]TaskTemplateResponse, error) {
	skills, err := s.skills(ctx)
	if err != nil {
		return nil, err
	}
	skillByID := skillCatalogByID(skills)
	items := make([]TaskTemplateResponse, 0, len(taskTemplateCatalog))
	for _, tmpl := range taskTemplateCatalog {
		items = append(items, taskTemplateResponse(tmpl, skillByID))
	}
	return items, nil
}

func (s *Service) taskTemplateByID(ctx context.Context, id string) (*taskTemplate, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, nil
	}
	for _, tmpl := range taskTemplateCatalog {
		if tmpl.ID == id || tmpl.Slug == id {
			copy := tmpl
			return &copy, nil
		}
	}
	return nil, httpx.BadRequest("template_id 不存在")
}

func taskTemplateResponse(tmpl taskTemplate, skillByID map[string]db.Skill) TaskTemplateResponse {
	return TaskTemplateResponse{
		ID:                    tmpl.ID,
		Slug:                  tmpl.Slug,
		Title:                 tmpl.Title,
		Category:              tmpl.Category,
		Summary:               tmpl.Summary,
		RequiredSkillIDs:      append([]string{}, tmpl.RequiredSkillIDs...),
		RequiredSkillRefs:     skillRefsForIDs(tmpl.RequiredSkillIDs, skillByID),
		RequiredMCPTools:      append([]string{}, tmpl.RequiredMCPTools...),
		RequiredMCPToolRefs:   mcpToolRefsForNames(tmpl.RequiredMCPTools),
		ExampleQuery:          tmpl.ExampleQuery,
		ExpectedArtifactTypes: append([]string{}, tmpl.ExpectedArtifactTypes...),
		DefaultVisibility:     taskVisibilityPrivate,
	}
}

func mergeTemplateSkillIDs(tmpl *taskTemplate, explicit []string) []string {
	if tmpl == nil || len(tmpl.RequiredSkillIDs) == 0 {
		return explicit
	}
	return mergeSkillIDs(tmpl.RequiredSkillIDs, explicit, maxTaskSkillRefs)
}

func mergeTemplateMCPTools(tmpl *taskTemplate, explicit []string) []string {
	if tmpl == nil || len(tmpl.RequiredMCPTools) == 0 {
		return explicit
	}
	return mergeSkillIDs(tmpl.RequiredMCPTools, explicit, maxTaskMCPTools)
}
