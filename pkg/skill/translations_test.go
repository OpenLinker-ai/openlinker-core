package skill

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func TestCanonicalSkillEnglishTranslations(t *testing.T) {
	require.Len(t, englishSkillTranslations, 30)
	han := regexp.MustCompile(`[\p{Han}]`)
	for skillID, translation := range englishSkillTranslations {
		require.NotEmpty(t, translation.Name, skillID)
		require.NotEmpty(t, translation.Description, skillID)
		require.False(t, han.MatchString(translation.Name), skillID)
		require.False(t, han.MatchString(translation.Description), skillID)
	}
}

func TestCanonicalSkillEnglishTranslationKeysMatchSeedCatalog(t *testing.T) {
	seedPath := filepath.Join("..", "..", "migrations", "086_current_schema_init.up.sql")
	seedSQL, err := os.ReadFile(seedPath)
	require.NoError(t, err)

	rowPattern := regexp.MustCompile(`(?m)^\s*\('([^']+)',\s*'(?:content|dev|data|media|ops|ai)',`)
	matches := rowPattern.FindAllStringSubmatch(string(seedSQL), -1)
	seedIDs := make([]string, 0, len(matches))
	for _, match := range matches {
		seedIDs = append(seedIDs, match[1])
	}
	translationIDs := make([]string, 0, len(englishSkillTranslations))
	for skillID := range englishSkillTranslations {
		translationIDs = append(translationIDs, skillID)
	}

	require.Len(t, seedIDs, 30)
	require.ElementsMatch(t, seedIDs, translationIDs)
}

func TestTranslationsForSkillReturnsIndependentStructuredCopy(t *testing.T) {
	first := translationsForSkill("dev/code-review")
	require.Equal(t, SkillTranslation{
		Name:        "Code Review",
		Description: "Review pull requests for style issues, risks, and potential bugs.",
	}, first["en"])

	first["en"] = SkillTranslation{Name: "mutated"}
	second := translationsForSkill("dev/code-review")
	require.Equal(t, "Code Review", second["en"].Name)
	require.Nil(t, translationsForSkill("custom/report-builder"))
}

func TestSkillItemJSONIncludesOnlyKnownStructuredTranslations(t *testing.T) {
	known, err := json.Marshal(toSkillItem(&db.Skill{
		ID: "dev/code-review", Category: "dev", Name: "代码审查", Description: "中文描述",
	}))
	require.NoError(t, err)
	require.JSONEq(t, `{
		"id":"dev/code-review",
		"category":"dev",
		"name":"代码审查",
		"description":"中文描述",
		"sort_order":0,
		"translations":{"en":{"name":"Code Review","description":"Review pull requests for style issues, risks, and potential bugs."}}
	}`, string(known))

	custom, err := json.Marshal(toSkillItem(&db.Skill{
		ID: "custom/report-builder", Category: "custom", Name: "自定义报告", Description: "中文描述",
	}))
	require.NoError(t, err)
	require.NotContains(t, string(custom), "translations")
}

func TestEnglishSkillSearchSortAndPaginationUseDisplayedCopy(t *testing.T) {
	rows := []db.Skill{
		{ID: "content/translation", Name: "翻译"},
		{ID: "dev/code-review", Name: "代码审查"},
		{ID: "ai/agent-orchestration", Name: "Agent 编排"},
	}

	descriptionMatch := filterEnglishSkillRows(rows, "pull requests")
	require.Equal(t, []string{"dev/code-review"}, skillIDs(descriptionMatch))
	require.Empty(t, filterEnglishSkillRows(rows, "代码审查"))
	require.Equal(t, []string{"dev/code-review"}, skillIDs(filterEnglishSkillRows(rows, "dev/code")))

	ascending := append([]db.Skill(nil), rows...)
	sortEnglishSkillRows(ascending, false)
	require.Equal(t, []string{
		"ai/agent-orchestration",
		"dev/code-review",
		"content/translation",
	}, skillIDs(ascending))

	descending := append([]db.Skill(nil), rows...)
	sortEnglishSkillRows(descending, true)
	require.Equal(t, []string{
		"content/translation",
		"dev/code-review",
		"ai/agent-orchestration",
	}, skillIDs(descending))
	require.Equal(t, []string{"dev/code-review"}, skillIDs(paginateSkillRows(ascending, 2, 1)))
	require.Empty(t, paginateSkillRows(ascending, 4, 1))
	require.Equal(t, "en", normalizeSkillLocale(" EN "))
	require.Equal(t, "zh", normalizeSkillLocale("fr"))
}

func skillIDs(rows []db.Skill) []string {
	ids := make([]string, len(rows))
	for i := range rows {
		ids[i] = rows[i].ID
	}
	return ids
}
