package task

import (
	"sort"
	"strings"
	"unicode"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

// ruleParse 在无 LLM 时的兜底：对 query 切词后与每个 skill 的 name/description
// 做关键词匹配，取前 3 个分数 > 0 的 skill_id。
//
// 评分：
//   - token 命中 skill.name        → +3
//   - token 命中 skill.description → +1
//   - 全部得分为 0 时返回空切片（让前端展示空状态）
func ruleParse(query string, skills []db.Skill) []string {
	tokens := tokenize(query)
	if len(tokens) == 0 || len(skills) == 0 {
		return []string{}
	}

	type scored struct {
		id    string
		score int
	}
	out := make([]scored, 0, len(skills))
	for i := range skills {
		s := &skills[i]
		nameLower := strings.ToLower(s.Name)
		descLower := strings.ToLower(s.Description)
		score := 0
		for _, tk := range tokens {
			if strings.Contains(nameLower, tk) {
				score += 3
			}
			if strings.Contains(descLower, tk) {
				score++
			}
		}
		if score > 0 {
			out = append(out, scored{id: s.ID, score: score})
		}
	}
	if len(out) == 0 {
		return []string{}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].score > out[j].score })
	if len(out) > 3 {
		out = out[:3]
	}
	ids := make([]string, len(out))
	for i := range out {
		ids[i] = out[i].id
	}
	return ids
}

// tokenize 对 query 切词。
//
// 中英文混排：
//   - 英文 / 数字：按非字母数字切分，转小写
//   - 中文：每个 CJK 字符当成独立 token（弱版方案，不引第三方分词器）
//     这样 "数据分析" 会变成 "数"/"据"/"分"/"析" 四个字，与 skill.name "数据分析"
//     的 toLower 子串匹配仍能命中（因为 "数" 是 "数据分析" 的子串）。
//
// 还会保留连续中文片段（>=2 字）作为整体 token，方便短语匹配。
func tokenize(query string) []string {
	q := strings.ToLower(query)
	seen := make(map[string]struct{}, 8)
	add := func(t string) {
		if t == "" {
			return
		}
		if _, ok := seen[t]; ok {
			return
		}
		seen[t] = struct{}{}
	}

	var (
		curAscii []rune
		curHan   []rune
	)
	flushAscii := func() {
		if len(curAscii) > 0 {
			// 英文冠词、代词及编号中的单字符很容易误命中 API、schema 等
			// Skill 文案。只过滤单个 ASCII 字母或数字；中文由 curHan 处理，
			// 其他非 ASCII 单字符仍保持原有匹配语义。
			if len(curAscii) > 1 || curAscii[0] > unicode.MaxASCII {
				add(string(curAscii))
			}
			curAscii = curAscii[:0]
		}
	}
	flushHan := func() {
		if len(curHan) > 0 {
			// 整段汉字也加进来
			if len(curHan) > 1 {
				add(string(curHan))
			}
			// 单字也各自加（覆盖 skill.name 含单字的情况）
			for _, r := range curHan {
				add(string(r))
			}
			curHan = curHan[:0]
		}
	}

	for _, r := range q {
		switch {
		case unicode.Is(unicode.Han, r):
			flushAscii()
			curHan = append(curHan, r)
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			flushHan()
			curAscii = append(curAscii, r)
		default:
			flushAscii()
			flushHan()
		}
	}
	flushAscii()
	flushHan()

	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	return out
}
