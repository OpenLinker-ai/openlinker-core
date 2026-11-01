package task

import (
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

var contextualQueryLabels = map[string]struct{}{
	"验收测试": {}, "验收": {}, "任务": {}, "需求": {}, "背景": {}, "场景": {}, "目标": {},
	"task": {}, "request": {}, "goal": {}, "context": {},
}

// distinctiveHanRunes 单独出现就能表达能力的汉字，如“中译英”“翻成英语”“做张图”“配音”“润一下”。
var distinctiveHanRunes = map[rune]struct{}{'译': {}, '翻': {}, '图': {}, '音': {}, '润': {}, '校': {}}

// ruleParse 在无 LLM 时的兜底：对 query 切词后与每个 skill 的 name/description
// 做关键词匹配，取前 3 个分数 > 0 的 skill_id。
//
// 评分：
//   - token 命中 skill.name        → +3
//   - token 命中 skill.description → +1
//   - 全部得分为 0 时返回空切片（让前端展示空状态）
//
// “验收测试：汇总销售指标”的场景前缀不代表要做什么，优先按正文匹配，正文匹配不到才回退
// 到完整查询。“翻译：销量下降”的前缀本身就是任务，不在 contextualQueryLabels 里，按完整查询匹配。
func ruleParse(query string, skills []db.Skill) []string {
	if body, ok := contextualQueryBody(query); ok {
		if ids := ruleParse(body, skills); len(ids) > 0 {
			return ids
		}
	}
	return matchSkills(query, skills)
}

// contextualQueryBody 拆出“场景前缀：正文”里的正文，其他前缀不拆。
func contextualQueryBody(query string) (string, bool) {
	i := strings.IndexAny(query, ":：")
	if i <= 0 {
		return "", false
	}
	if _, ok := contextualQueryLabels[strings.ToLower(strings.TrimSpace(query[:i]))]; !ok {
		return "", false
	}
	_, size := utf8.DecodeRuneInString(query[i:])
	body := strings.TrimSpace(query[i+size:])
	return body, body != ""
}

func matchSkills(query string, skills []db.Skill) []string {
	tokens := tokenize(query)
	if len(tokens) == 0 || len(skills) == 0 {
		return []string{}
	}

	// 同分时看命中词有多专门：“生成”出现在很多 Skill 里，不该和“海报”打平后靠目录顺序决胜。
	hits := make(map[string]int, len(tokens))
	for _, tk := range tokens {
		for i := range skills {
			if strings.Contains(strings.ToLower(skills[i].Name), tk) || strings.Contains(strings.ToLower(skills[i].Description), tk) {
				hits[tk]++
			}
		}
	}

	type scored struct {
		id     string
		score  int
		rarity int
	}
	out := make([]scored, 0, len(skills))
	for i := range skills {
		s := &skills[i]
		nameLower := strings.ToLower(s.Name)
		descLower := strings.ToLower(s.Description)
		score := 0
		rarity := 0
		for _, tk := range tokens {
			inName, inDesc := strings.Contains(nameLower, tk), strings.Contains(descLower, tk)
			if inName {
				score += 3
			}
			if inDesc {
				score++
			}
			if inName || inDesc {
				rarity += 1000 / hits[tk] // 整数累加与 token 顺序无关，结果稳定
			}
		}
		if score > 0 {
			out = append(out, scored{id: s.ID, score: score, rarity: rarity})
		}
	}
	if len(out) == 0 {
		return []string{}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].score != out[j].score {
			return out[i].score > out[j].score
		}
		return out[i].rarity > out[j].rarity
	})
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
//   - 中文：保留连续片段并切成相邻双字（弱版方案，不引第三方分词器）。
//     "数据分析" 会变成 "数据分析"/"数据"/"据分"/"分析"。单字不参与匹配，
//     否则 "验收测试" 的 "测" 会误中 "预测"、"安全评测"；只保留单字片段和 distinctiveHanRunes。
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
			add(string(curHan))
			for i := 0; i+1 < len(curHan); i++ {
				add(string(curHan[i : i+2]))
			}
			for _, r := range curHan {
				if _, ok := distinctiveHanRunes[r]; ok {
					add(string(r))
				}
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
