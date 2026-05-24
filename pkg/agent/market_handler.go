package agent

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"
)

// MarketHandler 市场（用户侧只读）HTTP 入口。
//
// 所有端点公开，无需 JWT。
type MarketHandler struct {
	svc *MarketService
}

// NewMarketHandler 构造 MarketHandler。
func NewMarketHandler(svc *MarketService) *MarketHandler {
	return &MarketHandler{svc: svc}
}

// Register 注册公开路由。
//
//	GET /agents          市场列表（支持 tags / q / page / size）
//	GET /agents/:slug    详情页
//
// 与模块 2 的 GET /agents/check-slug 共存：echo v4 的路由匹配中
// 静态前缀（"check-slug"）优先于参数（":slug"），因此两个端点都能命中。
func (h *MarketHandler) Register(api *echo.Group) {
	api.GET("/agents", h.ListMarket)
	api.GET("/agents/:slug", h.GetBySlug)
}

// ListMarket 市场列表。
//
// query params:
//
//	tags=finance,data    逗号分隔，任意命中即返回（OR）
//	q=审计               关键词，对 name/description ILIKE
//	page=1               1-based
//	size=12              默认 12，max 50
func (h *MarketHandler) ListMarket(c echo.Context) error {
	tags := parseTagsParam(c.QueryParam("tags"))
	keyword := strings.TrimSpace(c.QueryParam("q"))
	page := parseInt32QueryDefault(c.QueryParam("page"), defaultPage)
	size := parseInt32QueryDefault(c.QueryParam("size"), defaultSize)

	// clamp 到 [1, 50]，service 内还会再做一次防御
	if page < 1 {
		page = defaultPage
	}
	if size < 1 {
		size = defaultSize
	}
	if size > maxSize {
		size = maxSize
	}

	resp, err := h.svc.ListMarket(c.Request().Context(), tags, keyword, page, size)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// GetBySlug Agent 详情。
//
// 不存在 / 未公开 → 404 NOT_FOUND。
func (h *MarketHandler) GetBySlug(c echo.Context) error {
	slug := strings.TrimSpace(c.Param("slug"))
	resp, err := h.svc.GetBySlug(c.Request().Context(), slug)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

// parseTagsParam 解析 ?tags=a,b,c 形式的查询参数。
//
// 返回去重后的非空 tag 列表；输入为空 / 全空白时返回 nil（service 内归一化为空切片）。
func parseTagsParam(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	seen := make(map[string]struct{}, len(parts))
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// parseInt32QueryDefault 解析正整数 query 参数，失败 / 空时返回 def。
func parseInt32QueryDefault(raw string, def int32) int32 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return def
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || n <= 0 {
		return def
	}
	return int32(n)
}
