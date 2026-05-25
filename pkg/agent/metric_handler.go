package agent

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// MetricHandler 公开 GET 单 Agent 指标快照。
type MetricHandler struct {
	svc *MetricService
}

func NewMetricHandler(svc *MetricService) *MetricHandler {
	return &MetricHandler{svc: svc}
}

// Register 公开端点（无 JWT；snapshot 不含敏感字段）。
//
//	GET /api/v1/agents/:id/metrics
func (h *MetricHandler) Register(api *echo.Group) {
	api.GET("/agents/:id/metrics", h.GetMetrics)
}

func (h *MetricHandler) GetMetrics(c echo.Context) error {
	id, err := pathID(c)
	if err != nil {
		return err
	}
	resp, err := h.svc.GetSnapshots(c.Request().Context(), id)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}
