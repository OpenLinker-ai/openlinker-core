package runtime

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

func (h *Handler) CheckRuntimeNodeUpgrade(c echo.Context) error {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	svc, ok := h.svc.(interface {
		CheckRuntimeNodeUpgrade(context.Context, uuid.UUID) (*RuntimeNodeUpgradeStatus, error)
	})
	if !ok {
		return httpx.ServiceUnavailable("Runtime Node 管理能力不可用")
	}
	r, err := svc.CheckRuntimeNodeUpgrade(c.Request().Context(), id)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, r)
}

func (h *Handler) UpgradeRuntimeNode(c echo.Context) error {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	actor, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	// Exactly six distinct fields. Reject unknown, duplicate, omitted and trailing
	// values before they can be mistaken for a different idempotent intent.
	d := json.NewDecoder(http.MaxBytesReader(c.Response(), c.Request().Body, 4096))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return httpx.BadRequest("请求体格式错误")
	}
	fields := map[string]json.RawMessage{}
	allowed := map[string]bool{"operation_id": true, "kind": true, "expected_version": true, "expected_revision": true, "target_version": true, "deadline_at": true}
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return httpx.BadRequest("请求体格式错误")
		}
		name, ok := key.(string)
		if !ok || !allowed[name] || fields[name] != nil {
			return httpx.BadRequest("请求字段无效或重复")
		}
		var value json.RawMessage
		if err = d.Decode(&value); err != nil || string(value) == "null" {
			return httpx.BadRequest("请求字段无效")
		}
		fields[name] = value
	}
	if _, err = d.Token(); err != nil || len(fields) != 6 {
		return httpx.BadRequest("升级请求需要完整六个字段")
	}
	var trailing any
	if err = d.Decode(&trailing); err != io.EOF {
		return httpx.BadRequest("请求体包含多余内容")
	}
	raw, _ := json.Marshal(fields)
	var req RuntimeNodeUpgradeRequest
	if err = json.Unmarshal(raw, &req); err != nil {
		return httpx.BadRequest("升级请求字段类型错误")
	}
	svc, ok := h.svc.(interface {
		UpgradeRuntimeNode(context.Context, uuid.UUID, uuid.UUID, RuntimeNodeUpgradeRequest) (*RuntimeNodeUpgradeReceipt, error)
	})
	if !ok {
		return httpx.ServiceUnavailable("Runtime Node 管理能力不可用")
	}
	r, err := svc.UpgradeRuntimeNode(c.Request().Context(), id, actor, req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, r)
}
