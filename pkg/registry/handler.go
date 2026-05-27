package registry

import (
	"net/http"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

type Handler struct {
	svc       *Service
	validator *validator.Validate
}

func NewHandler(svc *Service) *Handler {
	return &Handler{
		svc:       svc,
		validator: validator.New(validator.WithRequiredStructEnabled()),
	}
}

// RegisterProtected mounts the first Registry / Bridge control-plane endpoints.
//
//	POST /registry-node/link       create a node identity and return node_secret once
//	GET  /registry-node/nodes      list current user's nodes
//	POST /registry-node/nodes/:id/revoke
//	POST /registry-node/nodes/:id/rotate-secret
//	POST /registry-node/heartbeat  node secret heartbeat
//	POST /cloud/listings           explicitly expose an Agent through a node
//	GET  /cloud/listings           list current user's explicit listing links
//	PATCH /cloud/listings/:id/status
//	POST /proxy/runs               create a pending run for a Cloud Listing
//	GET  /proxy/runs/:id           inspect a requester-owned Proxy Run
//	GET  /proxy/runs/claim         node secret claim next pending run
//	POST /proxy/runs/:id/result    node secret complete a claimed run
func (h *Handler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	node := api.Group("/registry-node")
	node.POST("/heartbeat", h.Heartbeat)

	protectedNode := api.Group("/registry-node", jwtMiddleware)
	protectedNode.POST("/link", h.CreateNode)
	protectedNode.GET("/nodes", h.ListNodes)
	protectedNode.POST("/nodes/:id/revoke", h.RevokeNode)
	protectedNode.POST("/nodes/:id/rotate-secret", h.RotateNodeSecret)

	cloud := api.Group("/cloud", jwtMiddleware)
	cloud.POST("/listings", h.CreateCloudListing)
	cloud.GET("/listings", h.ListCloudListings)
	cloud.PATCH("/listings/:id/status", h.UpdateCloudListingStatus)

	proxy := api.Group("/proxy")
	proxy.GET("/runs/claim", h.ClaimProxyRun)
	proxy.POST("/runs/:id/result", h.CompleteProxyRun)

	protectedProxy := api.Group("/proxy", jwtMiddleware)
	protectedProxy.POST("/runs", h.CreateProxyRun)
	protectedProxy.GET("/runs/:id", h.GetProxyRun)
}

func (h *Handler) CreateNode(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req CreateNodeRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.CreateNode(c.Request().Context(), uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}

func (h *Handler) ListNodes(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	items, err := h.svc.ListNodes(c.Request().Context(), uid)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, RegistryNodeListResponse{Items: items})
}

func (h *Handler) RevokeNode(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	nodeID, err := uuid.Parse(strings.TrimSpace(c.Param("id")))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	resp, err := h.svc.RevokeNode(c.Request().Context(), uid, nodeID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) RotateNodeSecret(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	nodeID, err := uuid.Parse(strings.TrimSpace(c.Param("id")))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	resp, err := h.svc.RotateNodeSecret(c.Request().Context(), uid, nodeID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) Heartbeat(c echo.Context) error {
	secret, err := bearerToken(c.Request().Header.Get(echo.HeaderAuthorization))
	if err != nil {
		return err
	}
	resp, err := h.svc.Heartbeat(c.Request().Context(), secret)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) CreateCloudListing(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req CreateCloudListingRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.CreateCloudListing(c.Request().Context(), uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}

func (h *Handler) ListCloudListings(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	items, err := h.svc.ListCloudListings(c.Request().Context(), uid)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, CloudListingListResponse{Items: items})
}

func (h *Handler) UpdateCloudListingStatus(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	cloudListingID, err := uuid.Parse(strings.TrimSpace(c.Param("id")))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	var req UpdateCloudListingStatusRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.UpdateCloudListingStatus(c.Request().Context(), uid, cloudListingID, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) CreateProxyRun(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req CreateProxyRunRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.CreateProxyRun(c.Request().Context(), uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}

func (h *Handler) GetProxyRun(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	runID, err := uuid.Parse(strings.TrimSpace(c.Param("id")))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	resp, err := h.svc.GetProxyRun(c.Request().Context(), uid, runID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) ClaimProxyRun(c echo.Context) error {
	secret, err := bearerToken(c.Request().Header.Get(echo.HeaderAuthorization))
	if err != nil {
		return err
	}
	resp, err := h.svc.ClaimProxyRun(c.Request().Context(), secret)
	if err != nil {
		return err
	}
	if resp == nil {
		return c.NoContent(http.StatusNoContent)
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) CompleteProxyRun(c echo.Context) error {
	secret, err := bearerToken(c.Request().Header.Get(echo.HeaderAuthorization))
	if err != nil {
		return err
	}
	runID, err := uuid.Parse(strings.TrimSpace(c.Param("id")))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	var req CompleteProxyRunRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.CompleteProxyRun(c.Request().Context(), secret, runID, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func userIDFromCtx(c echo.Context) (uuid.UUID, error) {
	idStr := httpx.UserIDFrom(c)
	if idStr == "" {
		return uuid.Nil, httpx.Unauthorized("")
	}
	uid, err := uuid.Parse(idStr)
	if err != nil {
		return uuid.Nil, httpx.Unauthorized("token 无效")
	}
	return uid, nil
}

func bearerToken(header string) (string, error) {
	parts := strings.SplitN(strings.TrimSpace(header), " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || strings.TrimSpace(parts[1]) == "" {
		return "", httpx.Unauthorized("缺少 Registry Node secret")
	}
	return strings.TrimSpace(parts[1]), nil
}
