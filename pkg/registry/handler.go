package registry

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/kinzhi/openlinker-core/pkg/httpx"
)

type Handler struct {
	svc       registryService
	validator *validator.Validate
}

type registryService interface {
	CreateNode(context.Context, uuid.UUID, *CreateNodeRequest) (*RegistryNodeResponse, error)
	ListNodes(context.Context, uuid.UUID) ([]RegistryNodeResponse, error)
	RevokeNode(context.Context, uuid.UUID, uuid.UUID) (*RegistryNodeResponse, error)
	RotateNodeSecret(context.Context, uuid.UUID, uuid.UUID) (*RegistryNodeResponse, error)
	Heartbeat(context.Context, string) (*HeartbeatResponse, error)
	SyncNodeMetadata(context.Context, string) (*NodeMetadataSyncResponse, error)
	CreateRegistryPeer(context.Context, uuid.UUID, *CreateRegistryPeerRequest) (*RegistryPeerResponse, error)
	ListRegistryPeers(context.Context, uuid.UUID) ([]RegistryPeerResponse, error)
	DeleteRegistryPeer(context.Context, uuid.UUID, uuid.UUID) error
	CreateRegistryFederationInvite(context.Context, uuid.UUID, *CreateRegistryFederationInviteRequest) (*RegistryFederationInviteResponse, error)
	ConsumeRegistryFederationInvite(context.Context, *ConsumeRegistryFederationInviteRequest) (*RegistryFederationExchangeMaterial, error)
	ExchangeRegistryFederationInvite(context.Context, uuid.UUID, *ExchangeRegistryFederationInviteRequest) (*RegistryFederationExchangeResponse, error)
	CreateCloudListing(context.Context, uuid.UUID, *CreateCloudListingRequest) (*CloudListingLinkResponse, error)
	ListCloudListings(context.Context, uuid.UUID) ([]CloudListingLinkResponse, error)
	UpdateCloudListingStatus(context.Context, uuid.UUID, uuid.UUID, *UpdateCloudListingStatusRequest) (*CloudListingLinkResponse, error)
	SyncCloudListingMetadata(context.Context, uuid.UUID, uuid.UUID) (*CloudListingLinkResponse, error)
	CreateProxyRun(context.Context, uuid.UUID, *CreateProxyRunRequest) (*ProxyRunResponse, error)
	CreateRemoteProxyRun(context.Context, uuid.UUID, *CreateRemoteProxyRunRequest) (*RemoteProxyRunResponse, error)
	GetProxyRun(context.Context, uuid.UUID, uuid.UUID) (*ProxyRunResponse, error)
	ListProxyRunArtifacts(context.Context, uuid.UUID, uuid.UUID) ([]ProxyRunArtifactResponse, error)
	DownloadProxyRunArtifact(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) (*ProxyRunArtifactDownload, error)
	ClaimProxyRun(context.Context, string) (*ProxyRunResponse, error)
	CompleteProxyRun(context.Context, string, uuid.UUID, *CompleteProxyRunRequest) (*ProxyRunResponse, error)
}

func NewHandler(svc registryService) *Handler {
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
//	POST /registry-node/metadata-sync node secret metadata sync
//	POST /registry-peers            save a trusted remote Registry endpoint
//	GET  /registry-peers            list trusted remote Registry endpoints
//	DELETE /registry-peers/:id      remove a trusted remote Registry endpoint
//	POST /registry-peers/federation-invitations create a one-time peer exchange token
//	POST /registry-peers/federation-invitations/exchange consume a one-time peer exchange token
//	POST /registry-peers/federation-exchanges exchange a remote invitation into a local peer
//	POST /registry/listings        explicitly expose an Agent through a node
//	GET  /registry/listings        list current user's explicit listing links
//	PATCH /registry/listings/:id/status
//	POST /registry/listings/:id/sync
//	POST /cloud/listings           legacy alias for /registry/listings
//	POST /proxy/runs               create a pending run for a Registry Listing
//	POST /proxy/remote-runs        route to another OpenLinker Registry API
//	GET  /proxy/runs/:id           inspect a requester-owned Proxy Run
//	GET  /proxy/runs/:id/artifacts inspect requester-owned Proxy Run artifacts
//	GET  /proxy/runs/:id/artifacts/:artifactID/download proxy-download artifact file_uri
//	GET  /proxy/runs/claim         node secret claim next pending run
//	POST /proxy/runs/:id/result    node secret complete a claimed run
func (h *Handler) RegisterProtected(api *echo.Group, jwtMiddleware echo.MiddlewareFunc) {
	node := api.Group("/registry-node")
	node.POST("/heartbeat", h.Heartbeat)
	node.POST("/metadata-sync", h.SyncNodeMetadata)

	protectedNode := api.Group("/registry-node", jwtMiddleware)
	protectedNode.POST("/link", h.CreateNode)
	protectedNode.GET("/nodes", h.ListNodes)
	protectedNode.POST("/nodes/:id/revoke", h.RevokeNode)
	protectedNode.POST("/nodes/:id/rotate-secret", h.RotateNodeSecret)

	peers := api.Group("/registry-peers", jwtMiddleware)
	peers.POST("", h.CreateRegistryPeer)
	peers.GET("", h.ListRegistryPeers)
	peers.POST("/federation-invitations", h.CreateRegistryFederationInvite)
	peers.POST("/federation-exchanges", h.ExchangeRegistryFederationInvite)
	peers.DELETE("/:id", h.DeleteRegistryPeer)

	publicPeers := api.Group("/registry-peers")
	publicPeers.POST("/federation-invitations/exchange", h.ConsumeRegistryFederationInvite)

	listings := api.Group("/registry/listings", jwtMiddleware)
	listings.POST("", h.CreateCloudListing)
	listings.GET("", h.ListCloudListings)
	listings.PATCH("/:id/status", h.UpdateCloudListingStatus)
	listings.POST("/:id/sync", h.SyncCloudListingMetadata)

	legacyCloud := api.Group("/cloud", jwtMiddleware)
	legacyCloud.POST("/listings", h.CreateCloudListing)
	legacyCloud.GET("/listings", h.ListCloudListings)
	legacyCloud.PATCH("/listings/:id/status", h.UpdateCloudListingStatus)
	legacyCloud.POST("/listings/:id/sync", h.SyncCloudListingMetadata)

	proxy := api.Group("/proxy")
	proxy.GET("/runs/claim", h.ClaimProxyRun)
	proxy.POST("/runs/:id/result", h.CompleteProxyRun)

	protectedProxy := api.Group("/proxy", jwtMiddleware)
	protectedProxy.POST("/runs", h.CreateProxyRun)
	protectedProxy.POST("/remote-runs", h.CreateRemoteProxyRun)
	protectedProxy.GET("/runs/:id", h.GetProxyRun)
	protectedProxy.GET("/runs/:id/artifacts", h.ListProxyRunArtifacts)
	protectedProxy.GET("/runs/:id/artifacts/:artifactID/download", h.DownloadProxyRunArtifact)
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

func (h *Handler) SyncNodeMetadata(c echo.Context) error {
	secret, err := bearerToken(c.Request().Header.Get(echo.HeaderAuthorization))
	if err != nil {
		return err
	}
	resp, err := h.svc.SyncNodeMetadata(c.Request().Context(), secret)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) CreateRegistryPeer(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req CreateRegistryPeerRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.CreateRegistryPeer(c.Request().Context(), uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}

func (h *Handler) ListRegistryPeers(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	items, err := h.svc.ListRegistryPeers(c.Request().Context(), uid)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, RegistryPeerListResponse{Items: items})
}

func (h *Handler) DeleteRegistryPeer(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	peerID, err := uuid.Parse(strings.TrimSpace(c.Param("id")))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	if err := h.svc.DeleteRegistryPeer(c.Request().Context(), uid, peerID); err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) CreateRegistryFederationInvite(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req CreateRegistryFederationInviteRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if strings.TrimSpace(req.BearerToken) == "" {
		token, tokenErr := bearerToken(c.Request().Header.Get(echo.HeaderAuthorization))
		if tokenErr != nil {
			return httpx.BadRequest("bearer_token 为空时必须使用 Authorization: Bearer 创建 invite")
		}
		req.BearerToken = token
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.CreateRegistryFederationInvite(c.Request().Context(), uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
}

func (h *Handler) ConsumeRegistryFederationInvite(c echo.Context) error {
	var req ConsumeRegistryFederationInviteRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.ConsumeRegistryFederationInvite(c.Request().Context(), &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) ExchangeRegistryFederationInvite(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req ExchangeRegistryFederationInviteRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.ExchangeRegistryFederationInvite(c.Request().Context(), uid, &req)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, resp)
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

func (h *Handler) SyncCloudListingMetadata(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	cloudListingID, err := uuid.Parse(strings.TrimSpace(c.Param("id")))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	resp, err := h.svc.SyncCloudListingMetadata(c.Request().Context(), uid, cloudListingID)
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

func (h *Handler) CreateRemoteProxyRun(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	var req CreateRemoteProxyRunRequest
	if err := c.Bind(&req); err != nil {
		return httpx.BadRequest("请求体格式错误")
	}
	if err := h.validator.Struct(&req); err != nil {
		return httpx.Unprocessable(err.Error())
	}
	resp, err := h.svc.CreateRemoteProxyRun(c.Request().Context(), uid, &req)
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

func (h *Handler) ListProxyRunArtifacts(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	runID, err := uuid.Parse(strings.TrimSpace(c.Param("id")))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	items, err := h.svc.ListProxyRunArtifacts(c.Request().Context(), uid, runID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, ProxyRunArtifactListResponse{
		ProxyRunID: runID.String(),
		Items:      items,
	})
}

func (h *Handler) DownloadProxyRunArtifact(c echo.Context) error {
	uid, err := userIDFromCtx(c)
	if err != nil {
		return err
	}
	runID, err := uuid.Parse(strings.TrimSpace(c.Param("id")))
	if err != nil {
		return httpx.BadRequest("id 不是合法 uuid")
	}
	artifactID, err := uuid.Parse(strings.TrimSpace(c.Param("artifactID")))
	if err != nil {
		return httpx.BadRequest("artifactID 不是合法 uuid")
	}
	download, err := h.svc.DownloadProxyRunArtifact(c.Request().Context(), uid, runID, artifactID)
	if err != nil {
		return err
	}
	c.Response().Header().Set(echo.HeaderContentDisposition, fmt.Sprintf(`attachment; filename="%s"`, safeDownloadFilename(download.FileName)))
	c.Response().Header().Set("X-OpenLinker-Artifact-Id", download.ArtifactID)
	c.Response().Header().Set("X-OpenLinker-Artifact-SHA256", download.SHA256)
	return c.Blob(http.StatusOK, download.ContentType, download.Body)
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

func safeDownloadFilename(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "artifact.bin"
	}
	replacer := strings.NewReplacer(`\`, "_", `"`, "_", "\r", "_", "\n", "_", "/", "_", "\x00", "_")
	return replacer.Replace(name)
}
