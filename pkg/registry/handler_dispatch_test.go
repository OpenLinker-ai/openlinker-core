package registry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
)

func TestRegistryHandlerDispatchesServiceSuccess(t *testing.T) {
	userID := uuid.New()
	nodeID := uuid.New()
	peerID := uuid.New()
	linkID := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	artifactID := uuid.New()
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC).Format(time.RFC3339)

	nodeResp := &RegistryNodeResponse{
		ID:              nodeID.String(),
		NodeName:        "Node",
		NodeType:        "bridge_proxy",
		SecretPrefix:    "rn_live_abcd",
		NodeSecret:      "rn_live_secret",
		Scopes:          []string{"heartbeat", "proxy:pull"},
		HeartbeatStatus: "healthy",
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	heartbeatResp := &HeartbeatResponse{
		NodeID:             nodeID.String(),
		HeartbeatStatus:    "healthy",
		LastHeartbeatAt:    now,
		LinkedListingCount: 2,
		PendingRunCount:    1,
	}
	nodeSyncResp := &NodeMetadataSyncResponse{
		RegistryNodeID:     nodeID.String(),
		SyncedListingCount: 1,
		SyncedAt:           now,
	}
	peerResp := &RegistryPeerResponse{
		ID:             peerID.String(),
		Name:           "Peer",
		APIBaseURL:     "https://peer.example/api/v1",
		CredentialHint: "sha256:abc",
		Status:         "active",
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	inviteResp := &RegistryFederationInviteResponse{
		ID:              uuid.NewString(),
		Name:            "Peer",
		APIBaseURL:      "https://peer.example/api/v1",
		CredentialHint:  "sha256:def",
		Status:          "active",
		TokenPrefix:     "rf_live_abcd",
		FederationToken: "rf_live_secret",
		ExchangeURL:     "https://peer.example/api/v1/registry-peers/federation-invitations/exchange",
		ExpiresAt:       now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	exchangeMaterial := &RegistryFederationExchangeMaterial{
		Name:           "Peer",
		APIBaseURL:     "https://peer.example/api/v1",
		BearerToken:    "token-123",
		CredentialHint: "sha256:def",
		ExpiresAt:      now,
	}
	listingResp := &CloudListingLinkResponse{
		ID:                 linkID.String(),
		RegistryListingID:  uuid.NewString(),
		CloudListingID:     linkID.String(),
		RegistryNodeID:     nodeID.String(),
		NodeName:           "Node",
		AgentID:            agentID.String(),
		AgentSlug:          "agent",
		AgentName:          "Agent",
		AvailabilityStatus: "healthy",
		RoutingMode:        "pull_proxy",
		PayloadPolicy:      "metadata_only",
		SyncStatus:         "linked",
		LastSyncAt:         now,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	proxyResp := &ProxyRunResponse{
		ID:                 runID.String(),
		CloudRunID:         uuid.NewString(),
		CloudListingLinkID: linkID.String(),
		CloudListingID:     linkID.String(),
		RegistryNodeID:     nodeID.String(),
		LocalAgentID:       agentID.String(),
		RequestingUserID:   userID.String(),
		IdempotencyKey:     "idem-12345",
		Status:             "pending",
		PayloadPolicy:      "metadata_only",
		AttemptCount:       1,
		MaxAttempts:        3,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	remoteProxyResp := &RemoteProxyRunResponse{
		RemoteAPIBaseURL: "https://peer.example/api/v1",
		RegistryPeerID:   peerID.String(),
		RouteMode:        "explicit_peer",
		RemoteRun:        *proxyResp,
	}
	artifactResp := ProxyRunArtifactResponse{
		ID:               artifactID.String(),
		ProxyRunID:       runID.String(),
		CloudRunID:       proxyResp.CloudRunID,
		SourceArtifactID: "orders",
		ArtifactType:     "file",
		Title:            "Orders",
		CreatedAt:        now,
	}

	t.Run("node lifecycle", func(t *testing.T) {
		mock := &mockRegistryService{
			nodeResp:     nodeResp,
			listNodesOut: []RegistryNodeResponse{*nodeResp},
			heartbeatOut: heartbeatResp,
			nodeSyncOut:  nodeSyncResp,
		}
		h := NewHandler(mock)

		createCtx, createRec := newRegistryDispatchContext(&registryDispatchRequest{
			method: http.MethodPost,
			target: "/registry-node/link",
			userID: userID.String(),
			body:   `{"node_name":"Node","base_url":"https://node.example","scopes":["heartbeat"]}`,
		})
		if err := h.CreateNode(createCtx); err != nil {
			t.Fatalf("CreateNode error = %v", err)
		}
		if createRec.Code != http.StatusCreated || mock.createNodeOwnerID != userID || mock.createNodeReq.NodeName != "Node" {
			t.Fatalf("create node code=%d owner=%s req=%#v", createRec.Code, mock.createNodeOwnerID, mock.createNodeReq)
		}

		listCtx, listRec := newRegistryDispatchContext(&registryDispatchRequest{
			method: http.MethodGet,
			target: "/registry-node/nodes",
			userID: userID.String(),
		})
		if err := h.ListNodes(listCtx); err != nil {
			t.Fatalf("ListNodes error = %v", err)
		}
		if listRec.Code != http.StatusOK || mock.listNodesOwnerID != userID {
			t.Fatalf("list nodes code=%d owner=%s", listRec.Code, mock.listNodesOwnerID)
		}
		var listBody RegistryNodeListResponse
		decodeRegistryDispatchJSON(t, listRec, &listBody)
		if len(listBody.Items) != 1 || listBody.Items[0].ID != nodeID.String() {
			t.Fatalf("list nodes body = %#v", listBody)
		}

		revokeCtx, revokeRec := newRegistryDispatchContext(&registryDispatchRequest{
			method: http.MethodPost,
			target: "/registry-node/nodes/" + nodeID.String() + "/revoke",
			userID: userID.String(),
			params: map[string]string{"id": nodeID.String()},
		})
		if err := h.RevokeNode(revokeCtx); err != nil {
			t.Fatalf("RevokeNode error = %v", err)
		}
		if revokeRec.Code != http.StatusOK || mock.revokeNodeID != nodeID || mock.revokeOwnerID != userID {
			t.Fatalf("revoke code=%d node=%s owner=%s", revokeRec.Code, mock.revokeNodeID, mock.revokeOwnerID)
		}

		rotateCtx, rotateRec := newRegistryDispatchContext(&registryDispatchRequest{
			method: http.MethodPost,
			target: "/registry-node/nodes/" + nodeID.String() + "/rotate-secret",
			userID: userID.String(),
			params: map[string]string{"id": nodeID.String()},
		})
		if err := h.RotateNodeSecret(rotateCtx); err != nil {
			t.Fatalf("RotateNodeSecret error = %v", err)
		}
		if rotateRec.Code != http.StatusOK || mock.rotateNodeID != nodeID || mock.rotateOwnerID != userID {
			t.Fatalf("rotate code=%d node=%s owner=%s", rotateRec.Code, mock.rotateNodeID, mock.rotateOwnerID)
		}

		heartbeatCtx, heartbeatRec := newRegistryDispatchContext(&registryDispatchRequest{
			method:  http.MethodPost,
			target:  "/registry-node/heartbeat",
			headers: map[string]string{echo.HeaderAuthorization: "Bearer rn_live_secret"},
		})
		if err := h.Heartbeat(heartbeatCtx); err != nil {
			t.Fatalf("Heartbeat error = %v", err)
		}
		if heartbeatRec.Code != http.StatusOK || mock.heartbeatSecret != "rn_live_secret" {
			t.Fatalf("heartbeat code=%d secret=%q", heartbeatRec.Code, mock.heartbeatSecret)
		}

		syncCtx, syncRec := newRegistryDispatchContext(&registryDispatchRequest{
			method:  http.MethodPost,
			target:  "/registry-node/metadata-sync",
			headers: map[string]string{echo.HeaderAuthorization: "Bearer rn_live_secret"},
		})
		if err := h.SyncNodeMetadata(syncCtx); err != nil {
			t.Fatalf("SyncNodeMetadata error = %v", err)
		}
		if syncRec.Code != http.StatusOK || mock.syncNodeSecret != "rn_live_secret" {
			t.Fatalf("sync node code=%d secret=%q", syncRec.Code, mock.syncNodeSecret)
		}
	})

	t.Run("peers and federation", func(t *testing.T) {
		mock := &mockRegistryService{
			peerResp:            peerResp,
			listPeersOut:        []RegistryPeerResponse{*peerResp},
			inviteResp:          inviteResp,
			exchangeMaterialOut: exchangeMaterial,
			exchangeResp: &RegistryFederationExchangeResponse{
				Peer:                 *peerResp,
				ExchangeURL:          inviteResp.ExchangeURL,
				RemoteCredentialHint: "sha256:def",
			},
		}
		h := NewHandler(mock)

		createPeerCtx, createPeerRec := newRegistryDispatchContext(&registryDispatchRequest{
			method: http.MethodPost,
			target: "/registry-peers",
			userID: userID.String(),
			body:   `{"name":"Peer","api_base_url":"https://peer.example/api/v1","bearer_token":"token-123","initial_status":"active"}`,
		})
		if err := h.CreateRegistryPeer(createPeerCtx); err != nil {
			t.Fatalf("CreateRegistryPeer error = %v", err)
		}
		if createPeerRec.Code != http.StatusCreated || mock.createPeerOwnerID != userID || mock.createPeerReq.BearerToken != "token-123" {
			t.Fatalf("create peer code=%d owner=%s req=%#v", createPeerRec.Code, mock.createPeerOwnerID, mock.createPeerReq)
		}

		listPeerCtx, listPeerRec := newRegistryDispatchContext(&registryDispatchRequest{
			method: http.MethodGet,
			target: "/registry-peers",
			userID: userID.String(),
		})
		if err := h.ListRegistryPeers(listPeerCtx); err != nil {
			t.Fatalf("ListRegistryPeers error = %v", err)
		}
		if listPeerRec.Code != http.StatusOK || mock.listPeersOwnerID != userID {
			t.Fatalf("list peers code=%d owner=%s", listPeerRec.Code, mock.listPeersOwnerID)
		}

		deletePeerCtx, deletePeerRec := newRegistryDispatchContext(&registryDispatchRequest{
			method: http.MethodDelete,
			target: "/registry-peers/" + peerID.String(),
			userID: userID.String(),
			params: map[string]string{"id": peerID.String()},
		})
		if err := h.DeleteRegistryPeer(deletePeerCtx); err != nil {
			t.Fatalf("DeleteRegistryPeer error = %v", err)
		}
		if deletePeerRec.Code != http.StatusNoContent || mock.deletePeerID != peerID || mock.deletePeerOwnerID != userID {
			t.Fatalf("delete peer code=%d peer=%s owner=%s", deletePeerRec.Code, mock.deletePeerID, mock.deletePeerOwnerID)
		}

		inviteCtx, inviteRec := newRegistryDispatchContext(&registryDispatchRequest{
			method:  http.MethodPost,
			target:  "/registry-peers/federation-invitations",
			userID:  userID.String(),
			body:    `{"name":"Peer","api_base_url":"https://peer.example/api/v1"}`,
			headers: map[string]string{echo.HeaderAuthorization: "Bearer fallback-token"},
		})
		if err := h.CreateRegistryFederationInvite(inviteCtx); err != nil {
			t.Fatalf("CreateRegistryFederationInvite error = %v", err)
		}
		if inviteRec.Code != http.StatusCreated || mock.createInviteOwnerID != userID || mock.createInviteReq.BearerToken != "fallback-token" {
			t.Fatalf("invite code=%d owner=%s req=%#v", inviteRec.Code, mock.createInviteOwnerID, mock.createInviteReq)
		}

		consumeCtx, consumeRec := newRegistryDispatchContext(&registryDispatchRequest{
			method: http.MethodPost,
			target: "/registry-peers/federation-invitations/exchange",
			body:   `{"federation_token":"rf_live_secret"}`,
		})
		if err := h.ConsumeRegistryFederationInvite(consumeCtx); err != nil {
			t.Fatalf("ConsumeRegistryFederationInvite error = %v", err)
		}
		if consumeRec.Code != http.StatusOK || mock.consumeInviteReq.FederationToken != "rf_live_secret" {
			t.Fatalf("consume code=%d req=%#v", consumeRec.Code, mock.consumeInviteReq)
		}

		exchangeCtx, exchangeRec := newRegistryDispatchContext(&registryDispatchRequest{
			method: http.MethodPost,
			target: "/registry-peers/federation-exchanges",
			userID: userID.String(),
			body:   `{"exchange_url":"https://peer.example/api/v1/registry-peers/federation-invitations/exchange","federation_token":"rf_live_secret","name":"Peer"}`,
		})
		if err := h.ExchangeRegistryFederationInvite(exchangeCtx); err != nil {
			t.Fatalf("ExchangeRegistryFederationInvite error = %v", err)
		}
		if exchangeRec.Code != http.StatusCreated || mock.exchangeInviteOwnerID != userID || mock.exchangeInviteReq.Name != "Peer" {
			t.Fatalf("exchange code=%d owner=%s req=%#v", exchangeRec.Code, mock.exchangeInviteOwnerID, mock.exchangeInviteReq)
		}
	})

	t.Run("listings and proxy runs", func(t *testing.T) {
		mock := &mockRegistryService{
			listingResp:      listingResp,
			listListingsOut:  []CloudListingLinkResponse{*listingResp},
			proxyResp:        proxyResp,
			claimResp:        proxyResp,
			remoteProxyResp:  remoteProxyResp,
			listArtifactsOut: []ProxyRunArtifactResponse{artifactResp},
			artifactDownload: &ProxyRunArtifactDownload{
				ArtifactID:  artifactID.String(),
				FileName:    `bad/"name` + "\n.csv",
				ContentType: "text/csv",
				SHA256:      strings.Repeat("a", 64),
				Body:        []byte("order_id,total\n1,42\n"),
			},
		}
		h := NewHandler(mock)

		createListingCtx, createListingRec := newRegistryDispatchContext(&registryDispatchRequest{
			method: http.MethodPost,
			target: "/registry/listings",
			userID: userID.String(),
			body:   `{"registry_node_id":"` + nodeID.String() + `","agent_id":"` + agentID.String() + `","routing_mode":"pull_proxy","payload_policy":"metadata_only"}`,
		})
		if err := h.CreateCloudListing(createListingCtx); err != nil {
			t.Fatalf("CreateCloudListing error = %v", err)
		}
		if createListingRec.Code != http.StatusCreated || mock.createListingOwnerID != userID || mock.createListingReq.AgentID != agentID.String() {
			t.Fatalf("create listing code=%d owner=%s req=%#v", createListingRec.Code, mock.createListingOwnerID, mock.createListingReq)
		}

		listListingCtx, listListingRec := newRegistryDispatchContext(&registryDispatchRequest{
			method: http.MethodGet,
			target: "/registry/listings",
			userID: userID.String(),
		})
		if err := h.ListCloudListings(listListingCtx); err != nil {
			t.Fatalf("ListCloudListings error = %v", err)
		}
		if listListingRec.Code != http.StatusOK || mock.listListingsOwnerID != userID {
			t.Fatalf("list listings code=%d owner=%s", listListingRec.Code, mock.listListingsOwnerID)
		}

		updateListingCtx, updateListingRec := newRegistryDispatchContext(&registryDispatchRequest{
			method: http.MethodPatch,
			target: "/registry/listings/" + linkID.String() + "/status",
			userID: userID.String(),
			body:   `{"sync_status":"paused"}`,
			params: map[string]string{"id": linkID.String()},
		})
		if err := h.UpdateCloudListingStatus(updateListingCtx); err != nil {
			t.Fatalf("UpdateCloudListingStatus error = %v", err)
		}
		if updateListingRec.Code != http.StatusOK || mock.updateListingID != linkID || mock.updateListingReq.SyncStatus != "paused" {
			t.Fatalf("update listing code=%d id=%s req=%#v", updateListingRec.Code, mock.updateListingID, mock.updateListingReq)
		}

		syncListingCtx, syncListingRec := newRegistryDispatchContext(&registryDispatchRequest{
			method: http.MethodPost,
			target: "/registry/listings/" + linkID.String() + "/sync",
			userID: userID.String(),
			params: map[string]string{"id": linkID.String()},
		})
		if err := h.SyncCloudListingMetadata(syncListingCtx); err != nil {
			t.Fatalf("SyncCloudListingMetadata error = %v", err)
		}
		if syncListingRec.Code != http.StatusOK || mock.syncListingID != linkID || mock.syncListingOwnerID != userID {
			t.Fatalf("sync listing code=%d id=%s owner=%s", syncListingRec.Code, mock.syncListingID, mock.syncListingOwnerID)
		}

		createProxyCtx, createProxyRec := newRegistryDispatchContext(&registryDispatchRequest{
			method: http.MethodPost,
			target: "/proxy/runs",
			userID: userID.String(),
			body:   `{"cloud_listing_id":"` + linkID.String() + `","idempotency_key":"idem-12345","input":{"task":"run"}}`,
		})
		if err := h.CreateProxyRun(createProxyCtx); err != nil {
			t.Fatalf("CreateProxyRun error = %v", err)
		}
		if createProxyRec.Code != http.StatusCreated || mock.createProxyOwnerID != userID || mock.createProxyReq.CloudListingID != linkID.String() {
			t.Fatalf("create proxy code=%d owner=%s req=%#v", createProxyRec.Code, mock.createProxyOwnerID, mock.createProxyReq)
		}

		createRemoteCtx, createRemoteRec := newRegistryDispatchContext(&registryDispatchRequest{
			method: http.MethodPost,
			target: "/proxy/remote-runs",
			userID: userID.String(),
			body:   `{"registry_peer_id":"` + peerID.String() + `","remote_cloud_listing_id":"` + linkID.String() + `","idempotency_key":"idem-remote"}`,
		})
		if err := h.CreateRemoteProxyRun(createRemoteCtx); err != nil {
			t.Fatalf("CreateRemoteProxyRun error = %v", err)
		}
		if createRemoteRec.Code != http.StatusCreated || mock.createRemoteOwnerID != userID || mock.createRemoteReq.RegistryPeerID != peerID.String() {
			t.Fatalf("create remote code=%d owner=%s req=%#v", createRemoteRec.Code, mock.createRemoteOwnerID, mock.createRemoteReq)
		}

		getProxyCtx, getProxyRec := newRegistryDispatchContext(&registryDispatchRequest{
			method: http.MethodGet,
			target: "/proxy/runs/" + runID.String(),
			userID: userID.String(),
			params: map[string]string{"id": runID.String()},
		})
		if err := h.GetProxyRun(getProxyCtx); err != nil {
			t.Fatalf("GetProxyRun error = %v", err)
		}
		if getProxyRec.Code != http.StatusOK || mock.getProxyRunID != runID || mock.getProxyOwnerID != userID {
			t.Fatalf("get proxy code=%d run=%s owner=%s", getProxyRec.Code, mock.getProxyRunID, mock.getProxyOwnerID)
		}

		listArtifactsCtx, listArtifactsRec := newRegistryDispatchContext(&registryDispatchRequest{
			method: http.MethodGet,
			target: "/proxy/runs/" + runID.String() + "/artifacts",
			userID: userID.String(),
			params: map[string]string{"id": runID.String()},
		})
		if err := h.ListProxyRunArtifacts(listArtifactsCtx); err != nil {
			t.Fatalf("ListProxyRunArtifacts error = %v", err)
		}
		if listArtifactsRec.Code != http.StatusOK || mock.listArtifactsRunID != runID || mock.listArtifactsOwnerID != userID {
			t.Fatalf("list artifacts code=%d run=%s owner=%s", listArtifactsRec.Code, mock.listArtifactsRunID, mock.listArtifactsOwnerID)
		}

		downloadCtx, downloadRec := newRegistryDispatchContext(&registryDispatchRequest{
			method: http.MethodGet,
			target: "/proxy/runs/" + runID.String() + "/artifacts/" + artifactID.String() + "/download",
			userID: userID.String(),
			params: map[string]string{"id": runID.String(), "artifactID": artifactID.String()},
		})
		if err := h.DownloadProxyRunArtifact(downloadCtx); err != nil {
			t.Fatalf("DownloadProxyRunArtifact error = %v", err)
		}
		if downloadRec.Code != http.StatusOK || mock.downloadRunID != runID || mock.downloadArtifactID != artifactID {
			t.Fatalf("download code=%d run=%s artifact=%s", downloadRec.Code, mock.downloadRunID, mock.downloadArtifactID)
		}
		if got := downloadRec.Header().Get(echo.HeaderContentDisposition); !strings.Contains(got, `bad__name_.csv`) {
			t.Fatalf("content disposition = %q", got)
		}

		claimCtx, claimRec := newRegistryDispatchContext(&registryDispatchRequest{
			method:  http.MethodGet,
			target:  "/proxy/runs/claim",
			headers: map[string]string{echo.HeaderAuthorization: "Bearer rn_live_secret"},
		})
		if err := h.ClaimProxyRun(claimCtx); err != nil {
			t.Fatalf("ClaimProxyRun error = %v", err)
		}
		if claimRec.Code != http.StatusOK || mock.claimSecret != "rn_live_secret" {
			t.Fatalf("claim code=%d secret=%q", claimRec.Code, mock.claimSecret)
		}

		mock.claimResp = nil
		emptyClaimCtx, emptyClaimRec := newRegistryDispatchContext(&registryDispatchRequest{
			method:  http.MethodGet,
			target:  "/proxy/runs/claim",
			headers: map[string]string{echo.HeaderAuthorization: "Bearer rn_live_secret"},
		})
		if err := h.ClaimProxyRun(emptyClaimCtx); err != nil {
			t.Fatalf("ClaimProxyRun empty error = %v", err)
		}
		if emptyClaimRec.Code != http.StatusNoContent {
			t.Fatalf("empty claim code=%d body=%s", emptyClaimRec.Code, emptyClaimRec.Body.String())
		}

		completeCtx, completeRec := newRegistryDispatchContext(&registryDispatchRequest{
			method:  http.MethodPost,
			target:  "/proxy/runs/" + runID.String() + "/result",
			headers: map[string]string{echo.HeaderAuthorization: "Bearer rn_live_secret"},
			body:    `{"status":"success","output":{"ok":true},"output_summary":"done"}`,
			params:  map[string]string{"id": runID.String()},
		})
		if err := h.CompleteProxyRun(completeCtx); err != nil {
			t.Fatalf("CompleteProxyRun error = %v", err)
		}
		if completeRec.Code != http.StatusOK || mock.completeSecret != "rn_live_secret" || mock.completeRunID != runID || mock.completeReq.Status != "success" {
			t.Fatalf("complete code=%d secret=%q run=%s req=%#v", completeRec.Code, mock.completeSecret, mock.completeRunID, mock.completeReq)
		}
	})
}

func TestRegistryHandlerPropagatesServiceErrors(t *testing.T) {
	userID := uuid.New()
	nodeID := uuid.New()
	peerID := uuid.New()
	linkID := uuid.New()
	agentID := uuid.New()
	runID := uuid.New()
	artifactID := uuid.New()
	serviceErr := httpx.Conflict("service failed")

	tests := []struct {
		name string
		call func(*Handler, echo.Context) error
		ctx  echo.Context
	}{
		{
			name: "create node",
			call: (*Handler).CreateNode,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method: http.MethodPost,
				target: "/registry-node/link",
				userID: userID.String(),
				body:   `{"node_name":"Node"}`,
			}),
		},
		{
			name: "list nodes",
			call: (*Handler).ListNodes,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method: http.MethodGet,
				target: "/registry-node/nodes",
				userID: userID.String(),
			}),
		},
		{
			name: "revoke node",
			call: (*Handler).RevokeNode,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method: http.MethodPost,
				target: "/registry-node/nodes/" + nodeID.String() + "/revoke",
				userID: userID.String(),
				params: map[string]string{"id": nodeID.String()},
			}),
		},
		{
			name: "rotate node",
			call: (*Handler).RotateNodeSecret,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method: http.MethodPost,
				target: "/registry-node/nodes/" + nodeID.String() + "/rotate-secret",
				userID: userID.String(),
				params: map[string]string{"id": nodeID.String()},
			}),
		},
		{
			name: "heartbeat",
			call: (*Handler).Heartbeat,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method:  http.MethodPost,
				target:  "/registry-node/heartbeat",
				headers: map[string]string{echo.HeaderAuthorization: "Bearer rn_live_secret"},
			}),
		},
		{
			name: "sync node",
			call: (*Handler).SyncNodeMetadata,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method:  http.MethodPost,
				target:  "/registry-node/metadata-sync",
				headers: map[string]string{echo.HeaderAuthorization: "Bearer rn_live_secret"},
			}),
		},
		{
			name: "create peer",
			call: (*Handler).CreateRegistryPeer,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method: http.MethodPost,
				target: "/registry-peers",
				userID: userID.String(),
				body:   `{"name":"Peer","api_base_url":"https://peer.example/api/v1","bearer_token":"token-123"}`,
			}),
		},
		{
			name: "list peers",
			call: (*Handler).ListRegistryPeers,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method: http.MethodGet,
				target: "/registry-peers",
				userID: userID.String(),
			}),
		},
		{
			name: "delete peer",
			call: (*Handler).DeleteRegistryPeer,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method: http.MethodDelete,
				target: "/registry-peers/" + peerID.String(),
				userID: userID.String(),
				params: map[string]string{"id": peerID.String()},
			}),
		},
		{
			name: "create invite",
			call: (*Handler).CreateRegistryFederationInvite,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method:  http.MethodPost,
				target:  "/registry-peers/federation-invitations",
				userID:  userID.String(),
				body:    `{"name":"Peer","api_base_url":"https://peer.example/api/v1"}`,
				headers: map[string]string{echo.HeaderAuthorization: "Bearer fallback-token"},
			}),
		},
		{
			name: "consume invite",
			call: (*Handler).ConsumeRegistryFederationInvite,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method: http.MethodPost,
				target: "/registry-peers/federation-invitations/exchange",
				body:   `{"federation_token":"rf_live_secret"}`,
			}),
		},
		{
			name: "exchange invite",
			call: (*Handler).ExchangeRegistryFederationInvite,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method: http.MethodPost,
				target: "/registry-peers/federation-exchanges",
				userID: userID.String(),
				body:   `{"exchange_url":"https://peer.example/api/v1/registry-peers/federation-invitations/exchange","federation_token":"rf_live_secret"}`,
			}),
		},
		{
			name: "create listing",
			call: (*Handler).CreateCloudListing,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method: http.MethodPost,
				target: "/registry/listings",
				userID: userID.String(),
				body:   `{"registry_node_id":"` + nodeID.String() + `","agent_id":"` + agentID.String() + `"}`,
			}),
		},
		{
			name: "list listings",
			call: (*Handler).ListCloudListings,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method: http.MethodGet,
				target: "/registry/listings",
				userID: userID.String(),
			}),
		},
		{
			name: "update listing",
			call: (*Handler).UpdateCloudListingStatus,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method: http.MethodPatch,
				target: "/registry/listings/" + linkID.String() + "/status",
				userID: userID.String(),
				body:   `{"sync_status":"paused"}`,
				params: map[string]string{"id": linkID.String()},
			}),
		},
		{
			name: "sync listing",
			call: (*Handler).SyncCloudListingMetadata,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method: http.MethodPost,
				target: "/registry/listings/" + linkID.String() + "/sync",
				userID: userID.String(),
				params: map[string]string{"id": linkID.String()},
			}),
		},
		{
			name: "create proxy",
			call: (*Handler).CreateProxyRun,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method: http.MethodPost,
				target: "/proxy/runs",
				userID: userID.String(),
				body:   `{"cloud_listing_id":"` + linkID.String() + `"}`,
			}),
		},
		{
			name: "create remote proxy",
			call: (*Handler).CreateRemoteProxyRun,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method: http.MethodPost,
				target: "/proxy/remote-runs",
				userID: userID.String(),
				body:   `{"remote_cloud_listing_id":"` + linkID.String() + `"}`,
			}),
		},
		{
			name: "get proxy",
			call: (*Handler).GetProxyRun,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method: http.MethodGet,
				target: "/proxy/runs/" + runID.String(),
				userID: userID.String(),
				params: map[string]string{"id": runID.String()},
			}),
		},
		{
			name: "list artifacts",
			call: (*Handler).ListProxyRunArtifacts,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method: http.MethodGet,
				target: "/proxy/runs/" + runID.String() + "/artifacts",
				userID: userID.String(),
				params: map[string]string{"id": runID.String()},
			}),
		},
		{
			name: "download artifact",
			call: (*Handler).DownloadProxyRunArtifact,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method: http.MethodGet,
				target: "/proxy/runs/" + runID.String() + "/artifacts/" + artifactID.String() + "/download",
				userID: userID.String(),
				params: map[string]string{"id": runID.String(), "artifactID": artifactID.String()},
			}),
		},
		{
			name: "claim proxy",
			call: (*Handler).ClaimProxyRun,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method:  http.MethodGet,
				target:  "/proxy/runs/claim",
				headers: map[string]string{echo.HeaderAuthorization: "Bearer rn_live_secret"},
			}),
		},
		{
			name: "complete proxy",
			call: (*Handler).CompleteProxyRun,
			ctx: mustRegistryDispatchContext(&registryDispatchRequest{
				method:  http.MethodPost,
				target:  "/proxy/runs/" + runID.String() + "/result",
				headers: map[string]string{echo.HeaderAuthorization: "Bearer rn_live_secret"},
				body:    `{"status":"success"}`,
				params:  map[string]string{"id": runID.String()},
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireRegistryHTTPStatus(t, tt.call(NewHandler(&mockRegistryService{err: serviceErr}), tt.ctx), http.StatusConflict)
		})
	}
}

type mockRegistryService struct {
	err error

	nodeResp          *RegistryNodeResponse
	createNodeOwnerID uuid.UUID
	createNodeReq     *CreateNodeRequest
	listNodesOwnerID  uuid.UUID
	listNodesOut      []RegistryNodeResponse
	revokeOwnerID     uuid.UUID
	revokeNodeID      uuid.UUID
	rotateOwnerID     uuid.UUID
	rotateNodeID      uuid.UUID
	heartbeatSecret   string
	heartbeatOut      *HeartbeatResponse
	syncNodeSecret    string
	nodeSyncOut       *NodeMetadataSyncResponse

	peerResp          *RegistryPeerResponse
	createPeerOwnerID uuid.UUID
	createPeerReq     *CreateRegistryPeerRequest
	listPeersOwnerID  uuid.UUID
	listPeersOut      []RegistryPeerResponse
	deletePeerOwnerID uuid.UUID
	deletePeerID      uuid.UUID

	inviteResp            *RegistryFederationInviteResponse
	createInviteOwnerID   uuid.UUID
	createInviteReq       *CreateRegistryFederationInviteRequest
	consumeInviteReq      *ConsumeRegistryFederationInviteRequest
	exchangeMaterialOut   *RegistryFederationExchangeMaterial
	exchangeInviteOwnerID uuid.UUID
	exchangeInviteReq     *ExchangeRegistryFederationInviteRequest
	exchangeResp          *RegistryFederationExchangeResponse

	listingResp          *CloudListingLinkResponse
	createListingOwnerID uuid.UUID
	createListingReq     *CreateCloudListingRequest
	listListingsOwnerID  uuid.UUID
	listListingsOut      []CloudListingLinkResponse
	updateListingOwnerID uuid.UUID
	updateListingID      uuid.UUID
	updateListingReq     *UpdateCloudListingStatusRequest
	syncListingOwnerID   uuid.UUID
	syncListingID        uuid.UUID

	proxyResp            *ProxyRunResponse
	createProxyOwnerID   uuid.UUID
	createProxyReq       *CreateProxyRunRequest
	createRemoteOwnerID  uuid.UUID
	createRemoteReq      *CreateRemoteProxyRunRequest
	remoteProxyResp      *RemoteProxyRunResponse
	getProxyOwnerID      uuid.UUID
	getProxyRunID        uuid.UUID
	listArtifactsOwnerID uuid.UUID
	listArtifactsRunID   uuid.UUID
	listArtifactsOut     []ProxyRunArtifactResponse
	downloadOwnerID      uuid.UUID
	downloadRunID        uuid.UUID
	downloadArtifactID   uuid.UUID
	artifactDownload     *ProxyRunArtifactDownload
	claimSecret          string
	claimResp            *ProxyRunResponse
	completeSecret       string
	completeRunID        uuid.UUID
	completeReq          *CompleteProxyRunRequest
}

func (m *mockRegistryService) CreateNode(_ context.Context, ownerID uuid.UUID, req *CreateNodeRequest) (*RegistryNodeResponse, error) {
	m.createNodeOwnerID = ownerID
	m.createNodeReq = req
	return m.nodeResp, m.err
}

func (m *mockRegistryService) ListNodes(_ context.Context, ownerID uuid.UUID) ([]RegistryNodeResponse, error) {
	m.listNodesOwnerID = ownerID
	return m.listNodesOut, m.err
}

func (m *mockRegistryService) RevokeNode(_ context.Context, ownerID, nodeID uuid.UUID) (*RegistryNodeResponse, error) {
	m.revokeOwnerID = ownerID
	m.revokeNodeID = nodeID
	return m.nodeResp, m.err
}

func (m *mockRegistryService) RotateNodeSecret(_ context.Context, ownerID, nodeID uuid.UUID) (*RegistryNodeResponse, error) {
	m.rotateOwnerID = ownerID
	m.rotateNodeID = nodeID
	return m.nodeResp, m.err
}

func (m *mockRegistryService) Heartbeat(_ context.Context, plaintextSecret string) (*HeartbeatResponse, error) {
	m.heartbeatSecret = plaintextSecret
	return m.heartbeatOut, m.err
}

func (m *mockRegistryService) SyncNodeMetadata(_ context.Context, plaintextSecret string) (*NodeMetadataSyncResponse, error) {
	m.syncNodeSecret = plaintextSecret
	return m.nodeSyncOut, m.err
}

func (m *mockRegistryService) CreateRegistryPeer(_ context.Context, ownerID uuid.UUID, req *CreateRegistryPeerRequest) (*RegistryPeerResponse, error) {
	m.createPeerOwnerID = ownerID
	m.createPeerReq = req
	return m.peerResp, m.err
}

func (m *mockRegistryService) ListRegistryPeers(_ context.Context, ownerID uuid.UUID) ([]RegistryPeerResponse, error) {
	m.listPeersOwnerID = ownerID
	return m.listPeersOut, m.err
}

func (m *mockRegistryService) DeleteRegistryPeer(_ context.Context, ownerID, peerID uuid.UUID) error {
	m.deletePeerOwnerID = ownerID
	m.deletePeerID = peerID
	return m.err
}

func (m *mockRegistryService) CreateRegistryFederationInvite(_ context.Context, ownerID uuid.UUID, req *CreateRegistryFederationInviteRequest) (*RegistryFederationInviteResponse, error) {
	m.createInviteOwnerID = ownerID
	m.createInviteReq = req
	return m.inviteResp, m.err
}

func (m *mockRegistryService) ConsumeRegistryFederationInvite(_ context.Context, req *ConsumeRegistryFederationInviteRequest) (*RegistryFederationExchangeMaterial, error) {
	m.consumeInviteReq = req
	return m.exchangeMaterialOut, m.err
}

func (m *mockRegistryService) ExchangeRegistryFederationInvite(_ context.Context, ownerID uuid.UUID, req *ExchangeRegistryFederationInviteRequest) (*RegistryFederationExchangeResponse, error) {
	m.exchangeInviteOwnerID = ownerID
	m.exchangeInviteReq = req
	return m.exchangeResp, m.err
}

func (m *mockRegistryService) CreateCloudListing(_ context.Context, ownerID uuid.UUID, req *CreateCloudListingRequest) (*CloudListingLinkResponse, error) {
	m.createListingOwnerID = ownerID
	m.createListingReq = req
	return m.listingResp, m.err
}

func (m *mockRegistryService) ListCloudListings(_ context.Context, ownerID uuid.UUID) ([]CloudListingLinkResponse, error) {
	m.listListingsOwnerID = ownerID
	return m.listListingsOut, m.err
}

func (m *mockRegistryService) UpdateCloudListingStatus(_ context.Context, ownerID, cloudListingID uuid.UUID, req *UpdateCloudListingStatusRequest) (*CloudListingLinkResponse, error) {
	m.updateListingOwnerID = ownerID
	m.updateListingID = cloudListingID
	m.updateListingReq = req
	return m.listingResp, m.err
}

func (m *mockRegistryService) SyncCloudListingMetadata(_ context.Context, ownerID, cloudListingID uuid.UUID) (*CloudListingLinkResponse, error) {
	m.syncListingOwnerID = ownerID
	m.syncListingID = cloudListingID
	return m.listingResp, m.err
}

func (m *mockRegistryService) CreateProxyRun(_ context.Context, requestingUserID uuid.UUID, req *CreateProxyRunRequest) (*ProxyRunResponse, error) {
	m.createProxyOwnerID = requestingUserID
	m.createProxyReq = req
	return m.proxyResp, m.err
}

func (m *mockRegistryService) CreateRemoteProxyRun(_ context.Context, requestingUserID uuid.UUID, req *CreateRemoteProxyRunRequest) (*RemoteProxyRunResponse, error) {
	m.createRemoteOwnerID = requestingUserID
	m.createRemoteReq = req
	return m.remoteProxyResp, m.err
}

func (m *mockRegistryService) GetProxyRun(_ context.Context, requestingUserID, runID uuid.UUID) (*ProxyRunResponse, error) {
	m.getProxyOwnerID = requestingUserID
	m.getProxyRunID = runID
	return m.proxyResp, m.err
}

func (m *mockRegistryService) ListProxyRunArtifacts(_ context.Context, requestingUserID, runID uuid.UUID) ([]ProxyRunArtifactResponse, error) {
	m.listArtifactsOwnerID = requestingUserID
	m.listArtifactsRunID = runID
	return m.listArtifactsOut, m.err
}

func (m *mockRegistryService) DownloadProxyRunArtifact(_ context.Context, requestingUserID, runID, artifactID uuid.UUID) (*ProxyRunArtifactDownload, error) {
	m.downloadOwnerID = requestingUserID
	m.downloadRunID = runID
	m.downloadArtifactID = artifactID
	return m.artifactDownload, m.err
}

func (m *mockRegistryService) ClaimProxyRun(_ context.Context, plaintextSecret string) (*ProxyRunResponse, error) {
	m.claimSecret = plaintextSecret
	return m.claimResp, m.err
}

func (m *mockRegistryService) CompleteProxyRun(_ context.Context, plaintextSecret string, runID uuid.UUID, req *CompleteProxyRunRequest) (*ProxyRunResponse, error) {
	m.completeSecret = plaintextSecret
	m.completeRunID = runID
	m.completeReq = req
	return m.proxyResp, m.err
}

type registryDispatchRequest struct {
	method  string
	target  string
	body    string
	userID  string
	params  map[string]string
	headers map[string]string
}

func newRegistryDispatchContext(spec *registryDispatchRequest) (echo.Context, *httptest.ResponseRecorder) {
	method := spec.method
	if method == "" {
		method = http.MethodGet
	}
	req := httptest.NewRequest(method, spec.target, strings.NewReader(spec.body))
	if spec.body != "" {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}
	for key, value := range spec.headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)
	if spec.userID != "" {
		c.Set(string(httpx.CtxKeyUserID), spec.userID)
	}
	if len(spec.params) > 0 {
		names := make([]string, 0, len(spec.params))
		values := make([]string, 0, len(spec.params))
		for name, value := range spec.params {
			names = append(names, name)
			values = append(values, value)
		}
		c.SetParamNames(names...)
		c.SetParamValues(values...)
	}
	return c, rec
}

func mustRegistryDispatchContext(spec *registryDispatchRequest) echo.Context {
	c, _ := newRegistryDispatchContext(spec)
	return c
}

func decodeRegistryDispatchJSON(t *testing.T, rec *httptest.ResponseRecorder, out any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
		t.Fatalf("decode json: %v body=%s", err, rec.Body.String())
	}
}
