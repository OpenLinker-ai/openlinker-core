package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

var errRemoteAPIKeyInvalid = errors.New("remote user token invalid")

const internalSecretHeader = "X-OpenLinker-Internal-Token"

type RemoteAPIKeyVerifier struct {
	endpoint string
	secret   string
	client   *http.Client
}

type remoteAPIKeyVerifyRequest struct {
	Token string `json:"token"`
}

type remoteAPIKeyVerifyResponse struct {
	UserID string   `json:"user_id"`
	Scopes []string `json:"scopes"`
}

func NewRemoteAPIKeyVerifier(endpoint string, internalSecret ...string) *RemoteAPIKeyVerifier {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil
	}
	secret := ""
	if len(internalSecret) > 0 {
		secret = strings.TrimSpace(internalSecret[0])
	}
	return &RemoteAPIKeyVerifier{
		endpoint: endpoint,
		secret:   secret,
		client:   &http.Client{Timeout: 5 * time.Second},
	}
}

func (v *RemoteAPIKeyVerifier) Verify(ctx context.Context, plaintextKey string) (uuid.UUID, []string, error) {
	if v == nil || v.endpoint == "" {
		return uuid.Nil, nil, errRemoteAPIKeyInvalid
	}
	payload, err := json.Marshal(remoteAPIKeyVerifyRequest{Token: plaintextKey})
	if err != nil {
		return uuid.Nil, nil, errRemoteAPIKeyInvalid
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.endpoint, bytes.NewReader(payload))
	if err != nil {
		log.Warn().Err(err).Msg("auth.remote_apikey: build request")
		return uuid.Nil, nil, errRemoteAPIKeyInvalid
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if v.secret != "" {
		req.Header.Set(internalSecretHeader, v.secret)
	}

	res, err := v.client.Do(req)
	if err != nil {
		log.Warn().Err(err).Str("endpoint", v.endpoint).Msg("auth.remote_apikey: verify request")
		return uuid.Nil, nil, errRemoteAPIKeyInvalid
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 1024))
		return uuid.Nil, nil, errRemoteAPIKeyInvalid
	}

	var out remoteAPIKeyVerifyResponse
	if err := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&out); err != nil {
		log.Warn().Err(err).Msg("auth.remote_apikey: decode response")
		return uuid.Nil, nil, errRemoteAPIKeyInvalid
	}
	uid, err := uuid.Parse(out.UserID)
	if err != nil {
		log.Warn().Err(err).Str("user_id", out.UserID).Msg("auth.remote_apikey: invalid user_id")
		return uuid.Nil, nil, errRemoteAPIKeyInvalid
	}
	return uid, out.Scopes, nil
}
