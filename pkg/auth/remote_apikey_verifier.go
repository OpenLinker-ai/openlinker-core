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

var errRemoteAPIKeyInvalid = errors.New("remote api key invalid")

type RemoteAPIKeyVerifier struct {
	endpoint string
	client   *http.Client
}

type remoteAPIKeyVerifyRequest struct {
	Key string `json:"key"`
}

type remoteAPIKeyVerifyResponse struct {
	UserID string   `json:"user_id"`
	Scopes []string `json:"scopes"`
}

func NewRemoteAPIKeyVerifier(endpoint string) *RemoteAPIKeyVerifier {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil
	}
	return &RemoteAPIKeyVerifier{
		endpoint: endpoint,
		client:   &http.Client{Timeout: 5 * time.Second},
	}
}

func (v *RemoteAPIKeyVerifier) Verify(ctx context.Context, plaintextKey string) (uuid.UUID, []string, error) {
	if v == nil || v.endpoint == "" {
		return uuid.Nil, nil, errRemoteAPIKeyInvalid
	}
	payload, err := json.Marshal(remoteAPIKeyVerifyRequest{Key: plaintextKey})
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
