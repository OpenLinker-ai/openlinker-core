package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

func TestNewRemoteAPIKeyVerifier(t *testing.T) {
	if NewRemoteAPIKeyVerifier(" \t\n ") != nil {
		t.Fatalf("blank endpoint should return nil verifier")
	}
	if NewRemoteAPIKeyVerifier(" https://cloud.example/internal/user-tokens/verify ") == nil {
		t.Fatalf("non-blank endpoint should return verifier")
	}
}

func TestRemoteAPIKeyVerifierVerifySuccess(t *testing.T) {
	userID := uuid.New()
	var seenToken string
	var seenContentType string
	var seenAccept string
	var seenSecret string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		seenContentType = r.Header.Get("Content-Type")
		seenAccept = r.Header.Get("Accept")
		seenSecret = r.Header.Get(internalSecretHeader)
		var body remoteAPIKeyVerifyRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		seenToken = body.Token
		_ = json.NewEncoder(w).Encode(remoteAPIKeyVerifyResponse{
			UserID: userID.String(),
			Scopes: []string{
				"agents:run",
				"runs:read",
			},
		})
	}))
	defer server.Close()

	verifier := NewRemoteAPIKeyVerifier(" "+server.URL+" ", " shared-secret ")
	gotUserID, scopes, err := verifier.Verify(context.Background(), "ol_user_test")
	if err != nil {
		t.Fatalf("Verify error = %v", err)
	}
	if gotUserID != userID || len(scopes) != 2 || scopes[0] != "agents:run" {
		t.Fatalf("Verify = %s %#v", gotUserID, scopes)
	}
	if seenToken != "ol_user_test" || seenContentType != "application/json" || seenAccept != "application/json" || seenSecret != "shared-secret" {
		t.Fatalf("request token=%q content-type=%q accept=%q secret=%q", seenToken, seenContentType, seenAccept, seenSecret)
	}
}

func TestRemoteAPIKeyVerifierVerifyFailures(t *testing.T) {
	var nilVerifier *RemoteAPIKeyVerifier
	if _, _, err := nilVerifier.Verify(context.Background(), "key"); !errors.Is(err, errRemoteAPIKeyInvalid) {
		t.Fatalf("nil verifier error = %v", err)
	}
	if _, _, err := (&RemoteAPIKeyVerifier{}).Verify(context.Background(), "key"); !errors.Is(err, errRemoteAPIKeyInvalid) {
		t.Fatalf("empty endpoint error = %v", err)
	}
	if _, _, err := NewRemoteAPIKeyVerifier("://bad-url").Verify(context.Background(), "key"); !errors.Is(err, errRemoteAPIKeyInvalid) {
		t.Fatalf("bad endpoint error = %v", err)
	}

	tests := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{
			name: "non ok status",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "nope", http.StatusUnauthorized)
			},
		},
		{
			name: "bad json",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte("{"))
			},
		},
		{
			name: "bad user id",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(remoteAPIKeyVerifyResponse{UserID: "not-a-uuid"})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			defer server.Close()

			_, _, err := NewRemoteAPIKeyVerifier(server.URL).Verify(context.Background(), "key")
			if !errors.Is(err, errRemoteAPIKeyInvalid) {
				t.Fatalf("Verify error = %v", err)
			}
		})
	}
}
