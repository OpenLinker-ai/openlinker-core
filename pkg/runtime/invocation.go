package runtime

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	runtimeInvocationCapabilityVersion = "openlinker.runtime-invocation.v2"
	runtimeInvocationTokenPrefix       = "ol_inv_v2"
	runtimeInvocationContextPrefix     = "ol_ctx_v2"
	runtimeInvocationTokenDomain       = "openlinker/runtime-v2/invocation-token"
	runtimeInvocationContextDomain     = "openlinker/runtime-v2/node-envelope"
	runtimeInvocationProofDomain       = "openlinker/runtime-v2/invocation-proof"
	runtimeInvocationAudience          = "openlinker.runtime.v2/call-agent"
	runtimeDelegationAudience          = "openlinker.runtime.v2/delegation"
	RuntimeDelegatedRunReadFeature     = "delegated_run_read.v1"
	runtimeInvocationMinimumKeyBytes   = 32
)

var (
	ErrInvalidRuntimeInvocation = errors.New("invalid runtime invocation capability")
	ErrExpiredRuntimeInvocation = errors.New("runtime invocation capability expired")
)

// RuntimeInvocationCapability is the immutable authority delegated to one
// accepted runtime offer. It intentionally contains only an input digest, not
// user input or a long-lived Agent token.
type RuntimeInvocationCapability struct {
	// Audience is immutable Session-negotiated authority. Empty preserves the
	// original create-only capability and its canonical bytes.
	Audience         string
	RunID            uuid.UUID
	AttemptID        uuid.UUID
	LeaseID          uuid.UUID
	FencingToken     int64
	AgentID          uuid.UUID
	CredentialID     uuid.UUID
	NodeID           uuid.UUID
	WorkerID         string
	RuntimeSessionID uuid.UUID
	InputSHA256      [sha256.Size]byte
	IssuedAt         time.Time
	ExpiresAt        time.Time
}

// RuntimeInvocationProofRequest binds a delegated call to its exact HTTP
// request. Body is hashed before signing, so no request payload is embedded in
// a header.
type RuntimeInvocationProofRequest struct {
	Method         string
	Path           string
	IdempotencyKey string
	Context        string
	Body           []byte
}

// RuntimeInvocationSigner issues and verifies stateless short-lived runtime
// capabilities. The configured secret is domain-derived before use so these
// signatures cannot be confused with user JWTs or another HMAC protocol.
type RuntimeInvocationSigner struct {
	activeKeyID string
	tokenKeys   map[string][sha256.Size]byte
	contextKeys map[string][sha256.Size]byte
}

func NewRuntimeInvocationSigner(secret string) (*RuntimeInvocationSigner, error) {
	return NewRuntimeInvocationSignerKeyring("current", map[string]string{"current": secret})
}

// NewRuntimeInvocationSignerWithPrevious is the configuration-oriented
// keyring constructor. Both predecessor values are optional together; partial
// or same-ID rotation configuration fails closed.
func NewRuntimeInvocationSignerWithPrevious(
	activeKeyID, activeSecret, previousKeyID, previousSecret string,
) (*RuntimeInvocationSigner, error) {
	secrets := map[string]string{activeKeyID: activeSecret}
	switch {
	case previousKeyID == "" && previousSecret == "":
	case previousKeyID == "" || previousSecret == "" || previousKeyID == activeKeyID:
		return nil, fmt.Errorf("%w: invalid predecessor signing key", ErrInvalidRuntimeInvocation)
	default:
		secrets[previousKeyID] = previousSecret
	}
	return NewRuntimeInvocationSignerKeyring(activeKeyID, secrets)
}

// NewRuntimeInvocationSignerKeyring creates a signer that issues with one
// active key while continuing to verify explicitly configured predecessor
// keys. Outstanding Attempt capabilities therefore survive a deliberate key
// rotation only for as long as operators retain the old key in this bounded
// ring.
func NewRuntimeInvocationSignerKeyring(activeKeyID string, secrets map[string]string) (*RuntimeInvocationSigner, error) {
	if !validRuntimeInvocationKeyID(activeKeyID) || len(secrets) == 0 || len(secrets) > 8 {
		return nil, fmt.Errorf("%w: invalid signing keyring", ErrInvalidRuntimeInvocation)
	}
	tokenKeys := make(map[string][sha256.Size]byte, len(secrets))
	contextKeys := make(map[string][sha256.Size]byte, len(secrets))
	for keyID, secret := range secrets {
		if !validRuntimeInvocationKeyID(keyID) || strings.TrimSpace(secret) != secret || len(secret) < runtimeInvocationMinimumKeyBytes {
			return nil, fmt.Errorf("%w: signing secret %q must contain at least %d non-whitespace bytes", ErrInvalidRuntimeInvocation, keyID, runtimeInvocationMinimumKeyBytes)
		}
		tokenKeys[keyID] = deriveRuntimeInvocationKey(secret, runtimeInvocationTokenDomain)
		contextKeys[keyID] = deriveRuntimeInvocationKey(secret, runtimeInvocationContextDomain)
	}
	if _, ok := tokenKeys[activeKeyID]; !ok {
		return nil, fmt.Errorf("%w: active signing key is absent", ErrInvalidRuntimeInvocation)
	}
	return &RuntimeInvocationSigner{
		activeKeyID: activeKeyID,
		tokenKeys:   tokenKeys,
		contextKeys: contextKeys,
	}, nil
}

// Issue returns a signed Node context and bearer capability. For the same
// immutable Attempt evidence it is deterministic, so a repeated claim cannot
// change the assignment journal digest.
func (s *RuntimeInvocationSigner) Issue(capability RuntimeInvocationCapability) (nodeEnvelope, invocationToken string, err error) {
	if s == nil {
		return "", "", fmt.Errorf("%w: signer is nil", ErrInvalidRuntimeInvocation)
	}
	payload, err := canonicalRuntimeInvocationCapability(capability)
	if err != nil {
		return "", "", err
	}
	contextKey, contextOK := s.contextKeys[s.activeKeyID]
	tokenKey, tokenOK := s.tokenKeys[s.activeKeyID]
	if !contextOK || !tokenOK {
		return "", "", ErrInvalidRuntimeInvocation
	}
	return encodeSignedRuntimeCapability(runtimeInvocationContextPrefix, s.activeKeyID, payload, contextKey[:]),
		encodeSignedRuntimeCapability(runtimeInvocationTokenPrefix, s.activeKeyID, payload, tokenKey[:]), nil
}

func (s *RuntimeInvocationSigner) VerifyNodeEnvelope(envelope string, databaseNow time.Time) (RuntimeInvocationCapability, error) {
	if s == nil {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	return verifySignedRuntimeCapability(envelope, runtimeInvocationContextPrefix, s.contextKeys, databaseNow)
}

func (s *RuntimeInvocationSigner) VerifyInvocationToken(token string, databaseNow time.Time) (RuntimeInvocationCapability, error) {
	if s == nil {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	return verifySignedRuntimeCapability(token, runtimeInvocationTokenPrefix, s.tokenKeys, databaseNow)
}

// BuildRuntimeInvocationProof is used by a Node holding the short-lived
// invocation token. The token itself is never put into the proof payload.
func BuildRuntimeInvocationProof(token string, request RuntimeInvocationProofRequest) (string, error) {
	canonical, err := canonicalRuntimeInvocationProof(request)
	if err != nil || !strings.HasPrefix(token, runtimeInvocationTokenPrefix+".") {
		return "", ErrInvalidRuntimeInvocation
	}
	key := sha256.Sum256([]byte(runtimeInvocationProofDomain + "\x00" + token))
	return base64.RawURLEncoding.EncodeToString(runtimeInvocationMAC(key[:], canonical)), nil
}

func VerifyRuntimeInvocationProof(token, proof string, request RuntimeInvocationProofRequest) error {
	want, err := BuildRuntimeInvocationProof(token, request)
	if err != nil {
		return err
	}
	got, err := base64.RawURLEncoding.DecodeString(proof)
	if err != nil {
		return ErrInvalidRuntimeInvocation
	}
	wantBytes, _ := base64.RawURLEncoding.DecodeString(want)
	if len(got) != len(wantBytes) || subtle.ConstantTimeCompare(got, wantBytes) != 1 {
		return ErrInvalidRuntimeInvocation
	}
	return nil
}

func deriveRuntimeInvocationKey(secret, domain string) [sha256.Size]byte {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(domain))
	var key [sha256.Size]byte
	copy(key[:], mac.Sum(nil))
	return key
}

func validRuntimeInvocationKeyID(value string) bool {
	if len(value) < 1 || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') &&
			(char < '0' || char > '9') && char != '-' && char != '_' {
			return false
		}
	}
	return true
}

func encodeSignedRuntimeCapability(prefix, keyID string, payload, key []byte) string {
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	signed := prefix + "." + keyID + "." + encoded
	signature := runtimeInvocationMAC(key, []byte(signed))
	return signed + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func verifySignedRuntimeCapability(raw, prefix string, keys map[string][sha256.Size]byte, databaseNow time.Time) (RuntimeInvocationCapability, error) {
	parts := strings.Split(raw, ".")
	if len(parts) != 4 || parts[0] != prefix || !validRuntimeInvocationKeyID(parts[1]) {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	key, ok := keys[parts[1]]
	if !ok {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(payload) == 0 || len(payload) > 4096 {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[3])
	if err != nil || len(signature) != sha256.Size {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	want := runtimeInvocationMAC(key[:], []byte(parts[0]+"."+parts[1]+"."+parts[2]))
	if subtle.ConstantTimeCompare(signature, want) != 1 {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	capability, err := decodeRuntimeInvocationCapability(payload)
	if err != nil {
		return RuntimeInvocationCapability{}, err
	}
	if databaseNow.Before(capability.IssuedAt) || !databaseNow.Before(capability.ExpiresAt) {
		return RuntimeInvocationCapability{}, ErrExpiredRuntimeInvocation
	}
	return capability, nil
}

func runtimeInvocationMAC(key, value []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(value)
	return mac.Sum(nil)
}

func canonicalRuntimeInvocationCapability(capability RuntimeInvocationCapability) ([]byte, error) {
	if err := validateRuntimeInvocationCapability(capability); err != nil {
		return nil, err
	}
	return CanonicalizeRFC8785(map[string]any{
		"agent_id":           capability.AgentID.String(),
		"audience":           runtimeCapabilityAudience(capability),
		"attempt_id":         capability.AttemptID.String(),
		"credential_id":      capability.CredentialID.String(),
		"expires_at":         capability.ExpiresAt.UTC().Format(time.RFC3339Nano),
		"fencing_token":      capability.FencingToken,
		"input_sha256":       hex.EncodeToString(capability.InputSHA256[:]),
		"issued_at":          capability.IssuedAt.UTC().Format(time.RFC3339Nano),
		"lease_id":           capability.LeaseID.String(),
		"node_id":            capability.NodeID.String(),
		"run_id":             capability.RunID.String(),
		"runtime_session_id": capability.RuntimeSessionID.String(),
		"version":            runtimeInvocationCapabilityVersion,
		"worker_id":          capability.WorkerID,
	})
}

func validateRuntimeInvocationCapability(capability RuntimeInvocationCapability) error {
	if audience := runtimeCapabilityAudience(capability); audience != runtimeInvocationAudience && audience != runtimeDelegationAudience {
		return ErrInvalidRuntimeInvocation
	}
	if capability.RunID == uuid.Nil || capability.AttemptID == uuid.Nil || capability.LeaseID == uuid.Nil ||
		capability.AgentID == uuid.Nil || capability.CredentialID == uuid.Nil || capability.NodeID == uuid.Nil ||
		capability.RuntimeSessionID == uuid.Nil || capability.FencingToken < 1 ||
		strings.TrimSpace(capability.WorkerID) == "" || utf8.RuneCountInString(capability.WorkerID) > 200 ||
		capability.IssuedAt.IsZero() || capability.ExpiresAt.IsZero() ||
		!capability.IssuedAt.Before(capability.ExpiresAt) {
		return ErrInvalidRuntimeInvocation
	}
	return nil
}

func decodeRuntimeInvocationCapability(payload []byte) (RuntimeInvocationCapability, error) {
	var wire struct {
		Version          string `json:"version"`
		Audience         string `json:"audience"`
		RunID            string `json:"run_id"`
		AttemptID        string `json:"attempt_id"`
		LeaseID          string `json:"lease_id"`
		FencingToken     int64  `json:"fencing_token"`
		AgentID          string `json:"agent_id"`
		CredentialID     string `json:"credential_id"`
		NodeID           string `json:"node_id"`
		WorkerID         string `json:"worker_id"`
		RuntimeSessionID string `json:"runtime_session_id"`
		InputSHA256      string `json:"input_sha256"`
		IssuedAt         string `json:"issued_at"`
		ExpiresAt        string `json:"expires_at"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	if err := rejectTrailingRuntimeInvocationJSON(decoder); err != nil ||
		wire.Version != runtimeInvocationCapabilityVersion || (wire.Audience != runtimeInvocationAudience && wire.Audience != runtimeDelegationAudience) {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	capability := RuntimeInvocationCapability{FencingToken: wire.FencingToken, WorkerID: wire.WorkerID}
	if wire.Audience != runtimeInvocationAudience {
		capability.Audience = wire.Audience
	}
	var err error
	if capability.RunID, err = uuid.Parse(wire.RunID); err != nil {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	if capability.AttemptID, err = uuid.Parse(wire.AttemptID); err != nil {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	if capability.LeaseID, err = uuid.Parse(wire.LeaseID); err != nil {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	if capability.AgentID, err = uuid.Parse(wire.AgentID); err != nil {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	if capability.CredentialID, err = uuid.Parse(wire.CredentialID); err != nil {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	if capability.NodeID, err = uuid.Parse(wire.NodeID); err != nil {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	if capability.RuntimeSessionID, err = uuid.Parse(wire.RuntimeSessionID); err != nil {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	inputDigest, err := hex.DecodeString(wire.InputSHA256)
	if err != nil || len(inputDigest) != sha256.Size {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	copy(capability.InputSHA256[:], inputDigest)
	if capability.IssuedAt, err = time.Parse(time.RFC3339Nano, wire.IssuedAt); err != nil {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	if capability.ExpiresAt, err = time.Parse(time.RFC3339Nano, wire.ExpiresAt); err != nil {
		return RuntimeInvocationCapability{}, ErrInvalidRuntimeInvocation
	}
	if err := validateRuntimeInvocationCapability(capability); err != nil {
		return RuntimeInvocationCapability{}, err
	}
	return capability, nil
}

func rejectTrailingRuntimeInvocationJSON(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return ErrInvalidRuntimeInvocation
}

func canonicalRuntimeInvocationProof(request RuntimeInvocationProofRequest) ([]byte, error) {
	method := strings.ToUpper(strings.TrimSpace(request.Method))
	path := strings.TrimSpace(request.Path)
	idempotencyKey := request.IdempotencyKey
	contextValue := strings.TrimSpace(request.Context)
	if method == "" || path == "" || contextValue == "" || !strings.HasPrefix(path, "/") {
		return nil, ErrInvalidRuntimeInvocation
	}
	if _, err := HashIdempotencyKey(idempotencyKey); err != nil {
		return nil, ErrInvalidRuntimeInvocation
	}
	bodyDigest := sha256.Sum256(request.Body)
	return CanonicalizeRFC8785(map[string]any{
		"body_sha256":     hex.EncodeToString(bodyDigest[:]),
		"context":         contextValue,
		"idempotency_key": idempotencyKey,
		"method":          method,
		"path":            path,
		"version":         runtimeInvocationProofDomain,
	})
}

// RuntimeInvocationProofRequestFromHTTP preserves the exact escaped path and
// body bytes used for proof verification. The caller must restore Body if a
// downstream JSON decoder still needs it.
func RuntimeInvocationProofRequestFromHTTP(r *http.Request, body []byte) RuntimeInvocationProofRequest {
	path := r.URL.EscapedPath()
	if path == "" {
		path = r.URL.Path
	}
	return RuntimeInvocationProofRequest{
		Method:         r.Method,
		Path:           path,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Context:        r.Header.Get("OpenLinker-Invocation-Context"),
		Body:           append([]byte(nil), body...),
	}
}
