package runtime

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

const (
	maxRuntimeSessionWorkerIDRunes  = 200
	maxRuntimeSessionVersionRunes   = 100
	maxRuntimeSessionFeatureRunes   = 100
	maxRuntimeSessionCapacity       = 1024
	maxRuntimeDisconnectReasonRunes = 200
)

// RuntimeDeviceIdentity is the immutable Node identity established from a
// mutually-authenticated TLS peer certificate. None of these fields may be
// sourced from an HTTP header or a runtime hello body.
type RuntimeDeviceIdentity struct {
	NodeID                       uuid.UUID `json:"node_id"`
	CertificateSerial            string    `json:"certificate_serial"`
	CertificateFingerprintSHA256 string    `json:"certificate_fingerprint_sha256"`
	PublicKeyThumbprintSHA256    string    `json:"public_key_thumbprint_sha256"`
}

// RuntimePresentedCertificate is the canonical certificate identity passed
// to the durable credential verifier. The verifier must use database time for
// credential expiry and must fail closed for revoked credentials.
type RuntimePresentedCertificate struct {
	Serial                    string
	FingerprintSHA256         string
	PublicKeyThumbprintSHA256 string
	NotBefore                 time.Time
	NotAfter                  time.Time
}

// RuntimeNodeCredentialVerifier resolves an enrolled, active Runtime Node by
// exact certificate identity. The current inventory pins serial plus SPKI;
// the authenticator also keeps the full leaf fingerprint bound across the
// certificate-verifier boundary so callers cannot substitute a peer.
type RuntimeNodeCredentialVerifier interface {
	VerifyRuntimeNodeCredential(context.Context, RuntimePresentedCertificate) (RuntimeDeviceIdentity, error)
}

// DBRuntimeNodeCredentialVerifier resolves the enrolled runtime_nodes record
// by certificate serial plus SPKI SHA-256 thumbprint. Certificate validity is
// compared with PostgreSQL clock_timestamp(), keeping credential decisions on
// the same clock as Session and lease state.
type DBRuntimeNodeCredentialVerifier struct {
	queries runtimeNodeCredentialQueries
	clock   runtimeDatabaseClock
}

func NewDBRuntimeNodeCredentialVerifier(pool *pgxpool.Pool) *DBRuntimeNodeCredentialVerifier {
	if pool == nil {
		return &DBRuntimeNodeCredentialVerifier{}
	}
	return &DBRuntimeNodeCredentialVerifier{queries: db.New(pool), clock: pool}
}

func newDBRuntimeNodeCredentialVerifier(
	queries runtimeNodeCredentialQueries,
	clock runtimeDatabaseClock,
) *DBRuntimeNodeCredentialVerifier {
	return &DBRuntimeNodeCredentialVerifier{queries: queries, clock: clock}
}

func (v *DBRuntimeNodeCredentialVerifier) VerifyRuntimeNodeCredential(
	ctx context.Context,
	presented RuntimePresentedCertificate,
) (RuntimeDeviceIdentity, error) {
	if v == nil || v.queries == nil || v.clock == nil ||
		strings.TrimSpace(presented.Serial) == "" ||
		!validSHA256Hex(presented.FingerprintSHA256) ||
		!validSHA256Hex(presented.PublicKeyThumbprintSHA256) ||
		presented.NotBefore.IsZero() || presented.NotAfter.IsZero() ||
		!presented.NotBefore.Before(presented.NotAfter) {
		return RuntimeDeviceIdentity{}, newRuntimeSessionError(RuntimeSessionErrorAuthenticationFailed, nil)
	}

	node, err := v.queries.GetRuntimeNodeByCertificate(ctx, db.GetRuntimeNodeByCertificateParams{
		DeviceCertificateSerial:   presented.Serial,
		DevicePublicKeyThumbprint: presented.PublicKeyThumbprintSHA256,
	})
	if err != nil {
		return RuntimeDeviceIdentity{}, newRuntimeSessionError(RuntimeSessionErrorAuthenticationFailed, err)
	}
	if (node.Status != "active" && node.Status != "draining") || node.NodeID == uuid.Nil ||
		!constantTimeStringEqual(node.DeviceCertificateSerial, presented.Serial) ||
		!constantTimeStringEqual(node.DevicePublicKeyThumbprint, presented.PublicKeyThumbprintSHA256) {
		return RuntimeDeviceIdentity{}, newRuntimeSessionError(RuntimeSessionErrorAuthenticationFailed, nil)
	}

	var databaseNow time.Time
	if err = v.clock.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&databaseNow); err != nil {
		return RuntimeDeviceIdentity{}, newRuntimeSessionError(RuntimeSessionErrorAuthenticationFailed, err)
	}
	if databaseNow.Before(presented.NotBefore) || !databaseNow.Before(presented.NotAfter) {
		return RuntimeDeviceIdentity{}, newRuntimeSessionError(RuntimeSessionErrorAuthenticationFailed, nil)
	}

	return RuntimeDeviceIdentity{
		NodeID:                       node.NodeID,
		CertificateSerial:            presented.Serial,
		CertificateFingerprintSHA256: presented.FingerprintSHA256,
		PublicKeyThumbprintSHA256:    presented.PublicKeyThumbprintSHA256,
	}, nil
}

// MTLSRuntimeDeviceAuthenticator authenticates only a TLS peer certificate.
// Forwarded certificate headers are deliberately ignored.
type MTLSRuntimeDeviceAuthenticator struct {
	verifier RuntimeNodeCredentialVerifier
}

func NewMTLSRuntimeDeviceAuthenticator(verifier RuntimeNodeCredentialVerifier) *MTLSRuntimeDeviceAuthenticator {
	return &MTLSRuntimeDeviceAuthenticator{verifier: verifier}
}

// AuthenticateHTTP authenticates the verified leaf certificate attached by
// net/http and resolves its durable Node credential. VerifiedChains is
// required so RequestClientCert-only server configurations cannot silently
// turn a presented, unverified certificate into a trusted identity.
func (a *MTLSRuntimeDeviceAuthenticator) AuthenticateHTTP(
	ctx context.Context,
	req *http.Request,
) (RuntimeDeviceIdentity, error) {
	if a == nil || a.verifier == nil || req == nil || req.TLS == nil ||
		len(req.TLS.PeerCertificates) == 0 || len(req.TLS.VerifiedChains) == 0 ||
		len(req.TLS.VerifiedChains[0]) == 0 {
		return RuntimeDeviceIdentity{}, newRuntimeSessionError(RuntimeSessionErrorAuthenticationFailed, nil)
	}

	leaf := req.TLS.PeerCertificates[0]
	verifiedLeaf := req.TLS.VerifiedChains[0][0]
	if leaf == nil || verifiedLeaf == nil || leaf.SerialNumber == nil ||
		leaf.SerialNumber.Sign() <= 0 || len(leaf.Raw) == 0 ||
		len(leaf.RawSubjectPublicKeyInfo) == 0 ||
		subtle.ConstantTimeCompare(leaf.Raw, verifiedLeaf.Raw) != 1 {
		return RuntimeDeviceIdentity{}, newRuntimeSessionError(RuntimeSessionErrorAuthenticationFailed, nil)
	}

	presented := runtimePresentedCertificate(leaf)
	identity, err := a.verifier.VerifyRuntimeNodeCredential(ctx, presented)
	if err != nil {
		return RuntimeDeviceIdentity{}, newRuntimeSessionError(RuntimeSessionErrorAuthenticationFailed, err)
	}
	if identity.NodeID == uuid.Nil ||
		!constantTimeStringEqual(identity.CertificateSerial, presented.Serial) ||
		!constantTimeStringEqual(identity.CertificateFingerprintSHA256, presented.FingerprintSHA256) ||
		!constantTimeStringEqual(identity.PublicKeyThumbprintSHA256, presented.PublicKeyThumbprintSHA256) {
		return RuntimeDeviceIdentity{}, newRuntimeSessionError(RuntimeSessionErrorAuthenticationFailed, nil)
	}
	return identity, nil
}

func runtimePresentedCertificate(cert *x509.Certificate) RuntimePresentedCertificate {
	fingerprint := sha256.Sum256(cert.Raw)
	publicKeyThumbprint := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	return RuntimePresentedCertificate{
		Serial:                    strings.ToLower(cert.SerialNumber.Text(16)),
		FingerprintSHA256:         hex.EncodeToString(fingerprint[:]),
		PublicKeyThumbprintSHA256: hex.EncodeToString(publicKeyThumbprint[:]),
		NotBefore:                 cert.NotBefore,
		NotAfter:                  cert.NotAfter,
	}
}

func constantTimeStringEqual(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

// AuthenticatedRuntimePrincipal combines the independently authenticated
// Agent Token and Node device identities. The request body may repeat these
// identifiers for protocol clarity, but it never establishes them.
type AuthenticatedRuntimePrincipal struct {
	AgentID      uuid.UUID             `json:"agent_id"`
	CredentialID uuid.UUID             `json:"credential_id"`
	Device       RuntimeDeviceIdentity `json:"device"`
}

// RuntimeSessionIdentity is the complete durable identity of one Node process
// session for one Agent. SessionEpoch changes on every process start, while
// WorkerID remains installation-stable.
type RuntimeSessionIdentity struct {
	RuntimeSessionID uuid.UUID `json:"runtime_session_id"`
	NodeID           uuid.UUID `json:"node_id"`
	AgentID          uuid.UUID `json:"agent_id"`
	WorkerID         string    `json:"worker_id"`
	SessionEpoch     int64     `json:"session_epoch"`
}

// RuntimeSessionRequest is the transport-neutral hello/session request.
type RuntimeSessionRequest struct {
	RuntimeSessionIdentity
	NodeVersion           string           `json:"node_version"`
	ProtocolVersion       int32            `json:"protocol_version"`
	RuntimeContractID     string           `json:"runtime_contract_id"`
	RuntimeContractDigest string           `json:"runtime_contract_digest"`
	Features              []string         `json:"features"`
	Capacity              int32            `json:"capacity"`
	AttachmentID          uuid.UUID        `json:"-"`
	Transport             RuntimeTransport `json:"-"`
	// ReportedTransportReason is an untrusted, bounded SDK hint received only
	// from the CreateSession request or WebSocket upgrade. Core validates it
	// against Transport, the prior durable attachment and TransportPolicy.
	ReportedTransportReason RuntimeTransportReason `json:"-"`
	TransportPolicy         RuntimeTransportPolicy `json:"-"`
}

// RuntimeSessionHeartbeatRequest repeats immutable contract identity so a
// reconnect cannot silently downgrade features or switch a worker/session.
type RuntimeSessionHeartbeatRequest = RuntimeSessionRequest

// RuntimeSessionCloseRequest closes an attachment and Session atomically.
// Status is either offline (reconnectable) or closed (permanent).
type RuntimeSessionCloseRequest struct {
	RuntimeSessionIdentity
	Status       string    `json:"status"`
	Reason       string    `json:"reason"`
	AttachmentID uuid.UUID `json:"-"`
}

// RuntimeSessionState is durable state returned after transaction commit.
// DatabaseTime is always taken from a PostgreSQL-written timestamp.
type RuntimeSessionState struct {
	Session      db.RuntimeSession            `json:"session"`
	Attachment   *db.RuntimeSessionAttachment `json:"attachment,omitempty"`
	DatabaseTime time.Time                    `json:"database_time"`
	Replayed     bool                         `json:"replayed"`
	Resumed      bool                         `json:"resumed"`
}

// RuntimeSessionPrincipal is the active, database-validated Session identity
// consumed by claim, command, Event, and Result adapters. It can be converted
// directly to the transport-neutral Event/Result principal.
type RuntimeSessionPrincipal struct {
	RuntimeSessionID                uuid.UUID
	NodeID                          uuid.UUID
	AgentID                         uuid.UUID
	CredentialID                    uuid.UUID
	WorkerID                        string
	SessionEpoch                    int64
	RuntimeContractDigest           string
	CoreInstanceID                  uuid.UUID
	AttachmentID                    uuid.UUID
	DeviceCertificateSerial         string
	DevicePublicKeyThumbprintSHA256 string
	Status                          string
	DatabaseTime                    time.Time
}

func (p RuntimeSessionPrincipal) EventPrincipal() RuntimeEventPrincipal {
	nodeID := p.NodeID
	sessionID := p.RuntimeSessionID
	workerID := p.WorkerID
	credentialID := p.CredentialID
	coreInstanceID := p.CoreInstanceID
	attachmentID := p.AttachmentID
	certificateSerial := p.DeviceCertificateSerial
	publicKeyThumbprint := p.DevicePublicKeyThumbprintSHA256
	return RuntimeEventPrincipal{
		AgentID:                         p.AgentID,
		CredentialID:                    &credentialID,
		NodeID:                          &nodeID,
		WorkerID:                        &workerID,
		RuntimeSessionID:                &sessionID,
		RuntimeContractDigest:           p.RuntimeContractDigest,
		CoreInstanceID:                  &coreInstanceID,
		AttachmentID:                    &attachmentID,
		DeviceCertificateSerial:         &certificateSerial,
		DevicePublicKeyThumbprintSHA256: &publicKeyThumbprint,
	}
}

type RuntimeSessionErrorCode string

const (
	RuntimeSessionErrorAuthenticationFailed   RuntimeSessionErrorCode = "AUTHENTICATION_FAILED"
	RuntimeSessionErrorValidationFailed       RuntimeSessionErrorCode = "VALIDATION_FAILED"
	RuntimeSessionErrorAgentMismatch          RuntimeSessionErrorCode = "AGENT_MISMATCH"
	RuntimeSessionErrorDeviceMismatch         RuntimeSessionErrorCode = "DEVICE_MISMATCH"
	RuntimeSessionErrorProtocolUnsupported    RuntimeSessionErrorCode = "PROTOCOL_UNSUPPORTED"
	RuntimeSessionErrorContractMismatch       RuntimeSessionErrorCode = "CONTRACT_MISMATCH"
	RuntimeSessionErrorRequiredFeatureMissing RuntimeSessionErrorCode = "REQUIRED_FEATURE_MISSING"
	RuntimeSessionErrorSessionConflict        RuntimeSessionErrorCode = "SESSION_CONFLICT"
	RuntimeSessionErrorPrincipalInactive      RuntimeSessionErrorCode = "PRINCIPAL_INACTIVE"
	RuntimeSessionErrorNotAttached            RuntimeSessionErrorCode = "SESSION_NOT_ATTACHED"
)

type RuntimeSessionError struct {
	Code  RuntimeSessionErrorCode `json:"code"`
	cause error
}

func (e *RuntimeSessionError) Error() string {
	if e == nil {
		return ""
	}
	switch e.Code {
	case RuntimeSessionErrorAuthenticationFailed:
		return "runtime device authentication failed"
	case RuntimeSessionErrorValidationFailed:
		return "runtime session validation failed"
	case RuntimeSessionErrorAgentMismatch:
		return "runtime Agent principal does not match the session"
	case RuntimeSessionErrorDeviceMismatch:
		return "runtime device principal does not match the session"
	case RuntimeSessionErrorProtocolUnsupported:
		return "runtime protocol version is unsupported"
	case RuntimeSessionErrorContractMismatch:
		return "runtime contract does not match Core"
	case RuntimeSessionErrorRequiredFeatureMissing:
		return "runtime client is missing a required feature"
	case RuntimeSessionErrorSessionConflict:
		return "runtime session conflicts with an active session"
	case RuntimeSessionErrorPrincipalInactive:
		return "runtime principal is revoked, expired, or inactive"
	case RuntimeSessionErrorNotAttached:
		return "runtime session is not attached to this Core instance"
	default:
		return "runtime session rejected"
	}
}

func (e *RuntimeSessionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func IsRuntimeSessionError(err error, code RuntimeSessionErrorCode) bool {
	var sessionErr *RuntimeSessionError
	return errors.As(err, &sessionErr) && sessionErr.Code == code
}

func newRuntimeSessionError(code RuntimeSessionErrorCode, cause error) error {
	return &RuntimeSessionError{Code: code, cause: cause}
}

// RuntimeSessionService owns Session lifecycle and the global lock order:
// Session -> Node -> Token -> Attachment.
type RuntimeSessionService struct {
	repository     runtimeSessionRepository
	coreInstanceID uuid.UUID
}

func NewRuntimeSessionService(pool *pgxpool.Pool, coreInstanceID uuid.UUID) *RuntimeSessionService {
	if pool == nil {
		return &RuntimeSessionService{coreInstanceID: coreInstanceID}
	}
	return &RuntimeSessionService{
		repository:     &postgresRuntimeSessionRepository{pool: pool, queries: db.New(pool)},
		coreInstanceID: coreInstanceID,
	}
}

func newRuntimeSessionService(repository runtimeSessionRepository, coreInstanceID uuid.UUID) *RuntimeSessionService {
	return &RuntimeSessionService{repository: repository, coreInstanceID: coreInstanceID}
}

// ResolveSessionPrincipal turns an authenticated Token/device pair plus an
// opaque Session ID into the exact active executor identity. The repository
// rechecks Session, Node, Token expiry/revocation, attachment, and Core owner in
// one database statement; callers must still perform the Attempt lock/fence
// check inside their own business transaction.
func (s *RuntimeSessionService) ResolveSessionPrincipal(
	ctx context.Context,
	principal AuthenticatedRuntimePrincipal,
	runtimeSessionID uuid.UUID,
) (RuntimeSessionPrincipal, error) {
	if err := validateAuthenticatedRuntimePrincipal(principal); err != nil {
		return RuntimeSessionPrincipal{}, err
	}
	if runtimeSessionID == uuid.Nil || s == nil || s.repository == nil || s.coreInstanceID == uuid.Nil {
		return RuntimeSessionPrincipal{}, newRuntimeSessionError(RuntimeSessionErrorValidationFailed, nil)
	}

	resolved, err := s.repository.ResolveRuntimeSessionPrincipal(ctx, runtimeSessionResolveParams{
		RuntimeSessionID:          runtimeSessionID,
		NodeID:                    principal.Device.NodeID,
		AgentID:                   principal.AgentID,
		CredentialID:              principal.CredentialID,
		CertificateSerial:         principal.Device.CertificateSerial,
		PublicKeyThumbprintSHA256: principal.Device.PublicKeyThumbprintSHA256,
		CoreInstanceID:            s.coreInstanceID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RuntimeSessionPrincipal{}, newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, err)
		}
		return RuntimeSessionPrincipal{}, err
	}
	return resolved, nil
}

// ResolveWorkerSessionPrincipal resolves the currently attached acting
// Session without trusting the immutable source Session ID carried by an
// Attempt. HTTP Event/Result uploads use this path so a replacement process
// can present a durable resume grant while the wire Attempt identity remains
// unchanged.
func (s *RuntimeSessionService) ResolveWorkerSessionPrincipal(
	ctx context.Context,
	principal AuthenticatedRuntimePrincipal,
	workerID string,
) (RuntimeSessionPrincipal, error) {
	if err := validateAuthenticatedRuntimePrincipal(principal); err != nil {
		return RuntimeSessionPrincipal{}, err
	}
	if s == nil || s.repository == nil || s.coreInstanceID == uuid.Nil ||
		!validRuntimeIdentityText(workerID, 1, maxRuntimeSessionWorkerIDRunes) {
		return RuntimeSessionPrincipal{}, newRuntimeSessionError(RuntimeSessionErrorValidationFailed, nil)
	}

	resolved, err := s.repository.ResolveRuntimeWorkerSessionPrincipal(ctx, runtimeWorkerSessionResolveParams{
		NodeID:                    principal.Device.NodeID,
		AgentID:                   principal.AgentID,
		CredentialID:              principal.CredentialID,
		WorkerID:                  workerID,
		CertificateSerial:         principal.Device.CertificateSerial,
		PublicKeyThumbprintSHA256: principal.Device.PublicKeyThumbprintSHA256,
		CoreInstanceID:            s.coreInstanceID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RuntimeSessionPrincipal{}, newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, err)
		}
		return RuntimeSessionPrincipal{}, err
	}
	return resolved, nil
}

func (s *RuntimeSessionService) CreateOrAttachSession(
	ctx context.Context,
	principal AuthenticatedRuntimePrincipal,
	request RuntimeSessionRequest,
) (RuntimeSessionState, error) {
	normalized, err := validateRuntimeSessionRequest(principal, request)
	if err != nil {
		return RuntimeSessionState{}, err
	}
	if s == nil || s.repository == nil || s.coreInstanceID == uuid.Nil {
		return RuntimeSessionState{}, newRuntimeSessionError(RuntimeSessionErrorValidationFailed, nil)
	}
	var state RuntimeSessionState
	err = s.repository.WithTransaction(ctx, func(tx runtimeSessionTransaction) error {
		if lockErr := tx.LockSessionIdentity(ctx, normalized.RuntimeSessionID); lockErr != nil {
			return lockErr
		}
		lockedSessionIDs, lockErr := lockRuntimeSessionPrincipal(
			ctx,
			tx,
			principal.Device.NodeID,
			principal.CredentialID,
			normalized.RuntimeSessionID,
		)
		if lockErr != nil {
			return lockErr
		}

		existing, getErr := tx.GetRuntimeSessionForUpdate(ctx, normalized.RuntimeSessionID)
		switch {
		case getErr == nil:
			return s.attachExistingSession(ctx, tx, principal, normalized, existing, &state)
		case !errors.Is(getErr, pgx.ErrNoRows):
			return getErr
		}
		// An idempotent attach/resume of an existing Session remains available
		// so accepted work can finish. Only a new Session insert is fenced by
		// hard maintenance, in this same transaction.
		if gateErr := tx.RequireRuntimeClusterOperation(ctx, RuntimeClusterNewSession); gateErr != nil {
			return gateErr
		}
		if generationErr := rejectStaleRuntimeSessionGeneration(ctx, tx, normalized); generationErr != nil {
			return generationErr
		}
		transportReason, validReason := resolveRuntimeTransportReason(
			normalized.Transport, "", normalized.ReportedTransportReason, normalized.TransportPolicy,
		)
		if !validReason {
			return newRuntimeSessionError(RuntimeSessionErrorValidationFailed, nil)
		}

		node, nodeErr := tx.GetRuntimeNode(ctx, principal.Device.NodeID)
		if nodeErr != nil {
			if errors.Is(nodeErr, pgx.ErrNoRows) {
				return newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, nodeErr)
			}
			return nodeErr
		}
		if node.Status != "active" {
			return newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, nil)
		}
		active, nodeErr := negotiateRuntimeNodeForSession(ctx, tx, node, principal, normalized)
		if nodeErr != nil {
			return nodeErr
		}
		for _, candidate := range active {
			if candidate.AgentID == principal.AgentID && candidate.WorkerID == normalized.WorkerID &&
				candidate.RuntimeSessionID != normalized.RuntimeSessionID {
				return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
			}
		}
		created, createErr := tx.CreateRuntimeSession(ctx, db.CreateRuntimeSessionParams{
			RuntimeSessionID:        normalized.RuntimeSessionID,
			NodeID:                  principal.Device.NodeID,
			AgentID:                 principal.AgentID,
			CredentialID:            principal.CredentialID,
			WorkerID:                normalized.WorkerID,
			SessionEpoch:            normalized.SessionEpoch,
			DeviceCertificateSerial: principal.Device.CertificateSerial,
			NodeVersion:             normalized.NodeVersion,
			ProtocolVersion:         normalized.ProtocolVersion,
			RuntimeContractID:       normalized.RuntimeContractID,
			RuntimeContractDigest:   normalized.RuntimeContractDigest,
			Features:                normalized.Features,
			Capacity:                normalized.Capacity,
			AttachedCoreInstanceID:  s.coreInstanceID,
		})
		if createErr != nil {
			if errors.Is(createErr, pgx.ErrNoRows) {
				return newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, createErr)
			}
			return createErr
		}

		attachment, attachmentErr := tx.CreateRuntimeSessionAttachment(ctx, db.CreateRuntimeSessionAttachmentParams{
			RuntimeSessionID: created.RuntimeSessionID,
			CoreInstanceID:   s.coreInstanceID,
			AttachmentKind:   "connected",
			Transport:        string(normalized.Transport),
			TransportReason:  string(transportReason),
		})
		if attachmentErr != nil {
			return attachmentErr
		}
		_ = lockedSessionIDs // documents that attachment scope was locked before writes
		state = runtimeSessionState(created, &attachment, false, false)
		return nil
	})
	return state, err
}

func (s *RuntimeSessionService) attachExistingSession(
	ctx context.Context,
	tx runtimeSessionTransaction,
	principal AuthenticatedRuntimePrincipal,
	request RuntimeSessionRequest,
	existing db.RuntimeSession,
	state *RuntimeSessionState,
) error {
	if err := validateStoredRuntimeSession(existing, principal, request); err != nil {
		return err
	}
	if err := rejectStaleRuntimeSessionGeneration(ctx, tx, request); err != nil {
		return err
	}

	node, err := tx.GetRuntimeNode(ctx, principal.Device.NodeID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, err)
		}
		return err
	}
	if existing.Status == "closed" || existing.Status == "revoked" {
		return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
	}
	if existing.AttachedCoreInstanceID != nil &&
		*existing.AttachedCoreInstanceID != s.coreInstanceID && existing.Status != "offline" {
		return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
	}
	wasOffline := existing.Status == "offline"
	var previousAttachment *db.RuntimeSessionAttachment
	if !wasOffline {
		previous, attachmentErr := tx.GetActiveRuntimeSessionAttachment(ctx, existing.RuntimeSessionID)
		if attachmentErr != nil {
			return attachmentErr
		}
		if previous.CoreInstanceID != s.coreInstanceID {
			return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
		}
		previousAttachment = &previous
	} else {
		history, historyErr := tx.ListRuntimeSessionAttachments(ctx, db.ListRuntimeSessionAttachmentsParams{
			RuntimeSessionID: existing.RuntimeSessionID,
			Limit:            1,
		})
		if historyErr != nil {
			return historyErr
		}
		if len(history) == 1 {
			previousAttachment = &history[0]
		}
	}
	previousTransport := RuntimeTransport("")
	if previousAttachment != nil {
		previousTransport = RuntimeTransport(previousAttachment.Transport)
	}
	transportReason, validReason := resolveRuntimeTransportReason(
		request.Transport, previousTransport, request.ReportedTransportReason, request.TransportPolicy,
	)
	if !validReason {
		return newRuntimeSessionError(RuntimeSessionErrorValidationFailed, nil)
	}
	if _, err = negotiateRuntimeNodeForSession(ctx, tx, node, principal, request); err != nil {
		return err
	}

	claimed, err := tx.ClaimRuntimeSessionForCore(ctx, db.ClaimRuntimeSessionForCoreParams{
		RuntimeSessionID: existing.RuntimeSessionID,
		NodeID:           existing.NodeID,
		AgentID:          existing.AgentID,
		CredentialID:     existing.CredentialID,
		WorkerID:         existing.WorkerID,
		SessionEpoch:     existing.SessionEpoch,
		CoreInstanceID:   s.coreInstanceID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, err)
		}
		return err
	}
	claimed, err = tx.HeartbeatRuntimeSession(ctx, db.HeartbeatRuntimeSessionParams{
		RuntimeSessionID:      claimed.RuntimeSessionID,
		CoreInstanceID:        s.coreInstanceID,
		NodeVersion:           request.NodeVersion,
		ProtocolVersion:       request.ProtocolVersion,
		RuntimeContractID:     request.RuntimeContractID,
		RuntimeContractDigest: request.RuntimeContractDigest,
		Features:              request.Features,
		Capacity:              request.Capacity,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, err)
		}
		return err
	}

	if !wasOffline {
		previous := *previousAttachment
		var attachmentErr error
		reason := "transport reattached"
		if _, attachmentErr = tx.CloseRuntimeSessionAttachment(ctx, db.CloseRuntimeSessionAttachmentParams{
			RuntimeSessionID: existing.RuntimeSessionID,
			CoreInstanceID:   s.coreInstanceID,
			AttachmentID:     previous.ID,
			DisconnectReason: &reason,
		}); attachmentErr != nil {
			return attachmentErr
		}
		attachment, attachmentErr := tx.CreateRuntimeSessionAttachment(ctx, db.CreateRuntimeSessionAttachmentParams{
			RuntimeSessionID: claimed.RuntimeSessionID,
			CoreInstanceID:   s.coreInstanceID,
			AttachmentKind:   "resumed",
			Transport:        string(request.Transport),
			TransportReason:  string(transportReason),
		})
		if attachmentErr != nil {
			return attachmentErr
		}
		*state = runtimeSessionState(claimed, &attachment, false, true)
		return nil
	}

	attachment, err := tx.CreateRuntimeSessionAttachment(ctx, db.CreateRuntimeSessionAttachmentParams{
		RuntimeSessionID: claimed.RuntimeSessionID,
		CoreInstanceID:   s.coreInstanceID,
		AttachmentKind:   "resumed",
		Transport:        string(request.Transport),
		TransportReason:  string(transportReason),
	})
	if err != nil {
		return err
	}
	*state = runtimeSessionState(claimed, &attachment, false, true)
	return nil
}

func (s *RuntimeSessionService) HeartbeatSession(
	ctx context.Context,
	principal AuthenticatedRuntimePrincipal,
	request RuntimeSessionHeartbeatRequest,
) (RuntimeSessionState, error) {
	normalized, err := validateRuntimeSessionRequest(principal, request)
	if err != nil {
		return RuntimeSessionState{}, err
	}
	if s == nil || s.repository == nil || s.coreInstanceID == uuid.Nil {
		return RuntimeSessionState{}, newRuntimeSessionError(RuntimeSessionErrorValidationFailed, nil)
	}
	if normalized.AttachmentID == uuid.Nil {
		return RuntimeSessionState{}, newRuntimeSessionError(RuntimeSessionErrorValidationFailed, nil)
	}

	var state RuntimeSessionState
	err = s.repository.WithTransaction(ctx, func(tx runtimeSessionTransaction) error {
		if lockErr := tx.LockSessionIdentity(ctx, normalized.RuntimeSessionID); lockErr != nil {
			return lockErr
		}
		if _, lockErr := lockRuntimeSessionPrincipal(
			ctx, tx, principal.Device.NodeID, principal.CredentialID, normalized.RuntimeSessionID,
		); lockErr != nil {
			return lockErr
		}
		existing, getErr := tx.GetRuntimeSessionForUpdate(ctx, normalized.RuntimeSessionID)
		if getErr != nil {
			if errors.Is(getErr, pgx.ErrNoRows) {
				return newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, getErr)
			}
			return getErr
		}
		if validateErr := validateStoredRuntimeSession(existing, principal, normalized); validateErr != nil {
			return validateErr
		}
		if existing.AttachedCoreInstanceID == nil || *existing.AttachedCoreInstanceID != s.coreInstanceID {
			return newRuntimeSessionError(RuntimeSessionErrorNotAttached, nil)
		}
		attachment, attachmentErr := tx.GetActiveRuntimeSessionAttachment(ctx, existing.RuntimeSessionID)
		if attachmentErr != nil {
			if errors.Is(attachmentErr, pgx.ErrNoRows) {
				return newRuntimeSessionError(RuntimeSessionErrorNotAttached, attachmentErr)
			}
			return attachmentErr
		}
		if attachment.CoreInstanceID != s.coreInstanceID || attachment.ID != normalized.AttachmentID {
			return newRuntimeSessionError(RuntimeSessionErrorNotAttached, nil)
		}
		if RuntimeTransport(attachment.Transport) != normalized.Transport {
			return newRuntimeSessionError(RuntimeSessionErrorNotAttached, nil)
		}
		if _, heartbeatErr := heartbeatRuntimeNodeForSession(ctx, tx, principal, normalized); heartbeatErr != nil {
			return heartbeatErr
		}
		heartbeat, heartbeatErr := tx.HeartbeatRuntimeSession(ctx, db.HeartbeatRuntimeSessionParams{
			RuntimeSessionID:      normalized.RuntimeSessionID,
			CoreInstanceID:        s.coreInstanceID,
			NodeVersion:           normalized.NodeVersion,
			ProtocolVersion:       normalized.ProtocolVersion,
			RuntimeContractID:     normalized.RuntimeContractID,
			RuntimeContractDigest: normalized.RuntimeContractDigest,
			Features:              normalized.Features,
			Capacity:              normalized.Capacity,
		})
		if heartbeatErr != nil {
			if errors.Is(heartbeatErr, pgx.ErrNoRows) {
				return newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, heartbeatErr)
			}
			return heartbeatErr
		}
		state = runtimeSessionState(heartbeat, &attachment, false, false)
		return nil
	})
	return state, err
}

type runtimeSessionDrainRecord struct {
	Session     db.RuntimeSession
	DeadlineAt  time.Time
	ReasonCode  string
	RequestedAt time.Time
}

// DrainSession commits the admission fence before acknowledging either
// transport. The client capacity/inflight snapshot is never authoritative:
// capacity is forced to zero and inflight is read back from PostgreSQL.
func (s *RuntimeSessionService) DrainSession(
	ctx context.Context,
	principal AuthenticatedRuntimePrincipal,
	request RuntimeSessionDrainRequest,
) (RuntimeDrainPayload, error) {
	if err := validateAuthenticatedRuntimePrincipal(principal); err != nil {
		return RuntimeDrainPayload{}, err
	}
	if request.RuntimeSessionID == uuid.Nil || request.AttachmentID == uuid.Nil ||
		s == nil || s.repository == nil || s.coreInstanceID == uuid.Nil {
		return RuntimeDrainPayload{}, newRuntimeSessionError(RuntimeSessionErrorValidationFailed, nil)
	}
	if err := ValidateRuntimePayload(request.Payload); err != nil {
		return RuntimeDrainPayload{}, newRuntimeSessionError(RuntimeSessionErrorValidationFailed, err)
	}

	var receipt RuntimeDrainPayload
	err := s.repository.WithTransaction(ctx, func(tx runtimeSessionTransaction) error {
		if err := tx.LockSessionIdentity(ctx, request.RuntimeSessionID); err != nil {
			return err
		}
		if _, err := lockRuntimeSessionPrincipal(
			ctx, tx, principal.Device.NodeID, principal.CredentialID, request.RuntimeSessionID,
		); err != nil {
			return err
		}
		existing, err := tx.GetRuntimeSessionForUpdate(ctx, request.RuntimeSessionID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, err)
			}
			return err
		}
		identity := RuntimeSessionIdentity{
			RuntimeSessionID: existing.RuntimeSessionID,
			NodeID:           existing.NodeID,
			AgentID:          existing.AgentID,
			WorkerID:         existing.WorkerID,
			SessionEpoch:     existing.SessionEpoch,
		}
		if err = validateStoredRuntimeSessionIdentity(existing, principal, identity); err != nil {
			return err
		}
		if existing.Status == "revoked" {
			return newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, nil)
		}
		if existing.Status != "active" && existing.Status != "draining" {
			return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
		}
		if existing.ProtocolVersion != RuntimeProtocolVersion ||
			existing.RuntimeContractID != RuntimeContractID ||
			!constantTimeStringEqual(existing.RuntimeContractDigest, RuntimeContractDigest) ||
			!containsRuntimeFeature(existing.Features, "session_drain") {
			return newRuntimeSessionError(RuntimeSessionErrorContractMismatch, nil)
		}
		if existing.AttachedCoreInstanceID == nil || *existing.AttachedCoreInstanceID != s.coreInstanceID {
			return newRuntimeSessionError(RuntimeSessionErrorNotAttached, nil)
		}
		attachment, err := tx.GetActiveRuntimeSessionAttachment(ctx, existing.RuntimeSessionID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return newRuntimeSessionError(RuntimeSessionErrorNotAttached, err)
			}
			return err
		}
		if attachment.CoreInstanceID != s.coreInstanceID || attachment.ID != request.AttachmentID {
			return newRuntimeSessionError(RuntimeSessionErrorNotAttached, nil)
		}
		node, err := tx.GetRuntimeNode(ctx, existing.NodeID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, err)
			}
			return err
		}
		if (node.Status != "active" && node.Status != "draining") ||
			node.ProtocolVersion != RuntimeProtocolVersion ||
			node.RuntimeContractID != RuntimeContractID ||
			!constantTimeStringEqual(node.RuntimeContractDigest, RuntimeContractDigest) ||
			!containsRuntimeFeature(node.Features, "session_drain") ||
			node.RuntimeContractID != existing.RuntimeContractID ||
			node.RuntimeContractDigest != existing.RuntimeContractDigest ||
			node.ProtocolVersion != existing.ProtocolVersion ||
			!constantTimeStringEqual(node.DeviceCertificateSerial, existing.DeviceCertificateSerial) ||
			!constantTimeStringEqual(node.DevicePublicKeyThumbprint, principal.Device.PublicKeyThumbprintSHA256) {
			return newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, nil)
		}

		drained, err := tx.DrainRuntimeSession(ctx, request.RuntimeSessionID, s.coreInstanceID, request.Payload)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, err)
			}
			return err
		}
		receipt = RuntimeDrainPayload{
			DeadlineAt: drained.DeadlineAt,
			ReasonCode: drained.ReasonCode,
			Capacity:   int64(drained.Session.Capacity),
			Inflight:   int64(drained.Session.Inflight),
		}
		return ValidateRuntimePayload(receipt)
	})
	return receipt, err
}

func (s *RuntimeSessionService) CloseSession(
	ctx context.Context,
	principal AuthenticatedRuntimePrincipal,
	request RuntimeSessionCloseRequest,
) (RuntimeSessionState, error) {
	if err := validateRuntimeSessionIdentity(principal, request.RuntimeSessionIdentity); err != nil {
		return RuntimeSessionState{}, err
	}
	if request.Status != "offline" && request.Status != "closed" {
		return RuntimeSessionState{}, newRuntimeSessionError(RuntimeSessionErrorValidationFailed, nil)
	}
	if !validRuntimeIdentityText(request.Reason, 1, maxRuntimeDisconnectReasonRunes) {
		return RuntimeSessionState{}, newRuntimeSessionError(RuntimeSessionErrorValidationFailed, nil)
	}
	if s == nil || s.repository == nil || s.coreInstanceID == uuid.Nil {
		return RuntimeSessionState{}, newRuntimeSessionError(RuntimeSessionErrorValidationFailed, nil)
	}
	if request.AttachmentID == uuid.Nil {
		return RuntimeSessionState{}, newRuntimeSessionError(RuntimeSessionErrorValidationFailed, nil)
	}

	var state RuntimeSessionState
	err := s.repository.WithTransaction(ctx, func(tx runtimeSessionTransaction) error {
		if err := tx.LockSessionIdentity(ctx, request.RuntimeSessionID); err != nil {
			return err
		}
		if _, err := lockRuntimeSessionPrincipal(
			ctx,
			tx,
			principal.Device.NodeID,
			principal.CredentialID,
			request.RuntimeSessionID,
		); err != nil {
			return err
		}
		existing, err := tx.GetRuntimeSessionForUpdate(ctx, request.RuntimeSessionID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, err)
			}
			return err
		}
		if err = validateStoredRuntimeSessionIdentity(existing, principal, request.RuntimeSessionIdentity); err != nil {
			return err
		}

		if existing.Status == request.Status {
			state = runtimeSessionState(existing, nil, true, false)
			return nil
		}
		if existing.Status == "offline" || existing.Status == "closed" {
			return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
		}
		if existing.Status == "revoked" {
			return newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, nil)
		}
		if existing.AttachedCoreInstanceID == nil || *existing.AttachedCoreInstanceID != s.coreInstanceID {
			return newRuntimeSessionError(RuntimeSessionErrorNotAttached, nil)
		}
		activeAttachment, err := tx.GetActiveRuntimeSessionAttachment(ctx, existing.RuntimeSessionID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return newRuntimeSessionError(RuntimeSessionErrorNotAttached, err)
			}
			return err
		}
		if activeAttachment.CoreInstanceID != s.coreInstanceID || request.AttachmentID != activeAttachment.ID {
			return newRuntimeSessionError(RuntimeSessionErrorNotAttached, nil)
		}

		reason := request.Reason
		attachment, err := tx.CloseRuntimeSessionAttachment(ctx, db.CloseRuntimeSessionAttachmentParams{
			RuntimeSessionID: existing.RuntimeSessionID,
			CoreInstanceID:   s.coreInstanceID,
			AttachmentID:     activeAttachment.ID,
			DisconnectReason: &reason,
		})
		if err != nil {
			return err
		}
		closed, err := tx.CloseRuntimeSession(ctx, db.CloseRuntimeSessionParams{
			RuntimeSessionID: existing.RuntimeSessionID,
			CoreInstanceID:   s.coreInstanceID,
			Status:           request.Status,
		})
		if err != nil {
			return err
		}
		state = runtimeSessionState(closed, &attachment, false, false)
		return nil
	})
	return state, err
}

func validateRuntimeSessionRequest(
	principal AuthenticatedRuntimePrincipal,
	request RuntimeSessionRequest,
) (RuntimeSessionRequest, error) {
	if err := validateRuntimeSessionIdentity(principal, request.RuntimeSessionIdentity); err != nil {
		return RuntimeSessionRequest{}, err
	}
	if !validRuntimeIdentityText(request.NodeVersion, 1, maxRuntimeSessionVersionRunes) ||
		request.Capacity < 0 || request.Capacity > maxRuntimeSessionCapacity ||
		!request.Transport.IsLive() {
		return RuntimeSessionRequest{}, newRuntimeSessionError(RuntimeSessionErrorValidationFailed, nil)
	}
	if request.ProtocolVersion != RuntimeProtocolVersion {
		return RuntimeSessionRequest{}, newRuntimeSessionError(RuntimeSessionErrorProtocolUnsupported, nil)
	}
	if request.RuntimeContractID != RuntimeContractID ||
		!runtimeWireContractSupported(request.RuntimeContractDigest) {
		return RuntimeSessionRequest{}, newRuntimeSessionError(RuntimeSessionErrorContractMismatch, nil)
	}

	features, err := normalizeRuntimeSessionFeatures(request.Features, request.RuntimeContractDigest)
	if err != nil {
		return RuntimeSessionRequest{}, err
	}
	request.Features = features
	return request, nil
}

func heartbeatRuntimeNodeForSession(
	ctx context.Context,
	tx runtimeSessionTransaction,
	principal AuthenticatedRuntimePrincipal,
	request RuntimeSessionRequest,
) (db.RuntimeNode, error) {
	node, err := tx.GetRuntimeNode(ctx, principal.Device.NodeID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return db.RuntimeNode{}, newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, err)
		}
		return db.RuntimeNode{}, err
	}
	if err = validateRuntimeNodeForSession(node, principal.Device, request); err != nil {
		return db.RuntimeNode{}, err
	}
	return persistRuntimeNodeHeartbeat(ctx, tx, principal, request)
}

// negotiateRuntimeNodeForSession is the only generation-switch path. The
// authenticated certificate fixes the Node identity; the supported Hello
// digest selects a Server-owned adapter generation. All Session rows for the
// Node are locked before this function is called. A Node may switch generation
// only when every live Session already uses the requested digest (which means
// there is no conflicting live generation during a real switch).
func negotiateRuntimeNodeForSession(
	ctx context.Context,
	tx runtimeSessionTransaction,
	node db.RuntimeNode,
	principal AuthenticatedRuntimePrincipal,
	request RuntimeSessionRequest,
) ([]db.RuntimeSession, error) {
	if err := validateRuntimeNodeForNegotiation(node, principal.Device, request); err != nil {
		return nil, err
	}
	active, err := tx.ListActiveRuntimeSessionsByNode(ctx, principal.Device.NodeID)
	if err != nil {
		return nil, err
	}
	for _, candidate := range active {
		if candidate.NodeID != principal.Device.NodeID ||
			candidate.ProtocolVersion != request.ProtocolVersion ||
			candidate.RuntimeContractID != request.RuntimeContractID ||
			candidate.RuntimeContractDigest != request.RuntimeContractDigest {
			return nil, newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
		}
	}
	switchingGeneration := node.RuntimeContractDigest != request.RuntimeContractDigest
	if switchingGeneration && len(active) != 0 {
		return nil, newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
	}
	if switchingGeneration {
		if _, err = tx.RetireOfflineRuntimeSessionsForGenerationSwitch(
			ctx, node.NodeID, request.RuntimeContractDigest,
		); err != nil {
			return nil, err
		}
	}
	if _, err = persistRuntimeNodeHeartbeat(ctx, tx, principal, request); err != nil {
		return nil, err
	}
	return active, nil
}

// rejectStaleRuntimeSessionGeneration makes Session epochs monotonic for one
// durable Node/Agent/worker identity. Terminal and offline rows count: a
// replayed older Session must never regain authority after a newer process
// generation has committed, even when every transport is currently offline.
func rejectStaleRuntimeSessionGeneration(
	ctx context.Context,
	tx runtimeSessionTransaction,
	request RuntimeSessionRequest,
) error {
	newer, err := tx.HasNewerRuntimeSessionGeneration(ctx, runtimeSessionGenerationParams{
		RuntimeSessionID: request.RuntimeSessionID,
		NodeID:           request.NodeID,
		AgentID:          request.AgentID,
		WorkerID:         request.WorkerID,
		SessionEpoch:     request.SessionEpoch,
	})
	if err != nil {
		return err
	}
	if newer {
		return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
	}
	return nil
}

func persistRuntimeNodeHeartbeat(
	ctx context.Context,
	tx runtimeSessionTransaction,
	principal AuthenticatedRuntimePrincipal,
	request RuntimeSessionRequest,
) (db.RuntimeNode, error) {
	node, err := tx.HeartbeatRuntimeNode(ctx, db.HeartbeatRuntimeNodeParams{
		NodeID:                    principal.Device.NodeID,
		NodeVersion:               request.NodeVersion,
		ProtocolVersion:           request.ProtocolVersion,
		RuntimeContractID:         request.RuntimeContractID,
		RuntimeContractDigest:     request.RuntimeContractDigest,
		Features:                  request.Features,
		Capacity:                  request.Capacity,
		DeviceCertificateSerial:   principal.Device.CertificateSerial,
		DevicePublicKeyThumbprint: principal.Device.PublicKeyThumbprintSHA256,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return db.RuntimeNode{}, newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, err)
	}
	return node, err
}

func validateRuntimeSessionIdentity(
	principal AuthenticatedRuntimePrincipal,
	identity RuntimeSessionIdentity,
) error {
	if err := validateAuthenticatedRuntimePrincipal(principal); err != nil {
		return err
	}
	if identity.AgentID != principal.AgentID {
		return newRuntimeSessionError(RuntimeSessionErrorAgentMismatch, nil)
	}
	if identity.NodeID != principal.Device.NodeID {
		return newRuntimeSessionError(RuntimeSessionErrorDeviceMismatch, nil)
	}
	if identity.RuntimeSessionID == uuid.Nil || identity.SessionEpoch <= 0 ||
		!validRuntimeIdentityText(identity.WorkerID, 1, maxRuntimeSessionWorkerIDRunes) {
		return newRuntimeSessionError(RuntimeSessionErrorValidationFailed, nil)
	}
	return nil
}

func validateAuthenticatedRuntimePrincipal(principal AuthenticatedRuntimePrincipal) error {
	if principal.AgentID == uuid.Nil || principal.CredentialID == uuid.Nil ||
		principal.Device.NodeID == uuid.Nil ||
		!validSHA256Hex(principal.Device.CertificateFingerprintSHA256) ||
		!validSHA256Hex(principal.Device.PublicKeyThumbprintSHA256) ||
		!validCertificateSerial(principal.Device.CertificateSerial) {
		return newRuntimeSessionError(RuntimeSessionErrorAuthenticationFailed, nil)
	}
	return nil
}

func normalizeRuntimeSessionFeatures(features []string, contractDigest string) ([]string, error) {
	required := runtimeRequiredFeaturesForDigest(contractDigest)
	if len(features) < len(required) {
		return nil, newRuntimeSessionError(RuntimeSessionErrorRequiredFeatureMissing, nil)
	}
	seen := make(map[string]struct{}, len(features))
	normalized := make([]string, 0, len(features))
	for _, feature := range features {
		if !validRuntimeIdentityText(feature, 1, maxRuntimeSessionFeatureRunes) {
			return nil, newRuntimeSessionError(RuntimeSessionErrorValidationFailed, nil)
		}
		if _, duplicate := seen[feature]; duplicate {
			return nil, newRuntimeSessionError(RuntimeSessionErrorValidationFailed, nil)
		}
		seen[feature] = struct{}{}
		normalized = append(normalized, feature)
	}
	for _, feature := range required {
		if _, ok := seen[feature]; !ok {
			return nil, newRuntimeSessionError(RuntimeSessionErrorRequiredFeatureMissing, nil)
		}
	}
	sort.Strings(normalized)
	return normalized, nil
}

func runtimeRequiredFeaturesForDigest(contractDigest string) []string {
	features := RuntimeRequiredFeatures()
	if constantTimeStringEqual(contractDigest, RuntimeContractDigest) {
		return features
	}
	withoutDrain := features[:0]
	for _, feature := range features {
		if feature != "session_drain" {
			withoutDrain = append(withoutDrain, feature)
		}
	}
	return withoutDrain
}

func validateRuntimeNodeForSession(
	node db.RuntimeNode,
	device RuntimeDeviceIdentity,
	request RuntimeSessionRequest,
) error {
	if node.Status != "active" && node.Status != "draining" {
		return newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, nil)
	}
	if node.NodeID != device.NodeID ||
		!constantTimeStringEqual(node.DeviceCertificateSerial, device.CertificateSerial) ||
		!constantTimeStringEqual(node.DevicePublicKeyThumbprint, device.PublicKeyThumbprintSHA256) {
		return newRuntimeSessionError(RuntimeSessionErrorDeviceMismatch, nil)
	}
	if node.NodeVersion != request.NodeVersion || node.ProtocolVersion != request.ProtocolVersion ||
		node.RuntimeContractID != request.RuntimeContractID ||
		node.RuntimeContractDigest != request.RuntimeContractDigest ||
		!sameRuntimeFeatureSet(node.Features, request.Features) {
		return newRuntimeSessionError(RuntimeSessionErrorContractMismatch, nil)
	}
	return nil
}

func validateRuntimeNodeForNegotiation(
	node db.RuntimeNode,
	device RuntimeDeviceIdentity,
	request RuntimeSessionRequest,
) error {
	if node.Status != "active" && node.Status != "draining" {
		return newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, nil)
	}
	if node.NodeID != device.NodeID ||
		!constantTimeStringEqual(node.DeviceCertificateSerial, device.CertificateSerial) ||
		!constantTimeStringEqual(node.DevicePublicKeyThumbprint, device.PublicKeyThumbprintSHA256) {
		return newRuntimeSessionError(RuntimeSessionErrorDeviceMismatch, nil)
	}
	if node.NodeVersion != request.NodeVersion || node.ProtocolVersion != request.ProtocolVersion ||
		node.RuntimeContractID != request.RuntimeContractID ||
		!runtimeWireContractSupported(node.RuntimeContractDigest) ||
		!runtimeWireContractSupported(request.RuntimeContractDigest) {
		return newRuntimeSessionError(RuntimeSessionErrorContractMismatch, nil)
	}
	if node.RuntimeContractDigest == request.RuntimeContractDigest {
		if !sameRuntimeFeatureSet(node.Features, request.Features) {
			return newRuntimeSessionError(RuntimeSessionErrorContractMismatch, nil)
		}
		return nil
	}

	// A ring switch changes the durable Node adapter generation. Extensions
	// that were valid within one generation cannot be silently carried across
	// that boundary: the target must advertise exactly its Server-owned feature
	// set. The caller locks and proves the complete live Session set is empty
	// before persistRuntimeNodeHeartbeat commits digest+features atomically.
	if !sameRuntimeFeatureSet(
		request.Features,
		runtimeRequiredFeaturesForDigest(request.RuntimeContractDigest),
	) {
		return newRuntimeSessionError(RuntimeSessionErrorContractMismatch, nil)
	}
	return nil
}

func validateStoredRuntimeSession(
	session db.RuntimeSession,
	principal AuthenticatedRuntimePrincipal,
	request RuntimeSessionRequest,
) error {
	if err := validateStoredRuntimeSessionIdentity(session, principal, request.RuntimeSessionIdentity); err != nil {
		return err
	}
	if session.NodeVersion != request.NodeVersion || session.ProtocolVersion != request.ProtocolVersion ||
		session.RuntimeContractID != request.RuntimeContractID ||
		session.RuntimeContractDigest != request.RuntimeContractDigest ||
		!sameRuntimeFeatureSet(session.Features, request.Features) {
		return newRuntimeSessionError(RuntimeSessionErrorContractMismatch, nil)
	}
	return nil
}

func validateStoredRuntimeSessionIdentity(
	session db.RuntimeSession,
	principal AuthenticatedRuntimePrincipal,
	identity RuntimeSessionIdentity,
) error {
	if session.RuntimeSessionID != identity.RuntimeSessionID ||
		session.WorkerID != identity.WorkerID || session.SessionEpoch != identity.SessionEpoch {
		return newRuntimeSessionError(RuntimeSessionErrorSessionConflict, nil)
	}
	if session.AgentID != principal.AgentID || session.CredentialID != principal.CredentialID ||
		session.AgentID != identity.AgentID {
		return newRuntimeSessionError(RuntimeSessionErrorAgentMismatch, nil)
	}
	if session.NodeID != principal.Device.NodeID || session.NodeID != identity.NodeID ||
		!constantTimeStringEqual(session.DeviceCertificateSerial, principal.Device.CertificateSerial) {
		return newRuntimeSessionError(RuntimeSessionErrorDeviceMismatch, nil)
	}
	return nil
}

func lockRuntimeSessionPrincipal(
	ctx context.Context,
	tx runtimeSessionTransaction,
	nodeID uuid.UUID,
	credentialID uuid.UUID,
	targetSessionID uuid.UUID,
) ([]uuid.UUID, error) {
	lockedSessions, err := tx.LockRuntimeSessionsForPrincipalRevocation(
		ctx,
		db.LockRuntimeSessionsForPrincipalRevocationParams{
			NodeIDs:  []uuid.UUID{nodeID},
			TokenIDs: []uuid.UUID{credentialID},
		},
	)
	if err != nil {
		return nil, err
	}

	lockedNodes, err := tx.LockRuntimeNodesForPrincipalRevocation(ctx, []uuid.UUID{nodeID})
	if err != nil {
		return nil, err
	}
	if len(lockedNodes) != 1 || lockedNodes[0] != nodeID {
		return nil, newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, nil)
	}
	lockedTokens, err := tx.LockAgentTokensForPrincipalRevocation(ctx, []uuid.UUID{credentialID})
	if err != nil {
		return nil, err
	}
	if len(lockedTokens) != 1 || lockedTokens[0] != credentialID {
		return nil, newRuntimeSessionError(RuntimeSessionErrorPrincipalInactive, nil)
	}

	sessionIDs := make([]uuid.UUID, 0, len(lockedSessions)+1)
	seen := make(map[uuid.UUID]struct{}, len(lockedSessions)+1)
	for _, session := range lockedSessions {
		if _, ok := seen[session.RuntimeSessionID]; ok {
			continue
		}
		seen[session.RuntimeSessionID] = struct{}{}
		sessionIDs = append(sessionIDs, session.RuntimeSessionID)
	}
	if _, ok := seen[targetSessionID]; !ok {
		sessionIDs = append(sessionIDs, targetSessionID)
	}
	sort.Slice(sessionIDs, func(i, j int) bool {
		return strings.Compare(sessionIDs[i].String(), sessionIDs[j].String()) < 0
	})
	if _, err = tx.LockActiveRuntimeSessionAttachmentsForPrincipalRevocation(ctx, sessionIDs); err != nil {
		return nil, err
	}
	return sessionIDs, nil
}

func runtimeSessionState(
	session db.RuntimeSession,
	attachment *db.RuntimeSessionAttachment,
	replayed bool,
	resumed bool,
) RuntimeSessionState {
	databaseTime := session.HeartbeatAt
	if attachment != nil && attachment.AttachedAt.After(databaseTime) {
		databaseTime = attachment.AttachedAt
	}
	return RuntimeSessionState{
		Session:      session,
		Attachment:   attachment,
		DatabaseTime: databaseTime,
		Replayed:     replayed,
		Resumed:      resumed,
	}
}

func sameRuntimeFeatureSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftCopy := append([]string(nil), left...)
	rightCopy := append([]string(nil), right...)
	sort.Strings(leftCopy)
	sort.Strings(rightCopy)
	for index := range leftCopy {
		if leftCopy[index] != rightCopy[index] {
			return false
		}
	}
	return true
}

func validRuntimeIdentityText(value string, minRunes, maxRunes int) bool {
	if !utf8.ValidString(value) || value != strings.TrimSpace(value) {
		return false
	}
	count := utf8.RuneCountInString(value)
	return count >= minRunes && count <= maxRunes
}

func validSHA256Hex(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validCertificateSerial(value string) bool {
	if len(value) < 1 || len(value) > 128 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	if err == nil {
		return true
	}
	// big.Int.Text(16) can produce an odd number of hexadecimal digits.
	_, err = hex.DecodeString("0" + value)
	return err == nil
}

type runtimeSessionRepository interface {
	WithTransaction(context.Context, func(runtimeSessionTransaction) error) error
	ResolveRuntimeSessionPrincipal(context.Context, runtimeSessionResolveParams) (RuntimeSessionPrincipal, error)
	ResolveRuntimeWorkerSessionPrincipal(context.Context, runtimeWorkerSessionResolveParams) (RuntimeSessionPrincipal, error)
}

type runtimeSessionResolveParams struct {
	RuntimeSessionID          uuid.UUID
	NodeID                    uuid.UUID
	AgentID                   uuid.UUID
	CredentialID              uuid.UUID
	CertificateSerial         string
	PublicKeyThumbprintSHA256 string
	CoreInstanceID            uuid.UUID
}

type runtimeWorkerSessionResolveParams struct {
	NodeID                    uuid.UUID
	AgentID                   uuid.UUID
	CredentialID              uuid.UUID
	WorkerID                  string
	CertificateSerial         string
	PublicKeyThumbprintSHA256 string
	CoreInstanceID            uuid.UUID
}

type runtimeSessionGenerationParams struct {
	RuntimeSessionID uuid.UUID
	NodeID           uuid.UUID
	AgentID          uuid.UUID
	WorkerID         string
	SessionEpoch     int64
}

type runtimeSessionTransaction interface {
	RequireRuntimeClusterOperation(context.Context, RuntimeClusterOperation) error
	LockSessionIdentity(context.Context, uuid.UUID) error
	GetRuntimeSessionForUpdate(context.Context, uuid.UUID) (db.RuntimeSession, error)
	LockRuntimeSessionsForPrincipalRevocation(context.Context, db.LockRuntimeSessionsForPrincipalRevocationParams) ([]db.LockRuntimeSessionsForPrincipalRevocationRow, error)
	LockRuntimeNodesForPrincipalRevocation(context.Context, []uuid.UUID) ([]uuid.UUID, error)
	LockAgentTokensForPrincipalRevocation(context.Context, []uuid.UUID) ([]uuid.UUID, error)
	LockActiveRuntimeSessionAttachmentsForPrincipalRevocation(context.Context, []uuid.UUID) ([]uuid.UUID, error)
	GetRuntimeNode(context.Context, uuid.UUID) (db.RuntimeNode, error)
	HasNewerRuntimeSessionGeneration(context.Context, runtimeSessionGenerationParams) (bool, error)
	HeartbeatRuntimeNode(context.Context, db.HeartbeatRuntimeNodeParams) (db.RuntimeNode, error)
	ListActiveRuntimeSessionsByNode(context.Context, uuid.UUID) ([]db.RuntimeSession, error)
	RetireOfflineRuntimeSessionsForGenerationSwitch(context.Context, uuid.UUID, string) (int64, error)
	CreateRuntimeSession(context.Context, db.CreateRuntimeSessionParams) (db.RuntimeSession, error)
	ClaimRuntimeSessionForCore(context.Context, db.ClaimRuntimeSessionForCoreParams) (db.RuntimeSession, error)
	HeartbeatRuntimeSession(context.Context, db.HeartbeatRuntimeSessionParams) (db.RuntimeSession, error)
	DrainRuntimeSession(context.Context, uuid.UUID, uuid.UUID, RuntimeDrainPayload) (runtimeSessionDrainRecord, error)
	GetActiveRuntimeSessionAttachment(context.Context, uuid.UUID) (db.RuntimeSessionAttachment, error)
	ListRuntimeSessionAttachments(context.Context, db.ListRuntimeSessionAttachmentsParams) ([]db.RuntimeSessionAttachment, error)
	CreateRuntimeSessionAttachment(context.Context, db.CreateRuntimeSessionAttachmentParams) (db.RuntimeSessionAttachment, error)
	CloseRuntimeSessionAttachment(context.Context, db.CloseRuntimeSessionAttachmentParams) (db.RuntimeSessionAttachment, error)
	CloseRuntimeSession(context.Context, db.CloseRuntimeSessionParams) (db.RuntimeSession, error)
	CloseStaleRuntimeSession(context.Context, db.CloseStaleRuntimeSessionParams) (db.RuntimeSession, error)
}

type postgresRuntimeSessionRepository struct {
	pool    *pgxpool.Pool
	queries *db.Queries
}

func (r *postgresRuntimeSessionRepository) WithTransaction(
	ctx context.Context,
	fn func(runtimeSessionTransaction) error,
) error {
	if r == nil || r.pool == nil || r.queries == nil {
		return fmt.Errorf("runtime session repository is not configured")
	}
	return pgx.BeginTxFunc(ctx, r.pool, pgx.TxOptions{
		IsoLevel:   pgx.ReadCommitted,
		AccessMode: pgx.ReadWrite,
	}, func(tx pgx.Tx) error {
		return fn(&postgresRuntimeSessionTransaction{tx: tx, queries: r.queries.WithTx(tx)})
	})
}

func (r *postgresRuntimeSessionRepository) ResolveRuntimeSessionPrincipal(
	ctx context.Context,
	params runtimeSessionResolveParams,
) (RuntimeSessionPrincipal, error) {
	const statement = `
SELECT s.runtime_session_id,
       s.node_id,
       s.agent_id,
       s.credential_id,
       s.worker_id,
       s.session_epoch,
       s.runtime_contract_digest,
       s.attached_core_instance_id,
	   s.device_certificate_serial,
	   n.device_public_key_thumbprint,
	   a.id,
       s.status,
       clock_timestamp()
FROM runtime_sessions s
JOIN runtime_nodes n
  ON n.node_id = s.node_id
JOIN agent_tokens t
  ON t.id = s.credential_id
 AND t.agent_id = s.agent_id
JOIN runtime_session_attachments a
  ON a.runtime_session_id = s.runtime_session_id
 AND a.core_instance_id = s.attached_core_instance_id
 AND a.detached_at IS NULL
JOIN runtime_wire_contracts wire
  ON wire.runtime_contract_id = s.runtime_contract_id
 AND wire.runtime_contract_digest = s.runtime_contract_digest
 AND wire.support_tier IN ('current', 'previous')
WHERE s.runtime_session_id = $1
  AND s.node_id = $2
  AND s.agent_id = $3
  AND s.credential_id = $4
  AND s.device_certificate_serial = $5
  AND n.device_certificate_serial = $5
  AND n.device_public_key_thumbprint = $6
  AND s.attached_core_instance_id = $7
  AND s.status IN ('active', 'draining')
  AND s.protocol_version = 2
  AND s.runtime_contract_id = 'openlinker.runtime.v2'
  AND n.status IN ('active', 'draining')
  AND n.protocol_version = s.protocol_version
  AND n.runtime_contract_id = s.runtime_contract_id
  AND n.runtime_contract_digest = s.runtime_contract_digest
  AND t.status = 'active_runtime'
  AND t.revoked_at IS NULL
  AND t.scopes @> ARRAY['agent:pull']::text[]
  AND (t.expires_at IS NULL OR t.expires_at > clock_timestamp())`

	var principal RuntimeSessionPrincipal
	err := r.pool.QueryRow(
		ctx,
		statement,
		params.RuntimeSessionID,
		params.NodeID,
		params.AgentID,
		params.CredentialID,
		params.CertificateSerial,
		params.PublicKeyThumbprintSHA256,
		params.CoreInstanceID,
	).Scan(
		&principal.RuntimeSessionID,
		&principal.NodeID,
		&principal.AgentID,
		&principal.CredentialID,
		&principal.WorkerID,
		&principal.SessionEpoch,
		&principal.RuntimeContractDigest,
		&principal.CoreInstanceID,
		&principal.DeviceCertificateSerial,
		&principal.DevicePublicKeyThumbprintSHA256,
		&principal.AttachmentID,
		&principal.Status,
		&principal.DatabaseTime,
	)
	return principal, err
}

func (r *postgresRuntimeSessionRepository) ResolveRuntimeWorkerSessionPrincipal(
	ctx context.Context,
	params runtimeWorkerSessionResolveParams,
) (RuntimeSessionPrincipal, error) {
	row, err := r.queries.ResolveRuntimeWorkerSessionPrincipal(ctx, db.ResolveRuntimeWorkerSessionPrincipalParams{
		NodeID:                    params.NodeID,
		AgentID:                   params.AgentID,
		CredentialID:              params.CredentialID,
		WorkerID:                  params.WorkerID,
		DeviceCertificateSerial:   params.CertificateSerial,
		DevicePublicKeyThumbprint: params.PublicKeyThumbprintSHA256,
		CoreInstanceID:            params.CoreInstanceID,
	})
	if err != nil {
		return RuntimeSessionPrincipal{}, err
	}
	return RuntimeSessionPrincipal{
		RuntimeSessionID:                row.RuntimeSessionID,
		NodeID:                          row.NodeID,
		AgentID:                         row.AgentID,
		CredentialID:                    row.CredentialID,
		WorkerID:                        row.WorkerID,
		SessionEpoch:                    row.SessionEpoch,
		RuntimeContractDigest:           row.RuntimeContractDigest,
		CoreInstanceID:                  row.AttachedCoreInstanceID,
		DeviceCertificateSerial:         row.DeviceCertificateSerial,
		DevicePublicKeyThumbprintSHA256: row.DevicePublicKeyThumbprint,
		AttachmentID:                    row.AttachmentID,
		Status:                          row.Status,
		DatabaseTime:                    row.DatabaseNow,
	}, nil
}

func (r *postgresRuntimeSessionRepository) ListStaleRuntimeSessionCandidates(
	ctx context.Context,
	heartbeatTTL time.Duration,
	limit int,
) ([]db.RuntimeSession, error) {
	if r == nil || r.queries == nil {
		return nil, fmt.Errorf("runtime session repository is not configured")
	}
	return r.queries.ListStaleRuntimeSessionCandidates(ctx, db.ListStaleRuntimeSessionCandidatesParams{
		HeartbeatTTLMS: heartbeatTTL.Milliseconds(),
		CandidateLimit: int32(limit),
	})
}

type postgresRuntimeSessionTransaction struct {
	tx      pgx.Tx
	queries *db.Queries
}

func (t *postgresRuntimeSessionTransaction) RequireRuntimeClusterOperation(ctx context.Context, operation RuntimeClusterOperation) error {
	return RequireRuntimeClusterOperation(ctx, t.tx, operation)
}

func (t *postgresRuntimeSessionTransaction) LockSessionIdentity(ctx context.Context, sessionID uuid.UUID) error {
	_, err := t.tx.Exec(
		ctx,
		"SELECT pg_advisory_xact_lock(hashtextextended('runtime-session:' || $1::text, 0))",
		sessionID,
	)
	return err
}

func (t *postgresRuntimeSessionTransaction) GetRuntimeSessionForUpdate(ctx context.Context, id uuid.UUID) (db.RuntimeSession, error) {
	return t.queries.GetRuntimeSessionForUpdate(ctx, id)
}

func (t *postgresRuntimeSessionTransaction) HasNewerRuntimeSessionGeneration(
	ctx context.Context,
	params runtimeSessionGenerationParams,
) (bool, error) {
	var newer bool
	err := t.tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM runtime_sessions
    WHERE node_id = $1
      AND agent_id = $2
      AND worker_id = $3
      AND (
          session_epoch > $4
          OR (session_epoch = $4 AND runtime_session_id <> $5)
      )
)`, params.NodeID, params.AgentID, params.WorkerID, params.SessionEpoch,
		params.RuntimeSessionID).Scan(&newer)
	return newer, err
}

func (t *postgresRuntimeSessionTransaction) RetireOfflineRuntimeSessionsForGenerationSwitch(
	ctx context.Context,
	nodeID uuid.UUID,
	targetDigest string,
) (int64, error) {
	tag, err := t.tx.Exec(ctx, `
UPDATE runtime_sessions
SET status = 'closed', updated_at = clock_timestamp()
WHERE node_id = $1
  AND status = 'offline'
  AND runtime_contract_digest <> $2`, nodeID, targetDigest)
	return tag.RowsAffected(), err
}

func (t *postgresRuntimeSessionTransaction) LockRuntimeSessionsForPrincipalRevocation(ctx context.Context, params db.LockRuntimeSessionsForPrincipalRevocationParams) ([]db.LockRuntimeSessionsForPrincipalRevocationRow, error) {
	return t.queries.LockRuntimeSessionsForPrincipalRevocation(ctx, params)
}

func (t *postgresRuntimeSessionTransaction) LockRuntimeNodesForPrincipalRevocation(ctx context.Context, ids []uuid.UUID) ([]uuid.UUID, error) {
	return t.queries.LockRuntimeNodesForPrincipalRevocation(ctx, ids)
}

func (t *postgresRuntimeSessionTransaction) LockAgentTokensForPrincipalRevocation(ctx context.Context, ids []uuid.UUID) ([]uuid.UUID, error) {
	return t.queries.LockAgentTokensForPrincipalRevocation(ctx, ids)
}

func (t *postgresRuntimeSessionTransaction) LockActiveRuntimeSessionAttachmentsForPrincipalRevocation(ctx context.Context, ids []uuid.UUID) ([]uuid.UUID, error) {
	return t.queries.LockActiveRuntimeSessionAttachmentsForPrincipalRevocation(ctx, ids)
}

func (t *postgresRuntimeSessionTransaction) GetRuntimeNode(ctx context.Context, id uuid.UUID) (db.RuntimeNode, error) {
	return t.queries.GetRuntimeNode(ctx, id)
}

func (t *postgresRuntimeSessionTransaction) HeartbeatRuntimeNode(ctx context.Context, params db.HeartbeatRuntimeNodeParams) (db.RuntimeNode, error) {
	return t.queries.HeartbeatRuntimeNode(ctx, params)
}

func (t *postgresRuntimeSessionTransaction) ListActiveRuntimeSessionsByNode(ctx context.Context, id uuid.UUID) ([]db.RuntimeSession, error) {
	return t.queries.ListActiveRuntimeSessionsByNode(ctx, id)
}

func (t *postgresRuntimeSessionTransaction) CreateRuntimeSession(ctx context.Context, params db.CreateRuntimeSessionParams) (db.RuntimeSession, error) {
	return t.queries.CreateRuntimeSession(ctx, params)
}

func (t *postgresRuntimeSessionTransaction) ClaimRuntimeSessionForCore(ctx context.Context, params db.ClaimRuntimeSessionForCoreParams) (db.RuntimeSession, error) {
	return t.queries.ClaimRuntimeSessionForCore(ctx, params)
}

func (t *postgresRuntimeSessionTransaction) HeartbeatRuntimeSession(ctx context.Context, params db.HeartbeatRuntimeSessionParams) (db.RuntimeSession, error) {
	return t.queries.HeartbeatRuntimeSession(ctx, params)
}

func (t *postgresRuntimeSessionTransaction) DrainRuntimeSession(
	ctx context.Context,
	sessionID uuid.UUID,
	coreInstanceID uuid.UUID,
	payload RuntimeDrainPayload,
) (runtimeSessionDrainRecord, error) {
	var record runtimeSessionDrainRecord
	err := t.tx.QueryRow(ctx, `
UPDATE runtime_sessions s
SET drain_requested_at = COALESCE(s.drain_requested_at, clock_timestamp()),
    drain_deadline_at = COALESCE(s.drain_deadline_at, $3),
    drain_reason_code = COALESCE(s.drain_reason_code, $4),
    resume_capacity = COALESCE(s.resume_capacity, s.capacity),
    status = 'draining',
    capacity = 0,
    updated_at = clock_timestamp()
WHERE s.runtime_session_id = $1
  AND s.attached_core_instance_id = $2
  AND s.status IN ('active', 'draining')
  AND EXISTS (
      SELECT 1
      FROM runtime_nodes n
      JOIN agent_tokens token
        ON token.id = s.credential_id
       AND token.agent_id = s.agent_id
      JOIN runtime_session_attachments attachment
        ON attachment.runtime_session_id = s.runtime_session_id
       AND attachment.core_instance_id = s.attached_core_instance_id
       AND attachment.detached_at IS NULL
      WHERE n.node_id = s.node_id
        AND n.status IN ('active', 'draining')
        AND n.revoked_at IS NULL
        AND n.protocol_version = $5
        AND n.runtime_contract_id = $6
        AND n.runtime_contract_digest = $7
        AND n.features @> ARRAY['session_drain']::text[]
        AND s.protocol_version = $5
        AND s.runtime_contract_id = $6
        AND s.runtime_contract_digest = $7
        AND s.features @> ARRAY['session_drain']::text[]
        AND n.protocol_version = s.protocol_version
        AND n.runtime_contract_id = s.runtime_contract_id
        AND n.runtime_contract_digest = s.runtime_contract_digest
        AND token.status = 'active_runtime'
        AND token.revoked_at IS NULL
        AND token.scopes @> ARRAY['agent:pull']::text[]
        AND (token.expires_at IS NULL OR token.expires_at > clock_timestamp())
  )
RETURNING s.runtime_session_id, s.node_id, s.agent_id, s.credential_id,
          s.worker_id, s.session_epoch, s.device_certificate_serial,
          s.node_version, s.protocol_version, s.runtime_contract_id,
          s.runtime_contract_digest, s.features, s.capacity, s.inflight,
          s.status, s.attached_core_instance_id, s.connected_at,
          s.heartbeat_at, s.disconnected_at, s.created_at, s.updated_at,
          s.drain_deadline_at, s.drain_reason_code, s.drain_requested_at`,
		sessionID, coreInstanceID, payload.DeadlineAt, payload.ReasonCode,
		RuntimeProtocolVersion, RuntimeContractID, RuntimeContractDigest,
	).Scan(
		&record.Session.RuntimeSessionID, &record.Session.NodeID,
		&record.Session.AgentID, &record.Session.CredentialID,
		&record.Session.WorkerID, &record.Session.SessionEpoch,
		&record.Session.DeviceCertificateSerial, &record.Session.NodeVersion,
		&record.Session.ProtocolVersion, &record.Session.RuntimeContractID,
		&record.Session.RuntimeContractDigest, &record.Session.Features,
		&record.Session.Capacity, &record.Session.Inflight, &record.Session.Status,
		&record.Session.AttachedCoreInstanceID, &record.Session.ConnectedAt,
		&record.Session.HeartbeatAt, &record.Session.DisconnectedAt,
		&record.Session.CreatedAt, &record.Session.UpdatedAt,
		&record.DeadlineAt, &record.ReasonCode, &record.RequestedAt,
	)
	return record, err
}

func (t *postgresRuntimeSessionTransaction) GetActiveRuntimeSessionAttachment(ctx context.Context, id uuid.UUID) (db.RuntimeSessionAttachment, error) {
	return t.queries.GetActiveRuntimeSessionAttachment(ctx, id)
}

func (t *postgresRuntimeSessionTransaction) ListRuntimeSessionAttachments(ctx context.Context, params db.ListRuntimeSessionAttachmentsParams) ([]db.RuntimeSessionAttachment, error) {
	return t.queries.ListRuntimeSessionAttachments(ctx, params)
}

func (t *postgresRuntimeSessionTransaction) CreateRuntimeSessionAttachment(ctx context.Context, params db.CreateRuntimeSessionAttachmentParams) (db.RuntimeSessionAttachment, error) {
	return t.queries.CreateRuntimeSessionAttachment(ctx, params)
}

func (t *postgresRuntimeSessionTransaction) CloseRuntimeSessionAttachment(ctx context.Context, params db.CloseRuntimeSessionAttachmentParams) (db.RuntimeSessionAttachment, error) {
	return t.queries.CloseRuntimeSessionAttachment(ctx, params)
}

func (t *postgresRuntimeSessionTransaction) CloseRuntimeSession(ctx context.Context, params db.CloseRuntimeSessionParams) (db.RuntimeSession, error) {
	return t.queries.CloseRuntimeSession(ctx, params)
}

func (t *postgresRuntimeSessionTransaction) CloseStaleRuntimeSession(ctx context.Context, params db.CloseStaleRuntimeSessionParams) (db.RuntimeSession, error) {
	return t.queries.CloseStaleRuntimeSession(ctx, params)
}

type runtimeNodeCredentialQueries interface {
	GetRuntimeNodeByCertificate(context.Context, db.GetRuntimeNodeByCertificateParams) (db.RuntimeNode, error)
}

type runtimeDatabaseClock interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}
