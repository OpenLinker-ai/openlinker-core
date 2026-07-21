package runtimepki

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/OpenLinker-ai/openlinker-core/pkg/config"
)

const (
	ClientCertificateLifetime = 24 * time.Hour
	clientRenewMinimum        = 12 * time.Hour
	clientRenewJitter         = 4 * time.Hour
	serverCertificateLifetime = 7 * 24 * time.Hour
	authorityLifetime         = 5 * 365 * 24 * time.Hour
	rootAuthorityLifetime     = 10 * 365 * 24 * time.Hour
)

const runtimeNodeURIPrefix = "urn:openlinker:runtime-node:"

type authority struct {
	id   string
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

// Manager owns the default self-managed Runtime trust hierarchy. CA private
// keys are encrypted before persistence and are shared through PostgreSQL, so
// HA replicas do not require operators to distribute CA files.
type Manager struct {
	pool      *pgxpool.Pool
	masterKey [32]byte
	root      authority
	client    authority
	server    authority

	serverNames []string
	serverIPs   []net.IP
	mu          sync.RWMutex
	serverLeaf  tls.Certificate
	serverUntil time.Time
}

type ClientCertificate struct {
	CertificatePEM      string
	CertificateChainPEM string
	TrustBundlePEM      string
	Serial              string
	FingerprintSHA256   string
	PublicKeySHA256     string
	NotBefore           time.Time
	NotAfter            time.Time
	RenewAfter          time.Time
}

func NewManager(ctx context.Context, pool *pgxpool.Pool, cfg *config.Config) (*Manager, error) {
	if pool == nil || cfg == nil {
		return nil, errors.New("runtime PKI requires database and configuration")
	}
	secret := strings.TrimSpace(cfg.RuntimePKIMasterSecret)
	if secret == "" {
		// JWT_SECRET already has a production minimum and is present in every
		// Core replica. Domain separation prevents protocol-key reuse while
		// keeping the default deployment zero-touch.
		secret = cfg.JWTSecret
	}
	if strings.TrimSpace(secret) == "" {
		return nil, errors.New("runtime PKI master secret is unavailable")
	}
	m := &Manager{pool: pool}
	m.masterKey = sha256.Sum256([]byte("openlinker/runtime-pki/master/v1\x00" + secret))
	if err := m.loadOrCreateAuthorities(ctx); err != nil {
		return nil, err
	}
	if cfg.RuntimeMTLSEnabled {
		if err := m.configureServerNames(cfg); err != nil {
			return nil, err
		}
		if err := m.rotateServerCertificate(time.Now()); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *Manager) Start(ctx context.Context) {
	if m == nil || (len(m.serverNames) == 0 && len(m.serverIPs) == 0) {
		return
	}
	go func() {
		ticker := time.NewTicker(6 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				m.mu.RLock()
				until := m.serverUntil
				m.mu.RUnlock()
				if until.Sub(now) < 72*time.Hour {
					_ = m.rotateServerCertificate(now)
				}
			}
		}
	}()
}

func (m *Manager) ServerTLSConfig() (*tls.Config, error) {
	if m == nil || m.client.cert == nil {
		return nil, errors.New("runtime PKI is unavailable")
	}
	clientCAs := x509.NewCertPool()
	clientCAs.AddCert(m.client.cert)
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  clientCAs,
		NextProtos: []string{"http/1.1"},
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			m.mu.RLock()
			defer m.mu.RUnlock()
			if len(m.serverLeaf.Certificate) == 0 {
				return nil, errors.New("runtime server certificate is unavailable")
			}
			certificate := m.serverLeaf
			return &certificate, nil
		},
	}, nil
}

func (m *Manager) TrustBundlePEM() string {
	if m == nil {
		return ""
	}
	return string(m.root.pem)
}

func (m *Manager) IssueClientCertificate(csr *x509.CertificateRequest, nodeID uuid.UUID) (ClientCertificate, error) {
	if m == nil || csr == nil || nodeID == uuid.Nil || m.client.cert == nil || m.client.key == nil {
		return ClientCertificate{}, errors.New("runtime client certificate issuer is unavailable")
	}
	if err := csr.CheckSignature(); err != nil {
		return ClientCertificate{}, fmt.Errorf("verify runtime CSR: %w", err)
	}
	if err := validateClientPublicKey(csr.PublicKey); err != nil {
		return ClientCertificate{}, err
	}
	// PostgreSQL persists timestamptz with microsecond precision, while X.509
	// validity encodes whole seconds. Normalize before returning the first
	// response so a retry can replay byte-for-byte equivalent JSON from the
	// committed inventory row.
	now := time.Now().UTC().Truncate(time.Microsecond)
	serialNumber, err := randomSerial()
	if err != nil {
		return ClientCertificate{}, err
	}
	nodeURI, _ := url.Parse(runtimeNodeURIPrefix + nodeID.String())
	notBefore := now.Add(-5 * time.Minute).Truncate(time.Second)
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{CommonName: "runtime-node-" + nodeID.String()},
		NotBefore:    notBefore,
		NotAfter:     notBefore.Add(ClientCertificateLifetime),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:         []*url.URL{nodeURI},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, m.client.cert, csr.PublicKey, m.client.key)
	if err != nil {
		return ClientCertificate{}, fmt.Errorf("issue runtime client certificate: %w", err)
	}
	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	chainPEM := append(append([]byte(nil), leafPEM...), m.client.pem...)
	fingerprint := sha256.Sum256(der)
	publicKey := sha256.Sum256(csr.RawSubjectPublicKeyInfo)
	jitter, err := randomDuration(clientRenewJitter)
	if err != nil {
		return ClientCertificate{}, err
	}
	return ClientCertificate{
		CertificatePEM:      string(leafPEM),
		CertificateChainPEM: string(chainPEM),
		TrustBundlePEM:      string(m.root.pem),
		Serial:              strings.ToLower(serialNumber.Text(16)),
		FingerprintSHA256:   hex.EncodeToString(fingerprint[:]),
		PublicKeySHA256:     hex.EncodeToString(publicKey[:]),
		NotBefore:           template.NotBefore,
		NotAfter:            template.NotAfter,
		RenewAfter:          now.Add(clientRenewMinimum + jitter).Truncate(time.Microsecond),
	}, nil
}

func (m *Manager) configureServerNames(cfg *config.Config) error {
	raw := strings.TrimSpace(cfg.RuntimeMTLSAPIURL)
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Hostname() == "" {
		return errors.New("runtime mTLS public origin is invalid")
	}
	host := parsed.Hostname()
	if ip := net.ParseIP(host); ip != nil {
		m.serverIPs = []net.IP{ip}
	} else {
		m.serverNames = []string{host}
	}
	if strings.EqualFold(host, "localhost") {
		m.serverIPs = append(m.serverIPs, net.ParseIP("127.0.0.1"), net.ParseIP("::1"))
	}
	return nil
}

func (m *Manager) rotateServerCertificate(now time.Time) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serialNumber, err := randomSerial()
	if err != nil {
		return err
	}
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{CommonName: "OpenLinker Runtime"},
		NotBefore:    now.UTC().Add(-5 * time.Minute),
		NotAfter:     now.UTC().Add(serverCertificateLifetime),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     append([]string(nil), m.serverNames...),
		IPAddresses:  append([]net.IP(nil), m.serverIPs...),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, m.server.cert, &key.PublicKey, m.server.key)
	if err != nil {
		return fmt.Errorf("issue runtime server certificate: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	certPEM := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), m.server.pem...)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	leaf, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return fmt.Errorf("load issued runtime server certificate: %w", err)
	}
	m.mu.Lock()
	m.serverLeaf = leaf
	m.serverUntil = template.NotAfter
	m.mu.Unlock()
	return nil
}

func (m *Manager) loadOrCreateAuthorities(ctx context.Context) error {
	tx, err := m.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin runtime PKI initialization: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('openlinker.runtime-pki.v1'))`); err != nil {
		return fmt.Errorf("lock runtime PKI initialization: %w", err)
	}
	rows, err := tx.Query(ctx, `
SELECT authority_id, certificate_pem, encrypted_private_key
FROM runtime_pki_authorities
ORDER BY authority_id`)
	if err != nil {
		return fmt.Errorf("load runtime PKI authorities: %w", err)
	}
	loaded := map[string]authority{}
	for rows.Next() {
		var id, certificatePEM string
		var encrypted []byte
		if err = rows.Scan(&id, &certificatePEM, &encrypted); err != nil {
			rows.Close()
			return err
		}
		a, parseErr := m.parseAuthority(id, []byte(certificatePEM), encrypted)
		if parseErr != nil {
			rows.Close()
			return parseErr
		}
		loaded[id] = a
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	if len(loaded) == 0 {
		root, client, server, createErr := createAuthorityHierarchy(time.Now().UTC())
		if createErr != nil {
			return createErr
		}
		for _, a := range []authority{root, client, server} {
			encrypted, encryptErr := m.encryptPrivateKey(a)
			if encryptErr != nil {
				return encryptErr
			}
			if _, err = tx.Exec(ctx, `
INSERT INTO runtime_pki_authorities (
    authority_id, certificate_pem, encrypted_private_key, not_before, not_after
) VALUES ($1, $2, $3, $4, $5)`, a.id, string(a.pem), encrypted, a.cert.NotBefore, a.cert.NotAfter); err != nil {
				return fmt.Errorf("persist runtime PKI authority %s: %w", a.id, err)
			}
			loaded[a.id] = a
		}
	}
	if len(loaded) != 3 || loaded["root"].cert == nil || loaded["client-intermediate"].cert == nil || loaded["server-intermediate"].cert == nil {
		return errors.New("runtime PKI authority set is incomplete")
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit runtime PKI initialization: %w", err)
	}
	m.root = loaded["root"]
	m.client = loaded["client-intermediate"]
	m.server = loaded["server-intermediate"]
	return nil
}

func createAuthorityHierarchy(now time.Time) (authority, authority, authority, error) {
	root, err := newAuthority("root", "OpenLinker Runtime Root CA", now, rootAuthorityLifetime, nil)
	if err != nil {
		return authority{}, authority{}, authority{}, err
	}
	client, err := newAuthority("client-intermediate", "OpenLinker Runtime Client CA", now, authorityLifetime, &root)
	if err != nil {
		return authority{}, authority{}, authority{}, err
	}
	server, err := newAuthority("server-intermediate", "OpenLinker Runtime Server CA", now, authorityLifetime, &root)
	return root, client, server, err
}

func newAuthority(id, commonName string, now time.Time, lifetime time.Duration, parent *authority) (authority, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return authority{}, err
	}
	serialNumber, err := randomSerial()
	if err != nil {
		return authority{}, err
	}
	template := &x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(lifetime),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		MaxPathLen:            0,
		MaxPathLenZero:        true,
	}
	signerCert := template
	signerKey := key
	if parent == nil {
		template.MaxPathLen = 1
		template.MaxPathLenZero = false
	} else {
		signerCert = parent.cert
		signerKey = parent.key
	}
	der, err := x509.CreateCertificate(rand.Reader, template, signerCert, &key.PublicKey, signerKey)
	if err != nil {
		return authority{}, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return authority{}, err
	}
	return authority{id: id, cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, nil
}

func (m *Manager) encryptPrivateKey(a authority) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(a.key)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(m.masterKey[:])
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, der, []byte(a.id)), nil
}

func (m *Manager) parseAuthority(id string, certificatePEM, encrypted []byte) (authority, error) {
	block, _ := pem.Decode(certificatePEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return authority{}, fmt.Errorf("runtime PKI authority %s certificate is invalid", id)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil || !cert.IsCA {
		return authority{}, fmt.Errorf("runtime PKI authority %s certificate is invalid", id)
	}
	aesBlock, err := aes.NewCipher(m.masterKey[:])
	if err != nil {
		return authority{}, err
	}
	gcm, err := cipher.NewGCM(aesBlock)
	if err != nil || len(encrypted) < gcm.NonceSize() {
		return authority{}, fmt.Errorf("runtime PKI authority %s key is invalid", id)
	}
	der, err := gcm.Open(nil, encrypted[:gcm.NonceSize()], encrypted[gcm.NonceSize():], []byte(id))
	if err != nil {
		return authority{}, fmt.Errorf("decrypt runtime PKI authority %s: %w", id, err)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(der)
	if err != nil {
		return authority{}, fmt.Errorf("parse runtime PKI authority %s key: %w", id, err)
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok {
		return authority{}, fmt.Errorf("runtime PKI authority %s key type is unsupported", id)
	}
	return authority{id: id, cert: cert, key: key, pem: append([]byte(nil), certificatePEM...)}, nil
}

func validateClientPublicKey(publicKey any) error {
	switch key := publicKey.(type) {
	case *ecdsa.PublicKey:
		if key.Curve != elliptic.P256() && key.Curve != elliptic.P384() {
			return errors.New("runtime CSR must use P-256 or P-384 ECDSA")
		}
	default:
		return errors.New("runtime CSR must use an ECDSA public key")
	}
	return nil
}

func randomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, err
	}
	if serialNumber.Sign() == 0 {
		serialNumber.SetInt64(1)
	}
	return serialNumber, nil
}

func randomDuration(limit time.Duration) (time.Duration, error) {
	value, err := rand.Int(rand.Reader, big.NewInt(int64(limit)))
	if err != nil {
		return 0, err
	}
	return time.Duration(value.Int64()), nil
}
