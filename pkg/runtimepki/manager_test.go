package runtimepki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestIssueClientCertificateUsesShortLivedRotatingLeaf(t *testing.T) {
	root, client, server, err := createAuthorityHierarchy(time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	manager := &Manager{root: root, client: client, server: server}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		t.Fatal(err)
	}
	issued, err := manager.IssueClientCertificate(csr, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	if lifetime := issued.NotAfter.Sub(issued.NotBefore); lifetime < 24*time.Hour || lifetime > 24*time.Hour+6*time.Minute {
		t.Fatalf("certificate lifetime = %s", lifetime)
	}
	if renewIn := issued.RenewAfter.Sub(time.Now()); renewIn < 12*time.Hour-10*time.Second || renewIn > 16*time.Hour+10*time.Second {
		t.Fatalf("renewal window = %s", renewIn)
	}
	if issued.CertificateChainPEM == "" || issued.TrustBundlePEM == "" || len(issued.Serial) < 2 {
		t.Fatalf("issued credential is incomplete: %#v", issued)
	}
}
