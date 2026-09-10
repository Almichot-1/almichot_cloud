package pki

import (
	"crypto/x509"
	"testing"
	"time"
)

func TestInternalCA_GenerationAndWorkerCertIssuance(t *testing.T) {
	ca, err := NewCertificateAuthority("Nebula Test Cluster CA", 24*time.Hour)
	if err != nil {
		t.Fatalf("failed to create CA: %v", err)
	}

	if len(ca.CACertPEM()) == 0 {
		t.Fatalf("expected non-empty CA certificate PEM")
	}

	ttl := 1 * time.Hour
	tlsCert, certPEM, keyPEM, err := ca.IssueWorkerCertificate("worker-node-01", ttl)
	if err != nil {
		t.Fatalf("failed to issue worker cert: %v", err)
	}
	if tlsCert == nil || len(certPEM) == 0 || len(keyPEM) == 0 {
		t.Fatalf("expected valid TLS certificate and PEMs")
	}

	cert, err := ParseCertificateFromPEM(certPEM)
	if err != nil {
		t.Fatalf("failed to parse issued certificate: %v", err)
	}

	// 1. Verify Common Name and SANs
	if cert.Subject.CommonName != "worker-node-01" {
		t.Errorf("expected CN worker-node-01, got %s", cert.Subject.CommonName)
	}

	var hasDNS bool
	for _, dns := range cert.DNSNames {
		if dns == "worker-node-01" {
			hasDNS = true
			break
		}
	}
	if !hasDNS {
		t.Errorf("expected SAN DNS worker-node-01")
	}

	// 2. Verify Expiry Math: NotAfter - NotBefore ≈ ttl + 1 minute (clock skew allowance)
	lifetime := cert.NotAfter.Sub(cert.NotBefore)
	expectedLifetime := ttl + 1*time.Minute
	diff := lifetime - expectedLifetime
	if diff < -2*time.Second || diff > 2*time.Second {
		t.Errorf("expiry math incorrect: expected lifetime ~%v, got %v", expectedLifetime, lifetime)
	}

	// 3. Verify trust chain against CA pool
	verifyOpts := x509.VerifyOptions{
		Roots: ca.CertPool(),
		KeyUsages: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
		},
	}
	if _, err := cert.Verify(verifyOpts); err != nil {
		t.Fatalf("certificate verification against CA pool failed: %v", err)
	}
}

func TestInternalCA_ExpiryAndRenewalMath(t *testing.T) {
	ca, _ := NewCertificateAuthority("Nebula Test CA", 24*time.Hour)
	_, certPEM, _, _ := ca.IssueWorkerCertificate("worker-renew-01", 10*time.Minute)
	cert, _ := ParseCertificateFromPEM(certPEM)

	// Fresh cert (< 75% elapsed) should NOT require renewal
	if ShouldRenew(cert, 0.75) {
		t.Errorf("expected fresh certificate not to trigger renewal at 75%% threshold")
	}

	// Simulated expired cert should require renewal
	expiredCert, err := ca.IssueExpiredCertificate("worker-expired-01")
	if err != nil {
		t.Fatalf("failed to issue expired cert: %v", err)
	}
	parsedExpired, _ := x509.ParseCertificate(expiredCert.Certificate[0])
	if !ShouldRenew(parsedExpired, 0.75) {
		t.Errorf("expected expired certificate to trigger renewal")
	}
}

func TestInternalCA_UnavailableFailureMode(t *testing.T) {
	ca, _ := NewCertificateAuthority("Nebula Test CA", 24*time.Hour)
	ca.SetAvailable(false) // simulate CA outage

	_, _, _, err := ca.IssueWorkerCertificate("worker-fail", 1*time.Hour)
	if err == nil {
		t.Fatalf("expected error when CA is unavailable")
	}
	if err != ErrCAUnavailable {
		t.Fatalf("expected ErrCAUnavailable, got: %v", err)
	}
}
