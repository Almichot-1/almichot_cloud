package pki

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"sync"
	"time"
)

var (
	// ErrCAUnavailable is returned when the internal CA service is unavailable.
	ErrCAUnavailable = errors.New("internal certificate authority is unavailable")
	// ErrInvalidTTL is returned when an invalid duration is requested for a certificate.
	ErrInvalidTTL = errors.New("certificate TTL must be positive")
)

// CertificateAuthority issues and manages short-lived x509 certificates for cluster nodes (§21.1, Phase 13).
type CertificateAuthority struct {
	mu        sync.RWMutex
	org       string
	caCert    *x509.Certificate
	caKey     *ecdsa.PrivateKey
	caCertPEM []byte
	caKeyPEM  []byte
	certPool  *x509.CertPool
	available bool
}

// NewCertificateAuthority creates and initializes an internal CA with a self-signed root.
func NewCertificateAuthority(org string, caTTL time.Duration) (*CertificateAuthority, error) {
	if org == "" {
		org = "Nebula Internal CA"
	}
	if caTTL <= 0 {
		caTTL = 10 * 365 * 24 * time.Hour // 10 years default for root CA
	}

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate CA private key: %w", err)
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, fmt.Errorf("generate CA serial number: %w", err)
	}

	now := time.Now().UTC()
	caTemplate := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   org,
			Organization: []string{org},
		},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(caTTL),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("create CA certificate: %w", err)
	}

	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, fmt.Errorf("parse CA certificate: %w", err)
	}

	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	keyDER, err := x509.MarshalECPrivateKey(caKey)
	if err != nil {
		return nil, fmt.Errorf("marshal CA private key: %w", err)
	}
	caKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	certPool := x509.NewCertPool()
	certPool.AddCert(caCert)

	return &CertificateAuthority{
		org:       org,
		caCert:    caCert,
		caKey:     caKey,
		caCertPEM: caCertPEM,
		caKeyPEM:  caKeyPEM,
		certPool:  certPool,
		available: true,
	}, nil
}

// SetAvailable configures whether the CA accepts issuance requests (used for failure testing).
func (ca *CertificateAuthority) SetAvailable(avail bool) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	ca.available = avail
}

// CACertPEM returns the PEM-encoded root CA certificate.
func (ca *CertificateAuthority) CACertPEM() []byte {
	ca.mu.RLock()
	defer ca.mu.RUnlock()
	out := make([]byte, len(ca.caCertPEM))
	copy(out, ca.caCertPEM)
	return out
}

// CertPool returns an x509.CertPool containing the root CA.
func (ca *CertificateAuthority) CertPool() *x509.CertPool {
	ca.mu.RLock()
	defer ca.mu.RUnlock()
	pool := x509.NewCertPool()
	pool.AddCert(ca.caCert)
	return pool
}

// IssueWorkerCertificate issues a short-lived client/server mTLS certificate for a Worker agent.
func (ca *CertificateAuthority) IssueWorkerCertificate(workerKey string, ttl time.Duration) (*tls.Certificate, []byte, []byte, error) {
	return ca.issueCertificate(workerKey, ttl, false, []string{workerKey, "localhost", "127.0.0.1"})
}

// IssueServerCertificate issues a short-lived server certificate for Control Plane gRPC endpoints.
func (ca *CertificateAuthority) IssueServerCertificate(hostname string, ttl time.Duration) (*tls.Certificate, []byte, []byte, error) {
	return ca.issueCertificate(hostname, ttl, true, []string{hostname, "localhost", "127.0.0.1"})
}

func (ca *CertificateAuthority) issueCertificate(cn string, ttl time.Duration, isServer bool, sans []string) (*tls.Certificate, []byte, []byte, error) {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	if !ca.available {
		return nil, nil, nil, ErrCAUnavailable
	}
	if ttl <= 0 {
		return nil, nil, nil, ErrInvalidTTL
	}

	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generate private key: %w", err)
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generate serial number: %w", err)
	}

	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   cn,
			Organization: []string{ca.org},
		},
		NotBefore: now.Add(-1 * time.Minute), // clock skew buffer
		NotAfter:  now.Add(ttl),
		KeyUsage:  x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
			x509.ExtKeyUsageServerAuth,
		},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}

	for _, s := range sans {
		if ip := net.ParseIP(s); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, s)
		}
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, ca.caCert, &privKey.PublicKey, ca.caKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(privKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal private key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parse TLS key pair: %w", err)
	}

	return &tlsCert, certPEM, keyPEM, nil
}

// ShouldRenew checks if a certificate has reached the given fraction (e.g. 0.75 for 75%) of its validity window.
func ShouldRenew(cert *x509.Certificate, threshold float64) bool {
	if cert == nil {
		return true
	}
	now := time.Now().UTC()
	if now.After(cert.NotAfter) {
		return true
	}
	totalLifetime := cert.NotAfter.Sub(cert.NotBefore)
	if totalLifetime <= 0 {
		return true
	}
	elapsed := now.Sub(cert.NotBefore)
	return float64(elapsed)/float64(totalLifetime) >= threshold
}

// IssueUntrustedCertificate creates a certificate signed by an untrusted rogue CA for testing Gate G-35.
func IssueUntrustedCertificate(cn string, ttl time.Duration) (*tls.Certificate, []byte, []byte, error) {
	rogueCA, err := NewCertificateAuthority("Rogue Untrusted CA", 1*time.Hour)
	if err != nil {
		return nil, nil, nil, err
	}
	return rogueCA.IssueWorkerCertificate(cn, ttl)
}

// IssueExpiredCertificate creates a certificate that is already expired for testing Gate G-35.
func (ca *CertificateAuthority) IssueExpiredCertificate(cn string) (*tls.Certificate, error) {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, _ := rand.Int(rand.Reader, serialNumberLimit)

	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: cn,
		},
		NotBefore: now.Add(-10 * time.Hour),
		NotAfter:  now.Add(-1 * time.Hour), // Expired 1 hour ago
		KeyUsage:  x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
			x509.ExtKeyUsageServerAuth,
		},
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, ca.caCert, &privKey.PublicKey, ca.caKey)
	if err != nil {
		return nil, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, _ := x509.MarshalECPrivateKey(privKey)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	return &tlsCert, nil
}

// ParseCertificateFromPEM parses the first certificate found in the PEM data.
func ParseCertificateFromPEM(certPEM []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, errors.New("failed to decode PEM block")
	}
	return x509.ParseCertificate(block.Bytes)
}

// Ensure certificate pool has the given CA certificate.
func HasCertInPool(pool *x509.CertPool, cert *x509.Certificate) bool {
	if pool == nil || cert == nil {
		return false
	}
	// Verify can build chain with itself
	opts := x509.VerifyOptions{
		Roots: pool,
	}
	_, err := cert.Verify(opts)
	return err == nil
}

// BytesEqual securely checks byte slice equality.
func BytesEqual(a, b []byte) bool {
	return bytes.Equal(a, b)
}
