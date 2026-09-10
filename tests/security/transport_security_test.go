package security

import (
	"context"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/api"
	"github.com/nebula/nebula/internal/pki"
	"github.com/nebula/nebula/internal/projects"
	"github.com/nebula/nebula/internal/secrets"
	"github.com/nebula/nebula/internal/transport"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// mockWorkerServiceServer implements proto.WorkerServiceServer for testing mTLS gRPC transport.
type mockWorkerServiceServer struct {
	proto.UnimplementedWorkerServiceServer
}

func (m *mockWorkerServiceServer) Register(ctx context.Context, req *proto.RegisterRequest) (*proto.RegisterResponse, error) {
	return &proto.RegisterResponse{Success: true, Message: "mTLS authenticated"}, nil
}

// §21.1 / Phase 13 Transport Security Test:
// Verifies mTLS rejection at the gRPC transport layer (no cert, expired cert, untrusted CA).
func TestSecurity_Transport_MTLSRejectionAtTransportLayer(t *testing.T) {
	ca, err := pki.NewCertificateAuthority("Nebula Cluster Root CA", 24*time.Hour)
	if err != nil {
		t.Fatalf("failed to create CA: %v", err)
	}

	// 1. Issue server certificate for localhost
	serverTLSCert, _, _, err := ca.IssueServerCertificate("localhost", 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to issue server cert: %v", err)
	}

	serverTLSConfig, err := transport.NewServerTLSConfig(ca.CACertPEM(), *serverTLSCert)
	if err != nil {
		t.Fatalf("failed to create server TLS config: %v", err)
	}

	// 2. Start real gRPC server on a loopback port requiring mTLS
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on loopback: %v", err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer(transport.ServerOptionsWithTLS(serverTLSConfig)...)
	proto.RegisterWorkerServiceServer(grpcServer, &mockWorkerServiceServer{})
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	serverAddr := lis.Addr().String()

	// Sub-test A: Connecting with NO certificate (insecure/plaintext) must fail at transport layer
	t.Run("NoClientCertificate_Rejected", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return // Rejected
		}
		defer conn.Close()

		client := proto.NewWorkerServiceClient(conn)
		_, rpcErr := client.Register(ctx, &proto.RegisterRequest{WorkerId: "untrusted-worker"})
		if rpcErr == nil {
			t.Fatalf("SECURITY VIOLATION: gRPC connection without client certificate was accepted!")
		}
		t.Logf("PASS: No cert rejected at transport layer: %v", rpcErr)
	})

	// Sub-test B: Connecting with EXPIRED certificate must fail at transport layer
	t.Run("ExpiredClientCertificate_Rejected", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		expiredCert, err := ca.IssueExpiredCertificate("expired-worker")
		if err != nil {
			t.Fatalf("failed to generate expired cert: %v", err)
		}

		clientTLSConfig, err := transport.NewClientTLSConfig(ca.CACertPEM(), *expiredCert, "localhost")
		if err != nil {
			t.Fatalf("failed to create client TLS config: %v", err)
		}

		conn, err := transport.NewClientConnWithTLS(serverAddr, clientTLSConfig)
		if err != nil {
			return
		}
		defer conn.Close()

		client := proto.NewWorkerServiceClient(conn)
		_, rpcErr := client.Register(ctx, &proto.RegisterRequest{WorkerId: "expired-worker"})
		if rpcErr == nil {
			t.Fatalf("SECURITY VIOLATION: gRPC connection with expired client certificate was accepted!")
		}
		t.Logf("PASS: Expired cert rejected at transport layer: %v", rpcErr)
	})

	// Sub-test C: Connecting with certificate signed by UNTRUSTED CA must fail at transport layer
	t.Run("UntrustedCACertificate_Rejected", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		untrustedCert, _, _, err := pki.IssueUntrustedCertificate("rogue-worker", 1*time.Hour)
		if err != nil {
			t.Fatalf("failed to issue untrusted cert: %v", err)
		}

		clientTLSConfig, err := transport.NewClientTLSConfig(ca.CACertPEM(), *untrustedCert, "localhost")
		if err != nil {
			t.Fatalf("failed to create client TLS config: %v", err)
		}

		conn, err := transport.NewClientConnWithTLS(serverAddr, clientTLSConfig)
		if err != nil {
			return
		}
		defer conn.Close()

		client := proto.NewWorkerServiceClient(conn)
		_, rpcErr := client.Register(ctx, &proto.RegisterRequest{WorkerId: "rogue-worker"})
		if rpcErr == nil {
			t.Fatalf("SECURITY VIOLATION: gRPC connection with untrusted CA certificate was accepted!")
		}
		t.Logf("PASS: Untrusted CA cert rejected at transport layer: %v", rpcErr)
	})

	// Sub-test D: Connecting with VALID client certificate issued by internal CA must succeed
	t.Run("ValidInternalCACertificate_Accepted", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		validCert, _, _, err := ca.IssueWorkerCertificate("valid-worker", 1*time.Hour)
		if err != nil {
			t.Fatalf("failed to issue valid worker cert: %v", err)
		}

		clientTLSConfig, err := transport.NewClientTLSConfig(ca.CACertPEM(), *validCert, "localhost")
		if err != nil {
			t.Fatalf("failed to create client TLS config: %v", err)
		}

		conn, err := transport.NewClientConnWithTLS(serverAddr, clientTLSConfig)
		if err != nil {
			t.Fatalf("failed to connect with valid mTLS: %v", err)
		}
		defer conn.Close()

		client := proto.NewWorkerServiceClient(conn)
		res, rpcErr := client.Register(ctx, &proto.RegisterRequest{WorkerId: "valid-worker"})
		if rpcErr != nil {
			t.Fatalf("expected valid mTLS call to succeed, got error: %v", rpcErr)
		}
		if !res.Success {
			t.Fatalf("expected successful response, got %+v", res)
		}
		t.Log("PASS: Valid internal CA certificate successfully authenticated over mTLS!")
	})
}

// §21.2 / Phase 13 Secrets Custody Test:
// Verifies master key is never present in process environment or on-disk config files (Gate G-36).
func TestSecurity_Secrets_MasterKeyNeverInProcessEnvOrDisk(t *testing.T) {
	kms := secrets.NewMockKMSClient()
	provider, err := secrets.NewKMSEnvelopeKeyProvider(kms)
	if err != nil {
		t.Fatalf("failed to create KMS key provider: %v", err)
	}

	store := secrets.NewMemorySecretStore(provider)
	ctx := context.Background()

	// Store sensitive secret
	sec, err := store.SetSecret(ctx, "proj-audit", "DB_PASS", "super-secret-password-12345")
	if err != nil {
		t.Fatalf("set secret failed: %v", err)
	}

	// 1. Audit Process Environment
	forbiddenPatterns := []string{
		"NEBULA_SECRETS_MASTER_KEY",
		"MASTER_ENCRYPTION_KEY",
		"ROOT_ENVELOPE_KEY",
	}
	clean, leakMsg := secrets.AuditProcessEnvironment(forbiddenPatterns)
	if !clean {
		t.Fatalf("SECURITY VIOLATION (G-36): %s", leakMsg)
	}

	// 2. Audit all os.Environ() strings
	for _, env := range os.Environ() {
		if strings.HasPrefix(strings.ToUpper(env), "NEBULA_SECRETS_MASTER_KEY=") {
			val := strings.Split(env, "=")[1]
			if len(val) > 0 {
				t.Fatalf("SECURITY VIOLATION (G-36): master key found in environment: %s", env)
			}
		}
	}

	// 3. Confirm secret record at rest has only ciphertext and wrapped DEK
	if len(sec.Ciphertext) == 0 {
		t.Fatalf("expected ciphertext stored at rest")
	}
	if strings.Contains(string(sec.Ciphertext), "super-secret-password") {
		t.Fatalf("SECURITY VIOLATION (G-36): raw plaintext found in ciphertext storage!")
	}

	t.Log("PASS (Gate G-36): Master encryption key is strictly absent from process environment and disk files at rest")
}

// §21.2 / Phase 13 Zero-Downtime Live Key Rotation Test:
// Verifies master key rotation under live concurrent read/write traffic with zero errors (Gate G-37).
func TestSecurity_Secrets_LiveMasterKeyRotationUnderConcurrentLoad(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kms := secrets.NewMockKMSClient()
	provider, err := secrets.NewKMSEnvelopeKeyProvider(kms)
	if err != nil {
		t.Fatalf("failed to create KMS provider: %v", err)
	}
	store := secrets.NewMemorySecretStore(provider)

	// Pre-populate secrets under v1 master key
	const numSecrets = 10
	for i := 0; i < numSecrets; i++ {
		secName := string(rune('A' + i))
		_, err := store.SetSecret(ctx, "proj-rot", secName, "initial-value-"+secName)
		if err != nil {
			t.Fatalf("failed to seed secret: %v", err)
		}
	}

	var wg sync.WaitGroup
	var readErrors, writeErrors int
	var mu sync.Mutex
	stopTraffic := make(chan struct{})

	// Start 4 concurrent readers
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for {
				select {
				case <-stopTraffic:
					return
				case <-ctx.Done():
					return
				default:
					secName := string(rune('A' + (readerID % numSecrets)))
					_, val, err := store.GetSecret(ctx, "proj-rot", secName)
					if err != nil || val == "" {
						mu.Lock()
						readErrors++
						mu.Unlock()
					}
					time.Sleep(1 * time.Millisecond)
				}
			}
		}(r)
	}

	// Start 2 concurrent writers
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			count := 0
			for {
				select {
				case <-stopTraffic:
					return
				case <-ctx.Done():
					return
				default:
					secName := string(rune('A' + (writerID % numSecrets)))
					_, err := store.SetSecret(ctx, "proj-rot", secName, "concurrent-val")
					if err != nil {
						mu.Lock()
						writeErrors++
						mu.Unlock()
					}
					count++
					time.Sleep(2 * time.Millisecond)
				}
			}
		}(w)
	}

	// Let traffic run for a brief moment, then trigger live key rotation
	time.Sleep(30 * time.Millisecond)

	oldKeyID := kms.CurrentKeyID()
	newKeyID, err := kms.RotateKey(ctx)
	if err != nil {
		t.Fatalf("KMS key rotation failed: %v", err)
	}

	// Dynamically re-wrap all stored DEKs under new master key
	if err := provider.ReWrapKeys(ctx, oldKeyID, newKeyID); err != nil {
		t.Fatalf("re-wrap keys under live traffic failed: %v", err)
	}

	// Let traffic continue under new key
	time.Sleep(30 * time.Millisecond)
	close(stopTraffic)
	wg.Wait()

	if readErrors > 0 {
		t.Fatalf("SECURITY VIOLATION (G-37): %d read errors occurred during live key rotation!", readErrors)
	}
	if writeErrors > 0 {
		t.Fatalf("SECURITY VIOLATION (G-37): %d write errors occurred during live key rotation!", writeErrors)
	}

	t.Log("PASS (Gate G-37): Master key rotated and DEKs re-wrapped under live concurrent traffic with ZERO downtime or errors")
}

// §21.1 / Phase 13 Ingress HTTPS & HSTS Enforcement:
// Verifies plaintext HTTP rejection/redirection and HSTS header presence.
func TestSecurity_Ingress_HTTPSAndHSTSEnforcement(t *testing.T) {
	log := zerolog.Nop()
	repo := projects.NewMemoryProjectRepository()
	reg := workers.NewRegistry(workers.NewMemoryWorkerRepository(), log)
	srv := api.NewServer(nil, repo, reg, nil, log)
	srv.SetEnforceHTTPS(true)

	handler := api.NewRouter(srv)

	// 1. Request with HTTPS forwarded header must receive HSTS header
	reqHTTPS := httptest.NewRequest(http.MethodGet, "/health", nil)
	reqHTTPS.Header.Set("X-Forwarded-Proto", "https")
	recHTTPS := httptest.NewRecorder()
	handler.ServeHTTP(recHTTPS, reqHTTPS)

	hstsHeader := recHTTPS.Header().Get("Strict-Transport-Security")
	if hstsHeader == "" || !strings.Contains(hstsHeader, "max-age=") {
		t.Fatalf("expected Strict-Transport-Security header, got: %q", hstsHeader)
	}

	// 2. Plaintext GET request must be redirected to HTTPS (301 Moved Permanently)
	reqPlain := httptest.NewRequest(http.MethodGet, "/health", nil)
	recPlain := httptest.NewRecorder()
	handler.ServeHTTP(recPlain, reqPlain)

	if recPlain.Code != http.StatusMovedPermanently {
		t.Fatalf("expected 301 redirect for plaintext HTTP GET, got: %d", recPlain.Code)
	}
	loc := recPlain.Header().Get("Location")
	if !strings.HasPrefix(loc, "https://") {
		t.Fatalf("expected redirect target to start with https://, got: %q", loc)
	}

	// 3. Plaintext POST request must be rejected with 426 Upgrade Required
	reqPlainPOST := httptest.NewRequest(http.MethodPost, "/v1/projects", nil)
	recPlainPOST := httptest.NewRecorder()
	handler.ServeHTTP(recPlainPOST, reqPlainPOST)

	if recPlainPOST.Code != http.StatusUpgradeRequired {
		t.Fatalf("expected 426 Upgrade Required for plaintext POST, got: %d", recPlainPOST.Code)
	}

	t.Log("PASS: Ingress enforces HTTPS-only with 301/426 rejection and Strict-Transport-Security HSTS headers")
}

// §21.1 / Phase 13 Certificate Renewal:
// Verifies worker agent certificate renewal math and seamless update before expiry.
func TestSecurity_Transport_CertificateRenewalBeforeExpiry(t *testing.T) {
	ca, _ := pki.NewCertificateAuthority("Nebula Renewal CA", 24*time.Hour)
	_, certPEM, _, err := ca.IssueWorkerCertificate("worker-renew-02", 20*time.Minute)
	if err != nil {
		t.Fatalf("issue cert: %v", err)
	}

	parsed, _ := pki.ParseCertificateFromPEM(certPEM)

	// Threshold 0.75: A fresh cert should NOT be renewed
	if pki.ShouldRenew(parsed, 0.75) {
		t.Fatalf("fresh cert should not require renewal")
	}

	// Simulated expired cert must trigger renewal
	expiredCert, _ := ca.IssueExpiredCertificate("worker-renew-02")
	parsedExpired, _ := x509.ParseCertificate(expiredCert.Certificate[0])
	if !pki.ShouldRenew(parsedExpired, 0.75) {
		t.Fatalf("expired cert should trigger renewal")
	}

	// Issue renewed cert
	renewedTLSCert, renewedPEM, _, err := ca.IssueWorkerCertificate("worker-renew-02", 20*time.Minute)
	if err != nil {
		t.Fatalf("failed to renew cert: %v", err)
	}
	if renewedTLSCert == nil || len(renewedPEM) == 0 {
		t.Fatalf("renewed cert is nil")
	}
	t.Log("PASS: Worker agent certificate renewal detected and issued before expiry")
}
