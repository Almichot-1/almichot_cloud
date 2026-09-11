//go:build realinfra

// Package gate contains real-infrastructure gate tests for the Nebula platform.
// This file provides shared helpers used across G-26, G-34, G-36/37, G-43/44, and G-46.
// All helpers skip (not fail) gracefully when required system dependencies are absent.
package gate

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nebula/nebula/internal/storage"
)

// â”€â”€â”€ Strict mode & skip tracking â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

var (
	realinfraSkipsMu sync.Mutex
	realinfraSkips   []string
)

// isStrictMode reports whether NEBULA_REALINFRA_STRICT=1 is set.
// When active, any missing dependency or failed setup immediately fails the test.
func isStrictMode() bool {
	return os.Getenv("NEBULA_REALINFRA_STRICT") == "1"
}

// trackGateTest registers a cleanup hook that records the test name if the test was skipped.
func trackGateTest(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		if t.Skipped() {
			realinfraSkipsMu.Lock()
			defer realinfraSkipsMu.Unlock()
			for _, name := range realinfraSkips {
				if name == t.Name() {
					return
				}
			}
			realinfraSkips = append(realinfraSkips, t.Name())
		}
	})
}

// skipOrFatal either fails immediately (in strict mode) or skips gracefully (in local dev).
func skipOrFatal(t *testing.T, format string, args ...any) {
	t.Helper()
	msg := fmt.Sprintf(format, args...)
	if isStrictMode() {
		t.Fatalf("STRICT MODE VIOLATION: %s", msg)
	}
	t.Skip(msg)
}

// requireBinary skips t if the named binary is not found on PATH. In strict mode,
// it fails the test immediately.
func requireBinary(t *testing.T, binName string) {
	t.Helper()
	trackGateTest(t)
	if _, err := exec.LookPath(binName); err != nil {
		skipOrFatal(t, "%s binary not found on PATH; skipping real-infra gate", binName)
	}
}

// â”€â”€â”€ Docker / Postgres helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

// requireDockerAvailable skips t if the `docker` binary is not on PATH or if
// the Docker daemon is not reachable. In strict mode, it fails the test immediately.
func requireDockerAvailable(t *testing.T) {
	t.Helper()
	trackGateTest(t)
	if _, err := exec.LookPath("docker"); err != nil {
		skipOrFatal(t, "docker binary not found on PATH; skipping real-infra gate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "info")
	if err := cmd.Run(); err != nil {
		skipOrFatal(t, "docker daemon not reachable (%v); skipping real-infra gate", err)
	}
}

// startPostgres launches a fresh postgres:18-alpine Docker container, waits until it
// is ready, and registers cleanup. Returns the DSN and an open pgxpool.Pool.
// Each call returns a new independent instance on a random host port.
func startPostgres(t *testing.T) (dsn string, pool *pgxpool.Pool) {
	t.Helper()
	trackGateTest(t)
	requireDockerAvailable(t)

	containerName := fmt.Sprintf("nebula-gate-pg-%d", time.Now().UnixNano())
	const pgUser, pgPass, pgDB = "nebula_gate", "nebula_gate", "nebula_gate"

	// --rm ensures the container is deleted when stopped.
	cmd := exec.Command("docker", "run", "--rm", "--name", containerName,
		"-e", "POSTGRES_USER="+pgUser,
		"-e", "POSTGRES_PASSWORD="+pgPass,
		"-e", "POSTGRES_DB="+pgDB,
		"-p", "0:5432", // random host port
		"-d", // detached
		"postgres:18-alpine",
		"-c", "log_min_messages=WARNING",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker run postgres: %v\n%s", err, out)
	}
	containerID := strings.TrimSpace(string(out))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		killCmd := exec.CommandContext(stopCtx, "docker", "rm", "-f", containerID)
		_ = killCmd.Run()
	})

	// Discover the host port Docker assigned.
	portOut, err := exec.Command("docker", "port", containerID, "5432").Output()
	if err != nil {
		t.Fatalf("docker port: %v", err)
	}
	hostPort, err := parseDockerPort(string(portOut))
	if err != nil {
		t.Fatalf("parse postgres port: %v", err)
	}

	dsn = fmt.Sprintf("postgres://%s:%s@127.0.0.1:%s/%s?sslmode=disable", pgUser, pgPass, hostPort, pgDB)

	// Wait for Postgres to be ready (up to 30s).
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := waitForTCPAddr("127.0.0.1:"+hostPort, ctx); err != nil {
		t.Fatalf("postgres did not become ready: %v", err)
	}

	// Open pool and apply schema migrations.
	pool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open pgxpool for gate postgres: %v", err)
	}
	t.Cleanup(pool.Close)

	// Wait for Postgres to accept queries (it may accept TCP before accepting SQL).
	if err := waitForPostgresReady(ctx, pool); err != nil {
		t.Fatalf("postgres not ready for queries: %v", err)
	}

	// Apply schema migrations.
	if err := storage.Migrate(ctx, pool); err != nil {
		t.Fatalf("apply migrations to gate postgres: %v", err)
	}

	t.Logf("gate Postgres ready: container=%s port=%s", containerID[:12], hostPort)
	return dsn, pool
}

func waitForPostgresReady(ctx context.Context, pool *pgxpool.Pool) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := pool.Ping(ctx); err == nil {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// â”€â”€â”€ LocalStack helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

const (
	localstackImage     = "localstack/localstack:4"
	localstackKMSPort   = "4566"
	localstackAccessKey = "test"
	localstackSecretKey = "test"
	localstackRegion    = "us-east-1"
)

// pullDockerImageWithRetry attempts to pull the specified docker image with bounded
// retries and exponential backoff (e.g. 2 attempts, exponential backoff: 2s, 4s).
func pullDockerImageWithRetry(image string, retries int, initialBackoff time.Duration) error {
	var lastErr error
	backoff := initialBackoff
	for attempt := 0; attempt <= retries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		cmd := exec.CommandContext(ctx, "docker", "pull", image)
		out, err := cmd.CombinedOutput()
		cancel()
		if err == nil {
			return nil
		}
		lastErr = fmt.Errorf("docker pull %s: %w (%s)", image, err, strings.TrimSpace(string(out)))
		if attempt < retries {
			time.Sleep(backoff)
			backoff *= 2
		}
	}
	return lastErr
}

// startLocalStack launches a LocalStack container and waits until the KMS endpoint
// is ready. Returns the base endpoint URL (e.g. "http://127.0.0.1:NNNNN").
func startLocalStack(t *testing.T) (endpointURL string) {
	t.Helper()
	trackGateTest(t)
	requireDockerAvailable(t)

	// Ensure LocalStack image is pulled with bounded retries (Fix 1).
	if err := pullDockerImageWithRetry(localstackImage, 2, 2*time.Second); err != nil {
		skipOrFatal(t, "failed to pull LocalStack image %q after retries: %v", localstackImage, err)
		return ""
	}

	containerName := fmt.Sprintf("nebula-gate-ls-%d", time.Now().UnixNano())
	cmd := exec.Command("docker", "run", "--rm", "--name", containerName,
		"-e", "SERVICES=kms",
		"-e", "DEFAULT_REGION="+localstackRegion,
		"-p", "0:4566",
		"-d",
		localstackImage,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		skipOrFatal(t, "docker run localstack (%s) failed: %v\n%s", localstackImage, err, out)
		return ""
	}
	containerID := strings.TrimSpace(string(out))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(ctx, "docker", "rm", "-f", containerID).Run()
	})

	portOut, err := exec.Command("docker", "port", containerID, localstackKMSPort).Output()
	if err != nil {
		skipOrFatal(t, "docker port localstack: %v", err)
		return ""
	}
	hostPort, err := parseDockerPort(string(portOut))
	if err != nil {
		skipOrFatal(t, "parse localstack port: %v", err)
		return ""
	}

	endpointURL = "http://127.0.0.1:" + hostPort

	// Wait for LocalStack health check.
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := waitForHTTPHealth(ctx, endpointURL+"/_localstack/health"); err != nil {
		skipOrFatal(t, "LocalStack not ready: %v", err)
		return ""
	}

	t.Logf("LocalStack ready: endpoint=%s", endpointURL)
	return endpointURL
}

// createLocalStackCMK creates a new CMK in LocalStack KMS and returns its key ID.
func createLocalStackCMK(t *testing.T, endpoint string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use AWS CLI if available; otherwise use raw HTTP.
	// LocalStack accepts any credentials; explicit env is required because the
	// CI runner has no configured AWS profiles (AWS CLI exits 253 otherwise).
	if _, err := exec.LookPath("aws"); err == nil {
		cmd := exec.CommandContext(ctx, "aws", "--endpoint-url="+endpoint,
			"--region="+localstackRegion,
			"kms", "create-key", "--query=KeyMetadata.KeyId", "--output=text",
		)
		cmd.Env = append(os.Environ(),
			"AWS_ACCESS_KEY_ID="+localstackAccessKey,
			"AWS_SECRET_ACCESS_KEY="+localstackSecretKey,
			"AWS_DEFAULT_REGION="+localstackRegion,
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("aws kms create-key: %v\n%s", err, out)
		}
		return strings.TrimSpace(string(out))
	}

	// Fallback: raw HTTP to LocalStack.
	body := `{"Description":"nebula-gate-test","KeyUsage":"ENCRYPT_DECRYPT"}`
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "TrentService.CreateKey")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/kms/aws4_request, SignedHeaders=host, Signature=fakesig")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create CMK HTTP: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	var respData struct {
		KeyMetadata struct {
			KeyId string `json:"KeyId"`
		} `json:"KeyMetadata"`
	}
	if err := json.Unmarshal(raw, &respData); err == nil && respData.KeyMetadata.KeyId != "" {
		return respData.KeyMetadata.KeyId
	}
	t.Fatalf("create CMK: failed to parse KeyId from response: %s", string(raw))
	return ""
}

// â”€â”€â”€ Subprocess / binary helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

// buildNebulaTestBinary compiles the Go package at pkgPath (relative to the
// module root, e.g. "./cmd/gate-registry-worker") into a temp directory and
// returns the absolute path to the resulting binary.
func buildNebulaTestBinary(t *testing.T, pkgPath string) string {
	t.Helper()
	tmpDir := t.TempDir()
	binaryName := "gate-worker"
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	outPath := filepath.Join(tmpDir, binaryName)

	cmd := exec.Command("go", "build", "-o", outPath, pkgPath)
	cmd.Dir = moduleRoot(t)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("go build %s: %v", pkgPath, err)
	}
	return outPath
}

// moduleRoot returns the absolute path to the Go module root by walking up from
// the test file's directory until go.mod is found.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find go.mod")
		}
		dir = parent
	}
}

// â”€â”€â”€ TCP Proxy (for G-44 partition simulation) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

// TCPProxy forwards TCP connections between clients and a backend, and can be
// "partitioned" to simulate a network split by closing all active connections.
// This provides a cross-platform alternative to iptables for G-44.
type TCPProxy struct {
	listener net.Listener
	backend  string // host:port of the real backend (Postgres)

	mu          sync.Mutex
	conns       []net.Conn
	partitioned bool
}

// NewTCPProxy starts a TCP proxy listening on a random loopback port and forwarding
// to backend. Returns the proxy and its listening address.
func NewTCPProxy(t *testing.T, backend string) (*TCPProxy, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("TCPProxy listen: %v", err)
	}
	p := &TCPProxy{listener: lis, backend: backend}
	go p.serve()
	t.Cleanup(func() { _ = lis.Close() })
	return p, lis.Addr().String()
}

// Partition closes all active proxy connections, simulating a network partition
// between the client (CP) and the backend (Postgres). New connections are also
// rejected until Restore is called.
func (p *TCPProxy) Partition() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.partitioned = true
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

// Restore re-allows connections through the proxy.
func (p *TCPProxy) Restore() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.partitioned = false
}

func (p *TCPProxy) serve() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return // listener closed
		}
		p.mu.Lock()
		partitioned := p.partitioned
		if !partitioned {
			p.conns = append(p.conns, client)
		}
		p.mu.Unlock()

		if partitioned {
			_ = client.Close()
			continue
		}

		backend, err := net.Dial("tcp", p.backend)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, backend)
		p.mu.Unlock()

		go io.Copy(backend, client) //nolint:errcheck
		go io.Copy(client, backend) //nolint:errcheck
	}
}

// â”€â”€â”€ General utilities â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

// waitForTCPAddr polls addr (host:port) until a successful TCP connect or ctx expires.
func waitForTCPAddr(addr string, ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for TCP %s: %w", addr, ctx.Err())
		default:
		}
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// parseDockerPort parses the assigned host port from the output of `docker port`.
// Handles multi-line output (IPv4 + IPv6).
func parseDockerPort(output string) (string, error) {
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if _, port, err := net.SplitHostPort(line); err == nil && port != "" {
			return port, nil
		}
		parts := strings.Split(line, ":")
		if len(parts) > 1 {
			p := strings.TrimSpace(parts[len(parts)-1])
			if p != "" {
				return p, nil
			}
		}
	}
	return "", fmt.Errorf("could not parse port from docker port output: %q", output)
}

// waitForHTTPHealth polls a URL until it returns HTTP 200 or ctx expires.
func waitForHTTPHealth(ctx context.Context, url string) error {
	client := &http.Client{Timeout: 2 * time.Second}
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for HTTP health %s: %w", url, ctx.Err())
		default:
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err == nil {
			resp, err := client.Do(req)
			if err == nil && resp.StatusCode == http.StatusOK {
				resp.Body.Close()
				return nil
			}
			if resp != nil {
				resp.Body.Close()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// isProcessAlive returns true if a process with the given PID is running.
// Uses kill(pid, 0) on Unix; TaskList on Windows.
func isProcessAlive(pid int) bool {
	if runtime.GOOS == "windows" {
		out, err := exec.Command("tasklist", "/fi", fmt.Sprintf("PID eq %d", pid), "/nh").Output()
		if err != nil {
			return false
		}
		return strings.Contains(string(out), fmt.Sprintf("%d", pid))
	}
	// Unix: kill(pid, 0) â€” no signal sent, just existence check
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// On Unix, FindProcess always succeeds; send signal 0 to check existence.
	err = proc.Signal(os.Signal(nil))
	return err == nil
}

// readLastLine reads the last non-empty line from cmd's combined stdout+stderr.
// Used to capture the digest written to stdout by the push subprocess in G-26.
func captureSubprocessStdout(t *testing.T, cmd *exec.Cmd) string {
	t.Helper()
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("subprocess failed: %v\noutput: %s", err, out)
	}
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	var last string
	for scanner.Scan() {
		if line := strings.TrimSpace(scanner.Text()); line != "" {
			last = line
		}
	}
	return last
}

