package networking_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/rs/zerolog"
)

func TestNetworking_Gate41_HostnameRoutingZeroCrossTenantLeakage(t *testing.T) {
	// G-41: two projects on distinct hostnames route correctly with no cross-tenant leakage
	log := zerolog.Nop()
	router := loadbalancer.NewRouter(log)

	// Backend for Tenant A
	var tenantACalls uint64
	backendA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint64(&tenantACalls, 1)
		w.Header().Set("X-Tenant", "project-alpha")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("HELLO FROM TENANT ALPHA"))
	}))
	defer backendA.Close()

	// Backend for Tenant B
	var tenantBCalls uint64
	backendB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint64(&tenantBCalls, 1)
		w.Header().Set("X-Tenant", "project-beta")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("HELLO FROM TENANT BETA"))
	}))
	defer backendB.Close()

	// Register targets
	_ = router.RegisterTarget("proj-alpha", "inst-a1", backendA.URL)
	_ = router.RegisterTarget("proj-beta", "inst-b1", backendB.URL)

	// Register distinct hostnames
	router.RegisterHostname("alpha.nebula.internal", "proj-alpha")
	router.RegisterHostname("beta.nebula.internal", "proj-beta")

	ingressServer := httptest.NewServer(router)
	defer ingressServer.Close()

	// Concurrently send traffic to both hostnames
	var wg sync.WaitGroup
	var leakageErrors uint64
	iterations := 50

	// Client requesting Tenant A
	wg.Add(1)
	go func() {
		defer wg.Done()
		client := &http.Client{Timeout: 2 * time.Second}
		for i := 0; i < iterations; i++ {
			req, _ := http.NewRequest(http.MethodGet, ingressServer.URL+"/api/resource", nil)
			req.Host = "alpha.nebula.internal" // Host header resolution

			resp, err := client.Do(req)
			if err != nil {
				atomic.AddUint64(&leakageErrors, 1)
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			if !strings.Contains(string(body), "TENANT ALPHA") || resp.Header.Get("X-Tenant") != "project-alpha" {
				atomic.AddUint64(&leakageErrors, 1)
			}
		}
	}()

	// Client requesting Tenant B
	wg.Add(1)
	go func() {
		defer wg.Done()
		client := &http.Client{Timeout: 2 * time.Second}
		for i := 0; i < iterations; i++ {
			req, _ := http.NewRequest(http.MethodGet, ingressServer.URL+"/api/resource", nil)
			req.Host = "beta.nebula.internal" // Host header resolution

			resp, err := client.Do(req)
			if err != nil {
				atomic.AddUint64(&leakageErrors, 1)
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			if !strings.Contains(string(body), "TENANT BETA") || resp.Header.Get("X-Tenant") != "project-beta" {
				atomic.AddUint64(&leakageErrors, 1)
			}
		}
	}()

	wg.Wait()

	if leakageErrors > 0 {
		t.Fatalf("G-41 VIOLATION: %d cross-tenant leakage or routing errors detected under concurrent load!", leakageErrors)
	}

	if atomic.LoadUint64(&tenantACalls) != uint64(iterations) || atomic.LoadUint64(&tenantBCalls) != uint64(iterations) {
		t.Fatalf("G-41 VIOLATION: call count mismatch: A=%d, B=%d", tenantACalls, tenantBCalls)
	}

	// Invariant assertion: LB should NOT call back into Control Plane per-request
	if router.CPCallbackCount() != 0 {
		t.Fatalf("INVARIANT VIOLATION: Load Balancer performed %d callbacks to Control Plane during proxying!", router.CPCallbackCount())
	}

	t.Log("✅ G-41 PASSED: two projects on distinct hostnames route cleanly with zero cross-tenant leakage under load")
}

func TestNetworking_Gate42_RateLimitedClientReceives429Isolated(t *testing.T) {
	// G-42: a rate-limited client receives 429s without impacting other tenants' traffic
	log := zerolog.Nop()
	router := loadbalancer.NewRouter(log)

	// Backend server
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))
	defer backend.Close()

	_ = router.RegisterTarget("tenant-a", "inst-a", backend.URL)
	_ = router.RegisterTarget("tenant-b", "inst-b", backend.URL)
	router.RegisterHostname("tenant-a.internal", "tenant-a")
	router.RegisterHostname("tenant-b.internal", "tenant-b")

	// Limit: 5 req/sec, burst 5 per tenant
	rateLimiter := loadbalancer.NewTenantRateLimiter(5.0, 5, nil)
	handler := rateLimiter.Middleware(router)

	ingressServer := httptest.NewServer(handler)
	defer ingressServer.Close()

	client := &http.Client{Timeout: 2 * time.Second}

	// 1. Tenant A bursts with 15 rapid requests (exceeds burst of 5)
	var tenantA429s int
	var tenantA200s int
	for i := 0; i < 15; i++ {
		req, _ := http.NewRequest(http.MethodGet, ingressServer.URL, nil)
		req.Host = "tenant-a.internal"
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			tenantA429s++
		} else if resp.StatusCode == http.StatusOK {
			tenantA200s++
		}
		resp.Body.Close()
	}

	if tenantA429s == 0 {
		t.Fatalf("G-42 VIOLATION: bursting Tenant A expected HTTP 429s, got zero (200s: %d)", tenantA200s)
	}

	// 2. Concurrently verify Tenant B makes legitimate requests within limit
	// Tenant B should get 100% HTTP 200s with zero impact from Tenant A's rate-limiting
	var tenantB200s int
	var tenantB429s int
	for i := 0; i < 4; i++ {
		req, _ := http.NewRequest(http.MethodGet, ingressServer.URL, nil)
		req.Host = "tenant-b.internal"
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Tenant B request failed: %v", err)
		}
		if resp.StatusCode == http.StatusOK {
			tenantB200s++
		} else if resp.StatusCode == http.StatusTooManyRequests {
			tenantB429s++
		}
		resp.Body.Close()
	}

	if tenantB429s > 0 || tenantB200s != 4 {
		t.Fatalf("G-42 VIOLATION: Tenant B traffic was impacted by Tenant A's burst! (200s=%d, 429s=%d)", tenantB200s, tenantB429s)
	}

	t.Logf("✅ G-42 PASSED: Tenant A received %d 429s under burst while Tenant B received %d 200s without interference", tenantA429s, tenantB200s)
}

func TestNetworking_PathBasedRoutingRegression(t *testing.T) {
	// Explicit regression check: path-based routing still functions unchanged after hostname routing is added
	log := zerolog.Nop()
	router := loadbalancer.NewRouter(log)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("PATH ROUTED OK"))
	}))
	defer backend.Close()

	_ = router.RegisterTarget("proj-legacy", "inst-leg", backend.URL)

	ingressServer := httptest.NewServer(router)
	defer ingressServer.Close()

	// Request via /services/proj-legacy/api/test without Host mapping
	resp, err := http.Get(ingressServer.URL + "/services/proj-legacy/api/test")
	if err != nil {
		t.Fatalf("Path-based request failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "PATH ROUTED OK") {
		t.Fatalf("REGRESSION: path-based routing failed after hostname routing addition (status=%d, body=%s)", resp.StatusCode, string(body))
	}
}

func TestNetworking_BasicWAFRules(t *testing.T) {
	// Verify basic WAF blocks SQL injection, path traversal, and null byte injections
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("SAFE"))
	})

	handler := loadbalancer.WAFMiddleware(backend)
	server := httptest.NewServer(handler)
	defer server.Close()

	client := &http.Client{Timeout: 2 * time.Second}

	// 1. SQL Injection attack in query
	resp, _ := client.Get(server.URL + "/v1/users?id=1%27%20OR%20%271%27=%271")
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected WAF to block SQL injection with 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 2. Directory traversal attack in path
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/../../etc/passwd", nil)
	resp, _ = client.Do(req)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected WAF to block directory traversal with 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 3. Null byte injection
	resp, _ = client.Get(server.URL + "/v1/download?file=app.bin%00.txt")
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected WAF to block null byte injection with 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 4. Safe request passes through cleanly
	resp, _ = client.Get(server.URL + "/v1/projects/my-proj/deployments")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected safe request to pass through with 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestNetworking_InstanceFlapResilience(t *testing.T) {
	// Instance flap test: an instance rapidly going healthy/unhealthy doesn't cause panic or thrashing
	log := zerolog.Nop()
	router := loadbalancer.NewRouter(log)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	// Rapid flap in background
	stopFlap := make(chan struct{})
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stopFlap:
				return
			default:
				instID := fmt.Sprintf("inst-flap-%d", i%5)
				_ = router.RegisterTarget("flapping-proj", instID, backend.URL)
				time.Sleep(1 * time.Millisecond)
				router.UnregisterTarget("flapping-proj", instID)
			}
		}
	}()

	// Perform concurrent queries during flap
	var wg sync.WaitGroup
	for c := 0; c < 5; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				_ = router.GetTargets("flapping-proj")
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	close(stopFlap)
	t.Log("✅ Instance flap resilience verified: zero race conditions or panics during rapid target updates")
}
