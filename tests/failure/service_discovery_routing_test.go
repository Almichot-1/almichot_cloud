package failure

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/nebula/nebula/internal/discovery"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/rs/zerolog"
)

// SD-05 & LB-01 (Gate G-10):
// - SD-05: UNHEALTHY instance is immediately removed from service discovery.
// - LB-01: Load balancer stops routing traffic to known-unhealthy endpoints.
func TestSD05_LB01_G10_UnhealthyEndpointsPulledFromLoadBalancer(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	// 1. Setup Load Balancer Router and Service Discovery Registry
	router := loadbalancer.NewRouter(log)
	serviceReg := discovery.NewServiceRegistry(router, log)

	var backend1Hits uint64
	var backend2Hits uint64

	// Backend 1: Healthy target
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint64(&backend1Hits, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"backend":"1","status":"healthy"}`))
	}))
	defer server1.Close()

	// Backend 2: Initially healthy target that will fail
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint64(&backend2Hits, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"backend":"2","status":"healthy"}`))
	}))
	defer server2.Close()

	projectID := "proj-g10-routing"

	// 2. Register both instances in Service Discovery
	ep1 := discovery.Endpoint{
		InstanceID:   "inst-1",
		DeploymentID: "dep-1",
		ProjectID:    projectID,
		WorkerID:     "worker-1",
		Address:      server1.URL,
		Status:       discovery.EndpointHealthy,
	}
	ep2 := discovery.Endpoint{
		InstanceID:   "inst-2",
		DeploymentID: "dep-1",
		ProjectID:    projectID,
		WorkerID:     "worker-2",
		Address:      server2.URL,
		Status:       discovery.EndpointHealthy,
	}

	_ = serviceReg.RegisterEndpoint(ctx, ep1)
	_ = serviceReg.RegisterEndpoint(ctx, ep2)

	// Verify both are registered in Service Discovery
	healthyList := serviceReg.GetHealthyEndpoints(projectID)
	if len(healthyList) != 2 {
		t.Fatalf("expected 2 healthy endpoints in SD, got %d", len(healthyList))
	}

	// 3. Send HTTP requests across load balancer: both backends should receive traffic
	for i := 0; i < 6; i++ {
		req := httptest.NewRequest(http.MethodGet, "http://nebula.cloud/api", nil)
		req.Header.Set("X-Project-ID", projectID)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Result().StatusCode != http.StatusOK {
			t.Fatalf("expected HTTP 200, got %d", rec.Result().StatusCode)
		}
	}

	hits1 := atomic.LoadUint64(&backend1Hits)
	hits2 := atomic.LoadUint64(&backend2Hits)
	if hits1 == 0 || hits2 == 0 {
		t.Fatalf("expected traffic to be balanced across both backends, got hits1=%d, hits2=%d", hits1, hits2)
	}

	// 4. Trigger Failure: Instance 2 becomes UNHEALTHY (SD-05 & G-10)
	updatedEP, err := serviceReg.SetEndpointHealth(ctx, ep2.InstanceID, false)
	if err != nil {
		t.Fatalf("failed to update endpoint health: %v", err)
	}
	if updatedEP.Status != discovery.EndpointUnhealthy {
		t.Fatalf("expected status UNHEALTHY, got %s", updatedEP.Status)
	}

	// Invariant SD-05: UNHEALTHY instance is NO LONGER listed as available in service discovery
	healthyAfter := serviceReg.GetHealthyEndpoints(projectID)
	if len(healthyAfter) != 1 {
		t.Fatalf("SD-05 failure: expected exactly 1 healthy endpoint remaining in SD, got %d", len(healthyAfter))
	}
	if healthyAfter[0].InstanceID != ep1.InstanceID {
		t.Fatalf("SD-05 failure: unexpected endpoint remaining: %s", healthyAfter[0].InstanceID)
	}

	// 5. Invariant LB-01: Load balancer STOPS routing to the unhealthy endpoint!
	// Send 10 consecutive requests through load balancer
	initialHits2 := atomic.LoadUint64(&backend2Hits)

	for i := 0; i < 10; i++ {
		req := httptest.NewRequest(http.MethodGet, "http://nebula.cloud/api", nil)
		req.Header.Set("X-Project-ID", projectID)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Result().StatusCode != http.StatusOK {
			t.Fatalf("expected HTTP 200 from healthy backend, got %d", rec.Result().StatusCode)
		}
		body, _ := io.ReadAll(rec.Result().Body)
		if strings.Contains(string(body), `"backend":"2"`) {
			t.Fatalf("LB-01 / G-10 VIOLATION: load balancer routed traffic to known-unhealthy backend 2!")
		}
	}

	finalHits2 := atomic.LoadUint64(&backend2Hits)
	if finalHits2 != initialHits2 {
		t.Fatalf("LB-01 / G-10 VIOLATION: backend 2 received %d new requests after becoming UNHEALTHY!",
			finalHits2-initialHits2)
	}

	t.Logf("SD-05 & LB-01 (Gate G-10) Passed: Unhealthy endpoint removed from service discovery and zero requests routed to it by the load balancer!")
}
