package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetrics_GoldenSetVerbatim(t *testing.T) {
	// The 8 minimum metrics specified in §22.1 verbatim
	expectedMetrics := []struct {
		name string
		typ  MetricType
	}{
		{"worker_heartbeat_age_seconds", TypeGauge},
		{"worker_cpu_percent", TypeGauge},
		{"worker_memory_percent", TypeGauge},
		{"deployment_duration_seconds", TypeHistogram},
		{"deployment_status_total", TypeCounter},
		{"container_restart_total", TypeCounter},
		{"scheduler_placement_total", TypeCounter},
		{"image_pull_failure_total", TypeCounter},
	}

	for _, em := range expectedMetrics {
		f, ok := DefaultRegistry.families[em.name]
		if !ok {
			t.Fatalf("GOLDEN REGRESSION: required §22.1 metric %q is missing from DefaultRegistry", em.name)
		}
		if f.Type != em.typ {
			t.Fatalf("GOLDEN REGRESSION: metric %q type mismatch: expected %v, got %v", em.name, em.typ, f.Type)
		}
	}
}

func TestMetrics_IsolatedComputationAndLabels(t *testing.T) {
	reg := NewRegistry()

	// 1. worker_heartbeat_age_seconds
	m1 := reg.RegisterGauge("worker_heartbeat_age_seconds", "test")
	m1.Set(map[string]string{"worker_id": "worker-alpha", "state": "HEALTHY"}, 3.5)
	if val := m1.Get(map[string]string{"worker_id": "worker-alpha", "state": "HEALTHY"}); val != 3.5 {
		t.Errorf("worker_heartbeat_age_seconds mismatch: expected 3.5, got %v", val)
	}

	// 2. worker_cpu_percent
	m2 := reg.RegisterGauge("worker_cpu_percent", "test")
	m2.Set(map[string]string{"worker_id": "worker-alpha"}, 42.8)
	if val := m2.Get(map[string]string{"worker_id": "worker-alpha"}); val != 42.8 {
		t.Errorf("worker_cpu_percent mismatch: expected 42.8, got %v", val)
	}

	// 3. worker_memory_percent
	m3 := reg.RegisterGauge("worker_memory_percent", "test")
	m3.Set(map[string]string{"worker_id": "worker-alpha"}, 68.2)
	if val := m3.Get(map[string]string{"worker_id": "worker-alpha"}); val != 68.2 {
		t.Errorf("worker_memory_percent mismatch: expected 68.2, got %v", val)
	}

	// 4. deployment_duration_seconds
	m4 := reg.RegisterHistogram("deployment_duration_seconds", "test")
	m4.Observe(map[string]string{"project_id": "proj-1", "status": "RUNNING"}, 12.4)
	if val := m4.Get(map[string]string{"project_id": "proj-1", "status": "RUNNING"}); val != 12.4 {
		t.Errorf("deployment_duration_seconds mismatch: expected 12.4, got %v", val)
	}

	// 5. deployment_status_total
	m5 := reg.RegisterCounter("deployment_status_total", "test")
	m5.Inc(map[string]string{"project_id": "proj-1", "status": "RUNNING"})
	m5.Inc(map[string]string{"project_id": "proj-1", "status": "RUNNING"})
	if val := m5.Get(map[string]string{"project_id": "proj-1", "status": "RUNNING"}); val != 2 {
		t.Errorf("deployment_status_total mismatch: expected 2, got %v", val)
	}

	// 6. container_restart_total
	m6 := reg.RegisterCounter("container_restart_total", "test")
	m6.Inc(map[string]string{"instance_id": "inst-99", "project_id": "proj-1", "reason": "OOMKilled"})
	if val := m6.Get(map[string]string{"instance_id": "inst-99", "project_id": "proj-1", "reason": "OOMKilled"}); val != 1 {
		t.Errorf("container_restart_total mismatch: expected 1, got %v", val)
	}

	// 7. scheduler_placement_total
	m7 := reg.RegisterCounter("scheduler_placement_total", "test")
	m7.Add(map[string]string{"worker_id": "worker-beta", "strategy": "SPREADING"}, 5)
	if val := m7.Get(map[string]string{"worker_id": "worker-beta", "strategy": "SPREADING"}); val != 5 {
		t.Errorf("scheduler_placement_total mismatch: expected 5, got %v", val)
	}

	// 8. image_pull_failure_total
	m8 := reg.RegisterCounter("image_pull_failure_total", "test")
	m8.Inc(map[string]string{"image_ref": "registry.nebula/bad:v1", "reason": "digest_mismatch"})
	if val := m8.Get(map[string]string{"image_ref": "registry.nebula/bad:v1", "reason": "digest_mismatch"}); val != 1 {
		t.Errorf("image_pull_failure_total mismatch: expected 1, got %v", val)
	}

	// Format output check
	out := reg.FormatPrometheus()
	for _, mName := range []string{
		"worker_heartbeat_age_seconds",
		"worker_cpu_percent",
		"worker_memory_percent",
		"deployment_duration_seconds",
		"deployment_status_total",
		"container_restart_total",
		"scheduler_placement_total",
		"image_pull_failure_total",
	} {
		if !strings.Contains(out, mName) {
			t.Errorf("Formatted output missing metric %s", mName)
		}
	}
}

func TestMetrics_HTTPExpositionHandler(t *testing.T) {
	reg := NewRegistry()
	g := reg.RegisterGauge("test_gauge", "test gauge metric")
	g.Set(map[string]string{"env": "test"}, 100)

	server := httptest.NewServer(reg.HTTPHandler())
	defer server.Close()

	resp, err := http.Get(server.URL)
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/plain") {
		t.Fatalf("Expected text/plain content type, got %s", ct)
	}
}
