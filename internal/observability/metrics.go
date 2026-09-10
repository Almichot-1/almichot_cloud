package observability

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// MetricType defines the Prometheus metric type (counter, gauge, histogram, summary).
type MetricType string

const (
	TypeCounter   MetricType = "counter"
	TypeGauge     MetricType = "gauge"
	TypeHistogram MetricType = "histogram"
)

// MetricFamily represents a named Prometheus metric with help text and type.
type MetricFamily struct {
	Name   string
	Help   string
	Type   MetricType
	mu     sync.RWMutex
	values map[string]float64 // label signature -> value
	labels map[string]map[string]string
}

// Registry stores all registered metric families and provides thread-safe formatting.
type Registry struct {
	mu       sync.RWMutex
	families map[string]*MetricFamily
}

// NewRegistry creates a fresh Prometheus metric registry.
func NewRegistry() *Registry {
	return &Registry{
		families: make(map[string]*MetricFamily),
	}
}

// Global registry instance.
var DefaultRegistry = NewRegistry()

// RegisterCounter registers a counter metric family.
func (r *Registry) RegisterCounter(name, help string) *MetricFamily {
	r.mu.Lock()
	defer r.mu.Unlock()
	if f, exists := r.families[name]; exists {
		return f
	}
	f := &MetricFamily{
		Name:   name,
		Help:   help,
		Type:   TypeCounter,
		values: make(map[string]float64),
		labels: make(map[string]map[string]string),
	}
	r.families[name] = f
	return f
}

// RegisterGauge registers a gauge metric family.
func (r *Registry) RegisterGauge(name, help string) *MetricFamily {
	r.mu.Lock()
	defer r.mu.Unlock()
	if f, exists := r.families[name]; exists {
		return f
	}
	f := &MetricFamily{
		Name:   name,
		Help:   help,
		Type:   TypeGauge,
		values: make(map[string]float64),
		labels: make(map[string]map[string]string),
	}
	r.families[name] = f
	return f
}

// RegisterHistogram registers a histogram metric family.
func (r *Registry) RegisterHistogram(name, help string) *MetricFamily {
	r.mu.Lock()
	defer r.mu.Unlock()
	if f, exists := r.families[name]; exists {
		return f
	}
	f := &MetricFamily{
		Name:   name,
		Help:   help,
		Type:   TypeHistogram,
		values: make(map[string]float64),
		labels: make(map[string]map[string]string),
	}
	r.families[name] = f
	return f
}

func labelKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(labels[k])
	}
	return sb.String()
}

// Inc increments a counter by 1.
func (f *MetricFamily) Inc(labels map[string]string) {
	f.Add(labels, 1)
}

// Add adds a given value to a counter.
func (f *MetricFamily) Add(labels map[string]string, val float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := labelKey(labels)
	f.values[key] += val
	if labels != nil {
		f.labels[key] = labels
	}
}

// Set sets the value of a gauge.
func (f *MetricFamily) Set(labels map[string]string, val float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := labelKey(labels)
	f.values[key] = val
	if labels != nil {
		f.labels[key] = labels
	}
}

// Observe records a sample in a histogram or gauge.
func (f *MetricFamily) Observe(labels map[string]string, val float64) {
	f.Set(labels, val)
}

// Get returns the current value for a given label set.
func (f *MetricFamily) Get(labels map[string]string) float64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	key := labelKey(labels)
	return f.values[key]
}

// FormatPrometheus serializes all registered metrics to Prometheus exposition text format (0.0.4).
func (r *Registry) FormatPrometheus() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var sb strings.Builder

	// Sort family names for deterministic output
	familyNames := make([]string, 0, len(r.families))
	for name := range r.families {
		familyNames = append(familyNames, name)
	}
	sort.Strings(familyNames)

	for _, name := range familyNames {
		f := r.families[name]
		f.mu.RLock()

		sb.WriteString(fmt.Sprintf("# HELP %s %s\n", f.Name, f.Help))
		sb.WriteString(fmt.Sprintf("# TYPE %s %s\n", f.Name, f.Type))

		if len(f.values) == 0 {
			// Print empty metric default if no observations
			sb.WriteString(fmt.Sprintf("%s 0\n", f.Name))
		} else {
			// Sort label signatures
			keys := make([]string, 0, len(f.values))
			for k := range f.values {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, k := range keys {
				val := f.values[k]
				labels := f.labels[k]
				if len(labels) == 0 {
					sb.WriteString(fmt.Sprintf("%s %g\n", f.Name, val))
				} else {
					var lParts []string
					lKeys := make([]string, 0, len(labels))
					for lk := range labels {
						lKeys = append(lKeys, lk)
					}
					sort.Strings(lKeys)
					for _, lk := range lKeys {
						lParts = append(lParts, fmt.Sprintf("%s=\"%s\"", lk, labels[lk]))
					}
					sb.WriteString(fmt.Sprintf("%s{%s} %g\n", f.Name, strings.Join(lParts, ","), val))
				}
			}
		}
		f.mu.RUnlock()
	}

	return sb.String()
}

// HTTPHandler returns an http.HandlerFunc that serves the Prometheus metrics endpoint.
func (r *Registry) HTTPHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(r.FormatPrometheus()))
	}
}

// §22.1 Minimum Metric Set — Singletons bound to DefaultRegistry verbatim
var (
	// 1. worker_heartbeat_age_seconds
	MetricWorkerHeartbeatAge = DefaultRegistry.RegisterGauge(
		"worker_heartbeat_age_seconds",
		"Time in seconds elapsed since last received heartbeat from worker",
	)

	// 2. worker_cpu_percent
	MetricWorkerCPUPercent = DefaultRegistry.RegisterGauge(
		"worker_cpu_percent",
		"Current CPU utilization percentage on the worker host",
	)

	// 3. worker_memory_percent
	MetricWorkerMemoryPercent = DefaultRegistry.RegisterGauge(
		"worker_memory_percent",
		"Current memory utilization percentage on the worker host",
	)

	// 4. deployment_duration_seconds
	MetricDeploymentDurationSeconds = DefaultRegistry.RegisterHistogram(
		"deployment_duration_seconds",
		"Total duration in seconds for deployment lifecycle completion",
	)

	// 5. deployment_status_total
	MetricDeploymentStatusTotal = DefaultRegistry.RegisterCounter(
		"deployment_status_total",
		"Cumulative count of deployments partitioned by terminal or active status",
	)

	// 6. container_restart_total
	MetricContainerRestartTotal = DefaultRegistry.RegisterCounter(
		"container_restart_total",
		"Cumulative count of container restarts on workers",
	)

	// 7. scheduler_placement_total
	MetricSchedulerPlacementTotal = DefaultRegistry.RegisterCounter(
		"scheduler_placement_total",
		"Cumulative count of scheduler placement decisions",
	)

	// 8. image_pull_failure_total
	MetricImagePullFailureTotal = DefaultRegistry.RegisterCounter(
		"image_pull_failure_total",
		"Cumulative count of image pull failures encountered by workers",
	)
)

// ResetAll resets all metrics in the registry to zero (useful in tests).
func (r *Registry) ResetAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, f := range r.families {
		f.mu.Lock()
		f.values = make(map[string]float64)
		f.labels = make(map[string]map[string]string)
		f.mu.Unlock()
	}
}
