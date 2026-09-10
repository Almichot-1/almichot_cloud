package workers

import (
	"fmt"
	"time"
)

type WorkerState string

const (
	StateUnregistered WorkerState = "UNREGISTERED"
	StateReady        WorkerState = "READY"
	StateDraining     WorkerState = "DRAINING"
	StateDown         WorkerState = "DOWN"
)

type WorkerHealth string

const (
	HealthUnknown     WorkerHealth = "UNKNOWN"
	HealthHealthy     WorkerHealth = "HEALTHY"
	HealthSuspected   WorkerHealth = "SUSPECTED"
	HealthUnhealthy   WorkerHealth = "UNHEALTHY"
	HealthUnreachable WorkerHealth = "UNREACHABLE"
)

// Worker represents a registered compute worker in the Nebula cluster.
type Worker struct {
	ID                string            `json:"id"`
	WorkerKey         string            `json:"worker_key"`
	Hostname          string            `json:"hostname"`
	IPAddress         string            `json:"ip_address"`
	GRPCPort          int               `json:"grpc_port"`
	Capacity          int               `json:"capacity"`
	Labels            map[string]string `json:"labels"`
	State             WorkerState       `json:"state"`
	Health            WorkerHealth      `json:"health"`
	Schedulable       bool              `json:"schedulable"`
	ActiveWorkloads   int               `json:"active_workloads"`
	LastBeatAt        *time.Time        `json:"last_beat_at,omitempty"`
	ConsecutiveBeats  int               `json:"consecutive_beats"`
	ConsecutiveMisses int               `json:"consecutive_misses"`
	IsPartitioned     bool              `json:"is_partitioned"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
}


// AvailableCapacity returns how many additional workloads this worker can accept.
func (w *Worker) AvailableCapacity() int {
	available := w.Capacity - w.ActiveWorkloads
	if available < 0 {
		return 0
	}
	return available
}

// Utilization returns the fraction of capacity currently in use (0.0 to 1.0+).
func (w *Worker) Utilization() float64 {
	if w.Capacity <= 0 {
		return 1.0
	}
	return float64(w.ActiveWorkloads) / float64(w.Capacity)
}

// Address returns the host:port string for connecting to this worker's gRPC agent.
func (w *Worker) Address() string {
	if w.GRPCPort == 0 {
		return w.IPAddress
	}
	return fmt.Sprintf("%s:%d", w.IPAddress, w.GRPCPort)
}

const (
	CapabilityBuild   = "build"
	CapabilityRuntime = "runtime"
)

// Capability returns the primary capability tag of the worker (default: "runtime") (§9.3, Phase 12).
func (w *Worker) Capability() string {
	if w.Labels != nil {
		if capVal, ok := w.Labels["capability"]; ok && capVal != "" {
			return capVal
		}
	}
	return CapabilityRuntime
}

// IsBuildWorker returns true if this worker has capability=build (§9.3, Phase 12).
func (w *Worker) IsBuildWorker() bool {
	return w.Capability() == CapabilityBuild
}

// IsRuntimeWorker returns true if this worker is dedicated to running container workloads.
func (w *Worker) IsRuntimeWorker() bool {
	return !w.IsBuildWorker()
}
