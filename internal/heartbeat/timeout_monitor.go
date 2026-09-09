package heartbeat

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nebula/nebula/internal/discovery"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

// HealthTransition records a health state change of a worker.
type HealthTransition struct {
	WorkerID    string               `json:"worker_id"`
	WorkerKey   string               `json:"worker_key"`
	OldHealth   workers.WorkerHealth `json:"old_health"`
	NewHealth   workers.WorkerHealth `json:"new_health"`
	MissedBeats int                  `json:"missed_beats"`
	Timestamp   time.Time            `json:"timestamp"`
}

// TimeoutMonitor scans worker heartbeats and drives the health state machine (WA-09, G-12).
type TimeoutMonitor struct {
	registry        *workers.Registry
	serviceRegistry *discovery.ServiceRegistry
	cfg             workers.HealthStateMachineConfig
	log             zerolog.Logger

	mu     sync.Mutex
	stopCh chan struct{}
}

// NewTimeoutMonitor creates a new TimeoutMonitor.
func NewTimeoutMonitor(
	registry *workers.Registry,
	serviceRegistry *discovery.ServiceRegistry,
	cfg workers.HealthStateMachineConfig,
	log zerolog.Logger,
) *TimeoutMonitor {
	return &TimeoutMonitor{
		registry:        registry,
		serviceRegistry: serviceRegistry,
		cfg:             cfg,
		log:             log.With().Str("component", "heartbeat-monitor").Logger(),
		stopCh:          make(chan struct{}),
	}
}

// SetConfig updates monitoring thresholds.
func (m *TimeoutMonitor) SetConfig(cfg workers.HealthStateMachineConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg = cfg
}

// CheckWorkers evaluates all registered workers against heartbeat timeout thresholds (WA-09).
// If a worker transitions to UNHEALTHY:
// - Its health state is updated in the registry.
// - All its endpoints are evicted from service discovery & load balancer (G-10, G-12).
func (m *TimeoutMonitor) CheckWorkers(ctx context.Context, now time.Time) []HealthTransition {
	m.mu.Lock()
	cfg := m.cfg
	m.mu.Unlock()

	allWorkers := m.registry.List()
	var transitions []HealthTransition

	for _, w := range allWorkers {
		// If worker is marked partitioned, maintain UNREACHABLE state without auto-healthy flip
		if w.IsPartitioned || w.Health == workers.HealthUnreachable {
			continue
		}

		if w.LastBeatAt == nil {
			continue
		}

		elapsed := now.Sub(*w.LastBeatAt)
		newHealth, missed := workers.EvaluateHealthTransition(w.Health, elapsed, cfg)

		if newHealth != w.Health {
			oldHealth := w.Health
			reason := fmt.Sprintf("missed %d heartbeats (elapsed: %v)", missed, elapsed)
			_, _ = m.registry.SetHealthWithReason(ctx, w.WorkerKey, newHealth, reason)

			// G-10 & G-12: When worker becomes UNHEALTHY, immediately evict endpoints from service discovery & router
			if newHealth == workers.HealthUnhealthy && m.serviceRegistry != nil {
				m.serviceRegistry.EvictWorkerEndpoints(ctx, w.ID)
				m.serviceRegistry.EvictWorkerEndpoints(ctx, w.WorkerKey)
				m.log.Warn().
					Str("worker_key", w.WorkerKey).
					Msg("worker marked UNHEALTHY; endpoints evicted from load balancer routing (G-10, G-12)")
			}

			transition := HealthTransition{
				WorkerID:    w.ID,
				WorkerKey:   w.WorkerKey,
				OldHealth:   oldHealth,
				NewHealth:   newHealth,
				MissedBeats: missed,
				Timestamp:   now,
			}
			transitions = append(transitions, transition)

			m.log.Warn().
				Str("worker_key", w.WorkerKey).
				Str("from", string(oldHealth)).
				Str("to", string(newHealth)).
				Int("missed_beats", missed).
				Msg("worker health transition recorded")
		}
	}

	return transitions
}

// Start runs the periodic heartbeat timeout monitor.
func (m *TimeoutMonitor) Start(ctx context.Context) {
	go func() {
		m.mu.Lock()
		checkInterval := m.cfg.HeartbeatInterval / 2
		if checkInterval < 10*time.Millisecond {
			checkInterval = 100 * time.Millisecond
		}
		m.mu.Unlock()

		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-m.stopCh:
				return
			case t := <-ticker.C:
				m.CheckWorkers(ctx, t)
			}
		}
	}()
}

// Stop stops the background monitor.
func (m *TimeoutMonitor) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	select {
	case <-m.stopCh:
	default:
		close(m.stopCh)
	}
}
