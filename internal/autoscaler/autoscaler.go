package autoscaler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/projects"
	"github.com/rs/zerolog"
)

// ErrMetricsUnavailable is returned when the metrics feed cannot provide data.
var ErrMetricsUnavailable = errors.New("metrics feed unavailable")

// MetricsProvider abstracts retrieving live application performance metrics (CPU, RPS) (§18).
type MetricsProvider interface {
	GetMetric(ctx context.Context, projectID string, metric MetricType) (float64, error)
}

// MemoryMetricsProvider is an in-memory test double for metrics feeds.
type MemoryMetricsProvider struct {
	mu      sync.RWMutex
	metrics map[string]map[MetricType]float64
	err     error
}

// NewMemoryMetricsProvider creates a new in-memory metrics provider.
func NewMemoryMetricsProvider() *MemoryMetricsProvider {
	return &MemoryMetricsProvider{
		metrics: make(map[string]map[MetricType]float64),
	}
}

// SetMetric records a metric value for a project.
func (p *MemoryMetricsProvider) SetMetric(projectID string, metric MetricType, val float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.metrics[projectID] == nil {
		p.metrics[projectID] = make(map[MetricType]float64)
	}
	normMetric := MetricType(strings.ToUpper(string(metric)))
	p.metrics[projectID][normMetric] = val
}

// SetError forces an error condition (simulating outage).
func (p *MemoryMetricsProvider) SetError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

// GetMetric returns the current metric value.
func (p *MemoryMetricsProvider) GetMetric(ctx context.Context, projectID string, metric MetricType) (float64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.err != nil {
		return 0, p.err
	}
	if p.metrics[projectID] == nil {
		return 0, ErrMetricsUnavailable
	}
	normMetric := MetricType(strings.ToUpper(string(metric)))
	val, ok := p.metrics[projectID][normMetric]
	if !ok {
		return 0, ErrMetricsUnavailable
	}
	return val, nil
}

// Autoscaler evaluates scaling policies against metrics and dispatches scale adjustments (§18).
type Autoscaler struct {
	mu            sync.RWMutex
	policies      map[string]*ScalingPolicy
	metrics       MetricsProvider
	svc           *deployments.Service
	projects      projects.ProjectRepository
	eventRepo     deployments.EventRepository
	lastScaleUp   map[string]time.Time
	lastScaleDown map[string]time.Time
	log           zerolog.Logger
}

// NewAutoscaler creates an Autoscaler instance.
func NewAutoscaler(
	metrics MetricsProvider,
	svc *deployments.Service,
	projectRepo projects.ProjectRepository,
	eventRepo deployments.EventRepository,
	log zerolog.Logger,
) *Autoscaler {
	return &Autoscaler{
		policies:      make(map[string]*ScalingPolicy),
		metrics:       metrics,
		svc:           svc,
		projects:      projectRepo,
		eventRepo:     eventRepo,
		lastScaleUp:   make(map[string]time.Time),
		lastScaleDown: make(map[string]time.Time),
		log:           log.With().Str("component", "autoscaler").Logger(),
	}
}

// SetPolicy sets or updates the scaling policy for a project.
func (a *Autoscaler) SetPolicy(policy *ScalingPolicy) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if policy == nil || policy.ProjectID == "" {
		return
	}
	clone := *policy
	clone.MetricType = MetricType(strings.ToUpper(string(clone.MetricType)))
	a.policies[policy.ProjectID] = &clone
}

// GetPolicy retrieves the scaling policy for a project.
func (a *Autoscaler) GetPolicy(projectID string) (*ScalingPolicy, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	p, ok := a.policies[projectID]
	if !ok {
		return nil, false
	}
	clone := *p
	return &clone, true
}

// SetLastScaleTimes allows overriding scale timestamps for testing cooldowns (§18.2).
func (a *Autoscaler) SetLastScaleTimes(projectID string, up, down time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !up.IsZero() {
		a.lastScaleUp[projectID] = up
	}
	if !down.IsZero() {
		a.lastScaleDown[projectID] = down
	}
}

// Evaluate evaluates current metrics against policy without modifying deployment state (§18).
func (a *Autoscaler) Evaluate(ctx context.Context, projectID string) (*ScalingDecision, error) {
	a.mu.RLock()
	policy, hasPolicy := a.policies[projectID]
	lastUp := a.lastScaleUp[projectID]
	lastDown := a.lastScaleDown[projectID]
	a.mu.RUnlock()

	now := time.Now().UTC()

	if !hasPolicy {
		return &ScalingDecision{
			ProjectID:   projectID,
			Action:      ActionNone,
			Reason:      "no scaling policy configured",
			EvaluatedAt: now,
		}, nil
	}

	// 1. Fetch current active running deployment for project
	deps, err := a.svc.ListDeployments(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to list deployments: %w", err)
	}

	var activeDep *deployments.Deployment
	for _, d := range deps {
		if d.Status == deployments.StatusRunning {
			activeDep = d
			break
		}
	}

	if activeDep == nil {
		return &ScalingDecision{
			ProjectID:   projectID,
			Action:      ActionNone,
			Reason:      "no running deployment found for project",
			EvaluatedAt: now,
		}, nil
	}

	currentReplicas := activeDep.InstanceCount
	if currentReplicas <= 0 {
		currentReplicas = 1
	}

	// 2. Fetch live metrics from provider (Failure-mode: fail-safe on outage)
	metricVal, err := a.metrics.GetMetric(ctx, projectID, policy.MetricType)
	if err != nil {
		a.log.Warn().
			Err(err).
			Str("project_id", projectID).
			Str("metric_type", string(policy.MetricType)).
			Msg("metrics feed unavailable: fail-safe holding current desired replica count")
		return &ScalingDecision{
			ProjectID:       projectID,
			CurrentReplicas: currentReplicas,
			DesiredReplicas: currentReplicas,
			MetricType:      policy.MetricType,
			Action:          ActionNone,
			Reason:          fmt.Sprintf("metrics feed unavailable (%v): holding last known count", err),
			EvaluatedAt:     now,
		}, nil
	}

	// 3. Compute desired replica count (§18)
	desired, action, reason := CalculateDesiredReplicas(
		currentReplicas,
		metricVal,
		policy.TargetValue,
		policy.MinReplicas,
		policy.MaxReplicas,
		policy.Tolerance,
	)

	// 4. Enforce cooldown windows (§18.2, Gate G-32)
	if action == ActionScaleUp {
		if !lastUp.IsZero() && time.Since(lastUp) < policy.ScaleUpCooldown {
			action = ActionNone
			desired = currentReplicas
			reason = fmt.Sprintf("scale-up suppressed: inside cooldown window (last scaled %v ago, cooldown %v)",
				time.Since(lastUp).Round(time.Second), policy.ScaleUpCooldown)
		}
	} else if action == ActionScaleDown {
		// Cooldown check for scale-down: check both last scale-down and last scale-up
		if !lastDown.IsZero() && time.Since(lastDown) < policy.ScaleDownCooldown {
			action = ActionNone
			desired = currentReplicas
			reason = fmt.Sprintf("scale-down suppressed: inside cooldown window (last scaled down %v ago, cooldown %v)",
				time.Since(lastDown).Round(time.Second), policy.ScaleDownCooldown)
		} else if !lastUp.IsZero() && time.Since(lastUp) < policy.ScaleDownCooldown {
			action = ActionNone
			desired = currentReplicas
			reason = fmt.Sprintf("scale-down suppressed: recently scaled up (%v ago, cooldown %v)",
				time.Since(lastUp).Round(time.Second), policy.ScaleDownCooldown)
		}
	}

	return &ScalingDecision{
		ProjectID:       projectID,
		CurrentReplicas: currentReplicas,
		DesiredReplicas: desired,
		MetricType:      policy.MetricType,
		MetricValue:     metricVal,
		TargetValue:     policy.TargetValue,
		Action:          action,
		Reason:          reason,
		EvaluatedAt:     now,
	}, nil
}

// EvaluateAndScale evaluates policy against metrics and performs scaling adjustment if needed (§18).
// Emits explainable event feed records (Gate G-33).
func (a *Autoscaler) EvaluateAndScale(ctx context.Context, projectID string) (*ScalingDecision, error) {
	decision, err := a.Evaluate(ctx, projectID)
	if err != nil {
		return nil, err
	}

	if decision.Action == ActionNone || decision.DesiredReplicas == decision.CurrentReplicas {
		return decision, nil
	}

	// Fetch active running deployment
	deps, err := a.svc.ListDeployments(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("list deployments: %w", err)
	}

	var activeDep *deployments.Deployment
	for _, d := range deps {
		if d.Status == deployments.StatusRunning {
			activeDep = d
			break
		}
	}
	if activeDep == nil {
		return decision, nil
	}

	// Trigger real placement/adjustment through the existing Scheduler code path
	scaledDep, _, err := a.svc.ScaleDeployment(ctx, activeDep.ID, decision.DesiredReplicas)
	if err != nil {
		return decision, fmt.Errorf("scale deployment failed: %w", err)
	}

	// Update scale timestamps
	now := time.Now().UTC()
	a.mu.Lock()
	if decision.Action == ActionScaleUp {
		a.lastScaleUp[projectID] = now
	} else if decision.Action == ActionScaleDown {
		a.lastScaleDown[projectID] = now
	}
	a.mu.Unlock()

	// Update project record if repo available
	if a.projects != nil {
		_ = a.projects.UpdateScale(ctx, projectID, decision.DesiredReplicas, 0, 0)
	}

	// Emit explainable autoscaler event (Gate G-33 & §19.1)
	if a.eventRepo != nil {
		evType := "AUTOSCALE_UP"
		if decision.Action == ActionScaleDown {
			evType = "AUTOSCALE_DOWN"
		}
		ev := &deployments.Event{
			ID:           uuid.New().String(),
			ProjectID:    projectID,
			DeploymentID: scaledDep.ID,
			EventType:    evType,
			Message: fmt.Sprintf("Autoscaler scaled %s from %d to %d replicas (%s=%.1f, target=%.1f)",
				projectID, decision.CurrentReplicas, decision.DesiredReplicas, decision.MetricType, decision.MetricValue, decision.TargetValue),
			Metadata: map[string]interface{}{
				"previous_replicas": decision.CurrentReplicas,
				"desired_replicas":  decision.DesiredReplicas,
				"metric_type":       string(decision.MetricType),
				"metric_value":      decision.MetricValue,
				"target_value":      decision.TargetValue,
				"reason":            decision.Reason,
				"timestamp":         now,
			},
			CreatedAt: now,
		}
		if cErr := a.eventRepo.Create(ctx, ev); cErr != nil {
			a.log.Error().Err(cErr).Msg("failed to record autoscaler event")
		}
	}

	a.log.Info().
		Str("project_id", projectID).
		Str("action", string(decision.Action)).
		Int("previous", decision.CurrentReplicas).
		Int("desired", decision.DesiredReplicas).
		Float64("metric", decision.MetricValue).
		Msg("autoscaler action executed successfully (§18)")

	return decision, nil
}
