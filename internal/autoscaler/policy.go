package autoscaler

import (
	"math"
	"time"
)

// MetricType identifies the trigger metric for scaling decisions (§18).
type MetricType string

const (
	MetricCPU MetricType = "CPU"
	MetricRPS MetricType = "RPS"
)

// ScalingAction represents the action recommended by the autoscaler.
type ScalingAction string

const (
	ActionNone      ScalingAction = "NONE"
	ActionScaleUp   ScalingAction = "SCALE_UP"
	ActionScaleDown ScalingAction = "SCALE_DOWN"
)

// ScalingPolicy configures the autoscaler behavior for an application/project (§18).
type ScalingPolicy struct {
	ProjectID          string        `json:"project_id"`
	MetricType         MetricType    `json:"metric_type"`
	TargetValue        float64       `json:"target_value"`               // e.g. 70.0 for 70% CPU, 100.0 for 100 RPS
	Tolerance          float64       `json:"tolerance"`                  // deadband tolerance (default 0.10 for 10%)
	MinReplicas        int           `json:"min_replicas"`               // minimum replica floor (never scale below this)
	MaxReplicas        int           `json:"max_replicas"`               // maximum replica ceiling
	ScaleUpCooldown    time.Duration `json:"scale_up_cooldown"`          // cooldown window after scale-up
	ScaleDownCooldown  time.Duration `json:"scale_down_cooldown"`        // cooldown window after scale-down
}

// DefaultPolicy returns a reasonable default scaling policy for an application.
func DefaultPolicy(projectID string) *ScalingPolicy {
	return &ScalingPolicy{
		ProjectID:         projectID,
		MetricType:        MetricCPU,
		TargetValue:       70.0,
		Tolerance:         0.10,
		MinReplicas:       1,
		MaxReplicas:       10,
		ScaleUpCooldown:   60 * time.Second,
		ScaleDownCooldown: 180 * time.Second,
	}
}

// ScalingDecision holds the evaluated result of a scaling check.
type ScalingDecision struct {
	ProjectID       string        `json:"project_id"`
	CurrentReplicas int           `json:"current_replicas"`
	DesiredReplicas int           `json:"desired_replicas"`
	MetricType      MetricType    `json:"metric_type"`
	MetricValue     float64       `json:"metric_value"`
	TargetValue     float64       `json:"target_value"`
	Action          ScalingAction `json:"action"`
	Reason          string        `json:"reason"`
	EvaluatedAt     time.Time     `json:"evaluated_at"`
}

// CalculateDesiredReplicas computes target replica count from current replicas and metric value (§18).
// Enforces minimum replica floor, maximum ceiling, and deadband tolerance.
func CalculateDesiredReplicas(
	currentReplicas int,
	metricValue float64,
	targetValue float64,
	minReplicas int,
	maxReplicas int,
	tolerance float64,
) (int, ScalingAction, string) {
	if minReplicas <= 0 {
		minReplicas = 1
	}
	if maxReplicas < minReplicas {
		maxReplicas = minReplicas
	}
	if currentReplicas <= 0 {
		currentReplicas = minReplicas
	}
	if targetValue <= 0 {
		return currentReplicas, ActionNone, "invalid target value <= 0"
	}
	if tolerance <= 0 {
		tolerance = 0.10
	}

	ratio := metricValue / targetValue

	// Deadband tolerance check (§18): within tolerance band, produce no scaling action
	if math.Abs(ratio-1.0) <= tolerance {
		return currentReplicas, ActionNone, "metric within tolerance deadband"
	}

	if ratio > 1.0 {
		// Scale Up: desired = ceil(current * ratio)
		desired := int(math.Ceil(float64(currentReplicas) * ratio))
		if desired <= currentReplicas {
			desired = currentReplicas + 1
		}
		if desired > maxReplicas {
			desired = maxReplicas
		}
		if desired == currentReplicas {
			return currentReplicas, ActionNone, "already at maximum replica ceiling"
		}
		return desired, ActionScaleUp, "metric exceeded target threshold"
	}

	// Scale Down: desired = floor(current * ratio)
	desired := int(math.Floor(float64(currentReplicas) * ratio))
	if desired >= currentReplicas {
		desired = currentReplicas - 1
	}
	// Enforce minimum replica floor (§18.2, Gate G-32)
	if desired < minReplicas {
		desired = minReplicas
	}
	if desired == currentReplicas {
		return currentReplicas, ActionNone, "already at minimum replica floor"
	}
	return desired, ActionScaleDown, "metric dropped below target threshold"
}
