package reconcile

import (
	"strings"
	"time"
)

// DesiredInstance represents the state an instance should be in according to Postgres.
type DesiredInstance struct {
	InstanceID    string            `json:"instance_id"`
	InstanceKey   string            `json:"instance_key"`
	DeploymentID  string            `json:"deployment_id"`
	WorkerID      string            `json:"worker_id"`
	Image         string            `json:"image"`
	Env           map[string]string `json:"env"`
	Labels        map[string]string `json:"labels"`
	DesiredStatus string            `json:"desired_status"` // e.g. "RUNNING"
}

// ObservedContainer represents an actual container found on a worker agent.
type ObservedContainer struct {
	WorkerID     string            `json:"worker_id"`
	WorkerKey    string            `json:"worker_key"`
	ContainerID  string            `json:"container_id"`
	InstanceKey  string            `json:"instance_key"`
	DeploymentID string            `json:"deployment_id"`
	Image        string            `json:"image"`
	State        string            `json:"state"` // "running", "exited", etc.
	Labels       map[string]string `json:"labels"`
	CreatedAt    time.Time         `json:"created_at"`
}

// MatchingPair pairs an active desired instance with its matching observed running container.
type MatchingPair struct {
	Desired  DesiredInstance   `json:"desired"`
	Observed ObservedContainer `json:"observed"`
}

// DiffResult holds the computed classification between desired and observed states.
type DiffResult struct {
	Matching []MatchingPair      `json:"matching"`
	Missing  []DesiredInstance   `json:"missing"`
	Extra    []ObservedContainer `json:"extra"`
}

// IsRunningState checks if container state indicates it's active without heap allocations.
func IsRunningState(state string) bool {
	if state == "running" || state == "RUNNING" || state == "Running" {
		return true
	}
	if len(state) >= 2 && (state[0] == 'u' || state[0] == 'U') && (state[1] == 'p' || state[1] == 'P') {
		return true
	}
	return strings.EqualFold(state, "running")
}

// ComputeDiff calculates the delta between desired instances and observed containers.
// Complexity: O(N + M) hash-indexed lookup, guaranteeing sub-second evaluation for thousands of instances (PERF-01).
func ComputeDiff(desired []DesiredInstance, observed []ObservedContainer) DiffResult {
	result := DiffResult{
		Matching: make([]MatchingPair, 0, len(desired)),
		Missing:  make([]DesiredInstance, 0, len(desired)/4+1),
		Extra:    make([]ObservedContainer, 0, len(observed)/4+1),
	}

	desiredIndexByKey := make(map[string]int, len(desired)*2)
	for i := range desired {
		if desired[i].InstanceKey != "" {
			desiredIndexByKey[desired[i].InstanceKey] = i
		}
		if desired[i].InstanceID != "" {
			desiredIndexByKey[desired[i].InstanceID] = i
		}
	}

	matchedIndices := make([]bool, len(desired))

	for i := range observed {
		instKey := observed[i].InstanceKey
		if instKey == "" && observed[i].Labels != nil {
			instKey = observed[i].Labels["nebula.instance_id"]
		}

		if instKey != "" {
			if idx, ok := desiredIndexByKey[instKey]; ok {
				if IsRunningState(observed[i].State) && !matchedIndices[idx] {
					result.Matching = append(result.Matching, MatchingPair{
						Desired:  desired[idx],
						Observed: observed[i],
					})
					matchedIndices[idx] = true
					continue
				}
			}
		}

		result.Extra = append(result.Extra, observed[i])
	}

	for i := range desired {
		if !matchedIndices[i] {
			result.Missing = append(result.Missing, desired[i])
		}
	}

	return result
}
