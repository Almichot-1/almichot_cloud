package deployments

import (
	"fmt"
)

// ErrIllegalTransition is returned when attempting an invalid state transition (§5).
type ErrIllegalTransition struct {
	From DeploymentStatus
	To   DeploymentStatus
}

func (e *ErrIllegalTransition) Error() string {
	return fmt.Sprintf("illegal deployment state transition from %s to %s", e.From, e.To)
}

// legalTransitions defines valid lifecycle transitions for the deployment state machine (§5, §8.2).
// Progression:
// QUEUED -> BUILDING -> BUILT -> SCHEDULING -> STARTING -> RUNNING
// Direct image deploy skips build: QUEUED -> SCHEDULING
// FAILED is reachable from all non-terminal states.
// STOPPED and ROLLED_BACK are terminal states reachable from RUNNING.
var legalTransitions = map[DeploymentStatus]map[DeploymentStatus]bool{
	StatusQueued: {
		StatusBuilding:   true,
		StatusScheduling: true,
		StatusFailed:     true,
	},
	StatusBuilding: {
		StatusBuilt:  true,
		StatusFailed: true,
	},
	StatusBuilt: {
		StatusScheduling: true,
		StatusFailed:     true,
	},
	StatusScheduling: {
		StatusStarting: true,
		StatusFailed:   true,
	},
	StatusStarting: {
		StatusRunning: true,
		StatusFailed:  true,
	},
	StatusRunning: {
		StatusStopped:    true,
		StatusFailed:     true,
		StatusRolledBack: true,
	},
	StatusFailed:     {}, // Terminal (§8.2)
	StatusStopped:    {}, // Terminal (§8.2)
	StatusRolledBack: {}, // Terminal (§8.2)
}

// ValidateTransition checks if transitioning from one status to another is legal according to §5 and §8.2.
func ValidateTransition(from, to DeploymentStatus) error {
	if from == to {
		return nil
	}
	allowed, ok := legalTransitions[from]
	if !ok || !allowed[to] {
		return &ErrIllegalTransition{From: from, To: to}
	}
	return nil
}
