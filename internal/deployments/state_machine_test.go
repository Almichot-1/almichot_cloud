package deployments_test

import (
	"testing"

	"github.com/nebula/nebula/internal/deployments"
)

func TestStateMachine_LegalTransitions(t *testing.T) {
	legalEdges := [][2]deployments.DeploymentStatus{
		{deployments.StatusQueued, deployments.StatusBuilding},
		{deployments.StatusQueued, deployments.StatusScheduling},
		{deployments.StatusBuilding, deployments.StatusBuilt},
		{deployments.StatusBuilt, deployments.StatusScheduling},
		{deployments.StatusScheduling, deployments.StatusStarting},
		{deployments.StatusStarting, deployments.StatusRunning},
		{deployments.StatusRunning, deployments.StatusStopped},
		{deployments.StatusRunning, deployments.StatusRolledBack},
		{deployments.StatusRunning, deployments.StatusFailed},
	}

	for _, edge := range legalEdges {
		from, to := edge[0], edge[1]
		if err := deployments.ValidateTransition(from, to); err != nil {
			t.Errorf("expected legal transition from %s to %s, got error: %v", from, to, err)
		}
	}
}

func TestStateMachine_FailedReachableFromIntermediateStates(t *testing.T) {
	intermediateStates := []deployments.DeploymentStatus{
		deployments.StatusQueued,
		deployments.StatusBuilding,
		deployments.StatusBuilt,
		deployments.StatusScheduling,
		deployments.StatusStarting,
		deployments.StatusRunning,
	}

	for _, st := range intermediateStates {
		if err := deployments.ValidateTransition(st, deployments.StatusFailed); err != nil {
			t.Errorf("expected FAILED to be reachable from %s, got error: %v", st, err)
		}
	}
}

func TestStateMachine_StoppedIsTerminal(t *testing.T) {
	if !deployments.StatusStopped.IsTerminal() {
		t.Fatalf("expected StatusStopped to be terminal")
	}

	illegalTargets := []deployments.DeploymentStatus{
		deployments.StatusQueued,
		deployments.StatusBuilding,
		deployments.StatusBuilt,
		deployments.StatusScheduling,
		deployments.StatusStarting,
		deployments.StatusRunning,
		deployments.StatusFailed,
		deployments.StatusRolledBack,
	}

	for _, to := range illegalTargets {
		if err := deployments.ValidateTransition(deployments.StatusStopped, to); err == nil {
			t.Errorf("expected illegal transition from STOPPED to %s to fail, got nil", to)
		}
	}
}

func TestStateMachine_IllegalEdgesInvolvingBuiltAndStopped(t *testing.T) {
	illegalEdges := [][2]deployments.DeploymentStatus{
		// Illegal edges involving BUILT
		{deployments.StatusQueued, deployments.StatusBuilt},
		{deployments.StatusBuilt, deployments.StatusStarting},
		{deployments.StatusBuilt, deployments.StatusRunning},
		{deployments.StatusBuilt, deployments.StatusStopped},
		{deployments.StatusBuilt, deployments.StatusRolledBack},
		// Illegal edges involving STOPPED
		{deployments.StatusQueued, deployments.StatusStopped},
		{deployments.StatusBuilding, deployments.StatusStopped},
		{deployments.StatusBuilt, deployments.StatusStopped},
		{deployments.StatusScheduling, deployments.StatusStopped},
		{deployments.StatusStarting, deployments.StatusStopped},
		{deployments.StatusStopped, deployments.StatusRunning},
		// Terminal states cannot transition out
		{deployments.StatusFailed, deployments.StatusRunning},
		{deployments.StatusRolledBack, deployments.StatusRunning},
	}

	for _, edge := range illegalEdges {
		from, to := edge[0], edge[1]
		if err := deployments.ValidateTransition(from, to); err == nil {
			t.Errorf("expected transition from %s to %s to be rejected as illegal, but it passed", from, to)
		}
	}
}
