package reconcile

import (
	"fmt"
	"testing"
	"time"
)

// CP-05: Diff correctly classifies missing / extra / matching instances across mixed scenarios.
func TestCP05_DiffLogicCorrectness(t *testing.T) {
	desired := []DesiredInstance{
		{
			InstanceID:    "inst-1",
			InstanceKey:   "inst-key-1",
			DeploymentID:  "dep-1",
			WorkerID:      "worker-1",
			Image:         "app:v1",
			DesiredStatus: "RUNNING",
		},
		{
			InstanceID:    "inst-2",
			InstanceKey:   "inst-key-2",
			DeploymentID:  "dep-1",
			WorkerID:      "worker-1",
			Image:         "app:v1",
			DesiredStatus: "RUNNING",
		},
		{
			InstanceID:    "inst-3",
			InstanceKey:   "inst-key-3",
			DeploymentID:  "dep-2",
			WorkerID:      "worker-2",
			Image:         "worker-app:v2",
			DesiredStatus: "RUNNING",
		},
	}

	observed := []ObservedContainer{
		// 1. Matches inst-key-1
		{
			WorkerID:    "worker-1",
			ContainerID: "ctr-inst-1",
			InstanceKey: "inst-key-1",
			Image:       "app:v1",
			State:       "running",
		},
		// 2. Extra managed orphan (old instance no longer in desired state)
		{
			WorkerID:    "worker-1",
			ContainerID: "ctr-old-orphan",
			InstanceKey: "inst-key-old",
			Image:       "app:v0",
			State:       "running",
			Labels: map[string]string{
				"nebula.instance_id":   "inst-key-old",
				"nebula.deployment_id": "dep-old",
			},
		},
		// 3. Extra unmanaged / third-party container (no nebula labels)
		{
			WorkerID:    "worker-2",
			ContainerID: "ctr-postgres-host",
			InstanceKey: "",
			Image:       "postgres:16",
			State:       "running",
			Labels:      map[string]string{"env": "local"},
		},
		// Notice: inst-key-2 and inst-key-3 are missing from observed containers!
	}

	diff := ComputeDiff(desired, observed)

	// Verify Matching
	if len(diff.Matching) != 1 {
		t.Fatalf("CP-05 failure: expected 1 matching pair, got %d", len(diff.Matching))
	}
	if diff.Matching[0].Desired.InstanceKey != "inst-key-1" {
		t.Fatalf("CP-05 failure: expected matched key inst-key-1, got %s", diff.Matching[0].Desired.InstanceKey)
	}

	// Verify Missing
	if len(diff.Missing) != 2 {
		t.Fatalf("CP-05 failure: expected 2 missing instances, got %d", len(diff.Missing))
	}
	missingKeys := make(map[string]bool)
	for _, m := range diff.Missing {
		missingKeys[m.InstanceKey] = true
	}
	if !missingKeys["inst-key-2"] || !missingKeys["inst-key-3"] {
		t.Fatalf("CP-05 failure: missing keys must contain inst-key-2 and inst-key-3, got %+v", missingKeys)
	}

	// Verify Extra
	if len(diff.Extra) != 2 {
		t.Fatalf("CP-05 failure: expected 2 extra containers, got %d", len(diff.Extra))
	}

	t.Logf("CP-05 Passed: Diff correctly classified 1 matching, 2 missing, 2 extra containers across mixed scenarios!")
}

// PERF-01: Diff computation stays sub-second at expected MVP scale (N=5,000 instances).
func TestPERF01_ReconcileDiffScaleAcceptable(t *testing.T) {
	const N = 5000

	desired := make([]DesiredInstance, N)
	observed := make([]ObservedContainer, N)

	for i := 0; i < N; i++ {
		key := fmt.Sprintf("inst-scale-%06d", i)
		desired[i] = DesiredInstance{
			InstanceID:    fmt.Sprintf("id-%06d", i),
			InstanceKey:   key,
			DeploymentID:  "dep-scale",
			WorkerID:      fmt.Sprintf("worker-%d", i%10),
			Image:         "scale-test:latest",
			DesiredStatus: "RUNNING",
		}

		// Half match, 25% missing, 25% extra
		if i < N*3/4 {
			observed[i] = ObservedContainer{
				WorkerID:    fmt.Sprintf("worker-%d", i%10),
				ContainerID: fmt.Sprintf("ctr-%06d", i),
				InstanceKey: key,
				Image:       "scale-test:latest",
				State:       "running",
			}
		} else {
			// Extra containers
			observed[i] = ObservedContainer{
				WorkerID:    fmt.Sprintf("worker-%d", i%10),
				ContainerID: fmt.Sprintf("ctr-extra-%06d", i),
				InstanceKey: fmt.Sprintf("foreign-inst-%06d", i),
				Image:       "foreign:latest",
				State:       "running",
			}
		}
	}

	start := time.Now()
	diff := ComputeDiff(desired, observed)
	duration := time.Since(start)

	t.Logf("PERF-01: Diff for N=%d instances completed in %v", N, duration)

	if duration > 500*time.Millisecond {
		t.Fatalf("PERF-01 failure: diff took %v, exceeded target threshold of 500ms (sub-second)", duration)
	}

	expectedMatches := N * 3 / 4
	if len(diff.Matching) != expectedMatches {
		t.Fatalf("PERF-01 failure: expected %d matches, got %d", expectedMatches, len(diff.Matching))
	}
	expectedMissing := N / 4
	if len(diff.Missing) != expectedMissing {
		t.Fatalf("PERF-01 failure: expected %d missing, got %d", expectedMissing, len(diff.Missing))
	}

	t.Logf("PERF-01 Passed: Diff cost scales sub-second (took %v for %d items, well under 500ms threshold)!", duration, N)
}
