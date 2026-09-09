package reconcile

import (
	"context"
	"testing"

	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

// CP-06: Orphan classification: managed (has instance ID / labels) vs. truly unknown correctly distinguished (G-03 feeds it).
func TestCP06_OrphanClassification(t *testing.T) {
	cases := []struct {
		name     string
		ctr      ObservedContainer
		expected OrphanType
	}{
		{
			name: "Managed orphan with InstanceKey set",
			ctr: ObservedContainer{
				ContainerID: "ctr-1",
				InstanceKey: "inst-managed-1",
				Image:       "app:v1",
			},
			expected: OrphanTypeManaged,
		},
		{
			name: "Managed orphan with nebula.instance_id label",
			ctr: ObservedContainer{
				ContainerID: "ctr-2",
				Image:       "app:v1",
				Labels: map[string]string{
					"nebula.instance_id": "inst-managed-2",
				},
			},
			expected: OrphanTypeManaged,
		},
		{
			name: "Managed orphan with nebula.deployment_id label",
			ctr: ObservedContainer{
				ContainerID: "ctr-3",
				Image:       "app:v1",
				Labels: map[string]string{
					"nebula.deployment_id": "dep-managed-3",
				},
			},
			expected: OrphanTypeManaged,
		},
		{
			name: "Unknown container without any Nebula labels",
			ctr: ObservedContainer{
				ContainerID: "ctr-redis-foreign",
				Image:       "redis:7.0",
				Labels: map[string]string{
					"tier": "cache",
				},
			},
			expected: OrphanTypeUnknown,
		},
		{
			name: "Unknown container with nil labels",
			ctr: ObservedContainer{
				ContainerID: "ctr-system-agent",
				Image:       "node-exporter:latest",
			},
			expected: OrphanTypeUnknown,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual := ClassifyOrphan(tc.ctr)
			if actual != tc.expected {
				t.Fatalf("CP-06 failure for %s: expected %s, got %s", tc.name, tc.expected, actual)
			}
		})
	}

	t.Logf("CP-06 Passed: Managed orphan vs. truly unknown container correctly classified across all cases!")
}

// CP-04 (G-03): Orphan policy enforcement:
// - Managed orphan -> Stopped.
// - Unknown container -> Untouched, flagged only.
func TestCP04_G03_OrphanPolicyEnforcement(t *testing.T) {
	log := zerolog.Nop()
	mockFactory := deployments.NewMockWorkerClientFactory()
	worker := &workers.Worker{ID: "w-1", WorkerKey: "worker-1", IPAddress: "127.0.0.1"}
	client, err := mockFactory.GetClient(context.Background(), worker)
	if err != nil {
		t.Fatalf("failed to get mock client: %v", err)
	}

	ctx := context.Background()

	// 1. Managed orphan: must be STOPPED
	managedOrphan := ObservedContainer{
		WorkerID:    worker.ID,
		ContainerID: "ctr-managed-orphan",
		InstanceKey: "inst-managed-orphan",
		Image:       "app:old",
		Labels: map[string]string{
			"nebula.instance_id": "inst-managed-orphan",
		},
	}
	dec1 := EnforceOrphanPolicy(ctx, managedOrphan, client, log)
	if dec1.Type != OrphanTypeManaged {
		t.Fatalf("CP-04 failure: expected Managed orphan type, got %s", dec1.Type)
	}
	if dec1.ActionTaken != "STOPPED" {
		t.Fatalf("CP-04 failure: expected STOPPED action for managed orphan, got %s", dec1.ActionTaken)
	}
	if len(mockFactory.Stopped) != 1 {
		t.Fatalf("CP-04 failure: expected StopContainer RPC to be called once, got %d", len(mockFactory.Stopped))
	}
	if mockFactory.Stopped[0].InstanceId != "inst-managed-orphan" {
		t.Fatalf("CP-04 failure: StopContainer called with wrong ID: %s", mockFactory.Stopped[0].InstanceId)
	}

	// 2. Unknown container: must be UNTOUCHED, FLAGGED ONLY (G-03)
	unknownContainer := ObservedContainer{
		WorkerID:    worker.ID,
		ContainerID: "ctr-unmanaged-postgres",
		Image:       "postgres:16",
		Labels: map[string]string{
			"team": "dba",
		},
	}
	dec2 := EnforceOrphanPolicy(ctx, unknownContainer, client, log)
	if dec2.Type != OrphanTypeUnknown {
		t.Fatalf("CP-04 failure: expected Unknown orphan type, got %s", dec2.Type)
	}
	if dec2.ActionTaken != "FLAGGED_ONLY" {
		t.Fatalf("CP-04 failure: expected FLAGGED_ONLY action for unknown container, got %s", dec2.ActionTaken)
	}
	// Crucial check: Stopped count MUST STILL BE 1 (not incremented)!
	if len(mockFactory.Stopped) != 1 {
		t.Fatalf("CP-04 / G-03 VIOLATION: unknown container was touched/stopped! Stopped count: %d", len(mockFactory.Stopped))
	}

	t.Logf("CP-04 & Gate G-03 Passed: Managed orphan stopped; unknown container remained completely untouched and flagged only!")
}
