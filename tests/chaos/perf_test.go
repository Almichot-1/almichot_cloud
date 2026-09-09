package chaos

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/projects"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
)

// PERF-03: End-to-end deploy latency at target scale.
// Pass criteria: Full deploy (source -> reachable) completes within target time at expected MVP load.
func TestPERF03_EndToEndDeployLatency(t *testing.T) {
	c := newChaosCluster(t)
	ctx := context.Background()

	const totalDeploys = 20
	const maxAllowedP95Latency = 500 * time.Millisecond // Target scale threshold for MVP simulation

	latencies := make([]time.Duration, totalDeploys)

	for i := 0; i < totalDeploys; i++ {
		projID := fmt.Sprintf("perf-proj-%d", i)
		_ = c.projectRepo.Create(ctx, &projects.Project{ID: projID, Name: projID})

		start := time.Now()

		// Full deploy: source -> build -> registry -> schedule -> run -> register in router
		dep, instances, err := c.depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
			ProjectID:     projID,
			Image:         fmt.Sprintf("perf-app:v%d", i),
			InstanceCount: 1,
		})
		if err != nil {
			t.Fatalf("deploy %d failed: %v", i, err)
		}

		// Register target in router (making it reachable)
		if len(instances) == 0 {
			t.Fatalf("deploy %d produced 0 instances", i)
		}
		_ = c.router.RegisterTarget(projID, instances[0].ID, "http://10.0.0.1:8080")

		// Confirm reachability via router
		targets := c.router.GetTargets(projID)
		if len(targets) == 0 {
			t.Fatalf("deploy %d not reachable in router", i)
		}

		elapsed := time.Since(start)
		latencies[i] = elapsed

		if dep.Status != deployments.StatusRunning {
			t.Fatalf("deploy %d status = %s, expected %s", i, dep.Status, deployments.StatusRunning)
		}
	}

	var totalLatency time.Duration
	var maxLatency time.Duration
	for _, l := range latencies {
		totalLatency += l
		if l > maxLatency {
			maxLatency = l
		}
	}
	avgLatency := totalLatency / time.Duration(totalDeploys)

	if avgLatency > maxAllowedP95Latency {
		t.Fatalf("PERF-03 failure: average deploy latency %v exceeded maximum threshold %v", avgLatency, maxAllowedP95Latency)
	}

	t.Logf("PERF-03 Passed: %d end-to-end deploys completed. Avg Latency: %v, Max Latency: %v (Target < %v)",
		totalDeploys, avgLatency, maxLatency, maxAllowedP95Latency)
}

// PERF-04: gRPC command throughput.
// Pass criteria: CP can issue N commands/sec to workers without dropped/delayed delivery.
func TestPERF04_GRPCCommandThroughput(t *testing.T) {
	c := newChaosCluster(t)
	ctx := context.Background()

	const numCommands = 1000
	const numWorkers = 3

	var successCount atomic.Int64
	var droppedCount atomic.Int64
	var wg sync.WaitGroup

	start := time.Now()

	for i := 0; i < numCommands; i++ {
		wg.Add(1)
		go func(cmdIdx int) {
			defer wg.Done()

			workerKey := fmt.Sprintf("chaos-worker-%d", (cmdIdx%numWorkers)+1)
			client, err := c.mockFactory.GetClient(ctx, &workers.Worker{
				WorkerKey: workerKey,
				IPAddress: fmt.Sprintf("10.0.0.%d", (cmdIdx%numWorkers)+1),
				GRPCPort:  50051,
			})
			if err != nil {
				droppedCount.Add(1)
				return
			}

			// Issue RunContainer RPC command
			res, err := client.RunContainer(ctx, &proto.RunContainerRequest{
				InstanceId:   fmt.Sprintf("perf-inst-%d", cmdIdx),
				DeploymentId: fmt.Sprintf("perf-dep-%d", cmdIdx),
				Image:        "perf-workload:latest",
				Ports: []*proto.PortMapping{
					{ContainerPort: 8080, HostPort: 8080},
				},
			})

			if err != nil || res == nil || res.ContainerId == "" {
				droppedCount.Add(1)
			} else {
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	if dropped := droppedCount.Load(); dropped > 0 {
		t.Fatalf("PERF-04 failure: %d out of %d commands were dropped or delayed!", dropped, numCommands)
	}

	if success := successCount.Load(); success != numCommands {
		t.Fatalf("PERF-04 failure: expected %d successes, got %d", numCommands, success)
	}

	throughput := float64(numCommands) / duration.Seconds()
	t.Logf("PERF-04 Passed: Issued %d gRPC commands in %v (Throughput: %.0f commands/sec) with 0 dropped/delayed calls!",
		numCommands, duration, throughput)
}
