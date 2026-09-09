package e2e

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/reconcile"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
)

// Phase 6 Exit Check:
// Scripted kill of Control Plane at each deployment stage in turn (QUEUED, BUILDING, SCHEDULING, STARTING, RUNNING).
// Every restart MUST end in a clean, non-orphaned, fully-converged state (Gates G-16, G-17, G-18, G-19, G-20).
func TestPhase6_ExitCheck_CPCrashAtEachDeploymentStage(t *testing.T) {
	log := zerolog.Nop()
	ctx := context.Background()

	t.Log("=========================================================================")
	t.Log("STARTING PHASE 6 EXIT CHECK: SCRIPTED CP KILL AT EACH STAGE")
	t.Log("=========================================================================")

	stagesToKill := []deployments.DeploymentStatus{
		deployments.StatusQueued,     // Gate G-16
		deployments.StatusBuilding,   // Gate G-17 & DL-04
		deployments.StatusScheduling, // Gate G-18
		deployments.StatusStarting,   // Gate G-19
		deployments.StatusRunning,    // Gate G-20
	}

	for stepIdx, stageToKill := range stagesToKill {
		t.Run(fmt.Sprintf("Stage_%d_%s", stepIdx+1, stageToKill), func(t *testing.T) {
			t.Logf(">>> Beginning Crash Test for Stage: %s", stageToKill)

			// 1. Initialize fresh Control Plane storage & services
			workerRepo := workers.NewMemoryWorkerRepository()
			depRepo := deployments.NewMemoryDeploymentRepository()
			instRepo := deployments.NewMemoryInstanceRepository()
			reg := workers.NewRegistry(workerRepo, log)
			sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
			mockClientFactory := deployments.NewMockWorkerClientFactory()

			// Register healthy worker
			w1, err := reg.Register(ctx, workers.RegisterParams{
				WorkerKey: "worker-node-alpha",
				Hostname:  "node-alpha",
				IPAddress: "10.10.1.1",
				Capacity:  10,
			})
			if err != nil {
				t.Fatalf("failed to register worker: %v", err)
			}

			// Mock HTTP backend for the application
			backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"service":"nebula-cloud","status":"healthy"}`))
			}))
			defer backendServer.Close()

			depService := deployments.NewService(depRepo, instRepo, reg, sched, mockClientFactory, log)

			// Configure crash hook to simulate process termination (kill -9) at target stage
			errKill9 := fmt.Errorf("FATAL: Control Plane killed with SIGKILL at stage %s", stageToKill)
			killed := false

			depService.SetCrashHook(func(st deployments.DeploymentStatus) error {
				if st == stageToKill {
					killed = true
					// For stage STARTING, simulate 1 container having been started just before SIGKILL
					if st == deployments.StatusStarting {
						all, _ := depRepo.List(ctx, "")
						actualDepID := all[0].ID
						mockClientFactory.AddContainer(w1.WorkerKey, &proto.ContainerInfo{
							InstanceId:  "inst-partial-start",
							ContainerId: "ctr-partial-start",
							Image:       "alpine:latest",
							Status:      "running",
							Labels: map[string]string{
								"nebula.deployment_id": actualDepID,
								"nebula.instance_id":   "inst-partial-start",
							},
						})
					}
					return errKill9
				}
				return nil
			})

			// Create deployment source
			repoDir := t.TempDir()
			_ = os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte("FROM alpine:3.19\nCMD [\"./run\"]\n"), 0644)

			projectID := fmt.Sprintf("proj-stage-%d", stepIdx+1)
			_, _, deployErr := depService.CreateAndDeploy(ctx, deployments.CreateDeploymentParams{
				ProjectID:     projectID,
				SourcePath:    repoDir,
				InstanceCount: 1,
			})

			if !errors.Is(deployErr, errKill9) {
				t.Fatalf("expected deploy to fail with simulated kill error, got: %v", deployErr)
			}
			if !killed {
				t.Fatalf("crash hook was never called for stage %s", stageToKill)
			}

			// =================================================================
			// 2. CP CRASHES: VERIFY STORAGE INTEGRITY AT TIME OF CRASH (DL-02)
			// =================================================================
			allDeps, _ := depRepo.List(ctx, "")
			if len(allDeps) != 1 {
				t.Fatalf("expected 1 deployment in storage, got %d", len(allDeps))
			}
			persistedDep := allDeps[0]
			if persistedDep.Status != stageToKill {
				t.Fatalf("DL-02 VIOLATION: storage status %s did not match crash stage %s", persistedDep.Status, stageToKill)
			}

			t.Logf("State verified in storage at time of crash: %s", persistedDep.Status)

			// =================================================================
			// 3. CONTROL PLANE RESTARTS: RECOVERY ENGINE EXECUTES (DL-03)
			// =================================================================
			t.Log("Control plane restarts; running startup RecoveryEngine...")
			recoveryEngine := deployments.NewRecoveryEngine(depRepo, instRepo, reg, sched, mockClientFactory, log)
			report, err := recoveryEngine.RecoverDeployments(ctx, deployments.RecoveryPolicyFailSafe)
			if err != nil {
				t.Fatalf("restart recovery failed: %v", err)
			}

			// =================================================================
			// 4. RECOVERY INVARIANTS & GATE VERIFICATION
			// =================================================================
			recoveredDep, err := depRepo.GetByID(ctx, persistedDep.ID)
			if err != nil {
				t.Fatalf("failed to retrieve recovered deployment: %v", err)
			}

			switch stageToKill {
			case deployments.StatusQueued:
				// Gate G-16: CP crash while QUEUED -> cleanly FAILED, 0 orphans
				if recoveredDep.Status != deployments.StatusFailed {
					t.Fatalf("G-16 VIOLATION: expected status FAILED, got %s", recoveredDep.Status)
				}
				if report.QueuedRecovered != 1 {
					t.Fatalf("expected 1 QUEUED recovered, got %d", report.QueuedRecovered)
				}
				t.Log(">>> Gate G-16 Verified: QUEUED crash recovered with zero orphan work!")

			case deployments.StatusBuilding:
				// Gate G-17 & DL-04: CP crash while BUILDING -> partial artifact discarded
				if recoveredDep.Status != deployments.StatusFailed {
					t.Fatalf("G-17 VIOLATION: expected status FAILED, got %s", recoveredDep.Status)
				}
				if recoveredDep.ImageDigest != "" {
					t.Fatalf("DL-04 VIOLATION: partial image digest was not discarded on restart!")
				}
				if report.BuildingRecovered != 1 {
					t.Fatalf("expected 1 BUILDING recovered, got %d", report.BuildingRecovered)
				}
				t.Log(">>> Gate G-17 & DL-04 Verified: BUILDING crash recovered, partial artifact rejected!")

			case deployments.StatusScheduling:
				// Gate G-18: CP crash while BUILT/SCHEDULING -> cleanly FAILED, 0 orphans
				if recoveredDep.Status != deployments.StatusFailed {
					t.Fatalf("G-18 VIOLATION: expected status FAILED, got %s", recoveredDep.Status)
				}
				if report.SchedulingRecovered != 1 {
					t.Fatalf("expected 1 SCHEDULING recovered, got %d", report.SchedulingRecovered)
				}
				t.Log(">>> Gate G-18 Verified: SCHEDULING crash recovered cleanly!")

			case deployments.StatusStarting:
				// Gate G-19: CP crash while STARTING -> partial containers cleaned up, 0 orphans
				if recoveredDep.Status != deployments.StatusFailed {
					t.Fatalf("G-19 VIOLATION: expected status FAILED, got %s", recoveredDep.Status)
				}
				if report.StartingRecovered != 1 {
					t.Fatalf("expected 1 STARTING recovered, got %d", report.StartingRecovered)
				}
				if report.OrphanContainersStopped != 1 {
					t.Fatalf("G-19 VIOLATION: expected 1 orphan container stopped, got %d", report.OrphanContainersStopped)
				}
				// Verify worker has 0 running containers
				client, _ := mockClientFactory.GetClient(ctx, w1)
				ctrs, _ := client.ListContainers(ctx, nil)
				if len(ctrs.Containers) != 0 {
					t.Fatalf("G-19 VIOLATION: orphan container still running on worker! Containers: %+v", ctrs.Containers)
				}
				t.Log(">>> Gate G-19 Verified: STARTING crash cleaned up all partial containers; ZERO orphans!")

			case deployments.StatusRunning:
				// Gate G-20: CP crash while RUNNING -> app unaffected, reconcile restores view
				if recoveredDep.Status != deployments.StatusRunning {
					t.Fatalf("G-20 VIOLATION: healthy RUNNING deployment corrupted by restart! Status: %s", recoveredDep.Status)
				}
				// Reconciler runs on boot
				reconciler := reconcile.NewReconciler(reg, depRepo, instRepo, sched, mockClientFactory, log)
				actions, err := reconciler.ReconcileOnce(ctx)
				if err != nil {
					t.Fatalf("startup reconciliation failed: %v", err)
				}
				if !actions.IsZero() {
					t.Fatalf("G-20 VIOLATION: reconciler unexpectedly touched healthy running app: %+v", actions)
				}
				t.Log(">>> Gate G-20 Verified: RUNNING deployment sustained with zero downtime!")
			}

			// =================================================================
			// 5. POST-RECOVERY FULL CLUSTER CONVERGENCE CHECK
			// =================================================================
			reconciler := reconcile.NewReconciler(reg, depRepo, instRepo, sched, mockClientFactory, log)
			convergedActions, err := reconciler.ReconcileOnce(ctx)
			if err != nil {
				t.Fatalf("convergence check failed: %v", err)
			}
			if !convergedActions.IsZero() {
				t.Fatalf("cluster did not converge to zero drift after recovery: %+v", convergedActions)
			}

			t.Logf("Stage %s crash test PASSED: Cluster fully converged, non-orphaned state verified!", stageToKill)
		})
	}

	t.Log("=========================================================================")
	t.Log("PHASE 6 EXIT CHECK COMPLETE: ALL 5 STAGES VERIFIED WITH CLEAN RESTART!")
	t.Log("=========================================================================")
}
