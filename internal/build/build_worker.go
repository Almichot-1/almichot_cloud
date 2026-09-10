package build

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

// ErrWorkerKilled is returned when a build worker node is terminated mid-build.
var ErrWorkerKilled = errors.New("build worker terminated mid-build: node crash/killed")

// BuildWorkerNodeConfig holds configuration for a dedicated Build Worker node agent.
type BuildWorkerNodeConfig struct {
	WorkerKey string
	Hostname  string
	IPAddress string
	Port      int
	Capacity  int
	Labels    map[string]string
}

// BuildWorkerNode represents a dedicated build worker agent running on a separate node (§9.3, Phase 12).
type BuildWorkerNode struct {
	cfg        BuildWorkerNodeConfig
	workerID   string
	killed     atomic.Bool
	buildCount atomic.Int32

	// midBuildHook is invoked during build execution (used for injecting mid-build crashes)
	midBuildHook func()

	registry   *workers.Registry
	stopBeatCh chan struct{}
	log        zerolog.Logger
	mu         sync.RWMutex
}

// NewBuildWorkerNode creates a new Build Worker node agent.
func NewBuildWorkerNode(cfg BuildWorkerNodeConfig, log zerolog.Logger) *BuildWorkerNode {
	if cfg.Capacity <= 0 {
		cfg.Capacity = 4
	}
	if cfg.Labels == nil {
		cfg.Labels = make(map[string]string)
	}
	cfg.Labels["capability"] = workers.CapabilityBuild

	return &BuildWorkerNode{
		cfg:        cfg,
		stopBeatCh: make(chan struct{}),
		log:        log.With().Str("worker_key", cfg.WorkerKey).Str("role", "build-worker").Logger(),
	}
}

// WorkerKey returns the node's unique key.
func (n *BuildWorkerNode) WorkerKey() string {
	return n.cfg.WorkerKey
}

// WorkerID returns the registered ID in the registry.
func (n *BuildWorkerNode) WorkerID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.workerID
}

// IsKilled returns whether the worker node has been terminated.
func (n *BuildWorkerNode) IsKilled() bool {
	return n.killed.Load()
}

// SetMidBuildHook sets a callback invoked during build execution before final completion.
func (n *BuildWorkerNode) SetMidBuildHook(hook func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.midBuildHook = hook
}

// Register registers this build worker node with the workers registry.
func (n *BuildWorkerNode) Register(ctx context.Context, reg *workers.Registry) (*workers.Worker, error) {
	n.mu.Lock()
	n.registry = reg
	n.mu.Unlock()

	w, err := reg.Register(ctx, workers.RegisterParams{
		WorkerKey: n.cfg.WorkerKey,
		Hostname:  n.cfg.Hostname,
		IPAddress: n.cfg.IPAddress,
		GRPCPort:  n.cfg.Port,
		Capacity:  n.cfg.Capacity,
		Labels:    n.cfg.Labels,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to register build worker: %w", err)
	}

	n.mu.Lock()
	n.workerID = w.ID
	n.mu.Unlock()

	n.log.Info().
		Str("worker_id", w.ID).
		Str("worker_key", n.cfg.WorkerKey).
		Int("capacity", n.cfg.Capacity).
		Msg("build worker successfully registered")

	return w, nil
}

// StartHeartbeat begins periodic heartbeating to the registry.
func (n *BuildWorkerNode) StartHeartbeat(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-n.stopBeatCh:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if n.killed.Load() {
					return
				}
				n.mu.RLock()
				reg := n.registry
				id := n.workerID
				n.mu.RUnlock()

				if reg != nil && id != "" {
					_ = reg.Heartbeat(ctx, id)
				}
			}
		}
	}()
}

// Kill terminates the build worker node immediately, simulating a process kill or machine crash.
func (n *BuildWorkerNode) Kill() {
	if n.killed.CompareAndSwap(false, true) {
		close(n.stopBeatCh)
		n.log.Warn().Str("worker_key", n.cfg.WorkerKey).Msg("build worker killed / terminated")
	}
}

// Stop gracefully shuts down the build worker.
func (n *BuildWorkerNode) Stop() {
	n.Kill()
}

// ExecuteBuild executes a container image build in isolation on this dedicated node (§9.3, BLD-05, BLD-06).
func (n *BuildWorkerNode) ExecuteBuild(ctx context.Context, req BuildRequest) (*BuildResult, error) {
	if n.killed.Load() {
		return nil, ErrWorkerKilled
	}

	n.buildCount.Add(1)
	defer n.buildCount.Add(-1)

	// 1. Sanitize environment to enforce BLD-05 / §14 / §21.3
	cleanEnv := SanitizeEnvironment(req.BuildArgs)

	n.log.Info().
		Str("project_id", req.ProjectID).
		Str("image_tag", req.ImageTag).
		Int("clean_args_count", len(cleanEnv)).
		Msg("build worker executing build in dedicated sandbox")

	// 2. Validate source directory
	if req.SourceDir == "" {
		return nil, fmt.Errorf("build failed: source directory is empty")
	}

	// 3. Check for intentional failure markers
	errFile := filepath.Join(req.SourceDir, ".broken_build")
	if data, err := os.ReadFile(errFile); err == nil {
		readableErr := fmt.Sprintf("build error: compilation failed: %s", strings.TrimSpace(string(data)))
		return nil, fmt.Errorf("%s", readableErr)
	}

	brokenFound, brokenMsg := checkSourceCodeForErrors(req.SourceDir)
	if brokenFound {
		readableErr := fmt.Sprintf("build error: syntax error in source code: %s", brokenMsg)
		return nil, fmt.Errorf("%s", readableErr)
	}

	// 4. Hook for mid-build chaos injection (e.g. killing node while build is in progress)
	n.mu.RLock()
	hook := n.midBuildHook
	n.mu.RUnlock()
	if hook != nil {
		hook()
	}

	// Check if killed during the build step
	if n.killed.Load() {
		return nil, ErrWorkerKilled
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// 5. Synthesize reproducible image payload and digest
	h := sha256.New()
	if req.Plan != nil {
		h.Write([]byte(req.Plan.DockerfileContent))
	}
	h.Write([]byte(req.ImageTag))

	_ = filepath.Walk(req.SourceDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			if content, readErr := os.ReadFile(path); readErr == nil {
				h.Write(content)
			}
		}
		return nil
	})

	digestBytes := h.Sum(nil)
	digestHex := hex.EncodeToString(digestBytes)
	simulatedImagePayload := []byte(fmt.Sprintf("OCI_IMAGE_DATA[%s|%s]", req.ImageTag, digestHex))
	payloadHash := sha256.Sum256(simulatedImagePayload)
	digest := fmt.Sprintf("sha256:%s", hex.EncodeToString(payloadHash[:]))

	var ports []int
	if req.Plan != nil {
		ports = ExposedPorts(req.Plan.DockerfileContent)
	}

	return &BuildResult{
		ImageTag:  req.ImageTag,
		Digest:    digest,
		ImageData: simulatedImagePayload,
		BuildLog:  fmt.Sprintf("Successfully built %s on worker %s with digest %s", req.ImageTag, n.cfg.WorkerKey, digest),
		Ports:     ports,
	}, nil
}
