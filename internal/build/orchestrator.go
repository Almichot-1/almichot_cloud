package build

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
)

// Orchestrator coordinates source detection and isolated sandbox builds.
type Orchestrator struct {
	detector *Detector
	sandbox  Sandbox
	log      zerolog.Logger
}

// NewOrchestrator creates a new build Orchestrator.
func NewOrchestrator(sandbox Sandbox, log zerolog.Logger) *Orchestrator {
	if sandbox == nil {
		sandbox = NewEphemeralSandbox(log)
	}
	return &Orchestrator{
		detector: NewDetector(),
		sandbox:  sandbox,
		log:      log.With().Str("component", "build-orchestrator").Logger(),
	}
}

// BuildFromSource detects the build plan and builds the image inside an ephemeral sandbox.
func (o *Orchestrator) BuildFromSource(ctx context.Context, projectID, sourceDir, imageTag string, buildArgs map[string]string) (*BuildResult, error) {
	plan, err := o.detector.Detect(sourceDir)
	if err != nil {
		return nil, fmt.Errorf("detection failed: %w", err)
	}

	o.log.Info().
		Str("project_id", projectID).
		Str("strategy", string(plan.Strategy)).
		Str("source_dir", sourceDir).
		Msg("build strategy identified; executing in sandbox")

	req := BuildRequest{
		ProjectID: projectID,
		SourceDir: sourceDir,
		Plan:      plan,
		ImageTag:  imageTag,
		BuildArgs: buildArgs,
	}

	return o.sandbox.Build(ctx, req)
}
