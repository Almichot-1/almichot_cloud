package build

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var (
	ErrUnsupportedProject = errors.New("unsupported project type: no Dockerfile or recognized language manifest found")
)

// Detector examines a project directory and generates an appropriate BuildPlan.
type Detector struct{}

// NewDetector creates a new Detector.
func NewDetector() *Detector {
	return &Detector{}
}

// Detect analyzes the files in sourceDir according to strict priority order:
// 1. Dockerfile present -> StrategyDockerfile (no auto-detection override, BLD-01)
// 2. go.mod present -> StrategyGo (BLD-02)
// 3. package.json present -> StrategyNode (BLD-03)
// 4. requirements.txt or pyproject.toml present -> StrategyPython (BLD-04)
func (d *Detector) Detect(sourceDir string) (*BuildPlan, error) {
	// Rule 1: Dockerfile presence takes absolute precedence (BLD-01)
	dockerfileCandidates := []string{"Dockerfile", "dockerfile"}
	for _, name := range dockerfileCandidates {
		path := filepath.Join(sourceDir, name)
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			content, _ := os.ReadFile(path)
			return &BuildPlan{
				Strategy:          StrategyDockerfile,
				DockerfilePath:    path,
				DockerfileContent: string(content),
				ExposedPort:       8080,
			}, nil
		}
	}

	// Rule 2: Zero-config Go detection (BLD-02)
	goModPath := filepath.Join(sourceDir, "go.mod")
	if info, err := os.Stat(goModPath); err == nil && !info.IsDir() {
		return &BuildPlan{
			Strategy:          StrategyGo,
			DockerfileContent: DefaultGoDockerfile,
			BaseImage:         "golang:1.22-alpine",
			ExposedPort:       8080,
		}, nil
	}

	// Rule 3: Zero-config Node detection (BLD-03)
	pkgJSONPath := filepath.Join(sourceDir, "package.json")
	if info, err := os.Stat(pkgJSONPath); err == nil && !info.IsDir() {
		return &BuildPlan{
			Strategy:          StrategyNode,
			DockerfileContent: DefaultNodeDockerfile,
			BaseImage:         "node:20-alpine",
			ExposedPort:       3000,
		}, nil
	}

	// Rule 4: Zero-config Python detection (BLD-04)
	reqPath := filepath.Join(sourceDir, "requirements.txt")
	pyprojectPath := filepath.Join(sourceDir, "pyproject.toml")
	hasReq := false
	hasPyproject := false

	if info, err := os.Stat(reqPath); err == nil && !info.IsDir() {
		hasReq = true
	}
	if info, err := os.Stat(pyprojectPath); err == nil && !info.IsDir() {
		hasPyproject = true
	}

	if hasReq || hasPyproject {
		return &BuildPlan{
			Strategy:          StrategyPython,
			DockerfileContent: DefaultPythonDockerfile,
			BaseImage:         "python:3.11-slim",
			ExposedPort:       8000,
		}, nil
	}

	return nil, fmt.Errorf("%w: checked %s", ErrUnsupportedProject, sourceDir)
}
