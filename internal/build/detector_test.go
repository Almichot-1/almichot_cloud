package build

import (
	"os"
	"path/filepath"
	"testing"
)

// BLD-01: Dockerfile-present repo detected and used directly.
// No auto-detection override even if go.mod or package.json are also present!
func TestBLD01_DockerfilePriority(t *testing.T) {
	tmpDir := t.TempDir()

	// Create Dockerfile AND go.mod
	dockerfileContent := "FROM alpine:3.19\nCMD [\"echo\", \"custom\"]"
	_ = os.WriteFile(filepath.Join(tmpDir, "Dockerfile"), []byte(dockerfileContent), 0644)
	_ = os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module myapp\ngo 1.22"), 0644)

	detector := NewDetector()
	plan, err := detector.Detect(tmpDir)
	if err != nil {
		t.Fatalf("Detect failed: %v", err)
	}

	if plan.Strategy != StrategyDockerfile {
		t.Fatalf("BLD-01 failure: expected StrategyDockerfile, got %s (auto-detection overrode Dockerfile!)", plan.Strategy)
	}
	if plan.DockerfileContent != dockerfileContent {
		t.Fatalf("expected custom Dockerfile content to be preserved")
	}
	t.Log("BLD-01 Passed: Dockerfile detected and used directly with priority over language manifests!")
}

// BLD-02: Zero-config detection: Go
func TestBLD02_ZeroConfig_Go(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module example.com/goapp\ngo 1.22"), 0644)
	_ = os.WriteFile(filepath.Join(tmpDir, "main.go"), []byte("package main\nfunc main(){}"), 0644)

	detector := NewDetector()
	plan, err := detector.Detect(tmpDir)
	if err != nil {
		t.Fatalf("Detect failed: %v", err)
	}

	if plan.Strategy != StrategyGo {
		t.Fatalf("BLD-02 failure: expected StrategyGo, got %s", plan.Strategy)
	}
	if plan.ExposedPort != 8080 {
		t.Fatalf("expected exposed port 8080, got %d", plan.ExposedPort)
	}
	t.Log("BLD-02 Passed: go.mod detected and Go build plan selected!")
}

// BLD-03: Zero-config detection: Node
func TestBLD03_ZeroConfig_Node(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmpDir, "package.json"), []byte("{\"name\": \"nodeapp\", \"version\": \"1.0.0\"}"), 0644)

	detector := NewDetector()
	plan, err := detector.Detect(tmpDir)
	if err != nil {
		t.Fatalf("Detect failed: %v", err)
	}

	if plan.Strategy != StrategyNode {
		t.Fatalf("BLD-03 failure: expected StrategyNode, got %s", plan.Strategy)
	}
	if plan.ExposedPort != 3000 {
		t.Fatalf("expected exposed port 3000, got %d", plan.ExposedPort)
	}
	t.Log("BLD-03 Passed: package.json detected and Node build plan selected!")
}

// BLD-04: Zero-config detection: Python
func TestBLD04_ZeroConfig_Python(t *testing.T) {
	// Subtest A: requirements.txt
	t.Run("requirements.txt", func(t *testing.T) {
		tmpDir := t.TempDir()
		_ = os.WriteFile(filepath.Join(tmpDir, "requirements.txt"), []byte("flask>=2.0"), 0644)

		detector := NewDetector()
		plan, err := detector.Detect(tmpDir)
		if err != nil {
			t.Fatalf("Detect failed: %v", err)
		}
		if plan.Strategy != StrategyPython {
			t.Fatalf("BLD-04 failure: expected StrategyPython, got %s", plan.Strategy)
		}
	})

	// Subtest B: pyproject.toml
	t.Run("pyproject.toml", func(t *testing.T) {
		tmpDir := t.TempDir()
		_ = os.WriteFile(filepath.Join(tmpDir, "pyproject.toml"), []byte("[tool.poetry]\nname=\"pyapp\""), 0644)

		detector := NewDetector()
		plan, err := detector.Detect(tmpDir)
		if err != nil {
			t.Fatalf("Detect failed: %v", err)
		}
		if plan.Strategy != StrategyPython {
			t.Fatalf("BLD-04 failure: expected StrategyPython, got %s", plan.Strategy)
		}
	})

	t.Log("BLD-04 Passed: requirements.txt and pyproject.toml detected and Python build plan selected!")
}

// Unsupported project
func TestDetector_Unsupported(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmpDir, "notes.txt"), []byte("hello"), 0644)

	detector := NewDetector()
	_, err := detector.Detect(tmpDir)
	if err == nil {
		t.Fatalf("expected ErrUnsupportedProject, got nil")
	}
}
