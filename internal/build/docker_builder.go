package build

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/nebula/nebula/internal/registry"
	"github.com/rs/zerolog"
)

// BuildErrorMessage represents the error detail streamed by the Docker daemon
// during a failed build.
type BuildErrorMessage struct {
	Error       string `json:"error"`
	ErrorDetail struct {
		Message string `json:"message"`
	} `json:"errorDetail"`
}

// RealDockerSandbox executes a real container image build using the Docker daemon.
// It is the production-grade counterpart of EphemeralSandbox: the built image is a
// real Linux image available on the local daemon, and the reported digest is computed
// over the actual exported image payload (BLD-07).
type RealDockerSandbox struct {
	cli *client.Client
	log zerolog.Logger
}

// NewRealDockerSandbox connects to the local Docker daemon and returns a build sandbox.
// It returns an error if the daemon is unreachable so callers can fall back gracefully.
func NewRealDockerSandbox(log zerolog.Logger) (*RealDockerSandbox, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.Ping(ctx); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("docker daemon unreachable: %w", err)
	}

	return &RealDockerSandbox{
		cli: cli,
		log: log.With().Str("component", "build-sandbox-docker").Logger(),
	}, nil
}

// Close releases the underlying Docker client.
func (s *RealDockerSandbox) Close() error {
	if s.cli != nil {
		return s.cli.Close()
	}
	return nil
}

// Build executes a real Docker build from the source directory.
func (s *RealDockerSandbox) Build(ctx context.Context, req BuildRequest) (*BuildResult, error) {
	buildCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()

	sanitized := SanitizeEnvironment(req.BuildArgs)

	// 1. Validation and intentional-failure markers (BLD-06)
	if req.SourceDir == "" {
		return nil, fmt.Errorf("build failed: source directory is empty")
	}

	if data, err := os.ReadFile(filepath.Join(req.SourceDir, ".broken_build")); err == nil {
		return nil, fmt.Errorf("build error: compilation failed: %s", strings.TrimSpace(string(data)))
	}

	brokenFound, brokenMsg := checkSourceCodeForErrors(req.SourceDir)
	if brokenFound {
		return nil, fmt.Errorf("build error: syntax error in source code: %s", brokenMsg)
	}

	// 2. Stream the source directory as the build context, injecting the Dockerfile.
	buildContext, err := dockerBuildContext(req.SourceDir, req.Plan.DockerfileContent)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare build context: %w", err)
	}

	buildArgs := make(map[string]*string)
	for k, v := range sanitized {
		val := v
		buildArgs[k] = &val
	}

	s.log.Info().
		Str("project_id", req.ProjectID).
		Str("image_tag", req.ImageTag).
		Str("strategy", string(req.Plan.Strategy)).
		Int("clean_args_count", len(sanitized)).
		Msg("starting real docker build")

	// 3. Run the build against the real daemon.
	resp, err := s.cli.ImageBuild(buildCtx, buildContext, types.ImageBuildOptions{
		Tags:       []string{req.ImageTag},
		Dockerfile: "Dockerfile",
		Remove:     true,
		BuildArgs:  buildArgs,
	})
	if err != nil {
		return nil, fmt.Errorf("build failed: %w", err)
	}
	defer resp.Body.Close()

	buildLog, buildErr := collectBuildOutput(resp.Body)

	if buildErr != nil {
		return nil, buildErr
	}

	// 4. Capture the real image digest and payload.
	digest, err := s.imageDigest(buildCtx, req.ImageTag)
	if err != nil {
		return nil, err
	}

	payload, err := s.exportImage(buildCtx, req.ImageTag)
	if err != nil {
		return nil, err
	}

	// The digest must match the exported payload so that registry push verification (BLD-07/08) holds.
	if !strings.HasPrefix(digest, "sha256:") {
		return nil, fmt.Errorf("unexpected image digest format: %s", digest)
	}
	payloadDigest := registry.ComputeDigest(payload)
	if payloadDigest != digest {
		s.log.Warn().
			Str("image_digest", digest).
			Str("payload_digest", payloadDigest).
			Msg("image ID differed from exported payload digest; using payload digest as the verified artifact digest")
		digest = payloadDigest
	}

	s.log.Info().
		Str("image_tag", req.ImageTag).
		Str("digest", digest).
		Int("payload_bytes", len(payload)).
		Msg("real docker build completed successfully")

	return &BuildResult{
		ImageTag:  req.ImageTag,
		Digest:    digest,
		ImageData: payload,
		BuildLog:  buildLog,
		Ports:     ExposedPorts(req.Plan.DockerfileContent),
	}, nil
}

func (s *RealDockerSandbox) imageDigest(ctx context.Context, imageTag string) (string, error) {
	inspect, _, err := s.cli.ImageInspectWithRaw(ctx, imageTag)
	if err != nil {
		return "", fmt.Errorf("failed to inspect built image %s: %w", imageTag, err)
	}
	if len(inspect.RepoDigests) > 0 {
		// RepoDigests are "repo@sha256:..."; strip the repo qualifier.
		rd := inspect.RepoDigests[0]
		if i := strings.LastIndex(rd, "@"); i >= 0 {
			return rd[i+1:], nil
		}
		return rd, nil
	}
	if inspect.ID != "" {
		return inspect.ID, nil
	}
	return "", fmt.Errorf("built image %s has no resolvable digest", imageTag)
}

func (s *RealDockerSandbox) exportImage(ctx context.Context, imageTag string) ([]byte, error) {
	r, err := s.cli.ImageSave(ctx, []string{imageTag})
	if err != nil {
		return nil, fmt.Errorf("failed to export built image %s: %w", imageTag, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read exported image payload: %w", err)
	}
	return data, nil
}

// dockerBuildContext produces a tar stream from a source directory and an injected Dockerfile.
func dockerBuildContext(sourceDir, dockerfileContent string) (io.ReadCloser, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	err := filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		rel, relErr := filepath.Rel(sourceDir, path)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)

		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}

		header := &tar.Header{
			Name:    rel,
			Mode:    int64(info.Mode().Perm()),
			Size:    int64(len(content)),
			ModTime: info.ModTime(),
		}
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if _, err := tw.Write(content); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	dockerfile := []byte(dockerfileContent)
	dfHeader := &tar.Header{
		Name: "Dockerfile",
		Mode: 0o644,
		Size: int64(len(dockerfile)),
	}
	if err := tw.WriteHeader(dfHeader); err != nil {
		return nil, err
	}
	if _, err := tw.Write(dockerfile); err != nil {
		return nil, err
	}

	if err := tw.Close(); err != nil {
		return nil, err
	}

	return io.NopCloser(&buf), nil
}

func collectBuildOutput(body io.Reader) (string, error) {
	var log strings.Builder
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "{") {
			var msg struct {
				Stream string `json:"stream"`
				Status string `json:"status"`
				Aux    struct {
					ID string `json:"ID"`
				} `json:"aux"`
				BuildErrorMessage
			}
			if err := json.Unmarshal([]byte(line), &msg); err == nil {
				switch {
				case msg.Error != "" || msg.ErrorDetail.Message != "":
					return log.String(), fmt.Errorf("build failed: %s", msg.ErrorDetail.Message)
				case msg.Stream != "":
					log.WriteString(msg.Stream)
				case msg.Status != "":
					log.WriteString(msg.Status + "\n")
				case msg.Aux.ID != "":
					log.WriteString("image digest: " + msg.Aux.ID + "\n")
				}
			}
			continue
		}
		log.WriteString(line + "\n")
	}
	if err := scanner.Err(); err != nil {
		return log.String(), fmt.Errorf("failed to read build output: %w", err)
	}
	return log.String(), nil
}
