package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	CurrentClientVersion = "v1.0.0"
	HeaderClientVersion  = "X-Nebula-Client-Version"
	DefaultConfigFile    = ".nebula/config.json"
)

// APIError wraps HTTP errors returned from the Control Plane API.
type APIError struct {
	StatusCode int    `json:"status_code"`
	Message    string `json:"error"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("API error (%d): %s", e.StatusCode, e.Message)
}

// Config stores locally persisted CLI configuration.
type Config struct {
	Endpoint       string `json:"endpoint"`
	Token          string `json:"token"`
	DefaultProject string `json:"default_project,omitempty"`
}

// SaveConfig writes configuration to disk.
func SaveConfig(path string, cfg *Config) error {
	if path == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		path = filepath.Join(home, DefaultConfigFile)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

// LoadConfig reads configuration from disk.
func LoadConfig(path string) (*Config, error) {
	if path == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		path = filepath.Join(home, DefaultConfigFile)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Project represents a project entity returned by the API.
type Project struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	RepoURL       string    `json:"repo_url,omitempty"`
	DefaultBranch string    `json:"default_branch,omitempty"`
	RootDir       string    `json:"root_dir,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

// Deployment represents a deployment entity returned by the API.
type Deployment struct {
	ID            string    `json:"id"`
	ProjectID     string    `json:"project_id"`
	Image         string    `json:"image"`
	ImageDigest   string    `json:"image_digest,omitempty"`
	Status        string    `json:"status"`
	Stage         string    `json:"stage"`
	InstanceCount int       `json:"instance_count"`
	RunningCount  int       `json:"running_count"`
	CreatedAt     time.Time `json:"created_at"`
}

// ProjectStatus summarizes current project deployment state.
type ProjectStatus struct {
	Project          *Project      `json:"project"`
	LatestDeployment *Deployment   `json:"latest_deployment,omitempty"`
	Deployments      []*Deployment `json:"deployments"`
	ServiceURL       string        `json:"service_url"`
}

// DeployParams holds parameters for creating a new deployment via CLI.
type DeployParams struct {
	ProjectID     string            `json:"project_id"`
	Image         string            `json:"image,omitempty"`
	SourcePath    string            `json:"source_path,omitempty"`
	InstanceCount int               `json:"instance_count,omitempty"`
	Env           map[string]string `json:"env,omitempty"`
	PollTimeout   time.Duration     `json:"-"`
}

// Client is the thin HTTP client communicating with the Nebula Control Plane API.
type Client struct {
	Endpoint      string
	Token         string
	ClientVersion string
	HTTPClient    *http.Client
}

// NewClient constructs a new CLI API client.
func NewClient(endpoint, token string) *Client {
	endpoint = strings.TrimRight(endpoint, "/")
	return &Client{
		Endpoint:      endpoint,
		Token:         token,
		ClientVersion: CurrentClientVersion,
		HTTPClient:    &http.Client{Timeout: 30 * time.Second},
	}
}

// do sends an HTTP request with authentication and version headers.
func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to encode request: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	url := c.Endpoint + path
	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if c.ClientVersion != "" {
		req.Header.Set(HeaderClientVersion, c.ClientVersion)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("connection to Control Plane failed at %s: %w", c.Endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		}
		respBytes, _ := io.ReadAll(resp.Body)
		if err := json.Unmarshal(respBytes, &apiErr); err == nil {
			msg := apiErr.Error
			if msg == "" {
				msg = apiErr.Message
			}
			if msg == "" {
				msg = string(respBytes)
			}
			return &APIError{StatusCode: resp.StatusCode, Message: msg}
		}
		return &APIError{StatusCode: resp.StatusCode, Message: strings.TrimSpace(string(respBytes))}
	}

	if out != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("failed to decode API response: %w", err)
		}
	}

	return nil
}

// Login validates user credentials against the Control Plane API.
func (c *Client) Login(ctx context.Context, token string) error {
	if token == "" {
		return errors.New("authentication token cannot be empty")
	}
	c.Token = token

	// Test authentication against public health / projects endpoint
	var projects []*Project
	err := c.do(ctx, http.MethodGet, "/v1/projects", nil, &projects)
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && (apiErr.StatusCode == http.StatusUnauthorized || apiErr.StatusCode == http.StatusForbidden) {
			return fmt.Errorf("authentication failed: invalid or unauthorized token")
		}
		return err
	}
	return nil
}

// InitProject creates a new project via the API.
func (c *Client) InitProject(ctx context.Context, name, defaultBranch, rootDir string) (*Project, error) {
	if strings.TrimSpace(name) == "" {
		return nil, errors.New("project name is required")
	}

	payload := map[string]string{
		"name":           name,
		"default_branch": defaultBranch,
		"root_dir":       rootDir,
	}

	var created Project
	if err := c.do(ctx, http.MethodPost, "/v1/projects", payload, &created); err != nil {
		return nil, err
	}
	return &created, nil
}

// Deploy creates a new deployment and polls until it reaches a terminal status or RUNNING.
func (c *Client) Deploy(ctx context.Context, params DeployParams) (*Deployment, error) {
	if params.ProjectID == "" {
		return nil, errors.New("project ID is required for deployment")
	}
	if params.Image == "" && params.SourcePath == "" {
		return nil, errors.New("either --image or source path must be specified")
	}

	payload := map[string]any{
		"image":          params.Image,
		"source_path":    params.SourcePath,
		"instance_count": params.InstanceCount,
		"env":            params.Env,
	}

	var dep Deployment
	path := fmt.Sprintf("/v1/projects/%s/deployments", params.ProjectID)
	if err := c.do(ctx, http.MethodPost, path, payload, &dep); err != nil {
		return nil, err
	}

	// Poll until terminal or RUNNING
	timeout := params.PollTimeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return &dep, ctx.Err()
		default:
		}

		if dep.Status == "RUNNING" || dep.Status == "FAILED" || dep.Status == "STOPPED" || dep.Status == "ROLLED_BACK" {
			break
		}

		time.Sleep(100 * time.Millisecond)

		var latest Deployment
		err := c.do(ctx, http.MethodGet, fmt.Sprintf("/v1/deployments/%s", dep.ID), nil, &latest)
		if err == nil {
			dep = latest
		}
	}

	if dep.Status == "FAILED" {
		return &dep, fmt.Errorf("deployment failed at stage %s", dep.Stage)
	}

	return &dep, nil
}

// Status fetches the current status of a project.
func (c *Client) Status(ctx context.Context, projectID string) (*ProjectStatus, error) {
	if projectID == "" {
		return nil, errors.New("project ID is required")
	}

	var proj Project
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/v1/projects/%s", projectID), nil, &proj); err != nil {
		return nil, err
	}

	var deps []*Deployment
	_ = c.do(ctx, http.MethodGet, fmt.Sprintf("/v1/projects/%s/deployments", projectID), nil, &deps)

	status := &ProjectStatus{
		Project:     &proj,
		Deployments: deps,
		ServiceURL:  fmt.Sprintf("%s/services/%s/", c.Endpoint, proj.ID),
	}

	if len(deps) > 0 {
		status.LatestDeployment = deps[len(deps)-1]
	}

	return status, nil
}

// Logs streams or prints logs for a project or deployment.
func (c *Client) Logs(ctx context.Context, projectID, deploymentID string, follow bool, out io.Writer) error {
	var path string
	if deploymentID != "" {
		path = fmt.Sprintf("/v1/deployments/%s/logs", deploymentID)
	} else if projectID != "" {
		path = fmt.Sprintf("/v1/projects/%s/logs", projectID)
	} else {
		return errors.New("either project ID or deployment ID must be provided")
	}

	if follow {
		path += "?follow=true"
	}

	url := c.Endpoint + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if c.ClientVersion != "" {
		req.Header.Set(HeaderClientVersion, c.ClientVersion)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to log stream: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return &APIError{StatusCode: resp.StatusCode, Message: strings.TrimSpace(string(body))}
	}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		_, _ = fmt.Fprintln(out, line)
	}

	return scanner.Err()
}
