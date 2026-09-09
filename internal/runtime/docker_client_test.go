package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestWA01_MockLifecycleWrappers(t *testing.T) {
	m := NewMockDockerClient()
	ctx := context.Background()

	id, err := m.CreateContainer(ctx, CreateContainerOptions{
		InstanceID:   "inst-wa01-a",
		DeploymentID: "deploy-wa01",
		Image:        "busybox:latest",
		Env:          []string{"FOO=bar"},
		Ports:        []PortMapping{{HostPort: 8080, ContainerPort: 80, Protocol: "tcp"}},
	})
	if err != nil {
		t.Fatalf("CreateContainer: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty container ID")
	}

	if err := m.StartContainer(ctx, id); err != nil {
		t.Fatalf("StartContainer: %v", err)
	}

	details, err := m.InspectContainer(ctx, id)
	if err != nil {
		t.Fatalf("InspectContainer: %v", err)
	}
	if details.State != "running" {
		t.Errorf("expected state running, got %s", details.State)
	}
	if details.Image != "busybox:latest" {
		t.Errorf("expected image busybox:latest, got %s", details.Image)
	}

	summary, err := m.ListContainers(ctx)
	if err != nil {
		t.Fatalf("ListContainers: %v", err)
	}
	if len(summary) != 1 {
		t.Fatalf("expected 1 container, got %d", len(summary))
	}
	if summary[0].InstanceID != "inst-wa01-a" {
		t.Errorf("expected instance inst-wa01-a, got %s", summary[0].InstanceID)
	}

	if err := m.StopContainer(ctx, id, nil); err != nil {
		t.Fatalf("StopContainer: %v", err)
	}

	stopped, err := m.InspectContainer(ctx, id)
	if err != nil {
		t.Fatalf("InspectContainer after stop: %v", err)
	}
	if stopped.State != "stopped" {
		t.Errorf("expected state stopped, got %s", stopped.State)
	}

	if err := m.RemoveContainer(ctx, id, true); err != nil {
		t.Fatalf("RemoveContainer: %v", err)
	}
	summary2, err := m.ListContainers(ctx)
	if err != nil {
		t.Fatalf("ListContainers after remove: %v", err)
	}
	if len(summary2) != 0 {
		t.Errorf("expected 0 containers after remove, got %d", len(summary2))
	}
}

func TestWA01_StopWithTimeoutPropagated(t *testing.T) {
	m := NewMockDockerClient()
	ctx := context.Background()

	id, err := m.CreateContainer(ctx, CreateContainerOptions{InstanceID: "inst-wa01-b", Image: "busybox"})
	if err != nil {
		t.Fatalf("CreateContainer: %v", err)
	}
	to := 30 * time.Second
	if err := m.StopContainer(ctx, id, &to); err != nil {
		t.Fatalf("StopContainer with timeout: %v", err)
	}
	c, ok := m.GetMockContainer(id)
	if !ok {
		t.Fatal("expected mock container to exist after stop")
	}
	if c.State != "stopped" {
		t.Errorf("expected stopped, got %s", c.State)
	}
}

func TestWA01_ErrorSurfacing(t *testing.T) {
	m := NewMockDockerClient()
	ctx := context.Background()

	cases := []struct {
		name string
		fn   func() error
	}{
		{
			name: "create",
			fn: func() error {
				m.FailCreate = errors.New("create boom")
				_, err := m.CreateContainer(ctx, CreateContainerOptions{InstanceID: "x", Image: "img"})
				return err
			},
		},
		{
			name: "start_not_found",
			fn: func() error {
				return m.StartContainer(ctx, "nope")
			},
		},
		{
			name: "start_injected",
			fn: func() error {
				m.FailStart = errors.New("start boom")
				id, _ := m.CreateContainer(ctx, CreateContainerOptions{InstanceID: "y", Image: "img"})
				return m.StartContainer(ctx, id)
			},
		},
		{
			name: "stop_injected",
			fn: func() error {
				m.FailStop = errors.New("stop boom")
				id, _ := m.CreateContainer(ctx, CreateContainerOptions{InstanceID: "z", Image: "img"})
				return m.StopContainer(ctx, id, nil)
			},
		},
		{
			name: "inspect_not_found",
			fn: func() error {
				_, err := m.InspectContainer(ctx, "ghost")
				return err
			},
		},
		{
			name: "inspect_injected",
			fn: func() error {
				m.FailInspect = errors.New("inspect boom")
				_, err := m.InspectContainer(ctx, "ghost")
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m.FailCreate = nil
			m.FailStart = nil
			m.FailStop = nil
			m.FailInspect = nil
			if err := tc.fn(); err == nil {
				t.Errorf("expected error to be surfaced")
			}
		})
	}
}

func TestWA01_NewRealDockerClient(t *testing.T) {
	cli, err := NewRealDockerClient(zerolog.Nop())
	if err != nil {
		t.Fatalf("NewRealDockerClient should construct without a live daemon: %v", err)
	}
	if cli == nil {
		t.Fatal("expected non-nil client")
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestWA07_LabelsInjectedOnCreate(t *testing.T) {
	m := NewMockDockerClient()
	ctx := context.Background()

	id, err := m.CreateContainer(ctx, CreateContainerOptions{
		InstanceID:   "inst-wa07-1",
		DeploymentID: "deploy-wa07",
		Image:        "busybox:latest",
		Labels:       map[string]string{"app": "cache"},
	})
	if err != nil {
		t.Fatalf("CreateContainer: %v", err)
	}

	c, ok := m.GetMockContainer(id)
	if !ok {
		t.Fatal("expected mock container to exist")
	}

	if c.Labels["nebula.instance_id"] != "inst-wa07-1" {
		t.Errorf("expected nebula.instance_id label, got %q", c.Labels["nebula.instance_id"])
	}
	if c.Labels["nebula.deployment_id"] != "deploy-wa07" {
		t.Errorf("expected nebula.deployment_id label, got %q", c.Labels["nebula.deployment_id"])
	}
	if c.Labels["app"] != "cache" {
		t.Errorf("expected user label app=cache preserved, got %q", c.Labels["app"])
	}
}

func TestWA07_ListSurfacesInstanceID(t *testing.T) {
	m := NewMockDockerClient()
	ctx := context.Background()

	_, _ = m.CreateContainer(ctx, CreateContainerOptions{InstanceID: "inst-wa07-list", Image: "img"})

	summaries, err := m.ListContainers(ctx)
	if err != nil {
		t.Fatalf("ListContainers: %v", err)
	}
	if len(summaries) != 1 || summaries[0].InstanceID != "inst-wa07-list" {
		t.Fatalf("expected instance id in summary, got %+v", summaries)
	}
	if summaries[0].Labels["nebula.instance_id"] != "inst-wa07-list" {
		t.Errorf("expected label nebula.instance_id in summary labels, got %v", summaries[0].Labels)
	}
}

func TestWA07_ContainerOpsPassesLabelsThrough(t *testing.T) {
	m := NewMockDockerClient()
	tracker := NewInstanceTracker()
	ops := NewContainerOps(m, tracker, zerolog.Nop())
	ctx := context.Background()

	opts := RunOptions{
		InstanceID:   "inst-wa07-ops",
		DeploymentID: "deploy-wa07-ops",
		Image:        "busybox:latest",
		Labels:       map[string]string{"tier": "test"},
	}
	res, err := ops.RunContainer(ctx, opts)
	if err != nil {
		t.Fatalf("RunContainer: %v", err)
	}

	c, ok := m.GetMockContainer(res.ContainerID)
	if !ok {
		t.Fatalf("expected mock container %s", res.ContainerID)
	}
	if c.Labels["nebula.instance_id"] != "inst-wa07-ops" {
		t.Errorf("expected nebula.instance_id label via ops, got %q", c.Labels["nebula.instance_id"])
	}
	if c.Labels["nebula.deployment_id"] != "deploy-wa07-ops" {
		t.Errorf("expected nebula.deployment_id label via ops, got %q", c.Labels["nebula.deployment_id"])
	}
	if c.Labels["tier"] != "test" {
		t.Errorf("expected user label tier=test via ops, got %q", c.Labels["tier"])
	}
}