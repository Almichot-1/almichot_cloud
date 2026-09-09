package security

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nebula/nebula/internal/api"
	"github.com/nebula/nebula/internal/auth"
	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/internal/loadbalancer"
	"github.com/nebula/nebula/internal/projects"
	"github.com/nebula/nebula/internal/scheduler"
	"github.com/nebula/nebula/internal/workers"
	"github.com/rs/zerolog"
)

type authTestStack struct {
	srv        *httptest.Server
	tokenStore *auth.TokenStore
	projectRepo projects.ProjectRepository
}

func newAuthTestStack(t *testing.T) *authTestStack {
	t.Helper()
	log := zerolog.Nop()

	workerRepo := workers.NewMemoryWorkerRepository()
	depRepo := deployments.NewMemoryDeploymentRepository()
	instRepo := deployments.NewMemoryInstanceRepository()
	projectRepo := projects.NewMemoryProjectRepository()

	reg := workers.NewRegistry(workerRepo, log)
	sched := scheduler.NewScheduler(reg, instRepo.CountByWorkerForDeployment, log)
	mockFactory := deployments.NewMockWorkerClientFactory()
	depService := deployments.NewService(depRepo, instRepo, reg, sched, mockFactory, log)
	router := loadbalancer.NewRouter(log)

	// Register a worker for deployments
	_, _ = reg.Register(context.Background(), workers.RegisterParams{
		WorkerKey: "auth-worker-1",
		Hostname:  "auth-node-1",
		Capacity:  10,
	})

	tokenStore := auth.NewTokenStore()
	tokenStore.RegisterToken("admin-token", &auth.User{
		ID:              "admin-user",
		Username:        "admin",
		Role:            "admin",
		AllowedProjects: []string{"*"},
	})
	tokenStore.RegisterToken("alice-token", &auth.User{
		ID:              "alice-user",
		Username:        "alice",
		Role:            "member",
		AllowedProjects: []string{"proj-alice"},
	})
	tokenStore.RegisterToken("bob-token", &auth.User{
		ID:              "bob-user",
		Username:        "bob",
		Role:            "member",
		AllowedProjects: []string{"proj-bob"},
	})

	server := api.NewServer(depService, projectRepo, reg, router, log)
	server.SetAuthenticator(auth.NewTokenAuthenticator(tokenStore))

	srv := httptest.NewServer(api.NewRouter(server))
	t.Cleanup(srv.Close)

	return &authTestStack{
		srv:         srv,
		tokenStore:  tokenStore,
		projectRepo: projectRepo,
	}
}

// SE-01: Unauthenticated API request rejected: 401 on any protected endpoint without valid auth (Gate G-25).
func TestSE01_UnauthenticatedAPIRequestRejected_401(t *testing.T) {
	stack := newAuthTestStack(t)

	testCases := []struct {
		name       string
		method     string
		path       string
		authHeader string
		body       string
		wantStatus int
	}{
		{
			name:       "Create project without auth",
			method:     http.MethodPost,
			path:       "/v1/projects",
			authHeader: "",
			body:       `{"name":"unauth-proj"}`,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "List projects without auth",
			method:     http.MethodGet,
			path:       "/v1/projects",
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "Create deployment without auth",
			method:     http.MethodPost,
			path:       "/v1/projects/proj-alice/deployments",
			authHeader: "",
			body:       `{"image":"alpine:latest","instance_count":1}`,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "List deployments without auth",
			method:     http.MethodGet,
			path:       "/v1/projects/proj-alice/deployments",
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "Get deployment without auth",
			method:     http.MethodGet,
			path:       "/v1/deployments/dep-any",
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "List workers without auth",
			method:     http.MethodGet,
			path:       "/v1/workers",
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "Invalid/forged bearer token",
			method:     http.MethodGet,
			path:       "/v1/projects",
			authHeader: "Bearer forged-invalid-token",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "Public health check succeeds without auth",
			method:     http.MethodGet,
			path:       "/health",
			authHeader: "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "Valid admin token succeeds",
			method:     http.MethodGet,
			path:       "/v1/projects",
			authHeader: "Bearer admin-token",
			wantStatus: http.StatusOK,
		},
	}

	client := &http.Client{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var bodyReader io.Reader
			if tc.body != "" {
				bodyReader = bytes.NewBufferString(tc.body)
			}

			req, err := http.NewRequest(tc.method, stack.srv.URL+tc.path, bodyReader)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}

			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("client.Do: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tc.wantStatus {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("expected status %d, got %d: %s", tc.wantStatus, resp.StatusCode, string(body))
			}
		})
	}
	t.Log("SE-01 Passed: Unauthenticated or forged API calls are strictly rejected with 401 Unauthorized (Gate G-25)!")
}

// SE-02: Authorization boundaries between projects: User authorized for Project A cannot act on Project B's deployments.
func TestSE02_ProjectAuthorizationBoundaries_403(t *testing.T) {
	stack := newAuthTestStack(t)
	ctx := context.Background()

	// Pre-create Project A and Project B
	_ = stack.projectRepo.Create(ctx, &projects.Project{ID: "proj-alice", Name: "app-alice"})
	_ = stack.projectRepo.Create(ctx, &projects.Project{ID: "proj-bob", Name: "app-bob"})

	client := &http.Client{}

	// 1. Alice successfully deploys to her own project
	aliceDeployPayload := `{"image":"alice-app:v1","instance_count":1}`
	reqAlice, _ := http.NewRequest(http.MethodPost, stack.srv.URL+"/v1/projects/proj-alice/deployments", bytes.NewBufferString(aliceDeployPayload))
	reqAlice.Header.Set("Authorization", "Bearer alice-token")
	reqAlice.Header.Set("Content-Type", "application/json")
	respAlice, err := client.Do(reqAlice)
	if err != nil {
		t.Fatalf("alice deploy to proj-alice failed: %v", err)
	}
	defer respAlice.Body.Close()
	if respAlice.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(respAlice.Body)
		t.Fatalf("expected Alice deploy to proj-alice to succeed (202), got %d: %s", respAlice.StatusCode, string(b))
	}

	var aliceDep struct {
		ID string `json:"id"`
	}
	_ = json.NewDecoder(respAlice.Body).Decode(&aliceDep)

	// 2. Alice tries to deploy to Bob's project -> MUST return 403 Forbidden
	bobDeployPayload := `{"image":"mallory-injected:v1","instance_count":1}`
	reqCross, _ := http.NewRequest(http.MethodPost, stack.srv.URL+"/v1/projects/proj-bob/deployments", bytes.NewBufferString(bobDeployPayload))
	reqCross.Header.Set("Authorization", "Bearer alice-token")
	reqCross.Header.Set("Content-Type", "application/json")
	respCross, err := client.Do(reqCross)
	if err != nil {
		t.Fatalf("cross-project request failed: %v", err)
	}
	defer respCross.Body.Close()
	if respCross.StatusCode != http.StatusForbidden {
		b, _ := io.ReadAll(respCross.Body)
		t.Fatalf("SE-02 violation: expected 403 Forbidden for cross-project deploy, got %d: %s", respCross.StatusCode, string(b))
	}

	// 3. Alice tries to list deployments of Bob's project -> MUST return 403 Forbidden
	reqListBob, _ := http.NewRequest(http.MethodGet, stack.srv.URL+"/v1/projects/proj-bob/deployments", nil)
	reqListBob.Header.Set("Authorization", "Bearer alice-token")
	respListBob, err := client.Do(reqListBob)
	if err != nil {
		t.Fatalf("alice list bob deployments: %v", err)
	}
	defer respListBob.Body.Close()
	if respListBob.StatusCode != http.StatusForbidden {
		t.Fatalf("SE-02 violation: expected 403 Forbidden for cross-project list, got %d", respListBob.StatusCode)
	}

	// 4. Bob creates a deployment in his own project
	reqBob, _ := http.NewRequest(http.MethodPost, stack.srv.URL+"/v1/projects/proj-bob/deployments", bytes.NewBufferString(bobDeployPayload))
	reqBob.Header.Set("Authorization", "Bearer bob-token")
	reqBob.Header.Set("Content-Type", "application/json")
	respBob, err := client.Do(reqBob)
	if err != nil {
		t.Fatalf("bob deploy: %v", err)
	}
	defer respBob.Body.Close()
	if respBob.StatusCode != http.StatusAccepted {
		t.Fatalf("expected Bob deploy to succeed, got %d", respBob.StatusCode)
	}

	var bobDep struct {
		ID string `json:"id"`
	}
	_ = json.NewDecoder(respBob.Body).Decode(&bobDep)

	// 5. Alice attempts to GET Bob's deployment by ID -> MUST return 403 Forbidden
	reqGetBobDep, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/deployments/%s", stack.srv.URL, bobDep.ID), nil)
	reqGetBobDep.Header.Set("Authorization", "Bearer alice-token")
	respGetBobDep, err := client.Do(reqGetBobDep)
	if err != nil {
		t.Fatalf("alice get bob dep: %v", err)
	}
	defer respGetBobDep.Body.Close()
	if respGetBobDep.StatusCode != http.StatusForbidden {
		t.Fatalf("SE-02 violation: expected 403 Forbidden for cross-project deployment get, got %d", respGetBobDep.StatusCode)
	}

	t.Log("SE-02 Passed: Strict authorization boundaries enforced across projects; cross-project actions return 403 Forbidden!")
}
