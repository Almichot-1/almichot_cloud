package api

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/nebula/nebula/internal/auth"
	"github.com/nebula/nebula/internal/deployments"
)

// WebhookPayload represents the Git push payload parsed from webhooks.
type WebhookPayload struct {
	Ref           string `json:"ref"`
	After         string `json:"after"`
	SourcePath    string `json:"source_path"`
	Image         string `json:"image"`
	InstanceCount int    `json:"instance_count"`
}

func (s *Server) handleProjectWebhook(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("projectID")
	if projectID == "" {
		writeErr(w, http.StatusBadRequest, "missing projectID")
		return
	}

	proj, err := s.projects.GetByID(r.Context(), projectID)
	if err != nil {
		writeErr(w, http.StatusNotFound, "project not found")
		return
	}

	bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "failed to read payload")
		return
	}

	sigHeader := r.Header.Get("X-Hub-Signature-256")
	if sigHeader == "" {
		sigHeader = r.Header.Get("X-Nebula-Signature")
	}
	if sigHeader == "" {
		sigHeader = r.Header.Get("X-Signature-256")
	}

	if sigHeader == "" {
		writeErr(w, http.StatusUnauthorized, "missing webhook signature header")
		return
	}

	secret := proj.WebhookSecret
	if secret == "" {
		secret = "nebula-default-webhook-secret"
	}

	if !auth.VerifyWebhookSignature([]byte(secret), bodyBytes, sigHeader) {
		s.log.Warn().
			Str("project_id", projectID).
			Str("signature", sigHeader).
			Msg("webhook signature verification failed: rejected")
		writeErr(w, http.StatusUnauthorized, "invalid webhook signature")
		return
	}

	var payload WebhookPayload
	if len(bodyBytes) > 0 {
		_ = json.Unmarshal(bodyBytes, &payload)
	}

	if payload.InstanceCount <= 0 {
		payload.InstanceCount = 1
	}

	depParams := deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		SourcePath:    payload.SourcePath,
		Image:         payload.Image,
		InstanceCount: payload.InstanceCount,
	}

	// Trigger deployment
	dep, instances, err := s.svc.CreateAndDeploy(r.Context(), depParams)
	if err != nil {
		body := ErrorResponse{Error: err.Error()}
		if dep != nil {
			body.DeploymentID = dep.ID
			body.Status = string(dep.Status)
		}
		writeJSON(w, http.StatusBadGateway, body)
		return
	}

	s.log.Info().
		Str("project_id", projectID).
		Str("deployment_id", dep.ID).
		Str("ref", payload.Ref).
		Msg("valid webhook accepted and deployment triggered")

	writeJSON(w, http.StatusAccepted, newDeploymentResponse(dep, instances))
}
