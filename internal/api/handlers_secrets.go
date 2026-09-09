package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/nebula/nebula/internal/auth"
	"github.com/nebula/nebula/internal/secrets"
)

type SecretCreateRequest struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type SecretRotateRequest struct {
	Value string `json:"value"`
}

type SecretResponse struct {
	ID         string    `json:"id"`
	ProjectID  string    `json:"project_id"`
	Name       string    `json:"name"`
	KeyVersion int       `json:"key_version"`
	Version    int       `json:"version"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func (s *Server) createSecret(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("projectID")
	if s.auth != nil {
		user := auth.UserFromContext(r.Context())
		if user != nil && !user.HasProjectAccess(projectID) {
			writeErr(w, http.StatusForbidden, "forbidden: project access denied")
			return
		}
	}

	if s.secretStore == nil {
		writeErr(w, http.StatusNotImplemented, "secret store is not configured")
		return
	}

	var req SecretCreateRequest
	if err := decodeBody(w, r, &req); err != nil {
		return
	}
	if req.Name == "" || req.Value == "" {
		writeErr(w, http.StatusBadRequest, "name and value are required")
		return
	}

	// Register secret value for redaction
	if s.redactor != nil {
		s.redactor.Register(req.Value)
	}

	sec, err := s.secretStore.SetSecret(r.Context(), projectID, req.Name, req.Value)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, SecretResponse{
		ID:         sec.ID,
		ProjectID:  sec.ProjectID,
		Name:       sec.Name,
		KeyVersion: sec.KeyVersion,
		Version:    sec.Version,
		CreatedAt:  sec.CreatedAt,
		UpdatedAt:  sec.UpdatedAt,
	})
}

func (s *Server) listSecrets(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("projectID")
	if s.auth != nil {
		user := auth.UserFromContext(r.Context())
		if user != nil && !user.HasProjectAccess(projectID) {
			writeErr(w, http.StatusForbidden, "forbidden: project access denied")
			return
		}
	}

	if s.secretStore == nil {
		writeErr(w, http.StatusNotImplemented, "secret store is not configured")
		return
	}

	list, err := s.secretStore.ListSecrets(r.Context(), projectID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	res := make([]SecretResponse, 0, len(list))
	for _, sec := range list {
		res = append(res, SecretResponse{
			ID:         sec.ID,
			ProjectID:  sec.ProjectID,
			Name:       sec.Name,
			KeyVersion: sec.KeyVersion,
			Version:    sec.Version,
			CreatedAt:  sec.CreatedAt,
			UpdatedAt:  sec.UpdatedAt,
		})
	}

	writeJSON(w, http.StatusOK, res)
}

func (s *Server) rotateSecret(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("projectID")
	secretName := r.PathValue("name")

	if s.auth != nil {
		user := auth.UserFromContext(r.Context())
		if user != nil && !user.HasProjectAccess(projectID) {
			writeErr(w, http.StatusForbidden, "forbidden: project access denied")
			return
		}
	}

	if s.secretStore == nil {
		writeErr(w, http.StatusNotImplemented, "secret store is not configured")
		return
	}

	var req SecretRotateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Value == "" {
		writeErr(w, http.StatusBadRequest, "new secret value is required")
		return
	}

	if s.redactor != nil {
		s.redactor.Register(req.Value)
	}

	sec, err := secrets.RotateSecret(r.Context(), s.secretStore, projectID, secretName, req.Value)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, SecretResponse{
		ID:         sec.ID,
		ProjectID:  sec.ProjectID,
		Name:       sec.Name,
		KeyVersion: sec.KeyVersion,
		Version:    sec.Version,
		CreatedAt:  sec.CreatedAt,
		UpdatedAt:  sec.UpdatedAt,
	})
}
