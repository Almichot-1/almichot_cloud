package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/nebula/nebula/internal/auth"
	"github.com/nebula/nebula/internal/deployments"
)

func (s *Server) createDeployment(w http.ResponseWriter, r *http.Request) {
	idempKey := r.Header.Get("Idempotency-Key")
	if idempKey == "" {
		idempKey = r.Header.Get("X-Idempotency-Key")
	}

	if idempKey != "" && s.idempotency != nil {
		entry, isFirst := s.idempotency.GetOrReserve(idempKey)
		if !isFirst {
			code, header, body := entry.WaitAndGet()
			for k, vv := range header {
				for _, v := range vv {
					w.Header().Add(k, v)
				}
			}
			w.Header().Set("Idempotent-Replayed", "true")
			w.WriteHeader(code)
			_, _ = w.Write(body)
			return
		}
	}

	respond := func(status int, v any) {
		data, _ := json.Marshal(v)
		w.Header().Set("Content-Type", "application/json")
		if idempKey != "" && s.idempotency != nil {
			s.idempotency.Complete(idempKey, status, w.Header(), data)
		}
		w.WriteHeader(status)
		_, _ = w.Write(data)
	}

	projectID := r.PathValue("projectID")
	if s.auth != nil {
		user := auth.UserFromContext(r.Context())
		if user != nil && !user.HasProjectAccess(projectID) {
			respond(http.StatusForbidden, ErrorResponse{Error: "forbidden: project access denied"})
			return
		}
	}

	if _, err := s.projects.GetByID(r.Context(), projectID); err != nil {
		respond(http.StatusNotFound, ErrorResponse{Error: err.Error()})
		return
	}

	var req DeploymentCreateRequest
	if err := decodeBody(w, r, &req); err != nil {
		if idempKey != "" && s.idempotency != nil {
			s.idempotency.Complete(idempKey, http.StatusBadRequest, w.Header(), []byte(`{"error":"invalid request body"}`))
		}
		return
	}
	if req.Image == "" && req.SourcePath == "" {
		respond(http.StatusBadRequest, ErrorResponse{Error: "either image or source_path is required"})
		return
	}

	params := deployments.CreateDeploymentParams{
		ProjectID:     projectID,
		SourcePath:    req.SourcePath,
		Image:         req.Image,
		InstanceCount: req.InstanceCount,
		Env:           req.Env,
		Labels:        req.Labels,
		Ports:         req.Ports,
	}

	dep, instances, err := s.svc.CreateAndDeploy(r.Context(), params)
	if err != nil {
		body := ErrorResponse{Error: err.Error()}
		if dep != nil {
			body.DeploymentID = dep.ID
			body.Status = string(dep.Status)
		}
		respond(http.StatusBadGateway, body)
		return
	}

	s.log.Info().
		Str("deployment_id", dep.ID).
		Str("project_id", projectID).
		Str("status", string(dep.Status)).
		Str("image_digest", dep.ImageDigest).
		Msg("deployment created via HTTP API")

	respond(http.StatusAccepted, newDeploymentResponse(dep, instances))
}

func (s *Server) getDeployment(w http.ResponseWriter, r *http.Request) {
	dep, instances, err := s.svc.GetDeployment(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, deployments.ErrDeploymentNotFound) {
			writeErr(w, http.StatusNotFound, err.Error())
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	if s.auth != nil {
		user := auth.UserFromContext(r.Context())
		if user != nil && !user.HasProjectAccess(dep.ProjectID) {
			writeErr(w, http.StatusForbidden, "forbidden: project access denied")
			return
		}
	}

	writeJSON(w, http.StatusOK, newDeploymentResponse(dep, instances))
}

func (s *Server) listDeployments(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("projectID")
	if s.auth != nil {
		user := auth.UserFromContext(r.Context())
		if user != nil && !user.HasProjectAccess(projectID) {
			writeErr(w, http.StatusForbidden, "forbidden: project access denied")
			return
		}
	}

	list, err := s.svc.ListDeployments(r.Context(), projectID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := make([]DeploymentResponse, 0, len(list))
	for _, dep := range list {
		response = append(response, newDeploymentResponse(dep, nil))
	}
	writeJSON(w, http.StatusOK, response)
}