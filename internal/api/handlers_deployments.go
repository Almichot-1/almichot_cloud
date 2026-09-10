package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/nebula/nebula/internal/auth"
	"github.com/nebula/nebula/internal/autoscaler"
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

	p, err := s.projects.GetByID(r.Context(), projectID)
	if err != nil {
		p, err = s.projects.GetByName(r.Context(), projectID)
		if err != nil {
			respond(http.StatusNotFound, ErrorResponse{Error: "project not found"})
			return
		}
		projectID = p.ID
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
	if p, err := s.projects.GetByID(r.Context(), projectID); err == nil {
		projectID = p.ID
	} else if p, err := s.projects.GetByName(r.Context(), projectID); err == nil {
		projectID = p.ID
	}

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

func (s *Server) rollbackDeployment(w http.ResponseWriter, r *http.Request) {
	depID := r.PathValue("id")
	dep, _, err := s.svc.GetDeployment(r.Context(), depID)
	if err != nil {
		if errors.Is(err, deployments.ErrDeploymentNotFound) {
			writeErr(w, http.StatusNotFound, "deployment not found")
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

	newDep, instances, err := s.svc.Rollback(r.Context(), depID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	w.Header().Set("X-Rolled-Back-Deployment-ID", depID)
	writeJSON(w, http.StatusOK, newDeploymentResponse(newDep, instances))
}

func (s *Server) stopDeployment(w http.ResponseWriter, r *http.Request) {
	depID := r.PathValue("id")
	dep, _, err := s.svc.GetDeployment(r.Context(), depID)
	if err != nil {
		if errors.Is(err, deployments.ErrDeploymentNotFound) {
			writeErr(w, http.StatusNotFound, "deployment not found")
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

	stoppedDep, err := s.svc.StopDeployment(r.Context(), depID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, newDeploymentResponse(stoppedDep, nil))
}

func (s *Server) listProjectEvents(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("projectID")
	if s.auth != nil {
		user := auth.UserFromContext(r.Context())
		if user != nil && !user.HasProjectAccess(projectID) {
			writeErr(w, http.StatusForbidden, "forbidden: project access denied")
			return
		}
	}

	if s.svc.EventRepo() == nil {
		writeJSON(w, http.StatusOK, []*deployments.Event{})
		return
	}

	events, err := s.svc.EventRepo().ListByProject(r.Context(), projectID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if events == nil {
		events = []*deployments.Event{}
	}
	writeJSON(w, http.StatusOK, events)
}

func (s *Server) listDeploymentEvents(w http.ResponseWriter, r *http.Request) {
	depID := r.PathValue("id")
	dep, _, err := s.svc.GetDeployment(r.Context(), depID)
	if err != nil {
		if errors.Is(err, deployments.ErrDeploymentNotFound) {
			writeErr(w, http.StatusNotFound, "deployment not found")
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

	if s.svc.EventRepo() == nil {
		writeJSON(w, http.StatusOK, []*deployments.Event{})
		return
	}

	events, err := s.svc.EventRepo().ListByDeployment(r.Context(), depID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if events == nil {
		events = []*deployments.Event{}
	}
	writeJSON(w, http.StatusOK, events)
}

// ScaleRequest carries desired replica count for manual scaling (§18).
type ScaleRequest struct {
	DesiredReplicas int `json:"desired_replicas"`
}

func (s *Server) scaleDeployment(w http.ResponseWriter, r *http.Request) {
	depID := r.PathValue("id")
	dep, _, err := s.svc.GetDeployment(r.Context(), depID)
	if err != nil {
		if errors.Is(err, deployments.ErrDeploymentNotFound) {
			writeErr(w, http.StatusNotFound, "deployment not found")
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

	var req ScaleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}

	scaledDep, instances, err := s.svc.ScaleDeployment(r.Context(), depID, req.DesiredReplicas)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, newDeploymentResponse(scaledDep, instances))
}

func (s *Server) scaleProject(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("projectID")
	if s.auth != nil {
		user := auth.UserFromContext(r.Context())
		if user != nil && !user.HasProjectAccess(projectID) {
			writeErr(w, http.StatusForbidden, "forbidden: project access denied")
			return
		}
	}

	var req ScaleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}

	dep, instances, err := s.svc.ScaleProject(r.Context(), projectID, req.DesiredReplicas)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, newDeploymentResponse(dep, instances))
}

func (s *Server) setAutoscalerPolicy(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("projectID")
	if s.auth != nil {
		user := auth.UserFromContext(r.Context())
		if user != nil && !user.HasProjectAccess(projectID) {
			writeErr(w, http.StatusForbidden, "forbidden: project access denied")
			return
		}
	}

	if s.autoscaler == nil {
		writeErr(w, http.StatusNotImplemented, "autoscaler not configured")
		return
	}

	var policy autoscaler.ScalingPolicy
	if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	policy.ProjectID = projectID

	s.autoscaler.SetPolicy(&policy)
	writeJSON(w, http.StatusOK, policy)
}

func (s *Server) getAutoscalerPolicy(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("projectID")
	if s.auth != nil {
		user := auth.UserFromContext(r.Context())
		if user != nil && !user.HasProjectAccess(projectID) {
			writeErr(w, http.StatusForbidden, "forbidden: project access denied")
			return
		}
	}

	if s.autoscaler == nil {
		writeErr(w, http.StatusNotImplemented, "autoscaler not configured")
		return
	}

	policy, ok := s.autoscaler.GetPolicy(projectID)
	if !ok {
		writeErr(w, http.StatusNotFound, "autoscaling policy not found")
		return
	}

	writeJSON(w, http.StatusOK, policy)
}

func (s *Server) evaluateAutoscaler(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("projectID")
	if s.auth != nil {
		user := auth.UserFromContext(r.Context())
		if user != nil && !user.HasProjectAccess(projectID) {
			writeErr(w, http.StatusForbidden, "forbidden: project access denied")
			return
		}
	}

	if s.autoscaler == nil {
		writeErr(w, http.StatusNotImplemented, "autoscaler not configured")
		return
	}

	decision, err := s.autoscaler.EvaluateAndScale(r.Context(), projectID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, decision)
}
