package api

import (
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/nebula/nebula/internal/auth"
	"github.com/nebula/nebula/internal/projects"
)

func (s *Server) createProject(w http.ResponseWriter, r *http.Request) {
	var req ProjectCreateRequest
	if err := decodeBody(w, r, &req); err != nil {
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		writeErr(w, http.StatusBadRequest, "project name is required")
		return
	}

	p := &projects.Project{
		ID:            uuid.New().String(),
		Name:          req.Name,
		Description:   req.Description,
		RepoURL:       req.RepoURL,
		WebhookSecret: req.WebhookSecret,
	}
	if err := s.projects.Create(r.Context(), p); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}

	s.log.Info().Str("project_id", p.ID).Str("name", p.Name).Msg("project created")
	writeJSON(w, http.StatusCreated, newProjectResponse(p))
}

func (s *Server) listProjects(w http.ResponseWriter, r *http.Request) {
	list, err := s.projects.List(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	if s.auth != nil {
		user := auth.UserFromContext(r.Context())
		if user != nil {
			filtered := make([]*projects.Project, 0, len(list))
			for _, p := range list {
				if user.HasProjectAccess(p.ID) {
					filtered = append(filtered, p)
				}
			}
			list = filtered
		}
	}

	projectsResponse := make([]ProjectResponse, 0, len(list))
	for _, p := range list {
		projectsResponse = append(projectsResponse, newProjectResponse(p))
	}
	writeJSON(w, http.StatusOK, projectsResponse)
}

func (s *Server) getProject(w http.ResponseWriter, r *http.Request) {
	p, err := s.projects.GetByID(r.Context(), r.PathValue("id"))
	if err != nil {
		notFound(w, err)
		return
	}

	if s.auth != nil {
		user := auth.UserFromContext(r.Context())
		if user != nil && !user.HasProjectAccess(p.ID) {
			writeErr(w, http.StatusForbidden, "forbidden: project access denied")
			return
		}
	}

	writeJSON(w, http.StatusOK, newProjectResponse(p))
}