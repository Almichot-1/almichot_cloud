package api

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/nebula/nebula/internal/auth"
	"github.com/nebula/nebula/internal/deployments"
)

// listProjectLogs returns logs/events for a project, with optional ?follow=true streaming.
func (s *Server) listProjectLogs(w http.ResponseWriter, r *http.Request) {
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

	follow := r.URL.Query().Get("follow") == "true"
	flusher, isFlusher := w.(http.Flusher)

	if follow && isFlusher {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Transfer-Encoding", "chunked")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		cursor := 0
		pollTicker := time.NewTicker(300 * time.Millisecond)
		defer pollTicker.Stop()

		timeout := time.After(30 * time.Second)

		for {
			select {
			case <-r.Context().Done():
				return
			case <-timeout:
				return
			case <-pollTicker.C:
				if s.svc.EventRepo() != nil {
					events, err := s.svc.EventRepo().ListByProject(r.Context(), projectID)
					if err == nil && len(events) > cursor {
						for _, ev := range events[cursor:] {
							line := formatEventAsLog(ev)
							_, _ = fmt.Fprintln(w, line)
						}
						cursor = len(events)
						flusher.Flush()
					}
				}
			}
		}
	}

	// Non-streaming response
	var events []*deployments.Event
	if s.svc.EventRepo() != nil {
		var err error
		events, err = s.svc.EventRepo().ListByProject(r.Context(), projectID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	if events == nil {
		events = []*deployments.Event{}
	}
	if len(events) == 0 && s.svc.DepRepo() != nil {
		deps, _ := s.svc.DepRepo().List(r.Context(), projectID)
		for _, d := range deps {
			events = append(events, &deployments.Event{
				ProjectID:    d.ProjectID,
				DeploymentID: d.ID,
				EventType:    "DEPLOYMENT_STATUS",
				Message:      fmt.Sprintf("Deployment %s is %s (stage: %s, image: %s)", d.ID[:8], d.Status, d.Stage, d.Image),
				CreatedAt:    d.CreatedAt,
			})
		}
	}

	if strings.Contains(r.Header.Get("Accept"), "application/json") {
		writeJSON(w, http.StatusOK, events)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	for _, ev := range events {
		_, _ = fmt.Fprintln(w, formatEventAsLog(ev))
	}
}

// listDeploymentLogs returns logs/events for a specific deployment, with optional ?follow=true.
func (s *Server) listDeploymentLogs(w http.ResponseWriter, r *http.Request) {
	depID := r.PathValue("id")
	dep, _, err := s.svc.GetDeployment(r.Context(), depID)
	if err != nil {
		writeErr(w, http.StatusNotFound, "deployment not found")
		return
	}

	if s.auth != nil {
		user := auth.UserFromContext(r.Context())
		if user != nil && !user.HasProjectAccess(dep.ProjectID) {
			writeErr(w, http.StatusForbidden, "forbidden: project access denied")
			return
		}
	}

	follow := r.URL.Query().Get("follow") == "true"
	flusher, isFlusher := w.(http.Flusher)

	if follow && isFlusher {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Transfer-Encoding", "chunked")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		cursor := 0
		pollTicker := time.NewTicker(300 * time.Millisecond)
		defer pollTicker.Stop()

		timeout := time.After(30 * time.Second)

		for {
			select {
			case <-r.Context().Done():
				return
			case <-timeout:
				return
			case <-pollTicker.C:
				if s.svc.EventRepo() != nil {
					events, err := s.svc.EventRepo().ListByDeployment(r.Context(), depID)
					if err == nil && len(events) > cursor {
						for _, ev := range events[cursor:] {
							line := formatEventAsLog(ev)
							_, _ = fmt.Fprintln(w, line)
						}
						cursor = len(events)
						flusher.Flush()
					}
				}
			}
		}
	}

	var events []*deployments.Event
	if s.svc.EventRepo() != nil {
		var err error
		events, err = s.svc.EventRepo().ListByDeployment(r.Context(), depID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	if events == nil {
		events = []*deployments.Event{}
	}

	if strings.Contains(r.Header.Get("Accept"), "application/json") {
		writeJSON(w, http.StatusOK, events)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	for _, ev := range events {
		_, _ = fmt.Fprintln(w, formatEventAsLog(ev))
	}
}

func formatEventAsLog(ev *deployments.Event) string {
	ts := ev.CreatedAt.Format(time.RFC3339)
	metaStr := ""
	if len(ev.Metadata) > 0 {
		var parts []string
		for k, v := range ev.Metadata {
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
		metaStr = " (" + strings.Join(parts, ", ") + ")"
	}
	return fmt.Sprintf("[%s] %s: %s%s", ts, ev.EventType, ev.Message, metaStr)
}
