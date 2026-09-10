package api

import "net/http"

func (s *Server) listWorkers(w http.ResponseWriter, r *http.Request) {
	list := s.registry.List()

	response := make([]WorkerResponse, 0, len(list))
	for _, wkr := range list {
		response = append(response, newWorkerResponse(wkr))
	}
	writeJSON(w, http.StatusOK, response)
}
