package auth

import (
	"encoding/json"
	"errors"
	"net/http"
)

var (
	// ErrForbidden is returned when a user attempts an action outside their authorized projects.
	ErrForbidden = errors.New("forbidden: project access denied")
)

// AuthorizeProject verifies that the user has permission to access projectID.
func AuthorizeProject(user *User, projectID string) error {
	if user == nil {
		return ErrForbidden
	}
	if !user.HasProjectAccess(projectID) {
		return ErrForbidden
	}
	return nil
}

// CheckProjectAccess checks if the user from the request context can access the project.
// If unauthorized, it writes a 403 Forbidden response and returns false.
func CheckProjectAccess(w http.ResponseWriter, r *http.Request, projectID string) (*User, bool) {
	user := UserFromContext(r.Context())
	if user == nil {
		// If auth middleware is active, user must be present; if not present, deny
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
		return nil, false
	}

	if err := AuthorizeProject(user, projectID); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "forbidden: project access denied",
		})
		return user, false
	}

	return user, true
}
