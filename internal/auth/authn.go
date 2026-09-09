package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

type userContextKey struct{}

var (
	// ErrUnauthenticated is returned when credentials are missing or invalid.
	ErrUnauthenticated = errors.New("unauthorized: missing or invalid credentials")
)

// Authenticator defines the contract for authenticating incoming HTTP requests.
type Authenticator interface {
	Authenticate(r *http.Request) (*User, error)
}

// TokenAuthenticator validates requests using a TokenStore.
type TokenAuthenticator struct {
	store *TokenStore
}

// NewTokenAuthenticator creates an authenticator backed by a TokenStore.
func NewTokenAuthenticator(store *TokenStore) *TokenAuthenticator {
	return &TokenAuthenticator{store: store}
}

// Authenticate extracts and validates a bearer token or API key from the request.
func (a *TokenAuthenticator) Authenticate(r *http.Request) (*User, error) {
	token := extractToken(r)
	if token == "" {
		return nil, ErrUnauthenticated
	}
	user, err := a.store.ValidateToken(token)
	if err != nil {
		return nil, ErrUnauthenticated
	}
	return user, nil
}

func extractToken(r *http.Request) string {
	authHeader := r.Header.Get("Authorization")
	if authHeader != "" {
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) == 2 && (strings.EqualFold(parts[0], "Bearer") || strings.EqualFold(parts[0], "Token")) {
			return strings.TrimSpace(parts[1])
		}
		// If raw token supplied in Authorization header
		if len(parts) == 1 && parts[0] != "" {
			return strings.TrimSpace(parts[0])
		}
	}
	if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
		return strings.TrimSpace(apiKey)
	}
	return ""
}

// WithUser returns a context carrying the authenticated User.
func WithUser(ctx context.Context, user *User) context.Context {
	return context.WithValue(ctx, userContextKey{}, user)
}

// UserFromContext retrieves the authenticated User from the context, or nil.
func UserFromContext(ctx context.Context) *User {
	if ctx == nil {
		return nil
	}
	u, _ := ctx.Value(userContextKey{}).(*User)
	return u
}

// AuthMiddleware creates HTTP middleware that enforces authentication.
// Paths exempt from Bearer auth include /health, /services/, and /v1/projects/{id}/webhooks (which use HMAC signatures).
func AuthMiddleware(auth Authenticator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path := r.URL.Path

			// Allow public routes
			if path == "/health" || path == "/v1/health" ||
				strings.HasPrefix(path, "/services/") ||
				strings.Contains(path, "/webhooks") {
				next.ServeHTTP(w, r)
				return
			}

			if auth == nil {
				next.ServeHTTP(w, r)
				return
			}

			user, err := auth.Authenticate(r)
			if err != nil || user == nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"error": "unauthorized: valid authentication required",
				})
				return
			}

			ctx := WithUser(r.Context(), user)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
