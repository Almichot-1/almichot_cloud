package auth

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrInvalidToken is returned when a token is unknown, expired, or malformed.
	ErrInvalidToken = errors.New("invalid or expired token")
)

// User represents an authenticated identity making requests to the Control Plane.
type User struct {
	ID              string   `json:"id"`
	Username        string   `json:"username"`
	Role            string   `json:"role"`             // "admin", "member", "service"
	AllowedProjects []string `json:"allowed_projects"` // Project IDs or names permitted; ["*"] for wildcard
}

// HasProjectAccess checks whether the user is authorized to perform actions on projectID.
func (u *User) HasProjectAccess(projectID string) bool {
	if u == nil {
		return false
	}
	if u.Role == "admin" {
		return true
	}
	for _, p := range u.AllowedProjects {
		if p == "*" || p == projectID {
			return true
		}
	}
	return false
}

// TokenStore manages tokens and maps them to User claims.
type TokenStore struct {
	mu     sync.RWMutex
	tokens map[string]*User
}

// NewTokenStore initializes an in-memory token store.
func NewTokenStore() *TokenStore {
	return &TokenStore{
		tokens: make(map[string]*User),
	}
}

// RegisterToken binds a specific token string to a User.
func (s *TokenStore) RegisterToken(token string, user *User) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens[token] = user
}

// RevokeToken removes a token from the store.
func (s *TokenStore) RevokeToken(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tokens, token)
}

// ValidateToken returns the User associated with the token or ErrInvalidToken.
func (s *TokenStore) ValidateToken(token string) (*User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	user, ok := s.tokens[token]
	if !ok || user == nil {
		return nil, ErrInvalidToken
	}
	return user, nil
}

// GenerateToken generates a cryptographically secure token and registers it for user.
func (s *TokenStore) GenerateToken(user *User) (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate random token: %w", err)
	}
	token := hex.EncodeToString(b)
	s.RegisterToken(token, user)
	return token, nil
}
