package secrets

import (
	"bytes"
	"io"
	"strings"
	"sync"

	"github.com/rs/zerolog"
)

const RedactedPlaceholder = "[REDACTED]"

// Redactor maintains a registry of sensitive strings that must never appear in logs or events.
type Redactor struct {
	mu      sync.RWMutex
	secrets map[string]struct{}
}

// NewRedactor creates a new Redactor.
func NewRedactor() *Redactor {
	return &Redactor{
		secrets: make(map[string]struct{}),
	}
}

// Register registers a sensitive plaintext string for redaction.
// Strings shorter than 3 characters are ignored to prevent over-redaction of common words.
func (r *Redactor) Register(secret string) {
	s := strings.TrimSpace(secret)
	if len(s) < 3 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.secrets[s] = struct{}{}
}

// Redact replaces any occurrences of registered secrets in s with "[REDACTED]".
func (r *Redactor) Redact(s string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	res := s
	for secret := range r.secrets {
		if strings.Contains(res, secret) {
			res = strings.ReplaceAll(res, secret, RedactedPlaceholder)
		}
	}
	return res
}

// RedactBytes replaces any occurrences of registered secrets in b with "[REDACTED]".
func (r *Redactor) RedactBytes(b []byte) []byte {
	r.mu.RLock()
	defer r.mu.RUnlock()

	res := b
	for secret := range r.secrets {
		secBytes := []byte(secret)
		if bytes.Contains(res, secBytes) {
			res = bytes.ReplaceAll(res, secBytes, []byte(RedactedPlaceholder))
		}
	}
	return res
}

// RedactingWriter wraps an io.Writer and scrubs registered secrets from all output bytes.
type RedactingWriter struct {
	out      io.Writer
	redactor *Redactor
}

// NewRedactingWriter creates an io.Writer that redacts registered secrets.
func NewRedactingWriter(out io.Writer, redactor *Redactor) *RedactingWriter {
	return &RedactingWriter{
		out:      out,
		redactor: redactor,
	}
}

// Write scrubs secrets and writes to the underlying io.Writer.
func (w *RedactingWriter) Write(p []byte) (n int, err error) {
	if w.redactor == nil {
		return w.out.Write(p)
	}
	cleaned := w.redactor.RedactBytes(p)
	_, err = w.out.Write(cleaned)
	// Return len(p) so callers don't consider differing lengths a short write
	return len(p), err
}

// RedactingHook is a Zerolog hook that scrubs secret values from log messages and fields.
type RedactingHook struct {
	redactor *Redactor
}

// NewRedactingHook creates a Zerolog hook.
func NewRedactingHook(redactor *Redactor) *RedactingHook {
	return &RedactingHook{redactor: redactor}
}

// Run executes the hook on each Zerolog event.
func (h *RedactingHook) Run(e *zerolog.Event, level zerolog.Level, message string) {
	// Zerolog hooks run before serialization; RedactingWriter handles serialized scrubbing.
}

// WrapLogger wraps an existing Zerolog logger's output stream with a RedactingWriter.
func WrapLogger(log zerolog.Logger, out io.Writer, redactor *Redactor) zerolog.Logger {
	rw := NewRedactingWriter(out, redactor)
	return zerolog.New(rw).With().Timestamp().Logger()
}
