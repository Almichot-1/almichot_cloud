package api

import (
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// responseWriter wraps http.ResponseWriter to capture status for logging.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}

// requestID injects a request ID header and adds it to the request context.
func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rid := r.Header.Get("X-Request-ID")
		if rid == "" {
			rid = uuid.New().String()[:8]
		}
		w.Header().Set("X-Request-ID", rid)
		next.ServeHTTP(w, r)
	})
}

// requestLogger logs method, path, status, and duration of each request.
func requestLogger(log zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rw, r)

			log.Info().
				Str("method", r.Method).
				Str("path", r.URL.Path).
				Str("request_id", rw.Header().Get("X-Request-ID")).
				Int("status", rw.status).
				Dur("duration_ms", time.Since(start).Round(time.Millisecond)).
				Msg("http request handled")
		})
	}
}

// recoverer recovers from panics in downstream handlers and returns a 500.
func recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				writeErr(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// RequireHTTPS enforces HTTPS traffic and injects HSTS headers (§21.1, Phase 13).
// If enforce is true:
// - Plaintext requests (r.TLS == nil and X-Forwarded-Proto != "https") are rejected/redirected.
// - Injects Strict-Transport-Security: max-age=63072000; includeSubDomains; preload.
func RequireHTTPS(enforce bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Always inject HSTS header
			w.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains; preload")

			if enforce {
				isHTTPS := r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https"
				if !isHTTPS {
					if r.Method == http.MethodGet || r.Method == http.MethodHead {
						target := "https://" + r.Host + r.URL.RequestURI()
						http.Redirect(w, r, target, http.StatusMovedPermanently)
						return
					}
					writeErr(w, http.StatusUpgradeRequired, "HTTPS is required for all API interactions")
					return
				}
			}

			next.ServeHTTP(w, r)
		})
	}
}