package loadbalancer

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	"golang.org/x/time/rate"
)

// TenantRateLimiter enforces token-bucket rate limits on a per-tenant or per-client basis (§17, Phase 15).
type TenantRateLimiter struct {
	mu          sync.Mutex
	limiters    map[string]*rate.Limiter
	r           rate.Limit
	b           int
	tenantFn    func(r *http.Request) string
	blockedHits uint64
}

// NewTenantRateLimiter constructs a rate limiter allowing `reqPerSec` with burst `burst`.
func NewTenantRateLimiter(reqPerSec float64, burst int, tenantExtractor func(r *http.Request) string) *TenantRateLimiter {
	if tenantExtractor == nil {
		tenantExtractor = DefaultTenantExtractor
	}
	return &TenantRateLimiter{
		limiters: make(map[string]*rate.Limiter),
		r:        rate.Limit(reqPerSec),
		b:        burst,
		tenantFn: tenantExtractor,
	}
}

// DefaultTenantExtractor resolves the client/tenant key from Host, X-Project-ID, or remote address.
func DefaultTenantExtractor(r *http.Request) string {
	if h := r.Header.Get("X-Project-ID"); h != "" {
		return "tenant:" + h
	}
	if host := r.Host; host != "" {
		h := strings.Split(host, ":")[0]
		return "host:" + strings.ToLower(h)
	}
	if ip := r.Header.Get("X-Forwarded-For"); ip != "" {
		return "ip:" + strings.Split(ip, ",")[0]
	}
	return "remote:" + strings.Split(r.RemoteAddr, ":")[0]
}

// getLimiter retrieves or creates a token bucket for a tenant.
func (trl *TenantRateLimiter) getLimiter(tenantKey string) *rate.Limiter {
	trl.mu.Lock()
	defer trl.mu.Unlock()

	lim, exists := trl.limiters[tenantKey]
	if !exists {
		lim = rate.NewLimiter(trl.r, trl.b)
		trl.limiters[tenantKey] = lim
	}
	return lim
}

// Middleware returns an HTTP middleware enforcing rate limits.
func (trl *TenantRateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantKey := trl.tenantFn(r)
		lim := trl.getLimiter(tenantKey)

		if !lim.Allow() {
			w.Header().Set("Retry-After", "1")
			w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%g", trl.r))
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"too many requests","message":"rate limit exceeded for tenant"}`))
			return
		}

		w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%g", trl.r))
		w.Header().Set("X-RateLimit-Remaining", "1")
		next.ServeHTTP(w, r)
	})
}
