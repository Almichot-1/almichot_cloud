package loadbalancer

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"regexp"
)

// WAFRule represents a single inspection rule.
type WAFRule struct {
	Name    string
	Pattern *regexp.Regexp
}

var (
	// Basic WAF rule set (§17, Phase 15)
	wafRules = []WAFRule{
		{
			Name:    "sql_injection",
			Pattern: regexp.MustCompile(`(?i)('|\b)(UNION\s+SELECT|SELECT\s+.*\s+FROM|INSERT\s+INTO|DROP\s+TABLE|--|\bOR\b\s+['"\d\w]+=['"\d\w]+)`),
		},
		{
			Name:    "path_traversal",
			Pattern: regexp.MustCompile(`(\.\./|\.\.\\|\.%2e/|\.%2e\\|%2e%2e)`),
		},
		{
			Name:    "null_byte_injection",
			Pattern: regexp.MustCompile(`(%00|\x00)`),
		},
		{
			Name:    "script_tag_xss",
			Pattern: regexp.MustCompile(`(?i)(<script.*?>|javascript:|onload=|<iframe)`),
		},
	}
)

// WAFMiddleware inspects incoming requests and blocks malicious patterns.
func WAFMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 1. Check URI length
		if len(r.RequestURI) > 4096 {
			http.Error(w, `{"error":"request blocked by WAF: uri too long"}`, http.StatusBadRequest)
			return
		}

		// 2. Decode raw path and query to inspect for attacks
		decodedURI, err := url.QueryUnescape(r.RequestURI)
		if err != nil {
			decodedURI = r.RequestURI
		}

		for _, rule := range wafRules {
			if rule.Pattern.MatchString(decodedURI) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"request blocked by WAF","rule":"` + rule.Name + `"}`))
				return
			}
		}

		// 3. Inspect request body if small text payload (e.g. JSON/form)
		if r.Body != nil && r.ContentLength > 0 && r.ContentLength < 65536 {
			bodyBytes, err := io.ReadAll(r.Body)
			if err == nil {
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes)) // restore body
				bodyStr := string(bodyBytes)
				for _, rule := range wafRules {
					if rule.Pattern.MatchString(bodyStr) {
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusBadRequest)
						_, _ = w.Write([]byte(`{"error":"request blocked by WAF in payload","rule":"` + rule.Name + `"}`))
						return
					}
				}
			}
		}

		next.ServeHTTP(w, r)
	})
}
