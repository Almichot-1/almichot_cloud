package build

import (
	"regexp"
	"strconv"
	"strings"
)

var exposeRe = regexp.MustCompile(`(?m)^\s*EXPOSE\s+(.+)$`)

// ExposedPorts extracts container ports declared via EXPOSE in a Dockerfile.
// Supports single ports or space/comma separated lists, optionally with protocol (e.g. "8080/tcp").
func ExposedPorts(dockerfileContent string) []int {
	if strings.TrimSpace(dockerfileContent) == "" {
		return nil
	}

	var ports []int
	seen := make(map[int]bool)
	for _, match := range exposeRe.FindAllStringSubmatch(dockerfileContent, -1) {
		for _, token := range strings.FieldsFunc(match[1], func(r rune) bool {
			return r == ' ' || r == '\t' || r == ','
		}) {
			token = strings.TrimSpace(token)
			if token == "" {
				continue
			}
			// Strip protocol suffix like "/tcp" and the value form "PORT:PORT".
			token = strings.SplitN(token, "/", 2)[0]
			p, err := strconv.Atoi(token)
			if err != nil || p <= 0 || p > 65535 {
				continue
			}
			if !seen[p] {
				seen[p] = true
				ports = append(ports, p)
			}
		}
	}
	return ports
}