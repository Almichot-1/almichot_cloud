package registry

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrDigestMismatch = errors.New("image digest verification failed: digest mismatch")
	ErrInvalidDigest  = errors.New("invalid digest format: expected sha256:<hex>")
)

// ComputeDigest calculates the sha256 digest of arbitrary binary data (e.g. image manifest/tar).
func ComputeDigest(data []byte) string {
	h := sha256.Sum256(data)
	return fmt.Sprintf("sha256:%s", hex.EncodeToString(h[:]))
}

// VerifyDigest validates that an actual image digest matches an expected digest (BLD-08).
// Mismatches are rejected, not silently trusted.
func VerifyDigest(expectedDigest, actualDigest string) error {
	expected := strings.TrimSpace(strings.ToLower(expectedDigest))
	actual := strings.TrimSpace(strings.ToLower(actualDigest))

	if expected == "" {
		return fmt.Errorf("%w: expected digest cannot be empty", ErrInvalidDigest)
	}
	if actual == "" {
		return fmt.Errorf("%w: actual digest cannot be empty", ErrInvalidDigest)
	}

	if expected != actual {
		return fmt.Errorf("%w: expected '%s', got '%s'", ErrDigestMismatch, expected, actual)
	}

	return nil
}
