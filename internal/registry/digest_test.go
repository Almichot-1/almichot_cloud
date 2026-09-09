package registry

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"
)

// BLD-07: Successful build pushes image and records digest.
func TestBLD07_SuccessfulPushRecordsDigest(t *testing.T) {
	log := zerolog.Nop()
	reg := NewMemoryRegistry(log)
	ctx := context.Background()

	payload := []byte("VALID_OCI_IMAGE_DATA_V1")
	tag := "nebula/demo-app:v1"
	expectedDigest := ComputeDigest(payload)

	verifiedDigest, err := reg.Push(ctx, tag, payload, expectedDigest)
	if err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	if verifiedDigest != expectedDigest {
		t.Fatalf("BLD-07 failure: expected digest %s, got %s", expectedDigest, verifiedDigest)
	}

	// Verify registry has the image and digest
	if !reg.HasImage(ctx, tag) {
		t.Fatalf("BLD-07 failure: registry does not report image present")
	}

	storedDigest, err := reg.GetDigest(ctx, tag)
	if err != nil || storedDigest != expectedDigest {
		t.Fatalf("BLD-07 failure: stored digest mismatch: %s vs %s", storedDigest, expectedDigest)
	}

	t.Logf("BLD-07 Passed: Image pushed and verified digest recorded: %s", verifiedDigest)
}

// BLD-08: Digest recorded is verified against the pushed image.
// Mismatch is detected and rejected, not silently trusted.
func TestBLD08_DigestMismatchDetectedAndRejected(t *testing.T) {
	log := zerolog.Nop()
	reg := NewMemoryRegistry(log)
	ctx := context.Background()

	payload := []byte("REAL_PAYLOAD")
	fakeDigest := "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	tag := "nebula/tampered-app:v1"

	// Attempt push with mismatched digest
	_, err := reg.Push(ctx, tag, payload, fakeDigest)
	if err == nil {
		t.Fatalf("BLD-08 failure: push with mismatched digest was silently accepted!")
	}

	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("BLD-08 failure: expected ErrDigestMismatch, got %v", err)
	}

	// Verify image was NOT saved to registry
	if reg.HasImage(ctx, tag) {
		t.Fatalf("BLD-08 failure: rejected image was erroneously stored in registry")
	}

	t.Logf("BLD-08 Passed: Mismatched digest was detected and rejected with error: %v", err)
}
