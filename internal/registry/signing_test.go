package registry

import (
	"context"
	"testing"
)

func TestImageSigning_ValidSignature(t *testing.T) {
	priv, pub, err := GenerateSigningKeyPair()
	if err != nil {
		t.Fatalf("generate key pair: %v", err)
	}

	signer := NewEd25519ImageSigner(priv)
	verifier := NewEd25519ImageVerifier(pub)

	ctx := context.Background()
	digest := "sha256:41a158d5f5c6072ec01ccb4e69eaa4b5c3644db26bcf6a8365997d30850b801d"

	sig, err := signer.Sign(ctx, digest)
	if err != nil {
		t.Fatalf("sign digest: %v", err)
	}
	if sig == "" {
		t.Fatalf("expected non-empty signature")
	}

	if err := verifier.Verify(ctx, "app:v1", digest, sig); err != nil {
		t.Fatalf("valid signature failed verification: %v", err)
	}
}

func TestImageSigning_UnsignedRejected(t *testing.T) {
	_, pub, _ := GenerateSigningKeyPair()
	verifier := NewEd25519ImageVerifier(pub)

	ctx := context.Background()
	digest := "sha256:41a158d5f5c6072ec01ccb4e69eaa4b5c3644db26bcf6a8365997d30850b801d"

	err := verifier.Verify(ctx, "app:v1", digest, "")
	if err == nil {
		t.Fatalf("expected unsigned image to be rejected (G-28)")
	}
}

func TestImageSigning_TamperedDigestRejected(t *testing.T) {
	priv, pub, _ := GenerateSigningKeyPair()
	signer := NewEd25519ImageSigner(priv)
	verifier := NewEd25519ImageVerifier(pub)

	ctx := context.Background()
	digest := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	tamperedDigest := "sha256:2222222222222222222222222222222222222222222222222222222222222222"

	sig, _ := signer.Sign(ctx, digest)

	err := verifier.Verify(ctx, "app:v1", tamperedDigest, sig)
	if err == nil {
		t.Fatalf("expected tampered digest to fail verification (G-28)")
	}
}

func TestImageSigning_UntrustedKeyRejected(t *testing.T) {
	privA, _, _ := GenerateSigningKeyPair()
	_, pubB, _ := GenerateSigningKeyPair()

	signerA := NewEd25519ImageSigner(privA)
	verifierB := NewEd25519ImageVerifier(pubB)

	ctx := context.Background()
	digest := "sha256:41a158d5f5c6072ec01ccb4e69eaa4b5c3644db26bcf6a8365997d30850b801d"

	sig, _ := signerA.Sign(ctx, digest)

	err := verifierB.Verify(ctx, "app:v1", digest, sig)
	if err == nil {
		t.Fatalf("expected signature from untrusted key to be rejected (G-28)")
	}
}
