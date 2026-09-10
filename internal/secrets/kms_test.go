package secrets

import (
	"bytes"
	"context"
	"testing"
)

func TestKMS_WrapUnwrapRoundTrip(t *testing.T) {
	ctx := context.Background()
	kms := NewMockKMSClient()

	keyID := kms.CurrentKeyID()
	plaintextDEK := []byte("32-byte-test-data-encrypt-key!!") // 32 bytes

	wrapped, err := kms.WrapKey(ctx, keyID, plaintextDEK)
	if err != nil {
		t.Fatalf("wrap key failed: %v", err)
	}

	if bytes.Equal(wrapped, plaintextDEK) {
		t.Fatalf("wrapped key should not equal plaintext DEK")
	}

	unwrapped, err := kms.UnwrapKey(ctx, keyID, wrapped)
	if err != nil {
		t.Fatalf("unwrap key failed: %v", err)
	}

	if !bytes.Equal(unwrapped, plaintextDEK) {
		t.Fatalf("expected unwrapped key to match original plaintext DEK")
	}
}

func TestKMS_RotateAndReWrap(t *testing.T) {
	ctx := context.Background()
	kms := NewMockKMSClient()

	oldKeyID := kms.CurrentKeyID()
	plaintextDEK := []byte("original-32-byte-dek-key-data!!")

	wrappedV1, err := kms.WrapKey(ctx, oldKeyID, plaintextDEK)
	if err != nil {
		t.Fatalf("wrap key failed: %v", err)
	}

	// Rotate KMS master key
	newKeyID, err := kms.RotateKey(ctx)
	if err != nil {
		t.Fatalf("rotate key failed: %v", err)
	}
	if newKeyID == oldKeyID {
		t.Fatalf("expected new key ID after rotation")
	}

	// Re-wrap DEK under new master key
	wrappedV2, err := kms.ReWrapKey(ctx, oldKeyID, newKeyID, wrappedV1)
	if err != nil {
		t.Fatalf("re-wrap key failed: %v", err)
	}

	// Verify unwrapping under new master key yields original DEK
	unwrappedV2, err := kms.UnwrapKey(ctx, newKeyID, wrappedV2)
	if err != nil {
		t.Fatalf("unwrap key with new master key failed: %v", err)
	}
	if !bytes.Equal(unwrappedV2, plaintextDEK) {
		t.Fatalf("re-wrapped DEK unwrap failed to yield original DEK")
	}
}

func TestKMS_EnvelopeKeyProviderWithSecretStore(t *testing.T) {
	ctx := context.Background()
	kms := NewMockKMSClient()

	provider, err := NewKMSEnvelopeKeyProvider(kms)
	if err != nil {
		t.Fatalf("create KMS key provider: %v", err)
	}

	store := NewMemorySecretStore(provider)

	// Set secret
	projectID := "proj-kms-test"
	secretName := "API_KEY"
	plaintext := "sk_live_99887766554433221100"

	sec, err := store.SetSecret(ctx, projectID, secretName, plaintext)
	if err != nil {
		t.Fatalf("set secret: %v", err)
	}

	// Verify ciphertext doesn't leak plaintext
	if bytes.Contains(sec.Ciphertext, []byte(plaintext)) {
		t.Fatalf("raw ciphertext contains plaintext!")
	}

	// Retrieve secret
	_, decrypted, err := store.GetSecret(ctx, projectID, secretName)
	if err != nil {
		t.Fatalf("get secret: %v", err)
	}
	if decrypted != plaintext {
		t.Fatalf("expected %q, got %q", plaintext, decrypted)
	}

	// Rotate KMS key and re-wrap DEK
	oldKeyID := kms.CurrentKeyID()
	newKeyID, _ := kms.RotateKey(ctx)
	if err := provider.ReWrapKeys(ctx, oldKeyID, newKeyID); err != nil {
		t.Fatalf("re-wrap keys failed: %v", err)
	}

	// Secret must still decrypt cleanly after re-wrapping
	_, decryptedAfter, err := store.GetSecret(ctx, projectID, secretName)
	if err != nil {
		t.Fatalf("get secret after re-wrap failed: %v", err)
	}
	if decryptedAfter != plaintext {
		t.Fatalf("expected %q after re-wrap, got %q", plaintext, decryptedAfter)
	}
}

func TestKMS_UnavailableFailureMode(t *testing.T) {
	ctx := context.Background()
	kms := NewMockKMSClient()
	kms.SetAvailable(false) // simulate outage

	_, err := kms.WrapKey(ctx, "key-1", []byte("dek"))
	if err != ErrKMSUnavailable {
		t.Fatalf("expected ErrKMSUnavailable, got: %v", err)
	}
}
