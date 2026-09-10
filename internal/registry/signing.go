package registry

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
)

var (
	ErrUnsignedImage      = errors.New("image is unsigned; cluster policy requires cryptographic signature (G-28)")
	ErrSignatureMismatch  = errors.New("image signature verification failed: signature does not match image digest (G-28)")
	ErrInvalidSignature   = errors.New("malformed or invalid signature encoding")
	ErrMissingImageDigest = errors.New("cannot verify signature on image without cryptographic digest")
)

// ImageSigner signs immutable image digests using cluster private keys (§21.3).
type ImageSigner interface {
	Sign(ctx context.Context, digest string) (string, error)
	PublicKey() ed25519.PublicKey
}

// ImageVerifier verifies that an image digest was signed by a trusted cluster public key (G-28).
type ImageVerifier interface {
	Verify(ctx context.Context, image, digest, signature string) error
}

// Ed25519ImageSigner signs image digests using an Ed25519 private key.
type Ed25519ImageSigner struct {
	privateKey ed25519.PrivateKey
	publicKey  ed25519.PublicKey
}

// NewEd25519ImageSigner creates a signer with the given private key.
func NewEd25519ImageSigner(priv ed25519.PrivateKey) *Ed25519ImageSigner {
	pub := priv.Public().(ed25519.PublicKey)
	return &Ed25519ImageSigner{
		privateKey: priv,
		publicKey:  pub,
	}
}

// GenerateSigningKeyPair generates a new random Ed25519 key pair for image signing.
func GenerateSigningKeyPair() (ed25519.PrivateKey, ed25519.PublicKey, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate ed25519 key: %w", err)
	}
	return priv, pub, nil
}

// Sign signs the image digest and returns a standard base64-encoded signature.
func (s *Ed25519ImageSigner) Sign(ctx context.Context, digest string) (string, error) {
	if digest == "" {
		return "", ErrMissingImageDigest
	}
	message := []byte("nebula-image-signature:" + digest)
	sig := ed25519.Sign(s.privateKey, message)
	return base64.StdEncoding.EncodeToString(sig), nil
}

// PublicKey returns the public verification key.
func (s *Ed25519ImageSigner) PublicKey() ed25519.PublicKey {
	return s.publicKey
}

// Ed25519ImageVerifier verifies image signatures against trusted cluster public keys.
type Ed25519ImageVerifier struct {
	trustedKeys []ed25519.PublicKey
	enforce     bool
}

// NewEd25519ImageVerifier creates a verifier with trusted public keys.
func NewEd25519ImageVerifier(trustedKeys ...ed25519.PublicKey) *Ed25519ImageVerifier {
	return &Ed25519ImageVerifier{
		trustedKeys: trustedKeys,
		enforce:     true,
	}
}

// SetEnforce toggles mandatory signature verification.
func (v *Ed25519ImageVerifier) SetEnforce(enforce bool) {
	v.enforce = enforce
}

// Verify verifies that the provided signature matches the image digest and was produced
// by at least one of the trusted public keys (Gate G-28).
func (v *Ed25519ImageVerifier) Verify(ctx context.Context, image, digest, signature string) error {
	if !v.enforce {
		return nil
	}

	if signature == "" {
		return fmt.Errorf("%w for image %s", ErrUnsignedImage, image)
	}

	if digest == "" {
		return ErrMissingImageDigest
	}

	sigBytes, err := base64.StdEncoding.DecodeString(signature)
	if err != nil || len(sigBytes) != ed25519.SignatureSize {
		return fmt.Errorf("%w: %v", ErrInvalidSignature, err)
	}

	message := []byte("nebula-image-signature:" + digest)
	for _, key := range v.trustedKeys {
		if ed25519.Verify(key, message, sigBytes) {
			return nil // Valid signature from a trusted key
		}
	}

	return fmt.Errorf("%w for image %s (digest: %s)", ErrSignatureMismatch, image, digest)
}
