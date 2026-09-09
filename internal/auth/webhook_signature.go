package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// VerifyWebhookSignature verifies an incoming webhook payload against an HMAC-SHA256 signature header.
// Supports both "sha256=<hex>" and raw hex formats, using constant-time comparison to prevent timing attacks.
func VerifyWebhookSignature(secret []byte, payload []byte, signatureHeader string) bool {
	if len(secret) == 0 || len(signatureHeader) == 0 {
		return false
	}

	sigHex := strings.TrimSpace(signatureHeader)
	if strings.HasPrefix(sigHex, "sha256=") {
		sigHex = strings.TrimPrefix(sigHex, "sha256=")
	}

	expectedMAC := ComputeHMACSHA256(secret, payload)
	providedMAC, err := hex.DecodeString(sigHex)
	if err != nil {
		return false
	}

	return hmac.Equal(expectedMAC, providedMAC)
}

// ComputeHMACSHA256 calculates raw HMAC-SHA256 bytes for a secret and payload.
func ComputeHMACSHA256(secret []byte, payload []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write(payload)
	return mac.Sum(nil)
}

// ComputeWebhookSignatureHeader generates the "sha256=<hex>" header value for a secret and payload.
func ComputeWebhookSignatureHeader(secret []byte, payload []byte) string {
	mac := ComputeHMACSHA256(secret, payload)
	return "sha256=" + hex.EncodeToString(mac)
}
