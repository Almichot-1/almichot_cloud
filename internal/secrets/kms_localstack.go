package secrets

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// LocalStackKMSClient implements KMSClient against a real AWS KMS-compatible endpoint
// (LocalStack or real AWS). All operations make genuine HTTP round trips over the network —
// no in-process interface swap. Used exclusively by G-36 and G-37 real-infra gate tests.
//
// Wire protocol: AWS KMS JSON over HTTPS (or HTTP for LocalStack dev).
// Reference: https://docs.aws.amazon.com/kms/latest/APIReference/
type LocalStackKMSClient struct {
	mu           sync.RWMutex
	endpoint     string // e.g. "http://localhost:4566"
	region       string // e.g. "us-east-1"
	accessKey    string // LocalStack accepts any non-empty value
	secretKey    string
	currentKeyID string // ARN or alias of the currently active CMK
	httpClient   *http.Client

	// requestLog records every outbound HTTP request/response for test assertions.
	requestLog   []LocalStackRequest
	requestLogMu sync.Mutex
}

// LocalStackRequest is a record of one real HTTP call to the KMS endpoint.
type LocalStackRequest struct {
	Action     string
	RequestAt  time.Time
	StatusCode int
}

// NewLocalStackKMSClient creates a real KMS client pointed at a LocalStack endpoint.
// keyID must be an existing CMK key ID or ARN created in that LocalStack instance.
func NewLocalStackKMSClient(endpoint, region, accessKey, secretKey, keyID string) *LocalStackKMSClient {
	return &LocalStackKMSClient{
		endpoint:     strings.TrimRight(endpoint, "/"),
		region:       region,
		accessKey:    accessKey,
		secretKey:    secretKey,
		currentKeyID: keyID,
		httpClient:   &http.Client{Timeout: 10 * time.Second},
	}
}

// CurrentKeyID returns the active CMK key ID.
func (c *LocalStackKMSClient) CurrentKeyID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.currentKeyID
}

// SetAvailable is a no-op on the real client (availability is determined by the real service).
func (c *LocalStackKMSClient) SetAvailable(_ bool) {}

// WrapKey encrypts a plaintext DEK using the specified CMK via a real KMS Encrypt call.
func (c *LocalStackKMSClient) WrapKey(ctx context.Context, keyID string, plaintextDEK []byte) ([]byte, error) {
	body := map[string]string{
		"KeyId":     keyID,
		"Plaintext": base64.StdEncoding.EncodeToString(plaintextDEK),
	}
	resp, err := c.doKMSRequest(ctx, "Encrypt", body)
	if err != nil {
		return nil, fmt.Errorf("KMS Encrypt: %w", err)
	}
	ciphertextB64, ok := resp["CiphertextBlob"].(string)
	if !ok {
		return nil, fmt.Errorf("KMS Encrypt: unexpected response shape: %v", resp)
	}
	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextB64)
	if err != nil {
		return nil, fmt.Errorf("KMS Encrypt: decode ciphertext: %w", err)
	}
	return ciphertext, nil
}

// UnwrapKey decrypts a wrapped DEK using the specified CMK via a real KMS Decrypt call.
func (c *LocalStackKMSClient) UnwrapKey(ctx context.Context, keyID string, wrappedDEK []byte) ([]byte, error) {
	body := map[string]string{
		"KeyId":          keyID,
		"CiphertextBlob": base64.StdEncoding.EncodeToString(wrappedDEK),
	}
	resp, err := c.doKMSRequest(ctx, "Decrypt", body)
	if err != nil {
		return nil, fmt.Errorf("KMS Decrypt: %w", err)
	}
	plaintextB64, ok := resp["Plaintext"].(string)
	if !ok {
		return nil, fmt.Errorf("KMS Decrypt: unexpected response shape: %v", resp)
	}
	plaintext, err := base64.StdEncoding.DecodeString(plaintextB64)
	if err != nil {
		return nil, fmt.Errorf("KMS Decrypt: decode plaintext: %w", err)
	}
	return plaintext, nil
}

// RotateKey creates a new CMK in LocalStack and marks it as the current key.
// This causes a real CreateKey call over the network.
func (c *LocalStackKMSClient) RotateKey(ctx context.Context) (string, error) {
	body := map[string]any{
		"Description": "nebula-gate-test-rotation-key",
		"KeyUsage":    "ENCRYPT_DECRYPT",
	}
	resp, err := c.doKMSRequest(ctx, "CreateKey", body)
	if err != nil {
		return "", fmt.Errorf("KMS CreateKey: %w", err)
	}
	meta, ok := resp["KeyMetadata"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("KMS CreateKey: unexpected response: %v", resp)
	}
	newKeyID, ok := meta["KeyId"].(string)
	if !ok {
		return "", fmt.Errorf("KMS CreateKey: KeyId not a string: %v", meta)
	}
	c.mu.Lock()
	c.currentKeyID = newKeyID
	c.mu.Unlock()
	return newKeyID, nil
}

// ReWrapKey decrypts with oldKeyID and re-encrypts with newKeyID via two real KMS calls.
func (c *LocalStackKMSClient) ReWrapKey(ctx context.Context, oldKeyID, newKeyID string, wrappedDEK []byte) ([]byte, error) {
	// Decrypt with old key
	plaintext, err := c.UnwrapKey(ctx, oldKeyID, wrappedDEK)
	if err != nil {
		return nil, fmt.Errorf("ReWrapKey decrypt: %w", err)
	}
	// Re-encrypt with new key
	newWrapped, err := c.WrapKey(ctx, newKeyID, plaintext)
	if err != nil {
		return nil, fmt.Errorf("ReWrapKey re-encrypt: %w", err)
	}
	return newWrapped, nil
}

// RequestLog returns all recorded HTTP requests. Used by gate tests to assert
// that real network round trips occurred (zero entries = mock snuck back in).
func (c *LocalStackKMSClient) RequestLog() []LocalStackRequest {
	c.requestLogMu.Lock()
	defer c.requestLogMu.Unlock()
	out := make([]LocalStackRequest, len(c.requestLog))
	copy(out, c.requestLog)
	return out
}

// doKMSRequest sends one real HTTP POST to the LocalStack KMS endpoint using the
// AWS JSON 1.1 wire protocol. Returns the decoded JSON response body.
func (c *LocalStackKMSClient) doKMSRequest(ctx context.Context, action string, body any) (map[string]any, error) {
	reqBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	url := fmt.Sprintf("%s/", c.endpoint)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// AWS KMS uses the X-Amz-Target header to route actions.
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "TrentService."+action)
	// LocalStack accepts any non-empty credentials.
	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/20240101/%s/kms/aws4_request, SignedHeaders=host, Signature=fakesig",
		c.accessKey, c.region,
	))

	start := time.Now()
	httpResp, err := c.httpClient.Do(req)

	// Record the request regardless of outcome.
	entry := LocalStackRequest{
		Action:    action,
		RequestAt: start,
	}
	if httpResp != nil {
		entry.StatusCode = httpResp.StatusCode
	}
	c.requestLogMu.Lock()
	c.requestLog = append(c.requestLog, entry)
	c.requestLogMu.Unlock()

	if err != nil {
		return nil, fmt.Errorf("HTTP %s: %w", action, err)
	}
	defer httpResp.Body.Close()

	respBytes, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if httpResp.StatusCode >= 400 {
		return nil, fmt.Errorf("KMS %s returned HTTP %d: %s", action, httpResp.StatusCode, string(respBytes))
	}

	var result map[string]any
	if err := json.Unmarshal(respBytes, &result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return result, nil
}

// compile-time assertion that LocalStackKMSClient satisfies KMSClient.
var _ KMSClient = (*LocalStackKMSClient)(nil)
