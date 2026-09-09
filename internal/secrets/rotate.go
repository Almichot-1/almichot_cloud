package secrets

import (
	"context"
	"fmt"
)

// RotateSecret updates a secret's value by creating a new version.
// In-flight containers continue running with their existing version (SEC-04),
// while subsequent deployments will capture the new version.
func RotateSecret(ctx context.Context, store SecretStore, projectID, name, newPlaintext string) (*Secret, error) {
	if store == nil {
		return nil, fmt.Errorf("secret store is nil")
	}

	sec, err := store.SetSecret(ctx, projectID, name, newPlaintext)
	if err != nil {
		return nil, fmt.Errorf("failed to rotate secret %s: %w", name, err)
	}

	return sec, nil
}
