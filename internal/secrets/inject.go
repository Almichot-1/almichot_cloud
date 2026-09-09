package secrets

import (
	"context"
	"fmt"
)

// InjectSecrets merges a deployment's decrypted secrets into its environment variables.
// Secrets are injected strictly at RunContainer time (SEC-02), never during image build.
func InjectSecrets(ctx context.Context, store SecretStore, deploymentID string, baseEnv map[string]string) (map[string]string, error) {
	merged := make(map[string]string)
	for k, v := range baseEnv {
		merged[k] = v
	}

	if store == nil || deploymentID == "" {
		return merged, nil
	}

	decryptedSecrets, err := store.GetSecretsForDeployment(ctx, deploymentID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch secrets for deployment %s: %w", deploymentID, err)
	}

	for k, v := range decryptedSecrets {
		merged[k] = v
	}

	return merged, nil
}
