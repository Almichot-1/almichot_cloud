package reconcile

import (
	"context"
	"fmt"

	"github.com/nebula/nebula/internal/deployments"
	"github.com/nebula/nebula/proto"
	"github.com/rs/zerolog"
)

// OrphanType identifies whether an orphaned container was created by Nebula or is an external foreign container.
type OrphanType string

const (
	// OrphanTypeManaged indicates the container was created by Nebula (has nebula labels/instance ID)
	// but is no longer part of active desired state (e.g. deleted deployment, scaled-down replica).
	OrphanTypeManaged OrphanType = "MANAGED"

	// OrphanTypeUnknown indicates the container has no Nebula metadata and was started externally.
	// Per §9.3 & G-03, Nebula NEVER stops or kills unknown containers; it flags them only.
	OrphanTypeUnknown OrphanType = "UNKNOWN"
)

// ClassifyOrphan determines whether an observed container is a managed orphan or an unknown third-party container (CP-06).
func ClassifyOrphan(container ObservedContainer) OrphanType {
	if container.InstanceKey != "" {
		return OrphanTypeManaged
	}
	if container.DeploymentID != "" {
		return OrphanTypeManaged
	}
	if container.Labels != nil {
		if _, ok := container.Labels["nebula.instance_id"]; ok {
			return OrphanTypeManaged
		}
		if _, ok := container.Labels["nebula.deployment_id"]; ok {
			return OrphanTypeManaged
		}
	}
	return OrphanTypeUnknown
}

// OrphanDecision summarizes the action taken for an orphaned container.
type OrphanDecision struct {
	Container   ObservedContainer `json:"container"`
	Type        OrphanType        `json:"type"`
	ActionTaken string            `json:"action_taken"` // "STOPPED", "FLAGGED_ONLY", "ERROR"
	Error       string            `json:"error,omitempty"`
}

// EnforceOrphanPolicy handles an orphaned container according to policy (G-03, CP-04):
// - Managed orphan: Stopped via WorkerClient.
// - Unknown container: NEVER touched; flagged only for audit/observability.
func EnforceOrphanPolicy(
	ctx context.Context,
	container ObservedContainer,
	client deployments.WorkerClient,
	log zerolog.Logger,
) OrphanDecision {
	orphanType := ClassifyOrphan(container)

	switch orphanType {
	case OrphanTypeManaged:
		// Target instance identifier for worker call
		targetID := container.InstanceKey
		if targetID == "" {
			targetID = container.ContainerID
		}

		log.Warn().
			Str("worker_id", container.WorkerID).
			Str("container_id", container.ContainerID).
			Str("instance_key", container.InstanceKey).
			Str("policy", "managed_orphan_stop").
			Msg("stopping managed orphan container per policy G-03")

		if client != nil {
			resp, err := client.StopContainer(ctx, &proto.StopContainerRequest{
				InstanceId:     targetID,
				TimeoutSeconds: 10,
			})
			if err != nil || (resp != nil && !resp.Success) {
				errMsg := "failed to stop container"
				if err != nil {
					errMsg = err.Error()
				} else if resp != nil && resp.Error != "" {
					errMsg = resp.Error
				}
				log.Error().
					Str("container_id", container.ContainerID).
					Str("err", errMsg).
					Msg("error stopping managed orphan")
				return OrphanDecision{
					Container:   container,
					Type:        orphanType,
					ActionTaken: "ERROR",
					Error:       errMsg,
				}
			}
		}

		return OrphanDecision{
			Container:   container,
			Type:        orphanType,
			ActionTaken: "STOPPED",
		}

	case OrphanTypeUnknown:
		// G-03 invariant: Never touch unknown containers
		log.Info().
			Str("worker_id", container.WorkerID).
			Str("container_id", container.ContainerID).
			Str("image", container.Image).
			Str("policy", "unknown_container_flag_only").
			Msg("flagged unknown unmanaged container: untouched per policy G-03")

		return OrphanDecision{
			Container:   container,
			Type:        orphanType,
			ActionTaken: "FLAGGED_ONLY",
		}

	default:
		return OrphanDecision{
			Container:   container,
			Type:        orphanType,
			ActionTaken: "FLAGGED_ONLY",
			Error:       fmt.Sprintf("unknown orphan classification: %s", orphanType),
		}
	}
}
