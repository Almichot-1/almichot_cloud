# Nebula Disaster Recovery Runbook: Total Cluster Loss & Restoration (§30)

This runbook documents the standard operating procedure for recovering the Nebula Control Plane and worker fleets following catastrophic failure, total cluster loss, or corrupted primary database storage (§19.1, §20.3, §30).

---

## 1. Architecture & Recovery Principles (§19.1, §30)

- **Single Source of Truth**: PostgreSQL serves as the sole durable store of desired state.
- **Worker Autonomy**: Workers run container instances independently using local OCI runtimes. During Control Plane or database outages, running workloads continue uninterrupted.
- **Continuous WAL Archiving**: Every state mutation (project creation, deployment transition, scaling event) emits an incremental WAL record archived to persistent storage.
- **Point-in-Time Recovery (PITR)**: Restoring a periodic Base Backup combined with replaying continuous WAL segments guarantees zero data loss up to the moment of failure.
- **Convergence via §20.3 Sequence**: When a restored Control Plane boots against recovered storage, its reconciler queries live Worker self-reported state, compares it with desired state, and converges without ghost or duplicate containers.

---

## 2. Total Cluster-Loss Disaster Scenarios

| Scenario | Impact | Recovery Path |
|---|---|---|
| **Scenario A: Database Loss, Workers Alive** | Control Plane cannot read or mutate state; workloads running. | Restore PostgreSQL from Base Backup + replayed WAL; run §20.3 reconciliation pass. |
| **Scenario B: Control Plane + DB Dead, Workers Alive** | Zero API/scheduling availability; live workloads running. | Provision fresh CP + restored DB; CP connects to existing Worker fleet via gRPC mTLS; §20.3 adopts live containers. |
| **Scenario C: Total Cluster Wipe (CP + DB + Workers Dead)** | All nodes destroyed. | Provision fresh infrastructure; restore DB from latest Base Backup + WAL; provision new Worker fleet; re-schedule all desired replicas. |

---

## 3. Step-by-Step Restoration Procedure (§30)

### Phase 1: Database Restoration (PITR)
1. **Identify Target Point in Time**:
   Identify the latest available Base Backup ($B_0$) and timestamp $T_{\text{target}}$:
   ```bash
   nebula-admin backup list
   ```
2. **Restore Base Snapshot**:
   Restore table schemas and records from base backup archive:
   ```bash
   nebula-admin backup restore --backup-id=<backup-id>
   ```
3. **Replay Continuous WAL Stream**:
   Replay all archived WAL segments up to $T_{\text{target}}$:
   ```bash
   nebula-admin backup replay-wal --until="2026-09-10T18:00:00Z"
   ```
4. **Validate Database Integrity**:
   Verify relational constraints and table counts across `projects`, `deployments`, `instances`, `releases`, and `events`.

---

### Phase 2: Control Plane Initialization
1. **Launch Standby / Primary Control Plane**:
   Boot Control Plane with connection string pointing to restored PostgreSQL instance.
2. **Advisory Lock Leader Election**:
   The Control Plane acquires leader lock (`pg_try_advisory_lock`) and initiates promotion lifecycle hooks.
3. **Internal CA & Secrets Re-Wrapping**:
   Verify the master encryption key unwrap via KMS/Vault and reload CA certificates.

---

### Phase 3: Worker Reconnection & State Convergence (§20.3)
1. **Worker Registry Discovery**:
   Workers re-establish mTLS gRPC connections to the new Control Plane address without restarting running workloads.
2. **Execute §20.3 Reconciliation Pass**:
   The Control Plane executes `ReconcileOnce`:
   - Queries desired state from restored database (`depRepo.List`, `instRepo.ListAll`).
   - Collects observed container state from all registered workers via `ListContainers`.
   - **Matching Instances**: Workloads matching desired state are verified healthy and registered with the Load Balancer router.
   - **Managed Orphans**: Workloads created after the backup point (if WAL was truncated) are identified, safely adopted or cleanly drained per §8.3 orphan policy.
   - **Missing Replicas**: If workers were lost, the Scheduler places missing replicas across healthy nodes adhering to spreading policies (G-08).
3. **Traffic Ingress Re-Routing**:
   The Load Balancer updates active target pools and verifies zero cross-tenant leakage.

---

## 4. Disaster Recovery Validation Checklist

- [x] Database restored to desired point in time with zero corruptions.
- [x] All active projects and deployments recovered from backup.
- [x] Worker nodes successfully reconnected via mTLS.
- [x] Running workloads adopted with zero worker disruption (no container restart required).
- [x] §20.3 reconciliation converged: Desired State == Observed State.
- [x] Load Balancer routes traffic to healthy backends.
