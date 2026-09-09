# Nebula MVP 25-Gate Verification & Real-Infrastructure Specification

This document defines the 25 core gates required to graduate the Nebula container cloud platform to Minimum Viable Product (MVP), along with the Real-Infrastructure (RI-01 → RI-06) verification suite.

---

## 1. Gate Matrix: Verification Level Breakdown

Every gate is explicitly classified as either **Logic-Verified & Infra-Verified** (proven against real Docker daemons, real elapsed wall-clock waits, and real network sockets) or **Logic-Verified** (intentionally verified in-process because the invariant being tested resides strictly in CP memory, scheduling algorithms, database concurrency, or internal state machines).

| Gate | Category | Description | Verification Level | Implementation & Real Infra Backing |
|---|---|---|---|---|
| **G-01** | Reconciliation | Missing instance repaired | **Logic-Verified & Infra-Verified** | Logic in `TestDA03`; Infra in `TestRI01` (Docker replacement created on real daemon) |
| **G-02** | Reconciliation | Idempotent reconciliation | **Logic-Verified** | In-process state diffing (`TestDA04`, `diff_test.go`); correctly tests CP logic with zero mutating actions |
| **G-03** | Reconciliation | Orphan container policy | **Logic-Verified** | Tests label evaluation and classification rules (`orphan_policy_test.go`) |
| **G-04** | Lifecycle | Reconcile before scheduling | **Logic-Verified** | Tests CP startup sequence ordering and database reload prior to scheduler loop |
| **G-05** | Pipeline | Happy-path deployment | **Logic-Verified & Infra-Verified** | Logic in `TestE2E_HappyPath`; Infra in `TestRI05` (real `docker build`, run, and HTTP reachability) |
| **G-06** | Scheduling | Explicit priority order | **Logic-Verified** | Algorithmic filter → rank priority in scheduler; correctly isolated from external infrastructure |
| **G-07** | Scheduling | Draining worker exclusion | **Logic-Verified** | Schedulability predicate evaluation in scheduler (`drain_migration_test.go`) |
| **G-08** | Scheduling | Spreading default | **Logic-Verified** | Spreading scoring heuristic across worker capacity vectors (`scheduler_test.go`) |
| **G-09** | Scheduling | Utilization tie-break | **Logic-Verified** | Deterministic sorting on equal workload counts (`scheduler_test.go`) |
| **G-10** | Routing | Unhealthy endpoint eviction | **Logic-Verified & Infra-Verified** | Logic in `TestSD05`; Infra in `TestRI01` and `TestRI02` (real socket eviction) |
| **G-11** | Reconciliation | Replica count bound | **Logic-Verified** | Invariant check asserting running count $\le$ desired count during concurrent reconcile |
| **G-12** | Failure Recovery | Worker death recovery | **Logic-Verified & Infra-Verified** | Logic in `TestFS01`; Infra in `TestRI01` (real process kill, 2.3s real wall-clock wait, real Docker recreate) |
| **G-13** | Failure Recovery | Partition isolation | **Logic-Verified & Infra-Verified** | Logic in `TestFS05`; Infra in `TestRI02` (real TCP proxy severed, real socket error, unaffected worker) |
| **G-14** | Failure Recovery | Container failure isolation | **Logic-Verified & Infra-Verified** | Logic in `TestFS02`; Infra in `TestRI04` (genuine Docker pull failure, real inspect, continuous heartbeats) |
| **G-15** | Failure Recovery | CP outage survivability | **Logic-Verified & Infra-Verified** | Logic in `TestFS04`; Infra in `TestRI03` (real Nginx container, real HTTP requests during CP outage, zero downtime) |
| **G-16** | Crash Safety | Crash while QUEUED | **Logic-Verified** | CP recovery engine state transition persisted in DB (`crash_queued_test.go`) |
| **G-17** | Crash Safety | Crash while BUILDING | **Logic-Verified** | Corrupted build artifact purge and state reset in DB (`crash_building_test.go`) |
| **G-18** | Crash Safety | Crash while SCHEDULING | **Logic-Verified** | Re-queueing deployment record across process restart (`crash_scheduling_test.go`) |
| **G-19** | Crash Safety | Crash while STARTING | **Logic-Verified** | Partial instance cleanup and recovery engine fail-safe (`crash_starting_test.go`) |
| **G-20** | Crash Safety | Crash while RUNNING | **Logic-Verified & Infra-Verified** | Logic in `TestCRASH20`; Infra in `TestRI03` (real container continues running across CP restart) |
| **G-21** | Concurrency | Simultaneous deployments | **Logic-Verified** | Concurrency gate testing row-level locking and project isolation in CP (`concurrency_test.go`) |
| **G-22** | Concurrency | Request idempotency | **Logic-Verified & Infra-Verified** | API-level deduplication in CP; Docker-level deduplication in `TestWA08` |
| **G-23** | Reconciliation | Manual removal repair | **Logic-Verified & Infra-Verified** | Logic in `TestMAN_DEL01`; Infra in `TestRI01` and `TestWA08` |
| **G-24** | Storage | Durable desired state | **Logic-Verified** | PostgreSQL persistence verified across restart cycles (`cp_postgres_test.go`) |
| **G-25** | Security | Security minimum | **Logic-Verified & Infra-Verified** | Auth/HMAC in CP (`auth_test.go`); real build container secret isolation in `TestSE05` |

---

## 2. Real-Infrastructure Test Suite (RI-01 → RI-06)

File: [`tests/integration/real_infra_test.go`](file:///c:/Users/NOOR%20AL%20MUSABAH/Documents/almichot_Cloud/nebula/tests/integration/real_infra_test.go)

| Test ID | Backed Gates | Real Infrastructure Mechanism | Observable Reality Assertions | Measured Wall-Clock Duration | Status |
|---|---|---|---|---|---|
| **RI-01** | G-01, G-10, G-12 | Real Worker Agent process killed abruptly (`SIGKILL` equivalent); real Docker container (`redis:alpine`) running on host daemon; CP `TimeoutMonitor` with real background ticker. | Transition to `UNHEALTHY` occurred only after real wall-clock elapsed duration ($\ge 2.0\text{s}$); replacement container created on Worker 2; verified `running` via Docker daemon inspect. | **7.89s** | ✅ **PASS** |
| **RI-02** | G-13 | Real TCP forwarder proxy sitting between CP and Worker 1; connectivity severed by closing listener and forcibly terminating all active TCP sockets. | CP attempted gRPC call and received genuine TCP socket failure (`connectex: connection refused`); Worker 2 on same network deployed container without disruption; proxy restored, container verified intact without duplication. | **5.15s** | ✅ **PASS** |
| **RI-03** | G-15, G-20 | Real Worker Agent managing real `nginx:alpine` container with host port binding; CP process killed completely; real HTTP GET requests sent to host port. | Container served 5 consecutive HTTP requests with 200 OK directly from Nginx daemon during CP outage (0 downtime); on CP restart, reconcile discovered running container without restart or duplication. | **3.16s** | ✅ **PASS** |
| **RI-04** | G-14 | Attempted container start with non-existent binary (`/nonexistent-executable`) and pull failure against unreachable registry; real Docker inspect confirmed genuine failure (`State=created`, `ExitCode=127`, `Error=OCI runtime create failed... stat /nonexistent-executable: no such file or directory`). | Real Docker inspect confirmed genuine container start failure (`ExitCode: 127`); Worker Agent continued emitting real gRPC heartbeats every 200ms; CP registry confirmed health stayed `HEALTHY` with 0 missed beats. | **2.40s** | ✅ **PASS** |
| **RI-05** | G-05, PERF-03 | Real `docker build` of custom Dockerfile, real container execution via Worker Agent, real HTTP reachability check over host port. | Replaces in-memory microsecond deploy measurement with real infrastructure duration: Docker Build (3.75s) + Container Run (1.12s) + HTTP Response (170ms) = 5.04s total latency. | **7.02s** (Deploy: 5.04s) | ✅ **PASS** |
| **RI-06** | PERF-04 | 3 real Worker Agent gRPC servers on 3 separate TCP ports (`127.0.0.1:<port>`); 3 real gRPC clients dispatching 300 concurrent calls over real TCP sockets. | Measures real TCP socket throughput over actual network stack: **2,369 calls/sec** with **0 dropped/failed calls**. | **1.31s** | ✅ **PASS** |

---

## 3. Real-World Findings from Infrastructure Testing

Moving from in-process mocks to the real Docker daemon and real network sockets uncovered two critical real-world behaviors that mocks could never have caught:

1. **Docker Container Name Collisions Across Workers on Shared Daemons**:
   - *Discovery*: When Worker 1 died and the Reconciler scheduled a replacement on Worker 2, Worker 2 attempted to create the container with the same instance ID (`nebula-inst-...`). Because both workers shared the local host's Docker engine, the Docker daemon rejected creation with `Conflict. The container name is already in use by container...`.
   - *Resolution*: Updated container naming to incorporate the unique worker key (`nebula-<workerKey>-<instanceID>`), guaranteeing namespace isolation and preventing collisions during multi-node failover on shared or co-located hosts.
2. **Heartbeat Aging Under Real Container Creation Overhead**:
   - *Discovery*: Mocked container runs take microseconds, but real Docker container creation (`docker run`) takes 1–2 seconds. Without an active heartbeat sender running concurrently during container initialization, the worker's last recorded heartbeat aged significantly before the kill was injected, causing the timeout monitor to trip earlier than expected from the kill timestamp.
   - *Resolution*: Confirmed that the heartbeat sender must run in a dedicated, independent goroutine (as implemented in `internal/heartbeat/sender.go`) completely decoupled from container creation syscalls.
