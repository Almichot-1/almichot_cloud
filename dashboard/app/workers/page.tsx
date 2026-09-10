"use client";

import React, { useState, useEffect } from "react";
import { TopNav } from "@/components/TopNav";
import { StatusBadge } from "@/components/StatusBadge";
import { StatusDot } from "@/components/StatusDot";
import { usePolling } from "@/lib/usePolling";
import { fetchWorkers, fetchWorkerInstances } from "@/lib/api";
import { Worker, WorkerInstance } from "@/lib/types";
import {
  Server,
  Cpu,
  Clock,
  Radio,
  Box,
  Layers,
  ChevronRight,
  RefreshCw,
  X,
  AlertCircle,
} from "lucide-react";

export default function WorkersPage() {
  const { data: workers, loading, error, lastUpdated, changedKeys, refresh } =
    usePolling<Worker[]>({
      fetcher: fetchWorkers,
      intervalMs: 2500,
      getId: (w) => w.id || w.worker_key,
      getStateSig: (w) => `${w.health}-${w.state}-${w.active_workloads}-${w.schedulable}`,
    });

  const [selectedWorker, setSelectedWorker] = useState<Worker | null>(null);
  const [instances, setInstances] = useState<WorkerInstance[]>([]);
  const [loadingInstances, setLoadingInstances] = useState(false);
  const [instancesError, setInstancesError] = useState<string | null>(null);

  // When selectedWorker changes, fetch instances on that worker
  useEffect(() => {
    if (!selectedWorker) {
      setInstances([]);
      return;
    }

    let active = true;
    const loadInstances = async () => {
      setLoadingInstances(true);
      setInstancesError(null);
      try {
        const id = selectedWorker.id || selectedWorker.worker_key;
        const res = await fetchWorkerInstances(id);
        if (active) {
          setInstances(res);
        }
      } catch (err: any) {
        if (active) {
          setInstancesError(err?.message || "Failed to fetch worker instances");
        }
      } finally {
        if (active) {
          setLoadingInstances(false);
        }
      }
    };

    loadInstances();
    const timer = setInterval(loadInstances, 3000);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [selectedWorker?.id, selectedWorker?.worker_key]);

  // Keep selectedWorker in sync with live polling updates
  useEffect(() => {
    if (selectedWorker && workers) {
      const match = workers.find(
        (w) =>
          (w.id && w.id === selectedWorker.id) ||
          w.worker_key === selectedWorker.worker_key
      );
      if (match) {
        setSelectedWorker(match);
      }
    }
  }, [workers]);

  const list = workers || [];
  const degraded = list.some(
    (w) =>
      w.health === "UNHEALTHY" ||
      w.health === "UNREACHABLE" ||
      w.health === "SUSPECTED"
  );

  return (
    <>
      <TopNav
        title="Workers Fleet"
        subtitle="Compute nodes & capacity telemetry"
        lastUpdated={lastUpdated}
        onRefresh={refresh}
        isDegraded={degraded}
      />

      <div style={{ padding: "20px 24px", display: "flex", flexDirection: "column", gap: 16 }}>
        {error && (
          <div
            style={{
              padding: "10px 14px",
              backgroundColor: "var(--red-subtle)",
              border: "1px solid rgba(179, 38, 30, 0.3)",
              color: "var(--red-600)",
              borderRadius: 4,
              display: "flex",
              alignItems: "center",
              gap: 8,
              fontSize: 12,
            }}
          >
            <AlertCircle size={14} />
            <span>{error}</span>
          </div>
        )}

        {/* Master-Detail Layout */}
        <div
          style={{
            display: "grid",
            gridTemplateColumns: selectedWorker ? "1fr 440px" : "1fr",
            gap: 16,
            alignItems: "start",
          }}
        >
          {/* Main Workers Table */}
          <div className="ops-panel">
            <div className="ops-panel-header">
              <div className="ops-panel-title">
                <Server size={14} />
                <span>Registered Workers ({list.length})</span>
              </div>
              <span style={{ fontSize: 11, color: "var(--slate-500)" }}>
                Click a worker row to inspect assigned container workloads
              </span>
            </div>

            <div style={{ overflowX: "auto" }}>
              <table className="ops-table">
                <thead>
                  <tr>
                    <th>Worker Name</th>
                    <th>Address / Node</th>
                    <th>Health State</th>
                    <th>Schedulability</th>
                    <th>Workload / Capacity</th>
                    <th>Last Heartbeat</th>
                    <th style={{ width: 30 }}></th>
                  </tr>
                </thead>
                <tbody>
                  {list.length === 0 ? (
                    <tr>
                      <td
                        colSpan={7}
                        style={{
                          textAlign: "center",
                          color: "var(--slate-500)",
                          padding: 30,
                        }}
                      >
                        {loading ? "Discovering worker nodes..." : "No workers registered"}
                      </td>
                    </tr>
                  ) : (
                    list.map((w) => {
                      const id = w.id || w.worker_key;
                      const isSelected =
                        selectedWorker &&
                        ((selectedWorker.id && selectedWorker.id === w.id) ||
                          selectedWorker.worker_key === w.worker_key);
                      const hasChanged = changedKeys.has(id);

                      return (
                        <tr
                          key={id}
                          className={`clickable ${isSelected ? "selected" : ""} ${
                            hasChanged ? "animate-state-shift" : ""
                          }`}
                          onClick={() => setSelectedWorker(w)}
                        >
                          <td>
                            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                              <StatusDot status={w.health} />
                              <div>
                                <div style={{ fontWeight: 600, color: "var(--slate)" }}>
                                  {w.worker_key}
                                </div>
                                <div
                                  className="font-mono"
                                  style={{ fontSize: 10, color: "var(--slate-500)" }}
                                >
                                  {w.id ? w.id.substring(0, 13) : "—"}
                                </div>
                              </div>
                            </div>
                          </td>

                          <td>
                            <div className="font-mono" style={{ fontSize: 12 }}>
                              {w.ip_address || "127.0.0.1"}
                              {w.grpc_port ? `:${w.grpc_port}` : ""}
                            </div>
                            <div
                              style={{ fontSize: 10.5, color: "var(--slate-500)", marginTop: 2 }}
                            >
                              {w.hostname || "local"}
                            </div>
                          </td>

                          <td>
                            <StatusBadge status={w.health} size="sm" />
                          </td>

                          <td>
                            <span
                              className="badge"
                              style={{
                                backgroundColor: w.schedulable
                                  ? "var(--trust-blue-subtle)"
                                  : "var(--amber-subtle)",
                                color: w.schedulable
                                  ? "var(--trust-blue)"
                                  : "var(--amber-500)",
                                fontSize: 10,
                              }}
                            >
                              {w.schedulable ? "SCHEDULABLE" : "DRAINING"}
                            </span>
                          </td>

                          <td>
                            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                              <span className="font-mono" style={{ fontWeight: 600 }}>
                                {w.active_workloads}
                              </span>
                              <span style={{ color: "var(--slate-500)", fontSize: 11 }}>
                                / {w.capacity}
                              </span>
                            </div>
                            <div
                              style={{
                                marginTop: 4,
                                height: 3,
                                width: 80,
                                backgroundColor: "var(--slate-200)",
                                borderRadius: 2,
                                overflow: "hidden",
                              }}
                            >
                              <div
                                style={{
                                  height: "100%",
                                  width: `${Math.min(
                                    w.capacity > 0
                                      ? (w.active_workloads / w.capacity) * 100
                                      : 0,
                                    100
                                  )}%`,
                                  backgroundColor: "var(--trust-blue)",
                                }}
                              />
                            </div>
                          </td>

                          <td>
                            <div
                              className="font-mono timestamp"
                              style={{ fontSize: 11, color: "var(--slate)" }}
                            >
                              {w.last_beat_at
                                ? w.last_beat_at.substring(11, 19) + " UTC"
                                : "No heartbeat"}
                            </div>
                          </td>

                          <td style={{ textAlign: "right" }}>
                            <ChevronRight
                              size={14}
                              color={isSelected ? "var(--trust-blue)" : "var(--slate-300)"}
                            />
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          </div>

          {/* Inspector Panel: Running Instances on Worker */}
          {selectedWorker && (
            <div className="ops-panel" style={{ position: "sticky", top: 72 }}>
              <div className="ops-panel-header">
                <div className="ops-panel-title">
                  <Box size={14} color="var(--trust-blue)" />
                  <span>Workload Inspector</span>
                </div>
                <button
                  onClick={() => setSelectedWorker(null)}
                  style={{
                    background: "transparent",
                    border: "none",
                    cursor: "pointer",
                    color: "var(--slate-500)",
                    display: "flex",
                    alignItems: "center",
                    padding: 4,
                  }}
                  title="Close Inspector"
                >
                  <X size={14} />
                </button>
              </div>

              <div style={{ padding: "14px 16px", borderBottom: "1px solid var(--slate-200)" }}>
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                  <div>
                    <h3 style={{ fontSize: 14, fontWeight: 700, color: "var(--slate)" }}>
                      {selectedWorker.worker_key}
                    </h3>
                    <div className="font-mono" style={{ fontSize: 11, color: "var(--slate-500)", marginTop: 2 }}>
                      {selectedWorker.id}
                    </div>
                  </div>
                  <StatusBadge status={selectedWorker.health} />
                </div>

                <div
                  style={{
                    marginTop: 12,
                    display: "grid",
                    gridTemplateColumns: "1fr 1fr",
                    gap: 8,
                    fontSize: 11,
                  }}
                >
                  <div>
                    <span style={{ color: "var(--slate-500)" }}>Endpoint:</span>{" "}
                    <span className="font-mono" style={{ fontWeight: 500 }}>
                      {selectedWorker.ip_address}:{selectedWorker.grpc_port}
                    </span>
                  </div>
                  <div>
                    <span style={{ color: "var(--slate-500)" }}>State:</span>{" "}
                    <span style={{ fontWeight: 600 }}>{selectedWorker.state}</span>
                  </div>
                  <div>
                    <span style={{ color: "var(--slate-500)" }}>Schedulable:</span>{" "}
                    <span style={{ fontWeight: 600 }}>
                      {selectedWorker.schedulable ? "Yes" : "No"}
                    </span>
                  </div>
                  <div>
                    <span style={{ color: "var(--slate-500)" }}>Capacity:</span>{" "}
                    <span className="font-mono" style={{ fontWeight: 600 }}>
                      {selectedWorker.active_workloads} / {selectedWorker.capacity} slots
                    </span>
                  </div>
                </div>
              </div>

              {/* Instances List on this Worker */}
              <div style={{ padding: "12px 16px" }}>
                <div
                  style={{
                    fontSize: 11,
                    fontWeight: 700,
                    textTransform: "uppercase",
                    letterSpacing: "0.05em",
                    color: "var(--slate-500)",
                    marginBottom: 10,
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                  }}
                >
                  <span>Assigned Containers ({instances.length})</span>
                  {loadingInstances && <span style={{ fontSize: 10 }}>Syncing...</span>}
                </div>

                {instancesError ? (
                  <div style={{ color: "var(--red-600)", fontSize: 11, padding: 8 }}>
                    {instancesError}
                  </div>
                ) : instances.length === 0 ? (
                  <div
                    style={{
                      padding: "24px 10px",
                      textAlign: "center",
                      color: "var(--slate-500)",
                      fontSize: 11,
                      backgroundColor: "var(--slate-50)",
                      borderRadius: 4,
                      border: "1px dashed var(--slate-200)",
                    }}
                  >
                    No container workloads currently running on this node.
                  </div>
                ) : (
                  <div style={{ display: "flex", flexDirection: "column", gap: 8, maxHeight: 400, overflowY: "auto" }}>
                    {instances.map((inst) => (
                      <div
                        key={inst.id}
                        style={{
                          padding: "10px 12px",
                          backgroundColor: "#FFFFFF",
                          border: "1px solid var(--slate-200)",
                          borderRadius: 4,
                          display: "flex",
                          flexDirection: "column",
                          gap: 6,
                        }}
                      >
                        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                            <StatusDot status={inst.status} size={7} />
                            <span className="font-mono" style={{ fontWeight: 600, fontSize: 12 }}>
                              {inst.instance_key || inst.id.substring(0, 12)}
                            </span>
                          </div>
                          <StatusBadge status={inst.status} size="sm" />
                        </div>

                        <div style={{ fontSize: 11, color: "var(--slate-500)", display: "flex", flexDirection: "column", gap: 3 }}>
                          <div>
                            <span>Container ID:</span>{" "}
                            <span className="font-mono" style={{ color: "var(--slate)", fontWeight: 500 }}>
                              {inst.container_id ? inst.container_id.substring(0, 16) : "simulated"}
                            </span>
                          </div>
                          {inst.image && (
                            <div>
                              <span>Image:</span>{" "}
                              <span className="font-mono" style={{ color: "var(--slate)" }}>
                                {inst.image}
                              </span>
                            </div>
                          )}
                          <div style={{ display: "flex", alignItems: "center", gap: 12, marginTop: 2 }}>
                            <span>
                              Uptime:{" "}
                              <strong style={{ color: "var(--slate)" }}>
                                {inst.uptime || "active"}
                              </strong>
                            </span>
                            <span className="timestamp">
                              Started {inst.created_at ? inst.created_at.substring(11, 19) : "—"}
                            </span>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </>
  );
}
