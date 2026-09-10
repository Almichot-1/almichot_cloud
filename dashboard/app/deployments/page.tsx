"use client";

import React, { useState, useEffect } from "react";
import { TopNav } from "@/components/TopNav";
import { StatusBadge } from "@/components/StatusBadge";
import { StatusDot } from "@/components/StatusDot";
import { usePolling } from "@/lib/usePolling";
import { fetchDeployments, fetchDeployment } from "@/lib/api";
import { Deployment, Instance, ControlLoopEvent } from "@/lib/types";
import {
  Layers,
  Box,
  Server,
  Clock,
  ChevronRight,
  X,
  AlertCircle,
  GitCommit,
  Activity,
  History,
} from "lucide-react";

export default function DeploymentsPage() {
  const { data: deployments, loading, error, lastUpdated, changedKeys, refresh } =
    usePolling<Deployment[]>({
      fetcher: fetchDeployments,
      intervalMs: 2500,
      getId: (d) => d.id,
      getStateSig: (d) => `${d.status}-${d.stage}-${d.running_count}/${d.instance_count}`,
    });

  const [selectedDeployment, setSelectedDeployment] = useState<Deployment | null>(null);
  const [deploymentDetail, setDeploymentDetail] = useState<Deployment | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);

  // When selectedDeployment changes, fetch detailed deployment data (instances + history events)
  useEffect(() => {
    if (!selectedDeployment) {
      setDeploymentDetail(null);
      return;
    }

    let active = true;
    const loadDetail = async () => {
      setLoadingDetail(true);
      setDetailError(null);
      try {
        const res = await fetchDeployment(selectedDeployment.id);
        if (active) {
          setDeploymentDetail(res);
        }
      } catch (err: any) {
        if (active) {
          setDetailError(err?.message || "Failed to fetch deployment details");
        }
      } finally {
        if (active) {
          setLoadingDetail(false);
        }
      }
    };

    loadDetail();
    const timer = setInterval(loadDetail, 3000);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [selectedDeployment?.id]);

  const list = deployments || [];
  const anyFailed = list.some((d) => d.status === "FAILED");

  return (
    <>
      <TopNav
        title="Deployments"
        subtitle="Active applications & replica convergence"
        lastUpdated={lastUpdated}
        onRefresh={refresh}
        isDegraded={anyFailed}
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
            gridTemplateColumns: selectedDeployment ? "1fr 440px" : "1fr",
            gap: 16,
            alignItems: "start",
          }}
        >
          {/* Main Deployments Table */}
          <div className="ops-panel">
            <div className="ops-panel-header">
              <div className="ops-panel-title">
                <Layers size={14} />
                <span>All Deployments ({list.length})</span>
              </div>
              <span style={{ fontSize: 11, color: "var(--slate-500)" }}>
                Click a deployment row to inspect instance placement & history
              </span>
            </div>

            <div style={{ overflowX: "auto" }}>
              <table className="ops-table">
                <thead>
                  <tr>
                    <th>Application / Image</th>
                    <th>Status</th>
                    <th>Stage</th>
                    <th>Replica Count</th>
                    <th>Project ID</th>
                    <th>Created</th>
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
                        {loading ? "Polling deployments..." : "No deployments recorded"}
                      </td>
                    </tr>
                  ) : (
                    list.map((d) => {
                      const isSelected =
                        selectedDeployment && selectedDeployment.id === d.id;
                      const hasChanged = changedKeys.has(d.id);
                      const isConverged =
                        d.status === "RUNNING" &&
                        d.running_count === d.instance_count;

                      return (
                        <tr
                          key={d.id}
                          className={`clickable ${isSelected ? "selected" : ""} ${
                            hasChanged ? "animate-state-shift" : ""
                          }`}
                          onClick={() => setSelectedDeployment(d)}
                        >
                          <td>
                            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                              <StatusDot status={d.status} />
                              <div>
                                <div style={{ fontWeight: 600, color: "var(--slate)" }}>
                                  {d.image || d.id.substring(0, 8)}
                                </div>
                                <div
                                  className="font-mono"
                                  style={{ fontSize: 10, color: "var(--slate-500)" }}
                                >
                                  {d.id.substring(0, 13)}
                                </div>
                              </div>
                            </div>
                          </td>

                          <td>
                            <StatusBadge status={d.status} size="sm" />
                          </td>

                          <td>
                            <span
                              className="font-mono"
                              style={{
                                fontSize: 11,
                                fontWeight: 500,
                                color: "var(--slate-500)",
                              }}
                            >
                              {d.stage || "—"}
                            </span>
                          </td>

                          <td>
                            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                              <span
                                className="font-mono"
                                style={{
                                  fontWeight: 700,
                                  color: isConverged
                                    ? "var(--eco-green)"
                                    : "var(--amber-500)",
                                }}
                              >
                                {d.running_count || 0}
                              </span>
                              <span style={{ color: "var(--slate-500)" }}>
                                / {d.instance_count || 1}
                              </span>
                              <span style={{ fontSize: 11, color: "var(--slate-500)" }}>
                                replicas
                              </span>
                            </div>
                          </td>

                          <td>
                            <span
                              className="font-mono"
                              style={{ fontSize: 11, color: "var(--slate-500)" }}
                            >
                              {d.project_id ? d.project_id.substring(0, 13) : "default"}
                            </span>
                          </td>

                          <td>
                            <div
                              className="font-mono timestamp"
                              style={{ fontSize: 11, color: "var(--slate)" }}
                            >
                              {d.created_at ? d.created_at.substring(11, 19) + " UTC" : "—"}
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

          {/* Inspector Panel: Instance Placement & State History */}
          {selectedDeployment && (
            <div className="ops-panel" style={{ position: "sticky", top: 72 }}>
              <div className="ops-panel-header">
                <div className="ops-panel-title">
                  <Activity size={14} color="var(--trust-blue)" />
                  <span>Deployment Inspector</span>
                </div>
                <button
                  onClick={() => setSelectedDeployment(null)}
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

              {/* Deployment Meta Overview */}
              <div style={{ padding: "14px 16px", borderBottom: "1px solid var(--slate-200)" }}>
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                  <div>
                    <h3 style={{ fontSize: 14, fontWeight: 700, color: "var(--slate)" }}>
                      {selectedDeployment.image || selectedDeployment.id.substring(0, 8)}
                    </h3>
                    <div className="font-mono" style={{ fontSize: 11, color: "var(--slate-500)", marginTop: 2 }}>
                      {selectedDeployment.id}
                    </div>
                  </div>
                  <StatusBadge status={selectedDeployment.status} />
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
                    <span style={{ color: "var(--slate-500)" }}>Desired Replicas:</span>{" "}
                    <strong className="font-mono">{selectedDeployment.instance_count}</strong>
                  </div>
                  <div>
                    <span style={{ color: "var(--slate-500)" }}>Running Replicas:</span>{" "}
                    <strong className="font-mono" style={{ color: "var(--eco-green)" }}>
                      {selectedDeployment.running_count || 0}
                    </strong>
                  </div>
                  <div>
                    <span style={{ color: "var(--slate-500)" }}>Stage:</span>{" "}
                    <span className="font-mono">{selectedDeployment.stage}</span>
                  </div>
                  <div>
                    <span style={{ color: "var(--slate-500)" }}>Project:</span>{" "}
                    <span className="font-mono">{selectedDeployment.project_id ? selectedDeployment.project_id.substring(0, 8) : "default"}</span>
                  </div>
                </div>

                {selectedDeployment.image_digest && (
                  <div
                    style={{
                      marginTop: 10,
                      padding: "6px 8px",
                      backgroundColor: "var(--slate-50)",
                      borderRadius: 3,
                      border: "1px solid var(--slate-200)",
                      fontSize: 10.5,
                    }}
                  >
                    <span style={{ color: "var(--slate-500)" }}>Digest: </span>
                    <span className="font-mono" style={{ color: "var(--slate)", wordBreak: "break-all" }}>
                      {selectedDeployment.image_digest}
                    </span>
                  </div>
                )}
              </div>

              {/* Instances Placement Breakdown */}
              <div style={{ padding: "12px 16px", borderBottom: "1px solid var(--slate-200)" }}>
                <div
                  style={{
                    fontSize: 11,
                    fontWeight: 700,
                    textTransform: "uppercase",
                    letterSpacing: "0.05em",
                    color: "var(--slate-500)",
                    marginBottom: 8,
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                  }}
                >
                  <span>Scheduled Instances ({deploymentDetail?.instances?.length || 0})</span>
                  {loadingDetail && <span style={{ fontSize: 10 }}>Syncing...</span>}
                </div>

                {!deploymentDetail?.instances || deploymentDetail.instances.length === 0 ? (
                  <div
                    style={{
                      padding: "16px 10px",
                      textAlign: "center",
                      color: "var(--slate-500)",
                      fontSize: 11,
                      backgroundColor: "var(--slate-50)",
                      borderRadius: 4,
                      border: "1px dashed var(--slate-200)",
                    }}
                  >
                    No instance records found for this release.
                  </div>
                ) : (
                  <div style={{ display: "flex", flexDirection: "column", gap: 6, maxHeight: 180, overflowY: "auto" }}>
                    {deploymentDetail.instances.map((inst) => (
                      <div
                        key={inst.id}
                        style={{
                          padding: "8px 10px",
                          backgroundColor: "#FFFFFF",
                          border: "1px solid var(--slate-200)",
                          borderRadius: 3,
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "space-between",
                          fontSize: 11,
                        }}
                      >
                        <div>
                          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                            <StatusDot status={inst.status} size={6} />
                            <span className="font-mono" style={{ fontWeight: 600 }}>
                              {inst.instance_key || inst.id.substring(0, 10)}
                            </span>
                          </div>
                          <div
                            className="font-mono"
                            style={{ fontSize: 10, color: "var(--slate-500)", marginTop: 2 }}
                          >
                            Worker: {inst.worker_id ? inst.worker_id.substring(0, 10) : "unassigned"}
                          </div>
                        </div>
                        <StatusBadge status={inst.status} size="sm" />
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* State-Transition History */}
              <div style={{ padding: "12px 16px" }}>
                <div
                  style={{
                    fontSize: 11,
                    fontWeight: 700,
                    textTransform: "uppercase",
                    letterSpacing: "0.05em",
                    color: "var(--slate-500)",
                    marginBottom: 8,
                    display: "flex",
                    alignItems: "center",
                    gap: 6,
                  }}
                >
                  <History size={12} />
                  <span>State-Transition History</span>
                </div>

                {!deploymentDetail?.events || deploymentDetail.events.length === 0 ? (
                  <div
                    style={{
                      padding: "16px 10px",
                      textAlign: "center",
                      color: "var(--slate-500)",
                      fontSize: 11,
                      backgroundColor: "var(--slate-50)",
                      borderRadius: 4,
                    }}
                  >
                    No historical transition events recorded.
                  </div>
                ) : (
                  <div style={{ display: "flex", flexDirection: "column", gap: 6, maxHeight: 180, overflowY: "auto" }}>
                    {deploymentDetail.events.map((ev) => (
                      <div
                        key={ev.id}
                        style={{
                          padding: "6px 8px",
                          backgroundColor: "var(--slate-50)",
                          borderLeft: `3px solid ${
                            ev.severity === "success"
                              ? "var(--eco-green)"
                              : ev.severity === "error"
                              ? "var(--red-600)"
                              : "var(--trust-blue)"
                          }`,
                          fontSize: 11,
                        }}
                      >
                        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                          <span style={{ fontWeight: 600, color: "var(--slate)" }}>
                            {ev.type}
                          </span>
                          <span className="font-mono timestamp" style={{ fontSize: 10, color: "var(--slate-500)" }}>
                            {ev.timestamp ? ev.timestamp.substring(11, 19) : ""}
                          </span>
                        </div>
                        <div style={{ color: "var(--slate-500)", marginTop: 2, fontSize: 10.5 }}>
                          {ev.message}
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
