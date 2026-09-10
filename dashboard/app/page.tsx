"use client";

import React, { useState, useEffect, useRef } from "react";
import Link from "next/link";
import { TopNav } from "@/components/TopNav";
import { StatusBadge } from "@/components/StatusBadge";
import { StatusDot } from "@/components/StatusDot";
import { usePolling } from "@/lib/usePolling";
import { fetchWorkers, fetchDeployments, fetchEvents } from "@/lib/api";
import { Worker, Deployment, ControlLoopEvent } from "@/lib/types";
import {
  Server,
  Layers,
  Cpu,
  ArrowRight,
  ShieldCheck,
  AlertTriangle,
  Flame,
  CheckCircle2,
  Terminal,
} from "lucide-react";

export default function OverviewPage() {
  const workersPolling = usePolling<Worker[]>({
    fetcher: fetchWorkers,
    intervalMs: 3000,
    getId: (w) => w.id || w.worker_key,
    getStateSig: (w) => `${w.health}-${w.state}-${w.active_workloads}`,
  });

  const deploymentsPolling = usePolling<Deployment[]>({
    fetcher: fetchDeployments,
    intervalMs: 3000,
    getId: (d) => d.id,
    getStateSig: (d) => `${d.status}-${d.running_count}/${d.instance_count}`,
  });

  // Events streaming with cursor
  const [events, setEvents] = useState<ControlLoopEvent[]>([]);
  const nextCursorRef = useRef<number>(0);
  const [eventsError, setEventsError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;

    const pollEvents = async () => {
      try {
        const res = await fetchEvents(nextCursorRef.current);
        if (!active) return;
        if (res.events && res.events.length > 0) {
          nextCursorRef.current = res.next_cursor;
          setEvents((prev) => {
            const combined = [...res.events, ...prev];
            // Deduplicate by event ID and limit to 100
            const seen = new Set<number>();
            const deduped: ControlLoopEvent[] = [];
            for (const ev of combined) {
              if (!seen.has(ev.id)) {
                seen.add(ev.id);
                deduped.push(ev);
              }
            }
            return deduped.slice(0, 100);
          });
        }
        setEventsError(null);
      } catch (err: any) {
        if (!active) return;
        setEventsError(err?.message || "Failed to fetch event feed");
      }
    };

    pollEvents();
    const timer = setInterval(pollEvents, 2500);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, []);

  const workers = workersPolling.data || [];
  const deployments = deploymentsPolling.data || [];

  // Aggregated KPIs
  const healthyWorkers = workers.filter(
    (w) => (w.health || "").toUpperCase() === "HEALTHY"
  ).length;
  const suspectedWorkers = workers.filter(
    (w) => (w.health || "").toUpperCase() === "SUSPECTED"
  ).length;
  const unhealthyWorkers = workers.filter(
    (w) =>
      (w.health || "").toUpperCase() === "UNHEALTHY" ||
      (w.health || "").toUpperCase() === "UNREACHABLE"
  ).length;

  const totalCapacity = workers.reduce((acc, w) => acc + (w.capacity || 0), 0);
  const totalActiveWorkloads = workers.reduce(
    (acc, w) => acc + (w.active_workloads || 0),
    0
  );
  const utilizationPct =
    totalCapacity > 0
      ? Math.round((totalActiveWorkloads / totalCapacity) * 100)
      : 0;

  const runningDeployments = deployments.filter(
    (d) => (d.status || "").toUpperCase() === "RUNNING"
  ).length;
  const inFlightDeployments = deployments.filter(
    (d) =>
      (d.status || "").toUpperCase() === "BUILDING" ||
      (d.status || "").toUpperCase() === "STARTING" ||
      (d.status || "").toUpperCase() === "QUEUED" ||
      (d.status || "").toUpperCase() === "SCHEDULING"
  ).length;
  const failedDeployments = deployments.filter(
    (d) => (d.status || "").toUpperCase() === "FAILED"
  ).length;

  const isClusterDegraded = unhealthyWorkers > 0 || suspectedWorkers > 0;

  return (
    <>
      <TopNav
        title="Cluster Overview"
        subtitle="Live state telemetry & control-loop events"
        lastUpdated={workersPolling.lastUpdated}
        onRefresh={() => {
          workersPolling.refresh();
          deploymentsPolling.refresh();
        }}
        isDegraded={isClusterDegraded}
      />

      <div style={{ padding: "20px 24px", display: "flex", flexDirection: "column", gap: 20 }}>
        {/* KPI Metrics Row */}
        <section
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))",
            gap: 14,
          }}
        >
          {/* Workers KPI */}
          <div className="ops-panel" style={{ padding: "14px 16px" }}>
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                marginBottom: 8,
              }}
            >
              <span
                style={{
                  fontSize: 11,
                  fontWeight: 600,
                  color: "var(--slate-500)",
                  textTransform: "uppercase",
                  letterSpacing: "0.05em",
                }}
              >
                Workers Fleet
              </span>
              <Server size={14} color="var(--trust-blue)" />
            </div>
            <div style={{ display: "flex", alignItems: "baseline", gap: 8 }}>
              <span
                className="font-mono"
                style={{ fontSize: 24, fontWeight: 700, color: "var(--slate)" }}
              >
                {workers.length}
              </span>
              <span style={{ fontSize: 12, color: "var(--slate-500)" }}>
                total registered
              </span>
            </div>
            <div
              style={{
                marginTop: 10,
                display: "flex",
                gap: 8,
                fontSize: 11,
                fontWeight: 600,
              }}
            >
              <span style={{ color: "var(--eco-green)" }}>
                ● {healthyWorkers} Healthy
              </span>
              {suspectedWorkers > 0 && (
                <span style={{ color: "var(--amber-500)" }}>
                  ● {suspectedWorkers} Suspected
                </span>
              )}
              {unhealthyWorkers > 0 && (
                <span style={{ color: "var(--red-600)" }}>
                  ● {unhealthyWorkers} Unhealthy
                </span>
              )}
            </div>
          </div>

          {/* Instances & Workloads KPI */}
          <div className="ops-panel" style={{ padding: "14px 16px" }}>
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                marginBottom: 8,
              }}
            >
              <span
                style={{
                  fontSize: 11,
                  fontWeight: 600,
                  color: "var(--slate-500)",
                  textTransform: "uppercase",
                  letterSpacing: "0.05em",
                }}
              >
                Running Workloads
              </span>
              <Cpu size={14} color="var(--chambray)" />
            </div>
            <div style={{ display: "flex", alignItems: "baseline", gap: 8 }}>
              <span
                className="font-mono"
                style={{ fontSize: 24, fontWeight: 700, color: "var(--slate)" }}
              >
                {totalActiveWorkloads}
              </span>
              <span style={{ fontSize: 12, color: "var(--slate-500)" }}>
                / {totalCapacity} slots ({utilizationPct}%)
              </span>
            </div>
            {/* Simple progress track */}
            <div
              style={{
                marginTop: 10,
                height: 4,
                width: "100%",
                backgroundColor: "var(--slate-200)",
                borderRadius: 2,
                overflow: "hidden",
              }}
            >
              <div
                style={{
                  height: "100%",
                  width: `${Math.min(utilizationPct, 100)}%`,
                  backgroundColor:
                    utilizationPct > 85 ? "var(--amber-500)" : "var(--trust-blue)",
                }}
              />
            </div>
          </div>

          {/* Deployments KPI */}
          <div className="ops-panel" style={{ padding: "14px 16px" }}>
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                marginBottom: 8,
              }}
            >
              <span
                style={{
                  fontSize: 11,
                  fontWeight: 600,
                  color: "var(--slate-500)",
                  textTransform: "uppercase",
                  letterSpacing: "0.05em",
                }}
              >
                Deployments
              </span>
              <Layers size={14} color="var(--eco-green)" />
            </div>
            <div style={{ display: "flex", alignItems: "baseline", gap: 8 }}>
              <span
                className="font-mono"
                style={{ fontSize: 24, fontWeight: 700, color: "var(--slate)" }}
              >
                {deployments.length}
              </span>
              <span style={{ fontSize: 12, color: "var(--slate-500)" }}>
                total releases
              </span>
            </div>
            <div
              style={{
                marginTop: 10,
                display: "flex",
                gap: 8,
                fontSize: 11,
                fontWeight: 600,
              }}
            >
              <span style={{ color: "var(--eco-green)" }}>
                ● {runningDeployments} Running
              </span>
              {inFlightDeployments > 0 && (
                <span style={{ color: "var(--amber-500)" }}>
                  ● {inFlightDeployments} In-Flight
                </span>
              )}
              {failedDeployments > 0 && (
                <span style={{ color: "var(--red-600)" }}>
                  ● {failedDeployments} Failed
                </span>
              )}
            </div>
          </div>
        </section>

        {/* 2-Column Section: Workers & Deployments at a Glance */}
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "1fr 1fr",
            gap: 16,
          }}
        >
          {/* Workers Quick Table */}
          <div className="ops-panel">
            <div className="ops-panel-header">
              <div className="ops-panel-title">
                <Server size={14} />
                <span>Workers Fleet</span>
              </div>
              <Link
                href="/workers"
                style={{
                  fontSize: 11,
                  fontWeight: 600,
                  color: "var(--trust-blue)",
                  textDecoration: "none",
                  display: "flex",
                  alignItems: "center",
                  gap: 4,
                }}
              >
                <span>View All</span>
                <ArrowRight size={12} />
              </Link>
            </div>
            <div style={{ overflowX: "auto" }}>
              <table className="ops-table">
                <thead>
                  <tr>
                    <th>Worker</th>
                    <th>Health</th>
                    <th>Schedulable</th>
                    <th>Workload</th>
                  </tr>
                </thead>
                <tbody>
                  {workers.length === 0 ? (
                    <tr>
                      <td colSpan={4} style={{ textAlign: "center", color: "var(--slate-500)", padding: 20 }}>
                        {workersPolling.loading ? "Polling worker registry..." : "No workers registered"}
                      </td>
                    </tr>
                  ) : (
                    workers.slice(0, 5).map((w) => {
                      const id = w.id || w.worker_key;
                      const hasChanged = workersPolling.changedKeys.has(id);
                      return (
                        <tr
                          key={id}
                          className={hasChanged ? "animate-state-shift" : ""}
                        >
                          <td>
                            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                              <StatusDot status={w.health} />
                              <span className="font-mono" style={{ fontWeight: 600 }}>
                                {w.worker_key}
                              </span>
                            </div>
                            <div
                              className="font-mono"
                              style={{ fontSize: 10.5, color: "var(--slate-500)", marginTop: 2 }}
                            >
                              {w.ip_address}
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
                          <td className="font-mono">
                            <strong>{w.active_workloads}</strong> / {w.capacity}
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          </div>

          {/* Deployments Quick Table */}
          <div className="ops-panel">
            <div className="ops-panel-header">
              <div className="ops-panel-title">
                <Layers size={14} />
                <span>Deployments</span>
              </div>
              <Link
                href="/deployments"
                style={{
                  fontSize: 11,
                  fontWeight: 600,
                  color: "var(--trust-blue)",
                  textDecoration: "none",
                  display: "flex",
                  alignItems: "center",
                  gap: 4,
                }}
              >
                <span>View All</span>
                <ArrowRight size={12} />
              </Link>
            </div>
            <div style={{ overflowX: "auto" }}>
              <table className="ops-table">
                <thead>
                  <tr>
                    <th>Deployment</th>
                    <th>Status</th>
                    <th>Replicas</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {deployments.length === 0 ? (
                    <tr>
                      <td colSpan={4} style={{ textAlign: "center", color: "var(--slate-500)", padding: 20 }}>
                        {deploymentsPolling.loading ? "Polling deployments..." : "No deployments recorded"}
                      </td>
                    </tr>
                  ) : (
                    deployments.slice(0, 5).map((d) => {
                      const hasChanged = deploymentsPolling.changedKeys.has(d.id);
                      return (
                        <tr
                          key={d.id}
                          className={hasChanged ? "animate-state-shift" : ""}
                        >
                          <td>
                            <div style={{ fontWeight: 600, color: "var(--slate)" }}>
                              {d.image || d.id.substring(0, 8)}
                            </div>
                            <div
                              className="font-mono"
                              style={{ fontSize: 10.5, color: "var(--slate-500)", marginTop: 2 }}
                            >
                              {d.id.substring(0, 8)}
                            </div>
                          </td>
                          <td>
                            <StatusBadge status={d.status} size="sm" />
                          </td>
                          <td className="font-mono">
                            <strong>{d.running_count || 0}</strong> / {d.instance_count || 1}
                          </td>
                          <td
                            className="font-mono timestamp"
                            style={{ fontSize: 11, color: "var(--slate-500)" }}
                          >
                            {d.created_at ? d.created_at.substring(11, 19) : "—"}
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>

        {/* Live-Updating Control-Loop Event Feed */}
        <section className="ops-panel">
          <div className="ops-panel-header">
            <div className="ops-panel-title">
              <Terminal size={14} color="var(--trust-blue)" />
              <span>Control-Loop Live Event Feed</span>
              <span
                style={{
                  fontSize: 10.5,
                  padding: "1px 6px",
                  borderRadius: 10,
                  backgroundColor: "var(--trust-blue-subtle)",
                  color: "var(--trust-blue)",
                  fontWeight: 600,
                }}
              >
                {events.length} events
              </span>
            </div>
            <div
              style={{
                fontSize: 11,
                color: "var(--slate-500)",
                display: "flex",
                alignItems: "center",
                gap: 6,
              }}
            >
              <span className="status-dot healthy polling-pulse" style={{ width: 6, height: 6 }} />
              <span>Live polling (2.5s)</span>
            </div>
          </div>

          <div
            style={{
              maxHeight: 380,
              overflowY: "auto",
              backgroundColor: "#FFFFFF",
              borderTop: "1px solid var(--slate-100)",
            }}
          >
            {events.length === 0 ? (
              <div
                style={{
                  padding: "36px 20px",
                  textAlign: "center",
                  color: "var(--slate-500)",
                  fontSize: 12,
                }}
              >
                No control-loop events recorded yet. Transitions and reconcile passes will appear here automatically.
              </div>
            ) : (
              <div style={{ display: "flex", flexDirection: "column" }}>
                {events.map((ev) => {
                  let badgeClass = "badge-neutral";
                  if (ev.severity === "success") badgeClass = "badge-healthy";
                  else if (ev.severity === "warning") badgeClass = "badge-suspected";
                  else if (ev.severity === "error") badgeClass = "badge-unhealthy";

                  return (
                    <div
                      key={ev.id}
                      style={{
                        padding: "8px 14px",
                        borderBottom: "1px solid var(--slate-100)",
                        display: "flex",
                        alignItems: "center",
                        gap: 12,
                        fontSize: 12,
                        transition: "background-color 0.1s ease",
                      }}
                      onMouseEnter={(e) => {
                        e.currentTarget.style.backgroundColor = "var(--slate-50)";
                      }}
                      onMouseLeave={(e) => {
                        e.currentTarget.style.backgroundColor = "transparent";
                      }}
                    >
                      {/* Monospace Timestamp */}
                      <span
                        className="font-mono timestamp"
                        style={{
                          fontSize: 11,
                          color: "var(--slate-500)",
                          flexShrink: 0,
                          minWidth: 70,
                        }}
                      >
                        {ev.timestamp ? ev.timestamp.substring(11, 19) : "00:00:00"}
                      </span>

                      {/* Component Tag */}
                      <span
                        className="badge badge-neutral font-mono"
                        style={{
                          fontSize: 10,
                          padding: "2px 5px",
                          flexShrink: 0,
                          minWidth: 105,
                          justifyContent: "center",
                        }}
                      >
                        {ev.component || "control-plane"}
                      </span>

                      {/* Event Type / Severity */}
                      <span
                        className={`badge ${badgeClass}`}
                        style={{
                          fontSize: 10,
                          padding: "2px 6px",
                          flexShrink: 0,
                        }}
                      >
                        {ev.type}
                      </span>

                      {/* Message */}
                      <span
                        style={{
                          flex: 1,
                          color: "var(--slate)",
                          fontFamily:
                            ev.type.includes("HEALTH") || ev.type.includes("RECONCILE")
                              ? "var(--font-mono)"
                              : "inherit",
                          fontSize: 12,
                        }}
                      >
                        {ev.message}
                      </span>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </section>
      </div>
    </>
  );
}
