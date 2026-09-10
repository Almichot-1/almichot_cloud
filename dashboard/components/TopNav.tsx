"use client";

import React from "react";
import { RefreshCw, Radio } from "lucide-react";

interface TopNavProps {
  title: string;
  subtitle?: string;
  lastUpdated?: Date | null;
  onRefresh?: () => void;
  isDegraded?: boolean;
}

export function TopNav({
  title,
  subtitle,
  lastUpdated,
  onRefresh,
  isDegraded = false,
}: TopNavProps) {
  return (
    <header
      style={{
        height: "var(--header-height)",
        backgroundColor: "#FFFFFF",
        borderBottom: "1px solid var(--slate-200)",
        padding: "0 24px",
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        position: "sticky",
        top: 0,
        zIndex: 5,
      }}
    >
      <div style={{ display: "flex", alignItems: "baseline", gap: 12 }}>
        <h1
          style={{
            fontSize: 16,
            fontWeight: 700,
            color: "var(--slate)",
            letterSpacing: "-0.01em",
          }}
        >
          {title}
        </h1>
        {subtitle && (
          <span
            style={{
              fontSize: 12,
              color: "var(--slate-500)",
            }}
          >
            {subtitle}
          </span>
        )}
      </div>

      <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
        {/* Cluster convergence state indicator */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 6,
            padding: "4px 10px",
            borderRadius: 3,
            backgroundColor: isDegraded
              ? "var(--amber-subtle)"
              : "var(--eco-green-subtle)",
            border: `1px solid ${
              isDegraded
                ? "rgba(199, 125, 24, 0.3)"
                : "rgba(46, 125, 50, 0.25)"
            }`,
          }}
        >
          <span
            className={`status-dot ${isDegraded ? "suspected" : "healthy"}`}
            style={{ width: 7, height: 7 }}
          />
          <span
            style={{
              fontSize: 11,
              fontWeight: 600,
              color: isDegraded ? "var(--amber-500)" : "var(--eco-green)",
              textTransform: "uppercase",
              letterSpacing: "0.04em",
            }}
          >
            {isDegraded ? "Cluster Degraded" : "Cluster Converged"}
          </span>
        </div>

        {/* Polling last updated & refresh trigger */}
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          {lastUpdated && (
            <span
              className="timestamp"
              style={{
                fontSize: 11,
                color: "var(--slate-500)",
              }}
              title={lastUpdated.toISOString()}
            >
              Updated {lastUpdated.toTimeString().substring(0, 8)}
            </span>
          )}
          {onRefresh && (
            <button
              onClick={onRefresh}
              style={{
                background: "transparent",
                border: "1px solid var(--slate-200)",
                borderRadius: 4,
                padding: "4px 8px",
                cursor: "pointer",
                display: "flex",
                alignItems: "center",
                gap: 4,
                color: "var(--slate-500)",
                fontSize: 11,
                fontWeight: 500,
              }}
              title="Trigger immediate poll"
            >
              <RefreshCw size={11} />
              <span>Poll</span>
            </button>
          )}
        </div>
      </div>
    </header>
  );
}
