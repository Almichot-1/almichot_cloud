"use client";

import React, { useState, useEffect } from "react";
import { RefreshCw, LogIn, FolderGit2, Rocket, Key } from "lucide-react";
import { WriteFlowModal, ModalType } from "./WriteFlowModal";
import { getAuthToken } from "@/lib/api";

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
  const [modalType, setModalType] = useState<ModalType>(null);
  const [hasToken, setHasToken] = useState(false);

  useEffect(() => {
    if (typeof window !== "undefined") {
      setHasToken(Boolean(getAuthToken()));
    }
  }, [modalType]);

  return (
    <>
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

        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          {/* Write Action Buttons */}
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <button
              onClick={() => setModalType("login")}
              style={{
                background: hasToken ? "var(--eco-green-subtle)" : "var(--slate-100)",
                border: `1px solid ${hasToken ? "rgba(46, 125, 50, 0.3)" : "var(--slate-300)"}`,
                borderRadius: 4,
                padding: "5px 10px",
                cursor: "pointer",
                display: "flex",
                alignItems: "center",
                gap: 5,
                color: hasToken ? "var(--eco-green)" : "var(--slate-700)",
                fontSize: 11.5,
                fontWeight: 600,
              }}
              title={hasToken ? "Auth token configured" : "Set API Bearer Token"}
            >
              {hasToken ? <Key size={12} /> : <LogIn size={12} />}
              <span>{hasToken ? "Authenticated" : "Sign In"}</span>
            </button>

            <button
              onClick={() => setModalType("project")}
              style={{
                background: "#FFFFFF",
                border: "1px solid var(--slate-300)",
                borderRadius: 4,
                padding: "5px 10px",
                cursor: "pointer",
                display: "flex",
                alignItems: "center",
                gap: 5,
                color: "var(--slate-700)",
                fontSize: 11.5,
                fontWeight: 600,
              }}
              title="Connect repository and create a new project"
            >
              <FolderGit2 size={12} color="var(--trust-blue)" />
              <span>+ Connect Repo</span>
            </button>

            <button
              onClick={() => setModalType("deploy")}
              style={{
                background: "var(--trust-blue)",
                border: "1px solid var(--trust-blue)",
                borderRadius: 4,
                padding: "5px 12px",
                cursor: "pointer",
                display: "flex",
                alignItems: "center",
                gap: 6,
                color: "#FFFFFF",
                fontSize: 11.5,
                fontWeight: 600,
                boxShadow: "0 1px 2px rgba(0,0,0,0.05)",
              }}
              title="Trigger a new deployment"
            >
              <Rocket size={12} />
              <span>Deploy</span>
            </button>
          </div>

          <div
            style={{
              height: 18,
              width: 1,
              backgroundColor: "var(--slate-200)",
              margin: "0 2px",
            }}
          />

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

      {/* Write Action Modal Dialog */}
      <WriteFlowModal
        type={modalType}
        onClose={() => setModalType(null)}
        onSuccess={() => {
          onRefresh?.();
        }}
      />
    </>
  );
}

