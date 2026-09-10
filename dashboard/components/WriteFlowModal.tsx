"use client";

import React, { useState, useEffect } from "react";
import {
  X,
  LogIn,
  FolderGit2,
  Rocket,
  CheckCircle,
  AlertCircle,
  Loader2,
} from "lucide-react";
import {
  setAuthToken,
  getAuthToken,
  createProject,
  createDeployment,
  fetchProjects,
} from "@/lib/api";
import { Project } from "@/lib/types";

export type ModalType = "login" | "project" | "deploy" | null;

interface WriteFlowModalProps {
  type: ModalType;
  onClose: () => void;
  onSuccess?: () => void;
}

export function WriteFlowModal({ type, onClose, onSuccess }: WriteFlowModalProps) {
  // Auth state
  const [tokenInput, setTokenInput] = useState("");
  const [currentToken, setCurrentToken] = useState("");

  // Project state
  const [projectName, setProjectName] = useState("");
  const [repoUrl, setRepoUrl] = useState("");
  const [defaultBranch, setDefaultBranch] = useState("main");
  const [rootDir, setRootDir] = useState("/");

  // Deploy state
  const [selectedProjectId, setSelectedProjectId] = useState("");
  const [imageName, setImageName] = useState("nginx:alpine");
  const [instanceCount, setInstanceCount] = useState(2);
  const [envVars, setEnvVars] = useState("PORT=8080\nENV=production");
  const [projectsList, setProjectsList] = useState<Project[]>([]);

  // Status state
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [successMsg, setSuccessMsg] = useState<string | null>(null);

  useEffect(() => {
    if (typeof window !== "undefined") {
      const tok = getAuthToken();
      setCurrentToken(tok);
      setTokenInput(tok || "nebula-adm-token");
    }
  }, [type]);

  useEffect(() => {
    if (type === "deploy") {
      fetchProjects()
        .then((projs) => {
          setProjectsList(projs || []);
          if (projs && projs.length > 0 && !selectedProjectId) {
            setSelectedProjectId(projs[0].id || projs[0].name);
          }
        })
        .catch(() => {
          // If fetching projects fails, user can type project ID manually
        });
    }
  }, [type]);

  if (!type) return null;

  const handleLoginSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!tokenInput.trim()) {
      setError("Token cannot be blank");
      return;
    }
    setAuthToken(tokenInput.trim());
    setCurrentToken(tokenInput.trim());
    setSuccessMsg("Auth token saved successfully!");
    setTimeout(() => {
      onSuccess?.();
      onClose();
    }, 600);
  };

  const handleCreateProjectSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setLoading(true);
    try {
      if (!projectName.trim()) {
        throw new Error("Project name is required");
      }
      const p = await createProject({
        name: projectName.trim(),
        repo_url: repoUrl.trim() || undefined,
        default_branch: defaultBranch.trim() || undefined,
        root_dir: rootDir.trim() || undefined,
      });
      setSuccessMsg(`Project '${p.name || projectName}' connected & created! (ID: ${p.id})`);
      setTimeout(() => {
        onSuccess?.();
        onClose();
      }, 900);
    } catch (err: any) {
      setError(err?.message || "Failed to create project");
    } finally {
      setLoading(false);
    }
  };

  const handleDeploySubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setLoading(true);
    try {
      if (!selectedProjectId.trim()) {
        throw new Error("Project ID is required");
      }

      // Parse env lines KEY=VALUE
      const parsedEnv: Record<string, string> = {};
      const lines = envVars.split("\n");
      for (const line of lines) {
        const trimmed = line.trim();
        if (trimmed && !trimmed.startsWith("#") && trimmed.includes("=")) {
          const idx = trimmed.indexOf("=");
          const k = trimmed.substring(0, idx).trim();
          const v = trimmed.substring(idx + 1).trim();
          if (k) parsedEnv[k] = v;
        }
      }

      const d = await createDeployment(selectedProjectId.trim(), {
        image: imageName.trim() || undefined,
        instance_count: Number(instanceCount) || 1,
        env: Object.keys(parsedEnv).length > 0 ? parsedEnv : undefined,
      });

      setSuccessMsg(`Deployment triggered! Release ID: ${d.id.substring(0, 8)}`);
      setTimeout(() => {
        onSuccess?.();
        onClose();
      }, 900);
    } catch (err: any) {
      setError(err?.message || "Failed to trigger deployment");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div
      style={{
        position: "fixed",
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        backgroundColor: "rgba(15, 23, 42, 0.65)",
        backdropFilter: "blur(4px)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 9999,
        padding: 16,
      }}
      onClick={onClose}
    >
      <div
        style={{
          width: "100%",
          maxWidth: 500,
          backgroundColor: "#FFFFFF",
          borderRadius: 8,
          boxShadow: "0 20px 25px -5px rgba(0, 0, 0, 0.2), 0 10px 10px -5px rgba(0, 0, 0, 0.08)",
          overflow: "hidden",
          border: "1px solid var(--slate-200)",
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Modal Header */}
        <div
          style={{
            padding: "16px 20px",
            borderBottom: "1px solid var(--slate-200)",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            backgroundColor: "var(--slate-50)",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            {type === "login" && <LogIn size={18} color="var(--trust-blue)" />}
            {type === "project" && <FolderGit2 size={18} color="var(--trust-blue)" />}
            {type === "deploy" && <Rocket size={18} color="var(--eco-green)" />}
            <span style={{ fontWeight: 700, fontSize: 15, color: "var(--slate)" }}>
              {type === "login" && "Sign In / Set API Token"}
              {type === "project" && "Connect Repository / Create Project"}
              {type === "deploy" && "Trigger New Deployment"}
            </span>
          </div>
          <button
            onClick={onClose}
            style={{
              background: "transparent",
              border: "none",
              cursor: "pointer",
              color: "var(--slate-400)",
              padding: 4,
            }}
          >
            <X size={16} />
          </button>
        </div>

        {/* Feedback Messages */}
        {error && (
          <div
            style={{
              margin: "12px 20px 0",
              padding: "10px 14px",
              backgroundColor: "var(--red-subtle)",
              border: "1px solid var(--red-200)",
              borderRadius: 4,
              display: "flex",
              alignItems: "center",
              gap: 8,
              fontSize: 12,
              color: "var(--red-600)",
            }}
          >
            <AlertCircle size={14} />
            <span>{error}</span>
          </div>
        )}

        {successMsg && (
          <div
            style={{
              margin: "12px 20px 0",
              padding: "10px 14px",
              backgroundColor: "var(--eco-green-subtle)",
              border: "1px solid rgba(46, 125, 50, 0.3)",
              borderRadius: 4,
              display: "flex",
              alignItems: "center",
              gap: 8,
              fontSize: 12,
              color: "var(--eco-green)",
            }}
          >
            <CheckCircle size={14} />
            <span>{successMsg}</span>
          </div>
        )}

        {/* Form Body */}
        {type === "login" && (
          <form onSubmit={handleLoginSubmit} style={{ padding: "20px" }}>
            <p style={{ fontSize: 12.5, color: "var(--slate-600)", marginBottom: 16 }}>
              Provide a Bearer API Token or Admin Key to authorize write actions against the Control Plane.
            </p>
            <div style={{ marginBottom: 16 }}>
              <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "var(--slate-700)", marginBottom: 6 }}>
                API Bearer Token
              </label>
              <input
                type="text"
                value={tokenInput}
                onChange={(e) => setTokenInput(e.target.value)}
                placeholder="e.g. nebula-adm-token or test-token"
                style={{
                  width: "100%",
                  padding: "8px 12px",
                  fontSize: 13,
                  fontFamily: "var(--font-mono)",
                  border: "1px solid var(--slate-300)",
                  borderRadius: 4,
                  outline: "none",
                }}
              />
            </div>
            {currentToken && (
              <div style={{ fontSize: 11.5, color: "var(--slate-500)", marginBottom: 16 }}>
                Currently active token: <code style={{ backgroundColor: "var(--slate-100)", padding: "2px 4px" }}>{currentToken.substring(0, 8)}...</code>
              </div>
            )}
            <div style={{ display: "flex", justifyContent: "flex-end", gap: 10 }}>
              <button
                type="button"
                onClick={onClose}
                style={{
                  padding: "7px 14px",
                  fontSize: 12,
                  border: "1px solid var(--slate-300)",
                  borderRadius: 4,
                  background: "#FFFFFF",
                  cursor: "pointer",
                }}
              >
                Cancel
              </button>
              <button
                type="submit"
                style={{
                  padding: "7px 16px",
                  fontSize: 12,
                  fontWeight: 600,
                  backgroundColor: "var(--trust-blue)",
                  color: "#FFFFFF",
                  border: "none",
                  borderRadius: 4,
                  cursor: "pointer",
                }}
              >
                Save Token
              </button>
            </div>
          </form>
        )}

        {type === "project" && (
          <form onSubmit={handleCreateProjectSubmit} style={{ padding: "20px" }}>
            <div style={{ marginBottom: 14 }}>
              <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "var(--slate-700)", marginBottom: 6 }}>
                Project Name / Identifier *
              </label>
              <input
                type="text"
                required
                value={projectName}
                onChange={(e) => setProjectName(e.target.value)}
                placeholder="my-web-service"
                style={{
                  width: "100%",
                  padding: "8px 12px",
                  fontSize: 13,
                  border: "1px solid var(--slate-300)",
                  borderRadius: 4,
                  outline: "none",
                }}
              />
            </div>

            <div style={{ marginBottom: 14 }}>
              <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "var(--slate-700)", marginBottom: 6 }}>
                Git Repository URL
              </label>
              <input
                type="text"
                value={repoUrl}
                onChange={(e) => setRepoUrl(e.target.value)}
                placeholder="https://github.com/organization/repo.git"
                style={{
                  width: "100%",
                  padding: "8px 12px",
                  fontSize: 13,
                  fontFamily: "var(--font-mono)",
                  border: "1px solid var(--slate-300)",
                  borderRadius: 4,
                  outline: "none",
                }}
              />
            </div>

            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 18 }}>
              <div>
                <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "var(--slate-700)", marginBottom: 6 }}>
                  Default Branch
                </label>
                <input
                  type="text"
                  value={defaultBranch}
                  onChange={(e) => setDefaultBranch(e.target.value)}
                  placeholder="main"
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    fontSize: 13,
                    border: "1px solid var(--slate-300)",
                    borderRadius: 4,
                    outline: "none",
                  }}
                />
              </div>
              <div>
                <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "var(--slate-700)", marginBottom: 6 }}>
                  Root Directory
                </label>
                <input
                  type="text"
                  value={rootDir}
                  onChange={(e) => setRootDir(e.target.value)}
                  placeholder="/"
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    fontSize: 13,
                    border: "1px solid var(--slate-300)",
                    borderRadius: 4,
                    outline: "none",
                  }}
                />
              </div>
            </div>

            <div style={{ display: "flex", justifyContent: "flex-end", gap: 10 }}>
              <button
                type="button"
                onClick={onClose}
                disabled={loading}
                style={{
                  padding: "7px 14px",
                  fontSize: 12,
                  border: "1px solid var(--slate-300)",
                  borderRadius: 4,
                  background: "#FFFFFF",
                  cursor: "pointer",
                }}
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={loading}
                style={{
                  padding: "7px 16px",
                  fontSize: 12,
                  fontWeight: 600,
                  backgroundColor: "var(--trust-blue)",
                  color: "#FFFFFF",
                  border: "none",
                  borderRadius: 4,
                  cursor: loading ? "wait" : "pointer",
                  display: "flex",
                  alignItems: "center",
                  gap: 6,
                }}
              >
                {loading && <Loader2 size={13} className="animate-spin" />}
                <span>Create Project</span>
              </button>
            </div>
          </form>
        )}

        {type === "deploy" && (
          <form onSubmit={handleDeploySubmit} style={{ padding: "20px" }}>
            <div style={{ marginBottom: 14 }}>
              <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "var(--slate-700)", marginBottom: 6 }}>
                Target Project *
              </label>
              {projectsList.length > 0 ? (
                <select
                  value={selectedProjectId}
                  onChange={(e) => setSelectedProjectId(e.target.value)}
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    fontSize: 13,
                    border: "1px solid var(--slate-300)",
                    borderRadius: 4,
                    outline: "none",
                    backgroundColor: "#FFFFFF",
                  }}
                >
                  {projectsList.map((p) => (
                    <option key={p.id} value={p.id}>
                      {p.name} ({p.id})
                    </option>
                  ))}
                </select>
              ) : (
                <input
                  type="text"
                  required
                  value={selectedProjectId}
                  onChange={(e) => setSelectedProjectId(e.target.value)}
                  placeholder="Enter project ID or name"
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    fontSize: 13,
                    border: "1px solid var(--slate-300)",
                    borderRadius: 4,
                    outline: "none",
                  }}
                />
              )}
            </div>

            <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr", gap: 12, marginBottom: 14 }}>
              <div>
                <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "var(--slate-700)", marginBottom: 6 }}>
                  Container Image / Tag
                </label>
                <input
                  type="text"
                  value={imageName}
                  onChange={(e) => setImageName(e.target.value)}
                  placeholder="e.g. nginx:alpine or registry/app:v1"
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    fontSize: 13,
                    fontFamily: "var(--font-mono)",
                    border: "1px solid var(--slate-300)",
                    borderRadius: 4,
                    outline: "none",
                  }}
                />
              </div>
              <div>
                <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "var(--slate-700)", marginBottom: 6 }}>
                  Desired Replicas
                </label>
                <input
                  type="number"
                  min={1}
                  max={20}
                  value={instanceCount}
                  onChange={(e) => setInstanceCount(parseInt(e.target.value) || 1)}
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    fontSize: 13,
                    border: "1px solid var(--slate-300)",
                    borderRadius: 4,
                    outline: "none",
                  }}
                />
              </div>
            </div>

            <div style={{ marginBottom: 18 }}>
              <label style={{ display: "block", fontSize: 12, fontWeight: 600, color: "var(--slate-700)", marginBottom: 6 }}>
                Environment Variables (KEY=VALUE)
              </label>
              <textarea
                rows={3}
                value={envVars}
                onChange={(e) => setEnvVars(e.target.value)}
                placeholder="KEY=VALUE"
                style={{
                  width: "100%",
                  padding: "8px 12px",
                  fontSize: 12,
                  fontFamily: "var(--font-mono)",
                  border: "1px solid var(--slate-300)",
                  borderRadius: 4,
                  outline: "none",
                  resize: "vertical",
                }}
              />
            </div>

            <div style={{ display: "flex", justifyContent: "flex-end", gap: 10 }}>
              <button
                type="button"
                onClick={onClose}
                disabled={loading}
                style={{
                  padding: "7px 14px",
                  fontSize: 12,
                  border: "1px solid var(--slate-300)",
                  borderRadius: 4,
                  background: "#FFFFFF",
                  cursor: "pointer",
                }}
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={loading}
                style={{
                  padding: "7px 16px",
                  fontSize: 12,
                  fontWeight: 600,
                  backgroundColor: "var(--eco-green)",
                  color: "#FFFFFF",
                  border: "none",
                  borderRadius: 4,
                  cursor: loading ? "wait" : "pointer",
                  display: "flex",
                  alignItems: "center",
                  gap: 6,
                }}
              >
                {loading && <Loader2 size={13} className="animate-spin" />}
                <span>Trigger Deploy</span>
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}
