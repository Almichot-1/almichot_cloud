import React from "react";

interface StatusDotProps {
  status: string;
  size?: number;
  className?: string;
}

export function StatusDot({ status, size = 8, className = "" }: StatusDotProps) {
  const normalized = (status || "").toLowerCase();

  let dotClass = "healthy";
  if (
    normalized === "healthy" ||
    normalized === "running" ||
    normalized === "ready" ||
    normalized === "converged"
  ) {
    dotClass = "healthy";
  } else if (
    normalized === "suspected" ||
    normalized === "draining" ||
    normalized === "building" ||
    normalized === "starting" ||
    normalized === "queued" ||
    normalized === "scheduling"
  ) {
    dotClass = "suspected";
  } else if (
    normalized === "unhealthy" ||
    normalized === "unreachable" ||
    normalized === "failed" ||
    normalized === "down"
  ) {
    dotClass = "unhealthy";
  }

  return (
    <span
      className={`status-dot ${dotClass} ${className}`}
      style={{ width: size, height: size }}
      title={`State: ${status}`}
    />
  );
}
