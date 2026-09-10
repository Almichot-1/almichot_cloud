import React from "react";
import { StatusDot } from "./StatusDot";

interface StatusBadgeProps {
  status: string;
  label?: string;
  size?: "sm" | "md";
}

export function StatusBadge({ status, label, size = "md" }: StatusBadgeProps) {
  const normalized = (status || "").toLowerCase();
  const text = label || status || "UNKNOWN";

  let badgeClass = "badge-neutral";
  if (
    normalized === "healthy" ||
    normalized === "running" ||
    normalized === "ready" ||
    normalized === "converged"
  ) {
    badgeClass = "badge-healthy";
  } else if (
    normalized === "suspected" ||
    normalized === "draining" ||
    normalized === "building" ||
    normalized === "starting" ||
    normalized === "queued" ||
    normalized === "scheduling"
  ) {
    badgeClass = "badge-suspected";
  } else if (
    normalized === "unhealthy" ||
    normalized === "unreachable" ||
    normalized === "failed" ||
    normalized === "down"
  ) {
    badgeClass = "badge-unhealthy";
  } else if (normalized === "schedulable") {
    badgeClass = "badge-blue";
  }

  return (
    <span className={`badge ${badgeClass} ${size === "sm" ? "text-xs" : ""}`}>
      <StatusDot status={status} size={6} />
      <span>{text}</span>
    </span>
  );
}
