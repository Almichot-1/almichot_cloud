"use client";

import React, { useEffect, useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  Activity,
  Server,
  Layers,
  Radio,
  Clock,
  Shield,
  RefreshCw,
} from "lucide-react";

export function Sidebar() {
  const pathname = usePathname();
  const [currentTime, setCurrentTime] = useState("");

  useEffect(() => {
    const updateTime = () => {
      const now = new Date();
      setCurrentTime(now.toISOString().substring(11, 19) + " UTC");
    };
    updateTime();
    const timer = setInterval(updateTime, 1000);
    return () => clearInterval(timer);
  }, []);

  const navItems = [
    {
      label: "Overview",
      href: "/",
      icon: Activity,
      active: pathname === "/",
    },
    {
      label: "Workers",
      href: "/workers",
      icon: Server,
      active: pathname === "/workers",
    },
    {
      label: "Deployments",
      href: "/deployments",
      icon: Layers,
      active: pathname === "/deployments",
    },
  ];

  return (
    <aside
      style={{
        width: "var(--sidebar-width)",
        backgroundColor: "var(--chambray-dark)",
        color: "#F5F6F7",
        display: "flex",
        flexDirection: "column",
        flexShrink: 0,
        height: "100vh",
        position: "sticky",
        top: 0,
        borderRight: "1px solid rgba(255, 255, 255, 0.08)",
        zIndex: 10,
      }}
    >
      {/* Brand Header */}
      <div
        style={{
          padding: "16px 18px",
          borderBottom: "1px solid rgba(255, 255, 255, 0.1)",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div
            style={{
              width: 24,
              height: 24,
              backgroundColor: "var(--trust-blue)",
              borderRadius: 4,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              color: "#FFFFFF",
              fontWeight: 700,
              fontSize: 14,
              letterSpacing: -0.5,
            }}
          >
            N
          </div>
          <div>
            <div
              style={{
                fontSize: 14,
                fontWeight: 700,
                letterSpacing: "0.06em",
                color: "#FFFFFF",
                textTransform: "uppercase",
              }}
            >
              Nebula
            </div>
            <div
              style={{
                fontSize: 10,
                color: "rgba(255, 255, 255, 0.6)",
                letterSpacing: "0.05em",
                textTransform: "uppercase",
              }}
            >
              Ops Console
            </div>
          </div>
        </div>

        {/* Live Cluster Dot */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 5,
            backgroundColor: "rgba(46, 125, 50, 0.25)",
            padding: "3px 7px",
            borderRadius: 12,
            border: "1px solid rgba(126, 188, 89, 0.4)",
          }}
          title="Connected to Control Plane"
        >
          <span
            className="status-dot healthy polling-pulse"
            style={{ width: 6, height: 6 }}
          />
          <span
            style={{
              fontSize: 9.5,
              fontWeight: 700,
              color: "var(--eco-green-light)",
              letterSpacing: "0.05em",
            }}
          >
            LIVE
          </span>
        </div>
      </div>

      {/* Navigation Links */}
      <nav style={{ padding: "14px 10px", flex: 1, display: "flex", flexDirection: "column", gap: 4 }}>
        <div
          style={{
            fontSize: 10,
            fontWeight: 700,
            color: "rgba(255, 255, 255, 0.4)",
            textTransform: "uppercase",
            letterSpacing: "0.08em",
            padding: "6px 10px 4px 10px",
          }}
        >
          Control Plane
        </div>

        {navItems.map((item) => {
          const Icon = item.icon;
          return (
            <Link
              key={item.href}
              href={item.href}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                padding: "8px 12px",
                borderRadius: 4,
                textDecoration: "none",
                fontSize: 13,
                fontWeight: item.active ? 600 : 500,
                color: item.active ? "#FFFFFF" : "rgba(255, 255, 255, 0.75)",
                backgroundColor: item.active
                  ? "var(--trust-blue)"
                  : "transparent",
                transition: "all 0.12s ease",
              }}
              onMouseEnter={(e) => {
                if (!item.active) {
                  e.currentTarget.style.backgroundColor =
                    "rgba(255, 255, 255, 0.08)";
                  e.currentTarget.style.color = "#FFFFFF";
                }
              }}
              onMouseLeave={(e) => {
                if (!item.active) {
                  e.currentTarget.style.backgroundColor = "transparent";
                  e.currentTarget.style.color = "rgba(255, 255, 255, 0.75)";
                }
              }}
            >
              <Icon size={16} strokeWidth={item.active ? 2.2 : 1.8} />
              <span>{item.label}</span>
            </Link>
          );
        })}
      </nav>

      {/* Bottom Status Footer */}
      <div
        style={{
          padding: "14px 16px",
          borderTop: "1px solid rgba(255, 255, 255, 0.1)",
          backgroundColor: "rgba(0, 0, 0, 0.15)",
          display: "flex",
          flexDirection: "column",
          gap: 8,
          fontSize: 11,
          color: "rgba(255, 255, 255, 0.65)",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
          <span style={{ display: "flex", alignItems: "center", gap: 5 }}>
            <Shield size={12} color="var(--trust-blue-light)" />
            <strong style={{ color: "rgba(255, 255, 255, 0.9)" }}>READ-ONLY</strong>
          </span>
          <span
            style={{
              fontSize: 10,
              padding: "1px 5px",
              borderRadius: 3,
              backgroundColor: "rgba(255, 255, 255, 0.1)",
              fontFamily: "var(--font-mono)",
            }}
          >
            POLL 3s
          </span>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11 }}>
          <Radio size={12} color="var(--eco-green-light)" />
          <span className="font-mono" style={{ color: "rgba(255, 255, 255, 0.85)" }}>
            127.0.0.1:8080
          </span>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10.5, color: "rgba(255, 255, 255, 0.5)" }}>
          <Clock size={11} />
          <span className="font-mono">{currentTime || "UTC"}</span>
        </div>
      </div>
    </aside>
  );
}
