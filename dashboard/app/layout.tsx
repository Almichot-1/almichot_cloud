import type { Metadata } from "next";
import "./globals.css";
import { Sidebar } from "@/components/Sidebar";

export const metadata: Metadata = {
  title: "Nebula — Read-Only Ops Dashboard",
  description: "High-dependability read-only ops monitoring dashboard for Nebula compute cluster",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>
        <div style={{ display: "flex", minHeight: "100vh", backgroundColor: "var(--slate-50)" }}>
          <Sidebar />
          <main
            style={{
              flex: 1,
              display: "flex",
              flexDirection: "column",
              minWidth: 0,
              overflowX: "hidden",
            }}
          >
            {children}
          </main>
        </div>
      </body>
    </html>
  );
}
