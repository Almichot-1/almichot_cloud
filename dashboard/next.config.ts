import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: true,
  async rewrites() {
    const apiUrl = process.env.NEBULA_API_URL || "http://127.0.0.1:8085";
    return [
      {
        source: "/api/:path*",
        destination: `${apiUrl}/api/:path*`,
      },
      {
        source: "/v1/:path*",
        destination: `${apiUrl}/v1/:path*`,
      },
    ];
  },
};

export default nextConfig;
