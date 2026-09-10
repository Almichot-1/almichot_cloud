import {
  Worker,
  WorkerInstance,
  Deployment,
  EventsResponse,
  Project,
} from "./types";

const API_BASE = ""; // Relative path allows Next.js rewrite proxy to handle API without CORS issues

let authToken: string = "";

export function setAuthToken(token: string) {
  authToken = token;
  if (typeof window !== "undefined") {
    localStorage.setItem("nebula_token", token);
  }
}

export function getAuthToken(): string {
  if (!authToken && typeof window !== "undefined") {
    authToken = localStorage.getItem("nebula_token") || "";
  }
  return authToken;
}

function getHeaders(extraHeaders: Record<string, string> = {}): Record<string, string> {
  const headers: Record<string, string> = {
    "Accept": "application/json",
    ...extraHeaders,
  };
  const token = getAuthToken();
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }
  return headers;
}

export async function fetchWorkers(): Promise<Worker[]> {
  const res = await fetch(`${API_BASE}/api/workers`, {
    headers: getHeaders(),
    cache: "no-store",
  });
  if (!res.ok) {
    throw new Error(`Failed to fetch workers: ${res.status} ${res.statusText}`);
  }
  const data: Worker[] = await res.json();
  return (data || []).sort((a, b) => (a.worker_key || "").localeCompare(b.worker_key || ""));
}

export async function fetchWorkerInstances(
  workerId: string
): Promise<WorkerInstance[]> {
  const res = await fetch(
    `${API_BASE}/api/workers/${encodeURIComponent(workerId)}/instances`,
    {
      headers: getHeaders(),
      cache: "no-store",
    }
  );
  if (!res.ok) {
    throw new Error(
      `Failed to fetch instances for worker ${workerId}: ${res.status}`
    );
  }
  return res.json();
}

export async function fetchDeployments(): Promise<Deployment[]> {
  const res = await fetch(`${API_BASE}/api/deployments`, {
    headers: getHeaders(),
    cache: "no-store",
  });
  if (!res.ok) {
    throw new Error(`Failed to fetch deployments: ${res.status} ${res.statusText}`);
  }
  const data: Deployment[] = await res.json();
  return (data || []).sort((a, b) =>
    (a.project_id || "").localeCompare(b.project_id || "") ||
    (a.created_at || "").localeCompare(b.created_at || "")
  );
}

export async function fetchDeployment(id: string): Promise<Deployment> {
  const res = await fetch(`${API_BASE}/api/deployments/${encodeURIComponent(id)}`, {
    headers: getHeaders(),
    cache: "no-store",
  });
  if (!res.ok) {
    throw new Error(`Failed to fetch deployment ${id}: ${res.status}`);
  }
  return res.json();
}

export async function fetchEvents(sinceCursor: number = 0): Promise<EventsResponse> {
  const res = await fetch(`${API_BASE}/api/events?since=${sinceCursor}&limit=60`, {
    headers: getHeaders(),
    cache: "no-store",
  });
  if (!res.ok) {
    throw new Error(`Failed to fetch events: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function fetchProjects(): Promise<Project[]> {
  const res = await fetch(`${API_BASE}/v1/projects`, {
    headers: getHeaders(),
    cache: "no-store",
  });
  if (!res.ok) {
    throw new Error(`Failed to fetch projects: ${res.status}`);
  }
  return res.json();
}

export async function createProject(params: {
  name: string;
  repo_url?: string;
  default_branch?: string;
  root_dir?: string;
}): Promise<Project> {
  const res = await fetch(`${API_BASE}/v1/projects`, {
    method: "POST",
    headers: getHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify(params),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || err.message || `Failed to create project: ${res.status}`);
  }
  return res.json();
}

export async function createDeployment(
  projectID: string,
  params: {
    image?: string;
    source_path?: string;
    instance_count?: number;
    env?: Record<string, string>;
  }
): Promise<Deployment> {
  const res = await fetch(`${API_BASE}/v1/projects/${encodeURIComponent(projectID)}/deployments`, {
    method: "POST",
    headers: getHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify(params),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || err.message || `Failed to create deployment: ${res.status}`);
  }
  return res.json();
}
