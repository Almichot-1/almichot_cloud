import {
  Worker,
  WorkerInstance,
  Deployment,
  EventsResponse,
} from "./types";

const API_BASE = ""; // Relative path allows Next.js rewrite proxy to handle http://localhost:8080 without CORS issues

export async function fetchWorkers(): Promise<Worker[]> {
  const res = await fetch(`${API_BASE}/api/workers`, {
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
    cache: "no-store",
  });
  if (!res.ok) {
    throw new Error(`Failed to fetch deployment ${id}: ${res.status}`);
  }
  return res.json();
}

export async function fetchEvents(sinceCursor: number = 0): Promise<EventsResponse> {
  const res = await fetch(`${API_BASE}/api/events?since=${sinceCursor}&limit=60`, {
    cache: "no-store",
  });
  if (!res.ok) {
    throw new Error(`Failed to fetch events: ${res.status} ${res.statusText}`);
  }
  return res.json();
}
