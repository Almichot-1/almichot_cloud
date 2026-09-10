export type WorkerHealth =
  | "HEALTHY"
  | "SUSPECTED"
  | "UNHEALTHY"
  | "UNREACHABLE"
  | "UNKNOWN";

export type WorkerState =
  | "READY"
  | "DRAINING"
  | "DOWN"
  | "UNREGISTERED";

export interface Worker {
  id: string;
  worker_key: string;
  hostname: string;
  ip_address: string;
  grpc_port: number;
  capacity: number;
  active_workloads: number;
  state: WorkerState;
  health: WorkerHealth;
  schedulable: boolean;
  labels: Record<string, string>;
  last_beat_at?: string | null;
}

export interface WorkerInstance {
  id: string;
  deployment_id: string;
  worker_id: string;
  instance_key: string;
  status: string;
  container_id: string;
  image?: string;
  uptime?: string;
  created_at: string;
  updated_at: string;
}

export type DeploymentStatus =
  | "QUEUED"
  | "BUILDING"
  | "BUILT"
  | "SCHEDULING"
  | "STARTING"
  | "RUNNING"
  | "FAILED"
  | "STOPPED";

export interface Instance {
  id: string;
  deployment_id: string;
  worker_id: string;
  instance_key: string;
  status: string;
  container_id: string;
  created_at: string;
  updated_at: string;
}

export interface Deployment {
  id: string;
  project_id: string;
  revision: string;
  image: string;
  image_digest: string;
  desired_state: string;
  status: DeploymentStatus;
  stage: string;
  instance_count: number;
  running_count: number;
  observed_count: number;
  instances?: Instance[];
  events?: ControlLoopEvent[];
  created_at: string;
  updated_at: string;
}

export type EventSeverity = "info" | "success" | "warning" | "error";

export interface ControlLoopEvent {
  id: number;
  type: string;
  severity: EventSeverity;
  component: string;
  message: string;
  metadata?: Record<string, any>;
  timestamp: string;
}

export interface EventsResponse {
  events: ControlLoopEvent[];
  next_cursor: number;
  total: number;
}

export interface Project {
  id: string;
  name: string;
  description?: string;
  repo_url?: string;
  default_branch?: string;
  root_dir?: string;
  desired_replicas?: number;
  created_at?: string;
}
