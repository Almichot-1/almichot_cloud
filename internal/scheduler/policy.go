package scheduler

// SchedulingPolicy defines the strategy used by the ranker.
type SchedulingPolicy string

const (
	// PolicySpreading spreads workloads evenly across feasible workers (G-08 default).
	PolicySpreading SchedulingPolicy = "SPREADING"
	// PolicyBinPacking packs workloads onto fewer workers.
	PolicyBinPacking SchedulingPolicy = "BIN_PACKING"
)
