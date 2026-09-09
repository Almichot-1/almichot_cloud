package scheduler

// WorkloadRequirement specifies requirements for scheduling an instance or deployment.
type WorkloadRequirement struct {
	DeploymentID     string
	RequiredCapacity int
	RequiredLabels   map[string]string
}
