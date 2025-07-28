package migrationManager

// keep update forces for each child on each resources
type childResourceUsage struct {
	write resourceUsage
	read  resourceUsage
}
type resourceUsage struct {
	CumulativeLatency int
	TotalRequests     int
}

func NewChildResourceUsage() *childResourceUsage {
	return &childResourceUsage{
		write: resourceUsage{CumulativeLatency: 0, TotalRequests: 0},
		read:  resourceUsage{CumulativeLatency: 0, TotalRequests: 0},
	}
}

func manageNewRequest(cr *resourceUsage, requestLatency int) {
	cr.TotalRequests++
	cr.CumulativeLatency += requestLatency
}

func GetElasticForce(cr resourceUsage) int {
	return cr.CumulativeLatency * cr.TotalRequests
}
