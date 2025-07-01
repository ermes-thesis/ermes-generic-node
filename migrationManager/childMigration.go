package migrationManager

// child data
type nodeData struct {
	latency float32 //geografical distance
}

func newChildData(latency float32) nodeData {
	return nodeData{latency: latency}
}

func (c nodeData) Latency() float32 {
	return c.latency
}

// keep update forces for each child on each resources
type childResourceUsage struct {
	write resourceUsage
	read  resourceUsage
}
type resourceUsage struct {
	cumulativeLatency int
	TotalRequests     int
}

func newChildResourceUsage() childResourceUsage {
	return childResourceUsage{
		write: resourceUsage{cumulativeLatency: 0, TotalRequests: 0},
		read:  resourceUsage{cumulativeLatency: 0, TotalRequests: 0},
	}
}

func manageNewRequest(cr resourceUsage, requestLatency int) {
	cr.TotalRequests++
	cr.cumulativeLatency += requestLatency
}

func getElasticReadForce(cr resourceUsage) int {
	return cr.cumulativeLatency * cr.TotalRequests
}
