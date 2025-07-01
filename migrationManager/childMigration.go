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
	CumulativeLatency int
	TotalRequests     int
}

func newChildResourceUsage() *childResourceUsage {
	return &childResourceUsage{
		write: resourceUsage{CumulativeLatency: 0, TotalRequests: 0},
		read:  resourceUsage{CumulativeLatency: 0, TotalRequests: 0},
	}
}

func manageNewRequest(cr *resourceUsage, requestLatency int) {
	cr.TotalRequests++
	cr.CumulativeLatency += requestLatency
}

func getElasticReadForce(cr resourceUsage) int {
	return cr.CumulativeLatency * cr.TotalRequests
}
