package childUsage

// keep update forces for each child on each resources
type childResourceUsage struct {
	Write resourceUsage
	Read  resourceUsage
}
type resourceUsage struct {
	CumulativeLatency int
	TotalRequests     int
}

func NewChildResourceUsage() *childResourceUsage {
	return &childResourceUsage{
		Write: resourceUsage{CumulativeLatency: 0, TotalRequests: 0},
		Read:  resourceUsage{CumulativeLatency: 0, TotalRequests: 0},
	}
}

func ManageNewRequest(cr *resourceUsage, requestLatency int) {
	cr.TotalRequests++
	cr.CumulativeLatency += requestLatency
}

func GetElasticForce(cr resourceUsage) int {
	return cr.CumulativeLatency * cr.TotalRequests
}

type GetUsage func(*CircularBuffer, int) resourceUsage

func GetRead(cb *CircularBuffer, idx int) resourceUsage {
	return cb.Data[idx].Read
}
func GetWrite(cb *CircularBuffer, idx int) resourceUsage {
	return cb.Data[idx].Write
}
