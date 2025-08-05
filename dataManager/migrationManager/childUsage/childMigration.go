package childUsage

// ChildResourceUsage keep update forces for each child on each resource
type ChildResourceUsage struct {
	Write ResourceUsage
	Read  ResourceUsage
}
type ResourceUsage struct {
	CumulativeLatency int
	TotalRequests     int
}

func NewChildResourceUsage() *ChildResourceUsage {
	return &ChildResourceUsage{
		Write: ResourceUsage{CumulativeLatency: 0, TotalRequests: 0},
		Read:  ResourceUsage{CumulativeLatency: 0, TotalRequests: 0},
	}
}

func (cr *ResourceUsage) ManageNewRequest(requestLatency int) {
	cr.TotalRequests++
	cr.CumulativeLatency += requestLatency
}

func (cr *ResourceUsage) GetElasticForce() int {
	return cr.CumulativeLatency * cr.TotalRequests
}

type GetUsage func(*ChildResourceUsage) ResourceUsage

func GetRead(data *ChildResourceUsage) ResourceUsage {
	return data.Read
}
func GetWrite(data *ChildResourceUsage) ResourceUsage {
	return data.Write
}
