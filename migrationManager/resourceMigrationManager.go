package migrationManager

type ConsistencyLevel int

const (
	Week ConsistencyLevel = iota
	Strong
)

type resourceData struct {
	consistency ConsistencyLevel
	leaderFlag  bool
}
