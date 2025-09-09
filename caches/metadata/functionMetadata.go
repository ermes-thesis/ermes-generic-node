package metadata

type FunctionMetadata struct {
	Name                  string
	Version               string
	TemplateNames         []string
	DeployedOn            []string
	StrongCollWriteAccess bool
}
