module config

go 1.24.4

require (
	github.com/ermes-thesis/ermes-common/caches v0.0.0-00010101000000-000000000000
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/ermes-thesis/ermes-common/model v0.0.0-00010101000000-000000000000 // indirect
	github.com/ermes-thesis/ermes-common/utils v0.0.0-00010101000000-000000000000 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
)

replace github.com/ermes-thesis/ermes-common/caches => ../../ermes-common/caches

replace github.com/ermes-thesis/ermes-common/model => ../../ermes-common/model

replace github.com/ermes-thesis/ermes-common/utils => ../../ermes-common/utils
