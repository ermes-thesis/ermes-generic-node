module caches

go 1.24.0

replace github.com/ermes-thesis/ermes-common/logger => ../../ermes-common/logger

replace github.com/ermes-thesis/ermes-common/caches => ../../ermes-common/caches

replace github.com/ermes-thesis/ermes-common/model => ../../ermes-common/model

replace github.com/ermes-thesis/ermes-common/utils => ../../ermes-common/utils

require (
	github.com/ermes-thesis/ermes-common/caches v0.0.0-00010101000000-000000000000
	github.com/ermes-thesis/ermes-common/logger v0.0.0-00010101000000-000000000000
	github.com/ermes-thesis/ermes-common/model v0.0.0-00010101000000-000000000000
	github.com/rs/zerolog v1.34.0
	github.com/stretchr/testify v1.11.1
	golang.org/x/sync v0.17.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/ermes-thesis/ermes-common/utils v0.0.0-00010101000000-000000000000 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/sys v0.36.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
