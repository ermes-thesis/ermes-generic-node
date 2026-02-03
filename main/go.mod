module main

go 1.24.4

require (
	github.com/ermes-thesis/ermes-common/logger v0.0.0-00010101000000-000000000000
	github.com/ermes-thesis/ermes-common/wasm-pool v0.0.0-00010101000000-000000000000
)

require (
	github.com/bytecodealliance/wasmtime-go/v40 v40.0.0 // indirect
	github.com/ermes-thesis/ermes-common/caches v0.0.0-00010101000000-000000000000 // indirect
	github.com/ermes-thesis/ermes-common/model v0.0.0-00010101000000-000000000000 // indirect
	github.com/ermes-thesis/ermes-common/utils v0.0.0-00010101000000-000000000000 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mitchellh/hashstructure/v2 v2.0.2 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/rs/zerolog v1.34.0 // indirect
	github.com/second-state/WasmEdge-go v0.14.0 // indirect
	github.com/tetratelabs/wazero v1.11.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/ermes-thesis/ermes-common/wasm-pool => ../../ermes-common/wasm-pool

replace github.com/ermes-thesis/ermes-common/model => ../../ermes-common/model

replace github.com/ermes-thesis/ermes-common/logger => ../../ermes-common/logger

replace github.com/ermes-thesis/ermes-common/utils => ../../ermes-common/utils

replace github.com/ermes-thesis/ermes-common/caches => ../../ermes-common/caches
