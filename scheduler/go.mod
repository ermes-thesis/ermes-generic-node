module scheduler

go 1.27.1

require (
	github.com/bytecodealliance/wasmtime-go/v44 v44.0.0
	github.com/ermes-thesis/ermes-common/logger v0.0.0-20261015092440-7c3be19f3b38
	github.com/ermes-thesis/ermes-common/model v0.0.0-20261015092440-7c3be19f3b38
	github.com/ermes-thesis/ermes-common/utils v0.0.0-20261015092440-7c3be19f3b38
	github.com/ermes-thesis/ermes-common/wasm-pool v0.0.0-20261015092440-7c3be19f3b38
	github.com/google/uuid v1.6.0
	github.com/redis/go-redis/v9 v9.23.0
	github.com/rs/xid v1.6.0
	github.com/second-state/WasmEdge-go v0.14.0
	github.com/stretchr/testify v1.12.1
	github.com/tetratelabs/wazero v1.12.0
)

replace github.com/ermes-thesis/ermes-common/wasm-pool => ../../ermes-common/wasm-pool

replace github.com/ermes-thesis/ermes-common/caches => ../../ermes-common/caches

replace github.com/ermes-thesis/ermes-common/logger => ../../ermes-common/logger

replace github.com/ermes-thesis/ermes-common/model => ../../ermes-common/model

replace github.com/ermes-thesis/ermes-common/utils => ../../ermes-common/utils

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/ermes-thesis/ermes-common/caches v0.0.0-20261015092440-7c3be19f3b38 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/mattn/go-colorable v0.1.16 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/mitchellh/hashstructure/v2 v2.0.2 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/rs/zerolog v1.35.1 // indirect
	github.com/stretchr/objx v0.5.3 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
