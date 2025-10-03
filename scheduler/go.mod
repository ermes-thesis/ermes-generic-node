module scheduler

go 1.24.4

require (
	github.com/bytecodealliance/wasmtime-go/v37 v37.0.0
	github.com/ermes-thesis/ermes-wasm-pool v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.6.0
	github.com/redis/go-redis/v9 v9.14.0
	github.com/rs/zerolog v1.34.0
	github.com/second-state/WasmEdge-go v0.14.0
	github.com/stretchr/testify v1.11.1
)

replace github.com/ermes-thesis/ermes-wasm-pool => ../../ermes-wasm-pool

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	golang.org/x/sys v0.12.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
