module server

go 1.24.4

replace github.com/ermes-thesis/ermes-common/logger => ../../ermes-common/logger

require github.com/ermes-thesis/ermes-common/logger v0.0.0-00010101000000-000000000000

require (
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/rs/zerolog v1.34.0 // indirect
	golang.org/x/sys v0.36.0 // indirect
)
