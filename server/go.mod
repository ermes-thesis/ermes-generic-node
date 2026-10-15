module server

go 1.27.1

replace github.com/ermes-thesis/ermes-common/logger => ../../ermes-common/logger

replace github.com/ermes-thesis/ermes-common/utils => ../../ermes-common/utils

require (
	github.com/ermes-thesis/ermes-common/logger v0.0.0-20261015092440-7c3be19f3b38
	github.com/ermes-thesis/ermes-common/utils v0.0.0-20261015092440-7c3be19f3b38
)

require (
	github.com/mattn/go-colorable v0.1.16 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/rs/zerolog v1.35.1 // indirect
	golang.org/x/sys v0.49.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
)
