FROM golang:1.24.4

RUN curl -sSf https://raw.githubusercontent.com/WasmEdge/WasmEdge/master/utils/install.sh | bash -s -- -p /usr/local

RUN go install github.com/vektra/mockery/v3@v3.5.5