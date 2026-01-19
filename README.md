# Ermes Node
Repository which contains the code to run an ermes node.

## Requirements
- [Go](https://go.dev/dl/) 1.25 or higher
- [WasmEdge](https://wasmedge.org/)
- [Redis](https://redis.io/open-source/) 8.4

For this repository to work properly, you must also clone
the [Ermes Common](https://github.com/ermes-thesis/ermes-common) repository with this structure.

```
projects
└───ermes-generic-node
│   │   ...
│   
└───ermes-common
    │   ...
```

To install WasmEdge run

```bash
curl -sSf https://raw.githubusercontent.com/WasmEdge/WasmEdge/master/utils/install.sh | sudo bash -s -- -p /usr/local
```

## How to build and run

```bash
$ cd main
$ go build main
```

To run the ermes node you need to provide the path to a configuration file.
An example configuration file can be found in the folder [./exampleConfigs](./exampleConfigs/).

To run the ermes node use

```bash
$ ./main {pathToConfigFile}
```

## How to test

This project uses [mockery](https://vektra.github.io/mockery/latest/) to generate mocks for testing purposes.
To install mockery run

```bash
go install github.com/vektra/mockery/v3@v3.6.1
```

To generate the mocks open a terminal in the root folder of the project and run

```bash
$ mockery
```

You can run tests of each module with

```bash
$ go test {moduleName}
```

To run tests of the entire project run

```bash
$ go list -f '{{.Dir}}' -m | xargs -I {} go test {}/... --tags test 
```
