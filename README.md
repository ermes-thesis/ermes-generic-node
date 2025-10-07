# Ermes Node
Repository which contains the code to run an ermes node.

## Requirements
- [Go](https://go.dev/dl/) 1.24.4 or higher
- [WasmEdge](https://wasmedge.org/)

To install WasmEdge run

```bash
curl -sSf https://raw.githubusercontent.com/WasmEdge/WasmEdge/master/utils/install.sh | sudo bash -s -- -p /usr/local
```

In alternative, you can use the provided DevContainer, which already has all the dependencies installed.

## How to build and run

```bash
$ cd main
$ go build main
```

## How to test

This project uses [mockery](https://vektra.github.io/mockery/latest/) to generate mocks for testing purposes.
To install mockery run

```bash
go install github.com/vektra/mockery/v3@v3.5.5
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

## Outdated stuff

An example architecture structured like:

```
Milan
  |
  -- Milan/POLIMI
  |   |
  |   -- Milan/POLIMI/DEIB
  |   -- Milan/POLIMI/DICA
  |
  -- Milan/UNIMI
      |
      -- Milan/UNIMI/inf
```

can be found in the folder [./exampleConfigs](./exampleConfigs/).

Each file is uniquely identified by the port where the node is run, for instance file [exampleArch1_1111.yaml](./exampleConfigs/exampleArch1_1111.yaml) contains the information of node `Milan`.

In order to run the architecture the command is simply:

```bash
./main ../exampleConfigs/exampleArch1_XXXX.yaml
```

```bash
./main 1111 Milan nil 2222 5555
./main 2222 Milan/POLIMI 1111 3333 44444
./main 3333 Milan/POLIMI/DEIB 2222 nil
./main 4444 Milan/POLIMI/DICA 2222 nil
./main 5555 Milan/UNIMI 1111 6666
./main 6666 Milan/UNIMI/inf 5555 nil
```

**Add a client to the node hierarchy:**

```bash
$ curl http://localhost:<port>/add-client -v
```

The body of the response should look something like:

```json
{"UserId":2}
```

Where the `2` in this case is an incremental unique id used decided by the root of the network.

**Check if a client is present in the hierarchy:**

```bash
$ curl --header "Content-Type: application/json" \
  --data '{"UserId":<num>}' \
  -v \
  http://localhost:<port>/ask-client-exists 
```

If *num* is valid you should get an ok response, if it is invalid an error 403 should pop.

**Adding an header to curl:**

The internal requests are hidden behind the usage of a secret token. In order to add the appropriate header to the `curl` request is:

```bash
--header "X-ERMES-SECRET: ermes-secret"
```

### DOCUMENTATION

Refer to [godoc](https://go.dev/blog/godoc).