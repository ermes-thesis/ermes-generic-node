# ermes-generic-node
Repository which contains the code to run an ermes node

To run:

```bash
$ cd main
$ go build main
$ ./main <port> <node_name> <father_name> <children_name...>
```

To test:

```bash
$ curl "http://localhost:<port>/info"
```