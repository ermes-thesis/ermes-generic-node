# ermes-generic-node
Repository which contains the code to run an ermes node

To run:

```bash
$ cd main
$ go build main
$ ./main <port> <node_name> <father_name> <children_name...>
```

To test:

**Add a client to the node hierarchy:**

```bash
$ curl http://localhost:<port>/add-client -v
```

The body of the response should look something like:

```json
{"UserId":2}
```

**Check if a client is present in the hierarchy:**

```bash
$ curl --header "Content-Type: application/json" \
  --data '{"UserId":<num>}' \
  -v \
  http://localhost:<port>/ask-client-exists 
```

If *num* is valid you should get an ok response, if it is invalid an error 403 should pop.

### DOCUMENTATION

Refer to [godoc](https://go.dev/blog/godoc).