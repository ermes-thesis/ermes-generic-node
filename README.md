# ermes-generic-node
Repository which contains the code to run an ermes node

To run:

```bash
$ cd main
$ go build main
$ ./main <port> <node_name> <father_name> <children_name...>
```

To test:

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

can be deployed as:

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