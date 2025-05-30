
Notes taken from [this talk](https://medium.com/@matryer/how-i-write-go-http-services-after-seven-years-37c208122831) from Mat Ryer. 

Objectives:

* maintainability
* glanceability <- how quickly can you understand whats going on
* code should be boring <- "obvious"
* clear to new Go people
* self-similar code <- consistent in the way you write code

NB: visit https://github.com/matryer/is .

In `main`, call a function -> `run` which can return errors.

Use a `server` struct:

```go

type server struct{
    db *someDatabase
    router *someRouter
    email EmailSender
}

```

NB: **never use global variables**.

Constructor:

```go
func newServer() *server{
    s := &server{}
    s.routes()
    return s
}
```

NB: no dependencies are created (as may be used for testing), but can be passed as arguments.

Make `server` and `http.Handler`

```go
func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request){
    s.router.ServeHTTP(w, r)
}
```

Better to have some middleware which explicitly handles requests.

Preferred to have a `routes.go` file:

```go
package main

func (s *server) routes() {
    s.router.get("/api/", s.handleAPI())
    s.router.get("/about", s.handleAbout())
    s.router.get("/", s.handleIndex())
}
```

Where all the apis are stored.

Handlers hang off the server -> methods:

```go
func (s *server) handleSomething() http.HandlerFunc {
    // code...
}

```

NB: Of course need to be careful about data races on `s`.

Naming: `handleTasks...`, `handleAuth...`.

Prefer to just return the handler

```go
return func(w http.ResponseWriter, r *http.Request){
    // use
}
```

The method `handleSomething()` can take arguments, which can be passed to `func` using closures.

If handlers go too big, can use many servers.

NB: `handlerFunc` vs `handler` -> more or less interchangeable.

The middleware are just Go functions.

The handlers can be nested:

```go
s.router.Get("/admin", s.adminOnly(s.handleAdminIndex()))
```

NB: common response handler:

```go
func (s *server) respond(w http.ResponseWriter, r *http.Request, data interface{}, status int){
    w.WriteHeader(status)
    if data != nil {
        err := json.NewEncoder(w).Encode(data)
        // TODO: handle err
    }
}
```

Just a very simple response abstraction which barely does anything, can become more sophisticated.

Similar structure for a decoder:

```go
func (s *server) decode(w http.ResponseWriter, r *http.Request, v interface{}) error {
    return json.NewDecoder(r.Body).Decode(v)
}
```

How do I decide response and request types ?

Very common to have them in the package, but another solution is:

```go
func (s *server) handleGreet() http.HandlerFunc {
    type request struct {
        Name string
    }
    type response struct {
        Greeting string `json:"greeting"`
    }
    return func (w http.ResonseWriter, r *http.Request) {
        // ...
    }
}
```

We can use `sync.Once` <- guarantee that operation will be called only once. For example used to improve startup time, and the setup of the handler can be done at the first execution.

Good practice for testing [net/http/httptest](https://golang.org/pkg/net/http/httptest/) .

Can do a `func NewRequest(method, target string, body io.Reader) *http.Request`.

And have `type ResponseRecorder`, `type Server`.

Example of unit test:

```go

funct TestHandleAbout(t *testing.T) {
    is := is.New(t)
    srv := newServer()
    db, cleanup := connectTestDatabase()
    defer cleanup()
    srv.db = db
    r := httptest.NewRequest("GET", "/about", nil)
    w := httptest.NewRecorder()
    srv.ServeHTTP(w, r) // Here we simulate the request received
    is.Equal(w.StatusCode, http.StatusOK)
}

```

It is possible to do also real HTTP requests with `httptest`.

Also middleware is tesable.

More info: https://medium.com/statuscode/37c208122831 .
