## PROFILING

Taken from [this blog](https://go.dev/blog/pprof).

Import from `runtime/pprof`:

```go
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
    flag.Parse()
    if *cpuprofile != "" {
        f, err := os.Create(*cpuprofile)
        if err != nil {
            log.Fatal(err)
        }
        pprof.StartCPUProfile(f)
        defer pprof.StopCPUProfile()
    }
    //...
}
```

Basically here we define a new flag `-cpuprofile=filename.prof` where the information will be stored.

To enter the profiling environment run `go tool pprof <EXEC_NAME> filename.prof`.

Important command: `topN` -> show top N samples in the profile.

NB: 

```
When CPU profiling is enabled, the Go program stops about 100 times per second and records a sample consisting of the program counters on the currently executing goroutine’s stack. The profile has 2525 samples, so it was running for a bit over 25 seconds.
```

There is a lot of information in the profiler output. THe first three columns represent the number of samples, the percentage and the cumulative percentage.

The fourth and fifth column represent the number of times the function or one called by it was present in the caller stack. To sort by those use `-cum`. NB: this number corresponds to the bottom 100 stack frames.

To show the caller stack use the command `web` which generates an .svg and opens it in a web browser. But you need [graphviz](https://www.graphviz.org/) installed.

`web functionname` shows the graph relative to a single function call.

We can use `list <REGEX>` to list every function name which matches the regex. It shows the function code and how many times the line was accessed. There exists also a `weblist` command.

In order to see a memory profile we can setup another flag `-memprofile`:

```go
var memprofile = flag.String("memprofile", "", "write memory profile to this file")
//...

    FindHavlakLoops(cfgraph, lsgraph)
    if *memprofile != "" {
        f, err := os.Create(*memprofile)
        if err != nil {
            log.Fatal(err)
        }
        pprof.WriteHeapProfile(f)
        f.Close()
        return
    }
 
```

If we run the `go tool pprof` with `--inuse_objects` we show the amount of allocations done.

To view the functions which are spending time allocating data we can check `web mallocgc`.

If we run `go tool pprof --nodefraction=0.1` we discard the functions which account for less than 10% of the samples.

There is also a default profiler for the HTTP package `net/http/pprof` which will install some handlers under a defined URL, for instance:

```
go tool pprof http://localhost:6060/debug/pprof/profile   # 30-second CPU profile
go tool pprof http://localhost:6060/debug/pprof/heap      # heap profile
go tool pprof http://localhost:6060/debug/pprof/block     # goroutine blocking profile
```

## RUNTIME PROFILING

In order to check at runtime the data allocated we can look more in detail in the [runtime package](https://pkg.go.dev/runtime).

For instance the [ReadMemStats](https://pkg.go.dev/runtime#ReadMemStats) function returns the information read about the memory allocated in a variable of type [MemStats](https://pkg.go.dev/runtime#MemStats).