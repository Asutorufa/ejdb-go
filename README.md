# ejdb-go

A pure Go, EJDB-style embedded JSON database. The current implementation uses Pebble as the default persistent storage engine and keeps storage behind the `StorageEngine` interface, so other KV, LSM, or custom engines can be added later.

## Compatibility

This project keeps the embedded EJDB2-style API, JQL-like queries, indexes, patches, projections, and joins, but it **does not aim to be binary-compatible with official EJDB2/IWKV database files**.

The on-disk format is this project's own Pebble key/value layout:

- `meta/state`: catalog, collection, and index metadata
- `seq/<collection>`: collection auto-increment sequence
- `doc/<collection>/<id>`: canonical JSON document
- `idx/<collection>/<mode>/<path>/<value>/<id>`: non-unique index entry
- `uidx/<collection>/<mode>/<path>/<value>`: unique index entry

## Features

- Default Pebble persistence backend
- Pluggable `StorageEngine` interface
- Atomic commits through Pebble batches
- Pebble checkpoint backups with `Backup(dst)`
- Official-style order-by planning: a single `ORDERBY` with a matching index scans index order directly; otherwise the query uses a sorter
- Sort overflow files: sorter input spills to a temporary file after `Options.SortBufferSize` bytes, defaulting to 16 MiB
- Official-like JQL comparison semantics for null, bool, numbers, strings, arrays, and objects
- Indexed equality, `in`, range, and string prefix planning
- Collection management: create, remove, rename
- Document CRUD: `PutNew`, `Put`, `Get`, `Delete`, `Patch`, `MergeOrPut`
- Indexes: `IdxUnique | IdxString/IdxInt64/IdxFloat`, including unique constraints and array element expansion
- Query object model
  - Filters: `/*`, `/**`, `/path`, `/path/[expr]`, `/=` primary-key matching
  - Expressions: `= != > >= < <= eq/gt/gte/lt/lte in ni re ~ not`
  - Placeholders: named `:name` and positional `:?`
  - Pipelines: `apply`, `upsert`, `del`, projection, official-style `asc/desc` order-by clauses, `skip/limit/count`
  - Order by: `asc /firstName desc /age`, `asc /firstName /rank`, and placeholder paths such as `desc :?`
  - Skip/limit: numeric literals or placeholders, for example `skip :offset limit :limit`
  - Collection joins: `/artist_ref<artists`
- Execution APIs: `Exec`, `ListQuery`, `Count`, `UpdateQuery`
- Transactions: `ReadTx`, `WriteTx`
- Error model: `error + ErrorCode`

## Quick Start

```go
package main

import (
 "fmt"
 "log"

 ejdb "github.com/Asutorufa/ejdb-go"
)

func main() {
 db, err := ejdb.Open(ejdb.Options{Path: "demo.pebble"})
 if err != nil {
  log.Fatal(err)
 }
 defer db.Close()

 _ = db.EnsureIndexMode("users", "/age", ejdb.IdxInt64)
 _, _ = db.PutNew("users", []byte(`{"name":"alice","age":20}`))

 q, _ := ejdb.NewQuery("users", "/[age >= :?] | asc /age")
 _ = q.SetI64("", 0, 18)
 res, _ := db.ListQuery(q, 0)
 fmt.Println("hits:", len(res))
}
```

## Options

- `Path`: Pebble database directory.
- `AutoSync`: use synced Pebble writes when true; the default uses Pebble `NoSync`.
- `Engine`: optional custom `StorageEngine`.
- `PebbleOptions`: optional Pebble configuration for the default engine.
- `SortBufferSize`: sorter in-memory document buffer before temporary-file overflow; default is 16 MiB.

## Complete Example

```bash
go run ./example
```

The example creates a Pebble database under `example/data/demo.pebble` and demonstrates queries, a unique index, `apply`, `del`, projection + join, visitors, backup, and reopening a backup.

## Tests And Benchmarks

```bash
go test ./...
go test -bench . -benchmem ./...
```

### Benchmark Coverage

- `BenchmarkPutNew`: continuous single-document inserts.
- `BenchmarkPutNewWriteTx`: continuous inserts inside one write transaction, useful for batch throughput.
- `BenchmarkGetByID`: document lookup by collection and primary key.
- `BenchmarkQueryScanVsIndex/scan`: non-indexed conditional query path.
- `BenchmarkQueryScanVsIndex/indexed`: indexed conditional query path for the same predicate.
- `BenchmarkRangeQuery`: range query with sorting.
- `BenchmarkSortPagination`: sorting plus pagination.
- `BenchmarkUpdateDelete`: indexed update + delete.

For a quick local trend check:

```bash
go test -bench . -benchmem -benchtime=200ms ./...
```

### Benchmark Results

Sample environment:

- `goos: darwin`
- `goarch: arm64`
- `cpu: Apple M4`
- Command: `go test -bench . -benchmem -benchtime=200ms ./...`

Latest sample:

- `BenchmarkPutNew-10`: `4657 ns/op`, `218729 docs/s`, `218729 ops/s`, `4008 B/op`, `54 allocs/op`
- `BenchmarkPutNewWriteTx-10`: `2006 ns/op`, `568544 docs/s`, `568544 ops/s`, `3779 B/op`, `44 allocs/op`
- `BenchmarkGetByID-10`: `32.16 ns/op`, `31946468 docs/s`, `31946468 ops/s`, `31 B/op`, `1 allocs/op`
- `BenchmarkQueryScanVsIndex/scan-10`: `5923565 ns/op`, `168.8 docs/s`, `168.8 ops/s`, `14559789 B/op`, `179747 allocs/op`
- `BenchmarkQueryScanVsIndex/indexed-10`: `910.2 ns/op`, `1098662 docs/s`, `1098662 ops/s`, `1648 B/op`, `29 allocs/op`
- `BenchmarkRangeQuery-10`: `14635008 ns/op`, `6965 docs/s`, `69.65 ops/s`, `16328087 B/op`, `209639 allocs/op`
- `BenchmarkSortPagination-10`: `13562271 ns/op`, `3725 docs/s`, `74.49 ops/s`, `16002939 B/op`, `170147 allocs/op`
- `BenchmarkUpdateDelete-10`: `17553 ns/op`, `116554 docs/s`, `58277 ops/s`, `18000 B/op`, `270 allocs/op`

Regular document writes, updates, and deletes use incremental Pebble batches. Structural operations such as collection rename, collection removal, and index rebuild still use a full refresh to keep the implementation simple and reliable.
