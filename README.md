# ejdb-go

A pure Go, EJDB-style embedded JSON database. The current implementation uses Pebble as the default persistent storage engine and keeps storage behind the `StorageEngine` interface, so other KV, LSM, or custom engines can be added later.

## Compatibility

This project keeps the embedded EJDB2-style API, JQL-like queries, indexes, patches, projections, and joins, but it **does not aim to be binary-compatible with official EJDB2/IWKV database files**.

The on-disk format is this project's own Pebble key/value layout:

- `meta/state`: catalog, collection, and index metadata
- `seq/<collection>`: collection auto-increment sequence
- `doc/<collection>/<id>`: EJBL binary JSON document
- `idx/<collection>/<mode>/<path>/<value>/<id>`: non-unique index entry
- `uidx/<collection>/<mode>/<path>/<value>`: unique index entry

The metadata contains a `format_version`. This implementation does not perform online migrations yet: opening an older or unknown format returns `EJDB_INCOMPATIBLE_FORMAT` and does not delete or rewrite the database directory.

Documents are accepted and returned as JSON, but persisted as an internal EJBL binary encoding with typed null, bool, int64, float64, string, array, and object values. The Pebble file layout is still project-specific and is not binary-compatible with official EJDB2/IWKV database files.

## Features

- Default Pebble persistence backend
- Pluggable `StorageEngine` interface
- Atomic commits through Pebble batches
- Pebble checkpoint backups with `Backup(dst)`
- Official-style order-by planning: a single `ORDERBY` with a matching index scans index order directly; otherwise the query uses a sorter
- Sort overflow files: sorter input spills to a temporary file after `Options.SortBufferSize` bytes, defaulting to 16 MiB
- Official-like JQL comparison semantics for null, bool, numbers, strings, arrays, and objects
- EJBL binary document persistence with JSON input/output at the public API boundary
- Indexed equality, `in`, range, and string prefix planning
- Query candidate and order-by index scans read from Pebble snapshot iterators; in-memory index maps are only planner/constraint caches
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

- `BenchmarkPutNew-10`: `5645 ns/op`, `180465 docs/s`, `180465 ops/s`, `5458 B/op`, `70 allocs/op`
- `BenchmarkPutNewWriteTx-10`: `2573 ns/op`, `427670 docs/s`, `427670 ops/s`, `5208 B/op`, `60 allocs/op`
- `BenchmarkGetByID-10`: `33.18 ns/op`, `30651327 docs/s`, `30651327 ops/s`, `31 B/op`, `1 allocs/op`
- `BenchmarkQueryScanVsIndex/scan-10`: `17917958 ns/op`, `55.81 docs/s`, `55.81 ops/s`, `22897980 B/op`, `409713 allocs/op`
- `BenchmarkQueryScanVsIndex/indexed-10`: `3625 ns/op`, `275864 docs/s`, `275864 ops/s`, `2776 B/op`, `65 allocs/op`
- `BenchmarkRangeQuery-10`: `20493796 ns/op`, `4963 docs/s`, `49.63 ops/s`, `24895487 B/op`, `451026 allocs/op`
- `BenchmarkSortPagination-10`: `18855000 ns/op`, `2702 docs/s`, `54.05 ops/s`, `24446640 B/op`, `411140 allocs/op`
- `BenchmarkUpdateDelete-10`: `35503 ns/op`, `57425 docs/s`, `28713 ops/s`, `22720 B/op`, `365 allocs/op`

Regular document writes, updates, and deletes use incremental Pebble batches. Structural operations such as collection rename, collection removal, and index rebuild still use a full refresh to keep the implementation simple and reliable.
