# ejdb-go

A pure Go, EJDB-style embedded JSON database. The current implementation uses Pebble as the default persistent storage engine and keeps storage behind the `StorageEngine` interface, so other KV, LSM, or custom engines can be added later.

## Compatibility

This project keeps the embedded EJDB2-style API, JQL-like queries, indexes, patches, projections, and joins, but it **does not aim to be binary-compatible with official EJDB2/IWKV database files**.

The on-disk format is this project's own Pebble key/value layout:

- `meta/state`: catalog, collection, and index metadata
- `seq/<collection>`: collection auto-increment sequence
- `doc/<collection>/<id>`: JBL/Binn-style binary JSON document
- `idx/<collection>/<mode>/<path>/<value>/<id>`: non-unique index entry
- `uidx/<collection>/<mode>/<path>/<value>`: unique index entry

The metadata contains a `format_version`. This implementation does not perform online migrations yet: opening an older or unknown format returns `EJDB_INCOMPATIBLE_FORMAT` and does not delete or rewrite the database directory.

Documents are accepted and returned as JSON, but persisted as a JBL/Binn-style binary encoding with typed null, bool, integer, float32, float64, string-family, blob-family, array, map, and object values. Object keys follow the official Binn 255-byte limit. The Pebble file layout is still project-specific and is not binary-compatible with official EJDB2/IWKV database files.

## Features

- Default Pebble persistence backend
- Pluggable `StorageEngine` interface
- Atomic commits through Pebble batches
- Pebble checkpoint backups with `Backup(dst)`
- Official-style order-by planning: a single `ORDERBY` with a matching index scans index order directly; otherwise the query uses a sorter
- Sort overflow files: sorter input spills to a temporary file after `Options.SortBufferSize` bytes, defaulting to 16 MiB
- Official-like JQL comparison semantics for null, bool, numbers, strings, arrays, and objects
- JBL/Binn-style binary document persistence with JSON input/output at the public API boundary, including official Binn numeric, string-family, blob-family, list, map, and object type coverage
- Indexed equality, `in`, range, and string prefix planning
- Query candidate and order-by index scans read from Pebble snapshot iterators; in-memory index maps are only planner/constraint caches
- Official-style JQL canonical printer with `Query.Canonical()`
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

- `BenchmarkPutNew-10`: `5400 ns/op`, `189433 docs/s`, `189433 ops/s`, `5389 B/op`, `72 allocs/op`
- `BenchmarkPutNewWriteTx-10`: `2565 ns/op`, `435091 docs/s`, `435091 ops/s`, `5155 B/op`, `62 allocs/op`
- `BenchmarkGetByID-10`: `32.30 ns/op`, `31716909 docs/s`, `31716909 ops/s`, `31 B/op`, `1 allocs/op`
- `BenchmarkQueryScanVsIndex/scan-10`: `12753336 ns/op`, `78.41 docs/s`, `78.41 ops/s`, `9612156 B/op`, `269698 allocs/op`
- `BenchmarkQueryScanVsIndex/indexed-10`: `2995 ns/op`, `333940 docs/s`, `333940 ops/s`, `1491 B/op`, `53 allocs/op`
- `BenchmarkRangeQuery-10`: `2243202 ns/op`, `45007 docs/s`, `450.1 ops/s`, `1792463 B/op`, `72374 allocs/op`
- `BenchmarkSortPagination-10`: `14381592 ns/op`, `3529 docs/s`, `70.59 ops/s`, `12212607 B/op`, `271236 allocs/op`
- `BenchmarkUpdateDelete-10`: `35863 ns/op`, `56848 docs/s`, `28424 ops/s`, `20002 B/op`, `342 allocs/op`

Regular document writes, updates, and deletes use incremental Pebble batches. Structural operations such as collection rename, collection removal, and index rebuild still use a full refresh to keep the implementation simple and reliable.
