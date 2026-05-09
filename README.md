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
- JSON encoding/decoding through `github.com/go-json-experiment/json` and `jsontext.Value` raw values
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

- `goos: linux`
- `goarch: amd64`
- `cpu: AMD Ryzen 5 5600G with Radeon Graphics`
- Command: `go test -bench . -benchmem -benchtime=200ms ./...`

Latest sample:

- `BenchmarkPutNew-12`: `7928 ns/op`, `126291 docs/s`, `126291 ops/s`, `4034 B/op`, `72 allocs/op`
- `BenchmarkPutNewWriteTx-12`: `4850 ns/op`, `216613 docs/s`, `216613 ops/s`, `3777 B/op`, `62 allocs/op`
- `BenchmarkGetByID-12`: `58.59 ns/op`, `17075168 docs/s`, `17075168 ops/s`, `31 B/op`, `1 allocs/op`
- `BenchmarkQueryScanVsIndex/scan-12`: `37160504 ns/op`, `26.91 docs/s`, `26.91 ops/s`, `9541929 B/op`, `319709 allocs/op`
- `BenchmarkQueryScanVsIndex/indexed-12`: `8262 ns/op`, `121035 docs/s`, `121035 ops/s`, `1486 B/op`, `58 allocs/op`
- `BenchmarkRangeQuery-12`: `4085015 ns/op`, `24489 docs/s`, `244.9 ops/s`, `1797940 B/op`, `72883 allocs/op`
- `BenchmarkSortPagination-12`: `38457820 ns/op`, `1301 docs/s`, `26.02 ops/s`, `12220489 B/op`, `321313 allocs/op`
- `BenchmarkUpdateDelete-12`: `67400 ns/op`, `29680 docs/s`, `14840 ops/s`, `18027 B/op`, `352 allocs/op`

Regular document writes, updates, and deletes use incremental Pebble batches. Structural operations such as collection rename, collection removal, and index rebuild still use a full refresh to keep the implementation simple and reliable.
