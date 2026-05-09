# ejdb-go

A pure Go, EJDB-style embedded JSON database. The current implementation uses Pebble as the default persistent storage engine and keeps storage behind the `StorageEngine` interface, so other KV, LSM, or custom engines can be added later.

## Compatibility

This project keeps the embedded EJDB2-style API, JQL-like queries, indexes, patches, projections, and joins, but it **does not aim to be binary-compatible with official EJDB2/IWKV database files**.

The on-disk format is this project's own Pebble key/value layout:

- `meta/state`: catalog, collection, and index metadata
- `seq/<collection-dbid>`: collection auto-increment sequence
- `doc/<collection-dbid>/<id>`: JBL/Binn-style binary JSON document
- `idx/<index-dbid>/<sortable-value>/<id>`: non-unique index entry
- `uidx/<index-dbid>/<sortable-value>`: unique index entry

The metadata contains a `format_version`. This implementation does not perform online migrations yet: opening an older or unknown format returns `EJDB_INCOMPATIBLE_FORMAT` and does not delete or rewrite the database directory.

Documents are accepted and returned as JSON, but persisted as a JBL/Binn-style binary encoding with typed null, bool, integer, float32, float64, string-family, blob-family, array, map, and object values. Object keys follow the official Binn 255-byte limit. The Pebble file layout is still project-specific and is not binary-compatible with official EJDB2/IWKV database files.

## Implementation Model

The public API boundary is JSON, while the durable document value is JBL/Binn binary. Pebble is the source of truth for document bodies and index entries; the in-memory database state keeps only lightweight catalog data such as collection names, DBIDs, next document IDs, record counts, index definitions, and index entry counts.

Opening a database no longer decodes every stored document into Go heap memory. Reads use Pebble `Get`, queries use Pebble snapshot iterators over document or index key ranges, and writes update document keys, index keys, and catalog metadata through Pebble batches. `ReadTx` uses a Pebble snapshot for stable reads, while `WriteTx` buffers uncommitted mutations in memory until commit.

Memory use is therefore driven mainly by catalog size, Pebble's own cache/page working set, active query result windows, sorter spill buffers, and active write-transaction overlays. Large unindexed scans still read and decode each visited document, and large unordered sorts still need temporary result metadata, but documents are not kept as a permanent process-wide cache.

## Compared With Softmotions/EJDB2

Softmotions EJDB2 is powered by IOWOW: a C storage engine based on mmap-backed persistent skiplist databases, WAL, and a single-file layout. This project uses Pebble's LSM-tree storage instead, so it does not reproduce EJDB2's mmap layout, IWKV/IOWOW file format, or WAL/checkpoint internals.

The intended compatibility is at the embedded JSON database layer: EJDB-style collections, document IDs, JQL-like query behavior, JSON patching, projections, joins, and typed indexes. The current implementation follows the same broad disk-oriented idea as EJDB2 by keeping documents and indexes in persistent storage rather than caching all JSON documents in memory.

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
- Query candidate and order-by index scans read from Pebble snapshot iterators; documents and index entries are not loaded into permanent in-memory maps
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

### Sample Core Benchmark Results

Sample environment:

- `goos: linux`
- `goarch: amd64`
- `cpu: AMD Ryzen 5 5600G with Radeon Graphics`
- Command: `go test -run '^$' -bench '^(BenchmarkRangeQuery|BenchmarkSortPagination|BenchmarkPutNewWriteTx|BenchmarkUpdateDelete)$' -benchmem ./...`

| Workload | ns/op | docs/s | ops/s | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| `BenchmarkPutNewWriteTx` | 101445 | 9881 | 9881 | 2161 | 41 |
| `BenchmarkRangeQuery` | 484321 | 206486 | 2065 | 139792 | 3254 |
| `BenchmarkSortPagination` | 666022 | 75077 | 1502 | 175237 | 4289 |
| `BenchmarkUpdateDelete` | 90787 | 22031 | 11015 | 14634 | 301 |

### Sample Pebble Config Comparison

Sample environment:

- `goos: linux`
- `goarch: amd64`
- `cpu: AMD Ryzen 5 5600G with Radeon Graphics`
- Command: `go test -run '^$' -bench '^BenchmarkPebbleConfigs$' -benchmem -benchtime=200ms ./...`

Configurations:

- `default`: Pebble defaults, unsynced writes.
- `small_cache_1m`: 1 MiB block cache, 1 MiB memtable, unsynced writes.
- `large_cache_64m`: 64 MiB block cache, 16 MiB memtable, unsynced writes.
- `sync_writes`: 8 MiB block cache, 4 MiB memtable, synced writes.
- `disable_wal`: 8 MiB block cache, 4 MiB memtable, Pebble WAL disabled; faster but not crash-safe.

| Config | Workload | ns/op | docs/s | ops/s | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `default` | PutNew | 11395 | 87835 | 87835 | 2383 | 51 |
| `default` | GetByID | 3837 | 260850 | 260850 | 918 | 25 |
| `default` | Indexed query | 8984 | 111346 | 111346 | 2031 | 58 |
| `default` | Range query | 512434 | 195199 | 1952 | 139875 | 3254 |
| `small_cache_1m` | PutNew | 15216 | 65734 | 65734 | 2485 | 52 |
| `small_cache_1m` | GetByID | 10986 | 91055 | 91055 | 717 | 23 |
| `small_cache_1m` | Indexed query | 24007 | 41665 | 41665 | 1607 | 54 |
| `small_cache_1m` | Range query | 1255292 | 79684 | 796.8 | 119442 | 3062 |
| `large_cache_64m` | PutNew | 8529 | 117282 | 117282 | 2329 | 51 |
| `large_cache_64m` | GetByID | 2569 | 389402 | 389402 | 849 | 23 |
| `large_cache_64m` | Indexed query | 6528 | 153235 | 153235 | 1957 | 57 |
| `large_cache_64m` | Range query | 340026 | 294200 | 2942 | 132265 | 3151 |
| `sync_writes` | PutNew | 14219 | 70343 | 70343 | 2372 | 51 |
| `sync_writes` | GetByID | 3782 | 264684 | 264684 | 920 | 25 |
| `sync_writes` | Indexed query | 8993 | 111293 | 111293 | 2032 | 58 |
| `sync_writes` | Range query | 488553 | 204899 | 2049 | 139704 | 3253 |
| `disable_wal` | PutNew | 11134 | 89836 | 89836 | 2362 | 51 |
| `disable_wal` | GetByID | 3725 | 268552 | 268552 | 920 | 25 |
| `disable_wal` | Indexed query | 9514 | 105137 | 105137 | 2024 | 58 |
| `disable_wal` | Range query | 484369 | 206521 | 2065 | 139598 | 3253 |

The storage model is disk-backed and benchmark results are sensitive to Pebble options, filesystem cache state, and benchmark duration. Re-run the benchmark on the target machine before using these numbers for capacity planning.

Regular document writes, updates, and deletes use incremental Pebble batches. Collection removal and index removal delete the affected DBID key ranges; index creation scans existing documents and builds the new persistent index.
