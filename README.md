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
- Collection management: create, remove, rename
- Document CRUD: `PutNew`, `Put`, `Get`, `Delete`, `Patch`, `MergeOrPut`
- Indexes: `IdxUnique | IdxString/IdxInt64/IdxFloat`, including unique constraints and array element expansion
- Query object model
  - Filters: `/*`, `/**`, `/path`, `/path/[expr]`, `/=` primary-key matching
  - Expressions: `= != > >= < <= eq/gt/gte/lt/lte in ni re ~ not`
  - Placeholders: named `:name` and positional `:?`
  - Pipelines: `apply`, `upsert`, `del`, projection, `asc/desc`, `skip/limit/count`
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

	ejdb "github.com/softmotions/ejdb-go"
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

- `BenchmarkPutNew-10`: `4684 ns/op`, `220617 docs/s`, `220617 ops/s`, `2501 B/op`, `47 allocs/op`
- `BenchmarkPutNewWriteTx-10`: `1885 ns/op`, `615773 docs/s`, `615773 ops/s`, `2448 B/op`, `37 allocs/op`
- `BenchmarkGetByID-10`: `33.15 ns/op`, `30812952 docs/s`, `30812952 ops/s`, `31 B/op`, `1 allocs/op`
- `BenchmarkQueryScanVsIndex/scan-10`: `4356805 ns/op`, `229.5 docs/s`, `229.5 ops/s`, `6327665 B/op`, `120000 allocs/op`
- `BenchmarkQueryScanVsIndex/indexed-10`: `607.6 ns/op`, `1645749 docs/s`, `1645749 ops/s`, `752 B/op`, `20 allocs/op`
- `BenchmarkRangeQuery-10`: `4971674 ns/op`, `20513 docs/s`, `205.1 ops/s`, `6501100 B/op`, `130238 allocs/op`
- `BenchmarkSortPagination-10`: `5078668 ns/op`, `10097 docs/s`, `201.9 ops/s`, `6648008 B/op`, `130040 allocs/op`
- `BenchmarkUpdateDelete-10`: `17528 ns/op`, `115721 docs/s`, `57860 ops/s`, `12257 B/op`, `232 allocs/op`

Regular document writes, updates, and deletes use incremental Pebble batches. Structural operations such as collection rename, collection removal, and index rebuild still use a full refresh to keep the implementation simple and reliable.
