package ejdb

import (
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkPutNew(b *testing.B) {
	db, err := Open(Options{Path: filepath.Join(b.TempDir(), "put.pebble")})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.PutNew("bench", []byte(fmt.Sprintf(`{"v":%d,"s":"value-%d"}`, i, i))); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
	reportThroughput(b, 1)
}

func BenchmarkPutNewWriteTx(b *testing.B) {
	db, err := Open(Options{Path: filepath.Join(b.TempDir(), "put_tx.pebble")})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	if err := db.WriteTx(func(tx *Tx) error {
		for i := 0; i < b.N; i++ {
			if _, err := tx.PutNew("bench", []byte(fmt.Sprintf(`{"v":%d,"s":"value-%d"}`, i, i))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatalf("write tx: %v", err)
	}
	reportThroughput(b, 1)
}

func BenchmarkGetByID(b *testing.B) {
	db := seedBenchDB(b)
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get("indexed", int64(i%10000+1)); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
	reportThroughput(b, 1)
}

func BenchmarkQueryScanVsIndex(b *testing.B) {
	db := seedBenchDB(b)
	defer db.Close()

	qScan, _ := NewQuery("scan", "/[v = 9000]")
	qIdx, _ := NewQuery("indexed", "/[v = 9000]")

	b.Run("scan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			res, err := db.ListQuery(qScan, 0)
			if err != nil {
				b.Fatalf("scan query: %v", err)
			}
			if len(res) != 1 {
				b.Fatalf("unexpected scan result count: %d", len(res))
			}
		}
		reportThroughput(b, 1)
	})

	b.Run("indexed", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			res, err := db.ListQuery(qIdx, 0)
			if err != nil {
				b.Fatalf("indexed query: %v", err)
			}
			if len(res) != 1 {
				b.Fatalf("unexpected indexed result count: %d", len(res))
			}
		}
		reportThroughput(b, 1)
	})
}

func BenchmarkRangeQuery(b *testing.B) {
	db := seedBenchDB(b)
	defer db.Close()
	q, _ := NewQuery("indexed", "/[v >= 100 and v < 200] | asc /v")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := db.ListQuery(q, 0)
		if err != nil {
			b.Fatalf("range query: %v", err)
		}
		if len(res) != 100 {
			b.Fatalf("unexpected range result count: %d", len(res))
		}
	}
	reportThroughput(b, 100)
}

func BenchmarkSortPagination(b *testing.B) {
	db := seedBenchDB(b)
	defer db.Close()
	q, _ := NewQuery("indexed", "/* | desc /v | skip 100 limit 50")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := db.ListQuery(q, 0)
		if err != nil {
			b.Fatalf("sort page: %v", err)
		}
		if len(res) != 50 {
			b.Fatalf("unexpected sort page result count: %d", len(res))
		}
	}
	reportThroughput(b, 50)
}

func BenchmarkUpdateDelete(b *testing.B) {
	db := seedBenchDB(b)
	defer db.Close()

	if err := db.WriteTx(func(tx *Tx) error {
		for i := 0; i < b.N; i++ {
			doc := []byte(fmt.Sprintf(`{"v":%d,"name":"m-%d"}`, 1000000+i, i))
			if _, err := tx.PutNew("mut", doc); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatalf("seed mut: %v", err)
	}
	if err := db.EnsureIndex("mut", "/v", IndexInt64, false); err != nil {
		b.Fatalf("ensure mut index: %v", err)
	}

	qUpdate, _ := NewQuery("mut", "/[v = :?] | apply {\"touched\":true}")
	qDelete, _ := NewQuery("mut", "/[v = :?] | del")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := int64(1000000 + i)
		if err := qUpdate.SetI64("", 0, v); err != nil {
			b.Fatalf("bind update: %v", err)
		}
		if _, err := db.UpdateQuery(qUpdate, 0); err != nil {
			b.Fatalf("update: %v", err)
		}
		if err := qDelete.SetI64("", 0, v); err != nil {
			b.Fatalf("bind delete: %v", err)
		}
		if _, err := db.UpdateQuery(qDelete, 0); err != nil {
			b.Fatalf("delete: %v", err)
		}
	}
	reportThroughput(b, 2)
}

func seedBenchDB(b *testing.B) *DB {
	b.Helper()
	db, err := Open(Options{Path: filepath.Join(b.TempDir(), "query.pebble")})
	if err != nil {
		b.Fatalf("open: %v", err)
	}

	if err := db.WriteTx(func(tx *Tx) error {
		for i := 0; i < 10000; i++ {
			doc := []byte(fmt.Sprintf(`{"v":%d,"name":"u-%d"}`, i, i))
			if _, err := tx.PutNew("scan", doc); err != nil {
				return err
			}
			if _, err := tx.PutNew("indexed", doc); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatalf("seed: %v", err)
	}
	if err := db.EnsureIndex("indexed", "/v", IndexInt64, false); err != nil {
		b.Fatalf("ensure index: %v", err)
	}
	return db
}

func reportThroughput(b *testing.B, docsPerOp float64) {
	elapsed := b.Elapsed().Seconds()
	if elapsed <= 0 {
		return
	}
	b.ReportMetric(float64(b.N)/elapsed, "ops/s")
	b.ReportMetric(float64(b.N)*docsPerOp/elapsed, "docs/s")
}
