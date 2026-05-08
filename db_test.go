package ejdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func mustOpen(t *testing.T, path string) *DB {
	t.Helper()
	db, err := Open(Options{Path: path})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return db
}

func mustQuery(t *testing.T, collection, text string) *Query {
	t.Helper()
	q, err := NewQuery(collection, text)
	if err != nil {
		t.Fatalf("new query %q: %v", text, err)
	}
	return q
}

func assertJSONEqual(t *testing.T, got []byte, want string) {
	t.Helper()
	var gv any
	if err := json.Unmarshal(got, &gv); err != nil {
		t.Fatalf("decode got: %v", err)
	}
	var wv any
	if err := json.Unmarshal([]byte(want), &wv); err != nil {
		t.Fatalf("decode want: %v", err)
	}
	if !reflect.DeepEqual(gv, wv) {
		t.Fatalf("json mismatch\n got: %s\nwant: %s", got, want)
	}
}

func assertJQLMatch(t *testing.T, docJSON, query string, want bool) {
	t.Helper()
	q := mustQuery(t, "c1", query)
	var doc any
	if err := json.Unmarshal([]byte(strings.ReplaceAll(docJSON, "'", `"`)), &doc); err != nil {
		t.Fatalf("decode doc: %v", err)
	}
	got, err := q.parsed.filter.match(doc, 22, q, newState())
	if err != nil {
		t.Fatalf("match %q: %v", query, err)
	}
	if got != want {
		t.Fatalf("match %q got=%v want=%v doc=%s", query, got, want, docJSON)
	}
}

func TestPebblePersistenceReopenAndBackup(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "db.pebble")
	db := mustOpen(t, path)

	if err := db.EnsureIndexMode("users", "/email", IdxUnique|IdxString); err != nil {
		t.Fatalf("ensure index: %v", err)
	}
	id, err := db.PutNew("users", []byte(`{"email":"a@example.com","name":"a","age":20}`))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db = mustOpen(t, path)
	got, err := db.Get("users", id)
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	assertJSONEqual(t, got, `{"email":"a@example.com","name":"a","age":20}`)
	if _, err := db.PutNew("users", []byte(`{"email":"a@example.com","name":"dup"}`)); !errors.Is(err, ErrUniqueConstraint) {
		t.Fatalf("expected unique constraint after reopen, got %v", err)
	}

	backup := filepath.Join(root, "backup.pebble")
	if err := db.Backup(backup); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db = mustOpen(t, backup)
	defer db.Close()
	got, err = db.Get("users", id)
	if err != nil {
		t.Fatalf("get from backup: %v", err)
	}
	assertJSONEqual(t, got, `{"email":"a@example.com","name":"a","age":20}`)
}

func TestDocumentsPersistAsEJBLBinary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.pebble")
	db := mustOpen(t, path)

	id, err := db.PutNew("docs", []byte(`{"s":"hello","i":-7,"f":1.5,"b":true,"n":null,"a":[1,"x"],"o":{"k":"v"}}`))
	if err != nil {
		t.Fatal(err)
	}
	stored, err := db.engine.Get(keyDoc("docs", id))
	if err != nil {
		t.Fatalf("read stored doc: %v", err)
	}
	if !isJBLDocument(stored) {
		t.Fatalf("document value is not EJBL binary: %x", stored[:min(len(stored), 8)])
	}
	if json.Valid(stored) {
		t.Fatalf("document value should not be stored as JSON: %s", stored)
	}
	raw, _, err := decodeStoredDocument(stored)
	if err != nil {
		t.Fatalf("decode stored doc: %v", err)
	}
	assertJSONEqual(t, raw, `{"s":"hello","i":-7,"f":1.5,"b":true,"n":null,"a":[1,"x"],"o":{"k":"v"}}`)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = mustOpen(t, path)
	defer db.Close()
	got, err := db.Get("docs", id)
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	assertJSONEqual(t, got, `{"s":"hello","i":-7,"f":1.5,"b":true,"n":null,"a":[1,"x"],"o":{"k":"v"}}`)
}

func TestUniqueIndexConflictRollback(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	if err := db.EnsureIndex("users", "/email", IndexString, true); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PutNew("users", []byte(`{"email":"a@example.com"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PutNew("users", []byte(`{"email":"a@example.com"}`)); !errors.Is(err, ErrUniqueConstraint) {
		t.Fatalf("expected unique constraint, got %v", err)
	}
	q := mustQuery(t, "users", "/*")
	docs, err := db.ListQuery(q, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected rollback to leave one doc, got %d", len(docs))
	}
}

func TestIncrementalUpdateDeleteSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.pebble")
	db := mustOpen(t, path)
	if err := db.EnsureIndex("users", "/email", IndexString, true); err != nil {
		t.Fatal(err)
	}
	id1, err := db.PutNew("users", []byte(`{"email":"a@example.com","name":"a"}`))
	if err != nil {
		t.Fatal(err)
	}
	id2, err := db.PutNew("users", []byte(`{"email":"b@example.com","name":"b"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Patch("users", id1, []byte(`{"name":"alice"}`)); err != nil {
		t.Fatalf("patch: %v", err)
	}
	if err := db.Delete("users", id2); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db = mustOpen(t, path)
	defer db.Close()
	got, err := db.Get("users", id1)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, got, `{"email":"a@example.com","name":"alice"}`)
	if _, err := db.Get("users", id2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected deleted doc to stay deleted, got %v", err)
	}
	if _, err := db.PutNew("users", []byte(`{"email":"a@example.com"}`)); !errors.Is(err, ErrUniqueConstraint) {
		t.Fatalf("expected unique index after reopen, got %v", err)
	}
}

func TestCollectionRenameRemoveAndMergeOrPut(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	if err := db.MergeOrPut("c1", 10, []byte(`{"a":1}`)); err != nil {
		t.Fatalf("merge or put insert: %v", err)
	}
	if err := db.MergeOrPut("c1", 10, []byte(`{"b":2}`)); err != nil {
		t.Fatalf("merge or put update: %v", err)
	}
	got, err := db.Get("c1", 10)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, got, `{"a":1,"b":2}`)

	if err := db.RenameCollection("c1", "c2"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if _, err := db.Get("c2", 10); err != nil {
		t.Fatalf("get renamed: %v", err)
	}
	if err := db.RemoveCollection("c2"); err != nil {
		t.Fatalf("remove collection: %v", err)
	}
	if _, err := db.Get("c2", 10); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found after remove, got %v", err)
	}
}

func TestReadWriteTxIsolationAndRollback(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	id, err := db.PutNew("users", []byte(`{"name":"a"}`))
	if err != nil {
		t.Fatal(err)
	}
	err = db.WriteTx(func(tx *Tx) error {
		if err := tx.Put("users", id, []byte(`{"name":"b"}`)); err != nil {
			return err
		}
		_, _ = tx.PutNew("users", []byte(`{"name":"c"}`))
		return errors.New("rollback")
	})
	if err == nil {
		t.Fatal("expected rollback error")
	}
	got, err := db.Get("users", id)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, got, `{"name":"a"}`)

	if err := db.ReadTx(func(tx *Tx) error {
		_, err := tx.PutNew("users", []byte(`{"name":"no"}`))
		if !errors.Is(err, ErrReadOnlyTx) {
			t.Fatalf("expected read-only tx error, got %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReadTxSnapshotAllowsConcurrentWrite(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	id, err := db.PutNew("users", []byte(`{"name":"before"}`))
	if err != nil {
		t.Fatal(err)
	}
	err = db.ReadTx(func(tx *Tx) error {
		if err := db.Put("users", id, []byte(`{"name":"after"}`)); err != nil {
			return err
		}
		got, err := tx.Get("users", id)
		if err != nil {
			return err
		}
		assertJSONEqual(t, got, `{"name":"before"}`)
		return nil
	})
	if err != nil {
		t.Fatalf("read tx: %v", err)
	}
	got, err := db.Get("users", id)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, got, `{"name":"after"}`)
}

func TestUnsupportedFormatVersionDoesNotClearData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.pebble")
	eng := NewPebbleEngine(nil)
	if err := eng.Open(path); err != nil {
		t.Fatalf("open engine: %v", err)
	}
	oldMeta := []byte(`{"format_version":1,"version":"old","next_collection_id":1,"created_at_unix_nano":1,"collections":{}}`)
	if err := eng.Set(keyMetaState, oldMeta); err != nil {
		t.Fatalf("write old metadata: %v", err)
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	db, err := Open(Options{Path: path})
	if err == nil {
		_ = db.Close()
		t.Fatal("expected incompatible format error")
	}
	if !errors.Is(err, &Error{Code: CodeIncompatibleFormat}) {
		t.Fatalf("expected incompatible format error, got %v", err)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("database directory should not be removed: %v", statErr)
	}
	eng = NewPebbleEngine(nil)
	if err := eng.Open(path); err != nil {
		t.Fatalf("reopen engine: %v", err)
	}
	got, err := eng.Get(keyMetaState)
	if err != nil {
		t.Fatalf("old metadata should remain: %v", err)
	}
	if string(got) != string(oldMeta) {
		t.Fatalf("old metadata changed: %s", got)
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
}

func TestQueryApplyProjectionJoinExecAndDelete(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	artistID, err := db.PutNew("artists", []byte(`{"name":"Leonardo","years":[1452,1519]}`))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = db.PutNew("paintings", []byte(`{"name":"Mona Lisa","artist_ref":1,"year":1490}`))
	_, _ = db.PutNew("paintings", []byte(`{"name":"Madonna","artist_ref":1,"year":1490}`))
	if artistID != 1 {
		t.Fatalf("expected artist id = 1, got %d", artistID)
	}

	qApply := mustQuery(t, "paintings", "/[name = :?] | apply :?")
	_ = qApply.SetString("", 0, "Mona Lisa")
	_ = qApply.SetJSON("", 1, map[string]any{"city": "Florence"})
	if _, err := db.UpdateQuery(qApply, 0); err != nil {
		t.Fatalf("apply: %v", err)
	}

	qJoin := mustQuery(t, "paintings", "/* | /{name, artist_ref<artists} - /artist_ref/years/0")
	docs, err := db.ListQuery(qJoin, 0)
	if err != nil {
		t.Fatalf("projection+join: %v", err)
	}
	if len(docs) != 2 || !strings.Contains(string(docs[0].Raw), "Leonardo") {
		t.Fatalf("unexpected join docs: %+v", docs)
	}

	qExec := mustQuery(t, "paintings", "/[year = 1490] | asc /name")
	var visited []string
	_, err = db.Exec(qExec, &ExecOptions{Visitor: func(doc Document, step *int64) error {
		var m map[string]any
		_ = json.Unmarshal(doc.Raw, &m)
		visited = append(visited, m["name"].(string))
		if len(visited) == 1 {
			*step = 2
		}
		return nil
	}})
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if len(visited) != 1 {
		t.Fatalf("expected visitor skip, got %v", visited)
	}

	qDel := mustQuery(t, "paintings", "/[name = 'Madonna'] | del")
	if _, err := db.UpdateQuery(qDel, 0); err != nil {
		t.Fatalf("delete query: %v", err)
	}
	left, err := db.ListQuery(mustQuery(t, "paintings", "/*"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 1 {
		t.Fatalf("expected one painting left, got %d", len(left))
	}
}

func TestOfficialJQLOrderByOptions(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	fixtures := []string{
		`{"name":"alice-young","firstName":"aa","age":20,"rank":2}`,
		`{"name":"bob","firstName":"bb","age":40,"rank":1}`,
		`{"name":"alice-older","firstName":"aa","age":30,"rank":1}`,
		`{"name":"carol","firstName":"cc","age":25,"rank":3}`,
	}
	for _, doc := range fixtures {
		if _, err := db.PutNew("users", []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}

	names := func(query string, bindPath string) []string {
		t.Helper()
		q := mustQuery(t, "users", query)
		if bindPath != "" {
			if err := q.SetString("", 0, bindPath); err != nil {
				t.Fatal(err)
			}
		}
		docs, err := db.ListQuery(q, 0)
		if err != nil {
			t.Fatalf("list %q: %v", query, err)
		}
		out := make([]string, 0, len(docs))
		for _, doc := range docs {
			var m map[string]any
			if err := json.Unmarshal(doc.Raw, &m); err != nil {
				t.Fatal(err)
			}
			out = append(out, m["name"].(string))
		}
		return out
	}

	if got, want := names("/* | asc /firstName desc /age", ""), []string{"alice-older", "alice-young", "bob", "carol"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("multi order got=%v want=%v", got, want)
	}
	if got, want := names("/* | asc /firstName desc /age skip 1 limit 2", ""), []string{"alice-young", "bob"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("order with skip/limit got=%v want=%v", got, want)
	}
	if got, want := names("/* | asc /firstName /rank", ""), []string{"alice-older", "alice-young", "bob", "carol"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("shared direction order nodes got=%v want=%v", got, want)
	}
	if got, want := names("/* | desc :?", "/age"), []string{"bob", "alice-older", "carol", "alice-young"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("placeholder order got=%v want=%v", got, want)
	}
	q := mustQuery(t, "users", "/* | asc /age skip :? limit :?")
	if err := q.SetI64("", 0, 1); err != nil {
		t.Fatal(err)
	}
	if err := q.SetI64("", 1, 2); err != nil {
		t.Fatal(err)
	}
	docs, err := db.ListQuery(q, 0)
	if err != nil {
		t.Fatalf("placeholder skip/limit: %v", err)
	}
	var got []string
	for _, doc := range docs {
		var m map[string]any
		if err := json.Unmarshal(doc.Raw, &m); err != nil {
			t.Fatal(err)
		}
		got = append(got, m["name"].(string))
	}
	if want := []string{"carol", "alice-older"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("placeholder skip/limit got=%v want=%v", got, want)
	}

	if err := db.EnsureIndexMode("users", "/age", IdxInt64); err != nil {
		t.Fatal(err)
	}
	var indexedLog strings.Builder
	if _, err := db.Exec(mustQuery(t, "users", "/* | asc /age"), &ExecOptions{Log: &indexedLog}); err != nil {
		t.Fatalf("indexed order exec: %v", err)
	}
	if log := indexedLog.String(); !strings.Contains(log, "[INDEX] SELECTED") || !strings.Contains(log, "ORDERBY") || strings.Contains(log, "[COLLECTOR] SORTER") {
		t.Fatalf("expected index order-by plan without sorter, got log:\n%s", log)
	}
	var noidxLog strings.Builder
	if _, err := db.Exec(mustQuery(t, "users", "/* | asc /age noidx"), &ExecOptions{Log: &noidxLog}); err != nil {
		t.Fatalf("noidx order exec: %v", err)
	}
	if log := noidxLog.String(); !strings.Contains(log, "[COLLECTOR] SORTER") {
		t.Fatalf("expected sorter with noidx, got log:\n%s", log)
	}
}

func TestOfficialJQLSorterOverflow(t *testing.T) {
	db, err := Open(Options{Path: filepath.Join(t.TempDir(), "db.pebble"), SortBufferSize: 1024})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	payload := strings.Repeat("x", 2048)
	fixtures := []string{
		`{"name":"third","rank":3,"payload":"` + payload + `"}`,
		`{"name":"first","rank":1,"payload":"` + payload + `"}`,
		`{"name":"second","rank":2,"payload":"` + payload + `"}`,
	}
	for _, doc := range fixtures {
		if _, err := db.PutNew("docs", []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}

	docs, err := db.ListQuery(mustQuery(t, "docs", "/* | asc /rank"), 0)
	if err != nil {
		t.Fatalf("list with overflow sorter: %v", err)
	}
	var got []string
	for _, doc := range docs {
		var m map[string]any
		if err := json.Unmarshal(doc.Raw, &m); err != nil {
			t.Fatal(err)
		}
		got = append(got, m["name"].(string))
	}
	if want := []string{"first", "second", "third"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("overflow sorter got=%v want=%v", got, want)
	}

	var log strings.Builder
	if _, err := db.Exec(mustQuery(t, "docs", "/* | asc /rank"), &ExecOptions{Log: &log}); err != nil {
		t.Fatalf("exec with overflow sorter: %v", err)
	}
	if !strings.Contains(log.String(), "[SORTER] OVERFLOW") {
		t.Fatalf("expected overflow sorter log, got:\n%s", log.String())
	}
}

func TestOfficialPlannerRangePrefixAndComparison(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	if err := db.EnsureIndexMode("nums", "/v", IdxInt64); err != nil {
		t.Fatal(err)
	}
	for _, v := range []int{-2, -1, 0, 1, 2} {
		doc := []byte(fmt.Sprintf(`{"v":%d}`, v))
		if _, err := db.PutNew("nums", doc); err != nil {
			t.Fatal(err)
		}
	}
	var rangeLog strings.Builder
	docs, err := db.ListQuery(mustQuery(t, "nums", "/[v >= -1 and v < 2] | asc /v"), 0)
	if err != nil {
		t.Fatalf("range list: %v", err)
	}
	if _, err := db.Exec(mustQuery(t, "nums", "/[v >= -1 and v < 2] | asc /v"), &ExecOptions{Log: &rangeLog}); err != nil {
		t.Fatalf("range explain: %v", err)
	}
	if log := rangeLog.String(); !strings.Contains(log, "[INDEX] SELECTED") || strings.Contains(log, "[COLLECTOR] SORTER") {
		t.Fatalf("expected indexed range/order plan, got:\n%s", log)
	}
	var nums []int
	for _, doc := range docs {
		var m map[string]any
		if err := json.Unmarshal(doc.Raw, &m); err != nil {
			t.Fatal(err)
		}
		nums = append(nums, int(m["v"].(float64)))
	}
	if want := []int{-1, 0, 1}; !reflect.DeepEqual(nums, want) {
		t.Fatalf("range docs got=%v want=%v", nums, want)
	}
	for _, rt := range db.state.Collections["nums"].runtime {
		rt.unique = map[string]int64{}
		rt.multi = map[string]map[int64]struct{}{}
	}
	docs, err = db.ListQuery(mustQuery(t, "nums", "/[v >= -1 and v < 2] | asc /v"), 0)
	if err != nil {
		t.Fatalf("range list after runtime cache clear: %v", err)
	}
	nums = nums[:0]
	for _, doc := range docs {
		var m map[string]any
		if err := json.Unmarshal(doc.Raw, &m); err != nil {
			t.Fatal(err)
		}
		nums = append(nums, int(m["v"].(float64)))
	}
	if want := []int{-1, 0, 1}; !reflect.DeepEqual(nums, want) {
		t.Fatalf("pebble iterator range got=%v want=%v", nums, want)
	}

	if err := db.EnsureIndexMode("names", "/lastName", IdxString); err != nil {
		t.Fatal(err)
	}
	for _, doc := range []string{
		`{"lastName":"Doe"}`,
		`{"lastName":"Doll"}`,
		`{"lastName":"Smith"}`,
	} {
		if _, err := db.PutNew("names", []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	var prefixLog strings.Builder
	prefixDocs, err := db.ListQuery(mustQuery(t, "names", "/[lastName ~ 'Do'] | asc /lastName"), 0)
	if err != nil {
		t.Fatalf("prefix list: %v", err)
	}
	if _, err := db.Exec(mustQuery(t, "names", "/[lastName ~ 'Do'] | asc /lastName"), &ExecOptions{Log: &prefixLog}); err != nil {
		t.Fatalf("prefix explain: %v", err)
	}
	if log := prefixLog.String(); !strings.Contains(log, "[INDEX] SELECTED") {
		t.Fatalf("expected indexed prefix plan, got:\n%s", log)
	}
	if len(prefixDocs) != 2 {
		t.Fatalf("expected two prefix docs, got %d", len(prefixDocs))
	}

	for _, doc := range []string{
		`{"s":"bbb"}`,
		`{"s":"aa"}`,
		`{"s":"c"}`,
	} {
		if _, err := db.PutNew("strings", []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	sdocs, err := db.ListQuery(mustQuery(t, "strings", "/* | asc /s"), 0)
	if err != nil {
		t.Fatalf("string order list: %v", err)
	}
	var got []string
	for _, doc := range sdocs {
		var m map[string]any
		if err := json.Unmarshal(doc.Raw, &m); err != nil {
			t.Fatal(err)
		}
		got = append(got, m["s"].(string))
	}
	if want := []string{"c", "aa", "bbb"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("official string order got=%v want=%v", got, want)
	}
}

func TestOfficialJQLParserErrors(t *testing.T) {
	if _, err := NewQuery("c", "/* | skip 1 skip 2"); !errors.Is(err, &Error{Code: CodeSkipAlreadySet}) {
		t.Fatalf("expected duplicate skip error, got %v", err)
	}
	if _, err := NewQuery("c", "/* | limit 1 limit 2"); !errors.Is(err, &Error{Code: CodeLimitAlreadySet}) {
		t.Fatalf("expected duplicate limit error, got %v", err)
	}
}

func TestOfficialJQLPlaceholderInArrayTypes(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	if err := db.EnsureIndexMode("items", "/v", IdxInt64); err != nil {
		t.Fatal(err)
	}
	for _, v := range []int{1, 2, 3} {
		if _, err := db.PutNew("items", []byte(fmt.Sprintf(`{"v":%d}`, v))); err != nil {
			t.Fatal(err)
		}
	}
	q := mustQuery(t, "items", "/[v in :?] | asc /v")
	if err := q.SetJSON("", 0, []int64{1, 3}); err != nil {
		t.Fatal(err)
	}
	docs, err := db.ListQuery(q, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var got []int
	for _, doc := range docs {
		var m map[string]any
		if err := json.Unmarshal(doc.Raw, &m); err != nil {
			t.Fatal(err)
		}
		got = append(got, int(m["v"].(float64)))
	}
	if want := []int{1, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("placeholder in got=%v want=%v", got, want)
	}
}

func TestOfficialJQLCoreMatchingCases(t *testing.T) {
	assertJQLMatch(t, "{}", "/*", true)
	assertJQLMatch(t, "{}", "/**", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/bar", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/baz", false)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/bar and /foo/bar or /foo", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "(/boo or /foo) and (/foo/daz or /foo/bar)", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar eq 22]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar !eq 22]", false)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/*/[bar > 20 and bar <= 23]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/*/[bar > 22 and bar <= 23]", false)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/[* re ^foo$]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/[* not re ^fo$]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar in [21, \"22\"]]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar in [21, 23]]", false)
	assertJQLMatch(t, "{'tags':['bar', 'foo']}", "/tags/[** in [\"bar\", \"baz\"]]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/**/bar", true)
	assertJQLMatch(t, "{'foo':{'bar':22, 'baz':{'zaz':33}}}", "/foo/**/[zaz > 30]", true)
	assertJQLMatch(t, "{'foo':[1,2]}", "/[foo = [1,2]]", true)
	assertJQLMatch(t, "{'foo':[1,2]}", "/[foo ni 2]", true)
	assertJQLMatch(t, "{'foo':{'obj':{'f':'d','e':'j'}}}", "/foo/[obj = {\"e\":\"j\",\"f\":\"d\"}]", true)
	assertJQLMatch(t, "{'f':22}", "/=22", true)
	assertJQLMatch(t, "{'f':22}", "@mycoll/=22", true)
	assertJQLMatch(t, "{'lastName':'Doe'}", "/[lastName ~ 'Do']", true)
	assertJQLMatch(t, "{'lastName':'Smith'}", "/[lastName ~ 'Do']", false)
	assertJQLMatch(t, "{'foo bar':22}", `/"foo bar"`, true)
	assertJQLMatch(t, "{'foo/bar':22}", `/foo\/bar`, true)
	assertJQLMatch(t, "{'snowman ☃':22}", `/snowman\u0020\u2603`, true)
}
