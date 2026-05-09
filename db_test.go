package ejdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	json "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
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

func TestOfficialEmbeddedMetaIndexRemoveAndPatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.pebble")
	db := mustOpen(t, path)
	if err := db.EnsureCollection("foo"); err != nil {
		t.Fatal(err)
	}
	meta, err := db.Meta()
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Collections) != 1 || meta.Collections[0].Name != "foo" || meta.File != path {
		t.Fatalf("unexpected meta after ensure collection: %+v", meta)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = mustOpen(t, path)
	if err := db.RemoveCollection("foo"); err != nil {
		t.Fatal(err)
	}
	meta, err = db.Meta()
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Collections) != 0 {
		t.Fatalf("expected no collections after remove, got %+v", meta.Collections)
	}

	id, err := db.PutNew("foocoll", []byte(`{"foo":"bar"}`))
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.Get("foocoll", id)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, got, `{"foo":"bar"}`)
	if err := db.Delete("foocoll", id); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get("foocoll", id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found after delete, got %v", err)
	}

	if err := db.EnsureIndexMode("col1", "/foo/bar", IdxUnique|IdxInt64); err != nil {
		t.Fatal(err)
	}
	if err := db.EnsureIndexMode("col1", "/foo/baz", IdxString); err != nil {
		t.Fatal(err)
	}
	if err := db.EnsureIndexMode("col1", "/foo/gaz", IdxUnique|IdxString); err != nil {
		t.Fatal(err)
	}
	meta, err = db.Meta()
	if err != nil {
		t.Fatal(err)
	}
	col1 := findMetaCollection(meta, "col1")
	if col1 == nil || len(col1.Indexes) != 3 {
		t.Fatalf("expected three indexes in meta, got %+v", col1)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = mustOpen(t, path)
	if err := db.RemoveIndexMode("col1", "/foo/baz", IdxString); err != nil {
		t.Fatal(err)
	}
	if err := db.RemoveIndexMode("col1", "/foo/gaz", IdxUnique|IdxString); err != nil {
		t.Fatal(err)
	}
	meta, err = db.Meta()
	if err != nil {
		t.Fatal(err)
	}
	col1 = findMetaCollection(meta, "col1")
	if col1 == nil || len(col1.Indexes) != 1 || col1.Indexes[0].Path != "/foo/bar" {
		t.Fatalf("expected one remaining index, got %+v", col1)
	}

	patchID, err := db.PutNew("c1", []byte(`{"foo":"bar"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Patch("c1", patchID, []byte(`[{"op":"add","path":"/baz","value":"qux"}]`)); err != nil {
		t.Fatal(err)
	}
	got, err = db.Get("c1", patchID)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, got, `{"foo":"bar","baz":"qux"}`)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func findMetaCollection(meta Meta, name string) *CollectionMeta {
	for i := range meta.Collections {
		if meta.Collections[i].Name == name {
			return &meta.Collections[i]
		}
	}
	return nil
}

func TestDocumentsPersistAsJBLBinnBinary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.pebble")
	db := mustOpen(t, path)

	id, err := db.PutNew("docs", []byte(`{"s":"hello","i":-7,"f":1.5,"b":true,"n":null,"a":[1,"x"],"o":{"k":"v"}}`))
	if err != nil {
		t.Fatal(err)
	}
	stored, err := db.engine.Get(keyDoc(db.state.Collections["docs"].DBID, id))
	if err != nil {
		t.Fatalf("read stored doc: %v", err)
	}
	if !isJBLDocument(stored) {
		t.Fatalf("document value is not JBL/Binn binary: %x", stored[:min(len(stored), 8)])
	}
	if stored[0] != binnObject {
		t.Fatalf("expected JBL/Binn object root, got 0x%x", stored[0])
	}
	if jsontext.Value(stored).IsValid(jsontext.AllowDuplicateNames(true), jsontext.AllowInvalidUTF8(true)) {
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

func TestJBLBinnArrayRootAndLargeContainer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.pebble")
	db := mustOpen(t, path)
	defer db.Close()

	arrID, err := db.PutNew("docs", []byte(`[1,true,null,{"k":"v"}]`))
	if err != nil {
		t.Fatal(err)
	}
	stored, err := db.engine.Get(keyDoc(db.state.Collections["docs"].DBID, arrID))
	if err != nil {
		t.Fatal(err)
	}
	if !isJBLDocument(stored) || stored[0] != binnList {
		t.Fatalf("expected JBL/Binn list root, got %x", stored[:min(len(stored), 8)])
	}
	raw, _, err := decodeStoredDocument(stored)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, raw, `[1,true,null,{"k":"v"}]`)

	big := make(map[string]any, 80)
	for i := 0; i < 80; i++ {
		big[fmt.Sprintf("k%02d", i)] = strings.Repeat("x", 8)
	}
	bigRaw, err := json.Marshal(big)
	if err != nil {
		t.Fatal(err)
	}
	bigID, err := db.PutNew("docs", bigRaw)
	if err != nil {
		t.Fatal(err)
	}
	stored, err = db.engine.Get(keyDoc(db.state.Collections["docs"].DBID, bigID))
	if err != nil {
		t.Fatal(err)
	}
	if !isJBLDocument(stored) || stored[0] != binnObject {
		t.Fatalf("expected large JBL/Binn object root, got %x", stored[:min(len(stored), 8)])
	}
	if _, size, count, header, ok := binnHeader(stored); !ok || size != len(stored) || count != len(big) || header <= 3 {
		t.Fatalf("bad large container header size=%d len=%d count=%d header=%d ok=%v", size, len(stored), count, header, ok)
	}
	raw, _, err = decodeStoredDocument(stored)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, raw, string(bigRaw))

	tooLongKey := map[string]any{strings.Repeat("x", maxBinnObjectKeyLen+1): "nope"}
	tooLongRaw, err := json.Marshal(tooLongKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.PutNew("docs", tooLongRaw); !errors.Is(err, &Error{Code: CodeInvalidValueType}) {
		t.Fatalf("expected official Binn object key length error, got %v", err)
	}
}

func TestJBLBinnOfficialTypeCoverage(t *testing.T) {
	values := []struct {
		name string
		val  any
		want any
	}{
		{"null", nil, nil},
		{"true", true, true},
		{"false", false, false},
		{"int8", int8(-7), jsonNumber("-7")},
		{"uint8", uint8(7), jsonNumber("7")},
		{"int16", int16(-32000), jsonNumber("-32000")},
		{"uint16", uint16(65000), jsonNumber("65000")},
		{"int32", int32(-70000), jsonNumber("-70000")},
		{"uint32", uint32(70000), jsonNumber("70000")},
		{"int64", int64(-5000000000), jsonNumber("-5000000000")},
		{"float32", float32(1.25), jsonNumber("1.25")},
		{"float64", 2.5, jsonNumber("2.5")},
		{"string", "hello", "hello"},
		{"datetime", binnTypedValue{Type: int(binnDateTime), Value: "2026-05-09 10:11:12"}, "2026-05-09 10:11:12"},
		{"date", binnTypedValue{Type: int(binnDate), Value: "2026-05-09"}, "2026-05-09"},
		{"time", binnTypedValue{Type: int(binnTime), Value: "10:11:12"}, "10:11:12"},
		{"decimal", binnTypedValue{Type: int(binnDecimal), Value: "1234567890.0123"}, "1234567890.0123"},
		{"currency-string", binnTypedValue{Type: int(binnCurrencyStr), Value: "USD 12.34"}, "USD 12.34"},
		{"single-string", binnTypedValue{Type: int(binnSingleStr), Value: "1.25"}, "1.25"},
		{"double-string", binnTypedValue{Type: int(binnDoubleStr), Value: "2.5"}, "2.5"},
		{"html", binnTypedValue{Type: binnHTML, Value: "<b>x</b>"}, "<b>x</b>"},
		{"xml", binnTypedValue{Type: binnXML, Value: "<x/>"}, "<x/>"},
		{"json", binnTypedValue{Type: binnJSON, Value: `{"x":1}`}, `{"x":1}`},
		{"javascript", binnTypedValue{Type: binnJavaScript, Value: "return 1"}, "return 1"},
		{"css", binnTypedValue{Type: binnCSS, Value: "body{}"}, "body{}"},
		{"blob", binnBlobValue{0x01, 0x02}, []byte{0x01, 0x02}},
		{"jpeg", binnTypedValue{Type: binnJPEG, Value: []byte{0xff, 0xd8}}, []byte{0xff, 0xd8}},
		{"gif", binnTypedValue{Type: binnGIF, Value: []byte("GIF")}, []byte("GIF")},
		{"png", binnTypedValue{Type: binnPNG, Value: []byte{0x89, 'P'}}, []byte{0x89, 'P'}},
		{"bmp", binnTypedValue{Type: binnBMP, Value: []byte("BM")}, []byte("BM")},
	}
	for _, tc := range values {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := binnEncodeValue(tc.val)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			dec := binnDecoder{buf: raw}
			got, err := dec.value()
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if dec.off != len(raw) {
				t.Fatalf("trailing bytes off=%d len=%d", dec.off, len(raw))
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v want %#v raw=%x", got, tc.want, raw)
			}
		})
	}

	mapPayload := make([]byte, 0)
	var id [4]byte
	binary.BigEndian.PutUint32(id[:], 42)
	mapPayload = append(mapPayload, id[:]...)
	item, err := binnEncodeValue("answer")
	if err != nil {
		t.Fatal(err)
	}
	mapPayload = append(mapPayload, item...)
	raw := binnContainer(int(binnMap), 1, mapPayload)
	if !isJBLDocument(raw) {
		t.Fatalf("expected Binn map to be a JBL document")
	}
	gotRaw, got, err := decodeStoredDocument(raw)
	if err != nil {
		t.Fatalf("decode map: %v", err)
	}
	if !reflect.DeepEqual(got, map[string]any{"42": "answer"}) {
		t.Fatalf("map got %#v", got)
	}
	assertJSONEqual(t, gotRaw, `{"42":"answer"}`)
}

func TestOfficialJQLParserFixtureInputs(t *testing.T) {
	fixtures := []struct {
		in   string
		want string
	}{
		{`/foo/bar and /foo/baz`, `/foo/bar and /foo/baz`},
		{`@one/**/[familyName re "D\n.*"] 
and /**/family/mother/[age > 30 and age <= 40 or name re "Grace.*"] 
and not /bar/"ba z\"zz" 
| apply {"foo":"bar", "nums": [1,2,3,4,5]} 
| all - /**/author/{givenName,familyName}`, `@one/**/[familyName re "D\n.*"] and /**/family/mother/[age > 30 and age <= 40 or name re "Grace.*"] and not /bar/ba z"zz
| apply {"foo":"bar","nums":[1,2,3,4,5]}
| all - /**/author/{givenName,familyName}`},
		{`/foo/bar`, `/foo/bar`},
		{`/"foo"/"b ar"`, `/foo/b ar`},
		{`/foo and /bar`, `/foo and /bar`},
		{`/foo/[bar = "val"]`, `/foo/[bar = "val"]`},
		{`/foo/[bar = :placeholder]`, `/foo/[bar = :placeholder]`},
		{`/foo/[bar = :? and "baz" = :?] or /root/**/[fname not re "John"]`, `/foo/[bar = :? and "baz" = :?] or /root/**/[fname not re "John"]`},
		{`/foo/[bar = [1, 2,3,{}, {"foo": "bar"}]]`, `/foo/[bar = [1,2,3,{},{"foo":"bar"}]]`},
		{`/tags/[* in ["sample", "foo"] and * re "ta.*"]`, `/tags/[* in ["sample","foo"] and * re "ta.*"]`},
		{`/**/[[* = "familyName"] = "Doe"]`, `/**/[[* = "familyName"] = "Doe"]`},
		{`(/foo/bar)`, `(/foo/bar)`},
		{`/foo/bar and (/foo/baz or /foo/gaz)`, `/foo/bar and (/foo/baz or /foo/gaz)`},
		{`/foo/bar and (/foo/baz or (/foo/gaz and /foo/daz) and (/foo/sss or /foo/vvv))`, `/foo/bar and (/foo/baz or (/foo/gaz and /foo/daz) and (/foo/sss or /foo/vvv))`},
		{`/foo/bar | skip 10`, "/foo/bar\n| skip 10"},
		{`/foo/bar | skip :numskip limit 1000`, "/foo/bar\n| skip :numskip limit 1000"},
		{`/foo/bar | apply {"foo":"bar","nums":[1,2,3,4,5]}
| all - /**/author/{givenName,familyName}
| asc /foo/bar desc :myfield
  skip :numskip limit 1000`, "/foo/bar\n| apply {\"foo\":\"bar\",\"nums\":[1,2,3,4,5]}\n| all - /**/author/{givenName,familyName}\n| asc /foo/bar\n  desc :myfield\n  skip :numskip limit 1000"},
		{`/= :? | apply {"foo":"bar","nums":[1,2,3,4,5]}`, "/=:?\n| apply {\"foo\":\"bar\",\"nums\":[1,2,3,4,5]}"},
	}
	fixtures = append(fixtures,
		struct{ in, want string }{`@users/= :id | apply {"foo":"bar","nums":[1,2,3,4,5]}`, "@users/=:id\n| apply {\"foo\":\"bar\",\"nums\":[1,2,3,4,5]}"},
		struct{ in, want string }{`@users/=122`, `@users/=122`},
	)
	for _, tc := range fixtures {
		q, err := NewQuery("c1", tc.in)
		if err != nil {
			t.Fatalf("expected official fixture to parse: %q: %v", tc.in, err)
		}
		if got := q.Canonical(); got != tc.want {
			t.Fatalf("canonical mismatch\ninput: %q\n got: %q\nwant: %q", tc.in, got, tc.want)
		}
	}

	invalid := []string{
		`/foo/[barlike22]`,
		`/foo/\"bar`,
		`foo/bar`,
	}
	for _, q := range invalid {
		if _, err := NewQuery("c1", q); err == nil {
			t.Fatalf("expected official invalid fixture to fail: %q", q)
		}
	}
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

func TestDiskBackedStateDoesNotCacheDocuments(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	for i := 0; i < 50; i++ {
		if _, err := db.PutNew("docs", []byte(fmt.Sprintf(`{"v":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	col := db.state.Collections["docs"]
	if col == nil || col.RNum != 50 {
		t.Fatalf("unexpected collection state: %+v", col)
	}
	if _, ok := reflect.TypeOf(*col).FieldByName("Docs"); ok {
		t.Fatal("collectionState must not keep a Docs cache")
	}
}

func TestWriteTxReadYourWrites(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	if err := db.WriteTx(func(tx *Tx) error {
		id, err := tx.PutNew("users", []byte(`{"name":"inside"}`))
		if err != nil {
			return err
		}
		got, err := tx.Get("users", id)
		if err != nil {
			return err
		}
		assertJSONEqual(t, got, `{"name":"inside"}`)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestWriteTxUniqueConstraintSeesPendingWrites(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	if err := db.EnsureIndexMode("users", "/email", IdxUnique|IdxString); err != nil {
		t.Fatal(err)
	}
	err := db.WriteTx(func(tx *Tx) error {
		if _, err := tx.PutNew("users", []byte(`{"email":"a@example.com"}`)); err != nil {
			return err
		}
		_, err := tx.PutNew("users", []byte(`{"email":"a@example.com"}`))
		return err
	})
	if !errors.Is(err, ErrUniqueConstraint) {
		t.Fatalf("expected pending unique conflict, got %v", err)
	}
	if meta, err := db.Meta(); err != nil {
		t.Fatal(err)
	} else if col := findMetaCollection(meta, "users"); col == nil || col.RNum != 0 {
		t.Fatalf("expected rollback to keep users empty, meta=%+v", meta)
	}
}

func TestWriteTxQuerySeesBaseAndPendingWrites(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	if _, err := db.PutNew("users", []byte(`{"name":"base"}`)); err != nil {
		t.Fatal(err)
	}
	if err := db.WriteTx(func(tx *Tx) error {
		if _, err := tx.PutNew("users", []byte(`{"name":"pending"}`)); err != nil {
			return err
		}
		docs, err := tx.ListQuery(mustQuery(t, "users", "/* | asc /name"), 0)
		if err != nil {
			return err
		}
		if len(docs) != 2 {
			t.Fatalf("expected base and pending docs, got %+v", docs)
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

func TestOfficialExecVisitorBackForwardSteps(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	for _, doc := range []string{
		`{"f":2}`,
		`{"f":1}`,
		`{"f":3}`,
		`{"a":"foo"}`,
		`{"a":"gaz"}`,
		`{"a":"bar"}`,
		`{"f":5}`,
		`{"f":6}`,
	} {
		if _, err := db.PutNew("a", []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	run := func(query string) string {
		t.Helper()
		var out strings.Builder
		stage := 0
		cnt := 0
		_, err := db.Exec(mustQuery(t, "a", query), &ExecOptions{Visitor: func(doc Document, step *int64) error {
			var m map[string]any
			if err := json.Unmarshal(doc.Raw, &m); err != nil {
				return err
			}
			if cnt > 0 && stage == 0 {
				stage = 1
				*step = 2
			} else if stage == 1 {
				stage = 2
				*step = -1
			}
			out.WriteString(strconv.Itoa(int(m["f"].(float64))))
			cnt++
			return nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	if got := run("/f"); got != "65112" {
		t.Fatalf("natural visitor stepping got=%q want=65112", got)
	}
	if got := run("/f | asc /f"); got != "12556" {
		t.Fatalf("sorted visitor stepping got=%q want=12556", got)
	}
}

func TestOfficialEmbeddedListSkipLimitAndRangeOrder(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	for _, doc := range []string{
		`{"f":2}`,
		`{"f":1}`,
		`{"f":3}`,
		`{"a":"foo"}`,
		`{"a":"gaz"}`,
		`{"a":"bar"}`,
	} {
		if _, err := db.PutNew("a", []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	listRaw := func(collection, query string, limit int64) []string {
		t.Helper()
		docs, err := db.ListQuery(mustQuery(t, collection, query), limit)
		if err != nil {
			t.Fatalf("list %q: %v", query, err)
		}
		out := make([]string, 0, len(docs))
		for _, doc := range docs {
			out = append(out, string(doc.Raw))
		}
		return out
	}
	if got := listRaw("not_exists", "/*", 0); len(got) != 0 {
		t.Fatalf("missing collection got %v", got)
	}
	if got, want := listRaw("a", "/*", 0), []string{`{"a":"bar"}`, `{"a":"gaz"}`, `{"a":"foo"}`, `{"f":3}`, `{"f":1}`, `{"f":2}`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("natural list got=%v want=%v", got, want)
	}
	if got := listRaw("a", "/*", 1); len(got) != 1 {
		t.Fatalf("limit override got %d", len(got))
	}
	if got := listRaw("a", "/f", 0); len(got) != 3 {
		t.Fatalf("path match count got %d", len(got))
	}
	if got, want := listRaw("a", "/* | skip 1", 0)[0], `{"a":"gaz"}`; got != want {
		t.Fatalf("skip first got=%s want=%s", got, want)
	}
	if got, want := listRaw("a", "/* | skip 2 limit 3", 0), []string{`{"a":"foo"}`, `{"f":3}`, `{"f":1}`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("skip limit got=%v want=%v", got, want)
	}
	for _, doc := range []string{`{"f":5}`, `{"f":6}`} {
		if _, err := db.PutNew("a", []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := listRaw("a", "/f | asc /f", 0), []string{`{"f":1}`, `{"f":2}`, `{"f":3}`, `{"f":5}`, `{"f":6}`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("asc /f got=%v want=%v", got, want)
	}
	if got, want := listRaw("a", "/f | desc /f", 0), []string{`{"f":6}`, `{"f":5}`, `{"f":3}`, `{"f":2}`, `{"f":1}`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("desc /f got=%v want=%v", got, want)
	}

	if err := db.EnsureIndexMode("c1", "/f/b", IdxUnique|IdxInt64); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 10; i++ {
		if _, err := db.PutNew("c1", []byte(fmt.Sprintf(`{"f":{"b":%d},"n":%d}`, i, i))); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := collectN(t, db, "c1", "/f/[b > 1]", 0), []int{2, 3, 4, 5, 6, 7, 8, 9, 10}; !reflect.DeepEqual(got, want) {
		t.Fatalf("indexed > order got=%v want=%v", got, want)
	}
	if got, want := collectN(t, db, "c1", "/f/[b < 9]", 0), []int{8, 7, 6, 5, 4, 3, 2, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("indexed < order got=%v want=%v", got, want)
	}
	if got, want := collectN(t, db, "c1", "/f/[b < 11 and b >= 4]", 0), []int{4, 5, 6, 7, 8, 9, 10}; !reflect.DeepEqual(got, want) {
		t.Fatalf("indexed merged range got=%v want=%v", got, want)
	}
	if got, want := collectN(t, db, "c1", "/f/[b > 2 and b < 10] | desc /f/b", 0), []int{9, 8, 7, 6, 5, 4, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("indexed desc range got=%v want=%v", got, want)
	}
}

func collectN(t *testing.T, db *DB, collection, query string, limit int64) []int {
	t.Helper()
	docs, err := db.ListQuery(mustQuery(t, collection, query), limit)
	if err != nil {
		t.Fatalf("list %q: %v", query, err)
	}
	out := make([]int, 0, len(docs))
	for _, doc := range docs {
		var m map[string]any
		if err := json.Unmarshal(doc.Raw, &m); err != nil {
			t.Fatal(err)
		}
		out = append(out, int(m["n"].(float64)))
	}
	return out
}

func TestIndexedOrderPaginationWindow(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	if err := db.EnsureIndexMode("page", "/v", IdxInt64); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		if _, err := db.PutNew("page", []byte(fmt.Sprintf(`{"v":%d,"n":%d}`, i, i))); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := collectN(t, db, "page", "/* | asc /v | skip 17 limit 9", 0), []int{17, 18, 19, 20, 21, 22, 23, 24, 25}; !reflect.DeepEqual(got, want) {
		t.Fatalf("asc indexed page got=%v want=%v", got, want)
	}
	if got, want := collectN(t, db, "page", "/* | desc /v | skip 17 limit 9", 0), []int{182, 181, 180, 179, 178, 177, 176, 175, 174}; !reflect.DeepEqual(got, want) {
		t.Fatalf("desc indexed page got=%v want=%v", got, want)
	}
}

func TestIndexedFloatRangeBounds(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	if err := db.EnsureIndexMode("float_unique", "/v", IdxUnique|IdxFloat); err != nil {
		t.Fatal(err)
	}
	for i, v := range []float64{-2.5, -1, 0, 1.25, 2.5, 4} {
		if _, err := db.PutNew("float_unique", []byte(fmt.Sprintf(`{"v":%g,"n":%d}`, v, i))); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := collectN(t, db, "float_unique", "/[v > -1 and v <= 2.5] | asc /v", 0), []int{2, 3, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unique float range got=%v want=%v", got, want)
	}
	if got, want := collectN(t, db, "float_unique", "/[v >= -1 and v < 4] | desc /v", 0), []int{4, 3, 2, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unique float desc range got=%v want=%v", got, want)
	}

	if err := db.EnsureIndexMode("float_multi", "/v", IdxFloat); err != nil {
		t.Fatal(err)
	}
	for i, v := range []float64{1.5, 2.5, 1.5, 3.5, 2.5} {
		if _, err := db.PutNew("float_multi", []byte(fmt.Sprintf(`{"v":%g,"n":%d}`, v, i+1))); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := collectN(t, db, "float_multi", "/[v >= 1.5 and v < 3] | asc /v", 0), []int{1, 3, 2, 5}; !reflect.DeepEqual(got, want) {
		t.Fatalf("non-unique float range got=%v want=%v", got, want)
	}
}

func TestConcurrentReadQueryAndWriteMutation(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	if err := db.EnsureIndexMode("concurrent", "/v", IdxInt64); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := db.PutNew("concurrent", []byte(fmt.Sprintf(`{"v":%d,"n":%d}`, i, i))); err != nil {
			t.Fatal(err)
		}
	}

	errCh := make(chan error, 16)
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if _, err := db.Get("concurrent", int64((i+offset)%100+1)); err != nil && !errors.Is(err, ErrNotFound) {
					errCh <- err
					return
				}
				q, err := NewQuery("concurrent", "/[v >= 10 and v < 90] | asc /v | skip 3 limit 7")
				if err != nil {
					errCh <- err
					return
				}
				docs, err := db.ListQuery(q, 0)
				if err != nil {
					errCh <- err
					return
				}
				for _, doc := range docs {
					var m map[string]any
					if err := json.Unmarshal(doc.Raw, &m); err != nil {
						errCh <- err
						return
					}
				}
				qCount, err := NewQuery("concurrent", "/[v >= 0]")
				if err != nil {
					errCh <- err
					return
				}
				if _, err := db.Count(qCount, 0); err != nil {
					errCh <- err
					return
				}
			}
		}(r)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			if err := db.Put("concurrent", int64(i%100+1), []byte(fmt.Sprintf(`{"v":%d,"n":%d}`, i%100, i))); err != nil {
				errCh <- err
				return
			}
			id, err := db.PutNew("concurrent", []byte(fmt.Sprintf(`{"v":%d,"n":%d}`, 1000+i, 1000+i)))
			if err != nil {
				errCh <- err
				return
			}
			q, err := NewQuery("concurrent", "/[v = :?] | apply {\"seen\":true}")
			if err != nil {
				errCh <- err
				return
			}
			if err := q.SetI64("", 0, int64(1000+i)); err != nil {
				errCh <- err
				return
			}
			if _, err := db.UpdateQuery(q, 0); err != nil {
				errCh <- err
				return
			}
			if err := db.Delete("concurrent", id); err != nil {
				errCh <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
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
	if log := rangeLog.String(); !strings.Contains(log, "EXPR2") {
		t.Fatalf("expected merged range index plan with EXPR2, got:\n%s", log)
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

func TestOfficialEmbeddedNonUniqueArrayPKDeleteRegexpAndRename(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	if err := db.EnsureIndexMode("a1", "/f/b", IdxInt64); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 10; i++ {
		v := 127
		if i%2 == 1 {
			v = 0xffffff
		}
		if _, err := db.PutNew("a1", []byte(fmt.Sprintf(`{"f":{"b":%d},"n":%d}`, v, i))); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range []int{11, 12} {
		if _, err := db.PutNew("a1", []byte(fmt.Sprintf(`{"f":{"b":126},"n":%d}`, n))); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := collectN(t, db, "a1", "/f/[b > 127]", 0), []int{1, 3, 5, 7, 9}; !reflect.DeepEqual(got, want) {
		t.Fatalf("non-unique gt got=%v want=%v", got, want)
	}
	if got, want := collectN(t, db, "a1", "/f/[b < 127]", 0), []int{12, 11}; !reflect.DeepEqual(got, want) {
		t.Fatalf("non-unique lt got=%v want=%v", got, want)
	}
	if got, want := collectN(t, db, "a1", "/f/[b = 127]", 0), []int{2, 4, 6, 8, 10}; !reflect.DeepEqual(got, want) {
		t.Fatalf("non-unique eq got=%v want=%v", got, want)
	}
	if got, want := collectN(t, db, "a1", "/f/[b in [333, 16777215, 127, 16777216]]", 0), []int{1, 3, 5, 7, 9, 2, 4, 6, 8, 10}; !reflect.DeepEqual(got, want) {
		t.Fatalf("non-unique in got=%v want=%v", got, want)
	}

	if err := db.EnsureIndexMode("a3", "/tags", IdxString); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PutNew("a3", []byte(`{"tags":["foo","bar","gaz"],"n":1}`)); err != nil {
		t.Fatal(err)
	}
	docID, err := db.PutNew("a3", []byte(`{"tags":["gaz","zaz"],"n":2}`))
	if err != nil {
		t.Fatal(err)
	}
	qTags := mustQuery(t, "a3", "/tags/[** in :tags]")
	if err := qTags.SetJSON("tags", 0, []any{"zaz", "gaz"}); err != nil {
		t.Fatal(err)
	}
	var tagLog strings.Builder
	tagDocs, err := db.ListQuery(qTags, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(qTags, &ExecOptions{Log: &tagLog}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(tagLog.String(), "[INDEX] SELECTED") {
		t.Fatalf("expected array index scanner, got:\n%s", tagLog.String())
	}
	if got, want := docsN(t, tagDocs), []int{2, 1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("array index duplicates got=%v want=%v", got, want)
	}
	if err := db.Put("a3", docID, []byte(`{"tags":["gaz","zaz","boo"],"n":2}`)); err != nil {
		t.Fatal(err)
	}
	qTags2 := mustQuery(t, "a3", "/tags/[** in :tags]")
	if err := qTags2.SetJSON("tags", 0, []any{"zaz", "boo"}); err != nil {
		t.Fatal(err)
	}
	if got, want := docsN(t, mustList(t, db, qTags2)), []int{2, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("array index after update got=%v want=%v", got, want)
	}
	if err := db.Delete("a3", docID); err != nil {
		t.Fatal(err)
	}
	qTags3 := mustQuery(t, "a3", "/tags/[** in :tags]")
	if err := qTags3.SetJSON("tags", 0, []any{"gaz"}); err != nil {
		t.Fatal(err)
	}
	if got, want := docsN(t, mustList(t, db, qTags3)), []int{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("array index after delete got=%v want=%v", got, want)
	}

	id1, err := db.PutNew("users", []byte(`{"name":"Andy"}`))
	if err != nil {
		t.Fatal(err)
	}
	id2, err := db.PutNew("users", []byte(`{"name":"John"}`))
	if err != nil {
		t.Fatal(err)
	}
	qPK := mustQuery(t, "", "@users/=:?")
	if err := qPK.SetI64("", 0, id1); err != nil {
		t.Fatal(err)
	}
	if docs := mustList(t, db, qPK); len(docs) != 1 || docs[0].ID != id1 {
		t.Fatalf("pk placeholder got=%+v", docs)
	}
	qPKArr := mustQuery(t, "", fmt.Sprintf("@users/=[%d,%d]", id1, id2))
	if docs := mustList(t, db, qPKArr); len(docs) != 2 {
		t.Fatalf("pk array docs=%+v", docs)
	}

	if err := db.EnsureIndexMode("mycoll", "/foo", IdxUnique|IdxString); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PutNew("mycoll", []byte(`{"foo":"baz","baz":"qux"}`)); err != nil {
		t.Fatal(err)
	}
	qRe := mustQuery(t, "", "@mycoll/[foo re :?]")
	if err := qRe.SetRegexp("", 0, ".*"); err != nil {
		t.Fatal(err)
	}
	if docs := mustList(t, db, qRe); len(docs) != 1 {
		t.Fatalf("regexp placeholder docs=%+v", docs)
	}

	if _, err := db.PutNew("delc", []byte(`{"f":{"b":1},"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PutNew("delc", []byte(`{"f":{"b":2},"n":2}`)); err != nil {
		t.Fatal(err)
	}
	deleted, err := db.ListQuery(mustQuery(t, "delc", "/f/[b = 2] | del"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := docsN(t, deleted), []int{2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delete returned got=%v want=%v", got, want)
	}
	if docs := mustList(t, db, mustQuery(t, "delc", "/f/[b = 2]")); len(docs) != 0 {
		t.Fatalf("deleted doc still matched: %+v", docs)
	}

	if _, err := db.PutNew("cc1", []byte(`{"foo":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := db.RenameCollection("cc1", "cc2"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get("cc2", 1); err != nil {
		t.Fatalf("get renamed: %v", err)
	}
	if err := db.RenameCollection("cc1", "cc2"); !errors.Is(err, ErrCollectionAbsent) {
		t.Fatalf("expected missing source rename error, got %v", err)
	}
	if err := db.RenameCollection("cc2", "cc2"); !errors.Is(err, ErrCollectionExists) {
		t.Fatalf("expected target exists rename error, got %v", err)
	}
}

func TestOfficialEmbeddedStringIndexRangeAndUpsert(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	if err := db.EnsureIndexMode("a2", "/f/b", IdxString); err != nil {
		t.Fatal(err)
	}
	values := []string{
		"A" + strings.Repeat("x", 199),
		"B" + strings.Repeat("x", 114),
		"C" + strings.Repeat("x", 63),
		"D" + strings.Repeat("x", 799),
	}
	for i, v := range values {
		if _, err := db.PutNew("a2", []byte(fmt.Sprintf(`{"f":{"b":"%s"},"n":%d}`, v, i+1))); err != nil {
			t.Fatal(err)
		}
	}
	q := mustQuery(t, "a2", "/f/[b >= :?]")
	if err := q.SetString("", 0, values[0]); err != nil {
		t.Fatal(err)
	}
	var log strings.Builder
	if _, err := db.Exec(q, &ExecOptions{Log: &log}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(log.String(), "[INDEX] SELECTED") {
		t.Fatalf("expected string index range plan, got:\n%s", log.String())
	}
	docs, err := db.ListQuery(q, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := docsN(t, docs), []int{1, 2, 3, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("string range got=%v want=%v", got, want)
	}
	if err := q.SetString("", 0, values[3]); err != nil {
		t.Fatal(err)
	}
	if got, want := docsN(t, mustList(t, db, q)), []int{4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("string range last got=%v want=%v", got, want)
	}

	if _, err := NewQuery("c1", `/* | apply {"pr":2.2E1,"b":1}`); err != nil {
		t.Fatalf("scientific JSON literal should parse: %v", err)
	}
	if err := db.EnsureIndexMode("users2", "/uuid", IdxUnique|IdxString); err != nil {
		t.Fatal(err)
	}
	upsert := mustQuery(t, "users2", "/[uuid = :?] | upsert :?")
	if err := upsert.SetString("", 0, "id-1"); err != nil {
		t.Fatal(err)
	}
	if err := upsert.SetJSON("", 1, map[string]any{"uuid": "id-1", "name": "a"}); err != nil {
		t.Fatal(err)
	}
	if n, err := db.UpdateQuery(upsert, 0); err != nil || n != 1 {
		t.Fatalf("upsert insert n=%d err=%v", n, err)
	}
	if err := upsert.SetJSON("", 1, map[string]any{"uuid": "id-1", "name": "b"}); err != nil {
		t.Fatal(err)
	}
	if n, err := db.UpdateQuery(upsert, 0); err != nil || n != 1 {
		t.Fatalf("upsert update n=%d err=%v", n, err)
	}
	names := mustList(t, db, mustQuery(t, "users2", "/* | /name"))
	if len(names) != 1 {
		t.Fatalf("expected one upserted doc, got %d", len(names))
	}
	assertJSONEqual(t, names[0].Raw, `{"name":"b"}`)
}

func mustList(t *testing.T, db *DB, q *Query) []Document {
	t.Helper()
	docs, err := db.ListQuery(q, 0)
	if err != nil {
		t.Fatalf("list %q: %v", q.Text(), err)
	}
	return docs
}

func docsN(t *testing.T, docs []Document) []int {
	t.Helper()
	out := make([]int, 0, len(docs))
	for _, doc := range docs {
		var m map[string]any
		if err := json.Unmarshal(doc.Raw, &m); err != nil {
			t.Fatal(err)
		}
		out = append(out, int(m["n"].(float64)))
	}
	return out
}

func TestOfficialPlannerLargeINFallsBackToPlainScan(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	if err := db.EnsureIndexMode("nums", "/v", IdxInt64); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := db.PutNew("nums", []byte(fmt.Sprintf(`{"v":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	values := make([]string, 0, 12)
	for i := 0; i < 12; i++ {
		values = append(values, strconv.Itoa(i))
	}
	q := mustQuery(t, "nums", fmt.Sprintf(`/[v in [%s]]`, strings.Join(values, ",")))
	var log strings.Builder
	docs, err := db.ListQuery(q, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(q, &ExecOptions{Log: &log}); err != nil {
		t.Fatal(err)
	}
	if len(docs) != 12 {
		t.Fatalf("expected 12 docs, got %d", len(docs))
	}
	if strings.Contains(log.String(), "[INDEX] SELECTED") {
		t.Fatalf("expected official-style large IN heuristic to avoid index, got:\n%s", log.String())
	}
	if !strings.Contains(log.String(), "[COLLECTOR] PLAIN") {
		t.Fatalf("expected plain scan log, got:\n%s", log.String())
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
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar = 22]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar !eq 22]", false)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar != 22]", false)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar >= 22]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/*/[bar >= 22]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar > 21]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar > 22]", false)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar < 23]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar <= 22]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar < 22]", false)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/*/[bar < 22]", false)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/*/[bar > 20 and bar <= 23]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/*/[bar > 22 and bar <= 23]", false)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/*/[bar > 23 or bar < 23]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/*/[bar < 23 or bar > 23]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[[* = bar] = 22]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[[* = bar] != 23]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/[* = foo]/[[* = bar] != 23]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/[* != foo]/[[* = bar] != 23]", false)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/[* re ^foo$]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/[* re fo]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/[* re ^fo$]", false)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/[* not re ^fo$]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar re 22]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar re \"2+\"]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar in [21, \"22\"]]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar in [21, 23]]", false)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/[* in [\"foo\"]]/[bar in [21, 22]]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/[* not in [\"foo\"]]/[bar in [21, 22]]", false)
	assertJQLMatch(t, "{'tags':['bar', 'foo']}", "/tags/[** in [\"bar\", \"baz\"]]", true)
	assertJQLMatch(t, "{'tags':['bar', 'foo']}", "/tags/[** in [\"zaz\", \"gaz\"]]", false)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/**/bar", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/**/baz", false)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/**/**/bar", true)
	assertJQLMatch(t, "{'foo':{'bar':22, 'baz':{'zaz':33}}}", "/foo/**/zaz", true)
	assertJQLMatch(t, "{'foo':{'bar':22, 'baz':{'zaz':33}}}", "/foo/**/[zaz > 30]", true)
	assertJQLMatch(t, "{'foo':{'bar':22, 'baz':{'zaz':33}}}", "/foo/**/[zaz < 30]", false)
	assertJQLMatch(t, "{'foo':[1,2]}", "/[foo = [1,2]]", true)
	assertJQLMatch(t, "{'foo':[1,2]}", "/[foo ni 2]", true)
	assertJQLMatch(t, "{'foo':[1,2]}", "/[foo in [[1,2]]]", true)
	assertJQLMatch(t, "{'foo':{'arr':[1,2,3,4]}}", "/foo/[arr = [1,2,3,4]]", true)
	assertJQLMatch(t, "{'foo':{'arr':[1,2,3,4]}}", "/foo/**/[arr = [1,2,3,4]]", true)
	assertJQLMatch(t, "{'foo':{'arr':[1,2,3,4]}}", "/foo/*/[arr = [1,2,3,4]]", false)
	assertJQLMatch(t, "{'foo':{'arr':[1,2,3,4]}}", "/foo/[arr = [1,2,3]]", false)
	assertJQLMatch(t, "{'foo':{'arr':[1,2,3,4]}}", "/foo/[arr = [1,12,3,4]]", false)
	assertJQLMatch(t, "{'foo':{'obj':{'f':'d','e':'j'}}}", "/foo/[obj = {\"e\":\"j\",\"f\":\"d\"}]", true)
	assertJQLMatch(t, "{'foo':{'obj':{'f':'d','e':'j'}}}", "/foo/[obj = {\"e\":\"j\",\"f\":\"dd\"}]", false)
	assertJQLMatch(t, "{'f':22}", "/=22", true)
	assertJQLMatch(t, "{'f':22}", "@mycoll/=22", true)
	assertJQLMatch(t, "{'lastName':'Doe'}", "/[lastName ~ 'Do']", true)
	assertJQLMatch(t, "{'lastName':'Smith'}", "/[lastName ~ 'Do']", false)
	assertJQLMatch(t, "{'foo bar':22}", `/"foo bar"`, true)
	assertJQLMatch(t, "{'foo/bar':22}", `/"foo/bar"`, true)
	assertJQLMatch(t, "{'foo/bar':22}", `/foo\/bar`, true)
	assertJQLMatch(t, "{'snowman ☃':22}", `/snowman\u0020\u2603`, true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar ! = 23]", true)
	assertJQLMatch(t, "{'foo':{'bar':22}}", "/foo/[bar ! eq 22]", false)
	assertJQLMatch(t, "{'lastName':'Doe'}", "/[lastName ! ~ 'Sm']", true)
	doc := "{'foo':{'bar': {'baz':{'zaz':33}},'sas': {'gaz':{'zaz':44, 'zarr':[42]}},'arr': [1,2,3,4]}}"
	assertJQLMatch(t, doc, "/foo/sas/gaz/zaz", true)
	assertJQLMatch(t, doc, "/foo/sas/gaz/[zaz = 44]", true)
	assertJQLMatch(t, doc, "/**/[zaz = 44]", true)
	assertJQLMatch(t, doc, "/foo/**/[zaz = 44]", true)
	assertJQLMatch(t, doc, "/foo/*/*/[zaz = 44]", true)
	assertJQLMatch(t, doc, "/foo/[arr ni 3]", true)
	assertJQLMatch(t, doc, "/**/[zarr ni 42]", true)
	assertJQLMatch(t, doc, "/**/[[* in [\"zarr\"]] in [[42]]]", true)
	assertJQLMatch(t, `[[ "one", "two" ]]`, "/*/[** = one]", true)
	assertJQLMatch(t, `[[ "red", "brown" ],[false]]`, "/*/[** = one]", false)
}

func TestOfficialJQLApplyAndProjectionCases(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	id, err := db.PutNew("c1", []byte(`{"foo":{"bar":22,"baz":{"gaz":444,"zaz":555}},"name":"test"}`))
	if err != nil {
		t.Fatal(err)
	}
	qApplyPatch := mustQuery(t, "c1", `/foo/bar | apply [{"op":"add","path":"/baz","value":"qux"}]`)
	if _, err := db.UpdateQuery(qApplyPatch, 0); err != nil {
		t.Fatalf("apply json patch: %v", err)
	}
	got, err := db.Get("c1", id)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, got, `{"foo":{"bar":22,"baz":{"gaz":444,"zaz":555}},"name":"test","baz":"qux"}`)

	id2, err := db.PutNew("c1", []byte(`{"foo":{"bar":22}}`))
	if err != nil {
		t.Fatal(err)
	}
	qApplyMerge := mustQuery(t, "c1", `/=2 | apply {"baz":"qux"}`)
	if _, err := db.UpdateQuery(qApplyMerge, 0); err != nil {
		t.Fatalf("apply merge patch: %v", err)
	}
	got, err = db.Get("c1", id2)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, got, `{"foo":{"bar":22},"baz":"qux"}`)

	cases := []struct {
		projection string
		want       string
	}{
		{`all`, `{"foo":{"bar":22,"baz":{"gaz":444,"zaz":555}},"name":"test","baz":"qux"}`},
		{`all+all + all`, `{"foo":{"bar":22,"baz":{"gaz":444,"zaz":555}},"name":"test","baz":"qux"}`},
		{`all - all`, `{}`},
		{`all-all +all`, `{}`},
		{`/foo/bar`, `{"foo":{"bar":22}}`},
		{`/foo/{daz,bar}`, `{"foo":{"bar":22}}`},
		{`/foo/bar + /foo/baz/zaz`, `{"foo":{"bar":22,"baz":{"zaz":555}}}`},
		{`/foo/bar + /foo/baz/zaz - /*/bar`, `{"foo":{"baz":{"zaz":555}}}`},
		{`all - /name`, `{"foo":{"bar":22,"baz":{"gaz":444,"zaz":555}},"baz":"qux"}`},
		{`/zzz`, `{}`},
	}
	for _, tc := range cases {
		q := mustQuery(t, "c1", fmt.Sprintf("/=%d | %s", id, tc.projection))
		docs, err := db.ListQuery(q, 1)
		if err != nil {
			t.Fatalf("projection %q: %v", tc.projection, err)
		}
		if len(docs) != 1 {
			t.Fatalf("projection %q docs=%d", tc.projection, len(docs))
		}
		assertJSONEqual(t, docs[0].Raw, tc.want)
	}
}

func TestOfficialJQLProjectionPlaceholderTypes(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	if _, err := db.PutNew("c1", []byte(`{"foo":1,"bar":2,"baz":3}`)); err != nil {
		t.Fatal(err)
	}
	q := mustQuery(t, "c1", "/* | /:name+/:?")
	if err := q.SetI64("name", 0, 1); !errors.Is(err, &Error{Code: CodeInvalidPlaceholder}) {
		t.Fatalf("expected invalid path placeholder type, got %v", err)
	}
	if err := q.SetString("name", 0, "foo"); err != nil {
		t.Fatal(err)
	}
	if err := q.SetString("", 0, "baz"); err != nil {
		t.Fatal(err)
	}
	docs, err := db.ListQuery(q, 0)
	if err != nil {
		t.Fatalf("projection placeholder list: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("docs=%d", len(docs))
	}
	assertJSONEqual(t, docs[0].Raw, `{"foo":1,"baz":3}`)
}

func TestOfficialCompatibilityCountPatchIDsIndexesAndMeta(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "db.pebble"))
	defer db.Close()

	artistID, err := db.PutNew("artists", []byte(`{"name":"Ada"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.PutNew("paintings", []byte(`{"artist":"1","n":1}`)); err != nil {
		t.Fatal(err)
	}
	if artistID != 1 {
		t.Fatalf("artist id=%d", artistID)
	}

	qPK := mustQuery(t, "", "@artists/=:id")
	if err := qPK.SetString("id", 0, "1"); err != nil {
		t.Fatal(err)
	}
	if docs := mustList(t, db, qPK); len(docs) != 1 || docs[0].ID != 1 {
		t.Fatalf("string pk got %+v", docs)
	}
	if docs := mustList(t, db, mustQuery(t, "", `@artists/=["1"]`)); len(docs) != 1 || docs[0].ID != 1 {
		t.Fatalf("string pk array got %+v", docs)
	}
	if docs := mustList(t, db, mustQuery(t, "paintings", `/* | /artist<artists`)); len(docs) != 1 {
		t.Fatalf("join docs=%+v", docs)
	} else {
		assertJSONEqual(t, docs[0].Raw, `{"artist":{"name":"Ada"}}`)
	}

	for _, raw := range []string{`{"v":1}`, `{"v":2}`, `{"v":3}`} {
		if _, err := db.PutNew("items", []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	qCount := mustQuery(t, "items", "/* | count")
	visited := 0
	cnt, err := db.Exec(qCount, &ExecOptions{Visitor: func(Document, *int64) error {
		visited++
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 3 || visited != 0 {
		t.Fatalf("count exec cnt=%d visited=%d", cnt, visited)
	}
	if docs, err := db.ListQuery(qCount, 0); err != nil || len(docs) != 0 {
		t.Fatalf("count list docs=%+v err=%v", docs, err)
	}
	if cnt, err := db.Count(qCount, 0); err != nil || cnt != 3 {
		t.Fatalf("count count cnt=%d err=%v", cnt, err)
	}

	patchID, err := db.PutNew("patches", []byte(`{"foo":{"bar":1},"arr":["bar"],"baz":{"gaz":11},"pets":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Patch("patches", patchID, []byte(`[{"op":"increment","path":"/foo/bar","value":2},{"op":"add_create","path":"/foo/zaz/gaz","value":22},{"op":"add","path":"/pets/-","value":{"name":"Neo"}},{"op":"swap","from":"/arr/0","path":"/baz/gaz"}]`)); err != nil {
		t.Fatal(err)
	}
	got, err := db.Get("patches", patchID)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, got, `{"foo":{"bar":3,"zaz":{"gaz":22}},"arr":[11],"baz":{"gaz":"bar"},"pets":[{"name":"Neo"}]}`)
	if err := db.Patch("patches", patchID, []byte(`[{"op":"add","path":"/missing/path","value":1}]`)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected standard add missing parent error, got %v", err)
	}
	if err := db.Patch("patches", patchID, []byte(`[{"op":"swap","from":"/arr/0","path":"/baz/zaz"}]`)); err != nil {
		t.Fatal(err)
	}
	got, err = db.Get("patches", patchID)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, got, `{"foo":{"bar":3,"zaz":{"gaz":22}},"arr":[],"baz":{"gaz":"bar","zaz":11},"pets":[{"name":"Neo"}]}`)

	if err := db.EnsureIndexMode("idx", "/v", IdxUnique|IdxString); err != nil {
		t.Fatal(err)
	}
	if err := db.EnsureIndexMode("idx", "/v", IdxString); !errors.Is(err, &Error{Code: CodeMismatchedIndexUniqueness}) {
		t.Fatalf("expected mismatched uniqueness, got %v", err)
	}
	if err := db.EnsureIndexMode("idx", "/bad/*/v", IdxString); !errors.Is(err, &Error{Code: CodeInvalidIndexMode}) {
		t.Fatalf("expected invalid wildcard index path, got %v", err)
	}
	if err := db.EnsureIndexMode("idx", "/n", IdxString); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PutNew("idx", []byte(`{"v":"a","n":42}`)); err != nil {
		t.Fatal(err)
	}
	if docs := mustList(t, db, mustQuery(t, "idx", `/[n = "42"]`)); len(docs) != 1 {
		t.Fatalf("converted string index docs=%+v", docs)
	}
	if err := db.RemoveIndexMode("missing", "/v", IdxString); err != nil {
		t.Fatalf("remove index missing collection: %v", err)
	}
	if err := db.RemoveIndexMode("idx", "/v", IdxUnique|IdxString); err != nil {
		t.Fatal(err)
	}
	meta, err := db.Meta()
	if err != nil {
		t.Fatal(err)
	}
	col := findMetaCollection(meta, "idx")
	if col == nil {
		t.Fatalf("idx collection missing in meta: %+v", meta)
	}
	if len(col.Indexes) != 1 || col.Indexes[0].Path != "/n" || col.Indexes[0].Kind != IndexString || col.Indexes[0].Mode != IdxString || col.Indexes[0].RNum != 1 || col.Indexes[0].DBID == 0 || col.Indexes[0].IDBF == 0 {
		t.Fatalf("unexpected index meta: %+v", col.Indexes)
	}
}
