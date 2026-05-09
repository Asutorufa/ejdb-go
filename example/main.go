package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	ejdb "github.com/Asutorufa/ejdb-go"
	json "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
)

func main() {
	workDir := filepath.Join(".", "example", "data")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		log.Fatal(err)
	}
	dbPath := filepath.Join(workDir, "demo.pebble")
	_ = os.RemoveAll(dbPath)

	db, err := ejdb.Open(ejdb.Options{Path: dbPath})
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	seedDemoData(db)

	fmt.Println("== JQL / SQL-like query examples ==")
	runQueryCatalog(db, 1)
	runUpdateExamples(db)
	runExecVisitorExample(db)

	must(db.ForceSync())
	backupPath := filepath.Join(workDir, "demo.backup.db")
	_ = os.RemoveAll(backupPath)
	must(db.Backup(backupPath))
	fmt.Printf("\nbackup created: %s\n", backupPath)

	backupDB, err := ejdb.Open(ejdb.Options{Path: backupPath})
	must(err)
	backupMeta, err := backupDB.Meta()
	must(err)
	must(backupDB.Close())
	fmt.Printf("backup reopened with %d collections\n", len(backupMeta.Collections))

	meta, err := db.Meta()
	must(err)
	mb, _ := marshalIndent(meta, "", "  ")
	fmt.Println("\nmeta:")
	fmt.Println(string(mb))

	fmt.Printf("\nDone. DB file: %s\n", dbPath)
}

func seedDemoData(db *ejdb.DB) {
	must(db.EnsureCollection("users"))
	must(db.EnsureCollection("profiles"))
	must(db.EnsureCollection("audit"))

	must(db.EnsureIndex("users", "/email", ejdb.IndexString, true))
	must(db.EnsureIndex("users", "/age", ejdb.IndexInt64, false))
	must(db.EnsureIndex("users", "/name", ejdb.IndexString, false))
	must(db.EnsureIndex("users", "/score", ejdb.IndexFloat, false))

	aliceID, err := db.PutNew("profiles", []byte(`{"name":"Alice Profile","years":[1990,2050]}`))
	must(err)
	_, err = db.PutNew("profiles", []byte(`{"name":"Bob Profile","years":[2003,2040]}`))
	must(err)

	_, err = db.PutNew("users", []byte(fmt.Sprintf(`{"name":"Alice","email":"alice@example.com","age":29,"active":true,"score":91.5,"tags":["admin","go"],"address":{"city":"Shanghai"},"profile_ref":%d}`, aliceID)))
	must(err)
	_, err = db.PutNew("users", []byte(`{"name":"Bob","email":"bob@example.com","age":20,"active":true,"score":77,"tags":["go","ops"],"address":{"city":"Beijing"},"profile_ref":2}`))
	must(err)
	_, err = db.PutNew("users", []byte(`{"name":"Carol","email":"carol@example.com","age":35,"active":false,"score":88.25,"tags":["design"],"address":{"city":"Shanghai"}}`))
	must(err)
	_, err = db.PutNew("users", []byte(`{"name":"Dave","email":"dave@example.com","age":17,"active":true,"score":62,"tags":["intern"],"address":{"city":"Shenzhen"}}`))
	must(err)

	_, err = db.PutNew("users", []byte(`{"name":"Alice-dup","email":"alice@example.com","age":31}`))
	if errors.Is(err, ejdb.ErrUniqueConstraint) {
		fmt.Println("unique index works: duplicate email rejected")
		return
	}
	must(err)
}

func runQueryCatalog(db *ejdb.DB, aliceID int64) {
	// Empty query or /*: all documents in the selected collection.
	showQuery(db, "select all users", "users", "/*")

	// @collection selects the collection inside the query string.
	showQuery(db, "explicit collection selector", "", "@users/*")

	// /= filters by primary key. Placeholders can be named or positional.
	qPK := mustQuery("", "@users/=:id")
	must(qPK.SetI64("id", 0, aliceID))
	showPreparedQuery(db, "primary key lookup with named placeholder", qPK, 0)

	// /path checks that a JSON pointer exists.
	showQuery(db, "path exists", "users", "/address/city")

	// Basic comparisons: = != > >= < <= and word aliases eq, !eq, gt, gte, lt, lte.
	showQuery(db, "comparison >=", "users", "/[age >= 21] | asc /age")
	showQuery(db, "comparison alias gt", "users", "/[score gt 80] | desc /score")
	showQuery(db, "not equal", "users", "/[name != 'Bob'] | asc /name limit 3")

	// in / not in match a value against an array literal.
	showQuery(db, "in array", "users", "/[age in [17,20,29]] | asc /age")
	showQuery(db, "not in array", "users", "/[age not in [17,20]] | asc /age")

	// re / not re run regular-expression comparisons.
	qRegexp := mustQuery("users", "/[name re :?] | asc /name")
	must(qRegexp.SetRegexp("", 0, "^A.*"))
	showPreparedQuery(db, "regexp placeholder", qRegexp, 0)
	showQuery(db, "not regexp", "users", "/[email not re '.*@example.com'] | asc /name")

	// ~ / !~ are string prefix and not-prefix operators.
	showQuery(db, "prefix", "users", "/[name ~ 'Al'] | asc /name")
	showQuery(db, "not prefix", "users", "/[name !~ 'Al'] | asc /name")

	// Array wildcard: ** matches descendants under the current array.
	showQuery(db, "array wildcard", "users", "/tags/[** in ['go']] | asc /name")

	// Recursive wildcard: /** searches descendants.
	showQuery(db, "recursive descendant search", "users", "/**/[city = 'Shanghai'] | asc /name")

	// Boolean composition supports and, or, not, and parentheses.
	showQuery(db, "boolean expression", "users", "/[age >= 18] and (/[active = true] or /[score >= 90]) | asc /name")
	showQuery(db, "negated filter", "users", "not /[active = false] | asc /name")
	runCombinationQueries(db)

	// asc/desc can sort by one or more paths. skip/limit page the result.
	showQuery(db, "multi order + pagination", "users", "/* | desc /address/city asc /age skip 1 limit 2")

	// inverse reverses default order; noidx forces a scan even if an index exists.
	showQuery(db, "inverse order", "users", "/* | inverse limit 2")
	showQuery(db, "noidx scan", "users", "/[age >= 18] | noidx asc /age")

	// count returns only the count. ListQuery for a count pipeline returns no documents.
	qCount := mustQuery("users", "/* | count")
	cnt, err := db.Count(qCount, 0)
	must(err)
	fmt.Printf("\n-- count pipeline --\n%s\ncount=%d\n", qCount.Canonical(), cnt)

	// Projection: /{a,b} includes fields, all - /path excludes fields, and <collection joins by id.
	showQuery(db, "projection include fields", "users", "/* | /{name,email,address/city} | asc /name")
	showQuery(db, "projection exclude field", "users", "/* | all - /email - /score | asc /name")
	showQuery(db, "projection join", "users", "/* | /{name,profile_ref<profiles} - /profile_ref/years/0")

	// Path placeholders can be used in projection and order-by paths.
	qPathPH := mustQuery("users", "/* | /:field | desc :?")
	must(qPathPH.SetString("field", 0, "name"))
	must(qPathPH.SetString("", 0, "/age"))
	showPreparedQuery(db, "path placeholders", qPathPH, 0)
}

func runCombinationQueries(db *ejdb.DB) {
	fmt.Println("\n== Combination query examples ==")

	// AND across different document paths. The indexed /age predicate narrows candidates,
	// then /address/city is checked against each candidate.
	showQuery(db, "AND across paths", "users", "/[age >= 18] and /address/[city = 'Shanghai'] | asc /age")

	// OR combines independent filters. Top-level OR is supported, but it is not index-planned
	// as a single candidate source, so it scans and evaluates both sides.
	showQuery(db, "OR across paths", "users", "/[age < 18] or /address/[city = 'Beijing'] | asc /name")

	// NOT can wrap a whole grouped expression.
	showQuery(db, "NOT grouped expression", "users", "not (/[age < 18] or /[active = false]) | asc /name")

	// Multiple comparisons can live inside one bracket expression.
	showQuery(db, "compound bracket expression", "users", "/[age >= 18 and score >= 80 and active = true] | desc /score")

	// Mixed AND/OR precedence can be made explicit with parentheses.
	showQuery(db, "nested boolean precedence", "users", "(/[age >= 30] and /address/[city = 'Shanghai']) or (/[age < 21] and /[active = true]) | asc /age")

	// Combine array matching with a scalar predicate.
	showQuery(db, "array + scalar combination", "users", "/tags/[** in ['go']] and /[score >= 70] | asc /name")

	// Combine prefix and regexp filters.
	showQuery(db, "prefix + regexp combination", "users", "/[name !~ 'A'] and /[email re '.*@example.com'] | asc /name")

	// Combine placeholders in multiple predicates.
	qCombo := mustQuery("users", "/[age >= :minAge] and /address/[city = :city] | asc /score")
	must(qCombo.SetI64("minAge", 0, 18))
	must(qCombo.SetString("city", 0, "Shanghai"))
	showPreparedQuery(db, "placeholder combination", qCombo, 0)

	// Explicit noidx is useful for verifying scan behavior against the same composed predicate.
	showQuery(db, "combination forced scan", "users", "/[age >= 18] and /address/[city = 'Shanghai'] | noidx asc /age")
}

func runUpdateExamples(db *ejdb.DB) {
	// apply patches matching rows. This example uses a placeholder JSON merge patch payload.
	qApply := mustQuery("users", "/[name = 'Bob'] | apply :?")
	must(qApply.SetJSON("", 0, map[string]any{"reviewed": true}))
	changed, err := db.UpdateQuery(qApply, 1)
	must(err)
	fmt.Printf("\n-- apply update --\n%s\nchanged=%d\n", qApply.Canonical(), changed)
	showQuery(db, "Bob after apply", "users", "/[name = 'Bob']")

	// upsert creates a document if no row matches the filter.
	qUpsert := mustQuery("audit", "/[event = 'query-catalog'] | upsert :?")
	must(qUpsert.SetJSON("", 0, map[string]any{"event": "query-catalog", "count": 1}))
	changed, err = db.UpdateQuery(qUpsert, 1)
	must(err)
	fmt.Printf("\n-- upsert --\n%s\nchanged=%d\n", qUpsert.Canonical(), changed)
	showQuery(db, "audit after upsert", "audit", "/*")

	// del deletes matching rows. This runs against a scratch collection so user rows remain intact.
	_, err = db.PutNew("audit", []byte(`{"event":"temporary","ttl":0}`))
	must(err)
	qDel := mustQuery("audit", "/[ttl = 0] | del")
	changed, err = db.UpdateQuery(qDel, 0)
	must(err)
	fmt.Printf("\n-- delete --\n%s\nchanged=%d\n", qDel.Canonical(), changed)
	showQuery(db, "audit after delete", "audit", "/*")
}

func runExecVisitorExample(db *ejdb.DB) {
	qExec := mustQuery("users", "/* | asc /name")
	fmt.Println("\n-- exec visitor with step control --")
	fmt.Println(qExec.Canonical())
	_, err := db.Exec(qExec, &ejdb.ExecOptions{Visitor: func(doc ejdb.Document, step *int64) error {
		var m map[string]any
		_ = json.Unmarshal(doc.Raw, &m)
		fmt.Printf("visit id=%d name=%v\n", doc.ID, m["name"])
		if strings.HasPrefix(fmt.Sprint(m["name"]), "Alice") {
			*step = 2
		}
		return nil
	}})
	must(err)
}

func showQuery(db *ejdb.DB, title, collection, text string) {
	showPreparedQuery(db, title, mustQuery(collection, text), 0)
}

func showPreparedQuery(db *ejdb.DB, title string, q *ejdb.Query, limit int64) {
	docs, err := db.ListQuery(q, limit)
	must(err)
	fmt.Printf("\n-- %s --\n%s\n", title, q.Canonical())
	printDocs(docs)
}

func mustQuery(collection, text string) *ejdb.Query {
	q, err := ejdb.NewQuery(collection, text)
	must(err)
	return q
}

func printDocs(docs []ejdb.Document) {
	if len(docs) == 0 {
		fmt.Println("(no documents)")
		return
	}
	for _, d := range docs {
		fmt.Printf("id=%d %s\n", d.ID, pretty(d.Raw))
	}
}

func pretty(raw []byte) []byte {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return raw
	}
	b, err := marshalIndent(v, "", "  ")
	if err != nil {
		return raw
	}
	return b
}

func marshalIndent(v any, prefix, indent string) ([]byte, error) {
	b, err := json.Marshal(v,
		json.Deterministic(true),
		json.FormatNilMapAsNull(true),
		json.FormatNilSliceAsNull(true),
		jsontext.AllowDuplicateNames(true),
		jsontext.AllowInvalidUTF8(true),
	)
	if err != nil {
		return nil, err
	}
	out := jsontext.Value(b)
	if err := out.Indent(jsontext.WithIndentPrefix(prefix), jsontext.WithIndent(indent)); err != nil {
		return nil, err
	}
	return out, nil
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
