package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	ejdb "github.com/softmotions/ejdb-go"
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

	must(db.EnsureCollection("users"))
	must(db.EnsureCollection("profiles"))
	must(db.EnsureIndex("users", "/email", ejdb.IndexString, true))
	must(db.EnsureIndex("users", "/age", ejdb.IndexInt64, false))

	aliceID, err := db.PutNew("profiles", []byte(`{"name":"Alice Profile","years":[1990,2050]}`))
	must(err)
	_, err = db.PutNew("users", []byte(fmt.Sprintf(`{"name":"Alice","email":"alice@example.com","age":29,"profile_ref":%d}`, aliceID)))
	must(err)
	_, err = db.PutNew("users", []byte(`{"name":"Bob","email":"bob@example.com","age":20}`))
	must(err)
	_, err = db.PutNew("users", []byte(`{"name":"Alice-dup","email":"alice@example.com","age":31}`))
	if errors.Is(err, ejdb.ErrUniqueConstraint) {
		fmt.Println("unique index works: duplicate email rejected")
	}

	qAdults := mustQuery("users", "/[age >= :?] | asc /age")
	must(qAdults.SetI64("", 0, 21))
	fmt.Println("\nquery: age >= 21, asc /age")
	adults, err := db.ListQuery(qAdults, 0)
	must(err)
	printDocs(adults)

	qApply := mustQuery("users", "/[email = :?] | apply :?")
	must(qApply.SetString("", 0, "alice@example.com"))
	must(qApply.SetJSON("", 1, map[string]any{"city": "Shanghai"}))
	_, err = db.UpdateQuery(qApply, 0)
	must(err)

	qAlice := mustQuery("users", "/[email = 'alice@example.com']")
	alice, err := db.ListQuery(qAlice, 1)
	must(err)
	fmt.Println("\nAlice after apply:")
	printDocs(alice)

	qJoin := mustQuery("users", "/* | /{name, profile_ref<profiles} - /profile_ref/years/0")
	joined, err := db.ListQuery(qJoin, 0)
	must(err)
	fmt.Println("\nprojection + join:")
	printDocs(joined)

	qExec := mustQuery("users", "/* | asc /name")
	fmt.Println("\nexec visitor (skip demo):")
	_, err = db.Exec(qExec, &ejdb.ExecOptions{Visitor: func(doc ejdb.Document, step *int64) error {
		var m map[string]any
		_ = json.Unmarshal(doc.Raw, &m)
		fmt.Printf("visit id=%d name=%v\n", doc.ID, m["name"])
		if strings.HasPrefix(fmt.Sprint(m["name"]), "Alice") {
			*step = 2
		}
		return nil
	}})
	must(err)

	qDel := mustQuery("users", "/[age < 25] | del")
	_, err = db.UpdateQuery(qDel, 0)
	must(err)
	qLeft := mustQuery("users", "/*")
	left, err := db.ListQuery(qLeft, 0)
	must(err)
	fmt.Println("\nleft users after delete:")
	printDocs(left)

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
	mb, _ := json.MarshalIndent(meta, "", "  ")
	fmt.Println("\nmeta:")
	fmt.Println(string(mb))

	fmt.Printf("\nDone. DB file: %s\n", dbPath)
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
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return raw
	}
	return b
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
