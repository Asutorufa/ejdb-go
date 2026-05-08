package ejdb

import (
	"encoding/json"
	"fmt"
)

type Tx struct {
	db    *DB
	state *dbState
	write bool
}

func (db *DB) ReadTx(fn func(*Tx) error) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	return fn(&Tx{db: db, state: db.state, write: false})
}

func (db *DB) WriteTx(fn func(*Tx) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	snapshot, err := db.state.clone()
	if err != nil {
		return err
	}
	pendingLen, dirtyMeta, dirtyFull := len(db.pending), db.dirtyMeta, db.dirtyFull
	tx := &Tx{db: db, state: db.state, write: true}
	if err := fn(tx); err != nil {
		db.state = snapshot
		db.truncatePending(pendingLen, dirtyMeta, dirtyFull)
		return err
	}
	if err := db.commitLocked(); err != nil {
		db.state = snapshot
		db.truncatePending(pendingLen, dirtyMeta, dirtyFull)
		return err
	}
	return nil
}

// Backward-compatible aliases for earlier API names.
func (db *DB) View(fn func(*Tx) error) error {
	return db.ReadTx(fn)
}

func (db *DB) Update(fn func(*Tx) error) error {
	return db.WriteTx(fn)
}

func (tx *Tx) Meta() Meta {
	return toMeta(tx.db.path, tx.state)
}

func (tx *Tx) EnsureCollection(name string) error {
	if !tx.write {
		return ErrReadOnlyTx
	}
	tx.ensureCollection(name)
	tx.db.markMetaDirty()
	return nil
}

func (tx *Tx) RemoveCollection(name string) error {
	if !tx.write {
		return ErrReadOnlyTx
	}
	delete(tx.state.Collections, name)
	tx.db.markFullDirty()
	return nil
}

func (tx *Tx) RenameCollection(oldName, newName string) error {
	if !tx.write {
		return ErrReadOnlyTx
	}
	if oldName == newName {
		return nil
	}
	if _, ok := tx.state.Collections[newName]; ok {
		return ErrCollectionExists
	}
	col, ok := tx.state.Collections[oldName]
	if !ok {
		return ErrCollectionAbsent
	}
	delete(tx.state.Collections, oldName)
	col.Name = newName
	tx.state.Collections[newName] = col
	tx.db.markFullDirty()
	return nil
}

func (tx *Tx) PutNew(collection string, raw []byte) (int64, error) {
	if !tx.write {
		return 0, ErrReadOnlyTx
	}
	col := tx.ensureCollection(collection)
	col.NextID++
	id := col.NextID
	if err := tx.db.putLocked(col, id, raw); err != nil {
		col.NextID--
		return 0, err
	}
	return id, nil
}

func (tx *Tx) Put(collection string, id int64, raw []byte) error {
	if !tx.write {
		return ErrReadOnlyTx
	}
	col := tx.ensureCollection(collection)
	if id > col.NextID {
		col.NextID = id
	}
	return tx.db.putLocked(col, id, raw)
}

func (tx *Tx) Get(collection string, id int64) (json.RawMessage, error) {
	col, ok := tx.state.Collections[collection]
	if !ok {
		return nil, ErrNotFound
	}
	raw, ok := col.Docs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return append(json.RawMessage(nil), raw...), nil
}

func (tx *Tx) Delete(collection string, id int64) error {
	if !tx.write {
		return ErrReadOnlyTx
	}
	col, ok := tx.state.Collections[collection]
	if !ok {
		return ErrNotFound
	}
	raw, ok := col.Docs[id]
	if !ok {
		return ErrNotFound
	}
	var doc any
	if err := decodeJSONDocument(raw, &doc); err != nil {
		return err
	}
	tx.db.removeDocFromIndexes(col, id, doc)
	delete(col.Docs, id)
	tx.db.recordDocDelete(col.Name, id, raw)
	return nil
}

func (tx *Tx) Patch(collection string, id int64, patch []byte) error {
	if !tx.write {
		return ErrReadOnlyTx
	}
	col, ok := tx.state.Collections[collection]
	if !ok {
		return ErrNotFound
	}
	raw, ok := col.Docs[id]
	if !ok {
		return ErrNotFound
	}
	newRaw, err := applyJSONPatch(raw, patch)
	if err != nil {
		return err
	}
	return tx.db.putLocked(col, id, newRaw)
}

func (tx *Tx) MergeOrPut(collection string, id int64, patch []byte) error {
	if !tx.write {
		return ErrReadOnlyTx
	}
	col := tx.ensureCollection(collection)
	raw, ok := col.Docs[id]
	if !ok {
		if id > col.NextID {
			col.NextID = id
		}
		return tx.db.putLocked(col, id, patch)
	}
	newRaw, err := applyMergePatch(raw, patch)
	if err != nil {
		return err
	}
	return tx.db.putLocked(col, id, newRaw)
}

func (tx *Tx) EnsureIndex(collection, path string, kind IndexKind, unique bool) error {
	if !tx.write {
		return ErrReadOnlyTx
	}
	if path == "" || path[0] != '/' {
		return fmt.Errorf("index path must be JSON pointer, got %q", path)
	}
	if kind != IndexString && kind != IndexInt64 && kind != IndexFloat {
		return fmt.Errorf("unsupported index kind: %q", kind)
	}
	col := tx.ensureCollection(collection)
	k := indexKey(path, kind, unique)
	if _, ok := col.Indexes[k]; ok {
		return nil
	}
	idx := indexState{Path: path, Kind: kind, Unique: unique}
	col.Indexes[k] = idx
	if col.runtime == nil {
		col.runtime = make(map[string]*indexRuntime)
	}
	col.runtime[k] = &indexRuntime{def: idx, unique: make(map[string]int64), multi: make(map[string]map[int64]struct{})}
	if err := tx.db.rebuildIndex(col, k); err != nil {
		delete(col.Indexes, k)
		delete(col.runtime, k)
		return err
	}
	tx.db.markFullDirty()
	return nil
}

func (tx *Tx) EnsureIndexMode(collection, path string, mode IndexMode) error {
	kind, unique, err := indexModeParts(mode)
	if err != nil {
		return err
	}
	return tx.EnsureIndex(collection, path, kind, unique)
}

func (tx *Tx) RemoveIndex(collection, path string, kind IndexKind) error {
	if !tx.write {
		return ErrReadOnlyTx
	}
	col, ok := tx.state.Collections[collection]
	if !ok {
		return ErrCollectionAbsent
	}
	removed := false
	for k, idx := range col.Indexes {
		if idx.Path == path && idx.Kind == kind {
			delete(col.Indexes, k)
			delete(col.runtime, k)
			removed = true
		}
	}
	if !removed {
		return ErrIndexNotFound
	}
	tx.db.markFullDirty()
	return nil
}

func (tx *Tx) RemoveIndexMode(collection, path string, mode IndexMode) error {
	kind, _, err := indexModeParts(mode)
	if err != nil {
		return err
	}
	return tx.RemoveIndex(collection, path, kind)
}

func (tx *Tx) Exec(q *Query, opts *ExecOptions) (int64, error) {
	if q == nil {
		return 0, withCode(CodeInvalidQuery, "nil query")
	}
	if q.parsed.action != actionNone && !tx.write {
		return 0, ErrReadOnlyTx
	}
	_, cnt, _, err := tx.db.runQueryLocked(tx.state, q, modeExec, opts)
	return cnt, err
}

func (tx *Tx) ListQuery(q *Query, limit int64) ([]Document, error) {
	if q == nil {
		return nil, withCode(CodeInvalidQuery, "nil query")
	}
	if q.parsed.action != actionNone && !tx.write {
		return nil, ErrReadOnlyTx
	}
	docs, _, _, err := tx.db.runQueryLocked(tx.state, q, modeList, &ExecOptions{Limit: limit})
	return docs, err
}

func (tx *Tx) Count(q *Query, limit int64) (int64, error) {
	if q == nil {
		return 0, withCode(CodeInvalidQuery, "nil query")
	}
	_, cnt, _, err := tx.db.runQueryLocked(tx.state, q, modeCount, &ExecOptions{Limit: limit})
	return cnt, err
}

func (tx *Tx) UpdateQuery(q *Query, limit int64) (int64, error) {
	if !tx.write {
		return 0, ErrReadOnlyTx
	}
	if q == nil {
		return 0, withCode(CodeInvalidQuery, "nil query")
	}
	if q.parsed.action == actionNone {
		return 0, withCode(CodeInvalidQuery, "query has no update action")
	}
	_, cnt, _, err := tx.db.runQueryLocked(tx.state, q, modeUpdate, &ExecOptions{Limit: limit})
	return cnt, err
}

func (tx *Tx) ensureCollection(name string) *collectionState {
	if col, ok := tx.state.Collections[name]; ok {
		col.Name = name
		if col.runtime == nil {
			col.initRuntime()
		}
		return col
	}
	tx.state.NextCollectionID++
	col := &collectionState{Name: name, DBID: tx.state.NextCollectionID, NextID: 0, Docs: make(map[int64]json.RawMessage), Indexes: make(map[string]indexState), runtime: make(map[string]*indexRuntime)}
	tx.state.Collections[name] = col
	return col
}
