package ejdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

type DB struct {
	mu     sync.RWMutex
	path   string
	engine StorageEngine
	state  *dbState
	closed bool

	pending   []storageMutation
	dirtyMeta bool
	dirtyFull bool
}

type storageMutationKind uint8

const (
	mutationPut storageMutationKind = iota + 1
	mutationDelete
)

type storageMutation struct {
	kind       storageMutationKind
	collection string
	id         int64
	oldRaw     json.RawMessage
	newRaw     json.RawMessage
}

func Open(opts Options) (*DB, error) {
	if opts.Path == "" {
		return nil, withCode(CodeInvalidQuery, "options.path is required")
	}
	if err := os.MkdirAll(filepath.Dir(opts.Path), 0o755); err != nil {
		return nil, err
	}
	engine := opts.Engine
	if engine == nil {
		engine = NewPebbleEngine(opts.PebbleOptions)
	}
	if pe, ok := engine.(*PebbleEngine); ok {
		pe.setSyncWrites(opts.AutoSync)
	}
	if err := engine.Open(opts.Path); err != nil {
		return nil, err
	}
	db := &DB{
		path:   opts.Path,
		engine: engine,
	}
	if err := db.load(); err != nil {
		_ = engine.Close()
		return nil, err
	}
	return db, nil
}

func (db *DB) load() error {
	raw, err := db.engine.Get(keyMetaState)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			db.state = newState()
		} else {
			return err
		}
	} else {
		st, err := decodeCatalog(raw)
		if err != nil {
			return err
		}
		db.state = st
	}
	db.prepareState()
	if err := db.loadDocs(); err != nil {
		return err
	}
	return db.rebuildAllIndexes()
}

func (db *DB) prepareState() {
	if db.state == nil {
		db.state = newState()
	}
	if db.state.Collections == nil {
		db.state.Collections = make(map[string]*collectionState)
	}
	for name, c := range db.state.Collections {
		c.Name = name
		if c.Docs == nil {
			c.Docs = make(map[int64]json.RawMessage)
		}
		if c.Indexes == nil {
			c.Indexes = make(map[string]indexState)
		}
		c.initRuntime()
	}
}

func (db *DB) loadDocs() error {
	return scanPrefix(db.engine, []byte{keyTagDoc}, func(key, value []byte) error {
		collName, id, ok := decodeDocKey(key)
		if !ok {
			return withCode(CodeInvalidQuery, "invalid document key in storage")
		}
		col, ok := db.state.Collections[collName]
		if !ok {
			col = db.ensureCollectionOnStateLocked(db.state, collName)
		}
		col.Docs[id] = append(json.RawMessage(nil), value...)
		if id > col.NextID {
			col.NextID = id
		}
		return nil
	})
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	return db.engine.Close()
}

func (db *DB) Meta() (Meta, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return Meta{}, ErrClosed
	}
	return toMeta(db.path, db.state), nil
}

func (db *DB) ForceSync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	return db.engine.Flush()
}

func (db *DB) Backup(dst string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	return db.engine.Backup(dst)
}

func (db *DB) EnsureCollection(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	db.ensureCollectionLocked(name)
	db.markMetaDirty()
	return db.commitLocked()
}

func (db *DB) RemoveCollection(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	delete(db.state.Collections, name)
	db.markFullDirty()
	return db.commitLocked()
}

func (db *DB) RenameCollection(oldName, newName string) error {
	if oldName == newName {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if _, ok := db.state.Collections[newName]; ok {
		return ErrCollectionExists
	}
	col, ok := db.state.Collections[oldName]
	if !ok {
		return ErrCollectionAbsent
	}
	delete(db.state.Collections, oldName)
	col.Name = newName
	db.state.Collections[newName] = col
	db.markFullDirty()
	return db.commitLocked()
}

func (db *DB) PutNew(collection string, raw []byte) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return 0, ErrClosed
	}
	col := db.ensureCollectionLocked(collection)
	col.NextID++
	id := col.NextID
	if err := db.putLocked(col, id, raw); err != nil {
		col.NextID--
		return 0, err
	}
	if err := db.commitLocked(); err != nil {
		return 0, err
	}
	return id, nil
}

func (db *DB) Put(collection string, id int64, raw []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	col := db.ensureCollectionLocked(collection)
	if id > col.NextID {
		col.NextID = id
	}
	if err := db.putLocked(col, id, raw); err != nil {
		return err
	}
	return db.commitLocked()
}

func (db *DB) Get(collection string, id int64) (json.RawMessage, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	col, ok := db.state.Collections[collection]
	if !ok {
		return nil, ErrNotFound
	}
	raw, ok := col.Docs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return append(json.RawMessage(nil), raw...), nil
}

func (db *DB) Delete(collection string, id int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	col, ok := db.state.Collections[collection]
	if !ok {
		return ErrNotFound
	}
	raw, ok := col.Docs[id]
	if !ok {
		return ErrNotFound
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return err
	}
	db.removeDocFromIndexes(col, id, doc)
	delete(col.Docs, id)
	db.recordDocDelete(col.Name, id, raw)
	return db.commitLocked()
}

func (db *DB) Patch(collection string, id int64, patch []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	col, ok := db.state.Collections[collection]
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
	if err := db.putLocked(col, id, newRaw); err != nil {
		return err
	}
	return db.commitLocked()
}

func (db *DB) MergeOrPut(collection string, id int64, patch []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	col := db.ensureCollectionLocked(collection)
	raw, ok := col.Docs[id]
	if !ok {
		if id > col.NextID {
			col.NextID = id
		}
		if err := db.putLocked(col, id, patch); err != nil {
			return err
		}
		return db.commitLocked()
	}
	newRaw, err := applyMergePatch(raw, patch)
	if err != nil {
		return err
	}
	if err := db.putLocked(col, id, newRaw); err != nil {
		return err
	}
	return db.commitLocked()
}

func (db *DB) EnsureIndex(collection, path string, kind IndexKind, unique bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if path == "" || path[0] != '/' {
		return withCodef(CodeInvalidQuery, "index path must be JSON pointer, got %q", path)
	}
	if kind != IndexString && kind != IndexInt64 && kind != IndexFloat {
		return withCodef(CodeInvalidQuery, "unsupported index kind: %q", kind)
	}
	col := db.ensureCollectionLocked(collection)
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
	if err := db.rebuildIndex(col, k); err != nil {
		delete(col.Indexes, k)
		delete(col.runtime, k)
		return err
	}
	db.markFullDirty()
	return db.commitLocked()
}

func (db *DB) EnsureIndexMode(collection, path string, mode IndexMode) error {
	kind, unique, err := indexModeParts(mode)
	if err != nil {
		return err
	}
	return db.EnsureIndex(collection, path, kind, unique)
}

func (db *DB) RemoveIndex(collection, path string, kind IndexKind) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	col, ok := db.state.Collections[collection]
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
	db.markFullDirty()
	return db.commitLocked()
}

func (db *DB) RemoveIndexMode(collection, path string, mode IndexMode) error {
	kind, _, err := indexModeParts(mode)
	if err != nil {
		return err
	}
	return db.RemoveIndex(collection, path, kind)
}

type ExecVisitor func(doc Document, step *int64) error

type ExecOptions struct {
	Skip    int64
	Limit   int64
	Visitor ExecVisitor
	Log     io.StringWriter
}

type queryMode int

const (
	modeList queryMode = iota
	modeCount
	modeExec
	modeUpdate
)

type matchedDoc struct {
	id   int64
	raw  json.RawMessage
	node any
}

func (db *DB) Exec(q *Query, opts *ExecOptions) (int64, error) {
	if q == nil {
		return 0, withCode(CodeInvalidQuery, "nil query")
	}
	mutates := q.parsed.action != actionNone
	if mutates {
		db.mu.Lock()
		defer db.mu.Unlock()
		if db.closed {
			return 0, ErrClosed
		}
		docs, cnt, changed, err := db.runQueryLocked(db.state, q, modeExec, opts)
		if err != nil {
			return 0, err
		}
		_ = docs
		if changed {
			if err := db.commitLocked(); err != nil {
				return 0, err
			}
		}
		return cnt, nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return 0, ErrClosed
	}
	docs, cnt, _, err := db.runQueryLocked(db.state, q, modeExec, opts)
	_ = docs
	return cnt, err
}

func (db *DB) ListQuery(q *Query, limit int64) ([]Document, error) {
	if q == nil {
		return nil, withCode(CodeInvalidQuery, "nil query")
	}
	opts := &ExecOptions{Limit: limit}
	mutates := q.parsed.action != actionNone
	if mutates {
		db.mu.Lock()
		defer db.mu.Unlock()
		if db.closed {
			return nil, ErrClosed
		}
		docs, _, changed, err := db.runQueryLocked(db.state, q, modeList, opts)
		if err != nil {
			return nil, err
		}
		if changed {
			if err := db.commitLocked(); err != nil {
				return nil, err
			}
		}
		return docs, nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	docs, _, _, err := db.runQueryLocked(db.state, q, modeList, opts)
	return docs, err
}

func (db *DB) Count(q *Query, limit int64) (int64, error) {
	if q == nil {
		return 0, withCode(CodeInvalidQuery, "nil query")
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return 0, ErrClosed
	}
	_, cnt, _, err := db.runQueryLocked(db.state, q, modeCount, &ExecOptions{Limit: limit})
	return cnt, err
}

func (db *DB) UpdateQuery(q *Query, limit int64) (int64, error) {
	if q == nil {
		return 0, withCode(CodeInvalidQuery, "nil query")
	}
	if q.parsed.action == actionNone {
		return 0, withCode(CodeInvalidQuery, "query has no update action")
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return 0, ErrClosed
	}
	_, cnt, changed, err := db.runQueryLocked(db.state, q, modeUpdate, &ExecOptions{Limit: limit})
	if err != nil {
		return 0, err
	}
	if changed {
		if err := db.commitLocked(); err != nil {
			return 0, err
		}
	}
	return cnt, nil
}

func (db *DB) runQueryLocked(state *dbState, q *Query, mode queryMode, opts *ExecOptions) ([]Document, int64, bool, error) {
	if opts == nil {
		opts = &ExecOptions{}
	}
	collection := q.collection
	pq := q.parsed
	col := state.Collections[collection]
	if col == nil {
		if mode == modeUpdate && pq.action == actionUpsert {
			col = db.ensureCollectionOnStateLocked(state, collection)
		} else {
			return []Document{}, 0, false, nil
		}
	}
	var candidateIDs []int64
	usedIndex := false
	if !pq.noidx {
		if ids, ok := pq.filter.candidate(col, q); ok {
			candidateIDs = ids
			usedIndex = true
		}
	}
	if candidateIDs == nil {
		candidateIDs = make([]int64, 0, len(col.Docs))
		for id := range col.Docs {
			candidateIDs = append(candidateIDs, id)
		}
		sort.Slice(candidateIDs, func(i, j int) bool { return candidateIDs[i] < candidateIDs[j] })
	}
	if opts.Log != nil {
		if usedIndex {
			_, _ = opts.Log.WriteString("[INDEX] SELECTED\n")
		} else {
			_, _ = opts.Log.WriteString("[COLLECTOR] PLAIN\n")
		}
	}

	matchedDocs := make([]matchedDoc, 0, len(candidateIDs))
	for _, id := range candidateIDs {
		raw := col.Docs[id]
		var node any
		if err := json.Unmarshal(raw, &node); err != nil {
			return nil, 0, false, err
		}
		ok, err := pq.filter.match(node, id, q, state)
		if err != nil {
			return nil, 0, false, err
		}
		if ok {
			matchedDocs = append(matchedDocs, matchedDoc{id: id, raw: append(json.RawMessage(nil), raw...), node: node})
		}
	}

	if pq.sort != nil {
		sortPath, err := pq.sort.path.resolve(q)
		if err != nil {
			return nil, 0, false, err
		}
		sort.Slice(matchedDocs, func(i, j int) bool {
			li := matchedDocs[i]
			lj := matchedDocs[j]
			lv, _ := pointerGet(li.node, sortPath)
			rv, _ := pointerGet(lj.node, sortPath)
			cmp := genericCmp(lv, rv)
			if cmp == 0 {
				if pq.sort.desc {
					return li.id > lj.id
				}
				return li.id < lj.id
			}
			if pq.sort.desc {
				return cmp > 0
			}
			return cmp < 0
		})
	} else {
		sort.Slice(matchedDocs, func(i, j int) bool { return matchedDocs[i].id < matchedDocs[j].id })
		if pq.inverse {
			for i, j := 0, len(matchedDocs)-1; i < j; i, j = i+1, j-1 {
				matchedDocs[i], matchedDocs[j] = matchedDocs[j], matchedDocs[i]
			}
		}
	}

	skip := pq.skip
	if opts.Skip > 0 {
		skip = int(opts.Skip)
	}
	limit := pq.limit
	if opts.Limit > 0 {
		limit = int(opts.Limit)
	}
	start := skip
	if start > len(matchedDocs) {
		start = len(matchedDocs)
	}
	end := len(matchedDocs)
	if limit >= 0 && start+limit < end {
		end = start + limit
	}
	window := matchedDocs[start:end]

	changed := false
	affectedCount := int64(0)
	if mode == modeUpdate || (mode == modeExec && pq.action != actionNone) || (mode == modeList && pq.action != actionNone) {
		affected, err := db.applyActionLocked(state, col, q, window)
		if err != nil {
			return nil, 0, false, err
		}
		affectedCount = affected
		if affected > 0 {
			changed = true
		}
		if pq.action == actionDelete {
			for i := range window {
				window[i].raw = nil
			}
		}
	}

	docs := make([]Document, 0, len(window))
	for _, m := range window {
		if len(m.raw) == 0 {
			continue
		}
		raw := m.raw
		if pq.projection != nil {
			proj, err := db.applyProjectionLocked(state, q, raw, pq.projection)
			if err != nil {
				return nil, 0, changed, err
			}
			raw = proj
		}
		docs = append(docs, Document{ID: m.id, Raw: raw})
	}

	if mode == modeExec && opts.Visitor != nil {
		for i := 0; i < len(docs); {
			step := int64(1)
			if err := opts.Visitor(docs[i], &step); err != nil {
				return nil, 0, changed, err
			}
			if step <= 0 {
				break
			}
			i += int(step)
		}
	}

	if mode == modeCount {
		return nil, int64(len(docs)), changed, nil
	}
	if mode == modeUpdate {
		return nil, affectedCount, changed, nil
	}
	if mode == modeExec {
		return nil, int64(len(docs)), changed, nil
	}
	return docs, int64(len(docs)), changed, nil
}

func (db *DB) applyActionLocked(state *dbState, col *collectionState, q *Query, window []matchedDoc) (int64, error) {
	pq := q.parsed
	switch pq.action {
	case actionNone:
		return 0, nil
	case actionDelete:
		for _, m := range window {
			db.removeDocFromIndexes(col, m.id, m.node)
			delete(col.Docs, m.id)
			db.recordDocDelete(col.Name, m.id, m.raw)
		}
		return int64(len(window)), nil
	case actionApply:
		payload, err := pq.actionArg.resolve(q)
		if err != nil {
			return 0, err
		}
		for _, m := range window {
			patched, err := applyPatchPayload(m.raw, payload)
			if err != nil {
				return 0, err
			}
			if err := db.putLocked(col, m.id, patched); err != nil {
				return 0, err
			}
		}
		return int64(len(window)), nil
	case actionUpsert:
		payload, err := pq.actionArg.resolve(q)
		if err != nil {
			return 0, err
		}
		if len(window) == 0 {
			raw, err := json.Marshal(payload)
			if err != nil {
				return 0, err
			}
			if col == nil {
				col = db.ensureCollectionOnStateLocked(state, q.collection)
			}
			col.NextID++
			if err := db.putLocked(col, col.NextID, raw); err != nil {
				col.NextID--
				return 0, err
			}
			return 1, nil
		}
		for _, m := range window {
			patched, err := applyPatchPayload(m.raw, payload)
			if err != nil {
				return 0, err
			}
			if err := db.putLocked(col, m.id, patched); err != nil {
				return 0, err
			}
		}
		return int64(len(window)), nil
	default:
		return 0, nil
	}
}

func applyPatchPayload(raw []byte, payload any) ([]byte, error) {
	patch, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return applyJSONPatch(raw, patch)
}

func (db *DB) applyProjectionLocked(state *dbState, q *Query, raw []byte, spec *projectionSpec) ([]byte, error) {
	var src any
	if err := json.Unmarshal(raw, &src); err != nil {
		return nil, err
	}
	var out any = map[string]any{}
	for _, term := range spec.terms {
		if term.all {
			if term.include {
				cl, err := cloneAny(src)
				if err != nil {
					return nil, err
				}
				out = cl
			} else {
				out = map[string]any{}
			}
			continue
		}
		path, err := term.path.resolve(q)
		if err != nil {
			return nil, err
		}
		if term.include {
			v, ok := pointerGet(src, path)
			if !ok {
				continue
			}
			if term.join != "" {
				jv, err := db.joinValueLocked(state, term.join, v)
				if err != nil {
					return nil, err
				}
				v = jv
			}
			if _, ok := out.(map[string]any); !ok {
				out = map[string]any{}
			}
			if err := pointerSet(out, path, v, true); err != nil {
				return nil, err
			}
		} else {
			_ = pointerRemove(out, path)
		}
	}
	return json.Marshal(out)
}

func (db *DB) joinValueLocked(state *dbState, coll string, v any) (any, error) {
	c := state.Collections[coll]
	if c == nil {
		return nil, nil
	}
	joinOne := func(id int64) (any, error) {
		raw, ok := c.Docs[id]
		if !ok {
			return nil, nil
		}
		var out any
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, err
		}
		return out, nil
	}
	switch x := v.(type) {
	case []any:
		res := make([]any, 0, len(x))
		for _, it := range x {
			id, ok := toInt64(it)
			if !ok {
				continue
			}
			j, err := joinOne(id)
			if err != nil {
				return nil, err
			}
			if j != nil {
				res = append(res, j)
			}
		}
		return res, nil
	default:
		id, ok := toInt64(v)
		if !ok {
			return nil, nil
		}
		return joinOne(id)
	}
}

func cloneAny(v any) (any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func genericCmp(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	if an, ok := toFloat64(a); ok {
		if bn, ok := toFloat64(b); ok {
			switch {
			case an < bn:
				return -1
			case an > bn:
				return 1
			default:
				return 0
			}
		}
	}
	as := fmt.Sprint(a)
	bs := fmt.Sprint(b)
	switch {
	case as < bs:
		return -1
	case as > bs:
		return 1
	default:
		return 0
	}
}

func (db *DB) ensureCollectionLocked(name string) *collectionState {
	return db.ensureCollectionOnStateLocked(db.state, name)
}

func (db *DB) ensureCollectionOnStateLocked(state *dbState, name string) *collectionState {
	if col, ok := state.Collections[name]; ok {
		col.Name = name
		if col.runtime == nil {
			col.initRuntime()
		}
		return col
	}
	state.NextCollectionID++
	col := &collectionState{Name: name, DBID: state.NextCollectionID, NextID: 0, Docs: make(map[int64]json.RawMessage), Indexes: make(map[string]indexState), runtime: make(map[string]*indexRuntime)}
	state.Collections[name] = col
	return col
}

func (db *DB) putLocked(col *collectionState, id int64, raw []byte) error {
	canon, doc, err := normalizeRawJSON(raw)
	if err != nil {
		return err
	}
	var oldRaw json.RawMessage
	if old, ok := col.Docs[id]; ok {
		oldRaw = append(json.RawMessage(nil), old...)
		var oldDoc any
		if err := json.Unmarshal(old, &oldDoc); err != nil {
			return err
		}
		db.removeDocFromIndexes(col, id, oldDoc)
	}
	if err := db.addDocToIndexes(col, id, doc); err != nil {
		if old, ok := col.Docs[id]; ok {
			var oldDoc any
			if err := json.Unmarshal(old, &oldDoc); err == nil {
				_ = db.addDocToIndexes(col, id, oldDoc)
			}
		}
		return err
	}
	col.Docs[id] = canon
	db.recordDocPut(col.Name, id, oldRaw, canon)
	return nil
}

func normalizeRawJSON(raw []byte) (json.RawMessage, any, error) {
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, nil, err
	}
	canon, err := json.Marshal(doc)
	if err != nil {
		return nil, nil, err
	}
	return canon, doc, nil
}

func (db *DB) rebuildAllIndexes() error {
	for _, col := range db.state.Collections {
		col.initRuntime()
		for k := range col.Indexes {
			if err := db.rebuildIndex(col, k); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB) rebuildIndex(col *collectionState, key string) error {
	idx, ok := col.runtime[key]
	if !ok {
		return nil
	}
	idx.unique = make(map[string]int64)
	idx.multi = make(map[string]map[int64]struct{})
	for id, raw := range col.Docs {
		var doc any
		if err := json.Unmarshal(raw, &doc); err != nil {
			return err
		}
		vals := valuesForIndex(doc, idx.def.Path, idx.def.Kind)
		for _, v := range vals {
			if idx.def.Unique {
				if cur, ok := idx.unique[v]; ok && cur != id {
					return ErrUniqueConstraint
				}
				idx.unique[v] = id
			} else {
				set := idx.multi[v]
				if set == nil {
					set = make(map[int64]struct{})
					idx.multi[v] = set
				}
				set[id] = struct{}{}
			}
		}
	}
	return nil
}

func (db *DB) addDocToIndexes(col *collectionState, id int64, doc any) error {
	type entry struct {
		rt  *indexRuntime
		key string
	}
	entries := make([]entry, 0, len(col.runtime))
	for _, rt := range col.runtime {
		vals := valuesForIndex(doc, rt.def.Path, rt.def.Kind)
		for _, k := range vals {
			if rt.def.Unique {
				if cur, ok := rt.unique[k]; ok && cur != id {
					return ErrUniqueConstraint
				}
			}
			entries = append(entries, entry{rt: rt, key: k})
		}
	}
	for _, e := range entries {
		if e.rt.def.Unique {
			e.rt.unique[e.key] = id
		} else {
			set := e.rt.multi[e.key]
			if set == nil {
				set = make(map[int64]struct{})
				e.rt.multi[e.key] = set
			}
			set[id] = struct{}{}
		}
	}
	return nil
}

func (db *DB) removeDocFromIndexes(col *collectionState, id int64, doc any) {
	for _, rt := range col.runtime {
		vals := valuesForIndex(doc, rt.def.Path, rt.def.Kind)
		for _, k := range vals {
			if rt.def.Unique {
				if cur, ok := rt.unique[k]; ok && cur == id {
					delete(rt.unique, k)
				}
				continue
			}
			if set, ok := rt.multi[k]; ok {
				delete(set, id)
				if len(set) == 0 {
					delete(rt.multi, k)
				}
			}
		}
	}
}

func valuesForIndex(doc any, path string, kind IndexKind) []string {
	v, ok := pointerGet(doc, path)
	if !ok {
		return nil
	}
	out := make([]string, 0, 1)
	appendOne := func(x any) {
		if s, ok := normalizeIndexValue(x, kind); ok {
			out = append(out, s)
		}
	}
	switch arr := v.(type) {
	case []any:
		for _, it := range arr {
			appendOne(it)
		}
	default:
		appendOne(v)
	}
	return out
}

func normalizeIndexValue(v any, kind IndexKind) (string, bool) {
	switch kind {
	case IndexString:
		s, ok := v.(string)
		if !ok {
			return "", false
		}
		return s, true
	case IndexInt64:
		i, ok := toInt64(v)
		if !ok {
			return "", false
		}
		return strconv.FormatInt(i, 10), true
	case IndexFloat:
		f, ok := toFloat64(v)
		if !ok {
			return "", false
		}
		return strconv.FormatFloat(f, 'f', -1, 64), true
	default:
		return "", false
	}
}

func (db *DB) commitLocked() error {
	if db.dirtyFull {
		return db.persistFullLocked()
	}
	if !db.dirtyMeta && len(db.pending) == 0 {
		return nil
	}
	return db.persistPendingLocked()
}

func (db *DB) markMetaDirty() {
	db.dirtyMeta = true
}

func (db *DB) markFullDirty() {
	db.dirtyFull = true
	db.dirtyMeta = true
}

func (db *DB) recordDocPut(collection string, id int64, oldRaw, newRaw json.RawMessage) {
	db.pending = append(db.pending, storageMutation{
		kind:       mutationPut,
		collection: collection,
		id:         id,
		oldRaw:     append(json.RawMessage(nil), oldRaw...),
		newRaw:     append(json.RawMessage(nil), newRaw...),
	})
	db.dirtyMeta = true
}

func (db *DB) recordDocDelete(collection string, id int64, oldRaw json.RawMessage) {
	db.pending = append(db.pending, storageMutation{
		kind:       mutationDelete,
		collection: collection,
		id:         id,
		oldRaw:     append(json.RawMessage(nil), oldRaw...),
	})
	db.dirtyMeta = true
}

func (db *DB) truncatePending(n int, dirtyMeta, dirtyFull bool) {
	db.pending = db.pending[:n]
	db.dirtyMeta = dirtyMeta
	db.dirtyFull = dirtyFull
}

func (db *DB) clearPending() {
	db.pending = nil
	db.dirtyMeta = false
	db.dirtyFull = false
}

func (db *DB) persistPendingLocked() error {
	meta, err := encodeCatalog(db.state)
	if err != nil {
		return err
	}
	b := db.engine.NewBatch()
	defer b.Close()
	if err := b.Set(keyMetaState, meta); err != nil {
		return err
	}
	for _, m := range db.pending {
		col := db.state.Collections[m.collection]
		if col != nil {
			if err := b.Set(keySeq(m.collection), putU64(nil, uint64(col.NextID))); err != nil {
				return err
			}
		}
		switch m.kind {
		case mutationPut:
			if len(m.oldRaw) > 0 {
				if err := db.deleteIndexKeys(b, m.collection, m.id, m.oldRaw); err != nil {
					return err
				}
			}
			if err := b.Set(keyDoc(m.collection, m.id), m.newRaw); err != nil {
				return err
			}
			if err := db.setIndexKeys(b, m.collection, m.id, m.newRaw); err != nil {
				return err
			}
		case mutationDelete:
			if err := db.deleteIndexKeys(b, m.collection, m.id, m.oldRaw); err != nil {
				return err
			}
			if err := b.Delete(keyDoc(m.collection, m.id)); err != nil {
				return err
			}
		}
	}
	if err := b.Commit(); err != nil {
		return err
	}
	db.clearPending()
	return nil
}

func (db *DB) deleteIndexKeys(b StorageBatch, collection string, id int64, raw []byte) error {
	col := db.state.Collections[collection]
	if col == nil || len(raw) == 0 {
		return nil
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return err
	}
	for _, idx := range col.Indexes {
		for _, value := range valuesForIndex(doc, idx.Path, idx.Kind) {
			if idx.Unique {
				if err := b.Delete(keyUniqueIndex(collection, idx, value)); err != nil {
					return err
				}
			} else if err := b.Delete(keyIndex(collection, idx, value, id)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB) setIndexKeys(b StorageBatch, collection string, id int64, raw []byte) error {
	col := db.state.Collections[collection]
	if col == nil || len(raw) == 0 {
		return nil
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return err
	}
	for _, idx := range col.Indexes {
		for _, value := range valuesForIndex(doc, idx.Path, idx.Kind) {
			if idx.Unique {
				if err := b.Set(keyUniqueIndex(collection, idx, value), putU64(nil, uint64(id))); err != nil {
					return err
				}
			} else if err := b.Set(keyIndex(collection, idx, value, id), []byte{1}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB) persistFullLocked() error {
	meta, err := encodeCatalog(db.state)
	if err != nil {
		return err
	}
	b := db.engine.NewBatch()
	defer b.Close()
	for _, prefix := range [][]byte{{keyTagDoc}, {keyTagSeq}, {keyTagIdx}, {keyTagUIdx}} {
		if err := b.DeleteRange(prefix, prefixEnd(prefix)); err != nil {
			return err
		}
	}
	if err := b.Set(keyMetaState, meta); err != nil {
		return err
	}
	for name, col := range db.state.Collections {
		if err := b.Set(keySeq(name), putU64(nil, uint64(col.NextID))); err != nil {
			return err
		}
		for id, raw := range col.Docs {
			if err := b.Set(keyDoc(name, id), raw); err != nil {
				return err
			}
			var doc any
			if err := json.Unmarshal(raw, &doc); err != nil {
				return err
			}
			for _, idx := range col.Indexes {
				for _, value := range valuesForIndex(doc, idx.Path, idx.Kind) {
					if idx.Unique {
						if err := b.Set(keyUniqueIndex(name, idx, value), putU64(nil, uint64(id))); err != nil {
							return err
						}
					} else if err := b.Set(keyIndex(name, idx, value, id), []byte{1}); err != nil {
						return err
					}
				}
			}
		}
	}
	if err := b.Commit(); err != nil {
		return err
	}
	db.clearPending()
	return nil
}
