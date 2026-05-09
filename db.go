package ejdb

import (
	"bytes"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/go-json-experiment/json/jsontext"
)

type DB struct {
	mu             sync.RWMutex
	path           string
	engine         StorageEngine
	state          *dbState
	closed         bool
	sortBufferSize int64

	pending   []storageMutation
	dirtyMeta bool
	dirtyFull bool
}

const defaultSortBufferSize = 16 * 1024 * 1024

type storageMutationKind uint8

const (
	mutationPut storageMutationKind = iota + 1
	mutationDelete
)

type storageMutation struct {
	kind       storageMutationKind
	collection string
	id         int64
	oldRaw     jsontext.Value
	newRaw     jsontext.Value
	oldDoc     any
	newDoc     any
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
		path:           opts.Path,
		engine:         engine,
		sortBufferSize: opts.SortBufferSize,
	}
	if db.sortBufferSize <= 0 {
		db.sortBufferSize = defaultSortBufferSize
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
	return nil
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
		if c.Indexes == nil {
			c.Indexes = make(map[string]indexState)
		}
		c.initRuntime()
	}
}

func (db *DB) loadDocs() error {
	return nil
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
	col, ok := db.state.Collections[name]
	if !ok {
		return nil
	}
	delete(db.state.Collections, name)
	return db.dropCollectionDataAndCommitLocked(col)
}

func (db *DB) RenameCollection(oldName, newName string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	col, ok := db.state.Collections[oldName]
	if !ok {
		return ErrCollectionAbsent
	}
	if _, ok := db.state.Collections[newName]; ok {
		return ErrCollectionExists
	}
	delete(db.state.Collections, oldName)
	col.Name = newName
	db.state.Collections[newName] = col
	db.markMetaDirty()
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

func (db *DB) Get(collection string, id int64) (jsontext.Value, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	col, ok := db.state.Collections[collection]
	if !ok {
		return nil, ErrNotFound
	}
	raw, err := db.getDocRaw(db.engine, col, id)
	if err != nil {
		return nil, err
	}
	return raw, nil
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
	raw, doc, err := db.getDocNode(db.engine, col, id)
	if err != nil {
		return err
	}
	db.recordDocDelete(col.Name, id, raw, doc)
	if col.RNum > 0 {
		col.RNum--
	}
	db.adjustIndexRNums(col, doc, nil)
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
	raw, err := db.getDocRaw(db.engine, col, id)
	if err != nil {
		return err
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
	raw, err := db.getDocRaw(db.engine, col, id)
	if errors.Is(err, ErrNotFound) {
		if id > col.NextID {
			col.NextID = id
		}
		if err := db.putLocked(col, id, patch); err != nil {
			return err
		}
		return db.commitLocked()
	}
	if err != nil {
		return err
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
	if err := validateIndexDefinition(path, kind); err != nil {
		return err
	}
	col := db.ensureCollectionLocked(collection)
	for _, idx := range col.Indexes {
		if idx.Path == path && idx.Kind == kind {
			if idx.Unique != unique {
				return withCodef(CodeMismatchedIndexUniqueness, "index %s %s exists with different uniqueness mode", path, kind)
			}
			return nil
		}
	}
	k := indexKey(path, kind, unique)
	nextDBID := db.state.NextCollectionID + 1
	idx := indexState{Path: path, Kind: kind, Unique: unique, DBID: nextDBID}
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
	db.state.NextCollectionID = nextDBID
	db.markMetaDirty()
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
	var removed []indexState
	for k, idx := range col.Indexes {
		if idx.Path == path && idx.Kind == kind {
			removed = append(removed, idx)
			delete(col.Indexes, k)
			delete(col.runtime, k)
		}
	}
	if len(removed) == 0 {
		return ErrIndexNotFound
	}
	return db.dropIndexesDataAndCommitLocked(removed)
}

func (db *DB) RemoveIndexMode(collection, path string, mode IndexMode) error {
	kind, unique, err := indexModeParts(mode)
	if err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	col, ok := db.state.Collections[collection]
	if !ok {
		return nil
	}
	k := indexKey(path, kind, unique)
	idx, ok := col.Indexes[k]
	if !ok {
		return ErrIndexNotFound
	}
	delete(col.Indexes, k)
	delete(col.runtime, k)
	return db.dropIndexesDataAndCommitLocked([]indexState{idx})
}

func validateIndexDefinition(path string, kind IndexKind) error {
	if path == "" || path[0] != '/' {
		return withCodef(CodeInvalidIndexMode, "index path must be JSON pointer, got %q", path)
	}
	if kind != IndexString && kind != IndexInt64 && kind != IndexFloat {
		return withCodef(CodeInvalidIndexMode, "unsupported index kind: %q", kind)
	}
	tokens, err := pointerTokens(path)
	if err != nil {
		return withCodef(CodeInvalidIndexMode, "%v", err)
	}
	for _, tok := range tokens {
		if tok == "*" || tok == "**" {
			return withCodef(CodeInvalidIndexMode, "index path must not contain wildcard token: %s", path)
		}
	}
	return nil
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
	id          int64
	raw         jsontext.Value
	node        any
	sortValues  []any
	spillOffset int64
	spillSize   int64
}

type queryReader interface {
	Get(key []byte) ([]byte, error)
	NewIterator(lower, upper []byte) (StorageIterator, error)
}

type sortSpill struct {
	file *os.File
	path string
	pos  int64
}

func newSortSpill() (*sortSpill, error) {
	f, err := os.CreateTemp("", "ejdb-sort-*")
	if err != nil {
		return nil, err
	}
	return &sortSpill{file: f, path: f.Name()}, nil
}

func (s *sortSpill) write(raw []byte) (int64, int64, error) {
	off := s.pos
	n, err := s.file.Write(raw)
	if err != nil {
		return 0, 0, err
	}
	if n != len(raw) {
		return 0, 0, io.ErrShortWrite
	}
	s.pos += int64(n)
	return off, int64(n), nil
}

func (s *sortSpill) read(off, size int64) ([]byte, error) {
	buf := make([]byte, size)
	_, err := s.file.ReadAt(buf, off)
	return buf, err
}

func (s *sortSpill) close() {
	if s == nil {
		return
	}
	if s.file != nil {
		_ = s.file.Close()
	}
	if s.path != "" {
		_ = os.Remove(s.path)
	}
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
	sortPaths := make([]string, 0, len(pq.sorts))
	for _, spec := range pq.sorts {
		path, err := spec.resolve(q)
		if err != nil {
			return nil, 0, false, err
		}
		sortPaths = append(sortPaths, path)
	}
	skip, err := resolveIntOption(q, pq.skip, pq.skipPH, "skip")
	if err != nil {
		return nil, 0, false, err
	}
	if opts.Skip > 0 {
		skip = int(opts.Skip)
	}
	limit, err := resolveIntOption(q, pq.limit, pq.limitPH, "limit")
	if err != nil {
		return nil, 0, false, err
	}
	if opts.Limit > 0 {
		limit = int(opts.Limit)
	}
	useStorage := db.canUseStorageQuery(state)
	var reader queryReader
	var snap StorageSnapshot
	if useStorage {
		snap = db.engine.NewSnapshot()
		reader = snap
		defer snap.Close()
	} else {
		reader = db.engine
	}
	var candidateIDs []int64
	var scanCandidateIDs func(func(int64) (bool, error)) error
	var candidate candidatePlan
	usedIndex := false
	usedOrderIndex := false
	if !pq.noidx && useStorage && len(db.pending) == 0 {
		if plan, ok := pq.filter.candidate(col, q); ok {
			candidate = plan
			usedIndex = true
			var err error
			orderByCandidate := len(sortPaths) == 1 && plan.idx.Path == sortPaths[0]
			if useStorage {
				desc := orderByCandidate && pq.sorts[0].desc
				scanCandidateIDs = func(yield func(int64) (bool, error)) error {
					return db.scanIndexCandidateIDsFunc(reader, collection, plan, desc, yield)
				}
			} else {
				candidateIDs = memoryIndexCandidateIDs(plan)
				if orderByCandidate {
					candidateIDs = orderIDsByRuntimeIndex(plan.index, candidateIDs, pq.sorts[0].desc)
				}
			}
			if err != nil {
				return nil, 0, false, err
			}
			usedOrderIndex = orderByCandidate
		}
		if !usedIndex && len(sortPaths) == 1 {
			if plan, ok := chooseOrderByIndexPlan(col, sortPaths[0], pq.sorts[0].desc); ok {
				candidate = plan
				var err error
				if useStorage {
					desc := pq.sorts[0].desc
					scanCandidateIDs = func(yield func(int64) (bool, error)) error {
						return db.scanIndexCandidateIDsFunc(reader, collection, plan, desc, yield)
					}
				} else {
					candidateIDs = orderedIDsFromIndex(plan.index, pq.sorts[0].desc)
				}
				if err != nil {
					return nil, 0, false, err
				}
				usedOrderIndex = true
				usedIndex = true
			}
		}
	}
	if candidateIDs == nil && scanCandidateIDs == nil {
		var err error
		if useStorage {
			candidateIDs, err = db.scanDocumentIDs(reader, col)
			if err != nil {
				return nil, 0, false, err
			}
		} else {
			candidateIDs, err = db.scanDocumentIDs(db.engine, col)
			if err != nil {
				return nil, 0, false, err
			}
		}
	}
	if opts.Log != nil {
		if usedIndex {
			_, _ = opts.Log.WriteString("[INDEX] MATCHED  " + candidatePlanLog(candidate, usedOrderIndex) + "\n")
			if usedOrderIndex {
				_, _ = opts.Log.WriteString("[INDEX] SELECTED " + candidatePlanLog(candidate, true) + "\n")
			} else {
				_, _ = opts.Log.WriteString("[INDEX] SELECTED " + candidatePlanLog(candidate, false) + "\n")
			}
		}
		if len(sortPaths) > 0 && !usedOrderIndex {
			_, _ = opts.Log.WriteString("[COLLECTOR] SORTER\n")
		} else {
			_, _ = opts.Log.WriteString("[COLLECTOR] PLAIN\n")
		}
	}

	willSort := len(sortPaths) > 0 && !usedOrderIndex
	var spill *sortSpill
	defer func() {
		if spill != nil {
			spill.close()
		}
	}()
	var bufferedSortBytes int64
	capHint := len(candidateIDs)
	if usedOrderIndex && limit >= 0 && limit < capHint {
		capHint = limit
	}
	matchedDocs := make([]matchedDoc, 0, capHint)
	addMatchedDoc := func(id int64) error {
		raw, node, ok, err := db.docForQuery(reader, col, collection, id, useStorage)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		matches, err := pq.filter.match(node, id, q, state)
		if err != nil {
			return err
		}
		if matches {
			m := matchedDoc{id: id, raw: append(jsontext.Value(nil), raw...), node: node}
			if willSort {
				m.sortValues = make([]any, len(sortPaths))
				for i, path := range sortPaths {
					m.sortValues[i], _ = pointerGet(node, path)
				}
				bufferedSortBytes += int64(len(m.raw))
			}
			matchedDocs = append(matchedDocs, m)
			if willSort && bufferedSortBytes > db.sortBufferSize {
				if spill == nil {
					var err error
					spill, err = newSortSpill()
					if err != nil {
						return err
					}
				}
				if err := spillMatchedDocs(spill, matchedDocs); err != nil {
					return err
				}
				bufferedSortBytes = 0
			}
		}
		return nil
	}
	streamedWindow := usedOrderIndex && scanCandidateIDs != nil
	if streamedWindow {
		if limit != 0 {
			skipped := 0
			err := scanCandidateIDs(func(id int64) (bool, error) {
				if limit >= 0 && len(matchedDocs) >= limit {
					return false, nil
				}
				raw, node, ok, err := db.docForQuery(reader, col, collection, id, useStorage)
				if err != nil {
					return false, err
				}
				if !ok {
					return true, nil
				}
				matches, err := pq.filter.match(node, id, q, state)
				if err != nil {
					return false, err
				}
				if !matches {
					return true, nil
				}
				if skipped < skip {
					skipped++
					return true, nil
				}
				matchedDocs = append(matchedDocs, matchedDoc{id: id, raw: append(jsontext.Value(nil), raw...), node: node})
				if limit >= 0 && len(matchedDocs) >= limit {
					return false, nil
				}
				return true, nil
			})
			if err != nil {
				return nil, 0, false, err
			}
		}
	} else {
		if candidateIDs == nil && scanCandidateIDs != nil {
			var err error
			candidateIDs, err = db.scanIndexCandidateIDs(reader, collection, candidate, usedOrderIndex && len(pq.sorts) > 0 && pq.sorts[0].desc)
			if err != nil {
				return nil, 0, false, err
			}
		}
		for _, id := range candidateIDs {
			if err := addMatchedDoc(id); err != nil {
				return nil, 0, false, err
			}
		}
	}

	if usedOrderIndex {
		// The candidate iterator already produced documents in the requested index order.
	} else if len(pq.sorts) > 0 {
		sort.Slice(matchedDocs, func(i, j int) bool {
			li := matchedDocs[i]
			lj := matchedDocs[j]
			for idx := range sortPaths {
				lv := li.sortValues[idx]
				rv := lj.sortValues[idx]
				cmp := genericCmp(lv, rv)
				if cmp == 0 {
					continue
				}
				if pq.sorts[idx].desc {
					return cmp > 0
				}
				return cmp < 0
			}
			if pq.sorts[0].desc {
				return li.id > lj.id
			}
			return li.id < lj.id
		})
	} else {
		if usedIndex {
			if pq.inverse {
				for i, j := 0, len(matchedDocs)-1; i < j; i, j = i+1, j-1 {
					matchedDocs[i], matchedDocs[j] = matchedDocs[j], matchedDocs[i]
				}
			}
		} else {
			sort.Slice(matchedDocs, func(i, j int) bool { return matchedDocs[i].id > matchedDocs[j].id })
			if pq.inverse {
				for i, j := 0, len(matchedDocs)-1; i < j; i, j = i+1, j-1 {
					matchedDocs[i], matchedDocs[j] = matchedDocs[j], matchedDocs[i]
				}
			}
		}
	}

	start := skip
	end := len(matchedDocs)
	if streamedWindow {
		start = 0
	} else {
		if start > len(matchedDocs) {
			start = len(matchedDocs)
		}
		if limit >= 0 && start+limit < end {
			end = start + limit
		}
	}
	window := matchedDocs[start:end]
	if spill != nil {
		if err := hydrateMatchedDocs(spill, window); err != nil {
			return nil, 0, false, err
		}
		if opts.Log != nil {
			_, _ = opts.Log.WriteString("[SORTER] OVERFLOW\n")
		}
	}

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
	}
	if pq.count {
		if pq.action != actionNone || mode == modeUpdate {
			return nil, affectedCount, changed, nil
		}
		return nil, int64(len(window)), changed, nil
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
			if step == 0 {
				break
			}
			if step < 0 {
				i += int(step) + 1
			} else {
				i += int(step)
			}
			if i < 0 || i >= len(docs) {
				break
			}
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

func (db *DB) canUseStorageQuery(state *dbState) bool {
	return len(db.pending) == 0 && !db.dirtyFull
}

func (db *DB) docForQuery(reader queryReader, col *collectionState, collection string, id int64, useStorage bool) (jsontext.Value, any, bool, error) {
	if raw, doc, ok, deleted, err := db.pendingDocState(collection, id); ok || deleted || err != nil {
		if err != nil {
			return nil, nil, false, err
		}
		if deleted {
			return nil, nil, false, nil
		}
		if doc == nil {
			if err := decodeJSONDocument(raw, &doc); err != nil {
				return nil, nil, false, err
			}
		}
		return raw, doc, true, nil
	}
	if useStorage || reader != nil {
		stored, err := reader.Get(keyDoc(col.DBID, id))
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, nil, false, nil
			}
			return nil, nil, false, err
		}
		raw, node, err := decodeStoredDocument(stored)
		if err != nil {
			return nil, nil, false, err
		}
		return raw, node, true, nil
	}
	return nil, nil, false, nil
}

func (db *DB) scanDocumentIDs(reader queryReader, col *collectionState) ([]int64, error) {
	if col == nil {
		return nil, nil
	}
	prefix := keyDocPrefix(col.DBID)
	seen := make(map[int64]struct{})
	deleted := make(map[int64]struct{})
	ids := make([]int64, 0)
	err := scanPrefix(reader, prefix, func(key, _ []byte) error {
		_, id, ok := decodeDocKey(key)
		if !ok {
			return withCode(CodeInvalidQuery, "invalid document key in storage")
		}
		if _, ok := deleted[id]; ok {
			return nil
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
		return nil
	})
	for _, m := range db.pending {
		if m.collection != col.Name {
			continue
		}
		switch m.kind {
		case mutationPut:
			if _, ok := seen[m.id]; !ok {
				ids = append(ids, m.id)
				seen[m.id] = struct{}{}
			}
		case mutationDelete:
			deleted[m.id] = struct{}{}
		}
	}
	if len(deleted) > 0 {
		out := ids[:0]
		for _, id := range ids {
			if _, ok := deleted[id]; !ok {
				out = append(out, id)
			}
		}
		ids = out
	}
	return ids, err
}

func (db *DB) pendingDoc(collection string, id int64) (jsontext.Value, bool, bool, error) {
	raw, _, ok, deleted, err := db.pendingDocState(collection, id)
	return raw, ok, deleted, err
}

func (db *DB) pendingDocState(collection string, id int64) (jsontext.Value, any, bool, bool, error) {
	for i := len(db.pending) - 1; i >= 0; i-- {
		m := db.pending[i]
		if m.collection != collection || m.id != id {
			continue
		}
		if m.kind == mutationDelete {
			return nil, nil, false, true, nil
		}
		return append(jsontext.Value(nil), m.newRaw...), m.newDoc, true, false, nil
	}
	return nil, nil, false, false, nil
}

func chooseOrderByIndexPlan(col *collectionState, path string, desc bool) (candidatePlan, bool) {
	rt := findOrderByIndex(col, path)
	if rt == nil {
		return candidatePlan{}, false
	}
	init := "IWKV_CURSOR_AFTER_LAST"
	step := "IWKV_CURSOR_PREV"
	if desc {
		init = "IWKV_CURSOR_BEFORE_FIRST"
		step = "IWKV_CURSOR_NEXT"
	}
	return candidatePlan{
		index:      rt,
		idx:        rt.def,
		weight:     80,
		explain:    path,
		cursorInit: init,
		cursorStep: step,
	}, true
}

func (db *DB) scanIndexCandidateIDs(reader queryReader, collection string, plan candidatePlan, desc bool) ([]int64, error) {
	out := make([]int64, 0)
	err := db.scanIndexCandidateIDsFunc(reader, collection, plan, desc, func(id int64) (bool, error) {
		out = append(out, id)
		return true, nil
	})
	return out, err
}

func (db *DB) scanIndexCandidateIDsFunc(reader queryReader, collection string, plan candidatePlan, desc bool, yield func(int64) (bool, error)) error {
	_ = collection
	if plan.index == nil {
		return nil
	}
	idx := plan.idx
	if idx.Path == "" {
		idx = plan.index.def
	}
	seen := make(map[int64]struct{})
	dedupIDs := plan.op != "in"
	addID := func(id int64) (bool, error) {
		if dedupIDs {
			if _, ok := seen[id]; ok {
				return true, nil
			}
			seen[id] = struct{}{}
		}
		return yield(id)
	}
	type indexEntry struct {
		value string
		id    int64
	}
	sortEntries := func(entries []indexEntry, valueDesc bool) {
		sort.Slice(entries, func(i, j int) bool {
			cmp := compareIndexKeys(idx.Kind, entries[i].value, entries[j].value)
			if cmp == 0 {
				if valueDesc {
					return entries[i].id > entries[j].id
				}
				return entries[i].id < entries[j].id
			}
			if valueDesc {
				return cmp > 0
			}
			return cmp < 0
		})
	}
	emitEntries := func(entries []indexEntry) error {
		for _, entry := range entries {
			cont, err := addID(entry.id)
			if err != nil || !cont {
				return err
			}
		}
		return nil
	}
	scanNonUniqueValue := func(value string) error {
		entries := make([]indexEntry, 0)
		if err := scanPrefix(reader, keyIndexValuePrefix(idx, value), func(key, _ []byte) error {
			dbid, _, id, ok := decodeIndexKey(key)
			if !ok || dbid != idx.DBID {
				return withCode(CodeInvalidQuery, "invalid index key in storage")
			}
			entries = append(entries, indexEntry{value: value, id: id})
			return nil
		}); err != nil {
			return err
		}
		sortEntries(entries, false)
		return emitEntries(entries)
	}
	addUniqueValue := func(value string) error {
		raw, err := reader.Get(keyUniqueIndex(idx, value))
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			return err
		}
		id, err := parseU64(raw)
		if err != nil {
			return err
		}
		_, err = addID(int64(id))
		return err
	}
	scanOrderedRange := func(lower, upper []byte, valueDesc bool) error {
		if len(lower) > 0 && len(upper) > 0 && bytes.Compare(lower, upper) >= 0 {
			return nil
		}
		it, err := reader.NewIterator(lower, upper)
		if err != nil {
			return err
		}
		defer it.Close()
		decode := func(key, value []byte) (indexEntry, error) {
			if idx.Unique {
				dbid, indexedValue, ok := decodeUniqueIndexKey(key)
				if !ok || dbid != idx.DBID {
					return indexEntry{}, withCode(CodeInvalidQuery, "invalid unique index key in storage")
				}
				uid, err := parseU64(value)
				if err != nil {
					return indexEntry{}, err
				}
				return indexEntry{value: indexedValue, id: int64(uid)}, nil
			}
			dbid, indexedValue, id, ok := decodeIndexKey(key)
			if !ok || dbid != idx.DBID {
				return indexEntry{}, withCode(CodeInvalidQuery, "invalid index key in storage")
			}
			return indexEntry{value: indexedValue, id: id}, nil
		}
		if valueDesc {
			if rev, ok := it.(reverseStorageIterator); ok {
				for ok := rev.Last(); ok; ok = rev.Prev() {
					entry, err := decode(it.Key(), it.Value())
					if err != nil {
						return err
					}
					cont, err := addID(entry.id)
					if err != nil || !cont {
						return err
					}
				}
				return it.Error()
			}
			entries := make([]indexEntry, 0)
			for ok := it.First(); ok; ok = it.Next() {
				entry, err := decode(it.Key(), it.Value())
				if err != nil {
					return err
				}
				entries = append(entries, entry)
			}
			if err := it.Error(); err != nil {
				return err
			}
			sortEntries(entries, true)
			return emitEntries(entries)
		}
		for ok := it.First(); ok; ok = it.Next() {
			entry, err := decode(it.Key(), it.Value())
			if err != nil {
				return err
			}
			cont, err := addID(entry.id)
			if err != nil || !cont {
				return err
			}
		}
		return it.Error()
	}
	scanPath := func(match func(value string) bool, valueDesc bool) error {
		entries := make([]indexEntry, 0)
		if idx.Unique {
			if err := scanPrefix(reader, keyUniqueIndexPathPrefix(idx), func(key, value []byte) error {
				dbid, indexedValue, ok := decodeUniqueIndexKey(key)
				if !ok || dbid != idx.DBID {
					return withCode(CodeInvalidQuery, "invalid unique index key in storage")
				}
				if !match(indexedValue) {
					return nil
				}
				id, err := parseU64(value)
				if err != nil {
					return err
				}
				entries = append(entries, indexEntry{value: indexedValue, id: int64(id)})
				return nil
			}); err != nil {
				return err
			}
			sortEntries(entries, valueDesc)
			return emitEntries(entries)
		}
		if err := scanPrefix(reader, keyIndexPathPrefix(idx), func(key, _ []byte) error {
			dbid, indexedValue, id, ok := decodeIndexKey(key)
			if !ok || dbid != idx.DBID {
				return withCode(CodeInvalidQuery, "invalid index key in storage")
			}
			if match(indexedValue) {
				entries = append(entries, indexEntry{value: indexedValue, id: id})
			}
			return nil
		}); err != nil {
			return err
		}
		sortEntries(entries, valueDesc)
		return emitEntries(entries)
	}
	switch plan.op {
	case "":
		if idx.Kind == IndexInt64 || idx.Kind == IndexFloat {
			pathPrefix := indexScanPathPrefix(idx)
			if err := scanOrderedRange(pathPrefix, prefixEnd(pathPrefix), desc); err != nil {
				return err
			}
		} else if err := scanPath(func(string) bool { return true }, desc); err != nil {
			return err
		}
	case "=":
		value, ok := normalizeIndexValue(plan.value, idx.Kind)
		if !ok {
			return nil
		}
		if idx.Unique {
			if err := addUniqueValue(value); err != nil {
				return err
			}
		} else if err := scanNonUniqueValue(value); err != nil {
			return err
		}
	case "in":
		arr, ok := toAnySlice(plan.value)
		if !ok {
			return nil
		}
		for _, it := range arr {
			value, ok := normalizeIndexValue(it, idx.Kind)
			if !ok {
				continue
			}
			if idx.Unique {
				if err := addUniqueValue(value); err != nil {
					return err
				}
			} else if err := scanNonUniqueValue(value); err != nil {
				return err
			}
		}
	case "prefix":
		prefix, ok := jqPrefixString(toJQValue(plan.value))
		if !ok {
			return nil
		}
		if err := scanPath(func(value string) bool { return strings.HasPrefix(value, prefix) }, desc); err != nil {
			return err
		}
	case ">", ">=", "<", "<=":
		bound, ok := normalizeIndexValue(plan.value, idx.Kind)
		if !ok {
			return nil
		}
		secondBound := ""
		if plan.op2 != "" {
			var ok bool
			secondBound, ok = normalizeIndexValue(plan.value2, idx.Kind)
			if !ok {
				return nil
			}
		}
		valueDesc := desc
		if !desc && plan.op2 == "" && (plan.op == "<" || plan.op == "<=") {
			valueDesc = true
		}
		matchRange := func(value string) bool {
			cmp := compareIndexKeys(idx.Kind, value, bound)
			ok := (plan.op == ">" && cmp > 0) ||
				(plan.op == ">=" && cmp >= 0) ||
				(plan.op == "<" && cmp < 0) ||
				(plan.op == "<=" && cmp <= 0)
			if !ok || plan.op2 == "" {
				return ok
			}
			cmp = compareIndexKeys(idx.Kind, value, secondBound)
			return (plan.op2 == ">" && cmp > 0) ||
				(plan.op2 == ">=" && cmp >= 0) ||
				(plan.op2 == "<" && cmp < 0) ||
				(plan.op2 == "<=" && cmp <= 0)
		}
		if idx.Kind == IndexInt64 || idx.Kind == IndexFloat {
			lower, upper, ok := indexRangeKeyBounds(idx, plan, bound, secondBound)
			if !ok {
				return nil
			}
			if err := scanOrderedRange(lower, upper, valueDesc); err != nil {
				return err
			}
		} else if err := scanPath(matchRange, valueDesc); err != nil {
			return err
		}
	default:
		return nil
	}
	return nil
}

func indexRangeKeyBounds(idx indexState, plan candidatePlan, bound, secondBound string) ([]byte, []byte, bool) {
	type rangeSide struct {
		value     string
		inclusive bool
		set       bool
	}
	lower := rangeSide{}
	upper := rangeSide{}
	add := func(op, value string) bool {
		switch op {
		case ">":
			lower = rangeSide{value: value, inclusive: false, set: true}
		case ">=":
			lower = rangeSide{value: value, inclusive: true, set: true}
		case "<":
			upper = rangeSide{value: value, inclusive: false, set: true}
		case "<=":
			upper = rangeSide{value: value, inclusive: true, set: true}
		default:
			return false
		}
		return true
	}
	if !add(plan.op, bound) {
		return nil, nil, false
	}
	if plan.op2 != "" && !add(plan.op2, secondBound) {
		return nil, nil, false
	}
	pathPrefix := indexScanPathPrefix(idx)
	var lowerKey []byte
	if lower.set {
		prefix := indexScanValuePrefix(idx, lower.value)
		if lower.inclusive {
			lowerKey = prefix
		} else {
			lowerKey = prefixEnd(prefix)
		}
	} else {
		lowerKey = pathPrefix
	}
	var upperKey []byte
	if upper.set {
		prefix := indexScanValuePrefix(idx, upper.value)
		if upper.inclusive {
			upperKey = prefixEnd(prefix)
		} else {
			upperKey = prefix
		}
	} else {
		upperKey = prefixEnd(pathPrefix)
	}
	if len(lowerKey) > 0 && len(upperKey) > 0 && bytes.Compare(lowerKey, upperKey) > 0 {
		return nil, nil, false
	}
	return lowerKey, upperKey, true
}

func indexScanPathPrefix(idx indexState) []byte {
	if idx.Unique {
		return keyUniqueIndexPathPrefix(idx)
	}
	return keyIndexPathPrefix(idx)
}

func indexScanValuePrefix(idx indexState, value string) []byte {
	if idx.Unique {
		return keyUniqueIndexValuePrefix(idx, value)
	}
	return keyIndexValuePrefix(idx, value)
}

func memoryIndexCandidateIDs(plan candidatePlan) []int64 {
	if plan.index == nil {
		return nil
	}
	rt := plan.index
	ids := make(map[int64]struct{})
	addKey := func(k string) {
		if rt.def.Unique {
			if id, ok := rt.unique[k]; ok {
				ids[id] = struct{}{}
			}
			return
		}
		for id := range rt.multi[k] {
			ids[id] = struct{}{}
		}
	}
	switch plan.op {
	case "=":
		if k, ok := normalizeIndexValue(plan.value, rt.def.Kind); ok {
			addKey(k)
		}
	case "in":
		if arr, ok := toAnySlice(plan.value); ok {
			for _, it := range arr {
				if k, ok := normalizeIndexValue(it, rt.def.Kind); ok {
					addKey(k)
				}
			}
		}
	case "prefix":
		prefix, ok := jqPrefixString(toJQValue(plan.value))
		if ok {
			for k := range allRuntimeIndexKeys(rt) {
				if strings.HasPrefix(k, prefix) {
					addKey(k)
				}
			}
		}
	case ">", ">=", "<", "<=":
		if bound, ok := normalizeIndexValue(plan.value, rt.def.Kind); ok {
			secondBound := ""
			secondOK := false
			if plan.op2 != "" {
				secondBound, secondOK = normalizeIndexValue(plan.value2, rt.def.Kind)
			}
			for k := range allRuntimeIndexKeys(rt) {
				cmp := compareIndexKeys(rt.def.Kind, k, bound)
				ok := (plan.op == ">" && cmp > 0) || (plan.op == ">=" && cmp >= 0) || (plan.op == "<" && cmp < 0) || (plan.op == "<=" && cmp <= 0)
				if ok && secondOK {
					cmp = compareIndexKeys(rt.def.Kind, k, secondBound)
					ok = (plan.op2 == ">" && cmp > 0) || (plan.op2 == ">=" && cmp >= 0) || (plan.op2 == "<" && cmp < 0) || (plan.op2 == "<=" && cmp <= 0)
				}
				if ok {
					addKey(k)
				}
			}
		}
	case "":
		return orderedIDsFromIndex(rt, false)
	}
	res := make([]int64, 0, len(ids))
	for id := range ids {
		res = append(res, id)
	}
	sort.Slice(res, func(i, j int) bool { return res[i] < res[j] })
	return res
}

func allRuntimeIndexKeys(rt *indexRuntime) map[string]struct{} {
	keys := make(map[string]struct{}, len(rt.unique)+len(rt.multi))
	for k := range rt.unique {
		keys[k] = struct{}{}
	}
	for k := range rt.multi {
		keys[k] = struct{}{}
	}
	return keys
}

func orderIDsByRuntimeIndex(rt *indexRuntime, ids []int64, desc bool) []int64 {
	allowed := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		allowed[id] = struct{}{}
	}
	ordered := orderedIDsFromIndex(rt, desc)
	out := make([]int64, 0, len(ids))
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ordered {
		if _, ok := allowed[id]; !ok {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func candidatePlanLog(plan candidatePlan, orderBy bool) string {
	rt := plan.index
	if rt == nil {
		if orderBy {
			return "ORDERBY"
		}
		return ""
	}
	parts := make([]string, 0, 4)
	if rt.def.Unique {
		parts = append(parts, "UNIQUE")
	}
	switch rt.def.Kind {
	case IndexString:
		parts = append(parts, "STR")
	case IndexInt64:
		parts = append(parts, "I64")
	case IndexFloat:
		parts = append(parts, "F64")
	default:
		parts = append(parts, string(rt.def.Kind))
	}
	if len(parts) == 0 {
		parts = append(parts, "INDEX")
	}
	out := strings.Join(parts, "|") + " " + rt.def.Path
	if plan.explain != "" {
		out += " EXPR1: '" + plan.explain + "'"
	}
	if plan.explain2 != "" {
		out += " EXPR2: '" + plan.explain2 + "'"
	}
	if plan.cursorInit != "" {
		out += " INIT: " + plan.cursorInit
	}
	if plan.cursorStep != "" {
		out += " STEP: " + plan.cursorStep
	}
	if orderBy {
		out += " ORDERBY"
	}
	return out
}

func findOrderByIndex(col *collectionState, path string) *indexRuntime {
	keys := make([]string, 0, len(col.runtime))
	for key, rt := range col.runtime {
		if rt.def.Path == path {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		li := col.runtime[keys[i]].def
		lj := col.runtime[keys[j]].def
		if li.Kind == lj.Kind {
			if li.Unique == lj.Unique {
				return keys[i] < keys[j]
			}
			return li.Unique && !lj.Unique
		}
		return li.Kind < lj.Kind
	})
	if len(keys) == 0 {
		return nil
	}
	return col.runtime[keys[0]]
}

func orderedIDsFromIndex(rt *indexRuntime, desc bool) []int64 {
	type entry struct {
		key string
		ids []int64
	}
	entries := make([]entry, 0, len(rt.unique)+len(rt.multi))
	if rt.def.Unique {
		for key, id := range rt.unique {
			entries = append(entries, entry{key: key, ids: []int64{id}})
		}
	} else {
		for key, set := range rt.multi {
			ids := make([]int64, 0, len(set))
			for id := range set {
				ids = append(ids, id)
			}
			sort.Slice(ids, func(i, j int) bool {
				if desc {
					return ids[i] > ids[j]
				}
				return ids[i] < ids[j]
			})
			entries = append(entries, entry{key: key, ids: ids})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		cmp := compareIndexKeys(rt.def.Kind, entries[i].key, entries[j].key)
		if cmp == 0 {
			return entries[i].key < entries[j].key
		}
		if desc {
			return cmp > 0
		}
		return cmp < 0
	})
	out := make([]int64, 0)
	seen := make(map[int64]struct{})
	for _, e := range entries {
		for _, id := range e.ids {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out
}

func spillMatchedDocs(spill *sortSpill, docs []matchedDoc) error {
	for i := range docs {
		if len(docs[i].raw) == 0 || docs[i].spillSize > 0 {
			continue
		}
		off, size, err := spill.write(docs[i].raw)
		if err != nil {
			return err
		}
		docs[i].spillOffset = off
		docs[i].spillSize = size
		docs[i].raw = nil
		docs[i].node = nil
	}
	return nil
}

func hydrateMatchedDocs(spill *sortSpill, docs []matchedDoc) error {
	for i := range docs {
		if len(docs[i].raw) == 0 && docs[i].spillSize > 0 {
			raw, err := spill.read(docs[i].spillOffset, docs[i].spillSize)
			if err != nil {
				return err
			}
			docs[i].raw = raw
		}
		if docs[i].node == nil && len(docs[i].raw) > 0 {
			var node any
			if err := decodeJSONDocument(docs[i].raw, &node); err != nil {
				return err
			}
			docs[i].node = node
		}
	}
	return nil
}

func compareIndexKeys(kind IndexKind, a, b string) int {
	switch kind {
	case IndexInt64:
		ai, aok := decodeI64IndexKey(a)
		bi, bok := decodeI64IndexKey(b)
		if aok && bok {
			switch {
			case ai < bi:
				return -1
			case ai > bi:
				return 1
			default:
				return 0
			}
		}
	case IndexFloat:
		af, aok := decodeF64IndexKey(a)
		bf, bok := decodeF64IndexKey(b)
		if aok && bok {
			switch {
			case af < bf:
				return -1
			case af > bf:
				return 1
			default:
				return 0
			}
		}
	}
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func resolveIntOption(q *Query, literal int, ph *placeholderRef, name string) (int, error) {
	if ph == nil {
		return literal, nil
	}
	v, err := q.resolvePlaceholder(*ph)
	if err != nil {
		return 0, err
	}
	n, ok := toInt64(v)
	if !ok || n < 0 {
		return 0, withCodef(CodeInvalidPlaceholder, "%s placeholder must resolve to a non-negative integer, got %T", name, v)
	}
	if n > int64(^uint(0)>>1) {
		return 0, withCodef(CodeInvalidPlaceholder, "%s placeholder is too large: %d", name, n)
	}
	return int(n), nil
}

func (db *DB) applyActionLocked(state *dbState, col *collectionState, q *Query, window []matchedDoc) (int64, error) {
	pq := q.parsed
	switch pq.action {
	case actionNone:
		return 0, nil
	case actionDelete:
		for _, m := range window {
			db.recordDocDelete(col.Name, m.id, m.raw, m.node)
			if col.RNum > 0 {
				col.RNum--
			}
			db.adjustIndexRNums(col, m.node, nil)
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
			raw, err := marshalJSON(payload)
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
	patch, err := marshalJSON(payload)
	if err != nil {
		return nil, err
	}
	return applyJSONPatch(raw, patch)
}

func (db *DB) applyProjectionLocked(state *dbState, q *Query, raw []byte, spec *projectionSpec) ([]byte, error) {
	var src any
	if err := decodeJSONDocument(raw, &src); err != nil {
		return nil, err
	}
	var out any = map[string]any{}
	for i, term := range spec.terms {
		if term.all {
			if term.include {
				if i == 0 {
					cl, err := cloneAny(src)
					if err != nil {
						return nil, err
					}
					out = cl
				}
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
			if strings.Contains(path, "*") {
				_ = pointerRemovePattern(out, path)
			} else {
				_ = pointerRemove(out, path)
			}
		}
	}
	return marshalJSON(out)
}

func (db *DB) joinValueLocked(state *dbState, coll string, v any) (any, error) {
	c := state.Collections[coll]
	if c == nil {
		return nil, nil
	}
	joinOne := func(id int64) (any, error) {
		_, out, err := db.getDocNode(db.engine, c, id)
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		if err != nil {
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
	b, err := marshalJSON(v)
	if err != nil {
		return nil, err
	}
	var out any
	if err := unmarshalJSON(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func genericCmp(a, b any) int {
	cmp, ok := jqCompare(a, b)
	if ok {
		return cmp
	}
	return compareKindFallback(a, b)
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
	col := &collectionState{Name: name, DBID: state.NextCollectionID, NextID: 0, Indexes: make(map[string]indexState), runtime: make(map[string]*indexRuntime)}
	state.Collections[name] = col
	return col
}

func (db *DB) putLocked(col *collectionState, id int64, raw []byte) error {
	canon, doc, err := normalizeRawJSON(raw)
	if err != nil {
		return err
	}
	var oldRaw jsontext.Value
	var oldDoc any
	exists := false
	if raw, doc, ok, deleted, perr := db.pendingDocState(col.Name, id); ok || deleted || perr != nil {
		if perr != nil {
			return perr
		}
		if ok {
			oldRaw = append(jsontext.Value(nil), raw...)
			oldDoc = doc
			exists = true
		}
	} else {
		old, doc, getErr := db.getDocNode(db.engine, col, id)
		exists = getErr == nil
		if getErr != nil && !errors.Is(getErr, ErrNotFound) {
			return getErr
		}
		if exists {
			oldRaw = append(jsontext.Value(nil), old...)
			oldDoc = doc
		}
	}
	if err := db.checkUniqueConstraints(col, id, doc); err != nil {
		return err
	}
	if !exists {
		col.RNum++
	}
	db.adjustIndexRNums(col, oldDoc, doc)
	db.recordDocPut(col.Name, id, oldRaw, canon, oldDoc, doc)
	return nil
}

func (db *DB) getDocRaw(reader queryReader, col *collectionState, id int64) (jsontext.Value, error) {
	if col == nil {
		return nil, ErrNotFound
	}
	stored, err := reader.Get(keyDoc(col.DBID, id))
	if err != nil {
		return nil, err
	}
	raw, _, err := decodeStoredDocument(stored)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func (db *DB) getDocNode(reader queryReader, col *collectionState, id int64) (jsontext.Value, any, error) {
	if col == nil {
		return nil, nil, ErrNotFound
	}
	stored, err := reader.Get(keyDoc(col.DBID, id))
	if err != nil {
		return nil, nil, err
	}
	return decodeStoredDocument(stored)
}

func (db *DB) checkUniqueConstraints(col *collectionState, id int64, doc any) error {
	for _, idx := range col.Indexes {
		if !idx.Unique {
			continue
		}
		newVals := valuesForIndex(doc, idx.Path, idx.Kind)
		for _, value := range newVals {
			raw, err := db.engine.Get(keyUniqueIndex(idx, value))
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					continue
				}
				return err
			}
			cur, err := parseU64(raw)
			if err != nil {
				return err
			}
			if int64(cur) != id && !db.pendingDeletes(col.Name, int64(cur)) {
				return ErrUniqueConstraint
			}
		}
		if len(db.pending) == 0 || len(newVals) == 0 {
			continue
		}
		want := make(map[string]struct{}, len(newVals))
		for _, v := range newVals {
			want[v] = struct{}{}
		}
		pending := make(map[int64]storageMutation)
		deleted := make(map[int64]struct{})
		for _, m := range db.pending {
			if m.collection != col.Name || m.id == id {
				continue
			}
			switch m.kind {
			case mutationPut:
				pending[m.id] = m
				delete(deleted, m.id)
			case mutationDelete:
				delete(pending, m.id)
				deleted[m.id] = struct{}{}
			}
		}
		_ = deleted
		for otherID, m := range pending {
			other := m.newDoc
			if other == nil {
				if err := decodeJSONDocument(m.newRaw, &other); err != nil {
					return err
				}
			}
			for _, v := range valuesForIndex(other, idx.Path, idx.Kind) {
				if _, ok := want[v]; ok && otherID != id {
					return ErrUniqueConstraint
				}
			}
		}
	}
	return nil
}

func (db *DB) pendingDeletes(collection string, id int64) bool {
	for i := len(db.pending) - 1; i >= 0; i-- {
		m := db.pending[i]
		if m.collection != collection || m.id != id {
			continue
		}
		return m.kind == mutationDelete
	}
	return false
}

func (db *DB) adjustIndexRNums(col *collectionState, oldDoc, newDoc any) {
	for k, idx := range col.Indexes {
		oldVals := valuesForIndex(oldDoc, idx.Path, idx.Kind)
		newVals := valuesForIndex(newDoc, idx.Path, idx.Kind)
		idx.RNum += len(newVals) - len(oldVals)
		if idx.RNum < 0 {
			idx.RNum = 0
		}
		col.Indexes[k] = idx
		if rt := col.runtime[k]; rt != nil {
			rt.def = idx
		}
	}
}

func normalizeRawJSON(raw []byte) (jsontext.Value, any, error) {
	var doc any
	if err := decodeJSONDocument(raw, &doc); err != nil {
		return nil, nil, err
	}
	canon, err := marshalJSON(doc)
	if err != nil {
		return nil, nil, err
	}
	return canon, doc, nil
}

func (db *DB) encodeStoredRaw(raw []byte) ([]byte, error) {
	var doc any
	if err := decodeJSONDocument(raw, &doc); err != nil {
		return nil, err
	}
	return encodeStoredDocument(doc)
}

func (db *DB) encodeStoredDoc(raw []byte, doc any) ([]byte, error) {
	if doc == nil {
		return db.encodeStoredRaw(raw)
	}
	return encodeStoredDocument(doc)
}

func decodeJSONDocument(raw []byte, out *any) error {
	v, err := decodeJSONAny(raw)
	if err != nil {
		return err
	}
	*out = v
	return nil
}

func (db *DB) rebuildAllIndexes() error {
	return nil
}

func (db *DB) rebuildIndex(col *collectionState, key string) error {
	idx, ok := col.Indexes[key]
	if !ok {
		return nil
	}
	b := db.engine.NewBatch()
	defer b.Close()
	if err := b.DeleteRange(keyIndexPathPrefix(idx), prefixEnd(keyIndexPathPrefix(idx))); err != nil {
		return err
	}
	if err := b.DeleteRange(keyUniqueIndexPathPrefix(idx), prefixEnd(keyUniqueIndexPathPrefix(idx))); err != nil {
		return err
	}
	seen := make(map[string]int64)
	rnum := 0
	err := db.scanDocsForCollection(db.engine, col, func(id int64, raw jsontext.Value, doc any) error {
		vals := valuesForIndex(doc, idx.Path, idx.Kind)
		for _, v := range vals {
			if idx.Unique {
				if cur, ok := seen[v]; ok && cur != id {
					return ErrUniqueConstraint
				}
				seen[v] = id
				if err := b.Set(keyUniqueIndex(idx, v), putU64(nil, uint64(id))); err != nil {
					return err
				}
			} else if err := b.Set(keyIndex(idx, v, id), []byte{1}); err != nil {
				return err
			}
			rnum++
		}
		return nil
	})
	if err != nil {
		return err
	}
	idx.RNum = rnum
	col.Indexes[key] = idx
	if rt := col.runtime[key]; rt != nil {
		rt.def = idx
	}
	meta, err := encodeCatalog(db.state)
	if err != nil {
		return err
	}
	if err := b.Set(keyMetaState, meta); err != nil {
		return err
	}
	return b.Commit()
}

func (db *DB) scanDocsForCollection(reader queryReader, col *collectionState, fn func(id int64, raw jsontext.Value, doc any) error) error {
	return scanPrefix(reader, keyDocPrefix(col.DBID), func(key, value []byte) error {
		_, id, ok := decodeDocKey(key)
		if !ok {
			return withCode(CodeInvalidQuery, "invalid document key in storage")
		}
		raw, doc, err := decodeStoredDocument(value)
		if err != nil {
			return err
		}
		return fn(id, raw, doc)
	})
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
	seen := make(map[string]struct{})
	appendOne := func(x any) {
		if s, ok := normalizeIndexValue(x, kind); ok {
			if _, ok := seen[s]; ok {
				return
			}
			seen[s] = struct{}{}
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
		return normalizeStringIndexValue(v)
	case IndexInt64:
		i, ok := toInt64(v)
		if !ok {
			return "", false
		}
		return encodeI64IndexKey(i), true
	case IndexFloat:
		f, ok := toFloat64(v)
		if !ok {
			return "", false
		}
		return encodeF64IndexKey(f), true
	default:
		return "", false
	}
}

func normalizeStringIndexValue(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case jsonNumber:
		return x.String(), true
	case float64:
		return formatJQFloat(x), true
	case float32:
		return formatJQFloat(float64(x)), true
	case int:
		return strconv.FormatInt(int64(x), 10), true
	case int64:
		return strconv.FormatInt(x, 10), true
	case int32:
		return strconv.FormatInt(int64(x), 10), true
	case int16:
		return strconv.FormatInt(int64(x), 10), true
	case int8:
		return strconv.FormatInt(int64(x), 10), true
	case uint:
		return strconv.FormatUint(uint64(x), 10), true
	case uint64:
		return strconv.FormatUint(x, 10), true
	case uint32:
		return strconv.FormatUint(uint64(x), 10), true
	case uint16:
		return strconv.FormatUint(uint64(x), 10), true
	case uint8:
		return strconv.FormatUint(uint64(x), 10), true
	default:
		return "", false
	}
}

func encodeI64IndexKey(v int64) string {
	return fixedHex64(uint64(v) ^ (uint64(1) << 63))
}

func decodeI64IndexKey(v string) (int64, bool) {
	u, err := strconv.ParseUint(v, 16, 64)
	if err != nil || len(v) != 16 {
		i, ierr := strconv.ParseInt(v, 10, 64)
		return i, ierr == nil
	}
	return int64(u ^ (uint64(1) << 63)), true
}

func encodeF64IndexKey(v float64) string {
	u := math.Float64bits(v)
	if u&(uint64(1)<<63) != 0 {
		u = ^u
	} else {
		u ^= uint64(1) << 63
	}
	return fixedHex64(u)
}

func decodeF64IndexKey(v string) (float64, bool) {
	u, err := strconv.ParseUint(v, 16, 64)
	if err != nil || len(v) != 16 {
		f, ferr := strconv.ParseFloat(v, 64)
		return f, ferr == nil
	}
	if u&(uint64(1)<<63) != 0 {
		u ^= uint64(1) << 63
	} else {
		u = ^u
	}
	return math.Float64frombits(u), true
}

func fixedHex64(v uint64) string {
	s := strconv.FormatUint(v, 16)
	if len(s) >= 16 {
		return s
	}
	return strings.Repeat("0", 16-len(s)) + s
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

func (db *DB) recordDocPut(collection string, id int64, oldRaw, newRaw jsontext.Value, oldDoc, newDoc any) {
	db.pending = append(db.pending, storageMutation{
		kind:       mutationPut,
		collection: collection,
		id:         id,
		oldRaw:     append(jsontext.Value(nil), oldRaw...),
		newRaw:     append(jsontext.Value(nil), newRaw...),
		oldDoc:     oldDoc,
		newDoc:     newDoc,
	})
	db.dirtyMeta = true
}

func (db *DB) recordDocDelete(collection string, id int64, oldRaw jsontext.Value, oldDoc any) {
	db.pending = append(db.pending, storageMutation{
		kind:       mutationDelete,
		collection: collection,
		id:         id,
		oldRaw:     append(jsontext.Value(nil), oldRaw...),
		oldDoc:     oldDoc,
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
			if err := b.Set(keySeq(col.DBID), putU64(nil, uint64(col.NextID))); err != nil {
				return err
			}
		}
		switch m.kind {
		case mutationPut:
			if len(m.oldRaw) > 0 {
				if err := db.deleteIndexKeys(b, m.collection, m.id, m.oldRaw, m.oldDoc); err != nil {
					return err
				}
			}
			stored, err := db.encodeStoredDoc(m.newRaw, m.newDoc)
			if err != nil {
				return err
			}
			if err := b.Set(keyDoc(col.DBID, m.id), stored); err != nil {
				return err
			}
			if err := db.setIndexKeys(b, m.collection, m.id, m.newRaw, m.newDoc); err != nil {
				return err
			}
		case mutationDelete:
			if err := db.deleteIndexKeys(b, m.collection, m.id, m.oldRaw, m.oldDoc); err != nil {
				return err
			}
			if col != nil {
				if err := b.Delete(keyDoc(col.DBID, m.id)); err != nil {
					return err
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

func (db *DB) dropCollectionDataAndCommitLocked(col *collectionState) error {
	meta, err := encodeCatalog(db.state)
	if err != nil {
		return err
	}
	b := db.engine.NewBatch()
	defer b.Close()
	if err := b.DeleteRange(keyDocPrefix(col.DBID), prefixEnd(keyDocPrefix(col.DBID))); err != nil {
		return err
	}
	if err := b.Delete(keySeq(col.DBID)); err != nil {
		return err
	}
	for _, idx := range col.Indexes {
		if err := b.DeleteRange(keyIndexPathPrefix(idx), prefixEnd(keyIndexPathPrefix(idx))); err != nil {
			return err
		}
		if err := b.DeleteRange(keyUniqueIndexPathPrefix(idx), prefixEnd(keyUniqueIndexPathPrefix(idx))); err != nil {
			return err
		}
	}
	if err := b.Set(keyMetaState, meta); err != nil {
		return err
	}
	return b.Commit()
}

func (db *DB) dropIndexesDataAndCommitLocked(indexes []indexState) error {
	meta, err := encodeCatalog(db.state)
	if err != nil {
		return err
	}
	b := db.engine.NewBatch()
	defer b.Close()
	for _, idx := range indexes {
		if err := b.DeleteRange(keyIndexPathPrefix(idx), prefixEnd(keyIndexPathPrefix(idx))); err != nil {
			return err
		}
		if err := b.DeleteRange(keyUniqueIndexPathPrefix(idx), prefixEnd(keyUniqueIndexPathPrefix(idx))); err != nil {
			return err
		}
	}
	if err := b.Set(keyMetaState, meta); err != nil {
		return err
	}
	return b.Commit()
}

func (db *DB) deleteIndexKeys(b StorageBatch, collection string, id int64, raw []byte, doc any) error {
	col := db.state.Collections[collection]
	if col == nil || len(raw) == 0 {
		return nil
	}
	if doc == nil {
		if err := decodeJSONDocument(raw, &doc); err != nil {
			return err
		}
	}
	for _, idx := range col.Indexes {
		for _, value := range valuesForIndex(doc, idx.Path, idx.Kind) {
			if idx.Unique {
				if err := b.Delete(keyUniqueIndex(idx, value)); err != nil {
					return err
				}
			} else if err := b.Delete(keyIndex(idx, value, id)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB) setIndexKeys(b StorageBatch, collection string, id int64, raw []byte, doc any) error {
	col := db.state.Collections[collection]
	if col == nil || len(raw) == 0 {
		return nil
	}
	if doc == nil {
		if err := decodeJSONDocument(raw, &doc); err != nil {
			return err
		}
	}
	for _, idx := range col.Indexes {
		for _, value := range valuesForIndex(doc, idx.Path, idx.Kind) {
			if idx.Unique {
				if err := b.Set(keyUniqueIndex(idx, value), putU64(nil, uint64(id))); err != nil {
					return err
				}
			} else if err := b.Set(keyIndex(idx, value, id), []byte{1}); err != nil {
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
	if err := b.Set(keyMetaState, meta); err != nil {
		return err
	}
	for _, col := range db.state.Collections {
		if err := b.Set(keySeq(col.DBID), putU64(nil, uint64(col.NextID))); err != nil {
			return err
		}
	}
	if err := b.Commit(); err != nil {
		return err
	}
	db.clearPending()
	return nil
}
