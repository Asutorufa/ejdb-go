package ejdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"time"

	"github.com/cockroachdb/pebble"
)

var keyMetaState = []byte("meta/state")

const currentFormatVersion = 5

const (
	keyTagDoc  byte = 0x10
	keyTagSeq  byte = 0x11
	keyTagIdx  byte = 0x20
	keyTagUIdx byte = 0x21
)

type StorageEngine interface {
	Open(path string) error
	Close() error
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Delete(key []byte) error
	NewBatch() StorageBatch
	NewSnapshot() StorageSnapshot
	NewIterator(lower, upper []byte) (StorageIterator, error)
	Compact(start, end []byte) error
	Flush() error
	Backup(dst string) error
}

type StorageBatch interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	DeleteRange(start, end []byte) error
	Commit() error
	Close() error
}

type StorageSnapshot interface {
	Get(key []byte) ([]byte, error)
	NewIterator(lower, upper []byte) (StorageIterator, error)
	Close() error
}

type StorageIterator interface {
	First() bool
	Next() bool
	Valid() bool
	Key() []byte
	Value() []byte
	Error() error
	Close() error
}

type PebbleEngine struct {
	opts      *pebble.Options
	writeOpts *pebble.WriteOptions
	db        *pebble.DB
}

func NewPebbleEngine(opts *pebble.Options) *PebbleEngine {
	if opts == nil {
		opts = &pebble.Options{}
	} else {
		opts = opts.Clone()
	}
	if opts.Logger == nil && opts.LoggerAndTracer == nil {
		opts.Logger = discardPebbleLogger{}
	}
	return &PebbleEngine{opts: opts, writeOpts: pebble.NoSync}
}

type discardPebbleLogger struct{}

func (discardPebbleLogger) Infof(string, ...any)  {}
func (discardPebbleLogger) Fatalf(string, ...any) {}

func (e *PebbleEngine) setSyncWrites(sync bool) {
	if sync {
		e.writeOpts = pebble.Sync
	} else {
		e.writeOpts = pebble.NoSync
	}
}

func (e *PebbleEngine) Open(path string) error {
	db, err := pebble.Open(path, e.opts)
	if err != nil {
		return err
	}
	e.db = db
	return nil
}

func (e *PebbleEngine) Close() error {
	if e == nil || e.db == nil {
		return nil
	}
	err := e.db.Close()
	e.db = nil
	return err
}

func (e *PebbleEngine) Get(key []byte) ([]byte, error) {
	val, closer, err := e.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	defer closer.Close()
	return append([]byte(nil), val...), nil
}

func (e *PebbleEngine) Set(key, value []byte) error {
	return e.db.Set(key, value, e.writeOpts)
}

func (e *PebbleEngine) Delete(key []byte) error {
	return e.db.Delete(key, e.writeOpts)
}

func (e *PebbleEngine) NewBatch() StorageBatch {
	return &pebbleBatch{b: e.db.NewBatch(), writeOpts: e.writeOpts}
}

func (e *PebbleEngine) NewSnapshot() StorageSnapshot {
	return &pebbleSnapshot{s: e.db.NewSnapshot()}
}

func (e *PebbleEngine) NewIterator(lower, upper []byte) (StorageIterator, error) {
	it, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	return &pebbleIterator{it: it}, nil
}

func (e *PebbleEngine) Compact(start, end []byte) error {
	return e.db.Compact(start, end, true)
}

func (e *PebbleEngine) Flush() error {
	return e.db.Flush()
}

func (e *PebbleEngine) Backup(dst string) error {
	if err := os.RemoveAll(dst); err != nil {
		return err
	}
	return e.db.Checkpoint(dst)
}

type pebbleBatch struct {
	b         *pebble.Batch
	writeOpts *pebble.WriteOptions
}

func (b *pebbleBatch) Set(key, value []byte) error {
	return b.b.Set(key, value, nil)
}

func (b *pebbleBatch) Delete(key []byte) error {
	return b.b.Delete(key, nil)
}

func (b *pebbleBatch) DeleteRange(start, end []byte) error {
	return b.b.DeleteRange(start, end, nil)
}

func (b *pebbleBatch) Commit() error {
	return b.b.Commit(b.writeOpts)
}

func (b *pebbleBatch) Close() error {
	return b.b.Close()
}

type pebbleSnapshot struct {
	s *pebble.Snapshot
}

func (s *pebbleSnapshot) Get(key []byte) ([]byte, error) {
	val, closer, err := s.s.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	defer closer.Close()
	return append([]byte(nil), val...), nil
}

func (s *pebbleSnapshot) NewIterator(lower, upper []byte) (StorageIterator, error) {
	it, err := s.s.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	return &pebbleIterator{it: it}, nil
}

func (s *pebbleSnapshot) Close() error {
	return s.s.Close()
}

type pebbleIterator struct {
	it *pebble.Iterator
}

func (i *pebbleIterator) First() bool {
	return i.it.First()
}

func (i *pebbleIterator) Next() bool {
	return i.it.Next()
}

func (i *pebbleIterator) Valid() bool {
	return i.it.Valid()
}

func (i *pebbleIterator) Key() []byte {
	return append([]byte(nil), i.it.Key()...)
}

func (i *pebbleIterator) Value() []byte {
	return append([]byte(nil), i.it.Value()...)
}

func (i *pebbleIterator) Error() error {
	return i.it.Error()
}

func (i *pebbleIterator) Close() error {
	return i.it.Close()
}

type catalogState struct {
	FormatVersion    int                          `json:"format_version"`
	Version          string                       `json:"version"`
	NextCollectionID int64                        `json:"next_collection_id"`
	CreatedAt        int64                        `json:"created_at_unix_nano"`
	Collections      map[string]catalogCollection `json:"collections"`
}

type catalogCollection struct {
	DBID    int64                 `json:"dbid"`
	NextID  int64                 `json:"next_id"`
	Indexes map[string]indexState `json:"indexes"`
}

func catalogFromState(s *dbState) catalogState {
	cat := catalogState{
		FormatVersion:    currentFormatVersion,
		Version:          s.Version,
		NextCollectionID: s.NextCollectionID,
		CreatedAt:        s.CreatedAt.UnixNano(),
		Collections:      make(map[string]catalogCollection, len(s.Collections)),
	}
	for name, col := range s.Collections {
		idx := make(map[string]indexState, len(col.Indexes))
		for k, v := range col.Indexes {
			idx[k] = v
		}
		cat.Collections[name] = catalogCollection{DBID: col.DBID, NextID: col.NextID, Indexes: idx}
	}
	return cat
}

func stateFromCatalog(cat catalogState) *dbState {
	st := newState()
	if cat.Version != "" {
		st.Version = cat.Version
	}
	if cat.NextCollectionID > 0 {
		st.NextCollectionID = cat.NextCollectionID
	}
	if cat.CreatedAt > 0 {
		st.CreatedAt = unixNanoUTC(cat.CreatedAt)
	}
	st.Collections = make(map[string]*collectionState, len(cat.Collections))
	for name, c := range cat.Collections {
		idx := make(map[string]indexState, len(c.Indexes))
		for k, v := range c.Indexes {
			idx[k] = v
		}
		st.Collections[name] = &collectionState{
			Name:    name,
			DBID:    c.DBID,
			NextID:  c.NextID,
			Docs:    make(map[int64]json.RawMessage),
			Indexes: idx,
			runtime: make(map[string]*indexRuntime),
		}
	}
	return st
}

func unixNanoUTC(v int64) time.Time {
	return time.Unix(0, v).UTC()
}

func encodeCatalog(s *dbState) ([]byte, error) {
	return json.Marshal(catalogFromState(s))
}

func decodeCatalog(raw []byte) (*dbState, error) {
	var cat catalogState
	if err := json.Unmarshal(raw, &cat); err != nil {
		return nil, err
	}
	if cat.FormatVersion != currentFormatVersion {
		return nil, withCodef(CodeIncompatibleFormat, "unsupported storage format version %d, expected %d", cat.FormatVersion, currentFormatVersion)
	}
	return stateFromCatalog(cat), nil
}

func keySeq(collection string) []byte {
	return appendSegment([]byte{keyTagSeq}, collection)
}

func keyDocPrefix(collection string) []byte {
	return appendSegment([]byte{keyTagDoc}, collection)
}

func keyDoc(collection string, id int64) []byte {
	k := keyDocPrefix(collection)
	return appendI64(k, id)
}

func keyIndexPrefix(collection string) []byte {
	return appendSegment([]byte{keyTagIdx}, collection)
}

func keyUniqueIndexPrefix(collection string) []byte {
	return appendSegment([]byte{keyTagUIdx}, collection)
}

func keyIndexPathPrefix(collection string, idx indexState) []byte {
	k := keyIndexPrefix(collection)
	k = appendSegment(k, string(idx.Kind))
	return appendSegment(k, idx.Path)
}

func keyUniqueIndexPathPrefix(collection string, idx indexState) []byte {
	k := keyUniqueIndexPrefix(collection)
	k = appendSegment(k, string(idx.Kind))
	return appendSegment(k, idx.Path)
}

func keyIndexValuePrefix(collection string, idx indexState, value string) []byte {
	return appendSegment(keyIndexPathPrefix(collection, idx), value)
}

func keyUniqueIndexValuePrefix(collection string, idx indexState, value string) []byte {
	return appendSegment(keyUniqueIndexPathPrefix(collection, idx), value)
}

func keyIndex(collection string, idx indexState, value string, id int64) []byte {
	k := keyIndexValuePrefix(collection, idx, value)
	return appendI64(k, id)
}

func keyUniqueIndex(collection string, idx indexState, value string) []byte {
	return keyUniqueIndexValuePrefix(collection, idx, value)
}

func appendSegment(dst []byte, s string) []byte {
	var lenbuf [4]byte
	binary.BigEndian.PutUint32(lenbuf[:], uint32(len(s)))
	dst = append(dst, lenbuf[:]...)
	return append(dst, s...)
}

func readSegment(src []byte, off *int) (string, bool) {
	if len(src)-*off < 4 {
		return "", false
	}
	n := int(binary.BigEndian.Uint32(src[*off : *off+4]))
	*off += 4
	if n < 0 || len(src)-*off < n {
		return "", false
	}
	s := string(src[*off : *off+n])
	*off += n
	return s, true
}

func appendI64(dst []byte, v int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v)^0x8000000000000000)
	return append(dst, buf[:]...)
}

func readI64(src []byte, off *int) (int64, bool) {
	if len(src)-*off < 8 {
		return 0, false
	}
	v := int64(binary.BigEndian.Uint64(src[*off:*off+8]) ^ 0x8000000000000000)
	*off += 8
	return v, true
}

func decodeDocKey(key []byte) (string, int64, bool) {
	if len(key) == 0 || key[0] != keyTagDoc {
		return "", 0, false
	}
	off := 1
	coll, ok := readSegment(key, &off)
	if !ok {
		return "", 0, false
	}
	id, ok := readI64(key, &off)
	return coll, id, ok && off == len(key)
}

func decodeIndexKey(key []byte) (collection string, kind IndexKind, path string, value string, id int64, ok bool) {
	if len(key) == 0 || key[0] != keyTagIdx {
		return "", "", "", "", 0, false
	}
	off := 1
	collection, ok = readSegment(key, &off)
	if !ok {
		return "", "", "", "", 0, false
	}
	kindRaw, ok := readSegment(key, &off)
	if !ok {
		return "", "", "", "", 0, false
	}
	path, ok = readSegment(key, &off)
	if !ok {
		return "", "", "", "", 0, false
	}
	value, ok = readSegment(key, &off)
	if !ok {
		return "", "", "", "", 0, false
	}
	id, ok = readI64(key, &off)
	if !ok || off != len(key) {
		return "", "", "", "", 0, false
	}
	return collection, IndexKind(kindRaw), path, value, id, true
}

func decodeUniqueIndexKey(key []byte) (collection string, kind IndexKind, path string, value string, ok bool) {
	if len(key) == 0 || key[0] != keyTagUIdx {
		return "", "", "", "", false
	}
	off := 1
	collection, ok = readSegment(key, &off)
	if !ok {
		return "", "", "", "", false
	}
	kindRaw, ok := readSegment(key, &off)
	if !ok {
		return "", "", "", "", false
	}
	path, ok = readSegment(key, &off)
	if !ok {
		return "", "", "", "", false
	}
	value, ok = readSegment(key, &off)
	if !ok || off != len(key) {
		return "", "", "", "", false
	}
	return collection, IndexKind(kindRaw), path, value, true
}

func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func scanPrefix(r interface {
	NewIterator(lower, upper []byte) (StorageIterator, error)
}, prefix []byte, fn func(key, value []byte) error) error {
	it, err := r.NewIterator(prefix, prefixEnd(prefix))
	if err != nil {
		return err
	}
	defer it.Close()
	for ok := it.First(); ok; ok = it.Next() {
		if !bytes.HasPrefix(it.Key(), prefix) {
			break
		}
		if err := fn(it.Key(), it.Value()); err != nil {
			return err
		}
	}
	return it.Error()
}

func putU64(dst []byte, v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return append(dst, buf[:]...)
}

func parseU64(raw []byte) (uint64, error) {
	if len(raw) != 8 {
		return 0, io.ErrUnexpectedEOF
	}
	return binary.BigEndian.Uint64(raw), nil
}
