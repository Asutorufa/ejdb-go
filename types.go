package ejdb

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/cockroachdb/pebble"
)

type IndexKind string

const (
	IndexString IndexKind = "str"
	IndexInt64  IndexKind = "i64"
	IndexFloat  IndexKind = "f64"
)

type IndexMode uint8

const (
	IdxUnique IndexMode = 0x01
	IdxString IndexMode = 0x04
	IdxInt64  IndexMode = 0x08
	IdxFloat  IndexMode = 0x10
)

func indexModeParts(mode IndexMode) (IndexKind, bool, error) {
	unique := mode&IdxUnique != 0
	kinds := 0
	kind := IndexKind("")
	if mode&IdxString != 0 {
		kinds++
		kind = IndexString
	}
	if mode&IdxInt64 != 0 {
		kinds++
		kind = IndexInt64
	}
	if mode&IdxFloat != 0 {
		kinds++
		kind = IndexFloat
	}
	if kinds != 1 {
		return "", false, withCodef(CodeInvalidQuery, "index mode must include exactly one type bit: %d", mode)
	}
	return kind, unique, nil
}

type Options struct {
	Path          string
	AutoSync      bool
	EnableWAL     bool
	Engine        StorageEngine
	PebbleOptions *pebble.Options
}

type Document struct {
	ID  int64           `json:"id"`
	Raw json.RawMessage `json:"raw"`
}

type IndexMeta struct {
	Path   string    `json:"ptr"`
	Kind   IndexKind `json:"kind"`
	Unique bool      `json:"unique"`
}

type CollectionMeta struct {
	Name    string      `json:"name"`
	DBID    int64       `json:"dbid"`
	RNum    int         `json:"rnum"`
	Indexes []IndexMeta `json:"indexes"`
}

type Meta struct {
	Version     string           `json:"version"`
	File        string           `json:"file"`
	Size        int64            `json:"size"`
	Collections []CollectionMeta `json:"collections"`
}

type dbState struct {
	Version          string                      `json:"version"`
	NextCollectionID int64                       `json:"next_collection_id"`
	CreatedAt        time.Time                   `json:"created_at"`
	Collections      map[string]*collectionState `json:"collections"`
}

type collectionState struct {
	Name    string                    `json:"-"`
	DBID    int64                     `json:"dbid"`
	NextID  int64                     `json:"next_id"`
	Docs    map[int64]json.RawMessage `json:"docs"`
	Indexes map[string]indexState     `json:"indexes"`
	runtime map[string]*indexRuntime  `json:"-"`
}

type indexState struct {
	Path   string    `json:"path"`
	Kind   IndexKind `json:"kind"`
	Unique bool      `json:"unique"`
}

type indexRuntime struct {
	def    indexState
	unique map[string]int64
	multi  map[string]map[int64]struct{}
}

func newState() *dbState {
	return &dbState{
		Version:          "2.0-go",
		NextCollectionID: 1,
		CreatedAt:        time.Now().UTC(),
		Collections:      make(map[string]*collectionState),
	}
}

func indexKey(path string, kind IndexKind, unique bool) string {
	return fmt.Sprintf("%s|%s|%t", path, kind, unique)
}

func (c *collectionState) initRuntime() {
	if c.runtime == nil {
		c.runtime = make(map[string]*indexRuntime, len(c.Indexes))
	}
	for k, idx := range c.Indexes {
		rt := &indexRuntime{
			def:    idx,
			unique: make(map[string]int64),
			multi:  make(map[string]map[int64]struct{}),
		}
		c.runtime[k] = rt
	}
}

func (s *dbState) clone() (*dbState, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	var out dbState
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	if out.Collections == nil {
		out.Collections = make(map[string]*collectionState)
	}
	for name, c := range out.Collections {
		c.Name = name
		if c.Docs == nil {
			c.Docs = make(map[int64]json.RawMessage)
		}
		if c.Indexes == nil {
			c.Indexes = make(map[string]indexState)
		}
		c.initRuntime()
	}
	return &out, nil
}

func toMeta(path string, state *dbState) Meta {
	m := Meta{
		Version: state.Version,
		File:    path,
	}
	if fi, err := os.Stat(path); err == nil {
		m.Size = fi.Size()
	}
	cols := make([]string, 0, len(state.Collections))
	for name := range state.Collections {
		cols = append(cols, name)
	}
	sort.Strings(cols)
	for _, name := range cols {
		c := state.Collections[name]
		cm := CollectionMeta{
			Name: name,
			DBID: c.DBID,
			RNum: len(c.Docs),
		}
		for _, idx := range c.Indexes {
			cm.Indexes = append(cm.Indexes, IndexMeta{Path: idx.Path, Kind: idx.Kind, Unique: idx.Unique})
		}
		sort.Slice(cm.Indexes, func(i, j int) bool {
			if cm.Indexes[i].Path == cm.Indexes[j].Path {
				if cm.Indexes[i].Kind == cm.Indexes[j].Kind {
					return !cm.Indexes[i].Unique && cm.Indexes[j].Unique
				}
				return cm.Indexes[i].Kind < cm.Indexes[j].Kind
			}
			return cm.Indexes[i].Path < cm.Indexes[j].Path
		})
		m.Collections = append(m.Collections, cm)
	}
	return m
}
