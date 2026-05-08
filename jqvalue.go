package ejdb

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type jqKind uint8

const (
	jqNull jqKind = iota
	jqBool
	jqI64
	jqF64
	jqString
	jqArray
	jqObject
)

type jqValue struct {
	kind jqKind
	raw  any
}

func toJQValue(v any) jqValue {
	switch x := v.(type) {
	case nil:
		return jqValue{kind: jqNull}
	case bool:
		return jqValue{kind: jqBool, raw: x}
	case string:
		return jqValue{kind: jqString, raw: x}
	case regexpExpr:
		return jqValue{kind: jqString, raw: string(x)}
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return jqValue{kind: jqI64, raw: i}
		}
		if f, err := x.Float64(); err == nil {
			return jqValue{kind: jqF64, raw: f}
		}
	case int:
		return jqValue{kind: jqI64, raw: int64(x)}
	case int64:
		return jqValue{kind: jqI64, raw: x}
	case int32:
		return jqValue{kind: jqI64, raw: int64(x)}
	case int16:
		return jqValue{kind: jqI64, raw: int64(x)}
	case int8:
		return jqValue{kind: jqI64, raw: int64(x)}
	case uint:
		if uint64(x) <= uint64(^uint64(0)>>1) {
			return jqValue{kind: jqI64, raw: int64(x)}
		}
		return jqValue{kind: jqF64, raw: float64(x)}
	case uint64:
		if x <= uint64(^uint64(0)>>1) {
			return jqValue{kind: jqI64, raw: int64(x)}
		}
		return jqValue{kind: jqF64, raw: float64(x)}
	case uint32:
		return jqValue{kind: jqI64, raw: int64(x)}
	case uint16:
		return jqValue{kind: jqI64, raw: int64(x)}
	case uint8:
		return jqValue{kind: jqI64, raw: int64(x)}
	case float64:
		if i := int64(x); float64(i) == x {
			return jqValue{kind: jqI64, raw: i}
		}
		return jqValue{kind: jqF64, raw: x}
	case float32:
		f := float64(x)
		if i := int64(f); float64(i) == f {
			return jqValue{kind: jqI64, raw: i}
		}
		return jqValue{kind: jqF64, raw: f}
	case []any:
		return jqValue{kind: jqArray, raw: x}
	case map[string]any:
		return jqValue{kind: jqObject, raw: x}
	}
	return jqValue{kind: jqString, raw: fmt.Sprint(v)}
}

func jqCompare(a, b any) (int, bool) {
	return compareJQValues(toJQValue(a), toJQValue(b))
}

func compareJQValues(a, b jqValue) (int, bool) {
	switch a.kind {
	case jqString:
		return cmpStringLeft(a.raw.(string), b)
	case jqI64:
		return cmpI64Left(a.raw.(int64), b)
	case jqF64:
		return cmpF64Left(a.raw.(float64), b)
	case jqBool:
		return cmpBoolLeft(a.raw.(bool), b)
	case jqNull:
		return cmpNullLeft(b), true
	case jqArray:
		if b.kind != jqArray {
			return 0, false
		}
		return compareArrays(a.raw.([]any), b.raw.([]any)), true
	case jqObject:
		if b.kind != jqObject {
			return 0, false
		}
		return compareObjects(a.raw.(map[string]any), b.raw.(map[string]any)), true
	default:
		return 0, false
	}
}

func cmpStringLeft(left string, right jqValue) (int, bool) {
	switch right.kind {
	case jqString:
		rs := right.raw.(string)
		if len(left) != len(rs) {
			return len(left) - len(rs), true
		}
		return strings.Compare(left, rs), true
	case jqBool:
		lb := left == "true"
		rb := right.raw.(bool)
		return boolInt(lb) - boolInt(rb), true
	case jqI64:
		return strings.Compare(left, strconv.FormatInt(right.raw.(int64), 10)), true
	case jqF64:
		return strings.Compare(left, formatJQFloat(right.raw.(float64))), true
	case jqNull:
		if left == "" {
			return 0, true
		}
		return 1, true
	default:
		return 0, false
	}
}

func cmpI64Left(left int64, right jqValue) (int, bool) {
	switch right.kind {
	case jqI64:
		return cmpInt64(left, right.raw.(int64)), true
	case jqF64:
		return cmpFloat(float64(left), right.raw.(float64)), true
	case jqString:
		return cmpInt64(left, atoiOfficial(right.raw.(string))), true
	case jqNull:
		return 1, true
	case jqBool:
		return boolInt(left != 0) - boolInt(right.raw.(bool)), true
	default:
		return 0, false
	}
}

func cmpF64Left(left float64, right jqValue) (int, bool) {
	switch right.kind {
	case jqF64:
		return cmpFloat(left, right.raw.(float64)), true
	case jqI64:
		return cmpFloat(left, float64(right.raw.(int64))), true
	case jqString:
		return cmpFloat(left, atofOfficial(right.raw.(string))), true
	case jqNull:
		return 1, true
	case jqBool:
		return cmpFloat(left, float64(boolInt(right.raw.(bool)))), true
	default:
		return 0, false
	}
}

func cmpBoolLeft(left bool, right jqValue) (int, bool) {
	switch right.kind {
	case jqBool:
		return boolInt(left) - boolInt(right.raw.(bool)), true
	case jqI64:
		return boolInt(left) - boolInt(right.raw.(int64) != 0), true
	case jqF64:
		return boolInt(left) - boolInt(right.raw.(float64) != 0), true
	case jqString:
		rs := right.raw.(string)
		switch rs {
		case "true":
			return boolInt(left) - 1, true
		case "false":
			return boolInt(left), true
		default:
			return -1, true
		}
	case jqNull:
		return boolInt(left), true
	default:
		return 0, false
	}
}

func cmpNullLeft(right jqValue) int {
	switch right.kind {
	case jqNull:
		return 0
	case jqString:
		if right.raw.(string) == "" {
			return 0
		}
		return -1
	default:
		return -1
	}
}

func compareArrays(a, b []any) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		cmp, ok := jqCompare(a[i], b[i])
		if !ok {
			cmp = compareKindFallback(a[i], b[i])
		}
		if cmp != 0 {
			return cmp
		}
	}
	return len(a) - len(b)
}

func compareObjects(a, b map[string]any) int {
	ak := sortedKeys(a)
	bk := sortedKeys(b)
	for i := 0; i < len(ak) && i < len(bk); i++ {
		if ak[i] != bk[i] {
			return strings.Compare(ak[i], bk[i])
		}
		cmp, ok := jqCompare(a[ak[i]], b[bk[i]])
		if !ok {
			cmp = compareKindFallback(a[ak[i]], b[bk[i]])
		}
		if cmp != 0 {
			return cmp
		}
	}
	return len(ak) - len(bk)
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func compareKindFallback(a, b any) int {
	ak := toJQValue(a).kind
	bk := toJQValue(b).kind
	switch {
	case ak < bk:
		return -1
	case ak > bk:
		return 1
	default:
		return 0
	}
}

func jqPrefixMatch(left, right any) (bool, bool) {
	ls, ok := jqPrefixString(toJQValue(left))
	if !ok {
		return false, false
	}
	rs, ok := jqPrefixString(toJQValue(right))
	if !ok {
		return false, false
	}
	return strings.HasPrefix(ls, rs), true
}

func jqPrefixString(v jqValue) (string, bool) {
	switch v.kind {
	case jqString:
		return v.raw.(string), true
	case jqI64:
		return strconv.FormatInt(v.raw.(int64), 10), true
	case jqF64:
		return formatJQFloat(v.raw.(float64)), true
	case jqBool:
		if v.raw.(bool) {
			return "true", true
		}
		return "false", true
	default:
		return "", false
	}
}

func formatJQFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func atoiOfficial(v string) int64 {
	i, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil {
		return 0
	}
	return i
}

func atofOfficial(v string) float64 {
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		return 0
	}
	return f
}

func cmpInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
