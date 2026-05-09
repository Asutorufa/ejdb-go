package ejdb

import (
	"fmt"
)

type patchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	From  string `json:"from,omitempty"`
	Value any    `json:"value,omitempty"`
}

func applyJSONPatch(raw []byte, patchRaw []byte) ([]byte, error) {
	trim := bytesTrimSpace(patchRaw)
	if len(trim) == 0 {
		return append([]byte(nil), raw...), nil
	}
	if trim[0] == '[' {
		return applyRFC6902(raw, trim)
	}
	return applyMergePatch(raw, trim)
}

func applyRFC6902(raw []byte, patchRaw []byte) ([]byte, error) {
	var doc any
	if err := decodeJSON(raw, &doc); err != nil {
		return nil, err
	}
	var ops []patchOp
	if err := decodeJSON(patchRaw, &ops); err != nil {
		return nil, err
	}
	for _, op := range ops {
		switch op.Op {
		case "add":
			if err := pointerAdd(doc, op.Path, op.Value, false); err != nil {
				return nil, err
			}
		case "add_create":
			if err := pointerAdd(doc, op.Path, op.Value, true); err != nil {
				return nil, err
			}
		case "replace":
			if err := pointerReplace(doc, op.Path, op.Value); err != nil {
				return nil, err
			}
		case "remove":
			if err := pointerRemove(doc, op.Path); err != nil {
				return nil, err
			}
		case "increment":
			cur, ok := pointerGet(doc, op.Path)
			if !ok {
				return nil, ErrNotFound
			}
			next, err := incrementValue(cur, op.Value)
			if err != nil {
				return nil, err
			}
			if err := pointerReplace(doc, op.Path, next); err != nil {
				return nil, err
			}
		case "move":
			v, ok := pointerGet(doc, op.From)
			if !ok {
				return nil, ErrNotFound
			}
			if err := pointerRemove(doc, op.From); err != nil {
				return nil, err
			}
			if err := pointerAdd(doc, op.Path, v, false); err != nil {
				return nil, err
			}
		case "copy":
			v, ok := pointerGet(doc, op.From)
			if !ok {
				return nil, ErrNotFound
			}
			cl, err := cloneAny(v)
			if err != nil {
				return nil, err
			}
			if err := pointerAdd(doc, op.Path, cl, false); err != nil {
				return nil, err
			}
		case "swap":
			from, ok := pointerGet(doc, op.From)
			if !ok {
				return nil, ErrNotFound
			}
			to, hasTo := pointerGet(doc, op.Path)
			fromCl, err := cloneAny(from)
			if err != nil {
				return nil, err
			}
			if hasTo {
				toCl, err := cloneAny(to)
				if err != nil {
					return nil, err
				}
				if err := pointerReplace(doc, op.From, toCl); err != nil {
					return nil, err
				}
				if err := pointerReplace(doc, op.Path, fromCl); err != nil {
					return nil, err
				}
				continue
			}
			if err := pointerAdd(doc, op.Path, fromCl, false); err != nil {
				return nil, err
			}
			if err := pointerRemove(doc, op.From); err != nil {
				return nil, err
			}
		case "test":
			v, ok := pointerGet(doc, op.Path)
			if !ok || !equalValue(v, op.Value) {
				return nil, withCodef(CodeInvalidQuery, "json patch test failed at %s", op.Path)
			}
		default:
			return nil, fmt.Errorf("unsupported patch op: %s", op.Op)
		}
	}
	return marshalJSON(doc)
}

func applyMergePatch(raw []byte, patchRaw []byte) ([]byte, error) {
	var target any
	if len(bytesTrimSpace(raw)) == 0 {
		target = map[string]any{}
	} else if err := decodeJSON(raw, &target); err != nil {
		return nil, err
	}
	var patch any
	if err := decodeJSON(patchRaw, &patch); err != nil {
		return nil, err
	}
	merged := mergePatchValue(target, patch)
	return marshalJSON(merged)
}

func decodeJSON(raw []byte, out any) error {
	switch x := out.(type) {
	case *any:
		v, err := decodeJSONAny(raw)
		if err != nil {
			return err
		}
		*x = v
		return nil
	case *[]patchOp:
		ops, err := decodePatchOps(raw)
		if err != nil {
			return err
		}
		*x = ops
		return nil
	default:
		return unmarshalJSON(raw, out)
	}
}

func decodePatchOps(raw []byte) ([]patchOp, error) {
	v, err := decodeJSONAny(raw)
	if err != nil {
		return nil, err
	}
	items, ok := v.([]any)
	if !ok {
		return nil, withCodef(CodeInvalidValueType, "JSON patch must be an array")
	}
	ops := make([]patchOp, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, withCodef(CodeInvalidValueType, "JSON patch op must be an object")
		}
		op := patchOp{Value: m["value"]}
		if s, ok := m["op"].(string); ok {
			op.Op = s
		}
		if s, ok := m["path"].(string); ok {
			op.Path = s
		}
		if s, ok := m["from"].(string); ok {
			op.From = s
		}
		ops = append(ops, op)
	}
	return ops, nil
}

func incrementValue(cur, inc any) (any, error) {
	if ci, ok := toInt64(cur); ok {
		if ii, ok := toInt64(inc); ok {
			return ci + ii, nil
		}
	}
	cf, ok := toFloat64(cur)
	if !ok {
		return nil, withCodef(CodeInvalidValueType, "increment target is not numeric: %v", cur)
	}
	iv, ok := toFloat64(inc)
	if !ok {
		return nil, withCodef(CodeInvalidValueType, "increment value is not numeric: %v", inc)
	}
	return cf + iv, nil
}

func mergePatchValue(target any, patch any) any {
	pm, ok := patch.(map[string]any)
	if !ok {
		return patch
	}
	tm, ok := target.(map[string]any)
	if !ok {
		tm = map[string]any{}
	}
	for k, v := range pm {
		if v == nil {
			delete(tm, k)
			continue
		}
		tm[k] = mergePatchValue(tm[k], v)
	}
	return tm
}

func bytesTrimSpace(in []byte) []byte {
	start := 0
	for start < len(in) && (in[start] == ' ' || in[start] == '\n' || in[start] == '\r' || in[start] == '\t') {
		start++
	}
	end := len(in)
	for end > start && (in[end-1] == ' ' || in[end-1] == '\n' || in[end-1] == '\r' || in[end-1] == '\t') {
		end--
	}
	return in[start:end]
}
