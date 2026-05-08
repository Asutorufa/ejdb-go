package ejdb

import (
	"encoding/json"
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
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	var ops []patchOp
	if err := json.Unmarshal(patchRaw, &ops); err != nil {
		return nil, err
	}
	for _, op := range ops {
		switch op.Op {
		case "add":
			if err := pointerSet(doc, op.Path, op.Value, true); err != nil {
				return nil, err
			}
		case "replace":
			if err := pointerSet(doc, op.Path, op.Value, false); err != nil {
				return nil, err
			}
		case "remove":
			if err := pointerRemove(doc, op.Path); err != nil {
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
			if err := pointerSet(doc, op.Path, v, true); err != nil {
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
			if err := pointerSet(doc, op.Path, cl, true); err != nil {
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
	return json.Marshal(doc)
}

func applyMergePatch(raw []byte, patchRaw []byte) ([]byte, error) {
	var target any
	if len(bytesTrimSpace(raw)) == 0 {
		target = map[string]any{}
	} else if err := json.Unmarshal(raw, &target); err != nil {
		return nil, err
	}
	var patch any
	if err := json.Unmarshal(patchRaw, &patch); err != nil {
		return nil, err
	}
	merged := mergePatchValue(target, patch)
	return json.Marshal(merged)
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
