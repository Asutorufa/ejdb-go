package ejdb

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func pointerTokens(ptr string) ([]string, error) {
	if ptr == "" || ptr == "/" {
		return nil, nil
	}
	if !strings.HasPrefix(ptr, "/") {
		return nil, fmt.Errorf("pointer must start with '/': %q", ptr)
	}
	parts := strings.Split(ptr[1:], "/")
	for i := range parts {
		parts[i] = strings.ReplaceAll(strings.ReplaceAll(parts[i], "~1", "/"), "~0", "~")
	}
	return parts, nil
}

func pointerEscapeToken(tok string) string {
	return strings.ReplaceAll(strings.ReplaceAll(tok, "~", "~0"), "/", "~1")
}

func pointerGet(doc any, ptr string) (any, bool) {
	tokens, err := pointerTokens(ptr)
	if err != nil {
		return nil, false
	}
	cur := doc
	for _, t := range tokens {
		switch v := cur.(type) {
		case map[string]any:
			n, ok := v[t]
			if !ok {
				return nil, false
			}
			cur = n
		case []any:
			i, err := strconv.Atoi(t)
			if err != nil || i < 0 || i >= len(v) {
				return nil, false
			}
			cur = v[i]
		default:
			return nil, false
		}
	}
	return cur, true
}

func pointerSet(doc any, ptr string, val any, createMissing bool) error {
	return pointerSetMode(doc, ptr, val, createMissing, createMissing, false)
}

func pointerAdd(doc any, ptr string, val any, createParents bool) error {
	return pointerSetMode(doc, ptr, val, createParents, true, true)
}

func pointerReplace(doc any, ptr string, val any) error {
	return pointerSetMode(doc, ptr, val, false, false, false)
}

func pointerSetMode(doc any, ptr string, val any, createParents, allowLeafCreate, arrayInsert bool) error {
	tokens, err := pointerTokens(ptr)
	if err != nil {
		return err
	}
	if len(tokens) == 0 {
		return errors.New("cannot replace document root with pointerSet")
	}
	_, err = setAt(doc, tokens, val, createParents, allowLeafCreate, arrayInsert)
	return err
}

func setAt(cur any, tokens []string, val any, createParents, allowLeafCreate, arrayInsert bool) (any, error) {
	if len(tokens) == 0 {
		return val, nil
	}
	t := tokens[0]
	if len(tokens) > 1 {
		switch v := cur.(type) {
		case map[string]any:
			n, ok := v[t]
			if !ok {
				if !createParents {
					return cur, ErrNotFound
				}
				n = map[string]any{}
			}
			updated, err := setAt(n, tokens[1:], val, createParents, allowLeafCreate, arrayInsert)
			if err != nil {
				return cur, err
			}
			v[t] = updated
			return v, nil
		case []any:
			idx, err := strconv.Atoi(t)
			if err != nil || idx < 0 || idx >= len(v) {
				return cur, ErrNotFound
			}
			updated, err := setAt(v[idx], tokens[1:], val, createParents, allowLeafCreate, arrayInsert)
			if err != nil {
				return cur, err
			}
			v[idx] = updated
			return v, nil
		default:
			return cur, ErrNotFound
		}
	}
	switch v := cur.(type) {
	case map[string]any:
		if _, ok := v[t]; !ok && !allowLeafCreate {
			return cur, ErrNotFound
		}
		v[t] = val
		return v, nil
	case []any:
		if t == "-" && allowLeafCreate && arrayInsert {
			v = append(v, val)
			return v, nil
		}
		idx, err := strconv.Atoi(t)
		if err != nil || idx < 0 {
			return cur, ErrNotFound
		}
		if allowLeafCreate && arrayInsert {
			if idx > len(v) {
				return cur, ErrNotFound
			}
			v = append(v, nil)
			copy(v[idx+1:], v[idx:])
			v[idx] = val
			return v, nil
		}
		if idx == len(v) && allowLeafCreate {
			v = append(v, val)
			return v, nil
		}
		if idx >= len(v) {
			return cur, ErrNotFound
		}
		v[idx] = val
		return v, nil
	default:
		return cur, ErrNotFound
	}
}

func pointerRemove(doc any, ptr string) error {
	tokens, err := pointerTokens(ptr)
	if err != nil {
		return err
	}
	if len(tokens) == 0 {
		return errors.New("cannot remove root")
	}
	_, err = removeAt(doc, tokens)
	return err
}

func pointerRemovePattern(doc any, ptr string) error {
	tokens, err := pointerTokens(ptr)
	if err != nil {
		return err
	}
	if len(tokens) == 0 {
		return errors.New("cannot remove root")
	}
	removePatternAt(doc, tokens)
	return nil
}

func removePatternAt(cur any, tokens []string) {
	if len(tokens) == 0 {
		return
	}
	t := tokens[0]
	if len(tokens) == 1 {
		removePatternLeaf(cur, t)
		return
	}
	switch t {
	case "*":
		for _, child := range immediateChildren(cur) {
			removePatternAt(child, tokens[1:])
		}
	case "**":
		removePatternAt(cur, tokens[1:])
		for _, child := range immediateChildren(cur) {
			removePatternAt(child, tokens)
		}
	default:
		switch v := cur.(type) {
		case map[string]any:
			if child, ok := v[t]; ok {
				removePatternAt(child, tokens[1:])
			}
		case []any:
			idx, err := strconv.Atoi(t)
			if err == nil && idx >= 0 && idx < len(v) {
				removePatternAt(v[idx], tokens[1:])
			}
		}
	}
}

func removePatternLeaf(cur any, token string) {
	switch v := cur.(type) {
	case map[string]any:
		switch token {
		case "*":
			for k := range v {
				delete(v, k)
			}
		case "**":
			for k := range v {
				delete(v, k)
			}
			for _, child := range immediateChildren(v) {
				removePatternLeaf(child, token)
			}
		default:
			delete(v, token)
		}
	case []any:
		if token == "*" || token == "**" {
			for i := range v {
				v[i] = nil
			}
			if token == "**" {
				for _, child := range immediateChildren(v) {
					removePatternLeaf(child, token)
				}
			}
			return
		}
		idx, err := strconv.Atoi(token)
		if err == nil && idx >= 0 && idx < len(v) {
			copy(v[idx:], v[idx+1:])
			v[len(v)-1] = nil
			v = v[:len(v)-1]
		}
	}
}

func removeAt(cur any, tokens []string) (any, error) {
	if len(tokens) == 0 {
		return cur, nil
	}
	t := tokens[0]
	if len(tokens) == 1 {
		switch v := cur.(type) {
		case map[string]any:
			if _, ok := v[t]; !ok {
				return cur, ErrNotFound
			}
			delete(v, t)
			return v, nil
		case []any:
			idx, err := strconv.Atoi(t)
			if err != nil || idx < 0 || idx >= len(v) {
				return cur, ErrNotFound
			}
			return append(v[:idx], v[idx+1:]...), nil
		default:
			return cur, ErrNotFound
		}
	}
	switch v := cur.(type) {
	case map[string]any:
		n, ok := v[t]
		if !ok {
			return cur, ErrNotFound
		}
		updated, err := removeAt(n, tokens[1:])
		if err != nil {
			return cur, err
		}
		v[t] = updated
		return v, nil
	case []any:
		idx, err := strconv.Atoi(t)
		if err != nil || idx < 0 || idx >= len(v) {
			return cur, ErrNotFound
		}
		updated, err := removeAt(v[idx], tokens[1:])
		if err != nil {
			return cur, err
		}
		v[idx] = updated
		return v, nil
	default:
		return cur, ErrNotFound
	}
}

func pointerJoin(base, key string) string {
	if base == "" || base == "/" {
		return "/" + strings.TrimPrefix(key, "/")
	}
	return strings.TrimSuffix(base, "/") + "/" + strings.TrimPrefix(key, "/")
}
