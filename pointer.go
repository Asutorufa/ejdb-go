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
	tokens, err := pointerTokens(ptr)
	if err != nil {
		return err
	}
	if len(tokens) == 0 {
		return errors.New("cannot replace document root with pointerSet")
	}
	cur := doc
	for i := 0; i < len(tokens)-1; i++ {
		t := tokens[i]
		switch v := cur.(type) {
		case map[string]any:
			n, ok := v[t]
			if !ok {
				if !createMissing {
					return ErrNotFound
				}
				n = map[string]any{}
				v[t] = n
			}
			cur = n
		case []any:
			idx, err := strconv.Atoi(t)
			if err != nil || idx < 0 || idx >= len(v) {
				return ErrNotFound
			}
			cur = v[idx]
		default:
			return ErrNotFound
		}
	}
	last := tokens[len(tokens)-1]
	switch v := cur.(type) {
	case map[string]any:
		if _, ok := v[last]; !ok && !createMissing {
			return ErrNotFound
		}
		v[last] = val
		return nil
	case []any:
		if last == "-" && createMissing {
			v = append(v, val)
			return nil
		}
		idx, err := strconv.Atoi(last)
		if err != nil || idx < 0 {
			return ErrNotFound
		}
		if idx == len(v) && createMissing {
			v = append(v, val)
			return nil
		}
		if idx >= len(v) {
			return ErrNotFound
		}
		v[idx] = val
		return nil
	default:
		return ErrNotFound
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
