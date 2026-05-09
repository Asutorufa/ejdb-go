package ejdb

import (
	"bytes"
	"fmt"
	"io"
	"strconv"

	json "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
)

type jsonNumber string

func (n jsonNumber) String() string {
	return string(n)
}

func (n jsonNumber) Float64() (float64, error) {
	return strconv.ParseFloat(string(n), 64)
}

func (n jsonNumber) Int64() (int64, error) {
	return strconv.ParseInt(string(n), 10, 64)
}

func (n jsonNumber) MarshalJSON() ([]byte, error) {
	v := jsontext.Value(n)
	if !v.IsValid(jsontext.AllowDuplicateNames(true), jsontext.AllowInvalidUTF8(true)) || v.Kind() != jsontext.KindNumber {
		return nil, fmt.Errorf("invalid JSON number %q", n)
	}
	return []byte(n), nil
}

func marshalJSON(v any) ([]byte, error) {
	return json.Marshal(v,
		json.Deterministic(true),
		json.FormatNilMapAsNull(true),
		json.FormatNilSliceAsNull(true),
		jsontext.AllowDuplicateNames(true),
		jsontext.AllowInvalidUTF8(true),
	)
}

func unmarshalJSON(raw []byte, out any) error {
	return json.Unmarshal(raw, out,
		jsontext.AllowDuplicateNames(true),
		jsontext.AllowInvalidUTF8(true),
	)
}

func decodeJSONAny(raw []byte) (any, error) {
	dec := jsontext.NewDecoder(bytes.NewReader(raw),
		jsontext.AllowDuplicateNames(true),
		jsontext.AllowInvalidUTF8(true),
	)
	v, err := readJSONAny(dec)
	if err != nil {
		return nil, err
	}
	if tok, err := dec.ReadToken(); err != io.EOF {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("invalid JSON after top-level value: %s", tok.Kind())
	}
	return v, nil
}

func readJSONAny(dec *jsontext.Decoder) (any, error) {
	switch dec.PeekKind() {
	case jsontext.KindNull:
		if _, err := dec.ReadToken(); err != nil {
			return nil, err
		}
		return nil, nil
	case jsontext.KindFalse, jsontext.KindTrue:
		tok, err := dec.ReadToken()
		if err != nil {
			return nil, err
		}
		return tok.Bool(), nil
	case jsontext.KindString:
		tok, err := dec.ReadToken()
		if err != nil {
			return nil, err
		}
		return tok.String(), nil
	case jsontext.KindNumber:
		tok, err := dec.ReadToken()
		if err != nil {
			return nil, err
		}
		return jsonNumber(tok.String()), nil
	case jsontext.KindBeginArray:
		if _, err := dec.ReadToken(); err != nil {
			return nil, err
		}
		out := []any{}
		for dec.PeekKind() != jsontext.KindEndArray {
			v, err := readJSONAny(dec)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		if _, err := dec.ReadToken(); err != nil {
			return nil, err
		}
		return out, nil
	case jsontext.KindBeginObject:
		if _, err := dec.ReadToken(); err != nil {
			return nil, err
		}
		out := make(map[string]any)
		for dec.PeekKind() != jsontext.KindEndObject {
			name, err := dec.ReadToken()
			if err != nil {
				return nil, err
			}
			if name.Kind() != jsontext.KindString {
				return nil, fmt.Errorf("invalid JSON object name: %s", name.Kind())
			}
			key := name.String()
			v, err := readJSONAny(dec)
			if err != nil {
				return nil, err
			}
			out[key] = v
		}
		if _, err := dec.ReadToken(); err != nil {
			return nil, err
		}
		return out, nil
	default:
		_, err := dec.ReadToken()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("invalid JSON value")
	}
}
