package ejdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"sort"
	"strconv"
)

var jblMagic = []byte{'E', 'J', 'B', 'L', 0x01}

const maxJBLInt64 = uint64(^uint64(0) >> 1)

const (
	jblNull byte = iota
	jblFalse
	jblTrue
	jblI64
	jblF64
	jblString
	jblArray
	jblObject
)

func encodeStoredDocument(doc any) ([]byte, error) {
	enc := jblEncoder{buf: append([]byte(nil), jblMagic...)}
	if err := enc.value(doc); err != nil {
		return nil, err
	}
	return enc.buf, nil
}

func decodeStoredDocument(raw []byte) (json.RawMessage, any, error) {
	if !isJBLDocument(raw) {
		return nil, nil, withCode(CodeIncompatibleFormat, "stored document is not EJBL binary")
	}
	dec := jblDecoder{buf: raw[len(jblMagic):]}
	v, err := dec.value()
	if err != nil {
		return nil, nil, err
	}
	if dec.off != len(dec.buf) {
		return nil, nil, withCode(CodeIncompatibleFormat, "stored document has trailing EJBL bytes")
	}
	canon, err := json.Marshal(v)
	if err != nil {
		return nil, nil, err
	}
	return json.RawMessage(canon), v, nil
}

func isJBLDocument(raw []byte) bool {
	return bytes.HasPrefix(raw, jblMagic)
}

type jblEncoder struct {
	buf []byte
}

func (e *jblEncoder) value(v any) error {
	switch x := v.(type) {
	case nil:
		e.buf = append(e.buf, jblNull)
	case bool:
		if x {
			e.buf = append(e.buf, jblTrue)
		} else {
			e.buf = append(e.buf, jblFalse)
		}
	case string:
		e.buf = append(e.buf, jblString)
		e.bytes([]byte(x))
	case json.Number:
		if i, err := x.Int64(); err == nil {
			e.i64(i)
			return nil
		}
		f, err := x.Float64()
		if err != nil {
			return withCodef(CodeInvalidValueType, "invalid JSON number %q", x)
		}
		e.f64(f)
	case float64:
		if i := int64(x); float64(i) == x {
			e.i64(i)
		} else {
			e.f64(x)
		}
	case float32:
		f := float64(x)
		if i := int64(f); float64(i) == f {
			e.i64(i)
		} else {
			e.f64(f)
		}
	case int:
		e.i64(int64(x))
	case int64:
		e.i64(x)
	case int32:
		e.i64(int64(x))
	case int16:
		e.i64(int64(x))
	case int8:
		e.i64(int64(x))
	case uint:
		if uint64(x) > maxJBLInt64 {
			return withCodef(CodeInvalidValueType, "unsigned integer overflows int64: %d", x)
		}
		e.i64(int64(x))
	case uint64:
		if x > maxJBLInt64 {
			return withCodef(CodeInvalidValueType, "unsigned integer overflows int64: %d", x)
		}
		e.i64(int64(x))
	case uint32:
		e.i64(int64(x))
	case uint16:
		e.i64(int64(x))
	case uint8:
		e.i64(int64(x))
	case []any:
		e.buf = append(e.buf, jblArray)
		e.uvarint(uint64(len(x)))
		for _, it := range x {
			if err := e.value(it); err != nil {
				return err
			}
		}
	case map[string]any:
		e.buf = append(e.buf, jblObject)
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		e.uvarint(uint64(len(keys)))
		for _, k := range keys {
			e.bytes([]byte(k))
			if err := e.value(x[k]); err != nil {
				return err
			}
		}
	default:
		return withCodef(CodeInvalidValueType, "unsupported EJBL value type %T", v)
	}
	return nil
}

func (e *jblEncoder) i64(v int64) {
	e.buf = append(e.buf, jblI64)
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
	e.buf = append(e.buf, tmp[:]...)
}

func (e *jblEncoder) f64(v float64) {
	e.buf = append(e.buf, jblF64)
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], math.Float64bits(v))
	e.buf = append(e.buf, tmp[:]...)
}

func (e *jblEncoder) bytes(v []byte) {
	e.uvarint(uint64(len(v)))
	e.buf = append(e.buf, v...)
}

func (e *jblEncoder) uvarint(v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	e.buf = append(e.buf, tmp[:n]...)
}

type jblDecoder struct {
	buf []byte
	off int
}

func (d *jblDecoder) value() (any, error) {
	tag, err := d.byte()
	if err != nil {
		return nil, err
	}
	switch tag {
	case jblNull:
		return nil, nil
	case jblFalse:
		return false, nil
	case jblTrue:
		return true, nil
	case jblI64:
		v, err := d.u64()
		if err != nil {
			return nil, err
		}
		return json.Number(strconv.FormatInt(int64(v), 10)), nil
	case jblF64:
		v, err := d.u64()
		if err != nil {
			return nil, err
		}
		return json.Number(strconv.FormatFloat(math.Float64frombits(v), 'g', -1, 64)), nil
	case jblString:
		v, err := d.bytes()
		if err != nil {
			return nil, err
		}
		return string(v), nil
	case jblArray:
		n, err := d.uvarint()
		if err != nil {
			return nil, err
		}
		out := make([]any, 0, n)
		for i := uint64(0); i < n; i++ {
			v, err := d.value()
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	case jblObject:
		n, err := d.uvarint()
		if err != nil {
			return nil, err
		}
		out := make(map[string]any, n)
		for i := uint64(0); i < n; i++ {
			k, err := d.bytes()
			if err != nil {
				return nil, err
			}
			v, err := d.value()
			if err != nil {
				return nil, err
			}
			out[string(k)] = v
		}
		return out, nil
	default:
		return nil, withCodef(CodeIncompatibleFormat, "unknown EJBL tag: %d", tag)
	}
}

func (d *jblDecoder) byte() (byte, error) {
	if d.off >= len(d.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	v := d.buf[d.off]
	d.off++
	return v, nil
}

func (d *jblDecoder) u64() (uint64, error) {
	if len(d.buf)-d.off < 8 {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint64(d.buf[d.off : d.off+8])
	d.off += 8
	return v, nil
}

func (d *jblDecoder) uvarint() (uint64, error) {
	v, n := binary.Uvarint(d.buf[d.off:])
	if n <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	d.off += n
	return v, nil
}

func (d *jblDecoder) bytes() ([]byte, error) {
	n, err := d.uvarint()
	if err != nil {
		return nil, err
	}
	if n > uint64(len(d.buf)-d.off) {
		return nil, io.ErrUnexpectedEOF
	}
	v := d.buf[d.off : d.off+int(n)]
	d.off += int(n)
	return v, nil
}
