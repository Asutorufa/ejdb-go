package ejdb

import (
	"encoding/binary"
	"io"
	"math"
	"sort"
	"strconv"

	"github.com/go-json-experiment/json/jsontext"
)

const (
	binnList   byte = 0xe0
	binnMap    byte = 0xe1
	binnObject byte = 0xe2

	binnNull  byte = 0x00
	binnTrue  byte = 0x01
	binnFalse byte = 0x02

	binnUint8  byte = 0x20
	binnInt8   byte = 0x21
	binnUint16 byte = 0x40
	binnInt16  byte = 0x41
	binnUint32 byte = 0x60
	binnInt32  byte = 0x61
	binnUint64 byte = 0x80
	binnInt64  byte = 0x81

	binnString      byte = 0xa0
	binnDateTime    byte = 0xa1
	binnDate        byte = 0xa2
	binnTime        byte = 0xa3
	binnDecimal     byte = 0xa4
	binnCurrencyStr byte = 0xa5
	binnSingleStr   byte = 0xa6
	binnDoubleStr   byte = 0xa7

	binnFloat32  byte = 0x62
	binnFloat64  byte = 0x82
	binnDouble   byte = binnFloat64
	binnCurrency byte = 0x83
	binnBlob     byte = 0xc0
)

const (
	binnStorageMask      = 0xe0
	binnStorageHasMore   = 0x10
	binnStorageNoBytes   = 0x00
	binnStorageByte      = 0x20
	binnStorageWord      = 0x40
	binnStorageDWord     = 0x60
	binnStorageQWord     = 0x80
	binnStorageString    = 0xa0
	binnStorageBlob      = 0xc0
	binnStorageContainer = 0xe0

	binnHTML       = 0xb001
	binnXML        = 0xb002
	binnJSON       = 0xb003
	binnJavaScript = 0xb004
	binnCSS        = 0xb005

	binnJPEG = 0xd001
	binnGIF  = 0xd002
	binnPNG  = 0xd003
	binnBMP  = 0xd004
)

const maxJBLInt64 = uint64(^uint64(0) >> 1)
const maxBinnObjectKeyLen = 255

type binnTypedValue struct {
	Type  int
	Value any
}

type binnBlobValue []byte

func encodeStoredDocument(doc any) ([]byte, error) {
	switch doc.(type) {
	case map[string]any, []any:
		return binnEncodeValue(doc)
	default:
		return nil, withCodef(CodeInvalidValueType, "JBL document root must be object or array, got %T", doc)
	}
}

func decodeStoredDocument(raw []byte) (jsontext.Value, any, error) {
	if !isJBLDocument(raw) {
		return nil, nil, withCode(CodeIncompatibleFormat, "stored document is not a JBL/Binn document")
	}
	dec := binnDecoder{buf: raw}
	v, err := dec.value()
	if err != nil {
		return nil, nil, err
	}
	if dec.off != len(dec.buf) {
		return nil, nil, withCode(CodeIncompatibleFormat, "stored document has trailing JBL/Binn bytes")
	}
	canon, err := marshalJSON(v)
	if err != nil {
		return nil, nil, err
	}
	return jsontext.Value(canon), v, nil
}

func isJBLDocument(raw []byte) bool {
	if len(raw) < 3 {
		return false
	}
	typ, size, _, header, ok := binnHeader(raw)
	return ok && (typ == int(binnObject) || typ == int(binnList) || typ == int(binnMap)) && size == len(raw) && header <= size
}

func binnEncodeValue(v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return []byte{binnNull}, nil
	case bool:
		if x {
			return []byte{binnTrue}, nil
		}
		return []byte{binnFalse}, nil
	case string:
		return appendBinnString(nil, int(binnString), x), nil
	case binnBlobValue:
		return appendBinnBlob(nil, int(binnBlob), []byte(x)), nil
	case binnTypedValue:
		return binnEncodeTypedValue(x)
	case jsonNumber:
		if i, err := x.Int64(); err == nil {
			return binnEncodeInt(i), nil
		}
		f, err := x.Float64()
		if err != nil {
			return nil, withCodef(CodeInvalidValueType, "invalid JSON number %q", x)
		}
		return binnEncodeDouble(f), nil
	case float64:
		if i := int64(x); float64(i) == x {
			return binnEncodeInt(i), nil
		}
		return binnEncodeDouble(x), nil
	case float32:
		return binnEncodeFloat32(x), nil
	case int:
		return binnEncodeInt(int64(x)), nil
	case int64:
		return binnEncodeInt(x), nil
	case int32:
		return binnEncodeInt(int64(x)), nil
	case int16:
		return binnEncodeInt(int64(x)), nil
	case int8:
		return binnEncodeInt(int64(x)), nil
	case uint:
		return binnEncodeUint(uint64(x))
	case uint64:
		return binnEncodeUint(x)
	case uint32:
		return binnEncodeUint(uint64(x))
	case uint16:
		return binnEncodeUint(uint64(x))
	case uint8:
		return binnEncodeUint(uint64(x))
	case []any:
		payload := make([]byte, 0)
		for _, it := range x {
			b, err := binnEncodeValue(it)
			if err != nil {
				return nil, err
			}
			payload = append(payload, b...)
		}
		return binnContainer(int(binnList), len(x), payload), nil
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			if len(k) > maxBinnObjectKeyLen {
				return nil, withCodef(CodeInvalidValueType, "JBL/Binn object key length exceeds %d bytes: %q", maxBinnObjectKeyLen, k)
			}
			keys = append(keys, k)
		}
		sort.Strings(keys)
		payload := make([]byte, 0)
		for _, k := range keys {
			payload = append(payload, byte(len(k)))
			payload = append(payload, k...)
			b, err := binnEncodeValue(x[k])
			if err != nil {
				return nil, err
			}
			payload = append(payload, b...)
		}
		return binnContainer(int(binnObject), len(keys), payload), nil
	default:
		return nil, withCodef(CodeInvalidValueType, "unsupported JBL value type %T", v)
	}
}

func binnEncodeTypedValue(v binnTypedValue) ([]byte, error) {
	switch binnStorage(v.Type) {
	case binnStorageString:
		s, ok := v.Value.(string)
		if !ok {
			return nil, withCodef(CodeInvalidValueType, "Binn string-family type 0x%x requires string value, got %T", v.Type, v.Value)
		}
		return appendBinnString(nil, v.Type, s), nil
	case binnStorageBlob:
		b, ok := v.Value.([]byte)
		if !ok {
			return nil, withCodef(CodeInvalidValueType, "Binn blob-family type 0x%x requires []byte value, got %T", v.Type, v.Value)
		}
		return appendBinnBlob(nil, v.Type, b), nil
	case binnStorageDWord:
		if v.Type == int(binnFloat32) {
			switch x := v.Value.(type) {
			case float32:
				return binnEncodeFloat32(x), nil
			case float64:
				return binnEncodeFloat32(float32(x)), nil
			}
		}
	case binnStorageQWord:
		switch v.Type {
		case int(binnFloat64):
			switch x := v.Value.(type) {
			case float64:
				return binnEncodeDouble(x), nil
			case float32:
				return binnEncodeDouble(float64(x)), nil
			}
		case int(binnCurrency):
			return binnEncodeIntLike(v.Type, v.Value)
		}
	}
	return nil, withCodef(CodeInvalidValueType, "unsupported Binn typed value 0x%x (%T)", v.Type, v.Value)
}

func binnEncodeIntLike(typ int, v any) ([]byte, error) {
	var i int64
	switch x := v.(type) {
	case int64:
		i = x
	case int:
		i = int64(x)
	case int32:
		i = int64(x)
	case uint64:
		if x > maxJBLInt64 {
			return nil, withCodef(CodeInvalidValueType, "unsigned integer overflows int64: %d", x)
		}
		i = int64(x)
	default:
		return nil, withCodef(CodeInvalidValueType, "Binn integer-family type 0x%x requires integer value, got %T", typ, v)
	}
	out := appendBinnType(nil, typ)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(i))
	return append(out, buf[:]...), nil
}

func binnEncodeUint(v uint64) ([]byte, error) {
	if v > maxJBLInt64 {
		return nil, withCodef(CodeInvalidValueType, "unsigned integer overflows int64: %d", v)
	}
	return binnEncodeInt(int64(v)), nil
}

func binnEncodeInt(v int64) []byte {
	switch {
	case v >= 0 && v <= math.MaxUint8:
		return []byte{binnUint8, byte(v)}
	case v >= math.MinInt8 && v <= math.MaxInt8:
		return []byte{binnInt8, byte(int8(v))}
	case v >= 0 && v <= math.MaxUint16:
		out := []byte{binnUint16, 0, 0}
		binary.BigEndian.PutUint16(out[1:], uint16(v))
		return out
	case v >= math.MinInt16 && v <= math.MaxInt16:
		out := []byte{binnInt16, 0, 0}
		binary.BigEndian.PutUint16(out[1:], uint16(int16(v)))
		return out
	case v >= 0 && v <= math.MaxUint32:
		out := []byte{binnUint32, 0, 0, 0, 0}
		binary.BigEndian.PutUint32(out[1:], uint32(v))
		return out
	case v >= math.MinInt32 && v <= math.MaxInt32:
		out := []byte{binnInt32, 0, 0, 0, 0}
		binary.BigEndian.PutUint32(out[1:], uint32(int32(v)))
		return out
	default:
		out := []byte{binnInt64, 0, 0, 0, 0, 0, 0, 0, 0}
		binary.BigEndian.PutUint64(out[1:], uint64(v))
		return out
	}
}

func binnEncodeFloat32(v float32) []byte {
	out := []byte{binnFloat32, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(out[1:], math.Float32bits(v))
	return out
}

func binnEncodeDouble(v float64) []byte {
	out := []byte{binnDouble, 0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(out[1:], math.Float64bits(v))
	return out
}

func appendBinnString(dst []byte, typ int, s string) []byte {
	dst = appendBinnType(dst, typ)
	dst = appendBinnSized(dst, len(s))
	dst = append(dst, s...)
	return append(dst, 0)
}

func appendBinnBlob(dst []byte, typ int, b []byte) []byte {
	dst = appendBinnType(dst, typ)
	var sz [4]byte
	binary.BigEndian.PutUint32(sz[:], uint32(len(b)))
	dst = append(dst, sz[:]...)
	return append(dst, b...)
}

func appendBinnType(dst []byte, typ int) []byte {
	if typ > 0xff {
		return append(dst, byte(typ>>8), byte(typ))
	}
	return append(dst, byte(typ))
}

func binnContainer(typ int, count int, payload []byte) []byte {
	size := 1 + 1 + binnSizedLen(count) + len(payload)
	for {
		next := 1 + binnSizedLen(size) + binnSizedLen(count) + len(payload)
		if next == size {
			break
		}
		size = next
	}
	dst := appendBinnType(nil, typ)
	dst = appendBinnSized(dst, size)
	dst = appendBinnSized(dst, count)
	return append(dst, payload...)
}

func binnSizedLen(v int) int {
	if v > 127 {
		return 4
	}
	return 1
}

func appendBinnSized(dst []byte, v int) []byte {
	if v > 127 {
		var tmp [4]byte
		binary.BigEndian.PutUint32(tmp[:], uint32(v)|0x80000000)
		return append(dst, tmp[:]...)
	}
	return append(dst, byte(v))
}

type binnDecoder struct {
	buf []byte
	off int
}

func (d *binnDecoder) value() (any, error) {
	typ, storage, err := d.readType()
	if err != nil {
		return nil, err
	}
	switch typ {
	case int(binnNull):
		return nil, nil
	case int(binnTrue):
		return true, nil
	case int(binnFalse):
		return false, nil
	case int(binnUint8):
		v, err := d.byte()
		return jsonNumber(strconv.FormatUint(uint64(v), 10)), err
	case int(binnInt8):
		v, err := d.byte()
		return jsonNumber(strconv.FormatInt(int64(int8(v)), 10)), err
	case int(binnUint16):
		v, err := d.u16()
		return jsonNumber(strconv.FormatUint(uint64(v), 10)), err
	case int(binnInt16):
		v, err := d.u16()
		return jsonNumber(strconv.FormatInt(int64(int16(v)), 10)), err
	case int(binnUint32):
		v, err := d.u32()
		return jsonNumber(strconv.FormatUint(uint64(v), 10)), err
	case int(binnInt32):
		v, err := d.u32()
		return jsonNumber(strconv.FormatInt(int64(int32(v)), 10)), err
	case int(binnUint64):
		v, err := d.u64()
		return jsonNumber(strconv.FormatUint(v, 10)), err
	case int(binnInt64), int(binnCurrency):
		v, err := d.u64()
		return jsonNumber(strconv.FormatInt(int64(v), 10)), err
	case int(binnFloat32):
		v, err := d.u32()
		return jsonNumber(strconv.FormatFloat(float64(math.Float32frombits(v)), 'g', -1, 32)), err
	case int(binnFloat64):
		v, err := d.u64()
		return jsonNumber(strconv.FormatFloat(math.Float64frombits(v), 'g', -1, 64)), err
	case int(binnString), int(binnDateTime), int(binnDate), int(binnTime), int(binnDecimal), int(binnCurrencyStr), int(binnSingleStr), int(binnDoubleStr), binnHTML, binnXML, binnJSON, binnJavaScript, binnCSS:
		v, err := d.string()
		return v, err
	case int(binnBlob), binnJPEG, binnGIF, binnPNG, binnBMP:
		v, err := d.blob()
		return []byte(v), err
	case int(binnList):
		_, count, end, err := d.containerAfterType(typ)
		if err != nil {
			return nil, err
		}
		out := make([]any, 0, count)
		for i := 0; i < count; i++ {
			v, err := d.value()
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		if d.off != end {
			return nil, withCode(CodeIncompatibleFormat, "invalid JBL/Binn list size")
		}
		return out, nil
	case int(binnObject):
		_, count, end, err := d.containerAfterType(typ)
		if err != nil {
			return nil, err
		}
		out := make(map[string]any, count)
		for i := 0; i < count; i++ {
			keyLen, err := d.byte()
			if err != nil {
				return nil, err
			}
			key, err := d.bytes(int(keyLen))
			if err != nil {
				return nil, err
			}
			v, err := d.value()
			if err != nil {
				return nil, err
			}
			out[string(key)] = v
		}
		if d.off != end {
			return nil, withCode(CodeIncompatibleFormat, "invalid JBL/Binn object size")
		}
		return out, nil
	case int(binnMap):
		_, count, end, err := d.containerAfterType(typ)
		if err != nil {
			return nil, err
		}
		out := make(map[string]any, count)
		for i := 0; i < count; i++ {
			id, err := d.u32()
			if err != nil {
				return nil, err
			}
			v, err := d.value()
			if err != nil {
				return nil, err
			}
			out[strconv.FormatInt(int64(int32(id)), 10)] = v
		}
		if d.off != end {
			return nil, withCode(CodeIncompatibleFormat, "invalid JBL/Binn map size")
		}
		return out, nil
	default:
		return nil, withCodef(CodeIncompatibleFormat, "unknown JBL/Binn tag: 0x%x storage=0x%x", typ, storage)
	}
}

func (d *binnDecoder) readType() (typ int, storage byte, err error) {
	b, err := d.byte()
	if err != nil {
		return 0, 0, err
	}
	storage = b & binnStorageMask
	if b&binnStorageHasMore != 0 {
		next, err := d.byte()
		if err != nil {
			return 0, 0, err
		}
		return int(b)<<8 | int(next), storage, nil
	}
	return int(b), storage, nil
}

func (d *binnDecoder) containerAfterType(typ int) (size int, count int, end int, err error) {
	size, err = d.sized()
	if err != nil {
		return 0, 0, 0, err
	}
	count, err = d.sized()
	if err != nil {
		return 0, 0, 0, err
	}
	end = d.off + size - (binnTypeLen(typ) + binnSizedLen(size) + binnSizedLen(count))
	if size < 3 || end < d.off || end > len(d.buf) || (typ != int(binnList) && typ != int(binnObject) && typ != int(binnMap)) {
		return 0, 0, 0, withCode(CodeIncompatibleFormat, "invalid JBL/Binn container header")
	}
	return size, count, end, nil
}

func (d *binnDecoder) string() (string, error) {
	n, err := d.sized()
	if err != nil {
		return "", err
	}
	b, err := d.bytes(n)
	if err != nil {
		return "", err
	}
	zero, err := d.byte()
	if err != nil {
		return "", err
	}
	if zero != 0 {
		return "", withCode(CodeIncompatibleFormat, "JBL/Binn string is not null terminated")
	}
	return string(b), nil
}

func (d *binnDecoder) blob() ([]byte, error) {
	n, err := d.u32()
	if err != nil {
		return nil, err
	}
	return d.bytes(int(n))
}

func (d *binnDecoder) byte() (byte, error) {
	if d.off >= len(d.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	v := d.buf[d.off]
	d.off++
	return v, nil
}

func (d *binnDecoder) bytes(n int) ([]byte, error) {
	if n < 0 || len(d.buf)-d.off < n {
		return nil, io.ErrUnexpectedEOF
	}
	v := d.buf[d.off : d.off+n]
	d.off += n
	return v, nil
}

func (d *binnDecoder) sized() (int, error) {
	b, err := d.byte()
	if err != nil {
		return 0, err
	}
	if b&0x80 == 0 {
		return int(b), nil
	}
	if len(d.buf)-d.off < 3 {
		return 0, io.ErrUnexpectedEOF
	}
	v := uint32(b)<<24 | uint32(d.buf[d.off])<<16 | uint32(d.buf[d.off+1])<<8 | uint32(d.buf[d.off+2])
	d.off += 3
	return int(v & 0x7fffffff), nil
}

func (d *binnDecoder) u16() (uint16, error) {
	if len(d.buf)-d.off < 2 {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint16(d.buf[d.off : d.off+2])
	d.off += 2
	return v, nil
}

func (d *binnDecoder) u32() (uint32, error) {
	if len(d.buf)-d.off < 4 {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint32(d.buf[d.off : d.off+4])
	d.off += 4
	return v, nil
}

func (d *binnDecoder) u64() (uint64, error) {
	if len(d.buf)-d.off < 8 {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint64(d.buf[d.off : d.off+8])
	d.off += 8
	return v, nil
}

func binnHeader(raw []byte) (typ int, size int, count int, header int, ok bool) {
	if len(raw) < 3 {
		return 0, 0, 0, 0, false
	}
	d := binnDecoder{buf: raw}
	typ, _, err := d.readType()
	if err != nil {
		return 0, 0, 0, 0, false
	}
	size, err = d.sized()
	if err != nil {
		return 0, 0, 0, 0, false
	}
	count, err = d.sized()
	if err != nil {
		return 0, 0, 0, 0, false
	}
	return typ, size, count, d.off, size >= d.off && size <= len(raw)
}

func binnStorage(typ int) byte {
	if typ > 0xff {
		return byte(typ>>8) & binnStorageMask
	}
	return byte(typ) & binnStorageMask
}

func binnTypeLen(typ int) int {
	if typ > 0xff {
		return 2
	}
	return 1
}
