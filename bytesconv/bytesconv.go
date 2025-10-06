package bytesconv

import (
	"bytes"
	"errors"
)

var base10Map = map[int]byte{0: '0',
	1: '1',
	2: '2',
	3: '3',
	4: '4',
	5: '5',
	6: '6',
	7: '7',
	8: '8',
	9: '9',
}

var revers10Map = map[byte]int{'0': 0,
	'1': 1,
	'2': 2,
	'3': 3,
	'4': 4,
	'5': 5,
	'6': 6,
	'7': 7,
	'8': 8,
	'9': 9,
}

var revers16Map = map[byte]int{'0': 0,
	'1': 1,
	'2': 2,
	'3': 3,
	'4': 4,
	'5': 5,
	'6': 6,
	'7': 7,
	'8': 8,
	'9': 9,
	'a': 10,
	'A': 10,
	'b': 11,
	'B': 11,
	'c': 12,
	'C': 12,
	'd': 13,
	'D': 13,
	'e': 14,
	'E': 14,
	'f': 15,
	'F': 15,
}

const (
	cMaxUint = ^uint(0)
	cMinUint = 0
	cMaxInt  = int(cMaxUint >> 1)
	cMinInt  = -cMaxInt - 1

	cMaxUint64 = ^uint64(0)
	cMinUint64 = 0
	cMaxInt64  = int64(cMaxUint64 >> 1)
	cMinInt64  = -cMaxInt64 - 1

	cInt8MaxBase   = 100
	cInt16MaxBase  = 10000
	cInt32MaxBase  = 1000000000
	cUInt64MaxBase = 10000000000000000000
	cInt64MaxBase  = 1000000000000000000
)

/*

uint8  : 0 to 255
uint16 : 0 to 65535
uint32 : 0 to 4294967295
uint64 : 0 to 18446744073709551615
int8   : -128 to 127
int16  : -32768 to 32767
int32  : -2147483648 to 2147483647
int64  : -9223372036854775808 to 9223372036854775807

*/

func FormatInt8(val int8, dst []byte) []byte {

	if val == 0 {
		dst = append(dst, '0')
		return dst
	}

	if val < 0 {
		dst = append(dst, '-')
		val = -val
	}

	base := int8(cInt8MaxBase)
	flag := false
	for base > 0 {
		v := val / base
		if v < 0 {
			v = -v
		}
		if flag || v != 0 {
			//dst = append(dst, base10Map[int(v)])
			dst = append(dst, byte(v)+48)
			flag = true
		}

		val -= (v * base)
		base /= 10
	}
	return dst
}

func FormatUint8(val uint8, dst []byte) []byte {
	if val == 0 {
		dst = append(dst, '0')
		return dst
	}

	base := uint8(cInt8MaxBase)
	flag := false
	for base > 0 {
		v := val / base
		if flag || v != 0 {
			dst = append(dst, base10Map[int(v)])
			flag = true
		}

		val -= (v * base)
		base /= 10
	}
	return dst
}

func FormatInt16(val int16, dst []byte) []byte {
	if val == 0 {
		dst = append(dst, '0')
		return dst
	}

	if val < 0 {
		dst = append(dst, '-')
		val = -val
	}

	base := int16(cInt16MaxBase)
	flag := false
	for base > 0 {
		v := val / base
		if flag || v != 0 {
			dst = append(dst, base10Map[int(v)])
			flag = true
		}

		val -= (v * base)
		base /= 10
	}
	return dst
}

func FormatUint16(val uint16, dst []byte) []byte {
	if val == 0 {
		dst = append(dst, '0')
		return dst
	}

	base := uint16(cInt16MaxBase)
	flag := false
	for base > 0 {
		v := val / base
		if flag || v != 0 {
			dst = append(dst, base10Map[int(v)])
			flag = true
		}

		val -= (v * base)
		base /= 10
	}
	return dst
}

func FormatInt32(val int32, dst []byte) []byte {
	if val == 0 {
		dst = append(dst, '0')
		return dst
	}

	if val < 0 {
		dst = append(dst, '-')
		val = -val
	}
	base := int32(cInt32MaxBase)
	flag := false
	for base > 0 {
		v := val / base
		if v < 0 {
			v = -v
		}
		if flag || v != 0 {
			dst = append(dst, base10Map[int(v)])
			flag = true
		}

		val -= (v * base)
		base /= 10
	}
	return dst
}

func FormatUint32(val uint32, dst []byte) []byte {
	if val == 0 {
		dst = append(dst, '0')
		return dst
	}

	base := uint32(cInt32MaxBase)
	flag := false
	for base > 0 {
		v := val / base
		if flag || v != 0 {
			dst = append(dst, base10Map[int(v)])
			flag = true
		}

		val -= (v * base)
		base /= 10
	}
	return dst
}

func FormatInt64(val int64, dst []byte) []byte {
	if val == 0 {
		dst = append(dst, '0')
		return dst
	}

	if val < 0 {
		dst = append(dst, '-')
		val = -val
	}
	base := int64(cInt64MaxBase)
	flag := false
	for base > 0 {
		v := val / base
		if v < 0 {
			v = -v
		}
		if flag || v != 0 {
			dst = append(dst, base10Map[int(v)])
			flag = true
		}

		val -= (v * base)
		base /= 10
	}
	return dst
}

func FormatUint64(val uint64, dst []byte) []byte {
	if val == 0 {
		dst = append(dst, '0')
		return dst
	}

	base := uint64(cUInt64MaxBase)
	flag := false
	for base > 0 {
		v := val / base
		if flag || v != 0 {
			dst = append(dst, base10Map[int(v)])
			flag = true
		}

		val -= (v * base)
		base /= 10
	}
	return dst
}

func ParseBool(src []byte) (res bool, err error) {
	if len(src) == 0 {
		err = errors.New("Incorrect data: empty source")
		return
	}

	src = bytes.ToLower(src)

	switch {
	case len(src) == 1 && (src[0] == '1' || src[0] == 't'):
		res = true

	case len(src) == 1 && (src[0] == '0' || src[0] == 'f'):
		res = false

	case len(src) == 4 && bytes.Equal(src, []byte("true")):
		res = true

	case len(src) == 5 && bytes.Equal(src, []byte("false")):
		res = false

	default:
		err = errors.New("Incorrect data: empty source: " + string(src))
		return
	}
	return
}

func checkSign(src []byte) (dst []byte, flag bool, err error) {
	if src[0] == '-' {
		if len(src) < 2 {
			err = errors.New("Incorrect data: " + string(src))
			return
		}
		flag = true
		src = src[1:]
	}
	if src[0] == '+' {
		if len(src) < 2 {
			err = errors.New("Incorrect data: " + string(src))
			return
		}
		src = src[1:]
	}
	dst = src
	return
}

func ParseInt8(src []byte) (result int8, err error) {
	if len(src) == 0 {
		err = errors.New("Incorrect data: empty source")
		return
	}
	flag := false
	if src, flag, err = checkSign(src); err != nil {
		return
	}

	base := int8(1)
	for i := len(src) - 1; i >= 0; i-- {
		if base > cInt8MaxBase {
			err = errors.New("Incorrect data: " + string(src))
			return
		}

		if v, ok := revers10Map[src[i]]; !ok {
			err = errors.New("Incorrect data: " + string(src))
			return
		} else {
			result += base * int8(v)
		}
		base *= 10
	}
	if flag {
		result = -result
	}
	return
}

func ParseUint8(src []byte) (result uint8, err error) {
	if len(src) == 0 {
		err = errors.New("Incorrect data: empty source")
		return
	}
	base := uint8(1)
	for i := len(src) - 1; i >= 0; i-- {
		if base > cInt8MaxBase {
			err = errors.New("Incorrect data: " + string(src))
			return
		}

		if v, ok := revers10Map[src[i]]; !ok {
			err = errors.New("Incorrect data: " + string(src))
			return
		} else {
			result += base * uint8(v)
		}
		base *= 10
	}
	return
}

func ParseInt16(src []byte) (result int16, err error) {
	if len(src) == 0 {
		err = errors.New("Incorrect data: empty source")
		return
	}
	flag := false
	if src, flag, err = checkSign(src); err != nil {
		return
	}

	base := int16(1)
	for i := len(src) - 1; i >= 0; i-- {
		if base > cInt16MaxBase {
			err = errors.New("Incorrect data: " + string(src))
			return
		}

		if v, ok := revers10Map[src[i]]; !ok {
			err = errors.New("Incorrect data: " + string(src))
			return
		} else {
			result += base * int16(v)
		}
		base *= 10
	}
	if flag {
		result = -result
	}
	return
}

func ParseUint16(src []byte) (result uint16, err error) {
	if len(src) == 0 {
		err = errors.New("Incorrect data: empty source")
		return
	}
	base := uint16(1)
	for i := len(src) - 1; i >= 0; i-- {
		if base > cInt16MaxBase {
			err = errors.New("Incorrect data: " + string(src))
			return
		}

		if v, ok := revers10Map[src[i]]; !ok {
			err = errors.New("Incorrect data: " + string(src))
			return
		} else {
			result += base * uint16(v)
		}
		base *= 10
	}
	return
}

func ParseInt32(src []byte) (result int32, err error) {
	if len(src) == 0 {
		err = errors.New("Incorrect data: empty source")
		return
	}
	flag := false
	if src, flag, err = checkSign(src); err != nil {
		return
	}

	base := int32(1)
	for i := len(src) - 1; i >= 0; i-- {
		if base > cInt32MaxBase {
			err = errors.New("Incorrect data: " + string(src))
			return
		}

		if v, ok := revers10Map[src[i]]; !ok {
			err = errors.New("Incorrect data: " + string(src))
			return
		} else {
			result += base * int32(v)
		}
		base *= 10
	}
	if flag {
		result = -result
	}
	return
}

func ParseUint32(src []byte) (result uint32, err error) {
	if len(src) == 0 {
		err = errors.New("Incorrect data: empty source")
		return
	}
	base := uint32(1)
	for i := len(src) - 1; i >= 0; i-- {
		if base > cInt32MaxBase {
			err = errors.New("Incorrect data: " + string(src))
			return
		}

		if v, ok := revers10Map[src[i]]; !ok {
			err = errors.New("Incorrect data: " + string(src))
			return
		} else {
			result += base * uint32(v)
		}
		base *= 10
	}
	return
}

func ParseInt64(src []byte) (result int64, err error) {
	if len(src) == 0 {
		err = errors.New("Incorrect data: empty source")
		return
	}
	flag := false
	if src, flag, err = checkSign(src); err != nil {
		return
	}

	base := int64(1)
	for i := len(src) - 1; i >= 0; i-- {
		if base > cInt64MaxBase {
			err = errors.New("Incorrect data: " + string(src))
			return
		}

		if v, ok := revers10Map[src[i]]; !ok {
			err = errors.New("Incorrect data: " + string(src))
			return
		} else {
			result += base * int64(v)
		}
		base *= 10
	}
	if flag {
		result = -result
	}
	return
}

func ParseInt(src []byte) (result int, err error) {
	if len(src) == 0 {
		err = errors.New("Incorrect data: empty source")
		return
	}
	flag := false
	if src, flag, err = checkSign(src); err != nil {
		return
	}

	maxIntBase := int(0)

	if int64(cMaxInt) < cMaxInt64 {
		maxIntBase = int(cInt32MaxBase)
	} else {
		maxIntBase = int(cInt64MaxBase)
	}

	base := int(1)
	for i := len(src) - 1; i >= 0; i-- {
		if base > maxIntBase {
			err = errors.New("Incorrect data: " + string(src))
			return
		}

		if v, ok := revers10Map[src[i]]; !ok {
			err = errors.New("Incorrect data: " + string(src))
			return
		} else {
			result += base * int(v)
		}
		base *= 10
	}
	if flag {
		result = -result
	}
	return
}

func ParseUint64(src []byte) (result uint64, err error) {
	if len(src) == 0 {
		err = errors.New("Incorrect data: empty source")
		return
	}
	base := uint64(1)
	for i := len(src) - 1; i >= 0; i-- {
		if base > cUInt64MaxBase {
			err = errors.New("Incorrect data: " + string(src))
			return
		}

		if v, ok := revers10Map[src[i]]; !ok {
			err = errors.New("Incorrect data: " + string(src))
			return
		} else {
			result += base * uint64(v)
		}
		base *= 10
	}
	return
}

func ParseUint(src []byte) (result uint, err error) {
	if len(src) == 0 {
		err = errors.New("Incorrect data: empty source")
		return
	}

	maxUintBase := uint(0)

	if uint64(cMaxUint) < cMaxUint64 {
		maxUintBase = uint(cInt32MaxBase)
	} else {
		maxUintBase = uint(cUInt64MaxBase)
	}

	base := uint(1)
	for i := len(src) - 1; i >= 0; i-- {
		if base > maxUintBase {
			err = errors.New("Incorrect data: " + string(src))
			return
		}

		if v, ok := revers10Map[src[i]]; !ok {
			err = errors.New("Incorrect data: " + string(src))
			return
		} else {
			result += base * uint(v)
		}
		base *= 10
	}
	return
}

func FormatFloat64(val float64, n int, dst []byte) []byte {
	if val == 0 {
		dst = append(dst, '0')
		return dst
	}

	// sign...
	if val < 0 {
		dst = append(dst, '-')
		val = -val
	}
	v := int64(val)
	if v != 0 {
		dst = FormatInt64(v, dst)
	} else {
		dst = append(dst, '0')
	}
	dst = append(dst, '.')

	val = val - float64(v)
	if val == 0 {
		dst = append(dst, '0')
		return dst
	}

	for i := 0; i < n; i++ {
		val *= 10
		v := int(val)
		dst = append(dst, base10Map[v])
		val = val - float64(v)
		if val == 0 {
			break
		}
	}
	return dst
}

func ParseFloat64(src []byte) (result float64, err error) {

	defer func() {
		if err != nil {
			result = 0
		}
	}()

	if len(src) == 0 {
		err = errors.New("Incorrect data: empty source")
		return
	}
	flag := false
	if src, flag, err = checkSign(src); err != nil {
		return
	}

	var exp = int(0)
	if pos := bytes.IndexByte(src, 'e'); pos > 0 {
		if pos < (len(src) - 1) {
			if exp, err = ParseInt(src[pos+1:]); err != nil {
				return
			}
		}
		src = src[:pos]
	}

	// find decimal point
	v := bytes.Split(src, []byte("."))
	if len(v) < 1 || len(v) > 2 {
		err = errors.New("Incorrect data: " + string(src))
		return
	}
	hasDecimal := (len(v) == 2)

	vv := uint64(0)
	if len(v[0]) > 0 {
		if vv, err = ParseUint64(v[0]); err != nil {
			return
		}
		result = float64(vv)
	}

	if hasDecimal {
		if vv, err = ParseUint64(v[1]); err != nil {
			return
		}

		base := uint64(1)
		for i := 0; i < len(v[1]); i++ {
			base *= 10
		}

		result += float64(vv) / float64(base)
	}

	if exp > 0 {
		for i := 0; i < exp; i++ {
			result *= 10
		}
	} else {
		exp = -exp
		for i := 0; i < exp; i++ {
			result /= 10
		}
	}

	if flag {
		result = -result
	}
	return

}

func HexParseUint32(src []byte) (result uint32, err error) {
	if len(src) == 0 {
		err = errors.New("Incorrect data: empty source")
		return
	}

	base := uint32(1)
	for i := len(src) - 1; i >= 0; i-- {
		if base > cInt32MaxBase {
			err = errors.New("Incorrect data: " + string(src))
			return
		}

		if v, ok := revers16Map[src[i]]; !ok {
			//err = errors.New("Incorrect data: " + string(src))
			return
		} else {
			result += base * uint32(v)
		}
		base *= 16
	}
	return
}

func HexCharUpper(c byte) byte {
	if c < 10 {
		return '0' + c
	}
	return c - 10 + 'A'
}
