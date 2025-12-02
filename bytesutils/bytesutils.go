package bytesutils

import (
	"bytes"
)

func ReplaceBytes(src []byte, old []byte, new []byte) []byte {

	if len(src) == 0 || len(old) == 0 {
		return src
	}

	var a []byte

	prev := 0
	for {
		pos := bytes.Index(src[prev:], old)
		if pos < 0 {
			break
		}

		pos += prev

		if len(new) <= len(old) {

			copy(src[pos:], new)

			// copy tail
			if len(src) >= pos+len(old) {
				copy(src[pos+len(new):], src[pos+len(old):])
			}
			src = src[:len(src)-len(old)+len(new)]
		} else {
			if cap(src) > (len(src) + len(new) - len(old)) {
				a = src[:len(src)+len(new)-len(old)]
			} else {
				a = make([]byte, len(src)+(len(new)-len(old)))
			}

			// copy head
			copy(a, src[:pos])

			// copy tail
			if len(src) >= pos+len(old) {
				copy(a[pos+len(new):], src[pos+len(old):])
			}

			// copy new
			copy(a[pos:], new)

			src = a
		}
		prev = pos + len(new)
		if prev >= len(src) {
			break
		}
	}
	return src
}

func AsciiToLower(src []byte) []byte {

	for i := 0; i < len(src); i++ {
		if src[i] >= 'A' && src[i] <= 'Z' {
			src[i] += 32
		}
	}
	return src
}

func AsciiToUpper(src []byte) []byte {

	for i := 0; i < len(src); i++ {
		if src[i] >= 'a' && src[i] <= 'z' {
			src[i] -= 32
		}
	}
	return src
}

func TrimBytes(src []byte) []byte {
	for {
		i := bytes.IndexByte(src, ' ')
		if i < 0 {
			return src
		}
		copy(src[i:], src[i+1:])
		src = src[:len(src)-1]
	}
}

func EqualAscii(a []byte, b []byte) (ok bool) {

	if len(a) != len(b) {
		return
	}

	for i := 0; i < len(a); i++ {
		x := a[i]
		y := b[i]
		if x >= 'A' && x <= 'Z' {
			x += 32
		}
		if y >= 'A' && y <= 'Z' {
			y += 32
		}
		if ok = (x == y); !ok {
			break
		}
	}
	return
}

func ContainsAscii(a []byte, b []byte) (ok bool) {

	if len(b) > len(a) {
		return
	}

	for i := 0; i <= (len(a) - len(b)); i++ {

		for j := 0; j < len(b); j++ {
			x := a[i+j]
			y := b[j]
			if x >= 'A' && x <= 'Z' {
				x += 32
			}
			if y >= 'A' && y <= 'Z' {
				y += 32
			}

			if ok = (x == y); !ok {
				break
			}
		}
		if ok {
			break
		}
	}
	return
}

func InsertHead(src []byte, head []byte) []byte {
	if len(src)+len(head) < cap(src) {
		src = src[:len(src)+len(head)]
		copy(src[len(head):], src)
		copy(src, head)
	} else {
		a := make([]byte, len(src)+len(head))
		copy(a[len(head):], src)
		copy(a, head)
		src = a
	}
	return src
}

var (
	gHTMLEscOldAmp   = []byte("&")
	gHTMLEscNewAmp   = []byte("&amp;")
	gHTMLEscOld39    = []byte("'")
	gHTMLEscNew39    = []byte("&#39;")
	gHTMLEscOldLt    = []byte("<")
	gHTMLEscNewLt    = []byte("&lt;")
	gHTMLEscOldGt    = []byte(">")
	gHTMLEscNewGt    = []byte("&gt;")
	gHTMLEscOldQuot  = []byte("\"")
	gHTMLEscNewQuot  = []byte("&#34;")
	gHTMLEscOldSpace = []byte(" ")
	gHTMLEscNewSpace = []byte("%20")
)

func EscHTML(src []byte) []byte {
	src = ReplaceBytes(src, gHTMLEscOldAmp, gHTMLEscNewAmp)
	src = ReplaceBytes(src, gHTMLEscOld39, gHTMLEscNew39)
	src = ReplaceBytes(src, gHTMLEscOldLt, gHTMLEscNewLt)
	src = ReplaceBytes(src, gHTMLEscOldGt, gHTMLEscNewGt)
	src = ReplaceBytes(src, gHTMLEscOldQuot, gHTMLEscNewQuot)
	src = ReplaceBytes(src, gHTMLEscOldSpace, gHTMLEscNewSpace)

	return src

}

func ReplaceByte(src []byte, old byte, new byte) []byte {
	if len(src) == 0 || old == new {
		return src
	}

	for i := 0; i < len(src); i++ {
		if src[i] == old {
			src[i] = new
		}
	}

	return src
}

func Split(src []byte, sep []byte, dst [][]byte) [][]byte {

	for {
		if n := bytes.Index(src, sep); n < 0 {
			dst = append(dst, src)
			return dst
		} else {
			dst = append(dst, src[:n])
			if n+len(sep) == len(src) {
				return dst
			}
			src = src[n+len(sep):]
		}
	}
}

func SplitByte(src []byte, sep byte) (res [][]byte) {

	for {
		if n := bytes.IndexByte(src, sep); n < 0 {
			res = append(res, src)
			return
		} else {
			res = append(res, src[:n])
			if n+1 == len(src) {
				return
			}
			src = src[n+1:]
		}
	}
}

func Trim(src []byte) (dst []byte) {

	dst = src

	if len(src) == 0 {
		return
	}

	for dst[0] == ' ' || dst[len(dst)-1] == ' ' {
		if dst[0] == ' ' {
			dst = dst[1:]
		}
		if len(dst) == 0 {
			break
		}
		if dst[len(dst)-1] == ' ' {
			dst = dst[:len(dst)-1]
		}
	}
	return dst
}

/*

\0     An ASCII NUL (0x00) character.
\'     A single quote (“'”) character.
\"     A double quote (“"”) character.
\b     A backspace character.
\n     A newline (linefeed) character.
\r     A carriage return character.
\t     A tab character.
\Z     ASCII 26 (Control-Z). See note following the table.
\\     A backslash (“\”) character.
\%     A “%” character. See note following the table.
\_     A “_” character. See note following the table.

*/

var (
	gMySQLOld = [][]byte{
		[]byte("\x00"),
		[]byte("'"),
		[]byte("\""),
		[]byte("\b"),
		[]byte("\n"),
		[]byte("\r"),
		[]byte("\t"),
		//[]byte("\x1a"),
		// []byte("\\"),
	}

	gMySQLNew = [][]byte{
		[]byte("\\0"),
		[]byte("\\'"),
		[]byte("\\\""),
		[]byte("\\b"),
		[]byte("\\n"),
		[]byte("\\r"),
		[]byte("\\t"),
		//[]byte("\\Z"),
		//[]byte("\\\\"),
	}
)

func MySQLEscape(src []byte) []byte {

	for i := 0; i < len(gMySQLOld); i++ {
		src = ReplaceBytes(src, gMySQLOld[i], gMySQLNew[i])
	}
	return src
}
