package avro

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// maxJSONValueNesting bounds how deeply decodeSchemaAny descends. It is the
// depth the stdlib decoder we replaced accepted, kept exactly so a schema at
// the boundary is decided the way it always was: 10000 nested values parse,
// 10001 do not.
//
// Distinct from maxSchemaJSONDepth, which bounds the bracket nesting of schema
// text in a pre-scan before any decode. That one rejects pathologically deep
// input up front and is far tighter. This one is the decoder's own stack
// guard, and it also covers the two callers that decode text the pre-scan
// never saw (a preserved default, a struct tag).
const maxJSONValueNesting = 10000

// decodeSchemaAny decodes one JSON value out of schema text into the generic
// tree every schema path is written against: map[string]any for objects,
// []any for arrays, string, bool, nil, and json.Number for every number. We
// return how many bytes the value consumed, so each caller decides what
// trailing content means. This is the one decoder for schema JSON;
// unmarshalAnyPreservePrecision layers normalizeJSONValue over it for the
// metadata paths.
//
// Numbers come back as their literal, never resolved to int64 or float64.
// The parse path re-marshals a decoded default into o.Default / f.Default for
// the wire encoder to parse later, and resolving first breaks that in two
// silent ways: an integer-syntax "-0" loses its sign and re-marshals as "0",
// so a float default written as -0.0 encodes +0.0; and a resolved number
// re-marshals to its shortest form, so a 1025-byte float literal (one past
// the length cap every other arm enforces) becomes "1000" and slips past the
// cap. Keeping the literal makes the sign and the cap properties of the
// input.
//
// A key or string value carrying no escapes comes back as a substring of
// schema rather than a copy. Nothing can be written through a string, so
// two [Schema.Root] trees stay independent; what changes is lifetime. A
// substring pins the whole schema text, which costs nothing inside a
// [Schema] (the text is retained for [Schema.String] anyway) and one copy of
// the text per schema wherever a piece outlives its schema.
func decodeSchemaAny(schema string) (any, int, error) {
	d := &schemaDecoder{in: schema}
	d.space()
	if d.at >= len(d.in) {
		// Text holding no value at all reports io.EOF; text that runs out
		// part way through one reports io.ErrUnexpectedEOF (see
		// schemaDecoder.eof). parse echoes the decode error rather than
		// replacing it, so errors.Is on either keeps working.
		return nil, 0, io.EOF
	}
	v, err := d.value(0)
	if err != nil {
		return nil, 0, err
	}
	return v, d.at, nil
}

// decodeSchemaAnyStrict is decodeSchemaAny plus the rule that the value must
// be the whole input: we consume trailing whitespace, and anything else
// rejects, since a schema string with a second value after it is not a
// schema.
func decodeSchemaAnyStrict(schema string) (any, error) {
	v, n, err := decodeSchemaAny(schema)
	if err != nil {
		return nil, err
	}
	for ; n < len(schema); n++ {
		switch schema[n] {
		case ' ', '\t', '\n', '\r':
		default:
			return nil, errors.New("invalid schema: unexpected trailing content")
		}
	}
	return v, nil
}

// schemaDecoder walks schema text with an index cursor. It never writes to in,
// so every string it hands back may share in's backing array.
type schemaDecoder struct {
	in  string
	at  int
	buf []byte // reused scratch for strings carrying escapes
}

func (d *schemaDecoder) errorf(format string, args ...any) error {
	return fmt.Errorf("invalid schema json: "+format, args...)
}

// eof reports text that ran out part way through a value, a truncated schema
// rather than an absent one, with io.ErrUnexpectedEOF against decodeSchemaAny's
// io.EOF.
func (d *schemaDecoder) eof(what string) error {
	return fmt.Errorf("invalid schema json: %s at offset %d: %w", what, d.at, io.ErrUnexpectedEOF)
}

func (d *schemaDecoder) space() {
	for d.at < len(d.in) {
		switch d.in[d.at] {
		case ' ', '\t', '\n', '\r':
			d.at++
		default:
			return
		}
	}
}

func (d *schemaDecoder) literal(tok string, v any) (any, error) {
	if d.at+len(tok) <= len(d.in) && d.in[d.at:d.at+len(tok)] == tok {
		d.at += len(tok)
		return v, nil
	}
	if strings.HasPrefix(tok, d.in[d.at:]) {
		return nil, d.eof("truncated literal")
	}
	return nil, d.errorf("invalid literal at offset %d", d.at)
}

// value decodes one value at the cursor, which space() has already advanced to
// a non-space byte. depth counts enclosing containers. We charge the nesting
// bound on the two container arms, not here: a scalar is a leaf, and charging
// it would spend the last unit of the budget on the innermost leaf, refusing
// 10000 nested arrays holding a number while accepting the same 10000 holding
// nothing.
func (d *schemaDecoder) value(depth int) (any, error) {
	if d.at >= len(d.in) {
		return nil, d.eof("unexpected end of input")
	}
	switch d.in[d.at] {
	case '{':
		if depth >= maxJSONValueNesting {
			return nil, d.errorf("exceeded max depth at offset %d", d.at)
		}
		return d.object(depth)
	case '[':
		if depth >= maxJSONValueNesting {
			return nil, d.errorf("exceeded max depth at offset %d", d.at)
		}
		return d.array(depth)
	case '"':
		return d.str()
	case 't':
		return d.literal("true", true)
	case 'f':
		return d.literal("false", false)
	case 'n':
		return d.literal("null", nil)
	default:
		return d.number()
	}
}

func (d *schemaDecoder) object(depth int) (any, error) {
	d.at++ // '{'
	m := make(map[string]any)
	d.space()
	if d.at < len(d.in) && d.in[d.at] == '}' {
		d.at++
		return m, nil
	}
	for {
		d.space()
		if d.at >= len(d.in) {
			return nil, d.eof("unexpected end of object")
		}
		if d.in[d.at] != '"' {
			return nil, d.errorf("expected object key at offset %d", d.at)
		}
		k, err := d.str()
		if err != nil {
			return nil, err
		}
		d.space()
		if d.at >= len(d.in) {
			return nil, d.eof("unexpected end of object")
		}
		if d.in[d.at] != ':' {
			return nil, d.errorf("expected ':' at offset %d", d.at)
		}
		d.at++
		d.space()
		v, err := d.value(depth + 1)
		if err != nil {
			return nil, err
		}
		// Last duplicate key wins, which is what a decode into a map does
		// and what every reference implementation produces.
		m[k.(string)] = v
		d.space()
		if d.at >= len(d.in) {
			return nil, d.eof("unexpected end of object")
		}
		switch d.in[d.at] {
		case ',':
			d.at++
		case '}':
			d.at++
			return m, nil
		default:
			return nil, d.errorf("expected ',' or '}' at offset %d", d.at)
		}
	}
}

func (d *schemaDecoder) array(depth int) (any, error) {
	d.at++ // '['
	d.space()
	if d.at < len(d.in) && d.in[d.at] == ']' {
		d.at++
		// Empty and absent are different values to every caller that
		// range-checks a decoded array, so an empty array is a non-nil
		// empty slice.
		return []any{}, nil
	}
	var a []any
	for {
		d.space()
		v, err := d.value(depth + 1)
		if err != nil {
			return nil, err
		}
		a = append(a, v)
		d.space()
		if d.at >= len(d.in) {
			return nil, d.eof("unexpected end of array")
		}
		switch d.in[d.at] {
		case ',':
			d.at++
		case ']':
			d.at++
			return a, nil
		default:
			return nil, d.errorf("expected ',' or ']' at offset %d", d.at)
		}
	}
}

// str decodes a JSON string. We return any so the value dispatch has one
// return shape; object() asserts the string back for a key.
//
// The common case, no escapes and valid UTF-8, hands back a substring of the
// input, so a schema's keys and string values cost no allocation at all.
func (d *schemaDecoder) str() (any, error) {
	d.at++ // opening quote
	start := d.at
	highByte := false
	for d.at < len(d.in) {
		c := d.in[d.at]
		switch {
		case c == '"':
			s := d.in[start:d.at]
			d.at++
			if highByte && !utf8.ValidString(s) {
				s = replaceInvalidUTF8(s)
			}
			return s, nil
		case c == '\\':
			return d.strEscaped(start)
		case c < 0x20:
			// RFC 8259 section 7: the control characters must be escaped.
			return nil, d.errorf("unescaped control character %#x in string at offset %d", c, d.at)
		}
		if c >= utf8.RuneSelf {
			highByte = true
		}
		d.at++
	}
	return nil, d.eof("unterminated string")
}

// strEscaped finishes a string that carries at least one escape, copying into
// scratch. The cursor sits on the first backslash and start marks the opening
// quote's first content byte.
func (d *schemaDecoder) strEscaped(start int) (any, error) {
	buf := append(d.buf[:0], d.in[start:d.at]...)
	for d.at < len(d.in) {
		c := d.in[d.at]
		switch {
		case c == '"':
			d.at++
			d.buf = buf
			s := string(buf)
			if !utf8.ValidString(s) {
				s = replaceInvalidUTF8(s)
			}
			return s, nil
		case c == '\\':
			d.at++
			if d.at >= len(d.in) {
				return nil, d.eof("unterminated escape")
			}
			e := d.in[d.at]
			d.at++
			switch e {
			case '"', '\\', '/':
				buf = append(buf, e)
			case 'b':
				buf = append(buf, '\b')
			case 'f':
				buf = append(buf, '\f')
			case 'n':
				buf = append(buf, '\n')
			case 'r':
				buf = append(buf, '\r')
			case 't':
				buf = append(buf, '\t')
			case 'u':
				r, err := d.unicodeEscape()
				if err != nil {
					return nil, err
				}
				buf = utf8.AppendRune(buf, r)
			default:
				return nil, d.errorf("invalid escape %q at offset %d", e, d.at-1)
			}
		case c < 0x20:
			return nil, d.errorf("unescaped control character %#x in string at offset %d", c, d.at)
		default:
			buf = append(buf, c)
			d.at++
		}
	}
	return nil, d.eof("unterminated string")
}

// unicodeEscape reads the four hex digits after \u, pairing a leading
// surrogate with a trailing one when the next escape supplies it. UTF-8 cannot
// represent an unpaired surrogate, so one becomes the replacement rune, as a
// decode into a Go string does.
func (d *schemaDecoder) unicodeEscape() (rune, error) {
	// One digit at a time, so a non-hex character reports itself and only
	// running out of text reports truncation.
	var r rune
	for i := 0; i < 4; i++ {
		if d.at >= len(d.in) {
			return 0, d.eof("truncated \\u escape")
		}
		h, ok := hexDigit(d.in[d.at])
		if !ok {
			return 0, d.errorf("invalid character %q in \\u escape at offset %d", d.in[d.at], d.at)
		}
		r = r<<4 | rune(h)
		d.at++
	}
	if !utf16.IsSurrogate(r) {
		return r, nil
	}
	// A leading surrogate pairs with a trailing one when the next escape
	// supplies it. This is a lookahead, so anything else here is not an error;
	// it just leaves the surrogate unpaired.
	if d.at+6 <= len(d.in) && d.in[d.at] == '\\' && d.in[d.at+1] == 'u' {
		if lo, ok := hex4(d.in[d.at+2 : d.at+6]); ok {
			if paired := utf16.DecodeRune(r, lo); paired != utf8.RuneError {
				d.at += 6
				return paired, nil
			}
		}
	}
	// UTF-8 cannot represent a lone surrogate, so it becomes the
	// replacement rune, which is what decoding into a Go string produces.
	return utf8.RuneError, nil
}

func hexDigit(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

func hex4(s string) (rune, bool) {
	var r rune
	for i := 0; i < 4; i++ {
		h, ok := hexDigit(s[i])
		if !ok {
			return 0, false
		}
		r = r<<4 | rune(h)
	}
	return r, true
}

// number consumes a JSON number and returns its literal. See the type doc for
// why the literal, and not a resolved value, is what leaves this decoder.
func (d *schemaDecoder) number() (any, error) {
	start := d.at
	if d.at < len(d.in) && d.in[d.at] == '-' {
		d.at++
	}
	switch {
	case d.at >= len(d.in):
		return nil, d.eof("truncated number")
	case d.in[d.at] == '0':
		// RFC 8259: no leading zeros. A second digit here ends the number
		// after the '0', which is what a decode does too: the leftover digit
		// becomes trailing content for the caller to rule on.
		d.at++
	case d.in[d.at] >= '1' && d.in[d.at] <= '9':
		for d.at < len(d.in) && isDigit(d.in[d.at]) {
			d.at++
		}
	default:
		return nil, d.errorf("expected number at offset %d", start)
	}
	if d.at < len(d.in) && d.in[d.at] == '.' {
		d.at++
		if d.at >= len(d.in) {
			return nil, d.eof("truncated number")
		}
		if !isDigit(d.in[d.at]) {
			return nil, d.errorf("expected digit after decimal point at offset %d", d.at)
		}
		for d.at < len(d.in) && isDigit(d.in[d.at]) {
			d.at++
		}
	}
	if d.at < len(d.in) && (d.in[d.at] == 'e' || d.in[d.at] == 'E') {
		d.at++
		if d.at < len(d.in) && (d.in[d.at] == '+' || d.in[d.at] == '-') {
			d.at++
		}
		if d.at >= len(d.in) {
			return nil, d.eof("truncated number")
		}
		if !isDigit(d.in[d.at]) {
			return nil, d.errorf("expected digit in exponent at offset %d", d.at)
		}
		for d.at < len(d.in) && isDigit(d.in[d.at]) {
			d.at++
		}
	}
	return json.Number(d.in[start:d.at]), nil
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// replaceInvalidUTF8 substitutes the replacement rune for each byte that is
// not part of a valid encoding: one per byte, which is what decoding into a Go
// string produces, and not one per invalid run.
func replaceInvalidUTF8(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			b.WriteRune(utf8.RuneError)
			i++
			continue
		}
		b.WriteString(s[i : i+size])
		i += size
	}
	return b.String()
}
