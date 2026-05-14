package avro

import (
	"fmt"
	"math"
	"unicode/utf8"
	"unsafe"
)

// jsonScanner is a minimal zero-allocation JSON byte scanner.
// It operates on a []byte input and exposes a cursor-style API
// for the schema-guided decoder to extract tokens.
type jsonScanner struct {
	data []byte
	pos  int
}

func (s *jsonScanner) skipWhitespace() {
	for s.pos < len(s.data) {
		switch s.data[s.pos] {
		case ' ', '\t', '\n', '\r':
			s.pos++
		default:
			return
		}
	}
}

// peek returns the next non-whitespace byte, or 0 if at EOF.
func (s *jsonScanner) peek() byte {
	s.skipWhitespace()
	if s.pos >= len(s.data) {
		return 0
	}
	return s.data[s.pos]
}

// peekAt returns the byte `n` positions past the current
// non-whitespace cursor, without consuming. Used to disambiguate
// negative-number vs bare "-Infinity" at the start of a token.
func (s *jsonScanner) peekAt(n int) byte {
	s.skipWhitespace()
	if s.pos+n >= len(s.data) {
		return 0
	}
	return s.data[s.pos+n]
}

func (s *jsonScanner) expect(b byte) error {
	s.skipWhitespace()
	if s.pos >= len(s.data) {
		return fmt.Errorf("avro json: unexpected EOF, expected %q", b)
	}
	if s.data[s.pos] != b {
		return fmt.Errorf("avro json: expected %q, got %q at offset %d", b, s.data[s.pos], s.pos)
	}
	s.pos++
	return nil
}

func (s *jsonScanner) consumeNull() error {
	s.skipWhitespace()
	if s.pos+4 > len(s.data) || string(s.data[s.pos:s.pos+4]) != "null" {
		return fmt.Errorf("avro json: expected null at offset %d", s.pos)
	}
	s.pos += 4
	return nil
}

// consumeBareSpecialFloat consumes a bare JSON token shaped like an
// optional leading '-' followed by an alphabetic run. The token's
// content is returned verbatim for parseSpecialFloat to validate, so
// casing leniency (NaN/nan/NAN, Infinity/inf/Inf, etc.) is identical
// between this bare path and the quoted-string path in decodeFloat/
// decodeDouble — preventing a quoted-vs-bare casing-parity gap.
func (s *jsonScanner) consumeBareSpecialFloat() (string, error) {
	s.skipWhitespace()
	if s.pos >= len(s.data) {
		return "", fmt.Errorf("avro json: expected bare NaN/Infinity at offset %d", s.pos)
	}
	start := s.pos
	if s.data[s.pos] == '-' {
		s.pos++
	}
	tokenStart := s.pos
	for s.pos < len(s.data) {
		c := s.data[s.pos]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') {
			s.pos++
			continue
		}
		break
	}
	if s.pos == tokenStart {
		s.pos = start
		return "", fmt.Errorf("avro json: expected bare NaN/Infinity at offset %d", start)
	}
	return string(s.data[start:s.pos]), nil
}

func (s *jsonScanner) consumeBool() (bool, error) {
	s.skipWhitespace()
	if s.pos+4 <= len(s.data) && string(s.data[s.pos:s.pos+4]) == "true" {
		s.pos += 4
		return true, nil
	}
	if s.pos+5 <= len(s.data) && string(s.data[s.pos:s.pos+5]) == "false" {
		s.pos += 5
		return false, nil
	}
	return false, fmt.Errorf("avro json: expected boolean at offset %d", s.pos)
}

// consumeStringRaw consumes a JSON string and returns the raw content
// between the quotes (with escape sequences unresolved). Returns start
// and end offsets into s.data. For strings without escapes (the common
// case), the caller can use s.data[start:end] directly.
func (s *jsonScanner) consumeStringRaw() (start, end int, hasEscapes bool, err error) {
	s.skipWhitespace()
	if s.pos >= len(s.data) || s.data[s.pos] != '"' {
		return 0, 0, false, fmt.Errorf("avro json: expected string at offset %d", s.pos)
	}
	s.pos++ // skip opening quote
	start = s.pos
	for s.pos < len(s.data) {
		b := s.data[s.pos]
		if b == '\\' {
			hasEscapes = true
			s.pos += 2 // skip escape + next char
			if s.pos > len(s.data) {
				return 0, 0, false, fmt.Errorf("avro json: unterminated escape at offset %d", s.pos-2)
			}
			continue
		}
		if b == '"' {
			end = s.pos
			s.pos++ // skip closing quote
			// Postel: don't validate UTF-8 here. Some producers emit
			// raw bytes 0x80-0xff (technically invalid JSON but seen
			// in real pipelines). The encoder canonicalizes invalid
			// bytes on output (replacement char), so a round-trip
			// from non-canonical input lands on canonical output.
			return start, end, hasEscapes, nil
		}
		s.pos++
	}
	return 0, 0, false, fmt.Errorf("avro json: unterminated string at offset %d", start-1)
}

// consumeString consumes a JSON string and returns the resolved Go string.
func (s *jsonScanner) consumeString() (string, error) {
	start, end, hasEscapes, err := s.consumeStringRaw()
	if err != nil {
		return "", err
	}
	if !hasEscapes {
		return string(s.data[start:end]), nil
	}
	return resolveJSONEscapes(s.data[start:end])
}

// consumeStringZeroCopy consumes a JSON string and returns a zero-copy
// string backed by the scanner's input data. The returned string is only
// valid during the current DecodeJSON call — do NOT store it in output.
// Falls back to a copying path for strings with escape sequences.
func (s *jsonScanner) consumeStringZeroCopy() (string, error) {
	start, end, hasEscapes, err := s.consumeStringRaw()
	if err != nil {
		return "", err
	}
	if !hasEscapes {
		return unsafe.String(unsafe.SliceData(s.data[start:end]), end-start), nil
	}
	return resolveJSONEscapes(s.data[start:end])
}

// consumeNumberBytes consumes a JSON number and returns the raw bytes.
func (s *jsonScanner) consumeNumberBytes() ([]byte, error) {
	s.skipWhitespace()
	start := s.pos
	if s.pos < len(s.data) && s.data[s.pos] == '-' {
		s.pos++
	}
	if s.pos >= len(s.data) || s.data[s.pos] < '0' || s.data[s.pos] > '9' {
		return nil, fmt.Errorf("avro json: expected number at offset %d", start)
	}
	// Per RFC 8259: leading zeros are not permitted (except "0" itself
	// or "0.xxx"). If the first digit is '0', the next must be '.' or
	// end-of-number.
	if s.data[s.pos] == '0' && s.pos+1 < len(s.data) {
		next := s.data[s.pos+1]
		if next >= '0' && next <= '9' {
			return nil, fmt.Errorf("avro json: leading zeros not permitted at offset %d", start)
		}
	}
	for s.pos < len(s.data) {
		b := s.data[s.pos]
		if (b >= '0' && b <= '9') || b == '.' || b == 'e' || b == 'E' || b == '+' || b == '-' {
			s.pos++
			continue
		}
		break
	}
	return s.data[start:s.pos], nil
}

// skipValue skips an entire JSON value (for unknown record fields).
func (s *jsonScanner) skipValue() error {
	s.skipWhitespace()
	if s.pos >= len(s.data) {
		return fmt.Errorf("avro json: unexpected EOF")
	}
	switch s.data[s.pos] {
	case '"':
		_, _, _, err := s.consumeStringRaw()
		return err
	case 't':
		_, err := s.consumeBool()
		return err
	case 'f':
		_, err := s.consumeBool()
		return err
	case 'n':
		return s.consumeNull()
	case '[':
		return s.skipCompound('[', ']')
	case '{':
		return s.skipCompound('{', '}')
	default:
		_, err := s.consumeNumberBytes()
		return err
	}
}

func (s *jsonScanner) skipCompound(open, close byte) error {
	s.pos++ // consume open
	depth := 1
	for s.pos < len(s.data) && depth > 0 {
		switch s.data[s.pos] {
		case open:
			depth++
		case close:
			depth--
		case '"':
			s.pos++
			for s.pos < len(s.data) {
				if s.data[s.pos] == '\\' {
					s.pos += 2
					continue
				}
				if s.data[s.pos] == '"' {
					break
				}
				s.pos++
			}
		}
		s.pos++
	}
	if depth != 0 {
		return fmt.Errorf("avro json: unterminated %c at offset %d", open, s.pos)
	}
	return nil
}

// parseJSONInt32 parses raw JSON number bytes directly as int32.
// Rejects fractional/exponent notation and out-of-range values.
func parseJSONInt32(b []byte) (int32, error) {
	n, err := parseJSONInt64(b)
	if err != nil {
		return 0, err
	}
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0, fmt.Errorf("avro json: value %d overflows int32", n)
	}
	return int32(n), nil
}

// parseJSONInt64 parses raw JSON number bytes directly as int64.
// Rejects fractional/exponent notation.
func parseJSONInt64(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, fmt.Errorf("avro json: empty number")
	}
	neg := false
	i := 0
	if b[0] == '-' {
		neg = true
		i = 1
	}
	if i >= len(b) {
		return 0, fmt.Errorf("avro json: invalid number %q", b)
	}
	// Per-digit pre-multiply guard. The naive "n*10+d wrapped if it
	// went down" check has a gap once n ≈ 2^64/9: n*10+d can wrap
	// mod 2^64 to a value still ≥ prev (e.g. parsing
	// "20496382304121724020" lands at 2049638230412172404 with no
	// post-multiply wrap visible). Java's JsonParser.getLongValue
	// throws InputCoercionException; goavro uses strconv.ParseInt;
	// twmb/avro now does an equivalent pre-multiply bound.
	//
	// MaxInt64 = 9223372036854775807 → cutoff 922337203685477580, last digit 7.
	// |MinInt64| = 9223372036854775808 → same cutoff, last digit 8.
	const cutoff = uint64(math.MaxInt64) / 10
	maxDigit := uint64(7)
	if neg {
		maxDigit = 8
	}
	var n uint64
	for ; i < len(b); i++ {
		c := b[i]
		if c == '.' || c == 'e' || c == 'E' {
			// Has fractional/exponent part — parse with arbitrary precision
			// so values near the int64 boundary aren't silently truncated
			// or rejected via float64 rounding (e.g. "-9.2233720368547758e18"
			// = -9223372036854775800 is a valid int64 that float64 would
			// round to int64.Min; "9.2233720368547758e18" = 9223372036854775800
			// would float64-round to int64.Max+1 and be rejected). See
			// parseInt64Lenient for the full rationale.
			n, err := parseInt64Lenient(string(b))
			if err != nil {
				return 0, fmt.Errorf("avro json: %w", err)
			}
			return n, nil
		}
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("avro json: invalid number %q", b)
		}
		d := uint64(c - '0')
		if n > cutoff || (n == cutoff && d > maxDigit) {
			return 0, fmt.Errorf("avro json: value %q overflows int64", b)
		}
		n = n*10 + d
	}
	if neg {
		// -int64(1<<63) wraps to MinInt64 in two's complement, which
		// is the correct value for the input "-9223372036854775808".
		return -int64(n), nil
	}
	return int64(n), nil
}

// walkJSONEscapes iterates raw JSON string content (between quotes),
// decoding escape sequences and UTF-8 multi-byte sequences into runes,
// then calling emit for each code point. Used by both resolveJSONEscapes
// (Avro string) and scanAvroJSONBytes (Avro bytes/fixed).
//
// The UTF-8 decoding is required for spec parity with Java/fastavro.
// Per the Avro 1.12 JSON spec, "each character represents one byte"
// and "Unicode code points 0-255 are mapped to unsigned 8-bit byte
// values 0-255". Both Java and fastavro decode the JSON string into
// Unicode characters first (Jackson getText() / Python str), then map
// code points to bytes. A byte-by-byte walker would (wrongly) emit
// JSON literal "é" (UTF-8 bytes c3 a9) as two output bytes [0xC3, 0xA9]
// rather than the spec-correct one byte [0xE9].
func walkJSONEscapes(raw []byte, emit func(r rune) error) error {
	for i := 0; i < len(raw); {
		if raw[i] != '\\' {
			// Decode multi-byte UTF-8 as a single rune so the round
			// trip preserves the value. For invalid UTF-8 (e.g. lone
			// 0x9e), DecodeRune returns RuneError with size 1 — emit
			// the raw byte as its codepoint (Postel; the encoder
			// canonicalizes on output).
			r, size := utf8.DecodeRune(raw[i:])
			if r == utf8.RuneError && size == 1 {
				r = rune(raw[i])
			}
			if err := emit(r); err != nil {
				return err
			}
			i += size
			continue
		}
		i++
		if i >= len(raw) {
			return fmt.Errorf("avro json: unterminated escape")
		}
		var r rune
		switch raw[i] {
		case '"', '\\', '/':
			r = rune(raw[i])
		case 'b':
			r = '\b'
		case 'f':
			r = '\f'
		case 'n':
			r = '\n'
		case 'r':
			r = '\r'
		case 't':
			r = '\t'
		case 'u':
			if i+4 >= len(raw) {
				return fmt.Errorf("avro json: short \\u escape")
			}
			var err error
			r, err = parseHex4(raw[i+1 : i+5])
			if err != nil {
				return err
			}
			i += 4
			// Handle UTF-16 surrogate pairs.
			if r >= 0xD800 && r <= 0xDBFF && i+2 < len(raw) && raw[i+1] == '\\' && raw[i+2] == 'u' {
				if i+6 < len(raw) {
					r2, err := parseHex4(raw[i+3 : i+7])
					if err == nil && r2 >= 0xDC00 && r2 <= 0xDFFF {
						r = 0x10000 + (r-0xD800)*0x400 + (r2 - 0xDC00)
						i += 6
					}
				}
			}
		default:
			r = rune(raw[i])
		}
		if err := emit(r); err != nil {
			return err
		}
		i++
	}
	return nil
}

// resolveJSONEscapes resolves JSON escape sequences in raw string content,
// producing a UTF-8 Go string.
func resolveJSONEscapes(raw []byte) (string, error) {
	var buf []byte
	err := walkJSONEscapes(raw, func(r rune) error {
		buf = utf8.AppendRune(buf, r)
		return nil
	})
	if err != nil {
		return "", err
	}
	return string(buf), nil
}


// scanAvroJSONBytes resolves a raw JSON string content into Avro bytes.
// In Avro's convention, each code point maps to a single byte (≤ 255).
func scanAvroJSONBytes(raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return []byte{}, nil
	}
	var buf []byte
	err := walkJSONEscapes(raw, func(r rune) error {
		if r > 255 {
			return fmt.Errorf("avro json: \\u%04X exceeds byte range in bytes field", r)
		}
		buf = append(buf, byte(r))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func parseHex4(b []byte) (rune, error) {
	if len(b) < 4 {
		return 0, fmt.Errorf("avro json: short hex escape")
	}
	var r rune
	for _, c := range b[:4] {
		r <<= 4
		switch {
		case c >= '0' && c <= '9':
			r |= rune(c - '0')
		case c >= 'a' && c <= 'f':
			r |= rune(c - 'a' + 10)
		case c >= 'A' && c <= 'F':
			r |= rune(c - 'A' + 10)
		default:
			return 0, fmt.Errorf("avro json: invalid hex digit %q", c)
		}
	}
	return r, nil
}
