package avro

import (
	"bytes"
	"encoding/json"
	"strconv"
	"unicode/utf8"
)

// appendCompactJSON appends raw JSON with insignificant whitespace removed,
// matching json.Marshal(json.RawMessage). Only the non-PCF "default"
// attribute reaches it, and the canon tree strips that; we keep it so the
// writer stays faithful on an unstripped tree.
func appendCompactJSON(dst, raw []byte) []byte {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return append(dst, raw...)
	}
	return append(dst, buf.Bytes()...)
}

// canonicalBytes serializes the first-occurrence-rewritten, stripped schema
// tree to its Parsing Canonical Form bytes in one O(n) pass. Strings go out
// as raw UTF-8 per the PCF [STRINGS] rule: only the mandatory JSON escapes
// (quote, backslash, controls) are escaped, and <, >, &, U+2028 and U+2029 go
// out verbatim, matching Java's SchemaNormalization.
func canonicalBytes(root aschema) []byte {
	dst := make([]byte, 0, 256)
	return appendCanonSchema(dst, &root)
}

func appendCanonSchema(dst []byte, s *aschema) []byte {
	switch {
	case s.primitive != "":
		return appendCanonString(dst, s.primitive)
	case s.object != nil:
		return appendCanonObject(dst, s.object)
	case s.union != nil:
		// Non-nil discriminates union-ness so a zero-branch union emits
		// `[]` rather than falling into the default arm.
		dst = append(dst, '[')
		for i := range s.union {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendCanonSchema(dst, &s.union[i])
		}
		return append(dst, ']')
	default:
		// An empty aschema cannot occur in a built canon tree; emit an
		// empty string so callers never see truncated JSON.
		return append(dst, '"', '"')
	}
}

// appendCanonObject writes an aobject in PCF key order, with the
// required-empty-array rules (record/error always emit "fields", enum always
// emits "symbols"). It also emits the non-PCF attributes (namespace, aliases,
// default, order, logicalType, precision, scale) when present, so it doubles
// as a general-purpose object writer for an unstripped tree. Canonical() feeds
// an already-stripped tree (canonicalFirstOccurrence), so there only name,
// type, and the required arrays appear and the attribute branches never run.
func appendCanonObject(dst []byte, o *aobject) []byte {
	dst = append(dst, '{')
	first := true
	key := func(dst []byte, k string) []byte {
		if !first {
			dst = append(dst, ',')
		}
		first = false
		dst = appendCanonString(dst, k)
		return append(dst, ':')
	}

	// A named kind always emits its name, including the empty fullname a
	// WithLaxNames fn can accept ("name":""), as fastavro's PCF does. The
	// Name != "" arm keeps emission for a hand-built object carrying a name
	// on a non-named kind.
	if o.Name != "" || isNamedKind(o.Type) {
		dst = key(dst, "name")
		dst = appendCanonString(dst, o.Name)
	}
	dst = key(dst, "type")
	dst = appendCanonString(dst, o.Type)

	// A kind that requires the key always emits it, even empty (a record
	// with no "fields" or an enum with no "symbols" is unparseable); any
	// other kind emits it only when it carries one. toJSONWalk states its
	// "fields" rule the same way.
	if isRecordKind(o.Type) || len(o.Fields) > 0 {
		dst = key(dst, "fields")
		dst = append(dst, '[')
		for i := range o.Fields {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendCanonField(dst, &o.Fields[i])
		}
		dst = append(dst, ']')
	}

	if o.Type == "enum" || len(o.Symbols) > 0 {
		dst = key(dst, "symbols")
		dst = appendCanonStringArray(dst, o.Symbols)
	}
	if o.Items != nil {
		dst = key(dst, "items")
		dst = appendCanonSchema(dst, o.Items)
	}
	if o.Values != nil {
		dst = key(dst, "values")
		dst = appendCanonSchema(dst, o.Values)
	}
	if o.Size != nil {
		dst = key(dst, "size")
		dst = strconv.AppendInt(dst, int64(*o.Size), 10)
	}

	// Non-PCF attributes (stripped from the canon tree, so normally
	// absent; emitted faithfully when present).
	if o.Namespace != nil {
		dst = key(dst, "namespace")
		dst = appendCanonString(dst, *o.Namespace)
	}
	if len(o.Aliases) > 0 {
		dst = key(dst, "aliases")
		dst = appendCanonStringArray(dst, o.Aliases)
	}
	if len(o.Default) > 0 {
		dst = key(dst, "default")
		dst = appendCompactJSON(dst, o.Default)
	}
	if o.Logical != "" {
		dst = key(dst, "logicalType")
		dst = appendCanonString(dst, o.Logical)
	}
	if o.Precision != nil {
		dst = key(dst, "precision")
		dst = strconv.AppendInt(dst, int64(*o.Precision), 10)
	}
	if o.Scale != nil {
		dst = key(dst, "scale")
		dst = strconv.AppendInt(dst, int64(*o.Scale), 10)
	}

	return append(dst, '}')
}

// appendCanonField writes an afield: name and type always, then the field
// attributes (aliases, default, order, logicalType, precision, scale) when
// present. Canonical() feeds a stripped tree, so only name and type appear
// there. The attribute branches are the general-purpose writer mode,
// symmetric with appendCanonObject.
func appendCanonField(dst []byte, f *afield) []byte {
	dst = append(dst, '{')
	// "name" leads, so every later key is comma-preceded: no first-key
	// bookkeeping, unlike appendCanonObject's key closure.
	key := func(dst []byte, k string) []byte {
		dst = append(dst, ',')
		dst = appendCanonString(dst, k)
		return append(dst, ':')
	}
	dst = appendCanonString(dst, "name")
	dst = append(dst, ':')
	dst = appendCanonString(dst, f.Name)
	dst = key(dst, "type")
	if f.Type != nil {
		dst = appendCanonSchema(dst, f.Type)
	} else {
		dst = append(dst, '"', '"')
	}
	if len(f.Aliases) > 0 {
		dst = appendCanonStringArray(key(dst, "aliases"), f.Aliases)
	}
	if len(f.Default) > 0 {
		dst = appendCompactJSON(key(dst, "default"), f.Default)
	}
	if f.Order != "" {
		dst = appendCanonString(key(dst, "order"), f.Order)
	}
	if f.Logical != "" {
		dst = appendCanonString(key(dst, "logicalType"), f.Logical)
	}
	if f.Precision != nil {
		dst = strconv.AppendInt(key(dst, "precision"), int64(*f.Precision), 10)
	}
	if f.Scale != nil {
		dst = strconv.AppendInt(key(dst, "scale"), int64(*f.Scale), 10)
	}
	return append(dst, '}')
}

func appendCanonStringArray(dst []byte, ss []string) []byte {
	dst = append(dst, '[')
	for i, s := range ss {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendCanonString(dst, s)
	}
	return append(dst, ']')
}

const hexDigits = "0123456789abcdef"

// appendCanonString writes s as a JSON string with only the mandatory escapes
// (PCF [STRINGS], i.e. raw UTF-8). We match encoding/json with
// SetEscapeHTML(false) and no U+2028/U+2029 escaping: <, >, &, the
// line/paragraph separators, and every other valid code point go out verbatim.
// Only ", \, and the control characters (< 0x20) are escaped, in the forms
// encoding/json uses. Invalid UTF-8 becomes U+FFFD, again matching
// encoding/json, so the canonical bytes and the fingerprint stay identical for
// every well-formed schema.
func appendCanonString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if b >= 0x20 && b != '"' && b != '\\' {
				i++
				continue
			}
			if start < i {
				dst = append(dst, s[start:i]...)
			}
			switch b {
			case '"':
				dst = append(dst, '\\', '"')
			case '\\':
				dst = append(dst, '\\', '\\')
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[b>>4], hexDigits[b&0xf])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			if start < i {
				dst = append(dst, s[start:i]...)
			}
			dst = append(dst, 0xef, 0xbf, 0xbd) // U+FFFD, as encoding/json emits
			i += size
			start = i
			continue
		}
		i += size
	}
	if start < len(s) {
		dst = append(dst, s[start:]...)
	}
	return append(dst, '"')
}
