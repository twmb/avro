package avro_test

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Combinatorial matrix: schema fragments × composition contexts × wire paths.
//
// The core invariant is calibration-free: whatever Go form the binary decoder
// produces (a1) is the canonical form, and every path must agree with it:
//
//	w1 = binEnc(v_in)   a1 = binDec(w1)
//	binEnc(a1)  == w1                      (binary re-encode stability)
//	j1 = jsonEnc(a1)    aj = jsonDec(j1)
//	aj ≈ a1                                (cross-wire value agreement)
//	binEnc(aj)  == w1                      (JSON round-trip lands on same wire)
//	jsonEnc(aj) == j1                      (JSON re-encode stability)
//
// plus, per composed schema:
//	s2 = s.Root().Schema():  fingerprint(s2)==fingerprint(s),
//	    s2-binEnc(a1)==w1, s2-jsonEnc(a1)==j1, s2-binDec(w1)≈a1
//	identity Resolve(s', s''): resolved binDec(w1) ≈ a1
// ---------------------------------------------------------------------------

// matEqual: Avro-semantic equality over decoded any-trees, extended for the
// logical-type canonical Go types and NaN.
func matEqual(a, b any) bool {
	switch av := a.(type) {
	case nil:
		return b == nil
	case []byte:
		bv, ok := b.([]byte)
		return ok && bytes.Equal(av, bv)
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !matEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			bvv, ok := bv[k]
			if !ok || !matEqual(v, bvv) {
				return false
			}
		}
		return true
	case float64:
		bv, ok := b.(float64)
		if !ok {
			return false
		}
		return av == bv || (math.IsNaN(av) && math.IsNaN(bv))
	case float32:
		bv, ok := b.(float32)
		if !ok {
			return false
		}
		return av == bv || (av != av && bv != bv)
	case time.Time:
		bv, ok := b.(time.Time)
		return ok && av.Equal(bv)
	case *big.Rat:
		bv, ok := b.(*big.Rat)
		return ok && av.Cmp(bv) == 0
	default:
		return reflect.DeepEqual(a, b)
	}
}

// uniq hands out unique name suffixes so one fragment can appear at several
// positions in one schema without named-type collisions.
type uniq struct{ n int }

func (u *uniq) name(base string) string {
	u.n++
	return fmt.Sprintf("%s_%d", base, u.n)
}

// tokenClass buckets Avro kinds by the JSON token that begins their bare
// (untagged) encoding, mirroring the documented bare-union dispatch. Unions
// built by contexts only mix class-distinct branches so the untagged JSON
// round-trip is information-preserving.
func tokenClass(kind string) string {
	switch kind {
	case "null":
		return "null"
	case "boolean":
		return "bool"
	case "int", "long", "float", "double",
		"date", "time-millis", "time-micros",
		"timestamp-millis", "timestamp-micros", "timestamp-nanos",
		"local-timestamp-millis", "local-timestamp-micros", "local-timestamp-nanos":
		return "digit"
	case "string", "bytes", "fixed", "enum", "uuid", "decimal", "duration", "big-decimal":
		return "string"
	case "array":
		return "array"
	case "record", "map":
		return "object"
	}
	return "other"
}

// frag is one schema fragment plus driving values (lenient input forms are
// fine; the harness calibrates the canonical form from the binary decoder).
type frag struct {
	label  string
	kind   string // token-class + null-guard dispatch
	schema func(u *uniq) string
	values []any
}

func prim(label, kind string, values ...any) frag {
	return frag{label: label, kind: kind, schema: func(*uniq) string { return fmt.Sprintf("%q", kind) }, values: values}
}

func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t
}

func matFrags() []frag {
	return []frag{
		prim("null", "null", nil),
		prim("boolean", "boolean", true, false),
		prim("int", "int", int32(0), int32(1), int32(-1), int32(math.MaxInt32), int32(math.MinInt32)),
		prim("long", "long", int64(0), int64(-1), int64(math.MaxInt64), int64(math.MinInt64), int64(1<<53+1)),
		prim("float", "float", float32(0), float32(math.Copysign(0, -1)), float32(1.5), float32(math.Inf(1)), float32(math.Inf(-1)), float32(math.NaN()), float32(math.SmallestNonzeroFloat32), float32(math.MaxFloat32)),
		prim("double", "double", float64(0), math.Copysign(0, -1), 1.5, math.Inf(1), math.Inf(-1), math.NaN(), math.SmallestNonzeroFloat64, math.MaxFloat64),
		prim("string", "string", "", "a", "héllo 日本 🎉", "with\nnewline\ttab", "\x00nul", "  ", strings.Repeat("x", 300)),
		prim("bytes", "bytes", []byte{}, []byte{0}, []byte{0xFF, 0x00, 0x7F}, bytes.Repeat([]byte{0xAB}, 64)),
		{label: "enum3", kind: "enum",
			schema: func(u *uniq) string {
				return fmt.Sprintf(`{"type":"enum","name":%q,"symbols":["A","B","C"]}`, u.name("E"))
			},
			values: []any{"A", "C"}},
		{label: "enum1", kind: "enum",
			schema: func(u *uniq) string {
				return fmt.Sprintf(`{"type":"enum","name":%q,"symbols":["Only"]}`, u.name("E1"))
			},
			values: []any{"Only"}},
		{label: "fixed0", kind: "fixed",
			schema: func(u *uniq) string {
				return fmt.Sprintf(`{"type":"fixed","name":%q,"size":0}`, u.name("F0"))
			},
			values: []any{[]byte{}}},
		{label: "fixed1", kind: "fixed",
			schema: func(u *uniq) string {
				return fmt.Sprintf(`{"type":"fixed","name":%q,"size":1}`, u.name("F1"))
			},
			values: []any{[]byte{0x00}, []byte{0xFF}}},
		{label: "fixed16", kind: "fixed",
			schema: func(u *uniq) string {
				return fmt.Sprintf(`{"type":"fixed","name":%q,"size":16}`, u.name("F16"))
			},
			values: []any{bytes.Repeat([]byte{0x5A}, 16)}},
		{label: "uuid-string", kind: "uuid",
			schema: func(*uniq) string { return `{"type":"string","logicalType":"uuid"}` },
			values: []any{"6ba7b810-9dad-11d1-80b4-00c04fd430c8", ""}},
		{label: "uuid-fixed", kind: "uuid",
			schema: func(u *uniq) string {
				return fmt.Sprintf(`{"type":"fixed","name":%q,"size":16,"logicalType":"uuid"}`, u.name("U"))
			},
			values: []any{"6ba7b810-9dad-11d1-80b4-00c04fd430c8"}},
		{label: "date", kind: "date",
			schema: func(*uniq) string { return `{"type":"int","logicalType":"date"}` },
			values: []any{mustTime("2024-01-01T00:00:00Z"), mustTime("1969-07-20T00:00:00Z"), mustTime("1970-01-01T00:00:00Z")}},
		{label: "time-millis", kind: "time-millis",
			schema: func(*uniq) string { return `{"type":"int","logicalType":"time-millis"}` },
			values: []any{time.Duration(0), 3*time.Hour + 7*time.Millisecond}},
		{label: "time-micros", kind: "time-micros",
			schema: func(*uniq) string { return `{"type":"long","logicalType":"time-micros"}` },
			values: []any{time.Duration(0), 23*time.Hour + 59*time.Minute + 5*time.Microsecond}},
		{label: "timestamp-millis", kind: "timestamp-millis",
			schema: func(*uniq) string { return `{"type":"long","logicalType":"timestamp-millis"}` },
			values: []any{mustTime("2024-06-01T12:34:56.789Z"), mustTime("1955-11-05T06:15:00Z")}},
		{label: "timestamp-micros", kind: "timestamp-micros",
			schema: func(*uniq) string { return `{"type":"long","logicalType":"timestamp-micros"}` },
			values: []any{mustTime("2024-06-01T12:34:56.789012Z")}},
		{label: "timestamp-nanos", kind: "timestamp-nanos",
			schema: func(*uniq) string { return `{"type":"long","logicalType":"timestamp-nanos"}` },
			values: []any{mustTime("2024-06-01T12:34:56.789012345Z")}},
		{label: "local-ts-millis", kind: "local-timestamp-millis",
			schema: func(*uniq) string { return `{"type":"long","logicalType":"local-timestamp-millis"}` },
			values: []any{mustTime("2024-06-01T12:34:56.789Z")}},
		{label: "decimal-bytes", kind: "decimal",
			schema: func(*uniq) string {
				return `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
			},
			values: []any{big.NewRat(12345, 100), big.NewRat(-1, 4), big.NewRat(0, 1)}},
		{label: "decimal-fixed", kind: "decimal",
			schema: func(u *uniq) string {
				return fmt.Sprintf(`{"type":"fixed","name":%q,"size":8,"logicalType":"decimal","precision":10,"scale":2}`, u.name("DF"))
			},
			values: []any{big.NewRat(99999, 100), big.NewRat(-12345, 100)}},
		{label: "duration", kind: "duration",
			schema: func(u *uniq) string {
				return fmt.Sprintf(`{"type":"fixed","name":%q,"size":12,"logicalType":"duration"}`, u.name("DUR"))
			},
			values: []any{avro.Duration{Months: 1, Days: 2, Milliseconds: 3}, avro.Duration{}}},
		{label: "big-decimal", kind: "big-decimal",
			schema: func(*uniq) string { return `{"type":"bytes","logicalType":"big-decimal"}` },
			values: []any{big.NewRat(314159, 100000), big.NewRat(-7, 8)}},
		{label: "rec2", kind: "record",
			schema: func(u *uniq) string {
				return fmt.Sprintf(`{"type":"record","name":%q,"fields":[{"name":"x","type":"int"},{"name":"y","type":"string"}]}`, u.name("Rec"))
			},
			values: []any{map[string]any{"x": int32(7), "y": "v"}}},
		{label: "rec0", kind: "record",
			schema: func(u *uniq) string {
				return fmt.Sprintf(`{"type":"record","name":%q,"fields":[]}`, u.name("Empty"))
			},
			values: []any{map[string]any{}}},
		{label: "arr-int", kind: "array",
			schema: func(*uniq) string { return `{"type":"array","items":"int"}` },
			values: []any{[]any{}, []any{int32(1), int32(2), int32(3)}}},
		{label: "map-str", kind: "map",
			schema: func(*uniq) string { return `{"type":"map","values":"string"}` },
			values: []any{map[string]any{}, map[string]any{"": "e"}, map[string]any{"k": "v"}}},
	}
}

// ctx composes a fragment into a larger schema. wrap/unwrapKind describe the
// value transformation; skip filters fragment kinds the context cannot hold.
type ctx struct {
	label  string
	skip   func(kind string) bool
	schema func(inner, kind string, u *uniq) string
	wrap   func(v any) any
}

func matCtxs() []ctx {
	// pad returns a union-padding branch whose bare-JSON token class is
	// distinct from the fragment's, so untagged round-trips stay
	// information-preserving (documented first-token-class-match dispatch).
	pad := func(kind string) string {
		switch tokenClass(kind) {
		case "digit":
			return `"string"`
		default:
			return `"long"`
		}
	}
	return []ctx{
		{label: "top",
			schema: func(in, _ string, _ *uniq) string { return in },
			wrap:   func(v any) any { return v }},
		{label: "field",
			schema: func(in, kind string, u *uniq) string {
				return fmt.Sprintf(`{"type":"record","name":%q,"fields":[{"name":"f","type":%s}]}`, u.name("W"), in)
			},
			wrap: func(v any) any { return map[string]any{"f": v} }},
		{label: "field-mid",
			schema: func(in, kind string, u *uniq) string {
				return fmt.Sprintf(`{"type":"record","name":%q,"fields":[{"name":"a","type":"int"},{"name":"f","type":%s},{"name":"z","type":"string"}]}`, u.name("W"), in)
			},
			wrap: func(v any) any { return map[string]any{"a": int32(1), "f": v, "z": "zz"} }},
		{label: "field-deep3",
			schema: func(in, kind string, u *uniq) string {
				inner := fmt.Sprintf(`{"type":"record","name":%q,"fields":[{"name":"f","type":%s}]}`, u.name("D3"), in)
				mid := fmt.Sprintf(`{"type":"record","name":%q,"fields":[{"name":"m","type":%s},{"name":"n","type":"long"}]}`, u.name("D2"), inner)
				return fmt.Sprintf(`{"type":"record","name":%q,"fields":[{"name":"o","type":%s}]}`, u.name("D1"), mid)
			},
			wrap: func(v any) any {
				return map[string]any{"o": map[string]any{"m": map[string]any{"f": v}, "n": int64(9)}}
			}},
		{label: "array",
			schema: func(in, kind string, u *uniq) string { return fmt.Sprintf(`{"type":"array","items":%s}`, in) },
			wrap:   func(v any) any { return []any{v, v} }},
		{label: "array-of-array",
			schema: func(in, kind string, u *uniq) string {
				return fmt.Sprintf(`{"type":"array","items":{"type":"array","items":%s}}`, in)
			},
			wrap: func(v any) any { return []any{[]any{v}, []any{}, []any{v, v}} }},
		{label: "map",
			schema: func(in, kind string, u *uniq) string { return fmt.Sprintf(`{"type":"map","values":%s}`, in) },
			wrap:   func(v any) any { return map[string]any{"k1": v} }},
		{label: "map-of-array",
			schema: func(in, kind string, u *uniq) string {
				return fmt.Sprintf(`{"type":"map","values":{"type":"array","items":%s}}`, in)
			},
			wrap: func(v any) any { return map[string]any{"k": []any{v}} }},
		{label: "nullfirst-union",
			skip: func(k string) bool { return k == "null" },
			schema: func(in, kind string, u *uniq) string {
				return fmt.Sprintf(`["null",%s]`, in)
			},
			wrap: func(v any) any { return v }},
		{label: "nullsecond-union",
			skip: func(k string) bool { return k == "null" },
			schema: func(in, kind string, u *uniq) string {
				return fmt.Sprintf(`[%s,"null"]`, in)
			},
			wrap: func(v any) any { return v }},
		{label: "multibranch-union",
			skip: func(k string) bool { return k == "null" || k == "boolean" },
			schema: func(in, kind string, u *uniq) string {
				return fmt.Sprintf(`["null","boolean",%s,%s]`, in, pad(kind))
			},
			wrap: func(v any) any { return v }},
		{label: "array-of-nullunion",
			skip: func(k string) bool { return k == "null" },
			schema: func(in, kind string, u *uniq) string {
				return fmt.Sprintf(`{"type":"array","items":["null",%s]}`, in)
			},
			wrap: func(v any) any { return []any{v, nil, v} }},
		{label: "map-of-nullunion",
			skip: func(k string) bool { return k == "null" },
			schema: func(in, kind string, u *uniq) string {
				return fmt.Sprintf(`{"type":"map","values":["null",%s]}`, in)
			},
			wrap: func(v any) any { return map[string]any{"a": v} }},
		{label: "field-nullunion",
			skip: func(k string) bool { return k == "null" },
			schema: func(in, kind string, u *uniq) string {
				return fmt.Sprintf(`{"type":"record","name":%q,"fields":[{"name":"u","type":["null",%s]}]}`, u.name("WU"), in)
			},
			wrap: func(v any) any { return map[string]any{"u": v} }},
	}
}

// pad fragments whose schema text embeds the fragment twice etc. are handled
// by uniq. The pad helper lives in matCtxs.

// runCore runs the calibration-free six-step core plus the rebuild and
// identity-resolve axes for one composed (schema, value).
func runCore(t *testing.T, schemaJSON string, vin any, opts ...avro.Opt) {
	t.Helper()
	s, err := avro.Parse(schemaJSON)
	if err != nil {
		t.Fatalf("Parse: %v\nschema: %s", err, schemaJSON)
	}
	w1, err := s.AppendEncode(nil, vin, opts...)
	if err != nil {
		t.Fatalf("binEnc(vin): %v\nschema: %s\nvin: %#v", err, schemaJSON, vin)
	}
	var a1 any
	rest, err := s.Decode(w1, &a1, opts...)
	if err != nil || len(rest) != 0 {
		t.Fatalf("binDec: err=%v rest=%d\nschema: %s", err, len(rest), schemaJSON)
	}
	w2, err := s.AppendEncode(nil, a1, opts...)
	if err != nil {
		t.Fatalf("binEnc(a1): %v\nschema: %s\na1: %#v", err, schemaJSON, a1)
	}
	if !bytes.Equal(w2, w1) {
		t.Fatalf("binary re-encode unstable:\n w1=%x\n w2=%x\nschema: %s\na1: %#v", w1, w2, schemaJSON, a1)
	}
	j1, err := s.AppendEncodeJSON(nil, a1, opts...)
	if err != nil {
		t.Fatalf("jsonEnc(a1): %v\nschema: %s\na1: %#v", err, schemaJSON, a1)
	}
	var aj any
	if err := s.DecodeJSON(j1, &aj, opts...); err != nil {
		t.Fatalf("jsonDec: %v\nschema: %s\nj1: %s", err, schemaJSON, j1)
	}
	if !matEqual(aj, a1) {
		t.Fatalf("cross-wire value disagreement:\n bin=%#v\njson=%#v\nschema: %s\nj1: %s", a1, aj, schemaJSON, j1)
	}
	wj, err := s.AppendEncode(nil, aj, opts...)
	if err != nil {
		t.Fatalf("binEnc(aj): %v\nschema: %s\naj: %#v", err, schemaJSON, aj)
	}
	if !bytes.Equal(wj, w1) {
		t.Fatalf("JSON round-trip lands on different wire:\n w1=%x\n wj=%x\nschema: %s", w1, wj, schemaJSON)
	}
	j2, err := s.AppendEncodeJSON(nil, aj, opts...)
	if err != nil {
		t.Fatalf("jsonEnc(aj): %v", err)
	}
	if !bytes.Equal(j2, j1) {
		t.Fatalf("JSON re-encode unstable:\n j1=%s\n j2=%s\nschema: %s", j1, j2, schemaJSON)
	}

	// Metadata rebuild: Root().Schema() must fingerprint-match and produce
	// byte-identical wires in both formats.
	root := s.Root()
	s2, err := root.Schema()
	if err != nil {
		t.Fatalf("Root().Schema(): %v\nschema: %s", err, schemaJSON)
	}
	if !bytes.Equal(s.Fingerprint(avro.NewRabin()), s2.Fingerprint(avro.NewRabin())) {
		t.Fatalf("rebuild fingerprint mismatch:\norig:    %s\nrebuilt: %s", schemaJSON, s2.String())
	}
	w3, err := s2.AppendEncode(nil, a1, opts...)
	if err != nil || !bytes.Equal(w3, w1) {
		t.Fatalf("rebuilt-schema binary differs: err=%v\n w1=%x\n w3=%x\nschema: %s", err, w1, w3, schemaJSON)
	}
	j3, err := s2.AppendEncodeJSON(nil, a1, opts...)
	if err != nil || !bytes.Equal(j3, j1) {
		t.Fatalf("rebuilt-schema JSON differs: err=%v\n j1=%s\n j3=%s", err, j1, j3)
	}
	var a3 any
	if _, err := s2.Decode(w1, &a3, opts...); err != nil || !matEqual(a3, a1) {
		t.Fatalf("rebuilt-schema decode differs: err=%v\n a1=%#v\n a3=%#v", err, a1, a3)
	}

	// Parse(String()) idempotence: the stored schema text re-parses to a
	// schema with the identical fingerprint and wire output.
	sStr, err := avro.Parse(s.String())
	if err != nil {
		t.Fatalf("Parse(String()): %v\nschema: %s", err, s.String())
	}
	if !bytes.Equal(sStr.Fingerprint(avro.NewRabin()), s.Fingerprint(avro.NewRabin())) {
		t.Fatalf("Parse(String()) fingerprint differs\nschema: %s", schemaJSON)
	}
	wStr, err := sStr.AppendEncode(nil, a1, opts...)
	if err != nil || !bytes.Equal(wStr, w1) {
		t.Fatalf("Parse(String()) wire differs: err=%v", err)
	}

	// Append semantics: encoding onto a non-empty dst appends exactly the
	// same bytes after the prefix, leaving the prefix untouched.
	prefix := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	wPre, err := s.AppendEncode(append([]byte{}, prefix...), a1, opts...)
	if err != nil || !bytes.HasPrefix(wPre, prefix) || !bytes.Equal(wPre[len(prefix):], w1) {
		t.Fatalf("append-to-prefix violated: err=%v\n got=%x\nwant=%x%x", err, wPre, prefix, w1)
	}

	// Concatenated stream: three values back-to-back decode sequentially,
	// each consuming exactly its own bytes.
	stream := append(append(append([]byte{}, w1...), w1...), w1...)
	rem := stream
	for k := 0; k < 3; k++ {
		var av any
		rem, err = s.Decode(rem, &av, opts...)
		if err != nil || !matEqual(av, a1) {
			t.Fatalf("stream decode #%d: err=%v", k, err)
		}
	}
	if len(rem) != 0 {
		t.Fatalf("stream left %d bytes", len(rem))
	}

	// Single Object Encoding: the framed message round-trips the same
	// canonical value through the same schema.
	soe, err := s.AppendSingleObject(nil, a1, opts...)
	if err != nil {
		t.Fatalf("AppendSingleObject: %v\nschema: %s", err, schemaJSON)
	}
	var aSoe any
	soeRest, err := s.DecodeSingleObject(soe, &aSoe, opts...)
	if err != nil || len(soeRest) != 0 || !matEqual(aSoe, a1) {
		t.Fatalf("SOE round-trip differs: err=%v rest=%d\n a1=%#v\nsoe=%#v\nschema: %s", err, len(soeRest), a1, aSoe, schemaJSON)
	}

	// Identity resolution through separately parsed writer/reader.
	sw := avro.MustParse(schemaJSON)
	sr := avro.MustParse(schemaJSON)
	res, err := avro.Resolve(sw, sr)
	if err != nil {
		t.Fatalf("identity Resolve: %v\nschema: %s", err, schemaJSON)
	}
	var ar any
	if _, err := res.Decode(w1, &ar, opts...); err != nil || !matEqual(ar, a1) {
		t.Fatalf("identity-resolved decode differs: err=%v\n a1=%#v\n ar=%#v\nschema: %s", err, a1, ar, schemaJSON)
	}
}

func TestMatrix_FragmentsByContext(t *testing.T) {
	frags := matFrags()
	ctxs := matCtxs()
	for _, fr := range frags {
		for _, cx := range ctxs {
			if cx.skip != nil && cx.skip(fr.kind) {
				continue
			}
			t.Run(fr.label+"/"+cx.label, func(t *testing.T) {
				for vi, v := range fr.values {
					u := &uniq{}
					schema := cx.schema(fr.schema(u), fr.kind, u)
					vin := cx.wrap(v)
					t.Run(fmt.Sprintf("v%d", vi), func(t *testing.T) {
						runCore(t, schema, vin)
					})
				}
			})
		}
	}
}

// Two-level composition: a representative outer set around every (fragment ×
// inner context) pair, exploding the nesting combinations.
func TestMatrix_TwoLevelComposition(t *testing.T) {
	frags := matFrags()
	ctxs := matCtxs()
	outers := map[string]bool{
		"field": true, "array": true, "map": true,
		"nullfirst-union": true, "field-nullunion": true, "array-of-nullunion": true,
	}
	unionFamily := map[string]bool{
		"nullfirst-union": true, "nullsecond-union": true, "multibranch-union": true,
		"field-nullunion": false, "array-of-nullunion": false, "map-of-nullunion": false,
	}
	for _, fr := range frags {
		for _, inner := range ctxs {
			if inner.skip != nil && inner.skip(fr.kind) {
				continue
			}
			innerKind := innerCtxKind(inner.label)
			if innerKind == "top" {
				innerKind = fr.kind
			}
			for _, outer := range ctxs {
				if !outers[outer.label] {
					continue
				}
				if outer.skip != nil && outer.skip(innerKind) {
					continue
				}
				// A union-producing inner cannot sit directly inside a
				// union-wrapping outer (unions may not contain unions).
				if innerKind == "union" && (unionFamily[outer.label] ||
					outer.label == "field-nullunion" || outer.label == "array-of-nullunion") {
					continue
				}
				t.Run(fr.label+"/"+inner.label+"/"+outer.label, func(t *testing.T) {
					v := fr.values[0]
					u := &uniq{}
					schema := outer.schema(inner.schema(fr.schema(u), fr.kind, u), innerKind, u)
					vin := outer.wrap(inner.wrap(v))
					runCore(t, schema, vin)
				})
			}
		}
	}
}

// innerCtxKind reports the Avro kind of a context's OUTPUT schema, for the
// outer context's skip filter (unions cannot directly nest unions).
func innerCtxKind(label string) string {
	switch label {
	case "top":
		return "top" // outer skip sees the fragment's own kind via union guard below
	case "field", "field-mid", "field-deep3", "field-nullunion":
		return "record"
	case "array", "array-of-array", "array-of-nullunion":
		return "array"
	case "map", "map-of-array", "map-of-nullunion":
		return "map"
	default: // the union contexts
		return "union"
	}
}
