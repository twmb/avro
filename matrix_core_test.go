package avro_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

// ---------- matrix_core_test.go ----------

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

// TestMatrix_FragmentsByContext (fragment × context × value through runCore) was
// RECONCILED INTO the axis-complete generator: TestMatrix_Generative in
// matrix_generative_test.go runs the identical runCore battery over a gtypes
// table that is a strict superset of matFrags (every primitive, every logical,
// container) crossed with these same matCtxs, and additionally drives the
// boundary-value axis, an independent wire oracle, and the metadata-API
// agreement that this loop omitted. matFrags/matCtxs/runCore stay here as the
// shared generative tables the broader matrix suite (and the generator) consume.

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

// ---------- matrix_generative_test.go ----------

// ===========================================================================
// THE axis-complete generative codec matrix.
//
// One generator, not hand-cases. The axes are tables; a new failing cell is a
// generator gap fixed by extending a table, never a hand-written round. This
// file RECONCILES (consumes + extends) the existing generative infrastructure
// rather than duplicating it:
//
//   - matFrags / matCtxs / runCore / matEqual / uniq  (matrix_core_test.go)
//   - typedFrags / typedPositions                     (matrix_typed_test.go)
//   - customFrags / customPositions                   (matrix_custom_test.go)
//   - recShapes                                       (matrix_recursion_test.go)
//   - resurrectionCells                               (custom_resurrection_parity_test.go)
//
// and adds the axes those omit: underlying-validity {valid, wrong-kind,
// wrong-size}, CustomType config {absent, passive, encode-only, decode-only,
// both}, and the boundary values current generators skip {2^53±1, MaxInt64/
// MinInt64, ±Inf, signaling-NaN, empty, large}.
//
// Per generated cell the matrix asserts THREE things:
//   (a) every codec path — binary-safe (generic), binary-unsafe (addressable
//       struct), JSON, resolved — plus the []T / map[string]T container
//       specializations agree byte-identically (within a wire format) and
//       value-identically (across formats), AND match an INDEPENDENT wire
//       oracle where one can be computed (the calibration-free runCore cannot
//       see encode-side canonicalization; the oracle can — e.g. a float32
//       encoder that quiets a signaling NaN);
//   (b) the wire round-trips through the schema's OWN reader (natural and
//       identity-resolved);
//   (c) parse-time / metadata-API observations (Root().Props, Fields[].Default,
//       Canonical, Fingerprint) agree with the wire and are deterministic.
// ===========================================================================

// gval is one boundary-classified value for a type: the generic (any-tree)
// form the reflect path consumes, plus an optional strongly-typed Go form for
// the unsafe/typed path, plus an optional independent wire oracle (the exact
// Avro binary bytes for the value at TOP context, computed without the code
// under test).
type gval struct {
	boundary string // normal | 2^53 | maxint | inf | snan | nzero | empty | large | ...
	generic  any    // the form the generic/reflect path encodes
	typed    any    // strongly-typed Go form (nil => same as generic)
	oracle   []byte // independent top-context wire bytes (nil => no oracle)
	// jsonLossy marks a value whose exact BINARY wire is provably not
	// representable in Avro JSON text: every NaN — quiet, signaling, any
	// payload — encodes to the single token "NaN" and decodes back to one
	// canonical quiet NaN (Java convention). The binary path stays bit-exact
	// (the oracle checks that); the JSON path round-trips only to a
	// value-equal NaN, never the same wire. This is a fact about the format,
	// not an assumption about inputs.
	jsonLossy bool
}

// gtype is one Avro type in its spec-VALID placement, enriched with the
// boundary-value axis the legacy frag tables omit.
type gtype struct {
	label  string
	kind   string // ctx.skip + token-class key (same vocabulary as matFrags kinds)
	schema func(u *uniq) string
	values []gval
}

// ---- independent wire-oracle builders (no code-under-test) -----------------

func jsonNum(s string) json.Number { return json.Number(s) }

func leF32(f float32) []byte {
	b := math.Float32bits(f)
	return []byte{byte(b), byte(b >> 8), byte(b >> 16), byte(b >> 24)}
}

func leF64(f float64) []byte {
	b := math.Float64bits(f)
	return []byte{byte(b), byte(b >> 8), byte(b >> 16), byte(b >> 24), byte(b >> 32), byte(b >> 40), byte(b >> 48), byte(b >> 56)}
}

// avroLen prefixes b with its zigzag-varlong length, as a bytes/string wire.
func avroLen(b []byte) []byte { return append(appendZig(nil, int64(len(b))), b...) }

// signaling NaNs: exponent all ones, top mantissa bit CLEAR, a low bit set.
// math.NaN() is quiet (top mantissa bit set); these are a distinct bit pattern
// a NaN-canonicalizing encoder would silently rewrite.
var (
	sNaN32 = math.Float32frombits(0x7f800001)
	sNaN64 = math.Float64frombits(0x7ff0000000000001)
)

func gtypes() []gtype {
	return []gtype{
		{"null", "null", func(*uniq) string { return `"null"` }, []gval{
			{boundary: "normal", generic: nil, oracle: []byte{}},
		}},
		{"boolean", "boolean", func(*uniq) string { return `"boolean"` }, []gval{
			{boundary: "true", generic: true, oracle: []byte{0x01}},
			{boundary: "false", generic: false, oracle: []byte{0x00}},
		}},
		{"int", "int", func(*uniq) string { return `"int"` }, []gval{
			{boundary: "zero", generic: int32(0), oracle: appendZig(nil, 0)},
			{boundary: "one", generic: int32(1), oracle: appendZig(nil, 1)},
			{boundary: "neg", generic: int32(-1), oracle: appendZig(nil, -1)},
			{boundary: "maxint", generic: int32(math.MaxInt32), oracle: appendZig(nil, math.MaxInt32)},
			{boundary: "minint", generic: int32(math.MinInt32), oracle: appendZig(nil, math.MinInt32)},
		}},
		{"long", "long", func(*uniq) string { return `"long"` }, []gval{
			{boundary: "zero", generic: int64(0), oracle: appendZig(nil, 0)},
			{boundary: "neg", generic: int64(-1), oracle: appendZig(nil, -1)},
			{boundary: "2^53-1", generic: int64(1<<53 - 1), oracle: appendZig(nil, 1<<53-1)},
			{boundary: "2^53", generic: int64(1 << 53), oracle: appendZig(nil, 1<<53)},
			{boundary: "2^53+1", generic: int64(1<<53 + 1), oracle: appendZig(nil, 1<<53+1)},
			{boundary: "maxint", generic: int64(math.MaxInt64), oracle: appendZig(nil, math.MaxInt64)},
			{boundary: "minint", generic: int64(math.MinInt64), oracle: appendZig(nil, math.MinInt64)},
		}},
		{"float", "float", func(*uniq) string { return `"float"` }, []gval{
			{boundary: "zero", generic: float32(0), oracle: leF32(0)},
			{boundary: "nzero", generic: float32(math.Copysign(0, -1)), oracle: leF32(float32(math.Copysign(0, -1)))},
			{boundary: "normal", generic: float32(1.5), oracle: leF32(1.5)},
			{boundary: "inf", generic: float32(math.Inf(1)), oracle: leF32(float32(math.Inf(1)))},
			{boundary: "ninf", generic: float32(math.Inf(-1)), oracle: leF32(float32(math.Inf(-1)))},
			{boundary: "qnan", generic: float32(math.NaN()), oracle: leF32(float32(math.NaN()))},
			{boundary: "snan", generic: sNaN32, oracle: leF32(sNaN32), jsonLossy: true},
			{boundary: "smallest", generic: float32(math.SmallestNonzeroFloat32), oracle: leF32(math.SmallestNonzeroFloat32)},
			{boundary: "max", generic: float32(math.MaxFloat32), oracle: leF32(math.MaxFloat32)},
		}},
		{"double", "double", func(*uniq) string { return `"double"` }, []gval{
			{boundary: "zero", generic: float64(0), oracle: leF64(0)},
			{boundary: "nzero", generic: math.Copysign(0, -1), oracle: leF64(math.Copysign(0, -1))},
			{boundary: "normal", generic: 1.5, oracle: leF64(1.5)},
			{boundary: "inf", generic: math.Inf(1), oracle: leF64(math.Inf(1))},
			{boundary: "ninf", generic: math.Inf(-1), oracle: leF64(math.Inf(-1))},
			{boundary: "qnan", generic: math.NaN(), oracle: leF64(math.NaN())},
			{boundary: "snan", generic: sNaN64, oracle: leF64(sNaN64), jsonLossy: true},
			{boundary: "smallest", generic: math.SmallestNonzeroFloat64, oracle: leF64(math.SmallestNonzeroFloat64)},
			{boundary: "max", generic: math.MaxFloat64, oracle: leF64(math.MaxFloat64)},
		}},
		{"string", "string", func(*uniq) string { return `"string"` }, []gval{
			{boundary: "empty", generic: "", oracle: avroLen(nil)},
			{boundary: "ascii", generic: "a", oracle: avroLen([]byte("a"))},
			{boundary: "unicode", generic: "héllo 日本 🎉", oracle: avroLen([]byte("héllo 日本 🎉"))},
			{boundary: "nul", generic: "\x00nul", oracle: avroLen([]byte("\x00nul"))},
			{boundary: "controls", generic: "with\nnewline\ttab", oracle: avroLen([]byte("with\nnewline\ttab"))},
			{boundary: "spaces", generic: "  ", oracle: avroLen([]byte("  "))},
			{boundary: "large", generic: strings.Repeat("x", 70000), oracle: avroLen([]byte(strings.Repeat("x", 70000)))},
		}},
		{"bytes", "bytes", func(*uniq) string { return `"bytes"` }, []gval{
			{boundary: "empty", generic: []byte{}, oracle: avroLen(nil)},
			{boundary: "zerobyte", generic: []byte{0x00}, oracle: avroLen([]byte{0x00})},
			{boundary: "highbytes", generic: []byte{0xFF, 0x00, 0x7F}, oracle: avroLen([]byte{0xFF, 0x00, 0x7F})},
			{boundary: "large", generic: bytes.Repeat([]byte{0xAB}, 70000), oracle: avroLen(bytes.Repeat([]byte{0xAB}, 70000))},
		}},
		{"enum3", "enum", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"enum","name":%q,"symbols":["A","B","C"]}`, u.name("GE"))
		}, []gval{
			{boundary: "first", generic: "A", oracle: appendZig(nil, 0)},
			{boundary: "last", generic: "C", oracle: appendZig(nil, 2)},
		}},
		{"fixed0", "fixed", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"fixed","name":%q,"size":0}`, u.name("GF0"))
		}, []gval{
			{boundary: "empty", generic: []byte{}, oracle: []byte{}},
		}},
		{"fixed16", "fixed", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"fixed","name":%q,"size":16}`, u.name("GF16"))
		}, []gval{
			{boundary: "zero", generic: make([]byte, 16), oracle: make([]byte, 16)},
			{boundary: "high", generic: bytes.Repeat([]byte{0xFF}, 16), oracle: bytes.Repeat([]byte{0xFF}, 16)},
		}},
		// ---- logicals in their SPEC-VALID placement (enriched round-trip) ----
		{"uuid-string", "uuid", func(*uniq) string { return `{"type":"string","logicalType":"uuid"}` }, []gval{
			{boundary: "normal", generic: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", oracle: avroLen([]byte("6ba7b810-9dad-11d1-80b4-00c04fd430c8"))},
		}},
		{"date", "date", func(*uniq) string { return `{"type":"int","logicalType":"date"}` }, []gval{
			{boundary: "epoch", generic: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), oracle: appendZig(nil, 0)},
			{boundary: "pre-epoch", generic: time.Date(1969, 7, 20, 0, 0, 0, 0, time.UTC)},
			{boundary: "far", generic: time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)},
		}},
		{"timestamp-millis", "timestamp-millis", func(*uniq) string { return `{"type":"long","logicalType":"timestamp-millis"}` }, []gval{
			{boundary: "normal", generic: time.UnixMilli(1717243496789).UTC()},
			{boundary: "epoch", generic: time.UnixMilli(0).UTC(), oracle: appendZig(nil, 0)},
		}},
		{"timestamp-micros", "timestamp-micros", func(*uniq) string { return `{"type":"long","logicalType":"timestamp-micros"}` }, []gval{
			{boundary: "normal", generic: time.UnixMicro(1717243496789012).UTC()},
		}},
		{"decimal-bytes", "decimal", func(*uniq) string {
			return `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
		}, []gval{
			{boundary: "normal", generic: big.NewRat(12345, 100)},
			{boundary: "neg", generic: big.NewRat(-1, 4)},
			{boundary: "zero", generic: big.NewRat(0, 1)},
		}},
		{"decimal-fixed", "decimal", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"fixed","name":%q,"size":8,"logicalType":"decimal","precision":10,"scale":2}`, u.name("GDF"))
		}, []gval{
			{boundary: "normal", generic: big.NewRat(99999, 100)},
			{boundary: "neg", generic: big.NewRat(-12345, 100)},
		}},
		{"duration", "duration", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"fixed","name":%q,"size":12,"logicalType":"duration"}`, u.name("GDUR"))
		}, []gval{
			{boundary: "normal", generic: avro.Duration{Months: 1, Days: 2, Milliseconds: 3}},
			{boundary: "zero", generic: avro.Duration{}},
			{boundary: "maxu32", generic: avro.Duration{Months: math.MaxUint32, Days: math.MaxUint32, Milliseconds: math.MaxUint32}},
		}},
		{"big-decimal", "big-decimal", func(*uniq) string { return `{"type":"bytes","logicalType":"big-decimal"}` }, []gval{
			{boundary: "normal", generic: big.NewRat(314159, 100000)},
			{boundary: "neg", generic: big.NewRat(-7, 8)},
		}},
		// Every remaining logical (axis-completeness: "every logical").
		{"time-millis", "time-millis", func(*uniq) string { return `{"type":"int","logicalType":"time-millis"}` }, []gval{
			{boundary: "zero", generic: time.Duration(0), oracle: appendZig(nil, 0)},
			{boundary: "max", generic: 23*time.Hour + 59*time.Minute + 59*time.Second + 999*time.Millisecond},
		}},
		{"time-micros", "time-micros", func(*uniq) string { return `{"type":"long","logicalType":"time-micros"}` }, []gval{
			{boundary: "zero", generic: time.Duration(0), oracle: appendZig(nil, 0)},
			{boundary: "max", generic: 23*time.Hour + 59*time.Minute + 59*time.Second + 999999*time.Microsecond},
		}},
		{"timestamp-nanos", "timestamp-nanos", func(*uniq) string { return `{"type":"long","logicalType":"timestamp-nanos"}` }, []gval{
			{boundary: "normal", generic: time.Unix(0, 1717243496789012345).UTC()},
			{boundary: "maxnanos", generic: time.Unix(0, math.MaxInt64).UTC()},
			{boundary: "minnanos", generic: time.Unix(0, math.MinInt64).UTC()},
		}},
		{"local-ts-millis", "local-timestamp-millis", func(*uniq) string { return `{"type":"long","logicalType":"local-timestamp-millis"}` }, []gval{
			{boundary: "normal", generic: time.UnixMilli(1717243496789).UTC()},
		}},
		{"local-ts-micros", "local-timestamp-micros", func(*uniq) string { return `{"type":"long","logicalType":"local-timestamp-micros"}` }, []gval{
			{boundary: "normal", generic: time.UnixMicro(1717243496789012).UTC()},
		}},
		{"local-ts-nanos", "local-timestamp-nanos", func(*uniq) string { return `{"type":"long","logicalType":"local-timestamp-nanos"}` }, []gval{
			{boundary: "normal", generic: time.Unix(0, 1717243496789012345).UTC()},
		}},
		{"uuid-fixed", "uuid", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"fixed","name":%q,"size":16,"logicalType":"uuid"}`, u.name("GUF"))
		}, []gval{
			{boundary: "normal", generic: "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		}},
		// Cardinality boundaries of enum/fixed the legacy gtypes set omits.
		{"enum1", "enum", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"enum","name":%q,"symbols":["Only"]}`, u.name("GE1"))
		}, []gval{
			{boundary: "only", generic: "Only", oracle: appendZig(nil, 0)},
		}},
		{"fixed1", "fixed", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"fixed","name":%q,"size":1}`, u.name("GF1"))
		}, []gval{
			{boundary: "zero", generic: []byte{0x00}, oracle: []byte{0x00}},
			{boundary: "high", generic: []byte{0xFF}, oracle: []byte{0xFF}},
		}},
		// ---- containers ----
		{"rec2", "record", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"record","name":%q,"fields":[{"name":"x","type":"int"},{"name":"y","type":"string"}]}`, u.name("GRec"))
		}, []gval{
			{boundary: "normal", generic: map[string]any{"x": int32(7), "y": "v"}},
		}},
		{"rec0", "record", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"record","name":%q,"fields":[]}`, u.name("GEmpty"))
		}, []gval{
			{boundary: "empty", generic: map[string]any{}, oracle: []byte{}},
		}},
		{"arr-int", "array", func(*uniq) string { return `{"type":"array","items":"int"}` }, []gval{
			{boundary: "empty", generic: []any{}, oracle: []byte{0x00}},
			{boundary: "some", generic: []any{int32(1), int32(2), int32(3)}},
		}},
		{"map-str", "map", func(*uniq) string { return `{"type":"map","values":"string"}` }, []gval{
			{boundary: "empty", generic: map[string]any{}, oracle: []byte{0x00}},
			{boundary: "one", generic: map[string]any{"k": "v"}},
		}},
	}
}

// gMetadata asserts (c) for the parse-time / metadata-API surface: Canonical()
// is deterministic and Canonical-equality implies Fingerprint-equality, and the
// metadata rebuild Root().Schema() preserves both (runCore already checks the
// rebuild's wire; this adds the canonical/fingerprint determinism axis).
func gMetadata(t *testing.T, schemaJSON string) {
	t.Helper()
	s := avro.MustParse(schemaJSON)
	c1 := s.Canonical()
	s2 := avro.MustParse(schemaJSON)
	c2 := s2.Canonical()
	if !bytes.Equal(c1, c2) {
		t.Fatalf("Canonical() not deterministic:\n a=%s\n b=%s\nschema: %s", c1, c2, schemaJSON)
	}
	// Same canonical form => same Rabin fingerprint.
	if !bytes.Equal(s.Fingerprint(avro.NewRabin()), s2.Fingerprint(avro.NewRabin())) {
		t.Fatalf("equal Canonical but different Fingerprint\nschema: %s", schemaJSON)
	}
	// The metadata rebuild's canonical form must match the original's: Root()
	// observed structure agrees with the parsed wire schema.
	root := s.Root()
	rebuilt, err := root.Schema()
	if err != nil {
		t.Fatalf("Root().Schema(): %v\nschema: %s", err, schemaJSON)
	}
	if !bytes.Equal(c1, rebuilt.Canonical()) {
		t.Fatalf("rebuilt Canonical differs:\n orig=%s\n reb =%s\nschema: %s", c1, rebuilt.Canonical(), schemaJSON)
	}
}

// gNaNCell runs the binary-bit-exact battery for a jsonLossy (NaN-payload)
// value: binary is asserted bit-exact (independent oracle + stable re-encode +
// SOE + identity-resolved), JSON is asserted only value-equal (a NaN), never
// wire-stable. This is the provable split runCore's stricter JSON-wire check
// cannot express.
func gNaNCell(t *testing.T, schemaJSON string, top bool, mv gval, vin any) {
	t.Helper()
	s := avro.MustParse(schemaJSON)
	w1, err := s.AppendEncode(nil, vin)
	if err != nil {
		t.Fatalf("binEnc: %v", err)
	}
	if top && mv.oracle != nil && !bytes.Equal(w1, mv.oracle) {
		t.Fatalf("binary wire diverges from independent NaN oracle (canonicalized?):\n got=%x\nwant=%x", w1, mv.oracle)
	}
	var a1 any
	if _, err := s.Decode(w1, &a1); err != nil {
		t.Fatalf("binDec: %v", err)
	}
	w2, err := s.AppendEncode(nil, a1)
	if err != nil || !bytes.Equal(w2, w1) {
		t.Fatalf("binary re-encode not bit-stable for NaN payload: err=%v\n w1=%x\n w2=%x", err, w1, w2)
	}
	// SOE preserves the exact binary body.
	soe, err := s.AppendSingleObject(nil, a1)
	if err != nil {
		t.Fatalf("AppendSingleObject: %v", err)
	}
	var aSoe any
	if _, err := s.DecodeSingleObject(soe, &aSoe); err != nil || !matEqual(aSoe, a1) {
		t.Fatalf("SOE NaN round-trip: err=%v", err)
	}
	// Identity-resolved binary decode is bit-exact too.
	res, err := avro.Resolve(s, s)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	var ar any
	if _, err := res.Decode(w1, &ar); err != nil {
		t.Fatalf("resolved decode: %v", err)
	}
	wr, err := s.AppendEncode(nil, ar)
	if err != nil || !bytes.Equal(wr, w1) {
		t.Fatalf("resolved NaN re-encode not bit-stable: err=%v\n w1=%x\n wr=%x", err, w1, wr)
	}
	// JSON: round-trips to a value-equal NaN (the achievable invariant), not
	// the same wire — the format cannot carry the payload.
	j1, err := s.AppendEncodeJSON(nil, a1)
	if err != nil {
		t.Fatalf("jsonEnc: %v", err)
	}
	var aj any
	if err := s.DecodeJSON(j1, &aj); err != nil {
		t.Fatalf("jsonDec: %v\n j=%s", err, j1)
	}
	if !matEqual(aj, a1) {
		t.Fatalf("JSON NaN round-trip not value-equal:\n bin=%#v\njson=%#v", a1, aj)
	}
}

// TestMatrix_Generative is the master cross: every type (boundary-rich) × every
// composition context × every boundary value, run through the calibration-free
// core battery (runCore: binary/JSON/resolved/rebuild/SOE/stream/append) plus
// the independent wire oracle and the metadata-API agreement.
func TestMatrix_Generative(t *testing.T) {
	ctxs := matCtxs()
	for _, gt := range gtypes() {
		for _, cx := range ctxs {
			if cx.skip != nil && cx.skip(gt.kind) {
				continue
			}
			t.Run(gt.label+"/"+cx.label, func(t *testing.T) {
				for _, mv := range gt.values {
					t.Run(mv.boundary, func(t *testing.T) {
						u := &uniq{}
						schema := cx.schema(gt.schema(u), gt.kind, u)
						vin := cx.wrap(mv.generic)
						if mv.jsonLossy {
							gNaNCell(t, schema, cx.label == "top", mv, vin)
							gMetadata(t, schema)
							return
						}
						runCore(t, schema, vin)
						gMetadata(t, schema)
						// Independent wire oracle: only at top context, where no
						// framing intervenes, and only when the value carries one.
						if cx.label == "top" && mv.oracle != nil {
							s := avro.MustParse(schema)
							w, err := s.AppendEncode(nil, mv.generic)
							if err != nil {
								t.Fatalf("encode for oracle: %v", err)
							}
							if !bytes.Equal(w, mv.oracle) {
								t.Fatalf("wire diverges from independent oracle (encode-side rewrite?):\n got=%x\nwant=%x\ntype=%s boundary=%s", w, mv.oracle, gt.label, mv.boundary)
							}
						}
					})
				}
			})
		}
	}
}

// ===========================================================================
// Layer 2 — the typed/unsafe path and container specializations.
//
// Assertion (a) demands all four codec paths agree byte-identically. The
// binary-unsafe path (addressable struct fields, unsafe.go) and the per-element
// container fast paths ([]T, map[string]T) are reached only with strongly-typed
// Go targets. This layer drives every typed scalar through five positions —
// bare top, struct field (the unsafe fast path), []T and map[string]T (the
// container specializations), and *T (the pointer path) — at the boundary
// values the legacy typed table omits. The float32 signaling-NaN cell is the
// sharp one: the float32 encoder has a documented fast/slow split keyed on
// "float32→float64→float32 is bit-exact for all NON-NaN values", so a signaling
// NaN takes the slow path, and every typed position must still emit the exact
// payload — caught by the independent oracle, not calibration.
// ===========================================================================

// gtyped is one typed scalar: the field/element schema, the Go type for the
// unsafe/typed targets, and boundary-tagged values in typed + generic form.
type gtyped struct {
	label  string
	schema string
	goType reflect.Type
	values []gtval
}

type gtval struct {
	boundary  string
	typed     any    // assignable to goType
	generic   any    // generic-path equivalent (map[string]any/[]any/scalar)
	oracle    []byte // bare top-context wire bytes (nil => no independent oracle)
	jsonLossy bool   // NaN payload: binary bit-exact, JSON value-equal only
}

func gtypedTypes() []gtyped {
	rat := big.NewRat(123, 4)
	ts := time.Date(2024, 6, 1, 12, 34, 56, 789000000, time.UTC)
	uuid16 := [16]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}
	return []gtyped{
		{"boolean", `"boolean"`, reflect.TypeOf(true), []gtval{
			{boundary: "true", typed: true, generic: true, oracle: []byte{0x01}},
			{boundary: "false", typed: false, generic: false, oracle: []byte{0x00}},
		}},
		{"int", `"int"`, reflect.TypeOf(int32(0)), []gtval{
			{boundary: "neg", typed: int32(-5), generic: int32(-5), oracle: appendZig(nil, -5)},
			{boundary: "maxint", typed: int32(math.MaxInt32), generic: int32(math.MaxInt32), oracle: appendZig(nil, math.MaxInt32)},
			{boundary: "minint", typed: int32(math.MinInt32), generic: int32(math.MinInt32), oracle: appendZig(nil, math.MinInt32)},
		}},
		{"int-as-int16", `"int"`, reflect.TypeOf(int16(0)), []gtval{
			{boundary: "normal", typed: int16(300), generic: int32(300), oracle: appendZig(nil, 300)},
			{boundary: "minint16", typed: int16(math.MinInt16), generic: int32(math.MinInt16), oracle: appendZig(nil, math.MinInt16)},
		}},
		{"long", `"long"`, reflect.TypeOf(int64(0)), []gtval{
			{boundary: "2^53+1", typed: int64(1<<53 + 1), generic: int64(1<<53 + 1), oracle: appendZig(nil, 1<<53+1)},
			{boundary: "maxint", typed: int64(math.MaxInt64), generic: int64(math.MaxInt64), oracle: appendZig(nil, math.MaxInt64)},
			{boundary: "minint", typed: int64(math.MinInt64), generic: int64(math.MinInt64), oracle: appendZig(nil, math.MinInt64)},
		}},
		{"long-as-uint32", `"long"`, reflect.TypeOf(uint32(0)), []gtval{
			{boundary: "big", typed: uint32(4000000000), generic: int64(4000000000), oracle: appendZig(nil, 4000000000)},
		}},
		{"float", `"float"`, reflect.TypeOf(float32(0)), []gtval{
			{boundary: "normal", typed: float32(2.5), generic: float32(2.5), oracle: leF32(2.5)},
			{boundary: "nzero", typed: float32(math.Copysign(0, -1)), generic: float32(math.Copysign(0, -1)), oracle: leF32(float32(math.Copysign(0, -1)))},
			{boundary: "inf", typed: float32(math.Inf(1)), generic: float32(math.Inf(1)), oracle: leF32(float32(math.Inf(1)))},
			{boundary: "qnan", typed: float32(math.NaN()), generic: float32(math.NaN()), oracle: leF32(float32(math.NaN()))},
			{boundary: "snan", typed: sNaN32, generic: sNaN32, oracle: leF32(sNaN32), jsonLossy: true},
			{boundary: "max", typed: float32(math.MaxFloat32), generic: float32(math.MaxFloat32), oracle: leF32(math.MaxFloat32)},
		}},
		{"double", `"double"`, reflect.TypeOf(float64(0)), []gtval{
			{boundary: "normal", typed: 6.25, generic: 6.25, oracle: leF64(6.25)},
			{boundary: "nzero", typed: math.Copysign(0, -1), generic: math.Copysign(0, -1), oracle: leF64(math.Copysign(0, -1))},
			{boundary: "inf", typed: math.Inf(-1), generic: math.Inf(-1), oracle: leF64(math.Inf(-1))},
			{boundary: "snan", typed: sNaN64, generic: sNaN64, oracle: leF64(sNaN64), jsonLossy: true},
			{boundary: "max", typed: math.MaxFloat64, generic: math.MaxFloat64, oracle: leF64(math.MaxFloat64)},
		}},
		{"string", `"string"`, reflect.TypeOf(""), []gtval{
			{boundary: "normal", typed: "typ", generic: "typ", oracle: avroLen([]byte("typ"))},
			{boundary: "empty", typed: "", generic: "", oracle: avroLen(nil)},
		}},
		{"bytes", `"bytes"`, reflect.TypeOf([]byte(nil)), []gtval{
			{boundary: "normal", typed: []byte{9, 8}, generic: []byte{9, 8}, oracle: avroLen([]byte{9, 8})},
			{boundary: "empty", typed: []byte{}, generic: []byte{}, oracle: avroLen(nil)},
		}},
		{"enum", `{"type":"enum","name":"GTYE","symbols":["A","B"]}`, reflect.TypeOf(""), []gtval{
			{boundary: "B", typed: "B", generic: "B", oracle: appendZig(nil, 1)},
		}},
		{"fixed2", `{"type":"fixed","name":"GTYF","size":2}`, reflect.TypeOf([2]byte{}), []gtval{
			{boundary: "normal", typed: [2]byte{1, 2}, generic: []byte{1, 2}, oracle: []byte{1, 2}},
		}},
		{"uuid-fixed16", `{"type":"fixed","name":"GTYU","size":16,"logicalType":"uuid"}`, reflect.TypeOf([16]byte{}), []gtval{
			{boundary: "normal", typed: uuid16, generic: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", oracle: uuid16[:]},
		}},
		{"date", `{"type":"int","logicalType":"date"}`, reflect.TypeOf(time.Time{}), []gtval{
			{boundary: "normal", typed: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), generic: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		}},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, reflect.TypeOf(time.Duration(0)), []gtval{
			{boundary: "normal", typed: 3 * time.Hour, generic: 3 * time.Hour},
		}},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, reflect.TypeOf(time.Time{}), []gtval{
			{boundary: "normal", typed: ts, generic: ts},
		}},
		{"decimal", `{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}`, reflect.TypeOf(&big.Rat{}), []gtval{
			{boundary: "normal", typed: rat, generic: rat},
		}},
		{"duration", `{"type":"fixed","name":"GTYD","size":12,"logicalType":"duration"}`, reflect.TypeOf(avro.Duration{}), []gtval{
			{boundary: "normal", typed: avro.Duration{Months: 3, Days: 1, Milliseconds: 9}, generic: avro.Duration{Months: 3, Days: 1, Milliseconds: 9}},
		}},
	}
}

// gEncEq encodes v against s on the binary path and asserts equality to want,
// returning the wire for further checks.
func gEncEq(t *testing.T, s *avro.Schema, v any, want []byte, what string) []byte {
	t.Helper()
	w, err := s.AppendEncode(nil, v)
	if err != nil {
		t.Fatalf("%s: encode: %v", what, err)
	}
	if want != nil && !bytes.Equal(w, want) {
		t.Fatalf("%s: wire mismatch:\n got=%x\nwant=%x", what, w, want)
	}
	return w
}

// gTypedCell drives one typed scalar value through the bare, struct (unsafe),
// []T, map[string]T, and *T positions, asserting byte-identity across the safe,
// unsafe, generic, and container paths plus the independent oracle, and the
// JSON twin parity. The jsonLossy (NaN payload) split drops only the JSON→wire
// re-encode step, never the binary bit-exactness.
func gTypedCell(t *testing.T, gd gtyped, tv gtval) {
	t.Helper()

	// ---- P1: bare top scalar (the typed scalar encoder; float32 slow path). ----
	sTop := avro.MustParse(gd.schema)
	wTop := gEncEq(t, sTop, tv.typed, tv.oracle, "bare-typed")
	wGen := gEncEq(t, sTop, tv.generic, tv.oracle, "bare-generic")
	if !bytes.Equal(wTop, wGen) {
		t.Fatalf("bare typed vs generic differ:\n t=%x\n g=%x", wTop, wGen)
	}
	// Decode into a fresh typed target, re-encode byte-stable.
	backTop := reflect.New(gd.goType)
	if _, err := sTop.Decode(wTop, backTop.Interface()); err != nil {
		t.Fatalf("bare typed decode: %v", err)
	}
	gEncEq(t, sTop, backTop.Elem().Interface(), wTop, "bare typed re-encode")

	// ---- P2: struct field (addressable => unsafe fast path; non-addr => reflect). ----
	st := reflect.StructOf([]reflect.StructField{{Name: "F", Type: gd.goType, Tag: `avro:"f"`}})
	recSchema := fmt.Sprintf(`{"type":"record","name":"GS","fields":[{"name":"f","type":%s}]}`, gd.schema)
	sRec := avro.MustParse(recSchema)
	pStruct := reflect.New(st)
	pStruct.Elem().Field(0).Set(reflect.ValueOf(tv.typed))
	wAddr := gEncEq(t, sRec, pStruct.Interface(), nil, "struct addressable (unsafe)")  // *struct => addressable
	wNon := gEncEq(t, sRec, pStruct.Elem().Interface(), nil, "struct non-addressable") // struct value => reflect
	wRecGen := gEncEq(t, sRec, map[string]any{"f": tv.generic}, nil, "struct generic")
	if !bytes.Equal(wAddr, wNon) || !bytes.Equal(wAddr, wRecGen) {
		t.Fatalf("struct safe/unsafe/generic diverge:\n addr=%x\n non =%x\n gen =%x", wAddr, wNon, wRecGen)
	}
	backStruct := reflect.New(st)
	if _, err := sRec.Decode(wAddr, backStruct.Interface()); err != nil {
		t.Fatalf("struct typed decode: %v", err)
	}
	gEncEq(t, sRec, backStruct.Interface(), wAddr, "struct decode→re-encode")

	// ---- P3: []T container specialization. ----
	arrSchema := avro.MustParse(fmt.Sprintf(`{"type":"array","items":%s}`, gd.schema))
	slice := reflect.MakeSlice(reflect.SliceOf(gd.goType), 0, 2)
	slice = reflect.Append(slice, reflect.ValueOf(tv.typed), reflect.ValueOf(tv.typed))
	wSlice := gEncEq(t, arrSchema, slice.Interface(), nil, "[]T")
	wSliceGen := gEncEq(t, arrSchema, []any{tv.generic, tv.generic}, nil, "[]any")
	if !bytes.Equal(wSlice, wSliceGen) {
		t.Fatalf("[]T vs []any diverge:\n t=%x\n g=%x", wSlice, wSliceGen)
	}
	backSlice := reflect.New(reflect.SliceOf(gd.goType))
	if _, err := arrSchema.Decode(wSlice, backSlice.Interface()); err != nil {
		t.Fatalf("[]T decode: %v", err)
	}
	gEncEq(t, arrSchema, backSlice.Interface(), wSlice, "[]T decode→re-encode")

	// ---- P4: map[string]T container specialization. ----
	mapSchema := avro.MustParse(fmt.Sprintf(`{"type":"map","values":%s}`, gd.schema))
	mt := reflect.MapOf(reflect.TypeOf(""), gd.goType)
	m := reflect.MakeMap(mt)
	m.SetMapIndex(reflect.ValueOf("k"), reflect.ValueOf(tv.typed))
	wMap := gEncEq(t, mapSchema, m.Interface(), nil, "map[string]T")
	wMapGen := gEncEq(t, mapSchema, map[string]any{"k": tv.generic}, nil, "map[string]any")
	if !bytes.Equal(wMap, wMapGen) {
		t.Fatalf("map[string]T vs map[string]any diverge:\n t=%x\n g=%x", wMap, wMapGen)
	}
	backMap := reflect.New(mt)
	if _, err := mapSchema.Decode(wMap, backMap.Interface()); err != nil {
		t.Fatalf("map[string]T decode: %v", err)
	}
	gEncEq(t, mapSchema, backMap.Interface(), wMap, "map[string]T decode→re-encode")

	// ---- P5: *T via a ["null",T] union (pointer typed path). ----
	if gd.label != "bytes" { // a nil []byte is the null branch already; *[]byte is redundant
		ptrUnionSchema := avro.MustParse(fmt.Sprintf(`["null",%s]`, gd.schema))
		p := reflect.New(gd.goType)
		p.Elem().Set(reflect.ValueOf(tv.typed))
		wPtr := gEncEq(t, ptrUnionSchema, p.Interface(), nil, "*T")
		wPtrGen := gEncEq(t, ptrUnionSchema, tv.generic, nil, "*T generic")
		if !bytes.Equal(wPtr, wPtrGen) {
			t.Fatalf("*T vs generic diverge:\n t=%x\n g=%x", wPtr, wPtrGen)
		}
	}

	// ---- JSON twins: typed-JSON == generic-JSON byte-identical (true even for
	// NaN — both emit "NaN"); for non-lossy also assert JSON→binary lands on the
	// original wire. ----
	jTyped, err := sRec.AppendEncodeJSON(nil, pStruct.Interface())
	if err != nil {
		t.Fatalf("struct typed encodeJSON: %v", err)
	}
	jGen, err := sRec.AppendEncodeJSON(nil, map[string]any{"f": tv.generic})
	if err != nil || !bytes.Equal(jTyped, jGen) {
		t.Fatalf("typed vs generic JSON differ: err=%v\n t=%s\n g=%s", err, jTyped, jGen)
	}
	jBack := reflect.New(st)
	if err := sRec.DecodeJSON(jTyped, jBack.Interface()); err != nil {
		t.Fatalf("struct typed decodeJSON: %v", err)
	}
	if !tv.jsonLossy {
		wj, err := sRec.AppendEncode(nil, jBack.Interface())
		if err != nil || !bytes.Equal(wj, wAddr) {
			t.Fatalf("typed JSON round-trip wire differs: err=%v\n w=%x\n j=%x", err, wAddr, wj)
		}
	}
}

func TestMatrix_GenerativeTyped(t *testing.T) {
	for _, gd := range gtypedTypes() {
		for _, tv := range gd.values {
			t.Run(gd.label+"/"+tv.boundary, func(t *testing.T) {
				gTypedCell(t, gd, tv)
			})
		}
	}
}

// ===========================================================================
// Layer 3a — resurrection regime × CONTEXT axis.
//
// A logical placed on an underlying it is not spec-valid for is soft-dropped to
// the bare underlying (validateLogical) UNLESS a CustomType with the matching
// LogicalType resurrects it. The contract: a resurrected wrong-kind/wrong-size
// logical must fall through to the RAW kind/size-checked codec on EVERY axis.
// custom_resurrection_parity_test.go proves this at TOP level across encode/
// decode × binary/JSON × natural/resolved × targets × three matching shapes.
//
// This layer adds the axis that file omits: COMPOSITION CONTEXT. A wrong-kind
// logical as an array element, map value, union branch, record field, or nested
// field reaches the per-element / per-branch fast paths — a different dispatch
// than the top-level codec — where a re-applied logical ser/deser would surface
// as a wire-byte or value divergence from the plain (soft-dropped) schema.
//
// Oracle: the PLAIN schema (same JSON, no CustomType). For every resurrecting
// shape the custom schema must be encode/decode-identical to plain in every
// context, and its wire must read back through its own natural and identity-
// resolved readers. Reuses resurrectionCells() and the encResult/decBin/decJSON
// helpers; *any decode targets catch a wrongly-enriched value (it appears as a
// logical Go type in the tree where plain yields the raw underlying).
// ===========================================================================

func TestMatrix_CustomResurrectedLogicalInContext(t *testing.T) {
	ctxs := []struct {
		label  string
		schema func(inner string) string
		wrap   func(v any) any
	}{
		{"field", func(in string) string {
			return fmt.Sprintf(`{"type":"record","name":"RC","fields":[{"name":"a","type":"long"},{"name":"f","type":%s}]}`, in)
		}, func(v any) any { return map[string]any{"a": int64(3), "f": v} }},
		{"array", func(in string) string {
			return fmt.Sprintf(`{"type":"array","items":%s}`, in)
		}, func(v any) any { return []any{v, v} }},
		{"map", func(in string) string {
			return fmt.Sprintf(`{"type":"map","values":%s}`, in)
		}, func(v any) any { return map[string]any{"k": v} }},
		{"union", func(in string) string {
			return fmt.Sprintf(`["null",%s]`, in)
		}, func(v any) any { return v }},
		{"nested", func(in string) string {
			return fmt.Sprintf(`{"type":"record","name":"RO","fields":[{"name":"o","type":{"type":"record","name":"RI","fields":[{"name":"f","type":%s}]}}]}`, in)
		}, func(v any) any { return map[string]any{"o": map[string]any{"f": v}} }},
	}
	anyTgt := func() any { return new(any) }
	for _, c := range resurrectionCells() {
		for _, cx := range ctxs {
			schema := cx.schema(c.schema)
			// Skip a context that cannot hold this cell's underlying (e.g. a
			// composed schema that fails to parse); none expected, but guard.
			if _, err := avro.Parse(schema); err != nil {
				continue
			}
			for _, sh := range []struct {
				name string
				opt  avro.SchemaOpt
			}{
				{"wildcard", avro.CustomType{LogicalType: c.logical}},
				{"avrotype-match", avro.CustomType{LogicalType: c.logical, AvroType: c.kind}},
				{"avrotype-mismatch", avro.CustomType{LogicalType: c.logical, AvroType: "boolean"}},
			} {
				t.Run(c.name+"/"+cx.label+"/"+sh.name, func(t *testing.T) {
					plain := avro.MustParse(schema)
					cs, err := avro.Parse(schema, sh.opt)
					if err != nil {
						t.Fatalf("parse custom: %v\nschema: %s", err, schema)
					}
					plainR := mustIdentityResolve(t, plain)
					csR := mustIdentityResolve(t, cs)
					for _, in := range c.inputs {
						v := cx.wrap(in)
						pbin, peb := plain.Encode(v)
						cbin, ceb := cs.Encode(v)
						if got, want := encResult(cbin, ceb), encResult(pbin, peb); got != want {
							t.Errorf("binary encode %T in %s: custom=%s plain=%s — logical ser applied to wrong kind/size", in, cx.label, got, want)
						}
						pjsn, pej := plain.EncodeJSON(v)
						cjsn, cej := cs.EncodeJSON(v)
						if got, want := encResult(cjsn, cej), encResult(pjsn, pej); got != want {
							t.Errorf("JSON encode %T in %s: custom=%q plain=%q — logical ser applied to wrong kind/size", in, cx.label, got, want)
						}
						if peb == nil && ceb == nil {
							if got, want := decBin(cs, cbin, anyTgt), decBin(plain, pbin, anyTgt); got != want {
								t.Errorf("binary decode natural %T in %s: custom=%s plain=%s — logical deser applied to wrong kind/size", in, cx.label, got, want)
							}
							if got, want := decBin(csR, cbin, anyTgt), decBin(plainR, pbin, anyTgt); got != want {
								t.Errorf("binary decode RESOLVED %T in %s: custom=%s plain=%s", in, cx.label, got, want)
							}
							var sink any
							if _, err := cs.Decode(cbin, &sink); err != nil {
								t.Errorf("custom binary wire (%T in %s) not self-readable: %v", in, cx.label, err)
							}
						}
						if pej == nil && cej == nil {
							if got, want := decJSON(cs, cjsn, anyTgt), decJSON(plain, pjsn, anyTgt); got != want {
								t.Errorf("JSON decode natural %T in %s: custom=%s plain=%s — logical deser applied to wrong kind/size", in, cx.label, got, want)
							}
							if got, want := decJSON(csR, cjsn, anyTgt), decJSON(plainR, pjsn, anyTgt); got != want {
								t.Errorf("JSON decode RESOLVED %T in %s: custom=%s plain=%s", in, cx.label, got, want)
							}
							var sink any
							if err := cs.DecodeJSON(cjsn, &sink); err != nil {
								t.Errorf("custom JSON wire (%T in %s) not self-readable: %v", in, cx.label, err)
							}
						}
					}
				})
			}
		}
	}
}

// ===========================================================================
// Layer 3b — the CustomType callback-config axis on VALID logicals.
//
// The five configs are {absent, passive, encode-only, decode-only, both}.
// absent (built-in both ways) is covered by the gtypes round-trip; passive
// (suppress), both (box), and count are covered by matrix_custom_test.go. This
// layer adds the two it omits — encode-only and decode-only — which exercise
// the ASYMMETRIC suppression gates (hasMatchingCustomTypeWithEncode keys on
// Encode!=nil for the encoder; hasMatchingCustomType keys on any non-wildcard
// match for the decoder).
//
// Oracles, anchored so the fixed/decimal JSON-suppression nuance can't false-
// fail: the PLAIN schema's wire (built-in) and the PASSIVE schema's raw decode
// (calibration). A cbox callback proves the custom side actually fired.
// ===========================================================================

func TestMatrix_GenerativeCustomConfigs(t *testing.T) {
	notEnriched := func(t *testing.T, v any, what string) {
		t.Helper()
		switch v.(type) {
		case time.Time, time.Duration, *big.Rat, avro.Duration:
			t.Fatalf("%s: raw value is enriched %T (logical deser fired where it must not)", what, v)
		}
	}
	for _, fr := range customFrags() {
		for _, pos := range customPositions() {
			posSchema := pos.schema(fr.schema)
			if pos.label == "multibranch" {
				posSchema = fmt.Sprintf(`["null","boolean",%s,%s]`, fr.schema, customPad(fr))
				if _, err := avro.Parse(posSchema); err != nil {
					continue
				}
			}
			plain := avro.MustParse(posSchema)
			vin := pos.wrap(fr.enriched)
			plainWire, err := plain.AppendEncode(nil, vin)
			if err != nil {
				t.Fatalf("%s/%s plain encode: %v", fr.label, pos.label, err)
			}
			plainJSON, err := plain.AppendEncodeJSON(nil, vin)
			if err != nil {
				t.Fatalf("%s/%s plain encodeJSON: %v", fr.label, pos.label, err)
			}
			// Calibrate the raw underlying tree via a passive (suppress) decode.
			passive := avro.MustParse(posSchema, avro.CustomType{LogicalType: fr.logical})
			var rawTree any
			if _, err := passive.Decode(plainWire, &rawTree); err != nil {
				t.Fatalf("%s/%s raw calibration: %v", fr.label, pos.label, err)
			}

			// ---- decode-only: built-in encode (byte-identical to plain),
			// custom Decode boxes the RAW underlying. ----
			t.Run(fr.label+"/"+pos.label+"/decode-only", func(t *testing.T) {
				ct := avro.CustomType{
					LogicalType: fr.logical,
					Decode:      func(v any, _ *avro.SchemaNode) (any, error) { return cbox{Raw: v}, nil },
				}
				s := avro.MustParse(posSchema, ct)
				// Encode is built-in => byte-identical wire and JSON to plain.
				if w, err := s.AppendEncode(nil, vin); err != nil || !bytes.Equal(w, plainWire) {
					t.Fatalf("decode-only encode not built-in: err=%v\n got=%x\nwant=%x", err, w, plainWire)
				}
				if j, err := s.AppendEncodeJSON(nil, vin); err != nil || !bytes.Equal(j, plainJSON) {
					t.Fatalf("decode-only encodeJSON not built-in: err=%v\n got=%s\nwant=%s", err, j, plainJSON)
				}
				// Decode boxes the raw underlying on both wire formats, equally.
				var aBin any
				if _, err := s.Decode(plainWire, &aBin); err != nil {
					t.Fatalf("decode-only binary decode: %v", err)
				}
				boxBin, ok := pos.unwrap(aBin).(cbox)
				if !ok {
					t.Fatalf("decode-only did not box (binary): %T", pos.unwrap(aBin))
				}
				notEnriched(t, boxBin.Raw, "decode-only binary")
				var aJSON any
				if err := s.DecodeJSON(plainJSON, &aJSON); err != nil {
					t.Fatalf("decode-only JSON decode: %v", err)
				}
				boxJSON, ok := pos.unwrap(aJSON).(cbox)
				if !ok {
					t.Fatalf("decode-only did not box (JSON): %T", pos.unwrap(aJSON))
				}
				if !matEqual(boxBin.Raw, boxJSON.Raw) {
					t.Fatalf("decode-only binary/JSON raw diverge:\n bin=%#v\njson=%#v", boxBin.Raw, boxJSON.Raw)
				}
			})

			// ---- encode-only: custom Encode unboxes to raw (built-in encode
			// suppressed), Decode is raw (suppressed). ----
			t.Run(fr.label+"/"+pos.label+"/encode-only", func(t *testing.T) {
				ct := avro.CustomType{
					LogicalType: fr.logical,
					Encode: func(v any, _ *avro.SchemaNode) (any, error) {
						if b, ok := v.(cbox); ok {
							return b.Raw, nil
						}
						return nil, avro.ErrSkipCustomType
					},
				}
				s := avro.MustParse(posSchema, ct)
				// Decode is suppressed => raw, identical to the passive schema.
				var aBin any
				if _, err := s.Decode(plainWire, &aBin); err != nil {
					t.Fatalf("encode-only binary decode: %v", err)
				}
				notEnriched(t, pos.unwrap(aBin), "encode-only binary decode")
				if !matEqual(aBin, rawTree) {
					t.Fatalf("encode-only decode not raw:\n got=%#v\nraw=%#v", aBin, rawTree)
				}
				// Encode the boxed raw tree: unbox => base encode => plain wire.
				boxed := boxRawTree(pos, rawTree)
				if w, err := s.AppendEncode(nil, boxed); err != nil || !bytes.Equal(w, plainWire) {
					t.Fatalf("encode-only boxed encode not raw-equivalent: err=%v\n got=%x\nwant=%x", err, w, plainWire)
				}
				// JSON encode of the boxed raw tree round-trips back to raw.
				jb, err := s.AppendEncodeJSON(nil, boxed)
				if err != nil {
					t.Fatalf("encode-only boxed encodeJSON: %v", err)
				}
				var jBack any
				if err := s.DecodeJSON(jb, &jBack); err != nil {
					t.Fatalf("encode-only JSON round-trip decode: %v\n j=%s", err, jb)
				}
				if !matEqual(jBack, rawTree) {
					t.Fatalf("encode-only JSON round-trip not raw:\n got=%#v\nraw=%#v", jBack, rawTree)
				}
			})
		}
	}
}

// boxRawTree wraps the inner (unwrapped) raw value of a position's raw tree in a
// cbox, leaving the surrounding structure intact — the encode-only callback
// unboxes exactly that inner value.
func boxRawTree(pos customPos, rawTree any) any {
	inner := pos.unwrap(rawTree)
	boxedInner := cbox{Raw: inner}
	switch pos.label {
	case "top", "nullunion", "multibranch":
		return boxedInner
	case "field":
		return map[string]any{"a": int64(4), "f": boxedInner}
	case "array":
		return []any{boxedInner, boxedInner}
	}
	return boxedInner
}

// ===========================================================================
// Layer 4 — metadata-API agreement with the wire (assertion (c) completion).
//
// gMetadata pins Canonical/Fingerprint determinism. This layer pins the two
// remaining metadata surfaces the user named: Fields[].Default and Root().Props.
//
// Fields[].Default: the contract is that the metadata default value, used AS a
// field value, encodes to the SAME wire as resolution/auto fill — i.e. the
// observed default agrees with the wire. Crossed with the resolution default-
// fill (writer lacks the field) and Resolve⇔CheckCompatibility agreement.
// ===========================================================================

func TestMatrix_GenerativeDefaultFill(t *testing.T) {
	kinds := []struct {
		label      string
		fieldType  string
		defaultLit string
	}{
		{"boolean", `"boolean"`, `true`},
		{"int", `"int"`, `7`},
		{"long", `"long"`, `9007199254740993`},
		{"float", `"float"`, `1.5`},
		{"double", `"double"`, `-2.25`},
		{"string", `"string"`, `"d"`},
		{"bytes", `"bytes"`, `"\u00ff"`},
		{"bytes-empty", `"bytes"`, `""`},
		{"enum", `{"type":"enum","name":"GDE","symbols":["A","B"]}`, `"B"`},
		{"fixed1", `{"type":"fixed","name":"GDF","size":1}`, `"\u00ab"`},
		{"fixed0", `{"type":"fixed","name":"GDF0","size":0}`, `""`},
		{"date", `{"type":"int","logicalType":"date"}`, `19723`},
		{"timestamp", `{"type":"long","logicalType":"timestamp-millis"}`, `1717243496789`},
		{"nullunion", `["null","int"]`, `null`},
		{"union-int-first", `["int","string"]`, `42`},
		{"array", `{"type":"array","items":"int"}`, `[1,2]`},
		{"map", `{"type":"map","values":"string"}`, `{"k":"v"}`},
		{"record", `{"type":"record","name":"GDR","fields":[{"name":"i","type":"int"}]}`, `{"i":3}`},
		{"empty-record", `{"type":"record","name":"GDER","fields":[]}`, `{}`},
	}
	for _, k := range kinds {
		t.Run(k.label, func(t *testing.T) {
			rSchema := fmt.Sprintf(`{"type":"record","name":"R","fields":[
				{"name":"pre","type":"string"},
				{"name":"f","type":%s,"default":%s}]}`, k.fieldType, k.defaultLit)
			wSchema := `{"type":"record","name":"R","fields":[{"name":"pre","type":"string"}]}`
			r := avro.MustParse(rSchema)
			w := avro.MustParse(wSchema)
			res, err := resolveBoth(t, w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}

			// Metadata observation: locate field "f" and its typed Default.
			root := r.Root()
			var fld *avro.SchemaField
			for i := range root.Fields {
				if root.Fields[i].Name == "f" {
					fld = &root.Fields[i]
				}
			}
			if fld == nil || !fld.HasDefault {
				t.Fatalf("metadata: field f missing or has no default; HasDefault must be true")
			}

			// (c) The metadata Default, used as the field value, must encode to
			// the SAME wire as the reader's own auto-fill of the missing field.
			autoWire, err := r.AppendEncode(nil, map[string]any{"pre": "p"})
			if err != nil {
				t.Fatalf("reader auto-fill encode: %v", err)
			}
			explicitWire, err := r.AppendEncode(nil, map[string]any{"pre": "p", "f": fld.Default})
			if err != nil {
				t.Fatalf("encode with metadata Default as the value: %v\ndefault=%#v", err, fld.Default)
			}
			if !bytes.Equal(autoWire, explicitWire) {
				t.Fatalf("metadata Fields[].Default disagrees with the wire:\n auto    =%x\n explicit=%x\n default =%#v", autoWire, explicitWire, fld.Default)
			}

			// Resolution default-fill lands on the same auto-fill wire, and the
			// reader's JSON fill agrees too (the three fill paths converge).
			wWire, err := w.AppendEncode(nil, map[string]any{"pre": "p"})
			if err != nil {
				t.Fatalf("writer encode: %v", err)
			}
			var got map[string]any
			if _, err := res.Decode(wWire, &got); err != nil {
				t.Fatalf("resolved default fill: %v", err)
			}
			gotWire, err := r.AppendEncode(nil, got)
			if err != nil || !bytes.Equal(gotWire, autoWire) {
				t.Fatalf("resolution fill wire differs from auto-fill: err=%v\n got =%x\n auto=%x", err, gotWire, autoWire)
			}
			var jfill map[string]any
			if err := r.DecodeJSON([]byte(`{"pre":"p"}`), &jfill); err != nil {
				t.Fatalf("reader JSON fill: %v", err)
			}
			if !matEqual(got, jfill) {
				t.Fatalf("resolution fill diverges from JSON fill:\n res =%#v\n json=%#v", got, jfill)
			}
		})
	}
}

// TestMatrix_GenerativeUnionContainerDefaultFill is the durable net for the
// union-default metadata↔wire class: when a union field's branches are CONTAINERS
// (record/array/map) holding a leaf and the field has a default, the branch+value
// the metadata selector (branchAcceptsDefault → coerceMetadataDefault) reports must
// match the branch+value the wire auto-fill produces, on BOTH wire formats. Two
// findings landed in this class from the SAME uncovered cells — a float string→float
// coercion in a nested branch, then an int64→int32 overflow wrap — both hidden
// because the prior nets (the flat TestMatrix_GenerativeDefaultFill above, and the
// matFrags×matCtxs core matrix) drove only IN-RANGE values. This matrix crosses the
// boundary/overflow value classes those miss.
//
// Wire-as-oracle (the AUDIT_CORE matrix contract — the binary auto-fill decode is
// canonical). Per cell × container:
//   - Root().Schema() rebuild re-encodes the auto-fill BYTE-IDENTICALLY on binary
//     AND JSON. This is the severe surface: a wrapped or wrong-branch default
//     silently changes the schema's own wire through the documented "Root preserves
//     all metadata" round-trip. Representation-agnostic, so it holds for a logical
//     leaf whose Default surfaces the raw Avro-native value (NOT_BUGS #30) while the
//     wire decodes the transformed value.
//   - the direct JSON decode auto-fill (DecodeJSON of an empty object, which
//     materializes the stored default via applyFieldDefault) agrees with the binary
//     auto-fill (matEqual).
//   - for non-logical leaves, Root().Fields[].Default equals the binary auto-fill
//     decode type-exactly (matEqual) — the direct metadata pin the int64→int32 wrap
//     violated (int32(-1294967296) where the wire decoded float64(3e9)).
//
// Each cell pairs a leaf branch with a VALUE-ADMITTING wider/other sibling branch so
// the schema parses: an overflow default the leaf rejects is held by the sibling and
// the divergence surfaces. A same-class-rejecting sibling would reject the schema at
// parse and hide the cell behind a parse error — exactly how these escaped (see
// TestMatrix_UnionContainerNestedIntDefaultOverflowMatchesWire and
// TestMatrix_UnionContainerNestedFloatDefaultSelectionMatchesWire, the
// single-shape pins this matrix generalizes).
func TestMatrix_GenerativeUnionContainerDefaultFill(t *testing.T) {
	for _, c := range udfCells() {
		for _, cont := range udfContainers() {
			t.Run(c.name+"/"+cont.name, func(t *testing.T) {
				branchA := fmt.Sprintf(`{"type":"record","name":"A","fields":[{"name":"x","type":%s}]}`, cont.wrap(c.leaf))
				branchB := fmt.Sprintf(`{"type":"record","name":"B","fields":[{"name":"x","type":%s}]}`, cont.wrap(c.sib))
				schema := fmt.Sprintf(`{"type":"record","name":"Outer","fields":[{"name":"f","type":[%s,%s],"default":%s}]}`,
					branchA, branchB, cont.def(c.defLit))
				udInvariant(t, schema, `{}`, map[string]any{}, udfIsLogical(c.leaf),
					func(s *avro.Schema, a1 map[string]any) [][2]any {
						return [][2]any{{s.Root().Fields[0].Default, a1["f"]}}
					})
			})
		}
	}
}

// TestMatrix_GenerativeUnionContainerDefaultFillRecursive runs the same union-
// default invariant when the leaf-bearing branch is a SELF-REFERENTIAL record
// (N{x:<leaf>, next:["null","N"]}), so the default-fill and the metadata coercion
// run through a type that references itself — the second-occurrence / self-ref
// path flat schemas and the matFrags×matCtxs core matrix never reach. Two default
// depths: a shallow one ("next":null — the self-reference declared but not
// traversed) and a one-level-deep one ("next":{...,"next":null} — the recursion
// actually walked, so the coercion fires at BOTH levels). The leaf-bearing branch
// N pairs with a value-admitting self-referential sibling S whose x is the wider
// type, so a boundary default the leaf rejects is held by S and the cell is
// reachable (the value-admitting-sibling rule).
func TestMatrix_GenerativeUnionContainerDefaultFillRecursive(t *testing.T) {
	// addNext appends a "next" field value to a {"x":...} default object.
	addNext := func(obj, next string) string { return strings.TrimSuffix(obj, "}") + `,"next":` + next + "}" }
	for _, c := range udfCells() {
		for _, cont := range udfContainers() {
			for _, depth := range []string{"shallow", "deep"} {
				t.Run(c.name+"/"+cont.name+"/"+depth, func(t *testing.T) {
					branchN := fmt.Sprintf(`{"type":"record","name":"N","fields":[{"name":"x","type":%s},{"name":"next","type":["null","N"]}]}`, cont.wrap(c.leaf))
					branchS := fmt.Sprintf(`{"type":"record","name":"S","fields":[{"name":"x","type":%s},{"name":"next","type":["null","S"]}]}`, cont.wrap(c.sib))
					inner := cont.def(c.defLit) // {"x":<container form>}
					def := addNext(inner, "null")
					if depth == "deep" {
						def = addNext(inner, addNext(inner, "null"))
					}
					schema := fmt.Sprintf(`{"type":"record","name":"Outer","fields":[{"name":"f","type":[%s,%s],"default":%s}]}`,
						branchN, branchS, def)
					udInvariant(t, schema, `{}`, map[string]any{}, udfIsLogical(c.leaf),
						func(s *avro.Schema, a1 map[string]any) [][2]any {
							return [][2]any{{s.Root().Fields[0].Default, a1["f"]}}
						})
				})
			}
		}
	}
}

// TestMatrix_GenerativeUnionContainerDefaultFillDiamond runs the same invariant
// when the union-default-bearing record DiaT is referenced from TWO positions
// (Outer{a:DiaT, b:DiaT}): DiaT is DEFINED at field a and a bare NAME REFERENCE at
// field b. This exercises the second-occurrence reference path — where the cache
// self-ref and type-alias cross-record bugs lived — for the default-fill class.
// The wire must fill BOTH a.f and b.f from DiaT's default (the reference resolves
// to the definition's default); Root() carries the default on the DEFINITION
// (Fields[0]; a bare reference correctly surfaces as a name node with no inline
// fields, so it is not separately asserted); and Root().Schema() must re-emit DiaT
// as ONE definition + a reference (not duplicated or renamed), byte-identically.
func TestMatrix_GenerativeUnionContainerDefaultFillDiamond(t *testing.T) {
	for _, c := range udfCells() {
		for _, cont := range udfContainers() {
			t.Run(c.name+"/"+cont.name, func(t *testing.T) {
				recA := fmt.Sprintf(`{"type":"record","name":"DiaA","fields":[{"name":"x","type":%s}]}`, cont.wrap(c.leaf))
				recB := fmt.Sprintf(`{"type":"record","name":"DiaB","fields":[{"name":"x","type":%s}]}`, cont.wrap(c.sib))
				tdef := fmt.Sprintf(`{"type":"record","name":"DiaT","fields":[{"name":"f","type":[%s,%s],"default":%s}]}`,
					recA, recB, cont.def(c.defLit))
				schema := fmt.Sprintf(`{"type":"record","name":"Outer","fields":[{"name":"a","type":%s},{"name":"b","type":"DiaT"}]}`, tdef)
				fill := map[string]any{"a": map[string]any{}, "b": map[string]any{}}
				_, a1, rebuilt := udInvariant(t, schema, `{"a":{},"b":{}}`, fill, udfIsLogical(c.leaf),
					func(s *avro.Schema, a1 map[string]any) [][2]any {
						// The DEFINITION (Fields[0]=a → DiaT → f) carries the default.
						defT := s.Root().Fields[0].Type.Fields[0].Default
						return [][2]any{{defT, a1["a"].(map[string]any)["f"]}}
					})
				// Reference path (every leaf, incl. logical): b.f must auto-fill from
				// the SAME default the definition's a.f did.
				aw := a1["a"].(map[string]any)["f"]
				if bw := a1["b"].(map[string]any)["f"]; !matEqual(bw, aw) {
					t.Errorf("reference branch b.f auto-fill diverges from definition a.f:\n  a.f = %#v\n  b.f = %#v", aw, bw)
				}
				// Structural: the rebuild defines DiaT exactly once (one definition + a
				// reference, not duplicated/renamed) and re-parses.
				if n := strings.Count(rebuilt.String(), `"name":"DiaT"`); n != 1 {
					t.Errorf("rebuild defines DiaT %d times, want 1 (one definition + a reference):\n  %s", n, rebuilt.String())
				}
				if _, err := avro.Parse(rebuilt.String()); err != nil {
					t.Errorf("rebuilt schema does not re-parse (duplicate/renamed type?): %v", err)
				}
			})
		}
	}
}

// udfCell is a leaf branch paired with a value-admitting wider/other sibling and a
// default literal spanning the in-range and boundary/overflow value classes. The
// three shape tests above share this table so flat, recursive, and diamond cross
// the identical (leaf × value-class × container) axes.
type udfCell struct{ name, leaf, sib, defLit string }

func udfCells() []udfCell {
	return []udfCell{
		// int: in-range, both int32 boundaries, and the two overflow forms of the
		// int64→int32 wrap — MaxInt32+1 wraps to a negative, 2^32 to a deceptively
		// valid 0 — both of which the leaf branch must REJECT (sibling long holds it).
		{"int_in_range", `"int"`, `"long"`, `42`},
		{"int_max", `"int"`, `"long"`, `2147483647`},
		{"int_min", `"int"`, `"long"`, `-2147483648`},
		{"int_overflow_negwrap", `"int"`, `"long"`, `2147483648`},
		{"int_overflow_zerowrap", `"int"`, `"long"`, `4294967296`},
		// long: in-range, both int64 boundaries, beyond-int64 (sibling double holds it).
		{"long_in_range", `"long"`, `"double"`, `42`},
		{"long_max", `"long"`, `"double"`, `9223372036854775807`},
		{"long_min", `"long"`, `"double"`, `-9223372036854775808`},
		{"long_beyond_int64", `"long"`, `"double"`, `99999999999999999999`},
		// float/double overflow is lossy-by-destination (float32→±Inf), CONSISTENT on
		// both surfaces — a control that must NOT be "fixed" to reject like the int arm.
		{"float_in_range", `"float"`, `"double"`, `1.5`},
		{"float_overflow_inf", `"float"`, `"double"`, `1e300`},
		{"double_in_range", `"double"`, `"string"`, `1.5`},
		{"double_large", `"double"`, `"string"`, `1e300`},
		// stringy leaves vs a string sibling: in-range picks the leaf, the boundary
		// (codepoint>0xFF / wrong fixed size / enum non-member) picks the sibling.
		{"bytes_in_range", `"bytes"`, `"string"`, `"Aÿ"`},
		{"bytes_codepoint_over_0xFF", `"bytes"`, `"string"`, `"Ā"`},
		{"fixed_right_size", `{"type":"fixed","name":"FX","size":2}`, `"string"`, `"AB"`},
		{"fixed_wrong_size", `{"type":"fixed","name":"FX","size":4}`, `"string"`, `"AB"`},
		{"enum_member", `{"type":"enum","name":"EN","symbols":["A","B"]}`, `"string"`, `"A"`},
		{"enum_nonmember", `{"type":"enum","name":"EN","symbols":["A","B"]}`, `"string"`, `"Z"`},
		{"string_any", `"string"`, `"bytes"`, `"hello"`},
		// logical leaf (long-backed): Default surfaces the raw long, the wire decodes
		// time.Time — checked by the representation-agnostic rebuild + JSON agreement.
		{"timestamp_millis_in_range", `{"type":"long","logicalType":"timestamp-millis"}`, `"double"`, `1717243496789`},
		{"timestamp_millis_beyond_int64", `{"type":"long","logicalType":"timestamp-millis"}`, `"double"`, `99999999999999999999`},
	}
}

func udfIsLogical(leaf string) bool { return strings.Contains(leaf, "logicalType") }

// udfContainer holds the leaf as a record field "x"'s value (so a union of two
// such records stays legal — a union cannot hold two arrays or two maps directly)
// at one of three container depths.
type udfContainer struct {
	name string
	wrap func(leaf string) string // the branch field "x"'s type
	def  func(lit string) string  // the default literal in field-"x" container form
}

func udfContainers() []udfContainer {
	return []udfContainer{
		{"record_field", func(l string) string { return l }, func(lit string) string { return `{"x":` + lit + `}` }},
		{"array_element", func(l string) string { return `{"type":"array","items":` + l + `}` }, func(lit string) string { return `{"x":[` + lit + `]}` }},
		{"map_value", func(l string) string { return `{"type":"map","values":` + l + `}` }, func(lit string) string { return `{"x":{"k":` + lit + `}}` }},
	}
}

// udInvariant runs the wire-as-oracle union-default invariant for one composed
// default-fill schema, shared by the flat/recursive/diamond shape tests above. The
// binary auto-fill decode of fillVal (an empty outer, whose missing nested defaults
// materialize) is canonical; fillJSON is its JSON form for the direct DecodeJSON
// fill. metaPairs returns, per metadata surface under test, the (Root-derived
// Default, wire-decoded value) pair that must match type-exactly for a NON-logical
// leaf (a logical leaf surfaces the raw Avro-native value per NOT_BUGS #30, so it
// is covered by the rebuild + JSON-decode checks instead). Returns the parsed
// schema, the canonical decode, and the metadata rebuild so a caller can add
// shape-specific checks (the diamond's reference path + one-definition structure).
func udInvariant(t *testing.T, schema, fillJSON string, fillVal map[string]any, logical bool,
	metaPairs func(s *avro.Schema, a1 map[string]any) [][2]any) (*avro.Schema, map[string]any, *avro.Schema) {
	t.Helper()
	s, err := avro.Parse(schema)
	if err != nil {
		t.Fatalf("parse: %v\n  %s", err, schema)
	}
	w1, err := s.Encode(fillVal)
	if err != nil {
		t.Fatalf("binary auto-fill encode: %v", err)
	}
	var a1 map[string]any
	mustDecode(t, s, w1, &a1)
	j1, err := s.AppendEncodeJSON(nil, fillVal)
	if err != nil {
		t.Fatalf("json auto-fill encode: %v", err)
	}

	// Non-logical: Root().Default equals the binary auto-fill decode, type-exactly
	// (the direct metadata pin the int64→int32 wrap violated).
	if !logical {
		for _, pr := range metaPairs(s, a1) {
			if meta, wire := pr[0], pr[1]; !matEqual(wire, meta) {
				t.Errorf("Root().Default disagrees with the binary auto-fill (wrong branch/value):\n  wire = %#v\n  meta = %#v", wire, meta)
			}
		}
	}

	// Direct JSON decode auto-fill agrees with the binary auto-fill. A DIRECT
	// DecodeJSON of the empty outer materializes the stored default via
	// applyFieldDefault — NOT a JSON encode→decode round-trip, which on these
	// overlapping record branches would hit the documented bare untagged-union
	// first-match loss (NOT_BUGS #5).
	var dj map[string]any
	if err := s.DecodeJSON([]byte(fillJSON), &dj); err != nil {
		t.Fatalf("json decode auto-fill: %v", err)
	}
	if !matEqual(dj, a1) {
		t.Errorf("JSON decode auto-fill disagrees with binary:\n  bin  = %#v\n  json = %#v", a1, dj)
	}

	// Root().Schema() rebuild re-encodes the auto-fill BYTE-IDENTICALLY on both wire
	// formats — the severe surface: a wrapped/wrong-branch default silently changes
	// the schema's own wire through the documented metadata round-trip. This is
	// representation-agnostic, so it covers logical leaves too.
	rn := s.Root()
	rebuilt, err := rn.Schema()
	if err != nil {
		t.Fatalf("Root().Schema(): %v", err)
	}
	rw, err := rebuilt.Encode(fillVal)
	if err != nil {
		t.Fatalf("rebuilt binary auto-fill: %v", err)
	}
	if !bytes.Equal(rw, w1) {
		t.Errorf("Root().Schema() binary rebuild auto-fill = %x, want %x (rebuilt default selects a different branch/value)", rw, w1)
	}
	rj, err := rebuilt.AppendEncodeJSON(nil, fillVal)
	if err != nil {
		t.Fatalf("rebuilt json auto-fill: %v", err)
	}
	if !bytes.Equal(rj, j1) {
		t.Errorf("Root().Schema() JSON rebuild auto-fill = %s, want %s (original)", rj, j1)
	}
	return s, a1, rebuilt
}

// TestMatrix_GenerativeProps pins Root().Props / Fields[].Props: they observe
// the parsed custom attributes, survive the metadata rebuild, and are stripped
// by Parsing Canonical Form (so they never perturb the fingerprint).
func TestMatrix_GenerativeProps(t *testing.T) {
	schema := `{"type":"record","name":"R","myrec":"v1","fields":[
		{"name":"f","type":"int","myfield":42},
		{"name":"g","type":{"type":"array","items":"long"},"tags":["a","b"]}]}`
	s := avro.MustParse(schema)
	root := s.Root()
	if root.Props["myrec"] != "v1" {
		t.Fatalf("Root().Props[myrec]=%#v want \"v1\"", root.Props["myrec"])
	}
	if len(root.Fields) != 2 || root.Fields[0].Props["myfield"] == nil {
		t.Fatalf("Fields[0].Props[myfield] missing: %#v", root.Fields[0].Props)
	}
	// PCF strips props: the fingerprint of the propful schema equals that of the
	// prop-stripped one.
	bare := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"f","type":"int"},
		{"name":"g","type":{"type":"array","items":"long"}}]}`)
	if !bytes.Equal(s.Fingerprint(avro.NewRabin()), bare.Fingerprint(avro.NewRabin())) {
		t.Fatalf("props perturb the canonical fingerprint:\n propful=%s\n bare   =%s", s.Canonical(), bare.Canonical())
	}
	// The rebuild preserves the props (observation survives a round-trip).
	rebuilt := mustNodeSchema(t, root)
	rr := rebuilt.Root()
	if rr.Props["myrec"] != "v1" || rr.Fields[0].Props["myfield"] == nil {
		t.Fatalf("rebuild dropped props: root=%#v field=%#v", rr.Props, rr.Fields[0].Props)
	}
}

// ===========================================================================
// Layer 6 — promotion decode-flavor × boundary values.
//
// The resolved/promoted decode-flavor with NORMAL values is covered by
// matrix_evolution (PromotionPairsByContext) and matrix_typed (PromotionIn
// EveryContext). This layer adds the boundary axis those omit, where promotion
// is width-changing and so value-lossy by design:
//
//   - int→float of MaxInt32 rounds (float32 cannot hold 2^31-1);
//   - long→double of 2^53+1 rounds (double cannot hold it);
//   - float→double of a signaling NaN quiets the payload (a width conversion,
//     exactly the float32→float64 case the codebase reasons about).
//
// The invariant is calibration-anchored, not bit-preserving: the resolved read
// of the writer wire, re-encoded against the reader, must equal the reader's own
// encoding of the GO-level promotion of the same value — i.e. the codec promotes
// exactly as a plain Go conversion would, with no extra corruption.
// ===========================================================================

func goPromote(wk, rk string, v any) any {
	switch wk + "->" + rk {
	case "int->long":
		return int64(v.(int32))
	case "int->float":
		return float32(v.(int32))
	case "int->double":
		return float64(v.(int32))
	case "long->float":
		return float32(v.(int64))
	case "long->double":
		return float64(v.(int64))
	case "float->double":
		return float64(v.(float32))
	case "string->bytes":
		return []byte(v.(string))
	case "bytes->string":
		return string(v.([]byte))
	}
	panic("no promotion " + wk + "->" + rk)
}

func TestMatrix_GenerativePromotionBoundary(t *testing.T) {
	pairs := []struct {
		wk, rk string
		vals   []any // writer-typed boundary values
	}{
		{"int", "long", []any{int32(math.MaxInt32), int32(math.MinInt32), int32(0)}},
		{"int", "float", []any{int32(math.MaxInt32), int32(math.MinInt32), int32(1 << 24), int32(1<<24 + 1)}},
		{"int", "double", []any{int32(math.MaxInt32), int32(math.MinInt32)}},
		{"long", "float", []any{int64(math.MaxInt64), int64(math.MinInt64)}},
		{"long", "double", []any{int64(1<<53 + 1), int64(math.MaxInt64), int64(math.MinInt64)}},
		{"float", "double", []any{float32(1.5), float32(math.Inf(1)), float32(math.Inf(-1)), float32(math.NaN()), sNaN32, float32(math.MaxFloat32), float32(math.Copysign(0, -1))}},
		{"string", "bytes", []any{"", "x", strings.Repeat("s", 70000)}},
		{"bytes", "string", []any{[]byte{}, []byte("y"), bytes.Repeat([]byte{0x41}, 70000)}},
	}
	// Representative contexts: top plus the per-element/per-field dispatches.
	ctxs := []struct {
		label  string
		schema func(kind string) string
		wrap   func(v any) any
	}{
		{"top", func(k string) string { return fmt.Sprintf("%q", k) }, func(v any) any { return v }},
		{"array", func(k string) string { return fmt.Sprintf(`{"type":"array","items":%q}`, k) }, func(v any) any { return []any{v, v} }},
		{"field", func(k string) string {
			return fmt.Sprintf(`{"type":"record","name":"PR","fields":[{"name":"f","type":%q}]}`, k)
		}, func(v any) any { return map[string]any{"f": v} }},
	}
	for _, p := range pairs {
		for _, cx := range ctxs {
			t.Run(fmt.Sprintf("%s->%s/%s", p.wk, p.rk, cx.label), func(t *testing.T) {
				w := avro.MustParse(cx.schema(p.wk))
				r := avro.MustParse(cx.schema(p.rk))
				res, err := resolveBoth(t, w, r)
				if err != nil {
					t.Fatalf("Resolve: %v", err)
				}
				for _, v := range p.vals {
					wire, err := w.AppendEncode(nil, cx.wrap(v))
					if err != nil {
						t.Fatalf("writer encode %v: %v", v, err)
					}
					var got any
					if _, err := res.Decode(wire, &got); err != nil {
						t.Fatalf("resolved decode %v: %v", v, err)
					}
					// Oracle: the reader's own encoding of the Go-level promotion.
					wantWire, err := r.AppendEncode(nil, cx.wrap(goPromote(p.wk, p.rk, v)))
					if err != nil {
						t.Fatalf("reader encode promoted %v: %v", v, err)
					}
					gotWire, err := r.AppendEncode(nil, got)
					if err != nil {
						t.Fatalf("re-encode promoted %v: %v", v, err)
					}
					if !bytes.Equal(gotWire, wantWire) {
						t.Fatalf("promotion %s->%s of %v (%s): wire diverges from Go-level promotion:\n got =%x\n want=%x\n value=%#v", p.wk, p.rk, v, cx.label, gotWire, wantWire, got)
					}
				}
			})
		}
	}
}

// ===========================================================================
// Layer 7 — the json.Number overflow boundary input (the "1e1000" axis value).
//
// gtypes covers the ±Inf VALUE; this covers the textual overflow INPUT that
// must narrow TO ±Inf. json.Number is a valid encode input and a bare JSON
// number is a valid decode input, so "1e1000" exercises a distinct narrow-to-Inf
// path on the float/double arms and an exact-or-reject path on the int/long
// arms. Oracle: the Go-parsed value's bits (float) or exact zigzag (integer),
// computed independently of the codec.
// ===========================================================================

func TestMatrix_GenerativeJSONNumberBoundary(t *testing.T) {
	parseF64 := func(s string) float64 { f, _ := strconv.ParseFloat(s, 64); return f }
	floatCases := []struct {
		schema string
		oracle func(s string) []byte
	}{
		{`"double"`, func(s string) []byte { return leF64(parseF64(s)) }},
		{`"float"`, func(s string) []byte { return leF32(float32(parseF64(s))) }},
	}
	floatInputs := []string{"1e1000", "-1e1000", "1.5", "0", "9007199254740993"}
	for _, fc := range floatCases {
		s := avro.MustParse(fc.schema)
		for _, in := range floatInputs {
			t.Run(fc.schema+"/"+in, func(t *testing.T) {
				// Binary encode of json.Number narrows to the IEEE bits.
				w, err := s.AppendEncode(nil, jsonNum(in))
				if err != nil {
					t.Fatalf("encode json.Number(%s): %v", in, err)
				}
				if want := fc.oracle(in); !bytes.Equal(w, want) {
					t.Fatalf("json.Number(%s) binary diverges from Go-parsed oracle:\n got=%x\nwant=%x", in, w, want)
				}
				// Bare-number JSON decode into *any narrows the same way and the
				// decoded value re-encodes onto the same wire.
				var dec any
				if err := s.DecodeJSON([]byte(in), &dec); err != nil {
					t.Fatalf("decodeJSON bare number %s: %v", in, err)
				}
				w2, err := s.AppendEncode(nil, dec)
				if err != nil || !bytes.Equal(w2, w) {
					t.Fatalf("bare-number JSON decode re-encode differs: err=%v\n got=%x\n want=%x", err, w2, w)
				}
			})
		}
	}

	// Integer arms: 2^53+1 is exact; 1e1000 (a non-integer overflow) must REJECT
	// with a bounded error, not silently truncate.
	t.Run("long/2^53+1-exact", func(t *testing.T) {
		s := avro.MustParse(`"long"`)
		w, err := s.AppendEncode(nil, jsonNum("9007199254740993"))
		if err != nil || !bytes.Equal(w, appendZig(nil, 1<<53+1)) {
			t.Fatalf("long json.Number 2^53+1 not exact: err=%v w=%x", err, w)
		}
	})
	for _, sc := range []string{`"int"`, `"long"`} {
		t.Run(sc+"/1e1000-rejects", func(t *testing.T) {
			s := avro.MustParse(sc)
			if _, err := s.AppendEncode(nil, jsonNum("1e1000")); err == nil {
				t.Fatalf("%s accepted overflow json.Number 1e1000 (must reject)", sc)
			}
		})
	}
}

// ===========================================================================
// Pointer-indirection depth × container context.
//
// The codec peels a pointer/interface chain on BOTH the encode input and the
// decode target, capped at maxIndirectDepth levels: a chain bottoming at a
// non-pointer base WITHIN the cap is accepted, one level deeper is rejected.
// That single cap must hold at EVERY context a value can sit in and across
// EVERY path — binary encode, JSON encode, and natural + identity-resolved
// decode of both wires — or a wire one path emits is a wire another path (or
// the same path's own reader) refuses: a binary↔JSON / encode↔decode
// round-trip break. The bug shape is a context-local peel that drifts from the
// cap by re-indirecting an already-peeled value: a union target indirected at
// two stages (unionTarget then the branch decode), or a container element
// unwrapped one level inline then handed to a full-budget indirect — each
// accepting up to one-or-two-times maxIndirectDepth where every leaf accepts
// exactly the cap. Such a drift is invisible to round-trip-from-typed-input
// (the input never nests past the cap) and to value/wire sweeps (depth carries
// no wire bytes); only crossing the depth axis with the context axis the peel
// is supposed to be identical across exposes it.
//
// Crosses pointer depth {0, 1, at-cap, past-cap} × context {top, record field,
// 2-branch null union, 3+-branch union, array, map} × base primitive ×
// {binary, JSON} encode × {binary, JSON} wire × {natural, identity-resolved}
// decode, asserting (a) all six paths agree on accept/reject at each depth
// AGAINST THE EXPLICIT CAP (so a "reject everything" regression is caught too,
// not merely mutual agreement), (b) an accepted value round-trips to the
// identical base on every path, and (c) pointer wrapping is wire-invariant — a
// deep input encodes to the same bytes as the bare base.
// ===========================================================================

// ptrIndirectCap mirrors the package-internal maxIndirectDepth (reflect.go):
// the deepest pointer/interface chain the codec peels. A chain bottoming at a
// non-pointer base within this many levels round-trips on every path; one level
// deeper is rejected on every path. If the internal cap changes, the past-cap
// entry in the depth list below changes with it.
const ptrIndirectCap = 5

// ptrTypeOf wraps base in depth pointer levels (depth 0 => base unchanged).
func ptrTypeOf(base reflect.Type, depth int) reflect.Type {
	for range depth {
		base = reflect.PointerTo(base)
	}
	return base
}

// ptrChain wraps sample in depth non-nil pointer levels (depth 0 => sample).
func ptrChain(sample reflect.Value, depth int) reflect.Value {
	for range depth {
		p := reflect.New(sample.Type())
		p.Elem().Set(sample)
		sample = p
	}
	return sample
}

// derefAll fully dereferences a pointer/interface chain to its base value.
func derefAll(v reflect.Value) reflect.Value {
	for v.IsValid() && (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return v
		}
		v = v.Elem()
	}
	return v
}

// ptrValEq compares a peeled base against the expected sample by type+value.
func ptrValEq(got, want reflect.Value) bool {
	return got.IsValid() && got.Type() == want.Type() && got.Interface() == want.Interface()
}

// ptrBase is one primitive base type for the pointer-indirection axis.
type ptrBase struct {
	avro   string        // the Avro schema text for this primitive
	sample reflect.Value // a representative non-zero value of the Go base type
	pad    string        // a union padding branch, token-class-distinct from avro
}

func ptrBases() []ptrBase {
	// pad is token-class-distinct from the base so a 3+-branch union dispatches
	// the sample to its OWN branch, never the pad (digit-class bases pad with a
	// string; string/boolean bases pad with a long).
	return []ptrBase{
		{`"int"`, reflect.ValueOf(int32(7)), `"string"`},
		{`"long"`, reflect.ValueOf(int64(7)), `"string"`},
		{`"float"`, reflect.ValueOf(float32(1.5)), `"string"`},
		{`"double"`, reflect.ValueOf(float64(1.5)), `"string"`},
		{`"string"`, reflect.ValueOf("v"), `"long"`},
		{`"boolean"`, reflect.ValueOf(true), `"long"`},
	}
}

// ptrCtx composes a base primitive into a context that holds it in one or more
// pointer-depth-D slots, building the encode input, a fresh decode target, and
// a round-trip checker for that context.
type ptrCtx struct {
	label  string
	schema func(b ptrBase) string
	input  func(b ptrBase, depth int) reflect.Value // context-shaped encode input
	target func(b ptrBase, depth int) reflect.Value // a *context-shaped fresh decode target
	check  func(t *testing.T, b ptrBase, target reflect.Value)
}

// ptrFieldStruct is a one-field record struct whose field is a depth-deep
// pointer chain over the base, avro-tagged to the schema field "f".
func ptrFieldStruct(b ptrBase, depth int) reflect.Type {
	return reflect.StructOf([]reflect.StructField{
		{Name: "F", Type: ptrTypeOf(b.sample.Type(), depth), Tag: `avro:"f"`},
	})
}

func ptrCtxs() []ptrCtx {
	stringType := reflect.TypeOf("")
	// top / 2-branch null union / 3+-branch union all carry the chain as the
	// value itself; only the schema (and thus the decode dispatch) differs.
	chainTop := func(b ptrBase, depth int) reflect.Value { return ptrChain(b.sample, depth) }
	newChain := func(b ptrBase, depth int) reflect.Value { return reflect.New(ptrTypeOf(b.sample.Type(), depth)) }
	checkTop := func(t *testing.T, b ptrBase, target reflect.Value) {
		t.Helper()
		if got := derefAll(target.Elem()); !ptrValEq(got, b.sample) {
			t.Fatalf("round-trip mismatch: got %v, want %v", safeIface(got), b.sample)
		}
	}
	return []ptrCtx{
		{
			label:  "top",
			schema: func(b ptrBase) string { return b.avro },
			input:  chainTop, target: newChain, check: checkTop,
		},
		{
			label:  "union-null2",
			schema: func(b ptrBase) string { return fmt.Sprintf(`["null",%s]`, b.avro) },
			input:  chainTop, target: newChain, check: checkTop,
		},
		{
			label:  "union-multi",
			schema: func(b ptrBase) string { return fmt.Sprintf(`["null",%s,%s]`, b.avro, b.pad) },
			input:  chainTop, target: newChain, check: checkTop,
		},
		{
			label: "field",
			schema: func(b ptrBase) string {
				return fmt.Sprintf(`{"type":"record","name":"PtrRec","fields":[{"name":"f","type":%s}]}`, b.avro)
			},
			input: func(b ptrBase, depth int) reflect.Value {
				v := reflect.New(ptrFieldStruct(b, depth)).Elem()
				v.Field(0).Set(ptrChain(b.sample, depth))
				return v
			},
			target: func(b ptrBase, depth int) reflect.Value { return reflect.New(ptrFieldStruct(b, depth)) },
			check: func(t *testing.T, b ptrBase, target reflect.Value) {
				t.Helper()
				if got := derefAll(target.Elem().Field(0)); !ptrValEq(got, b.sample) {
					t.Fatalf("field round-trip mismatch: got %v, want %v", safeIface(got), b.sample)
				}
			},
		},
		{
			label:  "array",
			schema: func(b ptrBase) string { return fmt.Sprintf(`{"type":"array","items":%s}`, b.avro) },
			input: func(b ptrBase, depth int) reflect.Value {
				st := reflect.SliceOf(ptrTypeOf(b.sample.Type(), depth))
				sl := reflect.MakeSlice(st, 2, 2)
				sl.Index(0).Set(ptrChain(b.sample, depth))
				sl.Index(1).Set(ptrChain(b.sample, depth))
				return sl
			},
			target: func(b ptrBase, depth int) reflect.Value {
				return reflect.New(reflect.SliceOf(ptrTypeOf(b.sample.Type(), depth)))
			},
			check: func(t *testing.T, b ptrBase, target reflect.Value) {
				t.Helper()
				sl := target.Elem()
				if sl.Len() != 2 {
					t.Fatalf("array round-trip length: got %d, want 2", sl.Len())
				}
				for i := range sl.Len() {
					if got := derefAll(sl.Index(i)); !ptrValEq(got, b.sample) {
						t.Fatalf("array[%d] round-trip mismatch: got %v, want %v", i, safeIface(got), b.sample)
					}
				}
			},
		},
		{
			label:  "map",
			schema: func(b ptrBase) string { return fmt.Sprintf(`{"type":"map","values":%s}`, b.avro) },
			input: func(b ptrBase, depth int) reflect.Value {
				mt := reflect.MapOf(stringType, ptrTypeOf(b.sample.Type(), depth))
				m := reflect.MakeMap(mt)
				m.SetMapIndex(reflect.ValueOf("k"), ptrChain(b.sample, depth))
				return m
			},
			target: func(b ptrBase, depth int) reflect.Value {
				return reflect.New(reflect.MapOf(stringType, ptrTypeOf(b.sample.Type(), depth)))
			},
			check: func(t *testing.T, b ptrBase, target reflect.Value) {
				t.Helper()
				got := derefAll(target.Elem().MapIndex(reflect.ValueOf("k")))
				if !ptrValEq(got, b.sample) {
					t.Fatalf("map[k] round-trip mismatch: got %v, want %v", safeIface(got), b.sample)
				}
			},
		},
	}
}

// safeIface renders a possibly-invalid/nil reflect.Value for an error message
// without panicking on the .Interface() of an unexported/invalid Value.
func safeIface(v reflect.Value) any {
	if !v.IsValid() {
		return "<invalid>"
	}
	if !v.CanInterface() {
		return v.String()
	}
	return v.Interface()
}

func TestMatrix_GenerativePointerIndirection(t *testing.T) {
	depths := []int{0, 1, ptrIndirectCap, ptrIndirectCap + 1} // {0, 1, at-cap, past-cap}
	for _, b := range ptrBases() {
		for _, pc := range ptrCtxs() {
			t.Run(strings.Trim(b.avro, `"`)+"/"+pc.label, func(t *testing.T) {
				s := avro.MustParse(pc.schema(b))
				res, err := avro.Resolve(s, s)
				if err != nil {
					t.Fatalf("identity Resolve: %v\nschema: %s", err, pc.schema(b))
				}
				// Canonical wires from the bare (depth-0) base in this context;
				// depth 0 is always within the cap, so both encodes must succeed.
				cbin, err := s.AppendEncode(nil, pc.input(b, 0).Interface())
				if err != nil {
					t.Fatalf("canonical binary encode: %v", err)
				}
				cjson, err := s.AppendEncodeJSON(nil, pc.input(b, 0).Interface())
				if err != nil {
					t.Fatalf("canonical JSON encode: %v", err)
				}
				for _, depth := range depths {
					t.Run(fmt.Sprintf("depth=%d", depth), func(t *testing.T) {
						accept := depth <= ptrIndirectCap

						// --- encode parity: binary and JSON agree with the cap ---
						binW, binErr := s.AppendEncode(nil, pc.input(b, depth).Interface())
						jsonW, jsonErr := s.AppendEncodeJSON(nil, pc.input(b, depth).Interface())
						if (binErr == nil) != accept {
							t.Fatalf("binary encode accept=%v, want %v (err=%v)", binErr == nil, accept, binErr)
						}
						if (jsonErr == nil) != accept {
							t.Fatalf("JSON encode accept=%v, want %v (err=%v)", jsonErr == nil, accept, jsonErr)
						}
						// An accepted deep input encodes to the SAME bytes as the
						// bare base: pointer wrapping is transparent on the wire.
						if accept {
							if !bytes.Equal(binW, cbin) {
								t.Fatalf("deep binary wire differs from bare base:\n got=%x\nwant=%x", binW, cbin)
							}
							if !bytes.Equal(jsonW, cjson) {
								t.Fatalf("deep JSON wire differs from bare base:\n got=%s\nwant=%s", jsonW, cjson)
							}
						}

						// --- decode parity: every {wire}×{natural,resolved} path
						// agrees with the cap, and an accepted value round-trips ---
						decPaths := []struct {
							name string
							dec  func(target any) error
						}{
							{"binary/natural", func(target any) error { _, e := s.Decode(cbin, target); return e }},
							{"binary/resolved", func(target any) error { _, e := res.Decode(cbin, target); return e }},
							{"json/natural", func(target any) error { return s.DecodeJSON(cjson, target) }},
							{"json/resolved", func(target any) error { return res.DecodeJSON(cjson, target) }},
						}
						for _, dp := range decPaths {
							target := pc.target(b, depth)
							err := dp.dec(target.Interface())
							if (err == nil) != accept {
								t.Fatalf("%s decode accept=%v, want %v (err=%v)", dp.name, err == nil, accept, err)
							}
							if accept {
								pc.check(t, b, target)
							}
						}
					})
				}
			})
		}
	}
}

// ===========================================================================
// Pointer-indirection depth × FIELD-OF-CONTAINER context — the unsafe struct-
// field CONTAINER fast paths.
//
// TestMatrix_GenerativePointerIndirection crosses depth × context, but its
// contexts all carry the chain at TOP LEVEL or as a SCALAR struct field, so they
// reach only the reflect serArray/serMap and the unsafe SCALAR field-pointer fast
// path — never the unsafe struct-field CONTAINER fast paths
// (tryCompileFieldSer/tryCompileFieldDeser's usArrayRecord→usArrayPtrRecord /
// udArrayPtrRecord, usNullUnionRecord / udNullUnionRecord, and
// usArrayNullUnionRecord / usArrayNullUnionPtr). Those arms fire ONLY for an
// array / null-union / array-of-null-union element INSIDE an ADDRESSABLE struct
// field. Each hand-peels exactly one pointer level inline (the element or the
// null-union optional) and delegates the remainder to a full-budget indirect
// (rec.ser / rec.deser); the recurring family bug is a MISSING multi-level-
// pointer decline at one such arm, so it accepts 1+maxIndirectDepth levels where
// the reflect element handler, the encode side, and every other context cap at
// maxIndirectDepth — emitting a wire the same struct encoded as a non-addressable
// VALUE (reflect), a top-level encode, and the wire's own reader all refuse.
//
// This net adds a RECORD base (so []*…*record / *…*record reach the record arms)
// and FIELD-OF-CONTAINER contexts that encode BOTH the addressable *struct (=>
// the unsafe container fast path) and the same struct as a non-addressable value
// (=> reflect), asserting both agree with each other, with the generic any-tree
// encode, and with the explicit cap at every depth {0,1,2,at-cap,past-cap}. A
// double-peeling arm accepts the past-cap depth on the *struct path while the
// reflect-value path rejects it: the divergence the scalar-field matrix above is
// structurally blind to.
// ===========================================================================

// ptrIndRec is the record base for the field-of-container pointer net: a minimal
// fully-unsafe-compileable struct, so a []*…*ptrIndRec / *…*ptrIndRec struct
// field reaches the unsafe record container arms (usArrayPtrRecord,
// usNullUnionRecord, usArrayNullUnionRecord, and their decode twins) until the
// element/optional pointer depth forces a decline back to reflect.
type ptrIndRec struct {
	X int32 `avro:"x"`
}

const ptrIndRecSchema = `{"type":"record","name":"PIRec","fields":[{"name":"x","type":"int"}]}`

// ptrCBase is one base type for the field-of-container pointer-depth net: its
// Avro text, Go base type, a representative typed sample, and the generic
// (any-tree) form. The generic form carries NO Go pointer wrapping, so it always
// encodes (depth-0-equivalent) and is the canonical wire every accepted typed
// depth must match and every decode reads.
type ptrCBase struct {
	label   string
	avro    string
	goType  reflect.Type
	sample  reflect.Value
	generic any
}

func ptrCBases() []ptrCBase {
	// A primitive base exercises the usArrayDirect / usNullUnionPtr /
	// usArrayNullUnionPtr arms (+ the scalar field-pointer fast path on the
	// element); a record base exercises the usArrayPtrRecord / usNullUnionRecord
	// / usArrayNullUnionRecord arms the family bug recurred in. string is a
	// second, differently-shaped primitive.
	return []ptrCBase{
		{"int", `"int"`, reflect.TypeOf(int32(0)), reflect.ValueOf(int32(7)), int32(7)},
		{"string", `"string"`, reflect.TypeOf(""), reflect.ValueOf("v"), "v"},
		{"record", ptrIndRecSchema, reflect.TypeOf(ptrIndRec{}), reflect.ValueOf(ptrIndRec{X: 7}), map[string]any{"x": int32(7)}},
	}
}

// ptrFieldCtx composes a base into an addressable struct field holding a
// container whose element / value / optional sits at pointer depth D. minDepth is
// the shallowest valid depth (0 for array/map; 1 for the null-union arms, whose
// optional IS the first pointer level).
type ptrFieldCtx struct {
	label        string
	minDepth     int
	fieldSchema  func(baseAvro string) string
	fieldType    func(baseGo reflect.Type, depth int) reflect.Type
	setField     func(field reflect.Value, baseSample reflect.Value, depth int)
	genericField func(baseGeneric any) any
	checkField   func(t *testing.T, field reflect.Value, baseSample reflect.Value)
}

func ptrFieldCtxs() []ptrFieldCtx {
	stringType := reflect.TypeOf("")
	// field-array and field-array-nullunion share the []*…*base field shape and
	// the two-element-slice input/check; only the schema and minDepth differ (the
	// null-union element's pointer IS the optional, so depth 0 is value-only).
	sliceType := func(baseGo reflect.Type, depth int) reflect.Type {
		return reflect.SliceOf(ptrTypeOf(baseGo, depth))
	}
	setSlice := func(field reflect.Value, baseSample reflect.Value, depth int) {
		sl := reflect.MakeSlice(field.Type(), 2, 2)
		sl.Index(0).Set(ptrChain(baseSample, depth))
		sl.Index(1).Set(ptrChain(baseSample, depth))
		field.Set(sl)
	}
	genSlice := func(baseGeneric any) any { return []any{baseGeneric, baseGeneric} }
	checkSlice := func(t *testing.T, field reflect.Value, baseSample reflect.Value) {
		t.Helper()
		if field.Len() != 2 {
			t.Fatalf("array field round-trip length: got %d, want 2", field.Len())
		}
		for i := range field.Len() {
			if got := derefAll(field.Index(i)); !ptrValEq(got, baseSample) {
				t.Fatalf("array field[%d] round-trip mismatch: got %v, want %v", i, safeIface(got), safeIface(baseSample))
			}
		}
	}
	return []ptrFieldCtx{
		{
			label:        "field-array",
			minDepth:     0,
			fieldSchema:  func(b string) string { return fmt.Sprintf(`{"type":"array","items":%s}`, b) },
			fieldType:    sliceType,
			setField:     setSlice,
			genericField: genSlice,
			checkField:   checkSlice,
		},
		{
			label:        "field-array-nullunion",
			minDepth:     1,
			fieldSchema:  func(b string) string { return fmt.Sprintf(`{"type":"array","items":["null",%s]}`, b) },
			fieldType:    sliceType,
			setField:     setSlice,
			genericField: genSlice,
			checkField:   checkSlice,
		},
		{
			label:        "field-nullunion",
			minDepth:     1,
			fieldSchema:  func(b string) string { return fmt.Sprintf(`["null",%s]`, b) },
			fieldType:    func(baseGo reflect.Type, depth int) reflect.Type { return ptrTypeOf(baseGo, depth) },
			setField:     func(field reflect.Value, baseSample reflect.Value, depth int) { field.Set(ptrChain(baseSample, depth)) },
			genericField: func(baseGeneric any) any { return baseGeneric },
			checkField: func(t *testing.T, field reflect.Value, baseSample reflect.Value) {
				t.Helper()
				if got := derefAll(field); !ptrValEq(got, baseSample) {
					t.Fatalf("null-union field round-trip mismatch: got %v, want %v", safeIface(got), safeIface(baseSample))
				}
			},
		},
		{
			label:       "field-map",
			minDepth:    0,
			fieldSchema: func(b string) string { return fmt.Sprintf(`{"type":"map","values":%s}`, b) },
			fieldType: func(baseGo reflect.Type, depth int) reflect.Type {
				return reflect.MapOf(stringType, ptrTypeOf(baseGo, depth))
			},
			setField: func(field reflect.Value, baseSample reflect.Value, depth int) {
				m := reflect.MakeMap(field.Type())
				m.SetMapIndex(reflect.ValueOf("k"), ptrChain(baseSample, depth))
				field.Set(m)
			},
			genericField: func(baseGeneric any) any { return map[string]any{"k": baseGeneric} },
			checkField: func(t *testing.T, field reflect.Value, baseSample reflect.Value) {
				t.Helper()
				if got := derefAll(field.MapIndex(reflect.ValueOf("k"))); !ptrValEq(got, baseSample) {
					t.Fatalf("map field[k] round-trip mismatch: got %v, want %v", safeIface(got), safeIface(baseSample))
				}
			},
		},
	}
}

func TestMatrix_GenerativePointerIndirectionUnsafeContainers(t *testing.T) {
	depths := []int{0, 1, 2, ptrIndirectCap, ptrIndirectCap + 1} // {0,1,2,at-cap,past-cap}
	for _, b := range ptrCBases() {
		for _, cx := range ptrFieldCtxs() {
			t.Run(b.label+"/"+cx.label, func(t *testing.T) {
				recSchema := fmt.Sprintf(`{"type":"record","name":"PtrCOuter","fields":[{"name":"f","type":%s}]}`, cx.fieldSchema(b.avro))
				s := avro.MustParse(recSchema)
				res, err := avro.Resolve(s, s)
				if err != nil {
					t.Fatalf("identity Resolve: %v\nschema: %s", err, recSchema)
				}
				// Canonical wires from the generic any-tree input: it carries no Go
				// pointer wrapping, so it always encodes (depth-0-equivalent) and is
				// both the wire every accepted typed depth must match and the wire
				// every decode reads — valid even at the reject depths (whose typed
				// encode fails, so they cannot supply their own wire).
				genVal := map[string]any{"f": cx.genericField(b.generic)}
				cbin, err := s.AppendEncode(nil, genVal)
				if err != nil {
					t.Fatalf("canonical binary encode: %v\nschema: %s", err, recSchema)
				}
				cjson, err := s.AppendEncodeJSON(nil, genVal)
				if err != nil {
					t.Fatalf("canonical JSON encode: %v\nschema: %s", err, recSchema)
				}
				for _, depth := range depths {
					if depth < cx.minDepth {
						continue
					}
					t.Run(fmt.Sprintf("depth=%d", depth), func(t *testing.T) {
						accept := depth <= ptrIndirectCap
						st := reflect.StructOf([]reflect.StructField{
							{Name: "F", Type: cx.fieldType(b.goType, depth), Tag: `avro:"f"`},
						})
						ps := reflect.New(st)
						cx.setField(ps.Elem().Field(0), b.sample, depth)

						// Encode parity: the addressable *struct (=> unsafe struct-field
						// container fast path) and the same struct as a non-addressable
						// VALUE (=> reflect) must agree with each other, with the generic
						// wire, and with the explicit cap. A double-peeling arm accepts
						// the past-cap depth on the *struct path alone.
						for _, fm := range []struct {
							name string
							in   any
						}{
							{"unsafe(*struct)", ps.Interface()},
							{"reflect(struct-value)", ps.Elem().Interface()},
						} {
							binW, binErr := s.AppendEncode(nil, fm.in)
							jsonW, jsonErr := s.AppendEncodeJSON(nil, fm.in)
							if (binErr == nil) != accept {
								t.Fatalf("%s binary encode accept=%v, want %v (err=%v)", fm.name, binErr == nil, accept, binErr)
							}
							if (jsonErr == nil) != accept {
								t.Fatalf("%s JSON encode accept=%v, want %v (err=%v)", fm.name, jsonErr == nil, accept, jsonErr)
							}
							if accept {
								if !bytes.Equal(binW, cbin) {
									t.Fatalf("%s binary wire != generic canonical (pointer wrapping not transparent):\n got=%x\nwant=%x", fm.name, binW, cbin)
								}
								if !bytes.Equal(jsonW, cjson) {
									t.Fatalf("%s JSON wire != generic canonical:\n got=%s\nwant=%s", fm.name, jsonW, cjson)
								}
							}
						}

						// Decode parity: each {wire}×{natural,resolved} path agrees with
						// the cap, decoding into a fresh depth-D typed *struct (=> the
						// unsafe container deser arm); an accepted value round-trips.
						for _, dp := range []struct {
							name string
							dec  func(target any) error
						}{
							{"binary/natural", func(target any) error { _, e := s.Decode(cbin, target); return e }},
							{"binary/resolved", func(target any) error { _, e := res.Decode(cbin, target); return e }},
							{"json/natural", func(target any) error { return s.DecodeJSON(cjson, target) }},
							{"json/resolved", func(target any) error { return res.DecodeJSON(cjson, target) }},
						} {
							target := reflect.New(st)
							err := dp.dec(target.Interface())
							if (err == nil) != accept {
								t.Fatalf("%s decode accept=%v, want %v (err=%v)", dp.name, err == nil, accept, err)
							}
							if accept {
								cx.checkField(t, target.Elem().Field(0), b.sample)
							}
						}
					})
				}
			})
		}
	}
}

// ===========================================================================
// Null-union nil-equivalence parity net (field-of-container).
//
// The 2-branch ["null",T] / [T,"null"] optimization picks the null branch
// exactly when isNilValue reports the value nil — which peels pointer/interface
// layers then nil-checks the bottom kind, so a non-nil pointer to a nil
// slice/map/interface/pointer is null. Three encode paths must agree on the
// branch for the SAME value: the unsafe struct fast path (reached only when the
// struct is addressable, Encode(&v)), the reflect path (Encode(v)), and JSON
// (EncodeJSON). This net crosses nil-equivalent base kind × container context ×
// union position and asserts all three pick the same branch. The unsafe fast
// path makes its nil decision on the outer pointer alone, so it must DECLINE
// every isNilableKind inner to the reflect path; this net is what proves it.
// ===========================================================================

// nilEqThreeWayParity asserts that addr (addressable -> unsafe fast path) and
// val (by value -> reflect path) encode to byte-identical binary, that the two
// JSON encodings agree, and that the binary and JSON wires decode to the same
// value (cross-format branch agreement). target1/target2 are fresh decode
// destinations of the value's concrete type.
func nilEqThreeWayParity(t *testing.T, schemaJSON string, addr, val, target1, target2 any) {
	t.Helper()
	s := avro.MustParse(schemaJSON)

	wAddr, err := s.AppendEncode(nil, addr)
	if err != nil {
		t.Fatalf("Encode(&v) [unsafe]: %v", err)
	}
	wVal, err := s.AppendEncode(nil, val)
	if err != nil {
		t.Fatalf("Encode(v) [reflect]: %v", err)
	}
	if !bytes.Equal(wAddr, wVal) {
		t.Errorf("binary addressable-vs-value branch divergence: Encode(&v)=% x  Encode(v)=% x", wAddr, wVal)
	}

	jAddr, err := s.AppendEncodeJSON(nil, addr)
	if err != nil {
		t.Fatalf("EncodeJSON(&v): %v", err)
	}
	jVal, err := s.AppendEncodeJSON(nil, val)
	if err != nil {
		t.Fatalf("EncodeJSON(v): %v", err)
	}
	if !bytes.Equal(jAddr, jVal) {
		t.Errorf("JSON addressable-vs-value branch divergence: %s vs %s", jAddr, jVal)
	}

	if _, err := s.Decode(wAddr, target1); err != nil {
		t.Fatalf("Decode(binary wire % x): %v", wAddr, err)
	}
	if err := s.DecodeJSON(jAddr, target2); err != nil {
		t.Fatalf("DecodeJSON(%s): %v", jAddr, err)
	}
	if !reflect.DeepEqual(target1, target2) {
		t.Errorf("binary<->JSON branch divergence: binary=%#v  json=%#v  (binWire=% x jsonWire=%s)", target1, target2, wAddr, jAddr)
	}
}

func TestMatrix_NullUnionNilEquivalenceParity(t *testing.T) {
	recField := func(inner string) string {
		return `{"type":"record","name":"R","fields":[{"name":"f","type":` + inner + `}]}`
	}
	recArr := func(items string) string {
		return `{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":` + items + `}}]}`
	}
	recMapVal := func(values string) string {
		return `{"type":"record","name":"R","fields":[{"name":"m","type":{"type":"map","values":` + values + `}}]}`
	}
	nf := func(x string) string { return `["null",` + x + `]` }
	ns := func(x string) string { return `[` + x + `,"null"]` }

	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		// ----- FIELD context: *Inner, slice base, both positions -----
		{"field/slice/null-first/nil", func(t *testing.T) {
			var x []string
			nilEqThreeWayParity(t, recField(nf(`{"type":"array","items":"string"}`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		{"field/slice/null-second/nil", func(t *testing.T) {
			var x []string
			nilEqThreeWayParity(t, recField(ns(`{"type":"array","items":"string"}`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		{"field/slice/null-first/nonnil-control", func(t *testing.T) {
			x := []string{"a", "b"}
			nilEqThreeWayParity(t, recField(nf(`{"type":"array","items":"string"}`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		// ----- FIELD context: bytes base (also a Slice inner) -----
		{"field/bytes/null-first/nil", func(t *testing.T) {
			type rec struct {
				F *[]byte `avro:"f"`
			}
			var x []byte
			nilEqThreeWayParity(t, recField(nf(`"bytes"`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		{"field/bytes/null-first/nonnil-control", func(t *testing.T) {
			type rec struct {
				F *[]byte `avro:"f"`
			}
			x := []byte{1, 2, 3}
			nilEqThreeWayParity(t, recField(nf(`"bytes"`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		// ----- FIELD context: map base -----
		{"field/map/null-first/nil", func(t *testing.T) {
			type rec struct {
				F *map[string]string `avro:"f"`
			}
			var x map[string]string
			nilEqThreeWayParity(t, recField(nf(`{"type":"map","values":"string"}`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		{"field/map/null-first/nonnil-control", func(t *testing.T) {
			type rec struct {
				F *map[string]string `avro:"f"`
			}
			x := map[string]string{"k": "v"}
			nilEqThreeWayParity(t, recField(nf(`{"type":"map","values":"string"}`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		// ----- FIELD context: **T (pointer inner), both positions -----
		{"field/ptrptr/null-first/nil", func(t *testing.T) {
			type rec struct {
				F **int `avro:"f"`
			}
			var x *int
			nilEqThreeWayParity(t, recField(nf(`"int"`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		{"field/ptrptr/null-second/nil", func(t *testing.T) {
			type rec struct {
				F **int `avro:"f"`
			}
			var x *int
			nilEqThreeWayParity(t, recField(ns(`"int"`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		{"field/ptrptr/null-first/nonnil-control", func(t *testing.T) {
			type rec struct {
				F **int `avro:"f"`
			}
			n := 7
			x := &n
			nilEqThreeWayParity(t, recField(nf(`"int"`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		// ----- FIELD context: deep chain ***int -----
		{"field/deep-ptr/null-first/nil", func(t *testing.T) {
			type rec struct {
				F ***int `avro:"f"`
			}
			var x **int
			nilEqThreeWayParity(t, recField(nf(`"int"`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		// ----- FIELD context: interface inner (*any) -----
		{"field/iface/null-first/nil", func(t *testing.T) {
			type rec struct {
				F *any `avro:"f"`
			}
			var x any
			nilEqThreeWayParity(t, recField(nf(`"int"`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		// ----- ARRAY-ELEMENT context: []*Inner -----
		{"array-elem/slice/null-first/nil", func(t *testing.T) {
			type rec struct {
				A []*[]string `avro:"a"`
			}
			var x []string
			nilEqThreeWayParity(t, recArr(nf(`{"type":"array","items":"string"}`)), &rec{A: []*[]string{&x}}, rec{A: []*[]string{&x}}, &rec{}, &rec{})
		}},
		{"array-elem/slice/null-second/nil", func(t *testing.T) {
			type rec struct {
				A []*[]string `avro:"a"`
			}
			var x []string
			nilEqThreeWayParity(t, recArr(ns(`{"type":"array","items":"string"}`)), &rec{A: []*[]string{&x}}, rec{A: []*[]string{&x}}, &rec{}, &rec{})
		}},
		{"array-elem/bytes/null-first/nil", func(t *testing.T) {
			type rec struct {
				A []*[]byte `avro:"a"`
			}
			var x []byte
			nilEqThreeWayParity(t, recArr(nf(`"bytes"`)), &rec{A: []*[]byte{&x}}, rec{A: []*[]byte{&x}}, &rec{}, &rec{})
		}},
		{"array-elem/ptrptr/null-first/nil", func(t *testing.T) {
			type rec struct {
				A []**int `avro:"a"`
			}
			var x *int
			nilEqThreeWayParity(t, recArr(nf(`"int"`)), &rec{A: []**int{&x}}, rec{A: []**int{&x}}, &rec{}, &rec{})
		}},
		{"array-elem/slice/null-first/nonnil-control", func(t *testing.T) {
			type rec struct {
				A []*[]string `avro:"a"`
			}
			x := []string{"z"}
			nilEqThreeWayParity(t, recArr(nf(`{"type":"array","items":"string"}`)), &rec{A: []*[]string{&x}}, rec{A: []*[]string{&x}}, &rec{}, &rec{})
		}},
		// ----- MAP-VALUE context: map[string]*Inner (declines to reflect both) -----
		{"map-value/slice/null-first/nil", func(t *testing.T) {
			type rec struct {
				M map[string]*[]string `avro:"m"`
			}
			var x []string
			nilEqThreeWayParity(t, recMapVal(nf(`{"type":"array","items":"string"}`)), &rec{M: map[string]*[]string{"k": &x}}, rec{M: map[string]*[]string{"k": &x}}, &rec{}, &rec{})
		}},
		// ----- NESTED context: nullunion field inside a nested record -----
		{"nested-record-field/slice/null-first/nil", func(t *testing.T) {
			type inner struct {
				F *[]string `avro:"f"`
			}
			type outer struct {
				M inner `avro:"m"`
			}
			var x []string
			sch := `{"type":"record","name":"O","fields":[{"name":"m","type":{"type":"record","name":"M","fields":[{"name":"f","type":["null",{"type":"array","items":"string"}]}]}}]}`
			nilEqThreeWayParity(t, sch, &outer{M: inner{F: &x}}, outer{M: inner{F: &x}}, &outer{}, &outer{})
		}},
	}

	for _, c := range cases {
		t.Run(c.name, c.run)
	}
}

// ---------- custom_resurrection_parity_test.go ----------

// The CustomType-resurrected-logical parity battery — the WHOLE matrix in one
// test, so this class can't dribble a cell at a time.
//
// Background. A logical type placed on an Avro underlying it is not spec-valid
// for (uuid on bytes, duration on a size!=12 fixed, a date/timestamp on string,
// big-decimal on fixed, …) is SOFT-DROPPED by validateLogical, leaving the bare
// underlying — matching Java/fastavro/hamba. A registered CustomType whose
// LogicalType matches RESURRECTS the dropped logical (schema.go buildComplex).
// The contract: a resurrected wrong-kind / wrong-size logical must fall through
// to the RAW size/kind-checked path on EVERY axis, applying logicalUnderlyingAccept
// (validateLogical's own predicate) uniformly. Prior rounds closed this one cell
// per round — JSON typed decode (25a6e66), binary primitive encode (8236008),
// fixed-encode wrong size (ff86592). This battery drives every cell at once:
//
//	  logical × {wrong-kind, wrong-size} × {encode, decode}
//	         × {binary, JSON} × {natural, resolved deser}
//	         × {decode-into-any, decode-into-logical-typed-target}
//	         × {wildcard custom, AvroType-match custom, AvroType-mismatch custom}
//
// Oracle (independent of the code under test): the PLAIN schema — the same
// schema string parsed with NO CustomType — which soft-drops the logical to its
// bare underlying. Invariant: for EVERY resurrecting CustomType shape, the
// custom schema must be byte/value/accept-identical to the plain schema on every
// axis, and every wire it emits must round-trip through its own (natural AND
// resolved) reader. A logical serializer or deserializer wrongly applied to the
// wrong kind/size shows up as a wire-byte, accept, or value divergence from the
// plain schema, or a wire its own reader can't read.
//
// The three resurrecting custom shapes matter independently:
//   - wildcard  ({LogicalType}):        resurrects AND suppresses (any kind).
//   - AvroType-match ({LogicalType,kind}): resurrects AND suppresses (kind ==).
//   - AvroType-mismatch ({LogicalType,"boolean"}): resurrects (LogicalType-keyed)
//     but does NOT suppress (kind !=) — the decode-side gap, where the bare
//     hasMatchingCustomType suppression doesn't fire but logicalUnderlyingAccept
//     must still keep the codec raw.
//
// Non-vacuity: each cell feeds BOTH a raw underlying value AND a "canary" value
// the wrongly-applied logical serializer would accept (a UUID string, an
// avro.Duration, a time.Time, a *big.Rat). A neutered gate (logical ser/deser
// applied kind/size-blind) makes the custom schema diverge from the plain one on
// the canary (encode) or on the typed target (decode) — verified by reverting any
// single fix in schema.go / json_decode.go.

func TestCensus_CustomResurrectedLogicalFullMatrixParity(t *testing.T) {
	for _, c := range resurrectionCells() {
		t.Run(c.name, func(t *testing.T) {
			runResurrectionCell(t, c)
		})
	}
}

// resurrectionCell is one (logical, underlying) placement the logical is NOT
// spec-valid for — a soft-droppable cell a CustomType can resurrect.
type resurrectionCell struct {
	name    string
	logical string
	kind    string // bytes/int/long/string/fixed
	size    int    // fixed size; 0 otherwise
	schema  string
	// inputs is raw underlying values plus logical-shaped canaries. The plain
	// (soft-dropped) schema is the oracle for how each is encoded/rejected.
	inputs []any
	// targets are the logical-typed decode targets (besides *any) the wrongly-
	// applied logical deserializer would transform into. Each returns a fresh
	// pointer target.
	targets []func() any
}

func resurrectionCells() []resurrectionCell {
	// validOn enumerates the spec-valid placements per logical; every OTHER
	// placement in the probe set below soft-drops and is therefore a cell.
	// decimal hard-errors on a fixed underlying without precision (handled
	// inline by validateLogical, never soft-dropped→resurrected), so it is
	// probed on the non-bytes/fixed primitives only.
	type lspec struct {
		name    string
		validOn func(kind string, size int) bool
		probe   []struct {
			kind string
			size int
		}
		inputs  []any
		targets []func() any
	}

	tm := time.Date(2023, 11, 14, 22, 13, 20, 0, time.UTC)
	uuidStr := "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	uuid16 := [16]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}
	dur := avro.Duration{Months: 1, Days: 2, Milliseconds: 3}
	rat := big.NewRat(12345, 100)

	timeTarget := func() any { return new(time.Time) }
	durTarget := func() any { return new(time.Duration) }
	ratTarget := func() any { return new(big.Rat) }
	strTarget := func() any { return new(string) }
	u16Target := func() any { return new([16]byte) }
	avroDurTarget := func() any { return new(avro.Duration) }

	// The full probe grid of underlyings. Each logical takes the subset that
	// soft-drops (i.e. !validOn), which is exactly the wrong-kind + wrong-size
	// matrix.
	grid := []struct {
		kind string
		size int
	}{
		{"int", 0}, {"long", 0}, {"string", 0}, {"bytes", 0},
		{"fixed", 8}, {"fixed", 12}, {"fixed", 16}, {"fixed", 20},
	}

	timeInputs := []any{tm}
	timeTargets := []func() any{timeTarget}
	durInputsLong := []func() any{timeTarget, durTarget} // time-millis/micros also map a duration

	specs := []lspec{
		{
			name:    "uuid",
			validOn: func(k string, s int) bool { return k == "string" || (k == "fixed" && s == 16) },
			inputs:  []any{uuidStr, uuid16},
			targets: []func() any{strTarget, u16Target},
		},
		{
			name:    "duration",
			validOn: func(k string, s int) bool { return k == "fixed" && s == 12 },
			inputs:  []any{dur},
			targets: []func() any{avroDurTarget},
		},
		{
			name:    "big-decimal",
			validOn: func(k string, s int) bool { return k == "bytes" },
			inputs:  []any{rat},
			targets: []func() any{ratTarget},
		},
		{
			// decimal soft-drops only off bytes/fixed; on fixed without
			// precision it hard-errors, so it is never resurrected there.
			name:    "decimal",
			validOn: func(k string, s int) bool { return k == "bytes" || k == "fixed" },
			inputs:  []any{rat},
			targets: []func() any{ratTarget},
		},
		{"date", func(k string, s int) bool { return k == "int" }, nil, timeInputs, timeTargets},
		{"time-millis", func(k string, s int) bool { return k == "int" }, nil, timeInputs, durInputsLong},
		{"time-micros", func(k string, s int) bool { return k == "long" }, nil, timeInputs, durInputsLong},
		{"timestamp-millis", func(k string, s int) bool { return k == "long" }, nil, timeInputs, timeTargets},
		{"timestamp-micros", func(k string, s int) bool { return k == "long" }, nil, timeInputs, timeTargets},
		{"timestamp-nanos", func(k string, s int) bool { return k == "long" }, nil, timeInputs, timeTargets},
		{"local-timestamp-millis", func(k string, s int) bool { return k == "long" }, nil, timeInputs, timeTargets},
		{"local-timestamp-micros", func(k string, s int) bool { return k == "long" }, nil, timeInputs, timeTargets},
		{"local-timestamp-nanos", func(k string, s int) bool { return k == "long" }, nil, timeInputs, timeTargets},
	}

	var cells []resurrectionCell
	for _, sp := range specs {
		probe := sp.probe
		if probe == nil {
			probe = grid
		}
		for _, g := range probe {
			if sp.validOn(g.kind, g.size) {
				continue // spec-valid placement; not a resurrection cell
			}
			// decimal on a fixed underlying without precision hard-errors at
			// Parse — never a soft-drop. Skip (it is not resurrectable).
			if sp.name == "decimal" && g.kind == "fixed" {
				continue
			}
			cells = append(cells, makeCell(sp.name, g.kind, g.size, sp.inputs, sp.targets))
		}
	}
	return cells
}

func makeCell(logical, kind string, size int, inputs []any, targets []func() any) resurrectionCell {
	name := fmt.Sprintf("%s_on_%s", logical, kind)
	var schema string
	if kind == "fixed" {
		name = fmt.Sprintf("%s_on_fixed%d", logical, size)
		nm := "F_" + strings.ReplaceAll(logical, "-", "_") + fmt.Sprintf("_%d", size)
		schema = fmt.Sprintf(`{"type":"fixed","name":%q,"size":%d,"logicalType":%q}`, nm, size, logical)
	} else {
		schema = fmt.Sprintf(`{"type":%q,"logicalType":%q}`, kind, logical)
	}
	// Add a raw underlying value the BARE type accepts, so every cell encodes at
	// least one input through the plain (soft-dropped) path.
	raw := rawUnderlyingValue(kind, size)
	return resurrectionCell{
		name:    name,
		logical: logical,
		kind:    kind,
		size:    size,
		schema:  schema,
		inputs:  append([]any{raw}, inputs...),
		targets: targets,
	}
}

func rawUnderlyingValue(kind string, size int) any {
	switch kind {
	case "int":
		return int32(7)
	case "long":
		return int64(7)
	case "string":
		return "raw-underlying-text"
	case "bytes":
		return []byte{1, 2, 3, 4, 5, 6, 7, 8}
	case "fixed":
		b := make([]byte, size)
		for i := range b {
			b[i] = byte(i + 1)
		}
		return b
	}
	panic("unknown kind " + kind)
}

func runResurrectionCell(t *testing.T, c resurrectionCell) {
	plain := avro.MustParse(c.schema)
	plainR := mustIdentityResolve(t, plain)

	// All three resurrecting CustomType shapes must reduce to the plain schema.
	shapes := []struct {
		name string
		opt  avro.SchemaOpt
	}{
		{"wildcard", avro.CustomType{LogicalType: c.logical}},
		{"avrotype-match", avro.CustomType{LogicalType: c.logical, AvroType: c.kind}},
		{"avrotype-mismatch", avro.CustomType{LogicalType: c.logical, AvroType: "boolean"}},
	}

	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			cs := avro.MustParse(c.schema, sh.opt)
			csR := mustIdentityResolve(t, cs)

			// Targets to decode into: *any plus every logical-typed target.
			targets := append([]func() any{func() any { return new(any) }}, c.targets...)

			for _, in := range c.inputs {
				// --- ENCODE parity: byte-identical wire (or identical reject). ---
				pbin, peb := plain.Encode(in)
				cbin, ceb := cs.Encode(in)
				if got, want := encResult(cbin, ceb), encResult(pbin, peb); got != want {
					t.Errorf("binary encode %T: custom=%s plain=%s — logical serializer applied to wrong kind/size", in, got, want)
				}
				pjsn, pej := plain.EncodeJSON(in)
				cjsn, cej := cs.EncodeJSON(in)
				if got, want := encResult(cjsn, cej), encResult(pjsn, pej); got != want {
					t.Errorf("JSON encode %T: custom=%q plain=%q — logical serializer applied to wrong kind/size", in, got, want)
				}

				// --- DECODE parity over the schema's own wire, into every
				// target, on binary+JSON x natural+resolved. ---
				if peb == nil && ceb == nil {
					for ti, mk := range targets {
						if got, want := decBin(cs, cbin, mk), decBin(plain, pbin, mk); got != want {
							t.Errorf("binary decode natural %T target#%d: custom=%s plain=%s — logical deser applied to wrong kind/size", in, ti, got, want)
						}
						if got, want := decBin(csR, cbin, mk), decBin(plainR, pbin, mk); got != want {
							t.Errorf("binary decode RESOLVED %T target#%d: custom=%s plain=%s — resolved deser diverged from plain", in, ti, got, want)
						}
					}
					// Self-readability: cs's own binary wire reads back via cs.
					assertSelfReadableBin(t, cs, csR, cbin, in)
				}
				if pej == nil && cej == nil {
					for ti, mk := range targets {
						if got, want := decJSON(cs, cjsn, mk), decJSON(plain, pjsn, mk); got != want {
							t.Errorf("JSON decode natural %T target#%d: custom=%s plain=%s — logical deser applied to wrong kind/size", in, ti, got, want)
						}
						if got, want := decJSON(csR, cjsn, mk), decJSON(plainR, pjsn, mk); got != want {
							t.Errorf("JSON decode RESOLVED %T target#%d: custom=%s plain=%s — resolved deser diverged from plain", in, ti, got, want)
						}
					}
					assertSelfReadableJSON(t, cs, csR, cjsn, in)
				}
			}

			// --- RESOLVED via TYPE PROMOTION. When the reader kind is a
			// promotion target (writer int→long, string→bytes, bytes→string),
			// doResolve wraps the widening deser with promotionDeserForLogical to
			// re-apply the reader's logical. A resurrected wrong-kind logical
			// must NOT be re-applied there either: the promoted decode must equal
			// the plain (soft-dropped) reader's promoted decode. This is the
			// resolved-deser axis the identity resolve above does not reach (it
			// hits the promotion branch, not maybeWrapResolvedNode). ---
			if src, ok := promotionSourceFor(c.kind); ok {
				w := avro.MustParse(src.schema)
				wire, werr := w.Encode(src.value)
				plainProm, e1 := avro.Resolve(w, plain)
				csProm, e2 := avro.Resolve(w, cs)
				if werr == nil && e1 == nil && e2 == nil {
					targets := append([]func() any{func() any { return new(any) }}, c.targets...)
					for ti, mk := range targets {
						if got, want := decBin(csProm, wire, mk), decBin(plainProm, wire, mk); got != want {
							t.Errorf("binary decode RESOLVED-PROMOTION %s->%s target#%d: custom=%s plain=%s — promotion re-applied a wrong-kind logical", src.kind, c.kind, ti, got, want)
						}
						if got, want := decJSON(csProm, encodeJSONOrNil(w, src.value), mk), decJSON(plainProm, encodeJSONOrNil(w, src.value), mk); got != want {
							t.Errorf("JSON decode RESOLVED-PROMOTION %s->%s target#%d: custom=%s plain=%s — promotion re-applied a wrong-kind logical", src.kind, c.kind, ti, got, want)
						}
					}
				}
			}
		})
	}
}

// promotionSourceFor returns the bare writer type that PROMOTES to readerKind,
// per the Avro promotion set (int→long, string→bytes, bytes→string), and a
// value to encode through it. Returns ok=false for reader kinds nothing promotes
// into (int, fixed). float/double readers carry no logical, so are skipped.
func promotionSourceFor(readerKind string) (struct {
	kind, schema string
	value        any
}, bool) {
	switch readerKind {
	case "long":
		return struct {
			kind, schema string
			value        any
		}{"int", `"int"`, int32(5)}, true
	case "bytes":
		return struct {
			kind, schema string
			value        any
		}{"string", `"string"`, "promo-text"}, true
	case "string":
		return struct {
			kind, schema string
			value        any
		}{"bytes", `"bytes"`, []byte{9, 8, 7}}, true
	}
	return struct {
		kind, schema string
		value        any
	}{}, false
}

// encodeJSONOrNil returns nil rather than failing, so a rejected encode
// compares equal across the two sides of a parity cell.
func encodeJSONOrNil(s *avro.Schema, v any) []byte {
	b, err := s.EncodeJSON(v)
	if err != nil {
		return nil
	}
	return b
}

func mustIdentityResolve(t *testing.T, s *avro.Schema) *avro.Schema {
	t.Helper()
	r, err := avro.Resolve(s, s)
	if err != nil {
		t.Fatalf("identity Resolve failed: %v", err)
	}
	return r
}

// encResult renders an encode outcome for comparison: "<rejected>" or the hex
// wire. Comparing renders asserts identical accept AND identical bytes.
func encResult(b []byte, err error) string {
	if err != nil {
		return "<rejected>"
	}
	return fmt.Sprintf("%x", b)
}

// decResult renders a decode outcome: "<rejected>" or the %#v of the decoded
// value. Error MESSAGES are not compared (plain and custom may word a reject
// differently); accept/reject and decoded value are.
func decResult(target any, err error) string {
	if err != nil {
		return "<rejected>"
	}
	return fmt.Sprintf("%#v", reflect.ValueOf(target).Elem().Interface())
}

func decBin(s *avro.Schema, wire []byte, mk func() any) string {
	tgt := mk()
	_, err := s.Decode(wire, tgt)
	return decResult(tgt, err)
}

func decJSON(s *avro.Schema, wire []byte, mk func() any) string {
	tgt := mk()
	err := s.DecodeJSON(wire, tgt)
	return decResult(tgt, err)
}

func assertSelfReadableBin(t *testing.T, cs, csR *avro.Schema, wire []byte, in any) {
	t.Helper()
	var v any
	if _, err := cs.Decode(wire, &v); err != nil {
		t.Errorf("custom binary wire (input %T) not self-readable: %v", in, err)
	}
	var v2 any
	if _, err := csR.Decode(wire, &v2); err != nil {
		t.Errorf("custom binary wire (input %T) not self-readable via RESOLVED reader: %v", in, err)
	}
}

func assertSelfReadableJSON(t *testing.T, cs, csR *avro.Schema, wire []byte, in any) {
	t.Helper()
	var v any
	if err := cs.DecodeJSON(wire, &v); err != nil {
		t.Errorf("custom JSON wire (input %T) not self-readable: %v", in, err)
	}
	var v2 any
	if err := csR.DecodeJSON(wire, &v2); err != nil {
		t.Errorf("custom JSON wire (input %T) not self-readable via RESOLVED reader: %v", in, err)
	}
}

// ---------- matrix_acceptance_test.go ----------

// ---------------------------------------------------------------------------
// Schema-ACCEPTANCE parity: the bug class that keeps producing interop
// regressions is not wire bytes but which schemas parse at all (size-0
// fixed, empty enums, empty unions were all acceptance divergences). This
// axis takes every composed matrix schema, derives structurally-broken
// mutants whose rejection is spec-required and reference-verified (each
// mutator class was checked against Java's parser source and conformance
// behavior), and asserts the originals parse everywhere and the mutants
// reject in twmb and Java (the cisuite twin asserts the full set) —
// fastavro validates only a subset at parse, and the executed
// fastavroLaxMutants calibration below witnesses which mutant classes
// it accepts.
//
// Mutators deliberately avoid the documented-divergence territory (quoted
// size leniency, logical-type soft-drop vs hard-reject of bad decimal
// params, alias grammar, forward references): those are policy entries,
// not parity targets.
// ---------------------------------------------------------------------------

type schemaMutant struct {
	label  string
	schema string
}

// mutateOnce decodes the schema JSON, applies fn to the first applicable
// node (walking objects and arrays), and re-encodes. Returns "" when no
// node was applicable.
func mutateOnce(schemaJSON string, fn func(obj map[string]any) bool) string {
	var tree any
	if err := json.Unmarshal([]byte(schemaJSON), &tree); err != nil {
		return ""
	}
	applied := false
	var walk func(n any)
	walk = func(n any) {
		if applied {
			return
		}
		switch v := n.(type) {
		case map[string]any:
			if fn(v) {
				applied = true
				return
			}
			for _, k := range []string{"type", "items", "values"} {
				if c, ok := v[k]; ok {
					walk(c)
				}
			}
			if fs, ok := v["fields"].([]any); ok {
				for _, f := range fs {
					walk(f)
				}
			}
		case []any:
			for _, b := range v {
				walk(b)
			}
		}
	}
	walk(tree)
	if !applied {
		return ""
	}
	out, err := json.Marshal(tree)
	if err != nil {
		return ""
	}
	return string(out)
}

// schemaMutants derives the reference-verified reject set for one schema.
func schemaMutants(schemaJSON string) []schemaMutant {
	var out []schemaMutant
	add := func(label, s string) {
		if s != "" && s != schemaJSON {
			out = append(out, schemaMutant{label, s})
		}
	}
	isType := func(obj map[string]any, t string) bool {
		s, _ := obj["type"].(string)
		return s == t
	}

	add("fixed-missing-size", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "fixed") {
			delete(o, "size")
			return true
		}
		return false
	}))
	add("fixed-negative-size", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "fixed") {
			o["size"] = -1
			return true
		}
		return false
	}))
	add("enum-missing-symbols", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "enum") {
			delete(o, "symbols")
			return true
		}
		return false
	}))
	add("enum-duplicate-symbol", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "enum") {
			if syms, ok := o["symbols"].([]any); ok && len(syms) > 0 {
				o["symbols"] = append(syms, syms[0])
				return true
			}
		}
		return false
	}))
	add("enum-default-not-member", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "enum") {
			if syms, ok := o["symbols"].([]any); ok && len(syms) > 0 {
				o["default"] = "__not_a_symbol__"
				return true
			}
		}
		return false
	}))
	add("named-missing-name", mutateOnce(schemaJSON, func(o map[string]any) bool {
		switch {
		case isType(o, "record"), isType(o, "enum"), isType(o, "fixed"):
			if _, ok := o["name"]; ok {
				delete(o, "name")
				return true
			}
		}
		return false
	}))
	add("record-missing-fields", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "record") {
			delete(o, "fields")
			return true
		}
		return false
	}))
	add("record-duplicate-field", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "record") {
			if fs, ok := o["fields"].([]any); ok && len(fs) > 0 {
				o["fields"] = append(fs, fs[0])
				return true
			}
		}
		return false
	}))
	add("record-empty-field-name", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "record") {
			if fs, ok := o["fields"].([]any); ok && len(fs) > 0 {
				if f0, ok := fs[0].(map[string]any); ok {
					f0["name"] = ""
					return true
				}
			}
		}
		return false
	}))
	add("array-missing-items", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "array") {
			delete(o, "items")
			return true
		}
		return false
	}))
	add("map-missing-values", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if isType(o, "map") {
			delete(o, "values")
			return true
		}
		return false
	}))
	add("missing-type-key", mutateOnce(schemaJSON, func(o map[string]any) bool {
		if _, ok := o["type"]; ok {
			delete(o, "type")
			return true
		}
		return false
	}))

	// Union mutants operate on the whole tree (the union is an array, not
	// an object the walker's fn sees).
	var tree any
	if json.Unmarshal([]byte(schemaJSON), &tree) == nil {
		if arr, ok := tree.([]any); ok && len(arr) > 0 {
			dup := append(append([]any{}, arr...), arr[0])
			if b, err := json.Marshal(dup); err == nil {
				out = append(out, schemaMutant{"union-duplicate-branch", string(b)})
			}
			nested := append([]any{}, arr...)
			nested[0] = []any{arr[0]}
			if b, err := json.Marshal(nested); err == nil {
				out = append(out, schemaMutant{"union-nested-union", string(b)})
			}
		}
	}
	return out
}

// acceptanceCells samples the composed schemas the acceptance axis sweeps:
// every fragment × three structural contexts.
func acceptanceCells() []string {
	var cells []string
	for _, fr := range matFrags() {
		for _, cx := range matCtxs() {
			switch cx.label {
			case "top", "field", "array":
			default:
				continue
			}
			if cx.skip != nil && cx.skip(fr.kind) {
				continue
			}
			u := &uniq{}
			cells = append(cells, cx.schema(fr.schema(u), fr.kind, u))
		}
	}
	return cells
}

// TestMatrix_AcceptanceMutantsRejectLocally: every mutant must fail twmb's
// Parse (the local half of the parity; the oracle halves assert fastavro
// and Java agree).
func TestMatrix_AcceptanceMutantsRejectLocally(t *testing.T) {
	for _, cell := range acceptanceCells() {
		if _, err := avro.Parse(cell); err != nil {
			t.Fatalf("unmutated cell must parse: %v\n%s", err, cell)
		}
		for _, m := range schemaMutants(cell) {
			if _, err := avro.Parse(m.schema); err == nil {
				t.Errorf("mutant %s unexpectedly parsed:\n%s", m.label, m.schema)
			}
		}
	}
}

// fastavroLaxMutants are mutator classes fastavro's parser does NOT
// validate per se (it defers them to read time or skips them entirely):
// missing/duplicate/empty-named record fields and negative fixed sizes
// parse there IN THEIR PLAIN FORM. The laxness is class-level, not
// uniform — a specific mutant cell can still reject when the mutation
// collaterally trips an orthogonal fastavro validation (a duplicated
// field whose type DEFINES a named type re-defines that name; a
// negative-size fixed carrying a decimal fails the precision-capacity
// check) — so the differential below requires an executed ACCEPT
// WITNESS per lax class rather than skipping or asserting uniformly: a
// fastavro upgrade that starts validating a class wholesale drops its
// witness count to zero and flips the calibration loudly. Java enforces
// every class — the cisuite twin (TestDifferentialJavaAcceptance)
// asserts the full set; the fastavro differential asserts reject only
// for what fastavro enforces.
var fastavroLaxMutants = map[string]bool{
	"record-missing-fields":   true,
	"record-duplicate-field":  true,
	"record-empty-field-name": true,
	"fixed-negative-size":     true,
}

// TestDifferentialAcceptance: fastavro must agree on every cell (accept)
// and every mutant it validates (reject); each documented-lax mutant
// class must produce at least one observed fastavro ACCEPT across the
// sweep (the executed fastavroLaxMutants calibration). Skips without the
// oracle python.
func TestDifferentialAcceptance(t *testing.T) {
	o := startOracle(t)
	laxSeen := map[string]int{}
	laxAccepted := map[string]int{}
	for _, cell := range acceptanceCells() {
		resp := o.call(oracleJob{Op: "parse", Schema: json.RawMessage(cell)})
		if !resp.OK {
			t.Fatalf("fastavro rejected a schema twmb accepts: %s\n%s", resp.Err, cell)
		}
		for _, m := range schemaMutants(cell) {
			resp := o.call(oracleJob{Op: "parse", Schema: json.RawMessage(m.schema)})
			if fastavroLaxMutants[m.label] {
				laxSeen[m.label]++
				if resp.OK {
					laxAccepted[m.label]++
				}
				continue
			}
			if resp.OK {
				t.Errorf("fastavro accepted mutant %s that twmb rejects:\n%s", m.label, m.schema)
			}
		}
	}
	for label := range fastavroLaxMutants {
		if laxSeen[label] > 0 && laxAccepted[label] == 0 {
			t.Errorf("fastavro now REJECTS every %s mutant (%d cells) — its parser started validating this class; recalibrate fastavroLaxMutants",
				label, laxSeen[label])
		}
	}
}

// ---------- matrix_cache_test.go ----------

// TestMatrix_CacheSelfContainedNamespaces is a generative cross-product over
// SchemaCache cross-parse references: a named type (record / enum / fixed)
// whose namespace is established four different ways (null, explicit, inherited
// from an enclosing record, dotted fullname) is referenced from a schema in
// three relative namespaces (null, same, different) at four positions (record
// field, array items, map values, union branch). For every cell the
// cache-referenced schema's canonical form (and thus its Rabin fingerprint)
// must be byte-identical to the logically-identical fully-inline schema parsed
// without a cache (the independent oracle: that path is the Java-validated PCF
// emitter). The cache canonical must also re-parse, and the inner type must
// resolve to the EXPECTED fullname — an oracle-independent check that catches a
// definition silently re-namespaced to the reference site's scope.
//
// The cache stores each definition's JSON for splicing at the first reference;
// a definition that inherited its namespace (no explicit "namespace") would,
// without normalization, re-inherit the enclosing namespace wherever it is
// spliced and resolve to the wrong fullname. Neutering that normalization fails
// 36 of these 144 cells (every inherited/null definition-namespace × non-equal
// reference scope, across all kinds and positions).
func TestMatrix_CacheSelfContainedNamespaces(t *testing.T) {
	type kind struct {
		name string
		def  func(name, nsAttr string) string
	}
	kinds := []kind{
		{"record", func(n, ns string) string {
			return fmt.Sprintf(`{"type":"record","name":%q%s,"fields":[{"name":"x","type":"int"}]}`, n, ns)
		}},
		{"enum", func(n, ns string) string {
			return fmt.Sprintf(`{"type":"enum","name":%q%s,"symbols":["A","B"]}`, n, ns)
		}},
		{"fixed", func(n, ns string) string {
			return fmt.Sprintf(`{"type":"fixed","name":%q%s,"size":4}`, n, ns)
		}},
	}
	short := func(fn string) string {
		if i := strings.LastIndex(fn, "."); i >= 0 {
			return fn[i+1:]
		}
		return fn
	}
	nsAttr := func(ns string) string { // natural form: omit attr for the null namespace
		if ns == "" {
			return ""
		}
		return fmt.Sprintf(`,"namespace":%q`, ns)
	}
	posWrap := func(pos, typeJSON string) string {
		switch pos {
		case "array":
			return `{"type":"array","items":` + typeJSON + `}`
		case "map":
			return `{"type":"map","values":` + typeJSON + `}`
		case "union":
			return `["null",` + typeJSON + `]`
		}
		return typeJSON // field
	}

	defNSs := []string{"null", "explicit", "inherited", "dotted"}
	refNSs := []string{"null", "same", "diff"}
	poss := []string{"field", "array", "map", "union"}

	for _, k := range kinds {
		for _, dns := range defNSs {
			for _, rns := range refNSs {
				for _, pos := range poss {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", k.name, dns, rns, pos), func(t *testing.T) {
						// Resolve the definition into (fullname, resolved namespace,
						// registration schemas).
						var fullname, defns string
						var regs []string
						switch dns {
						case "null":
							fullname, defns = "T", ""
							regs = []string{k.def("T", "")}
						case "explicit":
							fullname, defns = "a.b.T", "a.b"
							regs = []string{k.def("T", `,"namespace":"a.b"`)}
						case "inherited":
							fullname, defns = "a.b.T", "a.b"
							regs = []string{fmt.Sprintf(`{"type":"record","name":"Wrap","namespace":"a.b","fields":[{"name":"w","type":%s}]}`, k.def("T", ""))}
						case "dotted":
							fullname, defns = "a.b.T", "a.b"
							regs = []string{k.def("a.b.T", "")}
						}
						// Self-contained oracle definition: ALWAYS an explicit
						// namespace (incl. "" to force null inside a namespaced scope).
						selfDef := k.def(short(fullname), fmt.Sprintf(`,"namespace":%q`, defns))

						refNSval := map[string]string{"null": "", "same": defns, "diff": "z.z"}[rns]
						refSchema := fmt.Sprintf(`{"type":"record","name":"Ref"%s,"fields":[{"name":"f","type":%s}]}`,
							nsAttr(refNSval), posWrap(pos, fmt.Sprintf("%q", fullname)))
						inlineSchema := fmt.Sprintf(`{"type":"record","name":"Ref"%s,"fields":[{"name":"f","type":%s}]}`,
							nsAttr(refNSval), posWrap(pos, selfDef))

						var c avro.SchemaCache
						for _, r := range regs {
							if _, err := c.Parse(r); err != nil {
								t.Fatalf("register %q: %v", r, err)
							}
						}
						viaCache, err := c.Parse(refSchema)
						if err != nil {
							t.Fatalf("cache parse %q: %v", refSchema, err)
						}
						inline, err := avro.Parse(inlineSchema)
						if err != nil {
							t.Fatalf("inline parse %q: %v", inlineSchema, err)
						}

						cc, ic := string(viaCache.Canonical()), string(inline.Canonical())
						if cc != ic {
							t.Errorf("canonical diverges:\n cache : %s\n inline: %s", cc, ic)
						}
						if string(viaCache.Fingerprint(avro.NewRabin())) != string(inline.Fingerprint(avro.NewRabin())) {
							t.Errorf("fingerprint diverges")
						}
						if _, err := avro.Parse(cc); err != nil {
							t.Errorf("cache canonical not self-contained: %v\n  %s", err, cc)
						}
						// Oracle-independent: the inner type resolves to the expected
						// fullname and did NOT absorb the reference site's namespace.
						if !strings.Contains(cc, fmt.Sprintf(`"name":%q`, fullname)) {
							t.Errorf("inner type not at expected fullname %q:\n  %s", fullname, cc)
						}
						if rns == "diff" && refNSval != "" &&
							strings.Contains(cc, fmt.Sprintf(`"name":"%s.%s"`, refNSval, short(fullname))) {
							t.Errorf("inner type re-namespaced to reference site %q:\n  %s", refNSval, cc)
						}
					})
				}
			}
		}
	}
}

// ---------- matrix_cache_multiparse_test.go ----------

// This file is the generative MULTI-PARSE SchemaCache net (Family 7:
// 807c6d9 → 7cab9bd → de3dca3 → 254eee0). A schema built via a SchemaCache that
// references named types defined in PRIOR Parse calls resolves those references
// in the node tree (Encode/Decode work), but the JSON-derived metadata forms
// (Fingerprint / Canonical / Root / String) used to keep a dangling bare
// reference — non-self-contained, fingerprint-divergent, SOE/registry-interop-
// broken. The four-commit dribble fixed it one shape at a time: splice the
// inherited def in (807c6d9), preserve doc/order/props (7cab9bd), keep the
// spliced def namespace-stable (de3dca3), and dedupe overlapping/diamond defs
// (254eee0).
//
// The existing regressions pin those shapes as POINT cases, and
// TestMatrix_CacheSelfContainedNamespaces crosses a SINGLE cross-parse
// reference against namespace/position/kind. This net is the missing
// CROSS-PRODUCT over the reference-graph TOPOLOGY — single, transitive chains,
// diamonds, wide overlap, diamond-with-a-chain-arm, a nested type referenced
// before its container, and a single type referencing the same leaf TWICE
// (repeat2 / repeat_chain) — each crossed with the namespace regime
// (null / single-namespace / split-namespace), the position the shared leaf
// sits in (record field / array items / map values / union branch), the
// leaf kind (record / enum / fixed), AND the cross-parse reference SPELLING
// (bare "X" vs wrapped {"type":"X"}). Crossing topology with the other axes is
// where a gap in the dribble would hide; a point test per topology cannot
// reach it.
//
// The SPELLING axis is the one a later fix (collapse a non-splicing wrapped
// reference to bare) exposed: a cross-parse reference is accepted both bare and
// wrapped (NOT_BUGS #23), so the self-contained metadata must be identical for
// either. The bug shape it catches has two layers — (1) a wrapped cross-parse
// reference that splices must be replaced as a WHOLE (else it self-contains as
// the invalid {"type":{X-def}} and the rebuild silently falls back to a dangling
// reference); (2) a LATER wrapped occurrence of a type whose first occurrence was
// inlined must COLLAPSE to bare "X", else {"type":"X"} survives in String() where
// the canonical bare form belongs. Layer 2 is invisible to every single-reference
// topology and is exactly what repeat2 / repeat_chain / local_forwardref add.
//
// For every shape the cache-built schema is compared to a logically-identical
// inline twin emitted by an INDEPENDENT first-occurrence oracle (mpEmitTwin —
// DFS pre-order, full at first occurrence, bare fullname after, exactly Java's
// NamedSchema.writeNameRef rule). The twin is spelling-INDEPENDENT, so it anchors
// both spellings to one canonical form. The wire bytes for a sample value are the
// oracle-independent anchor: equal wire proves the two schemas ARE the same
// logical schema (the node tree resolved identically), so ANY divergence in
// Fingerprint / Canonical / Root / String is then provably a metadata-form bug,
// not a different schema. Non-vacuity is verified by neutering each fix in
// cache.go and observing the failures (see the test's closing comment).

// mpNode is one named type in an abstract multi-parse reference graph.
type mpNode struct {
	full  string   // fullname ("D" in the null namespace, else "ns.D")
	kind  string   // "record" | "enum" | "fixed"
	edges []mpEdge // record children, in field order (records only)
}

// mpEdge is a reference from a record to a named child, sitting in some
// position (a plain field, or inside an array / map / union).
type mpEdge struct {
	to    string // child fullname
	field string // field name carrying the reference
	pos   string // "field" | "array" | "map" | "union"
}

// mpGraph is a reference DAG plus its root and any types defined NESTED inside
// another type's parse (rather than registered standalone).
type mpGraph struct {
	nodes  map[string]*mpNode
	root   string
	nested map[string]string // child fullname → container it is defined inside
}

func mpShort(full string) string {
	if i := strings.LastIndex(full, "."); i >= 0 {
		return full[i+1:]
	}
	return full
}

func mpNS(full string) string {
	if i := strings.LastIndex(full, "."); i >= 0 {
		return full[:i]
	}
	return ""
}

func mpFull(ns, short string) string {
	if ns == "" {
		return short
	}
	return ns + "." + short
}

// mpPosWrap places x (a type definition object or a bare fullname-reference
// string) into the chosen reference position.
func mpPosWrap(pos string, x any) any {
	switch pos {
	case "array":
		return map[string]any{"type": "array", "items": x}
	case "map":
		return map[string]any{"type": "map", "values": x}
	case "union":
		return []any{"null", x}
	default: // field
		return x
	}
}

// mpNamedObj builds the JSON object for a named type. Every named object
// carries an EXPLICIT namespace (including "" — the null-namespace escape), so
// the spelling is identical to what the cache stores via
// defWithExplicitNamespace and the normalized String() forms compare
// byte-for-byte regardless of enclosing scope. childType supplies each record
// field's type (a nested def for a first occurrence, a bare fullname after).
//
// Every type and field also carries doc / a custom prop, and every field an
// "order" — the non-canonical attributes commit 7cab9bd preserves through the
// splice. They are stripped from Canonical (so it still matches) but must
// survive in String()/Root(); a splice that rebuilt from the attribute-poor
// node tree would drop them and fail this net.
func mpNamedObj(n *mpNode, childType func(e mpEdge) any) map[string]any {
	o := map[string]any{
		"type": n.kind, "name": mpShort(n.full), "namespace": mpNS(n.full),
		"doc": mpShort(n.full) + " doc", "io.tag": mpShort(n.full) + "-tag",
	}
	switch n.kind {
	case "record":
		if len(n.edges) == 0 {
			o["fields"] = []any{mpField("n", "int")}
		} else {
			fields := make([]any, len(n.edges))
			for i, e := range n.edges {
				fields[i] = mpField(e.field, childType(e))
			}
			o["fields"] = fields
		}
	case "enum":
		o["symbols"] = []any{"A", "B"}
	case "fixed":
		o["size"] = 2
	}
	return o
}

// mpField builds a record field object carrying doc / order / a custom prop —
// field-level attributes that live on the field, not the type, and must survive
// the splice (which rewrites only the field's "type" value).
func mpField(name string, typ any) map[string]any {
	return map[string]any{
		"name": name, "type": typ,
		"doc": name + " fdoc", "order": "ignore", "io.fprop": name + "-fp",
	}
}

// mpEmitTwin is the independent oracle: the single self-contained inline schema
// logically identical to the cache-built one. It walks the reference graph from
// the root in DFS pre-order, emitting each named type's full definition at its
// FIRST occurrence and a bare fullname reference at every later one — the
// canonical first-occurrence form (Java's writeNameRef; the cache's splice
// reaches the same shape by deduping). The oracle never consults the cache
// machinery, so a cache↔twin divergence is a real metadata-form bug.
func mpEmitTwin(g *mpGraph) string {
	seen := map[string]bool{}
	var emit func(full string) any
	emit = func(full string) any {
		if seen[full] {
			return full // already defined earlier: a bare fullname reference
		}
		seen[full] = true
		return mpNamedObj(g.nodes[full], func(e mpEdge) any { return mpPosWrap(e.pos, emit(e.to)) })
	}
	b, err := json.Marshal(emit(g.root))
	if err != nil {
		panic(err)
	}
	return string(b)
}

// mpRefSpell renders a cross-parse name reference in the chosen SPELLING — the
// axis the topology cross had missed. Avro accepts a name reference written
// two ways (NOT_BUGS #23): the bare fullname string "X", and the wrapped form
// {"type":"X"} whose sole key is "type". Both resolve to the same node, so the
// wire is identical; the splice that self-contains a cache schema must reach the
// same metadata for either spelling. The bug surface is the wrapped form: a
// wrapped cross-parse reference once hit the splice's general map path (recursing
// INTO the "type" value → invalid {"type":{X-def}}), so String()/Canonical()
// silently fell back to a dangling reference while the bare form self-contained.
func mpRefSpell(spelling, ref string) any {
	if spelling == "wrapped" {
		return map[string]any{"type": ref}
	}
	return ref
}

// mpEmitStandalone emits one type's standalone schema string for a cache Parse.
// References to other (already-registered) types are rendered in the given
// spelling (bare fullname or wrapped {"type":...}); a child marked
// nested-in-this-type is emitted inline (it is registered by THIS parse).
func mpEmitStandalone(full string, g *mpGraph, spelling string) string {
	n := g.nodes[full]
	tree := mpNamedObj(n, func(e mpEdge) any {
		if g.nested[e.to] == full {
			child := mpNamedObj(g.nodes[e.to], func(ce mpEdge) any { return mpPosWrap(ce.pos, mpRefSpell(spelling, ce.to)) })
			return mpPosWrap(e.pos, child)
		}
		return mpPosWrap(e.pos, mpRefSpell(spelling, e.to))
	})
	b, err := json.Marshal(tree)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// mpSampleValue builds a value the schema accepts, for the wire-equality anchor.
// Union positions take the non-null branch so the referenced type is exercised.
func mpSampleValue(full string, g *mpGraph) any {
	n := g.nodes[full]
	switch n.kind {
	case "enum":
		return "A"
	case "fixed":
		return make([]byte, 2)
	default: // record
		m := map[string]any{}
		if len(n.edges) == 0 {
			m["n"] = int32(0)
			return m
		}
		for _, e := range n.edges {
			cv := mpSampleValue(e.to, g)
			switch e.pos {
			case "array":
				m[e.field] = []any{cv}
			case "map":
				m[e.field] = map[string]any{"k": cv}
			default: // field, union (non-null branch)
				m[e.field] = cv
			}
		}
		return m
	}
}

// mpNormJSON re-marshals a JSON string into a canonical key-sorted form, so two
// schema strings that differ only in key order / whitespace compare equal.
// UseNumber keeps numeric literals verbatim, matching the cache's
// precision-preserving rebuild.
func mpNormJSON(t *testing.T, s string) string {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("normalize %q: %v", s, err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("normalize marshal: %v", err)
	}
	return string(b)
}

// mpBuildTopo constructs one reference-graph topology in the given namespace
// regime, with the shared leaf of the given kind, and the position axis applied
// to the single edge that points AT the shared leaf (so the leaf is exercised
// in a field / array / map / union). It returns the graph and the standalone
// parse order (dependencies first); ok is false for an unsupported combination.
func mpBuildTopo(name, regime, leafKind, pos string) (g *mpGraph, parseOrder []string, ok bool) {
	carrierNS := map[string]string{"null": "", "ns": "ns", "mixed": "a"}[regime]
	leafNS := map[string]string{"null": "", "ns": "ns", "mixed": "b"}[regime]

	mk := func(root string, nodes ...*mpNode) *mpGraph {
		g := &mpGraph{nodes: map[string]*mpNode{}, root: root, nested: map[string]string{}}
		for _, n := range nodes {
			g.nodes[n.full] = n
		}
		return g
	}
	rec := func(full string, edges ...mpEdge) *mpNode { return &mpNode{full: full, kind: "record", edges: edges} }
	leaf := func(full string) *mpNode { return &mpNode{full: full, kind: leafKind} }
	e := func(to, field, p string) mpEdge { return mpEdge{to: to, field: field, pos: p} }

	R := mpFull(carrierNS, "R")
	D := mpFull(leafNS, "D")

	switch name {
	case "single":
		return mk(R, rec(R, e(D, "d", pos)), leaf(D)), []string{D}, true

	case "chain2":
		B := mpFull(carrierNS, "B")
		return mk(R, rec(R, e(B, "b", "field")), rec(B, e(D, "d", pos)), leaf(D)),
			[]string{D, B}, true

	case "chain3":
		B, C := mpFull(carrierNS, "B"), mpFull(carrierNS, "C")
		return mk(R, rec(R, e(B, "b", "field")), rec(B, e(C, "c", "field")), rec(C, e(D, "d", pos)), leaf(D)),
			[]string{D, C, B}, true

	case "diamond":
		B, C := mpFull(carrierNS, "B"), mpFull(carrierNS, "C")
		return mk(R, rec(R, e(B, "b", "field"), e(C, "c", "field")),
				rec(B, e(D, "d", pos)), rec(C, e(D, "d", "field")), leaf(D)),
			[]string{D, B, C}, true

	case "wide3":
		B, C, E := mpFull(carrierNS, "B"), mpFull(carrierNS, "C"), mpFull(carrierNS, "E")
		return mk(R, rec(R, e(B, "b", "field"), e(C, "c", "field"), e(E, "e", "field")),
				rec(B, e(D, "d", pos)), rec(C, e(D, "d", "field")), rec(E, e(D, "d", "field")), leaf(D)),
			[]string{D, B, C, E}, true

	case "diamond_chain":
		// R→{B,C}; B→M→D, C→D. D is shared at depth 2 (via M) and depth 1 (via C).
		B, C, M := mpFull(carrierNS, "B"), mpFull(carrierNS, "C"), mpFull(carrierNS, "M")
		return mk(R, rec(R, e(B, "b", "field"), e(C, "c", "field")),
				rec(B, e(M, "m", "field")), rec(M, e(D, "d", pos)), rec(C, e(D, "d", "field")), leaf(D)),
			[]string{D, M, C, B}, true

	case "nested_before":
		// Outer is defined standalone with a NESTED Inner; R references Inner
		// (first, in `pos`) then Outer. Inner's def thus arrives twice — via the
		// standalone reference and inside Outer — so the splice must dedupe it.
		Inner := mpFull(leafNS, "Inner")
		Outer := mpFull(carrierNS, "Outer")
		g := mk(R, rec(R, e(Inner, "f1", pos), e(Outer, "f2", "field")),
			rec(Outer, e(Inner, "inner", "field")), leaf(Inner))
		g.nested[Inner] = Outer
		return g, []string{Outer}, true

	case "repeat2":
		// R references the SAME cached leaf D twice: d0 at a plain field (the
		// first occurrence, which the splice inlines as a full definition) and
		// d1 at `pos` (a LATER occurrence the splice must leave as a bare
		// reference). Under the wrapped spelling d1 is {"type":"D"} and reaches
		// the splice's no-splice fall-through (already-inlined) — exactly the
		// path the single-reference topologies never exercise. The wrapper must
		// collapse to bare "D" there, else String() diverges from the bare-
		// spelled / inline twin (whose later occurrence is bare "D").
		return mk(R, rec(R, e(D, "d0", "field"), e(D, "d1", pos)), leaf(D)), []string{D}, true

	case "repeat_chain":
		// The same repeated reference one level down: a cross-parse carrier B
		// references D twice (b0 field, b1 at `pos`), and R references B. B's
		// stored self-contained definition therefore already carries a later
		// reference to D; the wrapper on that stored reference must be collapsed
		// when B itself is self-contained AND it must survive re-splicing into R
		// as a bare reference, not a re-wrapped one.
		B := mpFull(carrierNS, "B")
		return mk(R, rec(R, e(B, "b", "field")), rec(B, e(D, "b0", "field"), e(D, "b1", pos)), leaf(D)),
			[]string{D, B}, true
	}
	return nil, nil, false
}

// mpRootEqual deep-compares two Root() SchemaNode trees. Root() resolves
// namespaces and preserves every attribute (doc/order/props/aliases/defaults),
// so equal trees mean the cache and inline String()/Root() forms describe the
// same schema down to every attribute — independent of bare-vs-dotted or
// inherited-vs-explicit spelling.
func mpRootEqual(a, b avro.SchemaNode) bool { return reflect.DeepEqual(a, b) }

// mpRunCache parses deps into a fresh SchemaCache (in order) and returns the
// schema for the final root parse.
func mpRunCache(t *testing.T, deps []string, root string) *avro.Schema {
	t.Helper()
	var c avro.SchemaCache
	for _, d := range deps {
		if _, err := c.Parse(d); err != nil {
			t.Fatalf("register %q: %v", d, err)
		}
	}
	s, err := c.Parse(root)
	if err != nil {
		t.Fatalf("cache parse root %q: %v", root, err)
	}
	return s
}

// mpAssertSelfContained is the four-form differential: a cache-built schema must
// match its logically-identical inline twin on Fingerprint / Canonical / Root /
// String, and every metadata form must re-parse standalone. The wire bytes (and
// a decode round-trip) are the oracle-independent anchor: equal wire proves the
// two are the same logical schema, so any metadata divergence is a real bug.
//
// canonReparses says whether Parse(Canonical()) is expected to succeed.
// Canonical (PCF) drops namespace attributes and writes fullnames, so a null-
// namespace type nested in a namespaced scope re-reads as inheriting that scope
// — an intentionally non-re-parseable, fingerprint-faithful form (NOT_BUGS #25,
// Java emits byte-identical ambiguity). The four forms still match the twin and
// the fingerprint is still correct; only standalone re-parse of the canonical
// is given up. String() keeps the explicit "namespace":"" escape, so it always
// re-parses.
func mpAssertSelfContained(t *testing.T, viaCache, inline *avro.Schema, val any, cacheSchema, twinSchema string, canonReparses bool) {
	t.Helper()

	wc, err := viaCache.AppendEncode(nil, val)
	if err != nil {
		t.Fatalf("cache encode: %v\n schema=%s", err, cacheSchema)
	}
	wi, err := inline.AppendEncode(nil, val)
	if err != nil {
		t.Fatalf("inline encode: %v\n twin=%s", err, twinSchema)
	}
	if !bytes.Equal(wc, wi) {
		t.Fatalf("twin is not logically identical (wire differs):\n cache =%x\n inline=%x\n cacheSchema=%s\n twin=%s", wc, wi, cacheSchema, twinSchema)
	}
	var ac, ai any
	if _, err := viaCache.Decode(wc, &ac); err != nil {
		t.Fatalf("cache decode: %v", err)
	}
	if _, err := inline.Decode(wi, &ai); err != nil {
		t.Fatalf("inline decode: %v", err)
	}
	if !matEqual(ac, ai) {
		t.Errorf("decoded values differ:\n c=%#v\n i=%#v", ac, ai)
	}

	// Canonical + Fingerprint (the cross-language / SOE interop forms).
	if cc, ic := string(viaCache.Canonical()), string(inline.Canonical()); cc != ic {
		t.Errorf("Canonical diverges:\n cache : %s\n inline: %s", cc, ic)
	}
	if !bytes.Equal(viaCache.Fingerprint(avro.NewRabin()), inline.Fingerprint(avro.NewRabin())) {
		t.Errorf("Fingerprint diverges (SOE/registry interop break)")
	}

	// String: normalized byte-equality (every preserved attribute, every named
	// type at its first occurrence).
	if cs, is := mpNormJSON(t, viaCache.String()), mpNormJSON(t, inline.String()); cs != is {
		t.Errorf("String diverges:\n cache : %s\n inline: %s", cs, is)
	}

	// Root: attribute-complete, namespace-resolved structural form.
	if !mpRootEqual(*viaCache.Root(), *inline.Root()) {
		t.Errorf("Root diverges:\n cache : %+v\n inline: %+v", viaCache.Root(), inline.Root())
	}

	// Self-containment: every metadata form re-parses standalone (except a
	// canonical with PCF-lossy null-ns-in-namespaced nesting — see canonReparses).
	if canonReparses {
		if _, err := avro.Parse(string(viaCache.Canonical())); err != nil {
			t.Errorf("Parse(cache.Canonical()) FAILS — not self-contained: %v\n %s", err, viaCache.Canonical())
		}
	}
	if _, err := avro.Parse(viaCache.String()); err != nil {
		t.Errorf("Parse(cache.String()) FAILS — not self-contained: %v\n %s", err, viaCache.String())
	}
	croot := viaCache.Root()
	if _, err := croot.Schema(); err != nil {
		t.Errorf("cache.Root().Schema() FAILS to rebuild: %v", err)
	}
}

// mpJSON marshals a generic tree to a compact JSON string.
func mpJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// mpRecObj builds an attributed record object with the given field objects.
func mpRecObj(full string, fields ...any) map[string]any {
	return map[string]any{
		"type": "record", "name": mpShort(full), "namespace": mpNS(full),
		"doc": mpShort(full) + " doc", "io.tag": mpShort(full) + "-tag",
		"fields": fields,
	}
}

// mpFwdObj builds an attributed named def for the forward-ref family: a record
// with nf int fields (v0..), or an enum / fixed. Two same-short-name types are
// made wire-distinguishable by their field count, so a mis-bound reference
// shows up in the wire-equality anchor.
func mpFwdObj(full, kind string, nf int) map[string]any {
	o := map[string]any{
		"type": kind, "name": mpShort(full), "namespace": mpNS(full),
		"doc": mpShort(full) + " doc", "io.tag": mpShort(full) + "-tag",
	}
	switch kind {
	case "record":
		fs := make([]any, nf)
		for i := range nf {
			fs[i] = mpField(fmt.Sprintf("v%d", i), "int")
		}
		o["fields"] = fs
	case "enum":
		o["symbols"] = []any{"A", "B"}
	case "fixed":
		o["size"] = 2
	}
	return o
}

// mpFwdVal builds a value for an mpFwdObj of the given kind / field count.
func mpFwdVal(kind string, nf int) any {
	switch kind {
	case "enum":
		return "A"
	case "fixed":
		return make([]byte, 2)
	default:
		m := map[string]any{}
		for i := range nf {
			m[fmt.Sprintf("v%d", i)] = int32(0)
		}
		return m
	}
}

// mpEmitInheritedWrapper registers leafFull NESTED inside a wrapper record whose
// namespace the leaf INHERITS — the leaf definition itself carries no explicit
// "namespace". This is the de3dca3 surface: such a definition, stored for later
// cross-parse splicing, must keep its resolved (inherited) fullname when spliced
// into a DIFFERENTLY-namespaced reference site, not re-inherit that site's
// namespace. (mpBuildTopo's "mixed" regime puts the leaf in namespace "b" and
// the carriers/root in "a", so the splice site's namespace differs.)
func mpEmitInheritedWrapper(leafFull, kind string) string {
	leafDef := mpNamedObj(&mpNode{full: leafFull, kind: kind}, func(mpEdge) any { return "int" })
	delete(leafDef, "namespace") // drop the explicit namespace → inherit the wrapper's
	wrap := map[string]any{
		"type": "record", "name": "Wrap", "namespace": mpNS(leafFull),
		"doc": "Wrap doc", "io.tag": "Wrap-tag",
		"fields": []any{map[string]any{
			"name": "w", "type": leafDef,
			"doc": "w fdoc", "order": "ignore", "io.fprop": "w-fp",
		}},
	}
	return mpJSON(wrap)
}

func TestMatrix_SchemaCacheMultiParseSelfContained(t *testing.T) {
	topos := []string{"single", "chain2", "chain3", "diamond", "wide3", "diamond_chain", "nested_before", "repeat2", "repeat_chain"}
	regimes := []string{"null", "ns", "mixed"}
	positions := []string{"field", "array", "map", "union"}
	kinds := []string{"record", "enum", "fixed"}
	// The spelling axis the net had missed. A cross-parse reference is written
	// either as the bare fullname "X" or the wrapped {"type":"X"} (both accepted,
	// NOT_BUGS #23); the self-contained metadata must be identical for either,
	// since the wire is. The twin is spelling-INDEPENDENT (always the canonical
	// first-occurrence inline form), so it anchors both spellings: bare is the
	// control that already self-contained, wrapped is the form whose splice was
	// the bug. The repeat2 / repeat_chain topologies make the wrapped column
	// non-vacuous — a LATER wrapped occurrence of an inlined type must collapse
	// to bare in String(), not survive as {"type":"X"}.
	spellings := []string{"bare", "wrapped"}

	var cells int

	// --- core net: spelling × topology × namespace regime × position × leaf kind ---
	for _, spelling := range spellings {
		for _, topo := range topos {
			for _, regime := range regimes {
				for _, pos := range positions {
					for _, kind := range kinds {
						g, parseOrder, ok := mpBuildTopo(topo, regime, kind, pos)
						if !ok {
							continue
						}
						name := fmt.Sprintf("%s/%s/%s/%s/%s", spelling, topo, regime, pos, kind)
						t.Run(name, func(t *testing.T) {
							cells++
							deps := make([]string, len(parseOrder))
							for i, fn := range parseOrder {
								deps[i] = mpEmitStandalone(fn, g, spelling)
							}
							rootSchema := mpEmitStandalone(g.root, g, spelling)
							viaCache := mpRunCache(t, deps, rootSchema)

							twinSchema := mpEmitTwin(g)
							inline, err := avro.Parse(twinSchema)
							if err != nil {
								t.Fatalf("inline twin parse %q: %v", twinSchema, err)
							}
							// Core net never nests a null-ns type in a namespaced scope
							// (a regime is all-null or all-namespaced), so the canonical
							// always re-parses.
							mpAssertSelfContained(t, viaCache, inline, mpSampleValue(g.root, g), rootSchema, twinSchema, true)
						})
					}
				}
			}
		}
	}

	// --- forward-ref family: positional binding of a reference relative to a
	// LOCAL definition, including the namespace-shadow corner ---
	//
	// inlineTreeDefs binds a reference the way the parser does: eager, in-scope-
	// first, and POSITIONAL — a local definition wins only for references AFTER
	// it; a reference BEFORE it binds the cache-inherited type. The cases below
	// cross the reference/def order with a distinct-name vs same-short-name-
	// across-namespaces collision; each expressible case carries an inline twin
	// for the four-form differential.
	type fwd struct {
		name           string
		deps           []string
		root           string
		twin           string // "" → inexpressible corner: binding-safe assertion only
		value          any
		canonNoReparse bool // canonical is PCF-lossy (null-ns nested in a namespaced scope)
	}
	var cases []fwd

	// The cached cross-parse reference is crossed with the spelling axis: it
	// splices in place (the local def is a distinct type or not yet in scope),
	// so the rebuild normalizes either spelling and the four forms match the
	// full-def twin. Wrapped here is the splice surface reached at the
	// positional / shadow corner the single-spelling family never crossed.
	for _, spelling := range spellings {
		for _, k := range kinds {
			P, L, R := "x.P", "x.L", "x.R"
			depP := mpJSON(mpFwdObj(P, k, 1))
			// Reference to the cached type BEFORE a local def of a distinct type:
			// the ref splices, the local def stays.
			cases = append(cases, fwd{
				name:  "distinct_ref_before_def/" + spelling + "/" + k,
				deps:  []string{depP},
				root:  mpJSON(mpRecObj(R, mpField("f1", mpRefSpell(spelling, P)), mpField("f2", mpFwdObj(L, k, 1)))),
				twin:  mpJSON(mpRecObj(R, mpField("f1", mpFwdObj(P, k, 1)), mpField("f2", mpFwdObj(L, k, 1)))),
				value: map[string]any{"f1": mpFwdVal(k, 1), "f2": mpFwdVal(k, 1)},
			})
			// Local def of a distinct type BEFORE the cached reference.
			cases = append(cases, fwd{
				name:  "distinct_def_before_ref/" + spelling + "/" + k,
				deps:  []string{depP},
				root:  mpJSON(mpRecObj(R, mpField("f1", mpFwdObj(L, k, 1)), mpField("f2", mpRefSpell(spelling, P)))),
				twin:  mpJSON(mpRecObj(R, mpField("f1", mpFwdObj(L, k, 1)), mpField("f2", mpFwdObj(P, k, 1)))),
				value: map[string]any{"f1": mpFwdVal(k, 1), "f2": mpFwdVal(k, 1)},
			})
		}
	}

	// Shadow corner, expressible direction: a bare ref to a cached NULL-namespace
	// type appears BEFORE a local same-short-name type in the enclosing
	// namespace. Eager positional binding sends the bare ref to the cached
	// null-namespace type (the local x.T is not yet in scope); the splice must
	// inline the 1-field null-ns T, not the 2-field x.T. Field counts differ so a
	// mis-bind would change the wire.
	{
		T, xT, R := "T", "x.T", "x.R"
		depT := mpJSON(mpFwdObj(T, "record", 1))
		posVal := func(pos string, v any) any {
			switch pos {
			case "array":
				return []any{v}
			case "map":
				return map[string]any{"k": v}
			default: // field, union (non-null branch)
				return v
			}
		}
		// Cross the shadowed reference over every position AND spelling:
		// inlineTreeDefs walks fields, array items, map values, and union branches
		// through distinct arms, so the forward-shadow splice must fire in each;
		// the wrapped spelling must reach the splice as a whole at the shadow
		// corner too (the bare ref binds the cached null-ns T because the local
		// x.T is not yet in scope).
		for _, spelling := range spellings {
			for _, pos := range []string{"field", "union", "array", "map"} {
				cases = append(cases, fwd{
					name:  "shadow_nullref_before_nsdef/" + spelling + "/" + pos,
					deps:  []string{depT},
					root:  mpJSON(mpRecObj(R, mpField("f1", mpPosWrap(pos, mpRefSpell(spelling, T))), mpField("f2", mpFwdObj(xT, "record", 2)))),
					twin:  mpJSON(mpRecObj(R, mpField("f1", mpPosWrap(pos, mpFwdObj(T, "record", 1))), mpField("f2", mpFwdObj(xT, "record", 2)))),
					value: map[string]any{"f1": posVal(pos, mpFwdVal("record", 1)), "f2": mpFwdVal("record", 2)},
					// f1's spliced null-ns T nests in the namespaced x.R: its PCF form
					// is the documented lossy-but-fingerprint-faithful kind (NOT_BUGS #25).
					canonNoReparse: true,
				})
			}
		}
		// Reverse: the local x.T is defined FIRST, so the later bare "T" binds the
		// LOCAL x.T (in scope), and the cached null-ns T is never referenced — no
		// splice, self-contained as written.
		cases = append(cases, fwd{
			name:  "shadow_nsdef_before_nullref",
			deps:  []string{depT},
			root:  mpJSON(mpRecObj(R, mpField("f1", mpFwdObj(xT, "record", 2)), mpField("f2", T))),
			twin:  mpJSON(mpRecObj(R, mpField("f1", mpFwdObj(xT, "record", 2)), mpField("f2", T))),
			value: map[string]any{"f1": mpFwdVal("record", 2), "f2": mpFwdVal("record", 2)},
		})
	}

	// Pure within-parse forward reference crossed with spelling: a parse that
	// references a cached type C (cross-parse, so the self-containment rebuild
	// runs) AND references a LOCAL type Q before Q's own definition. The forward
	// reference must NOT splice (Q is local, not inherited) — it stays a bare
	// reference to the later definition. The wrapped spelling exercises the
	// splice's no-splice fall-through directly: {"type":"Q"} must collapse to the
	// bare "Q" the canonical / inline twin carries, not survive the rebuild as a
	// wrapped object. The twin is written in the SAME reference-before-definition
	// order (the cache preserves source order; it does not reorder to def-first),
	// so the four forms compare field-for-field.
	for _, spelling := range spellings {
		for _, k := range kinds {
			C, Q, R := "x.C", "x.Q", "x.R"
			depC := mpJSON(mpFwdObj(C, k, 1))
			cases = append(cases, fwd{
				name: "local_forwardref/" + spelling + "/" + k,
				deps: []string{depC},
				root: mpJSON(mpRecObj(R,
					mpField("f0", mpRefSpell(spelling, C)), // cross-parse ref → splices, triggers rebuild
					mpField("f1", mpRefSpell(spelling, Q)), // forward ref to local Q (defined at f2)
					mpField("f2", mpFwdObj(Q, k, 1)))),     // Q defined here
				twin: mpJSON(mpRecObj(R,
					mpField("f0", mpFwdObj(C, k, 1)),
					mpField("f1", Q), // canonical bare reference
					mpField("f2", mpFwdObj(Q, k, 1)))),
				value: map[string]any{"f0": mpFwdVal(k, 1), "f1": mpFwdVal(k, 1), "f2": mpFwdVal(k, 1)},
			})
		}
	}

	// Shadow corner, INEXPRESSIBLE: a null-namespace D is carried (self-contained)
	// by two namespaced records F and G, while an enclosing-namespace x.D shadows
	// its short name. When the splice inlines x.D, then F (with null-ns D), then G
	// (with null-ns D again), the duplicate null-ns D inside G's subtree has NO
	// reference spelling — a bare "D" would re-bind to x.D at that position
	// (scopedRefKeys binds enclosing-first), and Avro has no absolute-null
	// reference. dupDefRef therefore declines, so NO re-parseable inline twin
	// exists; the four-form check is skipped with that provable reason and the
	// binding-safe check is asserted instead (matches
	// TestMatrix_SchemaCacheShortNameShadowNoMisbind, generated here).
	{
		D, xF, xG, xD, A := "D", "x.F", "x.G", "x.D", "x.A"
		cases = append(cases, fwd{
			name: "shadow_inexpressible_corner",
			deps: []string{
				mpJSON(mpFwdObj(D, "record", 1)),      // null-ns D (1 field)
				mpJSON(mpRecObj(xF, mpField("d", D))), // x.F.d → bare "D" (null-ns, x.D not yet defined)
				mpJSON(mpRecObj(xG, mpField("d", D))), // x.G.d → bare "D"
				mpJSON(mpFwdObj(xD, "record", 2)),     // shadowing x.D (2 fields)
			},
			root: mpJSON(mpRecObj(A, mpField("p", xD), mpField("f", xF), mpField("g", xG))),
			twin: "",
			value: map[string]any{
				"p": mpFwdVal("record", 2),
				"f": map[string]any{"d": mpFwdVal("record", 1)},
				"g": map[string]any{"d": mpFwdVal("record", 1)},
			},
		})
	}

	for _, fc := range cases {
		t.Run("forward/"+fc.name, func(t *testing.T) {
			cells++
			s := mpRunCache(t, fc.deps, fc.root)
			if fc.twin != "" {
				inline, err := avro.Parse(fc.twin)
				if err != nil {
					t.Fatalf("twin parse %q: %v", fc.twin, err)
				}
				mpAssertSelfContained(t, s, inline, fc.value, fc.root, fc.twin, !fc.canonNoReparse)
				return
			}
			// Inexpressible corner: assert binding-safety. The node-tree wire
			// codec must work, and if String() happens to re-parse it must
			// describe the SAME schema (identical wire) — never the shadowed type.
			wire, err := s.Encode(fc.value)
			if err != nil {
				t.Fatalf("wire encode (node tree must work): %v", err)
			}
			var dec any
			if _, err := s.Decode(wire, &dec); err != nil {
				t.Fatalf("wire decode: %v", err)
			}
			if reparsed, err := avro.Parse(s.String()); err == nil {
				w2, err := reparsed.Encode(fc.value)
				if err != nil {
					t.Errorf("String() re-parses but rejects a wire-valid value (mis-bound shadow ref): %v", err)
				} else if !bytes.Equal(w2, wire) {
					t.Errorf("String() re-parses but yields different wire (mis-bound shadow ref)")
				}
			}
		})
	}

	// --- inherited-namespace family: the cross-parse-referenced leaf DERIVES its
	// namespace from an enclosing wrapper in its defining parse (no explicit
	// "namespace"), then is referenced from a DIFFERENT namespace (mixed regime:
	// leaf in "b", carriers/root in "a"). The stored definition must keep its
	// resolved fullname (b.D) when spliced into the "a" scope, not re-inherit "a"
	// (de3dca3). Crossed with topology so the inherited def is reached directly,
	// through a chain, and through both diamond arms — coverage the single-
	// reference TestMatrix_CacheSelfContainedNamespaces does not have. Crossed
	// with spelling so a WRAPPED reference to an inherited-namespace cached leaf
	// splices the explicit-fullname def too. ---
	for _, spelling := range spellings {
		for _, topo := range []string{"single", "chain2", "diamond"} {
			for _, kind := range kinds {
				for _, pos := range []string{"field", "union"} {
					g, parseOrder, ok := mpBuildTopo(topo, "mixed", kind, pos)
					if !ok {
						continue
					}
					name := fmt.Sprintf("inherited_ns/%s/%s/%s/%s", spelling, topo, pos, kind)
					t.Run(name, func(t *testing.T) {
						cells++
						deps := make([]string, len(parseOrder))
						for i, fn := range parseOrder {
							deps[i] = mpEmitStandalone(fn, g, spelling)
						}
						// Register the leaf (parseOrder[0]) via an inherited-namespace
						// wrapper rather than a standalone explicit-namespace def.
						deps[0] = mpEmitInheritedWrapper(parseOrder[0], kind)
						rootSchema := mpEmitStandalone(g.root, g, spelling)
						viaCache := mpRunCache(t, deps, rootSchema)
						twinSchema := mpEmitTwin(g)
						inline, err := avro.Parse(twinSchema)
						if err != nil {
							t.Fatalf("inline twin parse %q: %v", twinSchema, err)
						}
						mpAssertSelfContained(t, viaCache, inline, mpSampleValue(g.root, g), rootSchema, twinSchema, true)
					})
				}
			}
		}
	}

	t.Logf("multi-parse self-containment net: %d cells", cells)
}

// Non-vacuity (neuter cache.go's inlineTreeDefs, observe the failures):
//
//   - Collapse the wrapper only when the wrapped reference SPLICES (restore the
//     `if _, stayedBare := spliced.(string); !stayedBare { return spliced }`
//     guard so a non-splicing wrapped reference falls through unchanged): the 76
//     repeat2 / repeat_chain / local_forwardref wrapped cells fail with "String
//     diverges" — the later/forward wrapped {"type":"X"} survives where the twin
//     carries bare "X". Canonical/Fingerprint/Root still match (PCF emits bare),
//     so String is the only surface — exactly the layer the single-reference
//     topologies cannot reach.
//
//   - Remove the whole wrapped-reference detection (so a wrapped reference hits
//     the general map path): all 356 wrapped cells across every topology fail
//     with Canonical/Fingerprint/Root/String diverging AND "not self-contained"
//     — the splice produced the invalid {"type":{X-def}} and the metadata fell
//     back to a dangling reference. The bare cells stay green throughout, proving
//     the spelling axis (not some shared regression) is what catches the bug.

// ---------- matrix_concurrent_test.go ----------

// ---------------------------------------------------------------------------
// Concurrency hammer: one *Schema shared by many goroutines doing every
// operation simultaneously. A *Schema is documented goroutine-safe; the
// risks are the lazily-initialized internals (sync.Once field tables, the
// slab pool, cached canonical bytes) and any shared scratch state. Every
// result is compared byte-for-byte against the precomputed single-threaded
// answer, under the race detector in CI's `go test -race ./...`.
// ---------------------------------------------------------------------------

func TestMatrix_ConcurrentSchemaUse(t *testing.T) {
	cases := []struct {
		label  string
		schema string
		value  any
	}{
		{"int", `"int"`, int32(7)},
		{"string", `"string"`, "concurrent"},
		{"record", `{"type":"record","name":"CC","fields":[
			{"name":"a","type":"int"},{"name":"b","type":["null","string"],"default":null}]}`,
			map[string]any{"a": int32(1), "b": "x"}},
		{"decimal", `{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}`,
			[]byte{0x30, 0x39}},
		{"timestamp", `{"type":"long","logicalType":"timestamp-millis"}`, int64(1717243496789)},
		{"recursive", `{"type":"record","name":"CN","fields":[
			{"name":"v","type":"int"},{"name":"next","type":["null","CN"],"default":null}]}`,
			map[string]any{"v": int32(1), "next": map[string]any{"v": int32(2), "next": nil}}},
		{"array-of-union", `{"type":"array","items":["null","long","string"]}`,
			[]any{int64(5), nil, "s"}},
	}
	const goroutines = 8
	const iters = 100

	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			// FRESH schema per case so the lazy once-init paths (field
			// table construction, canonical caching) are themselves raced.
			s := avro.MustParse(c.schema)
			ref := avro.MustParse(c.schema) // single-threaded reference
			wantWire, err := ref.AppendEncode(nil, c.value)
			if err != nil {
				t.Fatalf("reference encode: %v", err)
			}
			wantJSON, err := ref.AppendEncodeJSON(nil, c.value)
			if err != nil {
				t.Fatalf("reference encodeJSON: %v", err)
			}
			var wantDec any
			if _, err := ref.Decode(wantWire, &wantDec); err != nil {
				t.Fatalf("reference decode: %v", err)
			}
			wantCanon := string(ref.Canonical())

			res, err := avro.Resolve(avro.MustParse(c.schema), avro.MustParse(c.schema))
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}

			var wg sync.WaitGroup
			errs := make(chan error, goroutines)
			for g := 0; g < goroutines; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < iters; i++ {
						w, err := s.AppendEncode(nil, c.value)
						if err != nil || !bytes.Equal(w, wantWire) {
							errs <- fmt.Errorf("encode diverged: err=%v w=%x", err, w)
							return
						}
						var a any
						if _, err := s.Decode(w, &a); err != nil || !matEqual(a, wantDec) {
							errs <- fmt.Errorf("decode diverged: err=%v a=%#v", err, a)
							return
						}
						j, err := s.AppendEncodeJSON(nil, a)
						if err != nil || !bytes.Equal(j, wantJSON) {
							errs <- fmt.Errorf("encodeJSON diverged: err=%v j=%s", err, j)
							return
						}
						var aj any
						if err := s.DecodeJSON(j, &aj); err != nil || !matEqual(aj, wantDec) {
							errs <- fmt.Errorf("decodeJSON diverged: err=%v", err)
							return
						}
						var ar any
						if _, err := res.Decode(w, &ar); err != nil || !matEqual(ar, wantDec) {
							errs <- fmt.Errorf("resolved decode diverged: err=%v", err)
							return
						}
						if i%16 == 0 {
							if got := string(s.Canonical()); got != wantCanon {
								errs <- fmt.Errorf("Canonical diverged: %s", got)
								return
							}
							root := s.Root()
							if root.Type == "" {
								errs <- fmt.Errorf("Root returned zero node")
								return
							}
						}
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Error(err)
			}
		})
	}
}

// Concurrent SchemaCache use: many goroutines parsing the same and
// different named schemas through one cache, then cross-referencing.
func TestMatrix_ConcurrentSchemaCache(t *testing.T) {
	var cache avro.SchemaCache
	def := `{"type":"record","name":"CCD","fields":[{"name":"a","type":"int"}]}`
	if _, err := cache.Parse(def); err != nil {
		t.Fatalf("seed parse: %v", err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if _, err := cache.Parse(def); err != nil {
					errs <- fmt.Errorf("re-parse def: %v", err)
					return
				}
				ref, err := cache.Parse(`{"type":"array","items":"CCD"}`)
				if err != nil {
					errs <- fmt.Errorf("ref parse: %v", err)
					return
				}
				w, err := ref.AppendEncode(nil, []any{map[string]any{"a": int32(g)}})
				if err != nil {
					errs <- fmt.Errorf("encode: %v", err)
					return
				}
				var a any
				if _, err := ref.Decode(w, &a); err != nil {
					errs <- fmt.Errorf("decode: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// Concurrent FIRST use: goroutines hit a freshly parsed schema's decode
// path simultaneously, racing the lazily-built per-record skip/field
// tables (sync.Once internals).
func TestMatrix_ConcurrentFirstUse(t *testing.T) {
	for round := 0; round < 10; round++ {
		w := avro.MustParse(`{"type":"record","name":"FU","fields":[
			{"name":"drop","type":{"type":"record","name":"Inner","fields":[
				{"name":"x","type":"string"},{"name":"y","type":{"type":"array","items":"long"}}]}},
			{"name":"keep","type":"int"}]}`)
		r := avro.MustParse(`{"type":"record","name":"FU","fields":[
			{"name":"keep","type":"int"}]}`)
		res := mustResolve(t, w, r)
		wire := mustAppendEncode(t, w, nil, map[string]any{
			"drop": map[string]any{"x": "s", "y": []any{int64(1), int64(2)}},
			"keep": int32(9),
		})
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var got map[string]any
				if _, err := res.Decode(wire, &got); err != nil {
					errs <- fmt.Errorf("first-use resolved decode: %v", err)
					return
				}
				if got["keep"] != int32(9) {
					errs <- fmt.Errorf("first-use skip corrupted: %#v", got)
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	}
}

// ---------- matrix_custom_test.go ----------

// ---------------------------------------------------------------------------
// CustomType matrix: every logical-type fragment × five positions × three
// custom configurations, asserting binary↔JSON parity of what the callbacks
// see, what suppression yields, and how often callbacks fire.
//
// The raw Avro-native form is CALIBRATED, never hand-computed: a suppressing
// schema (no-callback CustomType match) decodes the plain schema's wire, and
// whatever it returns IS the raw form. Configs:
//
//	suppress — CustomType{LogicalType: L} (no callbacks): both decoders must
//	           yield the same RAW value tree, and that tree must re-encode
//	           onto the plain schema's exact binary wire.
//	box      — Decode wraps raw into cbox{...}; Encode unboxes. The boxed
//	           tree must round-trip identically through binary and JSON,
//	           and the binary wire must equal the plain schema's.
//	count    — wildcard CustomType{} whose callbacks just count and skip:
//	           the number of invocations must agree between the binary and
//	           JSON paths, per direction (a side-effect parity that value
//	           asserts can't see).
// ---------------------------------------------------------------------------

type cbox struct{ Raw any }

type customFrag struct {
	label    string
	schema   string
	logical  string
	enriched any
}

func customFrags() []customFrag {
	return []customFrag{
		{"date", `{"type":"int","logicalType":"date"}`, "date",
			time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, "time-millis",
			3*time.Hour + 7*time.Millisecond},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`, "time-micros",
			23*time.Hour + 5*time.Microsecond},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, "timestamp-millis",
			time.Date(2024, 6, 1, 12, 34, 56, 789000000, time.UTC)},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, "timestamp-micros",
			time.Date(2024, 6, 1, 12, 34, 56, 789012000, time.UTC)},
		{"timestamp-nanos", `{"type":"long","logicalType":"timestamp-nanos"}`, "timestamp-nanos",
			time.Date(2024, 6, 1, 12, 34, 56, 789012345, time.UTC)},
		{"local-ts-millis", `{"type":"long","logicalType":"local-timestamp-millis"}`, "local-timestamp-millis",
			time.Date(2024, 6, 1, 12, 34, 56, 789000000, time.UTC)},
		{"uuid-string", `{"type":"string","logicalType":"uuid"}`, "uuid",
			"6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{"uuid-fixed", `{"type":"fixed","name":"CUF","size":16,"logicalType":"uuid"}`, "uuid",
			"6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{"decimal-bytes", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, "decimal",
			big.NewRat(12345, 100)},
		{"decimal-fixed", `{"type":"fixed","name":"CDF","size":8,"logicalType":"decimal","precision":10,"scale":2}`, "decimal",
			big.NewRat(-9999, 100)},
		{"duration", `{"type":"fixed","name":"CDU","size":12,"logicalType":"duration"}`, "duration",
			avro.Duration{Months: 1, Days: 2, Milliseconds: 3}},
		{"big-decimal", `{"type":"bytes","logicalType":"big-decimal"}`, "big-decimal",
			big.NewRat(314, 100)},
	}
}

// customPos wraps a fragment schema/value into a position and can pull the
// inner value back out of a decoded tree.
type customPos struct {
	label  string
	skip   func(class string) bool
	schema func(inner string) string
	wrap   func(v any) any
	unwrap func(v any) any
}

func customPositions() []customPos {
	id := func(v any) any { return v }
	return []customPos{
		{"top", nil,
			func(in string) string { return in },
			id, id},
		{"field",
			nil,
			func(in string) string {
				return fmt.Sprintf(`{"type":"record","name":"CW","fields":[{"name":"a","type":"long"},{"name":"f","type":%s}]}`, in)
			},
			func(v any) any { return map[string]any{"a": int64(4), "f": v} },
			func(v any) any { return v.(map[string]any)["f"] }},
		{"array",
			nil,
			func(in string) string { return fmt.Sprintf(`{"type":"array","items":%s}`, in) },
			func(v any) any { return []any{v, v} },
			func(v any) any { return v.([]any)[0] }},
		{"nullunion",
			nil,
			func(in string) string { return fmt.Sprintf(`["null",%s]`, in) },
			id, id},
		{"multibranch",
			nil,
			func(in string) string { return fmt.Sprintf(`["null","boolean",%s,%s]`, in, `"long"`) },
			id, id},
	}
}

// customClass mirrors tokenClass for the multibranch pad: fragments whose
// bare JSON token is a digit collide with the "long" pad branch, so those
// swap the pad to "string"; string-class fragments keep "long".
func customPad(frag customFrag) string {
	switch frag.label {
	case "uuid-string", "uuid-fixed", "decimal-bytes", "decimal-fixed", "duration", "big-decimal":
		return `"long"`
	default:
		return `"string"`
	}
}

func TestMatrix_CustomTypes(t *testing.T) {
	for _, fr := range customFrags() {
		for _, pos := range customPositions() {
			posSchema := pos.schema(fr.schema)
			if pos.label == "multibranch" {
				posSchema = fmt.Sprintf(`["null","boolean",%s,%s]`, fr.schema, customPad(fr))
			}
			// The "long" pad collides with long-backed logicals' type in
			// unions (duplicate union type); those swap to "string" via
			// customPad, but a string pad collides with uuid-string's
			// type. Skip the genuinely uncomposable pairs.
			if pos.label == "multibranch" {
				if _, err := avro.Parse(posSchema); err != nil {
					continue
				}
			}

			plain := avro.MustParse(posSchema)
			vin := pos.wrap(fr.enriched)
			plainWire, err := plain.AppendEncode(nil, vin)
			if err != nil {
				t.Fatalf("%s/%s: plain encode: %v", fr.label, pos.label, err)
			}
			plainJSON, err := plain.AppendEncodeJSON(nil, vin)
			if err != nil {
				t.Fatalf("%s/%s: plain encodeJSON: %v", fr.label, pos.label, err)
			}

			t.Run(fr.label+"/"+pos.label+"/suppress", func(t *testing.T) {
				sup, err := avro.Parse(posSchema, avro.CustomType{LogicalType: fr.logical})
				if err != nil {
					t.Fatalf("Parse: %v", err)
				}
				var aBin, aJSON any
				if _, err := sup.Decode(plainWire, &aBin); err != nil {
					t.Fatalf("suppressed binary decode: %v", err)
				}
				if err := sup.DecodeJSON(plainJSON, &aJSON); err != nil {
					t.Fatalf("suppressed JSON decode: %v", err)
				}
				if !matEqual(aBin, aJSON) {
					t.Fatalf("suppressed decode diverges:\n bin=%#v\njson=%#v", aBin, aJSON)
				}
				// Suppression means RAW: the enriched Go type must be absent.
				switch pos.unwrap(aBin).(type) {
				case time.Time, time.Duration, *big.Rat, avro.Duration:
					t.Fatalf("suppressed decode yielded enriched %T", pos.unwrap(aBin))
				}
				// The raw tree re-encodes onto the plain schema's exact wire.
				w2, err := sup.AppendEncode(nil, aBin)
				if err != nil || !bytes.Equal(w2, plainWire) {
					t.Fatalf("raw re-encode differs: err=%v\n plain=%x\n raw=%x", err, plainWire, w2)
				}
			})

			t.Run(fr.label+"/"+pos.label+"/box", func(t *testing.T) {
				ct := avro.CustomType{
					LogicalType: fr.logical,
					Decode: func(v any, _ *avro.SchemaNode) (any, error) {
						return cbox{Raw: v}, nil
					},
					Encode: func(v any, _ *avro.SchemaNode) (any, error) {
						if b, ok := v.(cbox); ok {
							return b.Raw, nil
						}
						return nil, avro.ErrSkipCustomType
					},
				}
				bs, err := avro.Parse(posSchema, ct)
				if err != nil {
					t.Fatalf("Parse: %v", err)
				}
				// Custom decode fires on the plain wire (same bytes).
				var boxed any
				if _, err := bs.Decode(plainWire, &boxed); err != nil {
					t.Fatalf("boxed decode: %v", err)
				}
				inner := pos.unwrap(boxed)
				if _, ok := inner.(cbox); !ok {
					t.Fatalf("decode did not box: %T", inner)
				}
				// The boxed tree re-encodes to the IDENTICAL binary wire.
				w2, err := bs.AppendEncode(nil, boxed)
				if err != nil || !bytes.Equal(w2, plainWire) {
					t.Fatalf("boxed re-encode differs: err=%v\n plain=%x\n boxed=%x", err, plainWire, w2)
				}
				// JSON round-trip within the custom schema agrees with the
				// binary decode (suppressed logical → raw JSON forms).
				jb, err := bs.AppendEncodeJSON(nil, boxed)
				if err != nil {
					t.Fatalf("boxed encodeJSON: %v", err)
				}
				var jBack any
				if err := bs.DecodeJSON(jb, &jBack); err != nil {
					t.Fatalf("boxed decodeJSON: %v\n j=%s", err, jb)
				}
				if !matEqual(jBack, boxed) {
					t.Fatalf("boxed JSON round-trip diverges:\n bin=%#v\njson=%#v\n j=%s", boxed, jBack, jb)
				}
				// The metadata rebuild can re-wire the custom by passing it
				// through Schema(opts...): the rebuilt schema must box and
				// re-encode identically.
				root := bs.Root()
				rebuilt, err := root.Schema(ct)
				if err != nil {
					t.Fatalf("Root().Schema(ct): %v", err)
				}
				var reboxed any
				if _, err := rebuilt.Decode(plainWire, &reboxed); err != nil {
					t.Fatalf("rebuilt boxed decode: %v", err)
				}
				if !matEqual(reboxed, boxed) {
					t.Fatalf("rebuilt custom decode diverges:\n orig=%#v\n reb=%#v", boxed, reboxed)
				}
				w3, err := rebuilt.AppendEncode(nil, reboxed)
				if err != nil || !bytes.Equal(w3, plainWire) {
					t.Fatalf("rebuilt boxed re-encode differs: err=%v\n plain=%x\n reb=%x", err, plainWire, w3)
				}
			})

			t.Run(fr.label+"/"+pos.label+"/count", func(t *testing.T) {
				var encN, decN atomic.Int64
				ct := avro.CustomType{
					LogicalType: fr.logical,
					Encode: func(v any, _ *avro.SchemaNode) (any, error) {
						encN.Add(1)
						return nil, avro.ErrSkipCustomType
					},
					Decode: func(v any, _ *avro.SchemaNode) (any, error) {
						decN.Add(1)
						return nil, avro.ErrSkipCustomType
					},
				}
				cs := mustParse(t, posSchema, ct)
				// A matching custom WITH Encode suppresses the built-in
				// logical encoder on fixed/decimal builds (documented
				// per-build suppression), so enriched inputs reject there
				// once the callback skips. Drive the RAW tree instead —
				// calibrated by a suppressing decode of the plain wire —
				// which every build accepts.
				supCal := avro.MustParse(posSchema, avro.CustomType{LogicalType: fr.logical})
				var rawTree any
				if _, err := supCal.Decode(plainWire, &rawTree); err != nil {
					t.Fatalf("raw calibration decode: %v", err)
				}
				encN.Store(0)
				if _, err := cs.AppendEncode(nil, rawTree); err != nil {
					t.Fatalf("count encode: %v", err)
				}
				encBin := encN.Load()
				encN.Store(0)
				if _, err := cs.AppendEncodeJSON(nil, rawTree); err != nil {
					t.Fatalf("count encodeJSON: %v", err)
				}
				encJSON := encN.Load()
				if encBin != encJSON {
					t.Fatalf("encode callback count diverges: binary=%d json=%d", encBin, encJSON)
				}
				var sink any
				decN.Store(0)
				if _, err := cs.Decode(plainWire, &sink); err != nil {
					t.Fatalf("count decode: %v", err)
				}
				decBin := decN.Load()
				decN.Store(0)
				if err := cs.DecodeJSON(plainJSON, &sink); err != nil {
					t.Fatalf("count decodeJSON: %v", err)
				}
				decJSON := decN.Load()
				if decBin != decJSON {
					t.Fatalf("decode callback count diverges: binary=%d json=%d", decBin, decJSON)
				}
			})
		}
	}
}

// ---------- matrix_custom_cross_test.go ----------

// ---------------------------------------------------------------------------
// Second-order CustomType crosses: customs × schema evolution (resolution
// around a custom field, including writer-shaped resolved DecodeJSON),
// customs × SchemaCache (consistent registration across parses), and
// customs × OCF (WithSchemaOpts through the file layer). Each historical
// CustomType regression lived at one of these intersections.
// ---------------------------------------------------------------------------

// crossBoxCT returns a fresh boxing CustomType for the logical.
func crossBoxCT(logical string) avro.CustomType {
	return avro.CustomType{
		LogicalType: logical,
		Decode: func(v any, _ *avro.SchemaNode) (any, error) {
			return cbox{Raw: v}, nil
		},
		Encode: func(v any, _ *avro.SchemaNode) (any, error) {
			if b, ok := v.(cbox); ok {
				return b.Raw, nil
			}
			return nil, avro.ErrSkipCustomType
		},
	}
}

// crossRaw calibrates the raw Avro-native form of a fragment's enriched
// value (a suppressing decode of the plain wire).
func crossRaw(t *testing.T, fr customFrag) any {
	t.Helper()
	plain := avro.MustParse(fr.schema)
	w, err := plain.AppendEncode(nil, fr.enriched)
	if err != nil {
		t.Fatalf("calibrate encode: %v", err)
	}
	sup := avro.MustParse(fr.schema, avro.CustomType{LogicalType: fr.logical})
	var raw any
	if _, err := sup.Decode(w, &raw); err != nil {
		t.Fatalf("calibrate decode: %v", err)
	}
	return raw
}

// Customs × evolution: a custom field survives resolution while a sibling
// field is dropped and another is default-filled; the resolved binary
// decode, the resolved writer-shaped DecodeJSON, and the custom callbacks
// must all compose.
func TestMatrix_CustomTimesEvolution(t *testing.T) {
	for _, fr := range customFrags() {
		t.Run(fr.label, func(t *testing.T) {
			ct := crossBoxCT(fr.logical)
			raw := crossRaw(t, fr)
			wSchema := fmt.Sprintf(`{"type":"record","name":"R","fields":[
				{"name":"pre","type":"string"},
				{"name":"f","type":%s},
				{"name":"dropme","type":"int"}]}`, fr.schema)
			rSchema := fmt.Sprintf(`{"type":"record","name":"R","fields":[
				{"name":"pre","type":"string"},
				{"name":"f","type":%s},
				{"name":"added","type":"long","default":7}]}`, fr.schema)
			w, err := avro.Parse(wSchema, ct)
			if err != nil {
				t.Fatalf("writer Parse: %v", err)
			}
			r, err := avro.Parse(rSchema, ct)
			if err != nil {
				t.Fatalf("reader Parse: %v", err)
			}
			res, err := avro.Resolve(w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}

			vin := map[string]any{"pre": "p", "f": cbox{Raw: raw}, "dropme": int32(3)}
			wire, err := w.AppendEncode(nil, vin)
			if err != nil {
				t.Fatalf("writer encode: %v", err)
			}
			var got map[string]any
			if _, err := res.Decode(wire, &got); err != nil {
				t.Fatalf("resolved decode: %v", err)
			}
			b, ok := got["f"].(cbox)
			if !ok {
				t.Fatalf("custom did not fire through resolution: f=%#v", got["f"])
			}
			if !matEqual(b.Raw, raw) {
				t.Fatalf("custom payload corrupted through resolution:\n got=%#v\nwant=%#v", b.Raw, raw)
			}
			if got["added"] != int64(7) || got["pre"] != "p" {
				t.Fatalf("evolution around custom field broken: %#v", got)
			}
			if _, dropped := got["dropme"]; dropped {
				t.Fatalf("dropped field survived: %#v", got)
			}

			// Resolved DecodeJSON consumes WRITER-shaped JSON and must land
			// on the same tree as the resolved binary decode.
			wJSON, err := w.AppendEncodeJSON(nil, vin)
			if err != nil {
				t.Fatalf("writer encodeJSON: %v", err)
			}
			var gotJSON map[string]any
			if err := res.DecodeJSON(wJSON, &gotJSON); err != nil {
				t.Fatalf("resolved DecodeJSON: %v", err)
			}
			if !matEqual(any(gotJSON), any(got)) {
				t.Fatalf("resolved DecodeJSON diverges from resolved Decode:\n json=%#v\n bin=%#v", gotJSON, got)
			}
		})
	}
}

// Customs × SchemaCache: a named type defined in one Parse and referenced
// from a second (both registering the same CustomType — the documented
// consistent-registration path) must behave like the inline definition.
func TestMatrix_CustomTimesCache(t *testing.T) {
	for _, fr := range customFrags() {
		t.Run(fr.label, func(t *testing.T) {
			ct := crossBoxCT(fr.logical)
			raw := crossRaw(t, fr)
			def := fmt.Sprintf(`{"type":"record","name":"CN","fields":[{"name":"f","type":%s}]}`, fr.schema)

			var cache avro.SchemaCache
			if _, err := cache.Parse(def, ct); err != nil {
				t.Fatalf("cache.Parse(def): %v", err)
			}
			viaRef, err := cache.Parse(`{"type":"array","items":"CN"}`, ct)
			if err != nil {
				t.Fatalf("cache.Parse(ref): %v", err)
			}
			inline, err := avro.Parse(fmt.Sprintf(`{"type":"array","items":%s}`, def), ct)
			if err != nil {
				t.Fatalf("inline Parse: %v", err)
			}

			vin := []any{map[string]any{"f": cbox{Raw: raw}}}
			wRef, err := viaRef.AppendEncode(nil, vin)
			if err != nil {
				t.Fatalf("cache-ref encode: %v", err)
			}
			wInl, err := inline.AppendEncode(nil, vin)
			if err != nil || !bytes.Equal(wRef, wInl) {
				t.Fatalf("cache-ref vs inline wire: err=%v\n ref=%x\n inl=%x", err, wRef, wInl)
			}
			var aRef, aInl any
			if _, err := viaRef.Decode(wRef, &aRef); err != nil {
				t.Fatalf("cache-ref decode: %v", err)
			}
			if _, err := inline.Decode(wInl, &aInl); err != nil {
				t.Fatalf("inline decode: %v", err)
			}
			if !matEqual(aRef, aInl) {
				t.Fatalf("cache-ref decode diverges:\n ref=%#v\n inl=%#v", aRef, aInl)
			}
			f := aRef.([]any)[0].(map[string]any)["f"]
			if _, ok := f.(cbox); !ok {
				t.Fatalf("custom did not fire through cache reference: %#v", f)
			}
		})
	}
}

// Customs × OCF: custom-typed schemas through the file layer, write and
// read, with the CustomType supplied to the reader via WithSchemaOpts.
func TestMatrix_CustomTimesOCF(t *testing.T) {
	for _, fr := range customFrags() {
		t.Run(fr.label, func(t *testing.T) {
			ct := crossBoxCT(fr.logical)
			raw := crossRaw(t, fr)
			schemaJSON := fmt.Sprintf(`{"type":"record","name":"OC","fields":[{"name":"f","type":%s}]}`, fr.schema)
			ws := mustParse(t, schemaJSON, ct)
			var buf bytes.Buffer
			w := mustNewWriter(t, &buf, ws)
			for i := 0; i < 3; i++ {
				if err := w.Encode(map[string]any{"f": cbox{Raw: raw}}); err != nil {
					t.Fatalf("Encode #%d: %v", i, err)
				}
			}
			mustClose(t, w)

			r := mustNewReader(t, bytes.NewReader(buf.Bytes()), ocf.WithSchemaOpts(ct))
			defer r.Close()
			var n int
			for {
				var v map[string]any
				err := r.Decode(&v)
				if err != nil {
					break
				}
				b, ok := v["f"].(cbox)
				if !ok {
					t.Fatalf("datum %d: custom did not fire through OCF: %#v", n, v["f"])
				}
				if !matEqual(b.Raw, raw) {
					t.Fatalf("datum %d payload corrupted: %#v", n, b.Raw)
				}
				n++
			}
			if n != 3 {
				t.Fatalf("read %d of 3", n)
			}
		})
	}
}

// Options cube: every combination of the three Opt flags through the
// relational core, on fragments where each opt is semantically active.
func TestMatrix_OptionsCube(t *testing.T) {
	type optCase struct {
		label  string
		schema string
		value  any
	}
	cases := []optCase{
		{"nullunion-long", `["null","long"]`, int64(42)},
		{"timestamp-union", `["null",{"type":"long","logicalType":"timestamp-millis"}]`,
			time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)},
		{"double-array", `{"type":"array","items":"double"}`, []any{1.5, -2.25}},
		{"uuid-fixed-union", `["null",{"type":"fixed","name":"OCU","size":16,"logicalType":"uuid"}]`,
			"6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
	}
	flags := []struct {
		label string
		opts  []avro.Opt
	}{
		{"none", nil},
		{"tagged", []avro.Opt{avro.TaggedUnions()}},
		{"taglogical", []avro.Opt{avro.TagLogicalTypes()}},
		{"linkedin", []avro.Opt{avro.LinkedinFloats()}},
		{"tagged+taglogical", []avro.Opt{avro.TaggedUnions(), avro.TagLogicalTypes()}},
		{"tagged+linkedin", []avro.Opt{avro.TaggedUnions(), avro.LinkedinFloats()}},
		{"taglogical+linkedin", []avro.Opt{avro.TagLogicalTypes(), avro.LinkedinFloats()}},
		{"all", []avro.Opt{avro.TaggedUnions(), avro.TagLogicalTypes(), avro.LinkedinFloats()}},
	}
	for _, c := range cases {
		for _, fl := range flags {
			t.Run(c.label+"/"+fl.label, func(t *testing.T) {
				runCore(t, c.schema, c.value, fl.opts...)
			})
		}
	}
}

// ---------- matrix_dualpath_test.go ----------

// ---------------------------------------------------------------------------
// Dual-path parity net — the STANDING guarantee that closes the recurring
// "reflect path tested, unsafe path missed" class.
//
// twmb has two encode paths: the REFLECT serializers (top-level values, []any,
// map[string]any) and the UNSAFE serializers (selected for an ADDRESSABLE
// struct field, via serRecordFast). Bug after bug landed where a fix or a net
// covered the reflect path and silently missed its compiled unsafe twin
// (usArrayRecord's zero-byte cap; usTimeMillisTime/usTimeMicrosTime were never
// executed at all). Vigilance is not a fix; this driver is.
//
// For a single-field record the wire is exactly the field's encoding (records
// have no framing), so encoding a value TOP-LEVEL (reflect) and as the single
// field of an ADDRESSABLE record struct (unsafe) MUST produce byte-identical
// wire, and both must decode back. The battery below crosses every concrete Go
// type → schema mapping, so the unsafe encoder/decoder for each is exercised
// and held to parity with the reflect one. Any new type mapping MUST add a row
// here. (Run by default through BOTH paths — do not write a value-driven net
// that only drives top-level reflect.)
// ---------------------------------------------------------------------------

func TestMatrix_ReflectUnsafePathParity(t *testing.T) {
	type inner struct {
		N int32 `avro:"n"`
	}
	rat := func(n, d int64) *big.Rat { return big.NewRat(n, d) }

	rows := []struct {
		label  string
		schema string // the FIELD/value schema
		value  any    // a concrete-typed value (NOT any/interface) so unsafe engages
	}{
		{"int32", `"int"`, int32(-7)},
		{"int64", `"long"`, int64(1 << 40)},
		{"float32", `"float"`, float32(1.5)},
		{"float64", `"double"`, float64(2.5)},
		{"bool", `"boolean"`, true},
		{"string", `"string"`, "héllo"},
		{"bytes", `"bytes"`, []byte{1, 2, 3}},
		{"fixed16", `{"type":"fixed","name":"F16","size":16}`, [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
		{"uuid-fixed", `{"type":"fixed","name":"FU","size":16,"logicalType":"uuid"}`, [16]byte{15: 1}},

		// Logical TIME types — both the time.Duration carrier and the
		// time.Time carrier. The time.Time forms against time-millis/micros
		// are usTimeMillisTime/usTimeMicrosTime: ZERO test coverage until now.
		{"date", `{"type":"int","logicalType":"date"}`, time.Date(2020, 6, 1, 0, 0, 0, 0, time.UTC)},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, time.UnixMilli(1234567).UTC()},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, time.UnixMicro(1234567).UTC()},
		{"timestamp-nanos", `{"type":"long","logicalType":"timestamp-nanos"}`, time.Unix(0, 1234567890123).UTC()},
		// local-timestamp-* and timestamp-nanos exercise usTimestampNanos /
		// usLocalTimestamp{Millis,Micros,Nanos} — unsafe twins the parity
		// battery previously omitted (twin-path catalog GAP).
		{"local-timestamp-millis", `{"type":"long","logicalType":"local-timestamp-millis"}`, time.Date(2020, 1, 2, 3, 4, 5, 6000000, time.UTC)},
		{"local-timestamp-micros", `{"type":"long","logicalType":"local-timestamp-micros"}`, time.Date(2020, 1, 2, 3, 4, 5, 6000, time.UTC)},
		{"local-timestamp-nanos", `{"type":"long","logicalType":"local-timestamp-nanos"}`, time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC)},
		{"time-millis/duration", `{"type":"int","logicalType":"time-millis"}`, 3*time.Hour + 14*time.Minute},
		{"time-micros/duration", `{"type":"long","logicalType":"time-micros"}`, 3*time.Hour + 14*time.Minute + 159*time.Microsecond},
		{"time-millis/time", `{"type":"int","logicalType":"time-millis"}`, time.Date(2020, 1, 1, 3, 14, 15, 0, time.UTC)},
		{"time-micros/time", `{"type":"long","logicalType":"time-micros"}`, time.Date(2020, 1, 1, 3, 14, 15, 926000, time.UTC)},
		{"duration-fixed", `{"type":"fixed","name":"Dur","size":12,"logicalType":"duration"}`, avro.Duration{Months: 1, Days: 2, Milliseconds: 3}},
		// avro.Duration through the unsafe CONTAINER / POINTER field paths: the
		// bare duration-fixed row above only reaches the scalar-field unsafe arm
		// (usDuration). A nullable *avro.Duration field, a []avro.Duration field,
		// and a map[string]avro.Duration field reach the null-union / usArray /
		// usMap element handling for the duration leaf, held byte-identical to
		// their reflect twins.
		{"duration-ptr/nullable", `["null",{"type":"fixed","name":"DurP","size":12,"logicalType":"duration"}]`, func() *avro.Duration { d := avro.Duration{Months: 4, Days: 5, Milliseconds: 6}; return &d }()},
		{"duration-slice", `{"type":"array","items":{"type":"fixed","name":"DurA","size":12,"logicalType":"duration"}}`, []avro.Duration{{Months: 1}, {Days: 2}}},
		{"duration-map", `{"type":"map","values":{"type":"fixed","name":"DurM","size":12,"logicalType":"duration"}}`, map[string]avro.Duration{"k": {Milliseconds: 9}}},

		{"decimal", `{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}`, rat(1234, 100)},
		// uuid on a STRING carrier (usUUID/usFixedUUIDString string arm) —
		// another unsafe twin omitted from the battery.
		{"uuid-string", `{"type":"string","logicalType":"uuid"}`, "12345678-1234-1234-1234-123456789abc"},
		// Cross-carriers: a fixed+uuid schema with a Go STRING field exercises
		// usFixedUUIDString, and a string+uuid schema with a [16]byte field
		// exercises usUUID's byte arm — the two carrier/schema crossings the
		// uuid-fixed ([16]byte) and uuid-string (string) rows above don't reach.
		{"uuid-fixed/string-carrier", `{"type":"fixed","name":"FUS","size":16,"logicalType":"uuid"}`, "12345678-1234-5234-9234-123456789abc"},
		{"uuid-string/bytes-carrier", `{"type":"string","logicalType":"uuid"}`, [16]byte{0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x52, 0x34, 0x92, 0x34, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc}},
		// Non-time.Time / non-Duration carriers fall through to the raw usLong /
		// usInt arms; pin that the unsafe fallback matches the reflect fallback
		// for every long- and int-backed logical (a Duration / plain integer
		// carrier writes the raw wire value, no logical conversion).
		{"timestamp-millis/duration", `{"type":"long","logicalType":"timestamp-millis"}`, 90 * time.Minute},
		{"timestamp-nanos/duration", `{"type":"long","logicalType":"timestamp-nanos"}`, 90 * time.Minute},
		{"date/int32-carrier", `{"type":"int","logicalType":"date"}`, int32(19723)},
		{"time-millis/int32-carrier", `{"type":"int","logicalType":"time-millis"}`, int32(12345)},
		{"time-micros/int64-carrier", `{"type":"long","logicalType":"time-micros"}`, int64(12345678)},

		// Composites as struct fields (the unsafe array/map/union encoders).
		{"slice-int", `{"type":"array","items":"int"}`, []int32{1, 2, 3}},
		{"slice-record", `{"type":"array","items":{"type":"record","name":"AR","fields":[{"name":"n","type":"int"}]}}`, []inner{{1}, {2}}},
		{"slice-ptr-record", `{"type":"array","items":{"type":"record","name":"APR","fields":[{"name":"n","type":"int"}]}}`, []*inner{{1}, {2}}},
		{"map-int", `{"type":"map","values":"int"}`, map[string]int32{"a": 1}},
		{"nested-record", `{"type":"record","name":"NR","fields":[{"name":"n","type":"int"}]}`, inner{42}},
		{"ptr-int/nullable", `["null","int"]`, func() *int32 { x := int32(9); return &x }()},
		{"nil-ptr/nullable", `["null","int"]`, (*int32)(nil)},
		{"slice-null-union", `{"type":"array","items":["null","int"]}`, []*int32{nil, func() *int32 { x := int32(5); return &x }()}},
	}

	// Most rows round-trip value-faithfully, so the decoded field must equal the
	// input. The two TIME-OF-DAY logicals are the exception: time-millis and
	// time-micros encode only the clock time (ms/µs since midnight), and the
	// decoder reconstructs that time-of-day at the 1970-01-01 UTC epoch
	// reference — so a time.Time input keeps its time-of-day but NOT its date.
	// Compare those rows against the epoch-dated expected value rather than the
	// 2020-dated input (the input stays realistic; the date drop is documented
	// here, not hidden by picking a 1970 input). Other rows fall through to
	// r.value.
	expectedDecode := map[string]any{
		"time-millis/time": time.Date(1970, 1, 1, 3, 14, 15, 0, time.UTC),
		"time-micros/time": time.Date(1970, 1, 1, 3, 14, 15, 926000, time.UTC),
	}

	for _, r := range rows {
		t.Run(r.label, func(t *testing.T) {
			fieldS := avro.MustParse(r.schema)
			recS := avro.MustParse(fmt.Sprintf(
				`{"type":"record","name":"DP","fields":[{"name":"f","type":%s}]}`, r.schema))

			// Reflect path: encode the value top-level.
			topWire, err := fieldS.AppendEncode(nil, r.value)
			if err != nil {
				t.Fatalf("reflect (top-level) encode: %v", err)
			}

			// Unsafe path: the same value as the single field of an
			// ADDRESSABLE record struct. A single-field record's wire is
			// exactly the field's encoding, so the two MUST be byte-identical.
			st := reflect.StructOf([]reflect.StructField{
				{Name: "F", Type: reflect.TypeOf(r.value), Tag: `avro:"f"`},
			})
			pv := reflect.New(st) // pointer → addressable → unsafe field path
			pv.Elem().Field(0).Set(reflect.ValueOf(r.value))
			recWire, err := recS.AppendEncode(nil, pv.Interface())
			if err != nil {
				t.Fatalf("unsafe (struct-field) encode: %v", err)
			}

			if string(topWire) != string(recWire) {
				t.Fatalf("REFLECT↔UNSAFE WIRE DIVERGENCE for %s:\n reflect=%x\n unsafe =%x", r.label, topWire, recWire)
			}

			// Both wires must decode back through their own path. The reflect/
			// interface decode (into *any) is left a no-error SMOKE check: decode-
			// into-any yields the package's canonical Go types (int32 for int,
			// []any for arrays, map[string]any for records/maps), which do not
			// match the concrete input types of the composite rows (inner,
			// []inner, map[string]int32), so a value comparison here would be
			// brittle. The value guarantee is enforced on the unsafe side below,
			// where the field is type-aligned with the input.
			var topBack any
			if _, err := fieldS.Decode(topWire, &topBack); err != nil {
				t.Fatalf("reflect decode of own wire: %v", err)
			}
			recBack := reflect.New(st)
			if _, err := recS.Decode(recWire, recBack.Interface()); err != nil {
				t.Fatalf("unsafe decode of own wire: %v", err)
			}

			// Decode-no-error is not enough: a value-wrong-but-non-erroring
			// unsafe field decoder (e.g. a zeroed udDuration) would pass the
			// check above while corrupting the value. Assert the decoded field
			// round-trips to the expected value (the input, except for the
			// time-of-day rows above). matEqual handles time.Time (monotonic/loc
			// via Equal), *big.Rat (Cmp), and []byte, and DeepEqual-falls-through
			// for the slice/map/pointer rows; the field is type-aligned with
			// r.value (st was built from reflect.TypeOf).
			want := r.value
			if w, ok := expectedDecode[r.label]; ok {
				want = w
			}
			if got := recBack.Elem().Field(0).Interface(); !matEqual(got, want) {
				t.Fatalf("unsafe decode value mismatch for %s: got %#v, want %#v", r.label, got, want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Unsafe struct-field NARROWING parity.
//
// The addressable-struct-field decode path (unsafe.go's ud* closures) carries
// range guards of its own, written separately from the reflect path's. Nothing
// in the type system ties the two sets together, so each is free to drift: a
// missing bound there is a silent truncation or a sign wrap, which is
// wrong-value-no-error rather than a failure.
//
// A single-field record's wire IS its field's wire, so the SAME BYTES decoded
// into the SAME Go type through the two paths is the oracle, and it needs no
// expectation table: whatever the reflect path decides about a value is what
// the struct-field path must decide.
//
// Axes: narrowing pair (wire avro type × Go target type) × value class
// (over-max / under-min / in-range) × route (scalar field, *T field, []T field,
// []*T field). The routes are the axis a per-kind bound table cannot supply —
// the guard lives in the leaf closure, but only the composed route proves the
// leaf is still reached once a null-union or an array sits above it.
//
// Agreement is asserted on three things, not one. The accept/reject verdict;
// the REJECTION'S SHAPE, since an error naming the wrong Go type reads
// differently to a caller than the reflect path's does and a verdict-only check
// cannot see the difference; and the decoded value, since two paths that both
// accept can still land on different numbers.
// ---------------------------------------------------------------------------

// narrowRoute is one composition sitting between the record field and the
// narrowing leaf. schema wraps the leaf's schema text, goType wraps the leaf's
// Go type, and wide wraps a leaf-typed value into the shape the wrapped schema
// encodes — the three must describe the same shape or the row does not build.
type narrowRoute struct {
	name   string
	schema func(leaf string) string
	goType func(leaf reflect.Type) reflect.Type
	wide   func(leaf any) any
}

func narrowRoutes() []narrowRoute {
	one := func(t reflect.Type, v any) reflect.Value {
		s := reflect.MakeSlice(reflect.SliceOf(t), 1, 1)
		s.Index(0).Set(reflect.ValueOf(v))
		return s
	}
	ptr := func(v any) reflect.Value {
		p := reflect.New(reflect.TypeOf(v))
		p.Elem().Set(reflect.ValueOf(v))
		return p
	}
	return []narrowRoute{{
		name:   "scalar",
		schema: func(leaf string) string { return leaf },
		goType: func(leaf reflect.Type) reflect.Type { return leaf },
		wide:   func(leaf any) any { return leaf },
	}, {
		name:   "ptr",
		schema: func(leaf string) string { return `["null",` + leaf + `]` },
		goType: reflect.PointerTo,
		wide:   func(leaf any) any { return ptr(leaf).Interface() },
	}, {
		name:   "slice",
		schema: func(leaf string) string { return `{"type":"array","items":` + leaf + `}` },
		goType: reflect.SliceOf,
		wide:   func(leaf any) any { return one(reflect.TypeOf(leaf), leaf).Interface() },
	}, {
		name:   "slice-ptr",
		schema: func(leaf string) string { return `{"type":"array","items":["null",` + leaf + `]}` },
		goType: func(leaf reflect.Type) reflect.Type { return reflect.SliceOf(reflect.PointerTo(leaf)) },
		wide: func(leaf any) any {
			return one(reflect.PointerTo(reflect.TypeOf(leaf)), ptr(leaf).Interface()).Interface()
		},
	}}
}

// narrowRows is the narrowing-pair axis: one row per (wire type, Go target)
// crossing whose guard can reject. `wide` is a Go type the wire accepts without
// narrowing, so the probe values are written faithfully and only the READ side
// narrows. A nil over/under means the crossing has no such value — an int32
// wire cannot exceed uint32's range — and the class is skipped rather than
// faked.
var narrowRows = []struct {
	label              string
	wire               string
	narrow             reflect.Type
	over, under, inRng any
}{
	// int wire (int32 carrier).
	{"int/int8", `"int"`, reflect.TypeFor[int8](), int32(200), int32(-200), int32(100)},
	{"int/int16", `"int"`, reflect.TypeFor[int16](), int32(40000), int32(-40000), int32(1000)},
	{"int/int32", `"int"`, reflect.TypeFor[int32](), nil, nil, int32(1 << 30)},
	{"int/int64", `"int"`, reflect.TypeFor[int64](), nil, nil, int32(1 << 30)},
	{"int/int", `"int"`, reflect.TypeFor[int](), nil, nil, int32(1 << 30)},
	{"int/uint8", `"int"`, reflect.TypeFor[uint8](), int32(300), int32(-1), int32(7)},
	{"int/uint16", `"int"`, reflect.TypeFor[uint16](), int32(70000), int32(-1), int32(7)},
	{"int/uint32", `"int"`, reflect.TypeFor[uint32](), nil, int32(-1), int32(7)},
	{"int/uint64", `"int"`, reflect.TypeFor[uint64](), nil, int32(-1), int32(7)},
	{"int/uint", `"int"`, reflect.TypeFor[uint](), nil, int32(-1), int32(7)},

	// long wire (int64 carrier).
	{"long/int8", `"long"`, reflect.TypeFor[int8](), int64(200), int64(-200), int64(100)},
	{"long/int16", `"long"`, reflect.TypeFor[int16](), int64(40000), int64(-40000), int64(1000)},
	{"long/int32", `"long"`, reflect.TypeFor[int32](), int64(1) << 33, -(int64(1) << 33), int64(1 << 30)},
	{"long/int64", `"long"`, reflect.TypeFor[int64](), nil, nil, int64(1) << 40},
	{"long/uint8", `"long"`, reflect.TypeFor[uint8](), int64(300), int64(-1), int64(7)},
	{"long/uint16", `"long"`, reflect.TypeFor[uint16](), int64(100000), int64(-1), int64(7)},
	{"long/uint32", `"long"`, reflect.TypeFor[uint32](), int64(1) << 33, int64(-1), int64(7)},
	{"long/uint64", `"long"`, reflect.TypeFor[uint64](), nil, int64(-1), int64(7)},

	// double wire (float64 carrier): the narrowing is range, not width — a
	// finite float64 outside float32's range becomes ±Inf, which is a value
	// change the caller did not ask for. ±Inf and NaN are NOT overflow and
	// must pass, so they ride the in-range class of their own rows.
	{"double/float32", `"double"`, reflect.TypeFor[float32](), 1e300, -1e300, 1.5},
	{"double/float32-inf", `"double"`, reflect.TypeFor[float32](), nil, nil, math.Inf(1)},
	{"double/float32-neginf", `"double"`, reflect.TypeFor[float32](), nil, nil, math.Inf(-1)},
	{"double/float32-nan", `"double"`, reflect.TypeFor[float32](), nil, nil, math.NaN()},
	{"double/float64", `"double"`, reflect.TypeFor[float64](), nil, nil, 2.5},

	// float wire (float32 carrier): four bytes cannot overflow either Go
	// float, so these rows are the control that the parity itself is not
	// vacuously satisfied by two paths that both reject everything.
	{"float/float32", `"float"`, reflect.TypeFor[float32](), nil, nil, float32(1.5)},
	{"float/float64", `"float"`, reflect.TypeFor[float64](), nil, nil, float32(1.5)},
}

// narrowEqual compares two decoded route-shaped values. It peels the pointer
// and slice compositions the routes build and leans on matEqual at the leaf,
// which is where NaN's self-inequality has to be handled: reflect.DeepEqual
// reports two NaN-carrying slices unequal, so the double/float32-nan rows need
// the leaf rule carried through the composition rather than applied only at the
// scalar route.
func narrowEqual(a, b reflect.Value) bool {
	if a.Type() != b.Type() {
		return false
	}
	switch a.Kind() {
	case reflect.Pointer:
		if a.IsNil() || b.IsNil() {
			return a.IsNil() == b.IsNil()
		}
		return narrowEqual(a.Elem(), b.Elem())
	case reflect.Slice:
		if a.Len() != b.Len() {
			return false
		}
		for i := range a.Len() {
			if !narrowEqual(a.Index(i), b.Index(i)) {
				return false
			}
		}
		return true
	default:
		return matEqual(a.Interface(), b.Interface())
	}
}

// semShape reports the *SemanticError fields a caller reads off a rejection.
// A non-SemanticError rejection reports ok=false, which is itself a divergence
// when the other path produced one.
func semShape(err error) (avroType string, goType reflect.Type, ok bool) {
	var se *avro.SemanticError
	if !errors.As(err, &se) {
		return "", nil, false
	}
	return se.AvroType, se.GoType, true
}

func TestMatrix_UnsafeFieldNarrowingDecodeParity(t *testing.T) {
	classes := []string{"over-max", "under-min", "in-range"}
	for _, row := range narrowRows {
		for _, rt := range narrowRoutes() {
			for _, class := range classes {
				var probe any
				switch class {
				case "over-max":
					probe = row.over
				case "under-min":
					probe = row.under
				default:
					probe = row.inRng
				}
				if probe == nil {
					continue // the crossing has no value in this class
				}
				t.Run(row.label+"/"+rt.name+"/"+class, func(t *testing.T) {
					fieldSchema := rt.schema(row.wire)
					leafS := avro.MustParse(fieldSchema)
					recS := avro.MustParse(fmt.Sprintf(
						`{"type":"record","name":"NR","fields":[{"name":"f","type":%s}]}`, fieldSchema))

					// The wire is written through the WIDE Go type, so the
					// bytes carry the probe value exactly and only the read
					// side narrows.
					wire, err := leafS.AppendEncode(nil, rt.wide(probe))
					if err != nil {
						t.Fatalf("encode probe %v: %v", probe, err)
					}

					target := rt.goType(row.narrow)

					// Reflect path: a top-level target of the narrow type.
					safeDst := reflect.New(target)
					_, safeErr := leafS.Decode(wire, safeDst.Interface())

					// Unsafe path: the same bytes as the single field of an
					// addressable struct, which is what selects the ud* path.
					st := reflect.StructOf([]reflect.StructField{
						{Name: "F", Type: target, Tag: `avro:"f"`},
					})
					unsafeDst := reflect.New(st)
					_, unsafeErr := recS.Decode(wire, unsafeDst.Interface())

					if (safeErr == nil) != (unsafeErr == nil) {
						t.Fatalf("VERDICT DIVERGENCE decoding %v into %s:\n reflect=%v\n unsafe =%v",
							probe, target, safeErr, unsafeErr)
					}
					if safeErr != nil {
						sAvro, sGo, sOK := semShape(safeErr)
						uAvro, uGo, uOK := semShape(unsafeErr)
						if !sOK || !uOK {
							t.Fatalf("rejection is not a *SemanticError on both paths (reflect ok=%v, unsafe ok=%v):\n reflect=%v\n unsafe =%v",
								sOK, uOK, safeErr, unsafeErr)
						}
						if sAvro != uAvro {
							t.Errorf("AvroType divergence: reflect=%q unsafe=%q", sAvro, uAvro)
						}
						if sGo != uGo {
							t.Errorf("GoType divergence: reflect=%v unsafe=%v", sGo, uGo)
						}
						if uGo == nil {
							t.Errorf("unsafe rejection carries no GoType; the caller cannot tell which Go field overflowed")
						}
						return
					}
					if got, want := unsafeDst.Elem().Field(0), safeDst.Elem(); !narrowEqual(got, want) {
						t.Fatalf("VALUE DIVERGENCE: unsafe=%#v reflect=%#v", got.Interface(), want.Interface())
					}
				})
			}
		}
	}
}

// narrowEncodeRows is the decode matrix's mirror axis: a Go type WIDER than the
// wire type it is written to, so the guard being crossed is the us* range check
// rather than the ud* one. The float rows are the deliberate exception and the
// control at once — the lossy-destination policy accepts a finite float64 that
// becomes ±Inf on a float wire, so a matrix in which every out-of-range value
// rejected would be describing a rule this package does not have.
var narrowEncodeRows = []struct {
	label              string
	wire               string
	over, under, inRng any
}{
	{"int64/int", `"int"`, int64(math.MaxInt32) + 1, int64(math.MinInt32) - 1, int64(7)},
	{"uint32/int", `"int"`, uint32(math.MaxInt32) + 1, nil, uint32(7)},
	{"uint64/int", `"int"`, uint64(math.MaxInt32) + 1, nil, uint64(7)},
	{"uint64/long", `"long"`, uint64(math.MaxInt64) + 1, nil, uint64(7)},
	{"int8/int", `"int"`, nil, nil, int8(7)},
	{"uint8/int", `"int"`, nil, nil, uint8(7)},
	{"int16/long", `"long"`, nil, nil, int16(7)},
	{"float64/float", `"float"`, nil, nil, 1e300},
	{"float64/double", `"double"`, nil, nil, 1e300},
	{"float32/double", `"double"`, nil, nil, float32(1.5)},
	// int and uint are int32-wide on a 32-bit build, where no value of theirs
	// can overflow an int wire. math.MaxInt is an untyped constant, so the
	// guard resolves at compile time and the rows carry a probe only where one
	// exists rather than asserting an overflow that cannot happen.
	{"int/int", `"int"`, intOverflowProbe(), intUnderflowProbe(), int(7)},
	{"uint/int", `"int"`, uintOverflowProbe(), nil, uint(7)},
}

func intOverflowProbe() any {
	if math.MaxInt > math.MaxInt32 {
		return int(math.MaxInt32) + 1
	}
	return nil
}

func intUnderflowProbe() any {
	if math.MinInt < math.MinInt32 {
		return int(math.MinInt32) - 1
	}
	return nil
}

func uintOverflowProbe() any {
	if math.MaxUint > math.MaxInt32 {
		return uint(math.MaxInt32) + 1
	}
	return nil
}

func TestMatrix_UnsafeFieldNarrowingEncodeParity(t *testing.T) {
	classes := []string{"over-max", "under-min", "in-range"}
	for _, row := range narrowEncodeRows {
		for _, rt := range narrowRoutes() {
			for _, class := range classes {
				var probe any
				switch class {
				case "over-max":
					probe = row.over
				case "under-min":
					probe = row.under
				default:
					probe = row.inRng
				}
				if probe == nil {
					continue
				}
				t.Run(row.label+"/"+rt.name+"/"+class, func(t *testing.T) {
					fieldSchema := rt.schema(row.wire)
					leafS := avro.MustParse(fieldSchema)
					recS := avro.MustParse(fmt.Sprintf(
						`{"type":"record","name":"NE","fields":[{"name":"f","type":%s}]}`, fieldSchema))

					value := rt.wide(probe)
					goType := rt.goType(reflect.TypeOf(probe))

					// Reflect path: the value encoded at top level.
					safeWire, safeErr := leafS.AppendEncode(nil, value)

					// Unsafe path: the same value as the single field of an
					// addressable struct.
					st := reflect.StructOf([]reflect.StructField{
						{Name: "F", Type: goType, Tag: `avro:"f"`},
					})
					pv := reflect.New(st)
					pv.Elem().Field(0).Set(reflect.ValueOf(value))
					unsafeWire, unsafeErr := recS.AppendEncode(nil, pv.Interface())

					if (safeErr == nil) != (unsafeErr == nil) {
						t.Fatalf("VERDICT DIVERGENCE encoding %v (%s):\n reflect=%v\n unsafe =%v",
							probe, goType, safeErr, unsafeErr)
					}
					if safeErr != nil {
						sAvro, sGo, sOK := semShape(safeErr)
						uAvro, uGo, uOK := semShape(unsafeErr)
						if !sOK || !uOK {
							t.Fatalf("rejection is not a *SemanticError on both paths (reflect ok=%v, unsafe ok=%v):\n reflect=%v\n unsafe =%v",
								sOK, uOK, safeErr, unsafeErr)
						}
						if sAvro != uAvro {
							t.Errorf("AvroType divergence: reflect=%q unsafe=%q", sAvro, uAvro)
						}
						if sGo != uGo {
							t.Errorf("GoType divergence: reflect=%v unsafe=%v", sGo, uGo)
						}
						if uGo == nil {
							t.Errorf("unsafe rejection carries no GoType; the caller cannot tell which Go field overflowed")
						}
						return
					}
					// A single-field record's wire IS its field's wire.
					if !bytes.Equal(safeWire, unsafeWire) {
						t.Fatalf("WIRE DIVERGENCE: reflect=%x unsafe=%x", safeWire, unsafeWire)
					}
				})
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Unsafe array BLOCK-COUNT bound parity.
//
// A block header states how many items follow. Believing it allocates that many
// slots before a single item is read, so both array decoders bound the count by
// what the remaining bytes could hold — and that bound is item-aware: it divides
// by the element's minimum encoded size, which differs per element type. The
// unsafe path computes its own copy of that minimum.
//
// A path that keeps the guard but loses the DIVISOR still rejects the wildly
// hostile counts and admits the merely large ones, so the oracle here is the
// reflect path's error TEXT rather than the bare verdict: the text carries the
// minimum, which makes a drifting divisor visible where an accept/reject
// comparison reports agreement.
// ---------------------------------------------------------------------------

func TestMatrix_UnsafeArrayBlockBoundParity(t *testing.T) {
	// Element schemas chosen so the per-item minimum takes several distinct
	// values: 4, 8, 1, the fixed's own size, and 0 — the last selecting the
	// zero-byte element-count cap instead of the division, a different arm of
	// the same guard.
	//
	// The fixed8 and null rows are carried by the reflect decoders on all
	// three targets: dropping the unsafe path's divisor moves neither of them
	// while it moves every other row, which is how their element types are
	// known to decline the unsafe array path rather than assumed to. They stay
	// as the typed-vs-any half of the parity, and as the cells that reach the
	// zero-minimum arm at all.
	// oneItem is the element's own smallest legal encoding, which the
	// two-block cells put in front of the hostile header so the second block
	// is reached with the slice already populated.
	elems := []struct {
		name    string
		schema  string
		goElem  reflect.Type
		oneItem []byte
	}{
		{"float", `"float"`, reflect.TypeFor[float32](), []byte{0, 0, 0, 0}},
		{"double", `"double"`, reflect.TypeFor[float64](), []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{"long", `"long"`, reflect.TypeFor[int64](), []byte{0}},
		{"boolean", `"boolean"`, reflect.TypeFor[bool](), []byte{0}},
		{"string", `"string"`, reflect.TypeFor[string](), []byte{0}},
		{"fixed8", `{"type":"fixed","name":"BB8","size":8}`, reflect.TypeFor[[8]byte](), []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{"null", `"null"`, reflect.TypeFor[any](), nil},
		// Record elements take the OTHER unsafe array decoder
		// (udArrayPtrRecord, which is selected by a single-pointer element),
		// so the element axis spans both of them rather than only the
		// primitive one. The empty record is also a second zero-minimum
		// element, reached through that decoder instead of the reflect one.
		{"ptr-record-empty", `{"type":"record","name":"BPE","fields":[]}`, reflect.TypeFor[*struct{}](), nil},
		{"ptr-record-long", `{"type":"record","name":"BPL","fields":[{"name":"n","type":"long"}]}`,
			reflect.PointerTo(reflect.StructOf([]reflect.StructField{
				{Name: "N", Type: reflect.TypeFor[int64](), Tag: `avro:"n"`},
			})), []byte{0}},
	}
	// A case is a leading valid block (0 items = none) and the count the
	// hostile block that follows it declares.
	//
	// Three hostile counts: one that only an item-aware bound rejects for the
	// wider elements, one past every arm of the guard, and one within a few
	// thousand of MaxInt64. Each is driven from both block positions, because a
	// hostile FIRST block meets a full buffer and an empty slice while a hostile
	// SECOND block meets a shortened buffer, a non-zero running item total, and
	// a slice that already has a length to add to.
	//
	// cap-straddle is the case those six cannot express. The zero-byte element
	// cap is the only arm of the guard the running total participates in, and it
	// only shows when the two blocks are individually under the cap and jointly
	// over it — 2000 then 2500. A count far above the cap is rejected whether or
	// not the total accumulates, so it measures the cap and not the running sum.
	type boundCase struct {
		name        string
		lead, count int64
	}
	var cases []boundCase
	for _, c := range []int64{1000, 1 << 40, math.MaxInt64 - 3000} {
		cases = append(cases,
			boundCase{fmt.Sprintf("first-block/count=%d", c), 0, c},
			boundCase{fmt.Sprintf("second-block/count=%d", c), 1, c})
	}
	cases = append(cases, boundCase{"cap-straddle", 2000, 2500})

	for _, e := range elems {
		{
			for _, pos := range cases {
				count := pos.count
				t.Run(fmt.Sprintf("%s/%s", e.name, pos.name), func(t *testing.T) {
					schema := fmt.Sprintf(`{"type":"array","items":%s}`, e.schema)
					leafS := avro.MustParse(schema)
					recS := avro.MustParse(fmt.Sprintf(
						`{"type":"record","name":"BR","fields":[{"name":"f","type":%s}]}`, schema))

					// Optionally a valid leading block, then a block whose
					// header lies, eight payload bytes, then the end-of-blocks
					// terminator.
					var wire []byte
					if pos.lead > 0 {
						wire = binary.AppendVarint(wire, pos.lead)
						for range pos.lead {
							wire = append(wire, e.oneItem...)
						}
					}
					wire = binary.AppendVarint(wire, count)
					wire = append(wire, 0, 0, 0, 0, 0, 0, 0, 0)
					wire = append(wire, 0)

					var anyDst any
					_, anyErr := leafS.Decode(wire, &anyDst)

					typedDst := reflect.New(reflect.SliceOf(e.goElem))
					_, typedErr := leafS.Decode(wire, typedDst.Interface())

					st := reflect.StructOf([]reflect.StructField{
						{Name: "F", Type: reflect.SliceOf(e.goElem), Tag: `avro:"f"`},
					})
					_, unsafeErr := recS.Decode(wire, reflect.New(st).Interface())

					for _, other := range []struct {
						name string
						err  error
					}{{"reflect-typed", typedErr}, {"unsafe-field", unsafeErr}} {
						if (anyErr == nil) != (other.err == nil) {
							t.Fatalf("VERDICT DIVERGENCE vs %s:\n reflect-any=%v\n %s=%v",
								other.name, anyErr, other.name, other.err)
						}
						// The struct-field path prefixes the field's context
						// onto whatever the field's decoder returned, so the
						// bound text is the SUFFIX rather than the whole
						// string. Requiring it verbatim there still pins the
						// divisor: a per-item minimum that drifted would print
						// a different number.
						if anyErr != nil && !strings.HasSuffix(other.err.Error(), anyErr.Error()) {
							t.Errorf("BOUND DIVERGENCE vs %s:\n reflect-any  = %s\n %-12s = %s",
								other.name, anyErr.Error(), other.name, other.err.Error())
						}
					}
				})
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Decimal MAGNITUDE x TARGET parity across the wire axis.
//
// setDecimalRat is shared by the binary decoder and both JSON decimal arms, and
// its float guards are the only thing standing between an out-of-range decimal
// and a target silently set to +Inf. Sharing a helper is not the same as
// reaching it: the JSON arms route to it from their own call sites, and a route
// that stopped doing so would fall through to the byte/string handlers, which
// still ERROR for a float target — so a test that only asks "did it fail" sees
// nothing. The suite's overflow assertions were all on `Decode`.
//
// The oracle is the BINARY decode of the same value into the same Go type: no
// expectation table, and the wire axis is what it measures. Where both accept,
// the decoded values must match too — the guards are about a value being
// silently changed, so a verdict-only comparison would miss the thing they
// exist to prevent.
// ---------------------------------------------------------------------------

func TestMatrix_DecimalMagnitudeTargetParity(t *testing.T) {
	// The fixed carrier is sized to hold the widest unscaled value here, so
	// every magnitude rides both carriers and the axis stays crossed.
	const fixedSize = 176

	mags := []struct {
		name      string
		lit       string
		precision int
		scale     int
	}{
		{"in-range", "1234.56", 10, 2},
		// Finite in float64, +Inf in float32: the narrowing guard.
		{"float32-overflow", "1e39", 45, 0},
		// Past float64 itself: big.Rat.Float64 returns +Inf, the other guard.
		{"float64-overflow", "1e310", 400, 0},
	}
	carriers := []struct {
		name   string
		schema func(precision, scale int) string
	}{
		{"bytes", func(p, s int) string {
			return fmt.Sprintf(`{"type":"bytes","logicalType":"decimal","precision":%d,"scale":%d}`, p, s)
		}},
		{"fixed", func(p, s int) string {
			return fmt.Sprintf(`{"type":"fixed","name":"DM","size":%d,"logicalType":"decimal","precision":%d,"scale":%d}`, fixedSize, p, s)
		}},
	}
	targets := []struct {
		name string
		make func() any
	}{
		{"big.Rat", func() any { return new(big.Rat) }},
		{"float64", func() any { return new(float64) }},
		{"float32", func() any { return new(float32) }},
		{"string", func() any { return new(string) }},
		{"json.Number", func() any { return new(json.Number) }},
		{"any", func() any { return new(any) }},
	}

	for _, mag := range mags {
		for _, c := range carriers {
			s := avro.MustParse(c.schema(mag.precision, mag.scale))
			r, ok := new(big.Rat).SetString(mag.lit)
			if !ok {
				t.Fatalf("%s: bad literal", mag.lit)
			}
			binWire, err := s.AppendEncode(nil, r)
			if err != nil {
				t.Fatalf("%s/%s binary encode: %v", mag.name, c.name, err)
			}
			specJSON, err := s.AppendEncodeJSON(nil, r)
			if err != nil {
				t.Fatalf("%s/%s json encode: %v", mag.name, c.name, err)
			}
			// The two JSON input forms a decimal accepts: the spec's
			// codepoint-mapped string, and the lenient bare number. Both
			// reach setDecimalRat, by different routes.
			forms := []struct {
				name string
				body []byte
			}{
				{"json-spec", specJSON},
				{"json-bare", []byte(mag.lit)},
			}

			for _, tgt := range targets {
				for _, form := range forms {
					t.Run(mag.name+"/"+c.name+"/"+tgt.name+"/"+form.name, func(t *testing.T) {
						binDst := tgt.make()
						_, binErr := s.Decode(binWire, binDst)

						jsonDst := tgt.make()
						jsonErr := s.DecodeJSON(form.body, jsonDst)

						if (binErr == nil) != (jsonErr == nil) {
							t.Fatalf("VERDICT DIVERGENCE:\n binary=%v\n json  =%v", binErr, jsonErr)
						}
						if binErr != nil {
							return
						}
						got := reflect.ValueOf(jsonDst).Elem().Interface()
						want := reflect.ValueOf(binDst).Elem().Interface()
						if !matEqual(got, want) {
							t.Fatalf("VALUE DIVERGENCE: json=%#v binary=%#v", got, want)
						}
						// Agreeing is not enough when the two paths agree by
						// SHARING a helper: strip setDecimalRat's guards and
						// binary and JSON accept in lockstep, so a
						// cross-wire comparison stays green while both
						// silently write +Inf. Every literal in the table is
						// finite, which makes an infinite result a value the
						// caller never supplied — checked against the input,
						// not against the sibling path.
						for _, v := range []struct {
							wire string
							val  any
						}{{"binary", want}, {"json", got}} {
							var f float64
							switch n := v.val.(type) {
							case float64:
								f = n
							case float32:
								f = float64(n)
							default:
								continue
							}
							if math.IsInf(f, 0) {
								t.Fatalf("%s decode of the finite decimal %s silently produced %v in a %s target",
									v.wire, mag.lit, f, tgt.name)
							}
						}
					})
				}
			}
		}
	}
}

// ---------- matrix_evolution_test.go ----------

// ---------------------------------------------------------------------------
// Evolution matrix: writer→reader schema changes across every promotion
// pair × composition context, per-kind field add (resolution default fill) /
// drop (the skip path, per kind) / reorder, union evolution (reorder,
// widening, two-pass exact-before-promotion, fail-fast narrowing), enum
// reader-defaults, and alias renames — with Resolve ⇔ CheckCompatibility
// agreement asserted on every pair.
// ---------------------------------------------------------------------------

// resolveBoth asserts Resolve and CheckCompatibility agree on success.
func resolveBoth(t *testing.T, w, r *avro.Schema) (*avro.Schema, error) {
	t.Helper()
	res, rerr := avro.Resolve(w, r)
	cerr := avro.CheckCompatibility(w, r)
	if (rerr == nil) != (cerr == nil) {
		t.Fatalf("Resolve and CheckCompatibility disagree: resolve=%v compat=%v", rerr, cerr)
	}
	return res, rerr
}

func TestMatrix_PromotionPairsByContext(t *testing.T) {
	pairs := []struct {
		wKind, rKind string
		wVal, rVal   any
	}{
		{"int", "long", int32(-77), int64(-77)},
		{"int", "float", int32(123), float32(123)},
		{"int", "double", int32(-9), float64(-9)},
		{"long", "float", int64(1 << 10), float32(1 << 10)},
		{"long", "double", int64(-5), float64(-5)},
		{"float", "double", float32(1.5), float64(1.5)},
		{"string", "bytes", "sb", []byte("sb")},
		{"bytes", "string", []byte("bs"), "bs"},
	}
	for _, p := range pairs {
		for _, cx := range matCtxs() {
			if cx.skip != nil && (cx.skip(p.wKind) || cx.skip(p.rKind)) {
				continue
			}
			t.Run(fmt.Sprintf("%s→%s/%s", p.wKind, p.rKind, cx.label), func(t *testing.T) {
				uw, ur := &uniq{}, &uniq{}
				wSchema := cx.schema(fmt.Sprintf("%q", p.wKind), p.wKind, uw)
				rSchema := cx.schema(fmt.Sprintf("%q", p.rKind), p.rKind, ur)
				w := avro.MustParse(wSchema)
				r := avro.MustParse(rSchema)
				res, err := resolveBoth(t, w, r)
				if err != nil {
					t.Fatalf("Resolve: %v\nw: %s\nr: %s", err, wSchema, rSchema)
				}
				wire, err := w.AppendEncode(nil, cx.wrap(p.wVal))
				if err != nil {
					t.Fatalf("writer encode: %v", err)
				}
				var got any
				if _, err := res.Decode(wire, &got); err != nil {
					t.Fatalf("resolved decode: %v", err)
				}
				wantWire, err := r.AppendEncode(nil, cx.wrap(p.rVal))
				if err != nil {
					t.Fatalf("reader encode: %v", err)
				}
				gotWire, err := r.AppendEncode(nil, got)
				if err != nil || !bytes.Equal(gotWire, wantWire) {
					t.Fatalf("promoted tree wire differs: err=%v\n got=%x\nwant=%x\nvalue: %#v", err, gotWire, wantWire, got)
				}
			})
		}
	}
}

// Field DROP per kind: the writer carries a field of every kind that the
// reader lacks — the resolved decode must SKIP it (exercising every skipfn)
// and preserve the surrounding fields. Also nested inside an array.
func TestMatrix_FieldDropPerKind(t *testing.T) {
	kinds := []struct {
		label  string
		schema string
		value  any
	}{
		{"null", `"null"`, nil},
		{"boolean", `"boolean"`, true},
		{"int", `"int"`, int32(7)},
		{"long", `"long"`, int64(1 << 60)},
		{"float", `"float"`, float32(1.5)},
		{"double", `"double"`, 2.25},
		{"string", `"string"`, "drop"},
		{"bytes", `"bytes"`, []byte{1, 2}},
		{"enum", `{"type":"enum","name":"SKE","symbols":["A","B"]}`, "B"},
		{"fixed2", `{"type":"fixed","name":"SKF","size":2}`, []byte{3, 4}},
		{"fixed0", `{"type":"fixed","name":"SKF0","size":0}`, []byte{}},
		{"array", `{"type":"array","items":"int"}`, []any{int32(1), int32(2)}},
		{"array-empty", `{"type":"array","items":"int"}`, []any{}},
		{"map", `{"type":"map","values":"string"}`, map[string]any{"k": "v"}},
		{"record", `{"type":"record","name":"SKR","fields":[{"name":"i","type":"int"},{"name":"s","type":"string"}]}`,
			map[string]any{"i": int32(9), "s": "x"}},
		{"nullunion", `["null","int"]`, int32(5)},
		{"nullunion-nil", `["null","int"]`, nil},
		{"multibranch", `["null","boolean","int","string"]`, "u"},
		{"logical-ts", `{"type":"long","logicalType":"timestamp-millis"}`, int64(1717243496789)},
		{"decimal", `{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}`, []byte{0x30, 0x39}},
		{"recursive", `{"type":"record","name":"SKN","fields":[{"name":"v","type":"int"},{"name":"next","type":["null","SKN"],"default":null}]}`,
			map[string]any{"v": int32(1), "next": map[string]any{"v": int32(2), "next": nil}}},
	}
	for _, k := range kinds {
		wSchema := fmt.Sprintf(`{"type":"record","name":"R","fields":[
			{"name":"pre","type":"string"},
			{"name":"dropme","type":%s},
			{"name":"post","type":"long"}]}`, k.schema)
		rSchema := `{"type":"record","name":"R","fields":[
			{"name":"pre","type":"string"},
			{"name":"post","type":"long"}]}`
		t.Run(k.label, func(t *testing.T) {
			w := avro.MustParse(wSchema)
			r := avro.MustParse(rSchema)
			res, err := resolveBoth(t, w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			wire, err := w.AppendEncode(nil, map[string]any{"pre": "p", "dropme": k.value, "post": int64(11)})
			if err != nil {
				t.Fatalf("writer encode: %v", err)
			}
			var got map[string]any
			if _, err := res.Decode(wire, &got); err != nil {
				t.Fatalf("resolved decode (skip %s): %v", k.label, err)
			}
			if got["pre"] != "p" || got["post"] != int64(11) || len(got) != 2 {
				t.Fatalf("surrounding fields corrupted by skip: %#v", got)
			}
		})
		// Same drop with the record as an array item (skip inside blocks).
		t.Run(k.label+"/in-array", func(t *testing.T) {
			w := avro.MustParse(fmt.Sprintf(`{"type":"array","items":%s}`, wSchema))
			r := avro.MustParse(fmt.Sprintf(`{"type":"array","items":%s}`, rSchema))
			res, err := resolveBoth(t, w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			item := map[string]any{"pre": "p", "dropme": k.value, "post": int64(3)}
			wire, err := w.AppendEncode(nil, []any{item, item})
			if err != nil {
				t.Fatalf("writer encode: %v", err)
			}
			var got []any
			if _, err := res.Decode(wire, &got); err != nil {
				t.Fatalf("resolved decode: %v", err)
			}
			if len(got) != 2 || got[1].(map[string]any)["post"] != int64(3) {
				t.Fatalf("array skip corrupted items: %#v", got)
			}
		})
	}
}

// Field ADD per kind: the reader declares a defaulted field the writer
// lacks — the resolution fill must agree with the reader's own JSON fill
// and re-encode onto the reader's auto-fill wire.
func TestMatrix_FieldAddPerKind(t *testing.T) {
	kinds := []struct {
		label      string
		fieldType  string
		defaultLit string
	}{
		{"boolean", `"boolean"`, `true`},
		{"int", `"int"`, `7`},
		{"long", `"long"`, `9007199254740993`},
		{"float", `"float"`, `1.5`},
		{"double", `"double"`, `-2.25`},
		{"string", `"string"`, `"d"`},
		{"bytes", `"bytes"`, `"\u00ff"`},
		{"bytes-empty", `"bytes"`, `""`},
		{"enum", `{"type":"enum","name":"ADE","symbols":["A","B"]}`, `"B"`},
		{"fixed1", `{"type":"fixed","name":"ADF","size":1}`, `"\u00ab"`},
		{"fixed0", `{"type":"fixed","name":"ADF0","size":0}`, `""`},
		{"date", `{"type":"int","logicalType":"date"}`, `19723`},
		{"timestamp", `{"type":"long","logicalType":"timestamp-millis"}`, `1717243496789`},
		{"nullunion", `["null","int"]`, `null`},
		{"union-int-first", `["int","string"]`, `42`},
		{"array", `{"type":"array","items":"int"}`, `[1,2]`},
		{"map", `{"type":"map","values":"string"}`, `{"k":"v"}`},
		{"record", `{"type":"record","name":"ADR","fields":[{"name":"i","type":"int"}]}`, `{"i":3}`},
		{"empty-record", `{"type":"record","name":"ADER","fields":[]}`, `{}`},
	}
	for _, k := range kinds {
		t.Run(k.label, func(t *testing.T) {
			wSchema := `{"type":"record","name":"R","fields":[{"name":"pre","type":"string"}]}`
			rSchema := fmt.Sprintf(`{"type":"record","name":"R","fields":[
				{"name":"pre","type":"string"},
				{"name":"f","type":%s,"default":%s}]}`, k.fieldType, k.defaultLit)
			w := avro.MustParse(wSchema)
			r := avro.MustParse(rSchema)
			res, err := resolveBoth(t, w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			wire, err := w.AppendEncode(nil, map[string]any{"pre": "p"})
			if err != nil {
				t.Fatalf("writer encode: %v", err)
			}
			var got any
			if _, err := res.Decode(wire, &got); err != nil {
				t.Fatalf("resolution default fill: %v", err)
			}
			// The reader's own JSON fill is the reference.
			var jfill map[string]any
			if err := r.DecodeJSON([]byte(`{"pre":"p"}`), &jfill); err != nil {
				t.Fatalf("reader JSON fill: %v", err)
			}
			if !matEqual(got, jfill) {
				t.Fatalf("resolution fill diverges from JSON fill:\n res=%#v\njson=%#v", got, jfill)
			}
			// And re-encodes onto the reader's auto-fill wire.
			wantWire, err := r.AppendEncode(nil, map[string]any{"pre": "p"})
			if err != nil {
				t.Fatalf("reader auto-fill encode: %v", err)
			}
			gotWire, err := r.AppendEncode(nil, got)
			if err != nil || !bytes.Equal(gotWire, wantWire) {
				t.Fatalf("filled tree wire differs: err=%v\n got=%x\nwant=%x", err, gotWire, wantWire)
			}
		})
	}
}

// Field REORDER: same fields, different declaration order — resolution maps
// by name; every value must land on the right reader field.
func TestMatrix_FieldReorder(t *testing.T) {
	w := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"},
		{"name":"c","type":"bytes"},
		{"name":"d","type":["null","long"],"default":null}]}`)
	r := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"d","type":["null","long"],"default":null},
		{"name":"c","type":"bytes"},
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}]}`)
	res, err := resolveBoth(t, w, r)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	wire, err := w.AppendEncode(nil, map[string]any{
		"a": int32(1), "b": "two", "c": []byte{3}, "d": int64(4)})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got map[string]any
	mustDecode(t, res, wire, &got)
	if got["a"] != int32(1) || got["b"] != "two" || !bytes.Equal(got["c"].([]byte), []byte{3}) || got["d"] != int64(4) {
		t.Fatalf("reordered fields mismatched: %#v", got)
	}
}

func TestMatrix_UnionEvolution(t *testing.T) {
	t.Run("branch-reorder", func(t *testing.T) {
		w := avro.MustParse(`["int","string"]`)
		r := avro.MustParse(`["string","int"]`)
		res, err := resolveBoth(t, w, r)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		for _, v := range []any{int32(5), "s"} {
			wire, _ := w.AppendEncode(nil, v)
			var got any
			if _, err := res.Decode(wire, &got); err != nil {
				t.Fatalf("decode %v: %v", v, err)
			}
			if !matEqual(got, v) {
				t.Fatalf("reordered union value mismatch: %#v vs %#v", got, v)
			}
			// Re-encoding against the reader uses the READER's indices.
			wantWire, _ := r.AppendEncode(nil, v)
			gotWire, err := r.AppendEncode(nil, got)
			if err != nil || !bytes.Equal(gotWire, wantWire) {
				t.Fatalf("reader re-encode differs: err=%v got=%x want=%x", err, gotWire, wantWire)
			}
		}
	})
	t.Run("widening", func(t *testing.T) {
		w := avro.MustParse(`["int"]`)
		r := avro.MustParse(`["null","int","string"]`)
		res, err := resolveBoth(t, w, r)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		wire, _ := w.AppendEncode(nil, int32(9))
		var got any
		if _, err := res.Decode(wire, &got); err != nil || got != int32(9) {
			t.Fatalf("widening decode: %v %#v", err, got)
		}
	})
	t.Run("narrowing-fails-fast", func(t *testing.T) {
		w := avro.MustParse(`["int","string"]`)
		r := avro.MustParse(`["int"]`)
		if _, err := resolveBoth(t, w, r); err == nil {
			t.Fatal("narrowing union must fail Resolve eagerly (documented fail-fast)")
		}
	})
	t.Run("two-pass-exact-beats-promotion", func(t *testing.T) {
		w := avro.MustParse(`"int"`)
		r := avro.MustParse(`["double","int"]`)
		res, err := resolveBoth(t, w, r)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		wire, _ := w.AppendEncode(nil, int32(3))
		var got any
		mustDecode(t, res, wire, &got)
		if _, ok := got.(int32); !ok {
			t.Fatalf("exact int branch must win over double promotion, got %T", got)
		}
	})
	t.Run("promotion-fallback-into-union", func(t *testing.T) {
		w := avro.MustParse(`"long"`)
		r := avro.MustParse(`["int","double"]`)
		res, err := resolveBoth(t, w, r)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		wire, _ := w.AppendEncode(nil, int64(12))
		var got any
		mustDecode(t, res, wire, &got)
		if _, ok := got.(float64); !ok {
			t.Fatalf("long should promote into the double branch, got %T", got)
		}
	})
}

func TestMatrix_EnumEvolutionByPosition(t *testing.T) {
	wEnum := `{"type":"enum","name":"EE","symbols":["A","B","C"]}`
	rEnum := `{"type":"enum","name":"EE","symbols":["A","B"],"default":"A"}`
	rEnumNoDefault := `{"type":"enum","name":"EE","symbols":["A","B"]}`
	positions := []struct {
		label string
		wrap  func(inner string) string
		val   func(sym string) any
		out   func(decoded any) any
	}{
		{"top", func(in string) string { return in },
			func(s string) any { return s }, func(d any) any { return d }},
		{"field", func(in string) string {
			return fmt.Sprintf(`{"type":"record","name":"ER","fields":[{"name":"e","type":%s}]}`, in)
		},
			func(s string) any { return map[string]any{"e": s} },
			func(d any) any { return d.(map[string]any)["e"] }},
		{"array", func(in string) string { return fmt.Sprintf(`{"type":"array","items":%s}`, in) },
			func(s string) any { return []any{s} },
			func(d any) any { return d.([]any)[0] }},
	}
	for _, pos := range positions {
		t.Run(pos.label, func(t *testing.T) {
			w := avro.MustParse(pos.wrap(wEnum))
			r := avro.MustParse(pos.wrap(rEnum))
			res, err := resolveBoth(t, w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			// Mapped symbol survives; unmapped symbol takes the reader default.
			for _, tc := range []struct{ in, want string }{{"B", "B"}, {"C", "A"}} {
				wire, _ := w.AppendEncode(nil, pos.val(tc.in))
				var got any
				if _, err := res.Decode(wire, &got); err != nil {
					t.Fatalf("decode %s: %v", tc.in, err)
				}
				if pos.out(got) != tc.want {
					t.Fatalf("symbol %s: got %v want %s", tc.in, pos.out(got), tc.want)
				}
			}
			// Without a reader default the unmappable symbol fails Resolve
			// eagerly (documented fail-fast).
			rn := avro.MustParse(pos.wrap(rEnumNoDefault))
			if _, err := resolveBoth(t, w, rn); err == nil {
				t.Fatal("unmappable enum symbol without default must fail Resolve")
			}
		})
	}
}

func TestMatrix_AliasEvolution(t *testing.T) {
	cases := []struct {
		label   string
		wSchema string
		rSchema string
		value   any
		want    any // reader-shaped decoded value (field-alias RENAMES the field)
	}{
		{"type-alias",
			`{"type":"record","name":"Old","fields":[{"name":"a","type":"int"}]}`,
			`{"type":"record","name":"New","aliases":["Old"],"fields":[{"name":"a","type":"int"}]}`,
			map[string]any{"a": int32(1)}, map[string]any{"a": int32(1)}},
		{"type-alias-namespaced",
			`{"type":"record","name":"Old","namespace":"n1","fields":[{"name":"a","type":"int"}]}`,
			`{"type":"record","name":"New","namespace":"n2","aliases":["n1.Old"],"fields":[{"name":"a","type":"int"}]}`,
			map[string]any{"a": int32(2)}, map[string]any{"a": int32(2)}},
		{"field-alias",
			`{"type":"record","name":"R","fields":[{"name":"old","type":"string"}]}`,
			`{"type":"record","name":"R","fields":[{"name":"new","type":"string","aliases":["old"]}]}`,
			map[string]any{"old": "v"}, map[string]any{"new": "v"}},
		{"enum-alias",
			`{"type":"enum","name":"OldE","symbols":["A"]}`,
			`{"type":"enum","name":"NewE","aliases":["OldE"],"symbols":["A"]}`,
			"A", "A"},
		{"fixed-alias",
			`{"type":"fixed","name":"OldF","size":2}`,
			`{"type":"fixed","name":"NewF","aliases":["OldF"],"size":2}`,
			[]byte{7, 8}, []byte{7, 8}},
	}
	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			w := avro.MustParse(c.wSchema)
			r := avro.MustParse(c.rSchema)
			res, err := resolveBoth(t, w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			wire, err := w.AppendEncode(nil, c.value)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var got any
			if _, err := res.Decode(wire, &got); err != nil {
				t.Fatalf("aliased decode: %v", err)
			}
			// Not just no-error: the aliased value must land under the READER's
			// (renamed) shape. A resolution that decodes the wrong value without
			// erroring slips a no-error-only check.
			if !matEqual(got, c.want) {
				t.Fatalf("aliased value: got %#v want %#v", got, c.want)
			}
		})
		// The same alias pair nested in an array still resolves.
		t.Run(c.label+"/in-array", func(t *testing.T) {
			w := avro.MustParse(fmt.Sprintf(`{"type":"array","items":%s}`, c.wSchema))
			r := avro.MustParse(fmt.Sprintf(`{"type":"array","items":%s}`, c.rSchema))
			res, err := resolveBoth(t, w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			wire, err := w.AppendEncode(nil, []any{c.value})
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var got any
			if _, err := res.Decode(wire, &got); err != nil {
				t.Fatalf("aliased decode: %v", err)
			}
			if !matEqual(got, []any{c.want}) {
				t.Fatalf("aliased array value: got %#v want %#v", got, []any{c.want})
			}
		})
	}
}

// Promotion pairs × TYPED reader targets: the resolved decode of a writer
// wire into a concrete Go target must agree with the NATURAL decode (the
// reader schema reading its own wire) into that same target — the same
// accept/reject verdict and, on accept, the identical value — across
// composition contexts (top level, record field via a reflect-built struct,
// array element, null-union branch behind a pointer). The natural path is
// the independent oracle: the promotion arms delegate to the same target
// dispatchers (setLongValue, setFloatValue, setBytesValue, setStringValue,
// and the logical wrappers), so a promotion arm that hand-rolls its own
// target handling — or a union/record resolution that drops the reader's
// logical promotion wrapper — drifts observably here. Reject cells (a
// negative long into uint64, a UUID-invalid byte payload into [16]byte)
// pin verdict parity, not just value parity. The logical-reader rows drive
// promotionDeserForLogical's typed arms (time.Time / time.Duration /
// big.Rat / json.Number / [16]byte), which no other generative axis
// reaches.
func TestMatrix_PromotionTypedTargets(t *testing.T) {
	type row struct {
		name    string
		wSchema string
		rSchema string
		wVal    any // encoded against wSchema → the promoted wire
		rVal    any // encoded against rSchema → the natural (oracle) wire
		targets []reflect.Type
	}
	var (
		anyT = reflect.TypeFor[any]()
		i32  = reflect.TypeFor[int32]()
		i64  = reflect.TypeFor[int64]()
		u64  = reflect.TypeFor[uint64]()
		f32  = reflect.TypeFor[float32]()
		f64  = reflect.TypeFor[float64]()
		str  = reflect.TypeFor[string]()
		bs   = reflect.TypeFor[[]byte]()
		b2   = reflect.TypeFor[[2]byte]()
		b16  = reflect.TypeFor[[16]byte]()
		tt   = reflect.TypeFor[time.Time]()
		td   = reflect.TypeFor[time.Duration]()
		rat  = reflect.TypeFor[big.Rat]()
	)
	const uuidStr = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	rows := []row{
		// The negative value makes the uint64 target a reject-parity cell on
		// both int→long and long→double; float64 stays exact (accept).
		{"int-to-long", `"int"`, `"long"`, int32(-77), int64(-77),
			[]reflect.Type{i64, i32, u64, f64, anyT}},
		{"int-to-float", `"int"`, `"float"`, int32(123), float32(123),
			[]reflect.Type{f32, f64, anyT}},
		{"int-to-double", `"int"`, `"double"`, int32(-9), float64(-9),
			[]reflect.Type{f64, f32, anyT}},
		{"long-to-float", `"long"`, `"float"`, int64(1 << 10), float32(1 << 10),
			[]reflect.Type{f32, f64, anyT}},
		{"long-to-double", `"long"`, `"double"`, int64(-5), float64(-5),
			[]reflect.Type{f64, u64, anyT}},
		{"float-to-double", `"float"`, `"double"`, float32(1.5), float64(1.5),
			[]reflect.Type{f64, f32, anyT}},
		{"string-to-bytes", `"string"`, `"bytes"`, "sb", []byte("sb"),
			[]reflect.Type{bs, b2, str, anyT}},
		{"bytes-to-string", `"bytes"`, `"string"`, []byte("bs"), "bs",
			[]reflect.Type{str, bs, anyT}},
		// Logical readers: promotionDeserForLogical wraps the writer's wire
		// read with the reader's logical conversion.
		{"int-to-long-timestamp-millis", `"int"`, `{"type":"long","logicalType":"timestamp-millis"}`,
			int32(86400001), time.UnixMilli(86400001).UTC(),
			[]reflect.Type{tt, i64, anyT}},
		{"int-to-long-time-micros", `"int"`, `{"type":"long","logicalType":"time-micros"}`,
			int32(5_000_000), 5 * time.Second,
			[]reflect.Type{td, i64, anyT}},
		// The writer string's raw bytes 0x41 0x42 are the reader's unscaled
		// two's-complement payload: 16706 at scale 2 = 167.06.
		{"string-to-bytes-decimal", `"string"`, `{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}`,
			"AB", big.NewRat(16706, 100),
			[]reflect.Type{rat, bs, anyT}},
		{"bytes-to-string-uuid", `"bytes"`, `{"type":"string","logicalType":"uuid"}`,
			[]byte(uuidStr), uuidStr,
			[]reflect.Type{str, b16, bs, anyT}},
		// UUID-invalid payload: the [16]byte target must reject on BOTH the
		// promoted and natural paths (parseUUID), while string accepts.
		{"bytes-to-string-uuid-invalid", `"bytes"`, `{"type":"string","logicalType":"uuid"}`,
			[]byte("definitely-not-a-uuid-but-36-chars-x"), "definitely-not-a-uuid-but-36-chars-x",
			[]reflect.Type{str, b16, anyT}},
	}

	type ctx struct {
		label  string
		schema func(inner string) string
		wrap   func(v any) any
		target func(elem reflect.Type) reflect.Type
		unwrap func(target reflect.Value) reflect.Value // target is the *T decode destination
	}
	ctxs := []ctx{
		{
			label:  "top",
			schema: func(s string) string { return s },
			wrap:   func(v any) any { return v },
			target: func(e reflect.Type) reflect.Type { return e },
			unwrap: func(v reflect.Value) reflect.Value { return v.Elem() },
		},
		{
			label: "record-field",
			schema: func(s string) string {
				return fmt.Sprintf(`{"type":"record","name":"PTT","fields":[{"name":"f","type":%s}]}`, s)
			},
			wrap: func(v any) any { return map[string]any{"f": v} },
			target: func(e reflect.Type) reflect.Type {
				return reflect.StructOf([]reflect.StructField{{Name: "F", Type: e, Tag: `avro:"f"`}})
			},
			unwrap: func(v reflect.Value) reflect.Value { return v.Elem().Field(0) },
		},
		{
			label:  "array-elem",
			schema: func(s string) string { return fmt.Sprintf(`{"type":"array","items":%s}`, s) },
			wrap:   func(v any) any { return []any{v} },
			target: func(e reflect.Type) reflect.Type { return reflect.SliceOf(e) },
			unwrap: func(v reflect.Value) reflect.Value { return v.Elem().Index(0) },
		},
		{
			label:  "null-union",
			schema: func(s string) string { return fmt.Sprintf(`["null",%s]`, s) },
			wrap:   func(v any) any { return v },
			target: func(e reflect.Type) reflect.Type { return reflect.PointerTo(e) },
			unwrap: func(v reflect.Value) reflect.Value { return v.Elem().Elem() },
		},
	}

	for _, rw := range rows {
		for _, cx := range ctxs {
			t.Run(rw.name+"/"+cx.label, func(t *testing.T) {
				w := avro.MustParse(cx.schema(rw.wSchema))
				r := avro.MustParse(cx.schema(rw.rSchema))
				res, err := resolveBoth(t, w, r)
				if err != nil {
					t.Fatalf("Resolve: %v", err)
				}
				promotedWire, err := w.AppendEncode(nil, cx.wrap(rw.wVal))
				if err != nil {
					t.Fatalf("writer encode: %v", err)
				}
				naturalWire, err := r.AppendEncode(nil, cx.wrap(rw.rVal))
				if err != nil {
					t.Fatalf("reader encode: %v", err)
				}
				for _, elem := range rw.targets {
					tgtType := cx.target(elem)
					promoted := reflect.New(tgtType)
					natural := reflect.New(tgtType)
					_, perr := res.Decode(promotedWire, promoted.Interface())
					_, nerr := r.Decode(naturalWire, natural.Interface())
					if (perr == nil) != (nerr == nil) {
						t.Fatalf("target %v: verdict divergence: promoted err=%v, natural err=%v",
							elem, perr, nerr)
					}
					if perr != nil {
						continue
					}
					pv := cx.unwrap(promoted)
					nv := cx.unwrap(natural)
					if !reflect.DeepEqual(pv.Interface(), nv.Interface()) {
						t.Fatalf("target %v: value divergence: promoted %#v, natural %#v",
							elem, pv.Interface(), nv.Interface())
					}
				}
			})
		}
	}
}

// ---------- matrix_extensions_test.go ----------

// ---------------------------------------------------------------------------
// Extension axes: lenient encode-input forms (every accepted Go form of a
// value must produce the identical wire), metadata preservation through the
// rebuild (doc/aliases/props survive Root().Schema()), lax-name schemas, and
// nil-equivalent encode shapes across union arities.
// ---------------------------------------------------------------------------

// Every accepted input form for the same logical value must produce
// byte-identical wires — in every position.
func TestMatrix_LenientInputForms(t *testing.T) {
	cases := []struct {
		label  string
		schema string
		forms  []any
	}{
		{"int", `"int"`, []any{int32(42), int64(42), int16(42), uint8(42), int(42), float64(42), json.Number("42")}},
		{"long", `"long"`, []any{int64(42), int32(42), int(42), uint32(42), float64(42), json.Number("42")}},
		{"float", `"float"`, []any{float32(1.5), float64(1.5), json.Number("1.5")}},
		{"double", `"double"`, []any{float64(1.5), float32(1.5), json.Number("1.5")}},
		{"string", `"string"`, []any{"sv", []byte("sv")}},
		{"bytes", `"bytes"`, []any{[]byte("bv"), "bv"}},
		{"fixed", `{"type":"fixed","name":"LF","size":2}`, []any{[]byte{0x61, 0x62}, "ab", [2]byte{0x61, 0x62}}},
		{"enum-symbol-or-ordinal", `{"type":"enum","name":"LE","symbols":["A","B","C"]}`, []any{"C", 2, uint8(2), int64(2)}},
		{"timestamp-forms", `{"type":"long","logicalType":"timestamp-millis"}`,
			[]any{time.UnixMilli(1717243496789).UTC(), int64(1717243496789), json.Number("1717243496789")}},
		{"decimal-forms", `{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}`,
			[]any{big.NewRat(3, 2), float64(1.5), json.Number("1.5")}},
	}
	positions := []struct {
		label  string
		schema func(in string) string
		wrap   func(v any) any
	}{
		{"top", func(in string) string { return in }, func(v any) any { return v }},
		{"field", func(in string) string {
			return fmt.Sprintf(`{"type":"record","name":"LR","fields":[{"name":"f","type":%s}]}`, in)
		}, func(v any) any { return map[string]any{"f": v} }},
		{"array", func(in string) string { return fmt.Sprintf(`{"type":"array","items":%s}`, in) },
			func(v any) any { return []any{v} }},
	}
	for _, c := range cases {
		for _, pos := range positions {
			t.Run(c.label+"/"+pos.label, func(t *testing.T) {
				s := avro.MustParse(pos.schema(c.schema))
				var want []byte
				for i, form := range c.forms {
					got, err := s.AppendEncode(nil, pos.wrap(form))
					if err != nil {
						t.Fatalf("form %d (%T): %v", i, form, err)
					}
					if i == 0 {
						want = got
						continue
					}
					if !bytes.Equal(got, want) {
						t.Fatalf("form %d (%T) wire differs:\n got=%x\nwant=%x", i, form, got, want)
					}
				}
				// JSON wires agree across forms too.
				var wantJ []byte
				for i, form := range c.forms {
					got, err := s.AppendEncodeJSON(nil, pos.wrap(form))
					if err != nil {
						t.Fatalf("json form %d (%T): %v", i, form, err)
					}
					if i == 0 {
						wantJ = got
						continue
					}
					if !bytes.Equal(got, wantJ) {
						t.Fatalf("json form %d (%T) differs:\n got=%s\nwant=%s", i, form, got, wantJ)
					}
				}
			})
		}
	}
}

// Doc strings, aliases, and custom properties survive the metadata rebuild
// — at the type level, field level, and on named branch types.
func TestMatrix_MetadataPreservedThroughRebuild(t *testing.T) {
	schema := `{"type":"record","name":"R","namespace":"meta.ns","doc":"record doc",
		"aliases":["OldR","other.AliasedR"],"custom.top":"tv",
		"fields":[
			{"name":"f","type":"int","doc":"field doc","aliases":["oldf"],"order":"descending","custom.fld":7,"default":3},
			{"name":"e","type":{"type":"enum","name":"E","doc":"enum doc","symbols":["A","B"],"default":"B","custom.enum":true}},
			{"name":"x","type":{"type":"fixed","name":"F","size":2,"custom.fixed":"fv"}}]}`
	s := avro.MustParse(schema)
	check := func(t *testing.T, root avro.SchemaNode, tag string) {
		t.Helper()
		if root.Doc != "record doc" || root.Namespace != "meta.ns" {
			t.Fatalf("%s: doc/ns lost: %q %q", tag, root.Doc, root.Namespace)
		}
		if len(root.Aliases) != 2 {
			t.Fatalf("%s: aliases lost: %v", tag, root.Aliases)
		}
		if root.Props["custom.top"] != "tv" {
			t.Fatalf("%s: type prop lost: %v", tag, root.Props)
		}
		f := root.Fields[0]
		if f.Doc != "field doc" || len(f.Aliases) != 1 || f.Order != "descending" {
			t.Fatalf("%s: field metadata lost: %+v", tag, f)
		}
		if f.Props["custom.fld"] != int64(7) {
			t.Fatalf("%s: field prop lost or retyped: %#v", tag, f.Props["custom.fld"])
		}
		if f.Default != int32(3) || !f.HasDefault {
			t.Fatalf("%s: default lost: %#v", tag, f.Default)
		}
		e := root.Fields[1].Type
		if e.Doc != "enum doc" || e.Props["custom.enum"] != true {
			t.Fatalf("%s: enum metadata lost: %+v", tag, e)
		}
		x := root.Fields[2].Type
		if x.Props["custom.fixed"] != "fv" {
			t.Fatalf("%s: fixed prop lost: %+v", tag, x)
		}
	}
	root := s.Root()
	check(t, *root, "first Root()")
	rebuilt, err := root.Schema()
	if err != nil {
		t.Fatalf("Root().Schema(): %v", err)
	}
	check(t, *rebuilt.Root(), "rebuilt Root()")
	// Second-generation rebuild is stable too.
	rb2root := rebuilt.Root()
	rebuilt2, err := rb2root.Schema()
	if err != nil {
		t.Fatalf("second rebuild: %v", err)
	}
	check(t, *rebuilt2.Root(), "second rebuilt Root()")
}

// laxNameSamples is the CHARACTER axis of the lax-name nets. It was a
// hand-picked sample of six spellings, and the characters it happened to
// pick (tab, backslash, quote) reach three of the canonical escaper's seven
// short-form arms — so backspace and formfeed, which the escaper spells
// \b and \f rather than  / , were emitted by no net at all.
// A name is the only carrier that can hold them, and only under a
// permissive lax-name checker, so nothing else in the suite could reach
// them either.
//
// The escaper's arms are swept exhaustively by
// TestMatrix_CanonicalStringEscapeSweep; this list stays a sample, but one
// picked to hit every arm, so the full round trip (wire, JSON wire,
// metadata rebuild) runs on each escape form rather than only on the
// canonical bytes.
var laxNameSamples = []string{
	"with space", "tab\tname", `back\slash`, `qu"ote`, "uni🎉code", "1starts-digit",
	"bs\bname", "ff\fname", "nl\nname", "cr\rname", "ctl\x01name", "del\x7fname",
}

// Lax-name schemas: the wire paths work; the names survive Canonical()
// (escaped correctly) and the metadata rebuild.
func TestMatrix_LaxNames(t *testing.T) {
	lax := avro.WithLaxNames(nil)
	for _, name := range laxNameSamples {
		t.Run(name, func(t *testing.T) {
			nameJSON, _ := json.Marshal(name)
			schema := fmt.Sprintf(`{"type":"record","name":%s,"fields":[
				{"name":"f","type":{"type":"enum","name":%s,"symbols":["A"]}}]}`,
				nameJSON, string(mustJSON(name+"E")))
			s, err := avro.Parse(schema, lax)
			if err != nil {
				t.Fatalf("lax Parse: %v", err)
			}
			vin := map[string]any{"f": "A"}
			w1, err := s.AppendEncode(nil, vin)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var a1 any
			if _, err := s.Decode(w1, &a1); err != nil || !matEqual(a1, vin) {
				t.Fatalf("decode: %v %#v", err, a1)
			}
			j1, err := s.AppendEncodeJSON(nil, a1)
			if err != nil {
				t.Fatalf("encodeJSON: %v", err)
			}
			var aj any
			if err := s.DecodeJSON(j1, &aj); err != nil || !matEqual(aj, a1) {
				t.Fatalf("decodeJSON: %v", err)
			}
			// Canonical() must stay valid JSON with the name escaped.
			canon := s.Canonical()
			var any1 any
			if err := json.Unmarshal(canon, &any1); err != nil {
				t.Fatalf("canonical not valid JSON: %v\n%s", err, canon)
			}
			// The metadata rebuild must accept the same SchemaOpts the
			// original Parse needed: a lax-named schema is rebuildable by
			// passing WithLaxNames through Schema().
			root := s.Root()
			rebuilt, err := root.Schema(lax)
			if err != nil {
				t.Fatalf("Root().Schema(lax): %v", err)
			}
			w2, err := rebuilt.AppendEncode(nil, vin)
			if err != nil || !bytes.Equal(w2, w1) {
				t.Fatalf("rebuilt lax schema wire differs: err=%v\n w1=%x\n w2=%x", err, w1, w2)
			}
		})
	}
}

// canonShortForms are the SEVEN characters the canonical form spells with
// a two-character escape rather than the six-character \u00xx. Every other
// character below 0x20 takes the long form; everything at 0x20 and above
// rides raw, including the solidus, which is legal to escape and which the
// reference leaves alone.
//
// This is the reference spelling -- the parsing-canonical-form strings the
// fingerprint is computed over -- written out so a cell asserts the FORM,
// not merely that some escape was chosen: two spellings of one character
// are both valid JSON and both decode to the same string, but they hash to
// different fingerprints, so a round-trip check alone cannot see a
// divergence from the reference.
var canonShortForms = map[byte]string{
	'"':  `\"`,
	'\\': `\\`,
	0x08: `\b`,
	0x0c: `\f`,
	'\n': `\n`,
	'\r': `\r`,
	'\t': `\t`,
}

// TestMatrix_CanonicalStringEscapeSweep sweeps the canonical string
// escaper over EVERY byte it can be handed in a name, rather than over the
// characters a sample happened to contain. The escaper is a switch with
// seven short-form arms and a \u00xx default, and the lax-name sample above
// historically reached three of them; the two that no net reached at all
// were backspace and formfeed, whose short forms exist precisely because
// the reference emits them that way.
//
// Two independent checks per character, because either alone is blind:
// the canonical bytes must decode back to the exact original string (form-
// agnostic — catches a mangled or dropped escape), and the escape must be
// the reference SPELLING (catches a valid-but-divergent form, which
// round-trips perfectly and still changes the fingerprint).
//
// Both canonical string carriers are swept. A name and an enum symbol take
// different routes into the emitter, and a fix applied to one is not a fix
// to the other.
func TestMatrix_CanonicalStringEscapeSweep(t *testing.T) {
	t.Parallel()
	lax := avro.WithLaxNames(func(string) error { return nil })

	// want is the reference spelling of one character inside a canonical
	// string: a short form where the reference defines one, the six-
	// character escape for every other C0 control, and the raw character
	// otherwise.
	want := func(b byte) string {
		if s, ok := canonShortForms[b]; ok {
			return s
		}
		if b < 0x20 {
			const hex = "0123456789abcdef"
			return `\u00` + string([]byte{hex[b>>4], hex[b&0xf]})
		}
		return string([]byte{b})
	}

	carriers := []struct {
		name string
		// build wraps the payload string into a schema; read pulls the
		// same string back out of the parsed schema.
		build func(payload string) string
		read  func(s *avro.Schema) string
	}{
		{
			"record-name",
			func(p string) string {
				return `{"type":"record","name":` + string(mustJSON(p)) + `,"fields":[]}`
			},
			func(s *avro.Schema) string { return s.Root().Name },
		},
		{
			"enum-symbol",
			func(p string) string {
				return `{"type":"enum","name":"E","symbols":[` + string(mustJSON(p)) + `]}`
			},
			func(s *avro.Schema) string { return s.Root().Symbols[0] },
		},
	}

	// Liveness floor. Every character below is GENERATED, which says nothing
	// about whether it was EXERCISED: a carrier that started rejecting
	// control characters would skip its way to a green sweep. These counters
	// are therefore incremented INSIDE the cell, only once the assertion has
	// actually been made against the emitted bytes. The subtests are
	// sequential (no t.Parallel below), so the counts are exact.
	shortFormHits := map[byte]int{}
	longFormHits := 0

	for _, c := range carriers {
		for b := range 0x80 {
			payload := "a" + string([]byte{byte(b)}) + "z"
			t.Run(fmt.Sprintf("%s/%#02x", c.name, b), func(t *testing.T) {
				s, err := avro.Parse(c.build(payload), lax)
				if err != nil {
					t.Skipf("carrier rejects %#02x even under a permissive lax checker: %v", b, err)
				}
				if got := c.read(s); got != payload {
					t.Fatalf("parse did not preserve the payload: %q, want %q", got, payload)
				}
				canon := s.Canonical()
				if !json.Valid(canon) {
					t.Fatalf("canonical is not valid JSON: %q", canon)
				}
				// Form-agnostic: whatever escape was chosen must decode
				// back to the character we put in.
				var back any
				if err := json.Unmarshal(canon, &back); err != nil {
					t.Fatalf("canonical does not decode: %v (%q)", err, canon)
				}
				if !bytes.Contains(canon, []byte(`a`+want(byte(b))+`z`)) {
					t.Fatalf("canonical spells %#02x as something other than the reference form %q:\n  %q", b, want(byte(b)), canon)
				}
				if _, short := canonShortForms[byte(b)]; short {
					shortFormHits[byte(b)]++
				} else if b < 0x20 {
					longFormHits++
				}
			})
		}
	}

	// All seven short-form arms — the five C0 controls with two-character
	// escapes, plus quote and backslash — must have been asserted against
	// real emitted bytes, or the sweep has narrowed back toward the sample
	// it replaced.
	for b := range canonShortForms {
		if shortFormHits[b] == 0 {
			t.Errorf("short-form arm %#02x was never asserted; the escaper's %q arm is unexercised again", b, canonShortForms[b])
		}
	}
	if len(canonShortForms) != 7 {
		t.Errorf("the reference table names %d short forms, not 7; a change to it is a change to the fingerprint and needs its own ruling", len(canonShortForms))
	}
	// 32 C0 controls minus the 5 with short forms, across both carriers.
	if want := (32 - 5) * len(carriers); longFormHits != want {
		t.Errorf("%d characters took the \\u00xx default, want %d; the sweep is no longer covering the C0 range on every carrier", longFormHits, want)
	}
	t.Logf("canonical escape sweep: %d short-form arms asserted, %d long-form characters", len(shortFormHits), longFormHits)
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// Nil-equivalent encode shapes: a typed nil pointer, an interface-wrapped
// nil pointer, and a non-nil pointer to a nil pointer all route to the null
// branch — on both wires, across union arities and positions.
func TestMatrix_NilShapesAcrossUnions(t *testing.T) {
	var nilPtr *int32
	shapes := []struct {
		label string
		value any
	}{
		{"nil", nil},
		{"typed-nil-ptr", nilPtr},
		{"iface-nil-ptr", any(nilPtr)},
		{"ptr-to-nil-ptr", &nilPtr},
		{"nil-byte-slice", []byte(nil)},
		{"nil-map", map[string]any(nil)},
		{"nil-any-slice", []any(nil)},
	}
	unions := []struct {
		label   string
		schema  string
		nullIdx int32
	}{
		{"null-first-2", `["null","int"]`, 0},
		{"null-second-2", `["int","null"]`, 1},
		{"null-first-3", `["null","int","string"]`, 0},
		{"null-mid-3", `["int","null","string"]`, 1},
	}
	for _, un := range unions {
		s := avro.MustParse(un.schema)
		wantWire, err := s.AppendEncode(nil, nil)
		if err != nil {
			t.Fatalf("%s: encode nil: %v", un.label, err)
		}
		wantJSON, err := s.AppendEncodeJSON(nil, nil)
		if err != nil {
			t.Fatalf("%s: encodeJSON nil: %v", un.label, err)
		}
		for _, sh := range shapes {
			t.Run(un.label+"/"+sh.label, func(t *testing.T) {
				got := mustAppendEncode(t, s, nil, sh.value)
				if !bytes.Equal(got, wantWire) {
					t.Fatalf("binary nil-shape diverges: got=%x want=%x", got, wantWire)
				}
				gotJ := mustAppendEncodeJSON(t, s, nil, sh.value)
				if !bytes.Equal(gotJ, wantJSON) {
					t.Fatalf("JSON nil-shape diverges: got=%s want=%s", gotJ, wantJSON)
				}
			})
		}
		// The same shapes inside a record field and array items.
		fs := avro.MustParse(fmt.Sprintf(`{"type":"record","name":"NR","fields":[{"name":"u","type":%s}]}`, un.schema))
		fWant, _ := fs.AppendEncode(nil, map[string]any{"u": nil})
		for _, sh := range shapes {
			t.Run(un.label+"/field/"+sh.label, func(t *testing.T) {
				got := mustAppendEncode(t, fs, nil, map[string]any{"u": sh.value})
				if !bytes.Equal(got, fWant) {
					t.Fatalf("field nil-shape diverges: got=%x want=%x", got, fWant)
				}
			})
		}
	}
}

// ---------- matrix_feature_walker_test.go ----------

// The SCHEMA-FEATURE × WALKER parity net.
//
// The library has one wire parser and many parallel schema consumers
// ("walkers"): String()'s re-parse, Canonical() and fingerprints, the Root()
// metadata tree and its Schema() rebuild, the SchemaCache self-containment
// walkers (definition collection and reference splicing), schema resolution
// (including the custom-free writer view built for resolved JSON decoding),
// resolved DecodeJSON, CheckCompatibility, and single-object encoding. Every
// schema FEATURE the wire parser understands — alternate spellings, lifts,
// normalizations, acceptances — must mean the same thing to every walker.
// Historically, each feature × walker intersection that no test crossed has
// drifted independently: a walker gates on its own re-derivation of the
// parser's rule and covers a subset of the feature's reach.
//
// Structure: a table of feature rows × a table of walker drivers, run as the
// full cross product. Each row carries a schema spelled WITH the feature and
// its vanilla ("twin") spelling of the same logical schema, plus fragments
// for the cache directions (a cross-parse reference INTO the feature's
// subtree, and a named definition INSIDE the feature's subtree) and a
// resolve-compatible variant. Each driver asserts one consumer treats
// feature and twin identically. The invariant in every cell is PARITY with
// the wire parser / the vanilla twin — never a hardcoded expectation, so
// rows stay cheap to add.
//
// Adding a feature to the net = adding a row. Seeded families: the flat
// (goavro field-format) lift across all six kinds plus composition and the
// namespace-decoy trap; lax names (WithLaxNames-only shapes, split-vs-inline
// fullname twins); the three field-level logicalType lift shapes;
// case-variant reserved keys; wrapped ({"type":"X"}) and forward references
// (diamond + recursive); aliases-any-string; degenerate cardinalities
// (empty fields/symbols/branches, size-0 fixed); duplicate-key last-wins;
// and implicit null defaults. Each family block below documents what its
// twin means and where its feature survives to; drivers skip explicitly
// (with the reason at the row or driver) where a row has no structural
// position for them — a nil sample marks a parseable-but-unusable kind.
//
// One walker family lives OUTSIDE the row/driver shape because its
// feature deliberately breaks twin parity on the as-written surfaces:
// STRAY structural keys (a reserved container key on a kind that does not
// bind it — inert to the parser, surfaced as-written by the metadata
// walker: schema-shaped bodies structurally, non-schema-shaped bodies in
// Props verbatim). Its walkers-must-not-consume-strays net is
// TestMatrix_CacheStrayStructuralKey (SchemaCache collect / splice /
// metadata name table, carrier × key × def relation × surface),
// TestMatrix_CacheStrayRebuildSurface (the render + its dedup consult:
// rebuild succeeds, preserves strays props-independently, stays stable
// across generations), TestMatrix_StrayBodyShapeRouting (key × body
// shape × carrier × surfacing route, with the fastavro differential
// arm), TestMatrix_SchemaForStrayStructuralKey (the composition
// walkers, props and typed planting routes), and — for SIMULTANEOUS
// case-variant duplicate spellings of one reserved key, the axis the
// single-spelling rows here never cross — TestMatrix_ReservedKeyDuplicateSpellings
// + its field-level and cache-splice twins (only the CI pick is
// consulted/consumed; every other spelling rides to Props verbatim); a
// new consumer of walkNodeChildren — or any walker/consult of either
// tree representation (FIX.md item 3's representation checklist) — owes
// cells there, not a row here.
type featureWalkerRow struct {
	name string

	// feature and twin spell the SAME logical schema with and without the
	// feature. Both parse standalone (with opts). sample is a value both
	// spellings Encode.
	feature, twin string
	opts          []avro.SchemaOpt
	sample        map[string]any

	// resolveAgainst, when non-empty, is a vanilla-spelled schema that is
	// resolve-compatible with feature/twin in BOTH directions (fields
	// added only with defaults) but canonically different, so Resolve's
	// identical-canonical fast path cannot short-circuit and resolution
	// actually recurses the feature's subtree. resolveSample is a value
	// resolveAgainst Encodes.
	resolveAgainst string
	resolveSample  map[string]any

	// Cache cross-parse REFERENCE direction: refDefs are registered first
	// (in a fresh cache per spelling); refFeature/refTwin then hold a
	// reference to a cached name INSIDE the feature's subtree position;
	// refSample encodes against them. Empty refFeature marks a feature
	// with no reference-bearing subtree (flat enum/fixed carry none).
	refDefs             []string
	refFeature, refTwin string
	refSample           map[string]any

	// Cache DEFINITION direction: defFeature DEFINES a named type inside
	// the feature's subtree position (defTwin is its vanilla twin);
	// defFollow, parsed next in the same cache, references that type and
	// must splice its definition; defFollowSample encodes against it.
	defFeature, defTwin string
	defFollow           string
	defFollowSample     map[string]any
}

const (
	fwElemDef      = `{"type":"record","name":"ns.Elem","fields":[{"name":"x","type":"int"}]}`
	fwDecoyElemDef = `{"type":"record","name":"decoy.Elem","fields":[{"name":"y","type":"long"}]}`
)

var featureWalkerRows = []featureWalkerRow{
	{
		name:    "flat-record",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":"record","fields":[{"name":"x","type":"int"}]}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":{"type":"record","name":"rec","fields":[{"name":"x","type":"int"}]}}]}`,
		sample:  map[string]any{"rec": map[string]any{"x": int32(7)}},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":{"type":"record","name":"rec","fields":[{"name":"x","type":"int"},{"name":"pad","type":"int","default":5}]}}]}`,
		resolveSample:  map[string]any{"rec": map[string]any{"x": int32(7), "pad": int32(5)}},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":"record","fields":[{"name":"e","type":"Elem"}]}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":{"type":"record","name":"rec","fields":[{"name":"e","type":"Elem"}]}}]}`,
		refSample:  map[string]any{"rec": map[string]any{"e": map[string]any{"x": int32(1)}}},

		defFeature:      `{"type":"record","name":"ns.H1","fields":[{"name":"drec","type":"record","fields":[{"name":"x","type":"int"}]}]}`,
		defTwin:         `{"type":"record","name":"ns.H1","fields":[{"name":"drec","type":{"type":"record","name":"drec","fields":[{"name":"x","type":"int"}]}}]}`,
		defFollow:       `{"type":"record","name":"ns.F1","fields":[{"name":"d","type":"drec"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
	{
		name:    "flat-error",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"e","type":"error","fields":[{"name":"x","type":"int"}]}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"e","type":{"type":"error","name":"e","fields":[{"name":"x","type":"int"}]}}]}`,
		sample:  map[string]any{"e": map[string]any{"x": int32(3)}},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"e","type":{"type":"error","name":"e","fields":[{"name":"x","type":"int"},{"name":"pad","type":"int","default":5}]}}]}`,
		resolveSample:  map[string]any{"e": map[string]any{"x": int32(3), "pad": int32(5)}},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fields":[{"name":"e","type":"error","fields":[{"name":"el","type":"Elem"}]}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"e","type":{"type":"error","name":"e","fields":[{"name":"el","type":"Elem"}]}}]}`,
		refSample:  map[string]any{"e": map[string]any{"el": map[string]any{"x": int32(1)}}},

		defFeature:      `{"type":"record","name":"ns.H2","fields":[{"name":"derr","type":"error","fields":[{"name":"x","type":"int"}]}]}`,
		defTwin:         `{"type":"record","name":"ns.H2","fields":[{"name":"derr","type":{"type":"error","name":"derr","fields":[{"name":"x","type":"int"}]}}]}`,
		defFollow:       `{"type":"record","name":"ns.F2","fields":[{"name":"d","type":"derr"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
	{
		name:    "flat-enum",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"c","type":"enum","symbols":["A","B"]},{"name":"w","type":"int"}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"c","type":{"type":"enum","name":"c","symbols":["A","B"]}},{"name":"w","type":"int"}]}`,
		sample:  map[string]any{"c": "B", "w": int32(1)},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"c","type":{"type":"enum","name":"c","symbols":["A","B"]}},{"name":"w","type":"int"},{"name":"extra","type":"int","default":42}]}`,
		resolveSample:  map[string]any{"c": "B", "w": int32(1), "extra": int32(42)},

		// A flat enum field carries no sub-schema position, so there is no
		// reference INTO the feature; the definition direction below covers
		// the feature as a cross-parse definition.

		defFeature:      `{"type":"record","name":"ns.H3","fields":[{"name":"col","type":"enum","symbols":["R","G"]}]}`,
		defTwin:         `{"type":"record","name":"ns.H3","fields":[{"name":"col","type":{"type":"enum","name":"col","symbols":["R","G"]}}]}`,
		defFollow:       `{"type":"record","name":"ns.F3","fields":[{"name":"k","type":"col"}]}`,
		defFollowSample: map[string]any{"k": "G"},
	},
	{
		name:    "flat-fixed",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"fx","type":"fixed","size":2},{"name":"w","type":"int"}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"fx","type":{"type":"fixed","name":"fx","size":2}},{"name":"w","type":"int"}]}`,
		sample:  map[string]any{"fx": []byte{1, 2}, "w": int32(1)},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"fx","type":{"type":"fixed","name":"fx","size":2}},{"name":"w","type":"int"},{"name":"extra","type":"int","default":42}]}`,
		resolveSample:  map[string]any{"fx": []byte{1, 2}, "w": int32(1), "extra": int32(42)},

		// No sub-schema position (see flat-enum).

		defFeature:      `{"type":"record","name":"ns.H4","fields":[{"name":"dfx","type":"fixed","size":3}]}`,
		defTwin:         `{"type":"record","name":"ns.H4","fields":[{"name":"dfx","type":{"type":"fixed","name":"dfx","size":3}}]}`,
		defFollow:       `{"type":"record","name":"ns.F4","fields":[{"name":"d","type":"dfx"}]}`,
		defFollowSample: map[string]any{"d": []byte{1, 2, 3}},
	},
	{
		name:    "flat-array",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":"array","items":{"type":"record","name":"E5","fields":[{"name":"x","type":"int"}]}}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":{"type":"array","items":{"type":"record","name":"E5","fields":[{"name":"x","type":"int"}]}}}]}`,
		sample:  map[string]any{"list": []any{map[string]any{"x": int32(7)}}},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":{"type":"array","items":{"type":"record","name":"E5","fields":[{"name":"x","type":"int"},{"name":"pad","type":"int","default":5}]}}}]}`,
		resolveSample:  map[string]any{"list": []any{map[string]any{"x": int32(7), "pad": int32(5)}}},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":"array","items":"Elem"}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":{"type":"array","items":"Elem"}}]}`,
		refSample:  map[string]any{"list": []any{map[string]any{"x": int32(1)}}},

		defFeature:      `{"type":"record","name":"ns.H5","fields":[{"name":"list","type":"array","items":{"type":"record","name":"D5","fields":[{"name":"x","type":"int"}]}}]}`,
		defTwin:         `{"type":"record","name":"ns.H5","fields":[{"name":"list","type":{"type":"array","items":{"type":"record","name":"D5","fields":[{"name":"x","type":"int"}]}}}]}`,
		defFollow:       `{"type":"record","name":"ns.F5","fields":[{"name":"d","type":"D5"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
	{
		name:    "flat-map",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"m","type":"map","values":{"type":"record","name":"E6","fields":[{"name":"x","type":"int"}]}}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"m","type":{"type":"map","values":{"type":"record","name":"E6","fields":[{"name":"x","type":"int"}]}}}]}`,
		sample:  map[string]any{"m": map[string]any{"k": map[string]any{"x": int32(7)}}},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"m","type":{"type":"map","values":{"type":"record","name":"E6","fields":[{"name":"x","type":"int"},{"name":"pad","type":"int","default":5}]}}}]}`,
		resolveSample:  map[string]any{"m": map[string]any{"k": map[string]any{"x": int32(7), "pad": int32(5)}}},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fields":[{"name":"m","type":"map","values":"Elem"}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"m","type":{"type":"map","values":"Elem"}}]}`,
		refSample:  map[string]any{"m": map[string]any{"k": map[string]any{"x": int32(1)}}},

		defFeature:      `{"type":"record","name":"ns.H6","fields":[{"name":"m","type":"map","values":{"type":"record","name":"D6","fields":[{"name":"x","type":"int"}]}}]}`,
		defTwin:         `{"type":"record","name":"ns.H6","fields":[{"name":"m","type":{"type":"map","values":{"type":"record","name":"D6","fields":[{"name":"x","type":"int"}]}}}]}`,
		defFollow:       `{"type":"record","name":"ns.F6","fields":[{"name":"d","type":"D6"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
	{
		// The lift composes: a flat record field whose own fields hold a
		// flat array field. Walkers that handle only the first lift level
		// miss the inner one.
		name:    "flat-array-in-flat-record",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":"record","fields":[{"name":"list","type":"array","items":{"type":"record","name":"E7","fields":[{"name":"x","type":"int"}]}}]}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":{"type":"record","name":"rec","fields":[{"name":"list","type":{"type":"array","items":{"type":"record","name":"E7","fields":[{"name":"x","type":"int"}]}}}]}}]}`,
		sample:  map[string]any{"rec": map[string]any{"list": []any{map[string]any{"x": int32(7)}}}},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":{"type":"record","name":"rec","fields":[{"name":"list","type":{"type":"array","items":{"type":"record","name":"E7","fields":[{"name":"x","type":"int"},{"name":"pad","type":"int","default":5}]}}}]}}]}`,
		resolveSample:  map[string]any{"rec": map[string]any{"list": []any{map[string]any{"x": int32(7), "pad": int32(5)}}}},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":"record","fields":[{"name":"list","type":"array","items":"Elem"}]}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":{"type":"record","name":"rec","fields":[{"name":"list","type":{"type":"array","items":"Elem"}}]}}]}`,
		refSample:  map[string]any{"rec": map[string]any{"list": []any{map[string]any{"x": int32(1)}}}},

		defFeature:      `{"type":"record","name":"ns.H7","fields":[{"name":"drec","type":"record","fields":[{"name":"list","type":"array","items":{"type":"record","name":"D7","fields":[{"name":"x","type":"int"}]}}]}]}`,
		defTwin:         `{"type":"record","name":"ns.H7","fields":[{"name":"drec","type":{"type":"record","name":"drec","fields":[{"name":"list","type":{"type":"array","items":{"type":"record","name":"D7","fields":[{"name":"x","type":"int"}]}}}]}}]}`,
		defFollow:       `{"type":"record","name":"ns.F7","fields":[{"name":"d","type":"D7"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
	{
		// A stray "namespace" key on a flat array field is a FIELD prop:
		// the lift drops name/namespace keys for unnamed kinds, so the
		// items sit in the RECORD's namespace scope and a short reference
		// resolves there — never in the stray namespace. decoy.Elem (a
		// different shape) is registered alongside ns.Elem so a walker
		// that wrongly honors the stray namespace binds the WRONG type
		// and diverges from the twin, rather than merely dangling.
		name:    "flat-array-ns-decoy",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":"array","items":{"type":"record","name":"E8","fields":[{"name":"x","type":"int"}]},"namespace":"decoy"}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":{"type":"array","items":{"type":"record","name":"E8","fields":[{"name":"x","type":"int"}]}},"namespace":"decoy"}]}`,
		sample:  map[string]any{"list": []any{map[string]any{"x": int32(7)}}},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":{"type":"array","items":{"type":"record","name":"E8","fields":[{"name":"x","type":"int"},{"name":"pad","type":"int","default":5}]}}}]}`,
		resolveSample:  map[string]any{"list": []any{map[string]any{"x": int32(7), "pad": int32(5)}}},

		refDefs:    []string{fwElemDef, fwDecoyElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":"array","items":"Elem","namespace":"decoy"}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":{"type":"array","items":"Elem"},"namespace":"decoy"}]}`,
		refSample:  map[string]any{"list": []any{map[string]any{"x": int32(1)}}},

		defFeature:      `{"type":"record","name":"ns.H8","fields":[{"name":"list","type":"array","items":{"type":"record","name":"D8","fields":[{"name":"x","type":"int"}]},"namespace":"decoy"}]}`,
		defTwin:         `{"type":"record","name":"ns.H8","fields":[{"name":"list","type":{"type":"array","items":{"type":"record","name":"D8","fields":[{"name":"x","type":"int"}]}},"namespace":"decoy"}]}`,
		defFollow:       `{"type":"record","name":"ns.F8","fields":[{"name":"d","type":"D8"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},

	// ─── Lax names ───
	//
	// Names only a WithLaxNames user fn accepts: an empty namespace
	// component, the bare empty name, a trailing-dot fullname (empty final
	// component), and characters outside the strict grammar. There is no
	// strict spelling of these schemas; each row's twin is instead the SAME
	// fullname spelled the other way the grammar allows — split
	// name+namespace attributes vs the inline dotted fullname (for the bare
	// empty name: omitted namespace vs the explicit-empty-namespace escape).
	// Both spellings resolve to one fullname, so parity here exercises the
	// name split/join and namespace-inheritance logic of every walker on
	// name components the strict grammar never produces. The names survive
	// parse verbatim (validation never transforms them) and reach every
	// walker: canonical emission, String() re-parse, the Root() tree and its
	// rebuild, cache collection/splicing, resolution's name matching, and
	// SOE fingerprints.
	{
		// Empty namespace COMPONENT (ns "a..b"), recursive: the self
		// reference "a..b.R" exercises the second-occurrence reference
		// path through every walker, not just the definition path.
		name:    "lax-ns-empty-component",
		opts:    laxAcceptAll,
		feature: `{"type":"record","name":"R","namespace":"a..b","fields":[{"name":"x","type":"int"},{"name":"next","type":["null","a..b.R"]}]}`,
		twin:    `{"type":"record","name":"a..b.R","fields":[{"name":"x","type":"int"},{"name":"next","type":["null","a..b.R"]}]}`,
		sample:  map[string]any{"x": int32(7), "next": map[string]any{"x": int32(8), "next": nil}},

		resolveAgainst: `{"type":"record","name":"a..b.R","fields":[{"name":"x","type":"int"},{"name":"next","type":["null","a..b.R"]},{"name":"pad","type":"int","default":5}]}`,
		resolveSample:  map[string]any{"x": int32(7), "next": nil, "pad": int32(5)},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"R","namespace":"a..b","fields":[{"name":"e","type":"ns.Elem"}]}`,
		refTwin:    `{"type":"record","name":"a..b.R","fields":[{"name":"e","type":"ns.Elem"}]}`,
		refSample:  map[string]any{"e": map[string]any{"x": int32(1)}},

		defFeature:      `{"type":"record","name":"H","namespace":"a..b","fields":[{"name":"d","type":{"type":"record","name":"DL1","namespace":"nsd","fields":[{"name":"x","type":"int"}]}}]}`,
		defTwin:         `{"type":"record","name":"a..b.H","fields":[{"name":"d","type":{"type":"record","name":"DL1","namespace":"nsd","fields":[{"name":"x","type":"int"}]}}]}`,
		defFollow:       `{"type":"record","name":"ns.F","fields":[{"name":"d","type":"nsd.DL1"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
	{
		// Bare empty name at root (fullname ""). Twin: the explicit
		// empty-namespace escape, the one other spelling of fullname "".
		// No self/cross reference to "" is possible — the "" REFERENCE
		// spelling is structurally rejected (documented divergence from
		// fastavro), so the ref/def directions use ordinary names around
		// and inside the empty-named container instead.
		name:    "lax-empty-name",
		opts:    laxAcceptAll,
		feature: `{"type":"record","name":"","fields":[{"name":"x","type":"int"}]}`,
		twin:    `{"type":"record","name":"","namespace":"","fields":[{"name":"x","type":"int"}]}`,
		sample:  map[string]any{"x": int32(7)},

		resolveAgainst: `{"type":"record","name":"","fields":[{"name":"x","type":"int"},{"name":"pad","type":"int","default":5}]}`,
		resolveSample:  map[string]any{"x": int32(7), "pad": int32(5)},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"","fields":[{"name":"e","type":"ns.Elem"}]}`,
		refTwin:    `{"type":"record","name":"","namespace":"","fields":[{"name":"e","type":"ns.Elem"}]}`,
		refSample:  map[string]any{"e": map[string]any{"x": int32(1)}},

		defFeature:      `{"type":"record","name":"","fields":[{"name":"d","type":{"type":"record","name":"nsd.DL2","fields":[{"name":"x","type":"int"}]}}]}`,
		defTwin:         `{"type":"record","name":"","namespace":"","fields":[{"name":"d","type":{"type":"record","name":"nsd.DL2","fields":[{"name":"x","type":"int"}]}}]}`,
		defFollow:       `{"type":"record","name":"ns.F","fields":[{"name":"d","type":"nsd.DL2"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
	{
		// Trailing-dot fullname "ok." — namespace "ok", EMPTY final name
		// component. Twin: the split spelling (name "", namespace "ok").
		// Recursive via the "ok." self reference (the dotted reference
		// spelling is accepted; only the bare "" reference is not).
		name:    "lax-trailing-dot-name",
		opts:    laxAcceptAll,
		feature: `{"type":"record","name":"ok.","fields":[{"name":"x","type":"int"},{"name":"next","type":["null","ok."]}]}`,
		twin:    `{"type":"record","name":"","namespace":"ok","fields":[{"name":"x","type":"int"},{"name":"next","type":["null","ok."]}]}`,
		sample:  map[string]any{"x": int32(7), "next": map[string]any{"x": int32(8), "next": nil}},

		resolveAgainst: `{"type":"record","name":"ok.","fields":[{"name":"x","type":"int"},{"name":"next","type":["null","ok."]},{"name":"pad","type":"int","default":5}]}`,
		resolveSample:  map[string]any{"x": int32(7), "next": nil, "pad": int32(5)},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"ok.","fields":[{"name":"e","type":"ns.Elem"}]}`,
		refTwin:    `{"type":"record","name":"","namespace":"ok","fields":[{"name":"e","type":"ns.Elem"}]}`,
		refSample:  map[string]any{"e": map[string]any{"x": int32(1)}},

		defFeature:      `{"type":"record","name":"okh.","fields":[{"name":"d","type":{"type":"record","name":"nsd.DL3","fields":[{"name":"x","type":"int"}]}}]}`,
		defTwin:         `{"type":"record","name":"","namespace":"okh","fields":[{"name":"d","type":{"type":"record","name":"nsd.DL3","fields":[{"name":"x","type":"int"}]}}]}`,
		defFollow:       `{"type":"record","name":"ns.F","fields":[{"name":"d","type":"nsd.DL3"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
	{
		// Characters outside the strict grammar (space, '!'), in a DIAMOND:
		// field a defines "my ns.we!rd", field b references it again, so
		// the weird name travels the reference path as well as the
		// definition path. Feature spells the definition split and the
		// re-reference bare-short (in-scope binding); the twin spells the
		// definition inline-dotted and the re-reference fully qualified.
		name:    "lax-weird-chars",
		opts:    laxAcceptAll,
		feature: `{"type":"record","name":"Top","namespace":"my ns","fields":[{"name":"a","type":{"type":"record","name":"we!rd","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":"we!rd"}]}`,
		twin:    `{"type":"record","name":"my ns.Top","fields":[{"name":"a","type":{"type":"record","name":"my ns.we!rd","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":"my ns.we!rd"}]}`,
		sample:  map[string]any{"a": map[string]any{"x": int32(7)}, "b": map[string]any{"x": int32(8)}},

		resolveAgainst: `{"type":"record","name":"my ns.Top","fields":[{"name":"a","type":{"type":"record","name":"my ns.we!rd","fields":[{"name":"x","type":"int"},{"name":"pad","type":"int","default":5}]}},{"name":"b","type":"my ns.we!rd"}]}`,
		resolveSample:  map[string]any{"a": map[string]any{"x": int32(7), "pad": int32(5)}, "b": map[string]any{"x": int32(8), "pad": int32(5)}},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"Top","namespace":"my ns","fields":[{"name":"e","type":"ns.Elem"}]}`,
		refTwin:    `{"type":"record","name":"my ns.Top","fields":[{"name":"e","type":"ns.Elem"}]}`,
		refSample:  map[string]any{"e": map[string]any{"x": int32(1)}},

		defFeature:      `{"type":"record","name":"H","namespace":"my ns","fields":[{"name":"d","type":{"type":"record","name":"DL4","namespace":"nsd","fields":[{"name":"x","type":"int"}]}}]}`,
		defTwin:         `{"type":"record","name":"my ns.H","fields":[{"name":"d","type":{"type":"record","name":"DL4","namespace":"nsd","fields":[{"name":"x","type":"int"}]}}]}`,
		defFollow:       `{"type":"record","name":"ns.F","fields":[{"name":"d","type":"nsd.DL4"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
}

// laxAcceptAll parses names under an accept-everything user validator, the
// broadest WithLaxNames contract: every component string — including the
// empty string — passes through verbatim.
var laxAcceptAll = []avro.SchemaOpt{avro.WithLaxNames(func(string) error { return nil })}

// fwTime is a millisecond-precision instant every timestamp-millis row
// encodes; millisecond precision means the round trip is lossless, so
// cross-spelling decode comparisons see the identical instant.
var fwTime = time.Date(2021, 3, 4, 5, 6, 7, 891_000_000, time.UTC)

func init() {
	featureWalkerRows = append(featureWalkerRows, featureWalkerLiftRows...)
	featureWalkerRows = append(featureWalkerRows, featureWalkerCaseKeyRows...)
	featureWalkerRows = append(featureWalkerRows, featureWalkerRefFormRows...)
	featureWalkerRows = append(featureWalkerRows, featureWalkerAliasRows...)
	featureWalkerRows = append(featureWalkerRows, featureWalkerStrayNSRows...)
	featureWalkerRows = append(featureWalkerRows, featureWalkerDegenerateRows...)
	featureWalkerRows = append(featureWalkerRows, featureWalkerDupKeyRows...)
	featureWalkerRows = append(featureWalkerRows, featureWalkerImplicitNullRows...)
}

// Implicit null default: a ["null", T] union field with NO explicit default
// implicitly defaults to null (a twmb ergonomic beyond Java/fastavro). The
// twin spells "default": null explicitly — the same logical schema here.
// The sample OMITS the field, so the synthesized default must drive the
// encoder's auto-fill byte-identically to the explicit twin on every
// encode-bearing driver, and the resolve variant LACKS the field entirely,
// so reader-side resolution must fill it from the synthesized default. The
// cache directions cross the synthesis with a null union whose branch is a
// cross-parse REFERENCE and with one holding an inline DEFINITION.
var featureWalkerImplicitNullRows = []featureWalkerRow{
	{
		name:    "implicit-null-default",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"o","type":["null","int"]},{"name":"w","type":"int"}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"o","type":["null","int"],"default":null},{"name":"w","type":"int"}]}`,
		sample:  map[string]any{"w": int32(1)},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"w","type":"int"},{"name":"pad","type":"int","default":5}]}`,
		resolveSample:  map[string]any{"w": int32(1), "pad": int32(5)},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fields":[{"name":"o","type":["null","Elem"]},{"name":"w","type":"int"}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"o","type":["null","Elem"],"default":null},{"name":"w","type":"int"}]}`,
		refSample:  map[string]any{"w": int32(1)},

		defFeature:      `{"type":"record","name":"ns.HK","fields":[{"name":"o","type":["null",{"type":"record","name":"DK","fields":[{"name":"x","type":"int"}]}]}]}`,
		defTwin:         `{"type":"record","name":"ns.HK","fields":[{"name":"o","type":["null",{"type":"record","name":"DK","fields":[{"name":"x","type":"int"}]}],"default":null}]}`,
		defFollow:       `{"type":"record","name":"ns.FK","fields":[{"name":"d","type":"DK"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
}

// Duplicate JSON keys in the SCHEMA document: the last occurrence wins
// (encoding/json map decode; Java's Jackson ObjectNode and fastavro's dict
// behave the same). The duplicates survive in the schema text that every
// walker independently re-consumes — String()'s re-parse, the Root() tree,
// the cache splice — so each walker's own decode must collapse to the same
// LAST value the wire parser used; a walker swapped to an order-sensitive
// scanner that takes the FIRST occurrence diverges. Twins spell the
// single-key last-value form; first values are decoys chosen so a
// first-wins reading produces a structurally DIFFERENT schema (wrong
// size/symbols/items, a different registered type, an invalid default)
// rather than a coincidentally-equal one. Non-vacuity is proven by the
// twin-flip check (spell the twin with the FIRST values and watch the
// cells die) rather than an arm neuter: the collapse lives in the stdlib
// decoder, which has no production arm of ours to disable.
var featureWalkerDupKeyRows = []featureWalkerRow{
	{
		// Type-defining keys duplicated: record name, fields (empty decoy
		// then real), fixed size (999 then 2), enum symbols, array items
		// (string then int).
		name:    "dupkey-structural",
		feature: `{"type":"record","name":"decoy.Top","name":"ns.Top","fields":[],"fields":[{"name":"fx","type":{"type":"fixed","name":"fx","size":999,"size":2}},{"name":"c","type":{"type":"enum","name":"decoyc","name":"c","symbols":["X"],"symbols":["A","B"]}},{"name":"list","type":{"type":"array","items":"string","items":"int"}}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"fx","type":{"type":"fixed","name":"fx","size":2}},{"name":"c","type":{"type":"enum","name":"c","symbols":["A","B"]}},{"name":"list","type":{"type":"array","items":"int"}}]}`,
		sample:  map[string]any{"fx": []byte{1, 2}, "c": "B", "list": []any{int32(1)}},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"fx","type":{"type":"fixed","name":"fx","size":2}},{"name":"c","type":{"type":"enum","name":"c","symbols":["A","B"]}},{"name":"list","type":{"type":"array","items":"int"}},{"name":"pad","type":"int","default":5}]}`,
		resolveSample:  map[string]any{"fx": []byte{1, 2}, "c": "B", "list": []any{int32(1)}, "pad": int32(5)},

		// The dup-items pair references cached types: decoy.Elem first,
		// ns.Elem last, BOTH registered — a first-wins walker binds the
		// wrong type (different field shape) instead of dangling.
		refDefs:    []string{fwElemDef, fwDecoyElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":{"type":"array","items":"decoy.Elem","items":"ns.Elem"}}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":{"type":"array","items":"ns.Elem"}}]}`,
		refSample:  map[string]any{"list": []any{map[string]any{"x": int32(1)}}},

		// The DEFINITION carries duplicated name and fields keys; the
		// follow-up reference binds the last-wins name, whose last-wins
		// fields must have been collected.
		defFeature:      `{"type":"record","name":"ns.HI","fields":[{"name":"d","type":{"type":"record","name":"decoy.DD","name":"DD","fields":[],"fields":[{"name":"x","type":"int"}]}}]}`,
		defTwin:         `{"type":"record","name":"ns.HI","fields":[{"name":"d","type":{"type":"record","name":"DD","fields":[{"name":"x","type":"int"}]}}]}`,
		defFollow:       `{"type":"record","name":"ns.FI","fields":[{"name":"d","type":"DD"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
	{
		// Annotation keys duplicated: namespace (decoy then real — the
		// real one governs which cached Elem a bare reference binds),
		// field default (an INVALID first value: 7 cannot default a
		// null-first union, so a first-wins parse errors outright),
		// order, aliases, doc, and logicalType (time-micros then
		// timestamp-millis — the time.Time sample encodes only against
		// the last).
		name:    "dupkey-annotations",
		feature: `{"type":"record","name":"Top","namespace":"decoy","namespace":"ns","fields":[{"name":"o","type":["null","int"],"default":7,"default":null},{"name":"w","type":"int","order":"ascending","order":"descending","aliases":["a1"],"aliases":["a2"],"doc":"d1","doc":"d2"},{"name":"ts","type":{"type":"long","logicalType":"time-micros","logicalType":"timestamp-millis"}}]}`,
		twin:    `{"type":"record","name":"Top","namespace":"ns","fields":[{"name":"o","type":["null","int"],"default":null},{"name":"w","type":"int","order":"descending","aliases":["a2"],"doc":"d2"},{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}`,
		sample:  map[string]any{"w": int32(1), "ts": fwTime},

		resolveAgainst: `{"type":"record","name":"Top","namespace":"ns","fields":[{"name":"o","type":["null","int"],"default":null},{"name":"w","type":"int","order":"descending","aliases":["a2"],"doc":"d2"},{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"pad","type":"int","default":5}]}`,
		resolveSample:  map[string]any{"o": nil, "w": int32(1), "ts": fwTime, "pad": int32(5)},

		refDefs:    []string{fwElemDef, fwDecoyElemDef},
		refFeature: `{"type":"record","name":"Top","namespace":"decoy","namespace":"ns","fields":[{"name":"e","type":"Elem"}]}`,
		refTwin:    `{"type":"record","name":"Top","namespace":"ns","fields":[{"name":"e","type":"Elem"}]}`,
		refSample:  map[string]any{"e": map[string]any{"x": int32(1)}},

		defFeature:      `{"type":"record","name":"HJ","namespace":"decoy","namespace":"ns","fields":[{"name":"d","type":{"type":"record","name":"DJ","aliases":["z1"],"aliases":["z2"],"fields":[{"name":"x","type":"int"}]}}]}`,
		defTwin:         `{"type":"record","name":"HJ","namespace":"ns","fields":[{"name":"d","type":{"type":"record","name":"DJ","aliases":["z2"],"fields":[{"name":"x","type":"int"}]}}]}`,
		defFollow:       `{"type":"record","name":"ns.FJ","fields":[{"name":"d","type":"DJ"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
}

// Degenerate cardinalities, all reference-legal: the empty-fields record,
// the size-0 fixed, the zero-symbol enum, and the zero-branch union. The
// first two are USABLE (an empty record and a size-0 fixed both encode zero
// bytes); the last two are parseable-but-unusable — no value inhabits them,
// so encode-bearing drivers skip (sample == nil) and only the pure schema
// walkers (String() re-parse, canonical, Root() rebuild, compatibility)
// run. Where the kind can be spelled two ways the twin is the flat (goavro
// field-format) vs nested spelling, composing this family with the flat
// lift; the empty union has a single spelling, so its twin is the same
// text and parity degenerates to independent-parse determinism plus
// self-containment of every emitted form.
var featureWalkerDegenerateRows = []featureWalkerRow{
	{
		name:    "degenerate-empty-fields-record",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"er","type":"record","fields":[]},{"name":"w","type":"int"}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"er","type":{"type":"record","name":"er","fields":[]}},{"name":"w","type":"int"}]}`,
		sample:  map[string]any{"er": map[string]any{}, "w": int32(1)},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"er","type":{"type":"record","name":"er","fields":[]}},{"name":"w","type":"int"},{"name":"pad","type":"int","default":5}]}`,
		resolveSample:  map[string]any{"er": map[string]any{}, "w": int32(1), "pad": int32(5)},

		// An empty-fields record has no child position, so no reference
		// can sit INSIDE the feature's subtree; the definition direction
		// registers the empty record itself as a cross-parse definition.
		defFeature:      `{"type":"record","name":"ns.HG","fields":[{"name":"der","type":"record","fields":[]}]}`,
		defTwin:         `{"type":"record","name":"ns.HG","fields":[{"name":"der","type":{"type":"record","name":"der","fields":[]}}]}`,
		defFollow:       `{"type":"record","name":"ns.FG","fields":[{"name":"d","type":"der"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{}},
	},
	{
		name:    "degenerate-size0-fixed",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"fx","type":"fixed","size":0},{"name":"w","type":"int"}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"fx","type":{"type":"fixed","name":"fx","size":0}},{"name":"w","type":"int"}]}`,
		sample:  map[string]any{"fx": []byte{}, "w": int32(1)},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"fx","type":{"type":"fixed","name":"fx","size":0}},{"name":"w","type":"int"},{"name":"pad","type":"int","default":5}]}`,
		resolveSample:  map[string]any{"fx": []byte{}, "w": int32(1), "pad": int32(5)},

		// No child position inside a fixed (see empty-fields note).
		defFeature:      `{"type":"record","name":"ns.HH","fields":[{"name":"dfx0","type":"fixed","size":0}]}`,
		defTwin:         `{"type":"record","name":"ns.HH","fields":[{"name":"dfx0","type":{"type":"fixed","name":"dfx0","size":0}}]}`,
		defFollow:       `{"type":"record","name":"ns.FH","fields":[{"name":"d","type":"dfx0"}]}`,
		defFollowSample: map[string]any{"d": []byte{}},
	},
	{
		// Parseable-but-unusable: no value encodes against a zero-symbol
		// enum, so sample is nil (encode drivers skip) and the cache
		// definition direction — whose follow-up parse would have to
		// ENCODE through the reference — is likewise skipped; resolve
		// variants need an encodable sample too. The walkers that remain
		// must still treat flat and nested spellings identically.
		name:    "degenerate-empty-enum",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"c","type":"enum","symbols":[]},{"name":"w","type":"int"}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"c","type":{"type":"enum","name":"c","symbols":[]}},{"name":"w","type":"int"}]}`,
	},
	{
		// Zero-branch union: single spelling, twin is the same text —
		// parity is independent-parse determinism and self-containment.
		// Unusable (nothing inhabits []), so encode-bearing drivers and
		// both cache directions skip; there is also no subtree to hold a
		// reference or definition.
		name:    "degenerate-empty-union",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"u","type":[]},{"name":"w","type":"int"}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"u","type":[]},{"name":"w","type":"int"}]}`,
	},
}

// Aliases accept ANY string — never name-validated (type AND field aliases),
// including the leading-dot null-namespace escape. Aliases are stripped by
// the canonical form and inert on the wire, so the twin is the alias-FREE
// spelling: parity asserts exactly that inertness through every walker,
// while the weird alias strings survive verbatim into the forms String(),
// Root(), and the cache splice emit — each self-containment re-parse in the
// drivers re-accepts them (a walker that re-validated aliases, or dropped
// them and changed the emitted form's parse, dies here). Resolution parity
// holds because primary names match on both sides; the alias-MATCHING
// semantics (a reader alias binding a writer's legacy name) are a
// resolution feature deliberately outside this net's twin-parity shape,
// pinned by the alias regression tests.
var featureWalkerAliasRows = []featureWalkerRow{
	{
		name:    "aliases-any-string",
		feature: `{"type":"record","name":"ns.Top","aliases":["1st!","com.example.legacy x",".NullNs"],"fields":[{"name":"w","type":"int","aliases":["9 lives","!"]},{"name":"c","type":{"type":"enum","name":"c","symbols":["A","B"],"aliases":["énum,alias"]}}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"w","type":"int"},{"name":"c","type":{"type":"enum","name":"c","symbols":["A","B"]}}]}`,
		sample:  map[string]any{"w": int32(1), "c": "B"},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"w","type":"int"},{"name":"c","type":{"type":"enum","name":"c","symbols":["A","B"]}},{"name":"pad","type":"int","default":5}]}`,
		resolveSample:  map[string]any{"w": int32(1), "c": "B", "pad": int32(5)},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"ns.Top","aliases":["1st!"],"fields":[{"name":"e","type":"Elem","aliases":["old e"]}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"e","type":"Elem"}]}`,
		refSample:  map[string]any{"e": map[string]any{"x": int32(1)}},

		defFeature:      `{"type":"record","name":"ns.HF","fields":[{"name":"d","type":{"type":"record","name":"DE","aliases":["!weird def","."],"fields":[{"name":"x","type":"int"}]}}]}`,
		defTwin:         `{"type":"record","name":"ns.HF","fields":[{"name":"d","type":{"type":"record","name":"DE","fields":[{"name":"x","type":"int"}]}}]}`,
		defFollow:       `{"type":"record","name":"ns.FF","fields":[{"name":"d","type":"DE"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
}

// A stray "namespace" attribute on the unnamed container kinds (array,
// map) is inert metadata: accepted at parse, surfaced as-written, stripped
// by the canonical form, and NEVER a namespace scope — named types defined
// or referenced under it resolve in the ENCLOSING scope on every path
// (parser, cache walkers, metadata rebuild; fastavro executed the same
// scoping). The attrs here are DECOYS: a walker that scoped children by
// the stray attribute would bind decoy.AE / look up decoy-scoped names and
// die on parity or a dangling reference, not merely tolerate the key. The
// twin is the attribute-free spelling.
var featureWalkerStrayNSRows = []featureWalkerRow{
	{
		name: "stray-namespace-on-container",
		feature: `{"type":"record","name":"ns.Top","fields":[
			{"name":"ar","type":{"type":"array","namespace":"decoy","items":{"type":"record","name":"AE","fields":[{"name":"x","type":"int"}]}}},
			{"name":"mp","type":{"type":"map","namespace":"decoy","values":"AE"}}]}`,
		twin: `{"type":"record","name":"ns.Top","fields":[
			{"name":"ar","type":{"type":"array","items":{"type":"record","name":"AE","fields":[{"name":"x","type":"int"}]}}},
			{"name":"mp","type":{"type":"map","values":"AE"}}]}`,
		sample: map[string]any{
			"ar": []any{map[string]any{"x": int32(1)}},
			"mp": map[string]any{"k": map[string]any{"x": int32(2)}},
		},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[
			{"name":"ar","type":{"type":"array","items":{"type":"record","name":"AE","fields":[{"name":"x","type":"int"}]}}},
			{"name":"mp","type":{"type":"map","values":"AE"}},
			{"name":"pad","type":"int","default":5}]}`,
		resolveSample: map[string]any{
			"ar":  []any{map[string]any{"x": int32(1)}},
			"mp":  map[string]any{"k": map[string]any{"x": int32(2)}},
			"pad": int32(5),
		},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fields":[{"name":"e","type":{"type":"array","namespace":"decoy","items":"Elem"}}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"e","type":{"type":"array","items":"Elem"}}]}`,
		refSample:  map[string]any{"e": []any{map[string]any{"x": int32(1)}}},

		defFeature:      `{"type":"record","name":"ns.HN","fields":[{"name":"d","type":{"type":"array","namespace":"decoy","items":{"type":"record","name":"DN","fields":[{"name":"x","type":"int"}]}}}]}`,
		defTwin:         `{"type":"record","name":"ns.HN","fields":[{"name":"d","type":{"type":"array","items":{"type":"record","name":"DN","fields":[{"name":"x","type":"int"}]}}}]}`,
		defFollow:       `{"type":"record","name":"ns.FN","fields":[{"name":"d","type":"DN"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
}

// Name-reference FORMS: the wrapped-object reference spelling
// ({"type":"X"}, accepted where fastavro/hamba reject) and forward
// references (a reference textually preceding its definition). The parsed
// schema resolves every reference to the same named type either way; the
// AS-WRITTEN reference/definition positions survive in the text String(),
// Root(), and the cache splice re-consume, and the canonical form must
// re-home each definition to its first-occurrence position identically for
// both spellings. Rows are deliberately multi-occurrence (diamond) or
// recursive so the second-occurrence reference path is crossed, not just
// the definition path.
var featureWalkerRefFormRows = []featureWalkerRow{
	{
		// Wrapped BACKWARD reference: field b re-references the type
		// field a defined, spelled {"type":"WB"} vs the bare "WB" twin.
		// The cache-ref direction wraps a CROSS-PARSE reference to a
		// cached type, so the splice walker must resolve the wrapped
		// spelling too.
		name:    "wrapped-backward-ref",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"a","type":{"type":"record","name":"WB","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":{"type":"WB"}}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"a","type":{"type":"record","name":"WB","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":"WB"}]}`,
		sample:  map[string]any{"a": map[string]any{"x": int32(7)}, "b": map[string]any{"x": int32(8)}},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"a","type":{"type":"record","name":"WB","fields":[{"name":"x","type":"int"},{"name":"pad","type":"int","default":5}]}},{"name":"b","type":"WB"}]}`,
		resolveSample:  map[string]any{"a": map[string]any{"x": int32(7), "pad": int32(5)}, "b": map[string]any{"x": int32(8), "pad": int32(5)}},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fields":[{"name":"e","type":{"type":"Elem"}}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"e","type":"Elem"}]}`,
		refSample:  map[string]any{"e": map[string]any{"x": int32(1)}},

		defFeature:      `{"type":"record","name":"ns.HC","fields":[{"name":"a","type":{"type":"record","name":"DC","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":{"type":"DC"}}]}`,
		defTwin:         `{"type":"record","name":"ns.HC","fields":[{"name":"a","type":{"type":"record","name":"DC","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":"DC"}]}`,
		defFollow:       `{"type":"record","name":"ns.FC","fields":[{"name":"d","type":"DC"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
	{
		// Bare FORWARD reference in a diamond: field a references FR
		// before field b defines it, field c references it again after.
		// The twin defines at first use. Canonical form must re-home the
		// definition to field a (first occurrence) for BOTH spellings —
		// the position-dependent inlining is exactly where a walker that
		// re-derives the parser's resolution can drift.
		name:    "forward-ref-diamond",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"a","type":"FR"},{"name":"b","type":{"type":"record","name":"FR","fields":[{"name":"x","type":"int"}]}},{"name":"c","type":"FR"}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"a","type":{"type":"record","name":"FR","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":"FR"},{"name":"c","type":"FR"}]}`,
		sample:  map[string]any{"a": map[string]any{"x": int32(1)}, "b": map[string]any{"x": int32(2)}, "c": map[string]any{"x": int32(3)}},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"a","type":{"type":"record","name":"FR","fields":[{"name":"x","type":"int"},{"name":"pad","type":"int","default":5}]}},{"name":"b","type":"FR"},{"name":"c","type":"FR"}]}`,
		resolveSample:  map[string]any{"a": map[string]any{"x": int32(1), "pad": int32(5)}, "b": map[string]any{"x": int32(2), "pad": int32(5)}, "c": map[string]any{"x": int32(3), "pad": int32(5)}},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fields":[{"name":"a","type":"FR2"},{"name":"b","type":{"type":"record","name":"FR2","fields":[{"name":"e","type":"Elem"}]}}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"a","type":{"type":"record","name":"FR2","fields":[{"name":"e","type":"Elem"}]}},{"name":"b","type":"FR2"}]}`,
		refSample:  map[string]any{"a": map[string]any{"e": map[string]any{"x": int32(1)}}, "b": map[string]any{"e": map[string]any{"x": int32(1)}}},

		defFeature:      `{"type":"record","name":"ns.HD","fields":[{"name":"a","type":"FR3"},{"name":"b","type":{"type":"record","name":"FR3","fields":[{"name":"x","type":"int"}]}}]}`,
		defTwin:         `{"type":"record","name":"ns.HD","fields":[{"name":"a","type":{"type":"record","name":"FR3","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":"FR3"}]}`,
		defFollow:       `{"type":"record","name":"ns.FD","fields":[{"name":"d","type":"FR3"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
	{
		// Wrapped FORWARD reference, recursive: field a holds
		// {"type":"WF"} before WF exists, and WF's own definition closes
		// the loop with a wrapped SELF-reference inside a null union.
		name:    "wrapped-forward-ref-recursive",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"a","type":{"type":"WF"}},{"name":"b","type":{"type":"record","name":"WF","fields":[{"name":"x","type":"int"},{"name":"next","type":["null",{"type":"WF"}]}]}}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"a","type":{"type":"record","name":"WF","fields":[{"name":"x","type":"int"},{"name":"next","type":["null","WF"]}]}},{"name":"b","type":"WF"}]}`,
		sample:  map[string]any{"a": map[string]any{"x": int32(1), "next": nil}, "b": map[string]any{"x": int32(2), "next": map[string]any{"x": int32(3), "next": nil}}},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"a","type":{"type":"record","name":"WF","fields":[{"name":"x","type":"int"},{"name":"next","type":["null","WF"]},{"name":"pad","type":"int","default":5}]}},{"name":"b","type":"WF"}]}`,
		resolveSample:  map[string]any{"a": map[string]any{"x": int32(1), "next": nil, "pad": int32(5)}, "b": map[string]any{"x": int32(2), "next": nil, "pad": int32(5)}},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fields":[{"name":"a","type":{"type":"WF2"}},{"name":"b","type":{"type":"record","name":"WF2","fields":[{"name":"e","type":"Elem"}]}}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"a","type":{"type":"record","name":"WF2","fields":[{"name":"e","type":"Elem"}]}},{"name":"b","type":"WF2"}]}`,
		refSample:  map[string]any{"a": map[string]any{"e": map[string]any{"x": int32(1)}}, "b": map[string]any{"e": map[string]any{"x": int32(1)}}},

		defFeature:      `{"type":"record","name":"ns.HE","fields":[{"name":"a","type":{"type":"WF3"}},{"name":"b","type":{"type":"record","name":"WF3","fields":[{"name":"x","type":"int"}]}}]}`,
		defTwin:         `{"type":"record","name":"ns.HE","fields":[{"name":"a","type":{"type":"record","name":"WF3","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":"WF3"}]}`,
		defFollow:       `{"type":"record","name":"ns.FE","fields":[{"name":"d","type":"WF3"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
}

// Reserved Avro attribute keys spelled in non-canonical ASCII case
// ("nAmespace", "iTems", ...) are ordinary custom properties:
// reserved-attribute matching is exact-lowercase-only on parse AND on
// every metadata surface, so a case-variant key binds nothing — it must
// ride verbatim as a prop through every walker (the schema text that
// String(), the Root() tree, and the cache splice re-consume all carry
// it), and no walker may bind, fold, or drop it. Structure is spelled
// exact-case (the only spelling that binds); the twin is the identical
// text, so every driver asserts the decoy keys change nothing about wire
// behavior, identity, or resolution while surviving each metadata
// surface. The decimal fixed encodes a *big.Rat and the timestamp a
// time.Time, so the EXACT logical keys must stay effective with decoys
// riding beside them; field w's dEfault decoy is no default at all, so w
// must be supplied; DA's nAmespace decoy must not re-scope the spliced
// definition (its fullname stays ns.DA).
var featureWalkerCaseKeyRows = []featureWalkerRow{
	{
		name:    "variantkey-props",
		feature: `{"type":"record","name":"Top","namespace":"ns","nAmespace":"decoy","fIelds":"decoy","fields":[{"name":"c","type":{"type":"enum","name":"c","symbols":["A","B"],"default":"A","dEfault":"B","sYmbols":["Z"]},"aLiases":["c_old"],"oRder":"descending"},{"name":"w","type":"int","dEfault":9},{"name":"list","type":{"type":"array","items":"int","iTems":"decoy"}},{"name":"m","type":{"type":"map","values":"int","vAlues":"decoy"}},{"name":"fx","type":{"type":"fixed","name":"fx","size":2,"sIze":99}},{"name":"px","type":{"type":"fixed","name":"px","size":4,"logicalType":"decimal","precision":6,"scale":2,"pRecision":60}},{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis","lOgicalType":"decoy"}}]}`,
		twin:    `{"type":"record","name":"Top","namespace":"ns","nAmespace":"decoy","fIelds":"decoy","fields":[{"name":"c","type":{"type":"enum","name":"c","symbols":["A","B"],"default":"A","dEfault":"B","sYmbols":["Z"]},"aLiases":["c_old"],"oRder":"descending"},{"name":"w","type":"int","dEfault":9},{"name":"list","type":{"type":"array","items":"int","iTems":"decoy"}},{"name":"m","type":{"type":"map","values":"int","vAlues":"decoy"}},{"name":"fx","type":{"type":"fixed","name":"fx","size":2,"sIze":99}},{"name":"px","type":{"type":"fixed","name":"px","size":4,"logicalType":"decimal","precision":6,"scale":2,"pRecision":60}},{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis","lOgicalType":"decoy"}}]}`,
		sample:  map[string]any{"c": "B", "w": int32(7), "list": []any{int32(1)}, "m": map[string]any{"k": int32(1)}, "fx": []byte{1, 2}, "px": big.NewRat(1234, 100), "ts": fwTime},

		resolveAgainst: `{"type":"record","name":"Top","namespace":"ns","fields":[{"name":"c","type":{"type":"enum","name":"c","symbols":["A","B"],"default":"A"},"aliases":["c_old"],"order":"descending"},{"name":"w","type":"int"},{"name":"list","type":{"type":"array","items":"int"}},{"name":"m","type":{"type":"map","values":"int"}},{"name":"fx","type":{"type":"fixed","name":"fx","size":2}},{"name":"px","type":{"type":"fixed","name":"px","size":4,"logicalType":"decimal","precision":6,"scale":2}},{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"pad","type":"int","default":5}]}`,
		resolveSample:  map[string]any{"c": "B", "w": int32(7), "list": []any{int32(1)}, "m": map[string]any{"k": int32(1)}, "fx": []byte{1, 2}, "px": big.NewRat(1234, 100), "ts": fwTime, "pad": int32(5)},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fIelds":"decoy","fields":[{"name":"e","type":"Elem","nAme":"decoy"}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fIelds":"decoy","fields":[{"name":"e","type":"Elem","nAme":"decoy"}]}`,
		refSample:  map[string]any{"e": map[string]any{"x": int32(1)}},

		defFeature:      `{"type":"record","name":"ns.HA","fields":[{"name":"d","type":{"type":"record","name":"DA","nAmespace":"decoyns","fields":[{"name":"x","type":"int"}]}}]}`,
		defTwin:         `{"type":"record","name":"ns.HA","fields":[{"name":"d","type":{"type":"record","name":"DA","nAmespace":"decoyns","fields":[{"name":"x","type":"int"}]}}]}`,
		defFollow:       `{"type":"record","name":"ns.FA","fields":[{"name":"d","type":"DA"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
}

// The three supported field-level logicalType lift shapes: a field-level
// logicalType annotation (plus precision/scale for decimal) whose type is a
// primitive STRING form, a union STRING form (first non-null branch), or a
// SINGLE OBJECT without its own annotation, is lifted into the type
// definition at parse. Twins spell the canonical nested form. The lift
// happens in the wire parser, so the LIFTED schema (logical effective)
// reaches the codec, canonical form, resolution, and SOE walkers, while the
// AS-WRITTEN field-level spelling survives in the schema text that String()
// and the Root() metadata tree re-consume — both sides of that split must
// keep describing the same wire behavior as the nested twin. Sample values
// (time.Time / *big.Rat) encode only if the logical is EFFECTIVE, so every
// encode-bearing cell dies if the lift is dropped.
var featureWalkerLiftRows = []featureWalkerRow{
	{
		// Shape 1: primitive string form. No reference can appear inside
		// the lifted subtree (the shape wraps a primitive by definition)
		// and no named type can be defined there — both cache directions
		// are skipped as structurally inapplicable.
		name:    "lift-logical-primitive-form",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"ts","type":"long","logicalType":"timestamp-millis"},{"name":"w","type":"int"}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"w","type":"int"}]}`,
		sample:  map[string]any{"ts": fwTime, "w": int32(1)},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"w","type":"int"},{"name":"pad","type":"int","default":5}]}`,
		resolveSample:  map[string]any{"ts": fwTime, "w": int32(1), "pad": int32(5)},
	},
	{
		// Shape 2: union string form — the lift lands on the first
		// non-null branch. Cache directions skipped as in shape 1.
		name:    "lift-logical-union-string-form",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"ts","type":["null","long"],"logicalType":"timestamp-millis"},{"name":"w","type":"int"}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}]},{"name":"w","type":"int"}]}`,
		sample:  map[string]any{"ts": fwTime, "w": int32(1)},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}]},{"name":"w","type":"int"},{"name":"pad","type":"int","default":5}]}`,
		resolveSample:  map[string]any{"ts": fwTime, "w": int32(1), "pad": int32(5)},
	},
	{
		// Shape 3: single-object form, with the decimal precision/scale
		// pair riding the lift. The main pair lifts onto bytes; the cache
		// DEFINITION direction lifts onto a NAMED fixed, so the collected
		// definition and its later splice must carry the lifted logical
		// identically to the nested twin. No reference position exists
		// inside the lifted subtree (skip documented).
		name:    "lift-logical-object-form",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"px","type":{"type":"bytes"},"logicalType":"decimal","precision":6,"scale":2},{"name":"w","type":"int"}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"px","type":{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}},{"name":"w","type":"int"}]}`,
		sample:  map[string]any{"px": big.NewRat(1234, 100), "w": int32(1)},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"px","type":{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}},{"name":"w","type":"int"},{"name":"pad","type":"int","default":5}]}`,
		resolveSample:  map[string]any{"px": big.NewRat(1234, 100), "w": int32(1), "pad": int32(5)},

		defFeature:      `{"type":"record","name":"ns.H9","fields":[{"name":"px","type":{"type":"fixed","name":"DF9","size":4},"logicalType":"decimal","precision":6,"scale":2}]}`,
		defTwin:         `{"type":"record","name":"ns.H9","fields":[{"name":"px","type":{"type":"fixed","name":"DF9","size":4,"logicalType":"decimal","precision":6,"scale":2}}]}`,
		defFollow:       `{"type":"record","name":"ns.F9","fields":[{"name":"d","type":"DF9"}]}`,
		defFollowSample: map[string]any{"d": big.NewRat(1234, 100)},
	},
}

// fwParse parses a schema with the row's opts, failing the test on error.
func fwParse(t *testing.T, schema string, row featureWalkerRow, extra ...avro.SchemaOpt) *avro.Schema {
	t.Helper()
	opts := append(slices.Clone(row.opts), extra...)
	s, err := avro.Parse(schema, opts...)
	if err != nil {
		t.Fatalf("parse: %v\n schema=%s", err, schema)
	}
	return s
}

// fwCacheParse registers defs and then parses main in one fresh SchemaCache.
func fwCacheParse(t *testing.T, defs []string, main string, row featureWalkerRow) *avro.Schema {
	t.Helper()
	var c avro.SchemaCache
	for i, d := range defs {
		if _, err := c.Parse(d, row.opts...); err != nil {
			t.Fatalf("cache def %d: %v", i, err)
		}
	}
	s, err := c.Parse(main, row.opts...)
	if err != nil {
		t.Fatalf("cache parse: %v\n schema=%s", err, main)
	}
	return s
}

// fwAssertTwinParity asserts the full self-containment / parity contract
// between a feature-spelled schema and its twin-spelled equivalent: byte-equal
// wire encoding of val, equal Canonical() and Rabin fingerprints, a String()
// and Canonical() that re-parse standalone to the same canonical bytes, and a
// Root() tree that rebuilds to the same fingerprint.
func fwAssertTwinParity(t *testing.T, sF, sT *avro.Schema, val map[string]any, row featureWalkerRow) {
	t.Helper()

	encF, err := sF.Encode(val)
	if err != nil {
		t.Fatalf("feature encode: %v", err)
	}
	encT, err := sT.Encode(val)
	if err != nil {
		t.Fatalf("twin encode: %v", err)
	}
	if !bytes.Equal(encF, encT) {
		t.Errorf("wire bytes diverge:\n feature=%x\n twin   =%x", encF, encT)
	}

	if !bytes.Equal(sF.Canonical(), sT.Canonical()) {
		t.Errorf("Canonical() diverges:\n feature=%s\n twin   =%s", sF.Canonical(), sT.Canonical())
	}
	if !bytes.Equal(sF.Fingerprint(avro.NewRabin()), sT.Fingerprint(avro.NewRabin())) {
		t.Errorf("Rabin fingerprint diverges for the same logical schema")
	}

	rp, err := avro.Parse(sF.String(), row.opts...)
	if err != nil {
		t.Errorf("Parse(feature.String()) FAILS — not self-contained: %v", err)
	} else if !bytes.Equal(rp.Canonical(), sF.Canonical()) {
		t.Errorf("feature String() re-parses to a DIFFERENT schema:\n reparse=%s\n orig   =%s", rp.Canonical(), sF.Canonical())
	}
	if _, err := avro.Parse(string(sF.Canonical()), row.opts...); err != nil {
		t.Errorf("Parse(feature.Canonical()) FAILS — not self-contained: %v", err)
	}

	root := sF.Root()
	rebuilt, err := root.Schema(row.opts...)
	if err != nil {
		t.Errorf("feature Root().Schema() rebuild FAILS: %v", err)
	} else if !bytes.Equal(rebuilt.Fingerprint(avro.NewRabin()), sT.Fingerprint(avro.NewRabin())) {
		t.Errorf("feature Root().Schema() rebuild fingerprint diverges from twin")
	}
}

// fwCountNodes walks a Root() tree, counting nodes. Root() represents an
// already-defined named type as a bare reference node (no re-definition), so
// the walk terminates without cycle tracking for these rows.
func fwCountNodes(n *avro.SchemaNode) int {
	if n == nil {
		return 0
	}
	c := 1
	for i := range n.Fields {
		c += fwCountNodes(&n.Fields[i].Type)
	}
	c += fwCountNodes(n.Items)
	c += fwCountNodes(n.Values)
	for i := range n.Branches {
		c += fwCountNodes(&n.Branches[i])
	}
	return c
}

var featureWalkerDrivers = []struct {
	name string
	run  func(t *testing.T, row featureWalkerRow)
}{
	{
		// Control: feature and twin are the same logical schema on the
		// wire — byte-equal binary and JSON encodes, equal decodes.
		name: "wire-parity",
		run: func(t *testing.T, row featureWalkerRow) {
			if row.sample == nil {
				t.Skip("parseable-but-unusable kind: no value inhabits it, nothing encodes")
			}
			sF := fwParse(t, row.feature, row)
			sT := fwParse(t, row.twin, row)
			encF, err := sF.Encode(row.sample)
			if err != nil {
				t.Fatalf("feature encode: %v", err)
			}
			encT, err := sT.Encode(row.sample)
			if err != nil {
				t.Fatalf("twin encode: %v", err)
			}
			if !bytes.Equal(encF, encT) {
				t.Fatalf("binary wire diverges:\n feature=%x\n twin   =%x", encF, encT)
			}
			jF, err := sF.EncodeJSON(row.sample)
			if err != nil {
				t.Fatalf("feature EncodeJSON: %v", err)
			}
			jT, err := sT.EncodeJSON(row.sample)
			if err != nil {
				t.Fatalf("twin EncodeJSON: %v", err)
			}
			if !bytes.Equal(jF, jT) {
				t.Fatalf("JSON wire diverges:\n feature=%s\n twin   =%s", jF, jT)
			}
			var gotF, gotT map[string]any
			if _, err := sF.Decode(encT, &gotF); err != nil {
				t.Fatalf("feature decode of twin bytes: %v", err)
			}
			if _, err := sT.Decode(encF, &gotT); err != nil {
				t.Fatalf("twin decode of feature bytes: %v", err)
			}
			if !reflect.DeepEqual(gotF, gotT) {
				t.Errorf("cross-decoded values diverge:\n feature=%#v\n twin   =%#v", gotF, gotT)
			}
		},
	},
	{
		name: "string-reparse",
		run: func(t *testing.T, row featureWalkerRow) {
			for _, spelled := range []struct {
				which  string
				schema string
			}{{"feature", row.feature}, {"twin", row.twin}} {
				s := fwParse(t, spelled.schema, row)
				rp, err := avro.Parse(s.String(), row.opts...)
				if err != nil {
					t.Errorf("%s: Parse(String()) fails: %v", spelled.which, err)
					continue
				}
				if !bytes.Equal(rp.Canonical(), s.Canonical()) {
					t.Errorf("%s: String() re-parses to a different schema:\n reparse=%s\n orig   =%s", spelled.which, rp.Canonical(), s.Canonical())
				}
			}
		},
	},
	{
		name: "canonical-rabin",
		run: func(t *testing.T, row featureWalkerRow) {
			sF := fwParse(t, row.feature, row)
			sT := fwParse(t, row.twin, row)
			if !bytes.Equal(sF.Canonical(), sT.Canonical()) {
				t.Errorf("Canonical() diverges:\n feature=%s\n twin   =%s", sF.Canonical(), sT.Canonical())
			}
			if !bytes.Equal(sF.Fingerprint(avro.NewRabin()), sT.Fingerprint(avro.NewRabin())) {
				t.Errorf("Rabin fingerprint diverges")
			}
			if _, err := avro.Parse(string(sF.Canonical()), row.opts...); err != nil {
				t.Errorf("Parse(feature.Canonical()) fails — not self-contained: %v", err)
			}
		},
	},
	{
		name: "root-rebuild",
		run: func(t *testing.T, row featureWalkerRow) {
			sF := fwParse(t, row.feature, row)
			sT := fwParse(t, row.twin, row)
			rootF, rootT := sF.Root(), sT.Root()
			if nF, nT := fwCountNodes(rootF), fwCountNodes(rootT); nF != nT {
				t.Errorf("Root() tree size diverges: feature=%d twin=%d nodes", nF, nT)
			}
			for _, spelled := range []struct {
				which string
				s     *avro.Schema
				root  *avro.SchemaNode
			}{{"feature", sF, rootF}, {"twin", sT, rootT}} {
				rebuilt, err := spelled.root.Schema(row.opts...)
				if err != nil {
					t.Errorf("%s: Root().Schema() rebuild fails: %v", spelled.which, err)
					continue
				}
				if !bytes.Equal(rebuilt.Fingerprint(avro.NewRabin()), spelled.s.Fingerprint(avro.NewRabin())) {
					t.Errorf("%s: Root().Schema() rebuild fingerprint diverges from original", spelled.which)
				}
			}
		},
	},
	{
		// Cross-parse reference INTO the feature's subtree: the cache's
		// splice walkers must produce self-contained JSON forms equal to
		// the twin spelling's.
		name: "cache-ref-into",
		run: func(t *testing.T, row featureWalkerRow) {
			if row.refFeature == "" {
				t.Skip("feature has no reference-bearing subtree")
			}
			sF := fwCacheParse(t, row.refDefs, row.refFeature, row)
			sT := fwCacheParse(t, row.refDefs, row.refTwin, row)
			fwAssertTwinParity(t, sF, sT, row.refSample, row)
		},
	},
	{
		// Named definition INSIDE the feature's subtree: the cache's
		// collection walker must capture it so a later parse's reference
		// splices.
		name: "cache-def-inside",
		run: func(t *testing.T, row featureWalkerRow) {
			if row.defFeature == "" {
				t.Skip("feature carries no definition position")
			}
			sF := fwCacheParse(t, []string{row.defFeature}, row.defFollow, row)
			sT := fwCacheParse(t, []string{row.defTwin}, row.defFollow, row)
			fwAssertTwinParity(t, sF, sT, row.defFollowSample, row)
		},
	},
	{
		// Schema resolution recursing the feature's subtree, both
		// directions (feature as writer, feature as reader).
		name: "resolve-both-directions",
		run: func(t *testing.T, row featureWalkerRow) {
			if row.resolveAgainst == "" {
				t.Skip("row has no resolve variant")
			}
			sF := fwParse(t, row.feature, row)
			sT := fwParse(t, row.twin, row)
			mod := fwParse(t, row.resolveAgainst, row)

			wire, err := sF.Encode(row.sample)
			if err != nil {
				t.Fatalf("feature encode: %v", err)
			}
			rsF, err := avro.Resolve(sF, mod)
			if err != nil {
				t.Fatalf("Resolve(feature, mod): %v", err)
			}
			rsT, err := avro.Resolve(sT, mod)
			if err != nil {
				t.Fatalf("Resolve(twin, mod): %v", err)
			}
			var gotF, gotT map[string]any
			if _, err := rsF.Decode(wire, &gotF); err != nil {
				t.Fatalf("resolved(feature-writer) decode: %v", err)
			}
			if _, err := rsT.Decode(wire, &gotT); err != nil {
				t.Fatalf("resolved(twin-writer) decode: %v", err)
			}
			if !reflect.DeepEqual(gotF, gotT) {
				t.Errorf("feature-as-writer resolved decode diverges:\n feature=%#v\n twin   =%#v", gotF, gotT)
			}

			wireMod, err := mod.Encode(row.resolveSample)
			if err != nil {
				t.Fatalf("mod encode: %v", err)
			}
			rs2F, err := avro.Resolve(mod, sF)
			if err != nil {
				t.Fatalf("Resolve(mod, feature): %v", err)
			}
			rs2T, err := avro.Resolve(mod, sT)
			if err != nil {
				t.Fatalf("Resolve(mod, twin): %v", err)
			}
			var got2F, got2T map[string]any
			if _, err := rs2F.Decode(wireMod, &got2F); err != nil {
				t.Fatalf("resolved(feature-reader) decode: %v", err)
			}
			if _, err := rs2T.Decode(wireMod, &got2T); err != nil {
				t.Fatalf("resolved(twin-reader) decode: %v", err)
			}
			if !reflect.DeepEqual(got2F, got2T) {
				t.Errorf("feature-as-reader resolved decode diverges:\n feature=%#v\n twin   =%#v", got2F, got2T)
			}
		},
	},
	{
		// Resolved JSON decoding: writer-shaped JSON through the resolving
		// schema, feature vs twin as the writer.
		name: "resolved-decode-json",
		run: func(t *testing.T, row featureWalkerRow) {
			if row.resolveAgainst == "" {
				t.Skip("row has no resolve variant")
			}
			sF := fwParse(t, row.feature, row)
			sT := fwParse(t, row.twin, row)
			mod := fwParse(t, row.resolveAgainst, row)
			jsonWire, err := sF.EncodeJSON(row.sample)
			if err != nil {
				t.Fatalf("feature EncodeJSON: %v", err)
			}
			rsF, err := avro.Resolve(sF, mod)
			if err != nil {
				t.Fatalf("Resolve(feature, mod): %v", err)
			}
			rsT, err := avro.Resolve(sT, mod)
			if err != nil {
				t.Fatalf("Resolve(twin, mod): %v", err)
			}
			var gotF, gotT map[string]any
			if err := rsF.DecodeJSON(jsonWire, &gotF); err != nil {
				t.Fatalf("resolved(feature-writer) DecodeJSON: %v", err)
			}
			if err := rsT.DecodeJSON(jsonWire, &gotT); err != nil {
				t.Fatalf("resolved(twin-writer) DecodeJSON: %v", err)
			}
			if !reflect.DeepEqual(gotF, gotT) {
				t.Errorf("resolved DecodeJSON diverges:\n feature=%#v\n twin   =%#v", gotF, gotT)
			}
		},
	},
	{
		// Custom types through resolution: a custom-baked WRITER forces
		// Resolve to build the custom-free writer view (an internal
		// re-parse of the feature spelling) for resolved JSON decoding,
		// and a custom-carrying READER applies its decoders through the
		// resolved decode. Both must treat the spellings identically. The
		// decode transform is value-changing (×10) so a skipped custom
		// cannot masquerade as a fired one.
		name: "resolve-custom-views",
		run: func(t *testing.T, row featureWalkerRow) {
			if row.resolveAgainst == "" {
				t.Skip("row has no resolve variant")
			}
			xform := avro.WithCustomType(avro.NewCustomType(
				"",
				(func(int32, *avro.SchemaNode) (int32, error))(nil),
				func(a int32, _ *avro.SchemaNode) (int32, error) { return a * 10, nil },
			))
			sFC := fwParse(t, row.feature, row, xform)
			sTC := fwParse(t, row.twin, row, xform)
			mod := fwParse(t, row.resolveAgainst, row)

			// Custom-baked writer: the custom-free view re-parses the
			// feature spelling internally.
			jsonWire, err := sFC.EncodeJSON(row.sample)
			if err != nil {
				t.Fatalf("feature EncodeJSON: %v", err)
			}
			rsF, err := avro.Resolve(sFC, mod)
			if err != nil {
				t.Fatalf("Resolve(feature+custom, mod): %v", err)
			}
			rsT, err := avro.Resolve(sTC, mod)
			if err != nil {
				t.Fatalf("Resolve(twin+custom, mod): %v", err)
			}
			var gotF, gotT map[string]any
			if err := rsF.DecodeJSON(jsonWire, &gotF); err != nil {
				t.Fatalf("resolved(custom-writer) DecodeJSON: %v", err)
			}
			if err := rsT.DecodeJSON(jsonWire, &gotT); err != nil {
				t.Fatalf("resolved(custom-twin-writer) DecodeJSON: %v", err)
			}
			if !reflect.DeepEqual(gotF, gotT) {
				t.Errorf("custom-free writer view decode diverges:\n feature=%#v\n twin   =%#v", gotF, gotT)
			}

			// Custom-carrying reader: reader customs fire through the
			// resolved binary decode.
			wireMod, err := mod.Encode(row.resolveSample)
			if err != nil {
				t.Fatalf("mod encode: %v", err)
			}
			rs2F, err := avro.Resolve(mod, sFC)
			if err != nil {
				t.Fatalf("Resolve(mod, feature+custom): %v", err)
			}
			rs2T, err := avro.Resolve(mod, sTC)
			if err != nil {
				t.Fatalf("Resolve(mod, twin+custom): %v", err)
			}
			var got2F, got2T map[string]any
			if _, err := rs2F.Decode(wireMod, &got2F); err != nil {
				t.Fatalf("resolved(custom-reader) decode: %v", err)
			}
			if _, err := rs2T.Decode(wireMod, &got2T); err != nil {
				t.Fatalf("resolved(custom-twin-reader) decode: %v", err)
			}
			if !reflect.DeepEqual(got2F, got2T) {
				t.Errorf("custom-reader resolved decode diverges:\n feature=%#v\n twin   =%#v", got2F, got2T)
			}
		},
	},
	{
		name: "compat",
		run: func(t *testing.T, row featureWalkerRow) {
			sF := fwParse(t, row.feature, row)
			sT := fwParse(t, row.twin, row)
			for _, pair := range []struct {
				which          string
				writer, reader *avro.Schema
			}{
				{"feature-self", sF, sF},
				{"twin-self", sT, sT},
				{"feature-writer", sF, sT},
				{"feature-reader", sT, sF},
			} {
				if err := avro.CheckCompatibility(pair.writer, pair.reader); err != nil {
					t.Errorf("CheckCompatibility(%s): %v", pair.which, err)
				}
			}
		},
	},
	{
		// Single-object encoding: byte-identical framing (the header
		// carries the writer fingerprint) and cross-spelling decode.
		name: "soe-roundtrip",
		run: func(t *testing.T, row featureWalkerRow) {
			if row.sample == nil {
				t.Skip("parseable-but-unusable kind: no value inhabits it, nothing encodes")
			}
			sF := fwParse(t, row.feature, row)
			sT := fwParse(t, row.twin, row)
			bF, err := sF.AppendSingleObject(nil, row.sample)
			if err != nil {
				t.Fatalf("feature AppendSingleObject: %v", err)
			}
			bT, err := sT.AppendSingleObject(nil, row.sample)
			if err != nil {
				t.Fatalf("twin AppendSingleObject: %v", err)
			}
			if !bytes.Equal(bF, bT) {
				t.Fatalf("single-object bytes diverge:\n feature=%x\n twin   =%x", bF, bT)
			}
			var gotF, gotT map[string]any
			if _, err := sT.DecodeSingleObject(bF, &gotT); err != nil {
				t.Fatalf("twin DecodeSingleObject(feature bytes): %v", err)
			}
			if _, err := sF.DecodeSingleObject(bT, &gotF); err != nil {
				t.Fatalf("feature DecodeSingleObject(twin bytes): %v", err)
			}
			if !reflect.DeepEqual(gotF, gotT) {
				t.Errorf("single-object cross-decodes diverge:\n feature=%#v\n twin   =%#v", gotF, gotT)
			}
		},
	},
}

func TestMatrix_FeatureWalkerParity(t *testing.T) {
	for _, row := range featureWalkerRows {
		t.Run(row.name, func(t *testing.T) {
			for _, d := range featureWalkerDrivers {
				t.Run(d.name, func(t *testing.T) { d.run(t, row) })
			}
		})
	}
}

// ---------- matrix_framing_test.go ----------

// ---------------------------------------------------------------------------
// Foreign-framing matrix: Avro permits several wire framings for the same
// array/map value — multiple blocks, negative-count blocks carrying a byte
// size, and non-canonical (overlong) varint counts. twmb's encoder emits
// only single-block canonical framing, so round-trip tests never exercise
// the alternatives; Java emits size-prefixed blocks when configured and
// foreign writers split large containers into many blocks. Every variant
// must decode to the same value and re-encode onto the canonical wire.
// ---------------------------------------------------------------------------

func putZigzag(dst []byte, n int64) []byte {
	u := uint64(n)<<1 ^ uint64(n>>63)
	for u >= 0x80 {
		dst = append(dst, byte(u)|0x80)
		u >>= 7
	}
	return append(dst, byte(u))
}

// putZigzagOverlong writes n as a deliberately non-canonical varint with one
// redundant continuation byte (e.g. 0x06 → 0x86 0x00).
func putZigzagOverlong(dst []byte, n int64) []byte {
	u := uint64(n)<<1 ^ uint64(n>>63)
	dst = append(dst, byte(u&0x7f)|0x80)
	u >>= 7
	for u >= 0x80 {
		dst = append(dst, byte(u)|0x80)
		u >>= 7
	}
	return append(dst, byte(u))
}

// frameVariants builds alternative wire framings for a container whose
// per-item encodings are given (for maps, each "item" is key+value).
func frameVariants(items [][]byte) map[string][]byte {
	n := int64(len(items))
	cat := func(bs [][]byte) []byte {
		var out []byte
		for _, b := range bs {
			out = append(out, b...)
		}
		return out
	}
	all := cat(items)

	variants := map[string][]byte{}

	// One block per item.
	var perItem []byte
	for _, it := range items {
		perItem = putZigzag(perItem, 1)
		perItem = append(perItem, it...)
	}
	perItem = append(perItem, 0x00)
	variants["block-per-item"] = perItem

	// Split: first item alone, remainder together.
	if n >= 2 {
		var split []byte
		split = putZigzag(split, 1)
		split = append(split, items[0]...)
		split = putZigzag(split, n-1)
		split = append(split, cat(items[1:])...)
		split = append(split, 0x00)
		variants["split-1-rest"] = split
	}

	// Negative count with byte size (the size-prefixed form).
	var sized []byte
	sized = putZigzag(sized, -n)
	sized = putZigzag(sized, int64(len(all)))
	sized = append(sized, all...)
	sized = append(sized, 0x00)
	variants["size-prefixed"] = sized

	// Size-prefixed, one block per item.
	var sizedPer []byte
	for _, it := range items {
		sizedPer = putZigzag(sizedPer, -1)
		sizedPer = putZigzag(sizedPer, int64(len(it)))
		sizedPer = append(sizedPer, it...)
	}
	sizedPer = append(sizedPer, 0x00)
	variants["size-prefixed-per-item"] = sizedPer

	// Canonical count written as an overlong varint.
	var over []byte
	over = putZigzagOverlong(over, n)
	over = append(over, all...)
	over = append(over, 0x00)
	variants["overlong-count"] = over

	return variants
}

func TestMatrix_ForeignContainerFraming(t *testing.T) {
	for _, fr := range matFrags() {
		t.Run(fr.label, func(t *testing.T) {
			u := &uniq{}
			itemSchemaJSON := fr.schema(u)
			itemSchema := avro.MustParse(itemSchemaJSON)
			v := fr.values[0]

			// Standalone per-item encodings.
			item, err := itemSchema.AppendEncode(nil, v)
			if err != nil {
				t.Fatalf("item encode: %v", err)
			}
			items := [][]byte{item, item, item}

			// ---- array ----
			u2 := &uniq{}
			arrSchema := avro.MustParse(fmt.Sprintf(`{"type":"array","items":%s}`, fr.schema(u2)))
			canonicalWire, err := arrSchema.AppendEncode(nil, []any{v, v, v})
			if err != nil {
				t.Fatalf("array encode: %v", err)
			}
			var want any
			if _, err := arrSchema.Decode(canonicalWire, &want); err != nil {
				t.Fatalf("canonical array decode: %v", err)
			}
			for name, wire := range frameVariants(items) {
				var got any
				rest, err := arrSchema.Decode(wire, &got)
				if err != nil || len(rest) != 0 {
					t.Fatalf("array %s decode: err=%v rest=%d\nwire=%x", name, err, len(rest), wire)
				}
				if !matEqual(got, want) {
					t.Fatalf("array %s value differs:\n got=%#v\nwant=%#v", name, got, want)
				}
				re, err := arrSchema.AppendEncode(nil, got)
				if err != nil || !bytes.Equal(re, canonicalWire) {
					t.Fatalf("array %s re-encode not canonical: err=%v\n re=%x\nwant=%x", name, err, re, canonicalWire)
				}
			}

			// ---- map (entries are key + value) ----
			u3 := &uniq{}
			mapSchema := avro.MustParse(fmt.Sprintf(`{"type":"map","values":%s}`, fr.schema(u3)))
			strSchema := avro.MustParse(`"string"`)
			var entries [][]byte
			keys := []string{"a", "b", "c"}
			for _, k := range keys {
				kb, _ := strSchema.AppendEncode(nil, k)
				entries = append(entries, append(kb, item...))
			}
			mv := map[string]any{"a": v, "b": v, "c": v}
			var mwant any
			mCanon, err := mapSchema.AppendEncode(nil, mv)
			if err != nil {
				t.Fatalf("map encode: %v", err)
			}
			if _, err := mapSchema.Decode(mCanon, &mwant); err != nil {
				t.Fatalf("canonical map decode: %v", err)
			}
			for name, wire := range frameVariants(entries) {
				var got any
				rest, err := mapSchema.Decode(wire, &got)
				if err != nil || len(rest) != 0 {
					t.Fatalf("map %s decode: err=%v rest=%d\nwire=%x", name, err, len(rest), wire)
				}
				if !matEqual(got, mwant) {
					t.Fatalf("map %s value differs:\n got=%#v\nwant=%#v", name, got, mwant)
				}
			}

			// Typed-slice targets see the same variants (the per-primitive
			// container fast loops have their own block-walking code, distinct
			// from the any-decode loop above). Assert the decoded ELEMENT VALUES
			// against the canonical any-decode (want), not just len — a fast
			// loop that mis-assembles values across split/negative-count/overlong
			// framings while still landing 3 slots must red here.
			wantArr := want.([]any)
			if fr.label == "int" {
				for name, wire := range frameVariants(items) {
					var typed []int32
					if _, err := arrSchema.Decode(wire, &typed); err != nil {
						t.Fatalf("typed array %s decode: %v", name, err)
					}
					if len(typed) != len(wantArr) {
						t.Fatalf("typed array %s: got len %d want %d (%v)", name, len(typed), len(wantArr), typed)
					}
					for i, e := range typed {
						if !matEqual(e, wantArr[i]) {
							t.Fatalf("typed array %s elem %d: got %v want %v", name, i, e, wantArr[i])
						}
					}
				}
			}
			if fr.label == "string" {
				for name, wire := range frameVariants(items) {
					var typed []string
					if _, err := arrSchema.Decode(wire, &typed); err != nil {
						t.Fatalf("typed string array %s decode: %v", name, err)
					}
					if len(typed) != len(wantArr) {
						t.Fatalf("typed string array %s: got len %d want %d (%v)", name, len(typed), len(wantArr), typed)
					}
					for i, e := range typed {
						if !matEqual(e, wantArr[i]) {
							t.Fatalf("typed string array %s elem %d: got %v want %v", name, i, e, wantArr[i])
						}
					}
				}
			}
		})
	}
}

// The same foreign framings inside a SKIPPED field: the skip path has its
// own block walker (including the byte-size fast-skip), which must consume
// every variant exactly.
func TestMatrix_ForeignFramingThroughSkip(t *testing.T) {
	wSchema := `{"type":"record","name":"R","fields":[
		{"name":"drop","type":{"type":"array","items":"string"}},
		{"name":"keep","type":"int"}]}`
	rSchema := `{"type":"record","name":"R","fields":[
		{"name":"keep","type":"int"}]}`
	w := avro.MustParse(wSchema)
	r := avro.MustParse(rSchema)
	res := mustResolve(t, w, r)
	strSchema := avro.MustParse(`"string"`)
	var items [][]byte
	for _, s := range []string{"x", "yy", "zzz"} {
		b, _ := strSchema.AppendEncode(nil, s)
		items = append(items, b)
	}
	keepWire, _ := avro.MustParse(`"int"`).AppendEncode(nil, int32(42))
	for name, arrWire := range frameVariants(items) {
		wire := append(append([]byte{}, arrWire...), keepWire...)
		var got map[string]any
		if _, err := res.Decode(wire, &got); err != nil {
			t.Fatalf("skip %s: %v", name, err)
		}
		if got["keep"] != int32(42) {
			t.Fatalf("skip %s corrupted following field: %#v", name, got)
		}
	}
}

// Multi-block array wires into FIXED-SIZE Go array targets: the remaining-
// capacity bound in deserFixedArray (count > arrLen-idx) is only
// distinguishable from a broken one once idx > 0 — i.e. on the SECOND
// block — because at idx=0 the subtraction is inert. A second block that
// over-claims must error ("got more"), never walk past the array end; an
// exact multi-block fill must succeed; an under-fill must report the
// shortfall. Single-block over-counts cannot pin this bound.
func TestMatrix_MultiBlockIntoFixedArray(t *testing.T) {
	s := avro.MustParse(`{"type":"array","items":"int"}`)
	item := func(v int64) []byte { return putZigzag(nil, v) }
	wire := func(blocks ...[]byte) []byte {
		var w []byte
		for _, b := range blocks {
			w = append(w, b...)
		}
		return append(w, 0x00)
	}
	block := func(items ...[]byte) []byte {
		b := putZigzag(nil, int64(len(items)))
		for _, it := range items {
			b = append(b, it...)
		}
		return b
	}

	t.Run("exact-fill-across-blocks", func(t *testing.T) {
		var out [2]int32
		w := wire(block(item(7)), block(item(8)))
		if _, err := s.Decode(w, &out); err != nil {
			t.Fatalf("two 1-item blocks into [2]int32: %v", err)
		}
		if out != [2]int32{7, 8} {
			t.Fatalf("got %v", out)
		}
	})
	t.Run("second-block-overclaims", func(t *testing.T) {
		var out [2]int32
		w := wire(block(item(7)), block(item(8), item(9)))
		if _, err := s.Decode(w, &out); err == nil {
			t.Fatal("3 items across blocks into [2]int32 must error, not panic")
		}
	})
	t.Run("underfill-reports-shortfall", func(t *testing.T) {
		var out [2]int32
		w := wire(block(item(7)))
		if _, err := s.Decode(w, &out); err == nil {
			t.Fatal("1 item into [2]int32 must error")
		}
	})
	t.Run("json-parity", func(t *testing.T) {
		var out [2]int32
		if err := s.DecodeJSON([]byte(`[7,8]`), &out); err != nil {
			t.Fatalf("JSON exact fill: %v", err)
		}
		if err := s.DecodeJSON([]byte(`[7,8,9]`), &out); err == nil {
			t.Fatal("JSON 3 items into [2]int32 must error")
		}
		if err := s.DecodeJSON([]byte(`[7]`), &out); err == nil {
			t.Fatal("JSON 1 item into [2]int32 must error")
		}
	})
}

// ---------- matrix_fuzz_test.go ----------

// FuzzMatrixCore bridges the curated matrix and the fuzzer: the fuzz input
// selects a (fragment, context, value) cell and supplies wire mutations, so
// CI fuzz time explores cell combinations and hostile-byte interactions the
// curated sweeps don't enumerate. The relational core invariants must hold
// for every selected cell; mutated wires must never panic the decoder.
func FuzzMatrixCore(f *testing.F) {
	frags := matFrags()
	ctxs := matCtxs()
	f.Add(uint8(0), uint8(0), uint8(0), []byte{})
	f.Add(uint8(3), uint8(8), uint8(1), []byte{0x00, 0xFF})
	f.Add(uint8(10), uint8(4), uint8(0), []byte{0x80, 0x80, 0x80})
	f.Fuzz(func(t *testing.T, fi, ci, vi uint8, mut []byte) {
		fr := frags[int(fi)%len(frags)]
		cx := ctxs[int(ci)%len(ctxs)]
		if cx.skip != nil && cx.skip(fr.kind) {
			return
		}
		u := &uniq{}
		schemaJSON := cx.schema(fr.schema(u), fr.kind, u)
		v := fr.values[int(vi)%len(fr.values)]
		vin := cx.wrap(v)

		s, err := avro.Parse(schemaJSON)
		if err != nil {
			t.Fatalf("matrix schema failed to parse: %v\n%s", err, schemaJSON)
		}
		w1, err := s.AppendEncode(nil, vin)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var a1 any
		mustDecode(t, s, w1, &a1)
		w2, err := s.AppendEncode(nil, a1)
		if err != nil || string(w2) != string(w1) {
			t.Fatalf("re-encode unstable: err=%v\n w1=%x\n w2=%x\nschema: %s", err, w1, w2, schemaJSON)
		}

		// Fuzz-driven hostile mutation: XOR the fuzzer's bytes over the
		// valid wire and decode — errors are fine, panics are findings
		// (the fuzz engine catches them itself).
		if len(mut) > 0 && len(w1) > 0 {
			hostile := append([]byte{}, w1...)
			for i, b := range mut {
				hostile[i%len(hostile)] ^= b
			}
			var sink any
			_, _ = s.Decode(hostile, &sink)
			_ = s.DecodeJSON(hostile, &sink)
		}
	})
}

// ---------- matrix_hostile_test.go ----------

// ---------------------------------------------------------------------------
// Hostile-wire matrix: deterministic, exhaustive truncation and per-byte
// corruption of every composed schema's valid wire. Unlike fuzzing (random
// sampling), every prefix and every single-byte mutant of every cell runs
// on every test execution. The invariant is purely defensive: never panic,
// and a successful decode must have consumed the entire input.
// ---------------------------------------------------------------------------

func hostileDecode(t *testing.T, s *avro.Schema, schemaJSON string, wire []byte, what string) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("PANIC decoding %s of schema %s: %v\nwire: %x", what, schemaJSON, r, wire)
		}
	}()
	var sink any
	rest, err := s.Decode(wire, &sink)
	if err == nil && len(rest) != 0 {
		// Decode reports leftover bytes; a "successful" partial decode is
		// fine as long as it is honest about the remainder.
		_ = rest
	}
}

func hostileDecodeJSON(t *testing.T, s *avro.Schema, schemaJSON string, j []byte, what string) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("PANIC json-decoding %s of schema %s: %v\ninput: %q", what, schemaJSON, r, j)
		}
	}()
	var sink any
	_ = s.DecodeJSON(j, &sink)
}

func TestMatrix_HostileTruncationAndCorruption(t *testing.T) {
	frags := matFrags()
	ctxs := matCtxs()
	for _, fr := range frags {
		for _, cx := range ctxs {
			if cx.skip != nil && cx.skip(fr.kind) {
				continue
			}
			t.Run(fr.label+"/"+cx.label, func(t *testing.T) {
				u := &uniq{}
				schemaJSON := cx.schema(fr.schema(u), fr.kind, u)
				s := mustParse(t, schemaJSON)
				vin := cx.wrap(fr.values[0])
				w1 := mustAppendEncode(t, s, nil, vin)
				j1 := mustAppendEncodeJSON(t, s, nil, vin)

				// Every strict prefix of the valid binary wire.
				for i := 0; i < len(w1); i++ {
					hostileDecode(t, s, schemaJSON, w1[:i], fmt.Sprintf("prefix[:%d]", i))
				}
				// Every single-byte mutant (three mutations per position).
				mut := make([]byte, len(w1))
				for i := 0; i < len(w1); i++ {
					for _, b := range []byte{0x00, 0xFF, w1[i] + 1} {
						copy(mut, w1)
						mut[i] = b
						hostileDecode(t, s, schemaJSON, mut, fmt.Sprintf("mutant[%d]=%#x", i, b))
					}
				}
				// JSON prefixes (capped — JSON wires can be longer).
				step := 1
				if len(j1) > 64 {
					step = len(j1) / 64
				}
				for i := 0; i < len(j1); i += step {
					hostileDecodeJSON(t, s, schemaJSON, j1[:i], fmt.Sprintf("jsonprefix[:%d]", i))
				}
				// JSON single-byte mutants (same cap).
				jmut := make([]byte, len(j1))
				for i := 0; i < len(j1); i += step {
					for _, b := range []byte{0x00, '{', '"'} {
						copy(jmut, j1)
						jmut[i] = b
						hostileDecodeJSON(t, s, schemaJSON, jmut, fmt.Sprintf("jsonmutant[%d]=%#x", i, b))
					}
				}
			})
		}
	}
}

// The same defensive sweep through RESOLVED schemas: promotion wrappers and
// skip paths get the hostile bytes too (a dropped trailing field makes the
// skip path consume the mutated region).
func TestMatrix_HostileThroughResolution(t *testing.T) {
	wSchema := `{"type":"record","name":"R","fields":[
		{"name":"keep","type":"int"},
		{"name":"drop","type":{"type":"array","items":["null","string",{"type":"record","name":"N","fields":[
			{"name":"v","type":"long"},{"name":"next","type":["null","N"],"default":null}]}]}},
		{"name":"tail","type":"string"}]}`
	rSchema := `{"type":"record","name":"R","fields":[
		{"name":"keep","type":"long"},
		{"name":"tail","type":"string"}]}`
	w := avro.MustParse(wSchema)
	r := avro.MustParse(rSchema)
	res := mustResolve(t, w, r)
	vin := map[string]any{
		"keep": int32(7),
		"drop": []any{
			nil, "s",
			map[string]any{"v": int64(1), "next": map[string]any{"v": int64(2), "next": nil}},
		},
		"tail": "end",
	}
	w1 := mustAppendEncode(t, w, nil, vin)
	for i := 0; i < len(w1); i++ {
		hostileDecode(t, res, "resolved(R)", w1[:i], fmt.Sprintf("prefix[:%d]", i))
	}
	mut := make([]byte, len(w1))
	for i := 0; i < len(w1); i++ {
		for _, b := range []byte{0x00, 0xFF, w1[i] + 1} {
			copy(mut, w1)
			mut[i] = b
			hostileDecode(t, res, "resolved(R)", mut, fmt.Sprintf("mutant[%d]=%#x", i, b))
		}
	}
}

// ---------- matrix_hostile_size_test.go ----------

// ---------------------------------------------------------------------------
// Hostile-SIZE rejection axis: megabyte-scale wrong values driven at every
// encode arm. The rejection itself is correctness; the axis asserts the two
// DoS postures around it — the reject is FAST (no superlinear parse work
// before the type check) and the error message is BOUNDED (no echoing the
// hostile input back; the trunc-helper contract).
// ---------------------------------------------------------------------------

func TestMatrix_HostileSizeRejects(t *testing.T) {
	bigStr := strings.Repeat("x", 1<<20)
	bigBytes := []byte(bigStr)
	bigNum := json.Number(strings.Repeat("9", 1<<20))
	bigKeyMap := map[string]any{bigStr: int32(1)}

	cases := []struct {
		label  string
		schema string
		bad    any
	}{
		{"string-into-int", `"int"`, bigStr},
		{"bytes-into-long", `"long"`, bigBytes},
		{"string-into-boolean", `"boolean"`, bigStr},
		{"hugenum-into-int", `"int"`, bigNum},
		{"hugenum-into-long", `"long"`, bigNum},
		{"hugenum-into-float", `"float"`, bigNum},
		{"hugenum-into-timestamp", `{"type":"long","logicalType":"timestamp-millis"}`, bigNum},
		{"string-into-fixed16", `{"type":"fixed","name":"HF","size":16}`, bigStr},
		{"bytes-into-fixed16", `{"type":"fixed","name":"HF","size":16}`, bigBytes},
		{"symbol-into-enum", `{"type":"enum","name":"HE","symbols":["A","B"]}`, bigStr},
		{"hugenum-into-decimal", `{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}`, bigNum},
		{"string-into-array", `{"type":"array","items":"int"}`, bigStr},
		{"hugekey-into-record", `{"type":"record","name":"HR","fields":[{"name":"a","type":"int"}]}`, bigKeyMap},
		{"string-into-nullunion", `["null","int"]`, bigStr},
		{"string-into-uuid-fixed", `{"type":"fixed","name":"HU","size":16,"logicalType":"uuid"}`, bigStr},
	}
	// The reject is locally ~µs; 250ms is generous CI headroom. Under -race,
	// instrumentation inflates the bounded reject past 250ms, so relax to a
	// ~3s ceiling there — a superlinear blowup before the type check is
	// multi-second and still trips it (see raceRelaxed).
	maxDur := raceRelaxed(250 * time.Millisecond)
	const maxErrLen = 2 << 10

	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			s := avro.MustParse(c.schema)

			start := time.Now()
			_, err := s.AppendEncode(nil, c.bad)
			d := time.Since(start)
			if err == nil {
				t.Fatalf("hostile value unexpectedly accepted (binary)")
			}
			if d > maxDur {
				t.Errorf("binary reject took %v (> %v): superlinear work before the type check", d, maxDur)
			}
			if n := len(err.Error()); n > maxErrLen {
				t.Errorf("binary reject error echoes hostile input: %d bytes", n)
			}

			start = time.Now()
			_, jerr := s.AppendEncodeJSON(nil, c.bad)
			d = time.Since(start)
			if jerr == nil {
				t.Fatalf("hostile value unexpectedly accepted (JSON)")
			}
			if d > maxDur {
				t.Errorf("JSON reject took %v (> %v)", d, maxDur)
			}
			if n := len(jerr.Error()); n > maxErrLen {
				t.Errorf("JSON reject error echoes hostile input: %d bytes", n)
			}
		})
	}
}

// Hostile-size DECODE-target rejects: a valid small wire decoded into a
// mismatched target must reject with a bounded message too (the wire side
// of the same posture; the wire itself is small, so only message size and
// promptness are interesting).
func TestMatrix_HostileSizeDecodeMessages(t *testing.T) {
	s := avro.MustParse(`"string"`)
	big := strings.Repeat("y", 1<<20)
	wire := mustAppendEncode(t, s, nil, big)
	// Decoding a 1 MiB string wire into an int target: the rejection must
	// not echo the megabyte of wire content.
	var i int32
	start := time.Now()
	_, derr := s.Decode(wire, &i)
	d := time.Since(start)
	if derr == nil {
		t.Fatal("string wire into int target unexpectedly accepted")
	}
	if bound := raceRelaxed(250 * time.Millisecond); d > bound {
		t.Errorf("decode reject took %v (>%v)", d, bound)
	}
	if n := len(derr.Error()); n > 2<<10 {
		t.Errorf("decode reject error echoes wire content: %d bytes", n)
	}
}

// ---------- matrix_json_strictness_parity_test.go ----------

// ---------------------------------------------------------------------------
// JSON strictness-parity net — the standing guard for the "skip path is a
// second parser" class.
//
// DecodeJSON has TWO JSON parsers: the VALUE path (known reader fields, fully
// validating) and the SKIP path (unknown reader fields, json_scan.skipValue).
// Whether a byte sequence is "valid JSON" must NOT depend on which one
// processes it — but the skip path silently drifted lax (it accepted 1.2.3,
// "\q", [}], missing commas) because every strictness test drove only the
// value path. This is the same shape as the reflect/unsafe encode twins
// (TestMatrix_ReflectUnsafePathParity) and the scale axis: two paths that
// must agree, tested on only one.
//
// The invariant is calibration-free: for each fragment, the SKIP-path verdict
// must EQUAL the VALUE-path verdict. (The fragment's static type matches the
// known field's schema, so a verdict difference can only come from JSON-
// grammar strictness, not type-checking — the skip path is schema-less and
// correctly does not type-check.) Driven both as a leaf field and nested
// inside a skipped container, so the recursive skip validators are exercised.
// ---------------------------------------------------------------------------

func TestMatrix_JSONStrictnessParityKnownVsSkip(t *testing.T) {
	// Each fragment is paired with a reader field type that ACCEPTS its
	// well-formed form, so any known-vs-skip verdict difference is purely a
	// JSON-grammar-strictness difference.
	corpus := []struct {
		frag      string
		knownType string
	}{
		// numbers (malformed + valid) against "double"
		{`1.2.3`, `"double"`}, {`1e`, `"double"`}, {`5.`, `"double"`}, {`01`, `"double"`},
		{`-`, `"double"`}, {`.5`, `"double"`}, {`1.`, `"double"`}, {`1e+`, `"double"`},
		{`-3.14e10`, `"double"`}, {`0`, `"double"`}, {`0.5`, `"double"`}, {`123`, `"double"`},
		// strings (malformed + valid) against "string"
		{`"\q"`, `"string"`}, {`"\x41"`, `"string"`}, {`"\u00"`, `"string"`}, {`"abc`, `"string"`},
		{`"ok"`, `"string"`}, {`"A"`, `"string"`}, {`"with \"quote\""`, `"string"`}, {`""`, `"string"`},
		// arrays (malformed + valid) against array<long>
		{`[}]`, `{"type":"array","items":"long"}`},
		{`[1 2 3]`, `{"type":"array","items":"long"}`},
		{`[1,2,]`, `{"type":"array","items":"long"}`},
		{`[,1]`, `{"type":"array","items":"long"}`},
		{`[1,2,3]`, `{"type":"array","items":"long"}`},
		{`[]`, `{"type":"array","items":"long"}`},
		// objects/maps (malformed + valid) against map<long>
		{`{]}`, `{"type":"map","values":"long"}`},
		{`{"a" 1}`, `{"type":"map","values":"long"}`},
		{`{"a"::1}`, `{"type":"map","values":"long"}`},
		{`{"a":1,}`, `{"type":"map","values":"long"}`},
		{`{a:1}`, `{"type":"map","values":"long"}`},
		{`{"a":1}`, `{"type":"map","values":"long"}`},
		{`{}`, `{"type":"map","values":"long"}`},
		// booleans / null
		{`true`, `"boolean"`}, {`tru`, `"boolean"`}, {`null`, `["null","long"]`},
	}

	for _, c := range corpus {
		t.Run(c.frag, func(t *testing.T) {
			// Reader A KNOWS field "f" (value path); reader B does NOT, so "f"
			// is skipped (skip path). Same document fed to both.
			known := avro.MustParse(fmt.Sprintf(
				`{"type":"record","name":"R","fields":[{"name":"f","type":%s}]}`, c.knownType))
			skip := avro.MustParse(
				`{"type":"record","name":"R","fields":[{"name":"other","type":["null","long"],"default":null}]}`)

			doc := []byte(fmt.Sprintf(`{"f":%s}`, c.frag))
			var a, b any
			valueRejects := known.DecodeJSON(doc, &a) != nil
			skipRejects := skip.DecodeJSON(doc, &b) != nil

			if valueRejects != skipRejects {
				t.Fatalf("STRICTNESS DIVERGENCE for %s: value-path rejects=%v, skip-path rejects=%v (the two JSON parsers disagree)",
					c.frag, valueRejects, skipRejects)
			}

			// Cross-check the verdict against encoding/json for the leaf
			// fragment, so the parity isn't "both wrong the same way".
			if jv := json.Valid([]byte(c.frag)); jv == valueRejects {
				t.Errorf("%s: json.Valid=%v but DecodeJSON value-path rejects=%v — both twmb parsers may agree but disagree with stdlib",
					c.frag, jv, valueRejects)
			}
		})
	}
}

// TestMatrix_JSONStrictnessParityNested drives the same corpus NESTED inside a
// skipped container, exercising skipArrayStrict/skipObjectStrict recursion —
// the malformed fragment is buried one level down in an unknown field.
func TestMatrix_JSONStrictnessParityNested(t *testing.T) {
	skip := avro.MustParse(
		`{"type":"record","name":"R","fields":[{"name":"keep","type":"long"}]}`)

	frags := []struct {
		frag      string
		malformed bool
	}{
		{`1.2.3`, true}, {`"\q"`, true}, {`[}]`, true}, {`{"a" 1}`, true}, {`[1,2,]`, true},
		{`42`, false}, {`"ok"`, false}, {`[1,2,3]`, false}, {`{"a":1}`, false},
	}
	for _, c := range frags {
		t.Run(c.frag, func(t *testing.T) {
			// The malformed fragment is the value of an unknown field's
			// nested array and object, so the recursive skip validators
			// must reach it.
			for _, wrap := range []string{
				`{"keep":1,"x":[%s]}`,     // inside skipped array
				`{"keep":1,"x":{"y":%s}}`, // inside skipped object
				`{"keep":1,"x":[[%s]]}`,   // doubly nested
			} {
				doc := []byte(fmt.Sprintf(wrap, c.frag))
				var out any
				err := skip.DecodeJSON(doc, &out)
				if c.malformed && err == nil {
					t.Errorf("nested skipped malformed %s in %q ACCEPTED (skip recursion not validating)", c.frag, wrap)
				}
				if !c.malformed && err != nil {
					t.Errorf("nested skipped valid %s in %q REJECTED: %v", c.frag, wrap, err)
				}
			}
		})
	}
}

// ---------- matrix_jsonnumber_test.go ----------

// ---------------------------------------------------------------------------
// Generative json.Number policy net.
//
// The documented policy (doc.go "Encoding from JSON input") is: json.Number
// is a NUMERIC carrier — accepted for numeric Avro types (int/long/float/
// double and their logical variants), REJECTED for stringy types (string/
// bytes/fixed/enum) on BOTH encode and decode, with map keys the one
// content-validated exception. Before this net that policy was asserted only
// by ~12 hand-written TestRegression_JSONNumber* pins at specific call sites;
// neutering any single guard (the encode rejects, the decode rejects, the
// per-key validation, or the Pattern-14c fast-path gates) was caught ONLY by
// those pins — the combinatorial matrix, property, and invariant nets all
// stayed green, because the matrix carried json.Number as a numeric ACCEPT
// target but never swept the REJECT direction across positions. A json.Number
// bug in a position nobody pinned was therefore invisible.
//
// This net sweeps the policy as a cross-product: {numeric, stringy} schema ×
// {top, field, array-item, map-value} position × {encode-source,
// decode-target} direction × {binary, JSON} wire. Numeric cells must accept
// (and round-trip); stringy cells must REJECT on both wires. New schema
// fragments or positions inherit the invariant automatically.
// ---------------------------------------------------------------------------

var jnNumericSchemas = []struct {
	label  string
	schema string
}{
	{"int", `"int"`},
	{"long", `"long"`},
	{"float", `"float"`},
	{"double", `"double"`},
	{"date", `{"type":"int","logicalType":"date"}`},
	{"time-millis", `{"type":"int","logicalType":"time-millis"}`},
	{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`},
	{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`},
	// decimal/big-decimal are the EXEMPT bytes-backed case: json.Number
	// carries the RatString (RFC-8259-valid by construction), so they are
	// numeric-accept, not stringy-reject. scale 0 / precision 18 lets the
	// integer content battery round-trip exactly.
	{"decimal", `{"type":"bytes","logicalType":"decimal","precision":18,"scale":0}`},
	{"decimal-fixed", `{"type":"fixed","name":"JNDF","size":9,"logicalType":"decimal","precision":18,"scale":0}`},
	{"big-decimal", `{"type":"bytes","logicalType":"big-decimal"}`},
}

var jnStringySchemas = []struct {
	label  string
	schema string
}{
	{"string", `"string"`},
	{"bytes", `"bytes"`},
	{"fixed", `{"type":"fixed","name":"JNF","size":2}`},
	{"enum", `{"type":"enum","name":"JNE","symbols":["A","B"]}`},
}

// jnPositions wrap a leaf schema and a leaf value into a composed schema and
// value, and describe how to build a typed decode target carrying
// json.Number at the leaf.
var jnPositions = []struct {
	label     string
	schema    func(leaf string) string
	encodeVal func(leaf any) any                 // leaf is a json.Number
	target    func(t reflect.Type) reflect.Value // ptr to a value with json.Number leaves
}{
	{"top",
		func(leaf string) string { return leaf },
		func(leaf any) any { return leaf },
		func(t reflect.Type) reflect.Value { return reflect.New(t) }},
	{"field",
		func(leaf string) string {
			return fmt.Sprintf(`{"type":"record","name":"JNR","fields":[{"name":"f","type":%s}]}`, leaf)
		},
		func(leaf any) any { return map[string]any{"f": leaf} },
		func(t reflect.Type) reflect.Value {
			return reflect.New(reflect.MapOf(reflect.TypeFor[string](), t))
		}},
	{"array-item",
		func(leaf string) string { return fmt.Sprintf(`{"type":"array","items":%s}`, leaf) },
		func(leaf any) any { return []any{leaf} },
		func(t reflect.Type) reflect.Value { return reflect.New(reflect.SliceOf(t)) }},
	{"map-value",
		func(leaf string) string { return fmt.Sprintf(`{"type":"map","values":%s}`, leaf) },
		func(leaf any) any { return map[string]any{"k": leaf} },
		func(t reflect.Type) reflect.Value {
			return reflect.New(reflect.MapOf(reflect.TypeFor[string](), t))
		}},
	// struct-field exercises the ADDRESSABLE unsafe struct-field fast path
	// (unsafe.go gates on stringFastPathEligible{Encode,Decode}) — a code
	// path the map/slice/typed-map targets above never reach.
	{"struct-field",
		func(leaf string) string {
			return fmt.Sprintf(`{"type":"record","name":"JNSR","fields":[{"name":"f","type":%s}]}`, leaf)
		},
		func(leaf any) any {
			st := reflect.StructOf([]reflect.StructField{
				{Name: "F", Type: reflect.TypeOf(leaf), Tag: `avro:"f"`},
			})
			p := reflect.New(st) // POINTER → addressable → unsafe encode path
			p.Elem().Field(0).Set(reflect.ValueOf(leaf))
			return p.Interface()
		},
		func(t reflect.Type) reflect.Value {
			st := reflect.StructOf([]reflect.StructField{
				{Name: "F", Type: t, Tag: `avro:"f"`},
			})
			return reflect.New(st) // addressable struct → unsafe path
		}},
	// nullable-union exercises union branch dispatch: a numeric branch must
	// accept json.Number; a stringy-only union must reject it.
	{"nullable-union",
		func(leaf string) string { return fmt.Sprintf(`["null",%s]`, leaf) },
		func(leaf any) any { return leaf },
		func(t reflect.Type) reflect.Value { return reflect.New(reflect.PointerTo(t)) }},
}

func TestMatrix_JSONNumberPolicy(t *testing.T) {
	jnType := reflect.TypeFor[json.Number]()

	for _, pos := range jnPositions {
		// ---- NUMERIC schemas: json.Number must ACCEPT (encode + decode). ----
		for _, sc := range jnNumericSchemas {
			t.Run("numeric/"+sc.label+"/"+pos.label, func(t *testing.T) {
				s := avro.MustParse(pos.schema(sc.schema))
				in := pos.encodeVal(json.Number("42"))
				// Encode source: both wires must accept.
				wire, err := s.AppendEncode(nil, in)
				if err != nil {
					t.Fatalf("binary encode of json.Number source rejected for numeric schema: %v", err)
				}
				if _, err := s.AppendEncodeJSON(nil, in); err != nil {
					t.Fatalf("JSON encode of json.Number source rejected for numeric schema: %v", err)
				}
				// Decode target: a json.Number-leaf target must accept the
				// numeric wire on both formats.
				tgt := pos.target(jnType)
				if _, err := s.Decode(wire, tgt.Interface()); err != nil {
					t.Fatalf("binary decode into json.Number target rejected for numeric schema: %v", err)
				}
				jwire, _ := s.AppendEncodeJSON(nil, in)
				jtgt := pos.target(jnType)
				if err := s.DecodeJSON(jwire, jtgt.Interface()); err != nil {
					t.Fatalf("JSON decode into json.Number target rejected for numeric schema: %v", err)
				}

				// Resolved-decode path (identity resolution): a json.Number
				// target must decode the same through resolve.go's resolved
				// deser, not just the natural deser.
				res, rerr := avro.Resolve(avro.MustParse(pos.schema(sc.schema)), avro.MustParse(pos.schema(sc.schema)))
				if rerr != nil {
					t.Fatalf("identity Resolve: %v", rerr)
				}
				if _, err := res.Decode(wire, pos.target(jnType).Interface()); err != nil {
					t.Fatalf("resolved decode into json.Number target rejected for numeric schema: %v", err)
				}

				// Non-numeric / malformed json.Number content must REJECT on
				// encode (the type's RFC-8259 invariant: its underlying
				// string must be a valid number). This exercises the
				// content-validating arms — e.g. the decimal encode arm's
				// boundedRatFromString — which an integer-only battery never
				// reaches (a numerically-valid value coerces identically with
				// or without the validation).
				for _, bad := range []string{"notanumber", "", "1.2.3"} {
					if _, err := s.AppendEncode(nil, pos.encodeVal(json.Number(bad))); err == nil {
						t.Errorf("binary encode of malformed json.Number(%q) ACCEPTED for numeric schema (must reject)", bad)
					}
					if _, err := s.AppendEncodeJSON(nil, pos.encodeVal(json.Number(bad))); err == nil {
						t.Errorf("JSON encode of malformed json.Number(%q) ACCEPTED for numeric schema (must reject)", bad)
					}
				}

				// Content variety with a WIRE-STABLE round-trip: encode
				// json.Number(content) -> decode into a json.Number target ->
				// re-encode -> must reproduce the ORIGINAL wire. This is
				// calibration-free (no hardcoded expected string — date/
				// timestamp/decimal each transform the content differently)
				// and catches CONTENT corruption a success-only check misses:
				// neutering the json.Number numeric-setter / decimal arms lets
				// decode still succeed but produce a wrong value, which then
				// re-encodes to different bytes.
				for _, content := range []string{"0", "-1", "127", "2147483647"} {
					cin := pos.encodeVal(json.Number(content))
					cw, cerr := s.AppendEncode(nil, cin)
					if cerr != nil {
						t.Errorf("encode json.Number(%q) rejected for numeric schema: %v", content, cerr)
						continue
					}
					ctgt := pos.target(jnType)
					if _, err := s.Decode(cw, ctgt.Interface()); err != nil {
						t.Errorf("decode json.Number(%q) wire into json.Number target failed: %v", content, err)
						continue
					}
					// Re-encode the decoded json.Number tree; must match cw.
					reW, reErr := s.AppendEncode(nil, ctgt.Elem().Interface())
					if reErr != nil {
						t.Errorf("re-encode of decoded json.Number(%q) failed: %v", content, reErr)
						continue
					}
					if !bytes.Equal(reW, cw) {
						t.Errorf("json.Number(%q) NOT wire-stable through json.Number target:\n in=%x\n out=%x", content, cw, reW)
					}
				}
			})
		}

		// ---- STRINGY schemas: json.Number must REJECT (encode + decode). ----
		for _, sc := range jnStringySchemas {
			t.Run("stringy/"+sc.label+"/"+pos.label, func(t *testing.T) {
				s := avro.MustParse(pos.schema(sc.schema))

				// Encode source: a json.Number leaf must be rejected on both
				// wires (it is a numeric carrier; a text/binary target is a
				// type mismatch).
				in := pos.encodeVal(json.Number("42"))
				if _, err := s.AppendEncode(nil, in); err == nil {
					t.Errorf("binary encode of json.Number ACCEPTED for stringy schema %s (must reject)", sc.label)
				}
				if _, err := s.AppendEncodeJSON(nil, in); err == nil {
					t.Errorf("JSON encode of json.Number ACCEPTED for stringy schema %s (must reject)", sc.label)
				}

				// Decode target: a valid stringy wire decoded INTO a
				// json.Number leaf must reject on both wires. Build the wire
				// from a string-typed source the schema accepts.
				strLeaf := jnStringSample(sc.label)
				goodIn := pos.encodeVal(strLeaf)
				wire, err := s.AppendEncode(nil, goodIn)
				if err != nil {
					t.Fatalf("setup: encoding a valid stringy value failed: %v", err)
				}
				if _, err := s.Decode(wire, pos.target(jnType).Interface()); err == nil {
					t.Errorf("binary decode of stringy wire INTO json.Number target ACCEPTED for %s (must reject)", sc.label)
				}
				jwire, err := s.AppendEncodeJSON(nil, goodIn)
				if err != nil {
					t.Fatalf("setup: JSON-encoding a valid stringy value failed: %v", err)
				}
				if err := s.DecodeJSON(jwire, pos.target(jnType).Interface()); err == nil {
					t.Errorf("JSON decode of stringy wire INTO json.Number target ACCEPTED for %s (must reject)", sc.label)
				}
			})
		}
	}

	// ---- map[json.Number]V KEY: the documented content-validated exception. ----
	// Numeric-content keys round-trip; non-numeric keys reject — on both wires.
	t.Run("map-key-numeric-roundtrips", func(t *testing.T) {
		s := avro.MustParse(`{"type":"map","values":"int"}`)
		in := map[json.Number]int32{"7": 1, "42": 2}
		wire, err := s.AppendEncode(nil, in)
		if err != nil {
			t.Fatalf("encode map[json.Number]int32 with numeric keys: %v", err)
		}
		var out map[json.Number]int32
		if _, err := s.Decode(wire, &out); err != nil {
			t.Fatalf("decode into map[json.Number]int32: %v", err)
		}
		if out["7"] != 1 || out["42"] != 2 {
			t.Fatalf("map-key round-trip: %v", out)
		}
		// JSON parity.
		jwire, _ := s.AppendEncodeJSON(nil, in)
		var jout map[json.Number]int32
		if err := s.DecodeJSON(jwire, &jout); err != nil {
			t.Fatalf("JSON decode into map[json.Number]int32: %v", err)
		}
	})

	// map[json.Number]V with NON-NUMERIC key content must REJECT — the
	// per-key validation, distinct from the round-trip above (which uses
	// valid keys and passes regardless of the guard). A json.Number whose
	// underlying string is not a valid number violates the type's own RFC
	// 8259 contract, so it cannot be a map key.
	t.Run("map-key-nonnumeric-rejects", func(t *testing.T) {
		s := avro.MustParse(`{"type":"map","values":"int"}`)

		// Encode source: a non-numeric json.Number key rejects on both wires.
		bad := map[json.Number]int32{"notanumber": 1}
		if _, err := s.AppendEncode(nil, bad); err == nil {
			t.Error("binary encode of map[json.Number]int32 with non-numeric key ACCEPTED (must reject)")
		}
		if _, err := s.AppendEncodeJSON(nil, bad); err == nil {
			t.Error("JSON encode of map[json.Number]int32 with non-numeric key ACCEPTED (must reject)")
		}

		// Decode target: a valid map wire whose KEY is a non-numeric string,
		// decoded INTO map[json.Number]V, must reject (the wire key has no
		// json.Number representation). This is the path the fast-path gate
		// (deser.go: mapTyp.Key() != jsonNumberType) routes to the validating
		// slow loop — neutering that gate is caught here.
		wire, err := s.AppendEncode(nil, map[string]int32{"notanumber": 5})
		if err != nil {
			t.Fatalf("setup: encode valid string-key map: %v", err)
		}
		var out map[json.Number]int32
		if _, err := s.Decode(wire, &out); err == nil {
			t.Error("binary decode of non-numeric-key wire INTO map[json.Number]int32 ACCEPTED (must reject)")
		}
		jwire, _ := s.AppendEncodeJSON(nil, map[string]int32{"notanumber": 5})
		var jout map[json.Number]int32
		if err := s.DecodeJSON(jwire, &jout); err == nil {
			t.Error("JSON decode of non-numeric-key wire INTO map[json.Number]int32 ACCEPTED (must reject)")
		}
	})
}

// jnStringSample returns a value the given stringy schema accepts, used to
// build a valid wire that is then (mis-)decoded into a json.Number target.
func jnStringSample(label string) any {
	switch label {
	case "bytes", "fixed":
		return []byte{0x41, 0x42}
	case "enum":
		return "A"
	default: // string
		return "ab"
	}
}

// ---------------------------------------------------------------------------
// Class-elimination differential net: a logical-on-numeric type must treat a
// json.Number encode SOURCE identically to its underlying numeric type.
//
// json.Number is a numeric carrier (NOT_BUGS #35): its content must be a valid
// RFC 8259 number, so a logical layered on a numeric base — date on int;
// time-*/timestamp-*/local-timestamp-* on int/long — must never be MORE LENIENT
// about non-numeric json.Number content than the plain int/long it wraps. The
// ORACLE is calibration-free: the underlying numeric schema's own accept/reject
// verdict for the same json.Number. No hardcoded "what is a number" list, so it
// cannot rot as the numeric parser's grammar evolves.
//
// The discriminating input is content that is a valid TEMPORAL STRING but an
// INVALID number ("2024-01-01", "2024-01-01T00:00:00Z"): the date/timestamp
// encode string-convenience arms (tryParseDateString / tryParseTimeString) once
// fired for json.Number (whose Kind() is reflect.String), encoding it as a
// date/timestamp where the numeric twin rejects it. A generic non-numeric
// battery ("xyz", "1.2.3") never reaches that arm — those fail time.Parse too,
// so they reject with or without the leniency; only a temporal-shaped string
// separates the buggy path from the correct one. This net is the differential
// complement to TestMatrix_JSONNumberPolicy (which asserts the numeric base's
// ABSOLUTE reject of non-numeric content); together they pin both "the base
// rejects" and "the logical matches the base," across every encode context.
func TestMatrix_JSONNumberLogicalMatchesNumericTwin(t *testing.T) {
	logicals := []struct {
		label, schema, twin string
	}{
		{"date", `{"type":"int","logicalType":"date"}`, `"int"`},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, `"int"`},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`, `"long"`},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, `"long"`},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, `"long"`},
		{"timestamp-nanos", `{"type":"long","logicalType":"timestamp-nanos"}`, `"long"`},
		{"local-timestamp-millis", `{"type":"long","logicalType":"local-timestamp-millis"}`, `"long"`},
		{"local-timestamp-micros", `{"type":"long","logicalType":"local-timestamp-micros"}`, `"long"`},
		{"local-timestamp-nanos", `{"type":"long","logicalType":"local-timestamp-nanos"}`, `"long"`},
	}
	// valid-number (both accept), two temporal-shaped strings that are invalid
	// numbers (the discriminators — both must reject after the fix), and garbage
	// (both reject via the numeric parser regardless).
	contents := []string{"19723", "2024-01-01", "2024-01-01T00:00:00Z", "xyz"}

	// verdicts returns whether (binary, JSON) encode of val against schemaJSON
	// succeeds.
	verdicts := func(schemaJSON string, val any) (binOK, jsonOK bool) {
		s := avro.MustParse(schemaJSON)
		_, be := s.AppendEncode(nil, val)
		_, je := s.AppendEncodeJSON(nil, val)
		return be == nil, je == nil
	}

	// Reuse jnPositions for the ENCODE-CONTEXT axis (top / record field / array
	// element / map value / addressable struct field / nullable-union branch) —
	// a json.Number at a struct field or container element can reach a different
	// encode path than a top-level value.
	for _, pos := range jnPositions {
		for _, lg := range logicals {
			for _, content := range contents {
				t.Run(pos.label+"/"+lg.label+"/"+content, func(t *testing.T) {
					val := pos.encodeVal(json.Number(content))
					logBin, logJSON := verdicts(pos.schema(lg.schema), val)
					twBin, twJSON := verdicts(pos.schema(lg.twin), val)
					if logBin != twBin {
						t.Errorf("binary encode verdict divergence: %s(json.Number(%q))=%v but numeric twin %s=%v — a logical must match its numeric base for a json.Number source",
							lg.label, content, logBin, lg.twin, twBin)
					}
					if logJSON != twJSON {
						t.Errorf("JSON encode verdict divergence: %s(json.Number(%q))=%v but numeric twin %s=%v",
							lg.label, content, logJSON, lg.twin, twJSON)
					}
				})
			}
		}
	}
}

// ---------- matrix_logical_bounds_test.go ----------

// ---------------------------------------------------------------------------
// Logical-type boundary axis: every time/decimal logical at the edges of its
// representable range. Two relational invariants, no policy assumptions:
//
//   - known-good extremes round-trip EXACTLY (typed in, typed out, both
//     wires, byte-stable re-encode);
//   - for raw boundary WIRES (MaxInt64/MinInt64 and ±1 around each unit
//     conversion), decode either errors or yields a value that re-encodes
//     onto the identical wire — silent value corruption is the only
//     forbidden outcome.
// ---------------------------------------------------------------------------

func TestMatrix_LogicalTimeExtremes(t *testing.T) {
	cases := []struct {
		label  string
		schema string
		values []any // typed extremes that must round-trip exactly
	}{
		{"date", `{"type":"int","logicalType":"date"}`, []any{
			time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC),
		}},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, []any{
			time.Duration(0),
			23*time.Hour + 59*time.Minute + 59*time.Second + 999*time.Millisecond,
		}},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`, []any{
			time.Duration(0),
			23*time.Hour + 59*time.Minute + 59*time.Second + 999999*time.Microsecond,
		}},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, []any{
			time.UnixMilli(0).UTC(),
			time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(9999, 12, 31, 23, 59, 59, 999000000, time.UTC),
		}},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, []any{
			time.UnixMicro(0).UTC(),
			time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC),
		}},
		// timestamp-nanos: int64 nanoseconds bound the instant range to
		// ~[1677, 2262]; both edges exactly.
		{"timestamp-nanos", `{"type":"long","logicalType":"timestamp-nanos"}`, []any{
			time.Unix(0, math.MaxInt64).UTC(),
			time.Unix(0, math.MinInt64).UTC(),
			time.Unix(0, 0).UTC(),
		}},
		{"local-timestamp-millis", `{"type":"long","logicalType":"local-timestamp-millis"}`, []any{
			time.Date(9999, 12, 31, 23, 59, 59, 999000000, time.UTC),
		}},
	}
	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			for _, v := range c.values {
				runCore(t, c.schema, v)
			}
		})
	}
}

// Raw boundary wires through every long/int-backed logical: decode-then-
// re-encode must be the identity wherever decode succeeds.
func TestMatrix_LogicalBoundaryWires(t *testing.T) {
	longWires := [][]byte{
		appendZig(nil, math.MaxInt64),
		appendZig(nil, math.MinInt64),
		appendZig(nil, math.MaxInt64-1),
		appendZig(nil, math.MinInt64+1),
		appendZig(nil, 0),
	}
	intWires := [][]byte{
		appendZig(nil, math.MaxInt32),
		appendZig(nil, math.MinInt32),
		appendZig(nil, 0),
	}
	schemas := []struct {
		label  string
		schema string
		wires  [][]byte
	}{
		{"date", `{"type":"int","logicalType":"date"}`, intWires},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, intWires},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`, longWires},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, longWires},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, longWires},
		{"timestamp-nanos", `{"type":"long","logicalType":"timestamp-nanos"}`, longWires},
		{"local-timestamp-millis", `{"type":"long","logicalType":"local-timestamp-millis"}`, longWires},
		{"local-timestamp-micros", `{"type":"long","logicalType":"local-timestamp-micros"}`, longWires},
		{"local-timestamp-nanos", `{"type":"long","logicalType":"local-timestamp-nanos"}`, longWires},
	}
	for _, sc := range schemas {
		t.Run(sc.label, func(t *testing.T) {
			s := avro.MustParse(sc.schema)
			for _, w := range sc.wires {
				var a any
				rest, err := s.Decode(w, &a)
				if err != nil {
					continue // a bounded reject is a legal outcome
				}
				if len(rest) != 0 {
					t.Fatalf("wire %x: %d leftover bytes", w, len(rest))
				}
				re, err := s.AppendEncode(nil, a)
				if err != nil {
					t.Fatalf("wire %x decoded to %#v which cannot re-encode: %v", w, a, err)
				}
				if !bytes.Equal(re, w) {
					t.Fatalf("silent boundary corruption:\n wire=%x\n re  =%x\n via %#v", w, re, a)
				}
			}
		})
	}
}

// appendZig writes a zigzag varint (test-local; mirrors the wire format).
func appendZig(dst []byte, n int64) []byte {
	u := uint64(n)<<1 ^ uint64(n>>63)
	for u >= 0x80 {
		dst = append(dst, byte(u)|0x80)
		u >>= 7
	}
	return append(dst, byte(u))
}

// Duration at the uint32 edges, and decimal at the precision boundary.
func TestMatrix_DurationAndDecimalEdges(t *testing.T) {
	t.Run("duration-uint32-max", func(t *testing.T) {
		schema := `{"type":"fixed","name":"DBE","size":12,"logicalType":"duration"}`
		for _, v := range []any{
			avro.Duration{Months: math.MaxUint32, Days: math.MaxUint32, Milliseconds: math.MaxUint32},
			avro.Duration{},
			avro.Duration{Months: 1},
		} {
			runCore(t, schema, v)
		}
	})
	t.Run("decimal-precision-boundary", func(t *testing.T) {
		s := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`)
		// 99.99 = 4 digits at scale 2: the maximum magnitude.
		for _, ok := range []*big.Rat{
			big.NewRat(9999, 100), big.NewRat(-9999, 100), big.NewRat(0, 1),
		} {
			if _, err := s.AppendEncode(nil, ok); err != nil {
				t.Errorf("at-precision value %v rejected: %v", ok, err)
			}
		}
		// 100.00 needs 5 digits: one past the boundary rejects.
		for _, bad := range []*big.Rat{
			big.NewRat(10000, 100), big.NewRat(-10000, 100),
		} {
			if _, err := s.AppendEncode(nil, bad); err == nil {
				t.Errorf("over-precision value %v accepted", bad)
			}
		}
		// And the at-boundary value round-trips both wires exactly.
		runCore(t, `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`, big.NewRat(9999, 100))
	})
	t.Run("decimal-fixed-capacity-boundary", func(t *testing.T) {
		// fixed(2) holds 15 bits of two's complement: ±~3.27e4 unscaled.
		s := avro.MustParse(`{"type":"fixed","name":"DCF","size":2,"logicalType":"decimal","precision":4,"scale":0}`)
		for _, ok := range []*big.Rat{big.NewRat(9999, 1), big.NewRat(-9999, 1)} {
			if _, err := s.AppendEncode(nil, ok); err != nil {
				t.Errorf("fits-in-fixed value %v rejected: %v", ok, err)
			}
		}
	})
	t.Run("decimal-over-precision-wire-decodes", func(t *testing.T) {
		// VALUE precision — the unscaled magnitude's digit count against the
		// declared "precision" — is an ENCODE-side check only, matching Java's
		// Conversions.DecimalConversion: toBytes/toFixed run validate(), while
		// fromBytes/fromFixed build the BigDecimal unchecked. So a wire whose
		// unscaled value EXCEEDS the declared precision but still fits the byte
		// container must DECODE to a *big.Rat on both wire formats, and
		// re-encoding that same value must reject with the precision error.
		// Both directions are pinned: adding a decode-side precision reject
		// would refuse valid foreign wire Java accepts; relaxing the encode
		// check would diverge from Java's validate().
		overUnscaled := []byte{0x41, 0x41, 0x41, 0x41} // 1094795585: 10 digits > precision 9
		overRat := big.NewRat(1094795585, 100)         // at scale 2
		for _, tc := range []struct {
			name    string
			schema  string
			binWire []byte // binary wire carrying overUnscaled
		}{
			{"fixed", `{"type":"fixed","name":"DOP","size":4,"logicalType":"decimal","precision":9,"scale":2}`,
				overUnscaled},
			{"bytes", `{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}`,
				append([]byte{0x08}, overUnscaled...)}, // zigzag(len 4) prefix
		} {
			t.Run(tc.name, func(t *testing.T) {
				s := avro.MustParse(tc.schema)
				var fromBin big.Rat
				if _, err := s.Decode(tc.binWire, &fromBin); err != nil {
					t.Fatalf("binary decode of over-precision wire: %v", err)
				}
				if fromBin.Cmp(overRat) != 0 {
					t.Fatalf("binary decode: got %v, want %v", &fromBin, overRat)
				}
				// JSON wire: the spec codepoint-per-byte string of the unscaled
				// bytes (0x41 = "A").
				var fromJSON big.Rat
				if err := s.DecodeJSON([]byte(`"AAAA"`), &fromJSON); err != nil {
					t.Fatalf("JSON decode of over-precision wire: %v", err)
				}
				if fromJSON.Cmp(overRat) != 0 {
					t.Fatalf("JSON decode: got %v, want %v", &fromJSON, overRat)
				}
				// Re-encoding the decoded value rejects on BOTH wire formats,
				// specifically via the precision check.
				if _, err := s.AppendEncode(nil, &fromBin); err == nil || !strings.Contains(err.Error(), "exceeds schema precision") {
					t.Fatalf("binary re-encode of over-precision value: want precision reject, got %v", err)
				}
				if _, err := s.AppendEncodeJSON(nil, &fromBin); err == nil || !strings.Contains(err.Error(), "exceeds schema precision") {
					t.Fatalf("JSON re-encode of over-precision value: want precision reject, got %v", err)
				}
				// One digit narrower (9 digits = the declared precision) both
				// decodes and re-encodes: the boundary sits between the two
				// values, so the asymmetry above is precision-driven, not
				// container-driven.
				within := big.NewRat(123456789, 100)
				wbin, err := s.AppendEncode(nil, within)
				if err != nil {
					t.Fatalf("at-precision encode: %v", err)
				}
				var back big.Rat
				if _, err := s.Decode(wbin, &back); err != nil || back.Cmp(within) != 0 {
					t.Fatalf("at-precision round-trip: err=%v got=%v want=%v", err, &back, within)
				}
			})
		}
	})
}

// The decimalScaleLimit magnitude gate in boundedRatFromString must sit at
// EXACTLY ±65536 for string-form decimals whose exponent interacts with a
// fractional part: for "1.5e<E>" the net magnitude is E-1 (one fractional
// digit), so E=65537 is the last value the gate passes and E=65538 the
// first it rejects (mirrored on the negative side). The two sides are
// distinguished by WHICH error fires — the gate's "magnitude exceeds"
// versus the schema's downstream precision/scale rejection — so a shifted
// boundary (mis-derived fractional length) flips an assertion even though
// every input here errors. Pins the gate position itself, which no
// round-trip or oracle axis can see (the cap is twmb defense-in-depth).
func TestMatrix_DecimalStringMagnitudeBoundary(t *testing.T) {
	s := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}`)
	cases := []struct {
		in           string
		magnitudeErr bool // true: the ±65536 gate fires; false: it must NOT
	}{
		{"1.5e65538", true},   // netExp 65537: one past the limit
		{"1.5e65537", false},  // netExp 65536: at the limit — gate passes
		{"1.5e-65536", true},  // netExp -65537: one past, negative side
		{"1.5e-65535", false}, // netExp -65536: at the limit, negative side
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			// Encode-side caller (string/json.Number → decimal coercion).
			_, err := s.AppendEncode(nil, json.Number(c.in))
			if err == nil {
				t.Fatalf("encode %s: expected an error (precision 6 cannot hold it)", c.in)
			}
			if got := strings.Contains(err.Error(), "magnitude exceeds"); got != c.magnitudeErr {
				t.Fatalf("encode %s: magnitude-gate fired=%v want %v (err: %v)", c.in, got, c.magnitudeErr, err)
			}
			// JSON-decode caller (bare-number decimal form). Decode has no
			// precision check on this leniency path, so the discriminator
			// is sharper: at-limit values SUCCEED outright, past-limit
			// values fail with the gate's error.
			var sink any
			derr := s.DecodeJSON([]byte(c.in), &sink)
			if c.magnitudeErr {
				if derr == nil || !strings.Contains(derr.Error(), "magnitude exceeds") {
					t.Fatalf("decodeJSON %s: want magnitude-gate error, got %v", c.in, derr)
				}
			} else if derr != nil {
				t.Fatalf("decodeJSON %s: at-limit value must decode, got %v", c.in, derr)
			}
		})
	}
}

// ---------- matrix_names_test.go ----------

// ---------------------------------------------------------------------------
// Names matrix: namespaces (explicit, inherited, dotted-fullname,
// same-shortname-across-namespaces), forward references at every fixup
// position, and the documented field-level logicalType lift shapes.
// ---------------------------------------------------------------------------

func TestMatrix_Namespaces(t *testing.T) {
	cases := []struct {
		label  string
		schema string
		value  any
	}{
		{"explicit-ns",
			`{"type":"record","name":"R","namespace":"com.example","fields":[
				{"name":"e","type":{"type":"enum","name":"E","namespace":"com.example.sub","symbols":["A"]}}]}`,
			map[string]any{"e": "A"}},
		{"inherited-ns-shortref",
			`{"type":"record","name":"R","namespace":"n1","fields":[
				{"name":"a","type":{"type":"fixed","name":"F","size":1}},
				{"name":"b","type":"F"}]}`,
			map[string]any{"a": []byte{1}, "b": []byte{2}}},
		{"inherited-ns-fullref",
			`{"type":"record","name":"R","namespace":"n1","fields":[
				{"name":"a","type":{"type":"fixed","name":"F","size":1}},
				{"name":"b","type":"n1.F"}]}`,
			map[string]any{"a": []byte{1}, "b": []byte{2}}},
		{"dotted-fullname",
			`{"type":"record","name":"com.example.R","fields":[
				{"name":"x","type":"int"}]}`,
			map[string]any{"x": int32(5)}},
		{"same-shortname-two-ns",
			`{"type":"record","name":"R","namespace":"o","fields":[
				{"name":"a","type":{"type":"enum","name":"T","namespace":"n1","symbols":["X"]}},
				{"name":"b","type":{"type":"enum","name":"T","namespace":"n2","symbols":["Y","Z"]}},
				{"name":"c","type":"n1.T"},
				{"name":"d","type":"n2.T"}]}`,
			map[string]any{"a": "X", "b": "Z", "c": "X", "d": "Y"}},
		{"namespaced-recursive",
			`{"type":"record","name":"Node","namespace":"tree","fields":[
				{"name":"v","type":"int"},
				{"name":"next","type":["null","tree.Node"],"default":null}]}`,
			map[string]any{"v": int32(1), "next": map[string]any{"v": int32(2), "next": nil}}},
		{"namespaced-recursive-shortref",
			`{"type":"record","name":"Node","namespace":"tree","fields":[
				{"name":"v","type":"int"},
				{"name":"next","type":["null","Node"],"default":null}]}`,
			map[string]any{"v": int32(1), "next": map[string]any{"v": int32(2), "next": nil}}},
	}
	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			runCore(t, c.schema, c.value)
		})
		t.Run(c.label+"/tagged", func(t *testing.T) {
			runCore(t, c.schema, c.value, avro.TaggedUnions())
		})
	}
}

// Forward references at every finalize-fixup position: the named type is
// used by an EARLIER field than the one defining it.
func TestMatrix_ForwardRefPositions(t *testing.T) {
	def := `{"type":"record","name":"Inner","fields":[{"name":"i","type":"int"}]}`
	inner := map[string]any{"i": int32(7)}
	cases := []struct {
		label  string
		early  string // the forward-referencing field type
		value  any    // value for the early field
		tagged bool
	}{
		{"union-branch", `["null","Inner"]`, inner, true},
		{"direct-field", `"Inner"`, inner, false},
		{"array-items", `{"type":"array","items":"Inner"}`, []any{inner, inner}, false},
		{"map-values", `{"type":"map","values":"Inner"}`, map[string]any{"k": inner}, false},
		{"union-of-array-of-ref", `["null",{"type":"array","items":"Inner"}]`, []any{inner}, true},
	}
	for _, c := range cases {
		schema := fmt.Sprintf(`{"type":"record","name":"W","fields":[
			{"name":"early","type":%s},
			{"name":"def","type":%s}]}`, c.early, def)
		value := map[string]any{"early": c.value, "def": inner}
		t.Run(c.label, func(t *testing.T) {
			runCore(t, schema, value)
		})
		if c.tagged {
			t.Run(c.label+"/tagged", func(t *testing.T) {
				runCore(t, schema, value, avro.TaggedUnions())
			})
		}
	}
}

// Namespaced forward reference: the early short-name reference resolves
// in-scope to the later definition (documented eager in-scope-first rule).
func TestMatrix_ForwardRefNamespaced(t *testing.T) {
	schema := `{"type":"record","name":"W","namespace":"ns","fields":[
		{"name":"early","type":["null","Inner"],"default":null},
		{"name":"def","type":{"type":"record","name":"Inner","fields":[{"name":"i","type":"int"}]}}]}`
	value := map[string]any{
		"early": map[string]any{"i": int32(1)},
		"def":   map[string]any{"i": int32(2)},
	}
	runCore(t, schema, value)
	runCore(t, schema, value, avro.TaggedUnions())
}

// The three documented field-level logicalType lift shapes must produce
// wire bytes identical to the canonical nested form, accept the enriched
// Go type, and survive the metadata rebuild.
func TestMatrix_FieldLevelLogicalLift(t *testing.T) {
	nested := `{"type":"record","name":"R","fields":[
		{"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}],"default":null}]}`
	canonical := avro.MustParse(nested)
	v := map[string]any{"ts": time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)}
	wantWire, err := canonical.AppendEncode(nil, v)
	if err != nil {
		t.Fatalf("canonical encode: %v", err)
	}

	shapes := []struct {
		label  string
		schema string
	}{
		{"string-form-union", `{"type":"record","name":"R","fields":[
			{"name":"ts","type":["null","long"],"logicalType":"timestamp-millis","default":null}]}`},
		{"primitive-form", `{"type":"record","name":"R","fields":[
			{"name":"ts","type":"long","logicalType":"timestamp-millis"}]}`},
		{"single-object-form", `{"type":"record","name":"R","fields":[
			{"name":"ts","type":{"type":"long"},"logicalType":"timestamp-millis"}]}`},
	}
	for _, sh := range shapes {
		t.Run(sh.label, func(t *testing.T) {
			s, err := avro.Parse(sh.schema)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			got, err := s.AppendEncode(nil, v)
			if err != nil {
				t.Fatalf("lifted encode of time.Time: %v", err)
			}
			if sh.label != "string-form-union" {
				// Non-union shapes have no union index byte; compare against
				// their own canonical nested form instead.
				nestedFlat := `{"type":"record","name":"R","fields":[
					{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}`
				want, _ := avro.MustParse(nestedFlat).AppendEncode(nil, v)
				if !bytes.Equal(got, want) {
					t.Fatalf("lifted wire differs from nested form:\n got=%x\nwant=%x", got, want)
				}
			} else if !bytes.Equal(got, wantWire) {
				t.Fatalf("lifted wire differs from nested form:\n got=%x\nwant=%x", got, wantWire)
			}
			// Decode parity: the enriched type comes back on both wires.
			var back map[string]any
			if _, err := s.Decode(got, &back); err != nil {
				t.Fatalf("lifted decode: %v", err)
			}
			if _, ok := back["ts"].(time.Time); !ok {
				t.Fatalf("lifted decode yielded %T, want time.Time", back["ts"])
			}
			j, err := s.AppendEncodeJSON(nil, v)
			if err != nil {
				t.Fatalf("lifted encodeJSON: %v", err)
			}
			var jback map[string]any
			if err := s.DecodeJSON(j, &jback); err != nil {
				t.Fatalf("lifted decodeJSON: %v", err)
			}
			if !matEqual(jback["ts"], back["ts"]) {
				t.Fatalf("lifted JSON decode diverges: %#v vs %#v", jback["ts"], back["ts"])
			}
			// Metadata rebuild: the field-level annotation survives in
			// Props, re-parses, re-lifts, and encodes identically.
			root := s.Root()
			rebuilt, err := root.Schema()
			if err != nil {
				t.Fatalf("Root().Schema(): %v", err)
			}
			got2, err := rebuilt.AppendEncode(nil, v)
			if err != nil || !bytes.Equal(got2, got) {
				t.Fatalf("rebuilt lifted schema wire differs: err=%v\n got=%x\nreb=%x\nrebuilt: %s", err, got, got2, rebuilt.String())
			}
		})
	}
}

// ---------- matrix_phase3_test.go ----------

// ---------------------------------------------------------------------------
// Phase 3: defaults pipeline per kind × positions, same-token-class tagged
// unions, SchemaCache cross-parse references, and option axes.
// ---------------------------------------------------------------------------

// Per-kind defaulted fields: parse → JSON fill → binary auto-fill, and the
// two fills must land on the same wire as explicitly encoding the filled
// value. Default literals use the field type's JSON encoding per the spec
// (underlying form for logical types).
func TestMatrix_DefaultsPerKind(t *testing.T) {
	cases := []struct {
		label      string
		fieldType  string
		defaultLit string
	}{
		{"null", `"null"`, `null`},
		{"boolean", `"boolean"`, `true`},
		{"int", `"int"`, `7`},
		{"int-neg", `"int"`, `-2147483648`},
		{"long", `"long"`, `9007199254740993`},
		{"float", `"float"`, `1.5`},
		{"double", `"double"`, `-2.25`},
		{"string", `"string"`, `"dflt"`},
		{"string-empty", `"string"`, `""`},
		{"bytes", `"bytes"`, `"\u0001\u00ff"`},
		{"bytes-empty", `"bytes"`, `""`},
		{"enum", `{"type":"enum","name":"DE","symbols":["A","B"]}`, `"B"`},
		{"fixed1", `{"type":"fixed","name":"DF1","size":1}`, `"\u00ab"`},
		{"fixed0", `{"type":"fixed","name":"DF0","size":0}`, `""`},
		{"date", `{"type":"int","logicalType":"date"}`, `19723`},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, `3600000`},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, `1717243496789`},
		{"decimal", `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`, `"\u00d2"`},
		{"nullunion", `["null","int"]`, `null`},
		{"union-int-first", `["int","string"]`, `42`},
		{"array", `{"type":"array","items":"int"}`, `[1,2]`},
		{"array-empty", `{"type":"array","items":"int"}`, `[]`},
		{"map", `{"type":"map","values":"string"}`, `{"k":"v"}`},
		{"map-empty", `{"type":"map","values":"string"}`, `{}`},
		{"record", `{"type":"record","name":"DR","fields":[{"name":"i","type":"int"}]}`, `{"i":3}`},
		{"empty-record", `{"type":"record","name":"DER","fields":[]}`, `{}`},
	}
	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			schema := fmt.Sprintf(`{"type":"record","name":"W","fields":[
				{"name":"pre","type":"string"},
				{"name":"f","type":%s,"default":%s}]}`, c.fieldType, c.defaultLit)
			s, err := avro.Parse(schema)
			if err != nil {
				t.Fatalf("Parse: %v\nschema: %s", err, schema)
			}
			// JSON fill: absent field materializes the default.
			var filled map[string]any
			if err := s.DecodeJSON([]byte(`{"pre":"p"}`), &filled); err != nil {
				t.Fatalf("DecodeJSON fill: %v", err)
			}
			// Binary auto-fill on encode of a map missing the field.
			wFill, err := s.AppendEncode(nil, map[string]any{"pre": "p"})
			if err != nil {
				t.Fatalf("binary auto-fill encode: %v", err)
			}
			// Explicitly encoding the JSON-filled value must give the
			// same wire bytes.
			wExpl, err := s.AppendEncode(nil, filled)
			if err != nil {
				t.Fatalf("encode filled value %#v: %v", filled, err)
			}
			if !bytes.Equal(wFill, wExpl) {
				t.Fatalf("auto-fill wire differs from filled-value wire:\n fill=%x\n expl=%x\nfilled: %#v", wFill, wExpl, filled)
			}
			// Decode the auto-filled wire and re-encode: stable.
			var back any
			if _, err := s.Decode(wFill, &back); err != nil {
				t.Fatalf("decode auto-filled wire: %v", err)
			}
			w2, err := s.AppendEncode(nil, back)
			if err != nil || !bytes.Equal(w2, wFill) {
				t.Fatalf("re-encode of auto-filled wire differs: err=%v\n w=%x\n w2=%x", err, wFill, w2)
			}
			// The metadata Default round-trips through Root().Schema().
			root := s.Root()
			rebuilt, err := root.Schema()
			if err != nil {
				t.Fatalf("Root().Schema(): %v", err)
			}
			wReb, err := rebuilt.AppendEncode(nil, map[string]any{"pre": "p"})
			if err != nil || !bytes.Equal(wReb, wFill) {
				t.Fatalf("rebuilt-schema auto-fill differs: err=%v\n w=%x\n reb=%x\nrebuilt: %s", err, wFill, wReb, rebuilt.String())
			}
		})
	}
}

// Same-token-class union pairs are information-preserving only in TAGGED
// form (documented untagged first-match loss): the full core must hold
// with TaggedUnions on both wires.
func TestMatrix_TaggedSameClassUnions(t *testing.T) {
	cases := []struct {
		label  string
		schema string
		values []any
	}{
		{"int-long", `["int","long"]`,
			[]any{map[string]any{"int": int32(7)}, map[string]any{"long": int64(7)}}},
		{"float-double", `["float","double"]`,
			[]any{map[string]any{"float": float32(1.5)}, map[string]any{"double": float64(1.5)}}},
		{"string-bytes", `["string","bytes"]`,
			[]any{map[string]any{"string": "x"}, map[string]any{"bytes": []byte("x")}}},
		{"two-records", `[{"type":"record","name":"R1","fields":[{"name":"a","type":"int"}]},{"type":"record","name":"R2","fields":[{"name":"a","type":"int"}]}]`,
			[]any{map[string]any{"R1": map[string]any{"a": int32(1)}}, map[string]any{"R2": map[string]any{"a": int32(2)}}}},
		{"enum-string", `[{"type":"enum","name":"TE","symbols":["A"]},"string"]`,
			[]any{map[string]any{"TE": "A"}, map[string]any{"string": "A"}}},
		{"fixed-bytes", `[{"type":"fixed","name":"TF","size":2},"bytes"]`,
			[]any{map[string]any{"TF": []byte{1, 2}}, map[string]any{"bytes": []byte{1, 2}}}},
		{"map-record", `[{"type":"map","values":"int"},{"type":"record","name":"MR","fields":[{"name":"a","type":"int"}]}]`,
			[]any{map[string]any{"map": map[string]any{"k": int32(1)}}, map[string]any{"MR": map[string]any{"a": int32(1)}}}},
	}
	for _, c := range cases {
		for vi, v := range c.values {
			t.Run(fmt.Sprintf("%s/v%d", c.label, vi), func(t *testing.T) {
				runCore(t, c.schema, v, avro.TaggedUnions())
			})
		}
	}
}

// SchemaCache: a named type defined by one Parse and referenced by name from
// a second Parse must behave identically to the inline definition, across
// both wires, rebuild, and resolve.
func TestMatrix_SchemaCacheCrossRef(t *testing.T) {
	defs := []struct {
		label  string
		def    string
		ref    string
		inline string
		value  any
	}{
		{"record",
			`{"type":"record","name":"CR","fields":[{"name":"a","type":"int"},{"name":"b","type":["null","string"],"default":null}]}`,
			`{"type":"array","items":"CR"}`,
			`{"type":"array","items":{"type":"record","name":"CR","fields":[{"name":"a","type":"int"},{"name":"b","type":["null","string"],"default":null}]}}`,
			[]any{map[string]any{"a": int32(1), "b": "x"}, map[string]any{"a": int32(2), "b": nil}}},
		{"enum",
			`{"type":"enum","name":"CE","symbols":["X","Y"]}`,
			`{"type":"map","values":"CE"}`,
			`{"type":"map","values":{"type":"enum","name":"CE","symbols":["X","Y"]}}`,
			map[string]any{"k": "Y"}},
		{"fixed0",
			`{"type":"fixed","name":"CF0","size":0}`,
			`["null","CF0"]`,
			`["null",{"type":"fixed","name":"CF0","size":0}]`,
			[]byte{}},
		{"recursive",
			`{"type":"record","name":"CN","fields":[{"name":"v","type":"int"},{"name":"next","type":["null","CN"],"default":null}]}`,
			`{"type":"array","items":"CN"}`,
			`{"type":"array","items":{"type":"record","name":"CN","fields":[{"name":"v","type":"int"},{"name":"next","type":["null","CN"],"default":null}]}}`,
			[]any{map[string]any{"v": int32(1), "next": map[string]any{"v": int32(2), "next": nil}}}},
	}
	for _, d := range defs {
		t.Run(d.label, func(t *testing.T) {
			var cache avro.SchemaCache
			if _, err := cache.Parse(d.def); err != nil {
				t.Fatalf("cache.Parse(def): %v", err)
			}
			viaCache, err := cache.Parse(d.ref)
			if err != nil {
				t.Fatalf("cache.Parse(ref): %v", err)
			}
			inline := avro.MustParse(d.inline)

			wC, err := viaCache.AppendEncode(nil, d.value)
			if err != nil {
				t.Fatalf("cache-ref encode: %v", err)
			}
			wI, err := inline.AppendEncode(nil, d.value)
			if err != nil {
				t.Fatalf("inline encode: %v", err)
			}
			if !bytes.Equal(wC, wI) {
				t.Fatalf("cache-ref wire differs from inline:\n c=%x\n i=%x", wC, wI)
			}
			var aC, aI any
			if _, err := viaCache.Decode(wC, &aC); err != nil {
				t.Fatalf("cache-ref decode: %v", err)
			}
			if _, err := inline.Decode(wI, &aI); err != nil {
				t.Fatalf("inline decode: %v", err)
			}
			if !matEqual(aC, aI) {
				t.Fatalf("decoded values differ:\n c=%#v\n i=%#v", aC, aI)
			}
			// JSON parity.
			jC, err := viaCache.AppendEncodeJSON(nil, aC)
			if err != nil {
				t.Fatalf("cache-ref encodeJSON: %v", err)
			}
			jI, err := inline.AppendEncodeJSON(nil, aI)
			if err != nil || !bytes.Equal(jC, jI) {
				t.Fatalf("JSON differs: err=%v\n c=%s\n i=%s", err, jC, jI)
			}
			// Resolve cache-ref ↔ inline (identical structure).
			if _, err := avro.Resolve(viaCache, inline); err != nil {
				t.Fatalf("Resolve(cache→inline): %v", err)
			}
			if _, err := avro.Resolve(inline, viaCache); err != nil {
				t.Fatalf("Resolve(inline→cache): %v", err)
			}
			// Rebuild parity: the cache-referenced schema's metadata forms must
			// be self-contained and identical to the inline schema's — the
			// canonical form (hence the Rabin fingerprint, the cross-language /
			// single-object-encoding identity) byte-for-byte, the canonical must
			// re-parse, and Root().Schema() must rebuild. The cache stores only
			// the resolved node, so without inlining the inherited definition
			// these forms keep a dangling bare reference.
			if !bytes.Equal(viaCache.Canonical(), inline.Canonical()) {
				t.Fatalf("canonical differs:\n c=%s\n i=%s", viaCache.Canonical(), inline.Canonical())
			}
			if !bytes.Equal(viaCache.Fingerprint(avro.NewRabin()), inline.Fingerprint(avro.NewRabin())) {
				t.Fatalf("fingerprint differs (cache-ref not self-contained)")
			}
			if _, err := avro.Parse(string(viaCache.Canonical())); err != nil {
				t.Fatalf("cache-ref canonical not self-contained: %v", err)
			}
			root := viaCache.Root()
			if _, err := root.Schema(); err != nil {
				t.Fatalf("Root().Schema() rebuild failed: %v", err)
			}
		})
	}
}

// Option axes over representative fragments: LinkedinFloats float forms and
// TagLogicalTypes envelopes must round-trip within their own convention.
func TestMatrix_OptionAxes(t *testing.T) {
	t.Run("linkedin-floats", func(t *testing.T) {
		s := avro.MustParse(`{"type":"array","items":"double"}`)
		vin := []any{1.5, -2.25}
		runCore(t, `{"type":"array","items":"double"}`, vin, avro.LinkedinFloats())
		// Non-finite specials under the goavro convention: NaN→null is
		// not value-preserving by design, so only the finite path runs
		// through runCore; the specials get a one-way encode check.
		j, err := s.AppendEncodeJSON(nil, []any{math.Inf(1), math.Inf(-1)}, avro.LinkedinFloats())
		_ = j
		if err != nil {
			t.Fatalf("encode specials: %v", err)
		}
	})
	t.Run("tag-logical-types", func(t *testing.T) {
		schema := `["null",{"type":"long","logicalType":"timestamp-millis"}]`
		v := time.UnixMilli(1717243496789).UTC()
		runCore(t, schema, v, avro.TaggedUnions(), avro.TagLogicalTypes())
	})
	t.Run("tag-logical-named-fixed", func(t *testing.T) {
		schema := `["null",{"type":"fixed","name":"NU","size":16,"logicalType":"uuid"}]`
		v := "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
		runCore(t, schema, v, avro.TaggedUnions(), avro.TagLogicalTypes())
	})
}

// Deep × wide stress at the boundary of interesting structure: a 5-level
// alternating record/array/map/union tower over every leaf kind.
func TestMatrix_FiveLevelTower(t *testing.T) {
	leaves := []struct {
		label  string
		schema string
		value  any
	}{
		{"int", `"int"`, int32(9)},
		{"string", `"string"`, "s"},
		{"bytes", `"bytes"`, []byte{1}},
		{"decimal", `{"type":"bytes","logicalType":"decimal","precision":4,"scale":1}`, big.NewRat(15, 10)},
		{"fixed0", `{"type":"fixed","name":"TF0","size":0}`, []byte{}},
		{"enum", `{"type":"enum","name":"TE5","symbols":["A","B"]}`, "B"},
	}
	for _, leaf := range leaves {
		t.Run(leaf.label, func(t *testing.T) {
			schema := fmt.Sprintf(`{"type":"record","name":"L1","fields":[{"name":"a","type":
				{"type":"array","items":
					{"type":"map","values":
						["null",{"type":"record","name":"L4","fields":[
							{"name":"leaf","type":%s},
							{"name":"sib","type":"long"}]}]}}}]}`, leaf.schema)
			value := map[string]any{"a": []any{
				map[string]any{"k": map[string]any{"leaf": leaf.value, "sib": int64(5)}},
				map[string]any{"e": nil},
				map[string]any{},
			}}
			runCore(t, schema, value)
		})
	}
}

// ---------- matrix_promoteprec_test.go ----------

// ---------------------------------------------------------------------------
// Generative int→float promotion-precision net.
//
// Documented rule (BUG_AUDIT "Precision: the READER schema is the contract"):
// when the writer is int/long and the reader is float/double, the value is
// converted through the reader's float width — int/long → FLOAT rounds at the
// float32 mantissa (24 bits), long → DOUBLE at the float64 mantissa (53 bits)
// — and that rounding is PRESERVED when decoding into an any/float64 target.
// promoteIntFloatMantissa does `float64(float32(n))` for a 32-bit-wire reader.
//
// The existing TestMatrix_PromotionPairsByContext misses a bug in that
// rounding two ways: its values are small (exactly float-representable, so
// float64(float32(n)) == float64(n)), and it re-encodes the promoted value
// against the reader's FLOAT wire (which rounds both sides identically,
// hiding a wrong intermediate). A per-site neuter confirmed
// `float64(float32(n))` → `float64(n)` is caught only by the hand-written
// TestResolutionPromotionMatrix, not by the generative nets.
//
// This net drives values ACROSS the mantissa boundary and asserts the
// decoded-into-any VALUE (where the rounding is observable), across
// positions and through both the natural resolved path.
// ---------------------------------------------------------------------------

func TestMatrix_PromotionPrecision(t *testing.T) {
	const f32boundary = 1 << 24 // 16777216; +1 is not exactly float32-representable
	const f64boundary = 1 << 53 // 9007199254740992; +1 is not exactly float64-representable

	cases := []struct {
		label        string
		wKind, rKind string
		wVal         any
		want         float64 // decoded into a float64 target (reveals the intermediate rounding)
	}{
		// Decoded into a float64 target: the reader-width rounding of the
		// INTERMEDIATE is observable (an any target would be float32 for a
		// float reader and re-round, hiding it).
		// int/long → float: 2^24+1 rounds at the float32 mantissa -> 2^24.
		{"int→float@mantissa", "int", "float", int32(f32boundary + 1), float64(float32(f32boundary + 1))},
		{"long→float@mantissa", "long", "float", int64(f32boundary + 1), float64(float32(f32boundary + 1))},
		// long → double: 2^53+1 rounds at the float64 mantissa.
		{"long→double@mantissa", "long", "double", int64(f64boundary + 1), float64(f64boundary + 1)},
		// int → double is exact (every int32 fits the float64 mantissa).
		{"int→double-exact", "int", "double", int32(f32boundary + 1), float64(f32boundary + 1)},
	}

	positions := []struct {
		label  string
		wrap   func(leaf string) string
		val    func(v any) any
		target func() any // ptr to a tree of float64 leaves
		leaf   func(tgt any) float64
	}{
		{"top", func(l string) string { return l },
			func(v any) any { return v },
			func() any { return new(float64) },
			func(t any) float64 { return *(t.(*float64)) }},
		{"field", func(l string) string {
			return fmt.Sprintf(`{"type":"record","name":"PP","fields":[{"name":"f","type":%s}]}`, l)
		}, func(v any) any { return map[string]any{"f": v} },
			func() any {
				return &struct {
					F float64 `avro:"f"`
				}{}
			},
			func(t any) float64 {
				return t.(*struct {
					F float64 `avro:"f"`
				}).F
			}},
		{"array", func(l string) string { return fmt.Sprintf(`{"type":"array","items":%s}`, l) },
			func(v any) any { return []any{v} },
			func() any { return &[]float64{} },
			func(t any) float64 { return (*(t.(*[]float64)))[0] }},
	}

	for _, c := range cases {
		for _, pos := range positions {
			t.Run(c.label+"/"+pos.label, func(t *testing.T) {
				w := avro.MustParse(pos.wrap(fmt.Sprintf("%q", c.wKind)))
				r := avro.MustParse(pos.wrap(fmt.Sprintf("%q", c.rKind)))
				res, err := avro.Resolve(w, r)
				if err != nil {
					t.Fatalf("Resolve: %v", err)
				}
				wire, err := w.AppendEncode(nil, pos.val(c.wVal))
				if err != nil {
					t.Fatalf("writer encode: %v", err)
				}
				// Decode into any: the promoted value's PRECISION is
				// observable here (re-encoding against the reader's float
				// wire would re-round and hide a wrong intermediate).
				tgt := pos.target()
				if _, err := res.Decode(wire, tgt); err != nil {
					t.Fatalf("resolved decode: %v", err)
				}
				if leaf := pos.leaf(tgt); leaf != c.want {
					t.Fatalf("%s: promoted value %v, want %v (reader-width-rounded intermediate). A wrong mantissa conversion shows here.",
						c.label, leaf, c.want)
				}
			})
		}
	}
}

// ---------- matrix_recursion_test.go ----------

// ---------------------------------------------------------------------------
// Recursion matrix: recursive schemas through every carrier kind, at several
// depths, through every wire path (the six-step core + rebuild + identity
// resolve from matrix_core), plus TaggedUnions, typed-struct targets, and
// promotion-inside-recursion.
// ---------------------------------------------------------------------------

type recShape struct {
	label  string
	schema string
	// value builds a generic value chain of the given depth (depth 0 =
	// terminal node, no recursion taken).
	value func(depth int) any
	// tagged: whether the shape's unions are same-class ambiguous bare —
	// if so, runCore runs with TaggedUnions (branch fully recoverable).
	needTagged bool
}

func recShapes() []recShape {
	return []recShape{
		{label: "direct-nullunion",
			schema: `{"type":"record","name":"Node","fields":[
				{"name":"v","type":"int"},
				{"name":"next","type":["null","Node"],"default":null}]}`,
			value: func(d int) any {
				cur := map[string]any{"v": int32(d), "next": nil}
				for i := d - 1; i >= 0; i-- {
					cur = map[string]any{"v": int32(i), "next": cur}
				}
				return cur
			}},
		{label: "array-carrier",
			schema: `{"type":"record","name":"Node","fields":[
				{"name":"v","type":"int"},
				{"name":"kids","type":{"type":"array","items":"Node"}}]}`,
			value: func(d int) any {
				cur := map[string]any{"v": int32(d), "kids": []any{}}
				for i := d - 1; i >= 0; i-- {
					cur = map[string]any{"v": int32(i), "kids": []any{cur}}
				}
				return cur
			}},
		{label: "array-carrier-branch2",
			schema: `{"type":"record","name":"Node","fields":[
				{"name":"v","type":"int"},
				{"name":"kids","type":{"type":"array","items":"Node"}}]}`,
			value: func(d int) any {
				leaf := func(v int32) map[string]any { return map[string]any{"v": v, "kids": []any{}} }
				if d == 0 {
					return leaf(0)
				}
				cur := map[string]any{"v": int32(0), "kids": []any{leaf(1), leaf(2)}}
				for i := 1; i < d; i++ {
					cur = map[string]any{"v": int32(i), "kids": []any{cur, leaf(int32(100 + i))}}
				}
				return cur
			}},
		{label: "map-carrier",
			schema: `{"type":"record","name":"Node","fields":[
				{"name":"v","type":"int"},
				{"name":"kids","type":{"type":"map","values":"Node"}}]}`,
			value: func(d int) any {
				cur := map[string]any{"v": int32(d), "kids": map[string]any{}}
				for i := d - 1; i >= 0; i-- {
					cur = map[string]any{"v": int32(i), "kids": map[string]any{"c": cur}}
				}
				return cur
			}},
		{label: "nullable-array-carrier",
			schema: `{"type":"record","name":"Node","fields":[
				{"name":"v","type":"int"},
				{"name":"kids","type":["null",{"type":"array","items":"Node"}],"default":null}]}`,
			value: func(d int) any {
				cur := map[string]any{"v": int32(d), "kids": nil}
				for i := d - 1; i >= 0; i-- {
					cur = map[string]any{"v": int32(i), "kids": []any{cur}}
				}
				return cur
			}},
		{label: "multibranch-self",
			schema: `{"type":"record","name":"Node","fields":[
				{"name":"v","type":"int"},
				{"name":"next","type":["null","string","Node"],"default":null}]}`,
			value: func(d int) any {
				var cur any = "tail"
				for i := d - 1; i >= 0; i-- {
					cur = map[string]any{"v": int32(i), "next": cur}
				}
				if d == 0 {
					return map[string]any{"v": int32(0), "next": nil}
				}
				return cur
			}},
		{label: "mutual",
			schema: `{"type":"record","name":"A","fields":[
				{"name":"v","type":"int"},
				{"name":"b","type":["null",{"type":"record","name":"B","fields":[
					{"name":"w","type":"string"},
					{"name":"a","type":["null","A"],"default":null}]}],"default":null}]}`,
			value: func(d int) any {
				// Alternate A→B→A…, d levels of descent.
				var build func(level int) any
				build = func(level int) any {
					if level >= d {
						return map[string]any{"v": int32(level), "b": nil}
					}
					return map[string]any{"v": int32(level), "b": map[string]any{
						"w": fmt.Sprintf("w%d", level),
						"a": func() any {
							if level+1 >= d {
								return nil
							}
							return build(level + 1)
						}(),
					}}
				}
				return build(0)
			}},
		{label: "fwd-ref-union",
			schema: `{"type":"record","name":"Wrap","fields":[
				{"name":"early","type":["null","Node"],"default":null},
				{"name":"def","type":{"type":"record","name":"Node","fields":[
					{"name":"v","type":"int"},
					{"name":"next","type":["null","Node"],"default":null}]}}]}`,
			value: func(d int) any {
				chain := func() any {
					cur := map[string]any{"v": int32(d), "next": nil}
					for i := d - 1; i >= 0; i-- {
						cur = map[string]any{"v": int32(i), "next": cur}
					}
					return cur
				}
				return map[string]any{"early": chain(), "def": chain()}
			}},
		{label: "through-mid-record",
			schema: `{"type":"record","name":"Node","fields":[
				{"name":"v","type":"int"},
				{"name":"mid","type":{"type":"record","name":"Mid","fields":[
					{"name":"next","type":["null","Node"],"default":null}]}}]}`,
			value: func(d int) any {
				cur := map[string]any{"v": int32(d), "mid": map[string]any{"next": nil}}
				for i := d - 1; i >= 0; i-- {
					cur = map[string]any{"v": int32(i), "mid": map[string]any{"next": cur}}
				}
				return cur
			}},
	}
}

func TestMatrix_Recursion(t *testing.T) {
	depths := []int{0, 1, 3, 17}
	for _, sh := range recShapes() {
		for _, d := range depths {
			t.Run(fmt.Sprintf("%s/depth%d", sh.label, d), func(t *testing.T) {
				runCore(t, sh.schema, sh.value(d))
			})
			t.Run(fmt.Sprintf("%s/depth%d/tagged", sh.label, d), func(t *testing.T) {
				runCore(t, sh.schema, sh.value(d), avro.TaggedUnions())
			})
		}
	}
}

// Recursion composed INSIDE outer contexts: the recursive record as array
// item, map value, union branch, and nested record field.
func TestMatrix_RecursionInContext(t *testing.T) {
	node := `{"type":"record","name":"Node","fields":[
		{"name":"v","type":"int"},
		{"name":"next","type":["null","Node"],"default":null}]}`
	chain := func(d int) any {
		cur := map[string]any{"v": int32(d), "next": nil}
		for i := d - 1; i >= 0; i-- {
			cur = map[string]any{"v": int32(i), "next": cur}
		}
		return cur
	}
	cases := []struct {
		label  string
		schema string
		value  func(d int) any
	}{
		{"array-of-recursive", fmt.Sprintf(`{"type":"array","items":%s}`, node),
			func(d int) any { return []any{chain(d), chain(0)} }},
		{"map-of-recursive", fmt.Sprintf(`{"type":"map","values":%s}`, node),
			func(d int) any { return map[string]any{"k": chain(d)} }},
		{"nullunion-of-recursive", fmt.Sprintf(`["null",%s]`, node),
			func(d int) any { return chain(d) }},
		{"field-of-recursive", fmt.Sprintf(`{"type":"record","name":"Outer","fields":[{"name":"n","type":%s},{"name":"s","type":"string"}]}`, node),
			func(d int) any { return map[string]any{"n": chain(d), "s": "x"} }},
		{"array-of-nullunion-of-recursive", fmt.Sprintf(`{"type":"array","items":["null",%s]}`, node),
			func(d int) any { return []any{chain(d), nil, chain(1)} }},
	}
	for _, c := range cases {
		for _, d := range []int{0, 2, 9} {
			t.Run(fmt.Sprintf("%s/depth%d", c.label, d), func(t *testing.T) {
				runCore(t, c.schema, c.value(d))
			})
		}
	}
}

// Typed (struct) targets for recursive shapes: the unsafe struct path and
// the reflect path must agree with the generic path byte-for-byte, on both
// wire formats, for addressable and non-addressable encodes.
type recNode struct {
	V    int32    `avro:"v"`
	Next *recNode `avro:"next"`
}

type recArrNode struct {
	V    int32        `avro:"v"`
	Kids []recArrNode `avro:"kids"`
}

type recA struct {
	V int32 `avro:"v"`
	B *recB `avro:"b"`
}
type recB struct {
	W string `avro:"w"`
	A *recA  `avro:"a"`
}

func TestMatrix_RecursionTyped(t *testing.T) {
	mkChain := func(d int) *recNode {
		cur := &recNode{V: int32(d)}
		for i := d - 1; i >= 0; i-- {
			cur = &recNode{V: int32(i), Next: cur}
		}
		return cur
	}
	mkArr := func(d int) recArrNode {
		cur := recArrNode{V: int32(d), Kids: []recArrNode{}}
		for i := d - 1; i >= 0; i-- {
			cur = recArrNode{V: int32(i), Kids: []recArrNode{cur}}
		}
		return cur
	}
	mkAB := func(d int) *recA {
		var build func(level int) *recA
		build = func(level int) *recA {
			a := &recA{V: int32(level)}
			if level < d {
				b := &recB{W: fmt.Sprintf("w%d", level)}
				if level+1 < d {
					b.A = build(level + 1)
				}
				a.B = b
			}
			return a
		}
		return build(0)
	}

	directSchema := `{"type":"record","name":"Node","fields":[
		{"name":"v","type":"int"},
		{"name":"next","type":["null","Node"],"default":null}]}`
	arrSchema := `{"type":"record","name":"Node","fields":[
		{"name":"v","type":"int"},
		{"name":"kids","type":{"type":"array","items":"Node"}}]}`
	abSchema := `{"type":"record","name":"A","fields":[
		{"name":"v","type":"int"},
		{"name":"b","type":["null",{"type":"record","name":"B","fields":[
			{"name":"w","type":"string"},
			{"name":"a","type":["null","A"],"default":null}]}],"default":null}]}`

	for _, d := range []int{0, 1, 3, 17} {
		t.Run(fmt.Sprintf("direct/depth%d", d), func(t *testing.T) {
			typedCore(t, directSchema, *mkChain(d), func() any { return new(recNode) })
		})
		t.Run(fmt.Sprintf("array/depth%d", d), func(t *testing.T) {
			typedCore(t, arrSchema, mkArr(d), func() any { return new(recArrNode) })
		})
		t.Run(fmt.Sprintf("mutual/depth%d", d), func(t *testing.T) {
			typedCore(t, abSchema, *mkAB(d), func() any { return new(recA) })
		})
	}
}

// typedCore: encode the typed value (addressable AND non-addressable forms
// must agree), decode generic and typed, re-encode from the typed decode,
// and run the JSON twins — all byte-identical wires.
func typedCore(t *testing.T, schemaJSON string, typedVal any, newTarget func() any) {
	t.Helper()
	s, err := avro.Parse(schemaJSON)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	// Addressable (pointer) vs non-addressable (value) encode parity.
	wVal, err := s.AppendEncode(nil, typedVal)
	if err != nil {
		t.Fatalf("encode value-form: %v", err)
	}
	pv := newTarget()
	// Fill the pointer with the same value for the addressable encode:
	// decode the value-form wire into it (also exercises typed decode).
	if _, err := s.Decode(wVal, pv); err != nil {
		t.Fatalf("typed decode: %v", err)
	}
	wPtr, err := s.AppendEncode(nil, pv)
	if err != nil {
		t.Fatalf("encode pointer-form: %v", err)
	}
	if !bytes.Equal(wVal, wPtr) {
		t.Fatalf("addressable vs non-addressable wire differs:\n val=%x\n ptr=%x", wVal, wPtr)
	}
	// Generic decode agrees with typed round-trip.
	var generic any
	if _, err := s.Decode(wVal, &generic); err != nil {
		t.Fatalf("generic decode: %v", err)
	}
	wGen, err := s.AppendEncode(nil, generic)
	if err != nil || !bytes.Equal(wGen, wVal) {
		t.Fatalf("generic re-encode differs: err=%v\n val=%x\n gen=%x", err, wVal, wGen)
	}
	// JSON twins: typed encode, typed decode, generic agreement.
	jVal, err := s.AppendEncodeJSON(nil, typedVal)
	if err != nil {
		t.Fatalf("encodeJSON value-form: %v", err)
	}
	jPtr, err := s.AppendEncodeJSON(nil, pv)
	if err != nil || !bytes.Equal(jVal, jPtr) {
		t.Fatalf("JSON addressable/non-addressable differs: err=%v\n %s\n %s", err, jVal, jPtr)
	}
	pj := newTarget()
	if err := s.DecodeJSON(jVal, pj); err != nil {
		t.Fatalf("typed DecodeJSON: %v", err)
	}
	wFromJSON, err := s.AppendEncode(nil, pj)
	if err != nil || !bytes.Equal(wFromJSON, wVal) {
		t.Fatalf("typed JSON round-trip lands on different wire: err=%v\n w=%x\n j=%x", err, wVal, wFromJSON)
	}
}

// Promotion inside recursion across carriers: writer int chains resolve into
// reader long/double chains through every recursive carrier.
func TestMatrix_RecursionPromotion(t *testing.T) {
	pairs := []struct {
		label  string
		writer string
		reader string
	}{
		{"direct int→long",
			`{"type":"record","name":"N","fields":[{"name":"v","type":"int"},{"name":"next","type":["null","N"],"default":null}]}`,
			`{"type":"record","name":"N","fields":[{"name":"v","type":"long"},{"name":"next","type":["null","N"],"default":null}]}`},
		{"array int→double",
			`{"type":"record","name":"N","fields":[{"name":"v","type":"int"},{"name":"kids","type":{"type":"array","items":"N"}}]}`,
			`{"type":"record","name":"N","fields":[{"name":"v","type":"double"},{"name":"kids","type":{"type":"array","items":"N"}}]}`},
		{"map string→bytes",
			`{"type":"record","name":"N","fields":[{"name":"v","type":"string"},{"name":"kids","type":{"type":"map","values":"N"}}]}`,
			`{"type":"record","name":"N","fields":[{"name":"v","type":"bytes"},{"name":"kids","type":{"type":"map","values":"N"}}]}`},
		{"mutual int→long",
			`{"type":"record","name":"A","fields":[{"name":"v","type":"int"},{"name":"b","type":["null",{"type":"record","name":"B","fields":[{"name":"a","type":["null","A"],"default":null}]}],"default":null}]}`,
			`{"type":"record","name":"A","fields":[{"name":"v","type":"long"},{"name":"b","type":["null",{"type":"record","name":"B","fields":[{"name":"a","type":["null","A"],"default":null}]}],"default":null}]}`},
	}
	for _, p := range pairs {
		t.Run(p.label, func(t *testing.T) {
			w := avro.MustParse(p.writer)
			r := avro.MustParse(p.reader)
			res, err := avro.Resolve(w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			// Build a depth-3 writer value generically against the writer.
			var v any
			switch p.label {
			case "direct int→long":
				v = map[string]any{"v": int32(1), "next": map[string]any{"v": int32(2), "next": map[string]any{"v": int32(3), "next": nil}}}
			case "array int→double":
				v = map[string]any{"v": int32(1), "kids": []any{map[string]any{"v": int32(2), "kids": []any{map[string]any{"v": int32(3), "kids": []any{}}}}}}
			case "map string→bytes":
				v = map[string]any{"v": "a", "kids": map[string]any{"k": map[string]any{"v": "b", "kids": map[string]any{}}}}
			case "mutual int→long":
				v = map[string]any{"v": int32(1), "b": map[string]any{"a": map[string]any{"v": int32(2), "b": nil}}}
			}
			wire, err := w.AppendEncode(nil, v)
			if err != nil {
				t.Fatalf("writer encode: %v", err)
			}
			var got any
			if _, err := res.Decode(wire, &got); err != nil {
				t.Fatalf("resolved decode: %v", err)
			}
			// Spot-check the promoted leaf types at the top level.
			top := got.(map[string]any)
			switch p.label {
			case "direct int→long", "mutual int→long":
				if _, ok := top["v"].(int64); !ok {
					t.Fatalf("v promoted to %T, want int64", top["v"])
				}
			case "array int→double":
				if _, ok := top["v"].(float64); !ok {
					t.Fatalf("v promoted to %T, want float64", top["v"])
				}
			case "map string→bytes":
				if _, ok := top["v"].([]byte); !ok {
					t.Fatalf("v promoted to %T, want []byte", top["v"])
				}
			}
			// And the resolved value re-encodes cleanly against the READER.
			rs := avro.MustParse(p.reader)
			if _, err := rs.AppendEncode(nil, got); err != nil {
				t.Fatalf("re-encode promoted value against reader: %v", err)
			}
		})
	}
}

// ---------- matrix_reject_test.go ----------

// ---------------------------------------------------------------------------
// Rejection-parity matrix: for values that do NOT fit a schema, the binary
// and JSON encoders must AGREE on rejection, and for any wire the two
// decoders must agree on target rejection. Historically the largest bug
// class (encode accepts X / decode rejects X, or one wire format accepts
// what the other rejects); this asserts the parity generatively instead of
// pinning single instances.
// ---------------------------------------------------------------------------

func TestMatrix_EncodeRejectionParity(t *testing.T) {
	cases := []struct {
		label  string
		schema string
		bad    []any
	}{
		{"int", `"int"`, []any{"s", true, []byte{1}, 1.5, float64(math.MaxInt32) * 4, map[string]any{}, math.NaN()}},
		{"long", `"long"`, []any{"s", true, 2.5, math.Inf(1), []any{}}},
		{"float", `"float"`, []any{"s", true, []byte{1}, map[string]any{}}},
		{"double", `"double"`, []any{"s", true, []byte{1}, []any{}}},
		{"boolean", `"boolean"`, []any{int32(1), "true", 0.0, []byte{1}}},
		{"string", `"string"`, []any{true, int32(1), 1.5, []any{}, map[string]any{}}},
		{"bytes", `"bytes"`, []any{true, int32(1), 1.5, []any{1}, map[string]any{}}},
		{"null", `"null"`, []any{int32(0), "", false, []byte{}}},
		{"enum", `{"type":"enum","name":"RJE","symbols":["A","B"]}`,
			[]any{"Z", "", int32(2), int32(-1), true, 1.5}},
		{"fixed2", `{"type":"fixed","name":"RJF","size":2}`,
			[]any{[]byte{1}, []byte{1, 2, 3}, "x", "xyz", true, int32(1)}},
		{"fixed0", `{"type":"fixed","name":"RJF0","size":0}`,
			[]any{[]byte{1}, "x", int32(0)}},
		{"date", `{"type":"int","logicalType":"date"}`, []any{"2024-13-45", true, []byte{1}}},
		{"timestamp", `{"type":"long","logicalType":"timestamp-millis"}`, []any{true, []byte{1}, map[string]any{}}},
		{"uuid-fixed", `{"type":"fixed","name":"RJU","size":16,"logicalType":"uuid"}`,
			[]any{"not-a-uuid", "6ba7b810", true, int32(1)}},
		// NOTE: a NON-numeric string against bytes+decimal is ACCEPTED as
		// raw bytes (the documented bytes/fixed encode-side string-source
		// leniency; numeric strings coerce to decimal instead), so it is
		// not in the bad set.
		{"decimal", `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
			[]any{true, []any{}, map[string]any{}}},
		{"array", `{"type":"array","items":"int"}`, []any{int32(1), "s", map[string]any{"k": int32(1)}, []any{"s"}}},
		{"map", `{"type":"map","values":"int"}`, []any{int32(1), "s", []any{int32(1)}, map[string]any{"k": "s"}}},
		{"record", `{"type":"record","name":"RJR","fields":[{"name":"a","type":"int"}]}`,
			[]any{int32(1), "s", []any{}, map[string]any{"a": "s"}}},
		{"nullunion", `["null","int"]`, []any{"s", true, 1.5, []byte{1}}},
		{"multibranch", `["null","boolean","int"]`, []any{"s", []byte{1}, []any{}, 2.5}},
	}
	positions := []struct {
		label  string
		schema func(in string) string
		wrap   func(v any) any
	}{
		{"top", func(in string) string { return in }, func(v any) any { return v }},
		{"field", func(in string) string {
			return fmt.Sprintf(`{"type":"record","name":"RJW","fields":[{"name":"f","type":%s}]}`, in)
		}, func(v any) any { return map[string]any{"f": v} }},
		{"array-item", func(in string) string { return fmt.Sprintf(`{"type":"array","items":%s}`, in) },
			func(v any) any { return []any{v} }},
	}
	for _, c := range cases {
		for _, pos := range positions {
			t.Run(c.label+"/"+pos.label, func(t *testing.T) {
				s := avro.MustParse(pos.schema(c.schema))
				for i, bad := range c.bad {
					vin := pos.wrap(bad)
					_, binErr := s.AppendEncode(nil, vin)
					_, jsonErr := s.AppendEncodeJSON(nil, vin)
					if (binErr == nil) != (jsonErr == nil) {
						t.Errorf("bad[%d] %#v: encode rejection diverges: binary=%v json=%v",
							i, bad, binErr, jsonErr)
					}
					if binErr == nil {
						t.Errorf("bad[%d] %#v: unexpectedly accepted by both encoders", i, bad)
					}
				}
			})
		}
	}
}

// Decode-target rejection parity: for one valid wire, decoding into a
// mismatched Go target must reject on the binary and JSON paths alike.
func TestMatrix_DecodeTargetRejectionParity(t *testing.T) {
	mkTargets := func() map[string]any {
		return map[string]any{
			"int32":   new(int32),
			"int64":   new(int64),
			"float64": new(float64),
			"bool":    new(bool),
			"string":  new(string),
			"bytes":   new([]byte),
			"arr2":    new([2]byte),
			"slice":   new([]int32),
			"map":     new(map[string]int32),
		}
	}
	cases := []struct {
		label   string
		schema  string
		value   any
		accepts map[string]bool // target keys that must accept; all others must reject
	}{
		{"int", `"int"`, int32(7),
			map[string]bool{"int32": true, "int64": true, "float64": true}},
		{"boolean", `"boolean"`, true,
			map[string]bool{"bool": true}},
		{"string", `"string"`, "sv",
			map[string]bool{"string": true, "bytes": true}},
		// [2]byte is a legal exact-length target for 2-byte bytes values
		// (setBytesValue's array arm).
		{"bytes", `"bytes"`, []byte{1, 2},
			map[string]bool{"bytes": true, "string": true, "arr2": true}},
		{"fixed2", `{"type":"fixed","name":"DTF","size":2}`, []byte{1, 2},
			map[string]bool{"bytes": true, "string": true, "arr2": true}},
		// []byte is []uint8 — a legitimate typed slice target for
		// array<int> whose values fit uint8.
		{"array-int", `{"type":"array","items":"int"}`, []any{int32(1)},
			map[string]bool{"slice": true, "bytes": true}},
		{"map-int", `{"type":"map","values":"int"}`, map[string]any{"k": int32(1)},
			map[string]bool{"map": true}},
	}
	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			s := avro.MustParse(c.schema)
			wire := mustAppendEncode(t, s, nil, c.value)
			j := mustAppendEncodeJSON(t, s, nil, c.value)
			for name, target := range mkTargets() {
				_, binErr := s.Decode(wire, target)
				jsonTargets := mkTargets() // fresh, undamaged by the binary pass
				jsonErr := s.DecodeJSON(j, jsonTargets[name])
				if (binErr == nil) != (jsonErr == nil) {
					t.Errorf("target %s: decode rejection diverges: binary=%v json=%v", name, binErr, jsonErr)
					continue
				}
				if want := c.accepts[name]; (binErr == nil) != want {
					t.Errorf("target %s: accept=%v want=%v (binErr=%v)", name, binErr == nil, want, binErr)
				}
			}
		})
	}
}

// ---------- matrix_resolved_json_union_test.go ----------

// ---------------------------------------------------------------------------
// Resolved-schema DecodeJSON union matrix: input form {tagged envelope, bare
// value} × colliding-branch union shapes × reader resolution
// {identical-branch, per-branch-divergent}.
//
// The independent oracle for every cell is the resolved BINARY decode of the
// equivalent writer wire: DecodeJSON on a schema returned by Resolve consumes
// writer-shaped JSON and must land exactly where resolved.Decode lands on the
// writer binary carrying the same branch choice (the JSON is parsed against
// the writer, then resolved — Java's ResolvingDecoder over a JsonDecoder
// built with the writer schema).
//
//   - TAGGED cells name a branch via the spec's {"branch": value} envelope.
//     Every shape's branch pair accepts the same value, so the envelope is
//     the only carrier of the writer's choice; each cell drives BOTH
//     branches and asserts the resolved decode (plain and TaggedUnions
//     projections) matches the binary oracle for the SAME tagged choice —
//     branch identity must survive even though the value alone would
//     first-match an earlier branch.
//   - BARE cells carry the value without an envelope. The bare form does
//     not name the writer's branch, so the decoder commits to the FIRST
//     declaration-order branch of the matching JSON token class — the
//     documented lossy leniency (see the TaggedUnions doc) — and
//     resolution then applies to THAT branch. The oracle is the binary
//     wire of the first-match branch. This holds on a resolved schema
//     exactly as on a plain one, including where the first-match branch
//     is an enum or fixed declared before a string/bytes sibling.
//
// Shapes include a recursive record pair and a diamond (shared named-type
// reference) pair so the dispatch is exercised on reference paths, not only
// on first definitions.
// ---------------------------------------------------------------------------

type resolvedUnionBranch struct {
	name  string // branch name as it appears in the tagged envelope
	value any    // the branch value for the binary-oracle encode
	json  string // the branch value as writer-shaped JSON
}

type resolvedUnionShape struct {
	name        string
	writerUnion string
	// divergentUnion is a reader union whose resolution differs per branch
	// (a dropped enum symbol falling to the reader default, an added
	// defaulted record field, reordered branches) so a branch flip changes
	// the decoded VALUE or index mapping, not just the envelope key.
	divergentUnion string
	branches       []resolvedUnionBranch
	bareJSON       string
	bareValue      any
	bareFirstMatch string // first declaration-order branch of bareJSON's token class
}

func resolvedUnionShapes() []resolvedUnionShape {
	return []resolvedUnionShape{
		{
			name:           "enum-vs-string",
			writerUnion:    `["string",{"type":"enum","name":"E","symbols":["A"]}]`,
			divergentUnion: `["string",{"type":"enum","name":"E","symbols":["Z"],"default":"Z"}]`,
			branches: []resolvedUnionBranch{
				{"string", "A", `"A"`},
				{"E", "A", `"A"`},
			},
			bareJSON: `"A"`, bareValue: "A", bareFirstMatch: "string",
		},
		{
			// Mirrored declaration order: the enum is first, so the BARE form
			// commits to the enum branch (first token-class match in
			// declaration order, same as an unresolved DecodeJSON), not to
			// the string branch.
			name:           "enum-before-string",
			writerUnion:    `[{"type":"enum","name":"E","symbols":["A"]},"string"]`,
			divergentUnion: `[{"type":"enum","name":"E","symbols":["Z"],"default":"Z"},"string"]`,
			branches: []resolvedUnionBranch{
				{"E", "A", `"A"`},
				{"string", "A", `"A"`},
			},
			bareJSON: `"A"`, bareValue: "A", bareFirstMatch: "E",
		},
		{
			name:           "two-records",
			writerUnion:    `[{"type":"record","name":"R1","fields":[{"name":"f","type":"string"}]},{"type":"record","name":"R2","fields":[{"name":"f","type":"string"}]}]`,
			divergentUnion: `[{"type":"record","name":"R1","fields":[{"name":"f","type":"string"}]},{"type":"record","name":"R2","fields":[{"name":"f","type":"string"},{"name":"g","type":"int","default":9}]}]`,
			branches: []resolvedUnionBranch{
				{"R1", map[string]any{"f": "x"}, `{"f":"x"}`},
				{"R2", map[string]any{"f": "x"}, `{"f":"x"}`},
			},
			bareJSON: `{"f":"x"}`, bareValue: map[string]any{"f": "x"}, bareFirstMatch: "R1",
		},
		{
			name:           "two-enums",
			writerUnion:    `[{"type":"enum","name":"E1","symbols":["A","B"]},{"type":"enum","name":"E2","symbols":["A","C"]}]`,
			divergentUnion: `[{"type":"enum","name":"E1","symbols":["A","B"]},{"type":"enum","name":"E2","symbols":["C"],"default":"C"}]`,
			branches: []resolvedUnionBranch{
				{"E1", "A", `"A"`},
				{"E2", "A", `"A"`},
			},
			bareJSON: `"A"`, bareValue: "A", bareFirstMatch: "E1",
		},
		{
			name:        "two-fixed",
			writerUnion: `[{"type":"fixed","name":"F1","size":2},{"type":"fixed","name":"F2","size":2}]`,
			// Reordered reader branches: each writer branch maps to a
			// different reader index, so a flipped branch lands on the
			// wrong side of the index remap.
			divergentUnion: `[{"type":"fixed","name":"F2","size":2},{"type":"fixed","name":"F1","size":2}]`,
			branches: []resolvedUnionBranch{
				{"F1", []byte("ab"), `"ab"`},
				{"F2", []byte("ab"), `"ab"`},
			},
			bareJSON: `"ab"`, bareValue: []byte("ab"), bareFirstMatch: "F1",
		},
		{
			name:           "fixed-vs-bytes",
			writerUnion:    `[{"type":"fixed","name":"F","size":2},"bytes"]`,
			divergentUnion: `["bytes",{"type":"fixed","name":"F","size":2}]`,
			branches: []resolvedUnionBranch{
				{"F", []byte("ab"), `"ab"`},
				{"bytes", []byte("ab"), `"ab"`},
			},
			bareJSON: `"ab"`, bareValue: []byte("ab"), bareFirstMatch: "F",
		},
		{
			name:           "map-vs-record",
			writerUnion:    `[{"type":"map","values":"string"},{"type":"record","name":"R","fields":[{"name":"f","type":"string"}]}]`,
			divergentUnion: `[{"type":"map","values":"string"},{"type":"record","name":"R","fields":[{"name":"f","type":"string"},{"name":"g","type":"int","default":5}]}]`,
			branches: []resolvedUnionBranch{
				{"map", map[string]any{"f": "x"}, `{"f":"x"}`},
				{"R", map[string]any{"f": "x"}, `{"f":"x"}`},
			},
			bareJSON: `{"f":"x"}`, bareValue: map[string]any{"f": "x"}, bareFirstMatch: "map",
		},
		{
			// Namespaced records: the envelope key is the FULLNAME (spec;
			// fastavro emits and requires fullname keys), so the wrap and
			// the re-encode's tagged-map acceptance must agree on the
			// qualified form.
			name:           "two-records-namespaced",
			writerUnion:    `[{"type":"record","name":"com.ex.R1","fields":[{"name":"f","type":"string"}]},{"type":"record","name":"com.ex.R2","fields":[{"name":"f","type":"string"}]}]`,
			divergentUnion: `[{"type":"record","name":"com.ex.R1","fields":[{"name":"f","type":"string"}]},{"type":"record","name":"com.ex.R2","fields":[{"name":"f","type":"string"},{"name":"g","type":"int","default":9}]}]`,
			branches: []resolvedUnionBranch{
				{"com.ex.R1", map[string]any{"f": "x"}, `{"f":"x"}`},
				{"com.ex.R2", map[string]any{"f": "x"}, `{"f":"x"}`},
			},
			bareJSON: `{"f":"x"}`, bareValue: map[string]any{"f": "x"}, bareFirstMatch: "com.ex.R1",
		},
		{
			// Recursive branches: each record is self-referential, so the
			// tagged dispatch must hold on a node that re-enters itself (the
			// reference path, not only the definition path).
			name:           "two-records-recursive",
			writerUnion:    `[{"type":"record","name":"R1","fields":[{"name":"f","type":"string"},{"name":"next","type":["null","R1"],"default":null}]},{"type":"record","name":"R2","fields":[{"name":"f","type":"string"},{"name":"next","type":["null","R2"],"default":null}]}]`,
			divergentUnion: `[{"type":"record","name":"R1","fields":[{"name":"f","type":"string"},{"name":"next","type":["null","R1"],"default":null}]},{"type":"record","name":"R2","fields":[{"name":"f","type":"string"},{"name":"next","type":["null","R2"],"default":null},{"name":"g","type":"int","default":3}]}]`,
			branches: []resolvedUnionBranch{
				{"R1", map[string]any{"f": "x", "next": map[string]any{"R1": map[string]any{"f": "y", "next": nil}}}, `{"f":"x","next":{"R1":{"f":"y","next":null}}}`},
				{"R2", map[string]any{"f": "x", "next": map[string]any{"R2": map[string]any{"f": "y", "next": nil}}}, `{"f":"x","next":{"R2":{"f":"y","next":null}}}`},
			},
			bareJSON: `{"f":"x","next":null}`, bareValue: map[string]any{"f": "x", "next": nil}, bareFirstMatch: "R1",
		},
		{
			// Diamond: both records reference ONE shared enum definition, so
			// the dispatch must hold where a named type's second occurrence
			// is a name reference rather than an inline definition.
			name:           "diamond-shared-enum",
			writerUnion:    `[{"type":"record","name":"RA","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A"]}}]},{"type":"record","name":"RB","fields":[{"name":"e","type":"E"}]}]`,
			divergentUnion: `[{"type":"record","name":"RA","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A"]}}]},{"type":"record","name":"RB","fields":[{"name":"e","type":"E"},{"name":"g","type":"int","default":4}]}]`,
			branches: []resolvedUnionBranch{
				{"RA", map[string]any{"e": "A"}, `{"e":"A"}`},
				{"RB", map[string]any{"e": "A"}, `{"e":"A"}`},
			},
			bareJSON: `{"e":"A"}`, bareValue: map[string]any{"e": "A"}, bareFirstMatch: "RA",
		},
	}
}

// assertResolvedJSONMatchesBinary decodes writerJSON via the resolved
// schema's DecodeJSON and asserts both its plain and TaggedUnions
// projections land exactly where resolved.Decode lands on the writer binary
// wire carrying the same branch choice (oracleValue spells union choices as
// tagged maps for the writer encode, so the wire index is unambiguous).
func assertResolvedJSONMatchesBinary(t *testing.T, writer, resolved *avro.Schema, oracleValue any, writerJSON string) {
	t.Helper()
	wire, err := writer.Encode(oracleValue)
	if err != nil {
		t.Fatalf("writer Encode of oracle value: %v", err)
	}
	var binPlain, jsonPlain any
	if _, err := resolved.Decode(wire, &binPlain); err != nil {
		t.Fatalf("resolved.Decode (binary oracle): %v", err)
	}
	if err := resolved.DecodeJSON([]byte(writerJSON), &jsonPlain); err != nil {
		t.Fatalf("resolved.DecodeJSON: %v", err)
	}
	if !reflect.DeepEqual(jsonPlain, binPlain) {
		t.Errorf("resolved JSON decode != binary decode:\n  binary=%#v\n  json  =%#v", binPlain, jsonPlain)
	}
	var binTagged, jsonTagged any
	if _, err := resolved.Decode(wire, &binTagged, avro.TaggedUnions()); err != nil {
		t.Fatalf("resolved.Decode (binary oracle, TaggedUnions): %v", err)
	}
	if err := resolved.DecodeJSON([]byte(writerJSON), &jsonTagged, avro.TaggedUnions()); err != nil {
		t.Fatalf("resolved.DecodeJSON (TaggedUnions): %v", err)
	}
	if !reflect.DeepEqual(jsonTagged, binTagged) {
		t.Errorf("resolved JSON decode != binary decode under TaggedUnions (branch identity):\n  binary=%#v\n  json  =%#v", binTagged, jsonTagged)
	}
}

func TestMatrix_ResolvedJSONUnionInputForms(t *testing.T) {
	resolutions := []struct {
		name        string
		readerUnion func(s resolvedUnionShape) string
	}{
		{"identical-branch", func(s resolvedUnionShape) string { return s.writerUnion }},
		{"per-branch-divergent", func(s resolvedUnionShape) string { return s.divergentUnion }},
	}
	for _, s := range resolvedUnionShapes() {
		for _, res := range resolutions {
			writer := avro.MustParse(`{"type":"record","name":"Top","fields":[{"name":"u","type":` + s.writerUnion + `}]}`)
			// The reader always adds a defaulted field so writer≠reader and
			// Resolve returns a resolving schema (identical canonicals
			// short-circuit to the reader itself).
			reader := avro.MustParse(`{"type":"record","name":"Top","fields":[{"name":"u","type":` + res.readerUnion(s) + `},{"name":"pad","type":"int","default":0}]}`)
			resolved, err := avro.Resolve(writer, reader)
			if err != nil {
				t.Fatalf("%s/%s: Resolve: %v", s.name, res.name, err)
			}
			for _, b := range s.branches {
				t.Run(s.name+"/"+res.name+"/tagged-"+b.name, func(t *testing.T) {
					assertResolvedJSONMatchesBinary(t, writer, resolved,
						map[string]any{"u": map[string]any{b.name: b.value}},
						`{"u":{"`+b.name+`":`+b.json+`}}`)
				})
			}
			t.Run(s.name+"/"+res.name+"/bare", func(t *testing.T) {
				assertResolvedJSONMatchesBinary(t, writer, resolved,
					map[string]any{"u": map[string]any{s.bareFirstMatch: s.bareValue}},
					`{"u":`+s.bareJSON+`}`)
			})
		}
	}
}

// A map branch whose CONTENT is a single-entry object with a branch-named key
// is not a union envelope — schema position decides: at the union node a
// single-key object naming a branch is the envelope; one level down, inside
// the map branch, the same shape is plain map content. The intermediate
// round-trip must keep the two levels apart: {"map":{"int":3}} is the map
// branch holding entry "int"→3, never the int branch holding 3.
func TestMatrix_ResolvedJSONUnionEnvelopeShapedMapValue(t *testing.T) {
	writer := avro.MustParse(`{"type":"record","name":"Top","fields":[{"name":"u","type":["int",{"type":"map","values":"int"}]}]}`)
	reader := avro.MustParse(`{"type":"record","name":"Top","fields":[{"name":"u","type":["int",{"type":"map","values":"int"}]},{"name":"pad","type":"int","default":0}]}`)
	resolved := mustResolve(t, writer, reader)
	cells := []struct {
		name        string
		oracleValue any
		writerJSON  string
	}{
		// The disambiguation cell: the map's only entry is keyed by a
		// sibling branch's name.
		{"tagged-map-branch-with-branch-named-key",
			map[string]any{"u": map[string]any{"map": map[string]any{"int": 3}}},
			`{"u":{"map":{"int":3}}}`},
		// At the union node the same single-key shape IS the envelope.
		{"tagged-int-branch",
			map[string]any{"u": map[string]any{"int": 3}},
			`{"u":{"int":3}}`},
		// Bare map content with a non-branch key: tagged interpretation
		// fails, the bare fallback commits to the map branch.
		{"bare-map-noncolliding-key",
			map[string]any{"u": map[string]any{"map": map[string]any{"k": 3}}},
			`{"u":{"k":3}}`},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			assertResolvedJSONMatchesBinary(t, writer, resolved, c.oracleValue, c.writerJSON)
		})
	}
}

// A resolved DecodeJSON must keep both properties at once: the writer view
// used for the JSON→binary round-trip is CUSTOM-FREE (a Decode-only custom
// on the writer would otherwise produce a Go-domain intermediate the
// re-encode cannot invert), AND that raw view still preserves tagged union
// branch identity. The reader's custom Decode fires only in the final
// resolving decode — asserted with a domain type distinguishable from every
// built-in decode result, so a pass cannot come from plain coercion.
func TestMatrix_ResolvedJSONTaggedUnionWriterDecodeOnlyCustom(t *testing.T) {
	type domainTS struct{ ms int64 }
	ct := avro.CustomType{
		LogicalType: "timestamp-millis", AvroType: "long", GoType: reflect.TypeFor[domainTS](),
		Decode: func(v any, _ *avro.SchemaNode) (any, error) { return domainTS{ms: v.(int64)}, nil },
	}
	schemaJSON := func(extra string) string {
		return `{"type":"record","name":"Top","fields":[` +
			`{"name":"u","type":[{"type":"enum","name":"E1","symbols":["A","B"]},{"type":"enum","name":"E2","symbols":["A","C"]}]},` +
			`{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}` + extra + `]}`
	}
	w := avro.MustParse(schemaJSON(``), ct)
	r := avro.MustParse(schemaJSON(`,{"name":"pad","type":"int","default":0}`), ct)
	resolved, err := avro.Resolve(w, r)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	wire, err := w.Encode(map[string]any{"u": map[string]any{"E2": "A"}, "ts": time.UnixMilli(1700000000000).UTC()})
	if err != nil {
		t.Fatalf("writer Encode: %v", err)
	}
	var binOut, jsonOut any
	if _, err := resolved.Decode(wire, &binOut, avro.TaggedUnions()); err != nil {
		t.Fatalf("resolved.Decode: %v", err)
	}
	if err := resolved.DecodeJSON([]byte(`{"u":{"E2":"A"},"ts":1700000000000}`), &jsonOut, avro.TaggedUnions()); err != nil {
		t.Fatalf("resolved.DecodeJSON: %v", err)
	}
	if !reflect.DeepEqual(jsonOut, binOut) {
		t.Errorf("resolved JSON decode != binary decode:\n  binary=%#v\n  json  =%#v", binOut, jsonOut)
	}
	m, ok := jsonOut.(map[string]any)
	if !ok {
		t.Fatalf("decoded top not a map: %#v", jsonOut)
	}
	if _, ok := m["ts"].(domainTS); !ok {
		t.Errorf("reader custom Decode did not fire (vacuous pass): ts=%#v", m["ts"])
	}
	env, ok := m["u"].(map[string]any)
	if !ok || len(env) != 1 {
		t.Fatalf("union field not enveloped: %#v", m["u"])
	}
	if _, ok := env["E2"]; !ok {
		t.Errorf("tagged branch rewritten: envelope=%#v, want key E2", env)
	}
}

// Calibrates representative resolved-JSON union cells against fastavro's
// json_reader with writer→reader migration (the "jsonread" oracle op with a
// reader schema): the branch named by the tagged envelope — not the value's
// first-match — is what resolution applies to. Skips when the fastavro
// oracle is unavailable.
func TestDifferentialFastavroResolvedJSONUnion(t *testing.T) {
	o := startOracle(t)

	// jsonNorm routes twmb's decoded value through encoding/json so it
	// compares against the oracle's JSON-decoded values (numbers become
	// float64 on both sides).
	jsonNorm := func(t *testing.T, v any) any {
		t.Helper()
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("normalize twmb value: %v", err)
		}
		var out any
		if err := json.Unmarshal(b, &out); err != nil {
			t.Fatalf("normalize twmb value: %v", err)
		}
		return out
	}

	topWriter := func(union string) string {
		return `{"type":"record","name":"Top","fields":[{"name":"u","type":` + union + `}]}`
	}
	topReader := func(union string) string {
		return `{"type":"record","name":"Top","fields":[{"name":"u","type":` + union + `},{"name":"pad","type":"int","default":0}]}`
	}
	cells := []struct {
		name   string
		writer string
		reader string
		json   string
	}{
		// The value-divergence headline: the writer names E2/"A"; the reader
		// E2 drops "A", so resolving the TRUE branch yields the reader enum
		// default "Y" (a flip to E1 would keep "A").
		{"top-level-two-enums-default-remap",
			`[{"type":"enum","name":"E1","symbols":["A"]},{"type":"enum","name":"E2","symbols":["A","Y"]}]`,
			`[{"type":"enum","name":"E1","symbols":["A"]},{"type":"enum","name":"E2","symbols":["Y"],"default":"Y"}]`,
			`{"E2":"A"}`},
		{"two-records-divergent",
			topWriter(`[{"type":"record","name":"R1","fields":[{"name":"f","type":"string"}]},{"type":"record","name":"R2","fields":[{"name":"f","type":"string"}]}]`),
			topReader(`[{"type":"record","name":"R1","fields":[{"name":"f","type":"string"}]},{"type":"record","name":"R2","fields":[{"name":"f","type":"string"},{"name":"g","type":"int","default":9}]}]`),
			`{"u":{"R2":{"f":"x"}}}`},
		{"map-vs-record-identical",
			topWriter(`[{"type":"map","values":"string"},{"type":"record","name":"R","fields":[{"name":"f","type":"string"}]}]`),
			topReader(`[{"type":"map","values":"string"},{"type":"record","name":"R","fields":[{"name":"f","type":"string"}]}]`),
			`{"u":{"R":{"f":"x"}}}`},
		{"union-of-map-envelope-shaped-value",
			topWriter(`["int",{"type":"map","values":"int"}]`),
			topReader(`["int",{"type":"map","values":"int"}]`),
			`{"u":{"map":{"int":3}}}`},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			resp := o.call(oracleJob{Op: "jsonread",
				Schema: json.RawMessage(c.writer),
				Reader: json.RawMessage(c.reader),
				JSON:   c.json,
			})
			if !resp.OK {
				t.Fatalf("fastavro json_reader+migration: %s", resp.Err)
			}
			if len(resp.Values) != 1 {
				t.Fatalf("fastavro returned %d values: %v", len(resp.Values), resp.Values)
			}
			w := avro.MustParse(c.writer)
			r := avro.MustParse(c.reader)
			resolved := mustResolve(t, w, r)
			var got any
			if err := resolved.DecodeJSON([]byte(c.json), &got); err != nil {
				t.Fatalf("resolved.DecodeJSON: %v", err)
			}
			if norm := jsonNorm(t, got); !reflect.DeepEqual(norm, resp.Values[0]) {
				t.Errorf("twmb resolved JSON decode != fastavro json_reader migration:\n  twmb     = %#v\n  fastavro = %#v", norm, resp.Values[0])
			}
		})
	}
}

// ---------- matrix_reuse_test.go ----------

// ---------------------------------------------------------------------------
// Target-reuse matrix: decoding into already-populated targets. The matrix
// core always decodes into fresh targets; the reuse semantics — map targets
// retain non-schema keys while schema fields overwrite (encoding/json
// parity), slices are rewritten to exactly the new length, pointers are
// reused — have their own documented contracts and historically their own
// bugs. Every fragment kind drives a decode-twice cycle on the same target,
// on both wire formats.
// ---------------------------------------------------------------------------

func TestMatrix_TargetReusePerKind(t *testing.T) {
	for _, fr := range matFrags() {
		if fr.kind == "null" {
			continue // a null field decodes to nil regardless of reuse
		}
		if len(fr.values) < 2 {
			continue // reuse needs two distinct values
		}
		t.Run(fr.label, func(t *testing.T) {
			u := &uniq{}
			schema := fmt.Sprintf(`{"type":"record","name":"RU","fields":[{"name":"f","type":%s}]}`, fr.schema(u))
			s := avro.MustParse(schema)
			w1, err := s.AppendEncode(nil, map[string]any{"f": fr.values[0]})
			if err != nil {
				t.Fatalf("encode v0: %v", err)
			}
			w2, err := s.AppendEncode(nil, map[string]any{"f": fr.values[1]})
			if err != nil {
				t.Fatalf("encode v1: %v", err)
			}
			var want1, want2 any
			if _, err := s.Decode(w1, &want1); err != nil {
				t.Fatalf("fresh decode v0: %v", err)
			}
			if _, err := s.Decode(w2, &want2); err != nil {
				t.Fatalf("fresh decode v1: %v", err)
			}

			// Sequential binary decodes into the SAME *any: second value
			// fully replaces the schema field; a pre-seeded non-schema key
			// is retained (the documented stale-key contract).
			var reused any = map[string]any{"stale": "keepme"}
			if _, err := s.Decode(w1, &reused); err != nil {
				t.Fatalf("reuse decode #1: %v", err)
			}
			if _, err := s.Decode(w2, &reused); err != nil {
				t.Fatalf("reuse decode #2: %v", err)
			}
			m := reused.(map[string]any)
			if m["stale"] != "keepme" {
				t.Fatalf("non-schema key dropped on reuse: %#v", m)
			}
			if !matEqual(m["f"], want2.(map[string]any)["f"]) {
				t.Fatalf("reused decode field stale:\n got=%#v\nwant=%#v", m["f"], want2.(map[string]any)["f"])
			}
			// The reused tree re-encodes onto w2's wire after dropping the
			// foreign key (schema-driven encode ignores extra keys).
			re, err := s.AppendEncode(nil, m)
			if err != nil || !bytes.Equal(re, w2) {
				t.Fatalf("reused tree re-encode: err=%v\n re=%x\n w2=%x", err, re, w2)
			}

			// JSON decode into the same pre-populated map behaves alike.
			j2, err := s.AppendEncodeJSON(nil, want2)
			if err != nil {
				t.Fatalf("encodeJSON: %v", err)
			}
			var jreused any = map[string]any{"stale": "keepme", "f": "overwrite-me"}
			if err := s.DecodeJSON(j2, &jreused); err != nil {
				t.Fatalf("JSON reuse decode: %v", err)
			}
			jm := jreused.(map[string]any)
			if jm["stale"] != "keepme" {
				t.Fatalf("JSON reuse dropped non-schema key: %#v", jm)
			}
			if !matEqual(jm["f"], want2.(map[string]any)["f"]) {
				t.Fatalf("JSON reuse field stale: %#v", jm["f"])
			}
		})
	}
}

// Typed-container reuse: slices shrink and grow to exactly the decoded
// length; map targets accumulate per the documented retain semantics;
// pointer chains are reused rather than reallocated where pinned.
func TestMatrix_TypedContainerReuse(t *testing.T) {
	t.Run("slice-shrinks-and-grows", func(t *testing.T) {
		s := avro.MustParse(`{"type":"array","items":"int"}`)
		big, _ := s.AppendEncode(nil, []int32{1, 2, 3, 4, 5})
		small, _ := s.AppendEncode(nil, []int32{9})
		var target []int32
		if _, err := s.Decode(big, &target); err != nil {
			t.Fatalf("decode big: %v", err)
		}
		if _, err := s.Decode(small, &target); err != nil {
			t.Fatalf("decode small into used slice: %v", err)
		}
		if len(target) != 1 || target[0] != 9 {
			t.Fatalf("slice reuse left stale elements: %v", target)
		}
		if _, err := s.Decode(big, &target); err != nil {
			t.Fatalf("decode big into shrunk slice: %v", err)
		}
		if len(target) != 5 || target[4] != 5 {
			t.Fatalf("slice regrow failed: %v", target)
		}
	})
	t.Run("slice-of-pointers", func(t *testing.T) {
		s := avro.MustParse(`{"type":"array","items":["null","int"]}`)
		w1, _ := s.AppendEncode(nil, []any{int32(1), nil, int32(3)})
		w2, _ := s.AppendEncode(nil, []any{nil, int32(7), nil})
		var target []*int32
		if _, err := s.Decode(w1, &target); err != nil {
			t.Fatalf("decode #1: %v", err)
		}
		if _, err := s.Decode(w2, &target); err != nil {
			t.Fatalf("decode #2 into used []*int32: %v", err)
		}
		if target[0] != nil || target[1] == nil || *target[1] != 7 || target[2] != nil {
			t.Fatalf("pointer-slice reuse wrong: %v", target)
		}
	})
	t.Run("typed-map-accumulates", func(t *testing.T) {
		s := avro.MustParse(`{"type":"map","values":"int"}`)
		w1, _ := s.AppendEncode(nil, map[string]int32{"a": 1})
		w2, _ := s.AppendEncode(nil, map[string]int32{"b": 2})
		var target map[string]int32
		if _, err := s.Decode(w1, &target); err != nil {
			t.Fatalf("decode #1: %v", err)
		}
		if _, err := s.Decode(w2, &target); err != nil {
			t.Fatalf("decode #2 into used map: %v", err)
		}
		// Documented retain semantics: existing keys persist, new keys add.
		if target["a"] != 1 || target["b"] != 2 {
			t.Fatalf("typed map reuse: %v", target)
		}
	})
	t.Run("struct-field-reuse", func(t *testing.T) {
		type R struct {
			N *int32 `avro:"n"`
			S string `avro:"s"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"n","type":["null","int"]},{"name":"s","type":"string"}]}`)
		w1, _ := s.AppendEncode(nil, map[string]any{"n": int32(5), "s": "one"})
		w2, _ := s.AppendEncode(nil, map[string]any{"n": nil, "s": "two"})
		var r R
		if _, err := s.Decode(w1, &r); err != nil {
			t.Fatalf("decode #1: %v", err)
		}
		if r.N == nil || *r.N != 5 {
			t.Fatalf("first decode: %+v", r)
		}
		if _, err := s.Decode(w2, &r); err != nil {
			t.Fatalf("decode #2: %v", err)
		}
		if r.N != nil || r.S != "two" {
			t.Fatalf("struct reuse stale: %+v ptr=%v", r, r.N)
		}
		if _, err := s.Decode(w1, &r); err != nil {
			t.Fatalf("decode #3: %v", err)
		}
		if r.N == nil || *r.N != 5 || r.S != "one" {
			t.Fatalf("struct re-reuse: %+v", r)
		}
	})
}

type reuseArrayElemP struct {
	A int64 `avro:"a"`
}

type reuseArrayElemFast struct {
	F []*reuseArrayElemP `avro:"f"`
}

// The embedded-pointer twin routes every field through the reflect slow path
// (computeFieldOffset declines fields reached through an embedded pointer),
// so a value of this type decodes via the reflect array path while a plain
// reuseArrayElemFast decodes via the unsafe struct fast path.
type reuseArrayElemReflect struct {
	*reuseArrayElemFast
}

// TestRegression_ArrayPointerElementReuseAcrossDecodePaths pins that decoding a
// []*P struct field into a reused target reuses the retained non-nil element
// pointers identically on the unsafe struct fast path and the reflect path —
// the documented pointer-reuse contract (matrix header: "pointers are
// reused"). The unsafe path batch-allocated backing only for nil slots and
// wrote through retained pointers; the reflect path unconditionally installed
// fresh backing, so an aliased element from a prior decode was updated in
// place on one arm and orphaned on the other.
func TestRegression_ArrayPointerElementReuseAcrossDecodePaths(t *testing.T) {
	const schema = `{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"array","items":{"type":"record","name":"P","fields":[{"name":"a","type":"long"}]}}}]}`
	s := avro.MustParse(schema)
	w1, err := s.Encode(map[string]any{"f": []any{map[string]any{"a": int64(1)}}})
	if err != nil {
		t.Fatalf("encode w1: %v", err)
	}
	w2, err := s.Encode(map[string]any{"f": []any{map[string]any{"a": int64(2)}}})
	if err != nil {
		t.Fatalf("encode w2: %v", err)
	}

	// Unsafe struct fast path: a directly addressable struct target.
	var fast reuseArrayElemFast
	if _, err := s.Decode(w1, &fast); err != nil {
		t.Fatalf("fast decode w1: %v", err)
	}
	fastKeep := fast.F[0]
	if _, err := s.Decode(w2, &fast); err != nil {
		t.Fatalf("fast decode w2: %v", err)
	}

	// Reflect path: same logical target through the embedded-pointer twin.
	refl := reuseArrayElemReflect{reuseArrayElemFast: &reuseArrayElemFast{}}
	if _, err := s.Decode(w1, &refl); err != nil {
		t.Fatalf("reflect decode w1: %v", err)
	}
	reflKeep := refl.F[0]
	if _, err := s.Decode(w2, &refl); err != nil {
		t.Fatalf("reflect decode w2: %v", err)
	}

	if fastKeep.A != reflKeep.A {
		t.Fatalf("arm divergence: retained pointer reads %d (unsafe fast path) vs %d (reflect path)", fastKeep.A, reflKeep.A)
	}
	// Both paths reuse the slot, so the retained alias observes the second
	// decode's value.
	if fastKeep.A != 2 {
		t.Fatalf("retained pointer should observe the reused decode (want 2); got %d", fastKeep.A)
	}
}

type reuseNullUnionFast struct {
	F []*int64 `avro:"f"`
}

type reuseNullUnionReflect struct {
	*reuseNullUnionFast
}

// Sibling of the record-element case: null-union array elements ([]*int64 over
// array<["null","long"]>) flow through the same pointer-element branch, so the
// unsafe path (udNullUnionEnter) and reflect path must reuse retained element
// pointers identically.
func TestRegression_ArrayNullUnionPointerElementReuseAcrossDecodePaths(t *testing.T) {
	const schema = `{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"array","items":["null","long"]}}]}`
	s := avro.MustParse(schema)
	one, two := int64(1), int64(2)
	w1, err := s.Encode(map[string]any{"f": []any{&one}})
	if err != nil {
		t.Fatalf("encode w1: %v", err)
	}
	w2, err := s.Encode(map[string]any{"f": []any{&two}})
	if err != nil {
		t.Fatalf("encode w2: %v", err)
	}

	var fast reuseNullUnionFast
	if _, err := s.Decode(w1, &fast); err != nil {
		t.Fatalf("fast decode w1: %v", err)
	}
	fastKeep := fast.F[0]
	if _, err := s.Decode(w2, &fast); err != nil {
		t.Fatalf("fast decode w2: %v", err)
	}

	refl := reuseNullUnionReflect{reuseNullUnionFast: &reuseNullUnionFast{}}
	if _, err := s.Decode(w1, &refl); err != nil {
		t.Fatalf("reflect decode w1: %v", err)
	}
	reflKeep := refl.F[0]
	if _, err := s.Decode(w2, &refl); err != nil {
		t.Fatalf("reflect decode w2: %v", err)
	}

	if fastKeep == nil || reflKeep == nil {
		t.Fatalf("retained element pointers must be non-nil (value branch): fast=%v reflect=%v", fastKeep, reflKeep)
	}
	if *fastKeep != *reflKeep {
		t.Fatalf("arm divergence: retained pointer reads %d (unsafe) vs %d (reflect)", *fastKeep, *reflKeep)
	}
	if *fastKeep != 2 {
		t.Fatalf("retained pointer should observe the reused decode (want 2); got %d", *fastKeep)
	}
}

// ---------- matrix_selfreadable_test.go ----------

// ---------------------------------------------------------------------------
// Generative self-readability net (the SCALE axis).
//
// The combinatorial matrix sweeps SHAPE at small scale (collections of size
// 0..4, small schemas). Every DoS cap, by contrast, lives at LARGE scale
// (maxZeroByteItems=4096, ocfMetadataSafetyLimit=1 MiB, decimalScaleLimit=
// 65536, errTooDeep=1000), so a small-value generator structurally never
// reaches it. That blind spot is where reader-side caps with no producer-side
// compliance hide — an encoder that emits wire its own decoder rejects (a
// silent self-incompatible round-trip).
//
// The invariant here is calibration-free and the exact inverse of that bug:
// for every value, if Encode SUCCEEDS, Decode of that wire MUST also succeed
// (encode-accepts ⟹ decode-accepts-own-output) — on BOTH wires. A clean
// encode-time rejection is always fine; the only forbidden outcome is a wire
// the producer emits and the consumer refuses. Each generator drives a
// degenerate shape ACROSS its cap boundary (cap-1, cap, cap+1, and well
// past).
// ---------------------------------------------------------------------------

func TestMatrix_SelfReadableAtScale(t *testing.T) {
	zeroByteItem := func(label string) (string, any) {
		switch label {
		case "null":
			return `"null"`, nil
		case "emptyrecord":
			return `{"type":"record","name":"E","fields":[]}`, map[string]any{}
		case "size0fixed":
			return `{"type":"fixed","name":"Z","size":0}`, []byte{}
		}
		panic(label)
	}

	type gen struct {
		label  string
		schema string
		value  func() any
	}
	var gens []gen

	// Zero-byte-item arrays across the maxZeroByteItems boundary.
	for _, item := range []string{"null", "emptyrecord", "size0fixed"} {
		itemSchema, itemVal := zeroByteItem(item)
		for _, n := range []int{4095, 4096, 4097, 10000} {
			gens = append(gens, gen{
				label:  fmt.Sprintf("array<%s>×%d", item, n),
				schema: fmt.Sprintf(`{"type":"array","items":%s}`, itemSchema),
				value: func() any {
					a := make([]any, n)
					for i := range a {
						a[i] = itemVal
					}
					return a
				},
			})
		}
	}

	// Maps of zero-byte values across the same boundary (finding-1 claims
	// maps are immune because a key is ≥1 byte; this proves it by sweep).
	for _, n := range []int{4096, 4097, 10000} {
		gens = append(gens, gen{
			label:  fmt.Sprintf("map<null>×%d", n),
			schema: `{"type":"map","values":"null"}`,
			value: func() any {
				m := make(map[string]any, n)
				for i := range n {
					m[fmt.Sprintf("k%d", i)] = nil
				}
				return m
			},
		})
	}

	// Large strings / bytes / fixed (single-value scale, not collection).
	for _, sz := range []int{1 << 20, 4 << 20} {
		gens = append(gens,
			gen{fmt.Sprintf("string@%d", sz), `"string"`, func() any { return strings.Repeat("x", sz) }},
			gen{fmt.Sprintf("bytes@%d", sz), `"bytes"`, func() any { return make([]byte, sz) }},
		)
	}

	// Decimal scale across decimalScaleLimit (65536): a *big.Rat whose
	// denominator forces a large scale.
	for _, scale := range []int{65535, 65536, 65537} {
		gens = append(gens, gen{
			label:  fmt.Sprintf("decimal@scale%d", scale),
			schema: fmt.Sprintf(`{"type":"bytes","logicalType":"decimal","precision":%d,"scale":%d}`, scale+2, scale),
			value: func() any {
				// 1 / 10^scale → needs `scale` fractional digits.
				den := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
				return new(big.Rat).SetFrac(big.NewInt(1), den)
			},
		})
	}

	// Deeply nested arrays around errTooDeep (1000).
	for _, depth := range []int{998, 1000, 1002} {
		schema := "\"long\""
		for range depth {
			schema = fmt.Sprintf(`{"type":"array","items":%s}`, schema)
		}
		d := depth
		gens = append(gens, gen{
			label:  fmt.Sprintf("nested-array@%d", depth),
			schema: schema,
			value: func() any {
				var v any = int64(1)
				for range d {
					v = []any{v}
				}
				return v
			},
		})
	}

	check := func(t *testing.T, label string, v any,
		enc func([]byte, any) ([]byte, error), dec func([]byte, any) error, wire string) {
		data, encErr := enc(nil, v)
		if encErr != nil {
			return // encode-time rejection is always acceptable
		}
		var sink any
		if decErr := dec(data, &sink); decErr != nil {
			t.Errorf("SELF-INCOMPATIBLE [%s wire]: %s — Encode produced %d bytes the decoder REJECTS: %v",
				wire, label, len(data), decErr)
		}
	}

	for _, g := range gens {
		t.Run(g.label, func(t *testing.T) {
			s, err := avro.Parse(g.schema)
			if err != nil {
				return // schema itself rejected at parse — fine
			}
			check(t, g.label, g.value(),
				func(b []byte, v any) ([]byte, error) { return s.AppendEncode(b, v) },
				func(b []byte, tgt any) error { _, e := s.Decode(b, tgt); return e }, "binary")
			check(t, g.label, g.value(),
				func(b []byte, v any) ([]byte, error) { return s.AppendEncodeJSON(b, v) },
				func(b []byte, tgt any) error { return s.DecodeJSON(b, tgt) }, "json")
		})
	}

	// Decimal UNSCALED-LENGTH axis (maxDecimalUnscaledBytes = 32 KiB), the
	// bound orthogonal to the scale generator above.
	//
	// This axis has to sweep the CARRIER, because the carrier is what decides
	// whether any upstream gate is reached at all — and the gates differ:
	//
	//   - a numeric carrier on "decimal" is bounded by the DECLARED PRECISION,
	//     itself parse-capped, so it cannot reach the length bound;
	//   - a numeric carrier on "big-decimal" is bounded by NOTHING, because
	//     that logical type has no precision attribute to declare;
	//   - the opaque []byte escape hatch is bounded by neither, on either.
	//
	// The fixed container is a third route again: it pads to the schema's SIZE
	// whatever the value, so the size alone decides the emitted width and every
	// carrier lands in the same place. A net that drove only *big.Rat on a
	// bytes/decimal would see the precision gate fire and conclude the bound
	// was unreachable from the producer side.
	//
	// The single-object and OCF wires are here because they re-frame the same
	// encoder output: an escape that reaches them ships a FILE whose reader
	// cannot open it, which is strictly worse than a rejected call.
	const unscaledCap = 32 << 10
	rawOf := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = 0x01
		}
		return b
	}
	// The opaque carrier's payload must be the shape whose UNSCALED part is n
	// bytes, and that shape differs per logical: on "decimal" the payload IS
	// the unscaled value, while on "big-decimal" it is a framing that WRAPS it.
	// Handing big-decimal n raw bytes would test the framing grammar instead —
	// 0x01 reads as a zigzag -1 and dies on the length before the bound is ever
	// consulted, so the cell would red for a reason that has nothing to do with
	// this axis and would never exercise it.
	bigDecFramingOf := func(n int) []byte {
		out := zigzagEncode64(int64(n))
		out = append(out, rawOf(n)...)
		return append(out, zigzagEncode64(0)...)
	}
	// ratOfLen returns a rational whose minimal two's-complement unscaled form
	// is exactly n bytes: 2^(8n-9) has bit length 8n-8, so its magnitude fills
	// n-1 bytes with the top bit set, and the sign byte makes it n.
	ratOfLen := func(n int) *big.Rat {
		return new(big.Rat).SetInt(new(big.Int).Lsh(big.NewInt(1), uint(8*n-9)))
	}

	type decCell struct {
		label  string
		schema string
		value  any
	}
	var decCells []decCell
	for _, n := range []int{unscaledCap - 1, unscaledCap, unscaledCap + 1} {
		bytesDec := `{"type":"bytes","logicalType":"decimal","precision":65536,"scale":0}`
		bigDec := `{"type":"bytes","logicalType":"big-decimal"}`
		fixedDec := fmt.Sprintf(`{"type":"fixed","name":"F","size":%d,"logicalType":"decimal","precision":65536,"scale":0}`, n)
		for _, c := range []struct {
			carrier string
			schema  string
			value   any
		}{
			{"rat", bytesDec, ratOfLen(n)},
			{"opaque", bytesDec, rawOf(n)},
			{"text", bytesDec, ratOfLen(n).RatString()},
			{"rat", bigDec, ratOfLen(n)},
			{"opaque", bigDec, bigDecFramingOf(n)},
			{"text", bigDec, ratOfLen(n).RatString()},
			{"rat", fixedDec, big.NewRat(5, 1)},
			{"opaque", fixedDec, rawOf(n)},
			{"text", fixedDec, "5"},
		} {
			logical := "decimal"
			if c.schema == bigDec {
				logical = "big-decimal"
			}
			container := "bytes"
			if c.schema == fixedDec {
				container = fmt.Sprintf("fixed%+d", n-unscaledCap)
			}
			base := fmt.Sprintf("%s/%s/%s@%+d", logical, container, c.carrier, n-unscaledCap)
			decCells = append(decCells, decCell{base, c.schema, c.value})
			// The same value delivered through an `any`-typed record field, so
			// the record dispatch is crossed too and not just the top level.
			decCells = append(decCells, decCell{
				label:  base + "/in-record",
				schema: fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":"d","type":%s}]}`, c.schema),
				value:  map[string]any{"d": c.value},
			})
		}
	}
	for _, g := range decCells {
		t.Run("decimal-unscaled-length/"+g.label, func(t *testing.T) {
			s, err := avro.Parse(g.schema)
			if err != nil {
				return // schema itself rejected at parse — fine
			}
			check(t, g.label, g.value,
				func(b []byte, v any) ([]byte, error) { return s.AppendEncode(b, v) },
				func(b []byte, tgt any) error { _, e := s.Decode(b, tgt); return e }, "binary")
			check(t, g.label, g.value,
				func(b []byte, v any) ([]byte, error) { return s.AppendEncodeJSON(b, v) },
				func(b []byte, tgt any) error { return s.DecodeJSON(b, tgt) }, "json")
			check(t, g.label, g.value,
				func(b []byte, v any) ([]byte, error) { return s.AppendSingleObject(b, v) },
				func(b []byte, tgt any) error { _, e := s.DecodeSingleObject(b, tgt); return e }, "single-object")
			checkOCF(t, g.label, s, g.value)
		})
	}

	// The DEFAULT fill is a distinct emit route to the same bytes, and it is
	// the one route where the caller never chose a carrier: a bytes/fixed
	// default is []byte by construction. It is also pre-encoded at PARSE, so
	// its verdict has to travel to encode rather than being raised where it is
	// computed — a schema whose default cannot be written must still parse,
	// because a reader that DROPS the field never writes it.
	//
	// Four fill routes reach it and they are not one path: an absent key in a
	// map[string]any, an absent key in a typed map, a struct field tagged
	// omitzero (reflect), and the same field through the COMPILED unsafe
	// record path, which copies the pre-encoded bytes at compile time and so
	// can emit what its reflect twin refuses if the verdict does not travel
	// with them.
	for _, n := range []int{unscaledCap - 1, unscaledCap, unscaledCap + 1} {
		for _, c := range []struct {
			label string
			inner string
		}{
			{"bytes/decimal", `{"type":"bytes","logicalType":"decimal","precision":65536,"scale":0}`},
			{"bytes/big-decimal", `{"type":"bytes","logicalType":"big-decimal"}`},
			{"fixed/decimal", fmt.Sprintf(`{"type":"fixed","name":"DF","size":%d,"logicalType":"decimal","precision":65536,"scale":0}`, n)},
		} {
			payload := rawOf(n)
			if c.label == "bytes/big-decimal" {
				payload = bigDecFramingOf(n)
			}
			schema := fmt.Sprintf(
				`{"type":"record","name":"R","fields":[{"name":"d","type":%s,"default":%s},{"name":"keep","type":"int"}]}`,
				c.inner, codepointLit(payload))
			label := fmt.Sprintf("default/%s@%+d", c.label, n-unscaledCap)
			t.Run("decimal-unscaled-length/"+label, func(t *testing.T) {
				s, err := avro.Parse(schema)
				if err != nil {
					t.Fatalf("a schema whose default cannot be WRITTEN must still PARSE: %v", err)
				}
				absent := map[string]any{"keep": int32(7)}
				check(t, label, absent,
					func(b []byte, v any) ([]byte, error) { return s.AppendEncode(b, v) },
					func(b []byte, tgt any) error { _, e := s.Decode(b, tgt); return e }, "binary")
				check(t, label, absent,
					func(b []byte, v any) ([]byte, error) { return s.AppendEncodeJSON(b, v) },
					func(b []byte, tgt any) error { return s.DecodeJSON(b, tgt) }, "json")
				check(t, label, absent,
					func(b []byte, v any) ([]byte, error) { return s.AppendSingleObject(b, v) },
					func(b []byte, tgt any) error { _, e := s.DecodeSingleObject(b, tgt); return e }, "single-object")
				checkOCF(t, label, s, absent)
				// omitzero, reflect and compiled-unsafe both: an addressable
				// struct pointer is what routes into the compiled path.
				if c.label != "fixed/decimal" {
					check(t, label+"/omitzero", &srOmitBytes{Keep: 7},
						func(b []byte, v any) ([]byte, error) { return s.AppendEncode(b, v) },
						func(b []byte, tgt any) error { _, e := s.Decode(b, tgt); return e }, "binary-omitzero")
				}
			})
		}
	}

	// UNSAFE struct-field path. The generators above pass top-level []any /
	// map[string]any values, which route through the REFLECT encoders. A
	// zero-byte array that is an addressable struct field instead routes
	// through the UNSAFE encoders (usArrayRecord / usArrayPtrRecord /
	// usArrayDirect) — a structurally distinct code path that the first
	// producer-compliance fix missed, and which this net was blind to until
	// it drove typed struct fields. Each wrapper holds the same zero-byte
	// array element type as a TYPED slice field, swept across the cap.
	for _, uc := range unsafeArrayCases() {
		for _, n := range []int{4096, 4097, 10000} {
			t.Run(fmt.Sprintf("unsafe-field/%s×%d", uc.label, n), func(t *testing.T) {
				s := avro.MustParse(uc.schema)
				ptr := uc.value(n) // &struct{ A []ElemT }{...}, addressable → unsafe path
				check(t, fmt.Sprintf("unsafe-field/%s×%d", uc.label, n), ptr,
					func(b []byte, v any) ([]byte, error) { return s.AppendEncode(b, v) },
					func(b []byte, tgt any) error { _, e := s.Decode(b, tgt); return e }, "binary")
			})
		}
	}
}

// checkOCF is the container-wire arm of the self-readability invariant: a
// value the OCF writer accepts must be one the OCF reader can read back. It is
// a separate closure because the container re-frames encoder output rather
// than being another (encode, decode) pair — a wire an encoder emits and a
// reader refuses becomes a FILE on disk here.
func checkOCF(t *testing.T, label string, s *avro.Schema, v any) {
	t.Helper()
	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, s)
	if err != nil {
		return // writer construction rejected — acceptable
	}
	if err := w.Encode(v); err != nil {
		return // encode-time rejection is always acceptable
	}
	if err := w.Close(); err != nil {
		return
	}
	size := buf.Len()
	r, err := ocf.NewReader(&buf)
	if err != nil {
		t.Errorf("SELF-INCOMPATIBLE [ocf wire]: %s — the writer produced a %d-byte file NewReader REJECTS: %v",
			label, size, err)
		return
	}
	defer r.Close()
	var sink any
	if err := r.Decode(&sink); err != nil {
		t.Errorf("SELF-INCOMPATIBLE [ocf wire]: %s — the writer produced a %d-byte file the reader REJECTS: %v",
			label, size, err)
	}
}

// srOmitBytes routes a zero-valued defaulted field through the omitzero arm,
// as an addressable struct pointer so the COMPILED unsafe record path is the
// one that fills the default.
type srOmitBytes struct {
	D    []byte `avro:"d,omitzero"`
	Keep int32  `avro:"keep"`
}

// codepointLit renders bytes as an Avro-JSON codepoint default literal using
// \u escapes, so the source carries no raw control bytes.
func codepointLit(b []byte) string {
	var sb strings.Builder
	sb.Grow(len(b)*6 + 2)
	sb.WriteByte('"')
	for _, c := range b {
		fmt.Fprintf(&sb, "\\u%04x", c)
	}
	sb.WriteByte('"')
	return sb.String()
}

// srEmptyRec maps to an empty record; the typed slices below force the unsafe
// array encoders (a []any would stay on the reflect path).
type srEmptyRec struct{}

func unsafeArrayCases() []struct {
	label  string
	schema string
	value  func(n int) any
} {
	const recField = `{"type":"record","name":"H","fields":[{"name":"a","type":{"type":"array","items":{"type":"record","name":"E","fields":[]}}}]}`
	const fixedField = `{"type":"record","name":"H","fields":[{"name":"a","type":{"type":"array","items":{"type":"fixed","name":"Z","size":0}}}]}`
	return []struct {
		label  string
		schema string
		value  func(n int) any
	}{
		{"slice-empty-record", recField, func(n int) any {
			return &struct {
				A []srEmptyRec `avro:"a"`
			}{A: make([]srEmptyRec, n)}
		}},
		{"slice-ptr-empty-record", recField, func(n int) any {
			a := make([]*srEmptyRec, n)
			for i := range a {
				a[i] = &srEmptyRec{}
			}
			return &struct {
				A []*srEmptyRec `avro:"a"`
			}{A: a}
		}},
		{"slice-size0-fixed", fixedField, func(n int) any {
			return &struct {
				A [][0]byte `avro:"a"`
			}{A: make([][0]byte, n)}
		}},
	}
}

// ---------- matrix_textinterface_test.go ----------

// ---------------------------------------------------------------------------
// Generative text-interface precedence net.
//
// Documented policy (BUG_AUDIT "Text interfaces take precedence over the
// reflect.String fast path"): a string-kind Go type implementing a text
// method encodes/decodes through that method, NOT its raw string value —
// uniformly across binary and JSON, scalar and container, addressable
// (unsafe) and not. The combinatorial matrix carries a text type with an
// IDENTITY MarshalText, so "text method used" produces the same bytes as
// "raw string" and the matrix cannot distinguish them: neutering the
// precedence (ser.go's textOutFor-before-reflect.String check) was caught
// ONLY by hand-written pins that use a TRANSFORMING method.
//
// This net uses TRANSFORMING text methods (a "T:" / "A:" marker prefix) so
// the wire is observably different from the raw string, and sweeps the
// precedence across {top, field, array, map-value, struct-field} positions
// and both wires. The discriminators: decoding the wire as a PLAIN string
// must show the MARSHALED form (the text method ran on encode); decoding
// into the text type must round-trip through UnmarshalText.
// ---------------------------------------------------------------------------

// markerMarshal is a string-kind type whose MarshalText TRANSFORMS (prefixes
// "T:"), so its Avro-string wire differs from its raw string value.
type markerMarshal string

func (m markerMarshal) MarshalText() ([]byte, error) { return []byte("T:" + string(m)), nil }
func (m *markerMarshal) UnmarshalText(b []byte) error {
	*m = markerMarshal(strings.TrimPrefix(string(b), "T:"))
	return nil
}

// markerAppend exercises the TextAppender path (textOutFor prefers AppendText
// over MarshalText for the alloc-free inline write — ser.go:986).
type markerAppend string

func (m markerAppend) AppendText(b []byte) ([]byte, error) {
	return append(append(b, "A:"...), m...), nil
}
func (m *markerAppend) UnmarshalText(b []byte) error {
	*m = markerAppend(bytes.TrimPrefix(b, []byte("A:")))
	return nil
}

// textPositions wrap a leaf value into a composed value at a position, with a
// matching "string"-schema composition and a way to (a) build a PLAIN-string
// decode target tree and (b) read the leaf string out of it.
var textPositions = []struct {
	label   string
	schema  string
	wrap    func(leaf any) any
	plainT  func() any // ptr to a tree with string leaves
	leafOf  func(decoded any) string
	typedT  func(t reflect.Type) reflect.Value
	leafTyp func(decoded reflect.Value) reflect.Value
}{
	{"top", `"string"`,
		func(leaf any) any { return leaf },
		func() any { return new(string) },
		func(d any) string { return *(d.(*string)) },
		func(t reflect.Type) reflect.Value { return reflect.New(t) },
		func(d reflect.Value) reflect.Value { return d.Elem() }},
	{"field", `{"type":"record","name":"TIR","fields":[{"name":"f","type":"string"}]}`,
		func(leaf any) any { return map[string]any{"f": leaf} },
		func() any { return &map[string]string{} },
		func(d any) string { return (*(d.(*map[string]string)))["f"] },
		func(t reflect.Type) reflect.Value {
			st := reflect.StructOf([]reflect.StructField{{Name: "F", Type: t, Tag: `avro:"f"`}})
			return reflect.New(st)
		},
		func(d reflect.Value) reflect.Value { return d.Elem().Field(0) }},
	{"array-item", `{"type":"array","items":"string"}`,
		func(leaf any) any { return []any{leaf} },
		func() any { return &[]string{} },
		func(d any) string { return (*(d.(*[]string)))[0] },
		func(t reflect.Type) reflect.Value { return reflect.New(reflect.SliceOf(t)) },
		func(d reflect.Value) reflect.Value { return d.Elem().Index(0) }},
	{"map-value", `{"type":"map","values":"string"}`,
		func(leaf any) any { return map[string]any{"k": leaf} },
		func() any { return &map[string]string{} },
		func(d any) string { return (*(d.(*map[string]string)))["k"] },
		func(t reflect.Type) reflect.Value {
			return reflect.New(reflect.MapOf(reflect.TypeFor[string](), t))
		},
		func(d reflect.Value) reflect.Value {
			return d.Elem().MapIndex(reflect.ValueOf("k"))
		}},
}

func TestMatrix_TextInterfacePrecedence(t *testing.T) {
	cases := []struct {
		label  string
		typ    reflect.Type
		value  func() any // a value of typ holding "hello"
		marked string     // the marshaled wire form of "hello"
		raw    string     // the raw string value
	}{
		{"MarshalText", reflect.TypeFor[markerMarshal](),
			func() any { return markerMarshal("hello") }, "T:hello", "hello"},
		{"AppendText", reflect.TypeFor[markerAppend](),
			func() any { return markerAppend("hello") }, "A:hello", "hello"},
	}

	for _, c := range cases {
		for _, pos := range textPositions {
			t.Run(c.label+"/"+pos.label, func(t *testing.T) {
				s := avro.MustParse(pos.schema)

				// Build the source value at this position. For struct-field,
				// pass a POINTER so the addressable unsafe encode path is hit.
				var src any
				if pos.label == "field" {
					st := reflect.StructOf([]reflect.StructField{{Name: "F", Type: c.typ, Tag: `avro:"f"`}})
					p := reflect.New(st)
					p.Elem().Field(0).Set(reflect.ValueOf(c.value()))
					src = p.Interface()
				} else {
					src = pos.wrap(c.value())
				}

				for _, enc := range []struct {
					name   string
					encode func(any) ([]byte, error)
					decode func([]byte, any) error
				}{
					{"binary",
						func(v any) ([]byte, error) { return s.AppendEncode(nil, v) },
						func(b []byte, tgt any) error { _, err := s.Decode(b, tgt); return err }},
					{"json",
						func(v any) ([]byte, error) { return s.AppendEncodeJSON(nil, v) },
						func(b []byte, tgt any) error { return s.DecodeJSON(b, tgt) }},
				} {
					wire, err := enc.encode(src)
					if err != nil {
						t.Fatalf("%s encode: %v", enc.name, err)
					}
					// Discriminator 1: decode the wire as PLAIN strings. The
					// leaf must be the MARSHALED form — proving the text
					// method ran on encode, not the raw string fast path.
					pt := pos.plainT()
					if err := enc.decode(wire, pt); err != nil {
						t.Fatalf("%s plain decode: %v", enc.name, err)
					}
					if got := pos.leafOf(pt); got != c.marked {
						t.Fatalf("%s: text method BYPASSED on encode — wire leaf %q, want marshaled %q (raw would be %q)",
							enc.name, got, c.marked, c.raw)
					}
					// Discriminator 2: decode into the text TYPE — must
					// round-trip through UnmarshalText back to the raw value.
					tt := pos.typedT(c.typ)
					if err := enc.decode(wire, tt.Interface()); err != nil {
						t.Fatalf("%s typed decode: %v", enc.name, err)
					}
					if got := pos.leafTyp(tt).String(); got != c.raw {
						t.Fatalf("%s: UnmarshalText not applied on decode — got %q, want %q", enc.name, got, c.raw)
					}
				}
			})
		}
	}
}

// The [16]byte uuid "trusts raw bytes" exception: a [16]byte-shaped Go type
// carrying a uuid logical type uses its RAW BYTES, NOT a text method, on both
// wires — the 16 bytes ARE the UUID, and consulting MarshalText would let a
// non-canonical text method diverge binary from JSON. A transforming text
// method on the array type must therefore be IGNORED.
type markerUUID [16]byte

func (m markerUUID) MarshalText() ([]byte, error) { return []byte("IGNORED"), nil }

// TestMatrix_UUIDByteArrayTrustsRawBytes crosses the uuid logical's CARRIER
// with the Go spelling of the value. The carrier axis previously had one
// value, a size-16 fixed, and the two carriers do not share an encoder: a
// fixed uuid rides as opaque bytes on both wires, while a string uuid is
// text, and the JSON string encoder holds its own [16]byte-to-canonical-text
// conversion that the fixed carrier never reaches. Pinning the carrier left
// that conversion unrun.
//
// The invariant is SOURCE-INDEPENDENCE: within one carrier and one wire, a
// [16]byte and the canonical hex-dash string naming the same UUID must
// encode to the same bytes. The oracle is that agreement, not a spelled-out
// expectation, so neither spelling can be checked against a restatement of
// its own encoder. The carriers legitimately differ from each other, which
// is why the comparison is within a carrier and not across.
//
// markerUUID carries a MarshalText that returns "IGNORED", so a carrier that
// reached for the text interface instead of the raw bytes is caught rather
// than merely producing something plausible.
func TestMatrix_UUIDByteArrayTrustsRawBytes(t *testing.T) {
	const canonical = "550e8400-e29b-41d4-a716-446655440000"
	raw := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	marker := markerUUID(raw)

	carriers := []struct {
		name   string
		schema string
		// jsonIsText records whether this carrier's JSON form is the
		// canonical hex-dash text. A fixed uuid is opaque bytes on the
		// JSON wire too, so only the string carrier is text there.
		jsonIsText bool
	}{
		{"fixed16", `{"type":"fixed","name":"TUU","size":16,"logicalType":"uuid"}`, false},
		{"string", `{"type":"string","logicalType":"uuid"}`, true},
	}
	sources := []struct {
		name string
		v    any
	}{
		{"array", raw},
		{"named-array-with-marshaltext", marker},
		{"canonical-string", canonical},
	}

	// Liveness floor: every carrier must have been compared on every wire,
	// or a carrier that started erroring would leave its encoder unasserted.
	compared := 0

	for _, c := range carriers {
		s := avro.MustParse(c.schema)
		for _, wire := range []struct {
			name   string
			encode func(any) ([]byte, error)
		}{
			{"binary", func(x any) ([]byte, error) { return s.AppendEncode(nil, x) }},
			{"json", func(x any) ([]byte, error) { return s.AppendEncodeJSON(nil, x) }},
		} {
			t.Run(c.name+"/"+wire.name, func(t *testing.T) {
				var want []byte
				for i, src := range sources {
					got, err := wire.encode(src.v)
					if err != nil {
						t.Fatalf("%s encode: %v", src.name, err)
					}
					if bytes.Contains(got, []byte("IGNORED")) {
						t.Fatalf("%s used MarshalText instead of trusting the raw bytes: %x", src.name, got)
					}
					if i == 0 {
						want = got
						continue
					}
					if !bytes.Equal(got, want) {
						t.Fatalf("%s encodes differently from the plain [16]byte:\n got  %s\n want %s", src.name, got, want)
					}
				}
				if c.jsonIsText && wire.name == "json" {
					if string(want) != `"`+canonical+`"` {
						t.Fatalf("string-carrier JSON is not the canonical text: %s", want)
					}
				}
				// The raw bytes survive a round trip on this carrier.
				bin, err := s.AppendEncode(nil, raw)
				if err != nil {
					t.Fatalf("binary encode for round trip: %v", err)
				}
				var out markerUUID
				mustDecode(t, s, bin, &out)
				if [16]byte(out) != raw {
					t.Fatalf("uuid bytes not preserved: %x vs %x", out, raw)
				}
				compared++
			})
		}
	}
	if want := len(carriers) * 2; compared != want {
		t.Errorf("%d of %d carrier/wire pairs were compared; a carrier stopped encoding", compared, want)
	}
}

// ---------- matrix_typed_test.go ----------

// ---------------------------------------------------------------------------
// Typed-struct matrix: every fragment kind as a struct field (the unsafe
// fast path on addressable targets), in single-field, surrounded, pointered,
// and container positions. The struct types are assembled at runtime with
// reflect.StructOf; the invariant is the typed paths agree byte-for-byte
// with the generic path on both wire formats, addressable or not.
// ---------------------------------------------------------------------------

type typedFrag struct {
	label   string
	schema  string // field schema
	goType  reflect.Type
	value   any // assignable to goType
	generic any // the map[string]any-form equivalent for the generic path
}

func typedFrags() []typedFrag {
	rat := big.NewRat(123, 4)
	ts := time.Date(2024, 6, 1, 12, 34, 56, 789000000, time.UTC)
	return []typedFrag{
		{"boolean", `"boolean"`, reflect.TypeOf(true), true, true},
		{"int", `"int"`, reflect.TypeOf(int32(0)), int32(-5), int32(-5)},
		{"int-as-int16", `"int"`, reflect.TypeOf(int16(0)), int16(300), int32(300)},
		{"long", `"long"`, reflect.TypeOf(int64(0)), int64(1 << 40), int64(1 << 40)},
		{"long-as-uint32", `"long"`, reflect.TypeOf(uint32(0)), uint32(4000000000), int64(4000000000)},
		{"float", `"float"`, reflect.TypeOf(float32(0)), float32(2.5), float32(2.5)},
		{"double", `"double"`, reflect.TypeOf(float64(0)), 6.25, 6.25},
		{"string", `"string"`, reflect.TypeOf(""), "typ", "typ"},
		{"bytes", `"bytes"`, reflect.TypeOf([]byte(nil)), []byte{9, 8}, []byte{9, 8}},
		{"bytes-empty", `"bytes"`, reflect.TypeOf([]byte(nil)), []byte{}, []byte{}},
		{"enum", `{"type":"enum","name":"TYE","symbols":["A","B"]}`, reflect.TypeOf(""), "B", "B"},
		{"fixed2", `{"type":"fixed","name":"TYF","size":2}`, reflect.TypeOf([2]byte{}), [2]byte{1, 2}, []byte{1, 2}},
		{"fixed0", `{"type":"fixed","name":"TYF0","size":0}`, reflect.TypeOf([0]byte{}), [0]byte{}, []byte{}},
		{"uuid-fixed16", `{"type":"fixed","name":"TYU","size":16,"logicalType":"uuid"}`,
			reflect.TypeOf([16]byte{}), [16]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8},
			"6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{"date", `{"type":"int","logicalType":"date"}`, reflect.TypeOf(time.Time{}),
			time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, reflect.TypeOf(time.Duration(0)),
			3 * time.Hour, 3 * time.Hour},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, reflect.TypeOf(time.Time{}), ts, ts},
		{"decimal", `{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}`,
			reflect.TypeOf(&big.Rat{}), rat, rat},
		{"duration", `{"type":"fixed","name":"TYD","size":12,"logicalType":"duration"}`,
			reflect.TypeOf(avro.Duration{}), avro.Duration{Months: 3, Days: 1, Milliseconds: 9}, avro.Duration{Months: 3, Days: 1, Milliseconds: 9}},
		{"array-int", `{"type":"array","items":"int"}`, reflect.TypeOf([]int32(nil)), []int32{4, 5}, []any{int32(4), int32(5)}},
		{"map-long", `{"type":"map","values":"long"}`, reflect.TypeOf(map[string]int64(nil)),
			map[string]int64{"k": 11}, map[string]any{"k": int64(11)}},
		{"nullunion-ptr", `["null","int"]`, reflect.TypeOf((*int32)(nil)),
			func() *int32 { v := int32(8); return &v }(), int32(8)},
		{"nullunion-ptr-nil", `["null","int"]`, reflect.TypeOf((*int32)(nil)), (*int32)(nil), nil},
		{"nullsecond-ptr", `["int","null"]`, reflect.TypeOf((*int32)(nil)),
			func() *int32 { v := int32(9); return &v }(), int32(9)},
		{"nested-record", `{"type":"record","name":"TYN","fields":[{"name":"i","type":"int"}]}`,
			reflect.TypeOf(struct {
				I int32 `avro:"i"`
			}{}),
			struct {
				I int32 `avro:"i"`
			}{I: 3},
			map[string]any{"i": int32(3)}},
	}
}

// typedPositions wrap a (fieldSchema, goType) into a struct schema + type.
type typedPosition struct {
	label  string
	schema func(fieldSchema string) string
	build  func(ft reflect.Type) reflect.Type
	set    func(target reflect.Value, fv reflect.Value) // place value into struct
	wrapG  func(g any) any                              // generic-form wrapper
}

func typedPositions() []typedPosition {
	return []typedPosition{
		{"solo",
			func(fs string) string {
				return fmt.Sprintf(`{"type":"record","name":"S","fields":[{"name":"f","type":%s}]}`, fs)
			},
			func(ft reflect.Type) reflect.Type {
				return reflect.StructOf([]reflect.StructField{
					{Name: "F", Type: ft, Tag: `avro:"f"`},
				})
			},
			func(target, fv reflect.Value) { target.Field(0).Set(fv) },
			func(g any) any { return map[string]any{"f": g} }},
		{"surrounded",
			func(fs string) string {
				return fmt.Sprintf(`{"type":"record","name":"S","fields":[{"name":"a","type":"long"},{"name":"f","type":%s},{"name":"z","type":"string"}]}`, fs)
			},
			func(ft reflect.Type) reflect.Type {
				return reflect.StructOf([]reflect.StructField{
					{Name: "A", Type: reflect.TypeOf(int64(0)), Tag: `avro:"a"`},
					{Name: "F", Type: ft, Tag: `avro:"f"`},
					{Name: "Z", Type: reflect.TypeOf(""), Tag: `avro:"z"`},
				})
			},
			func(target, fv reflect.Value) {
				target.Field(0).SetInt(42)
				target.Field(1).Set(fv)
				target.Field(2).SetString("zz")
			},
			func(g any) any { return map[string]any{"a": int64(42), "f": g, "z": "zz"} }},
		{"pointered-field",
			func(fs string) string {
				return fmt.Sprintf(`{"type":"record","name":"S","fields":[{"name":"f","type":%s}]}`, fs)
			},
			func(ft reflect.Type) reflect.Type {
				return reflect.StructOf([]reflect.StructField{
					{Name: "F", Type: reflect.PointerTo(ft), Tag: `avro:"f"`},
				})
			},
			func(target, fv reflect.Value) {
				p := reflect.New(fv.Type())
				p.Elem().Set(fv)
				target.Field(0).Set(p)
			},
			func(g any) any { return map[string]any{"f": g} }},
		{"slice-of-struct",
			func(fs string) string {
				return fmt.Sprintf(`{"type":"array","items":{"type":"record","name":"S","fields":[{"name":"f","type":%s}]}}`, fs)
			},
			func(ft reflect.Type) reflect.Type {
				return reflect.SliceOf(reflect.StructOf([]reflect.StructField{
					{Name: "F", Type: ft, Tag: `avro:"f"`},
				}))
			},
			func(target, fv reflect.Value) {
				elem := reflect.New(target.Type().Elem()).Elem()
				elem.Field(0).Set(fv)
				target.Set(reflect.Append(target.Slice(0, 0), elem, elem))
			},
			func(g any) any {
				return []any{map[string]any{"f": g}, map[string]any{"f": g}}
			}},
	}
}

func TestMatrix_TypedStructFields(t *testing.T) {
	for _, fr := range typedFrags() {
		for _, pos := range typedPositions() {
			// Pointer-to-pointer fields for union-pointer fragments are a
			// **T shape; valid but the pointered position re-wraps unions:
			// ["null","int"] as **int32 is fine, keep it.
			t.Run(fr.label+"/"+pos.label, func(t *testing.T) {
				schema := pos.schema(fr.schema)
				s, err := avro.Parse(schema)
				if err != nil {
					t.Fatalf("Parse: %v\nschema: %s", err, schema)
				}
				st := pos.build(fr.goType)
				targetP := reflect.New(st) // addressable
				if pos.label == "slice-of-struct" {
					targetP.Elem().Set(reflect.MakeSlice(st, 0, 2))
				}
				pos.set(targetP.Elem(), reflect.ValueOf(fr.value))

				// Encode: addressable (pointer) and non-addressable (value).
				wPtr, err := s.AppendEncode(nil, targetP.Interface())
				if err != nil {
					t.Fatalf("typed encode (ptr): %v", err)
				}
				wVal, err := s.AppendEncode(nil, targetP.Elem().Interface())
				if err != nil {
					t.Fatalf("typed encode (val): %v", err)
				}
				if !bytes.Equal(wPtr, wVal) {
					t.Fatalf("addressable vs non-addressable differ:\n p=%x\n v=%x", wPtr, wVal)
				}
				// Generic path must produce the same wire.
				wGen, err := s.AppendEncode(nil, pos.wrapG(fr.generic))
				if err != nil {
					t.Fatalf("generic encode: %v", err)
				}
				if !bytes.Equal(wGen, wPtr) {
					t.Fatalf("typed vs generic wire differ:\n t=%x\n g=%x\nschema: %s", wPtr, wGen, schema)
				}
				// Typed decode lands the same value; re-encode is stable.
				back := reflect.New(st)
				if pos.label == "slice-of-struct" {
					back.Elem().Set(reflect.MakeSlice(st, 0, 2))
				}
				if _, err := s.Decode(wPtr, back.Interface()); err != nil {
					t.Fatalf("typed decode: %v", err)
				}
				wBack, err := s.AppendEncode(nil, back.Interface())
				if err != nil || !bytes.Equal(wBack, wPtr) {
					t.Fatalf("typed decode→re-encode differs: err=%v\n w=%x\n b=%x", err, wPtr, wBack)
				}
				// JSON twins.
				jTyped, err := s.AppendEncodeJSON(nil, targetP.Interface())
				if err != nil {
					t.Fatalf("typed encodeJSON: %v", err)
				}
				jGen, err := s.AppendEncodeJSON(nil, pos.wrapG(fr.generic))
				if err != nil || !bytes.Equal(jTyped, jGen) {
					t.Fatalf("typed vs generic JSON differ: err=%v\n t=%s\n g=%s", err, jTyped, jGen)
				}
				jBack := reflect.New(st)
				if pos.label == "slice-of-struct" {
					jBack.Elem().Set(reflect.MakeSlice(st, 0, 2))
				}
				if err := s.DecodeJSON(jTyped, jBack.Interface()); err != nil {
					t.Fatalf("typed decodeJSON: %v", err)
				}
				wj, err := s.AppendEncode(nil, jBack.Interface())
				if err != nil || !bytes.Equal(wj, wPtr) {
					t.Fatalf("typed JSON round-trip wire differs: err=%v\n w=%x\n j=%x", err, wPtr, wj)
				}
			})
		}
	}
}

// Promotion swept across every level-1 composition context: writer int
// inner, reader long inner, decoded through the resolving schema; the
// promoted tree must re-encode cleanly against the reader.
func TestMatrix_PromotionInEveryContext(t *testing.T) {
	for _, cx := range matCtxs() {
		if cx.skip != nil && cx.skip("int") {
			continue
		}
		t.Run(cx.label, func(t *testing.T) {
			uw, ur := &uniq{}, &uniq{}
			wSchema := cx.schema(`"int"`, "int", uw)
			rSchema := cx.schema(`"long"`, "long", ur)
			w := avro.MustParse(wSchema)
			r := avro.MustParse(rSchema)
			res, err := avro.Resolve(w, r)
			if err != nil {
				t.Fatalf("Resolve: %v\nw: %s\nr: %s", err, wSchema, rSchema)
			}
			vin := cx.wrap(int32(-77))
			wire, err := w.AppendEncode(nil, vin)
			if err != nil {
				t.Fatalf("writer encode: %v", err)
			}
			var got any
			if _, err := res.Decode(wire, &got); err != nil {
				t.Fatalf("resolved decode: %v\nw: %s\nr: %s", err, wSchema, rSchema)
			}
			// The promoted value re-encodes against the reader, and equals
			// the reader's own encoding of the promoted input.
			wantWire, err := r.AppendEncode(nil, cx.wrap(int64(-77)))
			if err != nil {
				t.Fatalf("reader encode: %v", err)
			}
			gotWire, err := r.AppendEncode(nil, got)
			if err != nil {
				t.Fatalf("re-encode promoted: %v\ngot: %#v", err, got)
			}
			if !bytes.Equal(gotWire, wantWire) {
				t.Fatalf("promoted value wire differs:\n got=%x\nwant=%x\ngot value: %#v", gotWire, wantWire, got)
			}
		})
	}
}

// textWrap is a string-kind type with text methods: the documented
// precedence says these win over the raw-string fast path uniformly.
type textWrap string

func (w textWrap) MarshalText() ([]byte, error)  { return []byte(w), nil }
func (w *textWrap) UnmarshalText(b []byte) error { *w = textWrap(b); return nil }

// typedExtraFrags are target types with their own documented contracts:
// json.Number numeric carriers (raw wire content, logical formatting
// bypassed) and TextMarshaler string-kind types.
func typedExtraFrags() []typedFrag {
	return []typedFrag{
		{"int-jsonNumber", `"int"`, reflect.TypeOf(json.Number("")),
			json.Number("42"), int32(42)},
		{"long-jsonNumber", `"long"`, reflect.TypeOf(json.Number("")),
			json.Number("9007199254740993"), int64(9007199254740993)},
		{"double-jsonNumber", `"double"`, reflect.TypeOf(json.Number("")),
			json.Number("1.5"), 1.5},
		{"timestamp-jsonNumber", `{"type":"long","logicalType":"timestamp-millis"}`,
			reflect.TypeOf(json.Number("")),
			json.Number("1717243496789"), time.UnixMilli(1717243496789).UTC()},
		{"string-textWrap", `"string"`, reflect.TypeOf(textWrap("")),
			textWrap("tw"), "tw"},
		{"enum-textWrap-name-match", `{"type":"enum","name":"TWE","symbols":["A","B"]}`,
			reflect.TypeOf(textWrap("")), textWrap("B"), "B"},
	}
}

func TestMatrix_TypedExtraFragments(t *testing.T) {
	for _, fr := range typedExtraFrags() {
		for _, pos := range typedPositions() {
			t.Run(fr.label+"/"+pos.label, func(t *testing.T) {
				runTypedCell(t, fr, pos)
			})
		}
	}
}

// runTypedCell factors the cell body of TestMatrix_TypedStructFields so the
// extra fragments run the identical battery.
func runTypedCell(t *testing.T, fr typedFrag, pos typedPosition) {
	t.Helper()
	schema := pos.schema(fr.schema)
	s, err := avro.Parse(schema)
	if err != nil {
		t.Fatalf("Parse: %v\nschema: %s", err, schema)
	}
	st := pos.build(fr.goType)
	targetP := reflect.New(st)
	if pos.label == "slice-of-struct" {
		targetP.Elem().Set(reflect.MakeSlice(st, 0, 2))
	}
	pos.set(targetP.Elem(), reflect.ValueOf(fr.value))

	wPtr, err := s.AppendEncode(nil, targetP.Interface())
	if err != nil {
		t.Fatalf("typed encode (ptr): %v", err)
	}
	wVal, err := s.AppendEncode(nil, targetP.Elem().Interface())
	if err != nil || !bytes.Equal(wPtr, wVal) {
		t.Fatalf("addressable vs non-addressable: err=%v\n p=%x\n v=%x", err, wPtr, wVal)
	}
	wGen, err := s.AppendEncode(nil, pos.wrapG(fr.generic))
	if err != nil || !bytes.Equal(wGen, wPtr) {
		t.Fatalf("typed vs generic wire: err=%v\n t=%x\n g=%x", err, wPtr, wGen)
	}
	back := reflect.New(st)
	if pos.label == "slice-of-struct" {
		back.Elem().Set(reflect.MakeSlice(st, 0, 2))
	}
	if _, err := s.Decode(wPtr, back.Interface()); err != nil {
		t.Fatalf("typed decode: %v", err)
	}
	wBack, err := s.AppendEncode(nil, back.Interface())
	if err != nil || !bytes.Equal(wBack, wPtr) {
		t.Fatalf("typed decode→re-encode: err=%v\n w=%x\n b=%x", err, wPtr, wBack)
	}
	jTyped, err := s.AppendEncodeJSON(nil, targetP.Interface())
	if err != nil {
		t.Fatalf("typed encodeJSON: %v", err)
	}
	jGen, err := s.AppendEncodeJSON(nil, pos.wrapG(fr.generic))
	if err != nil || !bytes.Equal(jTyped, jGen) {
		t.Fatalf("typed vs generic JSON: err=%v\n t=%s\n g=%s", err, jTyped, jGen)
	}
	jBack := reflect.New(st)
	if pos.label == "slice-of-struct" {
		jBack.Elem().Set(reflect.MakeSlice(st, 0, 2))
	}
	if err := s.DecodeJSON(jTyped, jBack.Interface()); err != nil {
		t.Fatalf("typed decodeJSON: %v", err)
	}
	wj, err := s.AppendEncode(nil, jBack.Interface())
	if err != nil || !bytes.Equal(wj, wPtr) {
		t.Fatalf("typed JSON round-trip wire: err=%v\n w=%x\n j=%x", err, wPtr, wj)
	}
}

// Typed containers per fragment: []T and map[string]T for EVERY typed
// fragment — the per-element fast-path gates (fastPathSafeForElem and the
// unsafe loops) dispatch per element type, and historically each new
// slow-path-only type-class missed a gate.
func TestMatrix_TypedContainersPerFragment(t *testing.T) {
	all := append(typedFrags(), typedExtraFrags()...)
	for _, fr := range all {
		t.Run(fr.label, func(t *testing.T) {
			// ---- []T ----
			arrSchema := avro.MustParse(fmt.Sprintf(`{"type":"array","items":%s}`, fr.schema))
			slice := reflect.MakeSlice(reflect.SliceOf(fr.goType), 0, 2)
			slice = reflect.Append(slice, reflect.ValueOf(fr.value), reflect.ValueOf(fr.value))
			wTyped, err := arrSchema.AppendEncode(nil, slice.Interface())
			if err != nil {
				t.Fatalf("typed slice encode: %v", err)
			}
			wGen, err := arrSchema.AppendEncode(nil, []any{fr.generic, fr.generic})
			if err != nil || !bytes.Equal(wTyped, wGen) {
				t.Fatalf("typed slice vs generic wire: err=%v\n t=%x\n g=%x", err, wTyped, wGen)
			}
			backP := reflect.New(reflect.SliceOf(fr.goType))
			if _, err := arrSchema.Decode(wTyped, backP.Interface()); err != nil {
				t.Fatalf("typed slice decode: %v", err)
			}
			wBack, err := arrSchema.AppendEncode(nil, backP.Interface())
			if err != nil || !bytes.Equal(wBack, wTyped) {
				t.Fatalf("typed slice re-encode: err=%v\n w=%x\n b=%x", err, wTyped, wBack)
			}
			jTyped, err := arrSchema.AppendEncodeJSON(nil, slice.Interface())
			if err != nil {
				t.Fatalf("typed slice encodeJSON: %v", err)
			}
			jBackP := reflect.New(reflect.SliceOf(fr.goType))
			if err := arrSchema.DecodeJSON(jTyped, jBackP.Interface()); err != nil {
				t.Fatalf("typed slice decodeJSON: %v", err)
			}
			wj, err := arrSchema.AppendEncode(nil, jBackP.Interface())
			if err != nil || !bytes.Equal(wj, wTyped) {
				t.Fatalf("typed slice JSON round-trip: err=%v", err)
			}

			// ---- map[string]T ----
			mapSchema := avro.MustParse(fmt.Sprintf(`{"type":"map","values":%s}`, fr.schema))
			mt := reflect.MapOf(reflect.TypeOf(""), fr.goType)
			m := reflect.MakeMap(mt)
			m.SetMapIndex(reflect.ValueOf("k"), reflect.ValueOf(fr.value))
			wmTyped, err := mapSchema.AppendEncode(nil, m.Interface())
			if err != nil {
				t.Fatalf("typed map encode: %v", err)
			}
			wmGen, err := mapSchema.AppendEncode(nil, map[string]any{"k": fr.generic})
			if err != nil || !bytes.Equal(wmTyped, wmGen) {
				t.Fatalf("typed map vs generic wire: err=%v\n t=%x\n g=%x", err, wmTyped, wmGen)
			}
			mBackP := reflect.New(mt)
			if _, err := mapSchema.Decode(wmTyped, mBackP.Interface()); err != nil {
				t.Fatalf("typed map decode: %v", err)
			}
			wmBack, err := mapSchema.AppendEncode(nil, mBackP.Interface())
			if err != nil || !bytes.Equal(wmBack, wmTyped) {
				t.Fatalf("typed map re-encode: err=%v", err)
			}
		})
	}
}

// ---------- magnitude_arithmetic_matrix_test.go ----------

// ---------------------------------------------------------------------------
// Integer arithmetic over a schema-declared magnitude.
//
// A `fixed` size is an integer the schema text names outright, and the parser
// deliberately leaves its upper bound open (schema.go's fixed arm: the lenient
// majority, matching fastavro and avro-rs, since a size past the datum simply
// fails at encode/decode). That makes it the one parse-time magnitude whose
// VALUE is not bounded by the length of the text declaring it: nineteen
// characters name 2^63. Every other schema-text quantity is bounded either by
// a parse cap (precision and scale, at decimalScaleLimit) or by the input
// length itself (field, branch and symbol counts each cost bytes to write).
//
// So any arithmetic that can carry such a magnitude has to say what happens at
// the top of the range. The failure this file pins is not the magnitude itself
// but a SUM over it: a per-item wire minimum is accumulated field by field, and
// an overflow guard that tests only `s >= MaxInt32` lets a wrapped-negative sum
// through, because a negative number is not greater than a positive one. The
// sum then reaches a divisor.
//
// The pins below are behavioral. The producer invariant and the source-derived
// site registry at the bottom are what keep the class closed rather than the
// instance.
// ---------------------------------------------------------------------------

// The three shapes a derived per-item minimum can take when the arithmetic
// carrying it has no ceiling. Named for what the ARITHMETIC does, not for the
// schema, because the schema is only the vehicle.
const (
	// sumWrapsToZero: 1 (the long) + MaxInt64 wraps to MinInt64, and
	// MinInt64 + MaxInt64 lands on exactly -1. A map's per-entry minimum is
	// 1 + that, i.e. zero, and the block bound divides by it.
	sumWrapsToZero = `{"type":"record","name":"WZ","fields":[
		{"name":"lead","type":"long"},
		{"name":"a","type":` + `{"type":"fixed","name":"WZA","size":9223372036854775807}` + `},
		{"name":"b","type":` + `{"type":"fixed","name":"WZB","size":9223372036854775807}` + `}]}`

	// sumWrapsNegative: the union contributes 1, then one MaxInt64 field
	// carries the sum to MinInt64 and it stays there.
	sumWrapsNegative = `{"type":"record","name":"WN","fields":[
		{"name":"u","type":[` + `{"type":"fixed","name":"WNU","size":9223372036854775807}` + `]},
		{"name":"a","type":` + `{"type":"fixed","name":"WNA","size":9223372036854775807}` + `}]}`

	// magnitudeAlone: no sum at all — the caller's own `1 + minimum` is what
	// wraps, which is why a ceiling inside the producer is the fix and a
	// guard at one consumer is not.
	magnitudeAlone = `{"type":"fixed","name":"MA","size":9223372036854775807}`
)

// wrapShapes are the value schemas whose derived minimum the arithmetic must
// survive. Every one of them describes a datum that cannot physically exist —
// a single value would need 2^63 bytes — so the ONLY correct outcome for a
// non-empty block is an error, on every container and every entry point.
var wrapShapes = []struct{ name, schema string }{
	{"sum-wraps-to-zero", sumWrapsToZero},
	{"sum-wraps-negative", sumWrapsNegative},
	{"magnitude-alone", magnitudeAlone},
}

// containers are the two block-framed walkers; each derives a per-element
// minimum from the element schema and bounds the block count against it.
var containers = []struct {
	name string
	wrap func(values string) string
}{
	{"map", func(v string) string { return `{"type":"map","values":` + v + `}` }},
	{"array", func(v string) string { return `{"type":"array","items":` + v + `}` }},
}

// nonEmptyBlock is a single block header claiming one element, with nothing
// after it. One element of any wrapShape needs at least 2^63 bytes and there
// are zero, so a correct decoder rejects it. emptyContainer is the terminator
// alone, which every one of these schemas can legitimately represent.
var (
	nonEmptyBlock  = []byte{0x02}
	emptyContainer = []byte{0x00}
)

// magDecode runs a decode and converts a panic into an error, so one bad
// cell reports as a failure instead of tearing down the whole matrix run and
// hiding every cell after it.
func magDecode(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("PANIC: %v", r)
		}
	}()
	return fn()
}

func magIsPanic(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "PANIC:")
}

// ocfFileWith frames a minimal container file around schema, carrying one
// block whose payload is body. The header schema of an OCF is supplied BY THE
// FILE, so this is the reachability that matters: a reader that never saw the
// schema before still has to derive its bounds from it.
func ocfFileWith(schema string, body []byte) []byte {
	var f []byte
	f = append(f, 'O', 'b', 'j', 1)
	f = binary.AppendVarint(f, 1)
	f = binary.AppendVarint(f, int64(len("avro.schema")))
	f = append(f, "avro.schema"...)
	f = binary.AppendVarint(f, int64(len(schema)))
	f = append(f, schema...)
	f = append(f, 0)
	var sync [16]byte
	f = append(f, sync[:]...)
	f = binary.AppendVarint(f, 1)
	f = binary.AppendVarint(f, int64(len(body)))
	f = append(f, body...)
	f = append(f, sync[:]...)
	return f
}

// magnitudeEntryPoints are the ways a caller reaches a derived per-element
// bound. They differ in WHICH copy of the derivation runs: the parse-time one,
// the resolver's rebuilt one, the skip built for a dropped writer field, and
// the one a container reader derives from a schema it read out of a file.
// A ceiling applied at one of them leaves the other three open.
var magnitudeEntryPoints = []struct {
	name string
	// decode returns the error for a container schema carrying `body`.
	decode func(t *testing.T, containerSchema string, body []byte) error
}{
	{
		name: "Parse+Decode",
		decode: func(t *testing.T, cs string, body []byte) error {
			s := mustParse(t, cs)
			return magDecode(func() error {
				var v any
				_, err := s.Decode(body, &v)
				return err
			})
		},
	},
	{
		name: "Resolve+Decode",
		decode: func(t *testing.T, cs string, body []byte) error {
			// Reader and writer are the same schema, so resolution takes its
			// own rebuild path rather than the canonical-equal shortcut only
			// when something differs; wrapping both in a record with an added
			// reader field forces the rebuild.
			w, err := avro.Parse(`{"type":"record","name":"RW","fields":[{"name":"c","type":` + cs + `}]}`)
			if err != nil {
				t.Fatalf("parse writer: %v", err)
			}
			r, err := avro.Parse(`{"type":"record","name":"RW","fields":[{"name":"c","type":` + cs + `},{"name":"added","type":"int","default":7}]}`)
			if err != nil {
				t.Fatalf("parse reader: %v", err)
			}
			res, err := avro.Resolve(w, r)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			return magDecode(func() error {
				var v any
				_, err := res.Decode(body, &v)
				return err
			})
		},
	},
	{
		name: "Resolve-drop+skip",
		decode: func(t *testing.T, cs string, body []byte) error {
			// The reader omits the container field, so resolution compiles a
			// SKIP for it — a second derivation of the same bound, in the walk
			// that advances past a value instead of decoding it.
			w, err := avro.Parse(`{"type":"record","name":"RD","fields":[{"name":"c","type":` + cs + `},{"name":"keep","type":"int"}]}`)
			if err != nil {
				t.Fatalf("parse writer: %v", err)
			}
			r, err := avro.Parse(`{"type":"record","name":"RD","fields":[{"name":"keep","type":"int"}]}`)
			if err != nil {
				t.Fatalf("parse reader: %v", err)
			}
			res, err := avro.Resolve(w, r)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			return magDecode(func() error {
				var v any
				_, err := res.Decode(body, &v)
				return err
			})
		},
	},
	{
		name: "ocf.NewReader",
		decode: func(t *testing.T, cs string, body []byte) error {
			return magDecode(func() error {
				rd, err := ocf.NewReader(bytes.NewReader(ocfFileWith(cs, body)))
				if err != nil {
					return err
				}
				var v any
				return rd.Decode(&v)
			})
		},
	},
}

// TestMatrix_SchemaMagnitudeArithmetic crosses wrap shape x container x entry
// point. The expectation comes from the datum, not from the code: an element
// of any wrapShape needs more bytes than the wire can hold, so a block
// claiming one must ERROR; the same schema with an empty container is
// perfectly representable and must DECODE. Neither expectation is read off
// current behavior, and a panic fails both.
func TestMatrix_SchemaMagnitudeArithmetic(t *testing.T) {
	for _, ws := range wrapShapes {
		for _, c := range containers {
			cs := c.wrap(ws.schema)
			for _, ep := range magnitudeEntryPoints {
				name := ws.name + "/" + c.name + "/" + ep.name
				t.Run(name, func(t *testing.T) {
					// A block claiming an element that cannot fit.
					err := ep.decode(t, cs, magFramed(ep.name, nonEmptyBlock))
					switch {
					case magIsPanic(err):
						t.Errorf("a block claiming one impossible element panicked instead of erroring: %v", err)
					case err == nil:
						t.Errorf("a block claiming an element needing 2^63 bytes was accepted with an empty remainder")
					}
					// The same schema, empty container: representable, must work.
					err = ep.decode(t, cs, magFramed(ep.name, emptyContainer))
					if magIsPanic(err) {
						t.Errorf("an EMPTY container panicked: %v", err)
					}
				})
			}
		}
	}
}

// magFramed prepends whatever the entry point's outer record needs before the
// container's own bytes. The record-wrapping entry points put the container
// first, so nothing is needed ahead of it; the dropped-field case still has to
// leave the trailing int readable, which a rejected block never reaches.
func magFramed(entryPoint string, body []byte) []byte {
	if entryPoint == "Resolve-drop+skip" {
		return append(append([]byte{}, body...), 0x02)
	}
	return body
}

// TestRegression_MapBlockBoundSurvivesWrappedMinimum is the instance pin: the
// smallest input that reached the divisor. Kept alongside the matrix because
// it names the exact arithmetic, and a matrix cell that stops driving this
// shape would otherwise take the pin with it.
func TestRegression_MapBlockBoundSurvivesWrappedMinimum(t *testing.T) {
	s := mustParse(t, `{"type":"map","values":`+sumWrapsToZero+`}`)
	err := magDecode(func() error {
		var v any
		_, err := s.Decode(nonEmptyBlock, &v)
		return err
	})
	if magIsPanic(err) {
		t.Fatalf("decoding a one-byte map block: %v", err)
	}
	if err == nil {
		t.Fatal("a map block claiming an entry that needs 2^63 bytes was accepted with an empty remainder")
	}
}

// TestInvariant_LegitimateBlockBoundsStillAccept is the control the ceiling
// must not break. A bound made safe by rejecting more is not safe, it is
// broken in the other direction, and every assertion above is satisfied by an
// implementation that refuses everything.
func TestInvariant_LegitimateBlockBoundsStillAccept(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		value  any
	}{
		{"map-of-int", `{"type":"map","values":"int"}`, map[string]any{"k": int32(1), "j": int32(2)}},
		{"array-of-int", `{"type":"array","items":"int"}`, []any{int32(1), int32(2), int32(3)}},
		{"array-of-null", `{"type":"array","items":"null"}`, []any{nil, nil, nil}},
		{"map-of-fixed", `{"type":"map","values":{"type":"fixed","name":"SF","size":4}}`,
			map[string]any{"k": []byte{1, 2, 3, 4}}},
		{"array-of-record", `{"type":"array","items":{"type":"record","name":"AR","fields":[{"name":"x","type":"int"},{"name":"y","type":"string"}]}}`,
			[]any{map[string]any{"x": int32(1), "y": "a"}}},
		{"map-of-large-fixed", `{"type":"map","values":{"type":"fixed","name":"LF","size":70000}}`,
			map[string]any{"k": bytes.Repeat([]byte{7}, 70000)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := mustParse(t, c.schema)
			b := mustEncode(t, s, c.value)
			var got any
			if _, err := s.Decode(b, &got); err != nil {
				t.Fatalf("the bound refused a block this schema's own encoder produced: %v", err)
			}
		})
	}
}
