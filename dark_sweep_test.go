package avro_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// ---------- dark_sweep_test.go ----------
//
// Cells for code paths the rest of the suite executes with NOTHING — derived
// from a coverage census that unions every Test, net and fuzz seed and then
// subtracts. A block reached by nothing is the only place a behavioral defect
// can hide, so each cell drives an input that lands on one and checks the result
// against an oracle OUTSIDE this package: encoding/json, big.Int arithmetic for
// the timestamp bounds, the binary auto-fill for the metadata default surface,
// and json.Valid for the strict skipper.
//
// The axes decide which arm runs, not which value: carrier shape, container
// shape, nesting parity, and wire path. Parity is what makes the recursion
// guards reachable at all — each schema level costs one depth unit, so which
// node sits on the limit depth is decided by its distance from the root.

// ---------------------------------------------------------------------------
// Timestamp scaling: the int64 overflow boundary on both sides.
//
// timeToTimestampScaled has three guards — the negative-second adjustment
// branch's floor, that branch's residual-underflow check, and the positive
// side's sub-second-carry check. Which one fires is decided by the sign of
// the second and by how much room the scale leaves at the extreme, so the
// cell crosses (unit) x (side) and computes the answer independently with
// math/big.
// ---------------------------------------------------------------------------

func TestMatrix_TimestampScaledOverflowBoundaries(t *testing.T) {
	t.Parallel()

	units := []struct {
		logical  string
		scale    int64 // ticks per second
		subScale int64 // nanoseconds per tick
		local    bool  // wall-clock fields are re-anchored at UTC first
	}{
		{"timestamp-millis", 1e3, 1e6, false},
		{"timestamp-micros", 1e6, 1e3, false},
		{"timestamp-nanos", 1e9, 1, false},
		{"local-timestamp-millis", 1e3, 1e6, true},
		{"local-timestamp-micros", 1e6, 1e3, true},
		{"local-timestamp-nanos", 1e9, 1, true},
	}

	// The local-* logicals encode WALL CLOCK, not an instant: the Go time's
	// calendar fields are re-read as if they were UTC before scaling, so the
	// oracle has to make the same move or it compares two different instants.
	asEncoded := func(local bool, t time.Time) time.Time {
		if !local {
			return t
		}
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(),
			t.Second(), t.Nanosecond(), time.UTC)
	}

	// Oracle: exact arithmetic, no int64 anywhere. The wire value for a
	// time is sec*scale + nsec/subScale (integer division), and the encode
	// must succeed exactly when that lands in [MinInt64, MaxInt64].
	oracle := func(sec, nsec, scale, subScale int64) (*big.Int, bool) {
		want := new(big.Int).Mul(big.NewInt(sec), big.NewInt(scale))
		want.Add(want, big.NewInt(nsec/subScale))
		min := new(big.Int).SetInt64(math.MinInt64)
		max := new(big.Int).SetInt64(math.MaxInt64)
		return want, want.Cmp(min) >= 0 && want.Cmp(max) <= 0
	}

	for _, u := range units {
		t.Run(u.logical, func(t *testing.T) {
			t.Parallel()
			s := mustParse(t, fmt.Sprintf(`{"type":"long","logicalType":%q}`, u.logical))
			maxSec := int64(math.MaxInt64) / u.scale

			// Every side of both guards. The nanosecond is chosen so the
			// sub-second term is non-zero (that is what routes the negative
			// case into the adjustment branch at all) and so the positive
			// case straddles the MaxInt64 remainder.
			cells := []struct {
				name string
				sec  int64
				nsec int64
			}{
				{"neg/below-floor", -maxSec - 2, 1},
				{"neg/at-floor", -maxSec - 1, 1},
				{"neg/at-floor-carrying", -maxSec - 1, u.subScale * (u.scale - 1)},
				{"neg/inside", -maxSec, 1},
				{"pos/inside", maxSec - 1, 0},
				{"pos/at-max-no-sub", maxSec, 0},
				{"pos/at-max-sub-fits", maxSec, (math.MaxInt64 - maxSec*u.scale) * u.subScale},
				{"pos/at-max-sub-overflows", maxSec, (math.MaxInt64 - maxSec*u.scale + 1) * u.subScale},
				{"pos/above-max", maxSec + 1, 0},
			}
			for _, c := range cells {
				if c.nsec < 0 || c.nsec >= 1e9 {
					continue // not expressible as a time.Time nanosecond
				}
				tv := time.Unix(c.sec, c.nsec)
				// time.Unix normalizes; re-read what it actually holds so the
				// oracle sees the same input the encoder does.
				enc := asEncoded(u.local, tv)
				gotSec, gotNsec := enc.Unix(), int64(enc.Nanosecond())
				want, wantOK := oracle(gotSec, gotNsec, u.scale, u.subScale)

				b, err := s.Encode(tv)
				if wantOK != (err == nil) {
					t.Fatalf("%s sec=%d nsec=%d: encode err=%v, exact value %s fits=%v",
						c.name, gotSec, gotNsec, err, want, wantOK)
				}
				if !wantOK {
					if !strings.Contains(err.Error(), "overflows int64") {
						t.Fatalf("%s: want overflow error, got %v", c.name, err)
					}
					continue
				}
				var back time.Time
				if _, err := s.Decode(b, &back); err != nil {
					t.Fatalf("%s: decode: %v", c.name, err)
				}
				// The decoded time must carry the same tick count the oracle
				// computed; compare in the wire domain, not the Go domain,
				// because a Time far outside the monotonic range still
				// round-trips its tick value.
				var raw int64
				rawSchema, err := avro.Parse(`"long"`)
				if err != nil {
					t.Fatalf("parse long: %v", err)
				}
				if _, err := rawSchema.Decode(b, &raw); err != nil {
					t.Fatalf("%s: raw decode: %v", c.name, err)
				}
				if want.Cmp(big.NewInt(raw)) != 0 {
					t.Fatalf("%s sec=%d nsec=%d: wire %d, exact %s", c.name, gotSec, gotNsec, raw, want)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Schema-tree values: the render's canonicalization must agree with
// encoding/json on every carrier shape.
//
// SchemaNode.Props holds any Go value, and SchemaNode.Schema() re-emits it.
// The render canonicalizes the shapes that have a marshal-identical canonical
// twin and leaves the rest opaque, and separately charges each value against
// a byte budget. Both walks branch on carrier shape, so the axes are
// (carrier) x (position: top-level, or nested under a value that forces the
// fixup walk to recurse). The oracle is encoding/json itself.
// ---------------------------------------------------------------------------

type darkPtrMarshaler struct{ V int }

func (m *darkPtrMarshaler) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"pm%d"`, m.V)), nil
}

type darkSepMarshaler struct{}

// Emits the two JSON line/paragraph separators, which encoding/json's
// compactor escapes from three bytes to six.
func (darkSepMarshaler) MarshalJSON() ([]byte, error) {
	return []byte("\"a\u2028b\u2029c\""), nil
}

type darkFailKey int

func (darkFailKey) MarshalText() ([]byte, error) { return nil, errors.New("darkFailKey: no") }

type darkOKKey int

func (k darkOKKey) MarshalText() ([]byte, error) { return []byte(fmt.Sprint(int(k))), nil }

// A string-KIND key with a MarshalText that would fail: encoding/json resolves
// string-kind keys by their raw string and never consults MarshalText, so this
// must marshal fine on both sides.
type darkFailStringKey string

func (darkFailStringKey) MarshalText() ([]byte, error) {
	return nil, errors.New("darkFailStringKey: no")
}

type darkUnexported struct {
	Exported   int
	unexported int //nolint:unused // presence is the point: json.Marshal skips it
}

type darkNamedMap map[string]any

type darkNamedFloats []float64

type darkNamedBytes []byte

func TestMatrix_SchemaTreeValueRenderMatchesEncodingJSON(t *testing.T) {
	t.Parallel()

	carriers := []struct {
		name string
		v    any
	}{
		// Elements whose marshal is reachable only through the addressable
		// slot: boxing them into []any would change the output, so the
		// container has to stay opaque.
		{"ptr-receiver-marshaler-slice", []darkPtrMarshaler{{1}, {2}}},
		{"ptr-receiver-marshaler-array", [2]darkPtrMarshaler{{1}, {2}}},
		{"named-byte-slice", darkNamedBytes{1, 2, 3}},
		{"named-map", darkNamedMap{"k": "v"}},
		{"named-float-slice", darkNamedFloats{1, 2}},
		{"text-key-map", map[darkOKKey]int{1: 2}},
		{"string-kind-key-with-failing-text", map[darkFailStringKey]int{"k": 1}},
		{"unexported-field-struct", darkUnexported{Exported: 1}},
		{"separator-marshaler", darkSepMarshaler{}},
		{"pointer-to-slice", &[]any{1.0}},
		{"nil-pointer", (*int)(nil)},
		{"int-array", [2]int{1, 2}},
		{"float32-plain", float32(1.5)},
		{"float64-plain", 1.5},
	}

	// Position axis: alone, or as a sibling of a value that forces the walk
	// to rebuild the whole container (a +Inf needs the numeric fixup, so
	// every sibling is revisited through the by-kind conversion arms rather
	// than passed through). path says where to find the carrier again in the
	// re-emitted tree, so the comparison stays about the carrier alone —
	// the trigger itself has no encoding/json image to compare against.
	positions := []struct {
		name string
		wrap func(v any) any
		path []string
	}{
		{"top-level", func(v any) any { return v }, nil},
		{"beside-fixup", func(v any) any {
			return map[string]any{"inf": math.Inf(1), "v": v}
		}, []string{"v"}},
		{"nested-twice", func(v any) any {
			return map[string]any{"outer": map[string]any{"inf": math.Inf(1), "v": v}}
		}, []string{"outer", "v"}},
	}

	for _, c := range carriers {
		for _, pos := range positions {
			t.Run(c.name+"/"+pos.name, func(t *testing.T) {
				t.Parallel()
				s, err := avro.Parse(`{"type":"record","name":"R","fields":[]}`)
				if err != nil {
					t.Fatalf("parse: %v", err)
				}
				n := s.Root()
				if n.Props == nil {
					n.Props = map[string]any{}
				}
				val := pos.wrap(c.v)
				n.Props["p"] = val

				// Oracle: what encoding/json makes of the CARRIER — not of
				// the wrapper, whose +Inf trigger encoding/json refuses by
				// design. The one documented substitution is the byte slice,
				// which Avro renders as its codepoint string rather than
				// base64.
				oracleBytes, oracleErr := json.Marshal(c.v)

				out, err := n.Schema()
				if oracleErr != nil {
					if err == nil {
						t.Fatalf("json.Marshal rejects %T (%v) but the render accepted it", val, oracleErr)
					}
					if !strings.Contains(err.Error(), oracleErr.Error()) {
						t.Fatalf("render error %v does not carry the json.Marshal cause %v", err, oracleErr)
					}
					return
				}
				if err != nil {
					t.Fatalf("render: %v (json.Marshal was fine: %s)", err, oracleBytes)
				}

				// UseNumber: the render emits 1e1000 for the +Inf trigger,
				// which overflows float64 on a plain Unmarshal.
				var got map[string]any
				dec := json.NewDecoder(strings.NewReader(out.String()))
				dec.UseNumber()
				if err := dec.Decode(&got); err != nil {
					t.Fatalf("re-emitted schema is not JSON: %v (%s)", err, out.String())
				}
				gotProp, ok := got["p"]
				if !ok {
					t.Fatalf("re-emitted schema dropped the prop: %s", out.String())
				}
				for _, step := range pos.path {
					m, ok := gotProp.(map[string]any)
					if !ok {
						t.Fatalf("re-emitted prop is not an object at %q: %#v", step, gotProp)
					}
					if gotProp, ok = m[step]; !ok {
						t.Fatalf("re-emitted prop lost key %q: %s", step, out.String())
					}
				}
				gotBytes, _ := json.Marshal(gotProp)

				var wantAny any
				odec := json.NewDecoder(bytes.NewReader(oracleBytes))
				odec.UseNumber()
				if err := odec.Decode(&wantAny); err != nil {
					t.Fatalf("oracle output is not JSON: %v", err)
				}
				want := darkSubstituteAvroImages(wantAny, c.v)
				wantBytes, _ := json.Marshal(want)
				if string(gotBytes) != string(wantBytes) {
					t.Fatalf("prop image mismatch\n got: %s\nwant: %s", gotBytes, wantBytes)
				}
			})
		}
	}
}

// darkSubstituteAvroImages applies the two documented differences between the
// re-emitted schema and a plain json.Marshal: +Inf/-Inf render as the
// 1e1000 number forms (json.Marshal refuses them outright, so they only reach
// here from a sibling position where the outer container marshaled), and a
// byte slice renders as Avro's codepoint-per-byte string rather than base64.
func darkSubstituteAvroImages(want any, orig any) any {
	switch w := want.(type) {
	case map[string]any:
		om, _ := orig.(map[string]any)
		out := make(map[string]any, len(w))
		for k, v := range w {
			var ov any
			if om != nil {
				ov = om[k]
			}
			out[k] = darkSubstituteAvroImages(v, ov)
		}
		return out
	case []any:
		return w
	case string:
		if b, ok := darkAsByteSlice(orig); ok {
			r := make([]rune, len(b))
			for i, c := range b {
				r[i] = rune(c)
			}
			return string(r)
		}
		return w
	case float64:
		if math.IsInf(w, 0) {
			return w
		}
		return w
	}
	if f, ok := orig.(float64); ok && math.IsInf(f, 0) {
		return json.Number(map[bool]string{true: "1e1000", false: "-1e1000"}[f > 0])
	}
	return want
}

func darkAsByteSlice(v any) ([]byte, bool) {
	if v == nil {
		return nil, false
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice || rv.Type().Elem().Kind() != reflect.Uint8 {
		return nil, false
	}
	b := make([]byte, rv.Len())
	for i := range b {
		b[i] = byte(rv.Index(i).Uint())
	}
	return b, true
}

// Non-finite and signed-zero floats have no encoding/json image at all —
// Marshal refuses +Inf/-Inf outright and renders -0.0 with integer syntax
// that re-parses sign-less. The render substitutes the JSON number forms Avro
// uses instead, so the oracle here is the ROUND TRIP: re-parsing the emitted
// schema has to give back a schema whose canonical bytes and prop image are
// stable, and the emitted literal has to be the documented one.
func TestMatrix_SchemaTreeNonFiniteFloatImages(t *testing.T) {
	t.Parallel()

	cells := []struct {
		name string
		v    any
		want string
	}{
		{"float64-pos-inf", math.Inf(1), "1e1000"},
		{"float64-neg-inf", math.Inf(-1), "-1e1000"},
		{"float64-neg-zero", math.Copysign(0, -1), "-0.0"},
		{"float32-pos-inf", float32(math.Inf(1)), "1e1000"},
		{"float32-neg-inf", float32(math.Inf(-1)), "-1e1000"},
		{"float32-neg-zero", float32(math.Copysign(0, -1)), "-0.0"},
	}
	positions := []struct {
		name string
		wrap func(v any) any
		path []string
	}{
		{"scalar", func(v any) any { return v }, nil},
		{"in-map", func(v any) any { return map[string]any{"v": v} }, []string{"v"}},
		{"in-slice", func(v any) any { return []any{v} }, []string{"0"}},
	}

	for _, c := range cells {
		for _, pos := range positions {
			t.Run(c.name+"/"+pos.name, func(t *testing.T) {
				t.Parallel()
				// json.Marshal has no image for these; that is the reason the
				// render substitutes one.
				if _, err := json.Marshal(c.v); err == nil && strings.Contains(c.want, "e1000") {
					t.Fatalf("oracle precondition broken: json.Marshal accepted %v", c.v)
				}
				s, err := avro.Parse(`{"type":"record","name":"R","fields":[]}`)
				if err != nil {
					t.Fatalf("parse: %v", err)
				}
				n := s.Root()
				if n.Props == nil {
					n.Props = map[string]any{}
				}
				n.Props["p"] = pos.wrap(c.v)
				out, err := n.Schema()
				if err != nil {
					t.Fatalf("render: %v", err)
				}
				emitted := out.String()
				if !strings.Contains(emitted, c.want) {
					t.Fatalf("emitted %s, want it to carry %s", emitted, c.want)
				}
				// Re-parsing and re-emitting is idempotent: the substituted
				// literal survives a full round trip rather than degrading.
				again, err := out.Root().Schema()
				if err != nil {
					t.Fatalf("re-render: %v", err)
				}
				if again.String() != emitted {
					t.Fatalf("round trip changed the tree:\n got %s\nwant %s", again.String(), emitted)
				}
				if string(again.Canonical()) != string(out.Canonical()) {
					t.Fatalf("round trip changed the canonical form")
				}
			})
		}
	}
}

// A key type that json.Marshal itself rejects must be reported with
// json.Marshal's own cause rather than silently charged or dropped.
func TestMatrix_SchemaTreeMapKeyMarshalFailureSurfacesJSONCause(t *testing.T) {
	t.Parallel()
	s := mustParse(t, `{"type":"record","name":"R","fields":[]}`)
	n := s.Root()
	if n.Props == nil {
		n.Props = map[string]any{}
	}
	v := map[darkFailKey]int{1: 2}
	n.Props["p"] = v

	_, oracleErr := json.Marshal(v)
	if oracleErr == nil {
		t.Fatal("oracle precondition broken: json.Marshal accepted a failing MarshalText key")
	}
	if _, err := n.Schema(); err == nil {
		t.Fatal("render accepted a value json.Marshal rejects")
	} else if !strings.Contains(err.Error(), "nope") && !strings.Contains(err.Error(), "darkFailKey") {
		t.Fatalf("render error %v does not carry the MarshalText cause %v", err, oracleErr)
	}
}

// ---------------------------------------------------------------------------
// Union defaults: the metadata branch selector against the wire auto-fill.
//
// Root().Fields[i].Default reports the default after choosing a union branch;
// the binary decoder fills the same field from the same default when the
// writer omits it. The two selectors are separate implementations, so the
// cell crosses (branch kind) x (default shape) and drives BOTH, requiring
// that they name the same branch.
// ---------------------------------------------------------------------------

func TestMatrix_UnionDefaultMetadataSelectionMatchesWireFill(t *testing.T) {
	t.Parallel()

	// Each cell is a two-branch union with a default that either matches the
	// first branch or does not, so selection is observable.
	cells := []struct {
		name    string
		branch  string // first branch, written into the union
		def     string // JSON default
		wantGo  string // Go type Root().Default must carry
		wantVal any    // and its value, when comparable
	}{
		{"null-first-null-default", `"null"`, `null`, "<nil>", nil},
		{"boolean-match", `"boolean"`, `true`, "bool", true},
		{"boolean-miss", `"boolean"`, `1`, "int32", int32(1)},
		{"enum-match", `{"type":"enum","name":"E2","symbols":["A","B"]}`, `"B"`, "string", "B"},
		{"enum-miss-nonstring", `{"type":"enum","name":"E2","symbols":["A"]}`, `1`, "int32", int32(1)},
		{"enum-miss-nonmember", `{"type":"enum","name":"E2","symbols":["A"]}`, `"Z"`, "", nil},
		{"record-match", `{"type":"record","name":"Inner","fields":[]}`, `{}`, "map[string]interface {}", nil},
		{"record-miss-nonmap", `{"type":"record","name":"Inner","fields":[]}`, `1`, "int32", int32(1)},
		{"record-defaulted-field-absent", `{"type":"record","name":"Inner","fields":[{"name":"x","type":"int","default":7}]}`, `{}`, "map[string]interface {}", nil},
		{"record-required-field-absent", `{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}`, `{}`, "", nil},
		{"array-match", `{"type":"array","items":"int"}`, `[1]`, "[]interface {}", nil},
		{"array-miss-nonarray", `{"type":"array","items":"int"}`, `1`, "int32", int32(1)},
		{"map-match", `{"type":"map","values":"int"}`, `{"k":1}`, "map[string]interface {}", nil},
		{"map-miss-nonmap", `{"type":"map","values":"int"}`, `1`, "int32", int32(1)},
		{"nameref-record-branch", `{"type":"record","name":"Inner","fields":[{"name":"e","type":"E"}]}`, `{"e":"B"}`, "map[string]interface {}", nil},
		{"nameref-array-branch", `{"type":"array","items":"E"}`, `["B"]`, "[]interface {}", nil},
	}

	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			// "E" is declared up front so a name-referenced branch resolves.
			reader := fmt.Sprintf(`{"type":"record","name":"R","fields":[
				{"name":"e0","type":{"type":"enum","name":"E","symbols":["A","B"]}},
				{"name":"f","type":[%s,"int"],"default":%s}]}`, c.branch, c.def)
			rs, err := avro.Parse(reader)
			if err != nil {
				if c.wantGo == "" {
					return // schema-invalid default; nothing to compare
				}
				t.Fatalf("parse: %v", err)
			}
			if c.wantGo == "" {
				t.Fatalf("expected the default to be rejected, but the schema parsed")
			}
			got := rs.Root().Fields[1].Default
			if fmt.Sprintf("%T", got) != c.wantGo {
				t.Fatalf("Default is %T(%v), want %s", got, got, c.wantGo)
			}
			if c.wantVal != nil && !reflect.DeepEqual(got, c.wantVal) {
				t.Fatalf("Default = %v, want %v", got, c.wantVal)
			}

			// Oracle: the wire auto-fill. A writer without "f" makes the
			// reader materialize the default; the branch the WIRE picked must
			// produce a value of the same Go type the metadata reported.
			writer := `{"type":"record","name":"R","fields":[
				{"name":"e0","type":{"type":"enum","name":"E","symbols":["A","B"]}}]}`
			ws, err := avro.Parse(writer)
			if err != nil {
				t.Fatalf("parse writer: %v", err)
			}
			res, err := avro.Resolve(ws, rs)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			wire, err := ws.Encode(map[string]any{"e0": "A"})
			if err != nil {
				t.Fatalf("encode writer: %v", err)
			}
			var filled map[string]any
			if _, err := res.Decode(wire, &filled); err != nil {
				t.Fatalf("resolved decode: %v", err)
			}
			if !darkSameBranchShape(filled["f"], got) {
				t.Fatalf("wire auto-fill produced %T(%v); metadata Default is %T(%v)",
					filled["f"], filled["f"], got, got)
			}
		})
	}
}

// darkSameBranchShape reports whether the wire auto-fill and the metadata
// Default named the same union branch. It compares the branch CLASS, not the
// exact Go type: the wire materializes a decoded value (a record fills every
// field, an enum arrives as a string) while the metadata surfaces the default
// as written, so the record/array/map/scalar class is the shared question.
func darkSameBranchShape(wire, meta any) bool {
	class := func(v any) string {
		switch v.(type) {
		case nil:
			return "null"
		case bool:
			return "bool"
		case int32, int64:
			return "int"
		case float32, float64:
			return "float"
		case string:
			return "string"
		case []byte:
			return "bytes"
		case map[string]any:
			return "map"
		case []any:
			return "array"
		}
		return fmt.Sprintf("%T", v)
	}
	return class(wire) == class(meta)
}

// ---------------------------------------------------------------------------
// The recursion limit, across container shapes AND nesting parity.
//
// Every schema level costs one depth unit, so a recursive schema puts its
// record nodes on one parity and its union / array / map nodes on the other.
// Only whichever lands ON the limit trips, which is why a single recursive
// shape exercises exactly one of the guards: the cell shifts the whole tree
// by one level to reach the other. The assertion is the same on both sides —
// deep input is refused with the recursion-limit error and nothing panics,
// shallow input of the same shape round-trips.
// ---------------------------------------------------------------------------

type darkNullNext struct {
	Next *darkNullNext `avro:"next"`
}

type darkArrKids struct {
	Kids []darkArrKids `avro:"kids"`
}

// The shift=1 wrappers: one extra record level moves the union / array node
// onto the depth the recursion guard trips at.
type darkWrapNullNext struct {
	O darkNullNext `avro:"o"`
}

type darkWrapArrKids struct {
	O darkArrKids `avro:"o"`
}

type darkPtrKids struct {
	Kids []*darkPtrKids `avro:"kids"`
}

type darkMapKids struct {
	Kids map[string]darkMapKids `avro:"kids"`
}

type darkPrimArr struct {
	Next *darkPrimArr `avro:"next"`
	Xs   []int32      `avro:"xs"`
}

func TestMatrix_RecursionLimitAcrossShapesAndParity(t *testing.T) {
	t.Parallel()

	const deep = 1200 // > the package's depth limit
	const shallow = 4

	shapes := []struct {
		name string
		// body is the recursive record's field list, named "T"; the writer
		// form appends one extra trailing field so the reader must skip
		// through the same depth.
		body string
		// mk builds a Go value nested d levels.
		mk func(d int) any
		// wire builds a binary encoding nested d levels, or nil.
		wire func(d int) []byte
		// wwire is the same nesting in the WRITER's shape (one extra
		// trailing int field per level), or nil to skip the resolve case.
		wwire func(d int) []byte
		// mkTarget allocates a CONCRETE decode target for the given shift,
		// which compiles a different (unsafe, field-offset) decoder than the
		// any/map targets.
		mkTarget func(shift int) any
	}{
		{
			name: "null-union-record",
			body: `{"name":"next","type":["null","T"]}`,
			mk: func(d int) any {
				root := &darkNullNext{}
				cur := root
				for range d {
					cur.Next = &darkNullNext{}
					cur = cur.Next
				}
				return *root
			},
			wire: func(d int) []byte {
				b := make([]byte, 0, d+1)
				for range d {
					b = append(b, 2)
				}
				return append(b, 0)
			},
			// f(0) = [null-union][extra=0]; f(n) = [non-null] f(n-1) [extra=0]
			wwire: func(d int) []byte {
				b := make([]byte, 0, 2*d+2)
				for range d {
					b = append(b, 2)
				}
				b = append(b, 0, 0)
				for range d {
					b = append(b, 0)
				}
				return b
			},
			mkTarget: func(shift int) any {
				if shift == 1 {
					return new(darkWrapNullNext)
				}
				return new(darkNullNext)
			},
		},
		{
			name: "array-record",
			body: `{"name":"kids","type":{"type":"array","items":"T"}}`,
			mk: func(d int) any {
				cur := darkArrKids{}
				for range d {
					cur = darkArrKids{Kids: []darkArrKids{cur}}
				}
				return cur
			},
			wire: func(d int) []byte {
				b := make([]byte, 0, 2*d+1)
				for range d {
					b = append(b, 2)
				}
				b = append(b, 0)
				for range d {
					b = append(b, 0)
				}
				return b
			},
			// g(0) = [empty block][extra=0]; g(n) = [count 1] g(n-1) [end][extra=0]
			wwire: func(d int) []byte {
				b := make([]byte, 0, 3*d+2)
				for range d {
					b = append(b, 2)
				}
				b = append(b, 0, 0)
				for range d {
					b = append(b, 0, 0)
				}
				return b
			},
			mkTarget: func(shift int) any {
				if shift == 1 {
					return new(darkWrapArrKids)
				}
				return new(darkArrKids)
			},
		},
		{
			name: "array-ptr-record",
			body: `{"name":"kids","type":{"type":"array","items":"T"}}`,
			mk: func(d int) any {
				cur := &darkPtrKids{}
				for range d {
					cur = &darkPtrKids{Kids: []*darkPtrKids{cur}}
				}
				return *cur
			},
		},
		{
			name: "array-null-union-record",
			body: `{"name":"kids","type":{"type":"array","items":["null","T"]}}`,
			mk: func(d int) any {
				cur := &darkPtrKids{}
				for range d {
					cur = &darkPtrKids{Kids: []*darkPtrKids{cur}}
				}
				return *cur
			},
		},
		{
			name: "map-record",
			body: `{"name":"kids","type":{"type":"map","values":"T"}}`,
			mk: func(d int) any {
				cur := darkMapKids{Kids: map[string]darkMapKids{}}
				for range d {
					cur = darkMapKids{Kids: map[string]darkMapKids{"k": cur}}
				}
				return cur
			},
		},
		{
			name: "null-union-with-primitive-array",
			body: `{"name":"next","type":["null","T"]},{"name":"xs","type":{"type":"array","items":"int"}}`,
			mk: func(d int) any {
				cur := &darkPrimArr{Xs: []int32{1}}
				for range d {
					cur = &darkPrimArr{Next: cur, Xs: []int32{1}}
				}
				return *cur
			},
		},
	}

	for _, sh := range shapes {
		for _, shift := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/shift=%d", sh.name, shift), func(t *testing.T) {
				t.Parallel()
				inner := fmt.Sprintf(`{"type":"record","name":"T","fields":[%s]}`, sh.body)
				schema := inner
				mk := sh.mk
				if shift == 1 {
					schema = fmt.Sprintf(`{"type":"record","name":"W","fields":[{"name":"o","type":%s}]}`, inner)
					inner := sh.mk
					mk = func(d int) any { return map[string]any{"o": inner(d)} }
				}
				s, err := avro.Parse(schema)
				if err != nil {
					t.Fatalf("parse: %v", err)
				}

				// Shallow: the same shape must work, so the deep failure is
				// the limit and not the shape.
				if _, err := s.Encode(mk(shallow)); err != nil {
					t.Fatalf("shallow encode: %v", err)
				}
				if _, err := s.EncodeJSON(mk(shallow)); err != nil {
					t.Fatalf("shallow encode json: %v", err)
				}

				// Deep: refused, and refused by the recursion limit.
				if _, err := s.Encode(mk(deep)); err == nil {
					t.Fatal("deep binary encode accepted")
				} else if !strings.Contains(err.Error(), "recursion limit exceeded") {
					t.Fatalf("deep binary encode: want recursion limit, got %v", err)
				}
				if _, err := s.EncodeJSON(mk(deep)); err == nil {
					t.Fatal("deep JSON encode accepted")
				} else if !strings.Contains(err.Error(), "recursion limit exceeded") {
					t.Fatalf("deep JSON encode: want recursion limit, got %v", err)
				}

				if sh.wire == nil {
					return
				}
				wire := sh.wire(deep)
				targets := []struct {
					name string
					mk   func() any
				}{
					{"any", func() any { return new(any) }},
					{"map", func() any { return &map[string]any{} }},
				}
				if sh.mkTarget != nil {
					targets = append(targets, struct {
						name string
						mk   func() any
					}{"struct", func() any { return sh.mkTarget(shift) }})
				}
				for _, target := range targets {
					out := target.mk()
					if _, err := s.Decode(wire, out); err == nil {
						t.Fatalf("deep decode into %s accepted", target.name)
					} else if !strings.Contains(err.Error(), "recursion limit exceeded") {
						t.Fatalf("deep decode into %s: want recursion limit, got %v", target.name, err)
					}
				}

				if sh.wwire == nil {
					return
				}
				// The skip path: a writer with one extra trailing field makes
				// the resolved reader skip through the same depth.
				winner := fmt.Sprintf(`{"type":"record","name":"T","fields":[%s,{"name":"extra","type":"int"}]}`, sh.body)
				wschema := winner
				if shift == 1 {
					wschema = fmt.Sprintf(`{"type":"record","name":"W","fields":[{"name":"o","type":%s}]}`, winner)
				}
				ws, err := avro.Parse(wschema)
				if err != nil {
					t.Fatalf("parse writer: %v", err)
				}
				res, err := avro.Resolve(ws, s)
				if err != nil {
					t.Fatalf("resolve: %v", err)
				}
				var skipped any
				if _, err := res.Decode(sh.wwire(deep), &skipped); err == nil {
					t.Fatal("deep resolved decode accepted")
				} else if !strings.Contains(err.Error(), "recursion limit exceeded") {
					t.Fatalf("deep resolved decode: want recursion limit, got %v", err)
				}
			})
		}
	}
}

// ---------------------------------------------------------------------------
// The strict JSON skipper on malformed tails.
//
// Unknown fields in Avro JSON are skipped by a strict scanner rather than a
// permissive one, so every malformed continuation has to be an error. Oracle:
// encoding/json's own json.Valid — an input the stdlib calls invalid must not
// decode here either.
// ---------------------------------------------------------------------------

func TestMatrix_StrictJSONSkipperRejectsMalformedSkippedValues(t *testing.T) {
	t.Parallel()

	skipped := []struct {
		name string
		body string
	}{
		{"unterminated-array", `[1`},
		{"unterminated-array-after-comma", `[1,`},
		{"unterminated-object", `{"a`},
		{"unterminated-object-value", `{"a":1`},
		{"bad-object-separator", `{"a":1 2}`},
		{"bad-object-key", `{a:1}`},
		{"missing-colon", `{"a" 1}`},
		{"bad-string-escape", `"a\q"`},
		{"nested-unterminated", `[[{"a":[1`},
		{"well-formed-array", `[1,2]`},
		{"well-formed-object", `{"a":{"b":[1,2]}}`},
	}

	s := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"keep","type":"int"}]}`)
	// Closing axis: a malformed value followed by the record's own '}' is a
	// different scanner state than the same value truncated at EOF — the
	// first has a byte to reject, the second has none.
	for _, closing := range []struct {
		name string
		tail string
	}{{"closed", "}"}, {"truncated", ""}} {
		for _, c := range skipped {
			t.Run(c.name+"/"+closing.name, func(t *testing.T) {
				t.Parallel()
				in := `{"keep":1,"extra":` + c.body + closing.tail
				wantOK := json.Valid([]byte(in))
				var out map[string]any
				err := s.DecodeJSON([]byte(in), &out)
				if wantOK != (err == nil) {
					t.Fatalf("json.Valid=%v but DecodeJSON err=%v for %s", wantOK, err, in)
				}
				if wantOK && out["keep"] != int32(1) {
					t.Fatalf("kept field lost: %#v", out)
				}
			})
		}
	}
}

// ---------------------------------------------------------------------------
// Enum carriers, both wires.
//
// The enum encoders dispatch on the Go carrier: the builtin string takes a
// fast path, a text-marshaling type takes the name path, a NAMED string type
// with no text method takes the generic string path, and an integer takes the
// ordinal path. The two wires implement this separately, so the cell crosses
// (carrier) x (wire) and requires the same accept/reject verdict from both.
// ---------------------------------------------------------------------------

type darkNamedEnum string

type darkTextEnum struct{ S string }

func (e darkTextEnum) MarshalText() ([]byte, error) { return []byte(e.S), nil }

func TestMatrix_EnumCarrierAcceptanceAgreesAcrossWires(t *testing.T) {
	t.Parallel()

	s := mustParse(t, `{"type":"enum","name":"E","symbols":["RED","BLUE"]}`)
	cells := []struct {
		name    string
		v       any
		wantOK  bool
		wantSym string
	}{
		{"builtin-string-member", "RED", true, "RED"},
		{"builtin-string-nonmember", "PINK", false, ""},
		{"named-string-member", darkNamedEnum("BLUE"), true, "BLUE"},
		{"named-string-nonmember", darkNamedEnum("PINK"), false, ""},
		{"text-marshaler-member", darkTextEnum{"RED"}, true, "RED"},
		{"text-marshaler-nonmember", darkTextEnum{"PINK"}, false, ""},
		{"ordinal-in-range", int32(1), true, "BLUE"},
		{"ordinal-out-of-range", int32(9), false, ""},
		{"json-number-carrier", json.Number("0"), false, ""},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			bin, binErr := s.Encode(c.v)
			jsn, jsnErr := s.EncodeJSON(c.v)
			if (binErr == nil) != (jsnErr == nil) {
				t.Fatalf("wire disagreement: binary err=%v, json err=%v", binErr, jsnErr)
			}
			if c.wantOK != (binErr == nil) {
				t.Fatalf("want ok=%v, got binary err=%v", c.wantOK, binErr)
			}
			if !c.wantOK {
				return
			}
			var sym string
			mustDecode(t, s, bin, &sym)
			if sym != c.wantSym {
				t.Fatalf("binary decoded %q, want %q", sym, c.wantSym)
			}
			if got := string(jsn); got != `"`+c.wantSym+`"` {
				t.Fatalf("json encoded %s, want %q", got, c.wantSym)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Records carried by a map whose key type is not plain string.
//
// map[string]any has a fast path; every other string-KIND key type takes the
// generic map arm, on both wires. json.Number is a string-kind type whose
// values must remain valid number literals, so a record field name that is
// not one has to be refused rather than silently written.
// ---------------------------------------------------------------------------

type darkMapKey string

func TestMatrix_RecordFromNonCanonicalMapCarrier(t *testing.T) {
	t.Parallel()

	s, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"string"}]}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	// A field name that IS a valid JSON number literal needs a relaxed name
	// validator (the Avro grammar forbids a leading digit), which is exactly
	// the shape a json.Number-keyed carrier can actually round-trip.
	numeric, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"12","type":"string"}]}`,
		avro.WithLaxNames(nil))
	if err != nil {
		t.Fatalf("parse numeric-named: %v", err)
	}

	cells := []struct {
		name   string
		schema *avro.Schema
		v      any
		wantOK bool
	}{
		{"named-string-key-good-value", s, map[darkMapKey]any{"f": "hi"}, true},
		{"named-string-key-bad-value", s, map[darkMapKey]any{"f": 3}, false},
		{"plain-map-typed-value", s, map[string]string{"f": "hi"}, true},
		{"plain-map-bad-typed-value", s, map[string]int{"f": 3}, false},
		{"json-number-key-nonnumeric-field", s, map[json.Number]any{json.Number("f"): "hi"}, false},
		{"json-number-key-numeric-field", numeric, map[json.Number]any{json.Number("12"): "hi"}, true},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			_, binErr := c.schema.Encode(c.v)
			_, jsnErr := c.schema.EncodeJSON(c.v)
			if (binErr == nil) != (jsnErr == nil) {
				t.Fatalf("wire disagreement: binary err=%v, json err=%v", binErr, jsnErr)
			}
			if c.wantOK != (binErr == nil) {
				t.Fatalf("want ok=%v, got %v", c.wantOK, binErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Resolved JSON decode: the writer-shaped JSON is transformed through the
// writer's own binary form before the resolving decode, so a writer JSON the
// writer schema rejects has to fail there rather than half-transform.
// ---------------------------------------------------------------------------

func TestMatrix_ResolvedJSONDecodeRejectsBadWriterJSON(t *testing.T) {
	t.Parallel()

	ws, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`)
	if err != nil {
		t.Fatalf("parse writer: %v", err)
	}
	rs, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`)
	if err != nil {
		t.Fatalf("parse reader: %v", err)
	}
	res, err := avro.Resolve(ws, rs)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	cells := []struct {
		name   string
		in     string
		wantOK bool
	}{
		{"well-formed", `{"a":1,"b":"x"}`, true},
		{"wrong-type", `{"a":"notanint","b":"x"}`, false},
		{"missing-writer-field", `{"a":1}`, false},
		{"malformed-json", `{"a":1,`, false},
		{"trailing-garbage", `{"a":1,"b":"x"}}`, false},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			var out map[string]any
			err := res.DecodeJSON([]byte(c.in), &out)
			if c.wantOK != (err == nil) {
				t.Fatalf("want ok=%v, got %v", c.wantOK, err)
			}
			if c.wantOK && out["a"] != int64(1) {
				t.Fatalf("resolved value %#v", out)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Field-level logicalType: where the lift points.
//
// A logicalType written as a SIBLING of "type" on a field object is lifted into
// the type definition for the WIRE path only — the metadata API keeps reporting
// the schema as written, the documented scope of the concession. Which node the
// lift lands on depends on the field's type shape: a bare primitive takes it, a
// union hands it to the first non-null branch, an object takes it unless it
// already carries one, and a union with NO non-null branch has nowhere to put
// it. The oracle is the equivalent NESTED-form schema.
// ---------------------------------------------------------------------------

func TestMatrix_FieldLevelLogicalLiftTargetShapes(t *testing.T) {
	t.Parallel()

	const dec = `,"logicalType":"decimal","precision":4,"scale":2`

	cells := []struct {
		name string
		// flat carries the annotation on the FIELD; nested is the same
		// schema written the spec-blessed way, and is the oracle.
		flatType, flatExtra string
		nestedType          string
		// metaLogical is what the metadata surface reports for the field's
		// type node: the lift never writes there, so this is "" unless the
		// TYPE itself carried an annotation.
		metaLogical string
	}{
		{"primitive-target", `"bytes"`, dec,
			`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`, ""},
		{"union-first-nonnull-target", `["null","bytes"]`, dec,
			`["null",{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}]`, ""},
		{"object-target", `{"type":"fixed","name":"F","size":8}`, dec,
			`{"type":"fixed","name":"F","size":8,"logicalType":"decimal","precision":4,"scale":2}`, ""},
		{"object-own-logical-wins", `{"type":"bytes","logicalType":"big-decimal"}`, dec,
			`{"type":"bytes","logicalType":"big-decimal"}`, "big-decimal"},
		{"union-all-null-no-target", `["null"]`, dec, `["null"]`, ""},
		{"union-all-null-uuid-no-target", `["null"]`, `,"logicalType":"uuid"`, `["null"]`, ""},
	}

	rat := new(big.Rat)
	rat.SetString("12/1")

	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			flat, err := avro.Parse(fmt.Sprintf(
				`{"type":"record","name":"R","fields":[{"name":"f","type":%s%s}]}`, c.flatType, c.flatExtra))
			if err != nil {
				t.Fatalf("parse flat: %v", err)
			}
			nested, err := avro.Parse(fmt.Sprintf(
				`{"type":"record","name":"R","fields":[{"name":"f","type":%s}]}`, c.nestedType))
			if err != nil {
				t.Fatalf("parse nested oracle: %v", err)
			}

			// The lift is a wire-path rewrite, so the flat form has to
			// behave EXACTLY like the nested form it stands in for —
			// including when it rejects.
			gotBin, gotErr := flat.Encode(map[string]any{"f": rat})
			wantBin, wantErr := nested.Encode(map[string]any{"f": rat})
			if (gotErr == nil) != (wantErr == nil) {
				t.Fatalf("binary verdict differs: flat err=%v, nested err=%v", gotErr, wantErr)
			}
			if gotErr == nil && string(gotBin) != string(wantBin) {
				t.Fatalf("binary bytes differ: flat %v, nested %v", gotBin, wantBin)
			}
			gotJSON, gotJErr := flat.EncodeJSON(map[string]any{"f": rat})
			wantJSON, wantJErr := nested.EncodeJSON(map[string]any{"f": rat})
			if (gotJErr == nil) != (wantJErr == nil) {
				t.Fatalf("json verdict differs: flat err=%v, nested err=%v", gotJErr, wantJErr)
			}
			if gotJErr == nil && string(gotJSON) != string(wantJSON) {
				t.Fatalf("json bytes differ: flat %s, nested %s", gotJSON, wantJSON)
			}
			if string(flat.Canonical()) != string(nested.Canonical()) {
				t.Fatalf("canonical differs:\n flat %s\nnested %s", flat.Canonical(), nested.Canonical())
			}

			// The metadata API reports the schema AS WRITTEN: the lift does
			// not reach it, so a field-level annotation stays in the field's
			// Props and the type node keeps only its own logicalType.
			f := flat.Root().Fields[0]
			if f.Type.LogicalType != c.metaLogical {
				t.Fatalf("metadata type logicalType = %q, want %q", f.Type.LogicalType, c.metaLogical)
			}
			if c.flatExtra != "" {
				if _, ok := f.Props["logicalType"]; !ok {
					t.Fatalf("field-level logicalType left no Props trace: %#v", f.Props)
				}
			}
			// And the re-emitted schema still round-trips to the same
			// canonical bytes, so nothing the lift did is lost or doubled.
			out, err := flat.Root().Schema()
			if err != nil {
				t.Fatalf("re-emit: %v", err)
			}
			if string(out.Canonical()) != string(flat.Canonical()) {
				t.Fatalf("re-emitted canonical differs:\n got %s\nwant %s", out.Canonical(), flat.Canonical())
			}
		})
	}
}
