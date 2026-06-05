package avro_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/twmb/avro"
)

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
