package avro_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

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
			ws, err := avro.Parse(schemaJSON, ct)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			var buf bytes.Buffer
			w, err := ocf.NewWriter(&buf, ws)
			if err != nil {
				t.Fatalf("NewWriter: %v", err)
			}
			for i := 0; i < 3; i++ {
				if err := w.Encode(map[string]any{"f": cbox{Raw: raw}}); err != nil {
					t.Fatalf("Encode #%d: %v", i, err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			r, err := ocf.NewReader(bytes.NewReader(buf.Bytes()), ocf.WithSchemaOpts(ct))
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
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
