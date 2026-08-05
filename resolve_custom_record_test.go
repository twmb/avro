package avro_test

import (
	"fmt"
	"testing"

	"github.com/twmb/avro"
)

// A record-level CustomType (AvroType:"record") Decode callback fires on a
// direct Decode (applyCustomTypes wires record nodes at build time). It must
// also fire through a resolved (writer→reader) decode: resolveRecord builds a
// fresh node, and unless it re-applies the reader's custom wiring (as every
// other resolve arm does), any real evolution silently returns the raw
// map[string]any instead of the callback's converted value — a direct-vs-
// resolved divergence. The callback is value-TRANSFORMING (wraps the value
// under a marker key) so "fired" is distinguishable from "raw passthrough".
func TestRegression_RecordCustomTypeThroughResolve(t *testing.T) {
	const marker = "WRAPPED_BY_CUSTOM"
	newCT := func() avro.CustomType {
		return avro.CustomType{
			AvroType: "record",
			Decode: func(v any, _ *avro.SchemaNode) (any, error) {
				return map[string]any{marker: v}, nil
			},
		}
	}

	readerJSON := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},{"name":"b","type":"string"}]}`

	// Each writer schema is compatible with the reader but has a different
	// canonical form, so Resolve builds a real resolving decoder (the
	// canonical-equality fast path that returns the reader as-is is bypassed).
	writers := map[string]string{
		"reorder": `{"type":"record","name":"R","fields":[
			{"name":"b","type":"string"},{"name":"a","type":"int"}]}`,
		"drop_extra_writer_field": `{"type":"record","name":"R","fields":[
			{"name":"a","type":"int"},{"name":"b","type":"string"},{"name":"c","type":"long"}]}`,
		"add_reader_default": `{"type":"record","name":"R","fields":[
			{"name":"a","type":"int"}]}`, // reader's "b" filled from... needs a default
	}

	// Control: a direct decode fires the custom (proves the harness + callback).
	reader := avro.MustParse(readerJSON, newCT())
	wireDirect, err := reader.AppendEncode(nil, map[string]any{"a": int32(1), "b": "x"})
	if err != nil {
		t.Fatalf("direct encode: %v", err)
	}
	var direct any
	if _, err := reader.Decode(wireDirect, &direct); err != nil {
		t.Fatalf("direct decode: %v", err)
	}
	if dm, ok := direct.(map[string]any); !ok || dm[marker] == nil {
		t.Fatalf("control: record custom did not fire on DIRECT decode: %#v", direct)
	}

	for name, writerJSON := range writers {
		t.Run(name, func(t *testing.T) {
			// The "add_reader_default" case needs the reader's dropped-from-writer
			// field to have a default; rebuild the reader accordingly.
			rJSON := readerJSON
			if name == "add_reader_default" {
				rJSON = `{"type":"record","name":"R","fields":[
					{"name":"a","type":"int"},{"name":"b","type":"string","default":"d"}]}`
			}
			r := avro.MustParse(rJSON, newCT())
			w := avro.MustParse(writerJSON)
			res, err := avro.Resolve(w, r)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}

			val := map[string]any{"a": int32(1), "b": "x"}
			if name == "drop_extra_writer_field" {
				val["c"] = int64(9)
			}
			if name == "add_reader_default" {
				delete(val, "b")
			}
			wire, err := w.AppendEncode(nil, val)
			if err != nil {
				t.Fatalf("writer encode: %v", err)
			}

			// Resolved binary decode must fire the custom.
			var gotBin any
			if _, err := res.Decode(wire, &gotBin); err != nil {
				t.Fatalf("resolved binary decode: %v", err)
			}
			if m, ok := gotBin.(map[string]any); !ok || m[marker] == nil {
				t.Fatalf("record custom DROPPED through resolved binary decode: %#v", gotBin)
			}

			// Resolved JSON decode (consumes writer-shaped JSON) must agree.
			wireJSON, err := w.AppendEncodeJSON(nil, val)
			if err != nil {
				t.Fatalf("writer encodeJSON: %v", err)
			}
			var gotJSON any
			if err := res.DecodeJSON(wireJSON, &gotJSON); err != nil {
				t.Fatalf("resolved JSON decode: %v", err)
			}
			if m, ok := gotJSON.(map[string]any); !ok || m[marker] == nil {
				t.Fatalf("record custom DROPPED through resolved JSON decode: %#v", gotJSON)
			}
		})
	}
}

// A record-level custom must also fire through resolution of a RECURSIVE
// (self-referential) record at every level: resolveNode's cycle placeholder
// copies the resolved node's contents, so the custom wrap applied before the
// copy must propagate to the inner recursive references.
func TestRegression_RecursiveRecordCustomThroughResolve(t *testing.T) {
	const marker = "WRAP"
	ct := avro.CustomType{
		AvroType: "record",
		Decode: func(v any, _ *avro.SchemaNode) (any, error) {
			return map[string]any{marker: v}, nil
		},
	}
	// Reader reorders fields vs writer so Resolve builds a real resolving decoder.
	reader := avro.MustParse(`{"type":"record","name":"LL","fields":[{"name":"v","type":"int"},{"name":"next","type":["null","LL"]}]}`, ct)
	writer := avro.MustParse(`{"type":"record","name":"LL","fields":[{"name":"next","type":["null","LL"]},{"name":"v","type":"int"}]}`)
	res, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	val := map[string]any{"v": int32(1), "next": map[string]any{"v": int32(2), "next": nil}}
	wire, err := writer.AppendEncode(nil, val)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got any
	if _, err := res.Decode(wire, &got); err != nil {
		t.Fatalf("resolved decode: %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok || m[marker] == nil {
		t.Fatalf("record custom dropped at outer level: %#v", got)
	}
	inner, ok := m[marker].(map[string]any)["next"].(map[string]any)
	if !ok || inner[marker] == nil {
		t.Fatalf("record custom dropped at inner recursive level: %#v", m[marker])
	}
}

// Control: a non-custom resolved record decode must NOT grow a marker key — the
// re-applied wiring is a no-op when no CustomType is registered.
func TestResolvedRecordWithoutCustomIsUnwrapped(t *testing.T) {
	r := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`)
	w := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"b","type":"string"},{"name":"a","type":"int"}]}`)
	res, err := avro.Resolve(w, r)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	wire, err := w.AppendEncode(nil, map[string]any{"a": int32(1), "b": "x"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got map[string]any
	if _, err := res.Decode(wire, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if fmt.Sprintf("%v", got["a"]) != "1" || got["b"] != "x" {
		t.Fatalf("plain resolved record decode wrong: %#v", got)
	}
}
