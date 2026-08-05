package avro_test

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/twmb/avro"
)

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
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
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
			resolved, err := avro.Resolve(w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
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
