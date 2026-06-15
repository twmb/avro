package avro

import (
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"runtime/debug"
	"sort"
	"strings"
	"testing"
	"time"
)

// ===========================================================================
// The generative SchemaFor round-trip self-consistency net.
//
// SchemaFor (Go type -> Avro schema) is twmb-unique: there is no spec, no Java,
// and no fastavro counterpart to differential against (Go->Avro inference is not
// a standardized transform), so the schema->value matrix, the encode/decode
// parity invariant, the fastavro/Java oracles, and a byte-fuzzer all sail past
// it (a fuzzer cannot synthesize a Go field type; SchemaFor[T] is compile-time
// generic). Its one machine-checkable contract is ROUND-TRIP SELF-CONSISTENCY:
// for every Go type T, SchemaFor[T] must EITHER
//
//	(a) build a schema that twmb's OWN codecs round-trip a value of T — through
//	    BOTH the binary (Encode/Decode) AND the JSON (EncodeJSON/DecodeJSON)
//	    wire, so a binary-vs-JSON path divergence is caught too — OR
//	(b) reject cleanly at build time (a non-empty error, no panic).
//
// The forbidden outcome is the highest-yield SchemaFor bug shape, build-accepts/
// encode-rejects: SchemaFor returns a schema, but Encode (or Decode) of a value
// of that very type fails far from the SchemaFor call — the schema "lies" about
// the Go type. Every historical SchemaFor bug is an instance: json.Number
// inferred as "string" (the codec rejects it), a one-directional text type
// inferred as "string" (encode XOR decode fails), a pointer chain deeper than
// the codec unwraps inferred as ["null",T] (Encode of a non-nil value fails).
//
// ONE generator crosses the four axes those bugs live at, and the SAME oracle
// runs on every cell:
//
//	Go type shape  x  struct tag  x  text interface  x  logical type
//
//	shape   : direct, *L, **L, at-cap and past-cap pointer chains, []L, [N]L,
//	          map[string]L, []*L (and recursive-struct + named-struct leaves)
//	tag     : none, rename, alias=, default=, every logical (valid- AND
//	          wrong-underlying for the leaf), decimal(p,s), and malformed forms
//	          (unknown option, unclosed bracket, decimal trailing junk, empty
//	          alias, the "-,opt" skip-with-options)
//	text    : a leaf implementing none / MarshalText-only / UnmarshalText-only /
//	          both, over string / []byte / non-string(struct) base kinds
//	logical : a logical whose required underlying wire MATCHES the leaf
//	          (date-on-int, uuid-on-[16]byte, timestamp-on-time.Time) and one
//	          whose underlying is WRONG for it (uuid-on-int, decimal-on-string)
//
// RECONCILIATION (not duplication): this net reuses the package's existing
// SchemaFor infrastructure rather than re-deriving it —
//   - schemaForType (embed_shape_generative_test.go): the reflect.Type-driven
//     replica of SchemaFor, pinned byte-identical to the generic entry point by
//     TestGenerative_SchemaForReplicaParity. The bulk of the cross is built from
//     reflect.StructOf types that SchemaFor[T] cannot take at run time;
//     rtRealEntryPointCells additionally drives the REAL SchemaFor[T] on the
//     compile-time-nameable leaves so the replica's fidelity is bridged.
//   - sampleValue (schema_for_test.go, made cycle-safe): the non-empty value
//     builder, so a leaf buried in *T / []T / map[K]T actually reaches the codec.
//   - the malformed-tag alphabet is the same family embed_shape_tagedge_test.go's
//     tagDefects pins for WALKER AGREEMENT; here the same tags are crossed with
//     the whole leaf x shape space under the ROUND-TRIP oracle instead.
//
// The per-axis nets each fix ONE axis: TestGenerative_EmbedShapeWalkerAgreement
// crosses struct SHAPE with an int32 leaf (field-selection), TestSchemaFor-
// EncodeParity crosses LEAF TYPE at direct depth (no tag/shape/text cross),
// TestGenerative_TagEdgeWalkerAgreement crosses TAGS with int leaves. None
// crosses leaf x shape x tag x text x logical, where the build-accepts/encode-
// rejects interactions hide (a one-way-text type behind a pointer or inside a
// slice; a logical tag on a pointer-to-named-int; uuid on []string; decimal on a
// map). That product is this net's job. Non-vacuity is recorded at the bottom:
// reverting the one-way-text refusal, the json.Number reject, or the pointer-
// chain cap each turns a measured set of cells red.
// ===========================================================================

// ---- text-interface leaf alphabet -----------------------------------------
//
// Each base kind (non-string struct, string, []byte) x {none, MarshalText-only,
// UnmarshalText-only, both}. The non-string base is the one the one-way-text
// refusal guards: a struct that round-trips as a string ONLY if it implements
// BOTH directions (a string/[]byte kind covers the missing direction via the
// kind itself, so a one-directional method on those still builds). Methods are
// identity transforms so the "both" cells round-trip the value faithfully.

type rtStructNone struct{ S string } // no text methods -> inferred as a RECORD
type rtStructMarshal struct{ S string }

func (v rtStructMarshal) MarshalText() ([]byte, error) { return []byte(v.S), nil }

type rtStructUnmarshal struct{ S string }

func (v *rtStructUnmarshal) UnmarshalText(b []byte) error { v.S = string(b); return nil }

type rtStructBoth struct{ S string }

func (v rtStructBoth) MarshalText() ([]byte, error)  { return []byte(v.S), nil }
func (v *rtStructBoth) UnmarshalText(b []byte) error { v.S = string(b); return nil }

type rtStrPlain string // string kind, no methods -> "string"
type rtStrMarshal string

func (v rtStrMarshal) MarshalText() ([]byte, error) { return []byte(v), nil }

type rtStrUnmarshal string

func (v *rtStrUnmarshal) UnmarshalText(b []byte) error { *v = rtStrUnmarshal(b); return nil }

type rtStrBoth string

func (v rtStrBoth) MarshalText() ([]byte, error)  { return []byte(v), nil }
func (v *rtStrBoth) UnmarshalText(b []byte) error { *v = rtStrBoth(b); return nil }

type rtStrAppend string // exercises the AppendText encode arm

func (v rtStrAppend) AppendText(b []byte) ([]byte, error) { return append(b, v...), nil }
func (v *rtStrAppend) UnmarshalText(b []byte) error       { *v = rtStrAppend(b); return nil }

type rtBytesPlain []byte // []byte kind, no methods -> "bytes"
type rtBytesMarshal []byte

func (v rtBytesMarshal) MarshalText() ([]byte, error) { return append([]byte(nil), v...), nil }

type rtBytesUnmarshal []byte

func (v *rtBytesUnmarshal) UnmarshalText(b []byte) error { *v = append((*v)[:0], b...); return nil }

type rtBytesBoth []byte

func (v rtBytesBoth) MarshalText() ([]byte, error)  { return append([]byte(nil), v...), nil }
func (v *rtBytesBoth) UnmarshalText(b []byte) error { *v = append((*v)[:0], b...); return nil }

// named primitives (distinct reflect.Type that must follow its Kind honestly).
type rtNamedInt int32
type rtNamedFloat float64

// a named struct (record) leaf and a recursive-struct (linked-list) leaf.
type rtRecord struct {
	A int32  `avro:"a"`
	B string `avro:"b"`
}
type rtLinked struct {
	Val  int32    `avro:"val"`
	Next *rtLinked `avro:"next"`
}

// ---- leaf specs ------------------------------------------------------------

type rtLeaf struct {
	typ      reflect.Type
	label    string
	faithful bool // round-trip preserves the value under reflect.DeepEqual
}

func rtLeaves() []rtLeaf {
	leaf := func(t reflect.Type, faithful bool) rtLeaf {
		return rtLeaf{typ: t, label: t.String(), faithful: faithful}
	}
	return []rtLeaf{
		// primitive kinds the inference switch handles (faithful: zero/"1"
		// round-trips bit-exactly through the codec).
		leaf(reflect.TypeFor[bool](), true),
		leaf(reflect.TypeFor[int8](), true),
		leaf(reflect.TypeFor[int32](), true),
		leaf(reflect.TypeFor[int64](), true),
		leaf(reflect.TypeFor[int](), true),
		leaf(reflect.TypeFor[uint8](), true),
		leaf(reflect.TypeFor[uint32](), true),
		leaf(reflect.TypeFor[uint64](), true),
		leaf(reflect.TypeFor[float32](), true),
		leaf(reflect.TypeFor[float64](), true),
		leaf(reflect.TypeFor[string](), true),
		// byte containers
		leaf(reflect.TypeFor[[]byte](), true),
		leaf(reflect.TypeFor[[4]byte](), true),
		leaf(reflect.TypeFor[[16]byte](), true),
		// named primitives
		leaf(reflect.TypeFor[rtNamedInt](), true),
		leaf(reflect.TypeFor[rtNamedFloat](), true),
		// codec-special-cased stdlib types whose Kind misleads (non-faithful
		// under DeepEqual: the encode/decode-accepts half of the oracle still
		// catches build-accepts/encode-rejects on them).
		leaf(reflect.TypeFor[json.Number](), false),
		leaf(reflect.TypeFor[time.Time](), false),
		leaf(reflect.TypeFor[time.Duration](), false),
		leaf(reflect.TypeFor[big.Rat](), false),
		leaf(reflect.TypeFor[*big.Rat](), false),
		// text-interface combos over three base kinds (the text axis).
		leaf(reflect.TypeFor[rtStructNone](), true), // a record
		leaf(reflect.TypeFor[rtStructMarshal](), false),
		leaf(reflect.TypeFor[rtStructUnmarshal](), false),
		leaf(reflect.TypeFor[rtStructBoth](), false),
		leaf(reflect.TypeFor[rtStrPlain](), true),
		leaf(reflect.TypeFor[rtStrMarshal](), false),
		leaf(reflect.TypeFor[rtStrUnmarshal](), false),
		leaf(reflect.TypeFor[rtStrBoth](), false),
		leaf(reflect.TypeFor[rtStrAppend](), false),
		leaf(reflect.TypeFor[rtBytesPlain](), true),
		leaf(reflect.TypeFor[rtBytesMarshal](), false),
		leaf(reflect.TypeFor[rtBytesUnmarshal](), false),
		leaf(reflect.TypeFor[rtBytesBoth](), false),
		// named struct + recursive struct
		leaf(reflect.TypeFor[rtRecord](), true),
		leaf(reflect.TypeFor[rtLinked](), false),
		// an interface leaf -> unsupported -> clean reject on every shape.
		leaf(reflect.TypeFor[any](), false),
	}
}

// ---- shape wrappers --------------------------------------------------------
//
// Each wraps a leaf type L into the struct field's Go type. The pointer chains
// straddle the codec's maxIndirectDepth unwrap cap: at-cap must build and
// round-trip a non-nil value; past-cap must reject at build (the build-accepts/
// encode-rejects pointer shape). Containers reset the cap, so an at-cap chain
// per element still builds.

type rtShape struct {
	label string
	wrap  func(reflect.Type) reflect.Type
}

func ptrChain(t reflect.Type, n int) reflect.Type {
	for range n {
		t = reflect.PointerTo(t)
	}
	return t
}

func rtShapes() []rtShape {
	return []rtShape{
		{"direct", func(t reflect.Type) reflect.Type { return t }},
		{"ptr", func(t reflect.Type) reflect.Type { return reflect.PointerTo(t) }},
		{"ptr2", func(t reflect.Type) reflect.Type { return ptrChain(t, 2) }},
		{"ptrAtCap", func(t reflect.Type) reflect.Type { return ptrChain(t, maxIndirectDepth) }},
		{"ptrPastCap", func(t reflect.Type) reflect.Type { return ptrChain(t, maxIndirectDepth+1) }},
		{"slice", func(t reflect.Type) reflect.Type { return reflect.SliceOf(t) }},
		{"array2", func(t reflect.Type) reflect.Type { return reflect.ArrayOf(2, t) }},
		{"map", func(t reflect.Type) reflect.Type { return reflect.MapOf(reflect.TypeFor[string](), t) }},
		{"slicePtr", func(t reflect.Type) reflect.Type { return reflect.SliceOf(reflect.PointerTo(t)) }},
	}
}

// ---- tag specs -------------------------------------------------------------
//
// "f" is the field name so a missing-name fallback never masks a tag effect.
// The logical set is applied uniformly: for a leaf whose wire matches it the
// cell must round-trip; for a leaf whose wire is wrong the cell must REJECT
// (uuid-on-int, decimal-on-bool) — never build a schema the codec then fights.
// The malformed forms must all reject. omitzero is deliberately absent: it is a
// runtime encode directive (skip-when-zero) that does not shape the schema, and
// is netted by omitzero_bsoft_test.go + tag_grammar_runtime_test.go; the only
// omitzero here is the malformed "-,omitzero" build-reject.

type rtTag struct {
	label string
	tag   string
}

func rtTags() []rtTag {
	return []rtTag{
		{"none", ``},
		{"name", `avro:"f"`},
		{"alias", `avro:"f,alias=old"`},
		{"type-alias", `avro:"f,type-alias=old"`},
		{"inline", `avro:",inline"`},
		{"default", `avro:"f,default=5"`},
		// logicals (valid-underlying for some leaves, wrong for others)
		{"uuid", `avro:"f,uuid"`},
		{"timestamp-millis", `avro:"f,timestamp-millis"`},
		{"timestamp-micros", `avro:"f,timestamp-micros"`},
		{"timestamp-nanos", `avro:"f,timestamp-nanos"`},
		{"date", `avro:"f,date"`},
		{"time-millis", `avro:"f,time-millis"`},
		{"time-micros", `avro:"f,time-micros"`},
		{"local-timestamp-millis", `avro:"f,local-timestamp-millis"`},
		{"local-timestamp-micros", `avro:"f,local-timestamp-micros"`},
		{"local-timestamp-nanos", `avro:"f,local-timestamp-nanos"`},
		{"decimal", `avro:"f,decimal(9,2)"`},
		// malformed forms (every one must reject at build)
		{"bad-option", `avro:"f,bogus"`},
		{"unclosed-bracket", `avro:"f,alias=[a"`},
		{"decimal-junk", `avro:"f,decimal(9,2,3)"`},
		{"empty-alias", `avro:"f,alias=[]"`},
		{"dash-options", `avro:"-,omitzero"`},
	}
}

// ---- the oracle ------------------------------------------------------------

type rtDivergence struct {
	label  string
	kind   string // "panic" | "encode-rejects" | "decode-own-wire" | "typed-decode" | "faithful" | "empty-error" | "json-encode-rejects" | "json-decode-own-wire" | "json-typed-decode" | "json-faithful"
	detail string
}

// rtRunCell applies the round-trip-or-clean-reject oracle to one cell. It never
// fails the test directly; it returns at most one divergence so the caller can
// tally the whole landscape and report it together (a single broken axis would
// otherwise spam thousands of Fatalf lines).
func rtRunCell(label string, fieldType reflect.Type, tag string, faithful bool) (built bool, div *rtDivergence) {
	defer func() {
		if r := recover(); r != nil {
			built = false
			div = &rtDivergence{label, "panic", fmt.Sprintf("%v\n%s", r, debug.Stack())}
		}
	}()

	st := reflect.StructOf([]reflect.StructField{{Name: "F", Type: fieldType, Tag: reflect.StructTag(tag)}})
	s, err := schemaForType(st, WithName("RT"))
	if err != nil {
		// (b) clean reject: a non-empty error and (via recover) no panic.
		if strings.TrimSpace(err.Error()) == "" {
			return false, &rtDivergence{label, "empty-error", "build rejected with an empty error string"}
		}
		return false, nil
	}

	// (a) build accepted -> Encode of a value of this exact type MUST accept.
	ptr := reflect.New(st)
	ptr.Elem().Field(0).Set(sampleValue(fieldType))
	wire, encErr := s.Encode(ptr.Interface())
	if encErr != nil {
		return true, &rtDivergence{label, "encode-rejects",
			fmt.Sprintf("schema=%s\n encErr=%v", s, encErr)}
	}
	// The schema's own wire must decode into any (internal consistency).
	var sink any
	if _, decErr := s.Decode(wire, &sink); decErr != nil {
		return true, &rtDivergence{label, "decode-own-wire",
			fmt.Sprintf("schema=%s\n decErr=%v", s, decErr)}
	}
	// ... and into a fresh typed value (the typed decode direction).
	dst := reflect.New(st)
	if _, decErr := s.Decode(wire, dst.Interface()); decErr != nil {
		return true, &rtDivergence{label, "typed-decode",
			fmt.Sprintf("schema=%s\n decErr=%v", s, decErr)}
	}
	if faithful {
		got := dst.Elem().Field(0).Interface()
		want := ptr.Elem().Field(0).Interface()
		if !reflect.DeepEqual(got, want) {
			return true, &rtDivergence{label, "faithful",
				fmt.Sprintf("got=%#v want=%#v\n schema=%s", got, want, s)}
		}
	}

	// The schema must also round-trip through the JSON wire (the package's other
	// codec): a build-accepts/JSON-encode-rejects asymmetry — a schema SchemaFor
	// builds whose value the JSON path then refuses, or that binary round-trips
	// but JSON cannot — is the binary-vs-JSON path-divergence class, invisible to
	// the binary checks above.
	jwire, jencErr := s.EncodeJSON(ptr.Interface())
	if jencErr != nil {
		return true, &rtDivergence{label, "json-encode-rejects",
			fmt.Sprintf("schema=%s\n jsonEncErr=%v", s, jencErr)}
	}
	var jsink any
	if decErr := s.DecodeJSON(jwire, &jsink); decErr != nil {
		return true, &rtDivergence{label, "json-decode-own-wire",
			fmt.Sprintf("schema=%s\n json=%s\n jsonDecErr=%v", s, jwire, decErr)}
	}
	jdst := reflect.New(st)
	if decErr := s.DecodeJSON(jwire, jdst.Interface()); decErr != nil {
		return true, &rtDivergence{label, "json-typed-decode",
			fmt.Sprintf("schema=%s\n json=%s\n jsonDecErr=%v", s, jwire, decErr)}
	}
	if faithful {
		got := jdst.Elem().Field(0).Interface()
		want := ptr.Elem().Field(0).Interface()
		if !reflect.DeepEqual(got, want) {
			return true, &rtDivergence{label, "json-faithful",
				fmt.Sprintf("got=%#v want=%#v\n json=%s\n schema=%s", got, want, jwire, s)}
		}
	}
	return true, nil
}

// shapePreservesFaithful reports whether wrapping a faithful leaf in this shape
// keeps the round-trip faithful under DeepEqual. Every shape does (a non-nil
// pointer, a one-element slice/array, a one-entry map all round-trip exactly);
// past-cap rejects at build so its faithfulness is moot. Kept explicit so a
// future lossy shape is a one-line opt-out rather than a silent false pass.
func shapePreservesFaithful(_ rtShape) bool { return true }

func TestGenerative_SchemaForRoundTripConsistency(t *testing.T) {
	leaves := rtLeaves()
	shapes := rtShapes()
	tags := rtTags()

	var (
		cells, builds, rejects, faithfulChecks int
		divs                                   []rtDivergence
		byKind                                 = map[string]int{}
	)

	for _, lf := range leaves {
		for _, sh := range shapes {
			ft := sh.wrap(lf.typ)
			faithful := lf.faithful && shapePreservesFaithful(sh)
			for _, tg := range tags {
				cells++
				label := fmt.Sprintf("leaf=%s shape=%s tag=%s", lf.label, sh.label, tg.label)
				built, div := rtRunCell(label, ft, tg.tag, faithful)
				if built {
					builds++
					if faithful {
						faithfulChecks++
					}
				} else {
					rejects++
				}
				if div != nil {
					byKind[div.kind]++
					divs = append(divs, *div)
				}
			}
		}
	}

	// Report the whole landscape together so a broken axis is one summary, not
	// thousands of lines.
	if len(divs) > 0 {
		kinds := make([]string, 0, len(byKind))
		for k := range byKind {
			kinds = append(kinds, k)
		}
		sort.Strings(kinds)
		var b strings.Builder
		fmt.Fprintf(&b, "%d/%d cells diverged from the round-trip-or-clean-reject oracle:\n", len(divs), cells)
		for _, k := range kinds {
			fmt.Fprintf(&b, "  %-16s %d\n", k, byKind[k])
		}
		const show = 40
		for i, d := range divs {
			if i >= show {
				fmt.Fprintf(&b, "  ... and %d more\n", len(divs)-show)
				break
			}
			fmt.Fprintf(&b, "\n[%s] %s\n  %s\n", d.kind, d.label, d.detail)
		}
		t.Fatal(b.String())
	}

	// Non-vacuity floor: both halves of the oracle must be substantially
	// exercised. If a generation change collapses the build set or the reject
	// set, the net silently stops testing one half — fail loudly instead.
	if builds < 400 || rejects < 400 || cells < 3000 {
		t.Fatalf("generator under-covered (a generation regression hides one half of the oracle): cells=%d builds=%d rejects=%d faithfulChecks=%d",
			cells, builds, rejects, faithfulChecks)
	}
	t.Logf("round-trip net: %d cells | %d built+round-tripped (%d faithful-value-checked) | %d clean rejects | 0 divergences",
		cells, builds, faithfulChecks, rejects)
}

// rtRealEntryPointCells drives the REAL generic SchemaFor[T] (not the
// reflect.Type replica) on the compile-time-nameable leaves, so the bulk net's
// reliance on schemaForType is bridged at the entry point under test: a bug in
// SchemaFor[T]'s own wrapper (top-level pointer deref, name/opts handling) that
// the replica does not share would be invisible to the StructOf cross but caught
// here. Each case applies the same round-trip-or-clean-reject oracle.
func TestGenerative_SchemaForRoundTripRealEntryPoint(t *testing.T) {
	check := func(name string, build func() (*Schema, error), mk func() any, faithful bool) {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic: %v\n%s", r, debug.Stack())
				}
			}()
			s, err := build()
			if err != nil {
				if strings.TrimSpace(err.Error()) == "" {
					t.Fatal("rejected with an empty error string")
				}
				return // clean reject
			}
			v := mk()
			wire, err := s.Encode(v)
			if err != nil {
				t.Fatalf("SchemaFor accepted but Encode rejects (build-accepts/encode-rejects):\n schema=%s\n err=%v", s, err)
			}
			var sink any
			if _, err := s.Decode(wire, &sink); err != nil {
				t.Fatalf("schema cannot decode its own wire: %v\n schema=%s", err, s)
			}
			if faithful {
				out := reflect.New(reflect.TypeOf(v).Elem()).Interface()
				if _, err := s.Decode(wire, out); err != nil {
					t.Fatalf("typed decode failed: %v", err)
				}
				if !reflect.DeepEqual(out, v) {
					t.Fatalf("faithful round-trip mismatch: got %#v want %#v", out, v)
				}
			}
		})
	}

	// struct, named-primitive, pointer chains, slice, array, map, interface,
	// recursive — exercised through the generic entry point.
	type Prim struct {
		A int32   `avro:"a"`
		B string  `avro:"b"`
		C float64 `avro:"c"`
	}
	check("struct", func() (*Schema, error) { return SchemaFor[Prim]() },
		func() any { return &Prim{A: 1, B: "x", C: 2} }, true)

	type NamedPrim struct {
		N rtNamedInt `avro:"n"`
	}
	check("named-primitive", func() (*Schema, error) { return SchemaFor[NamedPrim]() },
		func() any { return &NamedPrim{N: 7} }, true)

	type DoublePtr struct {
		V **int32 `avro:"v"`
	}
	check("multi-level-pointer", func() (*Schema, error) { return SchemaFor[DoublePtr]() },
		func() any { n := int32(3); p := &n; return &DoublePtr{V: &p} }, false)

	type AtCap struct {
		V *****int32 `avro:"v"`
	}
	check("ptr-at-cap", func() (*Schema, error) { return SchemaFor[AtCap]() },
		func() any {
			n := int32(9)
			p1 := &n
			p2 := &p1
			p3 := &p2
			p4 := &p3
			p5 := &p4
			return &AtCap{V: p5}
		}, false)

	type PastCap struct {
		V ******int32 `avro:"v"`
	}
	check("ptr-past-cap-rejects", func() (*Schema, error) { return SchemaFor[PastCap]() },
		func() any { return &PastCap{} }, false)

	type Slice struct {
		V []rtRecord `avro:"v"`
	}
	check("slice-of-record", func() (*Schema, error) { return SchemaFor[Slice]() },
		func() any { return &Slice{V: []rtRecord{{A: 1, B: "y"}}} }, true)

	type Map struct {
		V map[string]int64 `avro:"v"`
	}
	check("map", func() (*Schema, error) { return SchemaFor[Map]() },
		func() any { return &Map{V: map[string]int64{"k": 4}} }, true)

	type Iface struct {
		V any `avro:"v"`
	}
	check("interface-rejects", func() (*Schema, error) { return SchemaFor[Iface]() },
		func() any { return &Iface{} }, false)

	check("recursive-struct", func() (*Schema, error) { return SchemaFor[rtLinked]() },
		func() any { return &rtLinked{Val: 1, Next: &rtLinked{Val: 2}} }, false)

	type OneWayText struct {
		V rtStructMarshal `avro:"v"`
	}
	check("one-way-text-rejects", func() (*Schema, error) { return SchemaFor[OneWayText]() },
		func() any { return &OneWayText{} }, false)

	type JSONNum struct {
		V json.Number `avro:"v"`
	}
	check("json-number-rejects", func() (*Schema, error) { return SchemaFor[JSONNum]() },
		func() any { return &JSONNum{} }, false)
}

// ---- embed x leaf-inference composition ------------------------------------
//
// The main generator builds field types with reflect.StructOf, which cannot
// anonymously embed an UNNAMED per-leaf carrier, so the diamond/equal-depth
// embed shape over varying leaves is covered here with hand-declared carriers
// driven through the REAL SchemaFor[T]. The composition under test: a
// single-arm embed resolves the promoted field, then inferType runs on its
// LEAF type exactly as for a direct field — so the one-way-text refusal, the
// pointer-chain cap, the json.Number reject, and decimal acceptance must all
// compose through the embed. (TestGenerative_EmbedShapeWalkerAgreement owns
// embed FIELD SELECTION with an int32 leaf; this owns embed x leaf INFERENCE.)
// The diamond case pins that an equal-depth ambiguous collision still rejects
// independent of the leaf type.

// Special-leaf carriers: value-embedded or reject-at-build, so unexported is
// fine (a value embed of an unexported struct decodes; the refused cases never
// reach decode). The leaf each carries is the one whose inference must compose
// through the embed.
type rtEmbOneWay struct {
	V rtStructMarshal `avro:"v"` // non-string base, encode-only text -> refused
}
type rtEmbDeepPtr struct {
	V ******int32 `avro:"v"` // past the codec's pointer-unwrap cap -> refused
}
type rtEmbJSONNum struct {
	V json.Number `avro:"v"` // numeric-only carrier, no single Avro type -> refused
}
type rtEmbDecimal struct {
	V *big.Rat `avro:"v,decimal(9,2)"`
}

type rtTopDecimal struct{ rtEmbDecimal }
type rtTopOneWay struct{ rtEmbOneWay }
type rtTopOneWayPtr struct{ *rtEmbOneWay } // rejects at build, so the embed pointer is never decoded
type rtTopDeepPtr struct{ rtEmbDeepPtr }
type rtTopJSONNum struct{ rtEmbJSONNum }

// The plain / pointer-embed / diamond CONTROLS reuse the exported carriers from
// embed_shape_generative_test.go (GA, GL, GR over an int32 "N") — reconciling
// with TestGenerative_EmbedShapeWalkerAgreement (which owns field selection)
// rather than re-declaring int32 carriers. An exported carrier is required only
// for the pointer-embed control, where decode must allocate the embed.
type rtTopPlain struct{ GA }       // struct{ N int32 }
type rtTopPlainPtr struct{ *GA }   // exported pointer embed -> decode allocates it
type rtTopDiamond struct {         // "N" via GL.GBase.N and GR.GBase.N at equal depth -> ambiguous
	GL
	GR
}
type rtTopSingleArm struct{ GL } // one arm: "N" resolves

func TestGenerative_SchemaForEmbedLeafComposition(t *testing.T) {
	cases := []struct {
		name      string
		build     func() (*Schema, error)
		value     any  // non-nil when the schema is expected to build
		wantBuild bool
		faithful  bool // assert decode==value (off for decimal: *big.Rat repr)
	}{
		// Leaf inference composes through a single-arm embed exactly as for a
		// direct field: the refusals/cap/reject fire, decimal builds.
		{"decimal", func() (*Schema, error) { return SchemaFor[rtTopDecimal]() }, &rtTopDecimal{rtEmbDecimal{V: big.NewRat(3, 1)}}, true, false},
		{"one-way-text-refused", func() (*Schema, error) { return SchemaFor[rtTopOneWay]() }, nil, false, false},
		{"one-way-text-ptr-embed-refused", func() (*Schema, error) { return SchemaFor[rtTopOneWayPtr]() }, nil, false, false},
		{"deep-pointer-refused", func() (*Schema, error) { return SchemaFor[rtTopDeepPtr]() }, nil, false, false},
		{"json-number-refused", func() (*Schema, error) { return SchemaFor[rtTopJSONNum]() }, nil, false, false},
		// Controls (reused carriers): a clean embed builds + round-trips through
		// value and pointer embeds; an equal-depth diamond rejects.
		{"plain", func() (*Schema, error) { return SchemaFor[rtTopPlain]() }, &rtTopPlain{GA{N: 1}}, true, true},
		{"plain-ptr-embed", func() (*Schema, error) { return SchemaFor[rtTopPlainPtr]() }, &rtTopPlainPtr{&GA{N: 1}}, true, true},
		{"single-arm-resolves", func() (*Schema, error) { return SchemaFor[rtTopSingleArm]() }, &rtTopSingleArm{GL{GBase{N: 5}}}, true, true},
		{"diamond-ambiguous-refused", func() (*Schema, error) { return SchemaFor[rtTopDiamond]() }, nil, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := c.build()
			if !c.wantBuild {
				if err == nil {
					t.Fatalf("expected a clean build-time reject, got schema %s", s)
				}
				if strings.TrimSpace(err.Error()) == "" {
					t.Fatal("rejected with an empty error string")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected the embed to compose into a buildable schema, got: %v", err)
			}
			wire, err := s.Encode(c.value)
			if err != nil {
				t.Fatalf("embed composed but Encode rejects (build-accepts/encode-rejects): %v\n schema=%s", err, s)
			}
			var sink any
			if _, err := s.Decode(wire, &sink); err != nil {
				t.Fatalf("schema cannot decode its own wire: %v\n schema=%s", err, s)
			}
			out := reflect.New(reflect.TypeOf(c.value).Elem()).Interface()
			if _, err := s.Decode(wire, out); err != nil {
				t.Fatalf("typed decode of own wire failed: %v\n schema=%s", err, s)
			}
			if c.faithful && !reflect.DeepEqual(out, c.value) {
				t.Fatalf("embed round-trip mismatch: got %#v want %#v", out, c.value)
			}
		})
	}
}

// rtUnexpEmbed is value-decodable but, behind a pointer, names the one embed
// shape decode cannot fill: a field promoted through a nil UNEXPORTED embedded
// pointer, which reflect cannot allocate (it cannot Set an unexported field).
type rtUnexpEmbed struct {
	V int32 `avro:"v"`
}
type rtTopUnexpPtr struct{ *rtUnexpEmbed }

// TestRegression_SchemaForUnexportedEmbedPointerDecodeConstraint pins a
// DOCUMENTED decode constraint the round-trip net surfaced, NOT a SchemaFor
// divergence: SchemaFor builds a valid record for struct{ *unexportedEmbed },
// and Encode of a value whose embed is non-nil succeeds — but typed Decode into
// a fresh value must ALLOCATE the nil unexported embedded pointer to fill the
// promoted field, which reflect forbids. The codec rejects cleanly with a
// specific message rather than panicking or silently dropping the field. This
// is a general decode property of that Go shape (any hand-written schema hits
// it identically), so it is expected, not a build-accepts/encode-rejects bug;
// it also pins the otherwise-untested guard in reflect.go.
func TestRegression_SchemaForUnexportedEmbedPointerDecodeConstraint(t *testing.T) {
	s, err := SchemaFor[rtTopUnexpPtr]()
	if err != nil {
		t.Fatalf("SchemaFor must build a record for struct{ *unexportedEmbed }: %v", err)
	}
	wire, err := s.Encode(&rtTopUnexpPtr{&rtUnexpEmbed{V: 7}})
	if err != nil {
		t.Fatalf("Encode of a non-nil embed must succeed (the embed is read, not allocated): %v", err)
	}
	var sink any
	if _, err := s.Decode(wire, &sink); err != nil {
		t.Fatalf("decode into any must succeed: %v", err)
	}
	// Typed decode into a fresh value must error cleanly (cannot allocate the
	// nil unexported embedded pointer), never panic or silently drop.
	_, err = s.Decode(wire, &rtTopUnexpPtr{})
	if err == nil {
		t.Fatal("typed decode through a nil unexported embedded pointer must error, not silently drop the promoted field")
	}
	if !strings.Contains(err.Error(), "nil unexported embedded pointer") {
		t.Fatalf("decode error should name the unexported-embedded-pointer constraint, got: %v", err)
	}
}

// ---- neutering record (non-vacuity proof) ----------------------------------
//
// The round-trip oracle is proven to FAIL when each historical SchemaFor fix is
// reverted in inferType (schema_for.go). Counts below are MEASURED over the
// 7326-cell leaf x shape x tag cross by switching the divergence report from
// t.Fatal to a count. With every fix intact, divergences == 0.
//
//	NEUTER-1  One-way-text refusal (the text-interface x shape axis). Replace the
//	          inferType text block's enc/dec switch with an unconditional
//	          `return "string", nil` (revert 962f7b6):
//	            48 cells red (24 encode-rejects + 24 typed-decode) — the
//	            rtStructMarshal (encode-only) and rtStructUnmarshal (decode-only)
//	            non-string-base leaves now infer "string" and build, then Encode
//	            (decode-only) or typed Decode (encode-only) fails, across all 8
//	            leaf-materializing shapes (direct, ptr, ptr2, ptrAtCap, slice,
//	            array2, map, slicePtr) x the 3 string-building tags (none, name,
//	            alias). The string- and []byte-base one-directional leaves stay
//	            GREEN (their kind covers the missing direction), and the inline
//	            tag flattens the struct rather than inferring a string — proving
//	            the refusal is scoped to exactly the non-round-trippable types.
//
//	NEUTER-2  json.Number reject. Make the `case jsonNumberType:` arm return
//	          `"string", nil` (revert the json.Number fix):
//	            68 cells red, all encode-rejects, all on the json.Number leaf —
//	            it infers "string" (its Kind) and builds for every non-malformed
//	            tag (the case returns before the logical/text checks, so even
//	            uuid/timestamp-*/decimal build as a bare string), but the codec
//	            rejects json.Number against a string schema on encode, across
//	            every shape that materializes the leaf.
//
//	NEUTER-3  Pointer-chain cap. Remove the `ptrChain >= maxIndirectDepth`
//	          refusal in inferType's pointer arm:
//	            179 cells red, all encode-rejects, all on the ptrPastCap shape
//	            (6 pointer levels) — every leaf that itself builds now produces a
//	            ["null",T], then Encode of the non-nil sampled value fails with
//	            errIndirectDeep. ptr2 / ptrAtCap (within the cap) stay GREEN, so
//	            the boundary is exactly at the codec's unwrap depth. (The
//	            depth>=maxDepth ceiling still prevents any stack overflow, so the
//	            measurement is a clean count, not a crash.)
//
// The three counts are unchanged whether or not the JSON wire is exercised: each
// of these bugs manifests on the BINARY path, which rtRunCell checks first and
// which short-circuits the cell — so the JSON checks add coverage only for a
// hypothetical binary-passes/JSON-fails bug (none of the three are), and the
// full net currently finds zero such cells.
