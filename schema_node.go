package avro

import (
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

// SchemaNode is a read-write representation of an Avro schema. It can be
// obtained from a parsed schema via [Schema.Root], or constructed directly
// and converted to a [*Schema] via the [SchemaNode.Schema] method.
//
// The Type field determines which other fields are relevant:
//
//   - Primitives (null, boolean, int, long, float, double, string, bytes):
//     LogicalType, Precision, Scale, and Props are optional. Other fields
//     are ignored.
//   - record/error: Name and Fields are required. Namespace, Doc, and Props
//     are optional.
//   - enum: Name and Symbols are required. Namespace, Doc, and Props are
//     optional.
//   - array: Items is required.
//   - map: Values is required.
//   - fixed: Name and Size are required. LogicalType, Precision, Scale,
//     Namespace, and Props are optional.
//   - union: Branches lists the member schemas.
//
// A named type (record, enum, fixed) that has already been defined
// elsewhere in the schema can be referenced by setting Type to its full
// name (e.g. com.example.Address) with no other fields.
//
// In a tree obtained from [Schema.Root], such references also work the
// other way around: converting ANY node of the tree with
// [SchemaNode.Schema] resolves references against the schema the tree
// came from, so a field's type, a union branch, or any deeper node
// converts to a working schema even when the referenced definition lives
// outside the extracted node. Hand-built trees have no enclosing schema:
// there, every referenced name must be defined somewhere in the tree
// being converted, or Schema returns an error.
type SchemaNode struct {
	Type        string // Avro type or named type reference
	LogicalType string // e.g. date, timestamp-millis, decimal, uuid; empty if none (or if the attribute's value is not a string — see Props)

	Name string // name for record, enum, fixed

	// Namespace is the named type's RESOLVED namespace. [Schema.Root]
	// populates it for every named type: a child that inherits its
	// enclosing namespace surfaces that namespace here, and "" always
	// means the null namespace, never "inherit from the parent". When
	// constructing a SchemaNode by hand, set Namespace explicitly or
	// leave "" for the null namespace — [SchemaNode.Schema] emits a
	// "namespace":"" escape when a null-namespace type sits inside a
	// namespaced scope, so the distinction survives the round trip.
	// A dotted Name carries its own namespace and takes precedence
	// over this field.
	Namespace string

	Aliases []string // alternate names for named types (record, enum, fixed)
	Doc     string   // documentation string

	Fields   []SchemaField // record fields
	Items    *SchemaNode   // array element schema
	Values   *SchemaNode   // map value schema
	Branches []SchemaNode  // union member schemas
	Symbols  []string      // enum symbols
	Size     int           // fixed byte size

	EnumDefault    string // default symbol for enum schema evolution
	HasEnumDefault bool   // true if an enum default is defined

	// Precision and Scale are the decimal logical type's parameters, set
	// (and validated) exactly when LogicalType is "decimal" on a bytes or
	// fixed carrier. A stray "precision"/"scale" attribute anywhere else —
	// no logical type, an unknown one, a non-decimal one, or a decimal on
	// a carrier it soft-drops from — is inert metadata surfaced in Props,
	// matching the field level.
	Precision int // decimal precision
	Scale     int // decimal scale

	// Props holds custom (non-reserved) schema attributes — anything
	// in the schema JSON that is not a standard Avro field (e.g.
	// namespace-prefixed metadata like "com.example.tag"). A reserved
	// structural key whose value does not parse as that key's schema
	// shape, sitting on a kind that does not bind the key (a stray
	// "items":3 on an "int"), is inert metadata and is reported here
	// verbatim as its ONLY surface — the matching structural field stays
	// zero; a schema-shaped stray body instead surfaces as-written on
	// the matching structural field (Items / Values / Fields). A
	// logicalType attribute whose value is not a JSON string is likewise
	// inert and reported here verbatim (no value but a string can name a
	// logical).
	//
	// Values use the natural Go types from JSON: string, bool, nil,
	// []any, map[string]any, plus int64 for whole numbers and float64
	// for fractional. A number is preserved as json.Number when it
	// cannot be represented otherwise: a whole number too large for
	// int64, or a fractional literal too long to parse as a float
	// (over 1024 bytes), whose digits are kept verbatim rather than
	// rounded. Whole-valued exponents collapse to int64 (1e3 reads as
	// int64(1000)), and exponents that overflow float64 give ±Inf.
	//
	// math.NaN() stored in Props re-reads as string "NaN" after
	// round-tripping through Schema() and Root(), because JSON has
	// no NaN literal. ±Inf round-trips correctly as float64(±Inf).
	Props map[string]any

	// refTarget is set only by [Schema.Root], on nodes whose Type is a
	// name reference, and points at the referenced definition inside the
	// same Root tree. [SchemaNode.Schema] reads it to emit the referenced
	// definition when the tree being converted does not define the name
	// itself, which is what lets a node extracted at ANY depth of a Root
	// tree convert to a working schema: the definition its references
	// need travels with the node. It is invisible otherwise: hand-built
	// nodes leave it nil (a dangling reference stays a loud parse error),
	// struct copies and slice extractions carry it, and a node rebuilt
	// field-by-field drops it (the rebuilt node then behaves hand-built).
	refTarget *SchemaNode

	// present records which attributes were WRITTEN, which their own
	// fields cannot: an attribute whose body is the field's zero — `""`,
	// `[]`, `0` — leaves the field indistinguishable from one nobody
	// wrote, so `Doc != ""` / `len(Aliases) > 0` / `Size != 0` each mean
	// two things at once and drop exactly the value that was written as
	// the zero. It is hidden because a caller composing a SchemaNode by
	// hand has no empty-doc to express — the distinction exists only for
	// text that came from a parse — and because an exported companion
	// would be new API for a case the references reach without one.
	//
	// Presence is recorded ONLY where the attribute is CONSUMED, and it is
	// consulted per attribute rather than as one blanket rule, because the
	// authority differs per PLACEMENT:
	//
	//   - Where Apache Avro has the placement, its emission condition
	//     governs, and those conditions differ from each other: doc emits
	//     when non-null (Schema.java:1039/:1154/:1367/:1062) so an empty
	//     doc survives, while aliases emits when non-EMPTY (:886, :1070)
	//     so an empty alias list is dropped even on a kind that binds it.
	//   - Where NEITHER reference has the placement — a structural key on
	//     a kind that does not bind it, which Apache Avro skips wholesale
	//     as reserved and fastavro keeps wholesale as a property — this
	//     package's own stray-routing posture governs: as-written is the
	//     key's ONLY surface, so it must survive the rebuild rather than
	//     reaching neither surface.
	present presenceSet

	// refNS is the namespace scope refTarget was resolved in, recorded
	// alongside the stamp because the two are only meaningful together:
	// whether Type still names the target is a question about the scope the
	// reference was WRITTEN in, and an extracted node is re-rooted at the
	// null namespace, losing it. Set and read only with refTarget
	// (nodeRefTargetAgrees); nil stamp means the value is unused.
	refNS string
}

// SchemaField represents a field in an Avro record schema.
type SchemaField struct {
	Name string     // field name
	Type SchemaNode // field schema

	// Default is the field's default value, present when HasDefault is
	// true. The Go type matches the schema:
	//
	//   - int schemas give int32, long schemas give int64. Out-of-range
	//     defaults are rejected at parse.
	//   - float schemas give float32, double schemas give float64.
	//     Overflows narrow to ±Inf; NaN, ±Inf, and a float-syntax negative
	//     zero ("-0.0") round-trip correctly. An integer-syntax "-0" is the
	//     sign-less integer 0 and surfaces as +0.0 (matching Java/fastavro),
	//     even though the wire encoder writes -0.0 for that literal.
	//   - string and enum schemas give string.
	//   - bytes and fixed schemas give []byte, already decoded from
	//     the JSON spec's codepoint-per-byte form.
	//   - record, array, and map schemas give map[string]any or []any
	//     respectively, with each leaf following these same rules.
	//
	// Union defaults pick the first branch that accepts the value; the
	// Go type tells you which branch was chosen. For ["float","int"]
	// with default 42 you get float32(42) because float matches first.
	//
	// Unlike Props, Default for numeric fields is never json.Number:
	// schema parse rejects defaults that do not fit the declared type.
	Default any

	HasDefault bool     // true if a default value is defined in the schema
	Aliases    []string // field aliases for schema evolution
	Order      string   // sort order: "ascending" (default), "descending", or "ignore"
	Doc        string   // documentation string

	// docSet is the field-level twin of [SchemaNode]'s: a field's "doc"
	// key written as the empty string is a written doc, and Apache Avro
	// emits it (Schema.java:1062 asks f.doc() != null). See the SchemaNode
	// field for why the state is hidden.
	docSet bool

	// Props holds custom (non-reserved) field properties. Numbers decode as
	// in [SchemaNode.Props]. Field-level "logicalType", "precision", and
	// "scale" appear here as written: the wire-side lift is a codec
	// concession that never removes them from this surface, and an
	// unconsumed precision/scale — no field logicalType, a non-decimal
	// one, or a decimal whose lift target is not a bytes/fixed carrier —
	// is an ordinary property whatever its value's JSON shape (only a
	// consumed placement shape-validates the pair at parse).
	Props map[string]any
}

// Schema parses the SchemaNode into a [*Schema] that can be used for
// encoding and decoding. Returns an error if the node is invalid.
//
// Named types (record, enum, fixed) that appear multiple times in the
// tree are automatically deduplicated, matched by FULLNAME: the first
// occurrence emits the full definition and subsequent occurrences emit
// the fullname as a reference. Two types sharing a short name across
// namespaces are distinct and both emit full definitions.
//
// A node extracted from a [Schema.Root] tree may contain name references
// whose definitions live elsewhere in the enclosing schema — an earlier
// field, a prior [SchemaCache] parse, or the enclosing type itself for a
// recursive schema. Those resolve automatically: the referenced
// definition is emitted at the reference's first occurrence, so the
// result is self-contained and needs neither the enclosing schema nor
// any cache. A name the tree defines itself always wins over the
// enclosing schema's definition, and custom properties on a wrapped
// reference ride onto the emitted definition (reserved attributes at the
// usage site do not survive, matching the SchemaCache splice). Hand-built
// nodes carry no enclosing schema, so a reference the tree does not
// define is an error there.
//
// opts are passed through to the internal [Parse]: a schema originally
// parsed with [SchemaOpt]s that change what Parse accepts or wires —
// [WithLaxNames] for non-standard names, [CustomType] registrations —
// needs the same opts here, or the rebuilt schema fails to parse (lax
// names) or silently lacks the custom wiring.
func (n *SchemaNode) Schema(opts ...SchemaOpt) (*Schema, error) {
	d := &deduper{
		defined: make(map[string]*SchemaNode),
		visited: make(map[*SchemaNode]struct{}),
	}
	tree := n.toJSONDedup(d)
	if d.err != nil {
		return nil, d.err
	}
	b, err := marshalSchemaTree(tree)
	if err != nil {
		return nil, fmt.Errorf("avro: marshaling schema node: %w", err)
	}
	return Parse(string(b), opts...)
}

// deduper tracks named type definitions during toJSONDedup and records
// conflicting redefinitions. It also detects cycles introduced via
// *SchemaNode Items/Values pointers (which are the only way a SchemaNode
// tree can have true cycles — Fields and Branches are value slices).
type deduper struct {
	defined map[string]*SchemaNode   // fullname → first definition's node
	visited map[*SchemaNode]struct{} // seen *SchemaNode pointers (cycle detection)
	err     error                    // first conflict or cycle encountered

	// localNames holds the fullname of every named type DEFINED somewhere
	// in the tree being converted, collected up front (collectLocalNames).
	// The refTarget splice consults it so a reference whose definition is
	// present in the tree — before OR after the reference position — is
	// emitted as-written and binds to that local definition on re-parse
	// (forward references included), exactly as it does today. Only a
	// reference to a name the tree nowhere defines splices the stamped
	// target in.
	localNames map[string]bool
}

// Root returns a SchemaNode tree describing the parsed schema. All
// metadata is preserved (doc strings, namespaces, custom properties,
// numeric defaults). See [SchemaNode.Props] and [SchemaField.Default]
// for how values decode.
//
// Reserved Avro attribute names (such as "type", "name", "namespace",
// "doc", "aliases") are matched only by their exact lowercase spelling,
// as in the Avro reference implementations. A key differing from a
// reserved name only by letter case (for example "Aliases") is an
// ordinary custom property: it never binds the attribute, and it is
// reported in [SchemaNode.Props] verbatim. Parsing applies the same
// rule, so the metadata reported here stays consistent with the parsed
// schema and the encoded wire — in particular, a schema whose only
// spelling of a structural key is a case variant (say "ITEMS" on an
// array) fails Parse, because the structural attribute is absent.
//
// A field written in the flat (goavro-style) format — a bare string
// complex-kind type with the kind's defining key (symbols, items, values,
// fields, size) alongside the field's own keys — is described post-lift,
// exactly as it parses: the field's type is the lifted nested definition
// (named after the field for record/error/enum/fixed), and the keys the
// lift routed into the type (the defining key, doc, logicalType, precision,
// scale, and custom properties) appear on the type node rather than in
// [SchemaField.Props]. [SchemaNode.Schema] rebuilds the nested form, which
// parses to the same schema.
//
// Every node of the returned tree converts back to a usable [*Schema]
// via [SchemaNode.Schema], including nodes whose type is a name
// reference: the tree carries the schema's named-type definitions with
// it, so extracting a field's type, a union branch, or any deeper node
// yields a self-contained schema.
//
// Root re-parses the JSON on each call. Cache the result if you need
// to access it repeatedly (e.g. in a per-message processing loop).
func (s *Schema) Root() *SchemaNode {
	raw, err := unmarshalAnyPreservePrecision([]byte(s.full))
	if err != nil {
		panic("avro: Schema.Root: invalid stored JSON: " + err.Error())
	}
	// One shape memo for the whole walk: the stray gates validate a stray
	// body's schema shape once per node, and a nested-stray schema nests
	// those bodies, so a shared memo keeps the walk linear instead of
	// re-validating each subtree once per enclosing level.
	n := nodeFromJSON(raw, "", make(strayShapeMemo))
	table := fixupNameRefDefaults(&n)
	// Stamp every name-reference node with its resolved target so a
	// sub-tree extracted from this Root converts via [SchemaNode.Schema]
	// even when the referenced definition lives outside the extraction.
	// The table is the same one the default fixup resolved through, so the
	// two surfaces cannot bind a reference differently.
	stampNameRefs(&n, table, "")
	return &n
}

// toJSONDedup is like toJSON but deduplicates named types. The first
// occurrence of a named type (record, enum, fixed) emits the full
// definition; subsequent occurrences emit the name as a reference.
func (n *SchemaNode) toJSONDedup(d *deduper) any {
	b := newWalkBudget()
	d.localNames = make(map[string]bool)
	collectLocalNames(n, d.localNames, make(map[*SchemaNode]struct{}), 0)
	return n.toJSONWalk(d.visited, d, "", 0, &b, false)
}

// jsonSerializableValue returns v with three Avro-JSON-specific shape
// fixups applied (directly or under map[string]any / []any container
// layers):
//
//  1. ±Inf float → [json.Number]("±1e1000") literal that re-parses to
//     the same value via [parseFloatAcceptOverflow] (schema.go). The
//     inverse of normalizeJSONNumber's ErrRange-with-Inf accept.
//     Required because Go's standard JSON encoder unconditionally
//     rejects ±Inf and NaN, so a SchemaNode obtained from [Schema.Root]
//     for a schema whose Default / Props normalized an exponent-form
//     overflow to ±Inf cannot otherwise round-trip through
//     [SchemaNode.Schema].
//
//  2. NaN float → JSON string "NaN". RFC 8259 has no NaN literal, so
//     no JSON number can encode NaN (compare item 1's ±Inf overflow
//     trick — no analogue exists for NaN). Re-parse recovers
//     float64(NaN) only for SchemaField.Default of a float / double
//     field (via coerceMetadataDefault → defaultAsFloat's string
//     arm; the schema type drives the coercion). For SchemaNode.Props
//     and SchemaField.Props the string survives unchanged —
//     auto-coercing literal "NaN" in normalizeJSONValue would silently
//     reinterpret user-intentional string Props (Parse can never
//     produce NaN in Props; a hand-written {"x":"NaN"} is the user
//     storing a string). User-facing note lives on SchemaNode.Props.
//
//  3. []byte → codepoint-per-byte string (each byte 0x00-0xFF becomes
//     a rune at the same code point). The inverse of
//     [avroJSONBytesToBytes] / [coerceMetadataDefault]'s bytes/fixed
//     arm: that arm materializes Default as []byte (the wire form);
//     re-emitting requires putting it back in the Avro JSON
//     codepoint-string form (the spec form for bytes/fixed defaults).
//     A plain []byte marshal would base64-encode the slice
//     ("AQID" for {0x01,0x02,0x03}) which the Avro parser would then
//     re-read as raw bytes [0x41,0x51,0x49,0x44] — a silent value
//     corruption breaking [SchemaNode.Schema] round-trips for any
//     bytes/fixed default. Programmatically-constructed Props with
//     []byte values also get the codepoint encoding (Avro's
//     convention), not base64; users who need base64 in Props should
//     pre-encode to a string.
//
// Container values (map[string]any, []any) are deep-copied only when a
// descendant requires conversion, so the common no-fixup case is
// allocation-free and the user's SchemaNode storage is never mutated.
func jsonSerializableValue(v any) any {
	if !needsJSONFixup(v) {
		return v
	}
	return applyJSONFixup(v)
}

// isNegativeZero reports whether f is IEEE-754 negative zero (−0.0). Distinct
// from +0.0 only by the sign bit; both compare == 0.
func isNegativeZero(f float64) bool {
	return f == 0 && math.Signbit(f)
}

func needsJSONFixup(v any) bool {
	switch tv := v.(type) {
	case float64:
		return math.IsInf(tv, 0) || math.IsNaN(tv) || isNegativeZero(tv)
	case float32:
		return math.IsInf(float64(tv), 0) || math.IsNaN(float64(tv)) || isNegativeZero(float64(tv))
	case []byte:
		return true
	case map[string]any:
		for _, val := range tv {
			if needsJSONFixup(val) {
				return true
			}
		}
	case []any:
		return slices.ContainsFunc(tv, needsJSONFixup)
	case nil, string, bool, json.Number, int, int32, int64:
	default:
		return needsJSONFixupKind(v)
	}
	return false
}

var jsonMarshalerType = reflect.TypeFor[json.Marshaler]()

// treeValueMarshalOpaque reports whether v's JSON form is self-defined —
// its own MarshalJSON/MarshalText method, or json.Number (whose
// number-not-string marshal encoding/json special-cases internally). Such
// values keep their marshal semantics untouched: the fixups and the
// canonicalizing render copy leave them alone, and the composition
// walkers treat them as opaque leaves that Parse reads from the marshal.
// The assertions use the value's own method set, matching what
// encoding/json consults for an interface-carried (unaddressable) value.
func treeValueMarshalOpaque(v any) bool {
	switch v.(type) {
	case json.Number, json.Marshaler, encoding.TextMarshaler:
		return true
	}
	return false
}

// canonicalByteSliceKind reports whether t marshals as a raw byte string:
// a slice with uint8-kind elements whose element type supplies no marshal
// of its own — mirroring encoding/json's byte-slice rule, which consults
// the element's POINTER method set because slice elements are addressable.
func canonicalByteSliceKind(t reflect.Type) bool {
	if t.Kind() != reflect.Slice || t.Elem().Kind() != reflect.Uint8 {
		return false
	}
	p := reflect.PointerTo(t.Elem())
	return !p.Implements(jsonMarshalerType) && !p.Implements(textMarshalerType)
}

// sliceElemMarshalPositionDependent reports whether moving a t-typed
// slice/array element into an interface box would CHANGE its marshal: a
// pointer-receiver-only marshaler is reachable from an addressable element
// in place but not from an interface-carried copy, so containers of such
// elements stay opaque rather than being canonicalized into a
// semantically different []any.
func sliceElemMarshalPositionDependent(t reflect.Type) bool {
	p := reflect.PointerTo(t)
	if !p.Implements(jsonMarshalerType) && !p.Implements(textMarshalerType) {
		return false
	}
	return !t.Implements(jsonMarshalerType) && !t.Implements(textMarshalerType)
}

// canonicalStringKeyMap reports whether t's keys canonicalize to their
// plain string value: every string-KIND key does. encoding/json's key
// resolver checks the string kind FIRST — a string-kind key marshals as
// its raw string and any MarshalText on it is not consulted (executed;
// jsonv2 flips that precedence, so pinning the raw string here keeps the
// composed schema identical across toolchains). json.Marshaler is never
// consulted for keys on either toolchain. NON-string-kind keys are the
// opposite: their MarshalText output is the key under both
// implementations (executed), so those maps stay marshal-opaque
// image-owners.
func canonicalStringKeyMap(t reflect.Type) bool {
	return t.Key().Kind() == reflect.String
}

// needsJSONFixupKind extends the fixup detection to caller-typed values by
// reflect kind, so a named `type B []byte` or a named float behaves like
// the canonical twin its marshal is indistinguishable from. Marshal-opaque
// values (treeValueMarshalOpaque) are exempt — their marshal wins. One
// deliberate asymmetry: the numeric-PRESERVING fixups (±Inf, -0.0) apply
// to named float kinds, but the type-CHANGING NaN→"NaN"-string conversion
// stays canonical-only — a named float NaN keeps json.Marshal's loud
// unsupported-value error rather than being silently stringified.
func needsJSONFixupKind(v any) bool {
	if treeValueMarshalOpaque(v) {
		return false
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Float64, reflect.Float32:
		f := rv.Float()
		return math.IsInf(f, 0) || isNegativeZero(f)
	case reflect.Slice:
		if canonicalByteSliceKind(rv.Type()) {
			return true
		}
		fallthrough
	case reflect.Array:
		if sliceElemMarshalPositionDependent(rv.Type().Elem()) {
			return false
		}
		for i := range rv.Len() {
			if needsJSONFixup(rv.Index(i).Interface()) {
				return true
			}
		}
	case reflect.Map:
		if !canonicalStringKeyMap(rv.Type()) {
			return false
		}
		for it := rv.MapRange(); it.Next(); {
			if needsJSONFixup(it.Value().Interface()) {
				return true
			}
		}
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return false
		}
		return needsJSONFixup(rv.Elem().Interface())
	}
	return false
}

func applyJSONFixup(v any) any {
	switch tv := v.(type) {
	case float64:
		if math.IsInf(tv, 1) {
			return json.Number("1e1000")
		}
		if math.IsInf(tv, -1) {
			return json.Number("-1e1000")
		}
		if math.IsNaN(tv) {
			return "NaN"
		}
		if isNegativeZero(tv) {
			// encoding/json.Marshal renders -0.0 as integer-syntax "-0",
			// which re-parses to a sign-less integer 0 (+0.0 on the wire) —
			// silently flipping the rebuilt schema's default away from the
			// original -0.0. Emit float syntax so Root().Schema() round-trips
			// the sign (matching the wire and Java/fastavro).
			return json.Number("-0.0")
		}
		return tv
	case float32:
		if math.IsInf(float64(tv), 1) {
			return json.Number("1e1000")
		}
		if math.IsInf(float64(tv), -1) {
			return json.Number("-1e1000")
		}
		if math.IsNaN(float64(tv)) {
			return "NaN"
		}
		if isNegativeZero(float64(tv)) {
			return json.Number("-0.0")
		}
		return tv
	case []byte:
		return bytesToAvroJSONString(tv)
	case map[string]any:
		out := make(map[string]any, len(tv))
		for k, val := range tv {
			out[k] = applyJSONFixup(val)
		}
		return out
	case []any:
		out := make([]any, len(tv))
		for i, val := range tv {
			out[i] = applyJSONFixup(val)
		}
		return out
	}
	return applyJSONFixupKind(v)
}

// applyJSONFixupKind is needsJSONFixupKind's conversion twin: it rebuilds
// the caller-typed value in canonical shape with the same fixups the
// exact-type arms apply, leaving marshal-opaque values and the
// no-canonical-twin residuals untouched. A named float NaN deliberately
// falls through un-fixed (needsJSONFixupKind never selects it) so the
// marshal error stays loud.
func applyJSONFixupKind(v any) any {
	if v == nil || treeValueMarshalOpaque(v) {
		return v
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Float64, reflect.Float32:
		f := rv.Float()
		switch {
		case math.IsInf(f, 1):
			return json.Number("1e1000")
		case math.IsInf(f, -1):
			return json.Number("-1e1000")
		case isNegativeZero(f):
			return json.Number("-0.0")
		}
		return v
	case reflect.Slice:
		if canonicalByteSliceKind(rv.Type()) {
			b := make([]byte, rv.Len())
			for i := range b {
				b[i] = byte(rv.Index(i).Uint())
			}
			return bytesToAvroJSONString(b)
		}
		fallthrough
	case reflect.Array:
		if sliceElemMarshalPositionDependent(rv.Type().Elem()) {
			return v
		}
		out := make([]any, rv.Len())
		for i := range out {
			out[i] = applyJSONFixup(rv.Index(i).Interface())
		}
		return out
	case reflect.Map:
		if !canonicalStringKeyMap(rv.Type()) {
			return v
		}
		out := make(map[string]any, rv.Len())
		for it := rv.MapRange(); it.Next(); {
			out[it.Key().String()] = applyJSONFixup(it.Value().Interface())
		}
		return out
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return nil
		}
		return applyJSONFixup(rv.Elem().Interface())
	}
	return v
}

// maxSchemaJSONNodes bounds the TOTAL number of nodes one SchemaNode→JSON walk
// emits — every structural node in toJSONWalk PLUS every node inside every Props
// value and SchemaField.Default — shared across a single [SchemaNode.Schema] /
// [SchemaNode.toJSON] call. It is the expansion-axis companion to
// maxSchemaJSONDepth's depth axis. The depth bound caps the longest container
// PATH; it cannot see a shared-reference DAG: the same *SchemaNode reached via a
// node's Items AND Values pointer, or the same sub-value reached via two map
// keys ({"a":x,"b":x} repeated per level), is tiny in memory yet fans out into
// an exponential TREE when serialized, because neither toJSONWalk nor
// json.Marshal memoizes shared references — both re-expand every shared subtree.
// A ~40-node DAG demands 2^40 emitted nodes and hangs/OOMs the process before
// Schema's eventual Parse (whose maxSchemaJSONDepth pre-scan would reject the
// JSON) ever runs. Counting emitted nodes against one budget bounds the fan-out
// AND json.Marshal's subsequent cost (it processes the same expanded tree). The
// cap sits far above any real schema's node count — a tree this large is itself
// pathological — so a usable tree is never rejected; an over-budget walk stops
// with a clean error (dedup path) or a truncated subtree (bare path) instead of
// crashing, exactly like the depth bound.
const maxSchemaJSONNodes = 1 << 20

// maxSchemaJSONBytes bounds the TOTAL bytes of scalar payload one
// SchemaNode→JSON walk emits — every type / name / namespace / doc /
// logicalType / enum-default string, every enum symbol and alias, every Props
// key and string / []byte Props-or-Default value — shared across a single walk
// exactly like maxSchemaJSONNodes. It is the output-SIZE companion to the
// node-COUNT budget, and the cell the five depth/node rounds all missed.
//
// The node budget caps how many nodes the intermediate any-tree holds; it
// cannot see a leaf's SIZE, because the tree stores every string and []string
// BY REFERENCE (assigning n.Doc or n.Symbols is O(1) and charges exactly one
// node) while json.Marshal re-expands each one. So a single multi-megabyte
// Doc / Symbols, or a modest one shared across many distinct nodes (K nodes
// each emitting one L-byte shared string is O(K+L) in memory but K*L in the
// marshaled output, because Go strings/slices share backing storage and
// json.Marshal memoizes nothing), blows the output up past memory while the
// node count stays tiny — neither the depth nor the node budget catches it.
// Charging emitted bytes against one shared budget bounds json.Marshal's output
// (and the dedup conflict-comparison marshals) the same way the node budget
// bounds the fan-out. The cap sits far above any real schema's serialized size
// — a schema this large is itself pathological — so a usable tree is never
// rejected; an over-budget walk stops with a clean error (dedup path) or a
// truncated payload (bare path) instead of hanging, exactly like the depth and
// node bounds.
const maxSchemaJSONBytes = 1 << 26

// walkBudget is the shared per-walk resource budget threaded through toJSONWalk
// and valueWalkLimit. Both axes are decremented across the WHOLE walk
// (structural nodes plus every Props/Default value plus the dedup
// conflict-comparison marshals), so no single channel can blow either:
//
//   - nodes: the COUNT of emitted JSON nodes (objects plus array elements,
//     including every enum symbol and alias). Bounds the intermediate any-tree
//     and the walk's own fan-out — a shared-reference DAG re-expands per path
//     (see maxSchemaJSONNodes).
//   - bytes: the SIZE in bytes of every emitted scalar payload. Bounds
//     json.Marshal's output — leaves are stored by reference (O(1), invisible
//     to the node count) and re-expanded by json.Marshal (see
//     maxSchemaJSONBytes).
type walkBudget struct {
	nodes int
	bytes int
}

// newWalkBudget returns a fresh full budget for one SchemaNode→JSON walk.
func newWalkBudget() walkBudget {
	return walkBudget{nodes: maxSchemaJSONNodes, bytes: maxSchemaJSONBytes}
}

// takeNode charges one emitted node, reporting false when the node budget is
// already exhausted (this path never drives it below zero).
func (b *walkBudget) takeNode() bool {
	if b.nodes <= 0 {
		return false
	}
	b.nodes--
	return true
}

// takeNodes charges n emitted nodes (a slice's elements). When n exceeds the
// remainder the budget is driven to zero and false is reported, so the caller
// truncates rather than handing json.Marshal a giant array.
func (b *walkBudget) takeNodes(n int) bool {
	if n > b.nodes {
		b.nodes = 0
		return false
	}
	b.nodes -= n
	return true
}

// takeBytes charges n emitted payload bytes. When n exceeds the remainder the
// budget is driven negative (so toJSONWalk's top-of-call check and
// valueWalkLimit both observe exhaustion) and false is reported, so the
// over-large payload is never handed to json.Marshal.
func (b *walkBudget) takeBytes(n int) bool {
	if n > b.bytes {
		b.bytes = -1
		return false
	}
	b.bytes -= n
	return true
}

// emitString charges a structural scalar string's bytes, returning it for
// emission, or "" (recording the over-budget error) when the byte budget is
// exhausted — so json.Marshal never copies a payload past the bound.
func (b *walkBudget) emitString(d *deduper, s string) string {
	if b.takeBytes(len(s)) {
		return s
	}
	d.fail(errSchemaTreeBytes())
	return ""
}

// emitStrings charges a structural []string payload — its element COUNT against
// the node budget (each element becomes an emitted array node) and its content
// bytes against the byte budget — returning it for emission, or an empty slice
// (recording the over-budget error) when either is exhausted. The truncation is
// deterministic (always empty) so the dedup conflict comparison stays
// meaningful; an exhausted budget is reported by the post-comparison check, not
// as a spurious body conflict (asymmetric truncation could otherwise make
// identical bodies compare unequal — the same hazard toJSONShared addresses).
func (b *walkBudget) emitStrings(d *deduper, ss []string) []string {
	if !b.takeNodes(len(ss)) {
		d.fail(errSchemaTreeNodes())
		return []string{}
	}
	total := 0
	for _, s := range ss {
		total += len(s)
	}
	if !b.takeBytes(total) {
		d.fail(errSchemaTreeBytes())
		return []string{}
	}
	return ss
}

// fail records err as the deduper's first error. It is a no-op on the bare
// (d == nil) walk, which truncates over-budget output silently, and never
// overwrites an earlier error.
func (d *deduper) fail(err error) {
	if d != nil && d.err == nil {
		d.err = err
	}
}

func errSchemaTreeNodes() error {
	return fmt.Errorf("avro: SchemaNode tree expands to more than the supported %d nodes", maxSchemaJSONNodes)
}

func errSchemaTreeBytes() error {
	return fmt.Errorf("avro: SchemaNode tree expands to more than the supported %d bytes", maxSchemaJSONBytes)
}

// valueWalkLimit result codes.
const (
	valueWalkOK        = iota // safe to hand to jsonSerializableValue / json.Marshal
	valueWalkTooDeep          // nests past the depth budget (stack-overflow risk)
	valueWalkTooWide          // expands to too many nodes (fan-out / json.Marshal cost)
	valueWalkTooLarge         // expands to too many payload bytes (json.Marshal output size)
	valueWalkBadMapKey        // a map key json.Marshal's key resolver cannot name
)

// marshalEmitLen reports how many bytes json.Marshal will emit for a value
// that defines its own JSON form, and whether it is such a value at all.
// json.Marshal consults json.Marshaler first, then encoding.TextMarshaler,
// so this checks them in that order. json.Number is deliberately NOT here:
// it is a string-KIND value the String arm already charges by content.
//
// Measuring costs one call to the caller's own method, whose result is
// charged and immediately dropped. That keeps the MEASUREMENT bounded in the
// way that matters: the walk stops at the first value that busts the budget,
// so a tree of N over-budget marshalers materializes one image, not N, and
// the walk never accumulates or retains them. The single transient image is
// produced by the caller's own method on the caller's own value — no walk
// can be cheaper than asking it what it emits.
//
// A method returning an error is left uncharged and unhandled: the eventual
// json.Marshal will surface that same error, and inventing a budget verdict
// for a value that will never be emitted would reject a tree that actually
// fails for a different, better-named reason.
func marshalEmitLen(rv reflect.Value, limit int) (int, bool) {
	if !rv.IsValid() || !rv.CanInterface() {
		return 0, false
	}
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return 0, false // json.Marshal emits "null" without calling the method
		}
	}
	switch m := rv.Interface().(type) {
	case json.Marshaler:
		out, err := m.MarshalJSON()
		if err != nil {
			return 0, false
		}
		return compactedEmitLen(out, limit), true
	case encoding.TextMarshaler:
		out, err := m.MarshalText()
		if err != nil {
			return 0, false
		}
		// Emitted as a quoted JSON string, through the same escaper.
		return jsonEscapedLenBytes(out, limit) + 2, true
	}
	return 0, false
}

// marshalSchemaTree is the ONE call that turns a rendered schema tree into
// bytes. The walk budget charges against exactly this emitter's escaping and
// the census differential derives its expectation from it, so a change here
// — an Encoder with SetEscapeHTML(false), say — moves the charge and the
// test that proves the charge together, instead of silently parting them.
func marshalSchemaTree(tree any) ([]byte, error) { return json.Marshal(tree) }

// asciiEscapedLen is the emitted length of one byte below utf8.RuneSelf.
// Escaping is byte-LOCAL below RuneSelf — a byte's cost never depends on its
// neighbours — so this table plus the multi-byte arms below is a complete
// description of the emitter's string output, and testing all 256 values is
// a domain proof rather than a sample.
func asciiEscapedLen(b byte) int {
	switch b {
	case '\\', '"', '\b', '\f', '\n', '\r', '\t':
		return 2 // two-character escape
	case '<', '>', '&':
		return 6 // < — the emitter escapes HTML
	}
	if b < 0x20 {
		return 6 // \u00XX
	}
	return 1
}

// jsonEscapedLen reports how many bytes the emitter writes for s's CONTENT
// (what lands between the quotes), and stops once the running total passes
// limit, returning a value greater than it.
//
// It COUNTS; it never builds. Measuring by emitting would allocate the very
// image the budget exists to prevent, so the escape rules are restated here
// rather than delegated to. Restating an authority is the mistake this
// package works hardest to avoid, and it is permitted only because
// delegation is impossible for MEASUREMENT — so the restatement carries an
// executed differential over the authority's complete single-byte domain
// plus every multi-byte case (census Q9), derived from marshalSchemaTree
// itself.
//
// The early exit is what bounds the scan by the BUDGET instead of by the
// input: escaping never shrinks a string — every input byte costs at least
// one output byte — so the total passes limit within limit+1 input bytes. A
// hostile 1 GiB string is abandoned after ~64 MiB, and the bytes scanned
// were already resident in the caller's own value.
func jsonEscapedLen(s string, limit int) int {
	n := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			n += asciiEscapedLen(b)
			i++
		} else {
			c, size := utf8.DecodeRuneInString(s[i:])
			switch {
			case c == utf8.RuneError && size == 1:
				n += 6 // invalid UTF-8 is emitted as �
			case c == ' ' || c == ' ':
				n += 6 // escaped unconditionally, JSONP safety
			default:
				n += size // emitted verbatim
			}
			i += size
		}
		if n > limit {
			return n
		}
	}
	return n
}

// jsonEscapedLenBytes is jsonEscapedLen over a byte slice, for text a
// TextMarshaler returned (the emitter runs it through the same escaper).
func jsonEscapedLenBytes(s []byte, limit int) int {
	n := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			n += asciiEscapedLen(b)
			i++
		} else {
			c, size := utf8.DecodeRune(s[i:])
			switch {
			case c == utf8.RuneError && size == 1:
				n += 6
			case c == ' ' || c == ' ':
				n += 6
			default:
				n += size
			}
			i += size
		}
		if n > limit {
			return n
		}
	}
	return n
}

// avroCodepointEscapedLen is the emitted length of a byte slice that the JSON
// fixup renders as the Avro codepoint string: byte v becomes U+00v, so a byte
// at or above 0x80 costs the two bytes of its UTF-8 form and everything below
// costs what the ASCII table says. Charging the value's json-FACING image
// rather than its Go shape is the rule: a []byte never reaches the emitter as
// a byte slice.
func avroCodepointEscapedLen(rv reflect.Value, limit int) int {
	n := 0
	for i := range rv.Len() {
		if b := byte(rv.Index(i).Uint()); b < utf8.RuneSelf {
			n += asciiEscapedLen(b)
		} else {
			n += 2
		}
		if n > limit {
			return n
		}
	}
	return n
}

// compactedEmitLen upper-bounds what the emitter writes for JSON a
// json.Marshaler returned. That output is re-scanned by encoding/json's
// compactor, which escapes <, > and & (one byte becoming six) and U+2028 /
// U+2029 (three becoming six), and drops insignificant whitespace. Only the
// GROWTH is counted: ignoring the shrinkage over-charges slightly, which is
// the safe direction for a cap, and it avoids re-running a JSON scanner.
func compactedEmitLen(out []byte, limit int) int {
	n := len(out)
	for i := 0; i < len(out); i++ {
		switch c := out[i]; {
		case c == '<' || c == '>' || c == '&':
			n += 5
		case c == 0xE2 && i+2 < len(out) && out[i+1] == 0x80 && out[i+2]&^1 == 0xA8:
			n += 3
		}
		if n > limit {
			return n
		}
	}
	return n
}

// mapKeyEmitLen reports the bytes json.Marshal emits for one map key, and
// whether its key resolver can name the key at all.
//
// The arms mirror encoding/json's resolveKeyName IN ORDER, GUARDS INCLUDED,
// because that function is the authority on what a key emits: string KIND
// first (a string-kind key marshals as its raw string and any MarshalText on
// it is not consulted), then encoding.TextMarshaler, then integer
// formatting. Charging only the arms it is convenient to model leaves the
// rest free; skipping the guards is worse, because a guard is the arm that
// keeps a legal value from being handed to code that cannot take it.
//
// The nil-pointer guard is the one that matters: a nil pointer key whose
// type has a pointer-receiver MarshalText is an ordinary Go value, and
// json.Marshal resolves it to "" WITHOUT calling the method. Calling it
// dereferences nil — a panic raised INSIDE the walk whose whole purpose is
// to make an arbitrary caller-supplied tree safe to marshal.
//
// The final arm is resolveKeyName's `panic("unexpected map key type")`,
// reached by a key that named no arm — a nil interface key in a
// map[encoding.TextMarshaler]V, which json's encoder-construction check
// admits (the interface implements itself) and its resolver then cannot
// name. The walk reports it as a named error instead, which is also what
// the key kinds json rejects at encoder construction (float, array, a
// struct with no text method) now get: this budget's contract is that every
// key is accounted for, so "json cannot emit this key" is a verdict the
// walk owns, not a panic to forward.
func mapKeyEmitLen(k reflect.Value, limit int) (int, bool) {
	if k.Kind() == reflect.String {
		return jsonEscapedLen(k.String(), limit), true
	}
	if k.CanInterface() {
		if tm, ok := k.Interface().(encoding.TextMarshaler); ok {
			if k.Kind() == reflect.Pointer && k.IsNil() {
				return 0, true // resolved to "" without calling the method
			}
			out, err := tm.MarshalText()
			if err != nil {
				// Left uncharged, like marshalEmitLen: the eventual
				// json.Marshal surfaces this same error by its own name.
				return 0, true
			}
			return jsonEscapedLenBytes(out, limit), true
		}
	}
	switch k.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr:
		return 20, true // a signed 64-bit decimal is at most 20 bytes
	}
	return 0, false
}

// valueWalkLimit walks v — a Props value or a SchemaField.Default, an arbitrary
// user-supplied JSON tree — the way json.Marshal will, returning a non-OK code
// when the value is unsafe to serialize at [SchemaNode.Schema]. It enforces
// three orthogonal limits, because a value is handed to jsonSerializableValue
// (needsJSONFixup/applyJSONFixup) and then to json.Marshal, neither of which
// bounds anything:
//
//   - DEPTH (depthLeft): the longest container PATH. A value nested far enough
//     overflows the goroutine stack uncatchably (recover cannot catch a stack
//     overflow) in the fixup walk or in json.Marshal, before Schema's eventual
//     Parse can reject it. depthLeft mirrors toJSONWalk's structural depth
//     bound, charging the structural nesting already accrued so the total
//     marshaled nesting stays within one ceiling.
//
//   - EXPANSION (b.nodes, the node budget shared with the structural
//     toJSONWalk): the TOTAL nodes json.Marshal will emit. A value that shares a
//     sub-value across sibling paths is shallow yet fans out into a 2^depth tree
//     when serialized (see maxSchemaJSONNodes). The depth bound is blind to it;
//     only counting emitted nodes catches it. b.nodes is decremented on EVERY
//     node, so the walk itself terminates at the budget — it can neither overflow
//     its own stack nor hang on a shared-reference DAG or a cyclic Go type
//     (type P *P).
//
//   - PAYLOAD SIZE (b.bytes, the byte budget shared with the structural walk):
//     the TOTAL bytes of every emitted scalar — string and json.Number content,
//     []byte (codepoint-string) content, map keys, struct field names. A value
//     whose leaves are huge or share one big string across many nodes is small
//     in memory yet expands past memory in json.Marshal's output, invisible to
//     the node count (see maxSchemaJSONBytes).
//
// The walk mirrors what json.Marshal recurses into — maps, slices, arrays,
// structs, and pointer/interface indirection — not just the map[string]any /
// []any shapes [Schema.Root] produces (a hand-built node or a SchemaFor
// CustomType.Schema can store ANY Go value the map[string]any field accepts).
// []byte/[N]byte are a codepoint/base64 scalar (charged by length, not walked as
// a nested array).
func valueWalkLimit(rv reflect.Value, depthLeft int, b *walkBudget) int {
	if depthLeft < 0 {
		return valueWalkTooDeep
	}
	if b.bytes < 0 {
		return valueWalkTooLarge
	}
	if !b.takeNode() {
		return valueWalkTooWide
	}
	// A value carrying its own MarshalJSON / MarshalText does not get walked
	// by json.Marshal at all: the method's return IS the emission, so the
	// structural recursion below would charge the value's Go shape while
	// json.Marshal emits something else entirely (an empty struct whose
	// MarshalJSON returns a megabyte charges one node and no bytes). Charge
	// what the method actually emits, and stop the walk here — mirroring
	// json.Marshal's own dispatch, which never descends into such a value.
	// The value stays marshal-opaque: charging reads the method's output and
	// discards it, so nothing about its rendering changes.
	if n, ok := marshalEmitLen(rv, b.bytes); ok {
		if !b.takeBytes(n) {
			return valueWalkTooLarge
		}
		return valueWalkOK
	}
	switch rv.Kind() {
	case reflect.Interface, reflect.Pointer:
		if rv.IsNil() {
			return valueWalkOK
		}
		return valueWalkLimit(rv.Elem(), depthLeft-1, b)
	case reflect.Map:
		for iter := rv.MapRange(); iter.Next(); {
			// json.Marshal emits EVERY map key as an object key, whatever the
			// key's Kind: a string-kind key as its raw string, and any other
			// kind through MarshalText or integer formatting. Charging only
			// string-kind keys left the rest free (see mapKeyEmitLen).
			n, ok := mapKeyEmitLen(iter.Key(), b.bytes)
			if !ok {
				return valueWalkBadMapKey
			}
			if !b.takeBytes(n) {
				return valueWalkTooLarge
			}
			if r := valueWalkLimit(iter.Value(), depthLeft-1, b); r != valueWalkOK {
				return r
			}
		}
	case reflect.Slice, reflect.Array:
		if canonicalByteSliceKind(rv.Type()) || (rv.Kind() == reflect.Array && rv.Type().Elem().Kind() == reflect.Uint8) {
			// The JSON fixup renders these as the Avro codepoint STRING, so
			// that is the image to charge — not the Go length. The gate is
			// the fixup's OWN predicate, so a byte slice whose element type
			// carries a marshaler (which the fixup declines to rewrite, and
			// json emits as an ARRAY) falls through to the walk below
			// instead of being charged as a scalar it never becomes.
			if !b.takeBytes(avroCodepointEscapedLen(rv, b.bytes)) {
				return valueWalkTooLarge
			}
			return valueWalkOK
		}
		for i := 0; i < rv.Len(); i++ {
			if r := valueWalkLimit(rv.Index(i), depthLeft-1, b); r != valueWalkOK {
				return r
			}
		}
	case reflect.Struct:
		t := rv.Type()
		for i := 0; i < rv.NumField(); i++ {
			if !t.Field(i).IsExported() {
				continue // json.Marshal skips unexported fields
			}
			// json.Marshal emits the field name (or json tag) as an object key.
			if !b.takeBytes(len(t.Field(i).Name)) {
				return valueWalkTooLarge
			}
			if r := valueWalkLimit(rv.Field(i), depthLeft-1, b); r != valueWalkOK {
				return r
			}
		}
	case reflect.String:
		// string AND json.Number (type Number string) — charge what the
		// emitter WRITES, which is the escaped form: a control byte costs
		// six output bytes, and the emitter escapes HTML.
		if !b.takeBytes(jsonEscapedLen(rv.String(), b.bytes)) {
			return valueWalkTooLarge
		}
	}
	return valueWalkOK
}

// boundedSerializableValue applies jsonSerializableValue to a Props value or
// SchemaField.Default after bounding both its nesting depth AND its serialized
// node count against the shared per-walk budget *nodes (see valueWalkLimit), so
// neither the fixup walk nor the downstream json.Marshal overflows the stack or
// fans a shared-reference DAG out into an exponential tree. depth is the
// structural nesting already accrued by toJSONWalk, so the value may add at most
// maxSchemaJSONDepth-depth further levels. A value that exceeds either bound
// records the error on the dedup path (so [SchemaNode.Schema] returns it) and
// truncates to nil on the bare path (so the marshal cannot crash), mirroring
// toJSONWalk's own over-limit handling.
func boundedSerializableValue(d *deduper, depth int, b *walkBudget, v any) any {
	switch valueWalkLimit(reflect.ValueOf(v), maxSchemaJSONDepth-depth, b) {
	case valueWalkTooDeep:
		d.fail(fmt.Errorf("avro: SchemaNode default/property value nests deeper than the supported limit (%d)", maxSchemaJSONDepth))
		return nil
	case valueWalkTooWide:
		d.fail(fmt.Errorf("avro: SchemaNode default/property value expands to more than the supported %d nodes", maxSchemaJSONNodes))
		return nil
	case valueWalkTooLarge:
		d.fail(fmt.Errorf("avro: SchemaNode default/property value expands to more than the supported %d bytes", maxSchemaJSONBytes))
		return nil
	case valueWalkBadMapKey:
		d.fail(errors.New("avro: SchemaNode default/property value contains a map whose key type has no JSON object-key form (not a string kind, an integer kind, or a usable encoding.TextMarshaler)"))
		return nil
	}
	return jsonSerializableValue(v)
}

// toJSONShared snapshots n's full JSON body (no dedup) for the conflict
// comparison in toJSONWalk, charging the SHARED per-walk budget rather than a
// fresh one. A named type that re-occurs as a DISTINCT pointer with an identical
// body triggers a 2x re-marshal of its whole subtree; with k such copies of a
// w-node body that is O(k*w) work, and because the dedup walk only charges 1
// node per re-occurrence (it emits a bare reference, not the body), the outer
// budget alone leaves k*w unbounded even though the emitted schema is tiny (one
// definition + k-1 references). Sharing the budget caps the total comparison
// work at maxSchemaJSONNodes / maxSchemaJSONBytes. Once either axis is exhausted
// the walk returns truncated output; the caller checks the budget and reports
// over-budget rather than a spurious body conflict (asymmetric truncation of n
// vs prev could otherwise make identical bodies compare unequal).
func (n *SchemaNode) toJSONShared(b *walkBudget) any {
	return n.toJSONWalk(make(map[*SchemaNode]struct{}), nil, "", 0, b, false)
}

// toJSONWalk is the cycle-aware walker shared by toJSON and toJSONDedup.
// visited is threaded through every recursive call so cycles introduced
// via Items / Values pointers terminate — see
// TestRegression_SchemaNodeToJSONCycleSafe for the invariant. When d is
// non-nil it tracks named-type definitions and reports conflicting
// redefinitions; when nil it just emits the JSON tree. enclosingNS is
// the namespace scope at this node's position: named types emit their
// namespace relative to it (omitted when inherited; "namespace":"" when
// a null-namespace type sits inside a namespaced scope — Java's
// Name.writeName escape), and name references emit the fullname so they
// re-bind position-independently.
//
// depth is the structural nesting level (items/values/branches/fields
// descents). The visited map only terminates true *pointer cycles; a
// distinct-node-per-level acyclic chain (a hand-built array<array<…>> a
// million deep) has no repeated pointer, so without this bound the walk
// would recurse until the goroutine stack overflows and the process
// dies uncatchably — before Schema's eventual Parse (which bounds JSON
// bracket nesting at maxSchemaJSONDepth) ever runs. Cap the walk at the
// same maxSchemaJSONDepth ceiling: any tree shallow enough to encode or
// decode sits far below it (the wire codec's own maxDepth is 4× smaller),
// so a usable tree is never rejected, and a deeper one stops with a clean
// error (dedup path) or a truncated subtree Parse then rejects (bare
// path) instead of crashing.
//
// stray is true when n was reached through a structural key its parent's
// kind does not bind (a stray "items" on an "int", surfaced as-written by
// the metadata walker) — and, transitively, for everything below such a
// node. The wire parser never binds names at those positions, so the
// walk renders them verbatim but the dedup consult skips them entirely:
// no registration, no second-definition→reference rewrite, no conflict
// comparison. Otherwise a definition-shaped stray body would stand in
// for (or spuriously conflict with) the real definition of the same
// fullname, and the rebuilt text would either fail to re-parse (the
// reference points at a def sitting in a position the re-parse correctly
// ignores) or silently rewrite the as-written stray content.
func (n *SchemaNode) toJSONWalk(visited map[*SchemaNode]struct{}, d *deduper, enclosingNS string, depth int, b *walkBudget, stray bool) any {
	if depth > maxSchemaJSONDepth {
		d.fail(fmt.Errorf("avro: SchemaNode tree nests deeper than the supported limit (%d)", maxSchemaJSONDepth))
		return nil
	}
	// Charge this node against the shared budget. The depth bound above caps the
	// longest PATH; b.nodes caps the total number of emitted nodes, so a
	// shared-reference DAG (the same *SchemaNode reached via Items AND Values,
	// tiny in memory) cannot fan out into an exponential tree that hangs the walk
	// / json.Marshal before Parse runs (see maxSchemaJSONNodes); b.bytes caps the
	// total emitted scalar payload, so a huge or widely-shared string/slice
	// cannot blow json.Marshal's output up while the node count stays tiny (see
	// maxSchemaJSONBytes). Once either is exhausted, every further node returns
	// early without descending, so the fan-out is pruned at the frontier.
	if b.bytes < 0 {
		d.fail(errSchemaTreeBytes())
		return nil
	}
	if !b.takeNode() {
		d.fail(errSchemaTreeNodes())
		return nil
	}
	// Charge type / name / namespace BEFORE they are hashed into the dedup map,
	// scanned by strings.Contains, or emitted as a name reference, so a huge
	// shared Name/Namespace cannot amplify via the dedup map's per-occurrence
	// hashing or the reference emission. (The type switches below are
	// length-short-circuited — O(1) even for a huge Type — so charging once here
	// covers every later emission of these three without double-counting.)
	if !b.takeBytes(len(n.Type) + len(n.Name) + len(n.Namespace)) {
		d.fail(errSchemaTreeBytes())
		return nil
	}
	if _, cycle := visited[n]; cycle {
		// Cycle through Items/Values back to n. Named types emit the
		// fullname as a reference (the canonical Avro recursive-schema
		// shape). Unnamed cycles are an error in the dedup walker and
		// return nil-stable JSON in the bare walker (snapshot/equality
		// comparison stays meaningful: two equal cyclic subtrees
		// produce the same partial JSON).
		// Keyed on the FULLNAME being expressible, not the short name: an
		// empty short name with a namespace (fullname "ns.") is a valid
		// reference target (recursive "ns." types parse), while fullname
		// "" has no reference spelling and stays the cycle error. A
		// stray-reached name is no reference target at all (nothing
		// registers it), so a stray cycle takes the error path.
		if isNamedKind(n.Type) && nodeFullname(n) != "" && !stray {
			return nodeFullname(n)
		}
		if d != nil && d.err == nil {
			d.err = fmt.Errorf("avro: cyclic SchemaNode detected")
		}
		return nil
	}
	visited[n] = struct{}{}
	defer delete(visited, n)

	// Dedup: named types that have already been emitted become name refs;
	// a redefinition with a different body is reported as a conflict.
	// Keyed by the FULLNAME — equality of names is defined on the
	// fullname (spec, "Names"), so two distinct types sharing a short
	// name across namespaces are not redefinitions of each other. The
	// reference is the fullname too: a dotted reference re-binds exactly
	// anywhere, and a null-namespace type's bare fullname re-binds via
	// the parser's null-namespace fallback. (A bare reference from
	// inside a namespaced scope that ALSO collides with an in-scope
	// short name re-binds in-scope — the same inherent reference
	// ambiguity Java's getQualified/Names.get pair has; references have
	// no "namespace":"" escape syntax.)
	if d != nil && !stray && isNamedKind(n.Type) && nodeFullname(n) != "" {
		if prev, exists := d.defined[nodeFullname(n)]; exists {
			// A repeated fullname becomes a bare name reference. Marshal-
			// compare the bodies only when the two are DISTINCT nodes (a
			// possible conflicting redefinition); a named type referenced
			// multiple times resolves to the same *SchemaNode and is
			// definitionally equal, so it needs no marshal. Deferring the
			// comparison to an actual collision keeps the common all-
			// distinct-names case O(n) instead of marshaling every named
			// type's full subtree eagerly (O(depth*subtree) on nesting).
			// The comparison marshals share the walk's node budget
			// (toJSONShared, not toJSON) so many identical-bodied distinct-
			// pointer duplicates cannot amplify into O(k*subtree) work outside
			// the bound — see toJSONShared.
			if prev != n && d.err == nil {
				cur, _ := json.Marshal(n.toJSONShared(b))
				prevB, _ := json.Marshal(prev.toJSONShared(b))
				switch {
				case b.nodes <= 0:
					// The comparison exhausted the shared node budget: the
					// duplicated subtree is large enough to blow the bound, so
					// the truncated bodies can't be compared reliably. Report
					// over-budget, matching toJSONWalk's other over-limit exits.
					d.err = errSchemaTreeNodes()
				case b.bytes < 0:
					// Same, on the payload-size axis: a truncated body comparison
					// is meaningless, so report over-budget rather than risk a
					// spurious conflict from asymmetric string truncation.
					d.err = errSchemaTreeBytes()
				case string(cur) != string(prevB):
					d.err = fmt.Errorf("avro: conflicting definitions for named type %q", truncForError(nodeFullname(n)))
				}
			}
			return nodeFullname(n)
		}
	}

	// The namespace scope inside this node: a named type opens its own.
	childNS := nsForChildren(n, enclosingNS)

	// Name-reference resolution (the hidden Root stamp): a reference to a
	// name this tree does not define emits the stamped definition at its
	// first occurrence — walked through this same budgeted, cycle-checked,
	// deduped recursion — and the fullname thereafter, so a sub-tree
	// extracted from a Root converts to a self-contained schema. Gated on
	// d != nil (conflict snapshots stay splice-free on both sides of a
	// comparison) and !stray (the wire parser binds no names at stray
	// positions). References the tree DOES define locally — before or
	// after this position — stay as-written and re-bind to the local
	// definition, preserving today's output byte-for-byte for
	// self-contained trees (forward references included).
	refType := n.Type
	if d != nil && !stray && nodeRefTargetAgrees(n) && nodeIsNameRefShape(n) {
		if fn := nodeFullname(n.refTarget); fn != "" && !d.localNames[fn] {
			if _, emitted := d.defined[fn]; !emitted {
				// The target walk gets a FRESH visited map: a recursive
				// definition reaches back through the extraction point
				// (splicing Node re-enters the union the outer walk is
				// still inside), a revisit that is finite — the target
				// registers in d.defined before walking its children, so
				// every name splices at most once and interior re-visits
				// terminate at the fullname arm — but that the shared
				// map's cycle arm would misread as an unnamed cycle.
				// True cycles inside the target are still caught by the
				// fresh map, and the shared depth ceiling and node/byte
				// budgets bound the whole emission either way.
				spliced := n.refTarget.toJSONWalk(make(map[*SchemaNode]struct{}), d, enclosingNS, depth, b, false)
				// A wrapped reference's custom properties ride onto the
				// spliced definition — definition-wins, reserved keys
				// dropped — the same treatment the SchemaCache splice
				// gives wrapper props (inlineTreeDefs's wrapper arm).
				if m2, ok := spliced.(map[string]any); ok && len(n.Props) > 0 {
					defTyp, _ := m2["type"].(string)
					defLogical, _ := m2["logicalType"].(string)
					for k, v := range n.Props {
						if !b.takeBytes(len(k)) {
							d.fail(errSchemaTreeBytes())
							continue
						}
						pv := boundedSerializableValue(d, depth, b, v)
						if schemaReservedKeyForObject(k, pv, defTyp, defLogical, nil) {
							continue
						}
						if _, has := m2[k]; has {
							continue
						}
						m2[k] = pv
					}
				}
				return spliced
			}
			// Already emitted (an earlier splice, or the walk passed the
			// definition): reference it by fullname — the spelling that
			// re-binds exactly regardless of the standalone parse's
			// namespace scope at this position.
			refType = fn
		}
	}

	switch n.Type {
	case "null", "boolean", "int", "long", "float", "double", "string", "bytes":
		// Bare-string emission is only lossless when the node carries
		// NOTHING but its Type — see nodeCarriesOnlyType, which derives that
		// from the field set rather than listing the fields it remembers.
		if nodeCarriesOnlyType(n) {
			return n.Type
		}
	case "union":
		branches := make([]any, len(n.Branches))
		for i := range n.Branches {
			branches[i] = n.Branches[i].toJSONWalk(visited, d, childNS, depth+1, b, stray)
		}
		return branches
	}

	// The same losslessness question as the primitive arm above, for a NAME
	// REFERENCE: it may collapse to the bare name only when the node carries
	// nothing else. Both sites ask nodeCarriesOnlyType rather than repeating
	// a field list — they previously held two copies of the same incomplete
	// list, which is why a stray Symbols/Size/Aliases vanished here while a
	// stray Name was caught.
	if n.Type != "array" && n.Type != "map" && !isNamedKind(n.Type) &&
		n.Type != "union" && nodeCarriesOnlyType(n) {
		return refType
	}

	// Dedup: remember this named type's node for the next occurrence's
	// conflict check. Store the node, not its marshaled body — marshaling
	// every named type eagerly is O(depth*subtree) on nested schemas, and
	// the body is only needed if a duplicate fullname actually appears.
	if d != nil && !stray {
		// Fullname-keyed like the duplicate check above: fullname "" has
		// no reference spelling, so it stays un-deduped (inline is its
		// only representation). Stray-reached names register nothing —
		// the wire parser does not bind them, so they can neither be
		// referenced nor conflicted with.
		if isNamedKind(n.Type) && nodeFullname(n) != "" {
			d.defined[nodeFullname(n)] = n
		}
	}

	m := map[string]any{"type": refType}
	// A named KIND always emits its name — including the empty short name
	// a user WithLaxNames fn can accept — mirroring the canonical emitter
	// (appendCanonObject) and the parser, for which a missing and an empty
	// name are the same fullname; the Name != "" arm keeps emission for
	// hand-built names on non-named kinds.
	if n.Name != "" || isNamedKind(n.Type) || n.present.has(presName) {
		m["name"] = n.Name
	}
	if isNamedKind(n.Type) && !strings.Contains(n.Name, ".") {
		// Emit the namespace relative to the enclosing scope, mirroring
		// Java's Name.writeName: omit when equal (re-parse inherits it),
		// "namespace":"" to escape inheritance for a null-namespace type
		// inside a namespaced scope, the value otherwise. A dotted Name
		// carries its own namespace, so no attribute is emitted for it
		// (the spec ignores the attribute when the name is dotted).
		switch eff := n.Namespace; {
		case eff == enclosingNS:
			// inherited (or both null): omit
		case eff == "":
			m["namespace"] = ""
		default:
			m["namespace"] = eff
		}
	} else if (n.Namespace != "" || n.present.has(presNamespace)) && !isNamedKind(n.Type) {
		// Unnamed node with a namespace attribute: preserve as-written
		// (the parser ignores it; fidelity only). Presence carries the
		// explicit-empty form, which the value alone cannot show.
		m["namespace"] = n.Namespace
	}
	// aliases emits when NON-EMPTY where a kind BINDS it, which is Apache
	// Avro's own condition (Schema.java:886) — an empty alias list is
	// dropped there deliberately. On a kind that does not bind it there is
	// no such condition to follow, and the stray-routing posture says
	// as-written is the key's only surface, so presence decides.
	if len(n.Aliases) > 0 || (n.present.has(presAliases) && !strayKeyBinds(n.Type, "aliases")) {
		m["aliases"] = b.emitStrings(d, n.Aliases)
	}
	// Emitted when the attribute was WRITTEN, not when it is non-empty:
	// an empty doc is a doc, and Apache Avro emits it (Schema.java:1039 /
	// :1154 / :1367 all ask getDoc() != null). Contrast aliases just
	// above, whose emission condition is non-EMPTY (:886) — the per-
	// attribute difference is why presence is asked here and not there.
	if n.Doc != "" || n.present.has(presDoc) {
		m["doc"] = b.emitString(d, n.Doc)
	}
	if n.HasEnumDefault {
		m["default"] = b.emitString(d, n.EnumDefault)
	}
	// logicalType is not in Java's reserved set, so it survives as an
	// ordinary schema property whatever its content, empty string included
	// (parseProperties :1983, writeProps).
	if n.LogicalType != "" || n.present.has(presLogicalType) {
		m["logicalType"] = b.emitString(d, n.LogicalType)
	}
	if n.Precision != 0 {
		m["precision"] = n.Precision
	}
	if n.Scale != 0 {
		m["scale"] = n.Scale
	}
	// fixed.size is a required attribute and 0 is a legal size, so for
	// fixed types it is always emitted — omitting a zero value would make
	// the re-emitted schema unparseable ("fixed is missing size").
	if n.Type == "fixed" {
		m["size"] = n.Size
	} else if n.Size != 0 || n.present.has(presSize) {
		m["size"] = n.Size
	}
	// enum.symbols is a required attribute per the Avro spec (Complex
	// Types > Enums: "symbols: a JSON array, listing symbols, as JSON
	// strings (required)"), always emit for enum types even when empty.
	if n.Type == "enum" {
		if n.Symbols == nil {
			m["symbols"] = []string{}
		} else {
			m["symbols"] = b.emitStrings(d, n.Symbols)
		}
	} else if len(n.Symbols) > 0 || n.present.has(presSymbols) {
		m["symbols"] = b.emitStrings(d, n.Symbols)
	}
	if n.Items != nil {
		m["items"] = n.Items.toJSONWalk(visited, d, childNS, depth+1, b, stray || n.Type != "array")
	}
	if n.Values != nil {
		m["values"] = n.Values.toJSONWalk(visited, d, childNS, depth+1, b, stray || n.Type != "map")
	}
	// record.fields is a required attribute per the Avro spec (Complex
	// Types > Records: "fields: a JSON array, listing fields (required)"),
	// always emit for record/error types even when empty.
	if isRecordKind(n.Type) || len(n.Fields) > 0 || n.present.has(presFields) {
		fieldStray := stray || !isRecordKind(n.Type)
		fields := make([]map[string]any, len(n.Fields))
		for i, f := range n.Fields {
			fd := map[string]any{
				"name": b.emitString(d, f.Name),
				"type": f.Type.toJSONWalk(visited, d, childNS, depth+1, b, fieldStray),
			}
			if f.HasDefault || f.Default != nil {
				// jsonSerializableValue converts ±Inf — which a Root()
				// of "default":1e1000 normalizes to via normalizeJSONNumber
				// → parseFloatAcceptOverflow — back to a json.Number
				// literal so encoding/json.Marshal at SchemaNode.Schema()
				// doesn't fail. Inverse of the metadata-API normalization.
				fd["default"] = boundedSerializableValue(d, depth, b, f.Default)
			}
			if len(f.Aliases) > 0 {
				fd["aliases"] = b.emitStrings(d, f.Aliases)
			}
			if f.Order != "" {
				fd["order"] = b.emitString(d, f.Order)
			}
			if f.Doc != "" || f.docSet {
				fd["doc"] = b.emitString(d, f.Doc)
			}
			for k, v := range f.Props {
				// The Props KEY is emitted as a JSON object key; charge it, then
				// the value through the depth+node+byte-bounded value walk.
				if !b.takeBytes(len(k)) {
					d.fail(errSchemaTreeBytes())
					continue
				}
				fd[k] = boundedSerializableValue(d, depth, b, v)
			}
			fields[i] = fd
		}
		m["fields"] = fields
	}
	for k, v := range n.Props {
		if !b.takeBytes(len(k)) {
			d.fail(errSchemaTreeBytes())
			continue
		}
		m[k] = boundedSerializableValue(d, depth, b, v)
	}
	return m
}

// nodeFromJSON converts a parsed JSON value into a SchemaNode. parentNS
// is the enclosing namespace scope; named types without an explicit
// "namespace" attribute resolve into it (see [SchemaNode].Namespace).
func nodeFromJSON(v any, parentNS string, memo strayShapeMemo) SchemaNode {
	switch s := v.(type) {
	case string:
		return SchemaNode{Type: s}
	case []any:
		branches := make([]SchemaNode, len(s))
		for i, b := range s {
			branches[i] = nodeFromJSON(b, parentNS, memo)
		}
		return SchemaNode{Type: "union", Branches: branches}
	case map[string]any:
		return nodeFromJSONObject(s, parentNS, memo)
	default:
		return SchemaNode{}
	}
}

// Known schema keys that are NOT custom properties.
var schemaReservedKeys = map[string]bool{
	"type": true, "name": true, "namespace": true, "doc": true,
	"fields": true, "symbols": true, "items": true, "values": true,
	"size": true, "logicalType": true, "precision": true, "scale": true,
	"aliases": true, "default": true, "order": true,
}

// Known field keys that are NOT custom properties.
var fieldReservedKeys = map[string]bool{
	"name": true, "type": true, "default": true, "doc": true,
	"aliases": true, "order": true,
}

// jsonNumericInt accepts a value parsed via unmarshalAnyPreservePrecision
// (int64 for integer literals) and falls through to float64 / json.Number
// for compatibility with values originating from a bare encoding/json
// Unmarshal — primarily SchemaNode trees constructed programmatically
// and round-tripped through Schema().Root().
func jsonNumericInt(v any) (int, bool) {
	switch t := v.(type) {
	case int64:
		return int(t), true
	case float64:
		return int(t), true
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return int(i), true
		}
	case string:
		// The Avro [INTEGERS] rule allows quoted-string integers; kept
		// for symmetry with laxInt even though the current callers
		// (precision/scale on a validated decimal carrier — size reads
		// through decodeLaxInt) never see the quoted form post-parse.
		if len(t) <= maxLaxIntDataLen {
			if i, err := strconv.Atoi(t); err == nil {
				return i, true
			}
		}
	}
	return 0, false
}

// getString reads m[key] into dst when the body is a JSON string, and
// reports whether it did. The bool is the attribute's PRESENCE as distinct
// from its value: an empty string sets dst to the field's own zero, so the
// field alone can no longer say whether the key was written.
//
// The lookup is by exact name: reserved attribute names match ONLY their
// exact lowercase spelling, and a case-variant key is an ordinary custom
// property (Java's reserved sets are exact-lowercase HashSets,
// Schema.java:175-176; fastavro and goavro read exact names too).
func getString(m map[string]any, key string, dst *string) bool {
	s, ok := m[key].(string)
	if ok {
		*dst = s
	}
	return ok
}

// getInt assigns *dst to m[key] when present and parseable via
// jsonNumericInt (precision/scale/size).
func getInt(m map[string]any, key string, dst *int) {
	if v, ok := m[key]; ok {
		if p, ok := jsonNumericInt(v); ok {
			*dst = p
		}
	}
}

// isRecordKind reports whether typ names the Avro record kind in
// [SchemaNode.Type]. Both "record" and "error" are valid JSON literals
// for the same on-wire kind (the Avro RPC convention names error-record
// types with "error"); the schema builder normalizes both to
// node.kind=="record" at schema.go's `case "record", "error":` arm.
// SchemaNode.Type preserves the JSON-as-written name, so any
// metadata-API dispatcher on SchemaNode.Type that branches on the
// record kind must accept either alias — this helper centralizes the
// predicate so the alias set can't drift across call sites.
func isRecordKind(typ string) bool {
	return typ == "record" || typ == "error"
}

// isNamedKind reports whether typ is one of the four Avro named-type kinds
// (record / error / enum / fixed) — the set that carries a Name and can be
// referenced, deduped, and aliased. "error" is the record alias and must
// always travel with "record" here. Centralizes the four-element set so the
// many dedup / name-validation / alias call sites can't drift (the named-type
// analogue of [isRecordKind]).
func isNamedKind(typ string) bool {
	return typ == "record" || typ == "error" || typ == "enum" || typ == "fixed"
}

// coerceMetadataDefault is the metadata-API parallel of [coerceDefault]
// (schema.go). It transforms a parsed-JSON default value into the
// canonical Go form the wire-encode pipeline materializes for that
// field type — so SchemaField.Default surfaces a value type matching
// the wire bytes rather than the raw JSON form
// unmarshalAnyPreservePrecision returns.
//
// Currently:
//   - int / long / float / double fields → schema-width-faithful Go
//     type (int32 / int64 / float32 / float64), matching Java's
//     JacksonUtils.toObject(jsonNode, schema) at JacksonUtils.java:150-155.
//     Schema parse rejects out-of-range integer defaults so int32/int64
//     narrowing is lossless; float32 narrowing of finite-overflow
//     inputs surfaces ±Inf (matching the wire bytes).
//   - string defaults for bytes/fixed fields → []byte via Avro's
//     codepoint-per-byte mapping (mirrors [convertDefaultBytes] in
//     schema.go, which produces the same []byte for the wire-encode
//     pipeline's internal defaultVal). Without this conversion, a
//     metadata-API consumer doing
//     `defs[f.Name] = f.Default; s.Encode(defs)` succeeds for every
//     bytes/fixed default EXCEPT fixed+uuid (the encoder's UUID arm
//     hard-fails parseUUID on the 16-codepoint wire-form string), and
//     the round-trip contract breaks asymmetrically. Converting to
//     []byte here brings every bytes/fixed default into a form the
//     encoder accepts uniformly (raw bytes via serSize/serBytes /
//     the JSON fixed/string-slice/array arm).
//
// Walks unions (Avro 1.12: union default may match any branch) and
// nested record/array/map types. For single-field float/double fields
// with a string-form numeric default, this coerces the string to
// float32/float64 — Java parity with parseField's text→DoubleNode
// coercion at Schema.java:1899-1902, scoped to outer FLOAT/DOUBLE
// field types only. For union branches the coercion does NOT fire
// (matching Java's isValidDefault for the union arm, avro-rs's
// resolve_internal, and goavro's strict type assertions): union+
// numeric-string defaults are rejected at schema parse, so they never
// reach this function.
//
// Non-numeric / non-string defaults and non-handled types pass through
// unchanged.
func coerceMetadataDefault(val any, t *SchemaNode, table map[string]*SchemaNode, ns string) any {
	if t == nil {
		return val
	}
	// Name-ref resolution: when the caller passes a non-nil name-table
	// and t.Type is a bare name-reference (e.g. "Inner"), resolve to
	// the actual named SchemaNode and recurse — inside the TARGET's own
	// namespace scope. table == nil means the caller is doing
	// best-effort inline coercion only — used by the synchronous call
	// during nodeFromJSON construction where the full tree (and
	// therefore the name-table) isn't available yet.
	if resolved := lookupNameRef(t, table, ns); resolved != nil {
		return coerceMetadataDefault(val, resolved, table, nodeEffNS(resolved))
	}
	if t.Type == "union" {
		// Best-effort first pass (table == nil): name-referenced branches
		// can't be resolved yet, so a greedy earlier branch (e.g. a bytes
		// branch accepting a string) would destructively coerce the value
		// (string to []byte) and lock out the correct name-ref branch (e.g.
		// an enum) that the table-populated pass would have picked — the
		// enum arm only accepts a string, never a []byte, so the value can
		// never be reclaimed. Defer ALL union branch selection to
		// coerceTreeDefaults, which runs with the name table populated
		// (Schema.Root); leave the raw value untouched here.
		if table == nil {
			return val
		}
		// Pick the FIRST branch that accepts val's Go type — matches
		// the wire-encode pipeline's coerceDefault (which uses
		// validateDefault for branch selection) and Java's Schema.
		// parseField (which Jackson-coerces against the first
		// accepting branch). Picking "first transformation" instead
		// would diverge for ["string","float"] with default "1.5":
		// wire picks string (first accept), but a transform-based
		// helper would pick float because string→string is a no-op.
		if branch := firstMetadataBranchAcceptingDefault(t, val, table, ns); branch != nil {
			return coerceMetadataDefault(val, branch, table, nsForChildren(branch, ns))
		}
		return val
	}
	if val == nil {
		return val
	}
	if t.Type == "int" {
		// Schema-width-faithful narrowing: int defaults surface as
		// int32 so SchemaField.Default's Go type matches the wire
		// width AND the user's natural Go field type (`Foo int32
		// `avro:"default=42"`` → Default.(int32) works directly).
		//
		// Every numeric form routes through the range-checked defaultAsInt32. A
		// TOP-LEVEL out-of-int32 int default is rejected at parse, but during
		// union-branch SELECTION a wider sibling branch (e.g. double) makes the
		// schema parse-valid, so this can run on a value parse never rejected. A
		// blind int64→int32 cast would WRAP such a value (3000000000 →
		// -1294967296), and branchAcceptsDefault would then accept the in-range
		// wrapped value for the int branch the wire (validateLeaf → defaultAsInt32)
		// rejects — selecting a different branch than the wire auto-fill and
		// corrupting both Root().Default and the Root().Schema() rebuild. Leaving
		// an out-of-range int64 unchanged lets defaultAsInt32 reject it so
		// selection picks the wider sibling, matching the wire.
		switch val := val.(type) {
		case int32:
			return val
		case int64, json.Number, string, float64:
			if n, err := defaultAsInt32(val); err == nil {
				return n
			}
			return val
		}
		return val
	}
	if t.Type == "long" {
		// Coerce non-int64 numeric inputs (json.Number, string,
		// float64-whole-number) to int64. int64 inputs pass through.
		// Schema parse rejects out-of-int64 via defaultAsInt64.
		switch val := val.(type) {
		case int64:
			return val
		case int32:
			return int64(val)
		case json.Number, string, float64:
			if n, err := defaultAsInt64(val); err == nil {
				return n
			}
			return val
		}
		return val
	}
	if t.Type == "float" || t.Type == "double" {
		// Schema-width-faithful narrowing per Java's JacksonUtils.toObject
		// (lang/java/avro/src/main/java/org/apache/avro/util/internal/
		// JacksonUtils.java:150-155): "float" schema → float32,
		// "double" schema → float64. The wire-faithful narrowing means
		// Default == the value the wire encoder will emit, including
		// for finite-overflow inputs (`{"default":1e100,"type":"float"}`
		// → metadata float32(+Inf), wire +Inf bits) and integer-form
		// inputs whose magnitude exceeds the mantissa (silently IEEE-
		// rounded). Users get Default.(float32) for float fields and
		// Default.(float64) for double fields, matching their Go field
		// types directly.
		//
		// String inputs are handled inline via parseFloatAcceptOverflow
		// rather than through [defaultAsFloat], which is now strict
		// (no string arm) so it can be reused at union-branch
		// matching and encode-time arms without accepting strings
		// where the spec says it shouldn't. This single-field arm
		// mirrors [coerceDefault]'s parseField-style text→float
		// coercion (schema.go) for outer FLOAT/DOUBLE schemas.
		var f float64
		switch val := val.(type) {
		case float64:
			f = val
		case float32:
			f = float64(val)
		case string:
			var err error
			if f, err = parseFloatAcceptOverflow(val, 64); err != nil {
				return val
			}
		case int64, int32, json.Number:
			var err error
			if f, err = defaultAsFloat(val); err != nil {
				return val
			}
		default:
			return val
		}
		if t.Type == "float" {
			return float32(f)
		}
		return f
	}
	if t.Type == "bytes" || t.Type == "fixed" {
		if s, ok := val.(string); ok {
			if b, err := avroJSONBytesToBytes(s); err == nil {
				return b
			}
		}
		return val
	}
	if isRecordKind(t.Type) {
		if m, ok := val.(map[string]any); ok {
			childNS := nsForChildren(t, ns)
			out := make(map[string]any, len(m))
			for k, v := range m {
				inner := v
				for i := range t.Fields {
					if t.Fields[i].Name == k {
						inner = coerceMetadataDefault(v, &t.Fields[i].Type, table, childNS)
						break
					}
				}
				out[k] = inner
			}
			return out
		}
		return val
	}
	if t.Type == "array" && t.Items != nil {
		if a, ok := val.([]any); ok {
			out := make([]any, len(a))
			for i, v := range a {
				out[i] = coerceMetadataDefault(v, t.Items, table, ns)
			}
			return out
		}
		return val
	}
	if t.Type == "map" && t.Values != nil {
		if m, ok := val.(map[string]any); ok {
			out := make(map[string]any, len(m))
			for k, v := range m {
				out[k] = coerceMetadataDefault(v, t.Values, table, ns)
			}
			return out
		}
		return val
	}
	return val
}

// nodeEffNS returns n's effective namespace: a dotted Name carries its
// own namespace (taking precedence per the spec); otherwise Namespace,
// which is already resolved ("" means the null namespace).
func nodeEffNS(n *SchemaNode) string {
	if i := strings.LastIndexByte(n.Name, '.'); i >= 0 {
		return n.Name[:i]
	}
	return n.Namespace
}

// nodeFullname returns n's fullname: the dotted Name verbatim (with a
// single LEADING dot collapsing per the null-namespace escape
// (leadingDotName) the parser normalizes at build, so an as-written
// ".x" is the fullname "x" and "." is the bare empty name; nodeEffNS's
// prefix split already yields "" for both), or the resolved namespace
// joined with the name.
func nodeFullname(n *SchemaNode) string {
	if strings.Contains(n.Name, ".") {
		if short, ok := leadingDotName(n.Name); ok {
			return short
		}
		return n.Name
	}
	if n.Namespace != "" {
		return n.Namespace + "." + n.Name
	}
	return n.Name
}

// nsForChildren returns the namespace scope in effect inside n: a named
// type opens its own scope; unnamed nodes pass the enclosing scope
// through.
func nsForChildren(n *SchemaNode, enclosing string) string {
	// Named KINDS open their own scope even with an empty short name (a
	// user WithLaxNames fn can accept ""; nodeEffNS carries the resolved
	// namespace either way); the Name != "" arm keeps hand-built names on
	// non-named kinds scoping as before.
	if n != nil && (n.Name != "" || isNamedKind(n.Type)) {
		return nodeEffNS(n)
	}
	return enclosing
}

// lookupNameRef returns the named target of t if t.Type is a name
// reference (not a structural or primitive kind) AND table has it, else
// nil. A nil table always returns nil (synchronous-build callers disable
// name-ref resolution because the tree isn't fully walked yet). ns is
// the enclosing namespace scope at the reference site; the key order
// comes from scopedRefKeys (schema.go) so the metadata binding cannot
// drift from the wire's.
func lookupNameRef(t *SchemaNode, table map[string]*SchemaNode, ns string) *SchemaNode {
	if t == nil || table == nil {
		return nil
	}
	// Structural kinds (primitives, "record"/"error", "enum", "fixed",
	// "array", "map", "union") are schema definitions, not name-ref
	// targets. "error" is in this list per [isRecordKind] — without it,
	// a SchemaNode with t.Type=="error" would fall through to
	// table["error"] and wrongly resolve to any record literally named
	// "error" in the schema.
	switch t.Type {
	case "null", "boolean", "int", "long", "float", "double",
		"bytes", "string", "record", "error", "enum", "fixed", "array", "map", "union":
		return nil
	}
	var keys [2]string
	for _, k := range scopedRefKeys(&keys, t.Type, ns) {
		if r, ok := table[k]; ok {
			return r
		}
	}
	return nil
}

// fixupNameRefDefaults walks the SchemaNode tree once to populate a
// name-table of every reachable record/enum/fixed, then re-coerces
// HasDefault fields with the table so name-referenced defaults (and
// defaults whose union contains a name-ref branch) materialize the
// way inline-typed siblings already do via the synchronous coerce.
// The table is returned for Root's reference stamping, so both surfaces
// resolve through the identical name set.
func fixupNameRefDefaults(root *SchemaNode) map[string]*SchemaNode {
	table := map[string]*SchemaNode{}
	collectNamedTypes(root, table)
	if len(table) == 0 {
		return table
	}
	coerceTreeDefaults(root, table, "")
	return table
}

// stampNameRefs records, on every node whose Type is a name reference
// that resolves in table, the referenced definition (SchemaNode.refTarget).
// Resolution is lookupNameRef — the same scopedRefKeys precedence every
// other resolver derives from — at the reference's enclosing namespace
// scope, so the stamp cannot bind differently than the wire or the
// default coercion did. Descent is kind-bound like collectNamedTypes:
// a stray-surfaced body (an "items" on an "int") neither defines nor
// references, so nothing inside one is stamped. Root trees are
// JSON-derived and acyclic, and their depth is bounded by the parse's
// own nesting cap, so the plain recursion terminates.
func stampNameRefs(n *SchemaNode, table map[string]*SchemaNode, ns string) {
	if n == nil || len(table) == 0 {
		return
	}
	if t := lookupNameRef(n, table, ns); t != nil {
		n.refTarget = t
		n.refNS = ns
	}
	child := nsForChildren(n, ns)
	if n.Type == "array" {
		stampNameRefs(n.Items, table, child)
	}
	if n.Type == "map" {
		stampNameRefs(n.Values, table, child)
	}
	if isRecordKind(n.Type) {
		for i := range n.Fields {
			stampNameRefs(&n.Fields[i].Type, table, child)
		}
	}
	for i := range n.Branches {
		stampNameRefs(&n.Branches[i], table, child)
	}
}

// collectLocalNames gathers the fullnames of every named type defined in
// n's tree, descending the same kind-bound structure the emission walk
// treats as non-stray. Unlike collectNamedTypes it must survive arbitrary
// hand-built input, because it runs at the START of [SchemaNode.Schema],
// before the emission walk's own cycle and depth guards: visited
// terminates Items/Values pointer cycles, and depth stops chains past the
// emission walk's own ceiling (names below it sit in a region the walk
// rejects before any splice could consult them).
func collectLocalNames(n *SchemaNode, names map[string]bool, visited map[*SchemaNode]struct{}, depth int) {
	if n == nil || depth > maxSchemaJSONDepth {
		return
	}
	if _, ok := visited[n]; ok {
		return
	}
	visited[n] = struct{}{}
	if isNamedKind(n.Type) {
		if fn := nodeFullname(n); fn != "" {
			names[fn] = true
		}
	}
	if n.Type == "array" {
		collectLocalNames(n.Items, names, visited, depth+1)
	}
	if n.Type == "map" {
		collectLocalNames(n.Values, names, visited, depth+1)
	}
	if isRecordKind(n.Type) {
		for i := range n.Fields {
			collectLocalNames(&n.Fields[i].Type, names, visited, depth+1)
		}
	}
	for i := range n.Branches {
		collectLocalNames(&n.Branches[i], names, visited, depth+1)
	}
}

// nodeCarriesNothingBut walks n's exported fields and reports whether every
// one of them other than Type is zero, skipping the fields exempt reports as
// carrying no loss.
//
// The walk is DERIVED from the struct's field set rather than written as a
// list of the fields someone remembered, and that distinction is the whole
// point of both callers below. Each previously held its own hand-written
// list; both lists were missing members, so a value in a forgotten field
// vanished. A subset can always be missing a member; asking the field set
// cannot. There is one walk rather than two so a later field cannot be seen
// by one question and overlooked by the other.
//
// Unexported state is skipped deliberately: it is derived bookkeeping (the
// name-reference stamp and its scope), not as-written content, so it must
// not force a different emission.
func nodeCarriesNothingBut(n *SchemaNode, exempt func(*SchemaNode, string) bool) bool {
	rv := reflect.ValueOf(n).Elem()
	t := rv.Type()
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() || f.Name == "Type" || exempt(n, f.Name) {
			continue
		}
		if !rv.Field(i).IsZero() || nodePresenceSet(n, f.Name) {
			return false
		}
	}
	return true
}

// presenceSet is a bit per attribute that can be written with a body its
// destination stores as that destination's own zero. One field rather than
// one bool each keeps the hidden state on the public struct at a single
// member, and keeps every consumer asking the same question.
type presenceSet uint16

const (
	presDoc presenceSet = 1 << iota
	presLogicalType
	presName
	presNamespace
	presAliases
	presSymbols
	presSize
	presFields
)

func (p presenceSet) has(b presenceSet) bool { return p&b != 0 }

func (p *presenceSet) setIf(cond bool, b presenceSet) {
	if cond {
		*p |= b
	}
}

// presenceBitFor maps an exported SchemaNode field name to its bit, so the
// emptiness walk, the emitter and the guards all key on one vocabulary.
func presenceBitFor(field string) (presenceSet, bool) {
	switch field {
	case "Doc":
		return presDoc, true
	case "LogicalType":
		return presLogicalType, true
	case "Name":
		return presName, true
	case "Namespace":
		return presNamespace, true
	case "Aliases":
		return presAliases, true
	case "Symbols":
		return presSymbols, true
	case "Size":
		return presSize, true
	case "Fields":
		return presFields, true
	}
	return 0, false
}

// nodePresenceSet reports whether the named exported field carries content
// its VALUE cannot show — an attribute written as the field's own zero.
//
// Without it the emptiness walk above and the emitter disagree by
// construction: the walk asks IsZero, so a doc written as "" reads as an
// empty node and the shortcut collapses it to a bare type name, discarding
// the very attribute the emitter was taught to write. The two questions
// ("does this node carry anything" and "does this node emit anything") must
// read the SAME state or the answer depends on which one runs first.
//
// It is keyed by field NAME so it composes with the exemption sets rather
// than overriding them: at a name-reference splice Doc and LogicalType are
// exempt usage-site attributes (NOT_BUGS #25), so their presence is ignored
// there exactly as their value is.
func nodePresenceSet(n *SchemaNode, field string) bool {
	b, ok := presenceBitFor(field)
	return ok && n.present.has(b)
}

// bareEmissionExempt classifies the fields whose value cannot be lost by
// collapsing a node to its bare type name, because they have no emitted form
// there at all. An exemption is a CLAIM, and
// TestInvariant_BareEmissionCoversEverySchemaNodeField checks both halves of
// it: an exempt field must not block, and a non-exempt field must both block
// and survive the emit → re-parse round trip.
func bareEmissionExempt(n *SchemaNode, field string) bool {
	switch field {
	case "Branches":
		// No JSON key routes to Branches outside a union — the union arm
		// returns before reaching here — so a hand-built value on another
		// kind is inert and collapsing cannot lose it.
		return n.Type != "union"
	case "EnumDefault":
		// HasEnumDefault is the carrier: the "default" key is emitted from
		// it, and a node with EnumDefault set but HasEnumDefault false
		// declares no default at all, so there is nothing to carry. The
		// carrier itself is NOT exempt.
		return !n.HasEnumDefault
	}
	return false
}

// nameRefUsageSiteExempt classifies the fields a node may carry and still be
// emitted as a pure NAME REFERENCE — that is, the fields whose loss at a
// reference's usage site is already adjudicated, so blocking on them would
// convert a documented silent drop into a hard parse error.
//
//   - Doc, Aliases, Namespace and LogicalType are reserved USAGE-SITE
//     attributes on a wrapped reference (`{"type":"Inner","doc":"x"}`). The
//     parse lands them on these structural fields, and a definition cannot
//     carry a second name, namespace or doc for one of its usage sites, so
//     the splice-to-definition drops them by design.
//   - Props is the wrapper's custom properties, which the splice MERGES onto
//     the definition (definition-wins, reserved keys dropped) rather than
//     discarding.
//
// Every other field blocks: the node then renders as-written instead of
// splicing, so nothing it carries is silently discarded and the re-parse
// judges the hybrid loudly. Precision and Scale are NOT exempt even though
// they too are usage-site attributes, because the parse routes an unconsumed
// precision/scale to Props rather than to these fields — a non-zero value
// here can only have come from a caller writing the field directly, and that
// write must not vanish.
func nameRefUsageSiteExempt(_ *SchemaNode, field string) bool {
	switch field {
	case "Doc", "Aliases", "Namespace", "LogicalType", "Props":
		return true
	}
	return false
}

// nodeCarriesOnlyType reports whether n holds no information beyond its
// Type, so collapsing it to the bare type name (`"int"` rather than
// `{"type":"int"}`) loses nothing.
func nodeCarriesOnlyType(n *SchemaNode) bool {
	return nodeCarriesNothingBut(n, bareEmissionExempt)
}

// nodeIsNameRefShape reports whether n can be emitted as a pure name
// reference (bare, or wrapped with custom properties): no structural,
// naming, or kind-specific keys of its own beyond the usage-site attributes
// a splice is already documented to drop or merge. A stamped node that fails
// this (a caller grafted content onto an extracted reference) renders
// as-written instead of splicing, so nothing it carries is silently
// discarded — the re-parse then judges the hybrid loudly.
func nodeIsNameRefShape(n *SchemaNode) bool {
	return nodeCarriesNothingBut(n, nameRefUsageSiteExempt)
}

// nodeRefTargetAgrees reports whether n's stamped refTarget is still the type
// n's exported Type NAMES. The stamp is hidden state that survives a struct
// copy — which is exactly how a caller extracts a sub-node — so a caller who
// then edits Type would otherwise get the ORIGINAL spelling's definition
// spliced in, hidden state silently beating the exported field they just set.
//
// Agreement is decided by ASKING THE RESOLVER, never by restating which
// spellings it binds: lookupNameRef against a one-entry table holding only
// the stamped target. That inherits every form scopedRefKeys admits (the
// fullname, a short name qualified by the enclosing namespace, and the
// leading-dot null-namespace escape), the fullname-vs-Name distinction, and
// the structural-kind rejection — including any later change to the resolver.
// A hand-written list of accepted spellings is a snapshot that silently
// under-accepts the day the resolver grows a form.
//
// The scope asked at is the one the stamp was MADE in (refNS), not the
// walk's current enclosing namespace. Extraction is the whole point of the
// splice, and an extracted node is re-rooted at the null namespace, so
// asking at the walk's scope would call a short-name reference stale purely
// because it was lifted out of its namespace — the node was never edited.
// The question this predicate answers is "is Type still the spelling that
// produced this stamp", and only the stamping scope can answer it.
//
// Anything else — a primitive, a different name — means the node was edited
// after Root() stamped it, so the stamp is stale and ignored; the node then
// renders as an as-written reference and behaves exactly like a hand-built
// one, binding to a definition the converted tree provides or dangling
// loudly.
func nodeRefTargetAgrees(n *SchemaNode) bool {
	t := n.refTarget
	if t == nil {
		return false
	}
	fn := nodeFullname(t)
	if fn == "" {
		return false
	}
	return lookupNameRef(n, map[string]*SchemaNode{fn: t}, n.refNS) == t
}

func collectNamedTypes(n *SchemaNode, table map[string]*SchemaNode) {
	if n == nil {
		return
	}
	// Empty-short-name named kinds register too when their fullname is
	// expressible (fullname "ns."), matching the wire builder's
	// registration; fullname "" has no reference spelling to serve.
	if n.Name != "" || (isNamedKind(n.Type) && nodeFullname(n) != "") { // record / enum / fixed
		// Namespace is resolved at construction (see [SchemaNode]), so
		// the fullname is direct — no inheritance walk needed here.
		// Register exactly what the wire builder registers: every type
		// under its fullname ONLY. A null-namespace type's fullname IS
		// its bare name, so it owns the bare key; also registering
		// namespaced types under their short name would make the bare
		// key last-walked-wins, binding a bare reference at
		// null-namespace scope to a different type than the wire bound
		// whenever short names collide.
		table[nodeFullname(n)] = n
	}
	// Descend only the structural fields the node's KIND binds. The
	// metadata walker surfaces stray container keys as-written (a stray
	// "items" on an "int" populates Items), so an unconditional descent
	// would register a definition-shaped stray body under its fullname —
	// and the map is last-write-wins, so a stray walked after the real
	// definition would silently become the table's answer for that name,
	// coercing name-referenced defaults through a body the wire never
	// bound. Branches stay unconditional: only genuine union parsing
	// populates them (no JSON key routes a stray there).
	if n.Type == "array" && n.Items != nil {
		collectNamedTypes(n.Items, table)
	}
	if n.Type == "map" && n.Values != nil {
		collectNamedTypes(n.Values, table)
	}
	if isRecordKind(n.Type) {
		for i := range n.Fields {
			collectNamedTypes(&n.Fields[i].Type, table)
		}
	}
	for i := range n.Branches {
		collectNamedTypes(&n.Branches[i], table)
	}
}

func coerceTreeDefaults(n *SchemaNode, table map[string]*SchemaNode, ns string) {
	if n == nil {
		return
	}
	childNS := nsForChildren(n, ns)
	for i := range n.Fields {
		f := &n.Fields[i]
		if f.HasDefault {
			f.Default = coerceMetadataDefault(f.Default, &f.Type, table, childNS)
		}
		coerceTreeDefaults(&f.Type, table, childNS)
	}
	if n.Items != nil {
		coerceTreeDefaults(n.Items, table, childNS)
	}
	if n.Values != nil {
		coerceTreeDefaults(n.Values, table, childNS)
	}
	for i := range n.Branches {
		coerceTreeDefaults(&n.Branches[i], table, childNS)
	}
}

// defaultMatchesBytesOrFixedKind mirrors the wire-encode pipeline's
// validateLeaf for "bytes" / "fixed" branches: codepoint range
// (validateAvroByteString) and, for "fixed", the exact-rune-count
// size match. The metadata-side branch selection must enforce the
// same codepoint range and fixed-size constraint as the wire path
// or [fixed:8,"string"] with a 4-char default would metadata-match
// fixed (string-kind only) while wire matches string (size check
// rejects fixed) — pattern 14a sibling of the convertDefaultBytes/
// validateDefault delegation at
// TestRegression_UnionBytesFixedDefaultMisroutedToWrongBranch.
func defaultMatchesBytesOrFixedKind(t *SchemaNode, val any) bool {
	switch v := val.(type) {
	case string:
		if err := validateAvroByteString(v, t.Type); err != nil {
			return false
		}
		if t.Type == "fixed" && len([]rune(v)) != t.Size {
			return false
		}
		return true
	case []byte:
		if t.Type == "fixed" && len(v) != t.Size {
			return false
		}
		return true
	}
	return false
}

// branchAcceptsDefault reports whether the Avro type t natively accepts
// val as a default value, using the same Go-type → Avro-type
// compatibility the wire-encode pipeline's validateDefault enforces.
// Used by coerceMetadataDefault's union-branch selection: iterate
// branches in order; the first accepting branch is the chosen one
// (matches Java's Schema.parseField first-matching-branch behavior).
//
// The numeric arms (int/long/float/double) delegate to the wire-encode
// pipeline's defaultAsInt32 / defaultAsInt64 / defaultAsFloat so the
// metadata branch selector and the wire branch selector apply the same
// per-value predicates (int32-bounds, whole-number, JSON-grammar,
// ParseFloat-ErrRange-with-Inf). float/double accept any numeric input
// per the lossy-destination policy (matching Java/fastavro), so
// ["float","int"] default 42 picks the float branch (first match) on
// both surfaces.
//
// The float/double arm rejects string defaults at union-branch matching
// (Java parity: parseField text→DoubleNode coercion at
// Schema.java:1899-1902 fires only for OUTER FLOAT/DOUBLE field types,
// not union branches). bytes/fixed branch accepts string (codepoint-
// mapped form per Avro JSON spec) or []byte.
//
// The structural arms (record/error, array, map) recurse into children
// so per-element validity mirrors the wire-side walkDefault. The record
// arm enforces required-field-no-default presence; the array/map arms
// require every item/value to itself accept against items/values.
// Without these per-element checks, a union like [{record needing X},
// {record needing nothing}] with default {} metadata-matches the first
// branch (type-only) while the wire path picks the second (Java parity);
// users type-switching on Default see a Go type that contradicts the
// wire-decoded auto-fill.
func branchAcceptsDefault(t *SchemaNode, val any, table map[string]*SchemaNode, ns string) bool {
	// Resolve a bare name-reference if the caller supplied a name-table.
	if resolved := lookupNameRef(t, table, ns); resolved != nil {
		return branchAcceptsDefault(resolved, val, table, nodeEffNS(resolved))
	}
	switch t.Type {
	case "null":
		return val == nil
	case "boolean":
		_, ok := val.(bool)
		return ok
	case "int":
		_, err := defaultAsInt32(val)
		return err == nil
	case "long":
		_, err := defaultAsInt64(val)
		return err == nil
	case "float", "double":
		_, err := defaultAsFloat(val)
		return err == nil
	case "string":
		_, ok := val.(string)
		return ok
	case "enum":
		sym, ok := val.(string)
		if !ok {
			return false
		}
		// Wire-side validateLeaf for enum rejects non-member symbols
		// (schema.go's enum arm at the `slices.Contains(node.symbols,
		// sym)` check). Enum needs its own arm — falling into the
		// string arm accepts any string regardless of symbol membership,
		// so a union [enum:{A,B}, bytes] with default "Z" would
		// metadata-match enum (type-only) while wire rejects enum and
		// picks bytes. Membership is unconditional, mirroring the wire
		// side: an empty enum accepts no default, so the union walk must
		// fall through to a later branch exactly as the wire side does
		// (a name-referenced enum branch reaches here only after
		// lookupNameRef resolution, with its symbols final; the
		// table-nil construction pass defers union selection entirely).
		return slices.Contains(t.Symbols, sym)
	case "bytes", "fixed":
		return defaultMatchesBytesOrFixedKind(t, val)
	case "record", "error":
		m, ok := val.(map[string]any)
		if !ok {
			return false
		}
		childNS := nsForChildren(t, ns)
		for i := range t.Fields {
			f := &t.Fields[i]
			fv, present := m[f.Name]
			if !present {
				if !f.HasDefault {
					return false
				}
				continue
			}
			// Coerce the child the same way the wire selector does before the
			// accept-check: validateLeaf's record/array/map arms (schema.go)
			// rewrite each child via coerceDefault, so a string in a nested
			// float/double field becomes a float and the wire selects the
			// container branch. coerceMetadataDefault is the *SchemaNode twin of
			// coerceDefault and applies the identical float/double string→float
			// coercion (it also width-narrows int/long/bytes, which the per-kind
			// predicates below already accept and which never changes acceptance);
			// without it this selector would reject a nested string-numeric field
			// the wire accepts and pick a later branch. coerceMetadataDefault
			// returns fresh containers, so a rejected sibling branch's value is
			// never mutated. NOT applied to the scalar float/double arm above, so
			// a DIRECT scalar union branch (["double","string"] default "5") still
			// rejects the numeric branch (NOT_BUGS #10). The coerced value is also
			// what coerceMetadataDefault surfaces in Default, so selection and
			// surfacing agree on the branch.
			fv = coerceMetadataDefault(fv, &f.Type, table, childNS)
			if !branchAcceptsDefault(&f.Type, fv, table, childNS) {
				return false
			}
		}
		return true
	case "array":
		arr, ok := val.([]any)
		if !ok {
			return false
		}
		if t.Items == nil {
			return true
		}
		for _, item := range arr {
			// Coerce each element to mirror the wire selector — see the
			// record arm above.
			item = coerceMetadataDefault(item, t.Items, table, ns)
			if !branchAcceptsDefault(t.Items, item, table, ns) {
				return false
			}
		}
		return true
	case "map":
		m, ok := val.(map[string]any)
		if !ok {
			return false
		}
		if t.Values == nil {
			return true
		}
		for _, v := range m {
			// Coerce each value to mirror the wire selector — see the
			// record arm above.
			v = coerceMetadataDefault(v, t.Values, table, ns)
			if !branchAcceptsDefault(t.Values, v, table, ns) {
				return false
			}
		}
		return true
	case "union":
		return firstMetadataBranchAcceptingDefault(t, val, table, ns) != nil
	}
	return false
}

// firstMetadataBranchAcceptingDefault returns the first branch of the
// union t whose [branchAcceptsDefault] accepts val (with name-ref
// resolution applied to each branch), or nil if no branch accepts.
// Mirrors the wire-side [firstUnionBranchAcceptingDefault] (schema.go)
// on the *SchemaNode public type — the two implement Avro's "first
// matching branch wins" default-resolution rule (1.12 relaxed from
// "first branch" to "any branch," with deterministic first-match
// tie-break) on opposite sides of the dual-namespace boundary.
//
// Shared by [coerceMetadataDefault] (returns the resolved branch so
// the coerce step recurses against it) and [branchAcceptsDefault]'s
// union arm (returns nil/non-nil for the accept predicate).
func firstMetadataBranchAcceptingDefault(t *SchemaNode, val any, table map[string]*SchemaNode, ns string) *SchemaNode {
	for i := range t.Branches {
		branch := &t.Branches[i]
		if resolved := lookupNameRef(branch, table, ns); resolved != nil {
			branch = resolved
		}
		if branchAcceptsDefault(branch, val, table, nsForChildren(branch, ns)) {
			return branch
		}
	}
	return nil
}

func nodeFromJSONObject(m map[string]any, parentNS string, memo strayShapeMemo) SchemaNode {
	n := SchemaNode{}

	getString(m, "type", &n.Type)
	n.present.setIf(getString(m, "name", &n.Name), presName)
	// Namespace resolves at build: an explicit attribute wins (including
	// the explicit-empty "namespace":"" null-namespace form — a DIFFERENT
	// type than one inheriting the enclosing namespace, per the spec's
	// fullname rules); an undotted named type without the attribute
	// inherits the enclosing scope. A dotted name carries its own
	// namespace; any attribute alongside it is preserved as-written for
	// fidelity but ignored, exactly as the parser ignores it.
	explicitNS, hasExplicitNS := "", false
	if s, ok := m["namespace"].(string); ok {
		explicitNS, hasExplicitNS = s, true
	}
	n.present.setIf(hasExplicitNS, presNamespace)
	switch {
	// Named kinds with an empty short name inherit/take-explicit exactly
	// like any undotted name (the parser resolves "name":"" under an
	// enclosing namespace to fullname "ns.").
	case (n.Name != "" || isNamedKind(n.Type)) && !strings.Contains(n.Name, "."):
		if hasExplicitNS {
			n.Namespace = explicitNS
		} else {
			n.Namespace = parentNS
		}
	case hasExplicitNS:
		n.Namespace = explicitNS
	}
	childNS := nsForChildren(&n, parentNS)
	// getString consumes only a STRING body, so recording presence off the
	// same read keeps the two in step: a non-string doc/logicalType is
	// routed elsewhere (dropped and Props respectively) and must not count
	// as a written one.
	//
	// doc is recorded on EVERY kind, not only the ones Apache Avro reads it
	// on. The authority for a placement is whichever reference actually HAS
	// that placement, and it governs the empty and the non-empty body
	// alike: Apache Avro has no doc slot on a primitive or a container, so
	// it cannot rule there, and this package already sides with fastavro at
	// that placement by preserving `{"type":"int","doc":"d"}`. Deriving the
	// empty twin from Apache Avro's ABSENCE while the non-empty twin
	// follows fastavro's PRESENCE would split one placement between two
	// authorities.
	n.present.setIf(getString(m, "doc", &n.Doc), presDoc)
	n.present.setIf(getString(m, "logicalType", &n.LogicalType), presLogicalType)
	// precision/scale/size are int per spec. After
	// unmarshalAnyPreservePrecision, integer JSON literals come back as
	// int64 (not float64); jsonNumericInt accepts both. Precision/Scale
	// hold validated decimal parameters only: consumption happens exactly
	// on a recognized decimal carrier, and every other placement leaves
	// the keys to the Props loop below (decimalConsumesPrecisionScale,
	// mirrored by the wire parser's extra routing).
	if decimalConsumesPrecisionScale(n.Type, n.LogicalType) {
		getInt(m, "precision", &n.Precision)
		getInt(m, "scale", &n.Scale)
	}
	// Size/aliases/symbols capture through the SAME decodes the parser's
	// arms run (decodeLaxInt, stringSliceFrom), so a structural field is
	// set exactly when the parse consumes the key out of props: a
	// malformed stray body (mixed-type array, non-integral number) rides
	// to Props verbatim as its ONLY surface — capturing a coerced image
	// of it here would fabricate metadata that appears nowhere in the
	// input. At BOUND positions the parse already validated the value, so
	// the gates never decline there.
	sizeOK := false
	if v, ok := m["size"]; ok {
		if l, err := decodeLaxInt("size", v); err == nil {
			n.Size = int(l)
			sizeOK = true
			n.present |= presSize
		}
	}
	if ss, ok, err := stringSliceFrom(m, "aliases"); err == nil && ok {
		n.Aliases = ss
		n.present |= presAliases
	}
	if ss, ok, err := stringSliceFrom(m, "symbols"); err == nil && ok {
		n.Symbols = ss
		n.present |= presSymbols
	}

	if n.Type == "enum" {
		if d, ok := m["default"].(string); ok {
			n.EnumDefault = d
			n.HasEnumDefault = true
		}
	}

	// Child schemas (items / values / field types, with flat-form fields
	// lifted) come from walkNodeChildren, so the child set, the lift
	// decision and key routing (the wire parser's own flatFieldNeedsLift /
	// flatLiftTypeMap), and each child's namespace scope cannot drift from
	// the wire parser or the SchemaCache walkers. At a BOUND position a
	// field with no type key never parses (the record build rejects a nil
	// field type) — but inside a STRAY "fields" the record build never
	// runs, so a typeless element is parseable and fires fieldNoType
	// below; every element therefore fires exactly one callback and no
	// pre-sized zero SchemaField is left behind.
	//
	// strayKeys: this walker alone also enumerates container keys the
	// node's kind does not bind — a stray "items" on an "int" — because
	// SchemaNode's contract is to SURFACE such keys as-written on the
	// matching structural field (they are kept out of Props by the
	// reserved-key loop below). Surfacing is read-only: nothing here
	// registers a name or mutates the tree, which is why the stray
	// positions are safe for this walker and gated off for every other
	// (see nodeChildVisitor.strayKeys and
	// TestMatrix_MetadataStrayKeySurfacedAsWritten).
	walkNodeChildren(m, parentNS, childNS, nodeChildVisitor{
		strayKeys:      true,
		strayShapeMemo: memo,
		fields:         func(arr []any) { n.Fields = make([]SchemaField, len(arr)); n.present |= presFields },
		field: func(i int, fm map[string]any, typeKey, scope string) {
			n.Fields[i] = metadataField(fm, nodeFromJSON(fm[typeKey], scope, memo), nil)
		},
		fieldNoType: func(i int, fm map[string]any) {
			// Typeless element inside a stray "fields": surface the
			// written attributes (name / doc / aliases / order / default
			// / props) on the field with a zero Type — as-written, never
			// a fabricated zero element.
			n.Fields[i] = metadataField(fm, SchemaNode{}, nil)
		},
		flatField: func(i int, fm map[string]any, kind, scope string) {
			// Flat (goavro-style) field format: the wire parser lifts
			// the field's defining keys into a nested type definition,
			// naming a lifted named type after the field
			// (liftFlatFieldType, schema_parse.go). The metadata tree
			// must describe that same post-lift schema — otherwise the
			// type node surfaces as an empty shell (no name / symbols /
			// items / values / size / fields), Root().Schema() cannot
			// rebuild it (the rebuild emits a nested type OBJECT, which
			// the wire lift's bare-string gate ignores, so the flat
			// shape is unrepresentable through the round trip), and the
			// lifted named type is invisible to name-reference default
			// coercion (collectNamedTypes keys on Name).
			flatType := flatLiftTypeMap(fm, kind)
			n.Fields[i] = metadataField(fm, nodeFromJSONObject(flatType, scope, memo), flatType)
		},
		items: func(key, scope string) {
			node := nodeFromJSON(m[key], scope, memo)
			n.Items = &node
		},
		values: func(key, scope string) {
			node := nodeFromJSON(m[key], scope, memo)
			n.Values = &node
		},
	})

	// Collect custom properties (anything not in the reserved set;
	// precision/scale are reserved only when consumed by a recognized
	// decimal carrier above). The keys with structural surfaces already
	// recorded their verdicts: walkNodeChildren set n.Items/n.Values/
	// n.Fields exactly when the stray body was shape-OK (the gate that
	// fired each callback IS that check), and the size/aliases/symbols
	// captures above ran the parser's own decodes — so route on the
	// recorded results instead of decoding a second time here. The
	// remaining stray keys (name/namespace) are single string asserts
	// with no compounding cost, so a fresh check is fine.
	shapeOK := func(key string, v any) bool {
		switch key {
		case "items":
			return n.Items != nil
		case "values":
			return n.Values != nil
		case "fields":
			return n.Fields != nil
		case "symbols":
			return n.Symbols != nil
		case "aliases":
			return n.Aliases != nil
		case "size":
			return sizeOK
		}
		return strayBodyShapeOK(key, v)
	}
	for k, v := range m {
		if schemaReservedKeyForObject(k, v, n.Type, n.LogicalType, shapeOK) {
			continue
		}
		if n.Props == nil {
			n.Props = make(map[string]any)
		}
		n.Props[k] = v
	}

	return n
}

// metadataField builds one SchemaField from its raw field object and
// already-built type node. flatType, non-nil for a flat-form field, is the
// lifted key set (the flatLiftTypeMap output): keys the lift routed into
// the type are excluded from Props, and the routed doc belongs to the
// lifted type (nodeFromJSONObject reads it from flatType), not the field,
// exactly as the wire lift routes it.
func metadataField(fm map[string]any, typ SchemaNode, flatType map[string]any) SchemaField {
	sf := SchemaField{Type: typ}
	getString(fm, "name", &sf.Name)
	if d, ok := fm["default"]; ok {
		// Coerce string defaults to typed float64 for float/double
		// fields (and recurse through nested record/array/map/union
		// types), matching Java's Schema.parseField text→DoubleNode
		// coercion and the wire-encode pipeline's coerceDefault — so
		// SchemaField.Default reflects the materialized wire form
		// instead of the raw JSON string. nil name-table: best-effort
		// inline coercion only; fixupNameRefDefaults (called at the end
		// of Root) re-coerces with a populated table to resolve
		// name-references that aren't visible during this per-field
		// construction.
		sf.Default = coerceMetadataDefault(d, &sf.Type, nil, "")
		sf.HasDefault = true
	}
	if flatType == nil {
		sf.docSet = getString(fm, "doc", &sf.Doc)
	}
	// Field aliases read through the parser's own decode (stringSliceFrom):
	// bound fields are parse-validated and stray-fields elements are
	// shape-checked before this runs, so the gate never declines here — it
	// exists so this surface structurally cannot coerce a malformed body.
	if ss, ok, err := stringSliceFrom(fm, "aliases"); err == nil && ok {
		sf.Aliases = ss
	}
	getString(fm, "order", &sf.Order)
	// The exact-lowercase field reserved keys are consumed into the
	// SchemaField attributes above; every other key — including a
	// case-variant spelling of a reserved key, which is an ordinary
	// custom property — is preserved verbatim. Same rule as the
	// type-object Props routing (schemaReservedKeyForObject).
	for k, v := range fm {
		if fieldReservedKeys[k] {
			continue
		}
		if _, routed := flatType[k]; routed {
			continue
		}
		if sf.Props == nil {
			sf.Props = make(map[string]any)
		}
		sf.Props[k] = v
	}
	return sf
}

// decimalConsumesPrecisionScale reports whether a type object with the
// given type and logicalType (values as-written; matched exactly, like the
// parser's own logical dispatch) is a recognized decimal carrier — the one
// placement where the parser consumes "precision"/"scale" as decimal
// parameters and validates them (validateLogical's decimal arm). On every
// other placement the two keys are inert metadata surfaced as custom
// properties, matching the field level: the spec permits attributes it
// does not define as metadata, and no wire codec reads an unconsumed
// precision/scale.
func decimalConsumesPrecisionScale(typ, logical string) bool {
	return logical == "decimal" && (typ == "bytes" || typ == "fixed")
}

// schemaKeyBinds reports whether a type object of the given kind/logical
// BINDS reserved key k (raw value v) — the whole grammar in one place,
// [strayKeyBinds] for the keys the kind alone decides plus the two whose
// binding also depends on the value or the logical type.
//
// It exists so [schemaReservedKeyForObject] can ask the binding question
// once instead of enumerating the keys that are consumed. An enumeration
// of consumed keys is a hand-written list, and a subset can always be
// missing a member: "default" on a kind that binds nothing and "order" on
// every kind were captured by that list's fall-through and then dropped,
// reaching neither a structural field nor Props.
func schemaKeyBinds(k string, v any, typ, logical string) bool {
	switch k {
	case "precision", "scale":
		// Decimal parameters only on a recognized carrier; anywhere else
		// the pair is inert metadata (#71).
		return decimalConsumesPrecisionScale(typ, logical)
	case "logicalType":
		// Consumed only when string-typed: a non-string value can never
		// name a logical, so it is an ordinary custom property (Java reads
		// only textual logicalType props; fastavro and goavro treat any
		// non-matching value as inert). Mirrors the parse arm's
		// string-conditional read.
		_, isString := v.(string)
		return isString
	}
	return strayKeyBinds(typ, k)
}

// schemaReservedKeyForObject reports whether key k (value v) on a type
// object of the given kind/logical is consumed or structurally
// surfaced — and so kept OUT of Props.
//
// Reserved attribute names match ONLY their exact lowercase spelling.
// Every other key — including a case-variant spelling of a reserved key —
// is an ordinary custom property that rides to Props verbatim: it binds
// nothing, and nothing about its body changes its routing. Props == all
// raw keys minus the consumed reserved keys.
//
// The rule is a disjunction of exactly two ways a reserved key can be kept
// out of Props, and nothing else: the kind BINDS it, or the kind does not
// bind it but SURFACES it as-written on a structural field — which needs
// both a field for it to land on ([canonicalStrayKey]) and a body that
// parses as the key's schema shape. A reserved key that is neither bound
// nor surfaceable has Props as its only possible surface, which is where
// the type-level "default" and "order" land: no kind but enum binds
// "default", no kind binds "order", and neither has a SchemaNode field of
// its own on a kind that does not bind it. Java keeps both as schema
// properties for the same reason (SCHEMA_RESERVED omits both,
// Schema.java:175-176; ENUM_RESERVED adds "default" alone, :178-180), and
// fastavro 1.12.2 keeps both on every kind (executed).
//
// shapeOK answers "did this stray key's body parse as the key's schema
// shape" from a verdict the caller ALREADY computed — the parser's arms
// record it in the aobject, the metadata walker records it as it surfaces
// children — so the recorded verdict always describes the queried body.
// Consulting the recorded verdict is what keeps this routing from
// re-decoding a subtree the caller already walked: a fresh
// strayBodyShapeOK here would re-enter aschemaFromAny on the same body,
// and because that decode itself routes stray keys, the two decodes per
// level compound to O(2^depth) over a nested-stray schema. A nil shapeOK
// falls back to a fresh decode, for the one caller (the cache splice
// merge) that has no recorded verdict and walks no nested strays.
func schemaReservedKeyForObject(k string, v any, typ, logical string, shapeOK strayShapeVerdict) bool {
	if !schemaReservedKeys[k] {
		return false
	}
	if schemaKeyBinds(k, v, typ, logical) {
		return true
	}
	if canonicalStrayKey(k) == "" {
		return false
	}
	if shapeOK != nil {
		return shapeOK(k, v)
	}
	return strayBodyShapeOK(k, v)
}
