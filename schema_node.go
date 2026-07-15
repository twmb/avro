package avro

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
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
type SchemaNode struct {
	Type        string // Avro type or named type reference
	LogicalType string // e.g. date, timestamp-millis, decimal, uuid; empty if none

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
	// namespace-prefixed metadata like "com.example.tag").
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
	//     the JSON spec's codepoint-per-byte form so you can pass
	//     Default straight back to AppendEncode without conversion.
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

	// Props holds custom (non-reserved) field properties. Numbers decode as
	// in [SchemaNode.Props].
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
	b, err := json.Marshal(tree)
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
}

// Root returns a SchemaNode tree describing the parsed schema. All
// metadata is preserved (doc strings, namespaces, custom properties,
// numeric defaults). See [SchemaNode.Props] and [SchemaField.Default]
// for how values decode.
//
// Reserved Avro attribute names (such as "type", "name", "namespace",
// "doc", "aliases") are matched case-insensitively, so a custom property
// whose key differs from a reserved name only by ASCII letter case (for
// example "Aliases") is interpreted as that reserved attribute and is not
// reported in [SchemaNode.Props]. Parsing applies the same case-insensitive
// matching, so the metadata reported here stays consistent with the parsed
// schema and the encoded wire.
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
// Root re-parses the JSON on each call. Cache the result if you need
// to access it repeatedly (e.g. in a per-message processing loop).
func (s *Schema) Root() SchemaNode {
	raw, err := unmarshalAnyPreservePrecision([]byte(s.full))
	if err != nil {
		panic("avro: Schema.Root: invalid stored JSON: " + err.Error())
	}
	n := nodeFromJSON(raw, "")
	fixupNameRefDefaults(&n)
	return n
}

// toJSONDedup is like toJSON but deduplicates named types. The first
// occurrence of a named type (record, enum, fixed) emits the full
// definition; subsequent occurrences emit the name as a reference.
func (n *SchemaNode) toJSONDedup(d *deduper) any {
	b := newWalkBudget()
	return n.toJSONWalk(d.visited, d, "", 0, &b)
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
	valueWalkOK       = iota // safe to hand to jsonSerializableValue / json.Marshal
	valueWalkTooDeep         // nests past the depth budget (stack-overflow risk)
	valueWalkTooWide         // expands to too many nodes (fan-out / json.Marshal cost)
	valueWalkTooLarge        // expands to too many payload bytes (json.Marshal output size)
)

// valueWalkLimit walks v — a Props value or a SchemaField.Default, an arbitrary
// user-supplied JSON tree — the way json.Marshal will, returning a non-OK code
// when the value is unsafe to serialize at [SchemaNode.Schema]. It enforces two
// orthogonal limits, because a value is handed to jsonSerializableValue
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
	switch rv.Kind() {
	case reflect.Interface, reflect.Pointer:
		if rv.IsNil() {
			return valueWalkOK
		}
		return valueWalkLimit(rv.Elem(), depthLeft-1, b)
	case reflect.Map:
		for iter := rv.MapRange(); iter.Next(); {
			// json.Marshal emits each map key as an object key string.
			if k := iter.Key(); k.Kind() == reflect.String && !b.takeBytes(k.Len()) {
				return valueWalkTooLarge
			}
			if r := valueWalkLimit(iter.Value(), depthLeft-1, b); r != valueWalkOK {
				return r
			}
		}
	case reflect.Slice, reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			// []byte/[N]byte → codepoint/base64 scalar; charge its bytes, not a walk.
			if !b.takeBytes(rv.Len()) {
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
		// string AND json.Number (type Number string) — charge the content
		// bytes json.Marshal will copy.
		if !b.takeBytes(rv.Len()) {
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
	return n.toJSONWalk(make(map[*SchemaNode]struct{}), nil, "", 0, b)
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
func (n *SchemaNode) toJSONWalk(visited map[*SchemaNode]struct{}, d *deduper, enclosingNS string, depth int, b *walkBudget) any {
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
		// "" has no reference spelling and stays the cycle error.
		if isNamedKind(n.Type) && nodeFullname(n) != "" {
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
	if d != nil && isNamedKind(n.Type) && nodeFullname(n) != "" {
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

	switch n.Type {
	case "null", "boolean", "int", "long", "float", "double", "string", "bytes":
		if n.LogicalType == "" && len(n.Props) == 0 {
			return n.Type
		}
	case "union":
		branches := make([]any, len(n.Branches))
		for i := range n.Branches {
			branches[i] = n.Branches[i].toJSONWalk(visited, d, childNS, depth+1, b)
		}
		return branches
	}

	if n.Name == "" && n.Type != "array" && n.Type != "map" &&
		!isNamedKind(n.Type) &&
		n.Type != "union" && n.LogicalType == "" && len(n.Props) == 0 {
		return n.Type
	}

	// Dedup: remember this named type's node for the next occurrence's
	// conflict check. Store the node, not its marshaled body — marshaling
	// every named type eagerly is O(depth*subtree) on nested schemas, and
	// the body is only needed if a duplicate fullname actually appears.
	if d != nil {
		// Fullname-keyed like the duplicate check above: fullname "" has
		// no reference spelling, so it stays un-deduped (inline is its
		// only representation).
		if isNamedKind(n.Type) && nodeFullname(n) != "" {
			d.defined[nodeFullname(n)] = n
		}
	}

	m := map[string]any{"type": n.Type}
	// A named KIND always emits its name — including the empty short name
	// a user WithLaxNames fn can accept — mirroring the canonical emitter
	// (appendCanonObject) and the parser, for which a missing and an empty
	// name are the same fullname; the Name != "" arm keeps emission for
	// hand-built names on non-named kinds.
	if n.Name != "" || isNamedKind(n.Type) {
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
	} else if n.Namespace != "" && !isNamedKind(n.Type) {
		// Unnamed node with a namespace attribute set by hand: preserve
		// as-written (the parser ignores it; fidelity only).
		m["namespace"] = n.Namespace
	}
	if len(n.Aliases) > 0 {
		m["aliases"] = b.emitStrings(d, n.Aliases)
	}
	if n.Doc != "" {
		m["doc"] = b.emitString(d, n.Doc)
	}
	if n.HasEnumDefault {
		m["default"] = b.emitString(d, n.EnumDefault)
	}
	if n.LogicalType != "" {
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
	} else if n.Size != 0 {
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
	} else if len(n.Symbols) > 0 {
		m["symbols"] = b.emitStrings(d, n.Symbols)
	}
	if n.Items != nil {
		m["items"] = n.Items.toJSONWalk(visited, d, childNS, depth+1, b)
	}
	if n.Values != nil {
		m["values"] = n.Values.toJSONWalk(visited, d, childNS, depth+1, b)
	}
	// record.fields is a required attribute per the Avro spec (Complex
	// Types > Records: "fields: a JSON array, listing fields (required)"),
	// always emit for record/error types even when empty.
	if isRecordKind(n.Type) || len(n.Fields) > 0 {
		fields := make([]map[string]any, len(n.Fields))
		for i, f := range n.Fields {
			fd := map[string]any{
				"name": b.emitString(d, f.Name),
				"type": f.Type.toJSONWalk(visited, d, childNS, depth+1, b),
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
			if f.Doc != "" {
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
func nodeFromJSON(v any, parentNS string) SchemaNode {
	switch s := v.(type) {
	case string:
		return SchemaNode{Type: s}
	case []any:
		branches := make([]SchemaNode, len(s))
		for i, b := range s {
			branches[i] = nodeFromJSON(b, parentNS)
		}
		return SchemaNode{Type: "union", Branches: branches}
	case map[string]any:
		return nodeFromJSONObject(s, parentNS)
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
		// The Avro [INTEGERS] rule allows a quoted-string size (e.g.
		// "size":"16"), accepted at parse via laxInt; the metadata tree
		// must read it too, or Root().Size returns 0 and Root().Schema()
		// round-trips to "missing size".
		if len(t) <= maxLaxIntDataLen {
			if i, err := strconv.Atoi(t); err == nil {
				return i, true
			}
		}
	}
	return 0, false
}

// getCIString assigns *dst to m[key] when present and string-typed.
// Mirrors the lookupCI + type-assert pattern repeated ~6 times in
// nodeFromJSONObject and metadataField.
func getCIString(m map[string]any, key string, dst *string) {
	if v, ok := lookupCI(m, key); ok {
		if s, ok := v.(string); ok {
			*dst = s
		}
	}
}

// getCIInt assigns *dst to m[key] when present and parseable via
// jsonNumericInt (precision/scale/size).
func getCIInt(m map[string]any, key string, dst *int) {
	if v, ok := lookupCI(m, key); ok {
		if p, ok := jsonNumericInt(v); ok {
			*dst = p
		}
	}
}

// getCIStringSlice assigns *dst to m[key] when it is a []any of strings
// (aliases, symbols).
func getCIStringSlice(m map[string]any, key string, dst *[]string) {
	if v, ok := lookupCI(m, key); ok {
		if vs, ok := v.([]any); ok {
			out := make([]string, len(vs))
			for i, x := range vs {
				out[i], _ = x.(string)
			}
			*dst = out
		}
	}
}

// lookupCI looks up key k in m, matching case-insensitively, so schemas
// with non-canonical casing ("tYpe" instead of "type") round-trip through
// Root/Schema. An EXACT-case match wins first (the common path). When no
// exact match exists but multiple keys collide case-insensitively (e.g.
// both "tYpe" and "TYpe", with no plain "type"), the smallest by Unicode
// code-point wins — a deterministic tie-break for that malformed input
// (Avro keys are case-sensitive per spec, so two case-variants of one key
// are already non-conformant). This differs from a struct-decode's
// document-order resolution on that degenerate case, but is deterministic
// where a bare `for k := range m` was not — Go's randomized map iteration
// otherwise made Root() return different branches on different calls.
func lookupCI(m map[string]any, key string) (any, bool) {
	if v, ok := m[key]; ok {
		return v, true
	}
	var pickKey string
	var found bool
	for k := range m {
		if !strings.EqualFold(k, key) {
			continue
		}
		if !found || k < pickKey {
			pickKey = k
			found = true
		}
	}
	if found {
		return m[pickKey], true
	}
	return nil, false
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
func fixupNameRefDefaults(root *SchemaNode) {
	table := map[string]*SchemaNode{}
	collectNamedTypes(root, table)
	if len(table) == 0 {
		return
	}
	coerceTreeDefaults(root, table, "")
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
	if n.Items != nil {
		collectNamedTypes(n.Items, table)
	}
	if n.Values != nil {
		collectNamedTypes(n.Values, table)
	}
	for i := range n.Fields {
		collectNamedTypes(&n.Fields[i].Type, table)
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

func nodeFromJSONObject(m map[string]any, parentNS string) SchemaNode {
	n := SchemaNode{}

	getCIString(m, "type", &n.Type)
	getCIString(m, "name", &n.Name)
	// Namespace resolves at build: an explicit attribute wins (including
	// the explicit-empty "namespace":"" null-namespace form — a DIFFERENT
	// type than one inheriting the enclosing namespace, per the spec's
	// fullname rules); an undotted named type without the attribute
	// inherits the enclosing scope. A dotted name carries its own
	// namespace; any attribute alongside it is preserved as-written for
	// fidelity but ignored, exactly as the parser ignores it.
	explicitNS, hasExplicitNS := "", false
	if v, ok := lookupCI(m, "namespace"); ok {
		if s, ok := v.(string); ok {
			explicitNS, hasExplicitNS = s, true
		}
	}
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
	getCIString(m, "doc", &n.Doc)
	getCIString(m, "logicalType", &n.LogicalType)
	// precision/scale/size are int per spec. After
	// unmarshalAnyPreservePrecision, integer JSON literals come back as
	// int64 (not float64); jsonNumericInt accepts both. Precision/Scale
	// hold validated decimal parameters only: consumption happens exactly
	// on a recognized decimal carrier, and every other placement leaves
	// the keys to the Props loop below (decimalConsumesPrecisionScale,
	// mirrored by the wire parser's extra routing).
	if decimalConsumesPrecisionScale(n.Type, n.LogicalType) {
		getCIInt(m, "precision", &n.Precision)
		getCIInt(m, "scale", &n.Scale)
	}
	getCIInt(m, "size", &n.Size)
	getCIStringSlice(m, "aliases", &n.Aliases)
	getCIStringSlice(m, "symbols", &n.Symbols)

	if v, ok := lookupCI(m, "default"); ok && n.Type == "enum" {
		if d, ok := v.(string); ok {
			n.EnumDefault = d
			n.HasEnumDefault = true
		}
	}

	// Child schemas (items / values / field types, with flat-form fields
	// lifted) come from walkNodeChildren, so the child set, the lift
	// decision and key routing (the wire parser's own flatFieldNeedsLift /
	// flatLiftTypeMap), and each child's namespace scope cannot drift from
	// the wire parser or the SchemaCache walkers. A field with no type key
	// never parses (the build rejects a nil field type), so every
	// parseable field fires exactly one callback and no pre-sized zero
	// SchemaField is left behind.
	walkNodeChildren(m, parentNS, childNS, nodeChildVisitor{
		fields: func(arr []any) { n.Fields = make([]SchemaField, len(arr)) },
		field: func(i int, fm map[string]any, typeKey, scope string) {
			n.Fields[i] = metadataField(fm, nodeFromJSON(fm[typeKey], scope), nil)
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
			n.Fields[i] = metadataField(fm, nodeFromJSONObject(flatType, scope), flatType)
		},
		items: func(key, scope string) {
			node := nodeFromJSON(m[key], scope)
			n.Items = &node
		},
		values: func(key, scope string) {
			node := nodeFromJSON(m[key], scope)
			n.Values = &node
		},
	})

	// Collect custom properties (anything not in the reserved set;
	// precision/scale are reserved only when consumed by a recognized
	// decimal carrier above).
	for k, v := range m {
		if schemaReservedKeyForObject(k, n.Type, n.LogicalType) {
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
	getCIString(fm, "name", &sf.Name)
	if d, ok := lookupCI(fm, "default"); ok {
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
		getCIString(fm, "doc", &sf.Doc)
	}
	getCIStringSlice(fm, "aliases", &sf.Aliases)
	getCIString(fm, "order", &sf.Order)
	for k, v := range fm {
		if fieldReservedKeyCI(k) {
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

// reservedKeyCI is a case-insensitive wrapper for membership in a
// reserved-key map. Shared by fieldReservedKeyCI / schemaReservedKeyCI
// so the case-insensitive fall-through scan lives in one place.
func reservedKeyCI(k string, reserved map[string]bool) bool {
	if reserved[k] {
		return true
	}
	for rk := range reserved {
		if strings.EqualFold(k, rk) {
			return true
		}
	}
	return false
}

func fieldReservedKeyCI(k string) bool  { return reservedKeyCI(k, fieldReservedKeys) }
func schemaReservedKeyCI(k string) bool { return reservedKeyCI(k, schemaReservedKeys) }

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

// schemaReservedKeyForObject reports whether key k is reserved (consumed
// by the parser, excluded from custom properties) on a type object with
// the given type/logicalType: every schemaReservedKeys member, except
// that precision/scale are reserved only on a recognized decimal carrier.
// Shared by the wire parser's extra-property routing (aobjectFromMap) and
// the metadata tree (nodeFromJSONObject) so the two Props surfaces cannot
// drift.
func schemaReservedKeyForObject(k, typ, logical string) bool {
	if strings.EqualFold(k, "precision") || strings.EqualFold(k, "scale") {
		return decimalConsumesPrecisionScale(typ, logical)
	}
	return schemaReservedKeyCI(k)
}
