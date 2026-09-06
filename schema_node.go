package avro

import (
	"bytes"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

// SchemaNode is a read-write representation of an Avro schema. You get one
// from a parsed schema via [Schema.Root], or you build one directly and
// convert it with [SchemaNode.Schema].
//
// The Type field determines which other fields are relevant:
//
//   - Primitives (null, boolean, int, long, float, double, string, bytes):
//     LogicalType, Precision, Scale, Props optional; other fields ignored.
//   - record/error: Name, Fields required; Namespace, Doc, Props optional.
//   - enum: Name, Symbols required; Namespace, Doc, Props optional.
//   - array: Items required.
//   - map: Values required.
//   - fixed: Name, Size required; LogicalType, Precision, Scale, Namespace,
//     Props optional.
//   - union: Branches lists the member schemas.
//
// To reference a named type (record, enum, fixed) defined elsewhere in the
// schema, set Type to its full name (e.g. com.example.Address) and nothing
// else. In a [Schema.Root] tree, references also resolve outward. Converting
// *any* node with [SchemaNode.Schema] resolves names against the schema the
// tree came from, so a field type, union branch, or deeper node converts even
// when the definition lives outside the extracted node. A hand-built tree has
// no enclosing schema, so there you must define every referenced name within
// the tree you convert, or Schema returns an error.
type SchemaNode struct {
	Type        string // Avro type or named type reference
	LogicalType string // e.g. date, timestamp-millis, decimal, uuid; empty if none (or if the value is not a string; see Props)

	Name string // name for record, enum, fixed

	// Namespace is the named type's resolved namespace. [Schema.Root] fills it
	// for every named type, a child that inherits its enclosing namespace
	// shows that namespace here, and "" always means the null namespace,
	// never "inherit". [SchemaNode.Schema] emits a "namespace":"" escape when
	// a null-namespace type sits inside a namespaced scope, so the distinction
	// survives the round trip. A dotted Name takes precedence over this field.
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

	// Precision and Scale are the decimal logical type's parameters. We set
	// and validate them only when LogicalType is "decimal" on a bytes or
	// fixed type. Anywhere else (no logical type, an unknown or non-decimal
	// one, or a decimal on a type that does not support it) the attributes
	// are plain metadata and appear in Props, as at the field level.
	Precision int // decimal precision
	Scale     int // decimal scale

	// Props holds custom (non-reserved) schema attributes: anything in the
	// schema JSON that is not a standard Avro field (e.g. "com.example.tag").
	// A reserved structural key on a kind that does not use it ("items" on
	// an "int") also appears here when its body does not parse as a schema
	// (a stray "items":3), and the matching structural field stays zero. A
	// schema-shaped stray body instead appears as written on Items, Values,
	// or Fields. A non-string logicalType likewise appears here verbatim,
	// since only a string can name a logical type.
	//
	// Values use the natural Go types from JSON: string, bool, nil, []any,
	// map[string]any, int64 for whole numbers, float64 for fractional. A
	// number stays json.Number when neither fits: a whole number too large
	// for int64, or a fractional literal over 1024 bytes, whose digits are
	// kept verbatim rather than rounded. Whole-valued exponents collapse to
	// int64 (1e3 reads as int64(1000)); exponents overflowing float64 give
	// +/-Inf. math.NaN() re-reads as the string "NaN" after Schema()/Root(),
	// because JSON has no NaN literal; +/-Inf round-trips as float64(+/-Inf).
	//
	// When you build a node by hand, a map key with a MarshalText method
	// renders as that text whatever the key's kind, a float-kind key is an
	// error, and invalid UTF-8 in a string or key becomes U+FFFD. None of
	// this depends on the Go version. encoding/json changed all three
	// between Go 1.26 and Go 1.27, so we name keys and replace bytes
	// ourselves before it runs.
	Props map[string]any

	// refTarget is set only by [Schema.Root], on name-reference nodes, and
	// points at the referenced definition inside the same tree. Schema emits
	// that definition when the converted tree does not define the name
	// itself, which is what lets a node extracted at any depth convert.
	// Hand-built nodes leave it nil, copies carry it, and a node rebuilt
	// field-by-field drops it.
	refTarget *SchemaNode

	// present records which attributes were written, which the fields alone
	// cannot: a doc written as "" or an aliases written as [] is
	// indistinguishable from one nobody wrote. It is unexported because a
	// hand-built node has no empty doc to express. We consult it per
	// attribute, since the authority differs: Apache Avro emits doc when
	// non-null, so an empty doc survives, and aliases when non-empty, so an
	// empty list is dropped. A structural key on a kind that does not bind it
	// survives as written, since nothing else carries it.
	present presenceSet

	// refNS is the namespace scope refTarget was resolved in. We record it
	// alongside the stamp because the two are only meaningful together.
	// Whether Type still names the target is a question about the scope you
	// wrote the reference in, and an extracted node is re-rooted at the null
	// namespace, losing it. Set and read only with refTarget
	// (nodeRefTargetAgrees); a nil stamp means the value is unused.
	refNS string
}

// SchemaField represents a field in an Avro record schema.
type SchemaField struct {
	Name string     // field name
	Type SchemaNode // field schema

	// Default is the field's default value, present when HasDefault is
	// true. The Go type matches the schema:
	//
	//   - int schemas give int32, long schemas give int64. We reject
	//     out-of-range defaults at parse.
	//   - float schemas give float32, double float64. Overflows narrow to
	//     +/-Inf; NaN, +/-Inf, and a float-syntax "-0.0" round-trip. An
	//     integer-syntax "-0" is the sign-less integer 0 and reads as +0.0
	//     (matching Java and fastavro), though the wire encoder writes -0.0
	//     for it.
	//   - string and enum schemas give string.
	//   - bytes and fixed give []byte, already decoded from the JSON spec's
	//     codepoint-per-byte form.
	//   - record, array, and map give map[string]any or []any, each leaf
	//     following these same rules.
	//
	// Union defaults pick the first branch that accepts the value, and the Go
	// type tells you which: ["float","int"] with default 42 gives float32(42).
	//
	// Unlike Props, a numeric Default is never json.Number: we reject defaults
	// that do not fit the declared type.
	Default any

	HasDefault bool     // true if a default value is defined in the schema
	Aliases    []string // field aliases for schema evolution
	Order      string   // sort order: "ascending" (default), "descending", or "ignore"
	Doc        string   // documentation string

	// docSet is the field-level twin of SchemaNode's present: a field's
	// "doc" written as the empty string is a written doc, and Apache Avro
	// emits it.
	docSet bool

	// Props holds custom (non-reserved) field properties; numbers decode as
	// in [SchemaNode.Props]. A field-level "logicalType", "precision", and
	// "scale" appear here as written even when we lift them onto the
	// field's type for encoding and decoding. An unused precision or scale
	// is an ordinary property whatever its JSON shape; we only validate the
	// pair when a decimal logicalType on a bytes or fixed field uses it.
	Props map[string]any
}

// Schema parses the SchemaNode into a [*Schema] you can encode and decode
// with. We return an error if the node is invalid.
//
// Named types appearing multiple times are deduplicated by fullname: the
// first occurrence emits the definition, later ones emit the fullname as a
// reference. Two types sharing a short name across namespaces are distinct
// and both emit definitions.
//
// A node extracted from a [Schema.Root] tree may reference definitions living
// elsewhere in the enclosing schema: an earlier field, a prior [SchemaCache]
// parse, or the enclosing type itself for a recursive schema. Those resolve
// automatically. We emit the definition at the reference's first occurrence,
// so the result needs neither the enclosing schema nor any cache. A name the
// tree defines itself wins over the enclosing schema's definition. Custom
// properties on a wrapped reference move onto the emitted definition, while
// reserved usage-site attributes (doc, namespace) do not survive. Hand-built
// nodes have no enclosing schema, so there a reference the tree does not
// define is an error.
//
// opts pass through to the internal [Parse]. If you parsed the original schema
// with [SchemaOpt]s that change what Parse accepts or wires ([WithLaxNames],
// [CustomType] registrations), pass the same opts here. Otherwise the rebuilt
// schema fails to parse or silently lacks the custom wiring.
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

// ExpandReferences returns a copy of n's tree with every name reference
// replaced by the definition it names, so each occurrence of a repeated named
// type carries the full body rather than only the first. n is not modified.
//
// A reference resolves the way [SchemaNode.Schema] resolves it, so a subtree
// extracted from a [Schema.Root] tree expands even when the definition lives
// outside it.
//
// Some references stay as they are. A name that closes a cycle never expands,
// since expanding a recursive definition does not terminate. A reference that
// carries attributes of its own ({"type":"Inner","doc":"x"}) stays, because a
// definition cannot hold a second doc. And if the fully expanded tree would
// exceed an internal ceiling, nothing expands at all, since a chain of
// definitions each naming the previous twice doubles per level; we return an
// unexpanded copy rather than a partial one.
//
// [SchemaNode.Schema] collapses repeats back to references on emit, so
// n.ExpandReferences().Schema() and n.Schema() produce the same schema. It
// spells a collapsed repeat by fullname, so a reference you wrote as an
// in-scope short name comes back qualified.
func (n *SchemaNode) ExpandReferences() *SchemaNode {
	if n == nil {
		return nil
	}
	x := &expander{
		table:  map[string]*SchemaNode{},
		cyclic: map[string]bool{},
		onPath: map[string]bool{},
		done:   map[string]bool{},
		sizes:  map[string]int{},
	}
	collectNamedTypes(n, x.table)
	x.markCycles(n, "", 0)
	x.expand = x.sizeOf(n, "", 0) <= maxExpandedNodes
	out := x.copy(n, "", 0, true)
	return &out
}

// maxExpandedNodes bounds the node count a fully expanded tree may reach. It
// sits well below [maxSchemaJSONNodes] for two reasons. The unit is heavier: a
// SchemaNode is a few hundred bytes of Go struct where a JSON node is one map
// entry. And the ceiling has to leave a result you can actually hold. It is
// still far above any real schema, whose expanded node count is in the
// hundreds.
const maxExpandedNodes = 1 << 18

// expander carries one ExpandReferences run. The three passes are separate
// because each needs the previous one's answer for the whole tree: cycles
// decide which references may be followed, that set decides a name's
// expanded size, and that total decides whether anything expands. Each pass
// is linear in the tree.
type expander struct {
	table  map[string]*SchemaNode // fullname -> definition
	cyclic map[string]bool        // names that reach themselves; never expanded
	onPath map[string]bool        // markCycles: names open on the current path
	done   map[string]bool        // markCycles: names already walked
	sizes  map[string]int         // fullname -> expanded node count, saturating
	expand bool                   // false when the full expansion is over the ceiling
}

// resolve returns the definition n names and the fullname to key it by, or nil
// when n is not a bare reference to anything. n's own tree wins, then the
// [Schema.Root] stamp, the order [SchemaNode.Schema] splices in, so the two
// paths cannot bind one reference to different definitions. A stamped target
// from outside the tree joins the table, so every later pass sees it.
func (x *expander) resolve(n *SchemaNode, ns string) (*SchemaNode, string) {
	if !nodeCarriesOnlyType(n) {
		return nil, ""
	}
	t := lookupNameRef(n, x.table, ns)
	if t == nil {
		if !nodeRefTargetAgrees(n) {
			return nil, ""
		}
		t = n.refTarget
	}
	fn := nodeFullname(t)
	if fn == "" {
		return nil, ""
	}
	if _, have := x.table[fn]; !have {
		x.table[fn] = t
	}
	return t, fn
}

// markCycles fills x.cyclic with a name from every reference cycle, by DFS
// over the name graph: a reference to a name already open on the path is a
// back edge, and its head goes in the set. Cutting the heads cuts every
// cycle, so the follow in copy terminates. The set can be a superset of
// "on a cycle" in exotic graphs, which only leaves a reference unexpanded.
func (x *expander) markCycles(n *SchemaNode, ns string, depth int) {
	if n == nil || depth > maxSchemaJSONDepth {
		return
	}
	if t, fn := x.resolve(n, ns); t != nil {
		switch {
		case x.onPath[fn]:
			x.cyclic[fn] = true
		case !x.done[fn]:
			x.onPath[fn] = true
			x.markCycles(t, "", depth+1) // a definition opens its own scope
			delete(x.onPath, fn)
			x.done[fn] = true
		}
		return
	}
	child := nsForChildren(n, ns)
	if n.Type == "array" {
		x.markCycles(n.Items, child, depth+1)
	}
	if n.Type == "map" {
		x.markCycles(n.Values, child, depth+1)
	}
	if isRecordKind(n.Type) {
		for i := range n.Fields {
			x.markCycles(&n.Fields[i].Type, child, depth+1)
		}
	}
	for i := range n.Branches {
		x.markCycles(&n.Branches[i], child, depth+1)
	}
}

// sizeOf reports how many nodes n expands to, saturating at one past the
// ceiling so a doubling chain cannot overflow the sum. A reference to a cyclic
// name counts as the one node it stays.
func (x *expander) sizeOf(n *SchemaNode, ns string, depth int) int {
	if n == nil || depth > maxSchemaJSONDepth {
		return 0
	}
	if t, fn := x.resolve(n, ns); t != nil {
		if x.cyclic[fn] {
			return 1
		}
		if v, ok := x.sizes[fn]; ok {
			return v
		}
		// The non-cyclic names form a DAG, so one memo per name is exact and
		// the walk stays linear however many references reach it.
		v := x.sizeOf(t, "", depth+1)
		x.sizes[fn] = v
		return v
	}
	total := 1
	child := nsForChildren(n, ns)
	add := func(v int) {
		total = min(total+v, maxExpandedNodes+1)
	}
	if n.Type == "array" {
		add(x.sizeOf(n.Items, child, depth+1))
	}
	if n.Type == "map" {
		add(x.sizeOf(n.Values, child, depth+1))
	}
	if isRecordKind(n.Type) {
		for i := range n.Fields {
			add(x.sizeOf(&n.Fields[i].Type, child, depth+1))
		}
	}
	for i := range n.Branches {
		add(x.sizeOf(&n.Branches[i], child, depth+1))
	}
	return total
}

// copy deep-copies n, replacing it with the definition it names when n is a
// bare reference to a name the two verdicts admit.
//
// We clear follow for a definition-shaped body sitting at a stray key,
// matching stampNameRefs: the parser binds no names there, so nothing in it is
// a reference. We still copy such a body, we just do not read it for names.
func (x *expander) copy(n *SchemaNode, ns string, depth int, follow bool) SchemaNode {
	if follow && x.expand && depth <= maxSchemaJSONDepth {
		if t, fn := x.resolve(n, ns); t != nil && !x.cyclic[fn] {
			return x.copy(t, "", depth, true)
		}
	}
	out := *n // struct copy; carries the hidden Root stamps, as an extraction does
	out.Aliases = slices.Clone(n.Aliases)
	out.Symbols = slices.Clone(n.Symbols)
	out.Props = maps.Clone(n.Props)
	// Depth stops the descent, not just the follow: a tree this deep is past
	// what [SchemaNode.Schema] accepts anyway, and the alternative is the stack.
	if depth > maxSchemaJSONDepth {
		return out
	}
	child := nsForChildren(n, ns)
	if n.Items != nil {
		items := x.copy(n.Items, child, depth+1, follow && n.Type == "array")
		out.Items = &items
	}
	if n.Values != nil {
		values := x.copy(n.Values, child, depth+1, follow && n.Type == "map")
		out.Values = &values
	}
	if len(n.Fields) > 0 {
		out.Fields = slices.Clone(n.Fields)
		for i := range out.Fields {
			out.Fields[i].Aliases = slices.Clone(n.Fields[i].Aliases)
			out.Fields[i].Props = maps.Clone(n.Fields[i].Props)
			out.Fields[i].Type = x.copy(&n.Fields[i].Type, child, depth+1, follow && isRecordKind(n.Type))
		}
	}
	if len(n.Branches) > 0 {
		out.Branches = slices.Clone(n.Branches)
		for i := range out.Branches {
			// Branches stay followable on every kind: no JSON key routes a
			// stray there, so only genuine union parsing populates them.
			out.Branches[i] = x.copy(&n.Branches[i], child, depth+1, follow)
		}
	}
	return out
}

// deduper tracks named type definitions during toJSONDedup and records
// conflicting redefinitions. It also detects cycles introduced via
// *SchemaNode Items/Values pointers, the only way a SchemaNode tree can have
// true cycles, since Fields and Branches are value slices.
type deduper struct {
	defined map[string]*SchemaNode   // fullname -> first definition's node
	visited map[*SchemaNode]struct{} // seen *SchemaNode pointers (cycle detection)
	err     error                    // first conflict or cycle encountered

	// localNames holds the fullname of every named type defined anywhere in
	// the tree, collected up front (collectLocalNames). The refTarget splice
	// consults it so a reference whose definition is present, before or after
	// the reference and forward references included, stays as-written and
	// binds locally on re-parse. Only a reference to a name the tree nowhere
	// defines splices the stamped target in.
	localNames map[string]bool
}

// Root returns a SchemaNode tree describing the parsed schema. We preserve all
// metadata: doc strings, namespaces, custom properties, numeric defaults. See
// [SchemaNode.Props] and [SchemaField.Default] for how values decode.
//
// Reserved Avro attribute names ("type", "name", "namespace", "doc",
// "aliases", ...) match only by their exact lowercase spelling, as in the Avro
// reference implementations. A case variant such as "Aliases" is an ordinary
// custom property: it never binds the attribute, and we report it verbatim in
// [SchemaNode.Props]. Parsing applies the same rule, so a schema whose only
// spelling of a structural key is a case variant ("ITEMS" on an array) fails
// Parse, because the structural attribute is absent.
//
// A field written in the flat goavro-style format (a bare complex type name
// with the kind's defining key, such as "symbols" or "items", alongside the
// field's own keys) appears as it parses: the field's type is the nested
// definition we lifted out (named after the field for record, error, enum,
// and fixed), and the keys we moved into the type appear on the type node
// rather than in [SchemaField.Props]. [SchemaNode.Schema] rebuilds the nested
// form, which parses identically.
//
// Every node converts back to a usable [*Schema] via [SchemaNode.Schema],
// name-reference nodes included: the tree carries the schema's named-type
// definitions, so any subtree you extract is self-contained.
//
// Root re-parses the JSON on each call. Cache the result if you access it
// repeatedly (e.g. in a per-message loop).
func (s *Schema) Root() *SchemaNode {
	raw, err := unmarshalAnyPreservePrecision(s.full)
	if err != nil {
		panic("avro: Schema.Root: invalid stored JSON: " + err.Error())
	}
	// One shape memo for the whole walk: stray bodies nest, so re-validating
	// each subtree once per enclosing level would be O(depth^2). The wire
	// node tree rides alongside so each field default is read from the node
	// the parse validated it against.
	n := nodeFromJSON(raw, "", make(strayShapeMemo), s.node)
	// We stamp each name-reference node with its resolved target so an
	// extracted subtree converts even when the definition lives outside it.
	table := map[string]*SchemaNode{}
	collectNamedTypes(&n, table)
	stampNameRefs(&n, table, "")
	return &n
}

// toJSONDedup renders n with named types deduplicated: the first occurrence of
// a record, enum, or fixed emits the full definition, later ones emit the name
// as a reference.
func (n *SchemaNode) toJSONDedup(d *deduper) any {
	b := newWalkBudget()
	d.localNames = make(map[string]bool)
	collectLocalNames(n, d.localNames, make(map[*SchemaNode]struct{}), 0)
	return n.toJSONWalk(d.visited, d, "", 0, &b, false)
}

// jsonSerializableValue returns v with the Avro-JSON shape fixups applied,
// directly or under map[string]any / []any layers; applyJSONFixup documents
// what they are. We deep-copy a container only when a descendant needs
// converting, so the common case allocates nothing and we never mutate your
// SchemaNode storage.
func jsonSerializableValue(v any) any {
	if !needsJSONFixup(v) {
		return v
	}
	return applyJSONFixup(v)
}

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
	case string:
		return !utf8.ValidString(tv)
	case map[string]any:
		for k, val := range tv {
			if !utf8.ValidString(k) || needsJSONFixup(val) {
				return true
			}
		}
	case []any:
		return slices.ContainsFunc(tv, needsJSONFixup)
	case nil, bool, json.Number, int, int32, int64:
	default:
		return needsJSONFixupKind(v)
	}
	return false
}

// validUTF8 replaces each invalid byte of s with U+FFFD. Every string and
// map key we canonicalize goes through it, so the emitter never sees an
// invalid byte and the two encoding/json implementations, which spell the
// replacement differently (v1 as a six-byte escape, v2 as the three raw
// bytes), have nothing left to disagree on. Parse applies the same
// replacement on the way in, so a Root tree never carries one either.
func validUTF8(s string) string {
	return strings.ToValidUTF8(s, string(utf8.RuneError))
}

var jsonMarshalerType = reflect.TypeFor[json.Marshaler]()

// treeValueMarshalOpaque reports whether v defines its own JSON form: a
// MarshalJSON or MarshalText method, or json.Number. The fixups and the
// canonicalizing copies leave such values alone. The assertions use the
// value's own method set, as encoding/json does for an interface-carried
// value.
func treeValueMarshalOpaque(v any) bool {
	switch v.(type) {
	case json.Number, json.Marshaler, encoding.TextMarshaler:
		return true
	}
	return false
}

// canonicalByteSliceKind reports whether t marshals as a raw byte string: a
// uint8-kind slice whose element supplies no marshal of its own. Mirrors
// encoding/json's byte-slice rule, which consults the element's *pointer*
// method set because slice elements are addressable.
func canonicalByteSliceKind(t reflect.Type) bool {
	if t.Kind() != reflect.Slice || t.Elem().Kind() != reflect.Uint8 {
		return false
	}
	p := reflect.PointerTo(t.Elem())
	return !p.Implements(jsonMarshalerType) && !p.Implements(textMarshalerType)
}

// sliceElemMarshalPositionDependent reports whether boxing a t-typed
// slice/array element into an interface would change its marshal: a
// pointer-receiver-only marshaler is reachable from an addressable element in
// place but not from an interface-carried copy. Containers of such elements
// stay opaque rather than canonicalizing into a different []any.
func sliceElemMarshalPositionDependent(t reflect.Type) bool {
	p := reflect.PointerTo(t)
	if !p.Implements(jsonMarshalerType) && !p.Implements(textMarshalerType) {
		return false
	}
	return !t.Implements(jsonMarshalerType) && !t.Implements(textMarshalerType)
}

// canonicalStringKeyMap reports whether t's keys canonicalize to plain strings:
// every string-kind key does, named as mapKeyName names it. A non-string-kind
// key's JSON name comes from its MarshalText or its integer formatting under
// both encoding/json implementations, so those maps stay marshal-opaque
// image-owners and are never rewritten.
func canonicalStringKeyMap(t reflect.Type) bool {
	return t.Key().Kind() == reflect.String
}

// errBadMapKey is mapKeyName's verdict for a key kind with no JSON object
// name: float, bool, complex, array, a struct with no text method, or a nil
// interface.
var errBadMapKey = errors.New("avro: SchemaNode default/property value contains a map whose key type has no JSON object-key form (not a string kind, an integer kind, or a usable encoding.TextMarshaler)")

// mapKeyName returns the JSON object name we give map key k. The budget walk
// charges it and both canonicalizing copies emit it, so the name cannot
// depend on which encoding/json implementation the toolchain ships; v1 (Go
// 1.26) and v2 (Go 1.27) disagree on two arms. A key with a MarshalText
// method is named by that text whatever its kind (v2's rule; v1 skipped the
// method for string kinds). A nil pointer key is named "" without calling
// the method. A string kind is its raw string and an integer kind its
// decimal. Everything else has no name: a float has no single text form, so
// we keep v1's refusal where v2 formats one, and a nil interface key errors
// where encoding/json panics. The name comes back as written; the copies
// replace invalid UTF-8 in it.
func mapKeyName(k reflect.Value) (string, error) {
	if k.CanInterface() {
		if tm, ok := k.Interface().(encoding.TextMarshaler); ok {
			if k.Kind() == reflect.Pointer && k.IsNil() {
				return "", nil
			}
			out, err := tm.MarshalText()
			if err != nil {
				return "", fmt.Errorf("avro: SchemaNode default/property value map key MarshalText: %w", err)
			}
			return string(out), nil
		}
	}
	switch k.Kind() {
	case reflect.String:
		return k.String(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(k.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(k.Uint(), 10), nil
	}
	return "", errBadMapKey
}

// needsJSONFixupKind extends fixup detection to caller-typed values by
// reflect kind, so a named []byte or named float behaves like its canonical
// twin. Marshal-opaque values are exempt. The value-preserving fixups
// (+/-Inf, -0.0) apply to named float kinds, but the type-changing NaN to
// "NaN" stays canonical-only, so a named float NaN keeps json.Marshal's
// unsupported-value error.
func needsJSONFixupKind(v any) bool {
	if treeValueMarshalOpaque(v) {
		return false
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Float64, reflect.Float32:
		f := rv.Float()
		return math.IsInf(f, 0) || isNegativeZero(f)
	case reflect.String:
		return !utf8.ValidString(rv.String())
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
		// Only the copy honors a MarshalText on the key type, so such a map
		// always rebuilds. Left to json.Marshal, the name would depend on
		// the toolchain.
		if rv.Type().Key().Implements(textMarshalerType) {
			return true
		}
		for it := rv.MapRange(); it.Next(); {
			if !utf8.ValidString(it.Key().String()) || needsJSONFixup(it.Value().Interface()) {
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

// applyJSONFixup converts the four values encoding/json cannot round-trip
// through an Avro schema:
//
//   - +/-Inf becomes json.Number("1e1000") or "-1e1000", which re-parses to
//     Inf through parseFloatAcceptOverflow; Go's encoder rejects Inf outright.
//   - NaN becomes the string "NaN", since JSON has no NaN literal. Re-parse
//     restores NaN only for a float/double Default; in Props the string stays
//     a string.
//   - -0.0 becomes json.Number("-0.0"). Marshal renders "-0", integer syntax
//     that re-parses as +0 and flips the rebuilt default's sign.
//   - []byte becomes a codepoint-per-byte string. Marshal would base64 it and
//     the Avro parser would read the base64 text back as raw bytes.
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
	case string:
		return validUTF8(tv)
	case map[string]any:
		out := make(map[string]any, len(tv))
		for k, val := range tv {
			out[validUTF8(k)] = applyJSONFixup(val)
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

// applyJSONFixupKind is needsJSONFixupKind's conversion twin: it rebuilds the
// caller-typed value in canonical shape with the same fixups the exact-type
// arms apply, and leaves marshal-opaque values and the no-canonical-twin
// residuals untouched. A named float NaN falls through un-fixed
// (needsJSONFixupKind never selects it) so the marshal error stays loud.
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
	case reflect.String:
		return validUTF8(rv.String())
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
			name, err := mapKeyName(it.Key())
			if err != nil {
				// valueWalkLimit runs first and refuses every key it
				// cannot name, so this arm never observes an error.
				return v
			}
			out[validUTF8(name)] = applyJSONFixup(it.Value().Interface())
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

// maxSchemaJSONNodes bounds the count of JSON nodes one SchemaNode-to-JSON
// walk emits; walkBudget explains why counting is necessary. Far above any
// real schema's node count, so a usable tree is never rejected.
const maxSchemaJSONNodes = 1 << 20

// maxSchemaJSONBytes bounds the total size of scalar payload one walk emits;
// see walkBudget. Far above any real schema's serialized size.
const maxSchemaJSONBytes = 1 << 26

// walkBudget is the per-walk resource budget threaded through toJSONWalk and
// valueWalkLimit. Both axes decrement across the whole walk, structural
// nodes and Props values and Defaults alike. Neither is redundant with the
// depth bound: nodes counts emitted JSON nodes, since a shared-reference
// DAG is tiny in memory yet fans out exponentially when serialized; bytes
// counts emitted scalar payload, since K nodes sharing one L-byte string are
// O(K+L) in memory and K*L in the output. An over-budget walk stops with a
// clean error rather than hanging.
type walkBudget struct {
	nodes int
	bytes int
	// keyErr carries the map-key error behind a valueWalkBadMapKey verdict:
	// a key kind with no JSON name, a MarshalText that failed, or two keys
	// of one map that would share a name.
	keyErr error
}

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

// takeBytes charges n emitted payload bytes. Over-budget drives it negative,
// so toJSONWalk's top-of-call check and valueWalkLimit both observe
// exhaustion, and reports false, keeping the payload away from json.Marshal.
func (b *walkBudget) takeBytes(n int) bool {
	if n > b.bytes {
		b.bytes = -1
		return false
	}
	b.bytes -= n
	return true
}

// emitString charges a structural scalar string's escaped bytes, the same
// charge the value walk makes for a string, and returns it for emission with
// any invalid UTF-8 replaced (validUTF8), or "" (recording the over-budget
// error) when the byte budget is exhausted, so json.Marshal never copies a
// payload past the bound.
func (b *walkBudget) emitString(d *deduper, s string) string {
	if b.takeBytes(jsonEscapedLen(s, b.bytes)) {
		return validUTF8(s)
	}
	d.fail(errSchemaTreeBytes())
	return ""
}

// emitStrings charges a structural []string payload against both budgets and
// returns it, or an empty slice, recording the over-budget error, when
// either is exhausted. The truncation is deterministic so the dedup conflict
// comparison stays meaningful.
func (b *walkBudget) emitStrings(d *deduper, ss []string) []string {
	if !b.takeNodes(len(ss)) {
		d.fail(errSchemaTreeNodes())
		return []string{}
	}
	total := 0
	for _, s := range ss {
		total += jsonEscapedLen(s, b.bytes)
		if total > b.bytes {
			break
		}
	}
	if !b.takeBytes(total) {
		d.fail(errSchemaTreeBytes())
		return []string{}
	}
	// Your slice comes back as is unless an element carries invalid UTF-8,
	// in which case we hand back a replaced copy rather than write into it.
	for i, s := range ss {
		if utf8.ValidString(s) {
			continue
		}
		out := make([]string, len(ss))
		copy(out, ss[:i])
		for j := i; j < len(ss); j++ {
			out[j] = validUTF8(ss[j])
		}
		return out
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
// that defines its own JSON form, checking json.Marshaler then
// encoding.TextMarshaler in json.Marshal's order. json.Number is not here:
// the String arm charges it by content. Measuring costs one call to the
// caller's method, charged and dropped. A method returning an error is left
// uncharged, since json.Marshal will return that same error.
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

// jsonInvalidUTF8EmitLen is what marshalSchemaTree writes for one invalid
// UTF-8 byte in a string it receives uncanonicalized: a struct field, or text
// a marshaler returned. v1 encoding/json writes a six-byte escape; v2, the
// default from Go 1.27, writes the three raw bytes of U+FFFD. We measure it
// once so a toolchain that changes the spelling moves the charge with it.
var jsonInvalidUTF8EmitLen = func() int {
	out, err := marshalSchemaTree(string([]byte{0x80}))
	if err != nil || len(out) < 2 {
		return 6 // the larger spelling, should an emitter ever refuse the byte
	}
	return len(out) - 2 // minus the quotes
}()

// marshalSchemaTree is our one call that turns a rendered schema tree into
// bytes. The walk budget charges against exactly this emitter's escaping, and
// the census differential derives its expectation from it. A change here (an
// Encoder with SetEscapeHTML(false), say) therefore moves the charge and the
// check that proves the charge together, instead of silently parting them.
func marshalSchemaTree(tree any) ([]byte, error) { return json.Marshal(tree) }

// asciiEscapedLen is the emitted length of one byte below utf8.RuneSelf.
// Escaping is byte-local there, since a byte's cost never depends on its
// neighbours. This table plus the multi-byte arms below is therefore a complete
// description of the emitter's string output, and checking all 256 values is a
// domain proof rather than a sample.
func asciiEscapedLen(b byte) int {
	switch b {
	case '\\', '"', '\b', '\f', '\n', '\r', '\t':
		return 2 // two-character escape
	case '<', '>', '&':
		return 6 // < and friends: the emitter escapes HTML
	}
	if b < 0x20 {
		return 6 // \u00XX
	}
	return 1
}

// jsonEscapedLen reports how many bytes the emitter writes for s's content
// between the quotes, stopping once the running total passes limit. We
// count rather than emit, since emitting would allocate the image the budget
// exists to prevent, which is why the escape rules are restated here. The
// early exit bounds the scan by the budget rather than the input.
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
				n += jsonInvalidUTF8EmitLen
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
				n += jsonInvalidUTF8EmitLen
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
// fixup renders as the Avro codepoint string. Byte v becomes U+00v, so a byte
// at or above 0x80 costs the two bytes of its UTF-8 form and everything below
// costs what the ASCII table says. We charge the value's json-facing image
// rather than its Go shape: a []byte never reaches the emitter as a byte slice.
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
// U+2029 (three becoming six), and drops insignificant whitespace. We count
// only the growth: ignoring the shrinkage over-charges slightly, which is
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

// valueWalkLimit walks an arbitrary Props value or SchemaField.Default the
// way json.Marshal will, returning a non-OK code when it is unsafe to
// serialize. Three limits apply: depth, since nested far enough the fixup
// walk or json.Marshal overflows the stack uncatchably; expansion (b.nodes),
// since a value sharing a sub-value across sibling paths fans out into a
// 2^depth tree, and decrementing per node also terminates the walk on a
// cyclic Go type; and payload size (b.bytes), since huge or widely shared
// leaves are small in memory yet expand in the output. We mirror what
// json.Marshal recurses into, not only the shapes Root produces, since a
// hand-built node can store any Go value.
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
	// json.Marshal never walks a value carrying its own MarshalJSON /
	// MarshalText: the method's return is the emission. Recursing here would
	// charge the Go shape while json emits something else entirely (an empty
	// struct whose MarshalJSON returns a megabyte charges one node, no bytes).
	// Charge what the method emits and stop, mirroring json's own dispatch.
	// Charging reads the output and discards it, so rendering is unchanged.
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
		// We name every key ourselves (mapKeyName), so a key we cannot name
		// is refused here rather than forwarded to encoding/json, and two
		// keys that would share one object name are refused rather than
		// collapsed. Distinct Go keys can share a name only through
		// MarshalText, an interface key's dynamic types, or invalid UTF-8
		// becoming U+FFFD, so we track names for the first two shapes only
		// and check the third in a second pass only when a key was invalid.
		keyT := rv.Type().Key()
		track := rv.Len() > 1 && (keyT.Kind() == reflect.Interface || keyT.Implements(textMarshalerType))
		var seen map[string]struct{}
		invalid := false
		for iter := rv.MapRange(); iter.Next(); {
			name, err := mapKeyName(iter.Key())
			if err != nil {
				b.keyErr = err
				return valueWalkBadMapKey
			}
			if !b.takeBytes(jsonEscapedLen(name, b.bytes)) {
				return valueWalkTooLarge
			}
			if track {
				if seen == nil {
					seen = make(map[string]struct{}, rv.Len())
				}
				if r := b.noteMapKeyName(seen, validUTF8(name)); r != valueWalkOK {
					return r
				}
			} else if !utf8.ValidString(name) {
				invalid = true
			}
			if r := valueWalkLimit(iter.Value(), depthLeft-1, b); r != valueWalkOK {
				return r
			}
		}
		if invalid && rv.Len() > 1 {
			// Only a string-kind key can be invalid, and without a method
			// its name is the key itself.
			seen = make(map[string]struct{}, rv.Len())
			for iter := rv.MapRange(); iter.Next(); {
				if r := b.noteMapKeyName(seen, validUTF8(iter.Key().String())); r != valueWalkOK {
					return r
				}
			}
		}
	case reflect.Slice, reflect.Array:
		if canonicalByteSliceKind(rv.Type()) || (rv.Kind() == reflect.Array && rv.Type().Elem().Kind() == reflect.Uint8) {
			// The JSON fixup renders these as the Avro codepoint string, so
			// that is the image we charge, not the Go length. The gate is the
			// fixup's own predicate. A byte slice whose element type carries
			// a marshaler (which the fixup declines to rewrite, and json
			// emits as an array) falls through to the walk below instead of
			// being charged as a scalar it never becomes.
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
		// string and json.Number (type Number string): charge what the
		// emitter writes, which is the escaped form. A control byte costs
		// six output bytes, and the emitter escapes HTML.
		if !b.takeBytes(jsonEscapedLen(rv.String(), b.bytes)) {
			return valueWalkTooLarge
		}
	}
	return valueWalkOK
}

// noteMapKeyName records one emitted map key name in seen, refusing a repeat.
func (b *walkBudget) noteMapKeyName(seen map[string]struct{}, name string) int {
	if _, dup := seen[name]; dup {
		b.keyErr = fmt.Errorf("avro: SchemaNode default/property value contains a map with two keys that share the JSON object name %q", name)
		return valueWalkBadMapKey
	}
	seen[name] = struct{}{}
	return valueWalkOK
}

// boundedSerializableValue applies jsonSerializableValue to a Props value or
// SchemaField.Default after bounding it through valueWalkLimit. depth is the
// structural nesting toJSONWalk has already accrued, so the value may add at
// most maxSchemaJSONDepth-depth further levels. Exceeding any bound records the
// error on the dedup path (so [SchemaNode.Schema] returns it) and truncates to
// nil on the bare path, matching toJSONWalk's own over-limit handling.
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
		err := b.keyErr
		if err == nil {
			err = errBadMapKey
		}
		b.keyErr = nil
		d.fail(err)
		return nil
	}
	return jsonSerializableValue(v)
}

// toJSONShared snapshots n's full JSON body, no dedup, for toJSONWalk's
// conflict comparison, charging the shared budget. The dedup walk charges
// one node per re-occurrence, so the outer budget alone would leave k copies
// of a w-node body costing k*w in comparison work. On exhaustion the caller
// reports over-budget rather than a spurious conflict, since asymmetric
// truncation could make identical bodies compare unequal.
func (n *SchemaNode) toJSONShared(b *walkBudget) any {
	return n.toJSONWalk(make(map[*SchemaNode]struct{}), nil, "", 0, b, false)
}

// toJSONWalk is the cycle-aware walker behind both the dedup walk and the
// bare one. visited terminates Items/Values pointer cycles. A non-nil d
// tracks named-type definitions and reports conflicting redefinitions.
// enclosingNS is the scope at this node's position; named types emit their
// namespace relative to it and name references emit the fullname. depth
// bounds a distinct-node-per-level chain, which visited cannot see, at the
// same ceiling as maxSchemaJSONDepth. stray is true when we reached n
// through a structural key its parent's kind does not bind; the wire parser
// binds no names there, so we render verbatim and the dedup skips it, or a
// definition-shaped stray body would stand in for the real definition.
func (n *SchemaNode) toJSONWalk(visited map[*SchemaNode]struct{}, d *deduper, enclosingNS string, depth int, b *walkBudget, stray bool) any {
	if depth > maxSchemaJSONDepth {
		d.fail(fmt.Errorf("avro: SchemaNode tree nests deeper than the supported limit (%d)", maxSchemaJSONDepth))
		return nil
	}
	// Charge this node against the shared budget; walkBudget explains why the
	// depth bound alone is not enough. Once either axis is exhausted every
	// further node returns early without descending, pruning at the frontier.
	if b.bytes < 0 {
		d.fail(errSchemaTreeBytes())
		return nil
	}
	if !b.takeNode() {
		d.fail(errSchemaTreeNodes())
		return nil
	}
	// We charge type / name / namespace *before* hashing them into the dedup
	// map, scanning them, or emitting them as a reference, so a huge shared
	// Name/Namespace cannot amplify per occurrence. The type switches below
	// short-circuit on length, so charging once here covers every later
	// emission of the three.
	if !b.takeBytes(len(n.Type) + len(n.Name) + len(n.Namespace)) {
		d.fail(errSchemaTreeBytes())
		return nil
	}
	if _, cycle := visited[n]; cycle {
		// A cycle through Items/Values back to n. Named types emit the
		// fullname as a reference; unnamed cycles are an error in the dedup
		// walker and nil-stable JSON in the bare one. Keyed on the fullname
		// being expressible: "ns." is a valid reference target while "" has
		// no spelling.
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

	// Dedup: an already-emitted named type becomes a name reference, and a
	// redefinition with a different body is a conflict. We key by fullname,
	// as the spec defines name equality. A bare reference from inside a
	// namespaced scope that collides with an in-scope short name re-binds
	// in-scope, the same ambiguity Java has.
	if d != nil && !stray && isNamedKind(n.Type) && nodeFullname(n) != "" {
		if prev, exists := d.defined[nodeFullname(n)]; exists {
			// We marshal-compare bodies only for *distinct* nodes: a named
			// type referenced repeatedly resolves to the same *SchemaNode and
			// is definitionally equal. Deferring to an actual collision keeps
			// the common case O(n) instead of eagerly marshaling every named
			// type's subtree. The comparison uses toJSONShared, so many
			// identical-bodied duplicates stay inside the budget.
			if prev != n && d.err == nil {
				cur, _ := json.Marshal(n.toJSONShared(b))
				prevB, _ := json.Marshal(prev.toJSONShared(b))
				switch {
				case b.nodes <= 0:
					// Budget exhausted mid-comparison: the bodies are
					// truncated, so comparing them is meaningless. Report
					// over-budget rather than risk a spurious conflict from
					// asymmetric truncation.
					d.err = errSchemaTreeNodes()
				case b.bytes < 0:
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

	// A reference to a name this tree does not define emits the stamped
	// definition at its first occurrence through this same recursion, so an
	// extracted subtree is self-contained. Gated on d != nil so conflict
	// snapshots stay splice-free, and on !stray. A reference the tree defines
	// locally stays as written.
	refType := n.Type
	if d != nil && !stray && nodeRefTargetAgrees(n) && nodeIsNameRefShape(n) {
		if fn := nodeFullname(n.refTarget); fn != "" && !d.localNames[fn] {
			if _, emitted := d.defined[fn]; !emitted {
				// A fresh visited map: a recursive definition reaches back
				// through the extraction point, which the shared map would
				// misread as an unnamed cycle. The revisit is finite because
				// the target registers in d.defined before walking its
				// children, and the shared budgets bound the emission.
				spliced := n.refTarget.toJSONWalk(make(map[*SchemaNode]struct{}), d, enclosingNS, depth, b, false)
				// A wrapped reference's custom properties ride onto the
				// spliced definition, definition-wins and reserved keys
				// dropped, same as the SchemaCache splice's wrapper arm.
				if m2, ok := spliced.(map[string]any); ok && len(n.Props) > 0 {
					defTyp, _ := m2["type"].(string)
					defLogical, _ := m2["logicalType"].(string)
					for k, v := range n.Props {
						if !b.takeBytes(len(k)) {
							d.fail(errSchemaTreeBytes())
							continue
						}
						pv := boundedSerializableValue(d, depth, b, v)
						if schemaReservedKeyForObject(k, pv, defTyp, defLogical, strayPresence(k, pv)) {
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
			// definition): reference it by fullname, the spelling that
			// re-binds regardless of the standalone parse's
			// namespace scope at this position.
			refType = fn
		}
	}

	switch n.Type {
	case "null", "boolean", "int", "long", "float", "double", "string", "bytes":
		// Bare-string emission is only lossless when the node carries
		// nothing but its Type: see nodeCarriesOnlyType, which derives that
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

	// The same losslessness question as the primitive arm above, for a name
	// reference: it may collapse to the bare name only when the node carries
	// nothing else. Both sites ask nodeCarriesOnlyType rather than repeating
	// a field list: two hand-written copies drift, and one that misses a
	// field silently drops the stray attribute stored there.
	if n.Type != "array" && n.Type != "map" && !isNamedKind(n.Type) &&
		n.Type != "union" && nodeCarriesOnlyType(n) {
		return refType
	}

	// We remember this named type's node for the next occurrence's conflict
	// check, storing the node rather than its marshaled body: eager marshaling
	// is O(depth*subtree), and we need the body only if a duplicate appears.
	if d != nil && !stray {
		// Fullname-keyed like the duplicate check above. Fullname "" has no
		// reference spelling so it stays un-deduped, and stray-reached names
		// register nothing; the wire parser binds neither.
		if isNamedKind(n.Type) && nodeFullname(n) != "" {
			d.defined[nodeFullname(n)] = n
		}
	}

	m := map[string]any{"type": refType}
	// A named kind always emits its name, the empty short name a WithLaxNames
	// fn can accept included, matching appendCanonObject and the parser, for
	// which a missing and an empty name are the same fullname. The Name != ""
	// arm keeps emission for hand-built names on non-named kinds.
	if n.Name != "" || isNamedKind(n.Type) || n.present.has(presName) {
		m["name"] = n.Name
	}
	if isNamedKind(n.Type) && !strings.Contains(n.Name, ".") {
		// Namespace relative to the enclosing scope, mirroring Java's
		// Name.writeName: omitted when equal (re-parse inherits it),
		// "namespace":"" to escape inheritance for a null-namespace type in a
		// namespaced scope, the value otherwise. A dotted Name carries its own
		// namespace and gets no attribute, since the spec ignores it there.
		switch eff := n.Namespace; {
		case eff == enclosingNS:
			// inherited (or both null): omit
		case eff == "":
			m["namespace"] = ""
		default:
			m["namespace"] = eff
		}
	} else if (n.Namespace != "" || n.present.has(presNamespace)) && !isNamedKind(n.Type) {
		// Unnamed node with a namespace attribute: we preserve it as-written
		// (the parser ignores it; fidelity only). Presence carries the
		// explicit-empty form, which the value alone cannot show.
		m["namespace"] = n.Namespace
	}
	// Where a kind binds aliases, Apache Avro emits only a non-empty list.
	// Where it does not bind, presence decides, since the
	// key is carried nowhere else.
	if len(n.Aliases) > 0 || (n.present.has(presAliases) && !strayKeyBinds(n.Type, "aliases")) {
		m["aliases"] = b.emitStrings(d, n.Aliases)
	}
	// doc emits when written, not when non-empty: an empty doc is a doc, and
	// Apache Avro emits it.
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
	// size is required on fixed and 0 is a legal size, so omitting the zero
	// would make the re-emitted schema unparseable. On any other kind it is a
	// stray, surfaced as-written: the same required-or-as-written shape the
	// symbols and fields rules below use.
	if n.Type == "fixed" || n.Size != 0 || n.present.has(presSize) {
		m["size"] = n.Size
	}
	// symbols is required on enum (spec, Complex Types > Enums), so it emits
	// even when empty, and as [] rather than null, which is unparseable.
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
	// fields is required on record/error (spec, Complex Types > Records), so
	// it emits even when empty.
	if isRecordKind(n.Type) || len(n.Fields) > 0 || n.present.has(presFields) {
		fieldStray := stray || !isRecordKind(n.Type)
		fields := make([]map[string]any, len(n.Fields))
		for i, f := range n.Fields {
			fd := map[string]any{
				"name": b.emitString(d, f.Name),
				"type": f.Type.toJSONWalk(visited, d, childNS, depth+1, b, fieldStray),
			}
			if f.HasDefault || f.Default != nil {
				// Inverse of the metadata-API normalization: a Root() of
				// "default":1e1000 holds +/-Inf, which json.Marshal rejects,
				// so applyJSONFixup puts the literal back.
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
				// We emit the Props key as a JSON object key, so charge it,
				// then the value through the depth+node+byte-bounded walk.
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
// "namespace" attribute resolve into it (see [SchemaNode].Namespace). wire
// is the compiled node at this position, or nil where the parse built none
// (a stray body); the walk descends the two in step, since the JSON is what
// the node tree was built from.
func nodeFromJSON(v any, parentNS string, memo strayShapeMemo, wire *schemaNode) SchemaNode {
	switch s := v.(type) {
	case string:
		return SchemaNode{Type: s}
	case []any:
		branches := make([]SchemaNode, len(s))
		for i, b := range s {
			branches[i] = nodeFromJSON(b, parentNS, memo, wireBranch(wire, i))
		}
		return SchemaNode{Type: "union", Branches: branches}
	case map[string]any:
		return nodeFromJSONObject(s, parentNS, memo, wire)
	default:
		return SchemaNode{}
	}
}

// wireField, wireItems, wireValues and wireBranch return the compiled node
// at one child position of n, or nil when n is nil or its kind does not
// bind that position: a stray body has no compiled twin.
func wireField(n *schemaNode, i int) *fieldNode {
	if n == nil || !isRecordKind(n.kind) || i >= len(n.fields) {
		return nil
	}
	return &n.fields[i]
}

func wireFieldNode(n *schemaNode, i int) *schemaNode {
	if f := wireField(n, i); f != nil {
		return f.node
	}
	return nil
}

func wireItems(n *schemaNode) *schemaNode {
	if n == nil || n.kind != "array" {
		return nil
	}
	return n.items
}

func wireValues(n *schemaNode) *schemaNode {
	if n == nil || n.kind != "map" {
		return nil
	}
	return n.values
}

func wireBranch(n *schemaNode, i int) *schemaNode {
	if n == nil || n.kind != "union" || i >= len(n.branches) {
		return nil
	}
	return n.branches[i]
}

// Known schema keys that are *not* custom properties.
var schemaReservedKeys = map[string]bool{
	"type": true, "name": true, "namespace": true, "doc": true,
	"fields": true, "symbols": true, "items": true, "values": true,
	"size": true, "logicalType": true, "precision": true, "scale": true,
	"aliases": true, "default": true, "order": true,
}

// Known field keys that are *not* custom properties.
var fieldReservedKeys = map[string]bool{
	"name": true, "type": true, "default": true, "doc": true,
	"aliases": true, "order": true,
}

// jsonNumericInt accepts a value parsed via unmarshalAnyPreservePrecision
// (int64 for integer literals). It falls through to float64 / json.Number for
// values originating from a bare encoding/json Unmarshal, primarily SchemaNode
// trees you built programmatically and round-tripped through Schema().Root().
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
		// (precision/scale on a validated decimal carrier; size reads
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
// reports whether it did, which is the attribute's presence as distinct from
// its value. Reserved names match only their exact lowercase spelling, as in
// Java and fastavro; a case variant is an ordinary custom property.
func getString(m map[string]any, key string, dst *string) bool {
	s, ok := m[key].(string)
	if ok {
		*dst = s
	}
	return ok
}

func getInt(m map[string]any, key string, dst *int) {
	if v, ok := m[key]; ok {
		if p, ok := jsonNumericInt(v); ok {
			*dst = p
		}
	}
}

// isRecordKind reports whether typ names the Avro record kind in
// [SchemaNode.Type]. "record" and "error" are both JSON literals for the same
// on-wire kind (the RPC convention names error-record types "error"), and the
// builder normalizes both to node.kind=="record". SchemaNode.Type keeps the
// name as written, so every metadata dispatcher branching on the record kind
// must accept either alias. Centralizing it here stops the set drifting.
func isRecordKind(typ string) bool {
	return typ == "record" || typ == "error"
}

// isNamedKind reports whether typ is one of the four Avro named-type kinds
// (record / error / enum / fixed), the set that carries a Name and can be
// referenced, deduped, and aliased. "error" is the record alias and always
// travels with "record". The named-type analogue of [isRecordKind].
func isNamedKind(typ string) bool {
	return typ == "record" || typ == "error" || typ == "enum" || typ == "fixed"
}

// nodeEffNS returns n's effective namespace and nodeFullname its fullname,
// both by resolveScope over the node's own Name and Namespace. Namespace is
// already resolved, so it stands in for a written attribute and "" means the
// null namespace; a dotted Name takes precedence over it, as at parse.
func nodeEffNS(n *SchemaNode) string {
	_, ns := resolveScope(n.Name, n.Namespace, true, "")
	return ns
}

func nodeFullname(n *SchemaNode) string {
	fullname, _ := resolveScope(n.Name, n.Namespace, true, "")
	return fullname
}

// nsForChildren returns the namespace scope in effect inside n: a named
// type opens its own scope; unnamed nodes pass the enclosing scope
// through.
func nsForChildren(n *SchemaNode, enclosing string) string {
	// Named *kinds* open their own scope even with an empty short name (a user
	// WithLaxNames fn can accept ""; nodeEffNS carries the resolved namespace
	// either way). The Name != "" arm keeps hand-built names on non-named kinds
	// scoping as before.
	if n != nil && (n.Name != "" || isNamedKind(n.Type)) {
		return nodeEffNS(n)
	}
	return enclosing
}

// lookupNameRef returns the named target of t when t.Type is a name reference
// (not a structural or primitive kind) and table has it, else nil. A nil table
// always returns nil: synchronous-build callers disable name-ref resolution
// because the tree isn't fully walked yet. ns is the enclosing namespace scope
// at the reference site. The key order comes from scopedRefKeys (schema.go), so
// the metadata binding cannot drift from the wire's.
func lookupNameRef(t *SchemaNode, table map[string]*SchemaNode, ns string) *SchemaNode {
	if t == nil || table == nil {
		return nil
	}
	// Structural kinds (primitives, "record"/"error", "enum", "fixed",
	// "array", "map", "union") are schema definitions, not name-ref
	// targets. "error" is in this list per [isRecordKind]: without it,
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

// stampNameRefs records, on every node whose Type is a name reference that
// resolves in table, the referenced definition. Resolution is lookupNameRef
// at the reference's enclosing scope, so the stamp cannot bind differently
// than the wire did. Descent is kind-bound, so nothing inside a stray body
// is stamped. Root trees are acyclic and depth-bounded by the parse.
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

// collectLocalNames gathers the fullnames of every named type defined in n's
// tree, descending the same kind-bound structure the emission walk treats as
// non-stray. It runs before the emission walk's own guards on arbitrary
// hand-built input, so it carries its own cycle and depth guards.
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
// one other than Type is zero, skipping the fields exempt reports as carrying
// no loss. We derive the walk from the struct's field set rather than a
// written list, so a field added later cannot be overlooked. Unexported
// state is derived bookkeeping, not as-written content, so it is skipped.
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

// presenceSet is a bit per attribute a decoding arm consumed from a body of
// the attribute's shape. On a SchemaNode it tells a written zero from an
// unwritten one (a doc written as "", an aliases written as []), which the
// field alone cannot. On both the SchemaNode and the parser's aobject it is
// the stray-key routing verdict: a structural key the kind does not bind
// stays out of Props iff its arm set the bit. One field rather than one
// bool each keeps every consumer asking the same question.
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
	presItems
	presValues
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
	case "Items":
		return presItems, true
	case "Values":
		return presValues, true
	}
	return 0, false
}

// nodePresenceSet reports whether the named exported field carries content
// its value cannot show: an attribute written as the field's own zero.
// Without it the emptiness walk and the emitter disagree, since a doc
// written as "" reads as an empty node and collapses to a bare type name.
// It is keyed by field name so it composes with the exemption sets: at a
// name-reference splice Doc and LogicalType are exempt usage-site
// attributes, so we ignore their presence as we ignore their value.
func nodePresenceSet(n *SchemaNode, field string) bool {
	b, ok := presenceBitFor(field)
	return ok && n.present.has(b)
}

// bareEmissionExempt classifies the fields whose value cannot be lost by
// collapsing a node to its bare type name, because they have no emitted form
// there at all. An exemption is a claim with two halves, both checked: an
// exempt field must not block, and a non-exempt field must both block and
// survive the emit and re-parse round trip.
func bareEmissionExempt(n *SchemaNode, field string) bool {
	switch field {
	case "Branches":
		// No JSON key routes to Branches outside a union (the union arm
		// returns before reaching here), so a hand-built value on another
		// kind is inert and collapsing cannot lose it.
		return n.Type != "union"
	case "EnumDefault":
		// HasEnumDefault is the carrier: the "default" key is emitted from
		// it, and a node with EnumDefault set but HasEnumDefault false
		// declares no default at all, so there is nothing to carry. The
		// carrier itself is *not* exempt.
		return !n.HasEnumDefault
	}
	return false
}

// nameRefUsageSiteExempt classifies the fields a node may carry and still be
// emitted as a pure name reference. Doc, Aliases, Namespace and LogicalType
// are usage-site attributes on a wrapped reference that the splice drops,
// since a definition cannot carry a second doc per usage site, and Props are
// the wrapper's custom properties, which the splice merges onto the
// definition. Every other field blocks, so the node renders as written and
// the re-parse judges the hybrid. Precision and Scale are not exempt: the
// parse routes an unconsumed pair to Props, so a value here can only come
// from you writing the field directly.
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
// reference, bare or wrapped with custom properties. A stamped node that
// fails this renders as written instead of splicing, so nothing it carries
// is discarded.
func nodeIsNameRefShape(n *SchemaNode) bool {
	return nodeCarriesNothingBut(n, nameRefUsageSiteExempt)
}

// nodeRefTargetAgrees reports whether n's stamped refTarget is still the type
// n's exported Type names. The stamp survives a struct copy, which is how
// you extract a sub-node, so if you then edit Type the stamp is stale and we
// ignore it; the node then behaves like a hand-built reference. We ask the
// resolver against a one-entry table rather than restate which spellings it
// binds, at the stamp's own scope rather than the walk's, since an extracted
// node is re-rooted at the null namespace.
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
		// Namespace is resolved at construction, so the fullname is direct.
		// We register exactly what the wire builder registers, every type
		// under its fullname only; also registering short names would make
		// the bare key last-walked-wins whenever short names collide.
		table[nodeFullname(n)] = n
	}
	// We descend only the structural fields the node's kind binds: the
	// metadata walker carries stray container keys as written, so an
	// unconditional descent would register a definition-shaped stray body
	// under its fullname and, being last-write-wins, could replace the real
	// definition. Branches stay unconditional, since no stray routes there.
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

func nodeFromJSONObject(m map[string]any, parentNS string, memo strayShapeMemo, wire *schemaNode) SchemaNode {
	n := SchemaNode{}

	getString(m, "type", &n.Type)
	n.present.setIf(getString(m, "name", &n.Name), presName)
	// Namespace resolves at build through resolveScope, the parser's rule.
	// A dotted name carries its own namespace; we preserve any attribute
	// beside it as written but it scopes nothing. Named kinds with an empty
	// short name resolve like any undotted name (the parser registers
	// "name":"" under an enclosing namespace as fullname "ns.").
	n.present.setIf(getString(m, "namespace", &n.Namespace), presNamespace)
	if (n.Name != "" || isNamedKind(n.Type)) && !strings.Contains(n.Name, ".") {
		_, n.Namespace = resolveScope(n.Name, n.Namespace, n.present.has(presNamespace), parentNS)
	}
	childNS := nsForChildren(&n, parentNS)
	// getString consumes only a string body, so recording presence off the
	// same read keeps the two in step. We record doc on every kind: Apache
	// Avro has no doc slot on a primitive or container, and we already side
	// with fastavro by preserving {"type":"int","doc":"d"}.
	n.present.setIf(getString(m, "doc", &n.Doc), presDoc)
	n.present.setIf(getString(m, "logicalType", &n.LogicalType), presLogicalType)
	// Precision/Scale hold validated decimal parameters only: consumption
	// happens exactly on a recognized decimal carrier, and every other
	// placement leaves the keys to the Props loop below, mirroring the wire
	// parser's extra routing.
	if decimalConsumesPrecisionScale(n.Type, n.LogicalType) {
		getInt(m, "precision", &n.Precision)
		getInt(m, "scale", &n.Scale)
	}
	// We capture size/aliases/symbols through the same decodes the parser's
	// arms run, so a structural field is set exactly when the parse consumes
	// the key. A malformed stray body rides to Props verbatim as its only
	// surface.
	if v, ok := m["size"]; ok {
		if l, err := decodeLaxInt("size", v); err == nil {
			n.Size = int(l)
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

	// Child schemas come from walkNodeChildren, so the child set, the lift
	// decision and each child's namespace scope cannot drift from the wire
	// parser. Inside a stray "fields" the record build never runs, so a
	// typeless element is parseable and fires fieldNoType. This walker alone
	// also enumerates container keys the kind does not bind, since SchemaNode
	// promises to carry them as written; that is read-only, which is why the
	// stray positions are gated off for every other walker.
	walkNodeChildren(m, parentNS, childNS, nodeChildVisitor{
		strayKeys:      true,
		strayShapeMemo: memo,
		fields:         func(arr []any) { n.Fields = make([]SchemaField, len(arr)); n.present |= presFields },
		field: func(i int, fm map[string]any, typeKey, scope string) {
			n.Fields[i] = metadataField(fm, nodeFromJSON(fm[typeKey], scope, memo, wireFieldNode(wire, i)), nil, wireField(wire, i))
		},
		fieldNoType: func(i int, fm map[string]any) {
			// Typeless element inside a stray "fields": we carry the
			// written attributes (name / doc / aliases / order / default
			// / props) on the field with a zero Type, as-written, never
			// a fabricated zero element.
			n.Fields[i] = metadataField(fm, SchemaNode{}, nil, nil)
		},
		flatField: func(i int, fm map[string]any, kind, scope string) {
			// Flat goavro-style field format: the wire parser lifts the
			// field's defining keys into a nested type named after the field,
			// and the metadata tree must describe that same post-lift schema
			// or the rebuild emits an empty shell.
			flatType := flatLiftTypeMap(fm, kind)
			n.Fields[i] = metadataField(fm, nodeFromJSONObject(flatType, scope, memo, wireFieldNode(wire, i)), flatType, wireField(wire, i))
		},
		items: func(key, scope string) {
			node := nodeFromJSON(m[key], scope, memo, wireItems(wire))
			n.Items = &node
			n.present |= presItems
		},
		values: func(key, scope string) {
			node := nodeFromJSON(m[key], scope, memo, wireValues(wire))
			n.Values = &node
			n.present |= presValues
		},
	})

	// Collect custom properties: anything not reserved, with precision/scale
	// reserved only on a recognized decimal carrier. The structural reads
	// above recorded their verdicts in n.present, so the routing decodes
	// nothing a second time.
	for k, v := range m {
		if schemaReservedKeyForObject(k, v, n.Type, n.LogicalType, n.present) {
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
// lifted key set. We keep the keys the lift routed into the type out of Props,
// and the routed doc belongs to the lifted type rather than the field, as
// the wire lift routes it. wf is the compiled field, whose default the
// parse validated and coerced; a field inside a stray body has none and
// carries its default as written.
func metadataField(fm map[string]any, typ SchemaNode, flatType map[string]any, wf *fieldNode) SchemaField {
	sf := SchemaField{Type: typ}
	getString(fm, "name", &sf.Name)
	if d, ok := fm["default"]; ok {
		sf.Default = d
		if wf != nil {
			sf.Default = metadataDefault(wf.defaultVal, wf.node)
		}
		sf.HasDefault = true
	}
	if flatType == nil {
		sf.docSet = getString(fm, "doc", &sf.Doc)
	}
	// Field aliases read through the parser's own decode (stringSliceFrom):
	// bound fields are parse-validated and stray-fields elements are
	// shape-checked before this runs, so the gate never declines here. It
	// exists so this path cannot coerce a malformed body.
	if ss, ok, err := stringSliceFrom(fm, "aliases"); err == nil && ok {
		sf.Aliases = ss
	}
	getString(fm, "order", &sf.Order)
	// We consume the exact-lowercase field reserved keys into the SchemaField
	// attributes above and preserve every other key verbatim, a case-variant
	// spelling of a reserved key included, which is an ordinary custom
	// property. Same rule as the type-object Props routing
	// (schemaReservedKeyForObject).
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

// metadataDefault converts a field's wire default, which the parse validated
// and coerced, into the Go form [SchemaField.Default] promises: int32 for
// int, int64 for long, float32 for float, float64 for double, with
// containers rebuilt leaf by leaf. A union narrows along the branch the wire
// selected, so the metadata cannot report a branch the encoder does not
// fill. Every container and byte slice is a fresh copy: the wire value is
// shared by every Root call and must not be reachable through the tree we
// hand back.
func metadataDefault(val any, node *schemaNode) any {
	if node == nil {
		return deepCopyTree(val)
	}
	switch node.kind {
	case "int":
		if n, err := defaultAsInt32(val); err == nil {
			return n
		}
	case "long":
		if n, err := defaultAsInt64(val); err == nil {
			return n
		}
	case "float":
		if f, err := defaultAsFloat(val); err == nil {
			return float32(f)
		}
	case "double":
		if f, err := defaultAsFloat(val); err == nil {
			return f
		}
	case "bytes", "fixed":
		if b, ok := val.([]byte); ok {
			return bytes.Clone(b)
		}
	case "union":
		if branch := firstUnionBranchAcceptingDefault(val, node); branch != nil {
			return metadataDefault(val, branch)
		}
	case "record":
		if m, ok := val.(map[string]any); ok {
			out := make(map[string]any, len(m))
			for k, v := range m {
				out[k] = deepCopyTree(v)
			}
			for _, f := range node.fields {
				if v, ok := m[f.name]; ok {
					out[f.name] = metadataDefault(v, f.node)
				}
			}
			return out
		}
	case "array":
		if a, ok := val.([]any); ok {
			out := make([]any, len(a))
			for i, v := range a {
				out[i] = metadataDefault(v, node.items)
			}
			return out
		}
	case "map":
		if m, ok := val.(map[string]any); ok {
			out := make(map[string]any, len(m))
			for k, v := range m {
				out[k] = metadataDefault(v, node.values)
			}
			return out
		}
	}
	return val
}

// decimalConsumesPrecisionScale reports whether a type object with the given
// type and logicalType, matched exactly as written, is a recognized decimal
// carrier, the one placement where "precision"/"scale" are consumed and
// validated. Everywhere else they are inert metadata in Props.
func decimalConsumesPrecisionScale(typ, logical string) bool {
	return logical == "decimal" && (typ == "bytes" || typ == "fixed")
}

// schemaKeyBinds reports whether a type object of the given kind/logical
// binds reserved key k with raw value v: strayKeyBinds for the keys the kind
// alone decides, plus the two that also depend on the value or the logical.
// schemaReservedKeyForObject asks this once rather than enumerating consumed
// keys; a hand-written list once dropped type-level "default" and "order".
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

// schemaReservedKeyForObject reports whether key k with value v on a type
// object of the given kind/logical is consumed or structurally carried, and
// so kept out of Props. Reserved names match only their exact lowercase
// spelling. A reserved key stays out of Props in two ways: the kind binds
// it, or the kind does not bind it but carries it as written on a structural
// field, which needs both a field and a body of that key's shape. A key that
// is neither goes only to Props, which is where type-level
// "default" and "order" go; Java and fastavro keep both as properties too.
// present carries the shape verdicts the caller's decoding arms recorded,
// so the routing never decodes a body itself; a caller with no arms asks
// strayPresence for the one key in hand.
func schemaReservedKeyForObject(k string, v any, typ, logical string, present presenceSet) bool {
	if !schemaReservedKeys[k] {
		return false
	}
	if schemaKeyBinds(k, v, typ, logical) {
		return true
	}
	bit, stray := strayKeyBit(k)
	return stray && present.has(bit)
}
