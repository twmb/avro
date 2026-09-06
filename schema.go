package avro

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"maps"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Schema is a compiled Avro schema. Create one with [Parse] or [MustParse],
// then use [Schema.Encode] / [Schema.Decode] to convert between Go values and
// Avro binary. A Schema is safe for concurrent use.
type Schema struct {
	ser   serfn
	deser deserfn

	c    aschema     // canonical form, used for fingerprinting and schema comparison
	node *schemaNode // full metadata tree (aliases, defaults, etc.) for schema introspection and evolution
	full string      // original schema JSON, returned by String()

	// soe is the single-object encoding header: the 2-byte magic and the
	// little-endian CRC-64-AVRO fingerprint of the canonical form. It is
	// computed on first use by soeHeader, never at parse, since hashing was
	// up to a third of Parse. Read it only through soeHeader; a bare read
	// races with a concurrent first use. The header is a function of c, so a
	// site adopting another schema's canonical form adopts its header too.
	soeHashed atomic.Bool
	soeOnce   sync.Once
	soe       [10]byte

	// resolveWriter is the writer schema, set by Resolve when the writer and
	// reader differ. DecodeJSON uses it to apply writer-to-reader resolution
	// to writer-shaped JSON, and DecodeSingleObject uses it to accept wire
	// bearing the writer's fingerprint. nil means not resolved.
	resolveWriter *Schema

	// resolveWriterRaw is a custom-free view of the writer, used only by
	// decodeJSONResolved to round-trip writer JSON into writer binary. That
	// intermediate must hold raw Avro-native values: a writer CustomType's
	// Decode would produce Go-domain values the re-encode cannot invert.
	// Equal to resolveWriter when the writer has no custom types.
	resolveWriterRaw *Schema

	// Per-schema custom type overlay. Keyed by *schemaNode so we do not
	// mutate the shared node: different schemas parsed with different
	// custom types get different overlays.
	custom map[*schemaNode]*customWiring

	// customBaked reports custom-conversion effects reachable through this
	// schema's node tree even when the custom overlay above is empty: a
	// reference to a SchemaCache-inherited named type whose defining parse
	// wired custom types carries them inside the inherited ser/deser.
	// Resolve builds its custom-free writer view whenever either is set.
	customBaked bool

	// slabFree marks a schema whose compiled deser never touches the per-call
	// slab: a scalar leaf kind with no custom-decoder wiring. Decode then
	// passes a nil slab, so scalar decodes stay allocation-free even when GC
	// has drained the pool. false is the safe default.
	slabFree bool
}

// customWiring bundles the per-node custom-type artifacts. We allocate one
// per node matching at least one registered CustomType, and populate each
// slot independently based on which callbacks you provided.
type customWiring struct {
	// encode wraps your CustomType.Encode chain and runs before the
	// built-in serializer. nil if no encoders matched, or if every
	// matching CustomType had Encode == nil.
	encode func(reflect.Value) (reflect.Value, error)
	// decoders is the CustomType.Decode callback chain. We run it after
	// the built-in deserializer produces the raw Avro-native value. nil
	// if no decoders matched.
	decoders []func(any, *SchemaNode) (any, error)
	// sn is the public *SchemaNode we pass to the encode and decoder
	// callbacks. Built once at parse time and reused across calls.
	// Always populated when the wiring is non-nil.
	sn *SchemaNode
	// suppressLogical mirrors the binary decoder-suppression decision
	// (hasMatchingCustomType) so the JSON decode wrapper feeds the custom
	// decoder the same raw-vs-enriched value the binary path does. False
	// for wildcard CustomTypes (empty LogicalType and AvroType), which the
	// binary gate excludes: they must receive the enriched logical value.
	// Carried here so resolved nodes (resolve.go) reuse the parse-time
	// decision without re-running the gate.
	suppressLogical bool
	// encodeSuppresses mirrors the binary encoder-suppression decision
	// (hasMatchingCustomTypeWithEncode): the JSON logical encode arms skip
	// the built-in coercion iff the binary build replaced the logical
	// serializer with the base one, which is iff a non-wildcard matching
	// CustomType has an Encode. The runtime proxy custom[node].encode != nil
	// would wrongly skip the arm for a wildcard with Encode.
	encodeSuppresses bool
}

// schemaNode preserves full schema metadata that canonical form strips:
// aliases, defaults, enum defaults, and links to compiled ser/deser.
type schemaNode struct {
	kind        string        // "null","boolean","int","long","float","double","bytes","string","record","enum","array","map","fixed","union"
	name        string        // fully-qualified name (named types only)
	aliases     []string      // named type aliases (fully qualified)
	bareAliases []string      // aliases declared without a dot, as written (short-name match tier; see bareAliasShorts)
	logical     string        // logical type
	fields      []fieldNode   // record fields
	symbols     []string      // enum symbols
	enumDef     string        // enum default symbol
	hasEnumDef  bool          // whether enum default is specified
	items       *schemaNode   // array item type
	values      *schemaNode   // map value type
	size        int           // fixed size
	precision   int           // decimal precision
	scale       int           // decimal scale
	branches    []*schemaNode // union branches
	ser         serfn
	deser       deserfn
	decodeJSON  jsonDecodeFn // non-nil only when custom decoders are wired
	serRecord   *serRecord
	deserRecord *deserRecord

	props    map[string]any // extra schema properties (for CustomType callbacks)
	fieldIdx map[string]int // record field name to index; built at parse time

	// tags is a union's name-to-branch lookup and symbolIdx is an enum's
	// symbol-to-ordinal lookup. Both live here, beside fieldIdx, rather than on
	// the serializer, because a per-value question has to be answerable from
	// whatever the asker holds, and the JSON codec holds a *schemaNode, never
	// a *serUnion. A table only one wire can reach is a table the other wire
	// re-derives per value by scanning the siblings, which is linear in a
	// count the schema's author chooses.
	tags      *unionTags     // union: see unionTags
	symbolIdx map[string]int // enum: symbol to ordinal; nil below enumIndexMin

	// unknownLogical preserves the original logicalType when it failed
	// validateLogical. Only rejectCachedRefIfCustomTypeWouldMatch consults
	// it, so a later Parse registering a CustomType for this logical can
	// detect the silent drop and error.
	unknownLogical string
}

// jsonDecodeFn is the per-node JSON dispatch shape we use when custom
// decoders are wired (mirrors deserfn for the binary path). nil node
// means we fall back to kind dispatch.
type jsonDecodeFn func(*jsonDecoder, reflect.Value, *schemaNode) error

type fieldNode struct {
	name    string
	nameVal reflect.Value // pre-computed for map lookups without allocation
	// aliases: schema-evolution alternate field names. Every consumer is
	// decode/resolve side: JSON decode (via node.fieldIdx, built from this
	// slice at parse time), CheckCompatibility's findWriterField (compat.go),
	// and readerFieldLookup, which both Resolve and CheckCompatibility match
	// writer fields through (resolve.go). We never consult them on encode:
	// aliases are a reader-side concept per the Avro 1.12 spec.
	aliases    []string
	node       *schemaNode
	defaultVal any
	hasDefault bool
}

type parseOptLax struct{ fn func(string) error }

func (parseOptLax) schemaOpt() {}

// WithLaxNames relaxes name validation in [Parse] and [SchemaCache.Parse],
// overriding our default of the Avro strict name regex [A-Za-z_][A-Za-z0-9_]*.
// A nil fn requires only non-empty names; otherwise we split dot-separated
// fullnames and call fn for each name component, and you return an error for
// the names you reject. [SchemaFor] ignores this option.
func WithLaxNames(fn func(string) error) SchemaOpt { return parseOptLax{fn} }

// internalReparseNames is the name validator for our own re-parses of schema
// text we produced: Resolve's custom-free writer view and SchemaCache's
// self-contained splice. The original parse already validated the names
// under your validator, so we accept everything; WithLaxNames(nil) rejected
// empty name components your fn may have accepted. Names pass through
// verbatim, so the canonical and wire bytes match a standalone parse.
var internalReparseNames = WithLaxNames(func(string) error { return nil })

// MustParse is like [Parse] but panics on error.
func MustParse(schema string, opts ...SchemaOpt) *Schema {
	s, err := Parse(schema, opts...)
	if err != nil {
		panic("avro: " + err.Error())
	}
	return s
}

// Parse parses an Avro JSON schema string and returns a compiled [*Schema].
// The input can be a primitive name (e.g. `"string"`), a JSON object
// (record, enum, array, map, fixed), or a JSON array (union). Named types
// may self-reference. We fully validate: unknown types, duplicate names,
// invalid defaults, and so on all return errors.
//
// To parse schemas that reference named types from other schemas, use
// [SchemaCache].
func Parse(schema string, opts ...SchemaOpt) (*Schema, error) {
	b := &builder{
		named:      make(map[string]*namedType),
		building:   make(map[*schemaNode]struct{}),
		definedSet: make(map[*namedType]bool),
		minBytes:   newMinBytesWalk(),
	}
	applySchemaOpts(b, opts)
	return parse(schema, b)
}

func applySchemaOpts(b *builder, opts []SchemaOpt) {
	for _, o := range opts {
		switch o := o.(type) {
		case parseOptLax:
			if o.fn != nil {
				b.checkName = o.fn
			} else {
				b.checkName = func(s string) error {
					if s == "" {
						return errors.New("name must be non-empty")
					}
					return nil
				}
			}
		case CustomType:
			if o.needsAvroType && o.AvroType == "" {
				// Validated lazily: store now, error in parse.
				// We still append so the error is reported.
			}
			b.customTypes = append(b.customTypes, o)
		}
	}
	// The custom-match walk memos exist exactly when CustomTypes are
	// registered (their consumers are all gated on len(customTypes) > 0).
	// We allocate here, before any build work and before any nest(), which
	// shares them by reference, so the whole parse sees one memo. We never
	// append to b.customTypes after this point; that is what makes the memo
	// correct (see customMatchInSubtree).
	if len(b.customTypes) > 0 {
		b.customMatch = make(map[*schemaNode]string)
		b.overlayDone = make(map[*schemaNode]bool)
	}
}

func parse(schema string, b *builder) (*Schema, error) {
	// We bound nesting depth with a single linear scan before building. The
	// build's maxDepth guard fires per schema node, but the JSON bracket
	// nesting can run deeper than the node depth (and json.Decode has its
	// own ~10000 limit). This O(input) pre-scan rejects pathologically deep
	// input up front. (Parse itself is O(n) via parseSchemaTree: a single
	// generic decode, no per-node subtree re-scan.)
	if err := checkSchemaNestingDepth(schema); err != nil {
		return nil, err
	}
	orig, err := parseSchemaTree(schema)
	if err != nil {
		return nil, err
	}
	if err := b.build("", orig); err != nil {
		return nil, boundErrorLen(err)
	}
	if err := b.finalize(); err != nil {
		return nil, boundErrorLen(err)
	}
	s := &Schema{
		ser:         b.ser,
		deser:       b.deser,
		c:           b.canon,
		node:        b.node,
		full:        schema,
		custom:      b.custom,
		customBaked: len(b.custom) > 0 || b.sawInheritedCustom,
	}
	s.slabFree = slabFreeKinds[b.node.kind] && !s.customBaked
	return s, nil
}

// maxSchemaJSONDepth bounds the raw JSON bracket nesting of a schema string,
// a coarse early reject rather than the semantic depth limit. One schema
// level carries at most three JSON brackets (a record object, its fields
// array, a field object), so a build-acceptable schema reaches a bracket
// depth of at most 3*maxDepth and the pre-scan never rejects a schema the
// builder would accept.
const maxSchemaJSONDepth = maxDepth * 4

// checkSchemaNestingDepth is one linear pass counting '{'/'[' nesting against
// maxSchemaJSONDepth, skipping brackets inside JSON strings (backslash escapes
// honored). O(len(schema)) and constant space, cheap enough to gate every
// parse.
func checkSchemaNestingDepth(schema string) error {
	depth := 0
	inStr := false
	esc := false
	for i := 0; i < len(schema); i++ {
		c := schema[i]
		if inStr {
			switch {
			case esc:
				esc = false
			case c == '\\':
				esc = true
			case c == '"':
				inStr = false
			}
			continue
		}
		switch c {
		case '"':
			inStr = true
		case '{', '[':
			depth++
			if depth > maxSchemaJSONDepth {
				return fmt.Errorf("avro: schema JSON nests deeper than the supported limit (%d brackets)", maxSchemaJSONDepth)
			}
		case '}', ']':
			depth--
		}
	}
	return nil
}

// canonicalFirstOccurrence rewrites the parse-time canon tree so each named
// type's full definition is emitted at its first occurrence in field-walk
// order and referenced by bare fullname afterward, Apache Avro's
// SchemaNormalization rule. The parse-time tree puts the body at the textual
// definition site; the two differ only on a forward reference, where the
// untransformed tree would produce a fingerprint that differs from Java's.
// We also normalize references to the resolved fullname.
func canonicalFirstOccurrence(s aschema) aschema {
	defs := map[string]*aobject{}
	shortCount := map[string]int{}
	shortToFull := map[string]string{}
	collectCanonDefs(s, defs, shortCount, shortToFull)
	if len(defs) == 0 {
		return s // no named types: nothing can be relocated
	}
	return rewriteCanonFirstOcc(s, "", defs, shortCount, shortToFull, map[string]struct{}{})
}

// collectCanonDefs records every named-type definition (the aobject whose body
// is present) keyed by fullname, plus an unqualified-name index for resolving
// bare forward references into a namespaced scope.
func collectCanonDefs(s aschema, defs map[string]*aobject, shortCount map[string]int, shortToFull map[string]string) {
	switch {
	case s.object != nil:
		o := s.object
		if isNamedKind(o.Type) && o.Name != "" {
			if _, dup := defs[o.Name]; !dup {
				defs[o.Name] = o
				short := unqualified(o.Name)
				shortCount[short]++
				shortToFull[short] = o.Name
			}
		}
		for i := range o.Fields {
			if o.Fields[i].Type != nil {
				collectCanonDefs(*o.Fields[i].Type, defs, shortCount, shortToFull)
			}
		}
		if o.Items != nil {
			collectCanonDefs(*o.Items, defs, shortCount, shortToFull)
		}
		if o.Values != nil {
			collectCanonDefs(*o.Values, defs, shortCount, shortToFull)
		}
	case len(s.union) != 0:
		for i := range s.union {
			collectCanonDefs(s.union[i], defs, shortCount, shortToFull)
		}
	}
}

// lookupCanonDef resolves a reference name to its definition aobject. It
// returns nil when ref is a real Avro primitive (never a named ref) or names
// a type defined outside this schema (a SchemaCache cross-reference, left as
// a bare name). A bare reference resolves lexically in the enclosing
// namespace ns (mirroring parse-time resolveNamedRef), so a short name shared
// across namespaces still resolves to its in-scope fullname rather than being
// emitted verbatim.
func lookupCanonDef(ref, ns string, defs map[string]*aobject, shortCount map[string]int, shortToFull map[string]string) *aobject {
	if _, isPrim := serPrimitive[ref]; isPrim {
		return nil
	}
	var keys [2]string
	for _, k := range scopedRefKeys(&keys, ref, ns) {
		if o, ok := defs[k]; ok {
			return o
		}
	}
	// Globally-unique short-name fallback for bare references with no
	// other resolution.
	if !strings.Contains(ref, ".") && shortCount[ref] == 1 {
		return defs[shortToFull[ref]]
	}
	return nil
}

func rewriteCanonFirstOcc(s aschema, ns string, defs map[string]*aobject, shortCount map[string]int, shortToFull map[string]string, seen map[string]struct{}) aschema {
	switch {
	case s.primitive != "":
		def := lookupCanonDef(s.primitive, ns, defs, shortCount, shortToFull)
		if def == nil {
			return s // real primitive, or a cross-schema reference: emit as-is
		}
		if _, ok := seen[def.Name]; ok {
			return aschema{primitive: def.Name} // already emitted: bare fullname
		}
		seen[def.Name] = struct{}{}
		return aschema{object: rewriteCanonObj(def, ns, defs, shortCount, shortToFull, seen)}
	case s.object != nil:
		o := s.object
		if isNamedKind(o.Type) && o.Name != "" {
			if _, ok := seen[o.Name]; ok {
				return aschema{primitive: o.Name} // already emitted at an earlier occurrence
			}
			seen[o.Name] = struct{}{}
		}
		return aschema{object: rewriteCanonObj(o, ns, defs, shortCount, shortToFull, seen)}
	case s.union != nil:
		out := make([]aschema, len(s.union))
		for i := range s.union {
			out[i] = rewriteCanonFirstOcc(s.union[i], ns, defs, shortCount, shortToFull, seen)
		}
		return aschema{union: out}
	}
	return s
}

// rewriteCanonObj shallow-copies o, preserving every scalar/canonical
// attribute, and rebuilds only its recursive schema children (Fields[].Type /
// Items / Values) through rewriteCanonFirstOcc. Per-object PCF emission is
// unchanged; only the full-vs-reference placement of nested named types moves
// to first occurrence.
func rewriteCanonObj(o *aobject, ns string, defs map[string]*aobject, shortCount map[string]int, shortToFull map[string]string, seen map[string]struct{}) *aobject {
	// A named type (record/enum/fixed) establishes the namespace its children
	// resolve bare references in; array/map carry no name, so their children
	// inherit the enclosing namespace.
	childNS := ns
	if isNamedKind(o.Type) && o.Name != "" {
		childNS = namespaceOf(o.Name)
	}
	no := *o
	if len(o.Fields) > 0 {
		no.Fields = make([]afield, len(o.Fields))
		for i, f := range o.Fields {
			nf := f
			if f.Type != nil {
				t := rewriteCanonFirstOcc(*f.Type, childNS, defs, shortCount, shortToFull, seen)
				nf.Type = &t
			}
			no.Fields[i] = nf
		}
	}
	if o.Items != nil {
		t := rewriteCanonFirstOcc(*o.Items, childNS, defs, shortCount, shortToFull, seen)
		no.Items = &t
	}
	if o.Values != nil {
		t := rewriteCanonFirstOcc(*o.Values, childNS, defs, shortCount, shortToFull, seen)
		no.Values = &t
	}
	return &no
}

// Canonical returns the Parsing Canonical Form of the schema, stripping
// doc, aliases, defaults, and other non-essential attributes. The result
// is deterministic and matches Java's reference output byte-for-byte,
// so [Schema.Fingerprint] values are interoperable across implementations.
func (s *Schema) Canonical() []byte {
	// Single-pass writer emitting raw UTF-8 strings per the PCF [STRINGS]
	// rule (see canonicalBytes). O(n) over the schema, vs the former
	// nested-MarshalJSON path that re-copied each subtree at every level
	// (O(n^2)). It also never produces the HTML / U+2028 / U+2029 escapes
	// the former path had to un-escape with bytes.ReplaceAll, which
	// corrupted any name containing a literal backslash.
	return canonicalBytes(canonicalFirstOccurrence(s.c))
}

// Fingerprint hashes the schema's canonical form with h and returns the
// digest. Use [NewRabin] for the spec's CRC-64-AVRO algorithm, or
// crypto/sha256 for its 256-bit recommendation.
//
// Note that byte order matters for CRC-64-AVRO. Go writes integer hashes high
// byte first, as crc32, crc64, adler32 and fnv all do, so [NewRabin] returns
// the fingerprint big-endian, while Java, fastavro and the single-object
// header write the same 64-bit value little-endian. Compare as a uint64, or
// reverse the bytes. A crypto/sha256 fingerprint has no byte order and
// already matches Java and fastavro byte for byte.
//
// No call returns the little-endian CRC-64-AVRO form directly.
// [Schema.AppendSingleObject] writes it into the message header,
// [SingleObjectFingerprint] reads it back, and [Schema.DecodeSingleObject]
// verifies it.
func (s *Schema) Fingerprint(h hash.Hash) []byte {
	// We reset on the way IN, so the digest is a function of the schema and
	// the algorithm alone. Neither a hash already used for an earlier
	// fingerprint nor one you wrote into can reach the answer. Resetting on
	// the way out would cover only the first of those, and it would clear the
	// state you read back. Taking Sum64 off the hash you passed is a
	// supported way to get the CRC-64-AVRO value as a number.
	h.Reset()
	h.Write(s.Canonical())
	return h.Sum(nil)
}

// String returns the original JSON passed to [Parse], preserving all
// attributes (doc, aliases, defaults, etc.) unlike [Schema.Canonical].
func (s *Schema) String() string {
	return s.full
}

type aschema struct {
	primitive string
	object    *aobject
	union     []aschema
}

// isNullBranch reports whether s is the "null" type in either spelling, bare
// "null" or the wrapped {"type":"null"}. Props and a logicalType on a wrapped
// null are inert and do not make it a non-null branch: Avro defines no null
// logical type, and deciding otherwise would make [{"type":"null","x":1},T]
// a two-non-null-branch union no other reader agrees with.
func (s *aschema) isNullBranch() bool {
	if s.primitive == "null" {
		return true
	}
	return s.object != nil && s.object.Type == "null"
}

// isNullableUnion reports whether s is a union whose first branch is "null".
// Per the Avro spec, such unions implicitly default to null.
func (s *aschema) isNullableUnion() bool {
	return len(s.union) >= 2 && s.union[0].isNullBranch()
}

// aschema, aobject, and afield are populated by parseSchemaTree
// (schema_parse.go) from a single generic decode. They are
// not json.Marshaler / json.Unmarshaler, so the stdlib decoder does not
// re-scan each nested node's subtree (which made Parse O(depth*size)).
// canonicalBytes (schema_canonical.go) writes the canonical form.

type afield struct {
	Name string   `json:"name"`
	Type *aschema `json:"type"`

	// In canonical form, the following are stripped.

	Aliases []string        `json:"aliases,omitempty"`
	Default json.RawMessage `json:"default,omitempty"`
	Order   string          `json:"order,omitempty"`

	// orderSet records that "order" was written, which Order alone cannot:
	// the empty string is its zero, so a validator reading Order != "" would
	// skip the one written value that is not a legal order. Java decides on
	// the node's presence, and so do we.
	orderSet bool

	// Field-level logical type annotations: the Java/JDBC idiom putting
	// logicalType (and decimal's precision/scale) beside `type` on the field
	// object rather than inside the type definition. Confluent's code
	// generator, kafka-connect-avro-converter and most Debezium CDC sources
	// emit this shape. We capture them here so the lift can move them into
	// the type; after that the parser sees only the canonical nested form.
	Logical   string `json:"logicalType,omitempty"`
	Scale     *int   `json:"scale,omitempty"`
	Precision *int   `json:"precision,omitempty"`

	// hasDefault is true if the field has a default value. We set it in
	// canonical afields (which strip Default) so validateDefault can check
	// whether nested record fields have defaults.
	hasDefault bool
}

// afieldComplexKeys are the keys that signal a complex type definition at
// the field level (the "flat" field format accepted by linkedin/goavro).
var afieldComplexKeys = map[string]string{
	"symbols": "enum",
	"items":   "array",
	"values":  "map",
	"fields":  "record",
	"size":    "fixed",
}

// liftTarget returns the aschema that receives a field-level logicalType
// annotation, or nil when the field carries none or nothing can receive it.
// Both the lift and its precision/scale verdict navigate through here so
// they cannot address different types.
func (f *afield) liftTarget() *aschema {
	if f.Logical == "" || f.Type == nil {
		return nil
	}
	switch {
	case f.Type.primitive != "":
		return f.Type
	case len(f.Type.union) > 0:
		for i := range f.Type.union {
			if f.Type.union[i].isNullBranch() {
				continue
			}
			return &f.Type.union[i] // first non-null branch only, like the lift
		}
		return nil
	case f.Type.object != nil:
		return f.Type
	}
	return nil
}

// liftEffectiveLogical reports the lift target's kind and the logical type
// in effect there once the lift has run: the target's own annotation when it
// has one, otherwise the field's. A field-level "decimal" that never reaches
// its target cannot make that target read precision/scale, so the pair is
// inert metadata there.
func (f *afield) liftEffectiveLogical() (kind, logical string, ok bool) {
	t := f.liftTarget()
	if t == nil {
		return "", "", false
	}
	switch {
	case t.primitive != "":
		// A bare primitive carries no annotation of its own, so the field's
		// always takes effect.
		return t.primitive, f.Logical, true
	case t.object != nil:
		if t.object.Logical != "" {
			return t.object.Type, t.object.Logical, true
		}
		return t.object.Type, f.Logical, true
	}
	return "", "", false
}

// liftFieldLogicalIntoType moves a field-level logicalType annotation, with
// precision/scale for decimal, into the field's type definition, so the
// rest of the parser sees the canonical nested form. The form
//
//	{"name":"ts","type":"long","logicalType":"timestamp-millis"}
//
// is a common error (AVRO-2015, AVRO-3014) that hand-written .avsc files and
// older Java tooling emit widely. Apache Avro warns but does not lift;
// fastavro, hamba and goavro keep it as an inert field property. The wire
// encoding is identical either way. An annotation already inside the type
// definition wins.
func (f *afield) liftFieldLogicalIntoType() {
	// The target comes from the shared navigation, so the lift and the
	// consume verdict can never address different types. It is the first
	// non-null union branch, the type object, or the bare primitive. We do
	// not fall through to a later non-null branch. That would silently mutate
	// a different type than the spec-equivalent nested form would have
	// addressed, and on the `[null, T+logical, T]` shape it would even
	// synthesize a duplicate union member.
	target := f.liftTarget()
	if target == nil {
		return
	}

	// Closer to the type wins the annotation. The field still completes
	// missing precision/scale, but only where the effective logical is
	// "decimal"; anywhere else they annotate nothing.
	_, effLogical, _ := f.liftEffectiveLogical()
	fillParams := effLogical == "decimal"

	switch {
	case target.primitive != "":
		// A bare primitive, at the field's type position or as a union
		// branch: {"type":["null","long"], "logicalType":"x"} becomes
		//   {"type":["null",{"type":"long","logicalType":"x"}]}
		obj := &aobject{Type: target.primitive, Logical: f.Logical}
		if fillParams {
			obj.Scale = clonePtrInt(f.Scale)
			obj.Precision = clonePtrInt(f.Precision)
		}
		*target = aschema{object: obj}

	case target.object != nil:
		// {"type":{"type":"long"}, "logicalType":"x"} becomes
		//   {"type":{"type":"long","logicalType":"x"}}
		if target.object.Logical == "" {
			target.object.Logical = f.Logical
		}
		if fillParams {
			if target.object.Scale == nil {
				target.object.Scale = clonePtrInt(f.Scale)
			}
			if target.object.Precision == nil {
				target.object.Precision = clonePtrInt(f.Precision)
			}
		}
	}
}

// fieldDecimalLiftConsumesPrecisionScale reports whether the field-level
// logical lift consumes "precision"/"scale" as decimal parameters: the field
// declares logicalType "decimal" and the lift's target is a bytes/fixed
// carrier as written. Everywhere else the pair is inert field metadata.
func (f *afield) fieldDecimalLiftConsumesPrecisionScale() bool {
	kind, logical, ok := f.liftEffectiveLogical()
	return ok && decimalConsumesPrecisionScale(kind, logical)
}

func clonePtrInt(p *int) *int {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// maxParseErrorLen bounds the assembled length of a schema-parse error. The
// walkers wrap per nesting level, so a deeply nested schema can otherwise
// produce a multi-KB message from a small input; truncForError caps
// individual values and this caps the chain.
const maxParseErrorLen = 1024

// boundErrorLen returns err unchanged if its message fits maxParseErrorLen,
// otherwise a flattened error keeping the head (outer context) and the
// tail (the innermost cause, e.g. "recursion limit exceeded", which the
// wrap chain puts last) with the repeated middle elided.
func boundErrorLen(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if len(msg) <= maxParseErrorLen {
		return err
	}
	half := maxParseErrorLen / 2
	return errors.New(msg[:half] + " ...[truncated]... " + msg[len(msg)-half:])
}

// boundJSONErrorEcho truncates user-controlled input echoed verbatim by
// json and strconv error types, so a hostile MiB-sized literal cannot
// produce a MiB-sized Parse error. It must run before the caller wraps with
// fmt.Errorf, which caches the formatted message.
func boundJSONErrorEcho(err error) error {
	if err == nil {
		return nil
	}
	var ute *json.UnmarshalTypeError
	if errors.As(err, &ute) && len(ute.Value) > 80 {
		ute.Value = truncForError(ute.Value)
	}
	var ne *strconv.NumError
	if errors.As(err, &ne) && len(ne.Num) > 80 {
		ne.Num = truncForError(ne.Num)
	}
	return err
}

type aobject struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type"`

	// A complex type can be one of many options. In canonical form, the
	// json fields are ordered "type", "name", and then one of the fields
	// below.

	Fields  []afield `json:"fields,omitempty"`  // record
	Symbols []string `json:"symbols,omitempty"` // enum
	Items   *aschema `json:"items,omitempty"`   // array
	Values  *aschema `json:"values,omitempty"`  // map
	Size    *laxInt  `json:"size,omitempty"`    // fixed

	// In canonical form, the following are stripped.

	Namespace *string         `json:"namespace,omitempty"`
	Aliases   []string        `json:"aliases,omitempty"`
	Default   json.RawMessage `json:"default,omitempty"`

	Logical   string `json:"logicalType,omitempty"`
	Scale     *int   `json:"scale,omitempty"`     // decimal logical type
	Precision *int   `json:"precision,omitempty"` // decimal logical type

	extra map[string]any // non-reserved properties, populated by aobjectFromMap

	// present records which structural and naming keys the parse arms
	// consumed from a body of the key's shape; see presenceSet.
	present presenceSet
}

// laxInt is an int that also accepts JSON strings containing integers,
// per the Avro spec's [INTEGERS] canonical form rule which acknowledges
// that "size" may appear as a quoted integer.
type laxInt int

// maxLaxIntDataLen caps the raw JSON bytes laxInt.UnmarshalJSON accepts. An
// int64 fits in 20 chars, the quoted form adds 2, and 2 more cover a leading
// sign. We reject hostile literals at entry because both strconv.Atoi and
// json.Unmarshal embed the failing input verbatim in their errors.
const maxLaxIntDataLen = 24

func (l *laxInt) UnmarshalJSON(data []byte) error {
	if len(data) > maxLaxIntDataLen {
		return fmt.Errorf("integer value exceeds %d byte length cap", maxLaxIntDataLen)
	}
	data = bytes.TrimSpace(data)
	if len(data) > 0 && data[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		n, err := strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("invalid integer string: %w", err)
		}
		*l = laxInt(n)
		return nil
	}
	var n int
	if err := json.Unmarshal(data, &n); err != nil {
		return err
	}
	*l = laxInt(n)
	return nil
}

// validName reports whether s matches [A-Za-z_][A-Za-z0-9_]*.
func validName(s string) bool {
	if s == "" {
		return false
	}
	for i, c := range s {
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c == '_' {
			continue
		}
		if i > 0 && c >= '0' && c <= '9' {
			continue
		}
		return false
	}
	return true
}

// Fixup types for forward references. Avro allows named types to be
// referenced before they are defined (e.g. a union branch or record field
// whose type hasn't been parsed yet). We record what needs patching and
// resolve everything in finalize() once all types are built.

// unionMissing patches a union's ser and deser branch function tables
// (plus its branch nodes and name tables) when any branch type was a
// forward reference. One fixup record per union keeps the paired
// structures updated together.
type unionMissing struct {
	ser        *serUnion
	deser      *deserUnion
	branches   []*schemaNode  // union node's branch slice; fwd-ref branch nodes are patched in finalize
	missing    map[int]string // branch index to type name
	parentName string         // enclosing scope, for the finalize namespace-qualified retry
}

// fieldMeta carries Avro-level type info for a record field, used by the
// unsafe fast path to select specialized ser/deser routines.
type fieldMeta struct {
	avroType      string
	logical       string // logical type (e.g. "timestamp-millis"), empty if none
	serRecord     *serRecord
	deserRecord   *deserRecord
	inner         *fieldMeta // for nullunion fields: the inner branch's metadata
	nullSecond    bool       // true for ["T","null"] unions (null is index 1)
	hasCustomType bool       // true if a CustomType was applied; disables unsafe fast path
	// minBytes is the minimum wire bytes required to encode one value
	// of this type. Set on the items' fieldMeta when an array is built;
	// used by the unsafe array deser to bound block counts. 0 means
	// items can be zero-byte (array<null>, array<EmptyRecord>).
	minBytes int
}

// metaFixup patches a fieldMeta's serRecord/deserRecord when the inner
// type of a null-union was a forward reference.
type metaFixup struct {
	meta       *fieldMeta
	name       string
	parentName string
}

// recordFieldFixup patches a record field's ser/deser function, avroType,
// meta, and schemaNode when the field's type was a forward reference.
type recordFieldFixup struct {
	// nd is the enclosing record's node, and we reach the encode and decode
	// tables through it: nd.serRecord and nd.deserRecord are the very sr and
	// dr the record case builds, assigned into the node in the same literal.
	// Carrying them again would be three names for one record, which a fixup
	// could then be built holding a node from one record and tables from
	// another.
	nd         *schemaNode
	idx        int
	name       string
	parentName string // enclosing record fullname, for the namespace-qualified retry
	defaultVal any    // parsed JSON default; valid only when hasDefault is true
	hasDefault bool   // whether the field had a "default" in the schema
}

// containerFixup patches an array or map container whose element type
// (items / values) was a forward reference. Used by both case "array"
// and case "map" in buildComplex so the two contexts share one fixup
// path; the only per-container variation is the min-bytes computation,
// which a closure carries.
type containerFixup struct {
	serItem     *serfn       // address of serArray.serItem / serMap.serItem
	deserItem   *deserfn     // address of deserArray.deserItem / deserMap.deserItem
	setMinBytes func(int)    // setter for minItemBytes (array) or 1+min (map)
	nodeChild   **schemaNode // address of arrayNode.items / mapNode.values
	name        string       // referenced named-type name
	parentName  string       // enclosing scope, for the namespace-qualified retry
	ctxLabel    string       // "array" or "map" for error messages
}

// defaultFixup defers a record field's default-value encoding to finalize,
// for a field whose outer type resolved at build time but whose type tree
// contains a forward-referenced descendant. encodeDefault dereferences each
// child's kind, so running it against a not-yet-wired child panics.
// recordFieldFixup handles the case where the whole field type is a forward
// reference.
type defaultFixup struct {
	nd         *schemaNode // enclosing record; its serRecord/deserRecord are the field tables
	idx        int
	node       *schemaNode // the field's already-built outer node (children wired by other fixups)
	defaultVal any         // parsed-but-not-yet-coerced JSON default
}

// captureFwdRef is the shared boilerplate used by every site that might
// encounter a forward reference inside a nested build (record field,
// array items, map values). An unknownPrimitiveError returns (true, name,
// nil) so the caller can queue a fixup; any other error is wrapped with
// ctxLabel.
func captureFwdRef(err error, ctxLabel string) (isFwdRef bool, fwdName string, wrapped error) {
	if err == nil {
		return false, "", nil
	}
	if pe := (*unknownPrimitiveError)(nil); errors.As(err, &pe) {
		return true, pe.p, nil
	}
	return false, "", fmt.Errorf("invalid %s: %v", ctxLabel, err)
}

// namedType holds the compiled artifacts for a named Avro type (record,
// enum, fixed) so they can be looked up by name during schema building.
type namedType struct {
	// node carries the type's compiled artifacts as well as its metadata. We
	// read ser/deser through it rather than copy them beside it, since a
	// custom-typed reference wraps b.ser while leaving node.ser bare, and a
	// copy could not be re-pointed.
	node *schemaNode
	// hadCustomType is true when this named type was defined by a parse that
	// wired at least one CustomType anywhere (coarse: every
	// definition of a custom-wired parse counts; stamped at finalize, see
	// registerNamed). The cache-reference boundary guard compares it against
	// the referencing parse's registrations to allow consistent reuse and
	// reject mismatches; the documented remediation path "re-parse Inner with
	// the CT first" depends on this signal.
	hadCustomType bool
}

type builder struct {
	ser   serfn
	deser deserfn

	named        map[string]*namedType
	building     map[*schemaNode]struct{} // record/error nodes whose field loop is in progress (shared across nest, like named)
	definedNamed []*namedType             // named types defined by this parse (vs inherited); stamped custom-affected at finalize
	// definedSet is the membership form of this-parse definitions. Unlike
	// definedNamed, which merges up only at unnest, it is shared by reference
	// across nest(), so a guard running in a nested builder can test whether
	// a reference points at a name this parse defined. A re-registered
	// cached name's fresh *namedType is here; cachedNames cannot make that
	// distinction.
	definedSet      map[*namedType]bool
	missing         []unionMissing
	mfixups         []metaFixup
	fieldFixups     []recordFieldFixup
	containerFixups []containerFixup
	defaultFixups   []defaultFixup

	meta        fieldMeta
	canon       aschema
	node        *schemaNode
	checkName   func(string) error // nil means strict (default)
	customTypes []CustomType
	custom      map[*schemaNode]*customWiring
	// sawInheritedCustom is set when a reference resolves to a
	// SchemaCache-inherited named type stamped hadCustomType by its
	// defining parse: that type's subtree carries baked custom effects
	// this parse's own overlay (b.custom) knows nothing about. Feeds
	// Schema.customBaked.
	sawInheritedCustom bool
	// customMatch memoizes custom-match subtree verdicts per node for this
	// parse: "" is proven match-free, non-"" names a matched-type location,
	// and key presence marks a computed verdict. Allocated only when
	// CustomTypes are registered and shared across nest(); every write is
	// nil-guarded for white-box test builders.
	customMatch map[*schemaNode]string
	// overlayDone marks inherited subtrees overlayInheritedCustom has
	// completed, so N references to one cached type overlay its nodes once
	// per parse. Sharing is sound because the walk is idempotent: customTypes
	// is fixed after applySchemaOpts and existing entries are kept.
	overlayDone map[*schemaNode]bool
	cachedNames map[string]bool // names inherited from SchemaCache, not from this parse
	// allowReRegister permits re-defining an inherited (cachedNames) type
	// instead of erroring "duplicate named type". Set by SchemaCache.Parse only
	// for parses that skip dedup and re-parse to get fresh CustomType wiring
	// (custom parses, and re-parses of a previously-custom-parsed schema).
	allowReRegister bool
	depth           int // current build recursion depth, bounded by maxDepth
	// minBytes is the one min-bytes walk shared across every container built
	// in this parse. A backward reference to a cyclic type pays a full walk
	// at build, and a schema can point any number of containers at it, so a
	// fresh walk per container would multiply the bound by the container
	// count. It stays separate from finalize's walk, whose memo must not see
	// provisional results computed before forward references are wired.
	minBytes *minBytesWalk
	// skipWalk is the parse's walk for [SkipUnknown]'s per-field skippers,
	// which compile at decode time. Separate from minBytes for the same reason
	// minBytes is separate from finalize's: a walk drained by one phase must
	// not charge another. Shared across the parse's records, since the schema
	// picks how many there are.
	skipWalk *minBytesWalk
}

func (b *builder) validNameErr(s string) error {
	if b.checkName != nil {
		return b.checkName(s)
	}
	if !validName(s) {
		return fmt.Errorf("invalid name %q", truncForError(s))
	}
	return nil
}

func (b *builder) validFullnameErr(s string) error {
	if s == "" {
		if b.checkName != nil {
			return b.checkName(s)
		}
		return fmt.Errorf("invalid name %q", truncForError(s))
	}
	for part := range strings.SplitSeq(s, ".") {
		if err := b.validNameErr(part); err != nil {
			return err
		}
	}
	return nil
}

func (b *builder) nest() *builder {
	return &builder{
		named:           b.named,
		building:        b.building,
		checkName:       b.checkName,
		customTypes:     b.customTypes,
		custom:          b.custom,
		customMatch:     b.customMatch,
		overlayDone:     b.overlayDone,
		cachedNames:     b.cachedNames,
		definedSet:      b.definedSet,
		allowReRegister: b.allowReRegister,
		depth:           b.depth,
		minBytes:        b.minBytes,
		skipWalk:        b.skipWalk,
	}
}

func (b *builder) unnest(nest *builder) {
	b.missing = append(b.missing, nest.missing...)
	b.definedNamed = append(b.definedNamed, nest.definedNamed...)
	b.mfixups = append(b.mfixups, nest.mfixups...)
	b.fieldFixups = append(b.fieldFixups, nest.fieldFixups...)
	b.containerFixups = append(b.containerFixups, nest.containerFixups...)
	b.defaultFixups = append(b.defaultFixups, nest.defaultFixups...)
	if len(nest.custom) > 0 {
		if b.custom == nil {
			b.custom = make(map[*schemaNode]*customWiring, len(nest.custom))
		}
		maps.Copy(b.custom, nest.custom)
	}
	b.sawInheritedCustom = b.sawInheritedCustom || nest.sawInheritedCustom
}

func (b *builder) putCustomWiring(node *schemaNode, w *customWiring) {
	if b.custom == nil {
		b.custom = make(map[*schemaNode]*customWiring)
	}
	b.custom[node] = w
}

// makeCustomSer wraps base with the custom-Encode function ce: apply ce to the
// value, then encode the converted value via base. This is the single
// definition of the binary custom-Encode wrap, shared by applyCustomTypes
// (in-order references) and customWrappedSer (forward-ref finalize fixups) so
// the two paths cannot drift. A forward reference to a custom-encoded named
// type must emit the same wire as an in-order one.
func makeCustomSer(ce func(reflect.Value) (reflect.Value, error), base serfn) serfn {
	return func(dst []byte, v reflect.Value, depth int) ([]byte, error) {
		v, err := ce(v)
		if err != nil {
			return nil, err
		}
		// Pass depth unchanged: the custom wrapper annotates an existing
		// schema node, it is not a new nesting level. base does the node's
		// own depth accounting, and the decode wrapper + JSON path both
		// charge 0 here. Re-entering at depth+1 would make a custom on a
		// recursive node trip errTooDeep a level shallower per recursion
		// than decode, breaking round-trips.
		return base(dst, v, depth)
	}
}

// customWrappedSer returns base wrapped with node's custom-Encode chain when
// one is registered (the same wrap applyCustomTypes installs for an in-order
// reference), else base unchanged. The forward-ref fixups in finalize call this
// so a forward reference to a custom-encoded named type applies the CustomType
// on the binary path. Using the unwrapped namedType.ser instead silently
// diverges binary from JSON: the forward-referenced field encodes raw on
// binary but converted on JSON.
func (b *builder) customWrappedSer(node *schemaNode, base serfn) serfn {
	if w := b.custom[node]; w != nil && w.encode != nil {
		return makeCustomSer(w.encode, base)
	}
	return base
}

// customWrappedDeser is the decode dual of customWrappedSer: base wrapped with
// node's custom-Decode chain when one is registered, else base unchanged.
// The forward-ref fixups in finalize call it so a forward reference to a
// custom-decoded named type applies the CustomType on the binary path as the
// JSON path already does. Logical suppression with no Decode callback needs
// no wrap, since the raw deser is baked onto the shared leaf node.
func (b *builder) customWrappedDeser(node *schemaNode, base deserfn) deserfn {
	if w := b.custom[node]; w != nil && len(w.decoders) > 0 {
		return wrapDeserWithCustomDecoders(base, w.decoders, w.sn)
	}
	return base
}

// primFastInfo holds per-primitive bindings for both the array and map
// fast paths. Indexed by the canonical primitive name; missing entries
// fall back to the generic (function-pointer) per-element path.
type primFastInfo struct {
	elemKind            reflect.Kind
	serArrayFn          func(*serArray) serfn
	serMapFn            func(*serMap) serfn
	deserArrayLoop      func(src []byte, sliceVal reflect.Value, start, count int, sl *slab) ([]byte, error)
	deserArrayIfaceLoop func(src []byte, slice []any, start, count int, sl *slab) ([]byte, error)
	deserMapBlock       func(src []byte, mapVal, keyVal, elemVal reflect.Value, count int, sl *slab) ([]byte, error)
	deserMapIfaceVal    deserIfaceFn
	deserArrayNative    func(sliceVal reflect.Value, src []byte, start, count int, sl *slab) (bool, []byte, error)
	deserMapNative      func(mapVal reflect.Value, src []byte, count int, sl *slab) (bool, []byte, error)
}

var primFast = map[string]primFastInfo{
	"string": {
		reflect.String,
		func(s *serArray) serfn { return s.serString }, func(s *serMap) serfn { return s.serString },
		deserArrayStringLoop, deserArrayStringIfaceLoop, deserMapStringBlock, deserStringIface,
		deserNativeArrayStringLoop, deserNativeMapStringBlock,
	},
	"boolean": {
		reflect.Bool,
		func(s *serArray) serfn { return s.serBoolean }, func(s *serMap) serfn { return s.serBoolean },
		deserArrayBooleanLoop, deserArrayBooleanIfaceLoop, deserMapBooleanBlock, deserBooleanIface,
		deserNativeArrayBooleanLoop, deserNativeMapBooleanBlock,
	},
	"int": {
		reflect.Int32,
		func(s *serArray) serfn { return s.serInt }, func(s *serMap) serfn { return s.serInt },
		deserArrayIntLoop, deserArrayIntIfaceLoop, deserMapIntBlock, deserIntIface,
		deserNativeArrayIntLoop, deserNativeMapIntBlock,
	},
	"long": {
		reflect.Int64,
		func(s *serArray) serfn { return s.serLong }, func(s *serMap) serfn { return s.serLong },
		deserArrayLongLoop, deserArrayLongIfaceLoop, deserMapLongBlock, deserLongIface,
		deserNativeArrayLongLoop, deserNativeMapLongBlock,
	},
	"float": {
		reflect.Float32,
		func(s *serArray) serfn { return s.serFloat }, func(s *serMap) serfn { return s.serFloat },
		deserArrayFloatLoop, deserArrayFloatIfaceLoop, deserMapFloatBlock, deserFloatIface,
		deserNativeArrayFloatLoop, deserNativeMapFloatBlock,
	},
	"double": {
		reflect.Float64,
		func(s *serArray) serfn { return s.serDouble }, func(s *serMap) serfn { return s.serDouble },
		deserArrayDoubleLoop, deserArrayDoubleIfaceLoop, deserMapDoubleBlock, deserDoubleIface,
		deserNativeArrayDoubleLoop, deserNativeMapDoubleBlock,
	},
}

// registerNamed stores nt under name and records it as a definition of this
// parse. We stamp the custom-affected flag (hadCustomType) for all of this
// parse's definitions at finalize, after applyCustomTypes has wired every
// node. We register early so self-references resolve mid-build, and a stamp
// taken here would predate the wiring it reports. For a type whose own node
// matches the CustomType (fixed, enum) that is permanent, since no later
// per-arm re-stamp sees it.
func (b *builder) registerNamed(name string, nt *namedType) {
	b.named[name] = nt
	b.definedNamed = append(b.definedNamed, nt)
	// Mirror the append into the reference-shared membership set so the
	// cached-ref guard can recognize a this-parse definition from inside a
	// nested builder. Nil only for white-box test builders that construct a
	// builder literal without the top-level init; those degrade to the
	// cachedNames-only behavior (the guard's other early return).
	if b.definedSet != nil {
		b.definedSet[nt] = true
	}
}

// leadingDotName reports whether name spells the explicit null-namespace
// escape, a single leading dot with no other dot, and returns the fullname
// it denotes: ".x" is the null-namespace fullname "x", and "." is the empty
// name. This is Java's Name constructor rule. The definition build, reference
// resolution, and the metadata fullname computation all share it.
func leadingDotName(name string) (string, bool) {
	if strings.LastIndexByte(name, '.') == 0 {
		return name[1:], true
	}
	return name, false
}

// scopedRefKeys writes the lookup keys for a name reference into dst in
// binding-precedence order and returns the filled prefix. A dotted reference
// is an exact fullname lookup. A bare reference tries the
// enclosing-namespace-qualified key first, then the bare key, the order Java
// and fastavro use; the reverse would bind a null-namespace type over the
// in-scope one whenever the two share a short name. Every resolver derives
// its key order here.
func scopedRefKeys(dst *[2]string, ref, ns string) []string {
	if strings.Contains(ref, ".") {
		if short, ok := leadingDotName(ref); ok && short != "" {
			// ".x" is the null-namespace escape: an exact lookup of the
			// fullname "x", never qualified into the enclosing namespace. A
			// bare "." stays as written and can only miss, since nothing
			// registers ".".
			dst[0] = short
			return dst[:1]
		}
		dst[0] = ref
		return dst[:1]
	}
	if ns != "" {
		dst[0], dst[1] = ns+"."+ref, ref
		return dst[:2]
	}
	dst[0] = ref
	return dst[:1]
}

// resolveNamedRef looks up a named-type reference in scopedRefKeys precedence
// order against parentName's namespace, returning ("", nil) when unresolved.
// Both the build-time backward references and finalize's forward-reference
// fixups use it, so a forward reference into a namespaced scope resolves the
// same as the byte-identical backward-ordered schema.
func (b *builder) resolveNamedRef(name, parentName string) (string, *namedType) {
	var keys [2]string
	for _, k := range scopedRefKeys(&keys, name, namespaceOf(parentName)) {
		if nt := b.named[k]; nt != nil {
			return k, nt
		}
	}
	return "", nil
}

// tryAssignNamedRef resolves a named-type reference, possibly with
// namespace qualification against parentName. Returns true on hit (with
// b.ser / b.deser / b.meta / b.node populated and, when setCanon is
// true, b.canon set to the resolved name). Shared by buildPrimitive's
// bare-string named-ref path and buildComplex's wrapped-form
// {"type":"Name"} path so the rejectCachedRefIfCustomTypeWouldMatch
// gate and the namespace-qualified retry agree.
func (b *builder) tryAssignNamedRef(name, parentName string, setCanon bool) (bool, error) {
	resolved, nt := b.resolveNamedRef(name, parentName)
	if nt == nil {
		return false, nil
	}
	if err := b.rejectCachedRefIfCustomTypeWouldMatch(resolved, nt); err != nil {
		return true, err
	}
	// Only an inherited type can carry the stamp at reference time: local
	// definitions are stamped at this parse's own finalize, after build.
	if nt.hadCustomType {
		b.sawInheritedCustom = true
	}
	// A cross-parse inherited subtree: complete the overlay so
	// resolve-time custom re-application sees the inherited nodes.
	// b.overlayDone makes repeated references walk the subtree once per
	// parse.
	if len(b.customTypes) > 0 && !b.definedSet[nt] && b.cachedNames[resolved] {
		if b.overlayDone == nil {
			b.overlayDone = make(map[*schemaNode]bool)
		}
		b.overlayInheritedCustom(nt.node, b.overlayDone)
	}
	if setCanon {
		b.canon = aschema{primitive: resolved}
	}
	b.ser = nt.node.ser
	b.deser = nt.node.deser
	if nt.node.serRecord != nil {
		b.meta = fieldMeta{avroType: "record", serRecord: nt.node.serRecord, deserRecord: nt.node.deserRecord}
	}
	b.node = nt.node
	return true, nil
}

func (b *builder) finalize() error {
	for _, m := range b.missing {
		for idx, name := range m.missing {
			_, nt := b.resolveNamedRef(name, m.parentName)
			if nt == nil {
				return fmt.Errorf("unknown type %q", truncForError(name))
			}
			m.ser.fns[idx] = b.customWrappedSer(nt.node, nt.node.ser)
			m.deser.fns[idx] = b.customWrappedDeser(nt.node, nt.node.deser)
			// The builder left branches[idx] nil for this forward-ref
			// branch (the named type wasn't built yet). The binary path
			// dispatches through the ser/deser fn tables (patched above),
			// but the JSON encode/decode, schema-resolution, and
			// union-default-validation paths walk node.branches directly
			// and would dereference the nil node.
			if m.branches != nil {
				m.branches[idx] = nt.node
			}
		}
		// With every branch node wired, re-derive the union's
		// name-dependent artifacts (duplicate-branch check, TaggedUnions
		// branch-name tables) from the resolved names: buildUnion ran
		// them with the unresolved as-written names for fwd-ref branches.
		if m.branches != nil && m.deser != nil {
			if err := finalizeUnionNames(m.ser.tags, m.deser, m.branches); err != nil {
				return err
			}
		}
	}
	for _, m := range b.mfixups {
		_, nt := b.resolveNamedRef(m.name, m.parentName)
		if nt == nil {
			return fmt.Errorf("unknown type %q", truncForError(m.name))
		}
		m.meta.serRecord = nt.node.serRecord
		m.meta.deserRecord = nt.node.deserRecord
	}
	// Phase 1: wire every forward-referenced record-field node. We defer
	// default encoding to phase 2 below so it runs only after every field
	// and container child node is wired: encodeDefault recurses into a
	// field's child nodes, and a not-yet-wired child would nil-panic.
	for _, m := range b.fieldFixups {
		_, nt := b.resolveNamedRef(m.name, m.parentName)
		if nt == nil {
			return fmt.Errorf("unknown type %q", truncForError(m.name))
		}
		m.nd.serRecord.fields[m.idx].fn = b.customWrappedSer(nt.node, nt.node.ser)
		m.nd.deserRecord.fields[m.idx].fn = b.customWrappedDeser(nt.node, nt.node.deser)
		if nt.node.serRecord != nil {
			// The encode and decode entries for one field were handed the
			// same *fieldMeta when the field was built, so naming the type
			// once updates both.
			m.nd.serRecord.fields[m.idx].meta.avroType = "record"
			m.nd.serRecord.fields[m.idx].meta.serRecord = nt.node.serRecord
			m.nd.deserRecord.fields[m.idx].meta.deserRecord = nt.node.deserRecord
		}
		m.nd.fields[m.idx].node = nt.node
	}
	// Phase 1b: wire every forward-referenced array/map container child.
	// One walk is shared across the loop: a schema can point any number of
	// containers at one forward-referenced subtree, and a fresh walk per
	// fixup would recompute it that many times. The graph is fully wired
	// by this phase, so the shared memo is exact.
	mbw := newMinBytesWalk()
	for _, m := range b.containerFixups {
		_, nt := b.resolveNamedRef(m.name, m.parentName)
		if nt == nil {
			return fmt.Errorf("%s references unknown named type %q", m.ctxLabel, truncForError(m.name))
		}
		*m.serItem = b.customWrappedSer(nt.node, nt.node.ser)
		*m.deserItem = b.customWrappedDeser(nt.node, nt.node.deser)
		m.setMinBytes(mbw.minBytesOf(nt.node))
		*m.nodeChild = nt.node
	}
	// Phase 2: deferred field defaults, in two passes. encodeDefault fills
	// an absent nested record field from its resolved f.defaultVal, so we
	// record every field's default value before encoding any default's
	// bytes; otherwise a default nesting into a sibling-defaulted record
	// reads a nil defaultVal. Both deferral kinds participate.
	type pendingDefault struct {
		node      *schemaNode
		name      string
		converted any
		srf       *serRecordField
	}
	var pending []pendingDefault
	// Phase 2a: resolve + record every deferred default's value.
	for _, m := range b.fieldFixups {
		if !m.hasDefault {
			continue
		}
		_, nt := b.resolveNamedRef(m.name, m.parentName)
		if nt == nil {
			continue
		}
		node := nt.node
		if node == nil {
			continue
		}
		name := m.nd.serRecord.fields[m.idx].name
		converted, err := resolveFieldDefaultValue(
			coerceDefault(m.defaultVal, node), node, name,
			&m.nd.deserRecord.fields[m.idx], &m.nd.fields[m.idx],
		)
		if err != nil {
			return fmt.Errorf("type %q: %w", truncForError(m.name), err)
		}
		pending = append(pending, pendingDefault{node, name, converted, &m.nd.serRecord.fields[m.idx]})
	}
	for _, m := range b.defaultFixups {
		name := m.nd.serRecord.fields[m.idx].name
		converted, err := resolveFieldDefaultValue(
			coerceDefault(m.defaultVal, m.node), m.node, name,
			&m.nd.deserRecord.fields[m.idx], &m.nd.fields[m.idx],
		)
		if err != nil {
			return err
		}
		pending = append(pending, pendingDefault{m.node, name, converted, &m.nd.serRecord.fields[m.idx]})
	}
	// Phase 2b: encode binary default bytes now that every default value
	// (inline-built and deferred) is recorded on its field node.
	for _, p := range pending {
		if err := encodeFieldDefaultBytes(p.converted, p.node, p.name, p.srf); err != nil {
			return err
		}
	}
	// We stamp each named type this parse defined as custom-affected iff a
	// non-wildcard registration matches somewhere in its own subtree,
	// which is when baked effects live inside the type. The predicate is
	// the same one the cache-boundary guard applies on the reference side,
	// so stamp and guard cannot disagree. Asking about the type's own
	// subtree crosses into inherited subtrees, so a wrapper around an
	// inherited custom-baked reference is stampable, and a sibling with
	// no match is not stamped merely because the parse wired something
	// elsewhere. Taken at finalize, after the tree is built, because
	// registration happens early to support self-references.
	if len(b.customTypes) > 0 {
		for _, nt := range b.definedNamed {
			if b.customMatchInSubtree(nt.node) != "" {
				nt.hadCustomType = true
			}
		}
	}
	return nil
}

func (s *aschema) unionTypeName() (string, string, error) {
	if s.primitive != "" {
		return s.primitive, "", nil
	}
	// Non-nil (even when empty) means the JSON was an array: a nested
	// union, which a union may not immediately contain (Java: "Nested
	// union" fires for any union-typed member, zero branches included).
	if s.union != nil {
		return "union", "", errors.New("unions cannot immediately contain other unions")
	}
	if isNamedKind(s.object.Type) {
		return s.object.Type, s.object.Name, nil
	}
	return s.object.Type, "", nil
}

type unknownPrimitiveError struct{ p string }

func (e *unknownPrimitiveError) Error() string {
	return fmt.Sprintf("unknown primitive %q", truncForError(e.p))
}

func (b *builder) build(parentName string, s *aschema) error {
	// We discriminate union-ness by non-nil, not length. `[]` parses to a
	// non-nil zero-branch union, which is legal: Java's UnionSchema
	// constructor, fastavro, and avro-rs all accept it. No value can ever
	// encode or decode against it, but the schema itself is well-formed.
	if s == nil || s.primitive == "" && s.object == nil && s.union == nil {
		return errors.New("schema is not a primitive, complex, nor union")
	}
	if b.depth >= maxDepth {
		return fmt.Errorf("schema nests deeper than the supported limit (%d)", maxDepth)
	}
	// Seed the shared per-parse min-bytes walk at the root's first build, before
	// any nest() copies the pointer, so every container built below shares it.
	// Parse and SchemaCache.Parse seed it in their constructors; this covers a
	// builder constructed directly (white-box tests) so the build-path container
	// sites never dereference a nil walk. See b.minBytes.
	if b.minBytes == nil {
		b.minBytes = newMinBytesWalk()
	}
	if b.skipWalk == nil {
		b.skipWalk = newMinBytesWalk()
	}
	b.depth++
	defer func() { b.depth-- }()

	var err error
	switch {
	case s.primitive != "":
		err = b.buildPrimitive(parentName, s)
	case s.union != nil:
		err = b.buildUnion(parentName, s)
	default:
		err = b.buildComplex(parentName, s)
	}
	if err != nil {
		return err
	}
	// Propagate extra schema properties to the node (for CustomType callbacks).
	if b.node != nil && s.object != nil && len(s.object.extra) > 0 {
		b.node.props = s.object.extra
	}
	// Apply custom types to newly built nodes (not unions: custom
	// types fire on individual branches, not the union container).
	if len(b.customTypes) > 0 && b.node != nil && b.node.kind != "union" {
		if err := b.applyCustomTypes(b.node); err != nil {
			return err
		}
	}
	return nil
}

// buildCustomSN builds the public SchemaNode we hand to CustomType callbacks:
// once per node at parse time, then cached.
func buildCustomSN(node *schemaNode) *SchemaNode {
	sn := &SchemaNode{
		Type:        node.kind,
		LogicalType: node.logical,
		Name:        node.name,
		Size:        node.size,
		Precision:   node.precision,
		Scale:       node.scale,
		Symbols:     node.symbols,
	}
	if node.props != nil {
		sn.Props = node.props
	}
	return sn
}

// hasMatchingCustomType reports whether any registered custom type would
// match a node with the given kind and logical type. We use it to skip the
// built-in logical-type decoder when a custom type replaces it. The encode
// side suppresses only when you provided an Encode callback, so it uses
// hasMatchingCustomTypeWithEncode instead.
func (b *builder) hasMatchingCustomType(kind, logical string) bool {
	return b.hasMatchingCustomTypeCond(kind, logical, false)
}

// hasMatchingCustomTypeWithEncode reports whether any matching CustomType
// has a non-nil Encode callback. We gate suppression of the built-in logical
// encoder on it. Per [CustomType.Encode], an Encode==nil CustomType leaves
// the built-in encoder in place, so registering only Decode keeps the
// convenient time.Time / *big.Rat / avro.Duration encoder. An Encode!=nil
// CustomType wraps the base (raw) encoder with your callback.
func (b *builder) hasMatchingCustomTypeWithEncode(kind, logical string) bool {
	return b.hasMatchingCustomTypeCond(kind, logical, true)
}

// hasMatchingCustomTypeCond is the shared body. When requireEncode is true,
// the predicate additionally requires ct.Encode != nil, for the
// encoder-suppression gate. When false, it matches any registered
// CustomType, for the decoder-suppression gate (where Decode==nil still
// bypasses the built-in per the doc).
func (b *builder) hasMatchingCustomTypeCond(kind, logical string, requireEncode bool) bool {
	for _, ct := range b.customTypes {
		// Wildcards (both empty) should not suppress built-in
		// handlers: they use ErrSkipCustomType at runtime.
		if ct.LogicalType == "" && ct.AvroType == "" {
			continue
		}
		if ct.LogicalType != "" && ct.LogicalType != logical {
			continue
		}
		if ct.AvroType != "" && ct.AvroType != kind {
			continue
		}
		if requireEncode && ct.Encode == nil {
			continue
		}
		return true
	}
	return false
}

func (b *builder) applyCustomTypes(node *schemaNode) error {
	// Validate NewCustomType-created types with unsupported A type.
	for _, ct := range b.customTypes {
		if ct.needsAvroType && ct.AvroType == "" {
			return fmt.Errorf("avro: custom type %q: unsupported Avro native type for NewCustomType (use CustomType struct for non-primitive backing types)", ct.LogicalType)
		}
	}

	wiring := b.buildCustomWiring(node)
	if wiring == nil {
		return nil
	}

	if wiring.encode != nil {
		// Wrap the binary serializer. We update b.ser (which becomes the
		// Schema's ser) but not node.ser, so named types in the cache
		// keep their unwrapped ser/deser. The wrap closure is built by
		// makeCustomSer, shared with the forward-ref finalize fixups
		// (customWrappedSer) so an in-order reference and a forward reference
		// to the same custom-encoded named type apply the same wrap.
		b.ser = makeCustomSer(wiring.encode, node.ser)
	}

	// See buildCustomWiring for what this narrowing buys and the
	// suppression contract behind it.
	jsonAppliesLogical := wiring.suppressLogical && jsonDecodeAppliesLogical(node)
	if len(wiring.decoders) > 0 {
		b.deser = wrapDeserWithCustomDecoders(node.deser, wiring.decoders, wiring.sn)
		// JSON-side: wrap the node's per-decode dispatch with a
		// closure that captures the decoder chain. The JSON runtime
		// (decodeValue) checks node.decodeJSON first and falls back
		// to decodeKind otherwise: no per-call map lookup, no
		// recursion guard, no shared mutable state.
		node.decodeJSON = wrapDecodeJSONWithCustomDecoders(wiring.decoders, wiring.sn, wiring.suppressLogical)
	} else if jsonAppliesLogical {
		// No Decode callback on a logical node the binary path suppresses: you
		// receive the raw Avro-native value, so we install the raw-decode
		// wrapper with an empty chain to produce the same raw value through
		// DecodeJSON. A wildcard keeps the logical transform on both paths.
		node.decodeJSON = wrapDecodeJSONWithCustomDecoders(nil, wiring.sn, wiring.suppressLogical)
	}

	b.putCustomWiring(node, wiring)
	b.meta.hasCustomType = true
	return nil
}

// buildCustomWiring collects the per-node custom-type wiring from this parse's
// registrations, or nil when none matches. It is pure with respect to the
// builder and the node, which lets applyCustomTypes and
// overlayInheritedCustom share it; inherited nodes get the overlay only,
// since their wraps already live inside the inherited composition.
func (b *builder) buildCustomWiring(node *schemaNode) *customWiring {
	// Collect all matching encoders and decoders for this node.
	type encoder struct {
		goType reflect.Type
		fn     func(any, *SchemaNode) (any, error)
	}
	var encoders []encoder
	var decoders []func(any, *SchemaNode) (any, error)

	for _, ct := range b.customTypes {
		if !ct.matches(node) {
			continue
		}
		if ct.Encode != nil {
			encoders = append(encoders, encoder{goType: ct.GoType, fn: ct.Encode})
		}
		if ct.Decode != nil {
			decoders = append(decoders, ct.Decode)
		}
	}

	// suppressLogical mirrors the binary decoder-suppression gate: the binary
	// build replaces the logical deserializer with the raw one whenever any
	// non-wildcard CustomType matches, callbacks or not.
	suppressLogical := b.hasMatchingCustomType(node.kind, node.logical)
	// jsonAppliesLogical narrows that to nodes whose JSON decoder actually
	// transforms the raw value (a logical decodeKind would apply): only those
	// need a JSON-side suppress-wrapper to mirror the binary raw decode.
	jsonAppliesLogical := suppressLogical && jsonDecodeAppliesLogical(node)

	// Nothing to wire when there are no callbacks and the JSON path has no
	// suppression to mirror. A no-callback matcher on a logical node falls
	// through this guard (jsonAppliesLogical is true) so its JSON decode is
	// suppressed to raw, matching the binary path. Without this, DecodeJSON
	// returns the enriched logical type (time.Time / *big.Rat) while Decode
	// returns the raw Avro-native value. A wildcard, or a no-callback matcher
	// on a non-logical node, has nothing to mirror and returns here.
	if len(encoders) == 0 && len(decoders) == 0 && !jsonAppliesLogical {
		return nil
	}

	// Build the cached SchemaNode for callbacks and the wiring entry.
	sn := buildCustomSN(node)
	wiring := &customWiring{sn: sn}

	if len(encoders) > 0 {
		customEncode := func(v reflect.Value) (reflect.Value, error) {
			// Dereference pointers and interface wrappers so GoType
			// matching compares against the concrete type. Check GoType
			// at each level so pointer-valued GoTypes (e.g. *url.URL)
			// match before the pointer is stripped. Capped at
			// maxIndirectDepth so a self-referential interface
			// (var p any; p = &p) can't spin forever here.
			for range maxIndirectDepth {
				if v.Kind() != reflect.Pointer && v.Kind() != reflect.Interface {
					break
				}
				if v.IsNil() {
					return v, nil
				}
				for _, enc := range encoders {
					if enc.goType != nil && v.Type() == enc.goType {
						result, err := enc.fn(v.Interface(), sn)
						if err != nil {
							if errors.Is(err, ErrSkipCustomType) {
								// Skip this encoder, try the next
								// chain entry. Mirrors the
								// value-GoType scan below and the
								// decoder side (custom_type.go) so
								// the ErrSkipCustomType contract
								// holds regardless of GoType shape.
								continue
							}
							return reflect.Value{}, err
						}
						if result == nil {
							return reflect.Value{}, fmt.Errorf("avro: custom type encoder returned nil for %v", v.Type())
						}
						return reflect.ValueOf(result), nil
					}
				}
				v = v.Elem()
			}
			for _, enc := range encoders {
				if enc.goType != nil && v.Type() != enc.goType {
					continue
				}
				result, err := enc.fn(v.Interface(), sn)
				if err != nil {
					if errors.Is(err, ErrSkipCustomType) {
						continue
					}
					return reflect.Value{}, err
				}
				if result == nil {
					return reflect.Value{}, fmt.Errorf("avro: custom type encoder returned nil for %v", v.Type())
				}
				return reflect.ValueOf(result), nil
			}
			return v, nil // no encoder matched, pass through
		}

		// Store the customEncode in the wiring overlay (not on the
		// shared node) so it doesn't leak via the cache.
		wiring.encode = customEncode
	}

	wiring.suppressLogical = suppressLogical
	// encodeSuppresses mirrors the binary *encoder*-suppression gate
	// (hasMatchingCustomTypeWithEncode, which excludes wildcards), so the JSON
	// encode arms suppress the built-in logical coercion iff the binary build
	// did. See customWiring.encodeSuppresses.
	wiring.encodeSuppresses = b.hasMatchingCustomTypeWithEncode(node.kind, node.logical)

	if len(decoders) > 0 {
		wiring.decoders = decoders
	}
	return wiring
}

// overlayInheritedCustom completes this parse's custom overlay for a
// SchemaCache-inherited subtree. applyCustomTypes visits only newly built
// nodes, so a reference to an inherited type left its matching nodes without
// overlay entries: the wraps still fired on a direct decode, but Resolve,
// which re-applies customs from the overlay, dropped the reader's custom and
// returned raw values. We insert wiring only, no wraps, and keep existing
// entries. visited is the per-parse b.overlayDone set; sharing it is sound
// because the walk is idempotent within a parse.
func (b *builder) overlayInheritedCustom(node *schemaNode, visited map[*schemaNode]bool) {
	if node == nil || visited[node] {
		return
	}
	visited[node] = true
	if b.custom[node] == nil {
		if w := b.buildCustomWiring(node); w != nil {
			b.putCustomWiring(node, w)
		}
	}
	for _, f := range node.fields {
		b.overlayInheritedCustom(f.node, visited)
	}
	b.overlayInheritedCustom(node.items, visited)
	b.overlayInheritedCustom(node.values, visited)
	for _, br := range node.branches {
		b.overlayInheritedCustom(br, visited)
	}
}

func (b *builder) buildPrimitive(parentName string, s *aschema) error {
	b.canon = aschema{primitive: s.primitive}
	b.meta = fieldMeta{avroType: s.primitive}
	fn, exists := serPrimitive[s.primitive]
	if exists {
		b.ser = fn
		b.deser = deserPrimitive[s.primitive]
		b.node = &schemaNode{
			kind:  s.primitive,
			ser:   b.ser,
			deser: b.deser,
		}
		return nil
	}
	// A named type reference (record, enum, fixed). We pass setCanon for
	// both branches: only the namespace-qualified retry needs to rewrite
	// the canon, and the bare path overwrites it with the identical name
	// already set above.
	if found, err := b.tryAssignNamedRef(s.primitive, parentName, true); err != nil || found {
		return err
	}
	return &unknownPrimitiveError{s.primitive}
}

// rejectCachedRefIfCustomTypeWouldMatch errors when this Parse registered a
// CustomType that would match a node inside a cached named type's subtree.
// The cached node's handlers were baked at the original Parse, so reusing
// them would silently drop your CustomType on the cached fields.
func (b *builder) rejectCachedRefIfCustomTypeWouldMatch(refName string, nt *namedType) error {
	if nt == nil || nt.node == nil {
		return nil
	}
	// Only types inherited across Parses are in question. definedSet is the
	// authoritative "defined this parse" test: it holds every definition this
	// parse registered, a re-registered cached name included, and it is
	// shared across nest(), so it is populated when the guard runs while
	// building a self-referential field. cachedNames cannot make either
	// distinction.
	if b.definedSet[nt] {
		return nil
	}
	if !b.cachedNames[refName] {
		return nil
	}
	// A cached named type and the Parse referencing it must agree on whether
	// a matching CustomType is registered, since the CustomType's effect is
	// baked onto the shared cached node with no per-Schema overlay. We reject
	// both directions with the same remediation.
	currentMatches := ""
	if len(b.customTypes) > 0 {
		currentMatches = b.customMatchInSubtree(nt.node)
	}
	switch {
	case currentMatches != "" && !nt.hadCustomType:
		// Forward: this Parse registers a CustomType matching the cached
		// subtree, but the cached node was built without it: reusing it would
		// silently drop this Parse's custom (you get raw/unwrapped values on
		// the cached fields). Re-parse the inner type with the CustomType.
		return fmt.Errorf("avro: cached type %q contains %q which would match a CustomType on this Parse; re-parse %q with the CustomType first", truncForError(refName), truncForError(currentMatches), truncForError(refName))
	case nt.hadCustomType && currentMatches == "":
		// Reverse: the cached node was built with a CustomType (its raw
		// ser/deser and JSON decodeJSON bake that conversion onto the shared
		// node), but this Parse registers no matching CustomType. Reusing it
		// would silently apply the original conversion to a Schema that never
		// opted in, giving suppressed/raw values on both wire formats.
		// Register the same CustomType in this Parse, or parse the inner type
		// without one.
		return fmt.Errorf("avro: cached type %q was parsed with a CustomType affecting its subtree, but this Parse registers no matching CustomType; reusing it would apply that conversion here: register the CustomType in this Parse, or parse %q without one", truncForError(refName), truncForError(refName))
	}
	return nil
}

// customMatchInSubtree is the memoized entry point over
// findCustomTypeMatchInSubtree: one walk per node per parse, shared by the
// finalize stamping loop, the cache boundary guard, and the overlay
// completion. Without the memo, finalize is quadratic on a
// backward-reference chain. Two write rules keep verdicts exact on cyclic
// graphs: a clean completion writes "" for every visited node, since each
// one's reachable set is a subset of the root's; a "" that merely bubbled up
// mid-walk is not written, since a completed child can still reach a match
// through a back-edge to a node higher on the stack. A match writes its
// location for the nodes it unwinds through.
func (b *builder) customMatchInSubtree(node *schemaNode) string {
	if m, ok := b.customMatch[node]; ok {
		return m
	}
	visited := make(map[*schemaNode]bool)
	m := b.findCustomTypeMatchInSubtree(node, visited)
	if b.customMatch != nil && m == "" {
		for n := range visited {
			b.customMatch[n] = ""
		}
	}
	return m
}

// findCustomTypeMatchInSubtree is the per-node recursion step: consult the
// per-parse memo, else walk via findCustomTypeMatchInSubtreeWalk, and record
// a found match for this node on the unwind. Every node on the walk stack
// reaches the match, so the write is exact; see customMatchInSubtree for the
// full memo contract. White-box test builders may lack the memo map: reads
// of a nil map miss and writes are guarded, degrading to the unshared walk.
func (b *builder) findCustomTypeMatchInSubtree(node *schemaNode, visited map[*schemaNode]bool) string {
	if node == nil {
		return ""
	}
	if m, ok := b.customMatch[node]; ok {
		return m
	}
	m := b.findCustomTypeMatchInSubtreeWalk(node, visited)
	if m != "" && b.customMatch != nil {
		b.customMatch[node] = m
	}
	return m
}

// findCustomTypeMatchInSubtreeWalk walks node and its descendants,
// returning a short location string for the first node whose
// (kind, logical) would match any of b.customTypes. Returns "" if
// no descendant matches. The visited set handles recursive types,
// named-type recursion included; node.fields, node.items, node.values,
// node.branches cover every container shape (record, array, map, union).
func (b *builder) findCustomTypeMatchInSubtreeWalk(node *schemaNode, visited map[*schemaNode]bool) string {
	if node == nil || visited[node] {
		return ""
	}
	visited[node] = true
	// Effective logical: prefer the live logical, fall back to the
	// preserved-but-cleared unknownLogical. Built-in logicals are
	// always preserved; only unknown-at-original-Parse logicals end
	// up in unknownLogical. Either may match a CT registered now.
	effLogical := node.logical
	if effLogical == "" {
		effLogical = node.unknownLogical
	}
	for _, ct := range b.customTypes {
		// Wildcard CTs (both empty) opt into runtime ErrSkipCustomType
		// dispatch: they don't reliably suppress built-ins at parse
		// time, so they don't cause silent-drop on cached subtrees.
		// Skip them in this check; only explicitly-typed CTs would
		// silently fail to fire.
		if ct.LogicalType == "" && ct.AvroType == "" {
			continue
		}
		ltMatch := ct.LogicalType == "" || ct.LogicalType == effLogical
		atMatch := ct.AvroType == "" || ct.AvroType == node.kind
		if ltMatch && atMatch && (ct.LogicalType != "" || ct.AvroType != "") {
			if node.name != "" {
				return node.name
			}
			if effLogical != "" {
				return node.kind + "." + effLogical
			}
			return node.kind
		}
	}
	for _, f := range node.fields {
		if m := b.findCustomTypeMatchInSubtree(f.node, visited); m != "" {
			return m
		}
	}
	if m := b.findCustomTypeMatchInSubtree(node.items, visited); m != "" {
		return m
	}
	if m := b.findCustomTypeMatchInSubtree(node.values, visited); m != "" {
		return m
	}
	for _, br := range node.branches {
		if m := b.findCustomTypeMatchInSubtree(br, visited); m != "" {
			return m
		}
	}
	return ""
}

// Unions may not contain multiple schemas with the same type, except for
// record, fixed, and enum (of which we ensure unique names). Unions also
// cannot contain other immediate unions.
//
// If we see types we do not understand, it is possible they are referencing
// things that are not yet declared. We fixup at the very end.
func (b *builder) buildUnion(parentName string, s *aschema) error {
	var (
		// One table for the whole union, handed to the serializer and to the
		// node below so both wires ask the same allocation. finalizeUnionNames
		// refills it in place, so neither holder can go stale.
		tags        = new(unionTags)
		ser         = &serUnion{tags: tags}
		deser       = new(deserUnion)
		missing     = make(map[int]string)
		sawTypes    = make(map[string]bool)
		branchMetas = make([]fieldMeta, len(s.union))
		branchNodes = make([]*schemaNode, len(s.union))
		// Per-branch tag spellings, collected across the loop and turned
		// into the three tag tables in one place afterward: the collision
		// rule needs every branch's exact name, so it cannot be applied
		// branch-by-branch inside the loop.
		unionStd = make([]string, 0, len(s.union))
		unionLog = make([]string, 0, len(s.union))
	)
	// A zero-branch union appends nothing below; the canon tree still
	// needs a non-nil union so the canonical writer emits `[]` (union-ness
	// is discriminated by non-nil throughout the canon walk).
	if len(s.union) == 0 {
		b.canon.union = []aschema{}
	}

	for i, us := range s.union {
		u := b.nest()
		// The name captureFwdRef hands back comes from either the
		// bare-string form (where us.primitive is set) or the wrapped form
		// {"type":"FwdName"} (where us.object.Type is set).
		isFwdRef, fwdName, err := captureFwdRef(u.build(parentName, &us), "union")
		if err != nil {
			return err
		}
		if isFwdRef {
			missing[i] = fwdName
		}
		b.unnest(u)
		branchMetas[i] = u.meta
		branchNodes[i] = u.node

		typ, name, err := us.unionTypeName()
		if err != nil {
			return err
		}
		// Per the spec, named types must be defined once across a union. We
		// key by resolved fullname when available, so an inline definition
		// and a reference to the same type collide, and primitives collide on
		// kind. A forward-referenced branch is not keyed here: its as-written
		// name is not yet bound, and keying it would both miss real
		// duplicates and false-reject valid unions. finalizeUnionNames
		// re-checks over the resolved names.
		if missing[i] == "" {
			key := typ
			if u.node != nil && u.node.name != "" {
				key = u.node.name
			}
			if sawTypes[key] {
				return fmt.Errorf("duplicate union type %q", truncForError(key))
			}
			sawTypes[key] = true
		}

		b.canon.union = append(b.canon.union, u.canon)
		ser.fns = append(ser.fns, u.ser)
		deser.fns = append(deser.fns, u.deser)

		// Branch names for TaggedUnions wrapping.
		// u.node may be nil for forward-referenced types; use the
		// type name from the schema entry as fallback.
		var bn, ln string
		if u.node != nil {
			bn, ln = unionBranchNames(u.node)
		} else if name != "" {
			bn, ln = name, name
		} else {
			bn, ln = typ, typ
		}
		unionStd = append(unionStd, bn)
		unionLog = append(unionLog, ln)
	}
	fillUnionTagTables(tags, deser, branchNodes, unionStd, unionLog)
	// byKind serves type-name dispatch in the encoders and encodeDefault (see
	// unionTypeNameForValue). Primitive kinds only: named branches go through
	// tagged dispatch, and the spec guarantees primitive kinds are unique
	// within a union. finalizeUnionNames need not rebuild it, since a forward
	// reference always names a named type.
	for i, branch := range branchNodes {
		if branch == nil {
			continue
		}
		switch branch.kind {
		case "null", "boolean", "int", "long", "float", "double", "string", "bytes":
			if tags.byKind == nil {
				tags.byKind = make(map[string]int, len(branchNodes))
			}
			if _, exists := tags.byKind[branch.kind]; !exists {
				tags.byKind[branch.kind] = i
			}
		}
	}

	switch {
	case len(s.union) == 2 && s.union[0].isNullBranch():
		b.ser = serNullUnion(ser)
		b.deser = deserNullUnion(deser)
		b.meta = b.buildNullUnionMeta(parentName, missing, branchMetas, 1, false)
	case len(s.union) == 2 && s.union[1].isNullBranch():
		b.ser = serNullSecondUnion(ser)
		b.deser = deserNullSecondUnion(deser)
		b.meta = b.buildNullUnionMeta(parentName, missing, branchMetas, 0, true)
	default:
		b.ser = ser.ser
		b.deser = deser.deser
		b.meta = fieldMeta{avroType: "union"}
	}
	if len(missing) > 0 {
		b.missing = append(b.missing, unionMissing{
			ser:        ser,
			deser:      deser,
			branches:   branchNodes,
			missing:    missing,
			parentName: parentName,
		})
	}
	b.node = &schemaNode{
		kind:     "union",
		branches: branchNodes,
		tags:     tags,
		ser:      b.ser,
		deser:    b.deser,
	}
	return nil
}

// unionTags is a union's parse-time name lookup, answered once and asked by
// both wires. byName is findUnionBranch's accept set, built by offering every
// branch to every unionTagTiers tier; byKind routes a value whose Go type
// names an Avro primitive kind. We allocate once per union and refill in
// place, since finalizeUnionNames rebuilds after forward references bind and
// every holder must see that rebuild. The methods tolerate a nil receiver
// for hand-assembled nodes.
type unionTags struct {
	byName map[string]int // the tag you write, to branch index
	byKind map[string]int // primitive branch kind to branch index; first wins
}

func (t *unionTags) branchByName(name string) (int, bool) {
	if t == nil {
		return 0, false
	}
	i, ok := t.byName[name]
	return i, ok
}

func (t *unionTags) branchByKind(kind string) (int, bool) {
	if t == nil {
		return 0, false
	}
	i, ok := t.byKind[kind]
	return i, ok
}

// fillUnionTagTables builds a union's three tag tables: the two the decoder
// emits and the one the encoders and the JSON decoder resolve a written tag
// through. An exact branch name outranks a logical qualifier, so a branch
// never emits a tag the decoder would hand to a different branch.
func fillUnionTagTables(tags *unionTags, deser *deserUnion, branches []*schemaNode, standard, logical []string) {
	deser.branchNames = append(deser.branchNames[:0], standard...)
	deser.logicalNames = deser.logicalNames[:0]
	for i, ln := range logical {
		if ln != standard[i] && unionLogicalTagOwnedElsewhere(standard, i, ln) {
			ln = standard[i]
		}
		deser.logicalNames = append(deser.logicalNames, ln)
	}
	// We build the accept table by offering every branch to every tier in
	// order, so it is the same set findUnionBranch accepts: across tiers
	// first write wins, and within a guarded tier a name two branches could
	// claim is registered nowhere, on both wire formats. A nil branch node
	// is an unbound forward reference; its exact name registers from
	// standard and finalizeUnionNames rebuilds later.
	if tags.byName == nil {
		tags.byName = make(map[string]int, len(standard))
	} else {
		clear(tags.byName)
	}
	// One scratch set for the whole walk. A union can carry any number of
	// branches, so "does another branch claim this name" must answer in
	// constant time per branch; the count map is allocated on first use.
	claims := make([]string, len(branches))
	claimed := make([]bool, len(branches))
	var claimCount map[string]int
	for _, tier := range unionTagTiers {
		for i, b := range branches {
			switch {
			case b != nil:
				claims[i], claimed[i] = tierClaim(tier, b)
			case i == 0 || tier.name == unionTagTiers[0].name:
				// A forward reference has no node yet; its as-written name is
				// the exact-name tier's claim until finalizeUnionNames rebuilds.
				claims[i], claimed[i] = "", false
				if tier.name == unionTagTiers[0].name && i < len(standard) {
					claims[i], claimed[i] = standard[i], true
				}
			default:
				claims[i], claimed[i] = "", false
			}
		}
		if tier.guarded {
			if claimCount == nil {
				claimCount = make(map[string]int, len(branches))
			} else {
				clear(claimCount)
			}
			for i := range branches {
				if claimed[i] {
					claimCount[claims[i]]++
				}
			}
		}
		for i := range branches {
			if !claimed[i] {
				continue
			}
			// A name two branches could claim is registered *nowhere*: the
			// resolver refuses it rather than picking one, and the two sides
			// have to refuse the same set.
			if tier.guarded && claimCount[claims[i]] > 1 {
				continue
			}
			if _, taken := tags.byName[claims[i]]; !taken {
				tags.byName[claims[i]] = i
			}
		}
	}
}

// finalizeUnionNames re-derives a union's name-dependent artifacts after
// every forward-referenced branch node is wired, since buildUnion captured
// the unresolved as-written name in both the duplicate-branch key and the
// branch-name tables. Duplicate detection re-keys by resolved fullname, so a
// short-name forward reference and an inline definition of the same type now
// collide, and the tag tables rebuild on the resolved full name.
func finalizeUnionNames(tags *unionTags, deser *deserUnion, branches []*schemaNode) error {
	saw := make(map[string]bool, len(branches))
	for _, n := range branches {
		key := n.kind
		if n.name != "" {
			key = n.name
		}
		if saw[key] {
			return fmt.Errorf("duplicate union type %q", truncForError(key))
		}
		saw[key] = true
	}
	std := unionStandardNames(branches)
	log := make([]string, len(branches))
	for i, n := range branches {
		_, log[i] = unionBranchNames(n)
	}
	fillUnionTagTables(tags, deser, branches, std, log)
	return nil
}

// buildNullUnionMeta returns the fieldMeta for the 2-branch null-union
// fast path. nonNullIdx is the index of the non-null branch (1 for
// ["null", T]; 0 for ["T", "null"]). When that branch is a forward
// reference, the inner meta is queued for finalize-time fixup;
// otherwise the inner meta is copied from branchMetas.
func (b *builder) buildNullUnionMeta(parentName string, missing map[int]string, branchMetas []fieldMeta, nonNullIdx int, nullSecond bool) fieldMeta {
	if name, isMissing := missing[nonNullIdx]; isMissing {
		inner := &fieldMeta{}
		b.mfixups = append(b.mfixups, metaFixup{meta: inner, name: name, parentName: parentName})
		return fieldMeta{avroType: "nullunion", nullSecond: nullSecond, inner: inner}
	}
	inner := new(fieldMeta)
	*inner = branchMetas[nonNullIdx]
	return fieldMeta{avroType: "nullunion", nullSecond: nullSecond, inner: inner}
}

func (b *builder) buildComplex(parentName string, s *aschema) error {
	// If this object is a primitive in the shape of a complex, we convert
	// this to a primitive.
	o := s.object

	// Save original logical type before validation clears unknown ones.
	origLogical := o.Logical
	if err := o.validateLogical(); err != nil {
		return err
	}
	// Restore unknown logical types if a registered CustomType matches.
	if o.Logical == "" && origLogical != "" {
		for _, ct := range b.customTypes {
			if ct.LogicalType == origLogical {
				o.Logical = origLogical
				break
			}
		}
	}

	if ser, isPrimitive := serPrimitive[o.Type]; isPrimitive {
		// "bytes" is the only primitive underlying validateLogical permits for
		// decimal; fixed is built on the named-type path. The gate matters
		// because the CustomType resurrection above can restore a dropped
		// decimal onto any primitive with o.Precision still nil. A resurrected
		// logical on another primitive takes the plain path, where the
		// CustomType wraps the base ser/deser.
		if o.Logical == "decimal" && o.Type == "bytes" {
			scale := 0
			if o.Scale != nil {
				scale = *o.Scale
			}
			// Per-direction suppression, as on the timestamp/uuid path: the
			// built-in encoder stays unless you provided an Encode, while any
			// matching CustomType suppresses the built-in decoder. One gate
			// for both would break encoding *big.Rat with a Decode-only
			// CustomType.
			if b.hasMatchingCustomTypeWithEncode(o.Type, o.Logical) {
				b.ser = ser
			} else {
				b.ser = (&serBytesDecimal{precision: *o.Precision, scale: scale}).ser
			}
			if b.hasMatchingCustomType(o.Type, o.Logical) {
				b.deser = deserPrimitive[o.Type]
			} else {
				b.deser = (&deserBytesDecimal{scale: scale}).deser
			}
			b.canon = aschema{primitive: o.Type}
			b.meta = fieldMeta{avroType: o.Type, logical: o.Logical}
			nd := &schemaNode{
				kind:      o.Type,
				logical:   o.Logical,
				ser:       b.ser,
				deser:     b.deser,
				precision: *o.Precision,
				scale:     scale,
			}
			b.node = nd
			return nil
		}
		if o.Logical == "big-decimal" && o.Type == "bytes" {
			if b.hasMatchingCustomTypeWithEncode(o.Type, o.Logical) {
				b.ser = ser
			} else {
				b.ser = (&serBigDecimal{}).ser
			}
			if b.hasMatchingCustomType(o.Type, o.Logical) {
				b.deser = deserPrimitive[o.Type]
			} else {
				b.deser = (&deserBigDecimal{}).deser
			}
			b.canon = aschema{primitive: o.Type}
			b.meta = fieldMeta{avroType: o.Type, logical: o.Logical}
			nd := &schemaNode{
				kind:    o.Type,
				logical: o.Logical,
				ser:     b.ser,
				deser:   b.deser,
			}
			b.node = nd
			return nil
		}
		b.ser = ser
		b.deser = deserPrimitive[o.Type]
		// The logical serializer accepts a strict superset of the base one,
		// but only where the logical is valid for this kind: the CustomType
		// resurrection can restore a soft-dropped logical onto a kind it is
		// not valid for, and logicalSer keys on the name alone, so without
		// the gate binary would apply serUUID on bytes while the JSON path
		// stays raw. The deser gate on a matching CustomType is independent:
		// a CustomType naming a different AvroType resurrects the logical
		// without matching for suppression.
		if logSer := logicalSer(o.Logical); logSer != nil && logicalUnderlyingAcceptsObject(o) {
			b.ser = logSer
		}
		if !b.hasMatchingCustomType(o.Type, o.Logical) && logicalUnderlyingAcceptsObject(o) {
			if logDeser := logicalDeser(o.Logical); logDeser != nil {
				b.deser = logDeser
			}
		}
		b.canon = aschema{primitive: o.Type}
		b.meta = fieldMeta{avroType: o.Type, logical: o.Logical}
		nd := &schemaNode{
			kind:    o.Type,
			logical: o.Logical,
			ser:     b.ser,
			deser:   b.deser,
		}
		if o.Logical == "" && origLogical != "" {
			nd.unknownLogical = origLogical
		}
		// o.Precision/o.Scale are not copied here: the node
		// fields hold validated decimal parameters only (the bytes-decimal
		// branch above and the fixed build's decimal arm). On this path the
		// keys were never consumed (a soft-dropped/resurrected decimal on a
		// wrong carrier, or a stray placement), so their values are
		// unvalidated inert metadata, surfaced through extra as props instead.
		b.node = nd
		return nil
	}

	// A named-type reference wrapped in an object, {"type":"Node"} with no
	// type-defining keys, which Java's parser accepts. A forward reference
	// returns unknownPrimitiveError so the caller can queue a fixup, as on
	// the bare-string path. Any type-defining key present means you are
	// defining a new type, so we fall through to the regular dispatch.
	if o.Name == "" &&
		len(o.Fields) == 0 && len(o.Symbols) == 0 &&
		o.Items == nil && o.Values == nil && o.Size == nil {
		if found, err := b.tryAssignNamedRef(o.Type, parentName, true); err != nil || found {
			return err
		}
		// Not a recognized base/complex type and not a declared named
		// type: treat as a forward reference. The caller (record-field
		// build, union dispatch, etc.) catches unknownPrimitiveError and
		// queues a fixup keyed on the name in the error.
		switch o.Type {
		case "record", "error", "enum", "fixed", "array", "map":
			// real complex-type-without-required-fields: fall through
			// to the existing switch which reports the right error.
		default:
			if _, isPrim := serPrimitive[o.Type]; !isPrim {
				b.canon = aschema{primitive: o.Type}
				return &unknownPrimitiveError{o.Type}
			}
		}
	}

	// Preserve original aliases and enum default before canonical stripping.
	origAliases := s.object.Aliases
	origEnumDefault := s.object.Default
	origFieldAliases := make([][]string, len(s.object.Fields))
	for i, f := range s.object.Fields {
		origFieldAliases[i] = f.Aliases
	}

	// Canonical form keeps only type, name, fields, symbols, items, values,
	// and size, per the spec's STRIP rule. "error" normalizes to "record" so
	// the fingerprint matches Java and fastavro; Root().Type and String()
	// still preserve the as-written "error".
	canonType := o.Type
	if canonType == "error" {
		canonType = "record"
	}
	canonObj := &aobject{
		Name: o.Name,
		Type: canonType,

		Fields:  o.Fields,
		Symbols: o.Symbols,
		Items:   o.Items,
		Values:  o.Values,
		Size:    o.Size,

		Namespace: o.Namespace,
	}
	b.canon = aschema{object: canonObj}

	if isNamedKind(o.Type) {
		if err := b.validFullnameErr(o.Name); err != nil {
			return fmt.Errorf("invalid %s name %q: %w", truncForError(o.Type), truncForError(o.Name), err)
		}
		// The namespace attribute is a dot-separated sequence of names and
		// owes the same grammar as the name; without this
		// {"name":"R","namespace":"bad ns"} would skip validation while the
		// identical fullname {"name":"bad ns.R"} is rejected, and the
		// canonical form would fail to re-parse. A dotted name ignores the
		// attribute per the spec, and "" is the null-namespace escape.
		if o.Namespace != nil && *o.Namespace != "" && !strings.Contains(o.Name, ".") {
			if err := b.validFullnameErr(*o.Namespace); err != nil {
				return fmt.Errorf("invalid %s namespace %q: %w", truncForError(o.Type), truncForError(*o.Namespace), err)
			}
		}
		// We do not name-validate aliases: the spec says any string is
		// accepted as an alias, so evolution can alias a valid name to a
		// writer's illegal legacy name, and fastavro validates none. Java
		// validates type aliases but not field aliases. qualifyAliases still
		// applies namespace qualification and the leading-dot escape.
		ns := ""
		hasNS := false
		if o.Namespace != nil {
			ns = *o.Namespace
			hasNS = true
		}
		if strings.Contains(o.Name, ".") {
			// Fullname (dot-separated): ignore parent & our own namespace.
			parentName = ""
			hasNS = false
			if short, ok := leadingDotName(o.Name); ok {
				// ".x" is the null-namespace fullname "x" and "." collapses to
				// the empty name, reachable only under a WithLaxNames fn that
				// accepts "". Without this the name registered verbatim while
				// child registration and reference resolution disagreed, so a
				// bare sibling reference inside ".x" could not resolve.
				o.Name = short
			}
		}
		if hasNS && ns != "" {
			o.Name = ns + "." + o.Name // have namespace: prefix our name
		} else if hasNS && ns == "" {
			// Explicit empty namespace: clear inherited namespace.
		} else if parentName != "" {
			if dot := strings.LastIndexByte(parentName, '.'); dot >= 0 {
				o.Name = parentName[:dot+1] + o.Name // no namespace: prefix our name with parent namespace if there is one
			}
		}
		// Per the Avro spec (Names): a primitive type name has no
		// namespace and may not name a record/enum/fixed. o.Name is now
		// the resolved fullname; serPrimitive's keys are exactly the 8
		// bare primitive names, so this matches only a null-namespace
		// bare primitive name (a namespaced "a.int" has fullname "a.int",
		// not a key). Matches Java's NamedSchema "may not be named after
		// primitives".
		if _, isPrim := serPrimitive[o.Name]; isPrim {
			return fmt.Errorf("%s may not be named after the primitive type %q", truncForError(o.Type), truncForError(o.Name))
		}
		o.Namespace = nil      // canonical form omits namespace
		canonObj.Name = o.Name // use fully-qualified name
		canonObj.Namespace = nil
		if _, exists := b.named[o.Name]; exists {
			if !(b.cachedNames[o.Name] && b.allowReRegister) {
				return fmt.Errorf("duplicate named type %q", truncForError(o.Name))
			}
			// Inherited name re-registered by a custom (re-)parse, allowed so
			// it gets fresh CustomType wiring.
		}
	} else {
		// A stray "namespace" on an unnamed kind is inert metadata that never
		// scopes children, and the metadata tree carries it as written; Java
		// ignores it on every schema object and fastavro accepts it. A stray
		// "name" on a container kind still rejects: the metadata walkers scope
		// children by any non-empty SchemaNode.Name, so a parsed stray name
		// would make Root() scope named descendants differently than the
		// parser does. Primitive objects have no child positions, so they
		// keep accepting one.
		if o.Name != "" {
			return errors.New("only record, enum, and fixed can have a name")
		}
		// The inert attribute never reaches the canonical form (PCF has no
		// namespace key for unnamed kinds; fastavro's
		// to_parsing_canonical_form strips it, executed); mirror the
		// primitive type-object collapse, which drops it the same way.
		o.Namespace = nil
		canonObj.Namespace = nil
	}

	switch o.Type {
	default:
		return fmt.Errorf("unknown complex type %q", truncForError(o.Type))

	case "record", "error":
		if len(o.Symbols) > 0 ||
			o.Items != nil ||
			o.Values != nil ||
			o.Size != nil {
			return errors.New("invalid record has schema for other types")
		}
		// The fields attribute is required (Java: "Record has no fields"),
		// while an *empty* array is the legal empty record. Same
		// missing-vs-empty discrimination as enum symbols: we materialize
		// "fields":[] as a non-nil empty slice and leave the attribute's
		// absence as nil.
		if o.Fields == nil {
			return errors.New("record is missing fields")
		}

		// We create the record ser/deser and register early so
		// self-referencing fields (e.g. array items, map values)
		// can resolve the type by name during field building.
		sr := &serRecord{}
		dr := &deserRecord{}
		b.ser = sr.ser
		b.deser = dr.deser
		b.meta = fieldMeta{avroType: "record", serRecord: sr, deserRecord: dr}

		nd := &schemaNode{
			kind:        "record",
			name:        o.Name,
			logical:     o.Logical,
			aliases:     qualifyAliases(origAliases, o.Name),
			bareAliases: bareAliasShorts(origAliases),
			ser:         b.ser,
			deser:       b.deser,
			serRecord:   sr,
			deserRecord: dr,
		}
		// The node and the parse's skip walk back dr.fieldSkips, which
		// compiles lazily at decode time, after every field fixup has landed.
		dr.node = nd
		dr.mbw = b.skipWalk
		b.registerNamed(o.Name, &namedType{node: nd})
		b.node = nd

		// We mark this record as under construction. A field default whose
		// type subtree references this record must defer its default-encode
		// to finalize: encodeDefault recurses into the referenced record's
		// fields, and nd.fields holds only the fields declared so far. The set
		// is shared through nest so a nested record sees it.
		if b.building == nil {
			b.building = make(map[*schemaNode]struct{})
		}
		b.building[nd] = struct{}{}
		defer delete(b.building, nd)

		var names []string
		seenFields := make(map[string]bool, len(o.Fields))
		for i, of := range o.Fields {
			if err := b.validNameErr(of.Name); err != nil {
				return fmt.Errorf("invalid field name %q: %w", truncForError(of.Name), err)
			}
			// We do not name-validate field aliases: per the Avro spec any
			// string is accepted as an alias, so a reader can alias a writer's
			// illegal/legacy field name. We match them as-is against writer
			// field names during resolution, like Java and fastavro.
			if seenFields[of.Name] {
				return fmt.Errorf("duplicate record field name %q", truncForError(of.Name))
			}
			seenFields[of.Name] = true
			// Written-ness, not non-emptiness, admits the value: "order":"" is
			// a written order and not one of the spec's three, so it fails
			// like any other non-spec spelling. The comparison is exact-case:
			// Apache Avro upper-cases before its lookup, but reserved
			// attribute values match by exact spelling here.
			if of.orderSet && of.Order != "ascending" && of.Order != "descending" && of.Order != "ignore" {
				return fmt.Errorf("invalid field order %q for field %q", truncForError(of.Order), truncForError(of.Name))
			}
			bf := b.nest()
			isFwdRef, fwdRefName, err := captureFwdRef(bf.build(o.Name, of.Type), "record field")
			if err != nil {
				return err
			}
			b.unnest(bf)
			if isFwdRef {
				bf.canon = aschema{primitive: fwdRefName}
			}
			o.Fields[i] = afield{
				Name:       of.Name,
				Type:       &bf.canon,
				hasDefault: len(of.Default) > 0,
			}
			meta := new(fieldMeta)
			*meta = bf.meta
			fieldIdx := len(sr.fields)
			sr.fields = append(sr.fields, serRecordField{
				name:    of.Name,
				nameVal: reflect.ValueOf(of.Name),
				fn:      bf.ser,
				meta:    meta,
			})
			drf := deserRecordField{
				name:    of.Name,
				nameVal: reflect.ValueOf(of.Name),
				fn:      bf.deser,
				fnIface: ifaceFnForPrimitive(meta),
				meta:    meta,
			}
			fn := fieldNode{
				name:    of.Name,
				nameVal: reflect.ValueOf(of.Name),
				aliases: origFieldAliases[i],
				node:    bf.node,
			}
			if isFwdRef {
				fix := recordFieldFixup{
					nd:         nd,
					idx:        fieldIdx,
					name:       fwdRefName,
					parentName: o.Name, // fields build under the record's fullname
				}
				if len(of.Default) > 0 {
					fix.defaultVal = unmarshalDefault(of.Default)
					fix.hasDefault = true
				}
				b.fieldFixups = append(b.fieldFixups, fix)
			}
			if len(of.Default) > 0 {
				if isFwdRef {
					// Forward-ref: signal hasDefault so the dispatch
					// knows a default exists. finalize() runs the full
					// pipeline against the resolved schemaNode and
					// overwrites defaultVal there.
					drf.hasDefault = true
					fn.hasDefault = true
				} else if b.nodeAwaitsForwardRef(bf.node) {
					// The field's outer type resolved, but a descendant
					// encodeDefault traverses is not whole; see
					// nodeAwaitsForwardRef. Defer the resolve+encode to
					// finalize, after the fixups wire the descendants and every
					// in-construction record completes. Signal hasDefault so
					// dispatch knows a default exists; the deferred pass fills
					// defaultVal/defaultBytes.
					drf.hasDefault = true
					fn.hasDefault = true
					b.defaultFixups = append(b.defaultFixups, defaultFixup{
						nd:         nd,
						idx:        fieldIdx,
						node:       bf.node,
						defaultVal: unmarshalDefault(of.Default),
					})
				} else {
					defaultVal := unmarshalDefault(of.Default)
					defaultVal = coerceDefault(defaultVal, bf.node)
					if err := applyResolvedDefault(
						defaultVal, bf.node, of.Name,
						&drf, &fn, &sr.fields[fieldIdx],
					); err != nil {
						return err
					}
				}
			} else if bf.canon.isNullableUnion() {
				// Per the Avro spec, a union whose first branch is "null"
				// implicitly defaults to null when no explicit default is given.
				// fn.defaultVal stays nil: the JSON encoder treats a nil
				// default as the null encoding.
				drf.hasDefault = true
				fn.hasDefault = true
				sr.fields[fieldIdx].defaultBytes = []byte{0} // varint 0 = null branch
				sr.fields[fieldIdx].hasDefault = true
			}
			dr.fields = append(dr.fields, drf)
			nd.fields = append(nd.fields, fn)
			names = append(names, of.Name)
		}
		sr.names = names
		dr.names = names
		// DecodeJSON routes record keys through fieldIdx, so every alias maps
		// beside the canonical name; the binary side does the same through
		// readerFieldLookup. Aliases share one namespace with names within a
		// record, per the spec, so a later name shadowing a prior alias or a
		// later alias shadowing a prior name both reject. Checking only the
		// alias side would let [{name:"a",aliases:["x"]},{name:"x"}] parse and
		// route differently from Java's applyAliases.
		nd.fieldIdx = make(map[string]int, len(nd.fields))
		for i, f := range nd.fields {
			if _, exists := nd.fieldIdx[f.name]; exists {
				return fmt.Errorf("record field name %q collides with another field name or alias", truncForError(f.name))
			}
			nd.fieldIdx[f.name] = i
			for _, a := range f.aliases {
				if _, exists := nd.fieldIdx[a]; exists {
					return fmt.Errorf("record field alias %q collides with another field name or alias", truncForError(a))
				}
				nd.fieldIdx[a] = i
			}
		}
	case "enum":
		if len(o.Fields) > 0 ||
			o.Items != nil ||
			o.Values != nil ||
			o.Size != nil {
			return errors.New("invalid enum has schema for other types")
		}

		// The symbols attribute is required (Java: "Enum has no symbols"), but
		// an *empty* array is legal: the spec asks only for "a JSON array,
		// listing symbols", and Java, fastavro and avro-rs all accept zero.
		// Such an enum has no valid values, so every encode/decode errors, but
		// the schema parses, which matters for passing through foreign schemas
		// carrying a degenerate enum the data never uses.
		if o.Symbols == nil {
			return errors.New("enum is missing symbols")
		}
		seenSymbols := make(map[string]bool, len(o.Symbols))
		for _, e := range o.Symbols {
			if err := b.validNameErr(e); err != nil {
				return fmt.Errorf("invalid enum symbol %q: %w", truncForError(e), err)
			}
			if seenSymbols[e] {
				return fmt.Errorf("duplicate enum symbol %q", truncForError(e))
			}
			seenSymbols[e] = true
		}
		symbolIdx := enumSymbolIndex(o.Symbols)
		b.ser = newSerEnum(o.Symbols, symbolIdx).ser
		b.deser = (&deserEnum{symbols: o.Symbols}).deser
		b.meta = fieldMeta{avroType: "enum"}

		nd := &schemaNode{
			kind:        "enum",
			name:        o.Name,
			logical:     o.Logical,
			aliases:     qualifyAliases(origAliases, o.Name),
			bareAliases: bareAliasShorts(origAliases),
			symbols:     o.Symbols,
			symbolIdx:   symbolIdx,
			ser:         b.ser,
			deser:       b.deser,
		}
		if len(origEnumDefault) > 0 {
			// The default must be a JSON string token naming a symbol, decided
			// by token type before the membership check: on a non-string body
			// json.Unmarshal leaves "", which can be a member under a
			// WithLaxNames validator that accepts empty components. Neither
			// fastavro nor Java binds a non-string enum default.
			tok := bytes.TrimSpace(origEnumDefault)
			var defStr string
			if len(tok) == 0 || tok[0] != '"' || json.Unmarshal(tok, &defStr) != nil {
				return fmt.Errorf("enum default %s is not a string", truncForError(string(tok)))
			}
			if !seenSymbols[defStr] {
				return fmt.Errorf("enum default %q is not a member of symbols", truncForError(defStr))
			}
			nd.enumDef = defStr
			nd.hasEnumDef = true
		}
		b.registerNamed(o.Name, &namedType{node: nd})
		b.node = nd

	case "array":
		if len(o.Fields) > 0 ||
			len(o.Symbols) > 0 ||
			o.Values != nil ||
			o.Size != nil {
			return errors.New("invalid array has schema for other types")
		}
		if o.Items == nil {
			return errors.New("array is missing items schema")
		}
		af := b.nest()
		isFwdRef, fwdRefName, err := captureFwdRef(af.build(parentName, o.Items), "array")
		if err != nil {
			return err
		}
		b.unnest(af)
		if isFwdRef {
			af.canon = aschema{primitive: fwdRefName}
		}
		o.Items = &af.canon
		// canonObj captured o.Items by value before this recursion ran, so we
		// repoint it at the canonicalized child, as Java's SchemaNormalization
		// does. Record fields stay correct via the o.Fields slice alias; only
		// the Items/Values pointers need the sync.
		canonObj.Items = &af.canon
		sa := &serArray{serItem: af.ser}
		// One computation feeding both wire-side slots. They are the same
		// question asked by the safe and the unsafe array reader, and asking
		// it separately is how they came to hold different answers. Only
		// da's was patched by the forward-ref fixup below.
		itemMin := b.minBytes.minBytesOf(af.node)
		da := &deserArray{deserItem: af.deser, minItemBytes: itemMin}
		// Specialized array ser/deser fast paths bypass the inner
		// schema's wrapped ser/deser functions. They are correct only
		// when no per-element conversion is needed: no custom type,
		// no logical type, and no forward reference. The inner ser/
		// deser aren't wired until finalize() resolves the fwd-ref,
		// so the fast-path closure would capture nil fns at build
		// time.
		if isFwdRef || af.meta.hasCustomType || af.meta.logical != "" {
			b.ser = sa.ser
		} else if info, ok := primFast[af.canon.primitive]; ok {
			b.ser = info.serArrayFn(sa)
			da.fastLoop = info.deserArrayLoop
			da.fastElemKind = info.elemKind
			da.fastIfaceLoop = info.deserArrayIfaceLoop
			da.nativeLoop = info.deserArrayNative
		} else {
			b.ser = sa.ser
		}
		b.deser = da.deser
		inner := new(fieldMeta)
		*inner = af.meta
		inner.minBytes = itemMin
		b.meta = fieldMeta{avroType: "array", inner: inner}
		arrayNode := &schemaNode{
			kind:  "array",
			items: af.node,
			ser:   b.ser,
			deser: b.deser,
		}
		b.node = arrayNode
		if isFwdRef {
			// finalize() wires the fwd-ref's resolved node. We capture
			// pointers to all four wire-side slots that depend on the
			// resolved type so the fixup can patch them once
			// b.named[fwdRefName] becomes available.
			b.containerFixups = append(b.containerFixups, containerFixup{
				serItem:   &sa.serItem,
				deserItem: &da.deserItem,
				// Both slots: da backs the safe array path, inner.minBytes
				// the unsafe one. Patching one left the other holding the
				// build-time answer, computed while this child was still an
				// unwired forward reference.
				setMinBytes: func(n int) { da.minItemBytes = n; inner.minBytes = n },
				nodeChild:   &arrayNode.items,
				name:        fwdRefName,
				parentName:  parentName,
				ctxLabel:    "array",
			})
		}

	case "map":
		if len(o.Fields) > 0 ||
			len(o.Symbols) > 0 ||
			o.Items != nil ||
			o.Size != nil {
			return errors.New("invalid map has schema for other types")
		}
		if o.Values == nil {
			return errors.New("map is missing values schema")
		}
		mf := b.nest()
		isFwdRef, fwdRefName, err := captureFwdRef(mf.build(parentName, o.Values), "map")
		if err != nil {
			return err
		}
		b.unnest(mf)
		if isFwdRef {
			mf.canon = aschema{primitive: fwdRefName}
		}
		o.Values = &mf.canon
		// See the array case above: canonObj.Values still points at the
		// as-parsed values schema, so we repoint it at the canonicalized
		// child. Otherwise the canonical form (and fingerprint) diverges for
		// any map-of-wrapped-or-attribute-bearing-value schema.
		canonObj.Values = &mf.canon
		sm := &serMap{serItem: mf.ser}
		// minEntryBytes = 1 (empty-key length varint) + values' minimum
		// wire bytes. Matches deserArray.minItemBytes in spirit; bounds
		// block-count against remaining-buffer to prevent memory
		// amplification on hostile input.
		dm := &deserMap{deserItem: mf.deser, minEntryBytes: mapEntryMinBytes(b.minBytes.minBytesOf(mf.node))}
		// Same gate as the array case above: we skip specialization when
		// values have a custom type, a logical type, or a forward
		// reference (the fast-path closure can't capture an unresolved
		// inner ser/deser).
		if isFwdRef || mf.meta.hasCustomType || mf.meta.logical != "" {
			b.ser = sm.ser
		} else if info, ok := primFast[mf.canon.primitive]; ok {
			b.ser = info.serMapFn(sm)
			dm.fastBlock = info.deserMapBlock
			dm.fastElemKind = info.elemKind
			dm.fastIfaceVal = info.deserMapIfaceVal
			dm.nativeBlock = info.deserMapNative
		} else {
			b.ser = sm.ser
		}
		b.deser = dm.deser
		b.meta = fieldMeta{avroType: "map"}
		mapNode := &schemaNode{
			kind:   "map",
			values: mf.node,
			ser:    b.ser,
			deser:  b.deser,
		}
		b.node = mapNode
		if isFwdRef {
			b.containerFixups = append(b.containerFixups, containerFixup{
				serItem:     &sm.serItem,
				deserItem:   &dm.deserItem,
				setMinBytes: func(n int) { dm.minEntryBytes = mapEntryMinBytes(n) },
				nodeChild:   &mapNode.values,
				name:        fwdRefName,
				parentName:  parentName,
				ctxLabel:    "map",
			})
		}

	case "fixed":
		if len(o.Fields) > 0 ||
			len(o.Symbols) > 0 ||
			o.Items != nil ||
			o.Values != nil {
			return errors.New("invalid fixed has schema for other types")
		}
		if o.Size == nil {
			return errors.New("fixed is missing size")
		}
		size := int(*o.Size)
		// Size 0 is legal, and the upper bound is unbounded at parse as in
		// fastavro and avro-rs; a size beyond the datum fails at encode or
		// decode. No parse-time path may allocate proportional to this size.
		if size < 0 {
			return fmt.Errorf("invalid fixed size %v", size)
		}
		// Per-direction suppression: the built-in encoder stays unless you
		// provided an Encode, while any matching CustomType suppresses the
		// built-in decoder. One gate for both would route a Decode-only
		// CustomType onto raw serSize, which cannot accept *big.Rat.
		hasEnc := b.hasMatchingCustomTypeWithEncode("fixed", s.object.Logical)
		hasAny := b.hasMatchingCustomType("fixed", s.object.Logical)
		switch s.object.Logical {
		case "duration":
			// serDuration always emits 12 bytes, and the CustomType
			// resurrection can restore a duration validateLogical dropped
			// for a wrong size, so a wrong-size fixed keeps the raw serSize,
			// matching the suppressed decoder.
			if hasEnc || !logicalUnderlyingAccept["duration"](o) {
				b.ser = (&serSize{size}).ser
			} else {
				b.ser = serDuration
			}
			// deserDuration always reads 12 bytes, so it is only correct for a
			// size-12 fixed. Mirror the ser gate: a wrong-size duration (which
			// validateLogical soft-drops and a CustomType can resurrect) keeps
			// the raw deserFixed{size}, matching the plain fixed and the kind/
			// size-checked JSON decode. hasAny suppresses for a matching custom;
			// !logicalUnderlyingAccept covers a resurrection whose custom AvroType
			// names a different kind (so it does not match for suppression).
			if hasAny || !logicalUnderlyingAccept["duration"](o) {
				b.deser = (&deserFixed{size}).deser
			} else {
				b.deser = deserDuration
			}
		case "decimal":
			scale := 0
			if o.Scale != nil {
				scale = *o.Scale
			}
			if hasEnc {
				b.ser = (&serSize{size}).ser
			} else {
				b.ser = (&serFixedDecimal{size: size, precision: *o.Precision, scale: scale}).ser
			}
			if hasAny {
				b.deser = (&deserFixed{size}).deser
			} else {
				b.deser = (&deserFixedDecimal{size: size, scale: scale}).deser
			}
		case "uuid":
			// serFixedUUIDReflect always emits 16 bytes, so it is only correct for
			// a size-16 fixed. validateLogical soft-drops a uuid on a wrong
			// size, but the CustomType resurrection can restore it. Without
			// the size gate serFixedUUIDReflect would write 16 bytes into a
			// size != 16 fixed, a wire this schema's own deserFixed{size}
			// reader (and the JSON arm) cannot read. logicalUnderlyingAccept
			// is the same size predicate validateLogical uses to soft-drop.
			if hasEnc || !logicalUnderlyingAccept["uuid"](o) {
				b.ser = (&serSize{size}).ser
			} else {
				b.ser = serFixedUUIDReflect
			}
			// deserFixedUUIDReflect always reads 16 bytes; mirror the ser gate so a
			// wrong-size resurrected uuid keeps the raw deserFixed{size} (see the
			// duration case for the hasAny / !logicalUnderlyingAccept split).
			if hasAny || !logicalUnderlyingAccept["uuid"](o) {
				b.deser = (&deserFixed{size}).deser
			} else {
				b.deser = deserFixedUUIDReflect
			}
		default:
			b.ser = (&serSize{size}).ser
			b.deser = (&deserFixed{size}).deser
		}
		b.meta = fieldMeta{avroType: "fixed", logical: s.object.Logical}
		nd := &schemaNode{
			kind:        "fixed",
			name:        o.Name,
			aliases:     qualifyAliases(origAliases, o.Name),
			bareAliases: bareAliasShorts(origAliases),
			logical:     s.object.Logical,
			size:        size,
			ser:         b.ser,
			deser:       b.deser,
		}
		if s.object.Logical == "decimal" && s.object.Precision != nil {
			nd.precision = *s.object.Precision
			if s.object.Scale != nil {
				nd.scale = *s.object.Scale
			}
		}
		b.node = nd
		b.registerNamed(o.Name, &namedType{node: nd})
	}
	return nil
}

// bareAliasShorts collects the aliases declared without any dot, as written.
// A dotted alias is an explicit fullname and matches only exactly; a bare
// alias also short-name-matches a writer type in any namespace, fastavro's
// raw-string tier. The declared spelling is required: after qualification a
// same-namespace "a.Old" is indistinguishable from bare "Old".
func bareAliasShorts(aliases []string) []string {
	var shorts []string
	for _, a := range aliases {
		if !strings.Contains(a, ".") {
			shorts = append(shorts, a)
		}
	}
	return shorts
}

func qualifyAliases(aliases []string, fullname string) []string {
	if len(aliases) == 0 {
		return nil
	}
	ns := ""
	if dot := strings.LastIndexByte(fullname, '.'); dot >= 0 {
		ns = fullname[:dot+1]
	}
	out := make([]string, len(aliases))
	for i, a := range aliases {
		switch {
		case strings.ContainsRune(a, '.'):
			// Dotted aliases follow the names' dot rule: a single leading dot
			// with a dotless remainder is the null-namespace escape, and any
			// other dotted spelling is a fullname verbatim. Java's Name
			// constructor nulls the space only when it is empty, so ".a.b"
			// keeps its space; stripping any leading dot here made alias
			// ".a.b" match writer "a.b", which neither Java nor fastavro does.
			short, _ := leadingDotName(a)
			out[i] = short
		default:
			out[i] = ns + a
		}
	}
	return out
}

// logicalUnderlyingAccept maps known logical types to the predicate deciding
// whether the carrier's Avro type is permitted. A mismatch soft-drops the
// logical, per spec. "decimal" is handled inline in validateLogical, since
// its precision/scale validation does not fit a predicate.
var logicalUnderlyingAccept = map[string]func(o *aobject) bool{
	"uuid": func(o *aobject) bool {
		return o.Type == "string" || (o.Type == "fixed" && o.Size != nil && int(*o.Size) == 16)
	},
	"date":                   func(o *aobject) bool { return o.Type == "int" },
	"time-millis":            func(o *aobject) bool { return o.Type == "int" },
	"time-micros":            func(o *aobject) bool { return o.Type == "long" },
	"timestamp-millis":       func(o *aobject) bool { return o.Type == "long" },
	"timestamp-micros":       func(o *aobject) bool { return o.Type == "long" },
	"timestamp-nanos":        func(o *aobject) bool { return o.Type == "long" },
	"local-timestamp-millis": func(o *aobject) bool { return o.Type == "long" },
	"local-timestamp-micros": func(o *aobject) bool { return o.Type == "long" },
	"local-timestamp-nanos":  func(o *aobject) bool { return o.Type == "long" },
	"big-decimal":            func(o *aobject) bool { return o.Type == "bytes" },
	// Duration on non-fixed, or fixed with size != 12, soft-drops, as in
	// Java and hamba.
	"duration": func(o *aobject) bool {
		return o.Type == "fixed" && o.Size != nil && int(*o.Size) == 12
	},
}

// logicalUnderlyingAcceptsObject reports whether o.Logical is valid on o's
// Avro underlying type and size, the one predicate the primitive build's
// logical ser/deser selection gates on. It is false for decimal, which
// validateLogical handles inline, and for any logical with no name-keyed
// codec, where the gate is moot.
func logicalUnderlyingAcceptsObject(o *aobject) bool {
	accept := logicalUnderlyingAccept[o.Logical]
	return accept != nil && accept(o)
}

func (o *aobject) validateLogical() error {
	switch o.Logical {
	case "":
		// No logical type. Stray precision/scale are inert metadata;
		// see the note below the switch.

	case "decimal":
		// A wrong underlying type soft-drops, as the spec implies, but
		// precision/scale constraints are explicit rules, so violating them
		// hard-rejects, as fastavro does. Java soft-drops to bare bytes,
		// which would turn a producer-declared decimal into plain bytes.
		if o.Type != "bytes" && o.Type != "fixed" {
			o.Logical = ""
			return nil
		}
		if o.Precision == nil {
			return fmt.Errorf("decimal logical type requires precision")
		}
		if *o.Precision <= 0 {
			return fmt.Errorf("decimal precision %d must be positive", *o.Precision)
		}
		scale := 0
		if o.Scale != nil {
			scale = *o.Scale
		}
		if scale < 0 {
			return fmt.Errorf("decimal scale %d must not be negative", scale)
		}
		if scale > *o.Precision {
			return fmt.Errorf("decimal scale %d exceeds precision %d", scale, *o.Precision)
		}
		// DoS bound: precision/scale drive 10^scale allocations in
		// bytesToRat / ratToUnscaled at every decode/encode.
		if *o.Precision > decimalScaleLimit {
			return fmt.Errorf("decimal precision %d exceeds %d limit", *o.Precision, decimalScaleLimit)
		}
		if scale > decimalScaleLimit {
			return fmt.Errorf("decimal scale %d exceeds %d limit", scale, decimalScaleLimit)
		}
		if o.Type == "fixed" && o.Size != nil {
			maxDigits := maxDecimalDigits(int(*o.Size))
			if *o.Precision > maxDigits {
				return fmt.Errorf("decimal precision %d exceeds fixed(%d) capacity %d", *o.Precision, *o.Size, maxDigits)
			}
		}
		return nil

	// A wrong underlying type soft-drops, per the spec: "implementations
	// should ignore the logical type and use the underlying Avro type". Java,
	// fastavro and hamba all agree.
	default:
		if accept, known := logicalUnderlyingAccept[o.Logical]; known {
			if !accept(o) {
				o.Logical = ""
			}
		} else {
			// Per the Avro spec, unknown logical types are ignored and the
			// underlying type is used as-is.
			o.Logical = ""
		}
	}

	// Leftover precision/scale anywhere but the decimal arm are inert
	// metadata, not a parse error: the spec permits undefined attributes,
	// Java never consults precision without a logicalType, and fastavro
	// accepts every placement. They ride to Props.
	return nil
}

// maxDecimalDigits returns the maximum number of decimal digits that fit in
// a two's-complement signed integer of the given byte size:
// floor(log10(2^(8*size-1) - 1)).
func maxDecimalDigits(size int) int {
	if size <= 0 {
		return 0
	}
	// Saturate before the bit multiply: a fixed size can exceed 2^60 on a
	// 64-bit build, where 8*size-1 wraps negative and falsely rejects a valid
	// precision. The verdict is unchanged, since validateLogical already
	// rejects a precision above decimalScaleLimit.
	size = saturateSchemaMagnitude(size)
	bits := magnitudeWidestMultiplier*size - 1 // sign bit excluded
	// log10(2^bits - 1) ~ bits * log10(2)
	return int(math.Floor(float64(bits) * math.Log10(2)))
}

// logicalSer / logicalDeser look up the time-aware encoder / decoder
// for a logical type, or return nil if the logical has no specialized
// codec. Both encode and decode tables in one place so a new logical
// only needs to be wired in once.
var (
	logicalSers = map[string]serfn{
		"timestamp-millis":       serTimestampMillis,
		"timestamp-micros":       serTimestampMicros,
		"timestamp-nanos":        serTimestampNanos,
		"local-timestamp-millis": serLocalTimestampMillis,
		"local-timestamp-micros": serLocalTimestampMicros,
		"local-timestamp-nanos":  serLocalTimestampNanos,
		"date":                   serDate,
		"time-millis":            serTimeMillis,
		"time-micros":            serTimeMicros,
		"uuid":                   serUUID,
	}
	// Decode collapses local-timestamp-* with timestamp-* because both
	// resolve to the same UTC time.Time (the wire long is interpreted
	// identically; see logical.go for the encode-side rationale).
	logicalDesers = map[string]deserfn{
		"timestamp-millis":       deserTimestampMillis,
		"local-timestamp-millis": deserTimestampMillis,
		"timestamp-micros":       deserTimestampMicros,
		"local-timestamp-micros": deserTimestampMicros,
		"timestamp-nanos":        deserTimestampNanos,
		"local-timestamp-nanos":  deserTimestampNanos,
		"date":                   deserDate,
		"time-millis":            deserTimeMillis,
		"time-micros":            deserTimeMicros,
		"uuid":                   deserUUID,
	}
)

func logicalSer(logical string) serfn     { return logicalSers[logical] }
func logicalDeser(logical string) deserfn { return logicalDesers[logical] }

// unmarshalDefault parses a field's raw JSON default. Numeric literals stay
// json.Number rather than rounding through float64, which is what the shared
// decoder returns for every number anyway: long defaults above 2^53 would
// otherwise silently lose precision (9007199254740993 becomes ...92).
func unmarshalDefault(raw json.RawMessage) any {
	// Cannot fail: raw is preserved from the initial parse and is valid JSON.
	// Lenient rather than strict for the same reason: whatever the parse
	// accepted into these bytes is what comes back out.
	dv, _, _ := decodeSchemaAny(string(raw))
	return dv
}

// nodeAwaitsForwardRef reports whether node has any child encodeDefault would
// traverse that is not yet whole: a nil child, an unwired forward reference
// whose dereference would panic, or a partial record still in b.building,
// whose fields slice holds only the fields declared so far. True means the
// caller defers the default pipeline to finalize. A seen set makes the walk
// cycle-safe.
func (b *builder) nodeAwaitsForwardRef(node *schemaNode) bool {
	return nodeAwaitsForwardRefSeen(node, b.building, map[*schemaNode]struct{}{})
}

func nodeAwaitsForwardRefSeen(node *schemaNode, building, seen map[*schemaNode]struct{}) bool {
	if node == nil {
		return true
	}
	if _, ok := seen[node]; ok {
		return false
	}
	seen[node] = struct{}{}
	switch node.kind {
	case "array":
		return nodeAwaitsForwardRefSeen(node.items, building, seen)
	case "map":
		return nodeAwaitsForwardRefSeen(node.values, building, seen)
	case "record", "error":
		if _, ok := building[node]; ok {
			return true
		}
		for i := range node.fields {
			if nodeAwaitsForwardRefSeen(node.fields[i].node, building, seen) {
				return true
			}
		}
	case "union":
		for _, b := range node.branches {
			if nodeAwaitsForwardRefSeen(b, building, seen) {
				return true
			}
		}
	}
	return false
}

// resolveFieldDefaultValue runs the validate and convertDefaultBytes half of
// the default pipeline against a coerced default value and its resolved
// node, recording the converted default on the field metadata. It does not
// encode the binary defaultBytes; encodeFieldDefaultBytes must run only after
// every field's default value is recorded, since encodeDefault fills absent
// nested record fields from their defaultVal. convertDefaultBytes maps
// bytes/fixed string defaults to []byte so the JSON encoder's logical arms
// cannot misinterpret the string.
func resolveFieldDefaultValue(defaultVal any, node *schemaNode, fieldName string,
	drf *deserRecordField, fn *fieldNode,
) (any, error) {
	if err := validateDefault(defaultVal, node); err != nil {
		return nil, fmt.Errorf("record field %q: invalid default: %v", truncForError(fieldName), err)
	}
	defaultVal = convertDefaultBytes(defaultVal, node)
	drf.defaultVal = defaultVal
	drf.hasDefault = true
	fn.defaultVal = defaultVal
	fn.hasDefault = true
	return defaultVal, nil
}

// encodeFieldDefaultBytes encodes the (already-resolved) default value into
// the field's pre-encoded binary defaultBytes. Split from
// resolveFieldDefaultValue so deferred defaults can resolve every field's
// value first (encodeDefault reads sibling/nested f.defaultVal for absent
// fields).
func encodeFieldDefaultBytes(defaultVal any, node *schemaNode, fieldName string, srf *serRecordField) error {
	defaultBytes, deferred, err := encodeDefaultCharged(defaultVal, node)
	if err != nil {
		return fmt.Errorf("record field %q: encoding default: %v", truncForError(fieldName), err)
	}
	srf.defaultBytes = defaultBytes
	srf.hasDefault = true
	// Recorded, not returned: a default that cannot be *written* must not stop
	// the schema parsing, because a reader that drops this field never writes
	// it and reads such data correctly today. The encode-side consumers of
	// defaultBytes report this at the moment the default would reach the wire.
	// The verdict comes from inside the walk, where each leaf asked the same
	// predicate its serializer asks; asking here instead would answer for the
	// field's kind and miss every cap nested inside a container.
	srf.defaultErr = deferred
	return nil
}

// applyResolvedDefault runs the full validate, convert and encode pipeline
// for a coerced default value against its resolved node. The build-time path
// uses it for fields with no pending forward reference; the rest defer to
// finalize through the split pair above.
func applyResolvedDefault(defaultVal any, node *schemaNode, fieldName string,
	drf *deserRecordField, fn *fieldNode, srf *serRecordField,
) error {
	converted, err := resolveFieldDefaultValue(defaultVal, node, fieldName, drf, fn)
	if err != nil {
		return err
	}
	return encodeFieldDefaultBytes(converted, node, fieldName, srf)
}

// unmarshalAnyPreservePrecision parses raw JSON into the same shape as
// encoding/json's any decode, except integer-valued numbers become int64
// rather than float64, and integers past int64 stay json.Number. The
// metadata paths use it so a JSON int above 2^53 is not silently
// rounded.
func unmarshalAnyPreservePrecision(raw string) (any, error) {
	v, err := decodeSchemaAnyStrict(raw)
	if err != nil {
		return nil, err
	}
	return normalizeJSONValue(v), nil
}

func normalizeJSONValue(v any) any {
	switch tv := v.(type) {
	case json.Number:
		return normalizeJSONNumber(tv)
	case map[string]any:
		for k, val := range tv {
			tv[k] = normalizeJSONValue(val)
		}
		return tv
	case []any:
		for i, val := range tv {
			tv[i] = normalizeJSONValue(val)
		}
		return tv
	}
	return v
}

// normalizeJSONNumber resolves a json.Number to the idiomatic Go type by
// value, not by literal syntax: an exact integer fitting int64 becomes
// int64 whatever its spelling, an integer past int64 stays json.Number, and
// anything else becomes float64, with +/-Inf past float64's range. Going by
// value keeps the metadata value equal to the wire value at the int64
// boundary.
func normalizeJSONNumber(n json.Number) any {
	s := string(n)
	// Integer-syntax fast path: no decimal point, no exponent, so strconv
	// alone is enough, no need to spin up a big.Rat.
	if !strings.ContainsAny(s, ".eE") {
		if i, err := n.Int64(); err == nil {
			return i
		}
		// Overflows int64; preserve as json.Number for arbitrary precision.
		return n
	}
	// Fractional or exponent syntax: parse with arbitrary precision and check
	// whether the value is an exact integer, so "1.5e1" is int64(15) here as
	// on the wire.
	if r, ok, err := boundedRatFromString(s); err == nil && ok && r.IsInt() {
		// Negative zero in float syntax ("-0.0", "-0e5") is the one exact
		// integer whose IEEE sign the int64 collapse would erase (a big.Rat
		// has no signed zero). The wire encoder parses it via ParseFloat and
		// preserves the sign, and Java's Jackson produces a DoubleNode(-0.0);
		// keep the sign by falling through to parseFloatAcceptOverflow below
		// (giving -0.0) so the metadata Default matches the wire and re-parses
		// sign-stable. Integer syntax ("-0") stays integer 0 (no sign) above.
		negZero := r.Sign() == 0 && s != "" && s[0] == '-'
		if !negZero {
			if bi := r.Num(); bi.IsInt64() {
				return bi.Int64()
			}
		}
		// Exact integer beyond int64: return float64 when the magnitude
		// fits float64's exponent, +/-Inf when it does not. Both match what an
		// encode against a float/double schema puts on the wire.
	}
	if f, err := parseFloatAcceptOverflow(s, 64); err == nil {
		return f
	}
	return n
}

// numericDefault extracts a typed integer default, the shared body of
// defaultAsInt32 and defaultAsInt64. We accept whole numbers written in
// fractional or exponent form ("1.0", "4e1"), which Java and fastavro
// reject, matching what the runtime encoders already accept. The precision
// guard rejects the literals where the metadata path, which goes through
// float64, would round to a different integer than the exact wire fill, so a
// schema never carries a default whose metadata and wire values disagree.
func numericDefault[T int32 | int64](val any, parse func(string) (T, error), fromFloat func(float64) (T, error), fromInt64 func(int64) (T, error)) (T, error) {
	switch v := val.(type) {
	case json.Number:
		return parse(string(v))
	case float64:
		return fromFloat(v)
	case int64:
		return fromInt64(v)
	case int32:
		return fromInt64(int64(v))
	}
	var z T
	return z, fmt.Errorf("expected number, got %T", val)
}

func int64FitsInt32(n int64) (int32, error) {
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0, fmt.Errorf("integer %d overflows int32", n)
	}
	return int32(n), nil
}

func int64Identity(n int64) (int64, error) { return n, nil }

func defaultAsInt32(val any) (int32, error) {
	return numericDefault(val, parseInt32Lenient, floatFitsInt32, int64FitsInt32)
}

func defaultAsInt64(val any) (int64, error) {
	return numericDefault(val, parseInt64Lenient, floatFitsInt64, int64Identity)
}

// floatMantissaLimit returns the largest integer magnitude exactly
// representable in float32 (bitSize=32) or float64 (bitSize=64): the
// mantissa bound used for the float-to-int whole-number precision-loss
// checks at [floatFitsInt32From] and [floatFitsInt64From]. The reverse
// direction, int to float, is lossy by destination per Java/fastavro
// parity; see [appendAvroFloat32] / [appendAvroFloat64].
func floatMantissaLimit(bitSize int) int64 {
	if bitSize == 32 {
		return 1 << 24
	}
	return 1 << 53
}

// intFitsFloat reports whether an int64 value of magnitude n can be
// represented exactly in the target float (float32 or float64). Used
// by decode-time arms that write a long-wire value into a Go float
// target: you explicitly chose a smaller-precision Go type, so we
// report the precision loss rather than round. Encode-time
// arms use the lossy-destination policy and silently round; see
// [appendAvroFloat32] / [appendAvroFloat64].
func intFitsFloat(n int64, bitSize int) (float64, error) {
	lim := floatMantissaLimit(bitSize)
	if n < -lim || n > lim {
		return 0, fmt.Errorf("integer %d overflows float%d exact precision", n, bitSize)
	}
	return float64(n), nil
}

// parseFloatAcceptOverflow is strconv.ParseFloat with ErrRange on +/-Inf
// counting as success, since Java and fastavro return the Inf and the wire
// permits it. Every ParseFloat on user input routes through it, so the
// length cap covers them all: a legitimate float64 literal fits well under
// 350 chars, and ParseFloat is linear in the input. bitSize is 32 only for
// the JSON decode of a "float" schema, avoiding a double rounding.
func parseFloatAcceptOverflow(s string, bitSize int) (float64, error) {
	if len(s) > maxParseFloatLen {
		return 0, fmt.Errorf("float literal exceeds %d byte length cap", maxParseFloatLen)
	}
	f, err := strconv.ParseFloat(s, bitSize)
	if err == nil {
		return f, nil
	}
	if errors.Is(err, strconv.ErrRange) && math.IsInf(f, 0) {
		return f, nil
	}
	return 0, err
}

// maxParseFloatLen caps what parseFloatAcceptOverflow forwards to
// [strconv.ParseFloat]; see there for why. The longest legitimate float64
// literal fits in ~320 chars, so this leaves comfortable headroom.
const maxParseFloatLen = 1024

// defaultAsFloat extracts a numeric default for a float or double field. It
// accepts json.Number, float64, int64 and int32 but not a string: the spec
// requires a JSON number, and Java's text-to-double carveout for outer
// float/double fields is applied upstream in coerceDefault. Integers past
// the mantissa IEEE-round, as in Java and fastavro.
func defaultAsFloat(val any) (float64, error) {
	switch v := val.(type) {
	case json.Number:
		return parseJSONNumberAsFloat(v.String(), 64)
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case float32:
		// A float32 arrives only from the metadata side, where
		// coerceMetadataDefault narrows a "float" schema; widening is exact.
		return float64(v), nil
	}
	return 0, fmt.Errorf("expected number, got %T", val)
}

// firstUnionBranchAcceptingDefault returns the first union branch whose
// validateDefault accepts val, or nil. coerceDefault and walkDefault share it
// so both implement the "first matching branch" rule the same way. A string
// default matches only branches whose permitted JSON type is string (string,
// bytes, enum, fixed); Java's text-to-double coercion fires only for an
// outer float/double field, never a union branch.
func firstUnionBranchAcceptingDefault(val any, node *schemaNode) *schemaNode {
	for _, branch := range node.branches {
		// validateDefault coerces in place, so we validate a copy: a failed
		// branch's partial coercion must not leak into the next branch's
		// check, or acceptance becomes order-dependent. The caller re-coerces
		// the original val against the returned branch.
		if validateDefault(deepCopyTree(val), branch) == nil {
			return branch
		}
	}
	return nil
}

// coerceDefault converts a string default to float64 when the field type is
// literally float or double, an interop carveout for legacy Java-generated
// schemas modeled on Java's parseField; avro-rs and goavro do not implement
// it. A direct scalar float/double union branch does not coerce, so
// ["double"] default "5" rejects and ["double","string"] picks string, as
// Java does. A float/double field nested in a container does coerce, which
// is leniency beyond Java; the metadata selector applies the same coercion,
// so Root().Default reports the branch the wire fills. Walks the resolved
// node tree so name-referenced nested fields coerce too.
func coerceDefault(val any, node *schemaNode) any {
	if node == nil {
		return val
	}
	if node.kind == "union" {
		// First validateDefault-accepting branch wins; recurse so the coerced
		// value matches that branch's natural Go type. No numeric branch
		// accepts a string (defaultAsFloat has no string arm), so a string
		// default picks string/bytes/enum/fixed or nothing, and parse then
		// fails via validateDefault.
		if branch := firstUnionBranchAcceptingDefault(val, node); branch != nil {
			return coerceDefault(val, branch)
		}
		return val
	}
	if node.kind != "float" && node.kind != "double" {
		return val
	}
	s, ok := val.(string)
	if !ok {
		return val
	}
	// parseFloatAcceptOverflow directly, not defaultAsFloat: that one is the
	// strict validator union branches and the encode arms share, and this
	// leniency is scoped to this call site alone. On a parse failure keep the
	// original string so validateDefault produces the canonical error.
	if f, err := parseFloatAcceptOverflow(s, 64); err == nil {
		return f
	}
	return val
}

// walkDefault drives the (val, node) recursion shared by the default-tree
// walkers. visit is called once per non-union node and may mutate val; at a
// union, walkDefault picks the first validateDefault-accepting branch and
// recurses into it, returning the "does not match any union branch" error
// when none accepts. Container arms wrap nested errors with the field, index
// or key so every walker reports the same path. visit must be idempotent,
// since the union arm re-invokes it at every node of the matched branch.
// A nil node returns immediately, so forward-ref-deferred validation is a
// no-op.
func walkDefault(val any, node *schemaNode, visit func(any, *schemaNode) (any, error)) (any, error) {
	if node == nil {
		return val, nil
	}
	if node.kind == "union" {
		// Per Avro 1.12 a default may match any branch, not only the first
		// (AVRO-3649). The matcher is validateDefault, shared with
		// coerceDefault. A structural-only check like "is val a string?"
		// would pick a fixed:N branch for a string whose rune count does not
		// fit, mutate it into a length-N []byte no branch can encode, and
		// fail at encodeDefault on a schema validateDefault accepted.
		// Re-running validateDefault here is safe: it is idempotent.
		if branch := firstUnionBranchAcceptingDefault(val, node); branch != nil {
			return walkDefault(val, branch, visit)
		}
		return val, fmt.Errorf("default does not match any union branch: %T(%s)", val, truncValueForError(val))
	}
	val, err := visit(val, node)
	if err != nil {
		return val, err
	}
	switch node.kind {
	case "record":
		if m, ok := val.(map[string]any); ok {
			for _, f := range node.fields {
				fv, exists := m[f.name]
				if !exists {
					continue
				}
				fv2, err := walkDefault(fv, f.node, visit)
				if err != nil {
					return val, fmt.Errorf("field %q: %w", truncForError(f.name), err)
				}
				m[f.name] = fv2
			}
		}
	case "array":
		if arr, ok := val.([]any); ok && node.items != nil {
			for i, item := range arr {
				item2, err := walkDefault(item, node.items, visit)
				if err != nil {
					return val, fmt.Errorf("array element %d: %w", i, err)
				}
				arr[i] = item2
			}
		}
	case "map":
		if m, ok := val.(map[string]any); ok && node.values != nil {
			for k, v := range m {
				v2, err := walkDefault(v, node.values, visit)
				if err != nil {
					return val, fmt.Errorf("map key %q: %w", truncForError(k), err)
				}
				m[k] = v2
			}
		}
	}
	return val, nil
}

// convertDefaultBytes walks a validated default and converts string defaults
// to []byte for bytes/fixed nodes, so the JSON encoder's logical-type arms
// cannot misinterpret the codepoint string and both encode paths agree. For
// a forward-ref field validation is deferred, so the conversion is
// best-effort and the union-no-match error is discarded.
func convertDefaultBytes(val any, node *schemaNode) any {
	out, _ := walkDefault(val, node, func(val any, node *schemaNode) (any, error) {
		if node.kind != "bytes" && node.kind != "fixed" {
			return val, nil
		}
		if str, ok := val.(string); ok {
			if b, err := avroJSONBytesToBytes(str); err == nil {
				return b, nil
			}
		}
		return val, nil
	})
	return out
}

// validateAvroByteString reports an error when s contains a code point
// > 0xFF: the Avro JSON-bytes / JSON-fixed default form maps each
// codepoint to one byte, so values outside that range are not
// representable. fieldType is "bytes" or "fixed" for the message.
func validateAvroByteString(s, fieldType string) error {
	for _, r := range s {
		if r > 255 {
			return fmt.Errorf("%s default contains code point U+%04X, max allowed is U+00FF", fieldType, r)
		}
	}
	return nil
}

// validateDefault checks a parsed JSON default against the schema, driving
// walkDefault with the validateLeaf visit. It coerces record, array and map
// structures in place. A nil node returns nil; forward refs defer to
// finalize.
func validateDefault(val any, node *schemaNode) error {
	_, err := walkDefault(val, node, validateLeaf)
	return err
}

// defaultObjectShape asserts val is a non-null JSON object for an Avro record
// or map default, returning the canonical "expected object for <kind> default"
// error. Shared by the parse-time validator (validateLeaf) and the wire
// encoder (encodeDefault, resolve.go), two cross-path sites for one shape
// rule, so the error you see cannot drift between them. Only the shape
// assertion is shared; each caller keeps its own post-assertion work
// (validateLeaf coerces fields in place, encodeDefault emits wire bytes).
func defaultObjectShape(val any, kind string) (map[string]any, error) {
	if val == nil {
		return nil, fmt.Errorf("expected object for %s default, got null", kind)
	}
	m, ok := val.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected object for %s default, got %T", kind, val)
	}
	return m, nil
}

// defaultArrayShape is defaultObjectShape's array counterpart.
func defaultArrayShape(val any) ([]any, error) {
	if val == nil {
		return nil, fmt.Errorf("expected array for array default, got null")
	}
	arr, ok := val.([]any)
	if !ok {
		return nil, fmt.Errorf("expected array for array default, got %T", val)
	}
	return arr, nil
}

// validateLeaf is the per-node visit for validateDefault: primitive
// kind validation, plus container-shape checks + per-field coercion
// (walkDefault handles the actual recursion).
func validateLeaf(val any, node *schemaNode) (any, error) {
	switch node.kind {
	case "null":
		if val != nil {
			return val, fmt.Errorf("expected null, got %T", val)
		}
	case "boolean":
		if _, ok := val.(bool); !ok {
			return val, fmt.Errorf("expected boolean, got %T", val)
		}
	case "int":
		if _, err := defaultAsInt32(val); err != nil {
			return val, fmt.Errorf("int default: %w", err)
		}
	case "long":
		if _, err := defaultAsInt64(val); err != nil {
			return val, fmt.Errorf("long default: %w", err)
		}
	case "float", "double":
		if _, err := defaultAsFloat(val); err != nil {
			return val, fmt.Errorf("%s default: %w", node.kind, err)
		}
	case "string":
		if _, ok := val.(string); !ok {
			return val, fmt.Errorf("expected string, got %T", val)
		}
	case "bytes":
		s, ok := val.(string)
		if !ok {
			return val, fmt.Errorf("expected string for bytes, got %T", val)
		}
		return val, validateAvroByteString(s, "bytes")
	case "enum":
		sym, ok := val.(string)
		if !ok {
			return val, fmt.Errorf("expected string for enum default, got %T", val)
		}
		// A non-nil enum node always carries its final symbols, so an empty
		// enum rejects every default. Java and fastavro both accept a
		// non-member field default; we fail at parse because it can never
		// encode, and so a union-default selection can skip to a later
		// branch.
		if !slices.Contains(node.symbols, sym) {
			return val, fmt.Errorf("enum default %q is not a member of symbols", truncForError(sym))
		}
	case "fixed":
		s, ok := val.(string)
		if !ok {
			return val, fmt.Errorf("expected string for fixed default, got %T", val)
		}
		if err := validateAvroByteString(s, "fixed"); err != nil {
			return val, err
		}
		if len([]rune(s)) != node.size {
			return val, fmt.Errorf("fixed default length %d does not match size %d", len([]rune(s)), node.size)
		}
	case "record":
		// null is not a record (Java/fastavro/hamba all reject). Without
		// this, ["Record","null"] with default null would match the
		// Record branch, synthesizing an empty map and relying on
		// per-field defaults, instead of falling through to null.
		// encodeDefault would then emit Record(field-defaults) wire
		// bytes where null was intended.
		m, err := defaultObjectShape(val, "record")
		if err != nil {
			return val, err
		}
		// Required-field presence check before coercion: a missing
		// no-default field is an error regardless of per-field types.
		for _, f := range node.fields {
			if _, exists := m[f.name]; !exists && !f.hasDefault {
				return val, fmt.Errorf("record default missing field %q with no default", truncForError(f.name))
			}
		}
		// Coerce each present field in-place; walkDefault then recurses
		// to validate the coerced value at each child node.
		for _, f := range node.fields {
			if fv, exists := m[f.name]; exists {
				m[f.name] = coerceDefault(fv, f.node)
			}
		}
	case "array":
		arr, err := defaultArrayShape(val)
		if err != nil {
			return val, err
		}
		for i, item := range arr {
			arr[i] = coerceDefault(item, node.items)
		}
	case "map":
		m, err := defaultObjectShape(val, "map")
		if err != nil {
			return val, err
		}
		for k, v := range m {
			m[k] = coerceDefault(v, node.values)
		}
	}
	return val, nil
}
