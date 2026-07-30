package avro

import (
	"bytes"
	"encoding/binary"
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
)

// Schema is a compiled Avro schema. Create one with [Parse] or [MustParse],
// then use [Schema.Encode] / [Schema.Decode] to convert between Go values and
// Avro binary. A Schema is safe for concurrent use.
//
// A nil *Schema is invalid; every method panics on it. Obtain a *Schema only
// from [Parse], [MustParse], [Resolve], [SchemaFor], or [SchemaNode.Schema] —
// each returns a non-nil *Schema or an error, so a nil *Schema is a
// programming error and is surfaced as a panic rather than a returned error.
type Schema struct {
	ser   serfn
	deser deserfn

	c    aschema     // canonical form, used for fingerprinting and schema comparison
	soe  [10]byte    // Single Object Encoding header: 2-byte magic (0xC3, 0x01) + 8-byte LE CRC64-Avro fingerprint
	node *schemaNode // full metadata tree (aliases, defaults, etc.) for schema introspection and evolution
	full string      // original schema JSON, returned by String()

	// writerSoe is the writer schema's SOE header — populated only by
	// Resolve(writer, reader) and consulted by DecodeSingleObject so a
	// resolved schema can decode wire bytes bearing the writer's
	// fingerprint (the wire fingerprint identifies the schema that
	// produced the bytes, which is the writer when a resolution is
	// involved). Zero value (writerSoe[0] == 0x00) means "not a resolved
	// schema; accept only s.soe."
	writerSoe [10]byte

	// resolveWriter is the writer schema, populated only by
	// Resolve(writer, reader) when the writer and reader differ (an identity
	// resolution returns the reader schema directly, leaving this nil). It
	// lets DecodeJSON apply writer→reader resolution to WRITER-shaped JSON,
	// matching Java's ResolvingDecoder-over-JsonDecoder (the JsonDecoder is
	// constructed with the writer schema). Binary Decode resolves via s.deser;
	// JSON resolution composes the writer's JSON decode + binary re-encode with
	// that same resolving s.deser. nil ⇒ not resolved; DecodeJSON decodes
	// directly against s.node.
	resolveWriter *Schema

	// resolveWriterRaw is a custom-free view of the writer, used solely by
	// decodeJSONResolved for the JSON wire-shape round-trip (writer-JSON ->
	// writer-binary). That intermediate must hold RAW Avro-native values: if the
	// writer carries its own CustomType decoders, decoding writer-JSON through
	// resolveWriter would run them (producing Go-domain values) and the re-encode
	// would then need the writer's custom ENCODER to invert them -- which fails
	// for a Decode-only custom, or any custom whose Encode cannot reproduce the
	// decoded type. Binary Decode never re-encodes through the writer (it reads
	// raw writer bytes and applies the READER's custom), so it is unaffected;
	// this custom-free view makes the JSON path match it. Equal to resolveWriter
	// when the writer has no custom types (nothing to suppress); set alongside
	// resolveWriter by Resolve.
	resolveWriterRaw *Schema

	// Per-schema custom type overlay. Keyed by *schemaNode so the
	// shared node is not mutated — different schemas parsed with
	// different custom types get different overlays.
	custom map[*schemaNode]*customWiring

	// customBaked reports custom-conversion effects reachable through this
	// schema's node tree even when the custom overlay above is empty: a
	// reference to a SchemaCache-inherited named type whose DEFINING parse
	// wired custom types carries the binary callback wraps composed inside
	// the inherited ser/deser and the JSON wrap baked on the shared
	// node.decodeJSON, but no entry in THIS parse's overlay
	// (applyCustomTypes visits only newly built nodes). Resolve's
	// custom-free writer view must be built whenever either signal is set;
	// keying on len(custom) alone resurrected the decode-only re-encode
	// failure (3333e9b) for cache-parsed custom-typed writers.
	customBaked bool

	// slabFree marks a schema whose compiled deser provably never touches
	// the per-call *slab: a scalar leaf kind (see slabFreeKinds) with no
	// custom-decoder wiring anywhere (customBaked covers both this parse's
	// overlay and cache-inherited wraps). Decode skips the slab pool for
	// such schemas and passes a nil slab, so scalar decodes stay
	// allocation-free even when GC has drained the pool (issue #41). The
	// zero value false is the safe default: a construction path that does
	// not classify (Resolve's non-identity path, whose promote/skip
	// routines do use the slab) keeps the pool.
	slabFree bool
}

// customWiring bundles the per-node custom-type artifacts. Allocated
// once per node that matches at least one registered CustomType; the
// three slots are independently populated based on which callbacks
// the user provided.
type customWiring struct {
	// encode wraps the user's CustomType.Encode chain. Runs before
	// the built-in serializer. nil if no encoders matched, or if
	// every matching CustomType had Encode == nil.
	encode func(reflect.Value) (reflect.Value, error)
	// decoders is the CustomType.Decode callback chain. Run after the
	// built-in deserializer produces the raw Avro-native value. nil
	// if no decoders matched.
	decoders []func(any, *SchemaNode) (any, error)
	// sn is the public *SchemaNode passed to the encode and decoder
	// callbacks. Built once at parse time and reused across calls.
	// Always populated when the wiring is non-nil.
	sn *SchemaNode
	// suppressLogical mirrors the binary decoder-suppression decision
	// (hasMatchingCustomType) so the JSON decode wrapper feeds the custom
	// decoder the same raw-vs-enriched value the binary path does. False
	// for wildcard CustomTypes (empty LogicalType AND AvroType), which the
	// binary gate excludes — they must receive the enriched logical value.
	// Carried here so resolved nodes (resolve.go) reuse the parse-time
	// decision without re-running the gate.
	suppressLogical bool
	// encodeSuppresses mirrors the binary ENCODER-suppression decision
	// (hasMatchingCustomTypeWithEncode). The JSON encode arms (decimal /
	// big-decimal on bytes; all logicals on fixed) must skip the built-in
	// logical coercion arm iff the binary build replaced the logical
	// serializer with the base (raw-bytes) serializer — which is iff a
	// non-wildcard matching CustomType has an Encode. Gating the JSON arms
	// on the runtime proxy `custom[node].encode != nil` instead would
	// wrongly skip the arm for a WILDCARD-with-Encode (the binary keeps the
	// logical ser for wildcards), rejecting *big.Rat on JSON while binary
	// accepts it. Use this exact predicate, not the proxy.
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
	fieldIdx map[string]int // record field name → index; built at parse time

	// unknownLogical preserves the schema's original logicalType value
	// when it failed validateLogical (no built-in handler matched AND
	// no registered CustomType matched at this Parse). The runtime
	// ignores it — built-in handlers were already chosen based on the
	// validated (possibly cleared) logical, and runtime dispatches use
	// `logical`. unknownLogical is consulted ONLY by the cache-
	// reference rejection check (rejectCachedRefIfCustomTypeWouldMatch)
	// so a later Parse that registers a CT for this logical can
	// detect the silent-drop scenario and error loudly.
	unknownLogical string
}

// jsonDecodeFn is the per-node JSON dispatch shape used when custom
// decoders are wired (mirrors deserfn for the binary path). nil node
// means the runtime falls back to kind dispatch.
type jsonDecodeFn func(*jsonDecoder, reflect.Value, *schemaNode) error

// fieldNode represents a record field with full metadata.
type fieldNode struct {
	name    string
	nameVal reflect.Value // pre-computed for map lookups without allocation
	// aliases: schema-evolution alternate field names. Consumers are
	// all decode/resolve side — JSON decode (via node.fieldIdx, which
	// is built from this slice at parse time), CheckCompatibility's
	// findWriterField (compat.go), and Resolve's findReaderFieldIndex
	// (resolve.go). Not consulted on encode — aliases are a reader-
	// side concept per the Avro 1.12 spec.
	aliases    []string
	node       *schemaNode
	defaultVal any
	hasDefault bool
}

type parseOptLax struct{ fn func(string) error }

func (parseOptLax) schemaOpt() {}

// WithLaxNames relaxes name validation in [Parse] and [SchemaCache.Parse],
// overriding the default requirement that names match the Avro strict name
// regex [A-Za-z_][A-Za-z0-9_]*. If fn is nil, only non-empty names are
// required. If fn is non-nil, it is called for each name component and
// should return an error for invalid names. Dot-separated fullnames are
// split before calling fn. Ignored by [SchemaFor].
func WithLaxNames(fn func(string) error) SchemaOpt { return parseOptLax{fn} }

// internalReparseNames is the name validator for the library's internal
// re-parses of schema text it produced itself: Resolve's custom-free writer
// view (resolve.go) and SchemaCache's self-contained splice rebuild
// (cache.go). Both sites re-parse text whose names the ORIGINAL parse
// already validated under the user's chosen validator (strict, or any
// WithLaxNames fn), so validation here has no safety role — it can only
// wrongly reject names the user accepted. It therefore accepts everything:
// WithLaxNames(nil), the previous validator at both sites, rejected empty
// name components (namespace "a..b") — the one class a user fn can accept
// that lax(nil) does not — hard-failing Resolve on an already-parsed,
// wire-valid writer and silently degrading the cache's metadata forms to a
// dangling reference. Validation never transforms names — they pass
// through verbatim — so the canonical/fingerprint/wire bytes match a
// standalone parse of the same schema under the user's validator.
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
// may self-reference. The schema is fully validated: unknown types, duplicate
// names, invalid defaults, etc. all return errors.
//
// To parse schemas that reference named types from other schemas, use
// [SchemaCache].
func Parse(schema string, opts ...SchemaOpt) (*Schema, error) {
	b := &builder{
		named:      make(map[string]*namedType),
		building:   make(map[*schemaNode]struct{}),
		definedSet: make(map[*namedType]bool),
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
	// Allocated here — before any build work and before any nest(), which
	// shares them by reference — so the whole parse sees one memo.
	// b.customTypes is never appended to after this point; that is the
	// memo's correctness invariant (see customMatchInSubtree).
	if len(b.customTypes) > 0 {
		b.customMatch = make(map[*schemaNode]string)
		b.overlayDone = make(map[*schemaNode]bool)
	}
}

func parse(schema string, b *builder) (*Schema, error) {
	// Bound nesting depth with a single linear scan BEFORE building. The
	// build's maxDepth guard fires per schema node, but the JSON bracket
	// nesting can run deeper than the node depth (and json.Decode has its
	// own ~10000 limit); this O(input) pre-scan rejects pathologically
	// deep input up front. (Parse itself is O(n) via parseSchemaTree — a
	// single generic decode, no per-node subtree re-scan.)
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
	s.soe[0] = 0xC3
	s.soe[1] = 0x01
	h := NewRabin()
	h.Write(s.Canonical())
	binary.LittleEndian.PutUint64(s.soe[2:], h.Sum64())
	return s, nil
}

// maxSchemaJSONDepth bounds the raw JSON bracket nesting of a schema string.
// It is a coarse DoS backstop, NOT the semantic depth limit: the build's
// maxDepth caps SCHEMA-node nesting, and one schema level can carry up to
// three JSON brackets (a record's object + its "fields" array + a field
// object), so a build-acceptable schema (<= maxDepth nodes) reaches a JSON
// bracket depth of at most 3*maxDepth. The 4*maxDepth limit clears that
// provable ceiling by a full maxDepth, so the pre-scan never rejects a schema
// the builder would accept; it short-circuits input deeper than the builder
// accepts (and deeper than json's own ~10000 nesting cap) in one O(input)
// pass. Parse itself is O(n) — see parseSchemaTree and canonicalBytes — so
// this is a cheap early-reject, no longer the load-bearing DoS defense it was
// when the unmarshal and canonical marshal were quadratic.
const maxSchemaJSONDepth = maxDepth * 4

// checkSchemaNestingDepth reports an error if schema's JSON nests deeper than
// maxSchemaJSONDepth. It is a single linear pass that counts '{'/'[' nesting,
// skipping brackets inside JSON strings (honoring backslash escapes), so it
// runs in O(len(schema)) and constant space — cheap enough to gate every parse.
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

// Canonical returns the Parsing Canonical Form of the schema, stripping
// doc, aliases, defaults, and other non-essential attributes. The result
// is deterministic and matches Java's reference output byte-for-byte,
// so [Schema.Fingerprint] values are interoperable across implementations.
// canonicalFirstOccurrence rewrites the parse-time canon tree so each
// named type's full definition is emitted at its FIRST occurrence in the
// field-walk order, with bare (full)name references afterward — the rule
// Apache Avro's SchemaNormalization (the Parsing Canonical Form reference,
// SchemaNormalization.java's env-keyed build) applies. The parse-time
// canon tree instead places the full body at the textual DEFINITION site
// and a bare name at every reference. The two agree for the common
// define-before-reference shape (the definition IS the first occurrence),
// so output is byte-identical there; they diverge only when a reference
// precedes its definition (a forward reference, which twmb accepts): there
// the un-transformed tree emitted the full body at the SECOND occurrence,
// producing a PCF — and thus a schema fingerprint — that differed from
// Java for single-object-encoding / schema-registry interop.
//
// References are also normalized to the resolved fullname (bare forward
// references store the as-written short name), matching PCF's fullname
// rule and keeping the emission consistent across orderings.
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

// collectCanonDefs records every named-type definition (the aobject whose
// body is present) in the canon tree, keyed by fullname, plus an
// unqualified-name index for resolving bare forward references into a
// namespaced scope.
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

// lookupCanonDef resolves a reference name to its definition aobject, or
// nil when ref is a real Avro primitive (never a named ref) or names a
// type defined outside this schema (a SchemaCache cross-reference — left
// as a bare name, the pre-existing behavior). A bare reference resolves
// lexically in the enclosing namespace ns (mirroring parse-time
// resolveNamedRef), so a short name shared across namespaces still resolves
// to its in-scope fullname rather than being emitted verbatim.
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

// rewriteCanonObj shallow-copies o (preserving every scalar/canonical
// attribute) and rebuilds only its recursive schema children
// (Fields[].Type / Items / Values) through rewriteCanonFirstOcc, so
// per-object PCF emission is unchanged and only the full-vs-reference
// placement of nested named types moves to first occurrence.
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

func (s *Schema) Canonical() []byte {
	// Single-pass writer emitting raw UTF-8 strings per the PCF [STRINGS]
	// rule (see canonicalBytes). O(n) over the schema, vs the former
	// nested-MarshalJSON path that re-copied each subtree at every level
	// (O(n^2)); and it never produces the HTML / U+2028 / U+2029 escapes
	// the former path had to un-escape with bytes.ReplaceAll — which
	// corrupted any name containing a literal backslash.
	return canonicalBytes(canonicalFirstOccurrence(s.c))
}

// Fingerprint hashes the schema's canonical form with h. Use [NewRabin] for
// CRC-64-AVRO or crypto/sha256 for cross-language compatibility.
//
// The result is big-endian per [hash.Hash.Sum]. Single Object Encoding uses
// little-endian fingerprints; use [Schema.DecodeSingleObject] or
// [SingleObjectFingerprint] for that format.
func (s *Schema) Fingerprint(h hash.Hash) []byte {
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

// isNullBranch reports whether s is the "null" type, in EITHER spelling the
// grammar admits: the bare `"null"` primitive or the wrapped object form
// `{"type":"null"}`. The two are the same type — they select the same union
// branch and encode to the same bytes — so every decision made by matching a
// branch's written spelling routes through here, and none of them may see
// only the bare form.
//
// Props and a logicalType on a wrapped null are inert metadata and do NOT
// make it a non-null branch: Avro defines no null logical type, so nothing
// downstream can consume either key, and the branch's type and wire form are
// unchanged. Deciding it the other way would make `[{"type":"null","x":1},T]`
// a two-non-null-branch union whose first branch encodes zero bytes — a shape
// no other reader agrees with.
//
// The compiled tree answers the same question as schemaNode.kind, which
// normalizes both spellings; this is the as-written (pre-build) view of that
// same fact, so the two cannot disagree.
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
// (schema_parse.go) from a single generic decode — they are deliberately
// NOT json.Marshaler / json.Unmarshaler, so the stdlib decoder does not
// re-scan each nested node's subtree (which made Parse O(depth*size)).
// The canonical form is written by canonicalBytes (schema_canonical.go).

type afield struct {
	Name string   `json:"name"`
	Type *aschema `json:"type"`

	// In canonical form, the following are stripped.

	Aliases []string        `json:"aliases,omitempty"`
	Default json.RawMessage `json:"default,omitempty"`
	Order   string          `json:"order,omitempty"`

	// orderSet records that "order" was written, which Order alone cannot:
	// the empty string is its zero, so a validator reading `Order != ""` as
	// "the caller chose an order" skips exactly the one written value that
	// is not a legal order. Apache Avro has no such gap because it decides
	// on the NODE — `if (orderNode != null) Order.valueOf(...)`
	// (Schema.java:1895-1897) — where an empty string reaches valueOf and
	// throws. This restores that: presence and validity are one question.
	orderSet bool

	// Field-level logical type annotations — the Java/JDBC Avro idiom
	// where logicalType (and, for decimal, precision/scale) sit as
	// siblings of `type` on the field object rather than nested inside
	// the type definition. Confluent's Java code generator,
	// kafka-connect-avro-converter, and most Debezium CDC sources
	// (Oracle, MySQL, PostgreSQL) emit schemas in this shape.
	//
	// The on-wire encoding is identical to the spec-blessed nested form;
	// only the JSON layout differs. We capture these here so that
	// UnmarshalJSON can lift them into the type definition, after which
	// the rest of the parser sees the canonical nested form.
	Logical   string `json:"logicalType,omitempty"`
	Scale     *int   `json:"scale,omitempty"`
	Precision *int   `json:"precision,omitempty"`

	// hasDefault is true if the field has a default value. This is set
	// in canonical afields (which strip Default) so that validateDefault
	// can check whether nested record fields have defaults.
	hasDefault bool
}

// afieldKeys that signal a complex type definition at the field level
// (the "flat" field format accepted by linkedin/goavro).
var afieldComplexKeys = map[string]string{
	"symbols": "enum",
	"items":   "array",
	"values":  "map",
	"fields":  "record",
	"size":    "fixed",
}

// liftFieldLogicalIntoType moves a field-level logicalType annotation (with
// optional precision/scale for the decimal case) into the field's type
// definition, so the rest of the parser sees the canonical nested form.
// The form
//
//	{"name":"ts","type":"long","logicalType":"timestamp-millis"}
//	{"name":"ts","type":["null","long"],"logicalType":"timestamp-millis"}
//
// is documented as a common user error in AVRO-2015 / AVRO-3014; Apache
// Avro's official parser (Schema.java:1871-1877) detects and warns but
// does not lift, leaving the union bare. fastavro / hamba / linkedin-
// goavro preserve it as a field property only without applying it to
// any branch. The form is widely emitted by hand-written .avsc files,
// older Java tooling, and tutorial code (Confluent's production
// kafka-connect-avro-converter does NOT emit it — it puts logicalType on
// the type object, producing canonical nested form). Twmb performs the
// lift so these in-the-wild schemas round-trip correctly. Wire format
// is identical (raw long varint); only the parsed schema's Go-type
// interpretation differs.
//
// The on-wire encoding is identical to
//
//	{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}
//	{"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}]}
//
// — only the JSON layout differs.
//
// Conflict resolution: an annotation already present inside the type
// definition wins (closer-to-the-type wins). After lifting, the
// field-level copies are cleared so canonical re-emit does not duplicate
// them.
// liftTarget returns the aschema a field-level logicalType annotation lands
// on, or nil when the field carries none or nothing can receive it.
//
// This is the ONE navigation for the field-level lift: liftFieldLogicalIntoType
// moves the annotation through it, and fieldDecimalLiftConsumesPrecisionScale
// reads through it. Keeping both on one function is what stops the verdict
// from validating parameters against a type the lift never addressed; the two
// drifted apart once already, on the wrapped-null branch.
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

// liftEffectiveLogical reports the lift target's kind and the logical type in
// EFFECT there once the lift has run: the target's OWN annotation when it has
// one — closer to the type wins, so the field's is dropped — and otherwise the
// field's.
//
// Consumption is a question about what LANDS, not about where the lift points.
// A field-level "decimal" that never reaches its target cannot make that
// target read precision/scale, so the pair is inert metadata there and rides
// to Props like any custom property.
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

func (f *afield) liftFieldLogicalIntoType() {
	// The target comes from the SHARED navigation, so the lift and the
	// consume verdict can never address different types. It is the FIRST
	// non-null union branch, the type object, or the bare primitive — we do
	// NOT fall through to a later non-null branch (that would silently mutate
	// a different type than the spec-equivalent nested form would have
	// addressed, and on the `[null, T+logical, T]` shape would even
	// synthesize a duplicate union member).
	target := f.liftTarget()
	if target == nil {
		return
	}

	// ANNOTATION and PARAMETERS are separate questions, and conflating them
	// made the two spellings of one schema disagree. Closer-to-the-type wins
	// the ANNOTATION: a target that already declares its own logicalType
	// keeps it and the field's is dropped. The field still completes missing
	// PARAMETERS, but only where they mean something — where the EFFECTIVE
	// logical (the target's own if it has one, else the field's) is
	// "decimal". Anywhere else precision/scale annotate nothing, so copying
	// them in would write inert keys into the type.
	_, effLogical, _ := f.liftEffectiveLogical()
	fillParams := effLogical == "decimal"

	switch {
	case target.primitive != "":
		// A bare primitive, at the field's type position or as a union
		// branch: {"type":["null","long"], "logicalType":"x"} →
		//   {"type":["null",{"type":"long","logicalType":"x"}]}
		obj := &aobject{Type: target.primitive, Logical: f.Logical}
		if fillParams {
			obj.Scale = clonePtrInt(f.Scale)
			obj.Precision = clonePtrInt(f.Precision)
		}
		*target = aschema{object: obj}

	case target.object != nil:
		// {"type":{"type":"long"}, "logicalType":"x"} →
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
// logical lift consumes "precision"/"scale" as decimal parameters: the
// field declares logicalType "decimal" and the lift's target — the field's
// primitive type name, the first non-null union branch's kind, or the type
// object's kind — is a bytes/fixed carrier, matched as-written like the
// type level's decimalConsumesPrecisionScale (a named reference is not a
// carrier spelling, and the target's own logicalType annotation does not
// change where the lift points). Everywhere else the pair is inert field
// metadata and a malformed body rides to the field's props verbatim.
func (f *afield) fieldDecimalLiftConsumesPrecisionScale() bool {
	kind, logical, ok := f.liftEffectiveLogical()
	return ok && decimalConsumesPrecisionScale(kind, logical)
}

// newLogicalObject builds an aobject describing the field's primitive type
// promoted with the field-level logicalType / precision / scale.
func (f *afield) newLogicalObject(primitiveType string) *aobject {
	return &aobject{
		Type:      primitiveType,
		Logical:   f.Logical,
		Scale:     clonePtrInt(f.Scale),
		Precision: clonePtrInt(f.Precision),
	}
}

func clonePtrInt(p *int) *int {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// maxParseErrorLen bounds the assembled length of a schema-parse error
// message. The build/finalize walkers wrap per nesting level (e.g.
// "invalid array: %v" at each of up to maxDepth levels), so a deeply
// nested schema can otherwise produce a multi-KB message from a small
// input — the same log/RPC/metric amplification the per-value trunc
// helpers prevent, but accumulated over the wrap chain rather than in one
// value. truncForError caps individual interpolated values; this caps the
// whole chain.
const maxParseErrorLen = 1024

// boundErrorLen returns err unchanged if its message fits maxParseErrorLen,
// otherwise a flattened error keeping the head (outer context) and the
// tail (the innermost cause — e.g. "recursion limit exceeded", which the
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
	return errors.New(msg[:half] + " …[truncated]… " + msg[len(msg)-half:])
}

// boundJSONErrorEcho truncates user-controllable input echoed verbatim by
// stdlib json / strconv error types so a hostile MiB-sized literal can't
// produce a MiB-sized error string from [Parse]. Reaches
// *json.UnmarshalTypeError (returned by stdlib's reflect-based int
// decoder for the schema's *int Scale / Precision fields) and
// *strconv.NumError (defense-in-depth; [laxInt.UnmarshalJSON]'s own
// length cap is the primary guard for that path because [fmt.Errorf]'s
// %w wrap caches the formatted message at construction).
//
// Walks the chain via [errors.As] and mutates in place; the mutation must
// happen before the caller wraps the error with [fmt.Errorf]("%w", err),
// which caches its formatted message and locks in the pre-truncation
// content of any descendant.
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

	extra map[string]any // non-reserved properties, populated by aschema.UnmarshalJSON
}

// laxInt is an int that also accepts JSON strings containing integers,
// per the Avro spec's [INTEGERS] canonical form rule which acknowledges
// that "size" may appear as a quoted integer.
type laxInt int

// maxLaxIntDataLen caps the raw JSON bytes accepted by [laxInt.UnmarshalJSON].
// Legit int64 representations fit in 20 chars (-9223372036854775808); the
// quoted-string form (per the Avro [INTEGERS] rule) adds 2 chars; +2 chars
// of headroom covers the Go-style +sign that strconv.Atoi accepts. Hostile
// MiB-sized literals are rejected at entry so neither strconv.Atoi nor
// stdlib json.Unmarshal-into-int produces a multi-MB error string (both
// embed the failing input verbatim in *strconv.NumError.Num /
// *json.UnmarshalTypeError.Value). The string-arm's [fmt.Errorf] wrap
// caches the formatted message in *fmt.wrapError, defeating downstream
// truncation of the inner error — the only reliable defense is at entry.
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

// unionMissing patches a union's ser AND deser branch function tables
// (plus its branch nodes and name tables) when any branch type was a
// forward reference. One fixup record per union keeps the paired
// structures updated together by construction.
type unionMissing struct {
	ser        *serUnion
	deser      *deserUnion
	branches   []*schemaNode  // union node's branch slice; fwd-ref branch nodes are patched in finalize
	missing    map[int]string // branch index → type name
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
	sr         *serRecord
	dr         *deserRecord
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

// defaultFixup defers a record field's default-value resolution + encoding
// to finalize, for a field whose OUTER type resolved at build time but whose
// type tree contains a forward-referenced descendant (e.g. array<fwd-ref>
// items, map<fwd-ref> values, or an inline record with a fwd-ref field).
// encodeDefault recurses into items/values/fields and dereferences each
// child's kind, so running it at build time against a not-yet-wired child
// node panics; deferring runs it after every container/field fixup has wired
// the descendants. The fwd-ref-OUTER case (the whole field type is a bare
// forward-ref name) is handled by recordFieldFixup instead, which also
// carries the default and resolves the node by name in finalize.
type defaultFixup struct {
	sr         *serRecord
	dr         *deserRecord
	nd         *schemaNode
	idx        int
	node       *schemaNode // the field's already-built outer node (children wired by other fixups)
	defaultVal any         // parsed-but-not-yet-coerced JSON default
}

// captureFwdRef is the shared boilerplate used by every site that might
// encounter a forward reference inside a nested build (record field,
// array items, map values). On success it returns (false, "", nil). On
// an unknownPrimitiveError it returns (true, name, nil) so the caller
// can queue a fixup. On any other error it wraps with ctxLabel and
// returns (false, "", err).
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
	ser   serfn
	deser deserfn
	sr    *serRecord   // non-nil for records only
	dr    *deserRecord // non-nil for records only
	node  *schemaNode
	// hadCustomType is true when this named type was DEFINED by a parse
	// that wired at least one CustomType anywhere (deliberately coarse —
	// every definition of a custom-wired parse counts; stamped at
	// finalize, see registerNamed). The cache-reference boundary guard
	// compares it against the referencing parse's registrations to
	// allow consistent reuse and reject mismatches — the documented
	// remediation path "re-parse Inner with the CT first" depends on
	// this signal.
	hadCustomType bool
}

type builder struct {
	ser   serfn
	deser deserfn

	named        map[string]*namedType
	building     map[*schemaNode]struct{} // record/error nodes whose field loop is in progress (shared across nest, like named)
	definedNamed []*namedType             // named types DEFINED by this parse (vs inherited); stamped custom-affected at finalize
	// definedSet is the membership form of this-parse definitions. Unlike
	// definedNamed (a per-builder accumulator merged up only at unnest), it is
	// SHARED BY REFERENCE across nest() — like named/building/cachedNames — so a
	// guard running in a NESTED builder (e.g. while building a ["null","Self"]
	// field) can test whether a resolved reference points at a name THIS parse
	// defined, before unnest would have propagated definedNamed upward. A
	// re-registered cached name's fresh *namedType lands here; cachedNames cannot
	// make that distinction (it marks such a name as both inherited and redefined).
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
	// parse ("" = proven match-free, non-"" = a matched-type location; key
	// PRESENCE marks a computed verdict, so "" is a valid entry). Allocated
	// by applySchemaOpts only when CustomTypes are registered, and shared
	// by reference across nest() so the finalize stamping loop and the
	// per-reference cache walks collapse to one walk per node per parse.
	// See customMatchInSubtree for the soundness rules. Nil on white-box
	// test builders — every write is guarded, degrading to unshared walks.
	customMatch map[*schemaNode]string
	// overlayDone marks inherited subtrees overlayInheritedCustom has
	// completed, so N references to the same cached type overlay its nodes
	// once per parse instead of re-walking per reference. Sharing one set
	// across references is sound because the walk's effect is idempotent
	// and order-independent within a parse: buildCustomWiring is
	// deterministic (b.customTypes is fixed after applySchemaOpts) and
	// existing overlay entries are kept, so a second walk over the same
	// nodes could only rebuild identical wiring. Allocated alongside
	// customMatch; nil-tolerated the same way.
	overlayDone map[*schemaNode]bool
	cachedNames map[string]bool // names inherited from SchemaCache, not from this parse
	// allowReRegister permits re-DEFINING an inherited (cachedNames) type
	// instead of erroring "duplicate named type". Set by SchemaCache.Parse only
	// for parses that skip dedup and re-parse to get fresh CustomType wiring
	// (custom parses, and re-parses of a previously-custom-parsed schema).
	allowReRegister bool
	depth           int // current build recursion depth, bounded by maxDepth
}

// validNameErr validates a simple name using the builder's configured validator.
func (b *builder) validNameErr(s string) error {
	if b.checkName != nil {
		return b.checkName(s)
	}
	if !validName(s) {
		return fmt.Errorf("invalid name %q", truncForError(s))
	}
	return nil
}

// validFullnameErr validates a dot-separated fullname.
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

// putCustomWiring stores the wiring under node, allocating b.custom on
// demand. Used by applyCustomTypes after building the per-node closures.
func (b *builder) putCustomWiring(node *schemaNode, w *customWiring) {
	if b.custom == nil {
		b.custom = make(map[*schemaNode]*customWiring)
	}
	b.custom[node] = w
}

// makeCustomSer wraps base with the custom-Encode function ce: apply ce to the
// value, then encode the converted value via base. SINGLE definition of the
// binary custom-Encode wrap, shared by applyCustomTypes (in-order references)
// and customWrappedSer (forward-ref finalize fixups) so the two paths cannot
// drift — a forward reference to a custom-encoded named type must emit the same
// wire as an in-order one.
func makeCustomSer(ce func(reflect.Value) (reflect.Value, error), base serfn) serfn {
	return func(dst []byte, v reflect.Value, depth int) ([]byte, error) {
		v, err := ce(v)
		if err != nil {
			return nil, err
		}
		// Pass depth unchanged: the custom wrapper annotates an existing
		// schema node, it is not a new nesting level. base does the node's
		// own depth accounting, and the decode wrapper + JSON path both
		// charge 0 here — re-entering at depth+1 would make a custom on a
		// recursive node trip errTooDeep a level shallower per recursion
		// than decode, breaking round-trips.
		return base(dst, v, depth)
	}
}

// customWrappedSer returns base wrapped with node's custom-Encode chain when
// one is registered (the same wrap applyCustomTypes installs for an in-order
// reference), else base unchanged. The forward-ref fixups in finalize call this
// so a forward reference to a custom-encoded named type applies the CustomType
// on the binary path — previously they used the unwrapped namedType.ser while
// the JSON encoder applied the custom (a silent binary↔JSON divergence: the
// forward-referenced field encoded raw on binary but converted on JSON).
func (b *builder) customWrappedSer(node *schemaNode, base serfn) serfn {
	if w := b.custom[node]; w != nil && w.encode != nil {
		return makeCustomSer(w.encode, base)
	}
	return base
}

// customWrappedDeser is the decode dual of customWrappedSer: it returns base
// wrapped with node's custom-Decode chain when one is registered (the same wrap
// applyCustomTypes installs for an in-order reference), else base unchanged.
// The forward-ref fixups in finalize call this so a forward reference to a
// custom-decoded named type applies the CustomType on the binary path —
// otherwise the forward-referenced field decodes raw on binary while the JSON
// decoder applies the custom via the patched node.decodeJSON (the decode twin
// of the encode divergence). Logical-suppression with no Decode callback needs
// no wrap here: the suppressed (raw) deser is already baked onto the shared
// leaf node, so a forward reference inherits it on both wire formats.
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

// registerNamed stores nt under name and records it as a definition of
// this parse. The custom-affected flag (hadCustomType) is stamped for
// ALL of this parse's definitions at finalize, after applyCustomTypes
// has wired every node: registration happens early so self-references
// resolve mid-build, and a stamp taken here would predate the wiring it
// reports — permanently for a type whose OWN node matches the
// CustomType (fixed, enum), since no later per-arm re-stamp sees it.
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

// tryAssignNamedRef resolves a named-type reference, possibly with
// namespace qualification against parentName. Returns true on hit (with
// b.ser / b.deser / b.meta / b.node populated and, when setCanon is
// true, b.canon set to the resolved name). Shared by buildPrimitive's
// bare-string named-ref path and buildComplex's wrapped-form
// {"type":"Name"} path so the rejectCachedRefIfCustomTypeWouldMatch
// gate and the namespace-qualified retry agree.
// leadingDotName reports whether name spells the explicit
// null-namespace escape — a single leading dot with no other dot —
// and returns the fullname it denotes: ".x" is the null-namespace
// fullname "x", and "." is the bare empty name "". This is Java's Name
// constructor rule (Schema.java ~1455, release-1.12.0: lastDot split,
// then `if ("".equals(space)) space = null`), the same rule
// qualifyAliases applies to aliases; a name whose namespace part is
// non-empty (".a.b", "a.b") is a fullname verbatim. Shared by the
// definition build (aobject naming), reference resolution
// (scopedRefKeys), and the metadata fullname computation
// (nodeFullname) so the escape cannot drift between them.
func leadingDotName(name string) (string, bool) {
	if strings.LastIndexByte(name, '.') == 0 {
		return name[1:], true
	}
	return name, false
}

// scopedRefKeys writes the lookup keys for a name reference into dst in
// binding-precedence order and returns the filled prefix: a dotted
// reference is an exact fullname lookup; a bare reference tries the
// enclosing-namespace-qualified key first, then the bare key (the
// null-namespace type, when one exists). This IS the name-binding rule —
// Java's Names.get order (Schema.java: new Name(o, space) looked up
// before the null-space retry) and fastavro's schema_name qualification.
// Checking the bare key first would bind a null-namespace type in
// preference to the in-scope one whenever the two share a short name,
// silently changing the wire contract of every field using the
// reference. Every resolver — wire build/finalize (resolveNamedRef),
// canonical (lookupCanonDef), metadata (lookupNameRef) — derives its key
// order here so the precedence cannot drift between them.
func scopedRefKeys(dst *[2]string, ref, ns string) []string {
	if strings.Contains(ref, ".") {
		if short, ok := leadingDotName(ref); ok && short != "" {
			// ".x" is the explicit null-namespace escape (the same
			// Name-ctor rule the definition side normalizes by): an
			// exact lookup of the null-namespace fullname "x", never
			// qualified into the enclosing namespace. A bare "." stays
			// as-written and can only miss — nothing registers "." —
			// keeping the empty-name type unreferenceable in every
			// spelling (NOT_BUGS #60).
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

// resolveNamedRef looks up a named-type reference by name, in
// scopedRefKeys precedence order against parentName's namespace. Returns
// the resolved fullname and the namedType, or ("", nil) when unresolved.
//
// Shared by tryAssignNamedRef (build-time, backward references) and
// finalize (forward-reference fixups) so the qualification rule lives in
// ONE place. finalize previously did a bare b.named[name] lookup with no
// qualified retry, so a forward reference into a namespaced scope failed
// ("unknown type") even though the byte-identical backward-ordered schema
// resolved through tryAssignNamedRef's retry — an order-dependent parse
// rejection of a valid schema.
func (b *builder) resolveNamedRef(name, parentName string) (string, *namedType) {
	var keys [2]string
	for _, k := range scopedRefKeys(&keys, name, namespaceOf(parentName)) {
		if nt := b.named[k]; nt != nil {
			return k, nt
		}
	}
	return "", nil
}

func (b *builder) tryAssignNamedRef(name, parentName string, setCanon bool) (bool, error) {
	resolved, nt := b.resolveNamedRef(name, parentName)
	if nt == nil {
		return false, nil
	}
	if err := b.rejectCachedRefIfCustomTypeWouldMatch(resolved, nt); err != nil {
		return true, err
	}
	// Only an INHERITED type can carry the stamp at reference time: local
	// definitions are stamped at this parse's own finalize, after build.
	if nt.hadCustomType {
		b.sawInheritedCustom = true
	}
	// Cross-parse inherited subtree (same condition family as the guard
	// above: not defined this parse, name inherited from the cache):
	// complete the overlay so resolve-time custom re-application sees the
	// inherited nodes. Locally defined types are wired by applyCustomTypes
	// at their own build. The visited set is the per-parse b.overlayDone,
	// so repeated references to the same inherited type walk its subtree
	// once per parse, not once per reference (see the field's soundness
	// note); the nil fallback covers white-box test builders only.
	if len(b.customTypes) > 0 && !b.definedSet[nt] && b.cachedNames[resolved] {
		if b.overlayDone == nil {
			b.overlayDone = make(map[*schemaNode]bool)
		}
		b.overlayInheritedCustom(nt.node, b.overlayDone)
	}
	if setCanon {
		b.canon = aschema{primitive: resolved}
	}
	b.ser = nt.ser
	b.deser = nt.deser
	if nt.sr != nil {
		b.meta = fieldMeta{avroType: "record", serRecord: nt.sr, deserRecord: nt.dr}
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
			m.ser.fns[idx] = b.customWrappedSer(nt.node, nt.ser)
			m.deser.fns[idx] = b.customWrappedDeser(nt.node, nt.deser)
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
		// branch-name tables) from the RESOLVED names — buildUnion ran
		// them with the unresolved as-written names for fwd-ref branches.
		if m.branches != nil && m.deser != nil {
			if err := finalizeUnionNames(m.ser, m.deser, m.branches); err != nil {
				return err
			}
		}
	}
	for _, m := range b.mfixups {
		_, nt := b.resolveNamedRef(m.name, m.parentName)
		if nt == nil {
			return fmt.Errorf("unknown type %q", truncForError(m.name))
		}
		m.meta.serRecord = nt.sr
		m.meta.deserRecord = nt.dr
	}
	// Phase 1: wire every forward-referenced record-field node. Default
	// ENCODING is deferred to phase 2 below so it runs only after every
	// field AND container child node is wired — encodeDefault recurses into
	// a field's child nodes, and a not-yet-wired child would nil-panic.
	for _, m := range b.fieldFixups {
		_, nt := b.resolveNamedRef(m.name, m.parentName)
		if nt == nil {
			return fmt.Errorf("unknown type %q", truncForError(m.name))
		}
		m.sr.fields[m.idx].fn = b.customWrappedSer(nt.node, nt.ser)
		m.dr.fields[m.idx].fn = b.customWrappedDeser(nt.node, nt.deser)
		if nt.sr != nil {
			m.sr.fields[m.idx].avroType = "record"
			m.sr.fields[m.idx].meta.avroType = "record"
			m.sr.fields[m.idx].meta.serRecord = nt.sr
			m.dr.fields[m.idx].avroType = "record"
			m.dr.fields[m.idx].meta.avroType = "record"
			m.dr.fields[m.idx].meta.deserRecord = nt.dr
		}
		m.nd.fields[m.idx].node = nt.node
	}
	// Phase 1b: wire every forward-referenced array/map container child.
	for _, m := range b.containerFixups {
		_, nt := b.resolveNamedRef(m.name, m.parentName)
		if nt == nil {
			return fmt.Errorf("%s references unknown named type %q", m.ctxLabel, truncForError(m.name))
		}
		*m.serItem = b.customWrappedSer(nt.node, nt.ser)
		*m.deserItem = b.customWrappedDeser(nt.node, nt.deser)
		m.setMinBytes(schemaMinBytes(nt.node))
		*m.nodeChild = nt.node
	}
	// Phase 2 — deferred field defaults, in two passes. encodeDefault fills
	// an absent nested record field from its resolved f.defaultVal, so every
	// field's default VALUE must be recorded (phase 2a) before any default's
	// binary bytes are encoded (phase 2b); otherwise a field default that
	// nests into a sibling-defaulted record reads a nil f.defaultVal and
	// mis-encodes. Both deferral kinds participate: fwd-ref-OUTER fields
	// (recordFieldFixup, node resolved by name) and container/nested fields
	// whose outer type resolved but whose descendant was a fwd-ref
	// (defaultFixup, node already known).
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
		name := m.sr.fields[m.idx].name
		converted, err := resolveFieldDefaultValue(
			coerceDefault(m.defaultVal, node), node, name,
			&m.dr.fields[m.idx], &m.nd.fields[m.idx],
		)
		if err != nil {
			return fmt.Errorf("type %q: %w", truncForError(m.name), err)
		}
		pending = append(pending, pendingDefault{node, name, converted, &m.sr.fields[m.idx]})
	}
	for _, m := range b.defaultFixups {
		name := m.sr.fields[m.idx].name
		converted, err := resolveFieldDefaultValue(
			coerceDefault(m.defaultVal, m.node), m.node, name,
			&m.dr.fields[m.idx], &m.nd.fields[m.idx],
		)
		if err != nil {
			return err
		}
		pending = append(pending, pendingDefault{m.node, name, converted, &m.sr.fields[m.idx]})
	}
	// Phase 2b: encode binary default bytes now that every default value
	// (inline-built and deferred) is recorded on its field node.
	for _, p := range pending {
		if err := encodeFieldDefaultBytes(p.converted, p.node, p.name, p.srf); err != nil {
			return err
		}
	}
	// Stamp each named type THIS parse defined as custom-affected iff a
	// non-wildcard registration matches somewhere in ITS OWN subtree —
	// exactly when baked custom effects (logical suppression on the
	// shared leaf nodes, callback wraps in the composed ser/deser) live
	// inside the type. This is the SAME predicate the cache-boundary
	// guard (rejectCachedRefIfCustomTypeWouldMatch) applies on the
	// reference side — findCustomTypeMatchInSubtree, whose wildcard skip
	// mirrors that a wildcard bakes nothing onto shared nodes — so the
	// stamp and the guard cannot disagree in either direction: the walk
	// crosses into SchemaCache-inherited subtrees, so a wrapper defined
	// around an inherited custom-baked reference is stampable (a
	// wired-this-parse test made the guard's "re-parse <name> with the
	// CustomType first" remediation unsatisfiable for transitive chains
	// — define with the custom, wrap with the custom, reference with the
	// custom: rejected), and a sibling type whose own subtree has no
	// match is NOT stamped merely because the parse wired something
	// elsewhere (the earlier coarse posture, which false-rejected later
	// no-custom references to such types). Taken at finalize, after the
	// tree is fully built, because registration happens early to support
	// self-references. Inherited (cache) entries are not in definedNamed
	// and keep the flag from their defining parse.
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
	// Discriminate union-ness by non-nil, not length: `[]` parses to a
	// non-nil zero-branch union (legal — Java's UnionSchema constructor,
	// fastavro, and avro-rs all accept it; no value can ever encode or
	// decode against it, but the schema itself is well-formed).
	if s == nil || s.primitive == "" && s.object == nil && s.union == nil {
		return errors.New("schema is not a primitive, complex, nor union")
	}
	if b.depth >= maxDepth {
		return fmt.Errorf("schema nests deeper than the supported limit (%d)", maxDepth)
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
	// Apply custom types to newly built nodes (not unions — custom
	// types fire on individual branches, not the union container).
	if len(b.customTypes) > 0 && b.node != nil && b.node.kind != "union" {
		if err := b.applyCustomTypes(b.node); err != nil {
			return err
		}
	}
	return nil
}

// buildCustomSN builds a public SchemaNode from an internal schemaNode.
// Built once per node at parse time and cached for CustomType callbacks.
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

// hasMatchingCustomType checks if any registered custom type would match
// a node with the given kind and logical type. Used to skip the built-in
// logical-type *decoder* when a custom type replaces it (the deser-side
// of the suppression contract — see [CustomType.Decode]'s docstring:
// "If nil, the built-in logical type handler is bypassed and the base
// Avro type decoder is used directly").
//
// The encode side has different semantics ([CustomType.Encode]: "If nil,
// the built-in logical type encoder is used"), so encoder-suppression
// uses [hasMatchingCustomTypeWithEncode] instead — only suppress the
// built-in encoder when the user actually provided an Encode callback
// to wrap it with.
func (b *builder) hasMatchingCustomType(kind, logical string) bool {
	return b.hasMatchingCustomTypeCond(kind, logical, false)
}

// hasMatchingCustomTypeWithEncode reports whether any matching CustomType
// has a non-nil Encode callback. Used to gate suppression of the
// built-in logical encoder: per [CustomType.Encode]'s docstring, an
// Encode==nil CustomType leaves the built-in encoder in place (so a
// user registering only Decode keeps the convenient time.Time /
// *big.Rat / avro.Duration encoder), while an Encode!=nil CustomType
// wraps the base (raw) encoder with the user's callback.
func (b *builder) hasMatchingCustomTypeWithEncode(kind, logical string) bool {
	return b.hasMatchingCustomTypeCond(kind, logical, true)
}

// hasMatchingCustomTypeCond is the shared body. When requireEncode is
// true, the predicate additionally requires ct.Encode != nil — used by
// the encoder-suppression gate. When false, the predicate matches any
// registered CustomType — used by the decoder-suppression gate (where
// Decode==nil still bypasses the built-in per the doc).
func (b *builder) hasMatchingCustomTypeCond(kind, logical string, requireEncode bool) bool {
	for _, ct := range b.customTypes {
		// Wildcards (both empty) should not suppress built-in
		// handlers — they use ErrSkipCustomType at runtime.
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
		// Schema's ser) but NOT node.ser, so named types in the cache
		// keep their unwrapped ser/deser. The wrap closure is built by
		// makeCustomSer, shared with the forward-ref finalize fixups
		// (customWrappedSer) so an in-order reference and a forward reference
		// to the same custom-encoded named type apply the SAME wrap.
		b.ser = makeCustomSer(wiring.encode, node.ser)
	}

	// jsonAppliesLogical narrows suppression to nodes whose JSON decoder
	// actually transforms the raw value (a logical decodeKind would apply):
	// only those need a JSON-side suppress-wrapper to mirror the binary raw
	// decode. See buildCustomWiring for the suppression contract.
	jsonAppliesLogical := wiring.suppressLogical && jsonDecodeAppliesLogical(node)
	if len(wiring.decoders) > 0 {
		b.deser = wrapDeserWithCustomDecoders(node.deser, wiring.decoders, wiring.sn)
		// JSON-side: wrap the node's per-decode dispatch with a
		// closure that captures the decoder chain. The JSON runtime
		// (decodeValue) checks node.decodeJSON first and falls back
		// to decodeKind otherwise — no per-call map lookup, no
		// recursion guard, no shared mutable state.
		node.decodeJSON = wrapDecodeJSONWithCustomDecoders(wiring.decoders, wiring.sn, wiring.suppressLogical)
	} else if jsonAppliesLogical {
		// No Decode callback (Encode-only, OR no callbacks at all) on a logical
		// node that the binary path suppresses (non-wildcard): the user
		// receives the RAW Avro-native value (CustomType.Decode docstring:
		// "If nil, the built-in logical type handler is bypassed ... the
		// base Avro type decoder is used directly, producing raw values").
		// decodeKind applies the logical transform unless suppressed, so
		// install the raw-decode wrapper with an empty decoder chain to
		// produce the same raw value through DecodeJSON. A wildcard
		// (suppressLogical false ⇒ jsonAppliesLogical false) skips this —
		// decodeKind keeps the logical transform, matching the binary wildcard.
		node.decodeJSON = wrapDecodeJSONWithCustomDecoders(nil, wiring.sn, wiring.suppressLogical)
	}

	b.putCustomWiring(node, wiring)
	b.meta.hasCustomType = true
	return nil
}

// buildCustomWiring collects the per-node custom-type wiring from this
// parse's registrations: the encode chain closure, the decoder chain, the
// callback SchemaNode, and the suppression flags. Returns nil when no
// registration matches or applies. PURE with respect to the builder and
// the node — no ser/deser wraps, no node mutation, no overlay insertion —
// so it is shared by applyCustomTypes (newly built nodes: wraps + overlay)
// and overlayInheritedCustom (SchemaCache-inherited nodes: overlay only;
// the wraps already live inside the inherited composition, baked by the
// type's own defining parse under the guard-enforced identical
// registrations).
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

	// suppressLogical mirrors the binary decoder-suppression gate
	// (hasMatchingCustomType, the same predicate the primitive/decimal/fixed
	// builds use to decide between the raw and logical deserializer). The
	// binary build replaces the built-in logical deserializer with the raw one
	// whenever ANY non-wildcard CustomType matches — INCLUDING a matcher with
	// no Encode/Decode callbacks (per CustomType.Decode: "If nil, the built-in
	// logical type handler is bypassed ... producing raw Avro-native values").
	// Wildcards (empty LogicalType AND AvroType) are excluded by the gate.
	suppressLogical := b.hasMatchingCustomType(node.kind, node.logical)
	// jsonAppliesLogical narrows that to nodes whose JSON decoder actually
	// transforms the raw value (a logical decodeKind would apply): only those
	// need a JSON-side suppress-wrapper to mirror the binary raw decode.
	jsonAppliesLogical := suppressLogical && jsonDecodeAppliesLogical(node)

	// Nothing to wire when there are no callbacks AND the JSON path has no
	// suppression to mirror. A no-callback matcher on a LOGICAL node falls
	// THROUGH this guard (jsonAppliesLogical is true) so its JSON decode is
	// suppressed to raw, matching the binary path — without this, DecodeJSON
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
								// chain entry — mirrors the
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
	// encodeSuppresses mirrors the binary ENCODER-suppression gate exactly
	// (hasMatchingCustomTypeWithEncode — excludes wildcards), so the JSON
	// encode arms suppress the built-in logical coercion iff the binary build
	// did. See customWiring.encodeSuppresses.
	wiring.encodeSuppresses = b.hasMatchingCustomTypeWithEncode(node.kind, node.logical)

	if len(decoders) > 0 {
		wiring.decoders = decoders
	}
	return wiring
}

// overlayInheritedCustom completes this parse's custom overlay (b.custom)
// for a SchemaCache-inherited subtree: applyCustomTypes visits only newly
// built nodes, so a reference to an inherited type left its matching
// nodes without overlay entries — the binary/JSON callback wraps still
// fire on DIRECT decode (they are composed inside the inherited ser/deser
// by the type's defining parse), but every consumer that RE-APPLIES
// customs from the overlay silently skipped them: Resolve's
// resolveCtx.custom dropped the reader's custom (and the no-callback
// logical suppression gate) on rebuilt nodes, so a resolved decode
// returned raw values where the direct decode returned custom-wrapped
// ones. Walks exactly like findCustomTypeMatchInSubtree (fields, items,
// values, branches; visited set for recursion) and inserts pure wiring
// only — no ser/deser wraps, no node mutation — since the composition
// already carries the wraps. Existing entries are kept (a node can be
// reached through multiple references).
//
// visited is the PER-PARSE b.overlayDone set, shared across every
// reference this parse makes: sharing is sound because the walk's effect
// is idempotent and order-independent within a parse — b.customTypes is
// fixed after applySchemaOpts, so buildCustomWiring(node) is
// deterministic, and a re-visit could only rebuild wiring identical to
// the entry already kept. Skipping the re-visit therefore changes cost
// (one subtree walk per parse instead of per reference), never outcome.
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
	// Check if this is a named type reference (record, enum, fixed).
	// setCanon=false: the buildPrimitive path's canon was already set to
	// s.primitive above; only the namespace-qualified retry needs to
	// rewrite it, which tryAssignNamedRef handles internally when given
	// setCanon=true. To keep the bare-name canon as written (only the
	// qualified retry rewrites it), we tell the helper
	// to setCanon for both branches and let the bare path overwrite with
	// the identical name.
	if found, err := b.tryAssignNamedRef(s.primitive, parentName, true); err != nil || found {
		return err
	}
	return &unknownPrimitiveError{s.primitive}
}

// rejectCachedRefIfCustomTypeWouldMatch returns an error when the
// current Parse registered a CustomType that would match a node
// inside the cached named-type's subtree. The cached node's
// ser/deser/per-field handlers were baked at the original Parse with
// no knowledge of this Parse's customTypes; silently reusing them
// would mean the user's CustomType is dropped on the cached fields.
// Rather than fail at runtime (the user gets back unwrapped raw
// bytes), we fail at Parse time and tell them what to do: re-parse
// the inner type with the CustomType in place, or pass the CT when
// the inner type is first parsed.
//
// Per the package's "Intentional asymmetries": CustomTypes are
// scoped to the resulting Schema, which by definition includes its
// referenced types.
func (b *builder) rejectCachedRefIfCustomTypeWouldMatch(refName string, nt *namedType) error {
	if nt == nil || nt.node == nil {
		return nil
	}
	// This guard is ONLY about types inherited from the SchemaCache across
	// Parses. A name DEFINED in the current Parse (including a self-/forward
	// reference resolved mid-build, before its subtree's CTs are wired onto the
	// shared node) has this Parse's CustomTypes in scope and applies them to its
	// single definition, so it is never a stale cross-parse cache.
	//
	// definedSet membership is the authoritative "defined this parse" test: it
	// holds the *namedType of every definition THIS parse registered, INCLUDING
	// a cached name re-registered under allowReRegister (whose self-reference
	// resolves to that fresh node). cachedNames alone CANNOT make this
	// distinction — a re-registered name is in cachedNames (it was inherited)
	// AND defines a fresh node here — so keying the skip on cachedNames
	// false-rejects a re-parse of a cached self-referential schema once a
	// matching CustomType is added (the resolved nt is the fresh node, not the
	// stale cached one). It is also shared by reference across nest(), so it is
	// already populated when the guard runs in the nested builder that builds a
	// self-referential field (definedNamed would not be — it merges up only at
	// unnest). The cachedNames return below then handles only a name never
	// inherited at all; a genuine cross-parse REFERENCE (nt is the cloned cached
	// node, absent from definedSet, and refName IS in cachedNames) falls through
	// both returns to the match check, exactly as intended.
	if b.definedSet[nt] {
		return nil
	}
	if !b.cachedNames[refName] {
		return nil
	}
	// A cached named type and the Parse referencing it MUST AGREE on whether a
	// matching CustomType is registered. The CustomType's effect is baked onto
	// the SHARED cached node — the binary logical-codec suppression bakes the
	// raw ser/deser onto node.ser/node.deser (build sites), and the JSON wrapper
	// sets node.decodeJSON — and a named-type reference resolves through those
	// node fields, with no per-Schema overlay. So a mismatch silently changes
	// what the referencing Schema decodes/encodes, on BOTH wire formats. Both
	// directions are rejected with the same "make them consistent" remediation.
	currentMatches := ""
	if len(b.customTypes) > 0 {
		currentMatches = b.customMatchInSubtree(nt.node)
	}
	switch {
	case currentMatches != "" && !nt.hadCustomType:
		// Forward: this Parse registers a CustomType matching the cached
		// subtree, but the cached node was built WITHOUT it — reusing it would
		// silently DROP this Parse's custom (the user gets raw/unwrapped values
		// on the cached fields). Re-parse the inner type with the CustomType.
		return fmt.Errorf("avro: cached type %q contains %q which would match a CustomType on this Parse; re-parse %q with the CustomType first", truncForError(refName), truncForError(currentMatches), truncForError(refName))
	case nt.hadCustomType && currentMatches == "":
		// Reverse: the cached node was built WITH a CustomType (its raw ser/deser
		// and JSON decodeJSON bake that conversion onto the shared node), but
		// this Parse registers no matching CustomType — reusing it would
		// silently APPLY the original conversion to a Schema that never opted in
		// (suppressed/raw values on BOTH wire formats). Register the same
		// CustomType in this Parse, or parse the inner type without one.
		return fmt.Errorf("avro: cached type %q was parsed with a CustomType affecting its subtree, but this Parse registers no matching CustomType; reusing it would apply that conversion here — register the CustomType in this Parse, or parse %q without one", truncForError(refName), truncForError(refName))
	}
	return nil
}

// customMatchInSubtree is the memoized entry point over
// findCustomTypeMatchInSubtree: one walk per node per parse, shared by the
// finalize stamping loop, the cache boundary guard
// (rejectCachedRefIfCustomTypeWouldMatch), and — through the shared
// b.customMatch the recursion consults — the overlay completion's guard
// twin. Without the memo each caller re-walks the subtree from scratch:
// O(defs × reachable nodes) at finalize (quadratic on a backward-reference
// chain) and O(references × subtree) on a SchemaCache parse (the
// dos_battery_test.go C9 cells pin both bounds).
//
// The memo is sound because b.customTypes is FIXED for the builder's
// lifetime (applySchemaOpts runs before any build or finalize work, and
// nothing appends afterwards), so a node's verdict can never change within
// the parse; and every queried subtree is fully built when walked (cached
// inherited types are complete from their defining parse; this parse's own
// types are stamped at finalize, after the forward-ref fixups wire every
// child). Two write rules keep memoized verdicts EXACT on cyclic graphs:
//
//   - A clean completion writes "" for EVERY node the walk visited:
//     reachability is transitive, so each visited node's reachable set is a
//     subset of the root's, which the walk just proved match-free. (A "" that
//     merely bubbled up mid-walk is NOT written per-node — a completed child
//     can still reach a match through a back-edge to a node higher on the
//     walk stack, so only the top-level clean result proves anything.)
//   - A match writes the location for exactly the nodes it unwinds through
//     (see findCustomTypeMatchInSubtree): each stack node reaches the match
//     inside its own subtree, so the verdict is exact regardless of what the
//     rest of the walk would have found.
//
// The location STRING for a node is frozen at whichever walk order first
// found a match — different roots could name a different first match, but
// callers branch only on emptiness; the string just names a matched type in
// the guard's error message.
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
// a found match for this node on the unwind (every node on the walk stack
// reaches the match, so the write is exact; see customMatchInSubtree for the
// full memo contract). White-box test builders may lack the memo map — reads
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
// no descendant matches. Recursive types are handled via the
// visited set; node.fields, node.items, node.values, node.branches
// cover every container shape (record, array, map, union). Named-
// type recursion is rare in cached-reuse scenarios but the visited
// set keeps it safe.
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
		// dispatch — they don't reliably suppress built-ins at parse
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
		ser         = new(serUnion)
		deser       = new(deserUnion)
		missing     = make(map[int]string)
		sawTypes    = make(map[string]bool)
		branchMetas = make([]fieldMeta, len(s.union))
		branchNodes = make([]*schemaNode, len(s.union))
		// Per-branch tag spellings, collected across the loop and turned
		// into the three tag tables in one place afterward — the collision
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
		// captureFwdRef converts an unknownPrimitiveError into a
		// (true, name) signal so we can queue a missing-branch fixup
		// for finalize(); any other error is wrapped with the "union"
		// context label. pe.p inside captureFwdRef carries the
		// unresolved name from either the bare-string form (where
		// us.primitive is set) or the wrapped form {"type":"FwdName"}
		// (where us.object.Type is set).
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
		// Per Avro spec ("Unions"): "Names of named types must be
		// defined exactly once across all the schemas of the union."
		// Key by the resolved fullname when available so an inline
		// definition + a name reference to the same named type collide;
		// primitives still collide on kind. Forward-referenced branches
		// are NOT keyed here: their as-written name is not yet bound
		// (resolveNamedRef may qualify it in-scope or fall back to the
		// null namespace), so keying it would both miss real duplicates
		// ("Inner" vs the inline "n.Inner" it resolves to) and false-
		// reject valid unions (a fwd "Inner" destined for "n.Inner"
		// colliding with a null-namespace "Inner" branch). Every
		// fwd-ref-bearing union is re-checked over the RESOLVED names in
		// finalize via finalizeUnionNames.
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
	fillUnionTagTables(ser, deser, branchNodes, unionStd, unionLog)
	// Populate branchKinds for type-name dispatch in serUnion.ser /
	// appendAvroJSONUnion / encodeDefault's union case (see
	// unionTypeNameForValue). Primitive kinds only — record/enum/fixed
	// branches go through tagged-union dispatch, and the spec
	// guarantees primitive kinds are unique within a union (so this
	// map is unambiguous by construction).
	for i, branch := range branchNodes {
		if branch == nil {
			continue
		}
		switch branch.kind {
		case "null", "boolean", "int", "long", "float", "double", "string", "bytes":
			if ser.branchKinds == nil {
				ser.branchKinds = make(map[string]int, len(branchNodes))
			}
			if _, exists := ser.branchKinds[branch.kind]; !exists {
				ser.branchKinds[branch.kind] = i
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
		ser:      b.ser,
		deser:    b.deser,
	}
	return nil
}

// fillUnionTagTables builds a union's three tag tables: the two the DECODER
// EMITS (deser.branchNames / deser.logicalNames) and the one the ENCODER
// RESOLVES a caller-written tagged map through (ser.branchNames).
//
// The emit tables carry one precedence rule — an exact branch name outranks a
// logical qualifier, the order findUnionBranch resolves in — so a branch never
// emits a tag the decoder would hand to a different branch. The accept table is
// built by asking unionTagTiers instead of restating any of it; see below.
func fillUnionTagTables(ser *serUnion, deser *deserUnion, branches []*schemaNode, standard, logical []string) {
	deser.branchNames = append(deser.branchNames[:0], standard...)
	deser.logicalNames = deser.logicalNames[:0]
	for i, ln := range logical {
		if ln != standard[i] && unionLogicalTagOwnedElsewhere(standard, i, ln) {
			ln = standard[i]
		}
		deser.logicalNames = append(deser.logicalNames, ln)
	}
	// The degrade above is the OPERATIVE guard for the EMIT tables, and it is
	// the only one with a consumer of its own: deser.logicalNames is the tag
	// the BINARY decoder wraps a value in, and nothing else recomputes it.
	//
	// The ACCEPT table below is a different question and is built by asking
	// unionTagTiers (json_codec.go) rather than restating any of it. Each tier
	// is offered every branch, in tier order, so this table's accept-set is
	// findUnionBranch's accept-set by construction: a tier added there is
	// honored here without an edit, and neither can grow a tier the other
	// lacks. Two rules ride along, both of them the resolver's:
	//
	//   - Across tiers, FIRST WRITE WINS, because the resolver stops at the
	//     first tier that answers.
	//   - Within a guarded tier, a name two branches could claim is
	//     registered NOWHERE, because the resolver refuses it rather than
	//     picking one. The refusal has to be on BOTH wires or the caller gets
	//     a value on one and an error on the other.
	//
	// A branch node is nil only for a forward reference buildUnion has not
	// bound yet; its exact name is registered from `standard` so the table is
	// usable in the interim, and finalizeUnionNames rebuilds over the resolved
	// nodes.
	ser.branchNames = make(map[string]int, len(standard))
	// ONE scratch pair for the whole walk, not a map pair per tier. A union
	// has a handful of branches, so the duplicate check is a linear scan over
	// this slice; building maps per tier put six allocations on every union a
	// parse contains, which is a cost the tier factoring must not add.
	claims := make([]string, len(branches))
	claimed := make([]bool, len(branches))
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
		for i := range branches {
			if !claimed[i] {
				continue
			}
			if tier.guarded {
				dup := false
				for j := range branches {
					if j != i && claimed[j] && claims[j] == claims[i] {
						dup = true
						break
					}
				}
				if dup {
					continue
				}
			}
			if _, taken := ser.branchNames[claims[i]]; !taken {
				ser.branchNames[claims[i]] = i
			}
		}
	}
}

// finalizeUnionNames re-derives a union's name-dependent artifacts after
// every forward-referenced branch node has been wired. A named reference
// is position-independent in Avro, so nothing observable may depend on
// whether a branch was defined before or after the reference — but
// buildUnion runs before forward refs resolve, so for fwd-ref branches it
// captured the UNRESOLVED as-written name in (a) the duplicate-branch
// check key and (b) the TaggedUnions branch-name tables. This re-runs
// both over the resolved nodes:
//
//   - Duplicate detection (spec, "Unions": a union may not contain the
//     same named type twice): keyed by the resolved fullname for named
//     branches and the kind for unnamed ones — so a short-name forward
//     reference plus an inline definition of the same type collide here
//     even though their parse-time keys ("Inner" vs "n.Inner") did not.
//   - branchNames/logicalNames rebuild: the tagged-union envelope name
//     and the tagged-map encode acceptance use the resolved full name,
//     matching the JSON side's node-based unionBranchNames and making
//     the tables identical to what an in-order reference produces.
func finalizeUnionNames(ser *serUnion, deser *deserUnion, branches []*schemaNode) error {
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
	fillUnionTagTables(ser, deser, branches, std, log)
	return nil
}

// buildNullUnionMeta returns the fieldMeta for the 2-branch null-union
// fast path. nonNullIdx is the index of the non-null branch (1 for
// ["null", T]; 0 for ["T", "null"]). When that branch is a forward
// reference, the inner meta is queued for finalize-time fixup;
// otherwise the inner meta is copied from branchMetas. nullSecond
// distinguishes the two orderings.
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
		// "bytes" is the only primitive underlying that validateLogical
		// permits for decimal/big-decimal ("fixed" is built on the
		// named-type path below, never here). The o.Type=="bytes" gate
		// matters because the CustomType-resurrection above can restore a
		// dropped "decimal"/"big-decimal" logical onto ANY primitive
		// (validateLogical soft-drops decimal on a wrong underlying type
		// BEFORE its precision-required check, so o.Precision stays nil):
		// without the gate a `{"type":"int","logicalType":"decimal"}` +
		// decimal CustomType would enter this branch and dereference nil
		// o.Precision. A resurrected logical on a non-bytes primitive
		// instead falls through to the plain-primitive path, where the
		// base ser/deser carry the value and the CustomType wraps them.
		if o.Logical == "decimal" && o.Type == "bytes" {
			scale := 0
			if o.Scale != nil {
				scale = *o.Scale
			}
			// Per-direction suppression mirrors the timestamp/uuid path
			// below (line ~1660-1675): built-in encoder is preserved
			// whenever the user didn't provide an Encode callback (per
			// CustomType.Encode docstring "If nil, the built-in logical
			// type encoder is used"); built-in decoder is suppressed
			// whenever ANY matching CustomType exists (per
			// CustomType.Decode docstring "If nil, the built-in logical
			// type handler is bypassed and the base Avro type decoder
			// is used directly"). A single-gate suppression on any match
			// would break encode of *big.Rat with a Decode-only
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
		// Use the logical serializer when the logical is spec-valid for this
		// underlying kind — there it is a strict superset of the base serializer
		// (accepts time.Time etc. in addition to raw values). The deserializer
		// is suppressed below when a custom type matches, so Decode produces raw
		// Avro-native values for the custom decoder.
		//
		// The kind check matters because the CustomType resurrection near the
		// top of buildComplex can restore a soft-dropped logical onto a kind it
		// is not valid for (uuid on bytes, timestamp-millis on string).
		// logicalSer/logicalDeser are keyed only on the logical name, so without
		// the gate the binary codec would apply serUUID/serTimestamp* (and the
		// matching deser) on the wrong kind while the per-kind JSON encoder and
		// JSON decoder stay raw — diverging binary from JSON and, for the string
		// case, producing a wire this schema's own decoder cannot read.
		// logicalUnderlyingAcceptsObject is the same predicate validateLogical
		// uses to soft-drop a wrong-kind logical; encode and decode gate on it
		// identically so a resurrected wrong-kind logical stays raw on both.
		//
		// The deser ALSO suppresses on a matching CustomType (hasMatchingCustomType)
		// so a custom decoder sees the raw Avro-native value. The validity gate is
		// independent: a CustomType whose AvroType names a different kind resurrects
		// the logical (resurrection keys on LogicalType only) yet does NOT match for
		// suppression — without the validity gate the bare !hasMatchingCustomType
		// branch would then apply the wrong-kind logical deser on binary while the
		// kind-gated JSON path (assignBytes / decodeLogical*) stays raw.
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
		// o.Precision/o.Scale are deliberately NOT copied here: the node
		// fields hold validated decimal parameters only (the bytes-decimal
		// branch above and the fixed build's decimal arm). On this path the
		// keys were never consumed — a soft-dropped/resurrected decimal on a
		// wrong carrier, or a stray placement — so their values are
		// unvalidated inert metadata, surfaced via extra→props instead.
		b.node = nd
		return nil
	}

	// Named-type reference wrapped in an object: {"type":"Node"} where
	// "Node" is a record/enum/fixed and no type-defining fields are
	// present. Java's parser accepts this form (see apache/avro
	// TestUnionSelfReference). The bare-string form "Node" is the
	// canonical reference shape; this branch handles the equivalent
	// wrapped form for interop with producers that emit it. Forward
	// references — names not yet declared — return unknownPrimitiveError
	// so the field/union/array/map dispatch can queue a fixup, mirroring
	// the bare-string forward-reference path in buildPrimitive.
	// Any of Fields/Symbols/Items/Values/Size/Name being set means the
	// caller is trying to *define* a new type — fall through and let the
	// regular dispatch handle (or error on) that case.
	if o.Name == "" &&
		len(o.Fields) == 0 && len(o.Symbols) == 0 &&
		o.Items == nil && o.Values == nil && o.Size == nil {
		if found, err := b.tryAssignNamedRef(o.Type, parentName, true); err != nil || found {
			return err
		}
		// Not a recognized base/complex type and not a declared named
		// type — treat as a forward reference. The caller (record-field
		// build, union dispatch, etc.) catches unknownPrimitiveError and
		// queues a fixup keyed on the name in the error.
		switch o.Type {
		case "record", "error", "enum", "fixed", "array", "map":
			// real complex-type-without-required-fields — fall through
			// to the existing switch which will surface the right error.
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

	// Canonical form: per the Avro spec's Parsing Canonical Form STRIP
	// rule, keep only: type, name, fields, symbols, items, values, size.
	// Strip all others (logicalType, precision, scale, doc, aliases, etc.).
	//
	// "error" normalizes to "record" so the canonical form (and therefore
	// every Fingerprint hash) matches Java's SchemaNormalization.build
	// (`Schema.Type.RECORD.getName()` returns "record" for both record-
	// typed and error-typed records, since Java's parser stores both as
	// `Type.RECORD` with an `isError` flag the canonical form ignores)
	// and fastavro's `_to_parsing_canonical_form` (which explicitly
	// `elif schema_type == "record" or schema_type == "error":` emits
	// `"type":"record"`). Without this, Rabin / SHA-256 / MD5 fingerprints
	// for error-typed schemas diverge silently from Java's and
	// fastavro's, breaking Single Object Encoding interop and schema-
	// registry fingerprint indexing.
	//
	// Schema.Root().Type, Schema.String(), and SchemaNode.Schema()
	// round-trip continue to preserve the JSON-as-written "error" —
	// only the canonical-surface fingerprint normalizes.
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
		// The namespace attribute is itself a dot-separated sequence of names
		// (Avro spec §Names) and must satisfy the same grammar. The check
		// above only saw the (possibly bare) name attribute, so without this
		// a namespace spelled via the attribute
		// ({"name":"R","namespace":"bad ns"}) would skip validation entirely
		// while the identical fullname spelled inline ({"name":"bad ns.R"})
		// is rejected — and since the parsing canonical form inlines the
		// namespace into the fullname, the accepted schema's Canonical()
		// would otherwise fail to re-parse in the same mode. Validating here
		// also routes namespace components through a WithLaxNames validator,
		// honoring its documented "called for each name component" contract.
		// A dotted name ignores the attribute (the spec, handled below), and
		// the empty string is the explicit null-namespace escape — both exempt.
		if o.Namespace != nil && *o.Namespace != "" && !strings.Contains(o.Name, ".") {
			if err := b.validFullnameErr(*o.Namespace); err != nil {
				return fmt.Errorf("invalid %s namespace %q: %w", truncForError(o.Type), truncForError(*o.Namespace), err)
			}
		}
		// Aliases are NOT name-validated: the Avro spec (§Aliases) states
		// "any string is accepted as an alias", precisely so evolution can
		// alias a reader's valid name to a writer's illegal/legacy name.
		// fastavro does no alias validation (a "123 !bad" alias parses,
		// observed 1.12.2), and Java stores FIELD aliases as raw strings
		// (Field.addAlias, Schema.java:674-677) — though Java's default
		// parser DOES run its NameValidator over TYPE aliases
		// (parseAliases → NamedSchema.addAlias → new Name,
		// Schema.java:2000-2004/:847, rejecting e.g. digit-start), its
		// own divergence from the spec sentence. twmb follows the spec
		// and fastavro. qualifyAliases (below) still applies namespace
		// qualification and the leading-dot null-namespace escape;
		// resolution matches aliases as plain strings.
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
				// ".x" is the null-namespace fullname "x" and "."
				// collapses to the bare empty name "" (leadingDotName).
				// Reachable only under a WithLaxNames fn accepting ""
				// (validFullnameErr already rejected the empty component
				// for strict parses, above), so strict acceptance is
				// unchanged. Without this the name registered VERBATIM
				// while child registration prefixed parentName[:dot+1]
				// and reference resolution used namespaceOf — three
				// rules that disagree exactly when the namespace part is
				// empty, so a bare sibling reference inside ".x" could
				// not resolve at all.
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
			// Inherited name re-registered by a custom (re-)parse — allowed so
			// it gets fresh CustomType wiring.
		}
	} else {
		// A stray "namespace" on an unnamed kind is inert metadata: never
		// consumed (only the named branch above reads o.Namespace), never
		// scoping (children resolve in the enclosing scope on every path —
		// the walkers' nodeChildScope/nsForChildren are kind-keyed), and
		// surfaced as-written by the metadata tree, matching the primitive
		// type-object posture and both references (Java SCHEMA_RESERVED
		// ignores it on every schema object; fastavro accepts it and scopes
		// a named type under such an array in the ENCLOSING scope,
		// executed). A stray "name" on a CONTAINER kind still rejects: the
		// metadata walkers deliberately scope children by any non-empty
		// SchemaNode.Name (nsForChildren's hand-built posture), so a parsed
		// stray name on a kind with child positions would make Root() scope
		// named descendants differently than this parser — the same
		// walker-parity rationale as the structural-key exclusivity
		// rejects. Primitive objects keep accepting a stray name: they have
		// no child positions for that arm to act on.
		if o.Name != "" {
			return errors.New("only record, enum, and fixed can have a name")
		}
		// The inert attribute never reaches the canonical form (PCF has no
		// namespace key for unnamed kinds; fastavro's
		// to_parsing_canonical_form strips it, executed) — mirror the
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
		// while an EMPTY array is the legal empty record. Same
		// missing-vs-empty discrimination as enum symbols: the parser
		// materializes "fields":[] as a non-nil empty slice and leaves
		// the attribute's absence as nil.
		if o.Fields == nil {
			return errors.New("record is missing fields")
		}

		// Create record ser/deser and register early so
		// self-referencing fields (e.g. array items, map values)
		// can resolve the type by name during field building.
		sr := &serRecord{}
		dr := &deserRecord{}
		b.ser = sr.ser
		b.deser = dr.deser
		b.meta = fieldMeta{avroType: "record", serRecord: sr, deserRecord: dr}

		// Register early so self-referencing fields (e.g. array
		// items, map values) can resolve the type by name.
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
		b.registerNamed(o.Name, &namedType{ser: b.ser, deser: b.deser, sr: sr, dr: dr, node: nd})
		b.node = nd

		// Mark this record as under construction. A field default whose type
		// subtree references this record (a self- or mutual-recursive
		// reference) must defer its binary default-encode to finalize rather
		// than encode inline now: encodeDefault recurses into the referenced
		// record's fields, and nd.fields holds only the fields declared
		// before the current one until the loop below completes — encoding
		// inline against it emits truncated, non-decodable default bytes.
		// nodeAwaitsForwardRef consults this set so an in-construction record
		// is treated like a not-yet-wired forward-ref child. Cleared when the
		// build returns and nd is whole. Lazily created (the public entry
		// points seed it, but an internally-constructed builder may not) and
		// shared through nest, so a nested record built while this one is open
		// sees the same in-construction set.
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
			// Field aliases are NOT name-validated — per the Avro spec any
			// string is accepted as an alias (so a reader can alias a writer's
			// illegal/legacy field name); matched as-is against writer field
			// names during resolution. Matches Java/fastavro.
			if seenFields[of.Name] {
				return fmt.Errorf("duplicate record field name %q", truncForError(of.Name))
			}
			seenFields[of.Name] = true
			// Written-ness, not non-emptiness, is what admits the value to
			// the check: "order":"" is a written order and not one of the
			// three the spec defines, so it fails here like every other
			// non-spec spelling. The comparison stays EXACT-case — Apache
			// Avro upper-cases before its own lookup, but reserved
			// attribute VALUES are matched by exact spelling here (a
			// case-variant is a different string, not a different case of
			// the same one).
			if of.orderSet && of.Order != "ascending" && of.Order != "descending" && of.Order != "ignore" {
				return fmt.Errorf("invalid field order %q for field %q", truncForError(of.Order), truncForError(of.Name))
			}
			bf := b.nest()
			// captureFwdRef converts unknownPrimitiveError from a nested
			// build into an "isFwdRef" signal so the caller can queue a
			// fixup in finalize(); other errors are wrapped with the
			// "record field" context label. Shared with array/map sites
			// so all three contexts handle fwd-refs uniformly.
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
				name:     of.Name,
				nameVal:  reflect.ValueOf(of.Name),
				fn:       bf.ser,
				avroType: meta.avroType,
				meta:     meta,
			})
			drf := deserRecordField{
				name:     of.Name,
				nameVal:  reflect.ValueOf(of.Name),
				fn:       bf.deser,
				fnIface:  ifaceFnForPrimitive(meta),
				avroType: meta.avroType,
				meta:     meta,
			}
			fn := fieldNode{
				name:    of.Name,
				nameVal: reflect.ValueOf(of.Name),
				aliases: origFieldAliases[i],
				node:    bf.node,
			}
			if isFwdRef {
				fix := recordFieldFixup{
					sr:         sr,
					dr:         dr,
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
					// The field's outer type resolved at build time, but its
					// type tree has a descendant encodeDefault would traverse
					// that is not yet whole: a forward-referenced array/map
					// items/values or inline record field not yet wired (a nil
					// child encodeDefault would nil-panic on), OR a self-/
					// mutual-recursive reference back into a record still under
					// construction (a non-nil but partial node whose later
					// fields encodeDefault would silently drop). Either way,
					// defer the resolve+encode to finalize, after the
					// container/field fixups wire the descendants and every
					// in-construction record is whole. Signal hasDefault so
					// dispatch knows a default exists; the deferred pass fills
					// defaultVal/defaultBytes.
					drf.hasDefault = true
					fn.hasDefault = true
					b.defaultFixups = append(b.defaultFixups, defaultFixup{
						sr:         sr,
						dr:         dr,
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
				// fn.defaultVal stays nil — the JSON encoder treats a nil
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
		// JSON DecodeJSON uses fieldIdx to route record-field keys to their
		// schema slot. Register every alias→idx mapping in addition to the
		// canonical name so JSON producers that emit using an alias name
		// route to the right field. The binary path's resolve.go does the
		// equivalent alias-aware lookup via findReaderFieldIndex.
		// Per Avro spec ("Aliases are alternative names, and thus subject
		// to the same uniqueness constraints as names"), a field name AND
		// alias share one namespace within a record. Reject symmetrically:
		// either a later name shadowing a prior alias, or a later alias
		// shadowing a prior name/alias, breaks uniqueness. A check on only
		// the alias side would let `[{name:"a",aliases:["x"]},
		// {name:"x"}]` silently parse and then route differently from
		// Java's applyAliases (writer's "x" maps to literal-named "x"
		// here, but Java rewrites writer's "x" → "a" first via the
		// alias).
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

		// The symbols attribute is required (Java: "Enum has no symbols"),
		// but an EMPTY array is legal: the spec asks only for "a JSON
		// array, listing symbols", and Java (EnumSchema's constructor),
		// fastavro, and avro-rs all accept zero symbols. Such an enum has
		// no valid values — every encode/decode of it errors — but the
		// schema parses, which matters for passthrough of foreign schemas
		// carrying a degenerate enum in a position the data never uses.
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
		b.ser = newSerEnum(o.Symbols).ser
		b.deser = (&deserEnum{symbols: o.Symbols}).deser
		b.meta = fieldMeta{avroType: "enum"}

		nd := &schemaNode{
			kind:        "enum",
			name:        o.Name,
			logical:     o.Logical,
			aliases:     qualifyAliases(origAliases, o.Name),
			bareAliases: bareAliasShorts(origAliases),
			symbols:     o.Symbols,
			ser:         b.ser,
			deser:       b.deser,
		}
		if len(origEnumDefault) > 0 {
			// The default must be a JSON STRING token naming a symbol,
			// decided by token type BEFORE the membership check: on a
			// non-string body json.Unmarshal leaves the zero value ""
			// (for an explicit null it is even a no-error no-op), and ""
			// can be a legitimate MEMBER under a WithLaxNames validator
			// that accepts empty name components — membership alone would
			// silently bind such garbage to the "" symbol and schema
			// evolution would fill it. fastavro rejects every non-member
			// (hence every non-string) enum default at parse; Java binds
			// NO default for a non-text token (Schema.java:1921-1925,
			// textValue() → null skips EnumSchema's containment check) —
			// neither reference ever binds one.
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
		b.registerNamed(o.Name, &namedType{ser: b.ser, deser: b.deser, node: nd})
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
		// canonObj captured o.Items by value before this recursion ran, so
		// it still points at the as-parsed (possibly {"type":"X"}-wrapped or
		// attribute-bearing) items schema. Repoint it at the canonicalized
		// child so the Parsing Canonical Form's [PRIMITIVES] and [STRIP]
		// rules apply inside array items, matching Java's
		// SchemaNormalization.build (which recurses into getElementType) and
		// every other top-level/field/branch site that already uses the
		// child's canon. Record fields stay correct via the o.Fields slice
		// alias; only the Items/Values pointer fields need the explicit sync.
		canonObj.Items = &af.canon
		sa := &serArray{serItem: af.ser}
		da := &deserArray{deserItem: af.deser, minItemBytes: schemaMinBytes(af.node)}
		// Specialized array ser/deser fast paths bypass the inner
		// schema's wrapped ser/deser functions. They are correct only
		// when no per-element conversion is needed: no custom type,
		// no logical type, AND no forward reference (the inner ser/
		// deser aren't wired until finalize() resolves the fwd-ref,
		// so the fast-path closure would capture nil fns at build
		// time).
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
		inner.minBytes = schemaMinBytes(af.node)
		b.meta = fieldMeta{avroType: "array", inner: inner}
		arrayNode := &schemaNode{
			kind:  "array",
			items: af.node,
			ser:   b.ser,
			deser: b.deser,
		}
		b.node = arrayNode
		if isFwdRef {
			// fwd-ref's resolved node is wired in finalize().
			// Capture pointers to all four wire-side slots that
			// depend on the resolved type so the fixup can patch
			// them once b.named[fwdRefName] becomes available.
			b.containerFixups = append(b.containerFixups, containerFixup{
				serItem:     &sa.serItem,
				deserItem:   &da.deserItem,
				setMinBytes: func(n int) { da.minItemBytes = n },
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
		// as-parsed values schema, so repoint it at the canonicalized child
		// or the canonical form (and fingerprint) diverges for any
		// map-of-wrapped-or-attribute-bearing-value schema.
		canonObj.Values = &mf.canon
		sm := &serMap{serItem: mf.ser}
		// minEntryBytes = 1 (empty-key length varint) + values' minimum
		// wire bytes. Matches deserArray.minItemBytes in spirit; bounds
		// block-count against remaining-buffer to prevent memory
		// amplification on hostile input.
		dm := &deserMap{deserItem: mf.deser, minEntryBytes: 1 + schemaMinBytes(mf.node)}
		// Same gate as the array case above: skip specialization when
		// values have a custom type, a logical type, OR a forward
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
				setMinBytes: func(n int) { dm.minEntryBytes = 1 + n },
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
		// Size 0 is legal: the spec requires only "an integer", and Java,
		// fastavro, and avro-rs all accept it — every value of a size-0
		// fixed is the empty byte string. We reject only negatives. The upper
		// bound is intentionally unbounded at parse, matching fastavro and
		// avro-rs (which defer the size to value/wire time): a size beyond the
		// actual datum simply fails at encode/decode where it is value- and
		// buffer-bounded. (Java is stricter — FixedSchema's ctor caps size at
		// Integer.MAX_VALUE-8 via SystemLimitException.checkMaxBytesLength — but
		// we follow the lenient majority. No parse-time path may allocate
		// proportional to this size; see maxFixedLogicalLen in json_decode.go.)
		if size < 0 {
			return fmt.Errorf("invalid fixed size %v", size)
		}
		// Per-direction suppression: built-in encoder preserved when the
		// user didn't provide Encode (CustomType.Encode docstring: "If
		// nil, the built-in logical type encoder is used"); built-in
		// decoder suppressed whenever ANY matching CustomType exists
		// (CustomType.Decode docstring: "If nil, the built-in logical
		// type handler is bypassed and the base Avro type decoder is
		// used directly"). A single-gate suppression on any match would
		// route a Decode-only CustomType for fixed.decimal /
		// fixed.duration / fixed.uuid onto raw serSize which can't
		// accept *big.Rat / avro.Duration as input.
		hasEnc := b.hasMatchingCustomTypeWithEncode("fixed", s.object.Logical)
		hasAny := b.hasMatchingCustomType("fixed", s.object.Logical)
		switch s.object.Logical {
		case "duration":
			// serDuration always emits 12 bytes, so it is only correct for a
			// size-12 fixed. validateLogical soft-drops a duration on a wrong
			// size, but the CustomType resurrection near the top of buildComplex
			// can restore it; without the size gate serDuration would write 12
			// bytes into a size != 12 fixed — a wire this schema's own
			// deserFixed{size} reader (and the JSON arm) cannot read.
			// logicalUnderlyingAccept is the same size predicate validateLogical
			// uses to soft-drop, so a resurrected wrong-size logical keeps the
			// raw serSize, matching the suppressed decoder.
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
			// names a different kind (so it does NOT match for suppression).
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
			// a size-16 fixed. validateLogical soft-drops a uuid on a wrong size,
			// but the CustomType resurrection can restore it; without the size
			// gate serFixedUUIDReflect would write 16 bytes into a size != 16
			// fixed — a wire this schema's own deserFixed{size} reader (and the
			// JSON arm) cannot read. logicalUnderlyingAccept is the same size
			// predicate validateLogical uses to soft-drop.
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
		b.registerNamed(o.Name, &namedType{ser: b.ser, deser: b.deser, node: nd})
	}
	return nil
}

// bareAliasShorts collects the aliases declared WITHOUT any dot, as
// written. A dotted alias (including the ".Name" null-namespace escape) is
// an explicit fullname spelling and matches only exactly, via the
// qualifyAliases output; an alias declared bare ALSO short-name-matches a
// writer type in any namespace — fastavro's raw-string tier
// (match_schemas: `w_unqual_name in r_aliases`), the permissive side of
// the reference behaviors (Java's applyAliases map is fullname-keyed and
// has no short tier). Distinguishing bare from qualified requires the
// DECLARED spelling: reconstructing it from the qualified form would
// over-widen an explicitly-written same-namespace alias ("a.Old" declared
// on a type in namespace a is indistinguishable from bare "Old" after
// qualification).
func bareAliasShorts(aliases []string) []string {
	var shorts []string
	for _, a := range aliases {
		if !strings.Contains(a, ".") {
			shorts = append(shorts, a)
		}
	}
	return shorts
}

// qualifyAliases fully qualifies alias names using the parent name's namespace.
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
			// Dotted aliases follow the names' dot rule, via the SAME
			// helper (leadingDotName): a single leading dot with a dotless
			// remainder is the explicit null-namespace escape (".x" is the
			// fullname "x", "." the empty name), never qualified into the
			// type's own namespace; any other dotted spelling is a fullname
			// VERBATIM. Java's Name constructor nulls the space only when
			// it is EMPTY (Schema.java ~1455: lastDot split, then
			// `if ("".equals(space)) space = null`), so ".a.b" keeps its
			// non-empty space ".a" and denotes ".a.b" as written — a name
			// only a lax-parsed writer can carry. Stripping any leading dot
			// here made alias ".a.b" match writer "a.b", a match neither
			// Java (space kept) nor fastavro (raw-string comparison) makes.
			short, _ := leadingDotName(a)
			out[i] = short
		default:
			out[i] = ns + a
		}
	}
	return out
}

// logicalUnderlyingAccept maps known logical types to the predicate
// that decides whether the carrier's Avro type is permitted. Mismatches
// soft-drop the logical (returning the bare underlying type) per spec
// and Java/fastavro/hamba parity — see the soft-drop comment in
// validateLogical for the rationale.
//
// "decimal" is handled inline in validateLogical because its precision/
// scale validation is too involved to fit a one-line predicate.
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
	// Duration on non-fixed, or fixed with size != 12, soft-drops.
	// Java's Duration.validate at LogicalTypes.java:323-327 throws
	// IllegalArgumentException for `type != FIXED || size != 12`;
	// fromSchemaIgnoreInvalid catches and drops. hamba's
	// parseFixedLogicalType at schema_parse.go:517 only matches
	// `ltyp == Duration && size == 12` and drops everything else.
	"duration": func(o *aobject) bool {
		return o.Type == "fixed" && o.Size != nil && int(*o.Size) == 12
	},
}

// logicalUnderlyingAcceptsObject reports whether o.Logical is spec-valid on
// o's Avro underlying (type + size) — the single predicate the primitive
// build's logical ser/deser selection gates on, identical for both directions.
// Returns false for "decimal"/"big-decimal" (their underlying validity is
// handled inline by validateLogical / the dedicated bytes-decimal build, not
// this table) and for any logical with no specialized name-keyed codec, which
// is correct: logicalSer/logicalDeser are nil there, so the gate's result is
// moot. A CustomType-resurrected wrong-kind logical returns false here, so the
// raw base ser/deser are kept — matching validateLogical's soft-drop.
func logicalUnderlyingAcceptsObject(o *aobject) bool {
	accept := logicalUnderlyingAccept[o.Logical]
	return accept != nil && accept(o)
}

func (o *aobject) validateLogical() error {
	switch o.Logical {
	case "":
		// No logical type. Stray precision/scale are inert metadata —
		// see the note below the switch.

	case "decimal":
		// Wrong underlying type is the one fall-back-on-mismatch case
		// the spec implies: an unknown logical type pinned on the wrong
		// primitive should not block schema parse. Precision/scale
		// constraints, on the other hand, are explicit Avro 1.12 rules;
		// a schema that violates them is malformed. twmb hard-rejects,
		// aligning with fastavro's parse_schema (which raises for
		// negative precision/scale and scale > precision — though its
		// truthiness guards skip the checks for 0-or-missing values,
		// observed 1.12.2). Java's LogicalTypes.Decimal.validate throws
		// for each violation (precision <= 0, scale < 0,
		// scale > precision — LogicalTypes.java:383-394), but at schema
		// parse that throw is caught by fromSchemaIgnoreInvalid and the
		// logical soft-drops to bare bytes/fixed rather than failing
		// the parse. Hard-rejecting beats Java's silent drop here: a
		// producer-declared decimal quietly becoming plain bytes is a
		// silent interop divergence.
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

	// Wrong-underlying-type soft-drop for every known logical type
	// mirrors the decimal arm above and matches the spec:
	//   "If a logical type is invalid, …then implementations should
	//    ignore the logical type and use the underlying Avro type."
	//   (apache/avro Specification/_index.md, "Logical Types")
	// Java's default Schema parser wraps each LogicalType.validate() in
	// fromSchemaIgnoreInvalid (Schema.java:1979 → LogicalTypes.java:120-194):
	// a thrown IllegalArgumentException for wrong underlying type is
	// caught and the logical is silently dropped, leaving the schema as
	// bare underlying. fastavro's LOGICAL_READERS/WRITERS.get(<rt-lt>)
	// returns None for unknown rt-lt combos and falls through to bare
	// underlying decode/encode (_read_py.py:662, _write_py.py:205/313).
	// hamba's parsePrimitiveLogicalType (schema_parse.go:205-222) and
	// parseFixedLogicalType (:514-524) return nil for any combo not in
	// the (typ, ltyp) switch, dropping the logical silently. Three
	// reference impls + spec text all agree on soft-drop; hard-rejecting
	// would be an interop break against Java/fastavro producers that
	// emit schema-evolution / legacy combos.
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

	// Leftover precision/scale — any placement other than the decimal arm
	// above, which consumes and validates them — are inert metadata, NOT a
	// parse error: the spec permits attributes it does not define as
	// metadata, Java's LogicalTypes.fromSchemaImpl never consults
	// precision without a logicalType (extra attributes become props), and
	// fastavro accepts every such placement (executed 1.12.2). They
	// surface as custom properties (see decimalConsumesPrecisionScale) and
	// no wire codec reads them. Rejecting here used to make twmb disagree
	// with itself: the same stray keys parsed when an unknown logical or a
	// wrong-carrier decimal soft-dropped above, and the FIELD level always
	// kept them as inert props.
	return nil
}

// maxDecimalDigits returns the maximum number of decimal digits that fit in
// a two's-complement signed integer of the given byte size:
// floor(log10(2^(8*size-1) - 1)).
func maxDecimalDigits(size int) int {
	if size <= 0 {
		return 0
	}
	// Cap size before the bit multiply. A fixed size is an int that can
	// exceed 2^60 on a 64-bit build (twmb accepts sizes Java's int32 can't),
	// where 8*size-1 wraps the platform int negative and the capacity comes
	// back negative — falsely rejecting a valid precision. Any size past
	// decimalScaleLimit yields a capacity far above that limit, and precision
	// itself is capped at decimalScaleLimit upstream, so the exact digit count
	// is irrelevant there: returning the ceiling both avoids the wrap and
	// keeps the comparison correct (precision <= decimalScaleLimit can never
	// exceed it).
	if size > decimalScaleLimit {
		return decimalScaleLimit
	}
	bits := 8*size - 1 // sign bit excluded
	// log10(2^bits - 1) ≈ bits * log10(2)
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

// unmarshalDefault parses a field's raw JSON default. Uses
// json.Decoder.UseNumber() so that numeric literals are preserved as
// json.Number rather than rounded through float64 — int64 / long
// defaults > 2^53 would otherwise silently lose precision (e.g. 9007199254740993
// → 9007199254740992).
func unmarshalDefault(raw json.RawMessage) any {
	var dv any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	// Cannot fail: raw is preserved from the initial parse and is valid JSON.
	_ = dec.Decode(&dv)
	return dv
}

// nodeAwaitsForwardRef reports whether node has any child encodeDefault would
// traverse that is not yet whole. encodeDefault recurses into items / values /
// fields / branches and dereferences each child's kind, so two child shapes
// must defer the default pipeline to finalize:
//
//   - a nil child — a not-yet-wired forward reference to a named type declared
//     later in the schema; dereferencing it is a runtime nil-pointer panic.
//   - a non-nil but partial record/error node still under construction (in
//     b.building) — a self- or mutual-recursive reference back into a record
//     whose field loop has not finished. Its fields slice holds only the
//     fields declared before the current one, so encoding inline against it
//     silently drops the rest and emits truncated, non-decodable default
//     bytes. Deferring re-runs the encode at finalize once the node is whole.
//
// When this returns true at build time, the caller must defer the whole
// resolve+encode-default pipeline to finalize (after the container / field
// fixups have wired the descendants and every in-construction record has
// completed) rather than run it inline.
//
// Cycle-safe via a seen set: a back-edge to an already-built node (a recursive
// schema whose referenced record finished building) is a wired pointer not in
// b.building, so it is correctly not treated as pending.
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

// resolveFieldDefaultValue runs the validate + convertDefaultBytes half of
// the default pipeline against a coerced default value and its resolved
// schemaNode, recording the (converted) default on the deser/field metadata.
// It deliberately does NOT encode the binary defaultBytes — that is
// [encodeFieldDefaultBytes], which must run only after every field's default
// VALUE is recorded, because encodeDefault fills absent nested record fields
// from their f.defaultVal. Returns the converted value for the caller to hand
// to encodeFieldDefaultBytes.
//
// convertDefaultBytes maps bytes/fixed string defaults to []byte so the JSON
// encoder sees the wire form directly and its logical-type-aware arms can't
// misinterpret the string as decimal / UUID / etc. Walks the resolved
// schemaNode tree (not the aschema canon) so name-references — forward and
// backward — follow into the real type.
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
// VALUE first (encodeDefault reads sibling/nested f.defaultVal for absent
// fields).
func encodeFieldDefaultBytes(defaultVal any, node *schemaNode, fieldName string, srf *serRecordField) error {
	defaultBytes, deferred, err := encodeDefaultCharged(defaultVal, node)
	if err != nil {
		return fmt.Errorf("record field %q: encoding default: %v", truncForError(fieldName), err)
	}
	srf.defaultBytes = defaultBytes
	srf.hasDefault = true
	// Recorded, not returned: a default that cannot be WRITTEN must not stop
	// the schema PARSING, because a reader that drops this field never writes
	// it and reads such data correctly today. The encode-side consumers of
	// defaultBytes surface this at the moment the default would reach the wire.
	// The verdict comes from INSIDE the walk, where each leaf asked the same
	// predicate its serializer asks — asking here instead would answer for the
	// field's kind and miss every cap nested inside a container.
	srf.defaultErr = deferred
	return nil
}

// applyResolvedDefault runs the full validate + convertDefaultBytes +
// encodeDefault pipeline for a coerced default value against its resolved
// schemaNode, writing the result into the three field-slot triple
// (deserRecordField, fieldNode, serRecordField). fieldName is used for error
// context.
//
// Used by the build-time path for fields whose type tree is fully resolved
// (no pending forward reference — [nodeAwaitsForwardRef] is false). Fields
// with an unresolved forward-referenced descendant defer to finalize via the
// split resolveFieldDefaultValue / encodeFieldDefaultBytes pair so encodeDefault
// never dereferences a not-yet-wired child node.
func applyResolvedDefault(defaultVal any, node *schemaNode, fieldName string,
	drf *deserRecordField, fn *fieldNode, srf *serRecordField,
) error {
	converted, err := resolveFieldDefaultValue(defaultVal, node, fieldName, drf, fn)
	if err != nil {
		return err
	}
	return encodeFieldDefaultBytes(converted, node, fieldName, srf)
}

// unmarshalAnyPreservePrecision parses raw JSON into a Go value with the
// same shape as encoding/json's default any decode (map[string]any, []any,
// string, bool, nil for the structural pieces) BUT preserves integer
// precision: integer-valued JSON numbers materialize as int64 instead of
// float64, lifting the silent 2^53 round-down that bare
// json.Unmarshal(&v any) applies. Fractional / exponent-form numbers
// stay float64 since their natural domain is float64-precision anyway.
// Integers that overflow int64 are returned as json.Number so the
// caller still has arbitrary-precision access via .String() / .Int().
//
// Used by Schema metadata surfaces — schema parsing for record-level
// extras (forwarded to schemaNode.props → SchemaNode.Props for
// CustomType callbacks) and Schema.Root()'s re-parse — where the
// previous bare-Unmarshal silently rounded JSON ints > 2^53. The Avro
// internal encode/decode path was already protected via unmarshalDefault
// (which UseNumber-decodes and pushes json.Number through the
// defaultAsInt32/Int64/Float64 dispatch); this helper extends the
// guarantee to the user-facing metadata API. See
// TestRegression_SchemaExtraNumberPrecisionLoss.
func unmarshalAnyPreservePrecision(raw []byte) (any, error) {
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return normalizeJSONValue(v), nil
}

// normalizeJSONValue recursively walks a value parsed via UseNumber and
// converts json.Number to int64 / float64 / json.Number per
// normalizeJSONNumber. Maps and slices are walked in place; other types
// pass through.
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

// normalizeJSONNumber resolves a UseNumber-preserved json.Number to the
// idiomatic Go type by VALUE, not by literal syntax:
//
//   - Exact integer fitting int64 → int64. Applies to `42`, `1.5e1`
//     (= 15), `9.5e17` — the literal's syntactic shape (`.`/`e`)
//     doesn't matter.
//   - Exact integer exceeding int64, written in pure-digit syntax
//     → json.Number (preserves arbitrary precision).
//   - Non-integer, OR exact integer exceeding int64 in fractional/
//     exp-form syntax → float64 (parseFloatAcceptOverflow handles
//     ±Inf for magnitudes overflowing float64's exponent range,
//     e.g. "1e1000" → +Inf).
//
// The ±Inf-from-overflow path routes through [parseFloatAcceptOverflow]
// so the metadata-API observability surface (Schema.Root().Props,
// Fields[].Default, Fields[].Props, CustomType callbacks' *SchemaNode.Props)
// agrees with the encode/decode/schema-parse-time arms on the
// ErrRange-with-Inf predicate. Java's Jackson
// DoubleNode(Double.parseDouble("1e1000")) produces +Inf at the
// metadata layer; fastavro's float("1e1000") → inf via Python json.
//
// Value-based dispatch (vs syntax-based) is what eliminates a
// metadata-vs-wire divergence at the int64 boundary: under syntax-
// based dispatch, "9.2233720368547758e18" against a long field had
// wire = int64(9223372036854775800) but metadata = float64(rounded
// to 2^63); the two surfaces disagreed about the default value.
// Value-based dispatch normalizes both to int64(9223372036854775800).
func normalizeJSONNumber(n json.Number) any {
	s := string(n)
	// Integer-syntax fast path: no decimal point, no exponent — strconv
	// alone is enough, no need to spin up a big.Rat.
	if !strings.ContainsAny(s, ".eE") {
		if i, err := n.Int64(); err == nil {
			return i
		}
		// Overflows int64; preserve as json.Number for arbitrary precision.
		return n
	}
	// Fractional or exponent syntax. Value-based dispatch: parse with
	// arbitrary precision and check if the value is an exact integer.
	// Without this, a literal like "1.5e1" (= 15) or "9.5e17"
	// (= 950000000000000000) surfaces as float64 — silently rounding for
	// values exceeding float64's 53-bit mantissa and diverging from the
	// wire-encode pipeline's exact-integer parse for integer-defaultable
	// schemas. Going through boundedRatFromString lets metadata and wire
	// agree on the same int64 value regardless of how the user wrote the
	// literal.
	if r, ok, err := boundedRatFromString(s); err == nil && ok && r.IsInt() {
		// Negative zero in float syntax ("-0.0", "-0e5") is the one exact
		// integer whose IEEE sign the int64 collapse would erase (a big.Rat
		// has no signed zero). The wire encoder parses it via ParseFloat and
		// preserves the sign, and Java's Jackson surfaces a DoubleNode(-0.0);
		// keep the sign by falling through to parseFloatAcceptOverflow below
		// (→ -0.0) so the metadata Default matches the wire and re-parses
		// sign-stable. Integer syntax ("-0") stays integer 0 (no sign) above.
		negZero := r.Sign() == 0 && s != "" && s[0] == '-'
		if !negZero {
			if bi := r.Num(); bi.IsInt64() {
				return bi.Int64()
			}
		}
		// Exact integer beyond int64 range. Two sub-cases:
		//   - Magnitude fits float64's exponent → surface as float64,
		//     matching what an encode against a float/double schema
		//     emits on the wire (lossy by destination).
		//   - Magnitude overflows float64 → parseFloatAcceptOverflow
		//     returns ±Inf, matching the wire encoder's silent
		//     overflow-to-Inf path.
	}
	if f, err := parseFloatAcceptOverflow(s, 64); err == nil {
		return f
	}
	return n
}

// defaultAsInt32 / defaultAsInt64 / defaultAsFloat extract a numeric
// default. After unmarshalDefault, a JSON number arrives as json.Number
// (full precision); a few callers also pass float64 (e.g. round-tripped
// through coerceDefault). Float-defaulted-from-string is accepted for
// float / double (Java parser leniency).
//
// All three are precision-aware:
//   - defaultAsInt32 / defaultAsInt64 reject overflow via
//     parseInt{32,64}Lenient (which uses boundedRatFromString for
//     arbitrary-precision parsing).
//   - defaultAsFloat rejects integer-form magnitudes exceeding the
//     target's mantissa precision (1<<24 for float, 1<<53 for double)
//     so the schema's declared default is reachable at runtime via
//     the equivalent json.Number / typed-int encode arms, which apply
//     the same predicate.

// numericDefault extracts a typed integer default. After
// unmarshalDefault, a JSON number arrives as json.Number (full precision);
// callers may also pass float64 (e.g. round-tripped through coerceDefault).
// Shared body of defaultAsInt32 / defaultAsInt64.
//
// Whole-number values written in fractional or exponent form (e.g. "1.0",
// "4e1") are accepted, matching twmb's existing "Whole-number floats
// encode against int/long schemas" intentional divergence — the same
// rationale (encoding/json.Unmarshal produces float64 for every JSON
// number; rejecting forces explicit conversion) applies to JSON-encoded
// schema defaults written by humans or codegen tools.
//
// Precision guard: rejects only the subset where the metadata-API path
// (normalizeJSONNumber, which surfaces fractional/exponent literals as
// float64) would round to a different integer than the wire-fill path
// (parseInt{32,64}Lenient via big.Rat, precision-exact). For "1.0" and
// "4e1" the float64 representation equals the parsed int exactly — both
// surfaces report the same value, no divergence, accept. For
// "9.2233720368547758e18" the float64 form rounds up beyond the parsed
// int (wire=9223372036854775800 vs metadata-as-float64≈9.223372036854776e+18
// → int64(9223372036854775808)+, a >7-unit mismatch) — reject so the
// schema can't carry a default whose metadata-vs-wire values disagree.
//
// Diverges from Java's isIntegralNumber() gate at Schema.java LONG/INT
// cases and fastavro's isinstance(default, int) check, which both reject
// "1.0" outright; twmb's existing runtime-arm acceptance of json.Number
// fractional forms (TestEncodeJSONCoercion) is already a Java/fastavro
// divergence, and tightening only at schema-parse without tightening
// runtime would produce a within-twmb encode-vs-parse asymmetry. The
// precision guard preserves the cross-impl interop concern (wire bytes
// match Java/fastavro for accepted defaults) while keeping the ergonomic
// acceptance Go users expect.
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

// int64FitsInt32 narrows n to int32 with bounds check. Shared by
// numericDefault's int64/int32 arms (for defaultAsInt32 callers) and
// keeps the bounds rule in one place.
func int64FitsInt32(n int64) (int32, error) {
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0, fmt.Errorf("integer %d overflows int32", n)
	}
	return int32(n), nil
}

// int64Identity is numericDefault's fromInt64 for the int64 (long)
// target — pass through unchanged.
func int64Identity(n int64) (int64, error) { return n, nil }

func defaultAsInt32(val any) (int32, error) {
	return numericDefault(val, parseInt32Lenient, floatFitsInt32, int64FitsInt32)
}

func defaultAsInt64(val any) (int64, error) {
	return numericDefault(val, parseInt64Lenient, floatFitsInt64, int64Identity)
}

// floatMantissaLimit returns the largest integer magnitude exactly
// representable in float32 (bitSize=32) or float64 (bitSize=64) —
// the mantissa bound used for float→int whole-number precision-loss
// checks at [floatFitsInt32From] and [floatFitsInt64From]. The reverse
// direction (int→float) is lossy by destination per Java/fastavro parity;
// see [appendAvroFloat32] / [appendAvroFloat64].
func floatMantissaLimit(bitSize int) int64 {
	if bitSize == 32 {
		return 1 << 24
	}
	return 1 << 53
}

// intFitsFloat reports whether an int64 value of magnitude n can be
// represented exactly in the target float (float32 or float64). Used
// by decode-time arms that write a long-wire value into a Go float
// target: the user explicitly chose a smaller-precision Go type, so we
// surface the precision loss rather than silently rounding. Encode-time
// arms use the lossy-destination policy and silently round; see
// [appendAvroFloat32] / [appendAvroFloat64].
func intFitsFloat(n int64, bitSize int) (float64, error) {
	lim := floatMantissaLimit(bitSize)
	if n < -lim || n > lim {
		return 0, fmt.Errorf("integer %d overflows float%d exact precision", n, bitSize)
	}
	return float64(n), nil
}

// parseFloatAcceptOverflow is [strconv.ParseFloat] with one twist:
// ErrRange-with-±Inf is treated as success (Java/fastavro return the
// Inf; the wire format permits it). Other parse errors propagate.
//
// Length cap: strconv.ParseFloat is O(n) and processes ~30-50ms per MiB
// of input. Schema parse for a record with one float/double field
// calls this helper twice (validateDefault + encodeDefault), so a 1 MiB
// hostile default literal can drive ~130ms per parse — past the
// audit's 100ms DoS threshold. Legitimate float64 literals (including
// hex-float and max-exponent forms) fit in well under 350 chars;
// maxParseFloatLen=1024 is generous and rejects hostile input in O(1).
// Mirrors the same length-cap pattern as boundedRatFromString
// (deser.go:670, maxRatInputLen=128KiB) and parseInt64Lenient
// (ser.go:559, maxInt64LenientLen=64). The helper is the single
// source of truth for every ParseFloat-on-user-input caller across all
// axes: binary encode (jsonNumberToFloat), JSON encode
// (jsonCoerceToFloat64), schema-parse (defaultAsFloat), metadata-API
// (normalizeJSONNumber), and JSON decode (decodeJSONFloat) — the last
// reaching it via parseJSONNumberAsFloat. bitSize is 64 for every axis
// except the JSON decode of a "float" schema, which passes 32 to parse
// at float32 precision directly (single rounding, no float64→float32
// double-rounding shift).
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

// maxParseFloatLen caps the input length parseFloatAcceptOverflow
// forwards to [strconv.ParseFloat]. The longest legitimate float64
// literal (max-exponent + mantissa in scientific form, hex-float
// with 17-digit significand and 3-digit exponent) fits in ~320 chars;
// 1024 leaves comfortable headroom and remains O(1)-rejectable on
// hostile multi-MB inputs. See helper's docstring for full rationale.
const maxParseFloatLen = 1024

// defaultAsFloat extracts a numeric default for a float or double field.
// Accepts json.Number / float64 / int64 / int32 — i.e., the Go types
// produced by UseNumber-parsed JSON literals plus any post-coerce
// numerics. Does NOT accept Go string: the spec 1.12 §"Record" default-
// values table requires the JSON type of a float/double default to be
// `number`, never a JSON string. The narrow Java-deployed exception —
// Schema.java:1899-1902's parseField text→DoubleNode coercion for outer
// FLOAT/DOUBLE field types — is handled UPSTREAM in [coerceDefault] so
// the string never reaches this validator. Union branches and any
// downstream caller (encodeDefault, validateLeaf, the metadata-side
// branchAcceptsDefault path) see only post-coerce typed values; a
// string here is invalid by construction and rejected via the default
// `expected number, got %T` error.
//
// Encoding into a float/double field is lossy by destination — int64/
// int32 inputs exceeding the mantissa precision silently IEEE-round
// (matches Java's Schema.parseField text→DoubleNode coercion and
// fastavro's float()). The float32 narrowing to ±Inf happens at the
// caller's float64 → float32 cast.
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
		// A float32 reaches this predicate only on the METADATA side: the
		// union-branch selector branchAcceptsDefault coerces a container's
		// nested float/double child through coerceMetadataDefault before the
		// accept-check, and coerceMetadataDefault narrows a "float" schema to
		// float32 (schema_node.go). float32→float64 widening is exact, so the
		// value is a valid float default. The wire path never reaches here with
		// a float32 (coerceDefault yields float64 and the parsed default tree
		// has no float32), so this arm does not change wire or parse behavior.
		return float64(v), nil
	}
	return 0, fmt.Errorf("expected number, got %T", val)
}

// firstUnionBranchAcceptingDefault returns the first union branch whose
// validateDefault accepts val, or nil if none match. Shared by
// coerceDefault and walkDefault's union arms — both implement Avro's
// "first matching branch wins" default-resolution rule (1.12 relaxed
// from "first branch" to "any branch," with deterministic first-match
// tie-break). Keeping the iteration in one place ensures coerceDefault
// and walkDefault stay in lockstep if validateDefault's semantics
// change. coerceMetadataDefault (schema_node.go) uses the analogous
// branchAcceptsDefault predicate on the *SchemaNode public type — the
// pattern is the same but the type split prevents direct reuse.
//
// String defaults are matched ONLY by branches whose Avro type's
// permitted JSON type is `string` per spec 1.12 §"Record" default-
// values table (string, bytes, enum, fixed). Numeric branches
// (int, long, float, double) reject string defaults at this layer
// — [defaultAsFloat] has no string-acceptance arm; Java's
// parseField text→DoubleNode coercion fires only for the OUTER
// FLOAT/DOUBLE field type (handled in [coerceDefault] below) and
// never for union branches.
func firstUnionBranchAcceptingDefault(val any, node *schemaNode) *schemaNode {
	for _, branch := range node.branches {
		// validateDefault coerces in place: validateLeaf's record/array/map
		// arms rewrite fields via coerceDefault (e.g. a string "5" -> float64
		// against a double field, the documented outer-float carveout). Validate
		// a COPY so a FAILED branch's partial coercion cannot leak into the next
		// branch's check — otherwise acceptance is order-dependent (a later
		// string-typed branch sees a float64 a prior failed branch left behind),
		// violating Avro 1.12's order-independent "default matches any branch"
		// (Java isValidDefault is anyMatch over an immutable node). The caller
		// re-coerces the original val against the returned branch, so the
		// selected branch and its coerced value are unchanged for any default
		// that already parsed.
		if validateDefault(deepCopyTree(val), branch) == nil {
			return branch
		}
	}
	return nil
}

// coerceDefault converts string default values to float64 when the
// field type is literally float or double. Modeled on Java's parseField
// at Schema.java:1899-1902, which special-cases TextNode → DoubleNode
// coercion for a FLOAT or DOUBLE field type. Spec 1.12 §"Record"
// default-values table marks JSON string as invalid for float/double
// defaults; the Java-deployed coercion is an interop carveout preserved
// here for legacy Java-generated schemas. avro-rs and goavro do not
// implement this coercion.
//
// A DIRECT scalar float/double UNION branch does NOT coerce: this function
// only transforms a node whose kind is literally float/double, and a union
// node recurses into its first validateDefault-accepting branch, where the
// scalar float/double leaf has no string arm — so a numeric-only union with
// a string default rejects at parse (["double"] default "5" rejects), and
// ["double","string"] default "5" picks the string branch. Matches
// Java/avro-rs/goavro (see NOT_BUGS #10).
//
// A float/double field NESTED inside a record/array/map DOES coerce, even
// when that container is a union branch: validateLeaf's record/array/map
// arms (schema.go) call coerceDefault on each child, so a string "5" in a
// nested double field becomes float64(5) before the accept-check and the
// container branch is selected. This is a deliberate leniency BEYOND Java
// (Java's parseField coercion is outer-field-only, so Java rejects a nested
// string-numeric default and selects a string-accepting branch instead) —
// twmb leans permissive (cross-impl rule 2). The metadata-side branch
// selector (branchAcceptsDefault, schema_node.go) applies the SAME coercion
// via coerceMetadataDefault, so the union branch reported by Root().Default
// matches the wire auto-fill on both binary and JSON: there is NO
// metadata↔wire divergence. Do NOT re-file "twmb coerces nested
// string-double defaults where Java doesn't" or propose removing the nested
// coercion — it is intentional and both surfaces agree; revisit only on a
// real interop breakage with evidence. Pinned by
// TestRegression_UnionContainerNestedFloatDefaultSelectionMatchesWire.
//
// Walks *schemaNode so name-referenced nested fields coerce too (the
// resolved type tree, not the canon — name-refs lose type info on the
// canon side).
func coerceDefault(val any, node *schemaNode) any {
	if node == nil {
		return val
	}
	if node.kind == "union" {
		// First validateDefault-accepting branch wins; recurse so the
		// coerced value matches that branch's natural Go type. For
		// string defaults, no numeric branch accepts (defaultAsFloat
		// has no string arm), so this picks a string-
		// accepting branch (string/bytes/enum/fixed) or returns nil
		// — schema parse then fails via validateDefault.
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
	// Java parity (Schema.java:1899-1902): coerce text → float64 for
	// the outer single-field float/double case. Direct call to
	// parseFloatAcceptOverflow (not defaultAsFloat) because
	// defaultAsFloat is the strict validator used by union branches
	// and downstream encode-time arms; the lenient coerce is
	// specifically the parseField-special-case behavior, scoped to
	// this single call site. If parsing fails (syntax error), leave
	// the original string so validateDefault produces the canonical
	// error message.
	if f, err := parseFloatAcceptOverflow(s, 64); err == nil {
		return f
	}
	return val
}

// walkDefault drives the (val, node) recursion shared by the
// default-tree walkers. visit is called once per non-union node and
// may mutate val; for union nodes walkDefault picks the first
// validateDefault-accepting branch (skipping visit at the union
// itself) and recurses into the matched branch. If no union branch
// matches, walkDefault returns the canonical "default does not match
// any union branch" error so callers that don't care (the mutator
// walker convertDefaultBytes) can discard it while validateDefault
// surfaces it.
//
// Container arms wrap nested errors with "field %q:", "array element
// %d:", or "map key %q:" so the per-element error path is identical
// across walkers.
//
// Caller contract: visit MUST be idempotent. The union arm calls
// validateDefault to pick a branch and then re-invokes visit at every
// node of the matched branch — a non-idempotent visit (e.g. one that
// increments a counter) would double-fire at every union depth.
//
// Walks *schemaNode (the resolved type tree) so name-references —
// forward and backward — follow into the real type. Returns
// immediately for a nil node so fwd-ref-deferred validation is a
// no-op.
func walkDefault(val any, node *schemaNode, visit func(any, *schemaNode) (any, error)) (any, error) {
	if node == nil {
		return val, nil
	}
	if node.kind == "union" {
		// Per Avro 1.12 the default may match any branch, not only the
		// first. See AVRO-3649 / PR apache/avro#2503.
		//
		// Branch matcher is validateDefault (via
		// firstUnionBranchAcceptingDefault, shared with coerceDefault):
		// a structural-only check (e.g. "is val a string?") can pick a
		// fixed:N branch on a string default whose rune-count doesn't
		// fit, mutate it into a length-N []byte that no branch can
		// encode, and surface as "union default does not match any
		// branch" at encodeDefault time even though validateDefault
		// accepted the schema. validateDefault is idempotent so
		// re-running it here is safe.
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

// convertDefaultBytes walks a parsed-then-validated default value and
// converts string defaults to []byte for bytes/fixed schema nodes,
// recursively descending into records/arrays/maps/unions. The Avro
// JSON spec specifies that bytes/fixed defaults are codepoint-mapped
// strings; binary encodeDefault already takes the codepoint route via
// avroJSONBytesToBytes, while the JSON encoder's appendAvroJSON
// logical-type-aware arms (decimal, big-decimal, uuid) would otherwise
// misinterpret the string semantically. Storing the wire-form []byte
// up front makes both encode paths agree without requiring per-arm
// special cases.
//
// Called after validateDefault has succeeded for non-fwd-ref fields;
// for fwd-ref fields validation is deferred and the conversion is
// best-effort. The walkDefault union-no-match error is discarded —
// validateDefault would have caught it for non-fwd-ref defaults, and
// fwd-ref defaults shouldn't surface conversion-time errors.
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
// > 0xFF — the Avro JSON-bytes / JSON-fixed default form maps each
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

// validateDefault checks that a parsed JSON default value is
// compatible with the given Avro schema. Drives walkDefault with a
// validateLeaf visit that does the per-kind primitive validation and
// the container-shape checks; the structural recursion + union
// branch-matching + per-element error-path wrapping live in
// walkDefault so the validate / convert / coerce walkers can't drift
// on those invariants.
//
// Mutates record/array/map structures in place via coerceDefault
// (called from the validateLeaf record/array/map arms), propagating
// float-from-string coercions to nested fields reached through
// name-refs. Returns nil for a nil node — fwd-refs defer validation
// to finalize.
func validateDefault(val any, node *schemaNode) error {
	_, err := walkDefault(val, node, validateLeaf)
	return err
}

// validateLeaf is the per-node visit for validateDefault: primitive
// kind validation, plus container-shape checks + per-field coercion
// (walkDefault handles the actual recursion).
// defaultObjectShape asserts val is a non-null JSON object for an Avro record
// or map default, returning the canonical "expected object for <kind> default"
// error. Shared by the parse-time validator (validateLeaf) and the wire encoder
// (encodeDefault, resolve.go) — two cross-path sites for one shape rule — so
// the user-visible error cannot drift between them. Only the shape assertion is
// shared; each caller keeps its own post-assertion work (validateLeaf coerces
// fields in place, encodeDefault emits wire bytes).
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
		// Unconditional membership: a non-nil enum node always carries its
		// final symbols (definitions build them in one shot; forward refs
		// are nil until finalize and defaults resolve post-wiring). An
		// empty enum therefore rejects every default. Membership here is
		// deliberately STRICTER than the references' parse-time checks:
		// Java validates containment only for the enum-level "default"
		// attribute (EnumSchema's constructor, Schema.java:1100), while
		// its FIELD-default validation accepts any textual value
		// (isValidDefault's ENUM arm is isTextual() only,
		// Schema.java:1755-1759 — a non-member surfaces later, at
		// default-encode time), and fastavro 1.12.2 parses a non-member
		// enum field default outright (observed). twmb fails fast at
		// parse because a non-member default can never encode. The
		// membership check also makes union-default branch selection
		// skip an empty/non-member enum branch so a later branch can
		// accept.
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
		// Record branch (synthesizing an empty map + relying on per-
		// field defaults) instead of falling through to null —
		// encodeDefault would emit Record(field-defaults) wire bytes
		// where null was intended.
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
