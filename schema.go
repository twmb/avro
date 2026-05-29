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

	// Per-schema custom type overlay. Keyed by *schemaNode so the
	// shared node is not mutated — different schemas parsed with
	// different custom types get different overlays.
	custom map[*schemaNode]*customWiring
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
}

// schemaNode preserves full schema metadata that canonical form strips:
// aliases, defaults, enum defaults, and links to compiled ser/deser.
type schemaNode struct {
	kind        string        // "null","boolean","int","long","float","double","bytes","string","record","enum","array","map","fixed","union"
	name        string        // fully-qualified name (named types only)
	aliases     []string      // named type aliases (fully qualified)
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
		named: make(map[string]*namedType),
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
}

func parse(schema string, b *builder) (*Schema, error) {
	var orig aschema
	if err := json.Unmarshal([]byte(schema), &orig); err != nil {
		return nil, fmt.Errorf("invalid schema: %w", boundJSONErrorEcho(err))
	}
	if err := b.build("", &orig); err != nil {
		return nil, err
	}
	if err := b.finalize(); err != nil {
		return nil, err
	}
	s := &Schema{
		ser:    b.ser,
		deser:  b.deser,
		c:      b.canon,
		node:   b.node,
		full:   schema,
		custom: b.custom,
	}
	s.soe[0] = 0xC3
	s.soe[1] = 0x01
	h := NewRabin()
	h.Write(s.Canonical())
	binary.LittleEndian.PutUint64(s.soe[2:], h.Sum64())
	return s, nil
}

// Canonical returns the Parsing Canonical Form of the schema, stripping
// doc, aliases, defaults, and other non-essential attributes. The result
// is deterministic and matches Java's reference output byte-for-byte,
// so [Schema.Fingerprint] values are interoperable across implementations.
func (s *Schema) Canonical() []byte {
	// Use json.Encoder with HTML escaping disabled: PCF requires raw
	// UTF-8 (the "STRINGS" rule), so <, >, & must NOT be escaped to
	// \uXXXX. Java's PCF emitter does not escape them; json.Marshal
	// does, which would silently diverge fingerprints.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s.c)
	out := buf.Bytes()
	if n := len(out); n > 0 && out[n-1] == '\n' {
		out = out[:n-1] // strip Encoder's trailing newline
	}
	// Go's json.Encoder unconditionally escapes U+2028 and U+2029 even
	// with SetEscapeHTML(false) — those code points trigger browser-side
	// JS parser pitfalls that Go's stdlib defends against. PCF requires
	// raw UTF-8; replace the six-byte JSON escapes with their UTF-8
	// encodings. Safe: a valid JSON string cannot contain an unescaped
	// backslash, so the literal six-byte `\uXXXX` byte sequence only
	// appears as an escape we want to undo.
	out = bytes.ReplaceAll(out, []byte{'\\', 'u', '2', '0', '2', '8'}, []byte{0xe2, 0x80, 0xa8})
	out = bytes.ReplaceAll(out, []byte{'\\', 'u', '2', '0', '2', '9'}, []byte{0xe2, 0x80, 0xa9})
	return out
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

// isNullableUnion reports whether s is a union whose first branch is "null".
// Per the Avro spec, such unions implicitly default to null.
func (s *aschema) isNullableUnion() bool {
	return len(s.union) >= 2 && s.union[0].primitive == "null"
}

func (s aschema) MarshalJSON() ([]byte, error) {
	switch {
	case len(s.primitive) != 0:
		return json.Marshal(s.primitive)
	case s.object != nil:
		return json.Marshal(s.object)
	case len(s.union) != 0:
		return json.Marshal(s.union)
	default:
		return nil, errors.New("invalid empty schema")
	}
}

func (s *aschema) UnmarshalJSON(data []byte) error {
	// Reset state on every call. encoding/json invokes UnmarshalJSON
	// once PER duplicate key at the same level (e.g. {"tYpe":"int",
	// "tYpe":[]} ends up calling this twice on the same *aschema). The
	// last-wins contract requires the later call to fully replace the
	// earlier state, not merge into it — without this reset a string
	// primitive then a union would leave both s.primitive AND s.union
	// populated, and build()'s primitive-priority dispatch would diverge
	// from Root()'s map-decode last-wins.
	*s = aschema{}
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return errors.New("invalid empty schema")
	}
	switch data[0] {
	case '"':
		return json.Unmarshal(data, &s.primitive)
	case '{':
		// Decode directly into the struct. encoding/json struct decode
		// and map decode are both documented and implemented as
		// last-wins for duplicate keys in modern Go, matching Java's
		// Jackson and Python's json — no re-Marshal-for-dedup needed.
		// Avoiding the re-Marshal is what makes Parse cost O(n) over
		// the schema bytes instead of O(n²) over nested schema depth.
		if err := json.Unmarshal(data, &s.object); err != nil {
			return err
		}
		// Capture extra properties not in the struct tags.
		// encoding/json matches struct field names case-insensitively,
		// so keys like "tYpe" parse into Type and should not also land
		// in extras.
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(data, &raw); err != nil {
			return err
		}
		for k := range raw {
			if schemaReservedKeyCI(k) {
				continue
			}
			if s.object.extra == nil {
				s.object.extra = make(map[string]any)
			}
			v, err := unmarshalAnyPreservePrecision(raw[k])
			if err != nil {
				// raw[k] came from a successful map[string]json.RawMessage
				// decode above, so this is unreachable for well-formed input.
				// Silently drop the property rather than fail the whole
				// schema parse — extra props are advisory metadata.
				continue
			}
			s.object.extra[k] = v
		}
		return nil
	case '[':
		return json.Unmarshal(data, &s.union)
	default:
		return errors.New("invalid schema")
	}
}

type afield struct {
	Name string   `json:"name"`
	Type *aschema `json:"type"`

	// In canonical form, the following are stripped.

	Aliases []string        `json:"aliases,omitempty"`
	Default json.RawMessage `json:"default,omitempty"`
	Order   string          `json:"order,omitempty"`

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

func (f *afield) UnmarshalJSON(data []byte) error {
	// Direct struct decode — last-wins for duplicate keys. See
	// aschema.UnmarshalJSON for the rationale.
	type plain afield
	if err := json.Unmarshal(data, (*plain)(f)); err != nil {
		return err
	}
	// Detect the "flat" field format: "type" is a string naming a complex
	// type (e.g. "enum") and complex-type attributes (e.g. "symbols") live
	// alongside the field's own keys. When detected, lift those attributes
	// into a nested type object so the rest of the parser sees the
	// canonical form.
	if f.Type == nil || f.Type.primitive == "" {
		// No complex-type lift possible. Still need to handle the
		// field-level logicalType case for unions and already-object
		// type forms.
		f.liftFieldLogicalIntoType()
		return nil
	}
	tp := f.Type.primitive
	if tp != "enum" && tp != "array" && tp != "map" && tp != "record" && tp != "error" && tp != "fixed" {
		// Primitive type with possible field-level logicalType — the
		// Java/JDBC Avro idiom. Lift the annotation into the type so
		// the parser sees the canonical nested form.
		f.liftFieldLogicalIntoType()
		return nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	needsLift := false
	for key, forType := range afieldComplexKeys {
		if _, ok := raw[key]; ok && (forType == tp || (key == "fields" && tp == "error")) {
			needsLift = true
			break
		}
	}
	if !needsLift {
		return nil
	}
	// Build a JSON object for the type schema from the field's own keys,
	// excluding field-only keys ("default", "order", "aliases").
	// For non-named types (array, map), also exclude "name" and
	// "namespace" since those belong to the field, not the type.
	named := tp == "record" || tp == "error" || tp == "enum" || tp == "fixed"
	typeObj := make(map[string]json.RawMessage, len(raw))
	for k, v := range raw {
		switch k {
		case "default", "order":
			// Field-only keys, do not propagate.
		case "aliases":
			// In the flat format, aliases belong to the field, not
			// the type. Named types that need aliases can use the
			// nested format.
		case "name", "namespace":
			if named {
				typeObj[k] = v
			}
		default:
			typeObj[k] = v
		}
	}
	typeJSON, err := json.Marshal(typeObj)
	if err != nil {
		return err
	}
	var s aschema
	if err := json.Unmarshal(typeJSON, &s); err != nil {
		return err
	}
	f.Type = &s
	// The lift above already copied "logicalType"/"precision"/"scale" into
	// typeObj, so the freshly-built schema's aobject captures them. Clear
	// the field-level copies so canonical re-emit does not duplicate.
	f.Logical, f.Scale, f.Precision = "", nil, nil
	return nil
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
func (f *afield) liftFieldLogicalIntoType() {
	if f.Logical == "" || f.Type == nil {
		return
	}

	switch {
	case f.Type.primitive != "":
		// {"type":"long", "logicalType":"x"} →
		//   {"type":{"type":"long", "logicalType":"x"}}
		f.Type = &aschema{object: f.newLogicalObject(f.Type.primitive)}

	case len(f.Type.union) > 0:
		// {"type":["null","long"], "logicalType":"x"} →
		//   {"type":["null",{"type":"long","logicalType":"x"}]}
		// Apply to the FIRST non-null branch only. Closer-to-the-type
		// wins: if that branch is already an object with its own
		// annotation, the field-level annotation is redundant and we
		// drop it — we do NOT fall through to a later non-null branch
		// (that would silently mutate a different type than the spec-
		// equivalent nested form would have addressed, and on the
		// `[null, T+logical, T]` shape would even synthesize a
		// duplicate union member).
		for i := range f.Type.union {
			branch := &f.Type.union[i]
			if branch.primitive == "null" {
				continue
			}
			switch {
			case branch.primitive != "":
				f.Type.union[i] = aschema{object: f.newLogicalObject(branch.primitive)}
			case branch.object != nil && branch.object.Logical == "":
				branch.object.Logical = f.Logical
				if branch.object.Scale == nil {
					branch.object.Scale = clonePtrInt(f.Scale)
				}
				if branch.object.Precision == nil {
					branch.object.Precision = clonePtrInt(f.Precision)
				}
			}
			break // first non-null branch only
		}

	case f.Type.object != nil:
		// {"type":{"type":"long"}, "logicalType":"x"} →
		//   {"type":{"type":"long","logicalType":"x"}}.
		// Closer-to-the-type annotation wins; only fill in fields the
		// inner object didn't already declare.
		if f.Type.object.Logical == "" {
			f.Type.object.Logical = f.Logical
		}
		if f.Type.object.Scale == nil {
			f.Type.object.Scale = clonePtrInt(f.Scale)
		}
		if f.Type.object.Precision == nil {
			f.Type.object.Precision = clonePtrInt(f.Precision)
		}
	}

	f.Logical, f.Scale, f.Precision = "", nil, nil
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

func (l laxInt) MarshalJSON() ([]byte, error) {
	return json.Marshal(int(l))
}

// MarshalJSON serializes an aobject honoring two Avro spec requirements
// that encoding/json's struct-tag path cannot satisfy on its own:
//
//  1. record/error must emit "fields" even when empty (Complex Types >
//     Records: "fields: a JSON array, listing fields (required)") and
//     enum must emit "symbols" even when empty (Complex Types > Enums:
//     "symbols: a JSON array, listing symbols, as JSON strings
//     (required)"). The struct-tag omitempty would drop them.
//
//  2. Parsing Canonical Form's [ORDER] rule requires "name, type,
//     fields, symbols, items, values, size" to appear in that order.
//     encoding/json emits struct fields in declaration order, which
//     happens to match for the first two but places other attributes
//     (namespace, aliases, etc.) in between, violating PCF order when
//     those non-canonical attributes are present.
//
// We emit the PCF-ordered keys first, then the non-PCF attributes
// afterward. Non-PCF attributes are stripped from canonical form
// before MarshalJSON is called, so their ordering is irrelevant to
// canonical form — it only matters that PCF keys are correctly
// ordered and complete.
func (o aobject) MarshalJSON() ([]byte, error) {
	type kv struct {
		k string
		v any
	}
	parts := make([]kv, 0, 8)

	// PCF [ORDER]: name, type, fields, symbols, items, values, size.
	if o.Name != "" {
		parts = append(parts, kv{"name", o.Name})
	}
	parts = append(parts, kv{"type", o.Type})
	switch o.Type {
	case "record", "error":
		fields := o.Fields
		if fields == nil {
			fields = []afield{}
		}
		parts = append(parts, kv{"fields", fields})
	default:
		if len(o.Fields) > 0 {
			parts = append(parts, kv{"fields", o.Fields})
		}
	}
	if o.Type == "enum" {
		symbols := o.Symbols
		if symbols == nil {
			symbols = []string{}
		}
		parts = append(parts, kv{"symbols", symbols})
	} else if len(o.Symbols) > 0 {
		parts = append(parts, kv{"symbols", o.Symbols})
	}
	if o.Items != nil {
		parts = append(parts, kv{"items", o.Items})
	}
	if o.Values != nil {
		parts = append(parts, kv{"values", o.Values})
	}
	if o.Size != nil {
		parts = append(parts, kv{"size", o.Size})
	}

	// Non-PCF attributes (stripped in canonical form).
	if o.Namespace != nil {
		parts = append(parts, kv{"namespace", *o.Namespace})
	}
	if len(o.Aliases) > 0 {
		parts = append(parts, kv{"aliases", o.Aliases})
	}
	if len(o.Default) > 0 {
		parts = append(parts, kv{"default", o.Default})
	}
	if o.Logical != "" {
		parts = append(parts, kv{"logicalType", o.Logical})
	}
	if o.Precision != nil {
		parts = append(parts, kv{"precision", *o.Precision})
	}
	if o.Scale != nil {
		parts = append(parts, kv{"scale", *o.Scale})
	}

	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, p := range parts {
		if i > 0 {
			buf.WriteByte(',')
		}
		// Key: a string literal we control, never fails.
		kJSON, _ := json.Marshal(p.k)
		buf.Write(kJSON)
		buf.WriteByte(':')
		vJSON, err := json.Marshal(p.v)
		if err != nil {
			return nil, err
		}
		buf.Write(vJSON)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
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

// unionMissing / unionMissingDeser patch union branch function tables
// when a branch type was a forward reference.
type unionMissing struct {
	ser     *serUnion
	missing map[int]string // branch index → type name
}

type unionMissingDeser struct {
	deser   *deserUnion
	missing map[int]string
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
	meta *fieldMeta
	name string
}

// recordFieldFixup patches a record field's ser/deser function, avroType,
// meta, and schemaNode when the field's type was a forward reference.
type recordFieldFixup struct {
	sr         *serRecord
	dr         *deserRecord
	nd         *schemaNode
	idx        int
	name       string
	defaultVal any  // parsed JSON default; valid only when hasDefault is true
	hasDefault bool // whether the field had a "default" in the schema
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
	// hadCustomType is true when this named type was built under a
	// Parse that registered at least one CustomType matching some
	// node in its subtree. The cache-reference rejection check uses
	// this to allow re-references when the cached entry was already
	// CT-wired at its original Parse — the documented remediation
	// path "re-parse Inner with the CT first" depends on this signal.
	hadCustomType bool
}

type builder struct {
	ser   serfn
	deser deserfn

	named           map[string]*namedType
	missing         []unionMissing
	dmissing        []unionMissingDeser
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
	cachedNames map[string]bool // names inherited from SchemaCache, not from this parse
	depth       int             // current build recursion depth, bounded by maxDepth
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
		named:       b.named,
		checkName:   b.checkName,
		customTypes: b.customTypes,
		custom:      b.custom,
		cachedNames: b.cachedNames,
		depth:       b.depth,
	}
}

func (b *builder) unnest(nest *builder) {
	b.missing = append(b.missing, nest.missing...)
	b.dmissing = append(b.dmissing, nest.dmissing...)
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
}

// hasCustomTypeWired reports whether the builder has accumulated any
// custom encoders or decoders. Used to stamp namedType.hadCustomType so
// later cached references can skip the rejectCachedRefIfCustomTypeWouldMatch
// check when this Parse already wired its own CTs.
func (b *builder) hasCustomTypeWired() bool {
	return len(b.custom) > 0
}

// putCustomWiring stores the wiring under node, allocating b.custom on
// demand. Used by applyCustomTypes after building the per-node closures.
func (b *builder) putCustomWiring(node *schemaNode, w *customWiring) {
	if b.custom == nil {
		b.custom = make(map[*schemaNode]*customWiring)
	}
	b.custom[node] = w
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

// registerNamed stores nt under name, populating hadCustomType from the
// builder's current overlay maps. Shared by the record/enum/fixed
// registrations in buildComplex so the flag is set consistently.
func (b *builder) registerNamed(name string, nt *namedType) {
	nt.hadCustomType = b.hasCustomTypeWired()
	b.named[name] = nt
}

// tryAssignNamedRef resolves a named-type reference, possibly with
// namespace qualification against parentName. Returns true on hit (with
// b.ser / b.deser / b.meta / b.node populated and, when setCanon is
// true, b.canon set to the resolved name). Shared by buildPrimitive's
// bare-string named-ref path and buildComplex's wrapped-form
// {"type":"Name"} path so the rejectCachedRefIfCustomTypeWouldMatch
// gate and the namespace-qualified retry agree.
func (b *builder) tryAssignNamedRef(name, parentName string, setCanon bool) (bool, error) {
	assign := func(n string, nt *namedType) error {
		if err := b.rejectCachedRefIfCustomTypeWouldMatch(n, nt); err != nil {
			return err
		}
		if setCanon {
			b.canon = aschema{primitive: n}
		}
		b.ser = nt.ser
		b.deser = nt.deser
		if nt.sr != nil {
			b.meta = fieldMeta{avroType: "record", serRecord: nt.sr, deserRecord: nt.dr}
		}
		b.node = nt.node
		return nil
	}
	if nt := b.named[name]; nt != nil {
		return true, assign(name, nt)
	}
	if !strings.Contains(name, ".") && parentName != "" {
		if dot := strings.LastIndexByte(parentName, '.'); dot >= 0 {
			qualified := parentName[:dot+1] + name
			if nt := b.named[qualified]; nt != nil {
				return true, assign(qualified, nt)
			}
		}
	}
	return false, nil
}

func (b *builder) finalize() error {
	for _, m := range b.missing {
		for idx, name := range m.missing {
			nt := b.named[name]
			if nt == nil {
				return fmt.Errorf("unknown type %q", truncForError(name))
			}
			m.ser.fns[idx] = nt.ser
		}
	}
	for _, m := range b.dmissing {
		for idx, name := range m.missing {
			m.deser.fns[idx] = b.named[name].deser
		}
	}
	for _, m := range b.mfixups {
		nt := b.named[m.name]
		m.meta.serRecord = nt.sr
		m.meta.deserRecord = nt.dr
	}
	// Phase 1: wire every forward-referenced record-field node. Default
	// ENCODING is deferred to phase 2 below so it runs only after every
	// field AND container child node is wired — encodeDefault recurses into
	// a field's child nodes, and a not-yet-wired child would nil-panic.
	for _, m := range b.fieldFixups {
		nt := b.named[m.name]
		if nt == nil {
			return fmt.Errorf("unknown type %q", truncForError(m.name))
		}
		m.sr.fields[m.idx].fn = nt.ser
		m.dr.fields[m.idx].fn = nt.deser
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
		nt := b.named[m.name]
		if nt == nil {
			return fmt.Errorf("%s references unknown named type %q", m.ctxLabel, truncForError(m.name))
		}
		*m.serItem = nt.ser
		*m.deserItem = nt.deser
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
		node := b.named[m.name].node
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
	return nil
}

func (s *aschema) unionTypeName() (string, string, error) {
	if s.primitive != "" {
		return s.primitive, "", nil
	}
	if len(s.union) > 0 {
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
	if s == nil || s.primitive == "" && s.object == nil && len(s.union) == 0 {
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
	case len(s.union) != 0:
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

	if len(encoders) == 0 && len(decoders) == 0 {
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

		// Store the customEncode in the builder's overlay (not on the
		// shared node) so it doesn't leak via the cache.
		wiring.encode = customEncode

		// Wrap the binary serializer. We update b.ser (which becomes the
		// Schema's ser) but NOT node.ser, so named types in the cache
		// keep their unwrapped ser/deser.
		innerSer := node.ser
		ce := customEncode
		b.ser = func(dst []byte, v reflect.Value, depth int) ([]byte, error) {
			v, err := ce(v)
			if err != nil {
				return nil, err
			}
			return innerSer(dst, v, depth+1)
		}
	}

	if len(decoders) > 0 {
		wiring.decoders = decoders
		b.deser = wrapDeserWithCustomDecoders(node.deser, decoders, sn)
		// JSON-side: wrap the node's per-decode dispatch with a
		// closure that captures the decoder chain. The JSON runtime
		// (decodeValue) checks node.decodeJSON first and falls back
		// to decodeKind otherwise — no per-call map lookup, no
		// recursion guard, no shared mutable state.
		node.decodeJSON = wrapDecodeJSONWithCustomDecoders(decoders, sn)
	}

	b.putCustomWiring(node, wiring)
	b.meta.hasCustomType = true
	return nil
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
	// setCanon=true. To preserve the prior shape (bare-name canon stays
	// as written, only the qualified retry rewrites), we tell the helper
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
	if len(b.customTypes) == 0 || nt == nil || nt.node == nil {
		return nil
	}
	// If the cached entry was itself built under a CT-bearing Parse,
	// trust that its wiring is sufficient — the user's documented
	// remediation is to re-parse the inner type with the CT in scope,
	// which lands here. We don't try to detect CT shape mismatches
	// between the two parses; the cached wiring wins.
	if nt.hadCustomType {
		return nil
	}
	visited := make(map[*schemaNode]bool)
	if matched := b.findCustomTypeMatchInSubtree(nt.node, visited); matched != "" {
		return fmt.Errorf("avro: cached type %q contains %q which would match a CustomType on this Parse; re-parse %q with the CustomType first", truncForError(refName), truncForError(matched), truncForError(refName))
	}
	return nil
}

// findCustomTypeMatchInSubtree walks node and its descendants,
// returning a short location string for the first node whose
// (kind, logical) would match any of b.customTypes. Returns "" if
// no descendant matches. Recursive types are handled via the
// visited set; node.fields, node.items, node.values, node.branches
// cover every container shape (record, array, map, union). Named-
// type recursion is rare in cached-reuse scenarios but the visited
// set keeps it safe.
func (b *builder) findCustomTypeMatchInSubtree(node *schemaNode, visited map[*schemaNode]bool) string {
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
	)

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
		// definition + a name reference to the same named type
		// collide; primitives still collide on kind. Forward refs key
		// on the unresolved name (the common case where the user
		// writes consistent names is caught; namespace-qualification
		// mismatches that only resolve later may escape).
		key := typ
		switch {
		case u.node != nil && u.node.name != "":
			key = u.node.name
		case missing[i] != "":
			key = missing[i]
		}
		if sawTypes[key] {
			return fmt.Errorf("duplicate union type %q", truncForError(key))
		}
		sawTypes[key] = true

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
		deser.branchNames = append(deser.branchNames, bn)
		deser.logicalNames = append(deser.logicalNames, ln)
		if ser.branchNames == nil {
			ser.branchNames = make(map[string]int, len(s.union))
		}
		ser.branchNames[bn] = i
		if ln != bn {
			ser.branchNames[ln] = i
		}
	}
	// Fastavro-short-name fallback: for named branches (record/enum/
	// fixed) whose canonical name carries a namespace, also accept the
	// unqualified short name on tagged-union encode IFF it's unique
	// across the union. Mirrors findUnionBranch's JSON-side fallback
	// (json_codec.go:findUnionBranch) so binary and JSON encode accept
	// the same tagged input shape. The ambiguity guard prevents silent
	// misrouting when two namespaces share a short name.
	if len(deser.branchNames) > 0 {
		shortCount := make(map[string]int, len(deser.branchNames))
		for _, bn := range deser.branchNames {
			if short := unqualified(bn); short != bn {
				shortCount[short]++
			}
		}
		for i, bn := range deser.branchNames {
			short := unqualified(bn)
			if short == bn {
				continue
			}
			if shortCount[short] != 1 {
				continue
			}
			if _, exists := ser.branchNames[short]; exists {
				continue
			}
			ser.branchNames[short] = i
		}
	}
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
	case len(s.union) == 2 && s.union[0].primitive == "null":
		b.ser = serNullUnion(ser)
		b.deser = deserNullUnion(deser)
		b.meta = b.buildNullUnionMeta(missing, branchMetas, 1, false)
	case len(s.union) == 2 && s.union[1].primitive == "null":
		b.ser = serNullSecondUnion(ser)
		b.deser = deserNullSecondUnion(deser)
		b.meta = b.buildNullUnionMeta(missing, branchMetas, 0, true)
	default:
		b.ser = ser.ser
		b.deser = deser.deser
		b.meta = fieldMeta{avroType: "union"}
	}
	if len(missing) > 0 {
		b.missing = append(b.missing, unionMissing{
			ser,
			missing,
		})
		b.dmissing = append(b.dmissing, unionMissingDeser{
			deser,
			missing,
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

// buildNullUnionMeta returns the fieldMeta for the 2-branch null-union
// fast path. nonNullIdx is the index of the non-null branch (1 for
// ["null", T]; 0 for ["T", "null"]). When that branch is a forward
// reference, the inner meta is queued for finalize-time fixup;
// otherwise the inner meta is copied from branchMetas. nullSecond
// distinguishes the two orderings.
func (b *builder) buildNullUnionMeta(missing map[int]string, branchMetas []fieldMeta, nonNullIdx int, nullSecond bool) fieldMeta {
	if name, isMissing := missing[nonNullIdx]; isMissing {
		inner := &fieldMeta{}
		b.mfixups = append(b.mfixups, metaFixup{meta: inner, name: name})
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
		if o.Logical == "decimal" {
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
		if o.Logical == "big-decimal" {
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
		// Always use the logical type serializer if available — it's a
		// strict superset of the base serializer (accepts time.Time etc.
		// in addition to raw values). Only the deserializer is suppressed
		// when a custom type matches, so that Decode produces raw
		// Avro-native values for the custom decoder.
		if logSer := logicalSer(o.Logical); logSer != nil {
			b.ser = logSer
		}
		if !b.hasMatchingCustomType(o.Type, o.Logical) {
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
		if o.Precision != nil {
			nd.precision = *o.Precision
		}
		if o.Scale != nil {
			nd.scale = *o.Scale
		}
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
		for _, a := range origAliases {
			if err := b.validFullnameErr(a); err != nil {
				return fmt.Errorf("invalid %s alias %q: %w", truncForError(o.Type), truncForError(a), err)
			}
		}
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
		o.Namespace = nil      // canonical form omits namespace
		canonObj.Name = o.Name // use fully-qualified name
		canonObj.Namespace = nil
		if _, exists := b.named[o.Name]; exists {
			if !b.cachedNames[o.Name] {
				return fmt.Errorf("duplicate named type %q", truncForError(o.Name))
			}
			// Name exists from cache — allow re-registration
			// (custom types need to re-parse to get fresh wiring).
		}
	} else {
		if o.Name != "" || o.Namespace != nil {
			return errors.New("only record, enum, and fixed can have a name")
		}
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
			ser:         b.ser,
			deser:       b.deser,
			serRecord:   sr,
			deserRecord: dr,
		}
		b.registerNamed(o.Name, &namedType{ser: b.ser, deser: b.deser, sr: sr, dr: dr, node: nd})
		b.node = nd

		var names []string
		seenFields := make(map[string]bool, len(o.Fields))
		for i, of := range o.Fields {
			if err := b.validNameErr(of.Name); err != nil {
				return fmt.Errorf("invalid field name %q: %w", truncForError(of.Name), err)
			}
			for _, a := range origFieldAliases[i] {
				if err := b.validNameErr(a); err != nil {
					return fmt.Errorf("invalid field alias %q for field %q: %w", truncForError(a), truncForError(of.Name), err)
				}
			}
			if seenFields[of.Name] {
				return fmt.Errorf("duplicate record field name %q", truncForError(of.Name))
			}
			seenFields[of.Name] = true
			if of.Order != "" && of.Order != "ascending" && of.Order != "descending" && of.Order != "ignore" {
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
					sr:   sr,
					dr:   dr,
					nd:   nd,
					idx:  fieldIdx,
					name: fwdRefName,
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
				} else if nodeAwaitsForwardRef(bf.node) {
					// The field's outer type resolved at build time, but its
					// type tree has a forward-referenced descendant (array/map
					// items/values, or an inline record field) not yet wired.
					// encodeDefault would dereference the nil child and panic,
					// so defer the resolve+encode to finalize, after the
					// container/field fixups wire the descendants. Signal
					// hasDefault so dispatch knows a default exists; the
					// deferred pass fills defaultVal/defaultBytes.
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
		// Update hadCustomType after all fields are built. The named
		// entry was registered EARLY (before fields) to support self-
		// referencing record schemas, so the flag couldn't be set at
		// registration time — fields might apply CTs that won't show
		// up in the maps until unnest. Re-stamp now.
		if cached := b.named[o.Name]; cached != nil {
			cached.hadCustomType = cached.hadCustomType || b.hasCustomTypeWired()
		}

	case "enum":
		if len(o.Fields) > 0 ||
			o.Items != nil ||
			o.Values != nil ||
			o.Size != nil {
			return errors.New("invalid enum has schema for other types")
		}

		if len(o.Symbols) == 0 {
			return errors.New("enum must have at least one symbol")
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
			kind:    "enum",
			name:    o.Name,
			logical: o.Logical,
			aliases: qualifyAliases(origAliases, o.Name),
			symbols: o.Symbols,
			ser:     b.ser,
			deser:   b.deser,
		}
		if len(origEnumDefault) > 0 {
			var defStr string
			json.Unmarshal(origEnumDefault, &defStr)
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
		if size <= 0 {
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
			if hasEnc {
				b.ser = (&serSize{size}).ser
			} else {
				b.ser = serDuration
			}
			if hasAny {
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
			if hasEnc {
				b.ser = (&serSize{size}).ser
			} else {
				b.ser = serFixedUUIDReflect
			}
			if hasAny {
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
			kind:    "fixed",
			name:    o.Name,
			aliases: qualifyAliases(origAliases, o.Name),
			logical: s.object.Logical,
			size:    size,
			ser:     b.ser,
			deser:   b.deser,
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
		if strings.ContainsRune(a, '.') {
			out[i] = a // already fully qualified
		} else {
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
	// Java's Duration.validate at LogicalTypes.java:526-530 throws
	// IllegalArgumentException for `type != FIXED || size != 12`;
	// fromSchemaIgnoreInvalid catches and drops. hamba's
	// parseFixedLogicalType at schema_parse.go:517 only matches
	// `ltyp == Duration && size == 12` and drops everything else.
	"duration": func(o *aobject) bool {
		return o.Type == "fixed" && o.Size != nil && int(*o.Size) == 12
	},
}

func (o *aobject) validateLogical() error {
	switch o.Logical {
	case "":
		// No logical type: validate no scale / precision below.

	case "decimal":
		// Wrong underlying type is the one fall-back-on-mismatch case
		// the spec implies: an unknown logical type pinned on the wrong
		// primitive should not block schema parse. Precision/scale
		// constraints, on the other hand, are explicit Avro 1.12 rules
		// (LogicalTypes.Decimal.validate in Java rejects each); a
		// schema that violates them is malformed. Java and fastavro
		// reject; we do too, to avoid silent interop divergence.
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
				return nil
			}
		} else {
			// Per the Avro spec, unknown logical types are ignored and the
			// underlying type is used as-is.
			o.Logical = ""
			return nil
		}
	}

	if o.Scale != nil || o.Precision != nil {
		return fmt.Errorf("type %q logicalType %q: invalid scale or precision specified", truncForError(o.Type), truncForError(o.Logical))
	}

	return nil
}

// maxDecimalDigits returns the maximum number of decimal digits that fit in
// a two's-complement signed integer of the given byte size:
// floor(log10(2^(8*size-1) - 1)).
func maxDecimalDigits(size int) int {
	if size <= 0 {
		return 0
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

// nodeAwaitsForwardRef reports whether node has any not-yet-resolved
// forward-referenced child that encodeDefault would traverse. encodeDefault
// recurses into items / values / fields / branches and dereferences each
// child's kind, so a nil child there is a runtime nil-pointer panic, not an
// error. When this returns true at build time, the caller must defer the
// whole resolve+encode-default pipeline to finalize (after the container /
// field fixups have wired the descendants) rather than run it inline.
//
// Cycle-safe via a seen set: a back-edge to an already-built node (recursive
// schema) is a wired pointer, not nil, so it is not a pending forward ref.
func nodeAwaitsForwardRef(node *schemaNode) bool {
	return nodeAwaitsForwardRefSeen(node, map[*schemaNode]struct{}{})
}

func nodeAwaitsForwardRefSeen(node *schemaNode, seen map[*schemaNode]struct{}) bool {
	if node == nil {
		return true
	}
	if _, ok := seen[node]; ok {
		return false
	}
	seen[node] = struct{}{}
	switch node.kind {
	case "array":
		return nodeAwaitsForwardRefSeen(node.items, seen)
	case "map":
		return nodeAwaitsForwardRefSeen(node.values, seen)
	case "record", "error":
		for i := range node.fields {
			if nodeAwaitsForwardRefSeen(node.fields[i].node, seen) {
				return true
			}
		}
	case "union":
		for _, b := range node.branches {
			if nodeAwaitsForwardRefSeen(b, seen) {
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
	defaultBytes, err := encodeDefault(nil, defaultVal, node)
	if err != nil {
		return fmt.Errorf("record field %q: encoding default: %v", truncForError(fieldName), err)
	}
	srf.defaultBytes = defaultBytes
	srf.hasDefault = true
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
		if bi := r.Num(); bi.IsInt64() {
			return bi.Int64()
		}
		// Exact integer beyond int64 range. Two sub-cases:
		//   - Magnitude fits float64's exponent → surface as float64,
		//     matching what an encode against a float/double schema
		//     emits on the wire (lossy by destination).
		//   - Magnitude overflows float64 → parseFloatAcceptOverflow
		//     returns ±Inf, matching the wire encoder's silent
		//     overflow-to-Inf path.
	}
	if f, err := parseFloatAcceptOverflow(s); err == nil {
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
// source of truth for the four-axis ParseFloat callers
// (jsonNumberToFloat, jsonCoerceToFloat64, defaultAsFloat,
// normalizeJSONNumber), so capping here covers all axes.
func parseFloatAcceptOverflow(s string) (float64, error) {
	if len(s) > maxParseFloatLen {
		return 0, fmt.Errorf("float literal exceeds %d byte length cap", maxParseFloatLen)
	}
	f, err := strconv.ParseFloat(s, 64)
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
		return parseJSONNumberAsFloat(v.String())
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case int32:
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
// — [defaultAsFloat] no longer has a string-acceptance arm; Java's
// parseField text→DoubleNode coercion fires only for the OUTER
// FLOAT/DOUBLE field type (handled in [coerceDefault] below) and
// never for union branches.
func firstUnionBranchAcceptingDefault(val any, node *schemaNode) *schemaNode {
	for _, branch := range node.branches {
		if validateDefault(val, branch) == nil {
			return branch
		}
	}
	return nil
}

// coerceDefault converts string default values to float64 when the
// field type is literally float or double. Matches Java's parseField
// at Schema.java:1899-1902, which special-cases TextNode → DoubleNode
// coercion ONLY when the outer fieldSchema.getType() is FLOAT or
// DOUBLE directly — never for union branches, never for int/long
// fields. Spec 1.12 §"Record" default-values table marks JSON string
// as invalid for float/double defaults; the Java-deployed coercion is
// an interop carveout preserved here for legacy Java-generated
// schemas. avro-rs and goavro do not implement this coercion.
//
// Union defaults pass through unchanged: walkDefault picks the first
// branch whose validateDefault accepts, and validateDefault no longer
// has a string-to-float arm at the leaf level, so union+numeric-string
// defaults are rejected at parse (matching Java/avro-rs/goavro).
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
		// no longer has a string arm), so this picks a string-
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
	if f, err := parseFloatAcceptOverflow(s); err == nil {
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
		// Avro 1.12+ relaxed the union-default rule: the default may
		// match any branch (formerly required to match the first).
		// See AVRO-3649 / PR apache/avro#2503.
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
		if len(node.symbols) > 0 && !slices.Contains(node.symbols, sym) {
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
		if val == nil {
			return val, fmt.Errorf("expected object for record default, got null")
		}
		m, ok := val.(map[string]any)
		if !ok {
			return val, fmt.Errorf("expected object for record default, got %T", val)
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
		if val == nil {
			return val, fmt.Errorf("expected array for array default, got null")
		}
		arr, ok := val.([]any)
		if !ok {
			return val, fmt.Errorf("expected array for array default, got %T", val)
		}
		for i, item := range arr {
			arr[i] = coerceDefault(item, node.items)
		}
	case "map":
		if val == nil {
			return val, fmt.Errorf("expected object for map default, got null")
		}
		m, ok := val.(map[string]any)
		if !ok {
			return val, fmt.Errorf("expected object for map default, got %T", val)
		}
		for k, v := range m {
			m[k] = coerceDefault(v, node.values)
		}
	}
	return val, nil
}
