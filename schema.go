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

	// Per-schema custom type overlays. Keyed by *schemaNode so the
	// shared node is not mutated — different schemas parsed with
	// different custom types get different overlays.
	customEncodes  map[*schemaNode]func(v reflect.Value) (reflect.Value, error)
	customDecoders map[*schemaNode][]func(any, *SchemaNode) (any, error)
	customSNs      map[*schemaNode]*SchemaNode
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
	name       string
	nameVal    reflect.Value // pre-computed for map lookups without allocation
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
		return nil, fmt.Errorf("invalid schema: %w", err)
	}
	if err := b.build("", &orig); err != nil {
		return nil, err
	}
	if err := b.finalize(); err != nil {
		return nil, err
	}
	s := &Schema{
		ser:            b.ser,
		deser:          b.deser,
		c:              b.canon,
		node:           b.node,
		full:           schema,
		customEncodes:  b.customEncodes,
		customDecoders: b.customDecoderMap,
		customSNs:      b.customSNMap,
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
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return errors.New("invalid empty schema")
	}
	switch data[0] {
	case '"':
		return json.Unmarshal(data, &s.primitive)
	case '{':
		// Round-trip through map[string]RawMessage to normalize duplicate
		// keys (JSON allows them, encoding/json is "last wins" for map
		// decode but struct-decode ordering can diverge). After this,
		// the struct decode and any subsequent map-based re-parse see
		// identical keys.
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(data, &raw); err != nil {
			return err
		}
		normalized, err := json.Marshal(raw)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(normalized, &s.object); err != nil {
			return err
		}
		// Capture extra properties not in the struct tags.
		// encoding/json matches struct field names case-insensitively,
		// so keys like "tYpe" parse into Type and should not also land
		// in extras.
		for k := range raw {
			if aobjectKnownKeyInsensitive(k) {
				continue
			}
			if s.object.extra == nil {
				s.object.extra = make(map[string]any)
			}
			var v any
			json.Unmarshal(raw[k], &v)
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
	// Round-trip through map[string]RawMessage to normalize duplicate
	// keys. See aschema.UnmarshalJSON for the rationale.
	var dedupRaw map[string]json.RawMessage
	if err := json.Unmarshal(data, &dedupRaw); err != nil {
		return err
	}
	normalized, err := json.Marshal(dedupRaw)
	if err != nil {
		return err
	}
	data = normalized
	// Standard unmarshal into a type alias to avoid recursion.
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
// This is the Java/JDBC Avro idiom, e.g.
//
//	{"name":"ts","type":"long","logicalType":"timestamp-millis"}
//	{"name":"ts","type":["null","long"],"logicalType":"timestamp-millis"}
//
// emitted by Confluent's Java codegen, kafka-connect-avro-converter, and
// Debezium CDC sources. The on-wire encoding is identical to
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

func (l *laxInt) UnmarshalJSON(data []byte) error {
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

var aobjectKnownKeys = map[string]bool{
	"type": true, "name": true, "namespace": true, "doc": true,
	"fields": true, "symbols": true, "items": true, "values": true,
	"size": true, "logicalType": true, "precision": true, "scale": true,
	"aliases": true, "default": true, "order": true,
}

// aobjectKnownKeyInsensitive reports whether k is a known schema key,
// matching case-insensitively to mirror encoding/json's struct field
// matching. This keeps lenient decoders from double-parsing a field
// (once as a struct field, again as an extra prop).
func aobjectKnownKeyInsensitive(k string) bool {
	for known := range aobjectKnownKeys {
		if strings.EqualFold(k, known) {
			return true
		}
	}
	return false
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

	named       map[string]*namedType
	missing     []unionMissing
	dmissing    []unionMissingDeser
	mfixups     []metaFixup
	fieldFixups []recordFieldFixup

	meta             fieldMeta
	canon            aschema
	node             *schemaNode
	checkName        func(string) error // nil means strict (default)
	customTypes      []CustomType
	customEncodes    map[*schemaNode]func(v reflect.Value) (reflect.Value, error)
	customDecoderMap map[*schemaNode][]func(any, *SchemaNode) (any, error)
	customSNMap      map[*schemaNode]*SchemaNode
	cachedNames      map[string]bool // names inherited from SchemaCache, not from this parse
	depth            int             // current build recursion depth, bounded by maxDepth
}

// validNameErr validates a simple name using the builder's configured validator.
func (b *builder) validNameErr(s string) error {
	if b.checkName != nil {
		return b.checkName(s)
	}
	if !validName(s) {
		return fmt.Errorf("invalid name %q", s)
	}
	return nil
}

// validFullnameErr validates a dot-separated fullname.
func (b *builder) validFullnameErr(s string) error {
	if s == "" {
		if b.checkName != nil {
			return b.checkName(s)
		}
		return fmt.Errorf("invalid name %q", s)
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
		named:            b.named,
		checkName:        b.checkName,
		customTypes:      b.customTypes,
		customEncodes:    b.customEncodes,
		customDecoderMap: b.customDecoderMap,
		customSNMap:      b.customSNMap,
		cachedNames:      b.cachedNames,
		depth:            b.depth,
	}
}

func (b *builder) unnest(nest *builder) {
	b.missing = append(b.missing, nest.missing...)
	b.dmissing = append(b.dmissing, nest.dmissing...)
	b.mfixups = append(b.mfixups, nest.mfixups...)
	b.fieldFixups = append(b.fieldFixups, nest.fieldFixups...)
	// Merge custom type overlay maps from nested builders.
	if len(nest.customEncodes) > 0 {
		if b.customEncodes == nil {
			b.customEncodes = make(map[*schemaNode]func(reflect.Value) (reflect.Value, error), len(nest.customEncodes))
		}
		maps.Copy(b.customEncodes, nest.customEncodes)
	}
	if len(nest.customDecoderMap) > 0 {
		if b.customDecoderMap == nil {
			b.customDecoderMap = make(map[*schemaNode][]func(any, *SchemaNode) (any, error), len(nest.customDecoderMap))
		}
		maps.Copy(b.customDecoderMap, nest.customDecoderMap)
	}
	if len(nest.customSNMap) > 0 {
		if b.customSNMap == nil {
			b.customSNMap = make(map[*schemaNode]*SchemaNode, len(nest.customSNMap))
		}
		maps.Copy(b.customSNMap, nest.customSNMap)
	}
}

func (b *builder) finalize() error {
	for _, m := range b.missing {
		for idx, name := range m.missing {
			nt := b.named[name]
			if nt == nil {
				return fmt.Errorf("unknown type %q", name)
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
	for _, m := range b.fieldFixups {
		nt := b.named[m.name]
		if nt == nil {
			return fmt.Errorf("unknown type %q", m.name)
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
		if m.hasDefault && nt.node != nil {
			// Now that the type is resolved, run the validation +
			// coercion + conversion that was deferred at parse time
			// against the resolved schemaNode tree. coerceDefault
			// turns nested float-from-string into float64;
			// validateDefault enforces structural compatibility;
			// convertDefaultBytes maps bytes/fixed strings to []byte
			// for JSON-encoder parity.
			m.defaultVal = coerceDefault(m.defaultVal, nt.node)
			if err := validateDefault(m.defaultVal, nt.node); err != nil {
				return fmt.Errorf("field %q: invalid default for type %q: %v", m.sr.fields[m.idx].name, m.name, err)
			}
			m.defaultVal = convertDefaultBytes(m.defaultVal, nt.node)
			m.dr.fields[m.idx].defaultVal = m.defaultVal
			m.dr.fields[m.idx].hasDefault = true
			m.nd.fields[m.idx].defaultVal = m.defaultVal
			m.nd.fields[m.idx].hasDefault = true
			defaultBytes, err := encodeDefault(nil, m.defaultVal, nt.node)
			if err != nil {
				return fmt.Errorf("field %q: invalid default for type %q: %v", m.sr.fields[m.idx].name, m.name, err)
			}
			m.sr.fields[m.idx].defaultBytes = defaultBytes
			m.sr.fields[m.idx].hasDefault = true
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
	switch s.object.Type {
	case "record", "error", "fixed", "enum":
		return s.object.Type, s.object.Name, nil
	default:
		return s.object.Type, "", nil
	}
}

type unknownPrimitiveError struct{ p string }

func (e *unknownPrimitiveError) Error() string { return fmt.Sprintf("unknown primitive %q", e.p) }

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
// a node with the given kind and logical type. Used to skip built-in
// logical type handlers when a custom type replaces them.
func (b *builder) hasMatchingCustomType(kind, logical string) bool {
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

	// Build the cached SchemaNode for callbacks.
	sn := buildCustomSN(node)

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

		// Store the customEncode in the builder's overlay map (not on
		// the shared node) so it doesn't leak via the cache.
		if b.customEncodes == nil {
			b.customEncodes = make(map[*schemaNode]func(reflect.Value) (reflect.Value, error))
		}
		b.customEncodes[node] = customEncode

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
		if b.customDecoderMap == nil {
			b.customDecoderMap = make(map[*schemaNode][]func(any, *SchemaNode) (any, error))
		}
		if b.customSNMap == nil {
			b.customSNMap = make(map[*schemaNode]*SchemaNode)
		}
		b.customDecoderMap[node] = decoders
		b.customSNMap[node] = sn
		b.deser = wrapDeserWithCustomDecoders(node.deser, decoders, sn)
		// JSON-side: wrap the node's per-decode dispatch with a
		// closure that captures the decoder chain. The JSON runtime
		// (decodeValue) checks node.decodeJSON first and falls back
		// to decodeKind otherwise — no per-call map lookup, no
		// recursion guard, no shared mutable state.
		node.decodeJSON = wrapDecodeJSONWithCustomDecoders(decoders, sn)
	}

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
	name := s.primitive
	if nt := b.named[name]; nt != nil {
		if err := b.rejectCachedRefIfCustomTypeWouldMatch(name, nt); err != nil {
			return err
		}
		b.ser = nt.ser
		b.deser = nt.deser
		if nt.sr != nil {
			b.meta = fieldMeta{avroType: "record", serRecord: nt.sr, deserRecord: nt.dr}
		}
		b.node = nt.node
		return nil
	}
	// Try namespace-qualified lookup: if name is unqualified and parent
	// has a namespace, try parentNamespace + "." + name.
	if !strings.Contains(name, ".") && parentName != "" {
		if dot := strings.LastIndexByte(parentName, '.'); dot >= 0 {
			qualified := parentName[:dot+1] + name
			if nt := b.named[qualified]; nt != nil {
				if err := b.rejectCachedRefIfCustomTypeWouldMatch(qualified, nt); err != nil {
					return err
				}
				b.canon.primitive = qualified
				b.ser = nt.ser
				b.deser = nt.deser
				if nt.sr != nil {
					b.meta = fieldMeta{avroType: "record", serRecord: nt.sr, deserRecord: nt.dr}
				}
				b.node = nt.node
				return nil
			}
		}
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
// referenced types. Cached references previously bypassed this scope.
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
		return fmt.Errorf("avro: cached type %q contains %q which would match a CustomType on this Parse; re-parse %q with the CustomType first", refName, matched, refName)
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
		if err := u.build(parentName, &us); err != nil {
			pe := (*unknownPrimitiveError)(nil)
			if !errors.As(err, &pe) {
				return fmt.Errorf("invalid union: %w", err)
			}
			// pe.p is the unresolved name from either the bare-string
			// form (where us.primitive is set) or the wrapped form
			// {"type":"FwdName"} (where us.object.Type is set); the
			// error normalizes both into pe.p.
			missing[i] = pe.p
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
			return fmt.Errorf("duplicate union type %q", key)
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

	if len(s.union) == 2 && s.union[0].primitive == "null" {
		b.ser = serNullUnion(ser)
		b.deser = deserNullUnion(deser)
		if _, isMissing := missing[1]; isMissing {
			inner := &fieldMeta{}
			b.meta = fieldMeta{avroType: "nullunion", inner: inner}
			b.mfixups = append(b.mfixups, metaFixup{meta: inner, name: missing[1]})
		} else {
			inner := new(fieldMeta)
			*inner = branchMetas[1]
			b.meta = fieldMeta{avroType: "nullunion", inner: inner}
		}
	} else if len(s.union) == 2 && s.union[1].primitive == "null" {
		b.ser = serNullSecondUnion(ser)
		b.deser = deserNullSecondUnion(deser)
		if _, isMissing := missing[0]; isMissing {
			inner := &fieldMeta{}
			b.meta = fieldMeta{avroType: "nullunion", nullSecond: true, inner: inner}
			b.mfixups = append(b.mfixups, metaFixup{meta: inner, name: missing[0]})
		} else {
			inner := new(fieldMeta)
			*inner = branchMetas[0]
			b.meta = fieldMeta{avroType: "nullunion", nullSecond: true, inner: inner}
		}
	} else {
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
		if o.Logical == "decimal" && !b.hasMatchingCustomType(o.Type, o.Logical) {
			scale := 0
			if o.Scale != nil {
				scale = *o.Scale
			}
			b.ser = (&serBytesDecimal{precision: *o.Precision, scale: scale}).ser
			b.deser = (&deserBytesDecimal{scale: scale}).deser
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
		if o.Logical == "big-decimal" && !b.hasMatchingCustomType(o.Type, o.Logical) {
			b.ser = (&serBigDecimal{}).ser
			b.deser = (&deserBigDecimal{}).deser
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
		if nt := b.named[o.Type]; nt != nil {
			if err := b.rejectCachedRefIfCustomTypeWouldMatch(o.Type, nt); err != nil {
				return err
			}
			b.canon = aschema{primitive: o.Type}
			b.ser = nt.ser
			b.deser = nt.deser
			if nt.sr != nil {
				b.meta = fieldMeta{avroType: "record", serRecord: nt.sr, deserRecord: nt.dr}
			}
			b.node = nt.node
			return nil
		}
		if !strings.Contains(o.Type, ".") && parentName != "" {
			if dot := strings.LastIndexByte(parentName, '.'); dot >= 0 {
				qualified := parentName[:dot+1] + o.Type
				if nt := b.named[qualified]; nt != nil {
					if err := b.rejectCachedRefIfCustomTypeWouldMatch(qualified, nt); err != nil {
						return err
					}
					b.canon = aschema{primitive: qualified}
					b.ser = nt.ser
					b.deser = nt.deser
					if nt.sr != nil {
						b.meta = fieldMeta{avroType: "record", serRecord: nt.sr, deserRecord: nt.dr}
					}
					b.node = nt.node
					return nil
				}
			}
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
	canonObj := &aobject{
		Name: o.Name,
		Type: o.Type,

		Fields:  o.Fields,
		Symbols: o.Symbols,
		Items:   o.Items,
		Values:  o.Values,
		Size:    o.Size,

		Namespace: o.Namespace,
	}
	b.canon = aschema{object: canonObj}

	switch o.Type {
	case "record", "error", "enum", "fixed":
		if err := b.validFullnameErr(o.Name); err != nil {
			return fmt.Errorf("invalid %s name %q: %w", o.Type, o.Name, err)
		}
		for _, a := range origAliases {
			if err := b.validFullnameErr(a); err != nil {
				return fmt.Errorf("invalid %s alias %q: %w", o.Type, a, err)
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
				return fmt.Errorf("duplicate named type %q", o.Name)
			}
			// Name exists from cache — allow re-registration
			// (custom types need to re-parse to get fresh wiring).
		}
	default:
		if o.Name != "" || o.Namespace != nil {
			return errors.New("only record, enum, and fixed can have a name")
		}
	}

	switch o.Type {
	default:
		return fmt.Errorf("unknown complex type %q", o.Type)

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
		b.named[o.Name] = &namedType{ser: b.ser, deser: b.deser, sr: sr, dr: dr, node: nd,
			hadCustomType: len(b.customDecoderMap) > 0 || len(b.customEncodes) > 0}
		b.node = nd

		var names []string
		seenFields := make(map[string]bool, len(o.Fields))
		for i, of := range o.Fields {
			if err := b.validNameErr(of.Name); err != nil {
				return fmt.Errorf("invalid field name %q: %w", of.Name, err)
			}
			for _, a := range origFieldAliases[i] {
				if err := b.validNameErr(a); err != nil {
					return fmt.Errorf("invalid field alias %q for field %q: %w", a, of.Name, err)
				}
			}
			if seenFields[of.Name] {
				return fmt.Errorf("duplicate record field name %q", of.Name)
			}
			seenFields[of.Name] = true
			if of.Order != "" && of.Order != "ascending" && of.Order != "descending" && of.Order != "ignore" {
				return fmt.Errorf("invalid field order %q for field %q", of.Order, of.Name)
			}
			bf := b.nest()
			isFwdRef := false
			fwdRefName := ""
			if err := bf.build(o.Name, of.Type); err != nil {
				// An unknownPrimitiveError signals a not-yet-declared
				// named type — treat as a forward reference to be
				// resolved in finalize(). The error's `p` field carries
				// the name from either the bare-string form (where
				// of.Type.primitive is set) or the wrapped form
				// {"type":"FwdName"} (where of.Type.object.Type is set).
				if pe := (*unknownPrimitiveError)(nil); errors.As(err, &pe) {
					isFwdRef = true
					fwdRefName = pe.p
				} else {
					return fmt.Errorf("invalid record field: %v", err)
				}
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
				defaultVal := unmarshalDefault(of.Default)
				defaultVal = coerceDefault(defaultVal, bf.node)
				// Skip default validation for forward references since we
				// don't know the type yet.
				if !isFwdRef {
					if err := validateDefault(defaultVal, bf.node); err != nil {
						return fmt.Errorf("record field %q: invalid default: %v", of.Name, err)
					}
					// Convert bytes/fixed string defaults to []byte before
					// storing, so the JSON encoder sees the wire form
					// directly and its logical-type-aware arms can't
					// misinterpret the string as decimal / UUID / etc.
					// Mirrors encodeDefault's codepoint mapping; both
					// paths agree on default-fill output. Walks the
					// resolved schemaNode tree (not the aschema canon)
					// so name-references — both forward and backward —
					// follow into the real type.
					defaultVal = convertDefaultBytes(defaultVal, bf.node)
				}
				drf.defaultVal = defaultVal
				drf.hasDefault = true
				fn.defaultVal = defaultVal
				fn.hasDefault = true
				// Pre-encode the default to Avro binary for use
				// when encoding maps with missing keys.
				if !isFwdRef && bf.node != nil {
					defaultBytes, err := encodeDefault(nil, defaultVal, bf.node)
					if err != nil {
						return fmt.Errorf("record field %q: encoding default: %v", of.Name, err)
					}
					sr.fields[fieldIdx].defaultBytes = defaultBytes
					sr.fields[fieldIdx].hasDefault = true
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
		nd.fieldIdx = make(map[string]int, len(nd.fields))
		for i, f := range nd.fields {
			nd.fieldIdx[f.name] = i
			for _, a := range f.aliases {
				if _, exists := nd.fieldIdx[a]; exists {
					return fmt.Errorf("record field alias %q collides with another field name or alias", a)
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
			cached.hadCustomType = cached.hadCustomType ||
				len(b.customDecoderMap) > 0 || len(b.customEncodes) > 0
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
				return fmt.Errorf("invalid enum symbol %q: %w", e, err)
			}
			if seenSymbols[e] {
				return fmt.Errorf("duplicate enum symbol %q", e)
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
				return fmt.Errorf("enum default %q is not a member of symbols", defStr)
			}
			nd.enumDef = defStr
			nd.hasEnumDef = true
		}
		b.named[o.Name] = &namedType{ser: b.ser, deser: b.deser, node: nd,
			hadCustomType: len(b.customDecoderMap) > 0 || len(b.customEncodes) > 0}
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
		if err := af.build(parentName, o.Items); err != nil {
			return fmt.Errorf("invalid array: %v", err)
		}
		b.unnest(af)
		o.Items = &af.canon
		sa := &serArray{serItem: af.ser}
		da := &deserArray{deserItem: af.deser, minItemBytes: schemaMinBytes(af.node)}
		// Specialized array ser/deser fast paths bypass the inner
		// schema's wrapped ser/deser functions. They are correct only
		// when no per-element conversion is needed: no custom type,
		// and no logical type (logical-typed inners route through
		// serTimestampMillis / deserTimeMicros / serUUID / etc., which
		// the primitive-only specialization can't see).
		if af.meta.hasCustomType || af.meta.logical != "" {
			b.ser = sa.ser
		} else {
			switch af.canon.primitive {
			case "string":
				b.ser = sa.serString
				da.fastLoop = deserArrayStringLoop
				da.fastElemKind = reflect.String
				da.fastIfaceLoop = deserArrayStringIfaceLoop
			case "boolean":
				b.ser = sa.serBoolean
				da.fastLoop = deserArrayBooleanLoop
				da.fastElemKind = reflect.Bool
				da.fastIfaceLoop = deserArrayBooleanIfaceLoop
			case "int":
				b.ser = sa.serInt
				da.fastLoop = deserArrayIntLoop
				da.fastElemKind = reflect.Int32
				da.fastIfaceLoop = deserArrayIntIfaceLoop
			case "long":
				b.ser = sa.serLong
				da.fastLoop = deserArrayLongLoop
				da.fastElemKind = reflect.Int64
				da.fastIfaceLoop = deserArrayLongIfaceLoop
			case "float":
				b.ser = sa.serFloat
				da.fastLoop = deserArrayFloatLoop
				da.fastElemKind = reflect.Float32
				da.fastIfaceLoop = deserArrayFloatIfaceLoop
			case "double":
				b.ser = sa.serDouble
				da.fastLoop = deserArrayDoubleLoop
				da.fastElemKind = reflect.Float64
				da.fastIfaceLoop = deserArrayDoubleIfaceLoop
			default:
				b.ser = sa.ser
			}
		}
		b.deser = da.deser
		inner := new(fieldMeta)
		*inner = af.meta
		inner.minBytes = schemaMinBytes(af.node)
		b.meta = fieldMeta{avroType: "array", inner: inner}
		b.node = &schemaNode{
			kind:  "array",
			items: af.node,
			ser:   b.ser,
			deser: b.deser,
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
		if err := mf.build(parentName, o.Values); err != nil {
			return fmt.Errorf("invalid map: %v", err)
		}
		b.unnest(mf)
		o.Values = &mf.canon
		sm := &serMap{serItem: mf.ser}
		// minEntryBytes = 1 (empty-key length varint) + values' minimum
		// wire bytes. Matches deserArray.minItemBytes in spirit; bounds
		// block-count against remaining-buffer to prevent memory
		// amplification on hostile input.
		dm := &deserMap{deserItem: mf.deser, minEntryBytes: 1 + schemaMinBytes(mf.node)}
		// Same gate as the array case above: skip specialization when
		// values have a custom type or a logical type, so the inner
		// per-element function (serTimestampMillis / deserTimeMicros
		// / etc.) actually runs.
		if mf.meta.hasCustomType || mf.meta.logical != "" {
			b.ser = sm.ser
		} else {
			switch mf.canon.primitive {
			case "string":
				b.ser = sm.serString
				dm.fastBlock = deserMapStringBlock
				dm.fastElemKind = reflect.String
				dm.fastIfaceBlock = deserMapStringIfaceBlock
			case "boolean":
				b.ser = sm.serBoolean
				dm.fastBlock = deserMapBooleanBlock
				dm.fastElemKind = reflect.Bool
				dm.fastIfaceBlock = deserMapBooleanIfaceBlock
			case "int":
				b.ser = sm.serInt
				dm.fastBlock = deserMapIntBlock
				dm.fastElemKind = reflect.Int32
				dm.fastIfaceBlock = deserMapIntIfaceBlock
			case "long":
				b.ser = sm.serLong
				dm.fastBlock = deserMapLongBlock
				dm.fastElemKind = reflect.Int64
				dm.fastIfaceBlock = deserMapLongIfaceBlock
			case "float":
				b.ser = sm.serFloat
				dm.fastBlock = deserMapFloatBlock
				dm.fastElemKind = reflect.Float32
				dm.fastIfaceBlock = deserMapFloatIfaceBlock
			case "double":
				b.ser = sm.serDouble
				dm.fastBlock = deserMapDoubleBlock
				dm.fastElemKind = reflect.Float64
				dm.fastIfaceBlock = deserMapDoubleIfaceBlock
			default:
				b.ser = sm.ser
			}
		}
		b.deser = dm.deser
		b.meta = fieldMeta{avroType: "map"}
		b.node = &schemaNode{
			kind:   "map",
			values: mf.node,
			ser:    b.ser,
			deser:  b.deser,
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
		// Use raw fixed ser/deser when a custom type replaces the
		// built-in logical type handler.
		if b.hasMatchingCustomType("fixed", s.object.Logical) {
			b.ser = (&serSize{size}).ser
			b.deser = (&deserFixed{size}).deser
		} else {
			switch s.object.Logical {
			case "duration":
				b.ser = serDuration
				b.deser = deserDuration
			case "decimal":
				scale := 0
				if o.Scale != nil {
					scale = *o.Scale
				}
				b.ser = (&serFixedDecimal{size: size, precision: *o.Precision, scale: scale}).ser
				b.deser = (&deserFixedDecimal{size: size, scale: scale}).deser
			case "uuid":
				b.ser = serFixedUUIDReflect
				b.deser = deserFixedUUIDReflect
			default:
				b.ser = (&serSize{size}).ser
				b.deser = (&deserFixed{size}).deser
			}
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
		b.named[o.Name] = &namedType{ser: b.ser, deser: b.deser, node: nd,
			hadCustomType: len(b.customDecoderMap) > 0 || len(b.customEncodes) > 0}
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

	case "uuid":
		if o.Type != "string" && !(o.Type == "fixed" && o.Size != nil && int(*o.Size) == 16) {
			return fmt.Errorf("invalid logicalType uuid type %q, must be string or fixed(16)", o.Type)
		}

	case "date", "time-millis":
		if o.Type != "int" {
			return fmt.Errorf("invalid logicalType %s type %q, can only be int", o.Logical, o.Type)
		}

	case "time-micros",
		"timestamp-millis",
		"timestamp-micros",
		"timestamp-nanos",
		"local-timestamp-millis",
		"local-timestamp-micros",
		"local-timestamp-nanos":
		if o.Type != "long" {
			return fmt.Errorf("invalid logicalType %s type %q, can only be long", o.Logical, o.Type)
		}

	case "big-decimal":
		if o.Type != "bytes" {
			return fmt.Errorf("invalid logicalType big-decimal type %q, can only be bytes", o.Type)
		}

	case "duration":
		if o.Type != "fixed" {
			return fmt.Errorf("invalid logicalType duration type %q, can only be fixed", o.Type)
		}
		if o.Size == nil {
			return errors.New("invalid logicalType duration has no size")
		}
		if int(*o.Size) != 12 {
			return fmt.Errorf("invalid logicalType duration size %v is not the expected 12", int(*o.Size))
		}

	default:
		// Per the Avro spec, unknown logical types are ignored and the
		// underlying type is used as-is.
		o.Logical = ""
		return nil
	}

	if o.Scale != nil || o.Precision != nil {
		return fmt.Errorf("type %q logicalType %q: invalid scale or precision specified", o.Type, o.Logical)
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

// logicalSer returns a time-aware serializer for a given logical type,
// or nil if the logical type doesn't have special serialization.
func logicalSer(logical string) serfn {
	switch logical {
	case "timestamp-millis":
		return serTimestampMillis
	case "timestamp-micros":
		return serTimestampMicros
	case "timestamp-nanos":
		return serTimestampNanos
	case "local-timestamp-millis":
		return serLocalTimestampMillis
	case "local-timestamp-micros":
		return serLocalTimestampMicros
	case "local-timestamp-nanos":
		return serLocalTimestampNanos
	case "date":
		return serDate
	case "time-millis":
		return serTimeMillis
	case "time-micros":
		return serTimeMicros
	case "uuid":
		return serUUID
	default:
		return nil
	}
}

// logicalDeser returns a time-aware deserializer for a given logical type,
// or nil if the logical type doesn't have special deserialization.
func logicalDeser(logical string) deserfn {
	switch logical {
	case "timestamp-millis", "local-timestamp-millis":
		return deserTimestampMillis
	case "timestamp-micros", "local-timestamp-micros":
		return deserTimestampMicros
	case "timestamp-nanos", "local-timestamp-nanos":
		return deserTimestampNanos
	case "date":
		return deserDate
	case "time-millis":
		return deserTimeMillis
	case "time-micros":
		return deserTimeMicros
	case "uuid":
		return deserUUID
	default:
		return nil
	}
}

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

// defaultAsInt32 / defaultAsInt64 / defaultAsFloat64 extract a numeric
// default. After unmarshalDefault, a JSON number arrives as json.Number
// (full precision); a few callers also pass float64 (e.g. round-tripped
// through coerceDefault). Float-defaulted-from-string is accepted for
// float / double (Java parser leniency).

func defaultAsInt32(val any) (int32, error) {
	switch v := val.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			if i < math.MinInt32 || i > math.MaxInt32 {
				return 0, fmt.Errorf("value %d overflows int32", i)
			}
			return int32(i), nil
		}
		f, err := v.Float64()
		if err != nil {
			return 0, fmt.Errorf("invalid number %s", v.String())
		}
		return floatFitsInt32(f)
	case float64:
		return floatFitsInt32(v)
	}
	return 0, fmt.Errorf("expected number, got %T", val)
}

func defaultAsInt64(val any) (int64, error) {
	switch v := val.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i, nil
		}
		f, err := v.Float64()
		if err != nil {
			return 0, fmt.Errorf("invalid number %s", v.String())
		}
		return floatFitsInt64(f)
	case float64:
		return floatFitsInt64(v)
	}
	return 0, fmt.Errorf("expected number, got %T", val)
}

func defaultAsFloat64(val any) (float64, error) {
	switch v := val.(type) {
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return 0, fmt.Errorf("invalid number %s", v.String())
		}
		return f, nil
	case float64:
		return v, nil
	case string:
		// Java accepts "3.14" as a float/double default literal.
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid string default %q: %w", v, err)
		}
		return f, nil
	}
	return 0, fmt.Errorf("expected number, got %T", val)
}

// coerceDefault converts string default values to the expected type when
// the field's type is literally float or double, matching Java's
// Schema.parseField (Schema.java:1900-1902): the string→DoubleNode
// coercion is gated on Type.FLOAT.equals(...) || Type.DOUBLE.equals(...).
// Union-typed fields are NOT coerced at parse time — Java leaves the
// TextNode in place and lets the default flow through to the union's
// per-branch dispatch, where the first branch with isCompatible wins.
// We mirror that: union defaults pass through unchanged so a textual
// default into ["string","float"] picks the string branch (matches
// Java, fastavro, hamba). The float branch's defaultAsFloat64 has its
// own string-fallback for the `{"type":"float","default":"3.14"}`
// non-union case, kept here.
//
// Walks *schemaNode so that nested float fields reached through a name-
// reference are coerced too — the prior aschema-canon walker silently
// dropped name-refs (canon=aschema{primitive:<Name>}), leaving nested
// string defaults uncoerced and breaking JSON encode's
// jsonCoerceToFloat64 which rejects string inputs at default-fill time.
func coerceDefault(val any, node *schemaNode) any {
	if node == nil {
		return val
	}
	if node.kind == "union" {
		// Mirror convertDefaultBytes's union-case branch matcher:
		// the first branch validateDefault accepts is the one this
		// default resolves to, and we recurse so the coerced value
		// matches the picked branch's natural Go type. Without this,
		// a `["float","null"]` default of `"1.5"` stays as a Go
		// string and the JSON encoder's union try-each-branch loop
		// hits jsonCoerceToFloat64 (which rejects strings); the
		// binary encoder meanwhile coerces via defaultAsFloat64's
		// string fallback and picks the float branch — a silent
		// binary/JSON divergence on `["float","string"]` unions.
		for _, branch := range node.branches {
			if validateDefault(val, branch) == nil {
				return coerceDefault(val, branch)
			}
		}
		return val
	}
	if node.kind != "float" && node.kind != "double" {
		return val
	}
	str, ok := val.(string)
	if !ok {
		return val
	}
	if f, err := strconv.ParseFloat(str, 64); err == nil {
		return f
	}
	return val // leave as-is; validateDefault will report the error
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
// Walks *schemaNode (the resolved type tree) rather than the aschema
// canon so that named-type references — forward and backward — are
// followed naturally: a record field whose type is `aschema{primitive:
// "MyDec"}` has a node that already points at MyDec's resolved
// schemaNode. Called both from buildComplex's non-fwd-ref path and
// from finalize's fwd-ref fixup.
//
// Called after validateDefault has succeeded for non-fwd-ref fields;
// for fwd-ref fields validation is deferred and the conversion is
// best-effort (avroJSONBytesToBytes silently returns on error and we
// preserve the original value).
func convertDefaultBytes(val any, node *schemaNode) any {
	if node == nil {
		return val
	}
	switch node.kind {
	case "bytes", "fixed":
		if str, ok := val.(string); ok {
			if b, err := avroJSONBytesToBytes(str); err == nil {
				return b
			}
		}
		return val
	case "record":
		m, ok := val.(map[string]any)
		if !ok {
			return val
		}
		for _, f := range node.fields {
			if fv, exists := m[f.name]; exists {
				m[f.name] = convertDefaultBytes(fv, f.node)
			}
		}
		return val
	case "array":
		arr, ok := val.([]any)
		if !ok || node.items == nil {
			return val
		}
		for i, item := range arr {
			arr[i] = convertDefaultBytes(item, node.items)
		}
		return val
	case "map":
		m, ok := val.(map[string]any)
		if !ok || node.values == nil {
			return val
		}
		for k, v := range m {
			m[k] = convertDefaultBytes(v, node.values)
		}
		return val
	case "union":
		// First-matching-branch determines the type interpretation.
		// Reuse validateDefault as the matcher so we agree with the
		// validate pass on which branch this default lands on — a
		// structural-only check (e.g. "is val a string?") can pick a
		// fixed:N branch on a string default whose rune-count
		// doesn't fit, mutate it into a length-N []byte that no
		// branch can encode, and surface as "union default does not
		// match any branch" at encodeDefault time even though
		// validateDefault accepted the schema. validateDefault is
		// idempotent (coerceDefault on already-coerced floats is a
		// no-op) so re-running it here is safe.
		for _, branch := range node.branches {
			if validateDefault(val, branch) == nil {
				return convertDefaultBytes(val, branch)
			}
		}
	}
	return val
}

// validateDefault checks that a parsed JSON default value is compatible
// with the given Avro schema. Walks *schemaNode (the resolved type
// tree) so name-references — forward and backward — follow into the
// real type rather than slipping through with no validation (the prior
// aschema-canon walker hit validateDefaultPrimitive's "unknown
// primitive" silent-accept for any name-ref). Mutates record/array/map
// structures in place via coerceDefault, propagating float-from-string
// coercions to nested fields reached through name-refs.
//
// Returns nil for a nil node — fwd-refs defer validation to finalize,
// where the resolved node is wired up.
func validateDefault(val any, node *schemaNode) error {
	if node == nil {
		return nil
	}
	switch node.kind {
	case "null":
		if val != nil {
			return fmt.Errorf("expected null, got %T", val)
		}
	case "boolean":
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("expected boolean, got %T", val)
		}
	case "int":
		if _, err := defaultAsInt32(val); err != nil {
			return fmt.Errorf("int default: %w", err)
		}
	case "long":
		if _, err := defaultAsInt64(val); err != nil {
			return fmt.Errorf("long default: %w", err)
		}
	case "float", "double":
		if _, err := defaultAsFloat64(val); err != nil {
			return fmt.Errorf("%s default: %w", node.kind, err)
		}
	case "string":
		if _, ok := val.(string); !ok {
			return fmt.Errorf("expected string, got %T", val)
		}
	case "bytes":
		s, ok := val.(string)
		if !ok {
			return fmt.Errorf("expected string for bytes, got %T", val)
		}
		for _, r := range s {
			if r > 255 {
				return fmt.Errorf("bytes default contains code point U+%04X, max allowed is U+00FF", r)
			}
		}
	case "enum":
		sym, ok := val.(string)
		if !ok {
			return fmt.Errorf("expected string for enum default, got %T", val)
		}
		if len(node.symbols) > 0 && !slices.Contains(node.symbols, sym) {
			return fmt.Errorf("enum default %q is not a member of symbols", sym)
		}
	case "fixed":
		s, ok := val.(string)
		if !ok {
			return fmt.Errorf("expected string for fixed default, got %T", val)
		}
		for _, r := range s {
			if r > 255 {
				return fmt.Errorf("fixed default contains code point U+%04X, max allowed is U+00FF", r)
			}
		}
		if len([]rune(s)) != node.size {
			return fmt.Errorf("fixed default length %d does not match size %d", len([]rune(s)), node.size)
		}
	case "record":
		// null is not a record. Java's isValidDefault returns false for
		// null on RECORD (Schema.java case RECORD: `if (!defaultValue
		// .isObject()) return false;`); fastavro's _validate_record
		// requires isinstance(datum, Mapping); hamba's isValidDefault
		// returns false on type-assertion failure. Without this reject,
		// a union ["Record","null"] with default null would have its
		// validate-walk match the Record branch (synthesizing an empty
		// map + relying on per-field defaults) instead of falling
		// through to the null branch — encodeDefault would then emit
		// Record(field-defaults) wire bytes where null was intended.
		if val == nil {
			return fmt.Errorf("expected object for record default, got null")
		}
		m, ok := val.(map[string]any)
		if !ok {
			return fmt.Errorf("expected object for record default, got %T", val)
		}
		for _, f := range node.fields {
			fv, exists := m[f.name]
			if !exists {
				if !f.hasDefault {
					return fmt.Errorf("record default missing field %q with no default", f.name)
				}
				continue
			}
			fv = coerceDefault(fv, f.node)
			m[f.name] = fv
			if err := validateDefault(fv, f.node); err != nil {
				return fmt.Errorf("field %q: %w", f.name, err)
			}
		}
	case "array":
		if val == nil {
			return fmt.Errorf("expected array for array default, got null")
		}
		arr, ok := val.([]any)
		if !ok {
			return fmt.Errorf("expected array for array default, got %T", val)
		}
		for i, elem := range arr {
			coerced := coerceDefault(elem, node.items)
			arr[i] = coerced
			if err := validateDefault(coerced, node.items); err != nil {
				return fmt.Errorf("array element %d: %w", i, err)
			}
		}
	case "map":
		if val == nil {
			return fmt.Errorf("expected object for map default, got null")
		}
		m, ok := val.(map[string]any)
		if !ok {
			return fmt.Errorf("expected object for map default, got %T", val)
		}
		for k, v := range m {
			coerced := coerceDefault(v, node.values)
			m[k] = coerced
			if err := validateDefault(coerced, node.values); err != nil {
				return fmt.Errorf("map key %q: %w", k, err)
			}
		}
	case "union":
		// Avro 1.12+ relaxed the union-default rule: the default may
		// match any branch (formerly required to match the first
		// branch). See AVRO-3649 / PR apache/avro#2503. We walk
		// branches in declaration order and accept the first that
		// matches, matching Java 1.12+ and fastavro v1.7+.
		for _, branch := range node.branches {
			if validateDefault(val, branch) == nil {
				return nil
			}
		}
		return fmt.Errorf("default does not match any union branch: %T(%v)", val, val)
	}
	return nil
}

