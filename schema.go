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
	serRecord   *serRecord
	deserRecord *deserRecord

	props map[string]any // extra schema properties (for CustomType callbacks)
}

// fieldNode represents a record field with full metadata.
type fieldNode struct {
	name       string
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
// doc, aliases, defaults, and other non-essential attributes. The result is
// deterministic and suitable for comparison and fingerprinting.
func (s *Schema) Canonical() []byte {
	b, _ := json.Marshal(s.c)
	return b
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
		if err := json.Unmarshal(data, &s.object); err != nil {
			return err
		}
		// Capture extra properties not in the struct tags.
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(data, &raw); err == nil {
			for k := range raw {
				if aobjectKnownKeys[k] {
					continue
				}
				if s.object.extra == nil {
					s.object.extra = make(map[string]any)
				}
				var v any
				json.Unmarshal(raw[k], &v)
				s.object.extra[k] = v
			}
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

	// hasDefault is true if the field has a default value. This is set
	// in canonical afields (which strip Default) so that validateDefault
	// can check whether nested record fields have defaults.
	hasDefault bool
}

type aobject struct {
	Name string `json:"name"`
	Type string `json:"type"`

	// A complex type can be one of many options. In canonical form, the
	// json fields are ordered "type", "name", and then one of the fields
	// below.

	Fields  []afield `json:"fields,omitempty"`  // record
	Symbols []string `json:"symbols,omitempty"` // enum
	Items   *aschema `json:"items,omitempty"`   // array
	Values  *aschema `json:"values,omitempty"`  // map
	Size    *int     `json:"size,omitempty"`    // fixed

	// In canonical form, the following are stripped.

	Namespace *string         `json:"namespace,omitempty"`
	Aliases   []string        `json:"aliases,omitempty"`
	Default   json.RawMessage `json:"default,omitempty"`

	Logical   string `json:"logicalType,omitempty"`
	Scale     *int   `json:"scale,omitempty"`     // decimal logical type
	Precision *int   `json:"precision,omitempty"` // decimal logical type

	extra map[string]any // non-reserved properties, populated by aschema.UnmarshalJSON
}

var aobjectKnownKeys = map[string]bool{
	"type": true, "name": true, "namespace": true, "doc": true,
	"fields": true, "symbols": true, "items": true, "values": true,
	"size": true, "logicalType": true, "precision": true, "scale": true,
	"aliases": true, "default": true, "order": true,
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
			defaultBytes, err := encodeDefault(m.defaultVal, nt.node)
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

// applyCustomTypes wires matching CustomType registrations into the
// schema node's ser/deser/customEncode functions.
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
			// match before the pointer is stripped.
			for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
				if v.IsNil() {
					return v, nil
				}
				for _, enc := range encoders {
					if enc.goType != nil && v.Type() == enc.goType {
						result, err := enc.fn(v.Interface(), sn)
						if err != nil {
							if errors.Is(err, ErrSkipCustomType) {
								break
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
		b.ser = func(dst []byte, v reflect.Value) ([]byte, error) {
			v, err := ce(v)
			if err != nil {
				return nil, err
			}
			return innerSer(dst, v)
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
			if pe := (*unknownPrimitiveError)(nil); !errors.As(err, &pe) {
				return fmt.Errorf("invalid union: %w", err)
			}
			missing[i] = us.primitive
		}
		b.unnest(u)
		branchMetas[i] = u.meta
		branchNodes[i] = u.node

		typ, name, err := us.unionTypeName()
		if err != nil {
			return err
		}
		if sawTypes[typ] && name == "" {
			return fmt.Errorf("duplicate union type %q", typ)
		}
		sawTypes[typ] = true

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
	}

	if len(s.union) == 2 && s.union[0].primitive == "null" {
		b.ser = serNullUnion(ser)
		b.deser = deserNullUnion(deser)
		if _, isMissing := missing[1]; isMissing {
			inner := &fieldMeta{}
			b.meta = fieldMeta{avroType: "nullunion", inner: inner}
			b.mfixups = append(b.mfixups, metaFixup{meta: inner, name: s.union[1].primitive})
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
			b.mfixups = append(b.mfixups, metaFixup{meta: inner, name: s.union[0].primitive})
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
			b.ser = (&serBytesDecimal{scale: scale}).ser
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
		b.ser = ser
		b.deser = deserPrimitive[o.Type]
		// Skip built-in logical type handlers when a custom type
		// matches — the custom type replaces them entirely, receiving
		// the raw Avro-native value (not the enriched type).
		if !b.hasMatchingCustomType(o.Type, o.Logical) {
			if logSer := logicalSer(o.Logical); logSer != nil {
				b.ser = logSer
			}
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
		// Note: decimal primitives are handled by the early return
		// at the top of this block (L504-522), so they never reach here.
		b.node = nd
		return nil
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
		b.named[o.Name] = &namedType{ser: b.ser, deser: b.deser, sr: sr, dr: dr, node: nd}
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
				// An unknownPrimitiveError for a primitive ref means
				// the type hasn't been parsed yet — treat it as a
				// forward reference to be resolved in finalize().
				if pe := (*unknownPrimitiveError)(nil); errors.As(err, &pe) && of.Type != nil && of.Type.primitive != "" {
					isFwdRef = true
					fwdRefName = of.Type.primitive
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
				avroType: meta.avroType,
				meta:     meta,
			}
			fn := fieldNode{
				name:    of.Name,
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
					var dv any
					// json.Unmarshal cannot fail: of.Default is a json.RawMessage
					// preserved from the initial schema parse, so it is valid JSON.
					json.Unmarshal(of.Default, &dv)
					fix.defaultVal = dv
					fix.hasDefault = true
				}
				b.fieldFixups = append(b.fieldFixups, fix)
			}
			if len(of.Default) > 0 {
				var defaultVal any
				// json.Unmarshal cannot fail: of.Default is a json.RawMessage
				// preserved from the initial schema parse, so it is valid JSON.
				json.Unmarshal(of.Default, &defaultVal)
				// Skip default validation for forward references since we
				// don't know the type yet.
				if !isFwdRef {
					if err := validateDefault(defaultVal, &bf.canon); err != nil {
						return fmt.Errorf("record field %q: invalid default: %v", of.Name, err)
					}
				}
				drf.defaultVal = defaultVal
				drf.hasDefault = true
				fn.defaultVal = defaultVal
				fn.hasDefault = true
				// Pre-encode the default to Avro binary for use
				// when encoding maps with missing keys.
				if !isFwdRef && bf.node != nil {
					defaultBytes, err := encodeDefault(defaultVal, bf.node)
					if err != nil {
						return fmt.Errorf("record field %q: encoding default: %v", of.Name, err)
					}
					sr.fields[fieldIdx].defaultBytes = defaultBytes
					sr.fields[fieldIdx].hasDefault = true
				}
			}
			dr.fields = append(dr.fields, drf)
			nd.fields = append(nd.fields, fn)
			names = append(names, of.Name)
		}
		sr.names = names
		dr.names = names

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
		b.ser = (&serEnum{symbols: o.Symbols}).ser
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
		b.named[o.Name] = &namedType{ser: b.ser, deser: b.deser, node: nd}
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
		da := &deserArray{deserItem: af.deser}
		// When items have a custom type, skip specialized array
		// ser/deser fast paths — they bypass the item's wrapped
		// ser/deser functions.
		if af.meta.hasCustomType {
			b.ser = sa.ser
		} else {
			switch af.canon.primitive {
			case "string":
				b.ser = sa.serString
				da.fastLoop = deserArrayStringLoop
				da.fastElemKind = reflect.String
			case "boolean":
				b.ser = sa.serBoolean
				da.fastLoop = deserArrayBooleanLoop
				da.fastElemKind = reflect.Bool
			case "int":
				b.ser = sa.serInt
				da.fastLoop = deserArrayIntLoop
				da.fastElemKind = reflect.Int32
			case "long":
				b.ser = sa.serLong
				da.fastLoop = deserArrayLongLoop
				da.fastElemKind = reflect.Int64
			case "float":
				b.ser = sa.serFloat
				da.fastLoop = deserArrayFloatLoop
				da.fastElemKind = reflect.Float32
			case "double":
				b.ser = sa.serDouble
				da.fastLoop = deserArrayDoubleLoop
				da.fastElemKind = reflect.Float64
			default:
				b.ser = sa.ser
			}
		}
		b.deser = da.deser
		inner := new(fieldMeta)
		*inner = af.meta
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
		dm := &deserMap{deserItem: mf.deser}
		if mf.meta.hasCustomType {
			b.ser = sm.ser
		} else {
			switch mf.canon.primitive {
			case "string":
				b.ser = sm.serString
				dm.fastBlock = deserMapStringBlock
				dm.fastElemKind = reflect.String
			case "boolean":
				b.ser = sm.serBoolean
				dm.fastBlock = deserMapBooleanBlock
				dm.fastElemKind = reflect.Bool
			case "int":
				b.ser = sm.serInt
				dm.fastBlock = deserMapIntBlock
				dm.fastElemKind = reflect.Int32
			case "long":
				b.ser = sm.serLong
				dm.fastBlock = deserMapLongBlock
				dm.fastElemKind = reflect.Int64
			case "float":
				b.ser = sm.serFloat
				dm.fastBlock = deserMapFloatBlock
				dm.fastElemKind = reflect.Float32
			case "double":
				b.ser = sm.serDouble
				dm.fastBlock = deserMapDoubleBlock
				dm.fastElemKind = reflect.Float64
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
		if *o.Size <= 0 {
			return fmt.Errorf("invalid fixed size %v", *o.Size)
		}
		// Use raw fixed ser/deser when a custom type replaces the
		// built-in logical type handler.
		if b.hasMatchingCustomType("fixed", s.object.Logical) {
			b.ser = (&serSize{*o.Size}).ser
			b.deser = (&deserFixed{*o.Size}).deser
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
				b.ser = (&serFixedDecimal{size: *o.Size, scale: scale}).ser
				b.deser = (&deserFixedDecimal{size: *o.Size, scale: scale}).deser
			default:
				b.ser = (&serSize{*o.Size}).ser
				b.deser = (&deserFixed{*o.Size}).deser
			}
		}
		b.meta = fieldMeta{avroType: "fixed", logical: s.object.Logical}
		nd := &schemaNode{
			kind:    "fixed",
			name:    o.Name,
			aliases: qualifyAliases(origAliases, o.Name),
			logical: s.object.Logical,
			size:    *o.Size,
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
		b.named[o.Name] = &namedType{ser: b.ser, deser: b.deser, node: nd}
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
		if o.Type != "bytes" && o.Type != "fixed" {
			// Wrong underlying type: fall back to underlying type.
			o.Logical = ""
			return nil
		}
		if o.Precision == nil || *o.Precision <= 0 {
			// Invalid precision: fall back to underlying type.
			o.Logical = ""
			return nil
		}
		scale := 0
		if o.Scale != nil {
			scale = *o.Scale
		}
		if scale < 0 || scale > *o.Precision {
			// Invalid scale: fall back to underlying type.
			o.Logical = ""
			return nil
		}
		if o.Type == "fixed" && o.Size != nil {
			maxDigits := maxDecimalDigits(*o.Size)
			if *o.Precision > maxDigits {
				// Precision exceeds fixed capacity: fall back.
				o.Logical = ""
				return nil
			}
		}
		return nil

	case "uuid":
		if o.Type != "string" && !(o.Type == "fixed" && o.Size != nil && *o.Size == 16) {
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
		if *o.Size != 12 {
			return fmt.Errorf("invalid logicalType duration size %v is not the expected 12", *o.Size)
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
	case "timestamp-millis", "local-timestamp-millis":
		return serTimestampMillis
	case "timestamp-micros", "local-timestamp-micros":
		return serTimestampMicros
	case "timestamp-nanos", "local-timestamp-nanos":
		return serTimestampNanos
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

// validateDefault checks that a parsed JSON default value is compatible
// with the given Avro schema type.
func validateDefault(val any, s *aschema) error {
	if s.primitive != "" {
		return validateDefaultPrimitive(val, s.primitive)
	}
	if len(s.union) > 0 {
		// For unions, the default must match the first branch type.
		return validateDefault(val, &s.union[0])
	}
	if s.object != nil {
		switch s.object.Type {
		case "record", "error":
			m, ok := val.(map[string]any)
			if !ok && val != nil {
				return fmt.Errorf("expected object for record default, got %T", val)
			}
			if m == nil {
				m = make(map[string]any)
			}
			for _, f := range s.object.Fields {
				fv, exists := m[f.Name]
				if !exists {
					if !f.hasDefault {
						return fmt.Errorf("record default missing field %q with no default", f.Name)
					}
					continue
				}
				if err := validateDefault(fv, f.Type); err != nil {
					return fmt.Errorf("field %q: %w", f.Name, err)
				}
			}
		case "enum":
			sym, ok := val.(string)
			if !ok {
				return fmt.Errorf("expected string for enum default, got %T", val)
			}
			found := slices.Contains(s.object.Symbols, sym)
			if !found && len(s.object.Symbols) > 0 {
				return fmt.Errorf("enum default %q is not a member of symbols", sym)
			}
		case "array":
			arr, ok := val.([]any)
			if !ok && val != nil {
				return fmt.Errorf("expected array for array default, got %T", val)
			}
			if arr != nil && s.object.Items != nil {
				for i, elem := range arr {
					if err := validateDefault(elem, s.object.Items); err != nil {
						return fmt.Errorf("array element %d: %w", i, err)
					}
				}
			}
		case "map":
			m, ok := val.(map[string]any)
			if !ok && val != nil {
				return fmt.Errorf("expected object for map default, got %T", val)
			}
			if m != nil && s.object.Values != nil {
				for k, v := range m {
					if err := validateDefault(v, s.object.Values); err != nil {
						return fmt.Errorf("map key %q: %w", k, err)
					}
				}
			}
		case "fixed":
			str, ok := val.(string)
			if !ok {
				return fmt.Errorf("expected string for fixed default, got %T", val)
			}
			for _, r := range str {
				if r > 255 {
					return fmt.Errorf("fixed default contains code point U+%04X, max allowed is U+00FF", r)
				}
			}
			if s.object.Size != nil && len([]rune(str)) != *s.object.Size {
				return fmt.Errorf("fixed default length %d does not match size %d", len([]rune(str)), *s.object.Size)
			}
		}
	}
	return nil
}

func validateDefaultPrimitive(val any, prim string) error {
	switch prim {
	case "null":
		if val != nil {
			return fmt.Errorf("expected null, got %T", val)
		}
	case "boolean":
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("expected boolean, got %T", val)
		}
	case "int":
		f, ok := val.(float64)
		if !ok {
			return fmt.Errorf("expected number for int, got %T", val)
		}
		if f != math.Trunc(f) {
			return fmt.Errorf("int default %v is not a whole number", f)
		}
		if f < math.MinInt32 || f > math.MaxInt32 {
			return fmt.Errorf("int default %v out of range", f)
		}
	case "long":
		f, ok := val.(float64)
		if !ok {
			return fmt.Errorf("expected number for long, got %T", val)
		}
		if f != math.Trunc(f) {
			return fmt.Errorf("long default %v is not a whole number", f)
		}
		if f < -(1<<63) || f >= 1<<63 {
			return fmt.Errorf("long default %v out of range", f)
		}
	case "float", "double":
		if _, ok := val.(float64); !ok {
			return fmt.Errorf("expected number for %s, got %T", prim, val)
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
	}
	return nil
}
