package avro

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"regexp"
	"strings"
)

// Schema is a compiled Avro schema that supports encoding Go values to Avro
// binary format and decoding Avro binary data into Go values. A Schema is
// created by parsing an Avro JSON schema string with [NewSchema].
//
// A Schema is safe for concurrent use. It can be used with [Schema.Encode],
// [Schema.AppendEncode], and [Schema.Decode] for standard binary encoding, or
// with [Schema.AppendSingleObject] and [Schema.DecodeSingleObject] for Avro
// Single Object Encoding. Schemas can also be used with [Resolve] to support
// schema evolution between reader and writer schemas.
type Schema struct {
	ser   serfn
	deser deserfn

	c    aschema
	soe  [10]byte    // 2-byte magic (0xC3, 0x01) + 8-byte LE CRC64-Avro fingerprint
	node *schemaNode // full metadata tree (aliases, defaults, etc.)
}

// schemaNode preserves full schema metadata that canonical form strips:
// aliases, defaults, enum defaults, and links to compiled ser/deser.
type schemaNode struct {
	kind       string        // "null","boolean","int","long","float","double","bytes","string","record","enum","array","map","fixed","union"
	name       string        // fully-qualified name (named types only)
	aliases    []string      // named type aliases (fully qualified)
	logical    string        // logical type
	fields     []fieldNode   // record fields
	symbols    []string      // enum symbols
	enumDef    string        // enum default symbol
	hasEnumDef bool          // whether enum default is specified
	items      *schemaNode   // array item type
	values     *schemaNode   // map value type
	size       int           // fixed size
	branches   []*schemaNode // union branches
	ser        serfn
	deser      deserfn
	serRecord  *serRecord
	deserRecord *deserRecord
}

// fieldNode represents a record field with full metadata.
type fieldNode struct {
	name       string
	aliases    []string
	node       *schemaNode
	defaultVal any
	hasDefault bool
}

// NewSchema parses an Avro JSON schema string and returns a compiled
// [*Schema]. The input must be a valid Avro schema in JSON format: a
// primitive type name (e.g. "string"), a JSON object (record, enum, array,
// map, fixed), or a JSON array (union).
//
// Named types (records, enums, fixed) may reference each other and
// self-reference. Type names are resolved according to Avro namespace rules.
// The schema is validated during parsing: unknown types, duplicate names,
// invalid defaults, and malformed definitions all return errors.
func NewSchema(schema string) (*Schema, error) {
	var orig aschema
	if err := json.Unmarshal([]byte(schema), &orig); err != nil {
		return nil, fmt.Errorf("invalid schema: %v", err)
	}

	b := &builder{
		types:   make(map[string]serfn),
		dtypes:  make(map[string]deserfn),
		stypes:  make(map[string]*serRecord),
		drtypes: make(map[string]*deserRecord),
		ntypes:  make(map[string]*schemaNode),
	}
	if err := b.build("", &orig); err != nil {
		return nil, err
	}
	if err := b.finalize(); err != nil {
		return nil, err
	}
	s := &Schema{
		ser:   b.ser,
		deser: b.deser,
		c:     b.canon,
		node:  b.node,
	}
	s.soe[0] = 0xC3
	s.soe[1] = 0x01
	h := NewRabin()
	h.Write(s.Canonical())
	binary.LittleEndian.PutUint64(s.soe[2:], h.Sum64())
	return s, nil
}

// Canonical returns the Parsing Canonical Form of the schema as defined by
// the Avro specification. The canonical form strips doc, aliases, default
// values, and other non-essential attributes, producing a deterministic JSON
// representation suitable for schema comparison and fingerprinting.
func (s *Schema) Canonical() []byte {
	b, _ := json.Marshal(s.c)
	return b
}

// Fingerprint computes a fingerprint of the schema's canonical form using the
// provided hash. The Avro specification recommends CRC-64-AVRO (see
// [NewRabin]) for general use or SHA-256 for cross-language compatibility.
func (s *Schema) Fingerprint(h hash.Hash) []byte {
	h.Write(s.Canonical())
	return h.Sum(nil)
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
		return json.Unmarshal(data, &s.object)
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

	Aliases []string `json:"aliases,omitempty"`
	Default json.RawMessage `json:"default,omitempty"`
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

	Namespace string          `json:"namespace,omitempty"`
	Aliases   []string        `json:"aliases,omitempty"`
	Default   json.RawMessage `json:"default,omitempty"`

	Logical   string `json:"logicalType,omitempty"`
	Scale     *int   `json:"scale,omitempty"`     // decimal logical type
	Precision *int   `json:"precision,omitempty"` // decimal logical type
}

var acomplex = map[string]struct{}{
	"record": {},
	"enum":   {},
	"array":  {},
	"map":    {},
	"fixed":  {},
}

type unionMissing struct {
	ser     *serUnion
	missing map[int]string
}

type unionMissingDeser struct {
	deser   *deserUnion
	missing map[int]string
}

type fieldMeta struct {
	avroType    string
	logical     string // logical type (e.g. "timestamp-millis"), empty if none
	serRecord   *serRecord
	deserRecord *deserRecord
	inner       *fieldMeta
}

type metaFixup struct {
	meta *fieldMeta
	name string
}

type builder struct {
	ser   serfn
	deser deserfn

	types    map[string]serfn
	dtypes   map[string]deserfn
	stypes   map[string]*serRecord
	drtypes  map[string]*deserRecord
	missing  []unionMissing
	dmissing []unionMissingDeser
	mfixups  []metaFixup

	ntypes map[string]*schemaNode

	meta  fieldMeta
	canon aschema
	node  *schemaNode
}

func (b *builder) nest() *builder {
	return &builder{
		types:   b.types,
		dtypes:  b.dtypes,
		stypes:  b.stypes,
		drtypes: b.drtypes,
		ntypes:  b.ntypes,
	}
}

func (b *builder) unnest(nest *builder) {
	b.missing = append(b.missing, nest.missing...)
	b.dmissing = append(b.dmissing, nest.dmissing...)
	b.mfixups = append(b.mfixups, nest.mfixups...)
}

func (b *builder) finalize() error {
	for _, m := range b.missing {
		for idx, name := range m.missing {
			ser := b.types[name]
			if ser == nil {
				return fmt.Errorf("unknown type %q", name)
			}
			m.ser.fns[idx] = ser
		}
	}
	for _, m := range b.dmissing {
		for idx, name := range m.missing {
			m.deser.fns[idx] = b.dtypes[name]
		}
	}
	for _, m := range b.mfixups {
		m.meta.serRecord = b.stypes[m.name]
		m.meta.deserRecord = b.drtypes[m.name]
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
	case "record", "fixed", "enum":
		return s.object.Type, s.object.Name, nil
	default:
		return s.object.Type, "", nil
	}
}

type unknownPrimitiveError struct{ p string }

func (e *unknownPrimitiveError) Error() string { return fmt.Sprintf("unknown primitive %q", e.p) }

var (
	reName      = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	reNamespace = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$`)
)

func (b *builder) build(parentName string, s *aschema) error {
	if s == nil || s.primitive == "" && s.object == nil && len(s.union) == 0 {
		return errors.New("schema is not a primitive, complex, nor union")
	}

	switch {
	case s.primitive != "":
		return b.buildPrimitive(s)
	case len(s.union) != 0:
		return b.buildUnion(parentName, s)
	default:
		return b.buildComplex(parentName, s)
	}
}

func (b *builder) buildPrimitive(s *aschema) error {
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
	if ser := b.types[s.primitive]; ser != nil {
		b.ser = ser
		b.deser = b.dtypes[s.primitive]
		if sr := b.stypes[s.primitive]; sr != nil {
			b.meta = fieldMeta{avroType: "record", serRecord: sr, deserRecord: b.drtypes[s.primitive]}
		}
		b.node = b.ntypes[s.primitive]
		return nil
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
		ser          = new(serUnion)
		deser        = new(deserUnion)
		missing      = make(map[int]string)
		sawTypes     = make(map[string]bool)
		sawNames     = make(map[string]bool)
		branchMetas  = make([]fieldMeta, len(s.union))
		branchNodes  = make([]*schemaNode, len(s.union))
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
		if sawTypes[typ] {
			if name == "" {
				return fmt.Errorf("duplicate union type %q", typ)
			}
			if sawNames[name] {
				return fmt.Errorf("duplicate union name %q", name)
			}
		}
		sawTypes[typ] = true
		if name != "" {
			sawNames[name] = true
		}

		b.canon.union = append(b.canon.union, u.canon)
		ser.fns = append(ser.fns, u.ser)
		deser.fns = append(deser.fns, u.deser)
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

	if err := o.validateLogical(); err != nil {
		return err
	}

	if ser, isPrimitive := serPrimitive[o.Type]; isPrimitive {
		if o.Logical == "decimal" {
			scale := 0
			if o.Scale != nil {
				scale = *o.Scale
			}
			b.ser = (&serBytesDecimal{scale: scale}).ser
			b.deser = (&deserBytesDecimal{scale: scale}).deser
			b.canon = aschema{primitive: o.Type}
			b.meta = fieldMeta{avroType: o.Type, logical: o.Logical}
			return nil
		}
		b.ser = ser
		b.deser = deserPrimitive[o.Type]
		if logSer := logicalSer(o.Logical); logSer != nil {
			b.ser = logSer
		}
		if logDeser := logicalDeser(o.Logical); logDeser != nil {
			b.deser = logDeser
		}
		b.canon = aschema{primitive: o.Type}
		b.meta = fieldMeta{avroType: o.Type, logical: o.Logical}
		b.node = &schemaNode{
			kind:    o.Type,
			logical: o.Logical,
			ser:     b.ser,
			deser:   b.deser,
		}
		return nil
	}

	// Preserve original aliases and enum default before canonical stripping.
	origAliases := s.object.Aliases
	origEnumDefault := s.object.Default
	origFieldAliases := make([][]string, len(s.object.Fields))
	for i, f := range s.object.Fields {
		origFieldAliases[i] = f.Aliases
	}

	o = &aobject{
		Name: o.Name,
		Type: o.Type,

		Fields:  o.Fields,
		Symbols: o.Symbols,
		Items:   o.Items,
		Values:  o.Values,
		Size:    o.Size,

		Logical:   o.Logical,
		Precision: o.Precision,
		Scale:     o.Scale,

		Namespace: o.Namespace,
	}
	b.canon = aschema{object: o}

	switch o.Type {
	case "record", "enum", "fixed":
		if !reName.MatchString(o.Name) {
			if reNamespace.MatchString(o.Name) {
				parentName, o.Namespace = "", "" // fullname: ignore parent & our own namespace
			} else {
				return fmt.Errorf("invalid name %q", o.Name)
			}
		}
		if o.Namespace != "" {
			if !reNamespace.MatchString(o.Namespace) {
				return fmt.Errorf("invalid namespace %q", o.Namespace)
			}
			o.Name = o.Namespace + "." + o.Name // have namespace: prefix our name
			o.Namespace = ""
		} else if parentName != "" {
			if dot := strings.LastIndexByte(parentName, '.'); dot >= 0 {
				o.Name = parentName[:dot+1] + o.Name // no namespace: prefix our name with parent namespace if there is one
				o.Namespace = ""
			}
		}
	default:
		if o.Name != "" || o.Namespace != "" {
			return errors.New("only record, enum, and fixed can have a name")
		}
	}

	switch o.Type {
	default:
		return fmt.Errorf("unknown complex type %q", o.Type)

	case "record":
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
		b.types[o.Name] = b.ser
		b.dtypes[o.Name] = b.deser
		b.stypes[o.Name] = sr
		b.drtypes[o.Name] = dr
		b.meta = fieldMeta{avroType: "record", serRecord: sr, deserRecord: dr}

		// Register schemaNode early for self-referencing records.
		nd := &schemaNode{
			kind:        "record",
			name:        o.Name,
			aliases:     qualifyAliases(origAliases, o.Name),
			ser:         b.ser,
			deser:       b.deser,
			serRecord:   sr,
			deserRecord: dr,
		}
		b.ntypes[o.Name] = nd
		b.node = nd

		var names []string
		seenFields := make(map[string]bool, len(o.Fields))
		for i, of := range o.Fields {
			if !reName.MatchString(of.Name) {
				return fmt.Errorf("invalid record field name %q", of.Name)
			}
			if seenFields[of.Name] {
				return fmt.Errorf("duplicate record field name %q", of.Name)
			}
			seenFields[of.Name] = true
			bf := b.nest()
			if err := bf.build(o.Name, of.Type); err != nil {
				return fmt.Errorf("invalid record field: %v", err)
			}
			b.unnest(bf)
			o.Fields[i] = afield{
				Name: of.Name,
				Type: &bf.canon,
			}
			meta := new(fieldMeta)
			*meta = bf.meta
			sr.fields = append(sr.fields, serRecordField{
				name:     of.Name,
				fn:       bf.ser,
				avroType: meta.avroType,
				meta:     meta,
			})
			drf := deserRecordField{
				name:     of.Name,
				fn:       bf.deser,
				avroType: meta.avroType,
				meta:     meta,
			}
			fn := fieldNode{
				name:    of.Name,
				aliases: origFieldAliases[i],
				node:    bf.node,
			}
			if len(of.Default) > 0 {
				var defaultVal any
				// json.Unmarshal cannot fail: of.Default is a json.RawMessage
				// preserved from the initial schema parse, so it is valid JSON.
				json.Unmarshal(of.Default, &defaultVal) //nolint:errcheck
				if err := validateDefault(defaultVal, &bf.canon); err != nil {
					return fmt.Errorf("record field %q: invalid default: %v", of.Name, err)
				}
				drf.defaultVal = defaultVal
				drf.hasDefault = true
				fn.defaultVal = defaultVal
				fn.hasDefault = true
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

		seenSymbols := make(map[string]bool, len(o.Symbols))
		for _, e := range o.Symbols {
			if !reName.MatchString(e) {
				return fmt.Errorf("invalid enum symbol %q", e)
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
			aliases: qualifyAliases(origAliases, o.Name),
			symbols: o.Symbols,
			ser:     b.ser,
			deser:   b.deser,
		}
		if len(origEnumDefault) > 0 {
			var defStr string
			json.Unmarshal(origEnumDefault, &defStr) //nolint:errcheck
			nd.enumDef = defStr
			nd.hasEnumDef = true
		}
		b.ntypes[o.Name] = nd
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
		b.ser = (&serArray{af.ser}).ser
		b.deser = (&deserArray{af.deser}).deser
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
		b.ser = (&serMap{mf.ser}).ser
		b.deser = (&deserMap{mf.deser}).deser
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
		if *o.Size < 0 {
			return fmt.Errorf("invalid fixed size %v", *o.Size)
		}
		switch o.Logical {
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
		b.meta = fieldMeta{avroType: "fixed", logical: o.Logical}
		b.node = &schemaNode{
			kind:    "fixed",
			name:    o.Name,
			aliases: qualifyAliases(origAliases, o.Name),
			logical: o.Logical,
			size:    *o.Size,
			ser:     b.ser,
			deser:   b.deser,
		}
		b.ntypes[o.Name] = b.node
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
		if o.Precision == nil {
			return errors.New("logicalType decimal missing precision")
		}
		if o.Type != "bytes" && o.Type != "fixed" {
			return fmt.Errorf("invalid logicalType decimal type %q, can only be bytes or fixed", o.Type)
		}
		return nil

	case "uuid":
		if o.Type != "string" {
			return fmt.Errorf("invalid logicalType uuid type %q, can only be string", o.Type)
		}

	case "date",
		"time-millis":
		if o.Type != "int" {
			return fmt.Errorf("invalid logicalType %s type %q, can only be int", o.Logical, o.Type)
		}

	case "time-micros",
		"timestamp-millis",
		"timestamp-micros",
		"local-timestamp-millis",
		"local-timestamp-micros":
		if o.Type != "long" {
			return fmt.Errorf("invalid logicalType %s type %q, can only be long", o.Logical, o.Type)
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

// logicalSer returns a time-aware serializer for a given logical type,
// or nil if the logical type doesn't have special serialization.
func logicalSer(logical string) serfn {
	switch logical {
	case "timestamp-millis", "local-timestamp-millis":
		return serTimestampMillis
	case "timestamp-micros", "local-timestamp-micros":
		return serTimestampMicros
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
		case "record":
			if _, ok := val.(map[string]any); !ok && val != nil {
				return fmt.Errorf("expected object for record default, got %T", val)
			}
		case "enum":
			if _, ok := val.(string); !ok {
				return fmt.Errorf("expected string for enum default, got %T", val)
			}
		case "array":
			if _, ok := val.([]any); !ok && val != nil {
				return fmt.Errorf("expected array for array default, got %T", val)
			}
		case "map":
			if _, ok := val.(map[string]any); !ok && val != nil {
				return fmt.Errorf("expected object for map default, got %T", val)
			}
		case "fixed":
			if _, ok := val.(string); !ok {
				return fmt.Errorf("expected string for fixed default, got %T", val)
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
	case "int", "long":
		if _, ok := val.(float64); !ok {
			return fmt.Errorf("expected number for %s, got %T", prim, val)
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
		if _, ok := val.(string); !ok {
			return fmt.Errorf("expected string for bytes, got %T", val)
		}
	}
	return nil
}
