package avro

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

type Schema struct {
	ser   serfn
	deser deserfn

	c aschema
}

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
	}
	if err := b.build("", &orig); err != nil {
		return nil, err
	}
	if err := b.finalize(); err != nil {
		return nil, err
	}
	return &Schema{
		ser:   b.ser,
		deser: b.deser,
		c:     b.canon,
	}, nil
}

func (s *Schema) ParsingCanonicalForm() []byte {
	b, _ := json.Marshal(s.c)
	return b
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
	Default string   `json:"default,omitempty"`
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

	meta  fieldMeta
	canon aschema
}

func (b *builder) nest() *builder {
	return &builder{
		types:   b.types,
		dtypes:  b.dtypes,
		stypes:  b.stypes,
		drtypes: b.drtypes,
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
	reNamespace = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)$`)
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
		return nil
	}
	// Check if this is a named type reference (record, enum, fixed).
	if ser := b.types[s.primitive]; ser != nil {
		b.ser = ser
		b.deser = b.dtypes[s.primitive]
		if sr := b.stypes[s.primitive]; sr != nil {
			b.meta = fieldMeta{avroType: "record", serRecord: sr, deserRecord: b.drtypes[s.primitive]}
		}
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
		ser         = new(serUnion)
		deser       = new(deserUnion)
		missing     = make(map[int]string)
		sawTypes    = make(map[string]bool)
		sawNames    = make(map[string]bool)
		branchMetas = make([]fieldMeta, len(s.union))
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
		b.ser = ser
		b.deser = deserPrimitive[o.Type]
		b.canon = aschema{primitive: o.Type}
		b.meta = fieldMeta{avroType: o.Type}
		return nil
	}

	o = &aobject{
		Name: o.Name,
		Type: o.Type,

		Fields:  o.Fields,
		Symbols: o.Symbols,
		Items:   o.Items,
		Values:  o.Values,
		Size:    o.Size,

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
			if dot := strings.IndexByte(parentName, '.'); dot >= 0 {
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

		var names []string
		for i, of := range o.Fields {
			if !reName.MatchString(of.Name) {
				return fmt.Errorf("invalid record field name %q", of.Name)
			}
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
			dr.fields = append(dr.fields, deserRecordField{
				name:     of.Name,
				fn:       bf.deser,
				avroType: meta.avroType,
				meta:     meta,
			})
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

		for _, e := range o.Symbols {
			if !reName.MatchString(e) {
				return fmt.Errorf("invalid enum symbol %q", e)
			}
		}
		b.ser = (&serEnum{symbols: o.Symbols}).ser
		b.deser = (&deserEnum{symbols: o.Symbols}).deser
		b.meta = fieldMeta{avroType: "enum"}

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
		b.ser = (&serSize{*o.Size}).ser
		b.deser = (&deserFixed{*o.Size}).deser
		b.meta = fieldMeta{avroType: "fixed"}
	}
	return nil
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
		return fmt.Errorf("unknown logicalType %q", o.Logical)
	}

	if o.Scale != nil || o.Precision != nil {
		return fmt.Errorf("type %q logicalType %q: invalid scale or precision specified", o.Type, o.Logical)
	}

	return nil
}
