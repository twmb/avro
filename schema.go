package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
)

type Schema struct {
	s aschema
}

func NewSchema(schema string) (*Schema, error) {
	var s aschema
	if err := json.Unmarshal([]byte(schema), &s); err != nil {
		return nil, fmt.Errorf("invalid schema: %v", err)
	}
	_, err := s.canonicalize("")
	if err != nil {
		return nil, err
	}
	return &Schema{
		s: s,
	}, nil
}

func (s *Schema) ParsingCanonicalForm() string {
	c, _ := s.s.canonicalize("")
	b, _ := json.Marshal(c)
	return string(b)
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

	Order   string   `json:"order,omitempty"`
	Doc     string   `json:"doc,omitempty"`
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

	// In canonical form, the following four fields are stripped.

	Namespace string          `json:"namespace,omitempty"`
	Doc       string          `json:"doc,omitempty"`
	Aliases   []string        `json:"aliases,omitempty"`
	Default   json.RawMessage `json:"default,omitempty"`

	Logical   string `json:"logicalType,omitempty"`
	Scale     *int   `json:"scale,omitempty"`     // decimal logical type
	Precision *int   `json:"precision,omitempty"` // decmial logical type

	typeFields sync.Map // map[reflect.Type][]int
}

var (
	aprimitive = map[string]struct{}{
		"null":    {},
		"boolean": {},
		"int":     {},
		"long":    {},
		"float":   {},
		"double":  {},
		"bytes":   {},
		"string":  {},
	}

	acomplex = map[string]struct{}{
		"record": {},
		"enum":   {},
		"array":  {},
		"map":    {},
		"fixed":  {},
	}
)

func (s *aschema) unionTypeName() (string, string) {
	if s.primitive != "" {
		return s.primitive, ""
	}
	if len(s.union) > 0 {
		return "union", ""
	}
	switch s.object.Type {
	case "record", "fixed", "enum":
		return s.object.Type, s.object.Name
	default:
		return s.object.Type, ""
	}
}

var (
	reName      = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	reNamespace = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)$`)
)

// Canonicalize validates (almost entirely -- some parts missing, such as
// defaults) and canonicalizes a schema.
func (s *aschema) canonicalize(parentName string) (aschema, error) {
	if s == nil || s.primitive == "" && s.object == nil && len(s.union) == 0 {
		return aschema{}, errors.New("schema is not a primitive, complex, nor union")
	}

	// If this is a primitive, we ensure the primitive is known.
	if s.primitive != "" {
		if _, exists := aprimitive[s.primitive]; !exists {
			return aschema{}, fmt.Errorf("unknown primitive %q", s.primitive)
		}
		return aschema{primitive: s.primitive}, nil
	}

	// If this is a union, then: unions may not contain multiple schemas
	// with the same type, except for record, fixed, and enum. Unions also
	// cannot contain other immediate unions. For record, fixed, and enum,
	// we must not have duplicate names.
	if len(s.union) != 0 {
		sawTypes := make(map[string]bool)
		sawNames := make(map[string]bool)

		c := aschema{union: make([]aschema, 0, len(s.union))}
		for _, us := range s.union {
			uc, err := us.canonicalize(parentName)
			if err != nil {
				return aschema{}, fmt.Errorf("invalid union: %v", err)
			}
			typ, name := uc.unionTypeName()
			if typ == "union" {
				return aschema{}, errors.New("unions cannot contain other immediate unions")
			}
			if sawTypes[typ] {
				if name == "" {
					return aschema{}, fmt.Errorf("duplicate union type %q", typ)
				} else {
					if sawNames[name] {
						return aschema{}, fmt.Errorf("duplicate union name %q", name)
					}
					sawNames[name] = true
				}
			}
			sawTypes[typ] = true
			c.union = append(c.union, uc)
		}
		return c, nil
	}

	// If this object is a primitive in the shape of a complex,
	// we convert this to a primitive.
	o := s.object
	_, isPrimitive := aprimitive[o.Type]
	if isPrimitive {
		if o.Logical == "" {
			return aschema{primitive: o.Type}, nil
		}
	} else {
		if _, exists := acomplex[o.Type]; !exists {
			return aschema{}, fmt.Errorf("unknown complex type %q", o.Type)
		}
	}

	// We copy our object so we can make modifications to the copy. For any
	// fields / nested schemas (items, values), we will copy as we process.
	o = &aobject{
		Type:      o.Type,
		Name:      o.Name,
		Namespace: o.Namespace,

		Fields:  o.Fields,
		Symbols: o.Symbols,
		Items:   o.Items,
		Values:  o.Values,
		Size:    o.Size,

		Logical:   o.Logical,
		Scale:     o.Scale,
		Precision: o.Precision,
	}

	switch o.Type {
	case "record", "enum", "fixed":
		if !reName.MatchString(o.Name) {
			if reNamespace.MatchString(o.Name) {
				parentName, o.Namespace = "", "" // fullname: ignore parent & our own namespace
			} else {
				return aschema{}, fmt.Errorf("invalid name %q", o.Name)
			}
		}
		if o.Namespace != "" {
			if !reNamespace.MatchString(o.Namespace) {
				return aschema{}, fmt.Errorf("invalid namespace %q", o.Namespace)
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
			return aschema{}, errors.New("only record, enum, and fixed can have a name")
		}
	}

	switch o.Type {
	case "record":
		if len(o.Symbols) > 0 ||
			o.Items != nil ||
			o.Values != nil ||
			o.Size != nil {
			return aschema{}, errors.New("invalid record has schema for other types")
		}

		for i, f := range o.Fields {
			if !reName.MatchString(f.Name) {
				return aschema{}, fmt.Errorf("invalid record field name %q", f.Name)
			}
			fc, err := f.Type.canonicalize(o.Name)
			if err != nil {
				return aschema{}, fmt.Errorf("invalid record field: %v", err)
			}
			o.Fields[i] = afield{
				Name: f.Name,
				Type: &fc,
			}
		}

	case "enum":
		if len(o.Fields) > 0 ||
			o.Items != nil ||
			o.Values != nil ||
			o.Size != nil {
			return aschema{}, errors.New("invalid enum has schema for other types")
		}

		for _, e := range o.Symbols {
			if !reName.MatchString(e) {
				return aschema{}, fmt.Errorf("invalid enum symbol %q", e)
			}
		}

	case "array":
		if len(o.Fields) > 0 ||
			len(o.Symbols) > 0 ||
			o.Values != nil ||
			o.Size != nil {
			return aschema{}, errors.New("invalid array has schema for other types")
		}
		if o.Items == nil {
			return aschema{}, errors.New("array is missing items schema")
		}
		ic, err := o.Items.canonicalize(parentName)
		if err != nil {
			return aschema{}, fmt.Errorf("invalid array: %v", err)
		}
		o.Items = &ic

	case "map":
		if len(o.Fields) > 0 ||
			len(o.Symbols) > 0 ||
			o.Items != nil ||
			o.Size != nil {
			return aschema{}, errors.New("invalid map has schema for other types")
		}
		if o.Values == nil {
			return aschema{}, errors.New("map is missing values schema")
		}
		vc, err := o.Values.canonicalize(parentName)
		if err != nil {
			return aschema{}, fmt.Errorf("invalid map: %v", err)
		}
		o.Values = &vc

	case "fixed":
		if len(o.Fields) > 0 ||
			len(o.Symbols) > 0 ||
			o.Items != nil ||
			o.Values != nil {
			return aschema{}, errors.New("invalid fixed has schema for other types")
		}
		if o.Size == nil {
			return aschema{}, errors.New("fixed is missing size")
		}
		if *o.Size < 0 {
			return aschema{}, fmt.Errorf("invalid fixed size %v", *o.Size)
		}

	default: // primitive logical type
		if len(o.Fields) > 0 ||
			len(o.Symbols) > 0 ||
			o.Items != nil ||
			o.Values != nil ||
			o.Size != nil {
			return aschema{}, errors.New("invalid primitive has schema for other types")
		}
	}

	o.Doc, o.Aliases, o.Default = "", nil, nil

	switch o.Logical {
	case "":
		// No logical type: validate no scale / precision below.

	case "decimal":
		if o.Precision == nil {
			return aschema{}, errors.New("logicalType decimal missing precision")
		}
		if o.Type != "bytes" && o.Type != "fixed" {
			return aschema{}, fmt.Errorf("invalid logicalType decimal type %q, can only be bytes or fixed", o.Type)
		}

		// With decimal, we have finally validated everything that
		// should or should not exist. For all other logical types, we
		// require no scale / precision, which is validated below the
		// switch.
		return aschema{object: o}, nil

	case "uuid":
		if o.Type != "string" {
			return aschema{}, fmt.Errorf("invalid logicalType uuid type %q, can only be string", o.Type)
		}

	case "date":
		if o.Type != "int" {
			return aschema{}, fmt.Errorf("invalid logicalType date type %q, can only be int", o.Type)
		}

	case "time-millis":
		if o.Type != "int" {
			return aschema{}, fmt.Errorf("invalid logicalType time-millis type %q, can only be int", o.Type)
		}

	case "time-micros":
		if o.Type != "long" {
			return aschema{}, fmt.Errorf("invalid logicalType time-micros type %q, can only be long", o.Type)
		}

	case "timestamp-millis":
		if o.Type != "long" {
			return aschema{}, fmt.Errorf("invalid logicalType timestamp-millis type %q, can only be long", o.Type)
		}

	case "timestamp-micros":
		if o.Type != "long" {
			return aschema{}, fmt.Errorf("invalid logicalType timestamp-micros type %q, can only be long", o.Type)
		}

	case "local-timestamp-millis":
		if o.Type != "long" {
			return aschema{}, fmt.Errorf("invalid logicalType local-timestamp-millis type %q, can only be long", o.Type)
		}

	case "local-timestamp-micros":
		if o.Type != "long" {
			return aschema{}, fmt.Errorf("invalid logicalType local-timestamp-micros type %q, can only be long", o.Type)
		}

	case "duration":
		if o.Type != "fixed" {
			return aschema{}, fmt.Errorf("invalid logicalType duration type %q, can only be fixed", o.Type)
		}
		if o.Size == nil {
			return aschema{}, errors.New("invalid logicalType duration has no size")
		}
		if *o.Size != 12 {
			return aschema{}, fmt.Errorf("invalid logicalType duration size %v is not the expected 12", *o.Size)
		}

	default:
		return aschema{}, fmt.Errorf("unknown logicalType %q", o.Logical)
	}

	if o.Scale != nil || o.Precision != nil {
		return aschema{}, fmt.Errorf("type %q logicalType %q: invalid scale or precision specified", o.Type, o.Logical)
	}

	if o.Logical != "" {
		// Now that we have validated the logical type, for canonical
		// form we return the primitive.
		return aschema{primitive: o.Type}, nil
	}

	return aschema{object: o}, nil
}
