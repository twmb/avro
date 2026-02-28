package avro

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParsingCanonicalForm(t *testing.T) {
	s, err := NewSchema(`{"type":"record","name":"r","fields":[{"name":"a","type":"int"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	got := string(s.ParsingCanonicalForm())
	if !strings.Contains(got, `"name":"r"`) {
		t.Errorf("canonical form missing name: %s", got)
	}
}

func TestMarshalJSON(t *testing.T) {
	t.Run("primitive", func(t *testing.T) {
		s := aschema{primitive: "int"}
		b, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != `"int"` {
			t.Errorf("got %s, want \"int\"", b)
		}
	})

	t.Run("object", func(t *testing.T) {
		s := aschema{object: &aobject{Name: "r", Type: "record"}}
		b, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(b), `"name":"r"`) {
			t.Errorf("got %s, want object with name r", b)
		}
	})

	t.Run("union", func(t *testing.T) {
		s := aschema{union: []aschema{{primitive: "null"}, {primitive: "int"}}}
		b, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != `["null","int"]` {
			t.Errorf("got %s", b)
		}
	})

	t.Run("empty", func(t *testing.T) {
		s := aschema{}
		_, err := json.Marshal(s)
		if err == nil {
			t.Fatal("expected error for empty schema")
		}
	})
}

func TestUnmarshalJSONInvalid(t *testing.T) {
	var s aschema
	// Invalid first byte (number).
	err := s.UnmarshalJSON([]byte(`123`))
	if err == nil {
		t.Fatal("expected error")
	}

	// Empty data.
	err = s.UnmarshalJSON([]byte(``))
	if err == nil {
		t.Fatal("expected error for empty data")
	}
}

func TestNewSchemaErrors(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{"invalid json", `{invalid`},
		{"nil schema", `null`},
		{"unknown primitive", `"foobar"`},
		{"unknown complex type", `{"type":"foobar"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewSchema(tt.schema)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestUnknownPrimitiveErrorString(t *testing.T) {
	e := &unknownPrimitiveError{"foobar"}
	s := e.Error()
	if !strings.Contains(s, "foobar") {
		t.Errorf("error string missing primitive name: %s", s)
	}
}

func TestValidateLogical(t *testing.T) {
	intSize := 12
	zeroPrec := 0
	somePrec := 10

	tests := []struct {
		name    string
		obj     aobject
		wantErr bool
	}{
		{"no logical", aobject{Type: "int"}, false},

		// decimal
		{"decimal ok bytes", aobject{Type: "bytes", Logical: "decimal", Precision: &somePrec}, false},
		{"decimal ok fixed", aobject{Type: "fixed", Logical: "decimal", Precision: &somePrec, Size: &intSize}, false},
		{"decimal missing precision", aobject{Type: "bytes", Logical: "decimal"}, true},
		{"decimal wrong type", aobject{Type: "int", Logical: "decimal", Precision: &somePrec}, true},

		// uuid
		{"uuid ok", aobject{Type: "string", Logical: "uuid"}, false},
		{"uuid wrong type", aobject{Type: "int", Logical: "uuid"}, true},
		{"uuid with scale", aobject{Type: "string", Logical: "uuid", Scale: &zeroPrec}, true},

		// date
		{"date ok", aobject{Type: "int", Logical: "date"}, false},
		{"date wrong type", aobject{Type: "long", Logical: "date"}, true},

		// time-millis
		{"time-millis ok", aobject{Type: "int", Logical: "time-millis"}, false},
		{"time-millis wrong type", aobject{Type: "long", Logical: "time-millis"}, true},

		// time-micros
		{"time-micros ok", aobject{Type: "long", Logical: "time-micros"}, false},
		{"time-micros wrong type", aobject{Type: "int", Logical: "time-micros"}, true},

		// timestamp-millis
		{"timestamp-millis ok", aobject{Type: "long", Logical: "timestamp-millis"}, false},
		{"timestamp-millis wrong type", aobject{Type: "int", Logical: "timestamp-millis"}, true},

		// timestamp-micros
		{"timestamp-micros ok", aobject{Type: "long", Logical: "timestamp-micros"}, false},
		{"timestamp-micros wrong type", aobject{Type: "int", Logical: "timestamp-micros"}, true},

		// local-timestamp-millis
		{"local-timestamp-millis ok", aobject{Type: "long", Logical: "local-timestamp-millis"}, false},

		// local-timestamp-micros
		{"local-timestamp-micros ok", aobject{Type: "long", Logical: "local-timestamp-micros"}, false},

		// duration
		{"duration ok", aobject{Type: "fixed", Logical: "duration", Size: &intSize}, false},
		{"duration wrong type", aobject{Type: "int", Logical: "duration"}, true},
		{"duration no size", aobject{Type: "fixed", Logical: "duration"}, true},
		{"duration wrong size", aobject{Type: "fixed", Logical: "duration", Size: &somePrec}, true},

		// unknown logical types are ignored per spec
		{"unknown logical", aobject{Type: "int", Logical: "foobar"}, false},

		// scale/precision on non-decimal
		{"date with precision", aobject{Type: "int", Logical: "date", Precision: &somePrec}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.obj.validateLogical()
			if (err != nil) != tt.wantErr {
				t.Errorf("validateLogical() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBuildUnionErrors(t *testing.T) {
	t.Run("duplicate type", func(t *testing.T) {
		_, err := NewSchema(`["int","int"]`)
		if err == nil {
			t.Fatal("expected error for duplicate union type")
		}
	})

	t.Run("union in union", func(t *testing.T) {
		// Can't test this through JSON directly since `[["null","int"],"string"]`
		// won't parse the inner union as a separate union element in the same way.
		// But we can test via unionTypeName.
		s := &aschema{union: []aschema{{primitive: "null"}}}
		_, _, err := s.unionTypeName()
		if err == nil {
			t.Fatal("expected error for union containing union")
		}
	})

	t.Run("duplicate named type", func(t *testing.T) {
		// Two records with the same name in a union is a duplicate.
		_, err := NewSchema(`[
			{"type":"record","name":"a","fields":[{"name":"x","type":"int"}]},
			{"type":"record","name":"a","fields":[{"name":"y","type":"int"}]}
		]`)
		if err == nil {
			t.Fatal("expected error for duplicate named types")
		}
	})

	t.Run("two records different names", func(t *testing.T) {
		// Two records with different names in union is OK.
		_, err := NewSchema(`[
			{"type":"record","name":"a","fields":[{"name":"x","type":"int"}]},
			{"type":"record","name":"b","fields":[{"name":"x","type":"int"}]}
		]`)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})
}

func TestBuildComplexErrors(t *testing.T) {
	t.Run("invalid name", func(t *testing.T) {
		_, err := NewSchema(`{"type":"record","name":"123bad","fields":[]}`)
		if err == nil {
			t.Fatal("expected error for invalid name")
		}
	})

	t.Run("invalid namespace", func(t *testing.T) {
		_, err := NewSchema(`{"type":"record","name":"r","namespace":"123bad","fields":[]}`)
		if err == nil {
			t.Fatal("expected error for invalid namespace")
		}
	})

	t.Run("name with namespace only", func(t *testing.T) {
		// Name like "com.example" is treated as a fullname.
		_, err := NewSchema(`{"type":"record","name":"com.example","fields":[{"name":"a","type":"int"}]}`)
		if err != nil {
			t.Fatalf("expected no error for dotted name, got %v", err)
		}
	})

	t.Run("name and namespace", func(t *testing.T) {
		_, err := NewSchema(`{"type":"record","name":"r","namespace":"com.example","fields":[{"name":"a","type":"int"}]}`)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})

	t.Run("parent namespace inheritance", func(t *testing.T) {
		// Parent has a namespace, child inherits it.
		_, err := NewSchema(`{
			"type":"record","name":"parent","namespace":"com.example","fields":[
				{"name":"child","type":{"type":"record","name":"child","fields":[
					{"name":"x","type":"int"}
				]}}
			]
		}`)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})

	t.Run("named non-record", func(t *testing.T) {
		_, err := NewSchema(`{"type":"array","name":"x","items":"int"}`)
		if err == nil {
			t.Fatal("expected error for named array")
		}
	})

	t.Run("namespace on non-record", func(t *testing.T) {
		_, err := NewSchema(`{"type":"array","namespace":"com","items":"int"}`)
		if err == nil {
			t.Fatal("expected error for namespaced array")
		}
	})

	t.Run("record with extra fields", func(t *testing.T) {
		_, err := NewSchema(`{"type":"record","name":"r","fields":[{"name":"a","type":"int"}],"symbols":["x"]}`)
		if err == nil {
			t.Fatal("expected error for record with symbols")
		}
	})

	t.Run("record with items", func(t *testing.T) {
		_, err := NewSchema(`{"type":"record","name":"r","fields":[{"name":"a","type":"int"}],"items":"int"}`)
		if err == nil {
			t.Fatal("expected error for record with items")
		}
	})

	t.Run("record with values", func(t *testing.T) {
		_, err := NewSchema(`{"type":"record","name":"r","fields":[{"name":"a","type":"int"}],"values":"int"}`)
		if err == nil {
			t.Fatal("expected error for record with values")
		}
	})

	t.Run("record with size", func(t *testing.T) {
		_, err := NewSchema(`{"type":"record","name":"r","fields":[{"name":"a","type":"int"}],"size":4}`)
		if err == nil {
			t.Fatal("expected error for record with size")
		}
	})

	t.Run("invalid record field name", func(t *testing.T) {
		_, err := NewSchema(`{"type":"record","name":"r","fields":[{"name":"123bad","type":"int"}]}`)
		if err == nil {
			t.Fatal("expected error for invalid field name")
		}
	})

	t.Run("invalid record field type", func(t *testing.T) {
		_, err := NewSchema(`{"type":"record","name":"r","fields":[{"name":"a","type":"unknown"}]}`)
		if err == nil {
			t.Fatal("expected error for invalid field type")
		}
	})

	t.Run("enum with extra fields", func(t *testing.T) {
		_, err := NewSchema(`{"type":"enum","name":"e","symbols":["a"],"fields":[{"name":"x","type":"int"}]}`)
		if err == nil {
			t.Fatal("expected error for enum with fields")
		}
	})

	t.Run("enum with items", func(t *testing.T) {
		_, err := NewSchema(`{"type":"enum","name":"e","symbols":["a"],"items":"int"}`)
		if err == nil {
			t.Fatal("expected error for enum with items")
		}
	})

	t.Run("enum with values", func(t *testing.T) {
		_, err := NewSchema(`{"type":"enum","name":"e","symbols":["a"],"values":"int"}`)
		if err == nil {
			t.Fatal("expected error for enum with values")
		}
	})

	t.Run("enum with size", func(t *testing.T) {
		_, err := NewSchema(`{"type":"enum","name":"e","symbols":["a"],"size":4}`)
		if err == nil {
			t.Fatal("expected error for enum with size")
		}
	})

	t.Run("invalid enum symbol", func(t *testing.T) {
		_, err := NewSchema(`{"type":"enum","name":"e","symbols":["123bad"]}`)
		if err == nil {
			t.Fatal("expected error for invalid symbol")
		}
	})

	t.Run("array with extra fields", func(t *testing.T) {
		_, err := NewSchema(`{"type":"array","items":"int","symbols":["a"]}`)
		if err == nil {
			t.Fatal("expected error for array with symbols")
		}
	})

	t.Run("array missing items", func(t *testing.T) {
		_, err := NewSchema(`{"type":"array"}`)
		if err == nil {
			t.Fatal("expected error for array missing items")
		}
	})

	t.Run("array invalid items", func(t *testing.T) {
		_, err := NewSchema(`{"type":"array","items":"unknown"}`)
		if err == nil {
			t.Fatal("expected error for array with invalid items")
		}
	})

	t.Run("map with extra fields", func(t *testing.T) {
		_, err := NewSchema(`{"type":"map","values":"int","symbols":["a"]}`)
		if err == nil {
			t.Fatal("expected error for map with symbols")
		}
	})

	t.Run("map missing values", func(t *testing.T) {
		_, err := NewSchema(`{"type":"map"}`)
		if err == nil {
			t.Fatal("expected error for map missing values")
		}
	})

	t.Run("map invalid values", func(t *testing.T) {
		_, err := NewSchema(`{"type":"map","values":"unknown"}`)
		if err == nil {
			t.Fatal("expected error for map with invalid values")
		}
	})

	t.Run("fixed with extra fields", func(t *testing.T) {
		_, err := NewSchema(`{"type":"fixed","name":"f","size":4,"symbols":["a"]}`)
		if err == nil {
			t.Fatal("expected error for fixed with symbols")
		}
	})

	t.Run("fixed missing size", func(t *testing.T) {
		_, err := NewSchema(`{"type":"fixed","name":"f"}`)
		if err == nil {
			t.Fatal("expected error for fixed missing size")
		}
	})

	t.Run("fixed negative size", func(t *testing.T) {
		_, err := NewSchema(`{"type":"fixed","name":"f","size":-1}`)
		if err == nil {
			t.Fatal("expected error for negative fixed size")
		}
	})

	t.Run("primitive as object", func(t *testing.T) {
		// A primitive type name in object form is treated as a primitive.
		s, err := NewSchema(`{"type":"int"}`)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		dst, err := s.AppendEncode(nil, new(int32(42)))
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got int32
		_, err = s.Decode(dst, &got)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got != 42 {
			t.Errorf("got %d, want 42", got)
		}
	})
}

func TestFinalizeForwardRef(t *testing.T) {
	// Union references a type defined later in the schema.
	// This exercises finalize() resolving forward references.
	_, err := NewSchema(`{
		"type":"record","name":"outer","fields":[
			{"name":"u","type":["null","inner"]},
			{"name":"inner","type":{"type":"record","name":"inner","fields":[
				{"name":"x","type":"int"}
			]}}
		]
	}`)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestFinalizeUnknownRef(t *testing.T) {
	// Union references a type that is never defined → finalize error.
	_, err := NewSchema(`["null","neverDefined"]`)
	if err == nil {
		t.Fatal("expected error for unknown type reference in union")
	}
}

func TestUnionTypeName(t *testing.T) {
	// Array type in union returns ("array", "", nil).
	s := &aschema{object: &aobject{Type: "array"}}
	typ, name, err := s.unionTypeName()
	if err != nil {
		t.Fatal(err)
	}
	if typ != "array" || name != "" {
		t.Errorf("got (%s, %s)", typ, name)
	}

	// Map type in union returns ("map", "", nil).
	s = &aschema{object: &aobject{Type: "map"}}
	typ, name, err = s.unionTypeName()
	if err != nil {
		t.Fatal(err)
	}
	if typ != "map" || name != "" {
		t.Errorf("got (%s, %s)", typ, name)
	}
}

func TestBuildUnionInUnion(t *testing.T) {
	// A union directly containing another union is invalid.
	_, err := NewSchema(`["null", ["int","string"]]`)
	if err == nil {
		t.Fatal("expected error for union-in-union")
	}
}

func TestBuildUnionInvalidInnerError(t *testing.T) {
	// Union element that produces a non-unknownPrimitive error.
	_, err := NewSchema(`["null",{"type":"record","name":"r","fields":[{"name":"123bad","type":"int"}]}]`)
	if err == nil {
		t.Fatal("expected error for invalid union element")
	}
}

func TestBuildComplexUnknownLogicalIgnored(t *testing.T) {
	// Per Avro spec, unknown logical types are ignored and the underlying type is used.
	s, err := NewSchema(`{"type":"int","logicalType":"unknown_logical"}`)
	if err != nil {
		t.Fatalf("expected unknown logical type to be ignored, got error: %v", err)
	}
	dst, err := s.AppendEncode(nil, new(int32(42)))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got int32
	if _, err := s.Decode(dst, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != 42 {
		t.Errorf("got %d, want 42", got)
	}
}

func TestMultiDotNamespace(t *testing.T) {
	// Namespace with multiple dot-segments should be valid.
	_, err := NewSchema(`{"type":"record","name":"r","namespace":"com.example.foo","fields":[{"name":"a","type":"int"}]}`)
	if err != nil {
		t.Fatalf("expected no error for multi-dot namespace, got %v", err)
	}
}

func TestMultiDotFullname(t *testing.T) {
	// Fullname with multiple dot-segments should be valid.
	_, err := NewSchema(`{"type":"record","name":"com.example.foo.Bar","fields":[{"name":"a","type":"int"}]}`)
	if err != nil {
		t.Fatalf("expected no error for multi-dot fullname, got %v", err)
	}
}

func TestDeepNamespaceInheritance(t *testing.T) {
	// Parent is "com.example.Parent", child should inherit "com.example" namespace.
	_, err := NewSchema(`{
		"type":"record","name":"Parent","namespace":"com.example","fields":[
			{"name":"child","type":{"type":"record","name":"Child","fields":[
				{"name":"x","type":"int"}
			]}}
		]
	}`)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestDuplicateRecordFieldName(t *testing.T) {
	_, err := NewSchema(`{"type":"record","name":"r","fields":[
		{"name":"a","type":"int"},
		{"name":"a","type":"string"}
	]}`)
	if err == nil {
		t.Fatal("expected error for duplicate field name")
	}
}

func TestDuplicateEnumSymbol(t *testing.T) {
	_, err := NewSchema(`{"type":"enum","name":"e","symbols":["a","b","a"]}`)
	if err == nil {
		t.Fatal("expected error for duplicate enum symbol")
	}
}

func TestBuildNilSchema(t *testing.T) {
	b := &builder{
		types:  make(map[string]serfn),
		dtypes: make(map[string]deserfn),
	}
	err := b.build("", nil)
	if err == nil {
		t.Fatal("expected error for nil schema")
	}
}

func TestBuildEmptySchema(t *testing.T) {
	b := &builder{
		types:  make(map[string]serfn),
		dtypes: make(map[string]deserfn),
	}
	err := b.build("", &aschema{})
	if err == nil {
		t.Fatal("expected error for empty schema")
	}
}
