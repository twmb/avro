package avro

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"
)

func TestCanonical(t *testing.T) {
	s, err := Parse(`{"type":"record","name":"r","fields":[{"name":"a","type":"int"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	got := string(s.Canonical())
	if !strings.Contains(got, `"name":"r"`) {
		t.Errorf("canonical form missing name: %s", got)
	}
}

func TestFingerprint(t *testing.T) {
	s, err := Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	fp := s.Fingerprint(sha256.New())
	if len(fp) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(fp))
	}
	if bytes.Equal(fp, make([]byte, 32)) {
		t.Fatal("fingerprint is all zeros")
	}
}

func TestFingerprintRabin(t *testing.T) {
	tests := []struct {
		schema    string
		canonical string
		sum64     uint64
	}{
		{`"null"`, `"null"`, 7195948357588979594},
		{`{"type":"fixed","name":"foo","size":15}`, `{"name":"foo","type":"fixed","size":15}`, 1756455273707447556},
		{
			`{"type":"record","name":"foo","fields":[{"name":"f1","type":"boolean"}]}`,
			`{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"}]}`,
			7843277075252814651,
		},
	}
	for _, tt := range tests {
		s, err := Parse(tt.schema)
		if err != nil {
			t.Fatalf("schema %s: %v", tt.schema, err)
		}
		if got := string(s.Canonical()); got != tt.canonical {
			t.Errorf("canonical: got %s, want %s", got, tt.canonical)
		}
		h := NewRabin()
		fp := s.Fingerprint(h)
		if got := h.Sum64(); got != tt.sum64 {
			t.Errorf("schema %s: Sum64 = %d, want %d", tt.schema, got, tt.sum64)
		}
		if len(fp) != 8 {
			t.Fatalf("expected 8 bytes, got %d", len(fp))
		}
	}

	// Verify Sum bytes for "null".
	h := NewRabin()
	h.Write([]byte(`"null"`))
	got := h.Sum(nil)
	want := []byte{0x63, 0xdd, 0x24, 0xe7, 0xcc, 0x25, 0x8f, 0x8a}
	if !bytes.Equal(got, want) {
		t.Errorf("Sum bytes = %x, want %x", got, want)
	}
}

func TestRabinReset(t *testing.T) {
	h := NewRabin()
	h.Write([]byte("hello"))
	before := h.Sum64()
	h.Reset()
	h.Write([]byte("hello"))
	after := h.Sum64()
	if before != after {
		t.Errorf("after Reset: got %d, want %d", after, before)
	}
	if h.Size() != 8 {
		t.Errorf("Size() = %d, want 8", h.Size())
	}
	if h.BlockSize() != 1 {
		t.Errorf("BlockSize() = %d, want 1", h.BlockSize())
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

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{"invalid json", `{invalid`},
		{"nil schema", `null`},
		{"unknown primitive", `"foobar"`},
		{"unknown complex type", `{"type":"foobar"}`},
		{"record field with invalid union", `{"type":"record","name":"R","fields":[{"name":"f","type":["int","int"]}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.schema)
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
		{"decimal missing precision", aobject{Type: "bytes", Logical: "decimal"}, false},
		{"decimal wrong type", aobject{Type: "int", Logical: "decimal", Precision: &somePrec}, false},

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
		_, err := Parse(`["int","int"]`)
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
		_, err := Parse(`[
			{"type":"record","name":"a","fields":[{"name":"x","type":"int"}]},
			{"type":"record","name":"a","fields":[{"name":"y","type":"int"}]}
		]`)
		if err == nil {
			t.Fatal("expected error for duplicate named types")
		}
	})

	t.Run("two records different names", func(t *testing.T) {
		// Two records with different names in union is OK.
		_, err := Parse(`[
			{"type":"record","name":"a","fields":[{"name":"x","type":"int"}]},
			{"type":"record","name":"b","fields":[{"name":"x","type":"int"}]}
		]`)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})
}

func TestBuildComplexErrors(t *testing.T) {
	t.Run("name with namespace only", func(t *testing.T) {
		// Name like "com.example" is treated as a fullname.
		_, err := Parse(`{"type":"record","name":"com.example","fields":[{"name":"a","type":"int"}]}`)
		if err != nil {
			t.Fatalf("expected no error for dotted name, got %v", err)
		}
	})

	t.Run("name and namespace", func(t *testing.T) {
		_, err := Parse(`{"type":"record","name":"r","namespace":"com.example","fields":[{"name":"a","type":"int"}]}`)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})

	t.Run("parent namespace inheritance", func(t *testing.T) {
		// Parent has a namespace, child inherits it.
		_, err := Parse(`{
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
		_, err := Parse(`{"type":"array","name":"x","items":"int"}`)
		if err == nil {
			t.Fatal("expected error for named array")
		}
	})

	t.Run("namespace on non-record", func(t *testing.T) {
		_, err := Parse(`{"type":"array","namespace":"com","items":"int"}`)
		if err == nil {
			t.Fatal("expected error for namespaced array")
		}
	})

	t.Run("record with extra fields", func(t *testing.T) {
		_, err := Parse(`{"type":"record","name":"r","fields":[{"name":"a","type":"int"}],"symbols":["x"]}`)
		if err == nil {
			t.Fatal("expected error for record with symbols")
		}
	})

	t.Run("record with items", func(t *testing.T) {
		_, err := Parse(`{"type":"record","name":"r","fields":[{"name":"a","type":"int"}],"items":"int"}`)
		if err == nil {
			t.Fatal("expected error for record with items")
		}
	})

	t.Run("record with values", func(t *testing.T) {
		_, err := Parse(`{"type":"record","name":"r","fields":[{"name":"a","type":"int"}],"values":"int"}`)
		if err == nil {
			t.Fatal("expected error for record with values")
		}
	})

	t.Run("record with size", func(t *testing.T) {
		_, err := Parse(`{"type":"record","name":"r","fields":[{"name":"a","type":"int"}],"size":4}`)
		if err == nil {
			t.Fatal("expected error for record with size")
		}
	})

	t.Run("invalid record field type", func(t *testing.T) {
		_, err := Parse(`{"type":"record","name":"r","fields":[{"name":"a","type":"unknown"}]}`)
		if err == nil {
			t.Fatal("expected error for invalid field type")
		}
	})

	t.Run("enum with extra fields", func(t *testing.T) {
		_, err := Parse(`{"type":"enum","name":"e","symbols":["a"],"fields":[{"name":"x","type":"int"}]}`)
		if err == nil {
			t.Fatal("expected error for enum with fields")
		}
	})

	t.Run("enum with items", func(t *testing.T) {
		_, err := Parse(`{"type":"enum","name":"e","symbols":["a"],"items":"int"}`)
		if err == nil {
			t.Fatal("expected error for enum with items")
		}
	})

	t.Run("enum with values", func(t *testing.T) {
		_, err := Parse(`{"type":"enum","name":"e","symbols":["a"],"values":"int"}`)
		if err == nil {
			t.Fatal("expected error for enum with values")
		}
	})

	t.Run("enum with size", func(t *testing.T) {
		_, err := Parse(`{"type":"enum","name":"e","symbols":["a"],"size":4}`)
		if err == nil {
			t.Fatal("expected error for enum with size")
		}
	})

	t.Run("array with extra fields", func(t *testing.T) {
		_, err := Parse(`{"type":"array","items":"int","symbols":["a"]}`)
		if err == nil {
			t.Fatal("expected error for array with symbols")
		}
	})

	t.Run("array missing items", func(t *testing.T) {
		_, err := Parse(`{"type":"array"}`)
		if err == nil {
			t.Fatal("expected error for array missing items")
		}
	})

	t.Run("array invalid items", func(t *testing.T) {
		_, err := Parse(`{"type":"array","items":"unknown"}`)
		if err == nil {
			t.Fatal("expected error for array with invalid items")
		}
	})

	t.Run("map with extra fields", func(t *testing.T) {
		_, err := Parse(`{"type":"map","values":"int","symbols":["a"]}`)
		if err == nil {
			t.Fatal("expected error for map with symbols")
		}
	})

	t.Run("map missing values", func(t *testing.T) {
		_, err := Parse(`{"type":"map"}`)
		if err == nil {
			t.Fatal("expected error for map missing values")
		}
	})

	t.Run("map invalid values", func(t *testing.T) {
		_, err := Parse(`{"type":"map","values":"unknown"}`)
		if err == nil {
			t.Fatal("expected error for map with invalid values")
		}
	})

	t.Run("fixed with extra fields", func(t *testing.T) {
		_, err := Parse(`{"type":"fixed","name":"f","size":4,"symbols":["a"]}`)
		if err == nil {
			t.Fatal("expected error for fixed with symbols")
		}
	})

	t.Run("fixed missing size", func(t *testing.T) {
		_, err := Parse(`{"type":"fixed","name":"f"}`)
		if err == nil {
			t.Fatal("expected error for fixed missing size")
		}
	})

	t.Run("fixed negative size", func(t *testing.T) {
		_, err := Parse(`{"type":"fixed","name":"f","size":-1}`)
		if err == nil {
			t.Fatal("expected error for negative fixed size")
		}
	})

	t.Run("primitive as object", func(t *testing.T) {
		// A primitive type name in object form is treated as a primitive.
		s, err := Parse(`{"type":"int"}`)
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
	_, err := Parse(`{
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
	_, err := Parse(`["null","neverDefined"]`)
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
	_, err := Parse(`["null", ["int","string"]]`)
	if err == nil {
		t.Fatal("expected error for union-in-union")
	}
}

func TestBuildComplexUnknownLogicalIgnored(t *testing.T) {
	// Per Avro spec, unknown logical types are ignored and the underlying type is used.
	s, err := Parse(`{"type":"int","logicalType":"unknown_logical"}`)
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

func TestBuildComplexValidateLogicalError(t *testing.T) {
	// Known logical type on wrong underlying type should error through buildComplex.
	_, err := Parse(`{"type":"string","logicalType":"date"}`)
	if err == nil {
		t.Fatal("expected error for date on string type")
	}
}

func TestMultiDotNamespace(t *testing.T) {
	// Namespace with multiple dot-segments should be valid.
	_, err := Parse(`{"type":"record","name":"r","namespace":"com.example.foo","fields":[{"name":"a","type":"int"}]}`)
	if err != nil {
		t.Fatalf("expected no error for multi-dot namespace, got %v", err)
	}
}

func TestMultiDotFullname(t *testing.T) {
	// Fullname with multiple dot-segments should be valid.
	_, err := Parse(`{"type":"record","name":"com.example.foo.Bar","fields":[{"name":"a","type":"int"}]}`)
	if err != nil {
		t.Fatalf("expected no error for multi-dot fullname, got %v", err)
	}
}

func TestDeepNamespaceInheritance(t *testing.T) {
	// Parent is "com.example.Parent", child should inherit "com.example" namespace.
	_, err := Parse(`{
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
	_, err := Parse(`{"type":"record","name":"r","fields":[
		{"name":"a","type":"int"},
		{"name":"a","type":"string"}
	]}`)
	if err == nil {
		t.Fatal("expected error for duplicate field name")
	}
}

func TestDuplicateEnumSymbol(t *testing.T) {
	_, err := Parse(`{"type":"enum","name":"e","symbols":["a","b","a"]}`)
	if err == nil {
		t.Fatal("expected error for duplicate enum symbol")
	}
}

func TestBuildNilSchema(t *testing.T) {
	b := &builder{
		named: make(map[string]*namedType),
	}
	err := b.build("", nil)
	if err == nil {
		t.Fatal("expected error for nil schema")
	}
}

func TestBuildEmptySchema(t *testing.T) {
	b := &builder{
		named: make(map[string]*namedType),
	}
	err := b.build("", &aschema{})
	if err == nil {
		t.Fatal("expected error for empty schema")
	}
}

func TestNameValidation(t *testing.T) {
	t.Run("dashes rejected", func(t *testing.T) {
		// Per spec: names must match [A-Za-z_][A-Za-z0-9_]*
		_, err := Parse(`{"type":"record","name":"my-record","fields":[{"name":"my-field","type":"int"}]}`)
		if err == nil {
			t.Fatal("expected error for dashed record name")
		}
	})

	t.Run("fullname detection", func(t *testing.T) {
		// Fullnames (dot-separated) must be detected so namespace
		// handling works correctly.
		s, err := Parse(`{"type":"record","name":"com.example.MyRecord","fields":[{"name":"x","type":"int"}]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		canon := string(s.Canonical())
		if canon == "" {
			t.Fatal("expected non-empty canonical form")
		}
	})
}

func TestNamespaceFallback(t *testing.T) {
	// A record in a namespace can reference another type by unqualified name.
	schema := `{
		"type":"record","name":"Parent","namespace":"com.example","fields":[
			{"name":"child","type":{"type":"record","name":"Child","fields":[
				{"name":"x","type":"int"}
			]}},
			{"name":"ref","type":"Child"}
		]
	}`
	_, err := Parse(schema)
	if err != nil {
		t.Fatalf("expected namespace fallback to resolve unqualified ref, got %v", err)
	}
}

func TestForwardReferenceInRecord(t *testing.T) {
	// A record field references a type defined later in the same record.
	schema := `{
		"type":"record","name":"outer","fields":[
			{"name":"ref","type":"inner"},
			{"name":"inner_def","type":{"type":"record","name":"inner","fields":[
				{"name":"x","type":"int"}
			]}}
		]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatalf("expected forward reference to work, got %v", err)
	}
	// Verify round-trip works.
	type Inner struct {
		X int32 `avro:"x"`
	}
	type Outer struct {
		Ref      Inner `avro:"ref"`
		InnerDef Inner `avro:"inner_def"`
	}
	input := Outer{Ref: Inner{X: 42}, InnerDef: Inner{X: 99}}
	dst, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var output Outer
	rem, err := s.Decode(dst, &output)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("leftover bytes: %d", len(rem))
	}
	if output.Ref.X != 42 || output.InnerDef.X != 99 {
		t.Fatalf("unexpected output: %+v", output)
	}
}

func TestEmptyNamespace(t *testing.T) {
	// Explicit empty namespace clears inherited namespace.
	schema := `{
		"type":"record","name":"parent","namespace":"com.example","fields":[
			{"name":"child","type":{"type":"record","name":"child","namespace":"","fields":[
				{"name":"x","type":"int"}
			]}}
		]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	canon := string(s.Canonical())
	// The child should not have com.example prefix because namespace was
	// explicitly cleared.
	if strings.Contains(canon, "com.example.child") {
		t.Fatalf("expected empty namespace to clear parent, got %s", canon)
	}
}
