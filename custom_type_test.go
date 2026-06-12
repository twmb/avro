package avro

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

type testMoney struct {
	Cents    int64
	Currency string
}

type testGeoPoint struct {
	Lat, Lng float64
}

type testStatus int32

var moneyCT = NewCustomType[testMoney, int64]("money",
	func(m testMoney, _ *SchemaNode) (int64, error) { return m.Cents, nil },
	func(c int64, _ *SchemaNode) (testMoney, error) { return testMoney{Cents: c, Currency: "USD"}, nil },
)

func parseMoney(t *testing.T, schema string) *Schema {
	t.Helper()
	s, err := Parse(schema, moneyCT)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestCustomTypeRoundTrip(t *testing.T) {
	t.Run("struct", func(t *testing.T) {
		type Order struct {
			ID    int64     `avro:"id"`
			Price testMoney `avro:"price"`
		}
		s := parseMoney(t, `{"type":"record","name":"Order","fields":[
			{"name":"id","type":"long"},
			{"name":"price","type":{"type":"long","logicalType":"money"}}
		]}`)
		input := Order{ID: 1, Price: testMoney{Cents: 500, Currency: "USD"}}
		data, err := s.Encode(&input)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		var out Order
		if _, err := s.Decode(data, &out); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if out.ID != 1 || out.Price.Cents != 500 || out.Price.Currency != "USD" {
			t.Errorf("got %+v", out)
		}
	})

	t.Run("any", func(t *testing.T) {
		s := parseMoney(t, `{"type":"long","logicalType":"money"}`)
		data, err := s.Encode(testMoney{Cents: 999})
		if err != nil {
			t.Fatal(err)
		}
		var v any
		if _, err := s.Decode(data, &v); err != nil {
			t.Fatal(err)
		}
		if m := v.(testMoney); m.Cents != 999 {
			t.Errorf("got %d", m.Cents)
		}
	})

	t.Run("double_pointer", func(t *testing.T) {
		s := parseMoney(t, `{"type":"long","logicalType":"money"}`)
		data, _ := s.Encode(int64(55))
		var out *testMoney
		if _, err := s.Decode(data, &out); err != nil {
			t.Fatal(err)
		}
		if out == nil || out.Cents != 55 {
			t.Fatalf("got %+v", out)
		}
	})
}

func TestCustomTypeJSON(t *testing.T) {
	s := parseMoney(t, `{"type":"long","logicalType":"money"}`)

	t.Run("encode", func(t *testing.T) {
		j, err := s.EncodeJSON(testMoney{Cents: 1234})
		if err != nil {
			t.Fatal(err)
		}
		if string(j) != "1234" {
			t.Errorf("got %s", j)
		}
	})

	t.Run("decode", func(t *testing.T) {
		var v any
		if err := s.DecodeJSON([]byte("5678"), &v); err != nil {
			t.Fatal(err)
		}
		if m := v.(testMoney); m.Cents != 5678 {
			t.Errorf("got %d", m.Cents)
		}
	})
}

func TestCustomTypeSchemaFor(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		type Order struct {
			Price testMoney `avro:"price"`
		}
		s, err := SchemaFor[Order](moneyCT)
		if err != nil {
			t.Fatal(err)
		}
		f := s.Root().Fields[0]
		if f.Type.Type != "long" || f.Type.LogicalType != "money" {
			t.Errorf("got type=%q logical=%q", f.Type.Type, f.Type.LogicalType)
		}
	})

	t.Run("pointer", func(t *testing.T) {
		type R struct {
			Price *testMoney `avro:"price"`
		}
		s, err := SchemaFor[R](moneyCT)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		json.Unmarshal([]byte(s.String()), &m)
		typ := m["fields"].([]any)[0].(map[string]any)["type"].([]any)
		if len(typ) != 2 || typ[0] != "null" {
			t.Fatalf("expected nullable union, got %v", typ)
		}
		inner := typ[1].(map[string]any)
		if inner["type"] != "long" || inner["logicalType"] != "money" {
			t.Errorf("got %v", inner)
		}
	})

	t.Run("with_schema", func(t *testing.T) {
		type Data struct {
			Addr testGeoPoint `avro:"addr"`
		}
		s, err := SchemaFor[Data](CustomType{
			GoType: reflect.TypeFor[testGeoPoint](),
			Schema: &SchemaNode{Type: "fixed", Name: "geo", Size: 16},
		})
		if err != nil {
			t.Fatal(err)
		}
		f := s.Root().Fields[0]
		if f.Type.Type != "fixed" || f.Type.Size != 16 {
			t.Errorf("got type=%q size=%d", f.Type.Type, f.Type.Size)
		}
	})
}

func TestCustomTypeOverrideBuiltIn(t *testing.T) {
	s, err := Parse(`{"type":"long","logicalType":"timestamp-millis"}`, CustomType{
		LogicalType: "timestamp-millis",
		GoType:      reflect.TypeFor[time.Time](),
		Encode: func(v any, _ *SchemaNode) (any, error) {
			return v.(time.Time).UnixMilli(), nil
		},
		Decode: func(v any, _ *SchemaNode) (any, error) {
			return v, nil // pass through raw int64
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Millisecond).UTC()
	data, _ := s.Encode(now)
	var v any
	s.Decode(data, &v)
	if v.(int64) != now.UnixMilli() {
		t.Errorf("got %d, want %d", v, now.UnixMilli())
	}
}

func TestCustomTypeNullableUnion(t *testing.T) {
	type R struct {
		Price *testMoney `avro:"price"`
		Name  string     `avro:"name"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"price","type":["null",{"type":"long","logicalType":"money"}]},
		{"name":"name","type":"string"}
	]}`
	s := parseMoney(t, schema)

	t.Run("non_null", func(t *testing.T) {
		m := testMoney{Cents: 999}
		data, err := s.Encode(&R{Price: &m, Name: "test"})
		if err != nil {
			t.Fatal(err)
		}
		var out R
		if _, err := s.Decode(data, &out); err != nil {
			t.Fatal(err)
		}
		if out.Price == nil || out.Price.Cents != 999 || out.Name != "test" {
			t.Errorf("got %+v", out)
		}
		// Encode again to exercise fast-path compilation.
		data2, _ := s.Encode(&R{Price: &m, Name: "t2"})
		var out2 R
		s.Decode(data2, &out2)
		if out2.Name != "t2" {
			t.Errorf("got %q", out2.Name)
		}
	})

	t.Run("null", func(t *testing.T) {
		data, _ := s.Encode(&R{Price: nil, Name: "x"})
		var out R
		s.Decode(data, &out)
		if out.Price != nil {
			t.Errorf("got %+v, want nil", out.Price)
		}
	})

	t.Run("any_target", func(t *testing.T) {
		s2 := parseMoney(t, `{"type":"record","name":"R2","fields":[
			{"name":"v","type":["null",{"type":"long","logicalType":"money"}]}
		]}`)
		data, _ := s2.Encode(map[string]any{"v": int64(100)})
		var v any
		s2.Decode(data, &v)
		if m := v.(map[string]any); m["v"].(testMoney).Cents != 100 {
			t.Errorf("got %v", m["v"])
		}
	})
}

func TestCustomTypeErrors(t *testing.T) {
	t.Run("decode_fatal", func(t *testing.T) {
		myErr := errors.New("boom")
		s, _ := Parse(`{"type":"long","logicalType":"money"}`, CustomType{
			LogicalType: "money",
			Decode:      func(any, *SchemaNode) (any, error) { return nil, myErr },
		})
		data, _ := s.Encode(int64(1))
		var v any
		_, err := s.Decode(data, &v)
		if !errors.Is(err, myErr) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("encode_fatal", func(t *testing.T) {
		myErr := errors.New("encode boom")
		s, _ := Parse(`{"type":"long","logicalType":"money"}`, CustomType{
			LogicalType: "money",
			GoType:      reflect.TypeFor[testMoney](),
			Encode:      func(any, *SchemaNode) (any, error) { return nil, myErr },
		})
		_, err := s.Encode(testMoney{Cents: 1})
		if !errors.Is(err, myErr) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("encode_json_fatal", func(t *testing.T) {
		myErr := errors.New("json boom")
		s, _ := Parse(`{"type":"long","logicalType":"money"}`, CustomType{
			LogicalType: "money",
			GoType:      reflect.TypeFor[testMoney](),
			Encode:      func(any, *SchemaNode) (any, error) { return nil, myErr },
		})
		_, err := s.EncodeJSON(testMoney{Cents: 1})
		if !errors.Is(err, myErr) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("encode_skip", func(t *testing.T) {
		s, _ := Parse(`{"type":"long","logicalType":"money"}`, CustomType{
			LogicalType: "money",
			GoType:      reflect.TypeFor[testMoney](),
			Encode:      func(any, *SchemaNode) (any, error) { return nil, ErrSkipCustomType },
		})
		_, err := s.Encode(testMoney{Cents: 1})
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("decode_nil_result", func(t *testing.T) {
		s, _ := Parse(`{"type":"long","logicalType":"money"}`, CustomType{
			LogicalType: "money",
			Decode:      func(any, *SchemaNode) (any, error) { return nil, nil },
		})
		data, _ := s.Encode(int64(42))
		var v any
		s.Decode(data, &v)
		if v != nil {
			t.Errorf("got %v", v)
		}
	})

	t.Run("decode_short_buffer", func(t *testing.T) {
		s := parseMoney(t, `{"type":"long","logicalType":"money"}`)
		var v any
		_, err := s.Decode(nil, &v)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("invalid_avro_type", func(t *testing.T) {
		type Bad struct{}
		ct := NewCustomType[Bad, complex128]("bad",
			func(Bad, *SchemaNode) (complex128, error) { return 0, nil },
			func(complex128, *SchemaNode) (Bad, error) { return Bad{}, nil },
		)
		_, err := Parse(`{"type":"long","logicalType":"bad"}`, ct)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestCustomTypeMatching(t *testing.T) {
	t.Run("first_match_wins", func(t *testing.T) {
		s, _ := Parse(`{"type":"long","logicalType":"money"}`,
			NewCustomType[testMoney, int64]("money", nil,
				func(c int64, _ *SchemaNode) (testMoney, error) { return testMoney{Cents: c, Currency: "FIRST"}, nil },
			),
			NewCustomType[testMoney, int64]("money", nil,
				func(c int64, _ *SchemaNode) (testMoney, error) { return testMoney{Cents: c, Currency: "SECOND"}, nil },
			),
		)
		data, _ := s.Encode(int64(100))
		var v any
		s.Decode(data, &v)
		if v.(testMoney).Currency != "FIRST" {
			t.Errorf("got %q", v.(testMoney).Currency)
		}
	})

	t.Run("avro_type_mismatch", func(t *testing.T) {
		s, _ := Parse(`{"type":"long","logicalType":"money"}`, CustomType{
			LogicalType: "money",
			AvroType:    "string",
			Decode:      func(any, *SchemaNode) (any, error) { return "bad", nil },
		})
		data, _ := s.Encode(int64(42))
		var v any
		s.Decode(data, &v)
		if _, ok := v.(int64); !ok {
			t.Fatalf("expected int64, got %T", v)
		}
	})

	t.Run("encode_gotype_skip", func(t *testing.T) {
		s := parseMoney(t, `{"type":"long","logicalType":"money"}`)
		// Raw int64 → GoType doesn't match → passes through.
		data, err := s.Encode(int64(42))
		if err != nil {
			t.Fatal(err)
		}
		if len(data) == 0 {
			t.Fatal("empty")
		}
	})

	t.Run("nil_encode_func", func(t *testing.T) {
		s, _ := Parse(`{"type":"long","logicalType":"money"}`,
			NewCustomType[testMoney, int64]("money", nil,
				func(c int64, _ *SchemaNode) (testMoney, error) { return testMoney{Cents: c}, nil },
			),
		)
		data, _ := s.Encode(int64(42))
		var v any
		s.Decode(data, &v)
		if v.(testMoney).Cents != 42 {
			t.Errorf("got %d", v.(testMoney).Cents)
		}
	})

	t.Run("nil_decode_func", func(t *testing.T) {
		s, _ := Parse(`{"type":"long","logicalType":"money"}`,
			NewCustomType[testMoney, int64]("money",
				func(m testMoney, _ *SchemaNode) (int64, error) { return m.Cents, nil }, nil,
			),
		)
		data, _ := s.Encode(testMoney{Cents: 77})
		var v any
		s.Decode(data, &v)
		if _, ok := v.(int64); !ok {
			t.Fatalf("expected int64, got %T", v)
		}
	})

	t.Run("skip_fallthrough", func(t *testing.T) {
		s, _ := Parse(`{"type":"long"}`, CustomType{
			AvroType: "long",
			Decode:   func(any, *SchemaNode) (any, error) { return nil, ErrSkipCustomType },
		})
		data, _ := s.Encode(int64(42))
		var v any
		s.Decode(data, &v)
		if v.(int64) != 42 {
			t.Errorf("got %v", v)
		}
	})

	t.Run("empty_criteria", func(t *testing.T) {
		calls := 0
		s, _ := Parse(`{"type":"record","name":"R","fields":[
			{"name":"a","type":"int"},{"name":"b","type":"string"}
		]}`, CustomType{
			Decode: func(any, *SchemaNode) (any, error) { calls++; return nil, ErrSkipCustomType },
		})
		data, _ := s.Encode(map[string]any{"a": int32(1), "b": "hello"})
		var v any
		s.Decode(data, &v)
		if calls == 0 {
			t.Error("expected calls")
		}
	})

	t.Run("wildcard_preserves_builtins", func(t *testing.T) {
		s, _ := Parse(`{"type":"long","logicalType":"timestamp-millis"}`, CustomType{
			Decode: func(any, *SchemaNode) (any, error) { return nil, ErrSkipCustomType },
		})
		data, _ := s.Encode(int64(1687221496000))
		var v any
		s.Decode(data, &v)
		if _, ok := v.(time.Time); !ok {
			t.Fatalf("expected time.Time, got %T", v)
		}
	})
}

func TestCustomTypeBackedByRecord(t *testing.T) {
	s, _ := Parse(`{"type":"record","name":"R","fields":[
		{"name":"loc","type":{"type":"record","name":"Loc","logicalType":"geo",
			"fields":[{"name":"lat","type":"double"},{"name":"lng","type":"double"}]
		}}
	]}`, CustomType{
		LogicalType: "geo",
		AvroType:    "record",
		GoType:      reflect.TypeFor[testGeoPoint](),
		Encode: func(v any, _ *SchemaNode) (any, error) {
			g := v.(testGeoPoint)
			return map[string]any{"lat": g.Lat, "lng": g.Lng}, nil
		},
		Decode: func(v any, _ *SchemaNode) (any, error) {
			m := v.(map[string]any)
			return testGeoPoint{Lat: m["lat"].(float64), Lng: m["lng"].(float64)}, nil
		},
	})
	data, _ := s.Encode(map[string]any{"loc": testGeoPoint{Lat: 37.7749, Lng: -122.4194}})
	var v any
	s.Decode(data, &v)
	g := v.(map[string]any)["loc"].(testGeoPoint)
	if math.Abs(g.Lat-37.7749) > 0.0001 || math.Abs(g.Lng+122.4194) > 0.0001 {
		t.Errorf("got %+v", g)
	}
}

func TestCustomTypeSchemaProps(t *testing.T) {
	s, _ := Parse(`{"type":"record","name":"R","fields":[
		{"name":"ts","type":{"type":"long","connect.name":"io.debezium.time.Timestamp"}}
	]}`, CustomType{
		Decode: func(v any, sn *SchemaNode) (any, error) {
			if sn.Props["connect.name"] == "io.debezium.time.Timestamp" {
				return time.UnixMilli(v.(int64)).UTC(), nil
			}
			return nil, ErrSkipCustomType
		},
	})
	data, _ := s.Encode(map[string]any{"ts": int64(1687221496000)})
	var v any
	s.Decode(data, &v)
	if ts := v.(map[string]any)["ts"].(time.Time); ts.UnixMilli() != 1687221496000 {
		t.Errorf("got %v", ts)
	}
}

func TestCustomTypeSchemaCache(t *testing.T) {
	t.Run("no_leak", func(t *testing.T) {
		var cache SchemaCache
		s1, _ := cache.Parse(`{"type":"long","logicalType":"money"}`)
		s2, _ := cache.Parse(`{"type":"long","logicalType":"money"}`, moneyCT)
		if s1 == s2 {
			t.Error("should not return cached schema for custom parse")
		}
		data, _ := s1.Encode(int64(42))
		var v1, v2 any
		s1.Decode(data, &v1)
		s2.Decode(data, &v2)
		if _, ok := v1.(int64); !ok {
			t.Errorf("s1: expected int64, got %T", v1)
		}
		if _, ok := v2.(testMoney); !ok {
			t.Errorf("s2: expected testMoney, got %T", v2)
		}
		s3, _ := cache.Parse(`{"type":"long","logicalType":"money"}`)
		if s1 != s3 {
			t.Error("expected cached schema")
		}
	})

	t.Run("reparse", func(t *testing.T) {
		var cache SchemaCache
		s1, err := cache.Parse(`{"type":"record","name":"Order","fields":[
			{"name":"price","type":{"type":"long","logicalType":"money"}}
		]}`, moneyCT)
		if err != nil {
			t.Fatal(err)
		}
		s2, err := cache.Parse(`{"type":"record","name":"Order","fields":[
			{"name":"price","type":{"type":"long","logicalType":"money"}}
		]}`, moneyCT)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := s1.Encode(map[string]any{"price": testMoney{Cents: 1}})
		var v1, v2 any
		s1.Decode(data, &v1)
		s2.Decode(data, &v2)
		if v1.(map[string]any)["price"].(testMoney).Cents != 1 {
			t.Error("s1 failed")
		}
		if v2.(map[string]any)["price"].(testMoney).Cents != 1 {
			t.Error("s2 failed")
		}
	})
}

func TestCustomTypeResolve(t *testing.T) {
	t.Run("promotion", func(t *testing.T) {
		writer, _ := Parse(`"int"`)
		reader := parseMoney(t, `{"type":"long","logicalType":"money"}`)
		resolved, _ := Resolve(writer, reader)
		data, _ := writer.Encode(int32(500))
		var v any
		resolved.Decode(data, &v)
		if v.(testMoney).Cents != 500 {
			t.Errorf("got %d", v.(testMoney).Cents)
		}
	})

	t.Run("same_kind", func(t *testing.T) {
		writer := parseMoney(t, `{"type":"long","logicalType":"money"}`)
		reader := parseMoney(t, `{"type":"long","logicalType":"money"}`)
		resolved, _ := Resolve(writer, reader)
		data, _ := writer.Encode(testMoney{Cents: 42})
		var v any
		resolved.Decode(data, &v)
		if v.(testMoney).Cents != 42 {
			t.Errorf("got %d", v.(testMoney).Cents)
		}
	})
}

func TestNewCustomTypeAllAvroTypes(t *testing.T) {
	tests := []struct {
		name, want string
		ct         CustomType
	}{
		{"bool", "boolean", NewCustomType[testStatus, bool]("b", nil, nil)},
		{"int32", "int", NewCustomType[testStatus, int32]("i", nil, nil)},
		{"int64", "long", NewCustomType[testStatus, int64]("l", nil, nil)},
		{"float32", "float", NewCustomType[testStatus, float32]("f", nil, nil)},
		{"float64", "double", NewCustomType[testStatus, float64]("d", nil, nil)},
		{"string", "string", NewCustomType[testStatus, string]("s", nil, nil)},
		{"bytes", "bytes", NewCustomType[testStatus, []byte]("b2", nil, nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.ct.AvroType != tt.want {
				t.Errorf("got %q", tt.ct.AvroType)
			}
		})
	}
}

func TestCustomTypeSchemaCacheNonCustomAfterCustom(t *testing.T) {
	var cache SchemaCache
	_, err := cache.Parse(`{"type":"record","name":"Order","fields":[
		{"name":"price","type":{"type":"long","logicalType":"money"}}
	]}`, moneyCT)
	if err != nil {
		t.Fatalf("custom parse: %v", err)
	}
	s, err := cache.Parse(`{"type":"record","name":"Order","fields":[
		{"name":"price","type":{"type":"long","logicalType":"money"}}
	]}`)
	if err != nil {
		t.Fatalf("non-custom parse after custom: %v", err)
	}
	data, _ := s.Encode(map[string]any{"price": int64(42)})
	var v any
	s.Decode(data, &v)
	if v.(map[string]any)["price"].(int64) != 42 {
		t.Errorf("got %v", v)
	}
}

func TestCustomTypePointerGoType(t *testing.T) {
	type Wrapper struct{ V string }
	ct := CustomType{
		LogicalType: "wrapped",
		AvroType:    "string",
		GoType:      reflect.TypeFor[*Wrapper](),
		Encode: func(v any, _ *SchemaNode) (any, error) {
			return v.(*Wrapper).V, nil
		},
		Decode: func(v any, _ *SchemaNode) (any, error) {
			return &Wrapper{V: v.(string)}, nil
		},
	}
	s, err := Parse(`{"type":"string","logicalType":"wrapped"}`, ct)
	if err != nil {
		t.Fatal(err)
	}
	w := &Wrapper{V: "hello"}
	data, err := s.Encode(w)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Decode into *any — exercises the customEncode pointer-level GoType match.
	var out any
	if _, err := s.Decode(data, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.(*Wrapper).V; got != "hello" {
		t.Errorf("any: got %q", got)
	}

	// Decode into typed *Wrapper — exercises setCustomResult AssignableTo
	// for pointer-valued results into pointer targets.
	var typed *Wrapper
	if _, err := s.Decode(data, &typed); err != nil {
		t.Fatalf("typed decode: %v", err)
	}
	if typed == nil || typed.V != "hello" {
		t.Errorf("typed: got %+v", typed)
	}
}

func TestCustomTypePointerGoTypeEncodeError(t *testing.T) {
	type Wrapper struct{ V string }
	myErr := errors.New("ptr encode fail")
	ct := CustomType{
		LogicalType: "wrapped",
		AvroType:    "string",
		GoType:      reflect.TypeFor[*Wrapper](),
		Encode: func(v any, _ *SchemaNode) (any, error) {
			return nil, myErr
		},
	}
	s, _ := Parse(`{"type":"string","logicalType":"wrapped"}`, ct)
	_, err := s.Encode(&Wrapper{V: "x"})
	if !errors.Is(err, myErr) {
		t.Fatalf("expected myErr, got %v", err)
	}
}

func TestCustomTypePointerGoTypeEncodeSkip(t *testing.T) {
	type Wrapper struct{ V string }
	ct := CustomType{
		LogicalType: "wrapped",
		AvroType:    "string",
		GoType:      reflect.TypeFor[*Wrapper](),
		Encode: func(v any, _ *SchemaNode) (any, error) {
			return nil, ErrSkipCustomType
		},
	}
	s, _ := Parse(`{"type":"string","logicalType":"wrapped"}`, ct)
	// Pointer GoType match, but encoder skips → falls through to raw
	// string ser which fails for *Wrapper.
	_, err := s.Encode(&Wrapper{V: "x"})
	if err == nil {
		t.Fatal("expected error after skip")
	}
}

func TestCustomTypeNilPointerEncode(t *testing.T) {
	// Nil pointer value should pass through without panic.
	s, _ := Parse(`{"type":"string","logicalType":"wrapped"}`, CustomType{
		LogicalType: "wrapped",
		GoType:      reflect.TypeFor[*testMoney](),
		Encode: func(v any, _ *SchemaNode) (any, error) {
			return "converted", nil
		},
	})
	// Encode nil *testMoney via a map with nil value.
	_, err := s.Encode((*testMoney)(nil))
	// Should not panic. May error (nil for non-null string) but not panic.
	_ = err
}

func TestWithCustomTypeWrapper(t *testing.T) {
	// Exercises the WithCustomType discoverability wrapper.
	ct := NewCustomType[testMoney, int64]("money",
		func(m testMoney, _ *SchemaNode) (int64, error) { return m.Cents, nil },
		func(c int64, _ *SchemaNode) (testMoney, error) { return testMoney{Cents: c}, nil },
	)
	s, err := Parse(`{"type":"long","logicalType":"money"}`, WithCustomType(ct))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := s.Encode(testMoney{Cents: 1})
	var v any
	s.Decode(data, &v)
	if v.(testMoney).Cents != 1 {
		t.Errorf("got %v", v)
	}
}

func TestCustomTypeArrayFastPathDisabled(t *testing.T) {
	// Exercises schema.go array hasCustomType path.
	s, err := Parse(`{"type":"array","items":{"type":"long","logicalType":"money"}}`, moneyCT)
	if err != nil {
		t.Fatal(err)
	}
	data, err := s.Encode([]testMoney{{Cents: 1}, {Cents: 2}})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var out any
	if _, err := s.Decode(data, &out); err != nil {
		t.Fatal(err)
	}
	arr := out.([]any)
	if len(arr) != 2 || arr[0].(testMoney).Cents != 1 || arr[1].(testMoney).Cents != 2 {
		t.Errorf("got %v", arr)
	}
}

func TestCustomTypeMapFastPathDisabled(t *testing.T) {
	s, err := Parse(`{"type":"map","values":{"type":"long","logicalType":"money"}}`, moneyCT)
	if err != nil {
		t.Fatal(err)
	}
	data, err := s.Encode(map[string]testMoney{"a": {Cents: 10}})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var out any
	if _, err := s.Decode(data, &out); err != nil {
		t.Fatal(err)
	}
	m := out.(map[string]any)
	if m["a"].(testMoney).Cents != 10 {
		t.Errorf("got %v", m)
	}
}

// An AvroType-only CustomType (no logicalType) must fire on the JSON
// array/map element paths exactly as it does on binary. The binary
// fast-path gate disables specialization when the element carries a custom
// type (meta.hasCustomType); the JSON fast-path gate previously checked only
// logical=="" and emitted/parsed the raw element, silently skipping the
// custom codec — a binary↔JSON wire divergence. (The existing
// TestCustomType{Array,Map}FastPathDisabled use a logicalType-bearing custom
// type, so logical!="" also tripped the JSON gate and masked this gap.)
func TestCustomTypeJSONArrayAvroTypeOnly(t *testing.T) {
	ct := CustomType{
		AvroType: "long",
		Encode:   func(v any, _ *SchemaNode) (any, error) { return v.(int64) + 1000, nil },
		Decode:   func(v any, _ *SchemaNode) (any, error) { return v.(int64) - 1000, nil },
	}
	s, err := Parse(`{"type":"array","items":"long"}`, ct)
	if err != nil {
		t.Fatal(err)
	}
	in := []int64{5, 6}
	bin, err := s.Encode(in)
	if err != nil {
		t.Fatalf("binary encode: %v", err)
	}
	js, err := s.EncodeJSON(in)
	if err != nil {
		t.Fatalf("json encode: %v", err)
	}
	// Read the raw wire values each encoder wrote, via a no-custom schema.
	plain := MustParse(`{"type":"array","items":"long"}`)
	var rawBin, rawJSON []int64
	if _, err := plain.Decode(bin, &rawBin); err != nil {
		t.Fatal(err)
	}
	if err := plain.DecodeJSON(js, &rawJSON); err != nil {
		t.Fatal(err)
	}
	want := []int64{1005, 1006} // custom Encode added 1000
	if !reflect.DeepEqual(rawBin, want) {
		t.Fatalf("binary raw wire = %v, want %v", rawBin, want)
	}
	if !reflect.DeepEqual(rawJSON, want) {
		t.Fatalf("json raw wire = %v, want %v (custom Encode skipped on JSON array fast path)", rawJSON, want)
	}
	// JSON decode must apply the custom Decode (subtract 1000).
	var out []int64
	if err := s.DecodeJSON(js, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("json decode = %v, want %v (custom Decode skipped on JSON array fast path)", out, in)
	}
}

func TestCustomTypeJSONMapAvroTypeOnly(t *testing.T) {
	ct := CustomType{
		AvroType: "long",
		Encode:   func(v any, _ *SchemaNode) (any, error) { return v.(int64) + 1000, nil },
		Decode:   func(v any, _ *SchemaNode) (any, error) { return v.(int64) - 1000, nil },
	}
	s, err := Parse(`{"type":"map","values":"long"}`, ct)
	if err != nil {
		t.Fatal(err)
	}
	in := map[string]int64{"a": 5}
	bin, err := s.Encode(in)
	if err != nil {
		t.Fatalf("binary encode: %v", err)
	}
	js, err := s.EncodeJSON(in)
	if err != nil {
		t.Fatalf("json encode: %v", err)
	}
	plain := MustParse(`{"type":"map","values":"long"}`)
	var rawBin, rawJSON map[string]int64
	if _, err := plain.Decode(bin, &rawBin); err != nil {
		t.Fatal(err)
	}
	if err := plain.DecodeJSON(js, &rawJSON); err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{"a": 1005}
	if !reflect.DeepEqual(rawBin, want) {
		t.Fatalf("binary raw wire = %v, want %v", rawBin, want)
	}
	if !reflect.DeepEqual(rawJSON, want) {
		t.Fatalf("json raw wire = %v, want %v (custom Encode skipped on JSON map fast path)", rawJSON, want)
	}
	var out map[string]int64
	if err := s.DecodeJSON(js, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("json decode = %v, want %v (custom Decode skipped on JSON map fast path)", out, in)
	}
}

func TestCustomTypeFixedLogicalType(t *testing.T) {
	// Exercises hasMatchingCustomType("fixed", logical) path.
	type PackedID [8]byte
	ct := CustomType{
		LogicalType: "packed-id",
		AvroType:    "fixed",
		GoType:      reflect.TypeFor[string](),
		Encode: func(v any, _ *SchemaNode) (any, error) {
			s := v.(string)
			var b [8]byte
			copy(b[:], s)
			return b[:], nil
		},
		Decode: func(v any, _ *SchemaNode) (any, error) {
			b := v.([]byte)
			return string(b), nil
		},
	}
	s, err := Parse(`{"type":"fixed","name":"pid","size":8,"logicalType":"packed-id"}`, ct)
	if err != nil {
		t.Fatal(err)
	}
	data, err := s.Encode("hello!!!")
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var v any
	if _, err := s.Decode(data, &v); err != nil {
		t.Fatal(err)
	}
	if v.(string) != "hello!!!" {
		t.Errorf("got %q", v)
	}
}

func TestCustomTypeJsonNumberInt64Validation(t *testing.T) {
	// Exercises jsonNumberToInt64 non-whole-number error.
	s, err := Parse(`{"type":"array","items":"long"}`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Encode([]any{json.Number("1.5")})
	if err == nil {
		t.Fatal("expected error for non-whole json.Number in long array")
	}
}

func TestCustomTypeDecodeIntIntoAny(t *testing.T) {
	// Exercises setIntValue interface path through custom decode wrapper.
	ct := CustomType{
		AvroType: "int",
		Decode: func(v any, _ *SchemaNode) (any, error) {
			return v, nil // pass through raw int32
		},
	}
	s, err := Parse(`"int"`, ct)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := s.Encode(int32(42))
	var v any
	if _, err := s.Decode(data, &v); err != nil {
		t.Fatal(err)
	}
	if v.(int32) != 42 {
		t.Errorf("got %v", v)
	}
}

// TestRegression_DecodeJSONFillsDefaultThroughCustomDecoder locks in that
// DecodeJSON applies a registered CustomType.Decode to a record field's
// default value when the field is absent from the JSON input — matching
// the binary side, where the field's pre-encoded defaultBytes roundtrip
// through the same wrapped deserRecord.fields[i].fn as a present field's
// wire bytes.
//
// Without this, applyFieldDefault dispatched through the unwrapped
// node.fields[idx].node.deser (built before applyCustomTypes installed
// the chain), bypassing CustomType.Decode and surfacing the raw
// Avro-native value (int64 for a long-backed money type) directly into
// a target Go type that expects the user's custom domain type — failing
// with "cannot use X with Avro type Y" on any DecodeJSON of an empty or
// partially-omitted record into a typed struct/typed-map whose field
// type is the user's custom type.
//
// Sub-tests cover the three iterateRecordFields entry points:
//   - into_struct: *struct → decodeRecordStruct → applyFieldDefault.
//   - into_any: *any → decodeRecordAny.
//   - into_map_string_any: *map[string]any → decodeRecordMap (any-typed elem).
//
// Each sub-test pairs JSON-decode with the binary-roundtrip equivalent
// and asserts both produce the same user-domain Go value so the parity
// guarantee survives schema/codec changes.
func TestRegression_DecodeJSONFillsDefaultThroughCustomDecoder(t *testing.T) {
	s := parseMoney(t, `{"type":"record","name":"R","fields":[
		{"name":"price","type":{"type":"long","logicalType":"money"},"default":42}
	]}`)

	t.Run("into_struct", func(t *testing.T) {
		type R struct {
			Price testMoney `avro:"price"`
		}
		// Binary parity (default is encoded into wire then decoded through
		// the wrapped deser): produces the user's domain type.
		wire, err := s.AppendEncode(nil, map[string]any{})
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var rBin R
		if _, err := s.Decode(wire, &rBin); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if rBin.Price.Cents != 42 || rBin.Price.Currency != "USD" {
			t.Fatalf("binary: Price=%+v, want {Cents:42, Currency:USD}", rBin.Price)
		}

		// JSON decode of empty object must materialize the same value.
		var rJSON R
		if err := s.DecodeJSON([]byte(`{}`), &rJSON); err != nil {
			t.Fatalf("DecodeJSON({}): %v", err)
		}
		if rJSON.Price != rBin.Price {
			t.Fatalf("JSON Price=%+v, want %+v (binary parity)", rJSON.Price, rBin.Price)
		}
	})

	t.Run("into_any", func(t *testing.T) {
		var v any
		if err := s.DecodeJSON([]byte(`{}`), &v); err != nil {
			t.Fatalf("DecodeJSON({}): %v", err)
		}
		got, ok := v.(map[string]any)
		if !ok {
			t.Fatalf("decoded into %T, want map[string]any", v)
		}
		price, ok := got["price"].(testMoney)
		if !ok {
			t.Fatalf("price: got %T %#v, want testMoney", got["price"], got["price"])
		}
		if price.Cents != 42 || price.Currency != "USD" {
			t.Fatalf("price=%+v, want {Cents:42, Currency:USD}", price)
		}
	})

	t.Run("into_map_string_any", func(t *testing.T) {
		var got map[string]any
		if err := s.DecodeJSON([]byte(`{}`), &got); err != nil {
			t.Fatalf("DecodeJSON({}): %v", err)
		}
		price, ok := got["price"].(testMoney)
		if !ok {
			t.Fatalf("price: got %T %#v, want testMoney", got["price"], got["price"])
		}
		if price.Cents != 42 {
			t.Fatalf("price.Cents=%d, want 42", price.Cents)
		}
	})

	t.Run("partial_fill_present_and_default", func(t *testing.T) {
		// One field present, one filled from default — both must produce
		// the user's domain type through the custom decoder.
		s := parseMoney(t, `{"type":"record","name":"R","fields":[
			{"name":"price","type":{"type":"long","logicalType":"money"},"default":42},
			{"name":"shipping","type":{"type":"long","logicalType":"money"},"default":7}
		]}`)
		type R struct {
			Price    testMoney `avro:"price"`
			Shipping testMoney `avro:"shipping"`
		}
		var r R
		if err := s.DecodeJSON([]byte(`{"price":100}`), &r); err != nil {
			t.Fatalf("DecodeJSON: %v", err)
		}
		if r.Price.Cents != 100 || r.Shipping.Cents != 7 {
			t.Fatalf("got %+v, want Price.Cents=100 (present) Shipping.Cents=7 (default)", r)
		}
	})
}

// TestRegression_EncodeJSONBypassesCustomEncoderForDefaultFill locks in
// that AppendEncodeJSON does NOT invoke a registered CustomType.Encode
// for default-filled record fields — matching binary's encodeDefault
// which is a self-contained switch with no custom-wiring hook.
//
// Rationale: CustomType.Encode converts user-Go-type → Avro-native; the
// parsed default value is already in Avro-native form (json.Number /
// []byte / string per the schema's type) and never had a Go-domain-type
// representation, so the directional contract has nothing to apply.
// Pre-fix, appendJSONFieldDefault routed defaults through appendAvroJSON
// with a non-nil custom map, firing the user's Encode once per
// default-filled custom-typed field on JSON-encode of an empty map and
// passing a json.Number the encoder's GoType filter doesn't recognize.
// Binary-encode of the same empty map fired the encoder zero times.
//
// The asymmetry is silently benign for GoType-typed encoders that
// fallthrough via ErrSkipCustomType on a type-assertion miss, but
// surfaces as a behavioral surprise for GoType=nil encoders used for
// logging / validation / property-based dispatch.
func TestRegression_EncodeJSONBypassesCustomEncoderForDefaultFill(t *testing.T) {
	// GoType=nil so the encoder fires on every value reaching the long+
	// money node — instrumentation pattern that surfaces the asymmetry.
	calls := 0
	ct := CustomType{
		LogicalType: "money",
		AvroType:    "long",
		Encode: func(v any, _ *SchemaNode) (any, error) {
			calls++
			return v, nil
		},
	}
	s, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"f","type":{"type":"long","logicalType":"money"},"default":42}
	]}`, ct)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	calls = 0
	if _, err := s.AppendEncode(nil, map[string]any{}); err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	binaryCalls := calls
	if binaryCalls != 0 {
		t.Fatalf("binary AppendEncode fired the user encoder %d times on default-fill; defaults bypass encodeDefault", binaryCalls)
	}

	calls = 0
	if _, err := s.AppendEncodeJSON(nil, map[string]any{}); err != nil {
		t.Fatalf("AppendEncodeJSON: %v", err)
	}
	if calls != 0 {
		t.Fatalf("AppendEncodeJSON fired the user encoder %d times on default-fill; must match binary (0)", calls)
	}

	// User-supplied values still fire the encoder on both paths. Lock
	// that the bypass only applies to defaults.
	calls = 0
	if _, err := s.AppendEncodeJSON(nil, map[string]any{"f": int64(99)}); err != nil {
		t.Fatalf("AppendEncodeJSON with present field: %v", err)
	}
	if calls != 1 {
		t.Fatalf("AppendEncodeJSON with present field fired the user encoder %d times, want 1", calls)
	}
}

// A custom-decoded value whose decode TARGET is a recursive pointer type
// (cyclic type graph: ctRecursivePtr's element is itself) must terminate with
// an error, not loop forever allocating a pointer level per iteration.
// setCustomResult's pointer walk is bounded by maxIndirectDepth — the same
// ceiling the non-custom indirect/indirectAlloc decode path uses, which
// already errors for this target (so registering a CustomType must not turn a
// clean error into an unbounded loop). Watchdog so a regression fails by
// timeout rather than hanging the suite.
type ctRecursivePtr *ctRecursivePtr

func TestRegression_CustomDecodeBoundsRecursivePointerTarget(t *testing.T) {
	s, err := Parse(`"long"`, CustomType{
		AvroType: "long",
		Decode:   func(v any, _ *SchemaNode) (any, error) { return v, nil },
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	wire, err := s.Encode(int64(5))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		var p ctRecursivePtr
		_, derr := s.Decode(wire, &p)
		done <- derr
	}()
	select {
	case derr := <-done:
		if derr == nil {
			t.Fatal("decode into recursive pointer target must error, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("decode into recursive pointer target did not terminate (setCustomResult pointer walk unbounded)")
	}
}
