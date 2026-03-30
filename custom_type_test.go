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
