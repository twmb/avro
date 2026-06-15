package avro

import (
	"encoding/json"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSchemaForBasic(t *testing.T) {
	type User struct {
		Name  string `avro:"name"`
		Age   int32  `avro:"age"`
		Score int64  `avro:"score"`
	}

	t.Run("with namespace", func(t *testing.T) {
		s, err := SchemaFor[User](WithNamespace("com.example"))
		if err != nil {
			t.Fatal(err)
		}
		u := User{Name: "Alice", Age: 30, Score: 100}
		data, err := s.Encode(&u)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got User
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got != u {
			t.Errorf("got %+v, want %+v", got, u)
		}
	})

	t.Run("no namespace", func(t *testing.T) {
		s, err := SchemaFor[User]()
		if err != nil {
			t.Fatal(err)
		}
		data, err := s.Encode(&User{Name: "Bob", Age: 25, Score: 50})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got User
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Name != "Bob" {
			t.Errorf("got %q, want Bob", got.Name)
		}
	})

	t.Run("pointer to struct", func(t *testing.T) {
		s, err := SchemaFor[*User]()
		if err != nil {
			t.Fatal(err)
		}
		data, err := s.Encode(&User{Name: "C", Age: 1, Score: 2})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got User
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Name != "C" {
			t.Errorf("got %q, want C", got.Name)
		}
	})
}

func TestSchemaForNullable(t *testing.T) {
	type Record struct {
		Name  string  `avro:"name"`
		Email *string `avro:"email"`
	}
	s, err := SchemaFor[Record]()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("non-nil", func(t *testing.T) {
		email := "alice@example.com"
		r := Record{Name: "Alice", Email: &email}
		data, err := s.Encode(&r)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got Record
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Email == nil || *got.Email != *r.Email {
			t.Errorf("got %+v, want %+v", got, r)
		}
	})

	t.Run("nil", func(t *testing.T) {
		r := Record{Name: "Bob", Email: nil}
		data, err := s.Encode(&r)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got Record
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Email != nil {
			t.Errorf("expected nil email, got %v", *got.Email)
		}
	})
}

func TestSchemaForMultiLevelPointer(t *testing.T) {
	// The codecs collapse a whole pointer chain (**T, ***T, ...) to a
	// single nullable union via indirect/indirectAlloc, so SchemaFor must
	// emit one ["null", T] — not a union nested inside a union, which Avro
	// forbids ("unions cannot immediately contain other unions") and which
	// would make the schema unusable. The chain is capped at the codec's
	// own unwrap limit (maxIndirectDepth consecutive pointer levels); a
	// deeper chain is refused at build (see
	// TestRegression_SchemaForDeepPointerChainRefusedAtBuild) rather than
	// emitting a ["null",T] the encoder would reject with errIndirectDeep.
	t.Run("double pointer to int", func(t *testing.T) {
		type Rec struct {
			V **int32 `avro:"v"`
		}
		s, err := SchemaFor[Rec]()
		if err != nil {
			t.Fatal(err)
		}
		// The emitted schema must itself parse.
		if _, err := Parse(s.String()); err != nil {
			t.Fatalf("emitted schema does not parse: %v\nschema: %s", err, s.String())
		}
		n := int32(7)
		p := &n
		in := Rec{V: &p}
		data, err := s.Encode(&in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got Rec
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.V == nil || *got.V == nil || **got.V != 7 {
			t.Fatalf("round-trip mismatch: got %v, want **int32(7)", got.V)
		}
	})

	t.Run("double pointer to struct", func(t *testing.T) {
		type Inner struct {
			Value int32 `avro:"value"`
		}
		type Rec struct {
			V **Inner `avro:"v"`
		}
		s, err := SchemaFor[Rec]()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Parse(s.String()); err != nil {
			t.Fatalf("emitted schema does not parse: %v\nschema: %s", err, s.String())
		}
	})

	t.Run("triple pointer", func(t *testing.T) {
		type Rec struct {
			V ***int32 `avro:"v"`
		}
		s, err := SchemaFor[Rec]()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Parse(s.String()); err != nil {
			t.Fatalf("emitted schema does not parse: %v\nschema: %s", err, s.String())
		}
	})
}

// A pointer chain deeper than the codec unwraps (maxIndirectDepth consecutive
// levels) must be refused at BUILD, not emitted as a ["null",T] the encoder
// then rejects. The codec's indirect/indirectAlloc accept a chain bottoming at
// a non-pointer base within maxIndirectDepth levels, so a SchemaFor schema for
// a deeper chain would build but fail Encode of a non-nil value — a
// build-accepts/encode-rejects asymmetry. Boundary: a chain at the cap
// (maxIndirectDepth levels) builds AND round-trips a non-nil value; one level
// deeper is refused at build naming the pointer-chain cause.
func TestRegression_SchemaForDeepPointerChainRefusedAtBuild(t *testing.T) {
	// At-cap chain (maxIndirectDepth == 5 pointer levels) builds and round-trips
	// a NON-NIL value — the exact depth a deeper chain breaks and the depth the
	// encode↔decode off-by-one used to reject on encode while decode accepted.
	type AtCap struct {
		F *****int32 `avro:"f"`
	}
	s, err := SchemaFor[AtCap]()
	if err != nil {
		t.Fatalf("a %d-level pointer chain (at the cap) must build: %v", maxIndirectDepth, err)
	}
	if _, err := Parse(s.String()); err != nil {
		t.Fatalf("at-cap schema must re-parse: %v\n%s", err, s.String())
	}
	n := int32(42)
	p1 := &n
	p2 := &p1
	p3 := &p2
	p4 := &p3
	p5 := &p4
	wire, err := s.Encode(&AtCap{F: p5}) // non-nil all the way down
	if err != nil {
		t.Fatalf("at-cap chain must Encode a non-nil value: %v", err)
	}
	var got AtCap
	if _, err := s.Decode(wire, &got); err != nil {
		t.Fatalf("at-cap chain must Decode: %v", err)
	}
	if got.F == nil || *****got.F != 42 {
		t.Fatalf("at-cap round-trip mismatch: %v", got.F)
	}

	// One level past the cap (maxIndirectDepth+1 == 6 pointer levels) is refused
	// at BUILD, naming the pointer-chain cause — not deferred to an Encode
	// failure.
	type PastCap struct {
		F ******int32 `avro:"f"`
	}
	_, err = SchemaFor[PastCap]()
	if err == nil {
		t.Fatalf("a %d-level pointer chain (past the cap) must be refused at build", maxIndirectDepth+1)
	}
	if !strings.Contains(err.Error(), "pointer chain nests deeper") {
		t.Fatalf("build error should name the pointer-chain cause, got: %v", err)
	}

	// The cap resets at container boundaries: a slice/map whose element is an
	// at-cap pointer chain still builds (each element is unwrapped fresh).
	type Resets struct {
		A []*****int32          `avro:"a"`
		B map[string]*****int32 `avro:"b"`
	}
	if _, err := SchemaFor[Resets](); err != nil {
		t.Fatalf("per-element pointer chains under the cap must build (chain resets at container boundary): %v", err)
	}
}

func TestSchemaForNullableDefaultNull(t *testing.T) {
	type V2 struct {
		Name  string  `avro:"name"`
		Email *string `avro:"email"` // should get default null automatically
	}
	reader, err := SchemaFor[V2]()
	if err != nil {
		t.Fatal(err)
	}

	// Verify the default appears in the schema.
	var raw any
	if err := json.Unmarshal([]byte(reader.String()), &raw); err != nil {
		t.Fatal(err)
	}
	fields := raw.(map[string]any)["fields"].([]any)
	emailField := fields[1].(map[string]any)
	if emailField["default"] != nil {
		t.Fatalf("email default: got %v, want null", emailField["default"])
	}
	if _, ok := emailField["default"]; !ok {
		t.Fatal("email field should have a default key")
	}

	// Verify backward compatibility: reader has email, writer does not.
	writer, err := Parse(`{"type":"record","name":"V2","fields":[
		{"name":"name","type":"string"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}
	data, err := writer.Encode(map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	var got V2
	if _, err := resolved.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Name != "Alice" {
		t.Errorf("name: got %q, want Alice", got.Name)
	}
	if got.Email != nil {
		t.Errorf("email: got %v, want nil", got.Email)
	}
}

func TestSchemaForNullableExplicitDefault(t *testing.T) {
	// Explicit default should override the auto null default.
	type R struct {
		Value *string `avro:"value,default=hello"`
	}
	s, err := SchemaFor[R]()
	if err != nil {
		t.Fatal(err)
	}
	var raw any
	if err := json.Unmarshal([]byte(s.String()), &raw); err != nil {
		t.Fatal(err)
	}
	fields := raw.(map[string]any)["fields"].([]any)
	dflt := fields[0].(map[string]any)["default"]
	if dflt != "hello" {
		t.Fatalf("default: got %v, want \"hello\"", dflt)
	}
}

func TestSchemaForFixedTypeName(t *testing.T) {
	type MyHash [16]byte

	t.Run("named type", func(t *testing.T) {
		type R struct {
			Hash MyHash `avro:"hash"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		var raw any
		if err := json.Unmarshal([]byte(s.String()), &raw); err != nil {
			t.Fatal(err)
		}
		fields := raw.(map[string]any)["fields"].([]any)
		fixed := fields[0].(map[string]any)["type"].(map[string]any)
		if fixed["name"] != "MyHash" {
			t.Errorf("fixed name: got %v, want MyHash", fixed["name"])
		}
		if fixed["size"] != float64(16) {
			t.Errorf("fixed size: got %v, want 16", fixed["size"])
		}
	})

	t.Run("unnamed array", func(t *testing.T) {
		type R struct {
			Hash [16]byte `avro:"hash"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		var raw any
		if err := json.Unmarshal([]byte(s.String()), &raw); err != nil {
			t.Fatal(err)
		}
		fields := raw.(map[string]any)["fields"].([]any)
		fixed := fields[0].(map[string]any)["type"].(map[string]any)
		if fixed["name"] != "fixed_16" {
			t.Errorf("fixed name: got %v, want fixed_16", fixed["name"])
		}
	})

	t.Run("round trip", func(t *testing.T) {
		type R struct {
			Hash MyHash `avro:"hash"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		input := R{Hash: MyHash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}}
		data, err := s.Encode(&input)
		if err != nil {
			t.Fatal(err)
		}
		var got R
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatal(err)
		}
		if got.Hash != input.Hash {
			t.Errorf("got %v, want %v", got.Hash, input.Hash)
		}
	})
}

func TestSchemaForFixedTwoNamedTypes(t *testing.T) {
	type MD5 [16]byte
	type SHA1 [20]byte

	t.Run("different types", func(t *testing.T) {
		type R struct {
			A MD5  `avro:"a"`
			B SHA1 `avro:"b"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		var raw any
		if err := json.Unmarshal([]byte(s.String()), &raw); err != nil {
			t.Fatal(err)
		}
		fields := raw.(map[string]any)["fields"].([]any)
		a := fields[0].(map[string]any)["type"].(map[string]any)
		b := fields[1].(map[string]any)["type"].(map[string]any)
		if a["name"] != "MD5" {
			t.Errorf("first fixed name: got %v, want MD5", a["name"])
		}
		if b["name"] != "SHA1" {
			t.Errorf("second fixed name: got %v, want SHA1", b["name"])
		}

		// Round-trip.
		input := R{A: MD5{1}, B: SHA1{2}}
		data, err := s.Encode(&input)
		if err != nil {
			t.Fatal(err)
		}
		var got R
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatal(err)
		}
		if got != input {
			t.Errorf("got %+v, want %+v", got, input)
		}
	})

	t.Run("same type dedup", func(t *testing.T) {
		// Same named fixed type on two fields — second should be a
		// name reference, not a duplicate definition.
		type R struct {
			A MD5 `avro:"a"`
			B MD5 `avro:"b"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		input := R{A: MD5{1}, B: MD5{2}}
		data, err := s.Encode(&input)
		if err != nil {
			t.Fatal(err)
		}
		var got R
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatal(err)
		}
		if got != input {
			t.Errorf("got %+v, want %+v", got, input)
		}
	})

	t.Run("same type with alias dedup", func(t *testing.T) {
		// type-alias on first field, second field uses name reference.
		type R struct {
			A MD5 `avro:"a,type-alias=old_hash"`
			B MD5 `avro:"b"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		input := R{A: MD5{1}, B: MD5{2}}
		data, err := s.Encode(&input)
		if err != nil {
			t.Fatal(err)
		}
		var got R
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatal(err)
		}
		if got != input {
			t.Errorf("got %+v, want %+v", got, input)
		}
	})
}

func TestSchemaForTimestamp(t *testing.T) {
	type Event struct {
		ID        string    `avro:"id"`
		CreatedAt time.Time `avro:"created_at"`
		UpdatedAt time.Time `avro:"updated_at,timestamp-micros"`
		Birthday  time.Time `avro:"birthday,date"`
	}
	s, err := SchemaFor[Event]()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Millisecond)
	e := Event{
		ID:        "abc",
		CreatedAt: now,
		UpdatedAt: now,
		Birthday:  time.Date(2000, 1, 15, 0, 0, 0, 0, time.UTC),
	}
	data, err := s.Encode(&e)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got Event
	if _, err := s.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.CreatedAt.Equal(e.CreatedAt) {
		t.Errorf("created_at: got %v, want %v", got.CreatedAt, e.CreatedAt)
	}
}

func TestSchemaForDuration(t *testing.T) {
	type Record struct {
		Millis time.Duration `avro:"millis"`
		Micros time.Duration `avro:"micros,time-micros"`
	}
	s, err := SchemaFor[Record]()
	if err != nil {
		t.Fatal(err)
	}
	r := Record{Millis: 5 * time.Second, Micros: 5 * time.Second}
	data, err := s.Encode(&r)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got Record
	if _, err := s.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != r {
		t.Errorf("got %+v, want %+v", got, r)
	}
}

func TestSchemaForDecimal(t *testing.T) {
	type Product struct {
		Name  string  `avro:"name"`
		Price big.Rat `avro:"price,decimal(10,2)"`
	}
	s, err := SchemaFor[Product]()
	if err != nil {
		t.Fatal(err)
	}
	p := Product{Name: "Widget", Price: *new(big.Rat).SetFrac64(314, 100)}
	data, err := s.Encode(&p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got Product
	if _, err := s.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Price.Cmp(&p.Price) != 0 {
		t.Errorf("price: got %s, want %s", got.Price.RatString(), p.Price.RatString())
	}
}

func TestSchemaForDefault(t *testing.T) {
	type Record struct {
		Name  string `avro:"name,default=unknown"`
		Score int32  `avro:"score,default=42"`
	}
	s, err := SchemaFor[Record]()
	if err != nil {
		t.Fatal(err)
	}
	// Encode from map with missing fields — both defaults should apply.
	data, err := s.Encode(map[string]any{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got Record
	if _, err := s.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Name != "unknown" {
		t.Errorf("name: got %q, want %q", got.Name, "unknown")
	}
	if got.Score != 42 {
		t.Errorf("score: got %d, want 42", got.Score)
	}
}

func TestSchemaForAlias(t *testing.T) {
	type V2 struct {
		EmailAddress string `avro:"email_address,alias=email"`
		Name         string `avro:"name"`
	}
	reader, err := SchemaFor[V2]()
	if err != nil {
		t.Fatal(err)
	}
	writer, err := Parse(`{"type":"record","name":"V2","fields":[
		{"name":"email","type":"string"},
		{"name":"name","type":"string"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}
	data, err := writer.Encode(map[string]any{"email": "a@b.com", "name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	var got V2
	if _, err := resolved.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.EmailAddress != "a@b.com" {
		t.Errorf("email: got %q, want %q", got.EmailAddress, "a@b.com")
	}
}

func TestSplitTag(t *testing.T) {
	tests := []struct {
		tag  string
		want []string
	}{
		{"name", []string{"name"}},
		{"name,alias=foo", []string{"name", "alias=foo"}},
		{"name,decimal(10,2)", []string{"name", "decimal(10,2)"}},
		{"name,alias=[a,b]", []string{"name", "alias=[a,b]"}},
		{"name,alias=[a,b],uuid", []string{"name", "alias=[a,b]", "uuid"}},
		{"name,decimal(10,2),alias=[a,b],uuid", []string{"name", "decimal(10,2)", "alias=[a,b]", "uuid"}},
	}
	for _, tt := range tests {
		got, err := splitTag(tt.tag)
		if err != nil {
			t.Errorf("splitTag(%q) unexpected error: %v", tt.tag, err)
			continue
		}
		if len(got) != len(tt.want) {
			t.Errorf("splitTag(%q) = %v, want %v", tt.tag, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("splitTag(%q)[%d] = %q, want %q", tt.tag, i, got[i], tt.want[i])
			}
		}
	}

	// Unclosed and mismatched delimiters should error.
	for _, tag := range []string{
		"name,alias=[a,b",    // unclosed [
		"name,decimal(10,2",  // unclosed (
		"name,alias=[a,b)",   // [ closed by )
		"name,decimal(10,2]", // ( closed by ]
		"name,alias=[a)b]",   // ) inside [ context
	} {
		if _, err := splitTag(tag); err == nil {
			t.Errorf("splitTag(%q) expected error for bad delimiters", tag)
		}
	}
}

func TestSchemaForAliasMultiple(t *testing.T) {
	type V2 struct {
		EmailAddress string `avro:"email_address,alias=[email,e_mail]"`
		Name         string `avro:"name"`
	}
	reader, err := SchemaFor[V2]()
	if err != nil {
		t.Fatal(err)
	}
	for _, writerName := range []string{"email", "e_mail"} {
		writer, err := Parse(`{"type":"record","name":"V2","fields":[
			{"name":"` + writerName + `","type":"string"},
			{"name":"name","type":"string"}
		]}`)
		if err != nil {
			t.Fatal(err)
		}
		resolved, err := Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		data, err := writer.Encode(map[string]any{writerName: "a@b.com", "name": "Alice"})
		if err != nil {
			t.Fatal(err)
		}
		var got V2
		if _, err := resolved.Decode(data, &got); err != nil {
			t.Fatalf("decode with writer field %q: %v", writerName, err)
		}
		if got.EmailAddress != "a@b.com" {
			t.Errorf("writer field %q: got %q, want %q", writerName, got.EmailAddress, "a@b.com")
		}
	}
}

func TestSchemaForTypeAlias(t *testing.T) {
	type Inner struct {
		Value int32 `avro:"value"`
	}
	type Outer struct {
		Name  string  `avro:"name"`
		Inner Inner   `avro:"inner,type-alias=[legacy_inner,old_inner]"`
		List  []Inner `avro:"list"`
	}

	reader, err := SchemaFor[Outer]()
	if err != nil {
		t.Fatal(err)
	}

	// Verify the alias appears in the generated schema.
	var raw any
	if err := json.Unmarshal([]byte(reader.String()), &raw); err != nil {
		t.Fatal(err)
	}
	fields := raw.(map[string]any)["fields"].([]any)
	innerField := fields[1].(map[string]any)["type"].(map[string]any)
	aliases, _ := innerField["aliases"].([]any)
	if len(aliases) != 2 || aliases[0] != "legacy_inner" || aliases[1] != "old_inner" {
		t.Fatalf("inner record aliases: got %v, want [legacy_inner old_inner]", aliases)
	}

	// Verify resolution works against a writer using the old name.
	writer, err := Parse(`{"type":"record","name":"Outer","fields":[
		{"name":"name","type":"string"},
		{"name":"inner","type":{"type":"record","name":"legacy_inner","fields":[
			{"name":"value","type":"int"}
		]}},
		{"name":"list","type":{"type":"array","items":"legacy_inner"}}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}
	data, err := writer.Encode(map[string]any{
		"name":  "test",
		"inner": map[string]any{"value": int32(42)},
		"list":  []any{map[string]any{"value": int32(7)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got Outer
	if _, err := resolved.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Inner.Value != 42 {
		t.Errorf("inner.value: got %d, want 42", got.Inner.Value)
	}
	if len(got.List) != 1 || got.List[0].Value != 7 {
		t.Errorf("list: got %+v, want [{Value:7}]", got.List)
	}
}

func TestSchemaForTypeAliasNullable(t *testing.T) {
	type Inner struct {
		Value int32 `avro:"value"`
	}
	type Outer struct {
		Inner *Inner `avro:"inner,type-alias=old_inner"`
	}

	reader, err := SchemaFor[Outer]()
	if err != nil {
		t.Fatal(err)
	}

	writer, err := Parse(`{"type":"record","name":"Outer","fields":[
		{"name":"inner","type":["null",{"type":"record","name":"old_inner","fields":[
			{"name":"value","type":"int"}
		]}]}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}
	data, err := writer.Encode(map[string]any{
		"inner": map[string]any{"value": int32(99)},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got Outer
	if _, err := resolved.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Inner == nil || got.Inner.Value != 99 {
		t.Errorf("inner: got %+v, want &{Value:99}", got.Inner)
	}
}

func TestSchemaForTypeAliasMap(t *testing.T) {
	type Inner struct {
		Value int32 `avro:"value"`
	}
	type Outer struct {
		Items map[string]Inner `avro:"items,type-alias=old_inner"`
	}

	reader, err := SchemaFor[Outer]()
	if err != nil {
		t.Fatal(err)
	}

	writer, err := Parse(`{"type":"record","name":"Outer","fields":[
		{"name":"items","type":{"type":"map","values":{"type":"record","name":"old_inner","fields":[
			{"name":"value","type":"int"}
		]}}}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}
	data, err := writer.Encode(map[string]any{
		"items": map[string]any{"k": map[string]any{"value": int32(5)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got Outer
	if _, err := resolved.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Items["k"].Value != 5 {
		t.Errorf("items[k].value: got %d, want 5", got.Items["k"].Value)
	}
}

func TestSchemaForTypeAliasEnum(t *testing.T) {
	// SchemaFor can't infer enums from Go types, but CustomType with
	// Schema can produce one. Verify type-alias works on enum fields.
	type Status string

	enumNode := SchemaNode{
		Type:    "enum",
		Name:    "Status",
		Symbols: []string{"ACTIVE", "INACTIVE"},
	}
	type Outer struct {
		State Status `avro:"state,type-alias=OldStatus"`
	}
	reader, err := SchemaFor[Outer](CustomType{
		GoType: reflect.TypeFor[Status](),
		Schema: &enumNode,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify the alias appears on the enum type.
	var raw any
	if err := json.Unmarshal([]byte(reader.String()), &raw); err != nil {
		t.Fatal(err)
	}
	fields := raw.(map[string]any)["fields"].([]any)
	enumType := fields[0].(map[string]any)["type"].(map[string]any)
	if enumType["type"] != "enum" {
		t.Fatalf("expected enum type, got %v", enumType["type"])
	}
	aliases, _ := enumType["aliases"].([]any)
	if len(aliases) != 1 || aliases[0] != "OldStatus" {
		t.Fatalf("enum aliases: got %v, want [OldStatus]", aliases)
	}

	// Verify resolution against a writer using the old name.
	writer, err := Parse(`{"type":"record","name":"Outer","fields":[
		{"name":"state","type":{"type":"enum","name":"OldStatus","symbols":["ACTIVE","INACTIVE"]}}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}
	data, err := writer.Encode(map[string]any{"state": "ACTIVE"})
	if err != nil {
		t.Fatal(err)
	}
	var got Outer
	if _, err := resolved.Decode(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.State != "ACTIVE" {
		t.Errorf("state: got %q, want ACTIVE", got.State)
	}
}

// TestSchemaForTypeAliasErrors tests that type-alias is rejected on
// fields that don't reference a named type (record, enum, fixed).
func TestSchemaForTypeAliasErrors(t *testing.T) {
	t.Run("primitive int", func(t *testing.T) {
		type R struct {
			X int32 `avro:"x,type-alias=old_x"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("primitive string", func(t *testing.T) {
		type R struct {
			X string `avro:"x,type-alias=old_x"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("primitive bytes", func(t *testing.T) {
		type R struct {
			X []byte `avro:"x,type-alias=old_x"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("nullable primitive", func(t *testing.T) {
		type R struct {
			X *int32 `avro:"x,type-alias=old_x"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error: union of null+int has no named type")
		}
	})

	t.Run("slice of primitives", func(t *testing.T) {
		type R struct {
			X []int32 `avro:"x,type-alias=old_x"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error: array of int has no named type")
		}
	})

	t.Run("map of primitives", func(t *testing.T) {
		type R struct {
			X map[string]int32 `avro:"x,type-alias=old_x"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error: map of int has no named type")
		}
	})

	t.Run("nullable slice of primitives", func(t *testing.T) {
		type R struct {
			X *[]int32 `avro:"x,type-alias=old_x"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error: nullable array of int has no named type")
		}
	})
}

// TestSchemaForTypeAliasNamedRef tests that type-alias works correctly
// when the same record type appears multiple times (second occurrence
// is a named type reference string, not the full definition).
func TestSchemaForTypeAliasNamedRef(t *testing.T) {
	type Inner struct {
		Value int32 `avro:"value"`
	}

	t.Run("two fields same type identical aliases", func(t *testing.T) {
		// Identical type-alias on both fields is accepted.
		type Outer struct {
			A Inner `avro:"a,type-alias=old_inner"`
			B Inner `avro:"b,type-alias=old_inner"`
		}
		if _, err := SchemaFor[Outer](); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("two fields same type conflicting aliases", func(t *testing.T) {
		type Outer struct {
			A Inner `avro:"a,type-alias=old_inner"`
			B Inner `avro:"b,type-alias=different_inner"`
		}
		if _, err := SchemaFor[Outer](); err == nil {
			t.Fatal("expected error for conflicting type-alias")
		}
	})

	t.Run("only first field aliased", func(t *testing.T) {
		type Outer struct {
			A Inner `avro:"a,type-alias=old_inner"`
			B Inner `avro:"b"`
		}
		reader, err := SchemaFor[Outer]()
		if err != nil {
			t.Fatal(err)
		}
		writer, err := Parse(`{"type":"record","name":"Outer","fields":[
			{"name":"a","type":{"type":"record","name":"old_inner","fields":[
				{"name":"value","type":"int"}
			]}},
			{"name":"b","type":"old_inner"}
		]}`)
		if err != nil {
			t.Fatal(err)
		}
		resolved, err := Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		data, err := writer.Encode(map[string]any{
			"a": map[string]any{"value": int32(1)},
			"b": map[string]any{"value": int32(2)},
		})
		if err != nil {
			t.Fatal(err)
		}
		var got Outer
		if _, err := resolved.Decode(data, &got); err != nil {
			t.Fatal(err)
		}
		if got.A.Value != 1 || got.B.Value != 2 {
			t.Errorf("got A=%d B=%d, want A=1 B=2", got.A.Value, got.B.Value)
		}
	})

	t.Run("first not aliased second aliased errors", func(t *testing.T) {
		// The first field defines the record without an alias.
		// The second field has type-alias but gets a name reference.
		// This should error — the alias would be silently dropped.
		type Outer struct {
			A Inner `avro:"a"`
			B Inner `avro:"b,type-alias=old_inner"`
		}
		if _, err := SchemaFor[Outer](); err == nil {
			t.Fatal("expected error for type-alias on already-defined type")
		}
	})

	t.Run("array of named ref identical aliases accepted", func(t *testing.T) {
		// Identical type-alias on both fields is accepted.
		type Outer struct {
			Direct Inner   `avro:"direct,type-alias=old_inner"`
			List   []Inner `avro:"list,type-alias=old_inner"`
		}
		if _, err := SchemaFor[Outer](); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("array of named ref only first aliased", func(t *testing.T) {
		type Outer struct {
			Direct Inner   `avro:"direct,type-alias=old_inner"`
			List   []Inner `avro:"list"`
		}
		reader, err := SchemaFor[Outer]()
		if err != nil {
			t.Fatal(err)
		}
		writer, err := Parse(`{"type":"record","name":"Outer","fields":[
			{"name":"direct","type":{"type":"record","name":"old_inner","fields":[
				{"name":"value","type":"int"}
			]}},
			{"name":"list","type":{"type":"array","items":"old_inner"}}
		]}`)
		if err != nil {
			t.Fatal(err)
		}
		resolved, err := Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		data, err := writer.Encode(map[string]any{
			"direct": map[string]any{"value": int32(10)},
			"list":   []any{map[string]any{"value": int32(20)}},
		})
		if err != nil {
			t.Fatal(err)
		}
		var got Outer
		if _, err := resolved.Decode(data, &got); err != nil {
			t.Fatal(err)
		}
		if got.Direct.Value != 10 {
			t.Errorf("direct.value: got %d, want 10", got.Direct.Value)
		}
		if len(got.List) != 1 || got.List[0].Value != 20 {
			t.Errorf("list: got %+v, want [{Value:20}]", got.List)
		}
	})

	t.Run("deep nesting nullable map of array of record", func(t *testing.T) {
		type Outer struct {
			M *map[string][]Inner `avro:"m,type-alias=old_inner"`
		}
		_, err := SchemaFor[Outer]()
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("fixed type", func(t *testing.T) {
		type Outer struct {
			Hash [16]byte `avro:"hash,type-alias=old_hash"`
		}
		reader, err := SchemaFor[Outer]()
		if err != nil {
			t.Fatal(err)
		}
		// Verify the alias appears on the fixed type.
		var raw any
		if err := json.Unmarshal([]byte(reader.String()), &raw); err != nil {
			t.Fatal(err)
		}
		fields := raw.(map[string]any)["fields"].([]any)
		hashField := fields[0].(map[string]any)["type"].(map[string]any)
		if hashField["type"] != "fixed" {
			t.Fatalf("expected fixed type, got %v", hashField["type"])
		}
		aliases, _ := hashField["aliases"].([]any)
		if len(aliases) != 1 || aliases[0] != "old_hash" {
			t.Fatalf("fixed aliases: got %v, want [old_hash]", aliases)
		}
	})

	t.Run("namespaced identical aliases accepted", func(t *testing.T) {
		// A configured namespace must not change whether two fields of the
		// same named type with identical type-aliases are accepted: the
		// defining field and the referencing field identify the type by the
		// same identity (its fullname), so the dedup that recognizes
		// "identical aliases, accept" must key on that same identity in both
		// positions. (The defining field registers the type and the
		// referencing field resolves to a name reference — both are the
		// type's fullname com.example.Inner.)
		type Outer struct {
			A Inner `avro:"a,type-alias=old_inner"`
			B Inner `avro:"b,type-alias=old_inner"`
		}
		s, err := SchemaFor[Outer](WithNamespace("com.example"))
		if err != nil {
			t.Fatal(err)
		}
		// The alias must be present on the (namespaced) Inner definition.
		var raw map[string]any
		if err := json.Unmarshal([]byte(s.String()), &raw); err != nil {
			t.Fatal(err)
		}
		aField := raw["fields"].([]any)[0].(map[string]any)["type"].(map[string]any)
		aliases, _ := aField["aliases"].([]any)
		if len(aliases) != 1 || aliases[0] != "old_inner" {
			t.Fatalf("namespaced Inner aliases: got %v, want [old_inner]", aliases)
		}
	})

	t.Run("namespaced conflicting aliases rejected", func(t *testing.T) {
		// The conflict detection must still fire under a namespace.
		type Outer struct {
			A Inner `avro:"a,type-alias=old_inner"`
			B Inner `avro:"b,type-alias=different_inner"`
		}
		if _, err := SchemaFor[Outer](WithNamespace("com.example")); err == nil {
			t.Fatal("expected error for conflicting type-alias under namespace")
		}
	})
}

func TestSchemaForEmbeddedAndInline(t *testing.T) {
	type Base struct {
		ID int64 `avro:"id"`
	}
	type Addr struct {
		City string `avro:"city"`
		Zip  int32  `avro:"zip"`
	}

	t.Run("embedded", func(t *testing.T) {
		type User struct {
			Base
			Name string `avro:"name"`
		}
		s, err := SchemaFor[User]()
		if err != nil {
			t.Fatal(err)
		}
		u := User{Base: Base{ID: 123}, Name: "Alice"}
		data, err := s.Encode(&u)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got User
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got != u {
			t.Errorf("got %+v, want %+v", got, u)
		}
	})

	t.Run("inline", func(t *testing.T) {
		type User struct {
			Name    string `avro:"name"`
			Address Addr   `avro:",inline"`
		}
		s, err := SchemaFor[User]()
		if err != nil {
			t.Fatal(err)
		}
		u := User{Name: "Alice", Address: Addr{City: "Seattle", Zip: 98101}}
		data, err := s.Encode(&u)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got User
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got != u {
			t.Errorf("got %+v, want %+v", got, u)
		}
	})

	t.Run("pointer inline", func(t *testing.T) {
		type Inner struct {
			X int32 `avro:"x"`
		}
		type Outer struct {
			Name  string `avro:"name"`
			Inner *Inner `avro:",inline"`
		}
		s, err := SchemaFor[Outer]()
		if err != nil {
			t.Fatal(err)
		}
		o := Outer{Name: "test", Inner: &Inner{X: 42}}
		data, err := s.Encode(&o)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got Outer
		got.Inner = &Inner{}
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Inner.X != 42 {
			t.Errorf("got %d, want 42", got.Inner.X)
		}
	})

	t.Run("pointer embedded", func(t *testing.T) {
		type Inner struct {
			X int32 `avro:"x"`
		}
		type Outer struct {
			*Inner
			Y int32 `avro:"y"`
		}
		s, err := SchemaFor[Outer]()
		if err != nil {
			t.Fatal(err)
		}
		o := Outer{Inner: &Inner{X: 1}, Y: 2}
		data, err := s.Encode(&o)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got Outer
		got.Inner = &Inner{}
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Inner.X != 1 || got.Y != 2 {
			t.Errorf("got %+v, want x=1 y=2", got)
		}
	})

	t.Run("named embedded", func(t *testing.T) {
		type Inner struct {
			X int32 `avro:"x"`
		}
		type Outer struct {
			Inner `avro:"inner"`
			Y     int32 `avro:"y"`
		}
		s, err := SchemaFor[Outer]()
		if err != nil {
			t.Fatal(err)
		}
		o := Outer{Inner: Inner{X: 1}, Y: 2}
		data, err := s.Encode(&o)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got Outer
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got != o {
			t.Errorf("got %+v, want %+v", got, o)
		}
	})

	t.Run("ignored embedded", func(t *testing.T) {
		type Inner struct {
			X int32 `avro:"x"`
		}
		type Outer struct {
			Inner `avro:"-"`
			Y     int32 `avro:"y"`
		}
		s, err := SchemaFor[Outer]()
		if err != nil {
			t.Fatal(err)
		}
		data, err := s.Encode(&Outer{Y: 42})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got Outer
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Y != 42 {
			t.Errorf("got %d, want 42", got.Y)
		}
	})
}

func TestSchemaForNestedRecord(t *testing.T) {
	type Address struct {
		City string `avro:"city"`
		Zip  int32  `avro:"zip"`
	}
	type User struct {
		Name    string  `avro:"name"`
		Address Address `avro:"address"`
	}
	s, err := SchemaFor[User]()
	if err != nil {
		t.Fatal(err)
	}
	u := User{Name: "Alice", Address: Address{City: "Seattle", Zip: 98101}}
	data, err := s.Encode(&u)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got User
	if _, err := s.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != u {
		t.Errorf("got %+v, want %+v", got, u)
	}
}

func TestSchemaForDeepNesting(t *testing.T) {
	type Street struct {
		Name   string `avro:"name"`
		Number int32  `avro:"number"`
	}
	type Address struct {
		City   string `avro:"city"`
		Street Street `avro:"street"`
	}
	type User struct {
		Name    string  `avro:"name"`
		Address Address `avro:"address"`
	}
	s, err := SchemaFor[User]()
	if err != nil {
		t.Fatal(err)
	}
	u := User{Name: "Alice", Address: Address{City: "Seattle", Street: Street{Name: "Main St", Number: 42}}}
	data, err := s.Encode(&u)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got User
	if _, err := s.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != u {
		t.Errorf("got %+v, want %+v", got, u)
	}
}

func TestSchemaForDuplicateNestedType(t *testing.T) {
	type Address struct {
		City string `avro:"city"`
	}
	type User struct {
		Name string  `avro:"name"`
		Home Address `avro:"home"`
		Work Address `avro:"work"`
	}
	s, err := SchemaFor[User]()
	if err != nil {
		t.Fatal(err)
	}
	u := User{Name: "Alice", Home: Address{City: "Seattle"}, Work: Address{City: "Portland"}}
	data, err := s.Encode(&u)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got User
	if _, err := s.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != u {
		t.Errorf("got %+v, want %+v", got, u)
	}
}

func TestSchemaForFourLevelWithReuse(t *testing.T) {
	// Level 2 type reused at level 4.
	type L2 struct {
		V int32 `avro:"v"`
	}
	type L3 struct {
		Inner L2 `avro:"inner"`
	}
	type L4 struct {
		Deep  L3 `avro:"deep"`
		Reuse L2 `avro:"reuse"` // same type as L3.Inner
	}
	type L1 struct {
		Name string `avro:"name"`
		Sub  L4     `avro:"sub"`
	}
	s, err := SchemaFor[L1]()
	if err != nil {
		t.Fatal(err)
	}
	v := L1{
		Name: "test",
		Sub: L4{
			Deep:  L3{Inner: L2{V: 1}},
			Reuse: L2{V: 2},
		},
	}
	data, err := s.Encode(&v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got L1
	if _, err := s.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != v {
		t.Errorf("got %+v, want %+v", got, v)
	}
}

func TestSchemaForEmptyStruct(t *testing.T) {
	type Empty struct{}
	s, err := SchemaFor[Empty]()
	if err != nil {
		t.Fatal(err)
	}
	data, err := s.Encode(&Empty{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got Empty
	if _, err := s.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
}

func TestSchemaForCollections(t *testing.T) {
	t.Run("array and map", func(t *testing.T) {
		type Record struct {
			Tags     []string          `avro:"tags"`
			Metadata map[string]string `avro:"metadata"`
		}
		s, err := SchemaFor[Record]()
		if err != nil {
			t.Fatal(err)
		}
		r := Record{
			Tags:     []string{"go", "avro"},
			Metadata: map[string]string{"env": "prod"},
		}
		data, err := s.Encode(&r)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got Record
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !reflect.DeepEqual(got, r) {
			t.Errorf("got %+v, want %+v", got, r)
		}
	})

	t.Run("slice of structs", func(t *testing.T) {
		type Item struct {
			ID int32 `avro:"id"`
		}
		type Record struct {
			Items []Item `avro:"items"`
		}
		s, err := SchemaFor[Record]()
		if err != nil {
			t.Fatal(err)
		}
		r := Record{Items: []Item{{ID: 1}, {ID: 2}}}
		data, err := s.Encode(&r)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got Record
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !reflect.DeepEqual(got, r) {
			t.Errorf("got %+v, want %+v", got, r)
		}
	})

	t.Run("bytes", func(t *testing.T) {
		type Record struct {
			Data []byte `avro:"data"`
		}
		s, err := SchemaFor[Record]()
		if err != nil {
			t.Fatal(err)
		}
		r := Record{Data: []byte{1, 2, 3}}
		data, err := s.Encode(&r)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got Record
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !reflect.DeepEqual(got.Data, r.Data) {
			t.Errorf("got %v, want %v", got.Data, r.Data)
		}
	})

	t.Run("fixed array", func(t *testing.T) {
		type Record struct {
			A [3]int32 `avro:"a"`
		}
		s, err := SchemaFor[Record]()
		if err != nil {
			t.Fatal(err)
		}
		r := Record{A: [3]int32{1, 2, 3}}
		data, err := s.Encode(&r)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got Record
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got != r {
			t.Errorf("got %+v, want %+v", got, r)
		}
	})

	t.Run("fixed byte array", func(t *testing.T) {
		type Record struct {
			Hash [32]byte `avro:"hash"`
		}
		s, err := SchemaFor[Record]()
		if err != nil {
			t.Fatal(err)
		}
		r := Record{Hash: [32]byte{1, 2, 3}}
		data, err := s.Encode(&r)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got Record
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got != r {
			t.Errorf("got %v, want %v", got, r)
		}
	})
}

func TestSchemaForAllPrimitives(t *testing.T) {
	type Prims struct {
		B   bool    `avro:"b"`
		I8  int8    `avro:"i8"`
		I16 int16   `avro:"i16"`
		I32 int32   `avro:"i32"`
		I64 int64   `avro:"i64"`
		I   int     `avro:"i"`
		U8  uint8   `avro:"u8"`
		U16 uint16  `avro:"u16"`
		U32 uint32  `avro:"u32"`
		F32 float32 `avro:"f32"`
		F64 float64 `avro:"f64"`
		S   string  `avro:"s"`
	}
	s, err := SchemaFor[Prims]()
	if err != nil {
		t.Fatal(err)
	}
	p := Prims{B: true, I8: 1, I16: 2, I32: 3, I64: 4, I: 5, U8: 6, U16: 7, U32: 8, F32: 1.5, F64: 3.14, S: "hello"}
	data, err := s.Encode(&p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got Prims
	if _, err := s.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != p {
		t.Errorf("got %+v, want %+v", got, p)
	}
}

func TestSchemaForUUID(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		type Record struct {
			ID string `avro:"id,uuid"`
		}
		s, err := SchemaFor[Record]()
		if err != nil {
			t.Fatal(err)
		}
		r := Record{ID: "550e8400-e29b-41d4-a716-446655440000"}
		data, err := s.Encode(&r)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got Record
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.ID != r.ID {
			t.Errorf("got %q, want %q", got.ID, r.ID)
		}
	})

	t.Run("fixed16", func(t *testing.T) {
		type Record struct {
			ID [16]byte `avro:"id,uuid"`
		}
		s, err := SchemaFor[Record]()
		if err != nil {
			t.Fatal(err)
		}
		r := Record{ID: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}}
		data, err := s.Encode(&r)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got Record
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got != r {
			t.Errorf("got %v, want %v", got, r)
		}
	})
}

// TestSchemaForInlineRejectsOtherOptions locks the rule that the inline
// directive removes the field at this position (the embedded struct's
// fields are flattened into the parent). With no field at the inline
// position, options that apply to a field (default=, alias=, type-alias=,
// omitzero, logical-type tags) have no target — silently dropping them
// would hide user typos and produce a schema that doesn't reflect the
// user's tag. Reject any non-"inline" option (and any explicit name) on
// an inline tag at SchemaFor time.
func TestSchemaForInlineRejectsOtherOptions(t *testing.T) {
	cases := []struct {
		name string
		fn   func() (*Schema, error)
	}{
		{"inline + default=", func() (*Schema, error) {
			type Embed struct {
				A int32 `avro:"a"`
			}
			type Outer struct {
				Embed `avro:",inline,default=foo"`
			}
			return SchemaFor[Outer]()
		}},
		{"inline + alias=", func() (*Schema, error) {
			type Embed struct {
				A int32 `avro:"a"`
			}
			type Outer struct {
				Embed `avro:",inline,alias=old"`
			}
			return SchemaFor[Outer]()
		}},
		{"inline + type-alias=", func() (*Schema, error) {
			type Embed struct {
				A int32 `avro:"a"`
			}
			type Outer struct {
				Embed `avro:",inline,type-alias=old"`
			}
			return SchemaFor[Outer]()
		}},
		{"inline + omitzero", func() (*Schema, error) {
			type Embed struct {
				A int32 `avro:"a"`
			}
			type Outer struct {
				Embed `avro:",inline,omitzero"`
			}
			return SchemaFor[Outer]()
		}},
		{"inline + date", func() (*Schema, error) {
			type Embed struct {
				A int32 `avro:"a"`
			}
			type Outer struct {
				Embed `avro:",inline,date"`
			}
			return SchemaFor[Outer]()
		}},
		{"inline + uuid", func() (*Schema, error) {
			type Embed struct {
				A int32 `avro:"a"`
			}
			type Outer struct {
				Embed `avro:",inline,uuid"`
			}
			return SchemaFor[Outer]()
		}},
		{"inline + timestamp-millis", func() (*Schema, error) {
			type Embed struct {
				A int32 `avro:"a"`
			}
			type Outer struct {
				Embed `avro:",inline,timestamp-millis"`
			}
			return SchemaFor[Outer]()
		}},
		{"inline + decimal(10,2)", func() (*Schema, error) {
			type Embed struct {
				A int32 `avro:"a"`
			}
			type Outer struct {
				Embed `avro:",inline,decimal(10,2)"`
			}
			return SchemaFor[Outer]()
		}},
		{"explicit name + inline", func() (*Schema, error) {
			type Embed struct {
				A int32 `avro:"a"`
			}
			type Outer struct {
				Embed `avro:"Name,inline"`
			}
			return SchemaFor[Outer]()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.fn()
			if err == nil {
				t.Errorf("expected error for %s; SchemaFor should reject", tc.name)
			}
		})
	}

	// Positive control: plain inline (no other options) still works.
	t.Run("plain inline still accepted", func(t *testing.T) {
		type Embed struct {
			A int32 `avro:"a"`
		}
		type Outer struct {
			Embed `avro:",inline"`
			B     string `avro:"b"`
		}
		s, err := SchemaFor[Outer]()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got := s.String()
		// Flattened: should have both a and b at the top level.
		if !strings.Contains(got, `"name":"a"`) || !strings.Contains(got, `"name":"b"`) {
			t.Errorf("expected flattened a + b fields; got %s", got)
		}
	})
}

// InlineScalarAlias is a named non-struct type used by
// TestSchemaForInlineRejectsNonStructFieldType to exercise the
// anonymous-embed-of-named-scalar shape. Must live at package scope
// because Go field names for anonymous embeds come from the type name
// and the embed has to be exported (start with an uppercase letter)
// to reach the regular field-handling code path.
type InlineScalarAlias string

// TestSchemaForInlineRejectsNonStructFieldType locks the rule that the
// inline directive requires a struct (or pointer-to-struct) field type.
// Inline flattens an embedded struct's fields into the parent — on a
// non-struct field there is no struct to flatten, so the user's tag has
// no defensible meaning and the prior silent-drop produced a schema in
// which the field simply disappeared. The rejection rationale mirrors
// the sibling "inline is incompatible with X" errors: inline has nothing
// to apply itself to. Covers Go scalar, slice, map, pointer-to-scalar,
// and anonymous embed of a named non-struct exported type.
func TestSchemaForInlineRejectsNonStructFieldType(t *testing.T) {
	cases := []struct {
		name string
		fn   func() (*Schema, error)
	}{
		{"string field + ,inline", func() (*Schema, error) {
			type R struct {
				Foo string `avro:",inline"`
				Bar int32  `avro:"bar"`
			}
			return SchemaFor[R]()
		}},
		{"int32 field + ,inline", func() (*Schema, error) {
			type R struct {
				Foo int32 `avro:",inline"`
				Bar int32 `avro:"bar"`
			}
			return SchemaFor[R]()
		}},
		{"slice field + ,inline", func() (*Schema, error) {
			type R struct {
				Foo []int32 `avro:",inline"`
				Bar int32   `avro:"bar"`
			}
			return SchemaFor[R]()
		}},
		{"map field + ,inline", func() (*Schema, error) {
			type R struct {
				Foo map[string]int32 `avro:",inline"`
				Bar int32            `avro:"bar"`
			}
			return SchemaFor[R]()
		}},
		{"*string field + ,inline", func() (*Schema, error) {
			type R struct {
				Foo *string `avro:",inline"`
				Bar int32   `avro:"bar"`
			}
			return SchemaFor[R]()
		}},
		{"anon embed named-scalar + ,inline", func() (*Schema, error) {
			type R struct {
				InlineScalarAlias `avro:",inline"`
				Bar               int32 `avro:"bar"`
			}
			return SchemaFor[R]()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.fn()
			if err == nil {
				t.Errorf("expected error for %s; SchemaFor should reject", tc.name)
			}
		})
	}

	// Positive controls: ,inline on a struct and on a pointer-to-struct
	// still flattens the embed's fields into the parent.
	t.Run("struct field + ,inline still flattens", func(t *testing.T) {
		type Embed struct {
			A int32 `avro:"a"`
		}
		type R struct {
			Foo Embed `avro:",inline"`
			Bar int32 `avro:"bar"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got := s.String()
		if !strings.Contains(got, `"name":"a"`) || !strings.Contains(got, `"name":"bar"`) {
			t.Errorf("expected flattened a + bar fields; got %s", got)
		}
	})
	t.Run("*struct field + ,inline still flattens", func(t *testing.T) {
		type Embed struct {
			A int32 `avro:"a"`
		}
		type R struct {
			Foo *Embed `avro:",inline"`
			Bar int32  `avro:"bar"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got := s.String()
		if !strings.Contains(got, `"name":"a"`) || !strings.Contains(got, `"name":"bar"`) {
			t.Errorf("expected flattened a + bar fields; got %s", got)
		}
	})
}

// TestSchemaForTimeTypesRejectNonTimeLogicals locks the rule that
// time.Time and time.Duration accept only time/date logical-type
// tags (date, time-millis, time-micros, timestamp-*, local-timestamp-*).
// Non-time logicals (uuid, decimal) on time types previously produced
// a schema declaring a wire/logical combination that isn't valid Avro
// (e.g. {type:long, logicalType:uuid}) and would be soft-dropped at
// Parse, losing the user's tag. Reject at SchemaFor time.
func TestSchemaForTimeTypesRejectNonTimeLogicals(t *testing.T) {
	cases := []struct {
		name string
		fn   func() (*Schema, error)
	}{
		{"time.Time + uuid", func() (*Schema, error) {
			type R struct {
				T time.Time `avro:"t,uuid"`
			}
			return SchemaFor[R]()
		}},
		{"time.Time + decimal", func() (*Schema, error) {
			type R struct {
				T time.Time `avro:"t,decimal(10,2)"`
			}
			return SchemaFor[R]()
		}},
		{"time.Duration + uuid", func() (*Schema, error) {
			type R struct {
				T time.Duration `avro:"t,uuid"`
			}
			return SchemaFor[R]()
		}},
		{"time.Duration + decimal", func() (*Schema, error) {
			type R struct {
				T time.Duration `avro:"t,decimal(10,2)"`
			}
			return SchemaFor[R]()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.fn()
			if err == nil {
				t.Errorf("expected error for %s; SchemaFor should reject", tc.name)
			}
		})
	}

	// Positive control: time-related logicals still work.
	t.Run("time.Time + date still accepted", func(t *testing.T) {
		type R struct {
			T time.Time `avro:"t,date"`
		}
		_, err := SchemaFor[R]()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
	t.Run("time.Duration + timestamp-millis still accepted", func(t *testing.T) {
		type R struct {
			T time.Duration `avro:"t,timestamp-millis"`
		}
		_, err := SchemaFor[R]()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

// TestSchemaForDecimalRejectsNonBigRat locks the rule that the decimal
// logical type requires either *big.Rat or big.Rat. Other Go types (int,
// string, []byte, etc.) carrying the ",decimal(p,s)" tag are rejected at
// SchemaFor time. Prior behavior silently dropped the decimal tag,
// producing a schema that didn't reflect the user's intent.
func TestSchemaForDecimalRejectsNonBigRat(t *testing.T) {
	cases := []struct {
		name string
		fn   func() (*Schema, error)
	}{
		{"int32", func() (*Schema, error) {
			type R struct {
				X int32 `avro:"x,decimal(10,2)"`
			}
			return SchemaFor[R]()
		}},
		{"int64", func() (*Schema, error) {
			type R struct {
				X int64 `avro:"x,decimal(10,2)"`
			}
			return SchemaFor[R]()
		}},
		{"float64", func() (*Schema, error) {
			type R struct {
				X float64 `avro:"x,decimal(10,2)"`
			}
			return SchemaFor[R]()
		}},
		{"string", func() (*Schema, error) {
			type R struct {
				X string `avro:"x,decimal(10,2)"`
			}
			return SchemaFor[R]()
		}},
		{"[]byte", func() (*Schema, error) {
			type R struct {
				X []byte `avro:"x,decimal(10,2)"`
			}
			return SchemaFor[R]()
		}},
		{"[16]byte", func() (*Schema, error) {
			type R struct {
				X [16]byte `avro:"x,decimal(10,2)"`
			}
			return SchemaFor[R]()
		}},
		{"bool", func() (*Schema, error) {
			type R struct {
				X bool `avro:"x,decimal(10,2)"`
			}
			return SchemaFor[R]()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.fn()
			if err == nil {
				t.Errorf("expected error for decimal on %s; SchemaFor should reject", tc.name)
			}
		})
	}
}

// TestSchemaForLogicalOnNumericKind locks the rule that integer-wire
// logical types (date, time-millis, time-micros, timestamp-*,
// local-timestamp-*) attached to a plain Go integer field produce a
// schema carrying the logicalType annotation when the Go field's
// natural Avro wire type matches the logical's required wire type.
// Mismatched Go kinds (e.g., date on int64 — date requires int wire
// but int64 naturally maps to long) are rejected at SchemaFor time
// rather than silently dropping the user's logical-type tag.
//
// Acceptance and rejection both round-trip end-to-end for the
// accepted shape: encode + decode against the inferred schema.
func TestSchemaForLogicalOnNumericKind(t *testing.T) {
	intWireAccepted := []struct {
		name     string
		logical  string
		schemaFn func() (*Schema, error)
	}{
		{"date on int32", "date", func() (*Schema, error) {
			type R struct {
				D int32 `avro:"d,date"`
			}
			return SchemaFor[R]()
		}},
		{"date on int8", "date", func() (*Schema, error) {
			type R struct {
				D int8 `avro:"d,date"`
			}
			return SchemaFor[R]()
		}},
		{"time-millis on int16", "time-millis", func() (*Schema, error) {
			type R struct {
				T int16 `avro:"t,time-millis"`
			}
			return SchemaFor[R]()
		}},
		{"time-millis on uint16", "time-millis", func() (*Schema, error) {
			type R struct {
				T uint16 `avro:"t,time-millis"`
			}
			return SchemaFor[R]()
		}},
	}
	for _, tc := range intWireAccepted {
		t.Run(tc.name+" accepted (int wire)", func(t *testing.T) {
			s, err := tc.schemaFn()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got := s.String()
			if !strings.Contains(got, `"logicalType":"`+tc.logical+`"`) {
				t.Errorf("schema missing logicalType:%s; got %s", tc.logical, got)
			}
			if !strings.Contains(got, `"type":"int"`) {
				t.Errorf("schema missing int wire type; got %s", got)
			}
		})
	}

	longWireAccepted := []struct {
		name     string
		logical  string
		schemaFn func() (*Schema, error)
	}{
		{"timestamp-millis on int64", "timestamp-millis", func() (*Schema, error) {
			type R struct {
				T int64 `avro:"t,timestamp-millis"`
			}
			return SchemaFor[R]()
		}},
		{"timestamp-micros on int", "timestamp-micros", func() (*Schema, error) {
			type R struct {
				T int `avro:"t,timestamp-micros"`
			}
			return SchemaFor[R]()
		}},
		{"timestamp-nanos on uint64", "timestamp-nanos", func() (*Schema, error) {
			type R struct {
				T uint64 `avro:"t,timestamp-nanos"`
			}
			return SchemaFor[R]()
		}},
		{"time-micros on uint32", "time-micros", func() (*Schema, error) {
			type R struct {
				T uint32 `avro:"t,time-micros"`
			}
			return SchemaFor[R]()
		}},
		{"local-timestamp-millis on int64", "local-timestamp-millis", func() (*Schema, error) {
			type R struct {
				T int64 `avro:"t,local-timestamp-millis"`
			}
			return SchemaFor[R]()
		}},
	}
	for _, tc := range longWireAccepted {
		t.Run(tc.name+" accepted (long wire)", func(t *testing.T) {
			s, err := tc.schemaFn()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got := s.String()
			if !strings.Contains(got, `"logicalType":"`+tc.logical+`"`) {
				t.Errorf("schema missing logicalType:%s; got %s", tc.logical, got)
			}
			if !strings.Contains(got, `"type":"long"`) {
				t.Errorf("schema missing long wire type; got %s", got)
			}
		})
	}

	rejected := []struct {
		name string
		fn   func() (*Schema, error)
	}{
		{"date on int64 (long-wired Go type)", func() (*Schema, error) {
			type R struct {
				D int64 `avro:"d,date"`
			}
			return SchemaFor[R]()
		}},
		{"date on int (long-wired Go type)", func() (*Schema, error) {
			type R struct {
				D int `avro:"d,date"`
			}
			return SchemaFor[R]()
		}},
		{"date on uint32 (long-wired Go type)", func() (*Schema, error) {
			type R struct {
				D uint32 `avro:"d,date"`
			}
			return SchemaFor[R]()
		}},
		{"timestamp-millis on int32 (int-wired Go type)", func() (*Schema, error) {
			type R struct {
				T int32 `avro:"t,timestamp-millis"`
			}
			return SchemaFor[R]()
		}},
		{"timestamp-micros on int16 (int-wired Go type)", func() (*Schema, error) {
			type R struct {
				T int16 `avro:"t,timestamp-micros"`
			}
			return SchemaFor[R]()
		}},
		{"date on string", func() (*Schema, error) {
			type R struct {
				D string `avro:"d,date"`
			}
			return SchemaFor[R]()
		}},
		{"date on float32", func() (*Schema, error) {
			type R struct {
				D float32 `avro:"d,date"`
			}
			return SchemaFor[R]()
		}},
		{"date on bool", func() (*Schema, error) {
			type R struct {
				D bool `avro:"d,date"`
			}
			return SchemaFor[R]()
		}},
		{"timestamp-millis on float64", func() (*Schema, error) {
			type R struct {
				T float64 `avro:"t,timestamp-millis"`
			}
			return SchemaFor[R]()
		}},
	}
	for _, tc := range rejected {
		t.Run(tc.name+" rejected", func(t *testing.T) {
			_, err := tc.fn()
			if err == nil {
				t.Errorf("expected error for %s; SchemaFor should reject", tc.name)
			}
		})
	}

	t.Run("date on int32 round-trip", func(t *testing.T) {
		type R struct {
			D int32 `avro:"d,date"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
		enc, err := s.Encode(&R{D: 19723})
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		var got R
		if _, err := s.Decode(enc, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got.D != 19723 {
			t.Errorf("got %d, want 19723", got.D)
		}
	})

	t.Run("timestamp-millis on int64 round-trip", func(t *testing.T) {
		type R struct {
			T int64 `avro:"t,timestamp-millis"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
		enc, err := s.Encode(&R{T: 1700000000000})
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		var got R
		if _, err := s.Decode(enc, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got.T != 1700000000000 {
			t.Errorf("got %d, want 1700000000000", got.T)
		}
	})
}

// TestSchemaForUUIDRejectsUnsupportedKind locks the rule that the uuid
// logical type requires either Go string, [16]byte, or a type that
// implements TextMarshaler / TextUnmarshaler / TextAppender. Other Go
// kinds would produce a schema that declares string (or fixed of a
// non-16 size) while the Go field is something else — a schema that
// lies about the field type and causes Encode to fail at runtime far
// from the SchemaFor call.
func TestSchemaForUUIDRejectsUnsupportedKind(t *testing.T) {
	cases := []struct {
		name string
		fn   func() (*Schema, error)
	}{
		{"int32", func() (*Schema, error) {
			type R struct {
				U int32 `avro:"u,uuid"`
			}
			return SchemaFor[R]()
		}},
		{"int64", func() (*Schema, error) {
			type R struct {
				U int64 `avro:"u,uuid"`
			}
			return SchemaFor[R]()
		}},
		{"uint32", func() (*Schema, error) {
			type R struct {
				U uint32 `avro:"u,uuid"`
			}
			return SchemaFor[R]()
		}},
		{"float64", func() (*Schema, error) {
			type R struct {
				U float64 `avro:"u,uuid"`
			}
			return SchemaFor[R]()
		}},
		{"bool", func() (*Schema, error) {
			type R struct {
				U bool `avro:"u,uuid"`
			}
			return SchemaFor[R]()
		}},
		{"[]byte (slice)", func() (*Schema, error) {
			type R struct {
				U []byte `avro:"u,uuid"`
			}
			return SchemaFor[R]()
		}},
		{"[32]byte (wrong size)", func() (*Schema, error) {
			type R struct {
				U [32]byte `avro:"u,uuid"`
			}
			return SchemaFor[R]()
		}},
		{"plain struct (no text marshaler)", func() (*Schema, error) {
			type Inner struct{ X int32 }
			type R struct {
				U Inner `avro:"u,uuid"`
			}
			return SchemaFor[R]()
		}},
		{"map", func() (*Schema, error) {
			type R struct {
				U map[string]int32 `avro:"u,uuid"`
			}
			return SchemaFor[R]()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.fn()
			if err == nil {
				t.Errorf("expected error for uuid logical on %s; SchemaFor should reject", tc.name)
			}
		})
	}
}

func TestSchemaForIgnored(t *testing.T) {
	type Record struct {
		Name    string `avro:"name"`
		Ignored int    `avro:"-"`
	}
	s, err := SchemaFor[Record]()
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	json.Unmarshal([]byte(s.Canonical()), &m)
	fields := m["fields"].([]any)
	if len(fields) != 1 {
		t.Errorf("expected 1 field, got %d", len(fields))
	}
}

type recNode struct {
	Value int      `avro:"value"`
	Next  *recNode `avro:"next"`
}

type recTree struct {
	Value    int        `avro:"value"`
	Children []*recTree `avro:"children"`
}

type recMap struct {
	Name     string             `avro:"name"`
	Branches map[string]*recMap `avro:"branches"`
}

type recMutualA struct {
	AVal int         `avro:"a_val"`
	B    *recMutualB `avro:"b"`
}

type recMutualB struct {
	BVal int         `avro:"b_val"`
	A    *recMutualA `avro:"a"`
}

func TestSchemaForRecursive(t *testing.T) {
	t.Run("linked list", func(t *testing.T) {
		s, err := SchemaFor[recNode]()
		if err != nil {
			t.Fatal(err)
		}
		// Round-trip encode/decode a 3-node list.
		head := &recNode{Value: 1, Next: &recNode{Value: 2, Next: &recNode{Value: 3}}}
		data, err := s.Encode(head)
		if err != nil {
			t.Fatal(err)
		}
		var got recNode
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatal(err)
		}
		if got.Value != 1 || got.Next == nil || got.Next.Value != 2 || got.Next.Next == nil || got.Next.Next.Value != 3 || got.Next.Next.Next != nil {
			t.Errorf("decoded list mismatch: %+v", got)
		}
	})

	t.Run("with namespace", func(t *testing.T) {
		s, err := SchemaFor[recNode](WithNamespace("com.example"))
		if err != nil {
			t.Fatal(err)
		}
		// Ensure the namespace is applied to the root and the recursive
		// reference resolves to the namespaced name.
		head := &recNode{Value: 42}
		if _, err := s.Encode(head); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("tree via slice", func(t *testing.T) {
		s, err := SchemaFor[recTree]()
		if err != nil {
			t.Fatal(err)
		}
		root := &recTree{Value: 1, Children: []*recTree{{Value: 2}, {Value: 3, Children: []*recTree{{Value: 4}}}}}
		data, err := s.Encode(root)
		if err != nil {
			t.Fatal(err)
		}
		var got recTree
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatal(err)
		}
		if got.Value != 1 || len(got.Children) != 2 || got.Children[1].Children[0].Value != 4 {
			t.Errorf("decoded tree mismatch: %+v", got)
		}
	})

	t.Run("recursion via map", func(t *testing.T) {
		s, err := SchemaFor[recMap]()
		if err != nil {
			t.Fatal(err)
		}
		root := &recMap{Name: "root", Branches: map[string]*recMap{"a": {Name: "leaf"}}}
		data, err := s.Encode(root)
		if err != nil {
			t.Fatal(err)
		}
		var got recMap
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatal(err)
		}
		if got.Name != "root" || got.Branches["a"].Name != "leaf" {
			t.Errorf("decoded map mismatch: %+v", got)
		}
	})

	t.Run("mutual recursion", func(t *testing.T) {
		s, err := SchemaFor[recMutualA]()
		if err != nil {
			t.Fatal(err)
		}
		in := &recMutualA{AVal: 1, B: &recMutualB{BVal: 2, A: &recMutualA{AVal: 3}}}
		data, err := s.Encode(in)
		if err != nil {
			t.Fatal(err)
		}
		var got recMutualA
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatal(err)
		}
		if got.AVal != 1 || got.B.BVal != 2 || got.B.A.AVal != 3 {
			t.Errorf("mutual recursion mismatch: %+v", got)
		}
	})
}

func TestSchemaForMust(t *testing.T) {
	type Simple struct {
		X int32 `avro:"x"`
	}
	s := MustSchemaFor[Simple]()
	data, _ := s.Encode(&Simple{X: 1})
	var got Simple
	s.Decode(data, &got)
	if got.X != 1 {
		t.Errorf("got %d, want 1", got.X)
	}
}

func TestSchemaForMustPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	MustSchemaFor[string]()
}

func TestSchemaForWithName(t *testing.T) {
	type UserV2 struct {
		Name string `avro:"name"`
	}
	s, err := SchemaFor[UserV2](WithNamespace("com.example"), WithName("User"))
	if err != nil {
		t.Fatal(err)
	}
	// The schema should be compatible with a writer using the name "User".
	writer, err := Parse(`{"type":"record","name":"User","namespace":"com.example","fields":[{"name":"name","type":"string"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckCompatibility(writer, s); err != nil {
		t.Fatalf("schemas should be compatible: %v", err)
	}
}

func TestSchemaForFieldConflict(t *testing.T) {
	type Base struct {
		Name string // untagged, inlined at depth 1
	}
	type User struct {
		Base
		FullName string `avro:"Name"` // tagged as "Name", depth 0
	}
	s, err := SchemaFor[User]()
	if err != nil {
		t.Fatal(err)
	}
	u := User{FullName: "direct"}
	data, err := s.Encode(&u)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got User
	if _, err := s.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.FullName != "direct" {
		t.Errorf("got %q, want %q", got.FullName, "direct")
	}
}

// Types used for unexported field/embed tests. These must be package-level
// because unexported fields are only meaningful within the declaring package.

type unexportedInt int

type unexportedEmbedStruct struct {
	unexportedInt
	Name string `avro:"name"`
}

type unexportedFieldStruct struct {
	Name     string `avro:"name"`
	internal int
}

type embeddedBadTag struct {
	X int32 `avro:"x,bogus"`
}

type namedEmbeddedBadTag struct {
	X int32 `avro:"x,bogus"`
}

func TestSchemaForUnexportedFields(t *testing.T) {
	t.Run("unexported field", func(t *testing.T) {
		s, err := SchemaFor[unexportedFieldStruct]()
		if err != nil {
			t.Fatal(err)
		}
		data, _ := s.Encode(&unexportedFieldStruct{Name: "test"})
		var got unexportedFieldStruct
		s.Decode(data, &got)
		if got.Name != "test" {
			t.Errorf("got %q, want %q", got.Name, "test")
		}
	})

	t.Run("unexported embed", func(t *testing.T) {
		s, err := SchemaFor[unexportedEmbedStruct]()
		if err != nil {
			t.Fatal(err)
		}
		data, _ := s.Encode(&unexportedEmbedStruct{Name: "test"})
		var got unexportedEmbedStruct
		s.Decode(data, &got)
		if got.Name != "test" {
			t.Errorf("got %q, want %q", got.Name, "test")
		}
	})
}

func TestSchemaForErrors(t *testing.T) {
	t.Run("non-struct", func(t *testing.T) {
		if _, err := SchemaFor[string](); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("decimal requires tag", func(t *testing.T) {
		type R struct {
			Price big.Rat `avro:"price"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("bad decimal tag", func(t *testing.T) {
		type R struct {
			Price *big.Rat `avro:"price,decimal(bad)"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error")
		}
	})

	// A decimal(...) tag must contain exactly two integers and nothing
	// else. Trailing content after the scale (a third argument, junk
	// characters, an exponent) was silently discarded, producing a
	// decimal(precision,scale) schema that does not reflect what the user
	// wrote — inconsistent with decimal()/decimal(9)/decimal(9,)/decimal(bad),
	// which all error. Each of these must be rejected, not truncated.
	t.Run("decimal tag rejects trailing content", func(t *testing.T) {
		t.Run("three args", func(t *testing.T) {
			type R struct {
				Price *big.Rat `avro:"price,decimal(9,2,3)"`
			}
			if _, err := SchemaFor[R](); err == nil {
				t.Fatal("expected error for decimal(9,2,3)")
			}
		})
		t.Run("trailing junk", func(t *testing.T) {
			type R struct {
				Price *big.Rat `avro:"price,decimal(9,2x)"`
			}
			if _, err := SchemaFor[R](); err == nil {
				t.Fatal("expected error for decimal(9,2x)")
			}
		})
		t.Run("exponent scale", func(t *testing.T) {
			type R struct {
				Price *big.Rat `avro:"price,decimal(9,2e1)"`
			}
			if _, err := SchemaFor[R](); err == nil {
				t.Fatal("expected error for decimal(9,2e1)")
			}
		})
		// Boundary: the well-formed two-integer form must still parse.
		t.Run("well formed still accepted", func(t *testing.T) {
			type R struct {
				Price *big.Rat `avro:"price,decimal(9,2)"`
			}
			if _, err := SchemaFor[R](); err != nil {
				t.Fatalf("decimal(9,2) should parse: %v", err)
			}
		})
	})

	t.Run("unknown tag option", func(t *testing.T) {
		type R struct {
			X int32 `avro:"x,bogus"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("unsupported type", func(t *testing.T) {
		type R struct {
			C chan int `avro:"c"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("unsupported in slice", func(t *testing.T) {
		type R struct {
			C []chan int `avro:"c"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("unsupported in map", func(t *testing.T) {
		type R struct {
			M map[string]chan int `avro:"m"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("unsupported in array", func(t *testing.T) {
		type R struct {
			A [3]chan int `avro:"a"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("non-string map key", func(t *testing.T) {
		type R struct {
			M map[int]string `avro:"m"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("embedded bad tag", func(t *testing.T) {
		type R struct {
			embeddedBadTag
			Y int32 `avro:"y"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("named embedded bad tag", func(t *testing.T) {
		type R struct {
			namedEmbeddedBadTag `avro:"inner,bogus"`
			Y                   int32 `avro:"y"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("inline error", func(t *testing.T) {
		type Bad struct {
			C chan int `avro:"c"`
		}
		type R struct {
			Name string `avro:"name"`
			Bad  Bad    `avro:",inline"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("type-alias on primitive", func(t *testing.T) {
		type R struct {
			X int32 `avro:"x,type-alias=old_x"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error for type-alias on non-named type")
		}
	})

	t.Run("empty alias", func(t *testing.T) {
		type R struct {
			X int32 `avro:"x,alias="`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error for empty alias")
		}
	})

	t.Run("empty brackets", func(t *testing.T) {
		type R struct {
			X int32 `avro:"x,alias=[]"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error for empty brackets")
		}
	})

	t.Run("empty element in brackets", func(t *testing.T) {
		type R struct {
			X int32 `avro:"x,alias=[a,,b]"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error for empty element in brackets")
		}
	})

	t.Run("trailing comma in brackets", func(t *testing.T) {
		type R struct {
			X int32 `avro:"x,alias=[a,]"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error for trailing comma in brackets")
		}
	})

	t.Run("unclosed bracket", func(t *testing.T) {
		type R struct {
			X int32 `avro:"x,alias=[a,b"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error for unclosed bracket")
		}
	})
}

type customString struct{ v string }

func (c customString) MarshalText() ([]byte, error)  { return []byte(c.v), nil }
func (c *customString) UnmarshalText(b []byte) error { c.v = string(b); return nil }

type stringerType struct{ v string }

func (s stringerType) String() string { return s.v }

func TestSchemaForTextMarshalerInferredAsString(t *testing.T) {
	type Record struct {
		A customString `avro:"a"`
	}
	s, err := SchemaFor[Record]()
	if err != nil {
		t.Fatal(err)
	}
	root := s.Root()
	if len(root.Fields) == 0 {
		t.Fatal("expected fields")
	}
	if root.Fields[0].Type.Type != "string" {
		t.Fatalf("expected string, got %s", root.Fields[0].Type.Type)
	}
}

func TestSchemaForOmitzeroTag(t *testing.T) {
	type Record struct {
		Name string `avro:"name,omitzero"`
	}
	if _, err := SchemaFor[Record](); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaForDuplicateUUID(t *testing.T) {
	type TwoUUIDs struct {
		A [16]byte `avro:"a,uuid"`
		B [16]byte `avro:"b,uuid"`
	}
	s, err := SchemaFor[TwoUUIDs]()
	if err != nil {
		t.Fatalf("SchemaFor with duplicate UUID fields should succeed: %v", err)
	}
	input := TwoUUIDs{A: [16]byte{1}, B: [16]byte{2}}
	enc, err := s.Encode(&input)
	if err != nil {
		t.Fatal(err)
	}
	var out TwoUUIDs
	if _, err := s.Decode(enc, &out); err != nil {
		t.Fatal(err)
	}
	if out != input {
		t.Fatalf("round-trip: got %v, want %v", out, input)
	}
}

func TestSchemaForDuplicateFixedConflictErrors(t *testing.T) {
	type Conflict struct {
		A [16]byte `avro:"a"`
		B [8]byte  `avro:"b"`
	}
	// Both infer fixed with name "fixed_16" / "fixed_8" — no conflict.
	// But if we use custom types that map both to the same Avro name...
	_, err := SchemaFor[Conflict](
		CustomType{
			GoType:      reflect.TypeFor[[16]byte](),
			LogicalType: "uuid",
			Schema:      &SchemaNode{Type: "fixed", Name: "shared", Size: 16, LogicalType: "uuid"},
		},
		CustomType{
			GoType:      reflect.TypeFor[[8]byte](),
			LogicalType: "uuid",
			Schema:      &SchemaNode{Type: "fixed", Name: "shared", Size: 8}, // same name, different size
		},
	)
	if err == nil {
		t.Fatal("expected error for conflicting named type definitions in SchemaFor")
	}
}

func TestSchemaForCustomTypeNoAvroType(t *testing.T) {
	type MyType struct{ X int }
	type Rec struct {
		F MyType `avro:"f"`
	}
	// CustomType has GoType but neither AvroType nor Schema set.
	ct := CustomType{GoType: reflect.TypeFor[MyType]()}
	_, err := SchemaFor[Rec](ct)
	if err == nil {
		t.Fatal("expected error for CustomType without AvroType or Schema")
	}
}

// TestRegression_SchemaForShadowedEmbedShallowestWins pins the
// shadowed-embed precedence rule: doc.go:147-149 documents "the
// shallowest wins" for same-name fields, and reflect.go's
// typeFieldMapping (line 322-323) implements it at runtime. Without
// this, schema_for.go's collectFields dedup (line 313-321) would only
// special-case tagged-beats-untagged and keep first-seen for
// same-tagged-status — the deeper embedded field, because
// collectFields appends nested-struct fields BEFORE outer fields.
//
// Observable consequence without the rule: encode of a legal outer.X
// int64 value against the inferred schema fails with "overflows int32"
// because the schema declares the embedded int32 type while the
// runtime encoder uses the outer int64 value.
func TestRegression_SchemaForShadowedEmbedShallowestWins(t *testing.T) {
	t.Run("both_tagged_outer_wins", func(t *testing.T) {
		type Inner struct {
			X int32 `avro:"x"`
		}
		type Outer struct {
			Inner
			X int64 `avro:"x"`
		}
		s, err := SchemaFor[Outer]()
		if err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
		js := s.String()
		if !strings.Contains(js, `"type":"long"`) {
			t.Fatalf("expected outer field's int64→long; got: %s", js)
		}

		// Runtime encode of int64-fitting value should succeed.
		o := Outer{}
		o.X = 1 << 33 // beyond int32 range, fits int64
		if _, err := s.Encode(&o); err != nil {
			t.Fatalf("expected encode accept (schema is long, value fits int64); got: %v", err)
		}
	})

	t.Run("both_untagged_outer_wins", func(t *testing.T) {
		type Inner struct {
			X int32
		}
		type Outer struct {
			Inner
			X int64
		}
		s, err := SchemaFor[Outer]()
		if err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
		js := s.String()
		if !strings.Contains(js, `"type":"long"`) {
			t.Fatalf("expected outer field's int64→long for untagged shadowed embed; got: %s", js)
		}
	})
}

// TestRegression_SchemaForSameDepthTaggedBeatsUntagged pins that SchemaFor
// resolves a same-depth tagged-vs-untagged Avro-name collision via the tag
// tiebreaker, matching the documented contract (doc.go: "among fields at the
// same depth, a tagged field wins over an untagged one") and the runtime
// field mapping (reflect.go's typeFieldMapping). Only a same-depth collision
// with the SAME tagged status is the ambiguous case that errors; a
// tagged/untagged pair at the same depth has a clear winner. Without the
// tiebreaker ordering, collectFields raised "duplicate field name" before
// the tag tiebreaker could fire, so SchemaFor rejected a type that
// Encode/Decode handle.
func TestRegression_SchemaForSameDepthTaggedBeatsUntagged(t *testing.T) {
	type Collide struct {
		Renamed int32 `avro:"Shared"` // tagged → Avro name "Shared"
		Shared  int32 // untagged → Go field name is also "Shared"; the tagged field wins
	}
	s, err := SchemaFor[Collide]()
	if err != nil {
		t.Fatalf("SchemaFor rejected a same-depth tagged/untagged collision the runtime resolves: %v", err)
	}
	// The winning field is the tagged int32 (named "Shared").
	if c := strings.Count(s.String(), `"name":"Shared"`); c != 1 {
		t.Fatalf("want exactly one field named %q; got %d in %s", "Shared", c, s.String())
	}

	// The runtime must select the same (tagged) field — encode a value that
	// only fits if "shared" maps to the int32-typed Renamed field, then
	// confirm it round-trips into Renamed.
	in := Collide{Renamed: 7, Shared: 99}
	b, err := s.Encode(&in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var got Collide
	if _, err := s.Decode(b, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Renamed != 7 {
		t.Fatalf("tagged field should own the Avro name: got Renamed=%d want 7", got.Renamed)
	}

	// Boundary: a same-depth collision with the SAME tagged status stays an
	// ambiguous error (this is the case the tiebreaker does NOT resolve).
	type Ambiguous struct {
		A int32 `avro:"dup"`
		B int32 `avro:"dup"`
	}
	if _, err := SchemaFor[Ambiguous](); err == nil {
		t.Fatalf("same-depth same-tagged-status collision must remain an ambiguous error")
	}
}

// A single Go [N]byte type can be referenced both ,uuid-tagged (Avro
// fixed(16) + uuid logical, named "uuid") and plain (Avro fixed named after
// the Go type). Those are distinct Avro types, so SchemaFor must emit a
// definition for each form rather than a name reference under the other
// form's name (which would dangle and fail Parse). Both field orders are
// exercised because the definition/reference bookkeeping is order-sensitive.
func TestRegression_SchemaForMixedUUIDAndPlainSameType(t *testing.T) {
	type ID [16]byte

	t.Run("uuid then plain round-trips", func(t *testing.T) {
		type R struct {
			A ID `avro:"a,uuid"`
			B ID `avro:"b"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
		// Two distinct fixed(16) definitions, not one definition + a
		// dangling reference.
		if c := strings.Count(s.String(), `"size":16`); c != 2 {
			t.Fatalf("want 2 fixed(16) definitions, got %d in %s", c, s.String())
		}
		in := R{A: ID{1, 2, 3}, B: ID{4, 5, 6}}
		data, err := s.Encode(&in)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		var got R
		if _, err := s.Decode(data, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got != in {
			t.Fatalf("round trip: got %+v want %+v", got, in)
		}
	})

	t.Run("plain then uuid", func(t *testing.T) {
		type R struct {
			B ID `avro:"b"`
			A ID `avro:"a,uuid"`
		}
		if _, err := SchemaFor[R](); err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
	})

	// Boundary: the same type used the SAME way twice still collapses to
	// one definition plus a name reference (no duplicate-name error).
	t.Run("both uuid dedups to one definition", func(t *testing.T) {
		type R struct {
			A ID `avro:"a,uuid"`
			B ID `avro:"b,uuid"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
		if c := strings.Count(s.String(), `"size":16`); c != 1 {
			t.Fatalf("want 1 fixed(16) definition (rest references), got %d in %s", c, s.String())
		}
	})
}

// default= takes the remainder of the tag verbatim, so a string default
// whose value contains unbalanced parens/brackets — or commas, or JSON
// object braces — must be preserved rather than rejected by the tag
// bracket-balance scan (which exists only for the alias=[...] / decimal(...)
// option forms).
func TestRegression_SchemaForDefaultWithBrackets(t *testing.T) {
	t.Run("unbalanced open paren", func(t *testing.T) {
		type R struct {
			X string `avro:"x,default=note (a"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
		if !strings.Contains(s.String(), "note (a") {
			t.Fatalf("default not preserved: %s", s.String())
		}
	})

	t.Run("unbalanced close bracket", func(t *testing.T) {
		type R struct {
			X string `avro:"x,default=a]b"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
		if !strings.Contains(s.String(), "a]b") {
			t.Fatalf("default not preserved: %s", s.String())
		}
	})

	t.Run("commas in value", func(t *testing.T) {
		type R struct {
			X string `avro:"x,default=a,b,c"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
		if !strings.Contains(s.String(), "a,b,c") {
			t.Fatalf("default not preserved: %s", s.String())
		}
	})

	// Regression guard: a JSON-object default (internal commas + braces)
	// still survives — default= rejoins everything after it.
	t.Run("json object default", func(t *testing.T) {
		type R struct {
			M map[string]int32 `avro:"m,default={\"a\":1,\"b\":2}"`
		}
		if _, err := SchemaFor[R](); err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
	})

	// Boundary: a malformed bracketed NON-default option still errors — the
	// scan is only suppressed once a segment begins with default=.
	t.Run("non-default unbalanced bracket still errors", func(t *testing.T) {
		type R struct {
			X string `avro:"x,alias=[a,b"`
		}
		if _, err := SchemaFor[R](); err == nil {
			t.Fatal("expected error for unbalanced bracket in alias= option")
		}
	})
}

// A narrow Go integer kind maps to a wider Avro type (int8/16, uint8/16 ->
// int; uint32, uint -> long), so a default that fits the Avro type but not
// the Go field would build a schema whose own default overflows the field
// at decode-fill time. SchemaFor rejects it at build time, consistent with
// its other Go-type/tag compatibility checks. The default is parsed with
// the same lenient parser the wire path uses, so exponent / whole-number-
// float forms are caught too.
func TestRegression_SchemaForNarrowIntDefaultBounds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fn     func() (*Schema, error)
		reject bool
	}{
		{"int8 in range", func() (*Schema, error) {
			type R struct {
				X int8 `avro:"x,default=5"`
			}
			return SchemaFor[R]()
		}, false},
		{"int8 at max", func() (*Schema, error) {
			type R struct {
				X int8 `avro:"x,default=127"`
			}
			return SchemaFor[R]()
		}, false},
		{"int8 over max", func() (*Schema, error) {
			type R struct {
				X int8 `avro:"x,default=128"`
			}
			return SchemaFor[R]()
		}, true},
		{"int8 at min", func() (*Schema, error) {
			type R struct {
				X int8 `avro:"x,default=-128"`
			}
			return SchemaFor[R]()
		}, false},
		{"int8 under min", func() (*Schema, error) {
			type R struct {
				X int8 `avro:"x,default=-129"`
			}
			return SchemaFor[R]()
		}, true},
		{"int8 far over (valid Avro int)", func() (*Schema, error) {
			type R struct {
				X int8 `avro:"x,default=99999"`
			}
			return SchemaFor[R]()
		}, true},
		{"int8 exponent form over", func() (*Schema, error) {
			type R struct {
				X int8 `avro:"x,default=1e3"`
			}
			return SchemaFor[R]()
		}, true},
		{"uint8 at max", func() (*Schema, error) {
			type R struct {
				X uint8 `avro:"x,default=255"`
			}
			return SchemaFor[R]()
		}, false},
		{"uint8 over max", func() (*Schema, error) {
			type R struct {
				X uint8 `avro:"x,default=256"`
			}
			return SchemaFor[R]()
		}, true},
		{"uint8 negative", func() (*Schema, error) {
			type R struct {
				X uint8 `avro:"x,default=-1"`
			}
			return SchemaFor[R]()
		}, true},
		{"uint32 over max (valid Avro long)", func() (*Schema, error) {
			type R struct {
				X uint32 `avro:"x,default=4294967296"`
			}
			return SchemaFor[R]()
		}, true},
		{"int32 full range ok (no narrowing)", func() (*Schema, error) {
			type R struct {
				X int32 `avro:"x,default=2147483647"`
			}
			return SchemaFor[R]()
		}, false},
		{"pointer narrow int over", func() (*Schema, error) {
			type R struct {
				X *int8 `avro:"x,default=200"`
			}
			return SchemaFor[R]()
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.fn()
			if tc.reject && err == nil {
				t.Fatal("expected SchemaFor to reject out-of-range default")
			}
			if !tc.reject && err != nil {
				t.Fatalf("expected SchemaFor to accept in-range default, got: %v", err)
			}
		})
	}
}

// A default= tag whose value is a valid JSON prefix followed by trailing
// content must be preserved VERBATIM (the documented intent for non-JSON
// defaults), not silently truncated to the JSON prefix. json.Decoder.Decode
// stops at the end of the first value and ignores the rest, so "42 oops"
// formerly became the number 42 with "oops" silently dropped.
func TestRegression_SchemaForDefaultTrailingContentVerbatim(t *testing.T) {
	type R struct {
		X string `avro:"x,default=42 oops"`
	}
	s, err := SchemaFor[R]()
	if err != nil {
		t.Fatalf("SchemaFor: %v", err)
	}
	if !strings.Contains(s.String(), "oops") {
		t.Errorf("trailing content silently truncated; want verbatim \"42 oops\": %s", s.String())
	}

	// A clean JSON default (no trailing content) still parses as JSON.
	type Q struct {
		N int64 `avro:"n,default=42"`
	}
	sq, err := SchemaFor[Q]()
	if err != nil {
		t.Fatalf("SchemaFor Q: %v", err)
	}
	if !strings.Contains(sq.String(), `"default":42`) {
		t.Errorf("clean JSON default should stay numeric 42: %s", sq.String())
	}
}

// TestSchemaForRejectsJSONNumber locks the rule that json.Number cannot be a
// SchemaFor field type. json.Number's Kind() is reflect.String and it
// implements no text interface, so the Kind switch's String arm would emit
// an Avro "string" schema — but the package's documented json.Number policy
// is numeric-only: string/bytes/fixed/enum reject json.Number on both encode
// and decode. SchemaFor is the package's one builder; emitting the single
// Avro type its own codec is guaranteed to reject for that Go type is a
// build-accepts / encode-rejects deferred failure, exactly the shape the
// uuid/decimal/time SchemaFor strictness eliminated. So SchemaFor rejects
// json.Number up front, naming the alternatives.
func TestSchemaForRejectsJSONNumber(t *testing.T) {
	type Event struct {
		Seq json.Number `avro:"seq"`
	}
	if _, err := SchemaFor[Event](); err == nil {
		t.Fatal("SchemaFor[json.Number field] must reject at build time, not defer to Encode")
	}

	// Siblings: every shape that carries json.Number through inferType's
	// recursion must reject for the same root reason.
	type SliceR struct {
		V []json.Number `avro:"v"`
	}
	type MapValR struct {
		V map[string]json.Number `avro:"v"`
	}
	type PtrR struct {
		V *json.Number `avro:"v"`
	}
	for name, build := range map[string]func() (*Schema, error){
		"slice":     func() (*Schema, error) { return SchemaFor[SliceR]() },
		"map-value": func() (*Schema, error) { return SchemaFor[MapValR]() },
		"pointer":   func() (*Schema, error) { return SchemaFor[PtrR]() },
		"top-level": func() (*Schema, error) { return SchemaFor[json.Number]() },
	} {
		if _, err := build(); err == nil {
			t.Errorf("%s: SchemaFor with a json.Number must reject at build time", name)
		}
	}

	// map[json.Number]V as a KEY is the documented exception: Avro map keys
	// are strings whose json.Number form round-trips, so the key path must
	// stay accepted. The fix must not touch it.
	type KeyR struct {
		V map[json.Number]int32 `avro:"v"`
	}
	ks, err := SchemaFor[KeyR]()
	if err != nil {
		t.Fatalf("map[json.Number]V key must remain accepted (documented exception): %v", err)
	}
	if _, err := ks.Encode(&KeyR{V: map[json.Number]int32{"7": 1}}); err != nil {
		t.Errorf("map[json.Number]int32 must round-trip on encode: %v", err)
	}

	// A NAMED alias (type N json.Number) is a distinct reflect.Type that the
	// codec treats as a plain string, so it must stay a plain "string"
	// schema and round-trip — the reject is exact-type only.
	type NamedNum json.Number
	type NamedR struct {
		V NamedNum `avro:"v"`
	}
	ns, err := SchemaFor[NamedR]()
	if err != nil {
		t.Fatalf("named json.Number alias must stay a plain string schema: %v", err)
	}
	if _, err := ns.Encode(&NamedR{V: "hello"}); err != nil {
		t.Errorf("named-alias string field must round-trip: %v", err)
	}
}

// schemaForFieldType mirrors SchemaFor's internal pipeline for a struct
// type built at runtime via reflect.StructOf, so the parity sweep below can
// enumerate field types the compile-time-generic SchemaFor[T] cannot reach
// at runtime. It is faithful to SchemaFor's body (inferRecord →
// dedupNamedTypes → Marshal → Parse) except it supplies an explicit record
// name (a StructOf struct is anonymous).
func schemaForFieldType(ft reflect.Type) (*Schema, error) {
	st := reflect.StructOf([]reflect.StructField{
		{Name: "F", Type: ft, Tag: `avro:"f"`},
	})
	seen := make(map[reflect.Type]string)
	s, err := inferRecord(st, "R", "", seen, nil)
	if err != nil {
		return nil, err
	}
	s = dedupNamedTypes(s, make(map[string]string))
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return Parse(string(b))
}

// sampleValue builds a NON-EMPTY value of t so the encode-parity sweep
// actually materializes leaf types buried in pointers/slices/maps (a nil
// pointer or empty slice never encodes its element type, which would hide a
// build-accepts/encode-rejects bug on that element — e.g. *json.Number).
func sampleValue(t reflect.Type) reflect.Value { return sampleValuePath(t, nil) }

// sampleValuePath is sampleValue with a cycle guard so a recursive Go type
// (a linked-list `Next *Node`, a `type S []S`, a `map[string]M`) terminates
// instead of recursing forever: a type already on the construction path
// yields the zero value at that point (a nil pointer / empty slice / empty
// map / zero-filled array or struct), all of which are valid round-trip
// values. For a NON-recursive type no type ever reaches itself, so the guard
// never fires and the produced value is identical to the original
// sampleValue — the existing TestSchemaForEncodeParity sweep is unchanged.
// The cycle safety lets the round-trip-consistency net carry recursive-struct
// leaves through the same shared sampler.
func sampleValuePath(t reflect.Type, onPath map[reflect.Type]bool) reflect.Value {
	if t == timeType {
		// A representative IN-RANGE, whole-second, UTC time. The zero time.Time
		// is year 1, which overflows int64-nanoseconds-since-epoch (~1678..2262
		// AD): a timestamp-nanos schema — valid for in-range times, which
		// SchemaFor rightly builds for the explicit tag — would then reject the
		// zero value at Encode, masking a correct schema as a build-accepts/
		// encode-rejects. A whole-second 2020 time is representable by every
		// time/date logical (millis/micros/nanos, date, time-of-day) without
		// overflow or sub-unit truncation. No monotonic reading, UTC location,
		// so it round-trips identically.
		return reflect.ValueOf(time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC))
	}
	switch t.Kind() {
	case reflect.Pointer:
		if onPath[t.Elem()] {
			return reflect.Zero(t) // nil pointer breaks a *Node→Node→*Node cycle
		}
		p := reflect.New(t.Elem())
		p.Elem().Set(sampleValuePath(t.Elem(), withSamplePath(onPath, t.Elem())))
		return p
	case reflect.Slice:
		if onPath[t.Elem()] {
			return reflect.MakeSlice(t, 0, 0) // empty slice breaks a []S→S cycle
		}
		sl := reflect.MakeSlice(t, 1, 1)
		sl.Index(0).Set(sampleValuePath(t.Elem(), withSamplePath(onPath, t.Elem())))
		return sl
	case reflect.Array:
		a := reflect.New(t).Elem()
		if onPath[t.Elem()] {
			return a // zero-filled array breaks an [N]A→A cycle
		}
		next := withSamplePath(onPath, t.Elem())
		for i := 0; i < t.Len(); i++ {
			a.Index(i).Set(sampleValuePath(t.Elem(), next))
		}
		return a
	case reflect.Map:
		m := reflect.MakeMap(t)
		if onPath[t.Elem()] || onPath[t.Key()] {
			return m // empty map breaks a map[K]M→M cycle
		}
		m.SetMapIndex(sampleValuePath(t.Key(), withSamplePath(onPath, t.Key())),
			sampleValuePath(t.Elem(), withSamplePath(onPath, t.Elem())))
		return m
	case reflect.Struct:
		v := reflect.New(t).Elem()
		if onPath[t] {
			return v // zero struct breaks a by-value struct cycle
		}
		next := withSamplePath(onPath, t)
		for i := 0; i < t.NumField(); i++ {
			if t.Field(i).IsExported() {
				v.Field(i).Set(sampleValuePath(t.Field(i).Type, next))
			}
		}
		return v
	case reflect.String:
		// "1" is a valid json.Number AND a valid string/map-key, so it works
		// for every String-kind type the sweep carries.
		return reflect.ValueOf("1").Convert(t)
	default:
		return reflect.New(t).Elem() // zero is representative for scalars/time
	}
}

// withSamplePath returns a copy of onPath with t added (copy-on-descend so
// sibling fields do not see each other's path — only a type reaching ITSELF
// is cut).
func withSamplePath(onPath map[reflect.Type]bool, t reflect.Type) map[reflect.Type]bool {
	next := make(map[reflect.Type]bool, len(onPath)+1)
	for k := range onPath {
		next[k] = true
	}
	next[t] = true
	return next
}

// TestSchemaForEncodeParity is the generative net for the build-accepts /
// encode-rejects bug class (the shape of the json.Number SchemaFor bug):
// SchemaFor is the package's one Go-type → schema builder, and it has no
// wire-format counterpart, so the encode/decode-parity and oracle lenses
// never reach it. The invariant that DOES reach it: if SchemaFor ACCEPTS a
// field type, Encode of a value of that type MUST also accept — otherwise
// the schema builds but every Encode fails far from the SchemaFor call.
//
// The sweep crosses every codec-special-cased / Kind-misleading Go type
// (the high-risk surface — stdlib types whose reflect.Kind does not match
// the Avro type the codec wants) plus named aliases, pointers, slices,
// maps, and nesting. For each accepted type it encodes the zero value and
// confirms the wire is readable; a reject is always safe (build-time
// strictness cannot defer a failure to Encode). New field types are one
// table line and inherit the invariant automatically.
func TestSchemaForEncodeParity(t *testing.T) {
	type namedString string
	type namedInt int64
	type namedNumber json.Number // distinct reflect.Type → plain string
	type inner struct {
		A int32 `avro:"a"`
	}

	types := []reflect.Type{
		// primitives across every Kind the inference switch handles
		reflect.TypeFor[bool](), reflect.TypeFor[int](), reflect.TypeFor[int8](),
		reflect.TypeFor[int16](), reflect.TypeFor[int32](), reflect.TypeFor[int64](),
		reflect.TypeFor[uint8](), reflect.TypeFor[uint16](), reflect.TypeFor[uint32](),
		reflect.TypeFor[uint64](), reflect.TypeFor[uint](),
		reflect.TypeFor[float32](), reflect.TypeFor[float64](), reflect.TypeFor[string](),
		// byte containers
		reflect.TypeFor[[]byte](), reflect.TypeFor[[4]byte](), reflect.TypeFor[[16]byte](),
		// codec-special-cased stdlib types (Kind misleads)
		reflect.TypeFor[json.Number](),   // Kind String, codec rejects → SchemaFor must reject
		reflect.TypeFor[time.Time](),     // Kind Struct → logical long
		reflect.TypeFor[time.Duration](), // Kind Int64 → logical
		reflect.TypeFor[big.Rat](),       // requires decimal tag → reject untagged
		reflect.TypeFor[*big.Rat](),      // requires decimal tag → reject untagged
		// named aliases — distinct reflect.Type, must follow Kind honestly
		reflect.TypeFor[namedString](), reflect.TypeFor[namedInt](), reflect.TypeFor[namedNumber](),
		// pointers (nullable)
		reflect.TypeFor[*int](), reflect.TypeFor[*string](), reflect.TypeFor[*time.Time](),
		reflect.TypeFor[*json.Number](), // carries json.Number → reject
		// slices / maps / nesting
		reflect.TypeFor[[]int](), reflect.TypeFor[[]string](), reflect.TypeFor[[]time.Time](),
		reflect.TypeFor[[]json.Number](),          // carries json.Number → reject
		reflect.TypeFor[map[string]int](),         // value int
		reflect.TypeFor[map[string]json.Number](), // value json.Number → reject
		reflect.TypeFor[map[json.Number]int32](),  // KEY json.Number → documented exception, accept
		reflect.TypeFor[inner](),                  // nested struct
		reflect.TypeFor[[]inner](), reflect.TypeFor[map[string]inner](),
	}

	for _, ft := range types {
		t.Run(ft.String(), func(t *testing.T) {
			s, err := schemaForFieldType(ft)
			if err != nil {
				// Reject is always safe: a build-time error cannot become a
				// deferred Encode failure. (The targeted reject-set is
				// locked separately in TestSchemaForRejectsJSONNumber.)
				return
			}
			// SchemaFor ACCEPTED → the parity invariant: a value of this
			// type must Encode. The zero value is a representative input;
			// for json.Number the zero value (json.Number("")) is exactly
			// what the codec rejects, so the bug shape is caught here.
			st := reflect.StructOf([]reflect.StructField{
				{Name: "F", Type: ft, Tag: `avro:"f"`},
			})
			// A NON-EMPTY sample: pointers allocated, slices/maps with one
			// element — so a json.Number buried in a *T / []T / map[K]T
			// actually reaches the encoder. The zero value would leave those
			// nil/empty and never exercise the leaf type (the exact gap a
			// first version of this test had, revealed by neutering the fix:
			// only top-level json.Number was caught).
			sv := reflect.New(st).Elem()
			sv.Field(0).Set(sampleValue(ft))
			wire, encErr := s.Encode(sv.Interface())
			if encErr != nil {
				t.Fatalf("SchemaFor ACCEPTED field type %s but Encode of a value REJECTS it (build-accepts/encode-rejects deferred failure):\n schema: %s\n err: %v",
					ft, s, encErr)
			}
			// The wire SchemaFor's own schema produced must be decodable by
			// that same schema (no panic, consumes fully) — a sanity that
			// the emitted schema is internally consistent end to end.
			var sink any
			if _, decErr := s.Decode(wire, &sink); decErr != nil {
				t.Fatalf("SchemaFor schema for %s encoded but cannot decode its own wire: %v", ft, decErr)
			}
		})
	}
}

// Recursive non-struct Go types have a cyclic type graph (sfRecursiveSlice's
// element is itself, etc.). inferType's pointer/slice/map arms recurse on the
// element type, so without a depth bound SchemaFor recurses until the
// goroutine stack overflows and the whole process dies. The bound makes it
// return a clean error instead. A recursive STRUCT is unaffected — inferRecord
// registers the type name before recursing, so a self-reference becomes a name
// reference (pinned by TestSchemaForRecursiveStructStillBuilds below).
//
// Non-vacuity: reverting the inferType depth bound makes each of these
// stack-overflow at SchemaFor time, which kills the test binary rather than
// failing one case — so these pins assert the post-fix clean error directly.
type sfRecursiveSlice []sfRecursiveSlice
type sfRecursivePtr *sfRecursivePtr
type sfRecursiveMap map[string]sfRecursiveMap

func TestRegression_SchemaForRecursiveNonStructTypeErrors(t *testing.T) {
	wantErr := func(t *testing.T, _ *Schema, err error) {
		t.Helper()
		if err == nil {
			t.Fatal("expected a recursion error, got nil")
		}
		// slice/map recurse to the maxDepth ceiling ("nests too deeply or is
		// recursive"); a cyclic pointer type (type P *P) is an unbounded
		// consecutive-pointer chain, caught earlier at the codec's unwrap cap
		// ("pointer chain nests deeper than the codec supports"). Either names
		// the recursion/depth cause.
		if !strings.Contains(err.Error(), "recursive") &&
			!strings.Contains(err.Error(), "nests too deeply") &&
			!strings.Contains(err.Error(), "nests deeper") {
			t.Fatalf("error should name the recursion/depth cause, got: %v", err)
		}
	}
	t.Run("slice", func(t *testing.T) {
		type R struct {
			F sfRecursiveSlice `avro:"f"`
		}
		s, err := SchemaFor[R]()
		wantErr(t, s, err)
	})
	t.Run("pointer", func(t *testing.T) {
		type R struct {
			F sfRecursivePtr `avro:"f"`
		}
		s, err := SchemaFor[R]()
		wantErr(t, s, err)
	})
	t.Run("map", func(t *testing.T) {
		type R struct {
			F sfRecursiveMap `avro:"f"`
		}
		s, err := SchemaFor[R]()
		wantErr(t, s, err)
	})
}

// The depth bound must not false-reject ordinary nested non-struct containers
// (a handful of pointer/slice/map levels, far under the cap). This is the
// "still accepted" side of the boundary.
func TestSchemaForNestedNonStructContainersStillBuild(t *testing.T) {
	type R struct {
		A [][]int32                    `avro:"a"`
		B map[string][]*int64          `avro:"b"`
		C map[string]map[string]string `avro:"c"`
	}
	if _, err := SchemaFor[R](); err != nil {
		t.Fatalf("ordinary nested containers must build, got: %v", err)
	}
}

// Control: a self-referential STRUCT (linked list) still builds and
// round-trips — the depth bound must not break legitimate recursive structs,
// which terminate via inferRecord's seen[t] name registration.
func TestSchemaForRecursiveStructStillBuilds(t *testing.T) {
	type LinkedNode struct {
		Val  int32       `avro:"val"`
		Next *LinkedNode `avro:"next"`
	}
	s, err := SchemaFor[LinkedNode]()
	if err != nil {
		t.Fatalf("recursive struct must build: %v", err)
	}
	in := &LinkedNode{Val: 1, Next: &LinkedNode{Val: 2}}
	b, err := s.Encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got LinkedNode
	if _, err := s.Decode(b, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Val != 1 || got.Next == nil || got.Next.Val != 2 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

// checkIntDefaultFitsGoKind peels pointer levels off a field's Go type to
// range-check an integer default. When a CustomType matches the field,
// inferType returns before its own (bounded) recursion, so a recursive
// pointer field carrying a default reaches this peel — which must terminate
// (bounded by maxIndirectDepth), not loop forever. Watchdog so a regression
// fails by timeout rather than hanging the suite.
func TestRegression_SchemaForRecursivePtrDefaultTerminates(t *testing.T) {
	type R struct {
		F sfRecursivePtr `avro:"f,default=5"`
	}
	done := make(chan struct{}, 1)
	go func() {
		_, _ = SchemaFor[R](CustomType{
			GoType:   reflect.TypeFor[sfRecursivePtr](),
			AvroType: "long",
		})
		done <- struct{}{}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("SchemaFor did not terminate (checkIntDefaultFitsGoKind pointer peel unbounded)")
	}
}
