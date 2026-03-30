package avro

import (
	"encoding/json"
	"math/big"
	"reflect"
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

	t.Run("recursive type", func(t *testing.T) {
		type Node struct {
			Value int   `avro:"value"`
			Next  *Node `avro:"next"`
		}
		if _, err := SchemaFor[Node](); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("recursive with namespace", func(t *testing.T) {
		type Node struct {
			Value int   `avro:"value"`
			Next  *Node `avro:"next"`
		}
		if _, err := SchemaFor[Node](WithNamespace("com.example")); err == nil {
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
}

type customString struct{ v string }

func (c customString) MarshalText() ([]byte, error)  { return []byte(c.v), nil }
func (c *customString) UnmarshalText(b []byte) error { c.v = string(b); return nil }

type stringerType struct{ v string }

func (s stringerType) String() string { return s.v }

func TestSchemaForTextMarshalerNotInferredAsString(t *testing.T) {
	type Record struct {
		A customString `avro:"a"`
	}
	s, err := SchemaFor[Record]()
	if err != nil {
		t.Fatal(err)
	}
	// customString should NOT be inferred as "string" — it's a struct,
	// so SchemaFor infers it as a record.
	root := s.Root()
	if len(root.Fields) == 0 {
		t.Fatal("expected fields")
	}
	if root.Fields[0].Type.Type == "string" {
		t.Fatal("TextMarshaler type should not be inferred as string")
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
