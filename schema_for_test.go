package avro

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math/big"
	"os"
	"reflect"
	"runtime/debug"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
)

// ---------- schema_for_test.go ----------

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

// avro.Duration is the dedicated Go type for the Avro "duration" logical type
// (a fixed(12) carrying little-endian months/days/milliseconds — distinct from
// the time.Duration→time-millis mapping in [TestSchemaForDuration] above).
// SchemaFor recognizes it BY TYPE, with no tag, and must emit the duration
// fixed wherever the type appears — bare field, *avro.Duration (nullable
// union), slice/array element, map value, and nested-record field — never
// decompose its exported uint32 fields into a {Months,Days,Milliseconds}
// record. The metadata-tree assertions below are the neuter target: reverting
// inferType's avroDurationType case turns every leaf back into that record and
// reddens this test (the round-trips would still pass as a record, so the
// shape assertion is what locks the behavior).
func TestSchemaForAvroDuration(t *testing.T) {
	dur := Duration{Months: 5, Days: 10, Milliseconds: 1234}

	assertDurationFixed := func(t *testing.T, n SchemaNode) {
		t.Helper()
		if n.Type != "fixed" {
			t.Fatalf("duration leaf Type = %q, want \"fixed\" (a record decomposition means the avroDurationType case did not fire)", n.Type)
		}
		if n.LogicalType != "duration" {
			t.Errorf("duration leaf LogicalType = %q, want \"duration\"", n.LogicalType)
		}
		if n.Size != 12 {
			t.Errorf("duration leaf Size = %d, want 12", n.Size)
		}
		if n.Name != "duration" {
			t.Errorf("duration leaf Name = %q, want \"duration\"", n.Name)
		}
		if len(n.Fields) != 0 {
			t.Errorf("duration leaf has %d record fields; it must not decompose into Months/Days/Milliseconds", len(n.Fields))
		}
	}

	t.Run("bare", func(t *testing.T) {
		type R struct {
			D Duration `avro:"d"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		assertDurationFixed(t, s.Root().Fields[0].Type)
		// A single-field record's wire IS the field's encoding (no framing),
		// so it must be exactly the 12-byte duration fixed — a decomposed
		// record would emit three zig-zag varint longs (4 bytes here), so the
		// length alone separates the two even before the byte compare.
		w, err := s.Encode(R{D: dur})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		b := dur.Bytes()
		if string(w) != string(b[:]) {
			t.Fatalf("wire = %x, want the 12-byte duration fixed %x", w, b)
		}
		var got R
		if _, err := s.Decode(w, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.D != dur {
			t.Errorf("round-trip: got %v, want %v", got.D, dur)
		}
	})

	t.Run("pointer-nullable", func(t *testing.T) {
		type R struct {
			D *Duration `avro:"d"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		ft := s.Root().Fields[0].Type
		if ft.Type != "union" || len(ft.Branches) != 2 || ft.Branches[0].Type != "null" {
			t.Fatalf("pointer field type = %+v, want [\"null\", duration]", ft)
		}
		assertDurationFixed(t, ft.Branches[1])
		for _, v := range []*Duration{&dur, nil} {
			w, err := s.Encode(R{D: v})
			if err != nil {
				t.Fatalf("encode %v: %v", v, err)
			}
			var got R
			if _, err := s.Decode(w, &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if (v == nil) != (got.D == nil) || (v != nil && *got.D != *v) {
				t.Errorf("round-trip ptr: got %v want %v", got.D, v)
			}
		}
	})

	t.Run("slice", func(t *testing.T) {
		type R struct {
			D []Duration `avro:"d"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		ft := s.Root().Fields[0].Type
		if ft.Type != "array" || ft.Items == nil {
			t.Fatalf("slice field type = %+v, want array of duration", ft)
		}
		assertDurationFixed(t, *ft.Items)
		rt := R{D: []Duration{dur, {}}}
		w, err := s.Encode(rt)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got R
		if _, err := s.Decode(w, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(got.D) != 2 || got.D[0] != dur || got.D[1] != (Duration{}) {
			t.Errorf("round-trip slice: got %v", got.D)
		}
	})

	t.Run("array", func(t *testing.T) {
		type R struct {
			D [2]Duration `avro:"d"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		ft := s.Root().Fields[0].Type
		if ft.Type != "array" || ft.Items == nil {
			t.Fatalf("array field type = %+v, want array of duration", ft)
		}
		assertDurationFixed(t, *ft.Items)
		w, err := s.Encode(R{D: [2]Duration{dur, {}}})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got R
		if _, err := s.Decode(w, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.D[0] != dur || got.D[1] != (Duration{}) {
			t.Errorf("round-trip array: got %v", got.D)
		}
	})

	t.Run("map", func(t *testing.T) {
		type R struct {
			D map[string]Duration `avro:"d"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		ft := s.Root().Fields[0].Type
		if ft.Type != "map" || ft.Values == nil {
			t.Fatalf("map field type = %+v, want map of duration", ft)
		}
		assertDurationFixed(t, *ft.Values)
		w, err := s.Encode(R{D: map[string]Duration{"k": dur}})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got R
		if _, err := s.Decode(w, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.D["k"] != dur {
			t.Errorf("round-trip map: got %v", got.D)
		}
	})

	t.Run("nested-record-field", func(t *testing.T) {
		type Inner struct {
			D Duration `avro:"d"`
		}
		type R struct {
			In Inner `avro:"in"`
		}
		s, err := SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		inner := s.Root().Fields[0].Type
		if inner.Type != "record" {
			t.Fatalf("inner field type = %q, want record", inner.Type)
		}
		assertDurationFixed(t, inner.Fields[0].Type)
	})
}

// A non-empty logical tag attached to an avro.Duration field is always a
// mismatch — there is no "duration" tag option (recognition is purely by type),
// so any tag the user wrote is one of uuid / decimal / a time logical, none of
// which apply to the duration fixed. SchemaFor rejects it rather than silently
// emitting the duration schema and dropping the tag, matching the strict-reject
// posture of the time.Time/time.Duration/uuid/decimal arms.
func TestSchemaForAvroDurationRejectsLogicalTag(t *testing.T) {
	for _, tag := range []string{"uuid", "timestamp-millis", "date", "decimal(9,2)"} {
		t.Run(tag, func(t *testing.T) {
			st := reflect.StructOf([]reflect.StructField{{
				Name: "D",
				Type: avroDurationType,
				Tag:  reflect.StructTag(`avro:"d,` + tag + `"`),
			}})
			seen := make(map[reflect.Type]seenForm)
			if _, err := inferRecord(st, "R", "", seen, nil, make(appliedTypeAliases)); err == nil {
				t.Fatalf("avro.Duration with %q tag should be rejected", tag)
			}
		})
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

// TestSchemaForTypeAliasCrossRecord pins type-alias dedup scope ACROSS record
// boundaries. The dedup state (which type-aliases have been applied to each
// named type) is keyed on a type's fullname and must span the whole inference,
// exactly like the named-type registry (seen): a named type is defined once and
// may be referenced from any record. TestSchemaForTypeAliasNamedRef covers only
// the same-record case (defining and referencing field in one struct); this
// covers a named type defined in one record and referenced — with the SAME
// alias — from a DIFFERENT (nested) record reached through every inferType
// recursion arm. Per-record dedup state spuriously rejected these: the nested
// record never saw the earlier application, so a reference fell into the
// "defined without type-alias" branch with a factually false message.
func TestSchemaForTypeAliasCrossRecord(t *testing.T) {
	type Inner struct {
		Value int32 `avro:"value"`
	}

	// Identical alias across a record boundary is accepted, and the alias lands
	// on the type DEFINITION exactly once while the cross-record occurrence is a
	// bare name reference (not a second definition carrying the alias).
	t.Run("nested struct identical alias accepted and aliased once", func(t *testing.T) {
		type Nested struct {
			Ref Inner `avro:"ref,type-alias=old_inner"`
		}
		type Outer struct {
			Def    Inner  `avro:"def,type-alias=old_inner"` // defines Inner + alias (processed first)
			Nested Nested `avro:"nested"`                   // its Ref references Inner from another record
		}
		s, err := SchemaFor[Outer]()
		if err != nil {
			t.Fatalf("cross-record identical alias rejected: %v", err)
		}
		js := s.String()
		// The alias attaches to the single Inner definition; references are bare,
		// so the alias text appears exactly once.
		if n := strings.Count(js, "old_inner"); n != 1 {
			t.Fatalf("alias should appear once (on the definition), got %d occurrences: %s", n, js)
		}
		// Structural proof: the defining field's type is the Inner object with the
		// alias; the nested field's Ref is the bare string "Inner".
		var raw map[string]any
		if err := json.Unmarshal([]byte(js), &raw); err != nil {
			t.Fatal(err)
		}
		fields := raw["fields"].([]any)
		defType := fields[0].(map[string]any)["type"].(map[string]any)
		if defType["name"] != "Inner" {
			t.Fatalf("def field type name: got %v, want Inner", defType["name"])
		}
		aliases, _ := defType["aliases"].([]any)
		if len(aliases) != 1 || aliases[0] != "old_inner" {
			t.Fatalf("Inner definition aliases: got %v, want [old_inner]", aliases)
		}
		nestedType := fields[1].(map[string]any)["type"].(map[string]any)
		refType := nestedType["fields"].([]any)[0].(map[string]any)["type"]
		if refType != "Inner" {
			t.Fatalf("nested Ref should be the bare name reference %q, got %T %v", "Inner", refType, refType)
		}
	})

	// Threading coverage: the alias'd type is defined at the top level and
	// referenced from a record reached via each inferType recursion arm. A
	// recursion call that fails to thread the dedup state would give the reached
	// record a fresh (empty) state and spuriously reject the identical alias, so
	// each of these reds independently if its arm is not threaded.
	t.Run("cross-record via array element", func(t *testing.T) {
		type Elem struct {
			Ref Inner `avro:"ref,type-alias=old_inner"`
		}
		type Outer struct {
			Def  Inner  `avro:"def,type-alias=old_inner"`
			List []Elem `avro:"list"` // Elem record reached via array items
		}
		s, err := SchemaFor[Outer]()
		if err != nil {
			t.Fatalf("cross-record-via-array identical alias rejected: %v", err)
		}
		if n := strings.Count(s.String(), "old_inner"); n != 1 {
			t.Fatalf("alias occurrences: got %d, want 1: %s", n, s.String())
		}
	})

	t.Run("cross-record via map value", func(t *testing.T) {
		type Val struct {
			Ref Inner `avro:"ref,type-alias=old_inner"`
		}
		type Outer struct {
			Def Inner          `avro:"def,type-alias=old_inner"`
			M   map[string]Val `avro:"m"` // Val record reached via map values
		}
		s, err := SchemaFor[Outer]()
		if err != nil {
			t.Fatalf("cross-record-via-map identical alias rejected: %v", err)
		}
		if n := strings.Count(s.String(), "old_inner"); n != 1 {
			t.Fatalf("alias occurrences: got %d, want 1: %s", n, s.String())
		}
	})

	t.Run("cross-record via pointer", func(t *testing.T) {
		type Target struct {
			Ref Inner `avro:"ref,type-alias=old_inner"`
		}
		type Outer struct {
			Def Inner   `avro:"def,type-alias=old_inner"`
			P   *Target `avro:"p"` // Target record reached via pointer elem
		}
		s, err := SchemaFor[Outer]()
		if err != nil {
			t.Fatalf("cross-record-via-pointer identical alias rejected: %v", err)
		}
		if n := strings.Count(s.String(), "old_inner"); n != 1 {
			t.Fatalf("alias occurrences: got %d, want 1: %s", n, s.String())
		}
	})

	// A genuine cross-record conflict (same type, different aliases in two
	// records) must be reported truthfully as a conflict — NOT as the false
	// "defined without type-alias" message that per-record state produced (it
	// never saw the earlier application, so it mistook a conflict for a
	// no-earlier-alias case).
	t.Run("cross-record conflict reported as conflict", func(t *testing.T) {
		type Nested struct {
			Ref Inner `avro:"ref,type-alias=different_inner"`
		}
		type Outer struct {
			Def    Inner  `avro:"def,type-alias=old_inner"`
			Nested Nested `avro:"nested"`
		}
		_, err := SchemaFor[Outer]()
		if err == nil {
			t.Fatal("expected error for conflicting cross-record type-alias")
		}
		if !strings.Contains(err.Error(), "conflicts") {
			t.Errorf("conflict should be reported as a conflict, got: %v", err)
		}
		if strings.Contains(err.Error(), "without type-alias") {
			t.Errorf("conflict must not be reported as the false 'without type-alias' message: %v", err)
		}
	})

	// When the type is genuinely defined WITHOUT an alias and then referenced
	// WITH one from another record, the truthful "without type-alias" error
	// still fires (the dedup state correctly has no application recorded for it).
	t.Run("cross-record define-without then reference-with errors truthfully", func(t *testing.T) {
		type Nested struct {
			Ref Inner `avro:"ref,type-alias=late_inner"`
		}
		type Outer struct {
			Def    Inner  `avro:"def"` // defines Inner with NO alias
			Nested Nested `avro:"nested"`
		}
		_, err := SchemaFor[Outer]()
		if err == nil {
			t.Fatal("expected error for type-alias on a type already defined without one")
		}
		if !strings.Contains(err.Error(), "without type-alias") {
			t.Errorf("expected truthful 'without type-alias' error, got: %v", err)
		}
	})

	// Same-record identical control: the established contract (also pinned by
	// TestSchemaForTypeAliasNamedRef) must continue to accept.
	t.Run("same-record identical control still accepts", func(t *testing.T) {
		type Outer struct {
			A Inner `avro:"a,type-alias=old_inner"`
			B Inner `avro:"b,type-alias=old_inner"`
		}
		if _, err := SchemaFor[Outer](); err != nil {
			t.Fatalf("same-record identical alias control rejected: %v", err)
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

// A [16]byte Go type whose name is exactly the uuid logical name ("uuid")
// yields the SAME Avro fixed name ("uuid") for both its ,uuid-logical form and
// its plain form, so using it both ways would emit two distinct Avro types
// under one name — which Avro can't represent. SchemaFor rejects it rather than
// silently merging (dropping the ,uuid logical, or adding it to a plain field).
// Sibling of TestRegression_SchemaForMixedUUIDAndPlainSameType, which uses a
// distinct name (ID) where the two forms coexist; the distinct-name pin
// structurally cannot reach this name coincidence.
func TestMatrix_SchemaForUUIDNamedTypeMemoCollision(t *testing.T) {
	type uuid [16]byte // Name() == "uuid", colliding with the hard-coded logical name

	t.Run("uuid then plain rejected", func(t *testing.T) {
		type R struct {
			A uuid `avro:"a,uuid"`
			B uuid `avro:"b"`
		}
		_, err := SchemaFor[R]()
		if err == nil {
			t.Fatal("want error: type uuid used as both a uuid-logical and a plain fixed")
		}
		if !strings.Contains(err.Error(), "uuid") {
			t.Fatalf("error should name the conflict: %v", err)
		}
		// Lock that SchemaFor's dedup produced this, not Parse's fallback
		// duplicate-name error — both name the type, so without this the pin
		// passes even with dedupNamedTypes' conflict error reverted.
		if !strings.Contains(err.Error(), "two different") {
			t.Fatalf("conflict should be caught by SchemaFor's dedup, not the Parse fallback: %v", err)
		}
	})

	t.Run("plain then uuid rejected", func(t *testing.T) {
		type R struct {
			A uuid `avro:"a"`
			B uuid `avro:"b,uuid"`
		}
		_, err := SchemaFor[R]()
		if err == nil {
			t.Fatal("want error (plain first)")
		}
		if !strings.Contains(err.Error(), "two different") {
			t.Fatalf("conflict should be caught by SchemaFor's dedup, not the Parse fallback: %v", err)
		}
	})

	// No regression: a uuid-named type used CONSISTENTLY (all plain, or all
	// ,uuid) has no name conflict and must still succeed.
	t.Run("plain only ok", func(t *testing.T) {
		type R struct {
			A uuid `avro:"a"`
			B uuid `avro:"b"`
		}
		if _, err := SchemaFor[R](); err != nil {
			t.Fatalf("plain-only uuid-named type should succeed: %v", err)
		}
	})

	t.Run("uuid only ok", func(t *testing.T) {
		type R struct {
			A uuid `avro:"a,uuid"`
			B uuid `avro:"b,uuid"`
		}
		if _, err := SchemaFor[R](); err != nil {
			t.Fatalf("uuid-only should succeed: %v", err)
		}
	})
}

// The "one Avro name -> one definition" invariant is enforced GENERALLY by
// dedupNamedTypes, not just for uuid: two DIFFERENT Go types that map to the
// same fixed/record/enum name with different content are rejected. Here an
// anonymous [8]byte (auto-named "fixed_8") collides with a type literally named
// fixed_8 of a different size. This is the same check that will guard
// avro.Duration ("duration") against a plain [12]byte named "duration".
func TestRegression_SchemaForNamedTypeNameCollision(t *testing.T) {
	type fixed_8 [4]byte // the auto-name of an anonymous [8]byte is "fixed_8"
	type R struct {
		A [8]byte `avro:"a"` // -> fixed named "fixed_8", size 8
		B fixed_8 `avro:"b"` // -> fixed named "fixed_8", size 4  (conflict)
	}
	_, err := SchemaFor[R]()
	if err == nil {
		t.Fatal("want error: two different fixeds both named \"fixed_8\"")
	}
	if !strings.Contains(err.Error(), "fixed_8") {
		t.Fatalf("error should name the colliding type: %v", err)
	}
	// Lock that this is SchemaFor's dedup error (actionable: "rename a Go type"),
	// not Parse's cryptic "duplicate named type" fallback — both contain the
	// name, so without this the pin passes even if dedupNamedTypes is reverted.
	if !strings.Contains(err.Error(), "two different") {
		t.Fatalf("conflict should be caught by SchemaFor's dedup, not the Parse fallback: %v", err)
	}
}

// The avro.Duration realization of the collision class the comment above
// anticipates: avro.Duration infers a fixed named "duration" WITH
// logicalType:"duration", and a plain `type duration [12]byte` field infers a
// DIFFERENT fixed (size 12, no logicalType) also named "duration". Two
// definitions claiming one Avro name is rejected by the same general
// dedupNamedTypes check — not by any duration-specific code. The "two different"
// assertion proves SchemaFor's dedup fired and not Parse's weaker
// duplicate-name fallback: both messages contain "duration", so a bare
// err != nil + name check would pass even with dedupNamedTypes' conflict arm
// reverted (the exact hollow-pin failure mode a prior round shipped). Neuter-
// confirm by reverting that arm: this pin must redden.
func TestRegression_SchemaForAvroDurationCollision(t *testing.T) {
	type duration [12]byte // plain fixed named "duration", NO logicalType
	type R struct {
		A Duration `avro:"a"` // avro.Duration -> fixed "duration" + logicalType:"duration"
		B duration `avro:"b"` // plain [12]byte -> fixed "duration", no logicalType (conflict)
	}
	_, err := SchemaFor[R]()
	if err == nil {
		t.Fatal("want error: avro.Duration and a plain [12]byte both produce a fixed named \"duration\"")
	}
	if !strings.Contains(err.Error(), "duration") {
		t.Fatalf("error should name the colliding type: %v", err)
	}
	if !strings.Contains(err.Error(), "two different") {
		t.Fatalf("conflict should be caught by SchemaFor's dedup, not the Parse fallback: %v", err)
	}
}

// default= takes the remainder of the tag verbatim, so a string default
// whose value contains unbalanced parens/brackets — or commas, or JSON
// object braces — must be preserved rather than rejected by the tag
// bracket-balance scan (which exists only for the alias=[...] / decimal(...)
// option forms).
func TestMatrix_SchemaForDefaultWithBrackets(t *testing.T) {
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
func TestMatrix_SchemaForNarrowIntDefaultBounds(t *testing.T) {
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
	seen := make(map[reflect.Type]seenForm)
	s, err := inferRecord(st, "R", "", seen, nil, make(appliedTypeAliases))
	if err != nil {
		return nil, err
	}
	s, err = dedupNamedTypes(s, make(map[string]string), "")
	if err != nil {
		return nil, err
	}
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
	if t == avroDurationType {
		// A representative NON-ZERO duration so the parity net exercises the
		// 12-byte fixed payload rather than an all-zero wire. (Without this the
		// Struct case below would zero each uint32 field — a valid round-trip
		// but one that never moves a non-zero byte through the duration codec.)
		return reflect.ValueOf(Duration{Months: 3, Days: 4, Milliseconds: 5})
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
		reflect.TypeFor[json.Number](),         // Kind String, codec rejects → SchemaFor must reject
		reflect.TypeFor[time.Time](),           // Kind Struct → logical long
		reflect.TypeFor[time.Duration](),       // Kind Int64 → logical
		reflect.TypeFor[Duration](),            // Kind Struct → duration fixed(12), NOT a record
		reflect.TypeFor[*Duration](),           // nullable duration fixed
		reflect.TypeFor[[]Duration](),          // array of duration fixed
		reflect.TypeFor[map[string]Duration](), // map of duration fixed
		reflect.TypeFor[big.Rat](),             // requires decimal tag → reject untagged
		reflect.TypeFor[*big.Rat](),            // requires decimal tag → reject untagged
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

// ---------- schema_for_scope_test.go ----------

// A CustomType.Schema is an independently-authored schema tree with its own
// namespace scoping. SchemaFor embeds it into the tree it infers, so the
// composed schema must preserve every declared fullname exactly: the Avro
// spec ("Names") defines a type's identity as its FULLNAME, with the dotted
// name and the split name+namespace spellings denoting the same name, and
// bare references resolving in the namespace of the enclosing definition.
// These pins hold SchemaFor to that contract for the three composition
// shapes that exercise it: a namespaced type shared across fields (the
// second occurrence must reference the first by a spelling that re-binds to
// the same fullname), distinct fullnames sharing a short name (they must
// coexist), and a null-namespace type embedded under WithNamespace (its
// identity must not be captured by the surrounding namespace).

type scopePinMoney struct{ Cents int64 }

type scopePinTwoFields struct {
	F1 scopePinMoney
	F2 scopePinMoney
}

// customSchemaFor builds the CustomType wiring for a Schema-carrying custom
// used by the pins below: GoType matches the struct field, Schema supplies
// the emitted definition.
func customSchemaFor(t *testing.T, goType reflect.Type, schemaJSON string) CustomType {
	t.Helper()
	s, err := Parse(schemaJSON)
	if err != nil {
		t.Fatalf("parse custom schema: %v", err)
	}
	root := s.Root()
	return CustomType{GoType: goType, Schema: root}
}

// namedFullname reports the fullname a field's type denotes: for a named
// definition it joins the declared namespace and name; for a name REFERENCE
// (which the metadata API surfaces as a bare node whose Type holds the
// reference spelling) it is the spelling itself — a dotted reference is a
// fullname, and a bare reference is emitted only where it equals the
// referent's null-namespace fullname.
func namedFullname(n SchemaNode) string {
	switch n.Type {
	case "record", "error", "enum", "fixed":
		if n.Namespace == "" || strings.Contains(n.Name, ".") {
			return n.Name
		}
		return n.Namespace + "." + n.Name
	case "null", "boolean", "int", "long", "float", "double", "string", "bytes", "array", "map", "union":
		return ""
	default:
		return n.Type
	}
}

// A namespaced custom schema written in the SPLIT spelling ("name":"X",
// "namespace":"a"), used on two fields: one definition plus a reference
// that re-binds to the same fullname a.X. The spec makes the split and
// dotted spellings the same name, so this must behave exactly like the
// dotted-control pin below.
func TestRegression_SchemaForCustomSchemaSplitNamespaceSharedType(t *testing.T) {
	ct := customSchemaFor(t, reflect.TypeFor[scopePinMoney](),
		`{"type":"record","name":"X","namespace":"a","fields":[{"name":"n","type":"int"}]}`)
	s, err := SchemaFor[scopePinTwoFields](ct)
	if err != nil {
		t.Fatalf("SchemaFor with a split-namespace custom schema on two fields: %v", err)
	}
	root := s.Root()
	for i := range root.Fields {
		if got := namedFullname(root.Fields[i].Type); got != "a.X" {
			t.Errorf("field %q type fullname = %q, want %q", root.Fields[i].Name, got, "a.X")
		}
	}
	if _, err := Parse(s.String()); err != nil {
		t.Errorf("SchemaFor output does not re-parse: %v", err)
	}
}

type scopePinOther struct{ N int32 }

type scopePinCoexist struct {
	F1 scopePinMoney
	F2 scopePinOther
}

// Distinct fullnames that share a short name — "a.X" (split spelling) and
// null-namespace "X" — are different Avro types and must coexist in one
// SchemaFor output with both identities intact.
func TestRegression_SchemaForCustomSchemaShortNameAcrossNamespaces(t *testing.T) {
	ctA := customSchemaFor(t, reflect.TypeFor[scopePinMoney](),
		`{"type":"record","name":"X","namespace":"a","fields":[{"name":"n","type":"int"}]}`)
	ctNull := customSchemaFor(t, reflect.TypeFor[scopePinOther](),
		`{"type":"record","name":"X","fields":[{"name":"n","type":"int"}]}`)
	s, err := SchemaFor[scopePinCoexist](ctA, ctNull)
	if err != nil {
		t.Fatalf("SchemaFor with fullnames a.X and X coexisting: %v", err)
	}
	root := s.Root()
	if got := namedFullname(root.Fields[0].Type); got != "a.X" {
		t.Errorf("field F1 type fullname = %q, want %q", got, "a.X")
	}
	if got := namedFullname(root.Fields[1].Type); got != "X" {
		t.Errorf("field F2 type fullname = %q, want %q", got, "X")
	}
	if _, err := Parse(s.String()); err != nil {
		t.Errorf("SchemaFor output does not re-parse: %v", err)
	}
}

type scopePinOneField struct {
	F1 scopePinMoney
}

// A null-namespace custom schema embedded under WithNamespace: the user's
// Schema declares fullname "X" (null namespace), and embedding it inside a
// namespaced record must not let namespace inheritance capture it into
// "b.X" — that would be a wire-visible identity change breaking resolution
// against the user's own schema. The emitted definition needs the
// "namespace":"" inheritance escape.
func TestRegression_SchemaForNullNamespaceCustomUnderWithNamespace(t *testing.T) {
	ct := customSchemaFor(t, reflect.TypeFor[scopePinMoney](),
		`{"type":"record","name":"X","fields":[{"name":"n","type":"int"}]}`)
	s, err := SchemaFor[scopePinOneField](WithNamespace("b"), ct)
	if err != nil {
		t.Fatalf("SchemaFor with a null-namespace custom under WithNamespace: %v", err)
	}
	f := s.Root().Fields[0].Type
	if f.Namespace != "" || strings.Contains(f.Name, ".") {
		t.Errorf("null-namespace custom type captured into namespace %q (name %q); the Schema declared null-namespace \"X\"", f.Namespace, f.Name)
	}
	if _, err := Parse(s.String()); err != nil {
		t.Errorf("SchemaFor output does not re-parse: %v", err)
	}
}

type scopePinRecursive struct{ Next *scopePinRecursive }

type scopePinFixed [4]byte

type scopePinTwoFixed struct {
	A scopePinFixed
	B scopePinFixed
}

// Control rows for the INFERENCE-side name spellings, which use a different
// mechanism than the custom-schema dedup: seen[] registers a record under
// its fullname, so a recursive struct's self-reference must be the DOTTED
// fullname (position-independent); an inferred fixed definition carries no
// namespace attribute (it inherits the SchemaFor namespace) and its repeat
// reference is the bare short name, which binds in that same inherited
// scope. Both spellings must survive a re-parse under WithNamespace.
func TestRegression_SchemaForInferenceNameSpellings(t *testing.T) {
	s, err := SchemaFor[scopePinRecursive](WithNamespace("ns"))
	if err != nil {
		t.Fatalf("recursive struct under WithNamespace: %v", err)
	}
	if !strings.Contains(s.String(), `"ns.scopePinRecursive"`) {
		t.Errorf("recursive self-reference is not the dotted fullname: %s", s.String())
	}
	if _, err := Parse(s.String()); err != nil {
		t.Errorf("recursive output does not re-parse: %v", err)
	}

	s, err = SchemaFor[scopePinTwoFixed](WithNamespace("ns"))
	if err != nil {
		t.Fatalf("repeated fixed under WithNamespace: %v", err)
	}
	root := s.Root()
	if got := namedFullname(root.Fields[0].Type); got != "ns.scopePinFixed" {
		t.Errorf("fixed definition fullname = %q, want %q", got, "ns.scopePinFixed")
	}
	if got := root.Fields[1].Type.Type; got != "scopePinFixed" {
		t.Errorf("fixed repeat reference = %q, want the bare short name binding in the inherited scope", got)
	}
	if _, err := Parse(s.String()); err != nil {
		t.Errorf("fixed output does not re-parse: %v", err)
	}
}

type scopePinTagged struct {
	F scopePinMoney `avro:"f,uuid"`
}

type scopePinTaggedDecimal struct {
	F scopePinMoney `avro:"f,decimal(9,2)"`
}

// A logical-type tag on a field whose type matches a CustomType has no
// effect — the custom supplies the schema — so accepting it would silently
// drop the user's tag, the exact lying-schema outcome the logical-tag
// strictness rejects everywhere else (a tag that cannot be honored is an
// error, mirroring the avro.Duration and uuid/decimal wrong-kind rejects).
func TestRegression_SchemaForLogicalTagOnCustomMatchedFieldRejected(t *testing.T) {
	ct := customSchemaFor(t, reflect.TypeFor[scopePinMoney](),
		`{"type":"record","name":"M","fields":[{"name":"c","type":"long"}]}`)
	if _, err := SchemaFor[scopePinTagged](ct); err == nil || !strings.Contains(err.Error(), "has no effect") {
		t.Errorf("uuid tag on a custom-matched field must be rejected, got: %v", err)
	}
	if _, err := SchemaFor[scopePinTaggedDecimal](ct); err == nil || !strings.Contains(err.Error(), "has no effect") {
		t.Errorf("decimal tag on a custom-matched field must be rejected, got: %v", err)
	}
	// Control: the same custom without a tag still builds.
	type plain struct{ F scopePinMoney }
	if _, err := SchemaFor[plain](ct); err != nil {
		t.Errorf("untagged custom-matched field must build: %v", err)
	}
}

// SchemaFor builds on a private copy of a CustomType.Schema's rendered
// tree: the metadata walk hands Props container values over by reference
// when they need no JSON fixup, and the composition walkers (namespace
// pinning, named-type dedup) write into the tree they are given — so
// without the copy a build would write into the caller's own storage.
func TestRegression_SchemaForLeavesCallerSchemaStorageUnmutated(t *testing.T) {
	userOwned := map[string]any{"type": "fixed", "name": "F", "size": 1}
	want := map[string]any{"type": "fixed", "name": "F", "size": 1}
	ct := CustomType{
		GoType: reflect.TypeFor[scopePinMoney](),
		Schema: &SchemaNode{Type: "string", Props: map[string]any{"items": userOwned}},
	}
	if _, err := SchemaFor[scopePinOneField](WithNamespace("com.example"), ct); err != nil {
		t.Fatalf("build: %v", err)
	}
	if !reflect.DeepEqual(userOwned, want) {
		t.Fatalf("SchemaFor mutated caller-owned Props storage:\n got:  %v\n want: %v", userOwned, want)
	}
}

// Reserved attribute names match only their exact lowercase spelling (see
// Schema.Root's doc): a Props key differing from "namespace" only by
// letter case is an ordinary custom property, NOT a namespace
// declaration. The SchemaFor composition walkers read the same exact keys
// Parse binds, so the type's identity is its null-namespace fullname, the
// second occurrence references that fullname, and the variant key rides
// along as an inert prop.
func TestRegression_SchemaForCaseVariantNamespaceKeySharedType(t *testing.T) {
	ct := CustomType{
		GoType: reflect.TypeFor[scopePinMoney](),
		Schema: &SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"NAMESPACE": "x.y"}},
	}
	s, err := SchemaFor[scopePinTwoFields](ct)
	if err != nil {
		t.Fatalf("variant-namespace-prop custom on two fields: %v", err)
	}
	root := s.Root()
	for i := range root.Fields {
		if got := namedFullname(root.Fields[i].Type); got != "F" {
			t.Errorf("field %q type fullname = %q, want %q (a NAMESPACE variant key must not scope the type)", root.Fields[i].Name, got, "F")
		}
	}
	if got := root.Fields[0].Type.Props["NAMESPACE"]; !reflect.DeepEqual(got, "x.y") {
		t.Errorf(`definition Props["NAMESPACE"] = %#v; want the variant preserved verbatim`, got)
	}
	if _, err := Parse(s.String()); err != nil {
		t.Errorf("SchemaFor output does not re-parse: %v", err)
	}
}

// Under WithNamespace the frontier pin sees no exact "namespace" key on
// the node (the case-variant Props key is an ordinary custom property),
// so it injects the exact-case "namespace":"" inheritance escape exactly
// as it does for any null-namespace type: the type's identity stays its
// null-namespace fullname F under the surrounding com.example scope, and
// the variant prop rides along untouched.
func TestRegression_SchemaForCaseVariantNamespaceUnderWithNamespace(t *testing.T) {
	ct := CustomType{
		GoType: reflect.TypeFor[scopePinMoney](),
		Schema: &SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"NAMESPACE": "x.y"}},
	}
	s, err := SchemaFor[scopePinOneField](WithNamespace("com.example"), ct)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	node := s.Root().Fields[0].Type
	if got := namedFullname(node); got != "F" {
		t.Errorf("fixed fullname = %q, want %q (the variant key declares nothing; the injected escape keeps the null namespace)", got, "F")
	}
	if got := node.Props["NAMESPACE"]; !reflect.DeepEqual(got, "x.y") {
		t.Errorf(`Props["NAMESPACE"] = %#v; want the variant preserved verbatim`, got)
	}
}

// A CustomType.Schema whose rendered tree exceeds the schema-tree budgets
// must fail the build with the budget error. SchemaFor has an error
// channel, so the silent truncate-to-nil posture of the error-less
// surfaces (Schema.String, MarshalJSON) does not apply here: silently
// replacing an over-budget Props value with null would alter the user's
// schema, and the composed output still parses (a null prop is valid), so
// no downstream Parse catches it.
func TestRegression_SchemaForOverBudgetCustomSchemaErrors(t *testing.T) {
	huge := strings.Repeat("x", 1<<26+1024) // just over the tree byte budget
	ct := CustomType{
		GoType: reflect.TypeFor[scopePinMoney](),
		Schema: &SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"p": huge}},
	}
	if _, err := SchemaFor[scopePinOneField](ct); err == nil || !strings.Contains(err.Error(), "bytes") {
		t.Fatalf("over-budget custom schema must fail the build with the budget error, got: %v", err)
	}
}

// Every axis of the schema-tree walk budget must surface as a build error
// from SchemaFor, matching the error-reporting posture of SchemaNode.Schema
// (the same deduper-carrying walk): the BYTES axis (scalar payload), the
// NODES axis (emitted node count), and the unnamed-cycle detection. A
// modest schema stays well under every budget (the success control).
func TestMatrix_SchemaForCustomSchemaBudgetAxes(t *testing.T) {
	build := func(node *SchemaNode) error {
		ct := CustomType{GoType: reflect.TypeFor[scopePinMoney](), Schema: node}
		_, err := SchemaFor[scopePinOneField](ct)
		return err
	}

	t.Run("bytes", func(t *testing.T) {
		huge := strings.Repeat("x", 1<<26+1024)
		err := build(&SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"p": huge}})
		if err == nil || !strings.Contains(err.Error(), "bytes") {
			t.Fatalf("bytes-axis overflow must fail the build with the budget error, got: %v", err)
		}
	})
	t.Run("nodes", func(t *testing.T) {
		wide := make([]any, 1<<20+1024)
		for i := range wide {
			wide[i] = 0
		}
		err := build(&SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"p": wide}})
		if err == nil || !strings.Contains(err.Error(), "nodes") {
			t.Fatalf("nodes-axis overflow must fail the build with the budget error, got: %v", err)
		}
	})
	t.Run("cycle", func(t *testing.T) {
		n := &SchemaNode{Type: "array"}
		n.Items = n
		err := build(n)
		if err == nil || !strings.Contains(err.Error(), "cyclic") {
			t.Fatalf("an unnamed pointer cycle must fail the build with the cycle error, got: %v", err)
		}
	})
	t.Run("control", func(t *testing.T) {
		if err := build(&SchemaNode{Type: "fixed", Name: "F", Size: 4,
			Props: map[string]any{"p": strings.Repeat("x", 1<<10)}}); err != nil {
			t.Fatalf("a modest custom schema must build: %v", err)
		}
	})
}

// Control: the DOTTED spelling of the shared-type pin. The parser stores a
// dotted name verbatim, so this spelling worked before the split spelling
// did; it must keep working, and per the spec the two spellings must agree.
func TestRegression_SchemaForDottedCustomSchemaControl(t *testing.T) {
	ct := customSchemaFor(t, reflect.TypeFor[scopePinMoney](),
		`{"type":"record","name":"a.X","fields":[{"name":"n","type":"int"}]}`)
	s, err := SchemaFor[scopePinTwoFields](ct)
	if err != nil {
		t.Fatalf("SchemaFor with a dotted-name custom schema on two fields: %v", err)
	}
	root := s.Root()
	for i := range root.Fields {
		if got := namedFullname(root.Fields[i].Type); got != "a.X" {
			t.Errorf("field %q type fullname = %q, want %q", root.Fields[i].Name, got, "a.X")
		}
	}
	if _, err := Parse(s.String()); err != nil {
		t.Errorf("SchemaFor output does not re-parse: %v", err)
	}
}

// ---------- schema_for_straykey_test.go ----------

// Marker Go types for the stray-structural-key pins. Identity only matters
// within one test.
type (
	strayKeyCarrier struct{ X int64 }
	strayKeyRealA   struct{ Y int64 }
	strayKeyRealB   struct{ Z int64 }
)

// strayNXDef returns a fresh named-record definition tree, the shape a
// caller can legally park under a reserved structural key in Props (the
// parser captures such a key as inert metadata on kinds that do not bind
// it — see the structural-key routing on aobjectFromMap — so the value is
// never a name-binding definition).
func strayNXDef(fieldType string) map[string]any {
	return map[string]any{
		"type": "record", "name": "n.X",
		"fields": []any{map[string]any{"name": "a", "type": fieldType}},
	}
}

// realNXNode returns a caller SchemaNode defining n.X with an int field.
func realNXNode() *SchemaNode {
	return &SchemaNode{
		Type: "record", Name: "n.X",
		Fields: []SchemaField{{Name: "a", Type: SchemaNode{Type: "int"}}},
	}
}

// jsonReencode round-trips v through JSON so trees built with different
// container types (e.g. []any vs []map[string]any) and numeric widths
// compare structurally.
func jsonReencode(t *testing.T, v any) any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

// strayUnderF1 extracts fields[0].type[key] from a schema's stored text.
func strayUnderF1(t *testing.T, s *Schema, key string) any {
	t.Helper()
	var root map[string]any
	if err := json.Unmarshal([]byte(s.String()), &root); err != nil {
		t.Fatalf("composed schema text does not unmarshal: %v", err)
	}
	fields, _ := root["fields"].([]any)
	if len(fields) == 0 {
		t.Fatalf("composed schema has no fields: %s", s.String())
	}
	f0, _ := fields[0].(map[string]any)
	typ, _ := f0["type"].(map[string]any)
	if typ == nil {
		t.Fatalf("composed F1 type is not an object: %s", s.String())
	}
	return typ[key]
}

// A named definition parked under a stray "items" key on a primitive-kind
// CustomType.Schema is inert to Parse (never name-bound), so it must not
// register in the composition's dedup table. Here the stray body differs
// from the real definition of the same fullname: the build must accept —
// the parser binds n.X exactly once, at F2 — not report a false duplicate.
func TestRegression_StrayStructuralKeyFalseDuplicate(t *testing.T) {
	stray := strayNXDef("long") // differs from the real def (int)
	ct1 := CustomType{
		GoType: reflect.TypeFor[strayKeyCarrier](),
		Schema: &SchemaNode{Type: "int", Props: map[string]any{"items": stray}},
	}
	ct2 := CustomType{GoType: reflect.TypeFor[strayKeyRealA](), Schema: realNXNode()}
	type S struct {
		F1 strayKeyCarrier
		F2 strayKeyRealA
	}

	counterfactual := `{"type":"record","name":"S","fields":[
		{"name":"F1","type":{"type":"int","items":{"type":"record","name":"n.X","fields":[{"name":"a","type":"long"}]}}},
		{"name":"F2","type":{"type":"record","name":"n.X","fields":[{"name":"a","type":"int"}]}}]}`
	if _, err := Parse(counterfactual); err != nil {
		t.Fatalf("hand-composed counterfactual does not parse — the stray is not inert as documented: %v", err)
	}

	s, err := SchemaFor[S](WithCustomType(ct1), WithCustomType(ct2))
	if err != nil {
		t.Fatalf("build rejected a composition whose composed tree is Parse-valid: %v", err)
	}
	if got, want := strayUnderF1(t, s, "items"), jsonReencode(t, strayNXDef("long")); !reflect.DeepEqual(got, want) {
		t.Errorf("stray value altered by composition:\n got:  %#v\n want: %#v", got, want)
	}
}

// Same shape with the stray body IDENTICAL to the real definition: the
// real definition at F2 must stay a full inline definition. Deduping it
// into a reference to the stray-parked copy dangles — Parse never binds a
// definition inside an inert stray.
func TestRegression_StrayStructuralKeyDanglingRef(t *testing.T) {
	stray := strayNXDef("int")
	ct1 := CustomType{
		GoType: reflect.TypeFor[strayKeyCarrier](),
		Schema: &SchemaNode{Type: "int", Props: map[string]any{"items": stray}},
	}
	ct2 := CustomType{GoType: reflect.TypeFor[strayKeyRealA](), Schema: realNXNode()}
	type S struct {
		F1 strayKeyCarrier
		F2 strayKeyRealA
	}

	counterfactual := `{"type":"record","name":"S","fields":[
		{"name":"F1","type":{"type":"int","items":{"type":"record","name":"n.X","fields":[{"name":"a","type":"int"}]}}},
		{"name":"F2","type":{"type":"record","name":"n.X","fields":[{"name":"a","type":"int"}]}}]}`
	if _, err := Parse(counterfactual); err != nil {
		t.Fatalf("hand-composed counterfactual does not parse — the stray is not inert as documented: %v", err)
	}

	s, err := SchemaFor[S](WithCustomType(ct1), WithCustomType(ct2))
	if err != nil {
		t.Fatalf("build rejected a composition whose composed tree is Parse-valid: %v", err)
	}
	if got, want := strayUnderF1(t, s, "items"), jsonReencode(t, strayNXDef("int")); !reflect.DeepEqual(got, want) {
		t.Errorf("stray value altered by composition:\n got:  %#v\n want: %#v", got, want)
	}
}

// Under WithNamespace, the scope pin must not inject "namespace":"" into a
// named-kind-shaped value inside an inert stray — the parser treats the
// stray as captured metadata, so the injection is a silent alteration of
// caller metadata in the stored schema text.
func TestRegression_StrayStructuralKeyPinInjection(t *testing.T) {
	stray := map[string]any{
		"type": "record", "name": "Bare",
		"fields": []any{map[string]any{"name": "a", "type": "int"}},
	}
	ct := CustomType{
		GoType: reflect.TypeFor[strayKeyCarrier](),
		Schema: &SchemaNode{Type: "int", Props: map[string]any{"items": stray}},
	}
	type S struct{ F1 strayKeyCarrier }
	s, err := SchemaFor[S](WithCustomType(ct), WithNamespace("x.y"))
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	want := jsonReencode(t, map[string]any{
		"type": "record", "name": "Bare",
		"fields": []any{map[string]any{"name": "a", "type": "int"}},
	})
	if got := strayUnderF1(t, s, "items"); !reflect.DeepEqual(got, want) {
		t.Errorf("stray value altered by composition (scope pin wrote into it):\n got:  %#v\n want: %#v", got, want)
	}
}

// Stray values are inert as-written metadata, so the dedup identity
// compare treats them VERBATIM: two same-fullname definitions differing
// only in the spelling of a stray value (dotted name vs name+namespace
// attribute, parked on an inner primitive where the parser accepts the
// stray as inert) are different definitions. Collapsing them to one would
// silently discard one spelling; keeping both inline cannot parse (a
// fullname defines once). The build must reject exactly as Parse rejects
// the inline pair.
func TestRegression_StrayStructuralKeyVerbatimCompare(t *testing.T) {
	strayAttr := map[string]any{"type": "record", "name": "Bare", "namespace": "q",
		"fields": []any{map[string]any{"name": "a", "type": "int"}}}
	strayDotted := map[string]any{"type": "record", "name": "q.Bare",
		"fields": []any{map[string]any{"name": "a", "type": "int"}}}

	nodeWith := func(stray map[string]any) *SchemaNode {
		return &SchemaNode{
			Type: "record", Name: "n.X",
			Fields: []SchemaField{
				{Name: "a", Type: SchemaNode{Type: "int"}},
				{Name: "b", Type: SchemaNode{Type: "int", Props: map[string]any{"items": stray}}},
			},
		}
	}
	ct1 := CustomType{GoType: reflect.TypeFor[strayKeyRealA](), Schema: nodeWith(strayAttr)}
	ct2 := CustomType{GoType: reflect.TypeFor[strayKeyRealB](), Schema: nodeWith(strayDotted)}

	// A single occurrence is fine: the stray on the inner int is inert.
	type S1 struct{ F1 strayKeyRealA }
	if _, err := SchemaFor[S1](WithCustomType(ct1)); err != nil {
		t.Fatalf("single-occurrence control failed — the stray is not inert where placed: %v", err)
	}

	// The inline pair cannot parse: n.X defines twice.
	counterfactual := `{"type":"record","name":"S","fields":[
		{"name":"F1","type":{"type":"record","name":"n.X","fields":[{"name":"a","type":"int"},{"name":"b","type":{"type":"int","items":{"type":"record","name":"Bare","namespace":"q","fields":[{"name":"a","type":"int"}]}}}]}},
		{"name":"F2","type":{"type":"record","name":"n.X","fields":[{"name":"a","type":"int"},{"name":"b","type":{"type":"int","items":{"type":"record","name":"q.Bare","fields":[{"name":"a","type":"int"}]}}}]}}]}`
	if _, err := Parse(counterfactual); err == nil {
		t.Fatalf("counterfactual inline pair unexpectedly parsed; the scenario premise is wrong")
	}

	type S struct {
		F1 strayKeyRealA
		F2 strayKeyRealB
	}
	if _, err := SchemaFor[S](WithCustomType(ct1), WithCustomType(ct2)); err == nil {
		t.Fatalf("build accepted two same-fullname definitions differing in an inert stray value's as-written spelling; the parser cannot represent both")
	}
}

// Control: the identical builds without the stray succeed.
func TestStrayStructuralKeyControl(t *testing.T) {
	ct1 := CustomType{GoType: reflect.TypeFor[strayKeyCarrier](), Schema: &SchemaNode{Type: "int"}}
	ct2 := CustomType{GoType: reflect.TypeFor[strayKeyRealA](), Schema: realNXNode()}
	type S struct {
		F1 strayKeyCarrier
		F2 strayKeyRealA
	}
	if _, err := SchemaFor[S](WithCustomType(ct1), WithCustomType(ct2)); err != nil {
		t.Fatalf("no-stray control failed: %v", err)
	}
	type S2 struct{ F1 strayKeyRealA }
	if _, err := SchemaFor[S2](WithCustomType(ct2), WithNamespace("x.y")); err != nil {
		t.Fatalf("no-stray namespaced control failed: %v", err)
	}
}

// strayMatrixThird is the matrix's third marker type (alongside
// scopeMatrixPrimary / scopeMatrixPartner) so one cell can carry a
// stray-key custom plus two same-definition customs.
type strayMatrixThird struct{ C int64 }

// TestMatrix_SchemaForStrayStructuralKey crosses every carrier kind with
// every structural key the kind does NOT bind, a spread of stray bodies,
// both build scopes, and one-vs-two occurrences of a genuine same-fullname
// definition. The per-cell oracle is the parser itself:
//
//   - verdict parity: SchemaFor's accept/reject equals Parse's verdict on
//     the hand-composed counterfactual tree carrying the same stray
//     verbatim (kind-keyed grammar: a container kind carrying another
//     kind's defining key hard-rejects; a primitive captures it inert);
//   - preservation: on accepted cells the stray survives byte-identical
//     in the composed schema text — never walked, never rewritten, never
//     injected into;
//   - genuine behavior: the real definition stays a full inline body and
//     a second occurrence still dedups to a name reference.
//
// The cells where the key IS the kind's defining key (array items, map
// values, record fields) are the genuine-schema controls pinned by the
// scope and casefold matrices, so they are skipped here.
func TestMatrix_SchemaForStrayStructuralKey(t *testing.T) {
	// bodyJSON builds a FRESH tree per call: the planted copy and the
	// counterfactual copy must be independent so a (hypothetically)
	// misbehaving walker mutating one cannot corrupt the other's oracle.
	bodyJSON := func(body string) any {
		switch body {
		case "identdef":
			return map[string]any{"type": "record", "name": "n.X",
				"fields": []any{map[string]any{"name": "a", "type": "int"}}}
		case "diffdef":
			return map[string]any{"type": "record", "name": "n.X",
				"fields": []any{map[string]any{"name": "a", "type": "long"}}}
		case "baredef":
			// Bare-named: the shape the scope pin's injection arm targets
			// (a dotted name pins its own scope and is skipped).
			return map[string]any{"type": "record", "name": "Bare",
				"fields": []any{map[string]any{"name": "a", "type": "int"}}}
		case "plain":
			return map[string]any{"type": "array", "items": "long"}
		case "nonschema":
			return 42
		}
		return nil
	}

	carrierNode := func(kind string) *SchemaNode {
		switch kind {
		case "fixed":
			return &SchemaNode{Type: "fixed", Name: "FX", Size: 2}
		case "enum":
			return &SchemaNode{Type: "enum", Name: "EN", Symbols: []string{"A"}}
		case "record":
			return &SchemaNode{Type: "record", Name: "RC",
				Fields: []SchemaField{{Name: "a", Type: SchemaNode{Type: "int"}}}}
		case "array":
			return &SchemaNode{Type: "array", Items: &SchemaNode{Type: "int"}}
		case "map":
			return &SchemaNode{Type: "map", Values: &SchemaNode{Type: "int"}}
		}
		return &SchemaNode{Type: kind}
	}
	carrierJSON := func(kind string) map[string]any {
		switch kind {
		case "fixed":
			return map[string]any{"type": "fixed", "name": "FX", "size": 2}
		case "enum":
			return map[string]any{"type": "enum", "name": "EN", "symbols": []any{"A"}}
		case "record":
			return map[string]any{"type": "record", "name": "RC",
				"fields": []any{map[string]any{"name": "a", "type": "int"}}}
		case "array":
			return map[string]any{"type": "array", "items": "int"}
		case "map":
			return map[string]any{"type": "map", "values": "int"}
		}
		return map[string]any{"type": kind}
	}

	definingKey := map[string]string{"array": "items", "map": "values", "record": "fields"}

	for _, kind := range []string{"int", "string", "fixed", "enum", "record", "array", "map"} {
		for _, key := range []string{"items", "values", "fields"} {
			if definingKey[kind] == key {
				continue // the genuine schema position, not a stray
			}
			for _, route := range []string{"props", "typed"} {
				for _, body := range []string{"identdef", "diffdef", "baredef", "plain", "nonschema"} {
					if route == "typed" && body == "nonschema" {
						continue // a non-schema value has no SchemaNode spelling
					}
					for _, ns := range []string{"", "b"} {
						if route == "typed" && ns != "" {
							// The typed route's expected stray image is the
							// node's render, which at a namespaced scope adds
							// the "namespace":"" escape for bare-named defs;
							// the props route covers the ns axis.
							continue
						}
						for _, occ := range []int{1, 2} {
							name := kind + "/" + key + "/" + body + "/occ" + string(rune('0'+occ))
							if ns != "" {
								name += "/ns"
							}
							if route == "typed" {
								name += "/typed"
							}
							t.Run(name, func(t *testing.T) {
								// Two planting routes for a key the node's
								// kind does not bind. "props": the value rides
								// in Props and the render emits it verbatim.
								// "typed": the caller sets the STRUCTURAL
								// field (Items/Values/Fields) directly; the
								// render preserves it as-written too — the
								// bare-string emission requires structural
								// emptiness, so a stray-carrying primitive
								// takes the object render. Both routes
								// compose the same schema text.
								// A "fields" stray wraps its body in a proper
								// field list so the stray itself decodes.
								strayFor := func() any {
									switch {
									case body == "nonschema":
										return 42
									case key == "fields":
										return []any{map[string]any{"name": "f", "type": bodyJSON(body)}}
									}
									return bodyJSON(body)
								}
								bodyNode := func() *SchemaNode {
									switch body {
									case "identdef":
										return &SchemaNode{Type: "record", Name: "n.X",
											Fields: []SchemaField{{Name: "a", Type: SchemaNode{Type: "int"}}}}
									case "diffdef":
										return &SchemaNode{Type: "record", Name: "n.X",
											Fields: []SchemaField{{Name: "a", Type: SchemaNode{Type: "long"}}}}
									case "baredef":
										return &SchemaNode{Type: "record", Name: "Bare",
											Fields: []SchemaField{{Name: "a", Type: SchemaNode{Type: "int"}}}}
									case "plain":
										return &SchemaNode{Type: "array", Items: &SchemaNode{Type: "long"}}
									}
									return nil
								}
								carrier := carrierNode(kind)
								if route == "props" {
									carrier.Props = map[string]any{key: strayFor()}
								} else {
									switch key {
									case "items":
										carrier.Items = bodyNode()
									case "values":
										carrier.Values = bodyNode()
									case "fields":
										carrier.Fields = []SchemaField{{Name: "f", Type: *bodyNode()}}
									}
								}
								strayJSON := strayFor()

								customs := []CustomType{
									{GoType: reflect.TypeFor[scopeMatrixPrimary](), Schema: carrier},
									{GoType: reflect.TypeFor[scopeMatrixPartner](), Schema: realNXNode()},
								}
								fields := []reflect.StructField{
									{Name: "F1", Type: reflect.TypeFor[scopeMatrixPrimary]()},
									{Name: "F2", Type: reflect.TypeFor[scopeMatrixPartner]()},
								}
								if occ == 2 {
									customs = append(customs,
										CustomType{GoType: reflect.TypeFor[strayMatrixThird](), Schema: realNXNode()})
									fields = append(fields,
										reflect.StructField{Name: "F3", Type: reflect.TypeFor[strayMatrixThird]()})
								}

								// Hand-composed counterfactual: same carrier +
								// stray verbatim, the real definition inline
								// once, a reference at the second occurrence.
								cfCarrier := carrierJSON(kind)
								cfCarrier[key] = strayJSON
								cfFields := []any{
									map[string]any{"name": "F1", "type": cfCarrier},
									map[string]any{"name": "F2", "type": bodyJSON("identdef")},
								}
								if occ == 2 {
									cfFields = append(cfFields, map[string]any{"name": "F3", "type": "n.X"})
								}
								cfRoot := map[string]any{"type": "record", "name": "Top", "fields": cfFields}
								if ns != "" {
									cfRoot["namespace"] = ns
								}
								cfText, err := json.Marshal(cfRoot)
								if err != nil {
									t.Fatalf("marshal counterfactual: %v", err)
								}
								_, cfErr := Parse(string(cfText))

								s, err := schemaForScopeCell(t, fields, ns, customs)
								if (err == nil) != (cfErr == nil) {
									t.Fatalf("verdict parity broken:\n build: %v\n parse of counterfactual: %v", err, cfErr)
								}
								if err != nil {
									return // reject cell: parity established
								}

								var root map[string]any
								if err := json.Unmarshal([]byte(s.String()), &root); err != nil {
									t.Fatalf("composed text: %v", err)
								}
								composed, _ := root["fields"].([]any)
								if len(composed) < 2 {
									t.Fatalf("composed fields missing: %s", s.String())
								}
								f1type, _ := composed[0].(map[string]any)["type"].(map[string]any)
								if f1type == nil {
									t.Fatalf("composed F1 type not an object: %s", s.String())
								}
								if got, want := f1type[key], jsonReencode(t, strayJSON); !reflect.DeepEqual(got, want) {
									t.Errorf("stray not preserved verbatim:\n got:  %#v\n want: %#v", got, want)
								}
								f2type, _ := composed[1].(map[string]any)["type"].(map[string]any)
								if f2type == nil || f2type["name"] != "n.X" {
									t.Errorf("real definition not inline at F2: %s", s.String())
								}
								if occ == 2 {
									if ref, _ := composed[2].(map[string]any)["type"].(string); ref != "n.X" {
										t.Errorf("second genuine occurrence did not dedup to a reference: %s", s.String())
									}
								}
							})
						}
					}
				}
			}
		}
	}
}

type strayKeyFixed16 [16]byte

// A build never writes into caller-owned SchemaNode storage — including
// the [len:cap) region of a caller's Aliases backing array, which a
// deep-equal snapshot cannot see. The type-alias tag appends to the
// rendered type's aliases; with spare capacity in the caller's slice that
// append must land in the build's own copy, not the caller's array.
func TestRegression_TypeAliasSpareCapacityOwnership(t *testing.T) {
	backing := []string{"Old", "KeepMe"}
	node := &SchemaNode{
		Type: "fixed", Name: "F16", Size: 16,
		Aliases: backing[:1:2], // len 1, cap 2: one spare slot over caller memory
	}
	ct := CustomType{GoType: reflect.TypeFor[strayKeyFixed16](), Schema: node}
	type S struct {
		F strayKeyFixed16 `avro:",type-alias=NewAlias"`
	}
	s, err := SchemaFor[S](WithCustomType(ct))
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	if backing[1] != "KeepMe" {
		t.Fatalf("build wrote past len into the caller's aliases backing array: %q", backing[1])
	}
	// The tag still applies: the composed type carries both aliases.
	var root map[string]any
	if err := json.Unmarshal([]byte(s.String()), &root); err != nil {
		t.Fatalf("unmarshal composed: %v", err)
	}
	fields := root["fields"].([]any)
	typ := fields[0].(map[string]any)["type"].(map[string]any)
	if got := typ["aliases"]; !reflect.DeepEqual(got, []any{"Old", "NewAlias"}) {
		t.Fatalf("composed aliases = %#v, want [Old NewAlias]", got)
	}
}

// TestMatrix_TypeAliasAliasOwnership drives a type-alias'd field through
// every named kind whose CustomType.Schema carries caller []string inputs
// (type aliases; enum symbols; record field aliases), at both build
// scopes. The harness plants a sentinel past the length of every such
// slice, so any append the build makes into caller-owned backing memory —
// rather than into its own copy — fails the cell; the composed type must
// still carry the declared aliases plus the tag alias.
func TestMatrix_TypeAliasAliasOwnership(t *testing.T) {
	for _, kind := range []string{"fixed", "enum", "record"} {
		for _, ns := range []string{"", "b"} {
			name := kind
			if ns != "" {
				name += "/ns"
			}
			t.Run(name, func(t *testing.T) {
				var node *SchemaNode
				var declared string
				switch kind {
				case "fixed":
					declared = "OldF"
					node = &SchemaNode{Type: "fixed", Name: "FX", Size: 2,
						Aliases: []string{declared}}
				case "enum":
					declared = "OldE"
					node = &SchemaNode{Type: "enum", Name: "EN", Symbols: []string{"A"},
						Aliases: []string{declared}}
				case "record":
					declared = "OldR"
					node = &SchemaNode{Type: "record", Name: "RC",
						Aliases: []string{declared},
						Fields: []SchemaField{{Name: "a", Type: SchemaNode{Type: "int"},
							Aliases: []string{"olda"}}}}
				}
				customs := []CustomType{{GoType: reflect.TypeFor[scopeMatrixPrimary](), Schema: node}}
				fields := []reflect.StructField{{
					Name: "F1",
					Type: reflect.TypeFor[scopeMatrixPrimary](),
					Tag:  `avro:",type-alias=Extra"`,
				}}
				s, err := schemaForScopeCell(t, fields, ns, customs)
				if err != nil {
					t.Fatalf("build failed: %v", err)
				}
				var root map[string]any
				if err := json.Unmarshal([]byte(s.String()), &root); err != nil {
					t.Fatalf("composed text: %v", err)
				}
				typ := root["fields"].([]any)[0].(map[string]any)["type"].(map[string]any)
				if got := typ["aliases"]; !reflect.DeepEqual(got, []any{declared, "Extra"}) {
					t.Errorf("composed aliases = %#v, want [%s Extra]", got, declared)
				}
			})
		}
	}
}

// ---------- schemafor_roundtrip_generative_test.go ----------

// ===========================================================================
// The generative SchemaFor round-trip self-consistency net.
//
// SchemaFor (Go type -> Avro schema) is twmb-unique: there is no spec, no Java,
// and no fastavro counterpart to differential against (Go->Avro inference is not
// a standardized transform), so the schema->value matrix, the encode/decode
// parity invariant, the fastavro/Java oracles, and a byte-fuzzer all sail past
// it (a fuzzer cannot synthesize a Go field type; SchemaFor[T] is compile-time
// generic). Its one machine-checkable contract is ROUND-TRIP SELF-CONSISTENCY:
// for every Go type T, SchemaFor[T] must EITHER
//
//	(a) build a schema that twmb's OWN codecs round-trip a value of T — through
//	    BOTH the binary (Encode/Decode) AND the JSON (EncodeJSON/DecodeJSON)
//	    wire, so a binary-vs-JSON path divergence is caught too — OR
//	(b) reject cleanly at build time (a non-empty error, no panic).
//
// The forbidden outcome is the highest-yield SchemaFor bug shape, build-accepts/
// encode-rejects: SchemaFor returns a schema, but Encode (or Decode) of a value
// of that very type fails far from the SchemaFor call — the schema "lies" about
// the Go type. Every historical SchemaFor bug is an instance: json.Number
// inferred as "string" (the codec rejects it), a one-directional text type
// inferred as "string" (encode XOR decode fails), a pointer chain deeper than
// the codec unwraps inferred as ["null",T] (Encode of a non-nil value fails).
//
// ONE generator crosses the four axes those bugs live at, and the SAME oracle
// runs on every cell:
//
//	Go type shape  x  struct tag  x  text interface  x  logical type
//
//	shape   : direct, *L, **L, at-cap and past-cap pointer chains, []L, [N]L,
//	          map[string]L, []*L (and recursive-struct + named-struct leaves)
//	tag     : none, rename, alias=, default=, every logical (valid- AND
//	          wrong-underlying for the leaf), decimal(p,s), and malformed forms
//	          (unknown option, unclosed bracket, decimal trailing junk, empty
//	          alias, the "-,opt" skip-with-options)
//	text    : a leaf implementing none / MarshalText-only / UnmarshalText-only /
//	          both, over string / []byte / non-string(struct) base kinds
//	logical : a logical whose required underlying wire MATCHES the leaf
//	          (date-on-int, uuid-on-[16]byte, timestamp-on-time.Time) and one
//	          whose underlying is WRONG for it (uuid-on-int, decimal-on-string)
//
// RECONCILIATION (not duplication): this net reuses the package's existing
// SchemaFor infrastructure rather than re-deriving it —
//   - schemaForType (embed_shape_generative_test.go): the reflect.Type-driven
//     replica of SchemaFor, pinned byte-identical to the generic entry point by
//     TestGenerative_SchemaForReplicaParity. The bulk of the cross is built from
//     reflect.StructOf types that SchemaFor[T] cannot take at run time;
//     rtRealEntryPointCells additionally drives the REAL SchemaFor[T] on the
//     compile-time-nameable leaves so the replica's fidelity is bridged.
//   - sampleValue (schema_for_test.go, made cycle-safe): the non-empty value
//     builder, so a leaf buried in *T / []T / map[K]T actually reaches the codec.
//   - the malformed-tag alphabet is the same family embed_shape_tagedge_test.go's
//     tagDefects pins for WALKER AGREEMENT; here the same tags are crossed with
//     the whole leaf x shape space under the ROUND-TRIP oracle instead.
//
// The per-axis nets each fix ONE axis: TestGenerative_EmbedShapeWalkerAgreement
// crosses struct SHAPE with an int32 leaf (field-selection), TestSchemaFor-
// EncodeParity crosses LEAF TYPE at direct depth (no tag/shape/text cross),
// TestGenerative_TagEdgeWalkerAgreement crosses TAGS with int leaves. None
// crosses leaf x shape x tag x text x logical, where the build-accepts/encode-
// rejects interactions hide (a one-way-text type behind a pointer or inside a
// slice; a logical tag on a pointer-to-named-int; uuid on []string; decimal on a
// map). That product is this net's job. Non-vacuity is recorded at the bottom:
// reverting the one-way-text refusal, the json.Number reject, or the pointer-
// chain cap each turns a measured set of cells red.
// ===========================================================================

// ---- text-interface leaf alphabet -----------------------------------------
//
// Each base kind (non-string struct, string, []byte) x {none, MarshalText-only,
// UnmarshalText-only, both}. The non-string base is the one the one-way-text
// refusal guards: a struct that round-trips as a string ONLY if it implements
// BOTH directions (a string/[]byte kind covers the missing direction via the
// kind itself, so a one-directional method on those still builds). Methods are
// identity transforms so the "both" cells round-trip the value faithfully.

type rtStructNone struct{ S string } // no text methods -> inferred as a RECORD
type rtStructMarshal struct{ S string }

func (v rtStructMarshal) MarshalText() ([]byte, error) { return []byte(v.S), nil }

type rtStructUnmarshal struct{ S string }

func (v *rtStructUnmarshal) UnmarshalText(b []byte) error { v.S = string(b); return nil }

type rtStructBoth struct{ S string }

func (v rtStructBoth) MarshalText() ([]byte, error)  { return []byte(v.S), nil }
func (v *rtStructBoth) UnmarshalText(b []byte) error { v.S = string(b); return nil }

type rtStrPlain string // string kind, no methods -> "string"
type rtStrMarshal string

func (v rtStrMarshal) MarshalText() ([]byte, error) { return []byte(v), nil }

type rtStrUnmarshal string

func (v *rtStrUnmarshal) UnmarshalText(b []byte) error { *v = rtStrUnmarshal(b); return nil }

type rtStrBoth string

func (v rtStrBoth) MarshalText() ([]byte, error)  { return []byte(v), nil }
func (v *rtStrBoth) UnmarshalText(b []byte) error { *v = rtStrBoth(b); return nil }

type rtStrAppend string // exercises the AppendText encode arm

func (v rtStrAppend) AppendText(b []byte) ([]byte, error) { return append(b, v...), nil }
func (v *rtStrAppend) UnmarshalText(b []byte) error       { *v = rtStrAppend(b); return nil }

type rtBytesPlain []byte // []byte kind, no methods -> "bytes"
type rtBytesMarshal []byte

func (v rtBytesMarshal) MarshalText() ([]byte, error) { return append([]byte(nil), v...), nil }

type rtBytesUnmarshal []byte

func (v *rtBytesUnmarshal) UnmarshalText(b []byte) error { *v = append((*v)[:0], b...); return nil }

type rtBytesBoth []byte

func (v rtBytesBoth) MarshalText() ([]byte, error)  { return append([]byte(nil), v...), nil }
func (v *rtBytesBoth) UnmarshalText(b []byte) error { *v = append((*v)[:0], b...); return nil }

// named primitives (distinct reflect.Type that must follow its Kind honestly).
type rtNamedInt int32
type rtNamedFloat float64

// a named struct (record) leaf and a recursive-struct (linked-list) leaf.
type rtRecord struct {
	A int32  `avro:"a"`
	B string `avro:"b"`
}
type rtLinked struct {
	Val  int32     `avro:"val"`
	Next *rtLinked `avro:"next"`
}

// ---- leaf specs ------------------------------------------------------------

type rtLeaf struct {
	typ      reflect.Type
	label    string
	faithful bool // round-trip preserves the value under reflect.DeepEqual
}

func rtLeaves() []rtLeaf {
	leaf := func(t reflect.Type, faithful bool) rtLeaf {
		return rtLeaf{typ: t, label: t.String(), faithful: faithful}
	}
	return []rtLeaf{
		// primitive kinds the inference switch handles (faithful: zero/"1"
		// round-trips bit-exactly through the codec).
		leaf(reflect.TypeFor[bool](), true),
		leaf(reflect.TypeFor[int8](), true),
		leaf(reflect.TypeFor[int32](), true),
		leaf(reflect.TypeFor[int64](), true),
		leaf(reflect.TypeFor[int](), true),
		leaf(reflect.TypeFor[uint8](), true),
		leaf(reflect.TypeFor[uint32](), true),
		leaf(reflect.TypeFor[uint64](), true),
		leaf(reflect.TypeFor[float32](), true),
		leaf(reflect.TypeFor[float64](), true),
		leaf(reflect.TypeFor[string](), true),
		// byte containers
		leaf(reflect.TypeFor[[]byte](), true),
		leaf(reflect.TypeFor[[4]byte](), true),
		leaf(reflect.TypeFor[[16]byte](), true),
		// named primitives
		leaf(reflect.TypeFor[rtNamedInt](), true),
		leaf(reflect.TypeFor[rtNamedFloat](), true),
		// codec-special-cased stdlib types whose Kind misleads (non-faithful
		// under DeepEqual: the encode/decode-accepts half of the oracle still
		// catches build-accepts/encode-rejects on them).
		leaf(reflect.TypeFor[json.Number](), false),
		leaf(reflect.TypeFor[time.Time](), false),
		leaf(reflect.TypeFor[time.Duration](), false),
		// avro.Duration is a struct whose Kind would mislead to a record, but
		// inferType maps it to the duration fixed(12). It round-trips bit-exactly
		// (three uint32s), so faithful: true. Any logical TAG on it is rejected
		// (the duration logical takes no tag) — a clean reject the oracle allows.
		leaf(reflect.TypeFor[Duration](), true),
		leaf(reflect.TypeFor[big.Rat](), false),
		leaf(reflect.TypeFor[*big.Rat](), false),
		// text-interface combos over three base kinds (the text axis).
		leaf(reflect.TypeFor[rtStructNone](), true), // a record
		leaf(reflect.TypeFor[rtStructMarshal](), false),
		leaf(reflect.TypeFor[rtStructUnmarshal](), false),
		leaf(reflect.TypeFor[rtStructBoth](), false),
		leaf(reflect.TypeFor[rtStrPlain](), true),
		leaf(reflect.TypeFor[rtStrMarshal](), false),
		leaf(reflect.TypeFor[rtStrUnmarshal](), false),
		leaf(reflect.TypeFor[rtStrBoth](), false),
		leaf(reflect.TypeFor[rtStrAppend](), false),
		leaf(reflect.TypeFor[rtBytesPlain](), true),
		leaf(reflect.TypeFor[rtBytesMarshal](), false),
		leaf(reflect.TypeFor[rtBytesUnmarshal](), false),
		leaf(reflect.TypeFor[rtBytesBoth](), false),
		// named struct + recursive struct
		leaf(reflect.TypeFor[rtRecord](), true),
		leaf(reflect.TypeFor[rtLinked](), false),
		// an interface leaf -> unsupported -> clean reject on every shape.
		leaf(reflect.TypeFor[any](), false),
	}
}

// ---- shape wrappers --------------------------------------------------------
//
// Each wraps a leaf type L into the struct field's Go type. The pointer chains
// straddle the codec's maxIndirectDepth unwrap cap: at-cap must build and
// round-trip a non-nil value; past-cap must reject at build (the build-accepts/
// encode-rejects pointer shape). Containers reset the cap, so an at-cap chain
// per element still builds.

type rtShape struct {
	label string
	wrap  func(reflect.Type) reflect.Type
}

func ptrChain(t reflect.Type, n int) reflect.Type {
	for range n {
		t = reflect.PointerTo(t)
	}
	return t
}

func rtShapes() []rtShape {
	return []rtShape{
		{"direct", func(t reflect.Type) reflect.Type { return t }},
		{"ptr", func(t reflect.Type) reflect.Type { return reflect.PointerTo(t) }},
		{"ptr2", func(t reflect.Type) reflect.Type { return ptrChain(t, 2) }},
		{"ptrAtCap", func(t reflect.Type) reflect.Type { return ptrChain(t, maxIndirectDepth) }},
		{"ptrPastCap", func(t reflect.Type) reflect.Type { return ptrChain(t, maxIndirectDepth+1) }},
		{"slice", func(t reflect.Type) reflect.Type { return reflect.SliceOf(t) }},
		{"array2", func(t reflect.Type) reflect.Type { return reflect.ArrayOf(2, t) }},
		{"map", func(t reflect.Type) reflect.Type { return reflect.MapOf(reflect.TypeFor[string](), t) }},
		{"slicePtr", func(t reflect.Type) reflect.Type { return reflect.SliceOf(reflect.PointerTo(t)) }},
	}
}

// ---- tag specs -------------------------------------------------------------
//
// "f" is the field name so a missing-name fallback never masks a tag effect.
// The logical set is applied uniformly: for a leaf whose wire matches it the
// cell must round-trip; for a leaf whose wire is wrong the cell must REJECT
// (uuid-on-int, decimal-on-bool) — never build a schema the codec then fights.
// The malformed forms must all reject. omitzero is deliberately absent: it is a
// runtime encode directive (skip-when-zero) that does not shape the schema, and
// is netted by omitzero_bsoft_test.go + tag_grammar_runtime_test.go; the only
// omitzero here is the malformed "-,omitzero" build-reject.

type rtTag struct {
	label string
	tag   string
}

func rtTags() []rtTag {
	return []rtTag{
		{"none", ``},
		{"name", `avro:"f"`},
		{"alias", `avro:"f,alias=old"`},
		{"type-alias", `avro:"f,type-alias=old"`},
		{"inline", `avro:",inline"`},
		{"default", `avro:"f,default=5"`},
		// logicals (valid-underlying for some leaves, wrong for others)
		{"uuid", `avro:"f,uuid"`},
		{"timestamp-millis", `avro:"f,timestamp-millis"`},
		{"timestamp-micros", `avro:"f,timestamp-micros"`},
		{"timestamp-nanos", `avro:"f,timestamp-nanos"`},
		{"date", `avro:"f,date"`},
		{"time-millis", `avro:"f,time-millis"`},
		{"time-micros", `avro:"f,time-micros"`},
		{"local-timestamp-millis", `avro:"f,local-timestamp-millis"`},
		{"local-timestamp-micros", `avro:"f,local-timestamp-micros"`},
		{"local-timestamp-nanos", `avro:"f,local-timestamp-nanos"`},
		{"decimal", `avro:"f,decimal(9,2)"`},
		// malformed forms (every one must reject at build)
		{"bad-option", `avro:"f,bogus"`},
		{"unclosed-bracket", `avro:"f,alias=[a"`},
		{"decimal-junk", `avro:"f,decimal(9,2,3)"`},
		{"empty-alias", `avro:"f,alias=[]"`},
		{"dash-options", `avro:"-,omitzero"`},
	}
}

// ---- the oracle ------------------------------------------------------------

type rtDivergence struct {
	label  string
	kind   string // "panic" | "encode-rejects" | "decode-own-wire" | "typed-decode" | "faithful" | "empty-error" | "json-encode-rejects" | "json-decode-own-wire" | "json-typed-decode" | "json-faithful"
	detail string
}

// rtRunCell applies the round-trip-or-clean-reject oracle to one cell. It never
// fails the test directly; it returns at most one divergence so the caller can
// tally the whole landscape and report it together (a single broken axis would
// otherwise spam thousands of Fatalf lines).
func rtRunCell(label string, fieldType reflect.Type, tag string, faithful bool) (built bool, div *rtDivergence) {
	defer func() {
		if r := recover(); r != nil {
			built = false
			div = &rtDivergence{label, "panic", fmt.Sprintf("%v\n%s", r, debug.Stack())}
		}
	}()

	st := reflect.StructOf([]reflect.StructField{{Name: "F", Type: fieldType, Tag: reflect.StructTag(tag)}})
	s, err := schemaForType(st, WithName("RT"))
	if err != nil {
		// (b) clean reject: a non-empty error and (via recover) no panic.
		if strings.TrimSpace(err.Error()) == "" {
			return false, &rtDivergence{label, "empty-error", "build rejected with an empty error string"}
		}
		return false, nil
	}

	// (a) build accepted -> Encode of a value of this exact type MUST accept.
	ptr := reflect.New(st)
	ptr.Elem().Field(0).Set(sampleValue(fieldType))
	wire, encErr := s.Encode(ptr.Interface())
	if encErr != nil {
		return true, &rtDivergence{label, "encode-rejects",
			fmt.Sprintf("schema=%s\n encErr=%v", s, encErr)}
	}
	// The schema's own wire must decode into any (internal consistency).
	var sink any
	if _, decErr := s.Decode(wire, &sink); decErr != nil {
		return true, &rtDivergence{label, "decode-own-wire",
			fmt.Sprintf("schema=%s\n decErr=%v", s, decErr)}
	}
	// ... and into a fresh typed value (the typed decode direction).
	dst := reflect.New(st)
	if _, decErr := s.Decode(wire, dst.Interface()); decErr != nil {
		return true, &rtDivergence{label, "typed-decode",
			fmt.Sprintf("schema=%s\n decErr=%v", s, decErr)}
	}
	if faithful {
		got := dst.Elem().Field(0).Interface()
		want := ptr.Elem().Field(0).Interface()
		if !reflect.DeepEqual(got, want) {
			return true, &rtDivergence{label, "faithful",
				fmt.Sprintf("got=%#v want=%#v\n schema=%s", got, want, s)}
		}
	}

	// The schema must also round-trip through the JSON wire (the package's other
	// codec): a build-accepts/JSON-encode-rejects asymmetry — a schema SchemaFor
	// builds whose value the JSON path then refuses, or that binary round-trips
	// but JSON cannot — is the binary-vs-JSON path-divergence class, invisible to
	// the binary checks above.
	jwire, jencErr := s.EncodeJSON(ptr.Interface())
	if jencErr != nil {
		return true, &rtDivergence{label, "json-encode-rejects",
			fmt.Sprintf("schema=%s\n jsonEncErr=%v", s, jencErr)}
	}
	var jsink any
	if decErr := s.DecodeJSON(jwire, &jsink); decErr != nil {
		return true, &rtDivergence{label, "json-decode-own-wire",
			fmt.Sprintf("schema=%s\n json=%s\n jsonDecErr=%v", s, jwire, decErr)}
	}
	jdst := reflect.New(st)
	if decErr := s.DecodeJSON(jwire, jdst.Interface()); decErr != nil {
		return true, &rtDivergence{label, "json-typed-decode",
			fmt.Sprintf("schema=%s\n json=%s\n jsonDecErr=%v", s, jwire, decErr)}
	}
	if faithful {
		got := jdst.Elem().Field(0).Interface()
		want := ptr.Elem().Field(0).Interface()
		if !reflect.DeepEqual(got, want) {
			return true, &rtDivergence{label, "json-faithful",
				fmt.Sprintf("got=%#v want=%#v\n json=%s\n schema=%s", got, want, jwire, s)}
		}
	}
	return true, nil
}

// shapePreservesFaithful reports whether wrapping a faithful leaf in this shape
// keeps the round-trip faithful under DeepEqual. Every shape does (a non-nil
// pointer, a one-element slice/array, a one-entry map all round-trip exactly);
// past-cap rejects at build so its faithfulness is moot. Kept explicit so a
// future lossy shape is a one-line opt-out rather than a silent false pass.
func shapePreservesFaithful(_ rtShape) bool { return true }

func TestGenerative_SchemaForRoundTripConsistency(t *testing.T) {
	leaves := rtLeaves()
	shapes := rtShapes()
	tags := rtTags()

	var (
		cells, builds, rejects, faithfulChecks int
		divs                                   []rtDivergence
		byKind                                 = map[string]int{}
	)

	for _, lf := range leaves {
		for _, sh := range shapes {
			ft := sh.wrap(lf.typ)
			faithful := lf.faithful && shapePreservesFaithful(sh)
			for _, tg := range tags {
				cells++
				label := fmt.Sprintf("leaf=%s shape=%s tag=%s", lf.label, sh.label, tg.label)
				built, div := rtRunCell(label, ft, tg.tag, faithful)
				if built {
					builds++
					if faithful {
						faithfulChecks++
					}
				} else {
					rejects++
				}
				if div != nil {
					byKind[div.kind]++
					divs = append(divs, *div)
				}
			}
		}
	}

	// Report the whole landscape together so a broken axis is one summary, not
	// thousands of lines.
	if len(divs) > 0 {
		kinds := make([]string, 0, len(byKind))
		for k := range byKind {
			kinds = append(kinds, k)
		}
		sort.Strings(kinds)
		var b strings.Builder
		fmt.Fprintf(&b, "%d/%d cells diverged from the round-trip-or-clean-reject oracle:\n", len(divs), cells)
		for _, k := range kinds {
			fmt.Fprintf(&b, "  %-16s %d\n", k, byKind[k])
		}
		const show = 40
		for i, d := range divs {
			if i >= show {
				fmt.Fprintf(&b, "  ... and %d more\n", len(divs)-show)
				break
			}
			fmt.Fprintf(&b, "\n[%s] %s\n  %s\n", d.kind, d.label, d.detail)
		}
		t.Fatal(b.String())
	}

	// Non-vacuity floor: both halves of the oracle must be substantially
	// exercised. If a generation change collapses the build set or the reject
	// set, the net silently stops testing one half — fail loudly instead.
	if builds < 400 || rejects < 400 || cells < 3000 {
		t.Fatalf("generator under-covered (a generation regression hides one half of the oracle): cells=%d builds=%d rejects=%d faithfulChecks=%d",
			cells, builds, rejects, faithfulChecks)
	}
	t.Logf("round-trip net: %d cells | %d built+round-tripped (%d faithful-value-checked) | %d clean rejects | 0 divergences",
		cells, builds, faithfulChecks, rejects)
}

// rtRealEntryPointCells drives the REAL generic SchemaFor[T] (not the
// reflect.Type replica) on the compile-time-nameable leaves, so the bulk net's
// reliance on schemaForType is bridged at the entry point under test: a bug in
// SchemaFor[T]'s own wrapper (top-level pointer deref, name/opts handling) that
// the replica does not share would be invisible to the StructOf cross but caught
// here. Each case applies the same round-trip-or-clean-reject oracle.
func TestGenerative_SchemaForRoundTripRealEntryPoint(t *testing.T) {
	check := func(name string, build func() (*Schema, error), mk func() any, faithful bool) {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic: %v\n%s", r, debug.Stack())
				}
			}()
			s, err := build()
			if err != nil {
				if strings.TrimSpace(err.Error()) == "" {
					t.Fatal("rejected with an empty error string")
				}
				return // clean reject
			}
			v := mk()
			wire, err := s.Encode(v)
			if err != nil {
				t.Fatalf("SchemaFor accepted but Encode rejects (build-accepts/encode-rejects):\n schema=%s\n err=%v", s, err)
			}
			var sink any
			if _, err := s.Decode(wire, &sink); err != nil {
				t.Fatalf("schema cannot decode its own wire: %v\n schema=%s", err, s)
			}
			if faithful {
				out := reflect.New(reflect.TypeOf(v).Elem()).Interface()
				if _, err := s.Decode(wire, out); err != nil {
					t.Fatalf("typed decode failed: %v", err)
				}
				if !reflect.DeepEqual(out, v) {
					t.Fatalf("faithful round-trip mismatch: got %#v want %#v", out, v)
				}
			}
		})
	}

	// struct, named-primitive, pointer chains, slice, array, map, interface,
	// recursive — exercised through the generic entry point.
	type Prim struct {
		A int32   `avro:"a"`
		B string  `avro:"b"`
		C float64 `avro:"c"`
	}
	check("struct", func() (*Schema, error) { return SchemaFor[Prim]() },
		func() any { return &Prim{A: 1, B: "x", C: 2} }, true)

	type NamedPrim struct {
		N rtNamedInt `avro:"n"`
	}
	check("named-primitive", func() (*Schema, error) { return SchemaFor[NamedPrim]() },
		func() any { return &NamedPrim{N: 7} }, true)

	type DoublePtr struct {
		V **int32 `avro:"v"`
	}
	check("multi-level-pointer", func() (*Schema, error) { return SchemaFor[DoublePtr]() },
		func() any { n := int32(3); p := &n; return &DoublePtr{V: &p} }, false)

	type AtCap struct {
		V *****int32 `avro:"v"`
	}
	check("ptr-at-cap", func() (*Schema, error) { return SchemaFor[AtCap]() },
		func() any {
			n := int32(9)
			p1 := &n
			p2 := &p1
			p3 := &p2
			p4 := &p3
			p5 := &p4
			return &AtCap{V: p5}
		}, false)

	type PastCap struct {
		V ******int32 `avro:"v"`
	}
	check("ptr-past-cap-rejects", func() (*Schema, error) { return SchemaFor[PastCap]() },
		func() any { return &PastCap{} }, false)

	type Slice struct {
		V []rtRecord `avro:"v"`
	}
	check("slice-of-record", func() (*Schema, error) { return SchemaFor[Slice]() },
		func() any { return &Slice{V: []rtRecord{{A: 1, B: "y"}}} }, true)

	type Map struct {
		V map[string]int64 `avro:"v"`
	}
	check("map", func() (*Schema, error) { return SchemaFor[Map]() },
		func() any { return &Map{V: map[string]int64{"k": 4}} }, true)

	type Iface struct {
		V any `avro:"v"`
	}
	check("interface-rejects", func() (*Schema, error) { return SchemaFor[Iface]() },
		func() any { return &Iface{} }, false)

	check("recursive-struct", func() (*Schema, error) { return SchemaFor[rtLinked]() },
		func() any { return &rtLinked{Val: 1, Next: &rtLinked{Val: 2}} }, false)

	type OneWayText struct {
		V rtStructMarshal `avro:"v"`
	}
	check("one-way-text-rejects", func() (*Schema, error) { return SchemaFor[OneWayText]() },
		func() any { return &OneWayText{} }, false)

	type JSONNum struct {
		V json.Number `avro:"v"`
	}
	check("json-number-rejects", func() (*Schema, error) { return SchemaFor[JSONNum]() },
		func() any { return &JSONNum{} }, false)
}

// ---- embed x leaf-inference composition ------------------------------------
//
// The main generator builds field types with reflect.StructOf, which cannot
// anonymously embed an UNNAMED per-leaf carrier, so the diamond/equal-depth
// embed shape over varying leaves is covered here with hand-declared carriers
// driven through the REAL SchemaFor[T]. The composition under test: a
// single-arm embed resolves the promoted field, then inferType runs on its
// LEAF type exactly as for a direct field — so the one-way-text refusal, the
// pointer-chain cap, the json.Number reject, and decimal acceptance must all
// compose through the embed. (TestGenerative_EmbedShapeWalkerAgreement owns
// embed FIELD SELECTION with an int32 leaf; this owns embed x leaf INFERENCE.)
// The diamond case pins that an equal-depth ambiguous collision still rejects
// independent of the leaf type.

// Special-leaf carriers: value-embedded or reject-at-build, so unexported is
// fine (a value embed of an unexported struct decodes; the refused cases never
// reach decode). The leaf each carries is the one whose inference must compose
// through the embed.
type rtEmbOneWay struct {
	V rtStructMarshal `avro:"v"` // non-string base, encode-only text -> refused
}
type rtEmbDeepPtr struct {
	V ******int32 `avro:"v"` // past the codec's pointer-unwrap cap -> refused
}
type rtEmbJSONNum struct {
	V json.Number `avro:"v"` // numeric-only carrier, no single Avro type -> refused
}
type rtEmbDecimal struct {
	V *big.Rat `avro:"v,decimal(9,2)"`
}

type rtTopDecimal struct{ rtEmbDecimal }
type rtTopOneWay struct{ rtEmbOneWay }
type rtTopOneWayPtr struct{ *rtEmbOneWay } // rejects at build, so the embed pointer is never decoded
type rtTopDeepPtr struct{ rtEmbDeepPtr }
type rtTopJSONNum struct{ rtEmbJSONNum }

// The plain / pointer-embed / diamond CONTROLS reuse the exported carriers from
// embed_shape_generative_test.go (GA, GL, GR over an int32 "N") — reconciling
// with TestGenerative_EmbedShapeWalkerAgreement (which owns field selection)
// rather than re-declaring int32 carriers. An exported carrier is required only
// for the pointer-embed control, where decode must allocate the embed.
type rtTopPlain struct{ GA }     // struct{ N int32 }
type rtTopPlainPtr struct{ *GA } // exported pointer embed -> decode allocates it
type rtTopDiamond struct {       // "N" via GL.GBase.N and GR.GBase.N at equal depth -> ambiguous
	GL
	GR
}
type rtTopSingleArm struct{ GL } // one arm: "N" resolves

func TestGenerative_SchemaForEmbedLeafComposition(t *testing.T) {
	cases := []struct {
		name      string
		build     func() (*Schema, error)
		value     any // non-nil when the schema is expected to build
		wantBuild bool
		faithful  bool // assert decode==value (off for decimal: *big.Rat repr)
	}{
		// Leaf inference composes through a single-arm embed exactly as for a
		// direct field: the refusals/cap/reject fire, decimal builds.
		{"decimal", func() (*Schema, error) { return SchemaFor[rtTopDecimal]() }, &rtTopDecimal{rtEmbDecimal{V: big.NewRat(3, 1)}}, true, false},
		{"one-way-text-refused", func() (*Schema, error) { return SchemaFor[rtTopOneWay]() }, nil, false, false},
		{"one-way-text-ptr-embed-refused", func() (*Schema, error) { return SchemaFor[rtTopOneWayPtr]() }, nil, false, false},
		{"deep-pointer-refused", func() (*Schema, error) { return SchemaFor[rtTopDeepPtr]() }, nil, false, false},
		{"json-number-refused", func() (*Schema, error) { return SchemaFor[rtTopJSONNum]() }, nil, false, false},
		// Controls (reused carriers): a clean embed builds + round-trips through
		// value and pointer embeds; an equal-depth diamond rejects.
		{"plain", func() (*Schema, error) { return SchemaFor[rtTopPlain]() }, &rtTopPlain{GA{N: 1}}, true, true},
		{"plain-ptr-embed", func() (*Schema, error) { return SchemaFor[rtTopPlainPtr]() }, &rtTopPlainPtr{&GA{N: 1}}, true, true},
		{"single-arm-resolves", func() (*Schema, error) { return SchemaFor[rtTopSingleArm]() }, &rtTopSingleArm{GL{GBase{N: 5}}}, true, true},
		{"diamond-ambiguous-refused", func() (*Schema, error) { return SchemaFor[rtTopDiamond]() }, nil, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := c.build()
			if !c.wantBuild {
				if err == nil {
					t.Fatalf("expected a clean build-time reject, got schema %s", s)
				}
				if strings.TrimSpace(err.Error()) == "" {
					t.Fatal("rejected with an empty error string")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected the embed to compose into a buildable schema, got: %v", err)
			}
			wire, err := s.Encode(c.value)
			if err != nil {
				t.Fatalf("embed composed but Encode rejects (build-accepts/encode-rejects): %v\n schema=%s", err, s)
			}
			var sink any
			if _, err := s.Decode(wire, &sink); err != nil {
				t.Fatalf("schema cannot decode its own wire: %v\n schema=%s", err, s)
			}
			out := reflect.New(reflect.TypeOf(c.value).Elem()).Interface()
			if _, err := s.Decode(wire, out); err != nil {
				t.Fatalf("typed decode of own wire failed: %v\n schema=%s", err, s)
			}
			if c.faithful && !reflect.DeepEqual(out, c.value) {
				t.Fatalf("embed round-trip mismatch: got %#v want %#v", out, c.value)
			}
		})
	}
}

// rtUnexpEmbed is value-decodable but, behind a pointer, names the one embed
// shape decode cannot fill: a field promoted through a nil UNEXPORTED embedded
// pointer, which reflect cannot allocate (it cannot Set an unexported field).
type rtUnexpEmbed struct {
	V int32 `avro:"v"`
}
type rtTopUnexpPtr struct{ *rtUnexpEmbed }

// TestRegression_SchemaForUnexportedEmbedPointerDecodeConstraint pins a
// DOCUMENTED decode constraint the round-trip net surfaced, NOT a SchemaFor
// divergence: SchemaFor builds a valid record for struct{ *unexportedEmbed },
// and Encode of a value whose embed is non-nil succeeds — but typed Decode into
// a fresh value must ALLOCATE the nil unexported embedded pointer to fill the
// promoted field, which reflect forbids. The codec rejects cleanly with a
// specific message rather than panicking or silently dropping the field. This
// is a general decode property of that Go shape (any hand-written schema hits
// it identically), so it is expected, not a build-accepts/encode-rejects bug;
// it also pins the otherwise-untested guard in reflect.go.
func TestRegression_SchemaForUnexportedEmbedPointerDecodeConstraint(t *testing.T) {
	s, err := SchemaFor[rtTopUnexpPtr]()
	if err != nil {
		t.Fatalf("SchemaFor must build a record for struct{ *unexportedEmbed }: %v", err)
	}
	wire, err := s.Encode(&rtTopUnexpPtr{&rtUnexpEmbed{V: 7}})
	if err != nil {
		t.Fatalf("Encode of a non-nil embed must succeed (the embed is read, not allocated): %v", err)
	}
	var sink any
	if _, err := s.Decode(wire, &sink); err != nil {
		t.Fatalf("decode into any must succeed: %v", err)
	}
	// Typed decode into a fresh value must error cleanly (cannot allocate the
	// nil unexported embedded pointer), never panic or silently drop.
	_, err = s.Decode(wire, &rtTopUnexpPtr{})
	if err == nil {
		t.Fatal("typed decode through a nil unexported embedded pointer must error, not silently drop the promoted field")
	}
	if !strings.Contains(err.Error(), "nil unexported embedded pointer") {
		t.Fatalf("decode error should name the unexported-embedded-pointer constraint, got: %v", err)
	}
}

// ---- neutering record (non-vacuity proof) ----------------------------------
//
// The round-trip oracle is proven to FAIL when each historical SchemaFor fix is
// reverted in inferType (schema_for.go). Counts below are MEASURED over the
// 7326-cell leaf x shape x tag cross by switching the divergence report from
// t.Fatal to a count. With every fix intact, divergences == 0.
//
//	NEUTER-1  One-way-text refusal (the text-interface x shape axis). Replace the
//	          inferType text block's enc/dec switch with an unconditional
//	          `return "string", nil` (revert 962f7b6):
//	            48 cells red (24 encode-rejects + 24 typed-decode) — the
//	            rtStructMarshal (encode-only) and rtStructUnmarshal (decode-only)
//	            non-string-base leaves now infer "string" and build, then Encode
//	            (decode-only) or typed Decode (encode-only) fails, across all 8
//	            leaf-materializing shapes (direct, ptr, ptr2, ptrAtCap, slice,
//	            array2, map, slicePtr) x the 3 string-building tags (none, name,
//	            alias). The string- and []byte-base one-directional leaves stay
//	            GREEN (their kind covers the missing direction), and the inline
//	            tag flattens the struct rather than inferring a string — proving
//	            the refusal is scoped to exactly the non-round-trippable types.
//
//	NEUTER-2  json.Number reject. Make the `case jsonNumberType:` arm return
//	          `"string", nil` (revert the json.Number fix):
//	            68 cells red, all encode-rejects, all on the json.Number leaf —
//	            it infers "string" (its Kind) and builds for every non-malformed
//	            tag (the case returns before the logical/text checks, so even
//	            uuid/timestamp-*/decimal build as a bare string), but the codec
//	            rejects json.Number against a string schema on encode, across
//	            every shape that materializes the leaf.
//
//	NEUTER-3  Pointer-chain cap. Remove the `ptrChain >= maxIndirectDepth`
//	          refusal in inferType's pointer arm:
//	            179 cells red, all encode-rejects, all on the ptrPastCap shape
//	            (6 pointer levels) — every leaf that itself builds now produces a
//	            ["null",T], then Encode of the non-nil sampled value fails with
//	            errIndirectDeep. ptr2 / ptrAtCap (within the cap) stay GREEN, so
//	            the boundary is exactly at the codec's unwrap depth. (The
//	            depth>=maxDepth ceiling still prevents any stack overflow, so the
//	            measurement is a clean count, not a crash.)
//
// The three counts are unchanged whether or not the JSON wire is exercised: each
// of these bugs manifests on the BINARY path, which rtRunCell checks first and
// which short-circuits the cell — so the JSON checks add coverage only for a
// hypothetical binary-passes/JSON-fails bug (none of the three are), and the
// full net currently finds zero such cells.

// ---------- schemafor_skip_directive_test.go ----------

// The avro struct tag is validated on two structurally distinct paths in
// collectFields: the NAMED-FIELD path (an ordinary field, and an anonymous
// non-struct field, which falls through to it) and the ANONYMOUS EMBEDDED
// STRUCT path, which handles its own tag before the named path is reached.
// A validation that lives on only one path is a hole: the same tag string
// then means different things depending on where it is written.
//
// The census below is the executable form of that claim. Every row is a tag
// whose verdict must NOT depend on which path reads it, exercised through
// both a named field and an anonymous embed of the same struct type, under
// strict AND lax name validation — lax matters because a guard that only
// appears to work by way of Avro's name grammar (a field named "-" is not a
// valid Avro name) stops working the moment a caller supplies their own
// validator via WithLaxNames.

type skipCensusInner struct{ A string }

// skipCensusStruct builds `struct { F skipCensusInner "tag"; G string }` when
// embed is false, and `struct { skipCensusInner "tag"; G string }` when true.
func skipCensusStruct(tag string, embed bool) reflect.Type {
	first := reflect.StructField{
		Name: "F",
		Type: reflect.TypeFor[skipCensusInner](),
		Tag:  reflect.StructTag(tag),
	}
	if embed {
		first.Name = "SkipCensusInner"
		first.Type = reflect.TypeFor[skipCensusInner]()
		first.Anonymous = true
	}
	return reflect.StructOf([]reflect.StructField{
		first,
		{Name: "G", Type: reflect.TypeFor[string]()},
	})
}

// skipCensusBuild runs the SchemaFor pipeline over a runtime-built struct.
func skipCensusBuild(t *testing.T, tag string, embed bool, opts ...SchemaOpt) (*Schema, error) {
	t.Helper()
	st := skipCensusStruct(tag, embed)
	fields := make([]reflect.StructField, st.NumField())
	for i := range fields {
		fields[i] = st.Field(i)
	}
	return schemaForScopeCell(t, fields, "", nil, opts...)
}

// TestMatrix_SchemaForTagGuardPathCensus is the pattern-14a census: for every
// tag validation on the named-field path, the anonymous-embed path must reach
// the same verdict. A row's wantErr is the substring the error must name; an
// empty wantErr means the tag is valid and the build must succeed.
func TestMatrix_SchemaForTagGuardPathCensus(t *testing.T) {
	census := []struct {
		guard   string
		tag     string
		wantErr string
	}{
		{"exact skip directive", `avro:"-"`, ""},
		{"skip directive is exact-match only (options)", `avro:"-,omitzero"`, "exact-match only"},
		{"skip directive is exact-match only (suffix)", `avro:"-foo"`, "exact-match only"},
		{"splitTag unclosed bracket", `avro:"X,alias=[a"`, "unclosed"},
		{"splitTag unexpected close", `avro:"X,alias=a]"`, "unexpected"},
		{"inline with an explicit name", `avro:"X,inline"`, "inline is incompatible with an explicit field name"},
		{"inline with another option", `avro:",inline,omitzero"`, "inline is incompatible with option"},
		{"alias empty brackets", `avro:"X,alias=[]"`, "empty brackets"},
		{"alias empty element", `avro:"X,alias=[a,]"`, "empty element"},
		{"type-alias empty brackets", `avro:"X,type-alias=[]"`, "empty brackets"},
		{"decimal trailing junk", `avro:"X,decimal(1,2,3)"`, "invalid decimal tag"},
		{"unknown tag option", `avro:"X,bogusopt"`, "unknown avro tag option"},
		{"uuid on an incompatible Go type", `avro:"X,uuid"`, "uuid logical type"},
		{"decimal on an incompatible Go type", `avro:"X,decimal(4,2)"`, "decimal logical type requires"},
	}

	lax := WithLaxNames(func(string) error { return nil })
	for _, mode := range []struct {
		name string
		opts []SchemaOpt
	}{
		{"strict", nil},
		{"lax", []SchemaOpt{lax}},
	} {
		for _, row := range census {
			for _, embed := range []bool{false, true} {
				path := "named"
				if embed {
					path = "embed"
				}
				t.Run(fmt.Sprintf("%s/%s/%s", mode.name, path, row.guard), func(t *testing.T) {
					_, err := skipCensusBuild(t, row.tag, embed, mode.opts...)
					switch {
					case row.wantErr == "" && err != nil:
						t.Fatalf("tag %s must build on the %s path, got: %v", row.tag, path, err)
					case row.wantErr == "":
						return
					case err == nil:
						t.Fatalf("tag %s must be rejected on the %s path naming %q, but the build succeeded",
							row.tag, path, row.wantErr)
					case !strings.Contains(err.Error(), row.wantErr):
						t.Fatalf("tag %s on the %s path rejected with %q, which does not name %q",
							row.tag, path, err, row.wantErr)
					}
				})
			}
		}
	}
}

// TestMatrix_SchemaForEmbeddedSkipDirectiveExactMatch is the per-symptom
// pin for the census row that was open: the "-" skip directive is
// exact-match only, and the anonymous-embed path must say so in the same
// actionable terms as the named path rather than deferring to Avro's name
// grammar. Under WithLaxNames the grammar does not fire at all, so before
// the guard was shared the embed path emitted a field literally named "-"
// carrying the whole embedded record — the opposite of the skip the tag
// asked for.
func TestMatrix_SchemaForEmbeddedSkipDirectiveExactMatch(t *testing.T) {
	lax := WithLaxNames(func(string) error { return nil })

	for _, mode := range []struct {
		name string
		opts []SchemaOpt
	}{
		{"strict", nil},
		{"lax", []SchemaOpt{lax}},
	} {
		for _, tag := range []string{`avro:"-,omitzero"`, `avro:"-,inline"`, `avro:"-foo"`} {
			t.Run(mode.name+"/"+tag, func(t *testing.T) {
				s, err := skipCensusBuild(t, tag, true, mode.opts...)
				if err == nil {
					t.Fatalf("embedded %s accepted; emitted %s", tag, s.String())
				}
				if !strings.Contains(err.Error(), "exact-match only") {
					t.Fatalf("embedded %s rejected with %q, which does not name the skip directive", tag, err)
				}
			})
		}
	}

	// Controls: the exact "-" directive still skips on BOTH paths, in both
	// name modes. The guard must not widen into the directive it protects.
	for _, mode := range []struct {
		name string
		opts []SchemaOpt
	}{
		{"strict", nil},
		{"lax", []SchemaOpt{lax}},
	} {
		for _, embed := range []bool{false, true} {
			path := "named"
			if embed {
				path = "embed"
			}
			t.Run("control/"+mode.name+"/"+path+"/exact-dash-skips", func(t *testing.T) {
				s, err := skipCensusBuild(t, `avro:"-"`, embed, mode.opts...)
				if err != nil {
					t.Fatalf("exact avro:\"-\" must skip cleanly on the %s path: %v", path, err)
				}
				root := s.Root()
				if len(root.Fields) != 1 || root.Fields[0].Name != "G" {
					t.Fatalf("exact avro:\"-\" did not skip on the %s path: %s", path, s.String())
				}
			})
		}
	}
}

// dashEmbedRuntime carries the tag whose SchemaFor build is rejected, so the
// runtime field mapper can be exercised against a HAND-WRITTEN schema that
// SchemaFor would never emit.
type dashEmbedRuntime struct {
	skipCensusInner `avro:"-,omitzero"`
	G               string
}

// TestRegression_SkipDirectiveGuardIsSchemaForScoped pins the boundary of
// the tag guard: it is a SchemaFor-side build validation, and it does not
// change how the runtime field mapper (reflect.go's typeFieldMapping) binds
// Go fields to Avro names. The mapper answers "which Go field owns this Avro
// name" for a caller-supplied schema; it has never enforced tag grammar, and
// none of collectFields' other tag rejections (unknown option, bad decimal,
// inline-on-non-struct) has a mapper counterpart either. A caller who
// hand-writes a lax schema with a field named "-" therefore keeps the exact
// encode/decode behavior they had.
func TestRegression_SkipDirectiveGuardIsSchemaForScoped(t *testing.T) {
	lax := WithLaxNames(func(string) error { return nil })
	s, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"-","type":{"type":"record","name":"Inner","fields":[{"name":"A","type":"string"}]}},
		{"name":"G","type":"string"}]}`, lax)
	if err != nil {
		t.Fatalf("hand-written lax schema must parse: %v", err)
	}
	in := dashEmbedRuntime{skipCensusInner: skipCensusInner{A: "a"}, G: "g"}
	wire, err := s.Encode(in)
	if err != nil {
		t.Fatalf("encode against the hand-written schema: %v", err)
	}
	var out dashEmbedRuntime
	if _, err := s.Decode(wire, &out); err != nil {
		t.Fatalf("decode against the hand-written schema: %v", err)
	}
	if out != in {
		t.Fatalf("runtime mapping changed: got %#v want %#v", out, in)
	}
}

// ---------- matrix_schemafor_exactcase_test.go ----------

// TestMatrix_SchemaForReservedKeyExactCase pins the contract that SchemaFor's
// composition walkers (resolveNameScope, pinCustomSchemaScope,
// dedupNamedTypes, normalizeSchemaScope) read reserved attribute keys the
// way the Parse they feed does: by exact lowercase name only. A Props key
// differing from a reserved name only by letter case is an ordinary custom
// property (see Schema.Root's doc) — the walkers must neither key, descend,
// nor inject through it, and it must survive composition verbatim.
//
// Axes: reserved key {namespace — the identity axis: only the exact
// spelling scopes the type; items / values / a union slice under items —
// the descent routes; fields — the field-descent axis} × spelling
// {exact-case, UPPER, mIxed} × occurrences {1, 2} × SchemaFor scope
// {default, WithNamespace}.
//
// Oracles per cell family:
//   - namespace: the EXACT spelling declares identity x.y.F
//     (canonical-visible, one definition + a dotted reference at two
//     occurrences). A VARIANT spelling declares nothing: the identity is
//     the null-namespace F for every variant cell — byte-identical to the
//     no-namespace control — and the variant key rides to the composed
//     definition's Props verbatim. The exact and variant identities MUST
//     diverge; asserting that divergence is what makes a reintroduced
//     case-fold visible.
//   - items/values/union-slice/fields: an exact-spelled stray keeps the
//     structural-key inertness posture (composition passes it through
//     untouched), and a variant spelling is a plain prop — both compose
//     verbatim with identical verdicts, canonicals, and inline-body
//     counts, because NO spelling of a key on a kind that does not bind it
//     may be walked, registered, or deduped.
func TestMatrix_SchemaForReservedKeyExactCase(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	variants := map[string]func(string) string{
		"upper": strings.ToUpper,
		"mixed": func(k string) string {
			// First letter upper, rest as-is: "Namespace", "Items", ...
			return strings.ToUpper(k[:1]) + k[1:]
		},
	}

	// namespace × occurrences × scope: the identity axis.
	for _, occ := range []int{1, 2} {
		for _, ns := range []string{"", "b"} {
			// Exact spelling: the namespace attribute, identity x.y.F.
			t.Run(fmt.Sprintf("namespace/exact/occ%d/ns=%q", occ, ns), func(t *testing.T) {
				node := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
					Props: map[string]any{"namespace": "x.y"}}
				s, err := schemaForScopeCell(t, scopeCellFields(occ, primary), ns, []CustomType{{GoType: primary, Schema: node}})
				if err != nil {
					t.Fatalf("cell errored: %v", err)
				}
				assertScopeFullnames(t, s, []string{topName(ns), "x.y.F"})
				if !strings.Contains(string(s.Canonical()), `"x.y.F"`) {
					t.Errorf("declared identity x.y.F missing from canonical: %s", s.Canonical())
				}
				if occ == 2 {
					if n := strings.Count(s.String(), `"size"`); n != 1 {
						t.Errorf("want one inline definition + a reference at two occurrences, found %d bodies: %s", n, s.String())
					}
				}
			})
			// Variant spellings: inert props; the identity is the
			// null-namespace F, verdict- and byte-identical to the
			// no-namespace control — including the control's documented
			// reject when a null-namespace type recurs under
			// WithNamespace (no reference spelling can denote it) — with
			// the variant preserved on the definition in success cells.
			control := &SchemaNode{Type: "fixed", Name: "F", Size: 4}
			sControl, controlErr := schemaForScopeCell(t, scopeCellFields(occ, primary), ns, []CustomType{{GoType: primary, Schema: control}})
			for spell, f := range variants {
				t.Run(fmt.Sprintf("namespace/%s/occ%d/ns=%q", spell, occ, ns), func(t *testing.T) {
					key := f("namespace")
					node := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
						Props: map[string]any{key: "x.y"}}
					s, err := schemaForScopeCell(t, scopeCellFields(occ, primary), ns, []CustomType{{GoType: primary, Schema: node}})
					if controlErr != nil {
						if err == nil || err.Error() != controlErr.Error() {
							t.Fatalf("variant %q verdict diverges from the no-namespace control:\n control: %v\n varied:  %v", key, controlErr, err)
						}
						return
					}
					if err != nil {
						t.Fatalf("cell errored where the control built: %v", err)
					}
					assertScopeFullnames(t, s, []string{topName(ns), "F"})
					if got, want := string(s.Canonical()), string(sControl.Canonical()); got != want {
						t.Errorf("variant %q canonical diverges from the no-namespace control (the variant must be inert):\n control: %s\n varied:  %s", key, want, got)
					}
					def := findNodeByTypeName(*s.Root(), "fixed", "F")
					if def == nil {
						t.Fatalf("definition F not found")
					}
					if got := def.Props[key]; !reflect.DeepEqual(got, "x.y") {
						t.Errorf("Props[%q] = %#v; want the variant preserved verbatim", key, got)
					}
				})
			}
		}
	}

	// Stray-carried routes: items, values, union slice, fields. The
	// carrier is an unnamed node whose Props hold a named definition
	// under a container key the carrier's kind does not bind. NO spelling
	// may be walked as a schema position (exact: the stray-key inertness
	// posture; variant: not a reserved key at all), so every spelling
	// composes verbatim: same verdict, same canonical, same inline-body
	// count.
	spellings := map[string]func(string) string{
		"exact": func(k string) string { return k },
		"upper": strings.ToUpper,
		"mixed": func(k string) string { return strings.ToUpper(k[:1]) + k[1:] },
	}
	routes := []struct {
		route string
		key   string // reserved key the carried value sits under
		build func(spelledKey string) *SchemaNode
	}{
		{"items", "items", func(k string) *SchemaNode {
			return &SchemaNode{Type: "string", Props: map[string]any{
				k: map[string]any{"type": "fixed", "name": "G", "size": 1}}}
		}},
		{"values", "values", func(k string) *SchemaNode {
			return &SchemaNode{Type: "string", Props: map[string]any{
				k: map[string]any{"type": "fixed", "name": "G", "size": 1}}}
		}},
		{"unionslice", "items", func(k string) *SchemaNode {
			return &SchemaNode{Type: "string", Props: map[string]any{
				k: []any{map[string]any{"type": "fixed", "name": "G", "size": 1}}}}
		}},
		{"fields", "fields", func(k string) *SchemaNode {
			// A record body carried under an exact-case stray items, with
			// its FIELDS key case-varied: only the exact spelling makes
			// the body a well-formed record, but the body sits at a stray
			// position either way, so every spelling stays inert.
			return &SchemaNode{Type: "string", Props: map[string]any{
				"items": map[string]any{"type": "record", "name": "R", "namespace": "x.y",
					k: []map[string]any{{"name": "f", "type": "int"}}}}}
		}},
	}
	for _, r := range routes {
		for _, occ := range []int{1, 2} {
			for _, ns := range []string{"", "b"} {
				verdicts := map[string]string{}
				canonicals := map[string]string{}
				bodies := map[string]int{}
				for spell, f := range spellings {
					spelledKey := f(r.key)
					t.Run(fmt.Sprintf("%s/%s/occ%d/ns=%q", r.route, spell, occ, ns), func(t *testing.T) {
						node := r.build(spelledKey)
						s, err := schemaForScopeCell(t, scopeCellFields(occ, primary), ns, []CustomType{{GoType: primary, Schema: node}})
						if err != nil {
							verdicts[spell] = err.Error()
							return
						}
						verdicts[spell] = "ok"
						canonicals[spell] = string(s.Canonical())
						// Inline-body marker, spelling-neutral: the carried
						// fixed G always emits "size"; the carried record R
						// always emits its field "f" (the container KEY
						// spelling varies by cell, the body content never
						// does).
						marker := `"size"`
						if r.route == "fields" {
							marker = `"name":"f"`
						}
						bodies[spell] = strings.Count(s.String(), marker)
					})
				}
				name := fmt.Sprintf("%s/occ%d/ns=%q", r.route, occ, ns)
				assertOneValue(t, name+" verdict", verdicts)
				if verdicts["exact"] == "ok" {
					assertOneCanonical(t, name, canonicals)
					assertOneIntValue(t, name+" inline bodies", bodies)
				}
			}
		}
	}

	// Inert controls: the render always emits exact-case "name" and "type",
	// so a case-variant of either is an extra custom property that neither
	// the walkers nor Parse bind. The composed output must equal the
	// variant-free control's canonical.
	for _, extra := range []string{"NAME", "TYPE"} {
		t.Run("inertcontrol/"+extra, func(t *testing.T) {
			control := &SchemaNode{Type: "fixed", Name: "F", Namespace: "x.y", Size: 4}
			varied := &SchemaNode{Type: "fixed", Name: "F", Namespace: "x.y", Size: 4,
				Props: map[string]any{extra: "Zed"}}
			sControl, err := schemaForScopeCell(t, scopeCellFields(2, primary), "b", []CustomType{{GoType: primary, Schema: control}})
			if err != nil {
				t.Fatalf("control: %v", err)
			}
			sVaried, err := schemaForScopeCell(t, scopeCellFields(2, primary), "b", []CustomType{{GoType: primary, Schema: varied}})
			if err != nil {
				t.Fatalf("varied: %v", err)
			}
			if string(sControl.Canonical()) != string(sVaried.Canonical()) {
				t.Errorf("case-variant %s prop is not inert:\n control: %s\n varied:  %s", extra, sControl.Canonical(), sVaried.Canonical())
			}
		})
	}
}

// findNodeAliases returns the Aliases slice of the named-type definition
// called name anywhere in the tree, or nil if no definition carries it
// (references are bare type-name nodes with no Name, so only the
// definition matches).
func findNodeAliases(n SchemaNode, name string) []string {
	if f := findNodeByTypeName(n, "", name); f != nil {
		return f.Aliases
	}
	return nil
}

// findNodeByTypeName walks a SchemaNode tree for the first node with the
// given Name (and Type, when non-empty).
func findNodeByTypeName(n SchemaNode, typ, name string) *SchemaNode {
	if n.Name == name && (typ == "" || n.Type == typ) {
		return &n
	}
	if n.Items != nil {
		if f := findNodeByTypeName(*n.Items, typ, name); f != nil {
			return f
		}
	}
	if n.Values != nil {
		if f := findNodeByTypeName(*n.Values, typ, name); f != nil {
			return f
		}
	}
	for i := range n.Branches {
		if f := findNodeByTypeName(n.Branches[i], typ, name); f != nil {
			return f
		}
	}
	for i := range n.Fields {
		if f := findNodeByTypeName(n.Fields[i].Type, typ, name); f != nil {
			return f
		}
	}
	return nil
}

// typeAliasExactCaseDefX is the named record definition the binding-key
// cells park behind a container key.
func typeAliasExactCaseDefX() map[string]any {
	return map[string]any{"type": "record", "name": "X",
		"fields": []any{map[string]any{"name": "c", "type": "long"}}}
}

// TestRegression_TypeAliasBindingKeyExactCase pins that the type-alias walk
// reads a container's binding key (items here) the way Parse does: by
// exact name only. A custom array whose items exist only as an
// exact-spelled Props key binds (the rendered "items" IS the array's
// items, so the walk descends it and the alias lands on X); a case-variant
// spelling is an ordinary prop, so the array has NO items to route the
// alias through and the build fails loudly at the walk's own diagnosis —
// there is no named type behind the tagged field.
func TestRegression_TypeAliasBindingKeyExactCase(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "L", Type: primary, Tag: `avro:"l,type-alias=Old"`}}

	t.Run("items", func(t *testing.T) {
		node := &SchemaNode{Type: "array", Props: map[string]any{"items": typeAliasExactCaseDefX()}}
		s, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
		if err != nil {
			t.Fatalf("exact-case items: %v", err)
		}
		if got := findNodeAliases(*s.Root(), "X"); !reflect.DeepEqual(got, []string{"Old"}) {
			t.Errorf("alias not applied to X: got %#v, want [Old]", got)
		}
	})
	for _, spell := range []string{"Items", "ITEMS"} {
		t.Run(spell, func(t *testing.T) {
			node := &SchemaNode{Type: "array", Props: map[string]any{spell: typeAliasExactCaseDefX()}}
			_, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
			if err == nil {
				t.Fatalf("spelling %q built; the array has no items so the type-alias has no named type to land on", spell)
			}
			if !strings.Contains(err.Error(), "type is not a named type") {
				t.Errorf("spelling %q error = %q; want the no-named-type diagnosis", spell, err.Error())
			}
		})
	}
}

// TestRegression_TypeAliasUnionPlacementExactCase pins WHERE the alias
// lands in a union under exact-case reads: on the first named type in walk
// order — record X behind the array branch's exact-spelled items. With a
// case-variant spelling the array branch has no items at all and the
// build fails loudly rather than silently rerouting the alias to a later
// named branch.
func TestRegression_TypeAliasUnionPlacementExactCase(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "U", Type: primary, Tag: `avro:"u,type-alias=Old"`}}
	build := func(itemsKey string) (*Schema, error) {
		node := &SchemaNode{Type: "union", Branches: []SchemaNode{
			{Type: "array", Props: map[string]any{itemsKey: typeAliasExactCaseDefX()}},
			{Type: "record", Name: "Y", Fields: []SchemaField{{Name: "n", Type: SchemaNode{Type: "int"}}}},
		}}
		return schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
	}
	s, err := build("items")
	if err != nil {
		t.Fatalf("exact-case items: %v", err)
	}
	root := s.Root()
	if got := findNodeAliases(*root, "X"); !reflect.DeepEqual(got, []string{"Old"}) {
		t.Errorf("alias not on first named type X: got %#v, want [Old]", got)
	}
	if got := findNodeAliases(*root, "Y"); got != nil {
		t.Errorf("alias misdirected to later branch Y: %#v", got)
	}
	if _, err := build("Items"); err == nil || !strings.Contains(err.Error(), "array is missing items schema") {
		t.Errorf("variant Items: got %v; want the missing-items reject", err)
	}
}

// TestRegression_TypeAliasVariantAliasesInert pins that the type-alias tag
// EXTENDS only the real aliases attribute routes — the SchemaNode.Aliases
// field and an exact-spelled Props "aliases" key. A case-variant Props
// spelling ("Aliases") is an ordinary custom property: the tag writes a
// fresh exact-case "aliases" beside it, Parse binds only the exact key,
// and the variant survives verbatim in Props, un-merged.
func TestRegression_TypeAliasVariantAliasesInert(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f,type-alias=Old"`}}

	control := &SchemaNode{Type: "fixed", Name: "F", Size: 4, Aliases: []string{"prior.P"}}
	sc, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: control}})
	if err != nil {
		t.Fatalf("control build: %v", err)
	}
	if got := findNodeAliases(*sc.Root(), "F"); len(got) != 2 {
		t.Fatalf("control aliases: got %#v, want both prior.P and Old", got)
	}

	variant := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
		Props: map[string]any{"Aliases": []any{"prior.P"}}}
	sv, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: variant}})
	if err != nil {
		t.Fatalf("variant build: %v", err)
	}
	def := findNodeByTypeName(*sv.Root(), "fixed", "F")
	if def == nil {
		t.Fatalf("definition F not found")
	}
	if !reflect.DeepEqual(def.Aliases, []string{"Old"}) {
		t.Errorf("Aliases = %#v; want [Old] alone (the variant key is not the aliases attribute)", def.Aliases)
	}
	if got := def.Props["Aliases"]; !reflect.DeepEqual(got, []any{"prior.P"}) {
		t.Errorf(`Props["Aliases"] = %#v; want the variant preserved verbatim, un-merged`, got)
	}
}

// TestMatrix_TypeAliasExactCase extends the reserved-key exact-case
// contract (TestMatrix_SchemaForReservedKeyExactCase) with the type-alias
// axis: the type-alias tag's walk routes through a container's binding key
// and reads/extends the aliases attribute exactly as Parse binds them — by
// exact name only.
//
// Axes:
//   - binding-key routing: carrier {array, map, union whose first named
//     type sits behind the carrier's binding key} × spelling {exact,
//     upper, mixed} × structural-field {nil — only the spelled Props key
//     exists; set — the real field renders exact-case and the spelled
//     Props key rides along}. Exact-spelling cells and every
//     structural=set cell build with the alias on X; a variant-only cell
//     (structural=nil, variant spelling) has no binding key and fails its
//     parse loudly. All accepting cells of one carrier agree on canonical
//     bytes (props are canonical-stripped).
//   - aliases-attribute routes: the field route and the exact-Props route
//     are EXTENDED identically; a variant-Props route gets a fresh exact
//     "aliases" with the variant preserved verbatim.
//   - name/namespace case-variant Props riding beside the real attributes:
//     inert (the exact attributes win; the variants ride as props).
//   - two tagged fields sharing the custom type: the namespace-field route
//     composes one x.y.X definition + one dotted reference; a "NameSpace"
//     variant-Props route declares nothing — the type is null-namespace X,
//     one definition + one bare reference, variant preserved.
func TestMatrix_TypeAliasExactCase(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	tagged := []reflect.StructField{{Name: "L", Type: primary, Tag: `avro:"l,type-alias=Old"`}}
	spellings := map[string]func(string) string{
		"exact": func(k string) string { return k },
		"upper": strings.ToUpper,
		"mixed": func(k string) string { return strings.ToUpper(k[:1]) + k[1:] },
	}
	itemsX := func() *SchemaNode {
		return &SchemaNode{Type: "record", Name: "X",
			Fields: []SchemaField{{Name: "c", Type: SchemaNode{Type: "long"}}}}
	}

	// Binding-key routing: array / map / union carriers.
	type carrierShape struct {
		name    string
		key     string // binding key the carrier's kind consumes
		missing string // the missing-structural-key reject for the kind
		build   func(spelledKey string, structuralSet bool) *SchemaNode
	}
	// The variant-only reject differs by carrier: with no named type
	// reachable at all (array/map), the type-alias walk itself fails
	// loudly first; the union's later named branch satisfies the walk, so
	// the itemless array branch is caught by the composed schema's parse.
	carriers := []carrierShape{
		{"array", "items", "type is not a named type", func(k string, set bool) *SchemaNode {
			n := &SchemaNode{Type: "array", Props: map[string]any{k: typeAliasExactCaseDefX()}}
			if set {
				n.Items = itemsX()
			}
			return n
		}},
		{"map", "values", "type is not a named type", func(k string, set bool) *SchemaNode {
			n := &SchemaNode{Type: "map", Props: map[string]any{k: typeAliasExactCaseDefX()}}
			if set {
				n.Values = itemsX()
			}
			return n
		}},
		{"union", "items", "array is missing items schema", func(k string, set bool) *SchemaNode {
			arr := SchemaNode{Type: "array", Props: map[string]any{k: typeAliasExactCaseDefX()}}
			if set {
				arr.Items = itemsX()
			}
			return &SchemaNode{Type: "union", Branches: []SchemaNode{arr,
				{Type: "record", Name: "Y", Fields: []SchemaField{{Name: "n", Type: SchemaNode{Type: "int"}}}}}}
		}},
	}
	for _, c := range carriers {
		canonicals := map[string]string{}
		for spell, f := range spellings {
			for _, set := range []bool{false, true} {
				cell := fmt.Sprintf("%s/%v", spell, set)
				t.Run(fmt.Sprintf("route/%s/%s/structural=%v", c.name, spell, set), func(t *testing.T) {
					node := c.build(f(c.key), set)
					s, err := schemaForScopeCell(t, tagged, "", []CustomType{{GoType: primary, Schema: node}})
					if spell != "exact" && !set {
						// The only spelling present is a variant: an
						// ordinary prop, so the container has no binding
						// key and the composed schema fails its parse.
						if err == nil || !strings.Contains(err.Error(), c.missing) {
							t.Errorf("variant-only cell: got %v; want the %q reject", err, c.missing)
						}
						return
					}
					if err != nil {
						t.Fatalf("cell errored: %v", err)
					}
					canonicals[cell] = string(s.Canonical())
					root := s.Root()
					if got := findNodeAliases(*root, "X"); !reflect.DeepEqual(got, []string{"Old"}) {
						t.Errorf("alias not on X: %#v", got)
					}
					if got := findNodeAliases(*root, "Y"); got != nil {
						t.Errorf("alias misdirected to later branch Y: %#v", got)
					}
				})
			}
		}
		assertOneValue(t, "route/"+c.name+" canonical", canonicals)
	}

	// Aliases-attribute routes.
	{
		aliasSets := map[string]string{}
		build := func(name string, node *SchemaNode, wantLen int) {
			t.Run("aliases/"+name, func(t *testing.T) {
				s, err := schemaForScopeCell(t, tagged, "", []CustomType{{GoType: primary, Schema: node}})
				if err != nil {
					t.Fatalf("build: %v", err)
				}
				got := findNodeAliases(*s.Root(), "F")
				aliasSets[name] = fmt.Sprintf("%v", got)
				if len(got) != wantLen {
					t.Errorf("aliases = %#v; want %d entries", got, wantLen)
				}
			})
		}
		// The real attribute routes are extended: caller alias + tag alias.
		build("field", &SchemaNode{Type: "fixed", Name: "F", Size: 4, Aliases: []string{"prior.P"}}, 2)
		build("props-exact", &SchemaNode{Type: "fixed", Name: "F", Size: 4,
			Props: map[string]any{"aliases": []any{"prior.P"}}}, 2)
		if aliasSets["field"] != aliasSets["props-exact"] {
			t.Errorf("field and exact-Props aliases routes diverge: %v vs %v", aliasSets["field"], aliasSets["props-exact"])
		}
		// Variant routes are inert: only the tag's alias binds.
		build("props-upper", &SchemaNode{Type: "fixed", Name: "F", Size: 4,
			Props: map[string]any{"ALIASES": []any{"prior.P"}}}, 1)
		build("props-mixed", &SchemaNode{Type: "fixed", Name: "F", Size: 4,
			Props: map[string]any{"Aliases": []any{"prior.P"}}}, 1)
	}

	// name / namespace case-variant Props riding beside the real
	// attributes: inert for the walk and for Parse (the exact attributes
	// win), so the composed output equals the variant-free control's.
	for _, extra := range []struct{ key, val string }{{"NAME", "Zed"}, {"NAMESPACE", "zed"}} {
		t.Run("inert/"+extra.key, func(t *testing.T) {
			control := &SchemaNode{Type: "fixed", Name: "F", Namespace: "x.y", Size: 4}
			varied := &SchemaNode{Type: "fixed", Name: "F", Namespace: "x.y", Size: 4,
				Props: map[string]any{extra.key: extra.val}}
			sc, err := schemaForScopeCell(t, tagged, "", []CustomType{{GoType: primary, Schema: control}})
			if err != nil {
				t.Fatalf("control: %v", err)
			}
			sv, err := schemaForScopeCell(t, tagged, "", []CustomType{{GoType: primary, Schema: varied}})
			if err != nil {
				t.Fatalf("varied: %v", err)
			}
			if string(sc.Canonical()) != string(sv.Canonical()) {
				t.Errorf("case-variant %s not inert under a type-alias tag:\n control: %s\n varied:  %s",
					extra.key, sc.Canonical(), sv.Canonical())
			}
			if got := findNodeAliases(*sv.Root(), "F"); !reflect.DeepEqual(got, []string{"Old"}) {
				t.Errorf("alias not applied: %#v", got)
			}
		})
	}

	// Two tagged fields sharing the custom type.
	{
		twoTagged := []reflect.StructField{
			{Name: "F1", Type: primary, Tag: `avro:"f1,type-alias=Old"`},
			{Name: "F2", Type: primary, Tag: `avro:"f2,type-alias=Old"`},
		}
		// The exact namespace-field route: one x.y.X definition + one
		// dotted reference.
		t.Run("twofields/nsfield", func(t *testing.T) {
			node := &SchemaNode{Type: "record", Name: "X", Namespace: "x.y",
				Fields: []SchemaField{{Name: "c", Type: SchemaNode{Type: "long"}}}}
			s, err := schemaForScopeCell(t, twoTagged, "", []CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			defs := strings.Count(s.String(), `"c"`)
			refs := strings.Count(s.String(), `"x.y.X"`)
			if defs != 1 || refs != 1 {
				t.Errorf("want one definition + one dotted reference, got %d defs %d refs: %s", defs, refs, s.String())
			}
			if got := findNodeAliases(*s.Root(), "X"); !reflect.DeepEqual(got, []string{"Old"}) {
				t.Errorf("alias not applied: %#v", got)
			}
		})
		// A "NameSpace" variant-Props route declares nothing: the type is
		// null-namespace X — one definition + one bare reference — and
		// the variant rides on the definition verbatim.
		t.Run("twofields/nsprops", func(t *testing.T) {
			node := &SchemaNode{Type: "record", Name: "X",
				Fields: []SchemaField{{Name: "c", Type: SchemaNode{Type: "long"}}},
				Props:  map[string]any{"NameSpace": "x.y"}}
			s, err := schemaForScopeCell(t, twoTagged, "", []CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			if strings.Contains(s.String(), `"x.y.X"`) {
				t.Errorf("variant NameSpace scoped the type: %s", s.String())
			}
			if defs := strings.Count(s.String(), `"c"`); defs != 1 {
				t.Errorf("want one inline definition, got %d bodies: %s", defs, s.String())
			}
			def := findNodeByTypeName(*s.Root(), "record", "X")
			if def == nil {
				t.Fatalf("definition X not found")
			}
			if got := def.Props["NameSpace"]; !reflect.DeepEqual(got, "x.y") {
				t.Errorf(`Props["NameSpace"] = %#v; want the variant preserved verbatim`, got)
			}
			if got := findNodeAliases(*s.Root(), "X"); !reflect.DeepEqual(got, []string{"Old"}) {
				t.Errorf("alias not applied: %#v", got)
			}
		})
	}
}

func topName(ns string) string {
	if ns == "" {
		return "Top"
	}
	return ns + ".Top"
}

func assertOneCanonical(t *testing.T, name string, got map[string]string) {
	t.Helper()
	assertOneValue(t, name+" canonical", got)
}

func assertOneValue(t *testing.T, name string, got map[string]string) {
	t.Helper()
	var first string
	var firstKey string
	for k, v := range got {
		if firstKey == "" {
			firstKey, first = k, v
			continue
		}
		if v != first {
			t.Errorf("%s diverges across spellings:\n %s: %s\n %s: %s", name, firstKey, first, k, v)
		}
	}
}

func assertOneIntValue(t *testing.T, name string, got map[string]int) {
	t.Helper()
	asStr := make(map[string]string, len(got))
	for k, v := range got {
		asStr[k] = fmt.Sprint(v)
	}
	assertOneValue(t, name, asStr)
}

// ---------- matrix_schemafor_scope_test.go ----------

// TestMatrix_SchemaForCustomSchemaScope crosses the namespace-composition
// space of CustomType.Schema embedding: a custom schema is an independently
// authored tree with its own namespace scoping, and SchemaFor must preserve
// every declared fullname when composing it into the inferred tree.
//
// Axes: custom-schema spelling {split Root()-derived, dotted hand-built
// SchemaNode, null-namespace} × kind {record, enum, fixed} × occurrences
// {one, two fields} × SchemaFor scope {default, WithNamespace} × shape
// {flat; recursive — the custom schema references itself, so its internal
// references must still bind after embedding; a nested named type in a
// DIFFERENT namespace inside the custom subtree}, plus coexistence cells
// (a.X + null-namespace X; a.X + b.X; two customs carrying IDENTICAL
// definitions dedup to one definition + a reference) and the
// unrepresentable corner: a null-namespace type recurring under
// WithNamespace has no reference spelling (a bare name binds in the
// enclosing namespace; references have no "namespace":"" escape), so that
// cell must produce exactly the named error — never a dangling reference or
// a namespace capture.
//
// Oracle per cell: the SchemaFor pipeline succeeds (or hits exactly the
// corner error); the output re-parses; the parsed metadata preserves every
// declared fullname; split and dotted spellings of the same schema produce
// byte-identical Canonical() — the spec ("Names") makes the two spellings
// one name, so their canonical forms must agree; and an EXECUTED fastavro
// arm parses representative outputs (which carry dotted references and
// "namespace":"" escapes) and must agree on the full parsing canonical
// form, which subsumes fingerprint equality without any byte-order
// presentation trap.

// Marker Go types the matrix's CustomTypes match on. Identity only matters
// within one cell, so two markers cover every layout.
type (
	scopeMatrixPrimary struct{ A int64 }
	scopeMatrixPartner struct{ B int64 }
)

// schemaForScopeCell mirrors SchemaFor's pipeline (inferRecord →
// dedupNamedTypes → Marshal → Parse with the same opts) over a
// reflect.StructOf-built struct, so cells can vary field layout at runtime
// where the compile-time-generic SchemaFor[T] cannot.
//
// Every cell doubles as a mutation probe: each CustomType.Schema is
// deep-snapshotted before the build and deep-compared after, pinning the
// contract that a build never writes into caller-owned SchemaNode storage
// (the metadata render hands Props containers over by reference, and the
// composition walkers mutate the tree they are given — the boundary copy
// in renderCustomSchemaTree is what keeps those writes off the caller's
// maps). The comparison runs whether or not the build errors: a mutation
// on an error path is just as much a contract break.
// extra carries SchemaOpts beyond the customs (e.g. WithLaxNames) through to
// the final Parse, so a cell can vary the name validator the emitted schema
// is read back under.
func schemaForScopeCell(t *testing.T, fields []reflect.StructField, namespace string, customs []CustomType, extra ...SchemaOpt) (*Schema, error) {
	t.Helper()
	// Every []string reachable from a cell's SchemaNode gets one sentinel
	// element hidden past its length (len < cap) before the build: a
	// deep-equal of the tree cannot see a write into the [len:cap) region
	// of a caller-owned backing array (an append with spare capacity lands
	// exactly there), so the sentinels are checked separately after.
	var sentinels []func() error
	for _, ct := range customs {
		plantStringSliceSentinels(ct.Schema, make(map[*SchemaNode]bool), &sentinels)
	}
	snaps := make([]*SchemaNode, len(customs))
	for i, ct := range customs {
		snaps[i] = snapshotSchemaNode(ct.Schema, make(map[*SchemaNode]*SchemaNode))
	}
	defer func() {
		for i, ct := range customs {
			if !reflect.DeepEqual(snaps[i], ct.Schema) {
				t.Errorf("build mutated caller-owned CustomType.Schema storage (custom %d):\n before: %#v\n after:  %#v", i, snaps[i], ct.Schema)
			}
		}
		for _, check := range sentinels {
			if err := check(); err != nil {
				t.Error(err)
			}
		}
	}()
	st := reflect.StructOf(fields)
	seen := make(map[reflect.Type]seenForm)
	s, err := inferRecord(st, "Top", namespace, seen, customs, make(appliedTypeAliases))
	if err != nil {
		return nil, err
	}
	s, err = dedupNamedTypes(s, make(map[string]string), "")
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	opts := make([]SchemaOpt, 0, len(customs)+len(extra))
	for _, ct := range customs {
		opts = append(opts, ct)
	}
	opts = append(opts, extra...)
	return Parse(string(b), opts...)
}

// plantStringSliceSentinels rebuilds every []string reachable from n
// (type aliases, enum symbols, field aliases) as a slice with one sentinel
// element past its length over a fresh backing array, and appends a
// checker per slice that verifies the sentinel after the build. A build
// that appends into one of these slices in place — instead of into its own
// copy — overwrites the sentinel.
func plantStringSliceSentinels(n *SchemaNode, visited map[*SchemaNode]bool, checks *[]func() error) {
	if n == nil || visited[n] {
		return
	}
	visited[n] = true
	n.Aliases = plantOneStringSentinel(n.Aliases, "SchemaNode.Aliases", checks)
	n.Symbols = plantOneStringSentinel(n.Symbols, "SchemaNode.Symbols", checks)
	plantStringSliceSentinels(n.Items, visited, checks)
	plantStringSliceSentinels(n.Values, visited, checks)
	for i := range n.Branches {
		plantStringSliceSentinels(&n.Branches[i], visited, checks)
	}
	for i := range n.Fields {
		n.Fields[i].Aliases = plantOneStringSentinel(n.Fields[i].Aliases, "SchemaField.Aliases", checks)
		plantStringSliceSentinels(&n.Fields[i].Type, visited, checks)
	}
}

func plantOneStringSentinel(ss []string, what string, checks *[]func() error) []string {
	if ss == nil {
		return nil
	}
	const sentinel = "caller-owned-past-len"
	backing := make([]string, len(ss)+1)
	copy(backing, ss)
	backing[len(ss)] = sentinel
	*checks = append(*checks, func() error {
		if got := backing[len(backing)-1]; got != sentinel {
			return fmt.Errorf("build wrote past len into a caller-owned %s backing array: %q", what, got)
		}
		return nil
	})
	return backing[: len(ss) : len(ss)+1]
}

// snapshotSchemaNode deep-copies a SchemaNode tree, including the dynamic
// containers reachable through Props and Default values, so a post-build
// reflect.DeepEqual against the original detects any write the build made
// into caller-owned storage. visited maps original Items/Values pointers to
// their copies so pointer-built cycles copy with their topology intact.
func snapshotSchemaNode(n *SchemaNode, visited map[*SchemaNode]*SchemaNode) *SchemaNode {
	if n == nil {
		return nil
	}
	if c, ok := visited[n]; ok {
		return c
	}
	c := &SchemaNode{}
	visited[n] = c
	*c = *n
	c.Aliases = append([]string(nil), n.Aliases...)
	c.Symbols = append([]string(nil), n.Symbols...)
	c.Items = snapshotSchemaNode(n.Items, visited)
	c.Values = snapshotSchemaNode(n.Values, visited)
	if n.Props != nil {
		c.Props = snapshotAnyValue(n.Props).(map[string]any)
	}
	if n.Branches != nil {
		c.Branches = make([]SchemaNode, len(n.Branches))
		for i := range n.Branches {
			c.Branches[i] = *snapshotSchemaNode(&n.Branches[i], visited)
		}
	}
	if n.Fields != nil {
		c.Fields = make([]SchemaField, len(n.Fields))
		for i, f := range n.Fields {
			cf := f
			cf.Aliases = append([]string(nil), f.Aliases...)
			cf.Type = *snapshotSchemaNode(&f.Type, visited)
			cf.Default = snapshotAnyValue(f.Default)
			if f.Props != nil {
				cf.Props = snapshotAnyValue(f.Props).(map[string]any)
			}
			c.Fields[i] = cf
		}
	}
	return c
}

// snapshotAnyValue deep-copies the JSON-shaped dynamic containers a Props
// or Default value can hold; scalars are immutable and copy by value. The
// snapshot must reproduce the value EXACTLY for the post-build DeepEqual,
// so every arm preserves nil-ness (nil in, nil out; empty in, empty out) —
// a snapshot that normalized nil would report a phantom mutation.
func snapshotAnyValue(v any) any {
	switch v := v.(type) {
	case map[string]any:
		if v == nil {
			return v
		}
		out := make(map[string]any, len(v))
		for k, val := range v {
			out[k] = snapshotAnyValue(val)
		}
		return out
	case []any:
		if v == nil {
			return v
		}
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = snapshotAnyValue(e)
		}
		return out
	case []map[string]any:
		if v == nil {
			return v
		}
		out := make([]map[string]any, len(v))
		for i, m := range v {
			out[i] = snapshotAnyValue(m).(map[string]any)
		}
		return out
	case []string:
		if v == nil {
			return v
		}
		out := make([]string, len(v))
		copy(out, v)
		return out
	case []byte:
		if v == nil {
			return v
		}
		out := make([]byte, len(v))
		copy(out, v)
		return out
	}
	return v
}

// buildScopeCustomNode returns the custom schema for one (spelling, kind,
// shape) combination plus the fullnames it declares. The spelling axis also
// varies the construction route: split and null-namespace schemas arrive
// via Parse(...).Root() (the metadata-derived path), the dotted spelling is
// a hand-built SchemaNode (the literal-construction path).
func buildScopeCustomNode(t *testing.T, spelling, kind, shape string) (*SchemaNode, []string) {
	t.Helper()
	if kind != "record" && shape != "flat" {
		t.Fatalf("shape %q applies to records only", shape)
	}
	// The declared name per spelling: base short name with namespace "a"
	// for split/dotted, bare for null-namespace. Recursive cells use a
	// distinct short name so the corner error's identity is visible.
	short := "X"
	if shape == "recursive" {
		short = "N"
	}
	if spelling == "dotted" {
		n := &SchemaNode{Type: kind, Name: "a." + short}
		switch kind {
		case "enum":
			n.Symbols = []string{"A", "B"}
		case "fixed":
			n.Size = 4
		case "record":
			switch shape {
			case "flat":
				n.Fields = []SchemaField{{Name: "n", Type: SchemaNode{Type: "int"}}}
			case "recursive":
				n.Fields = []SchemaField{{Name: "next", Type: SchemaNode{
					Type: "union", Branches: []SchemaNode{{Type: "null"}, {Type: "a." + short}},
				}}}
			case "nestedforeign":
				n.Fields = []SchemaField{{Name: "inner", Type: SchemaNode{
					Type: "record", Name: "q.Inner",
					Fields: []SchemaField{{Name: "m", Type: SchemaNode{Type: "int"}}},
				}}}
			}
		}
		full := []string{"a." + short}
		if shape == "nestedforeign" {
			full = append(full, "q.Inner")
		}
		return n, full
	}

	nsAttr := `,"namespace":"a"`
	fullPrefix := "a."
	if spelling == "nullns" {
		nsAttr = ""
		fullPrefix = ""
	}
	var body string
	switch kind {
	case "enum":
		body = fmt.Sprintf(`{"type":"enum","name":"%s"%s,"symbols":["A","B"]}`, short, nsAttr)
	case "fixed":
		body = fmt.Sprintf(`{"type":"fixed","name":"%s"%s,"size":4}`, short, nsAttr)
	case "record":
		switch shape {
		case "flat":
			body = fmt.Sprintf(`{"type":"record","name":"%s"%s,"fields":[{"name":"n","type":"int"}]}`, short, nsAttr)
		case "recursive":
			body = fmt.Sprintf(`{"type":"record","name":"%s"%s,"fields":[{"name":"next","type":["null","%s"]}]}`, short, nsAttr, short)
		case "nestedforeign":
			body = fmt.Sprintf(`{"type":"record","name":"%s"%s,"fields":[{"name":"inner","type":{"type":"record","name":"Inner","namespace":"q","fields":[{"name":"m","type":"int"}]}}]}`, short, nsAttr)
		}
	}
	s, err := Parse(body)
	if err != nil {
		t.Fatalf("parse custom schema %s: %v", body, err)
	}
	root := s.Root()
	full := []string{fullPrefix + short}
	if shape == "nestedforeign" {
		full = append(full, "q.Inner")
	}
	return root, full
}

// collectScopeNames walks the metadata tree with the parser's scope rules,
// gathering every named DEFINITION's resolved fullname into defs and every
// name-reference spelling with its enclosing namespace into refs. Root()
// resolves a definition's Namespace field, so a definition's fullname reads
// directly off the node; a reference surfaces as a bare node whose Type
// holds the spelling as written, whose meaning depends on the enclosing
// scope.
func collectScopeNames(n SchemaNode, enclosingNS string, defs map[string]bool, refs *[][2]string) {
	switch n.Type {
	case "record", "error", "enum", "fixed":
		full := n.Name
		if n.Namespace != "" && !strings.Contains(n.Name, ".") {
			full = n.Namespace + "." + n.Name
		}
		defs[full] = true
		childNS := ""
		if i := strings.LastIndex(full, "."); i >= 0 {
			childNS = full[:i]
		}
		for i := range n.Fields {
			collectScopeNames(n.Fields[i].Type, childNS, defs, refs)
		}
	case "array":
		if n.Items != nil {
			collectScopeNames(*n.Items, enclosingNS, defs, refs)
		}
	case "map":
		if n.Values != nil {
			collectScopeNames(*n.Values, enclosingNS, defs, refs)
		}
	case "union":
		for i := range n.Branches {
			collectScopeNames(n.Branches[i], enclosingNS, defs, refs)
		}
	case "null", "boolean", "int", "long", "float", "double", "string", "bytes":
	default:
		*refs = append(*refs, [2]string{n.Type, enclosingNS})
	}
}

// assertScopeFullnames asserts the schema's DEFINITION fullname set equals
// want exactly (a namespace capture or a duplicated definition both change
// the set), and that every name reference binds to one of those
// definitions under the parser's rules: enclosing-namespace-qualified
// first, then the null-namespace fallback for a bare spelling.
func assertScopeFullnames(t *testing.T, s *Schema, want []string) {
	t.Helper()
	defs := make(map[string]bool)
	var refs [][2]string
	root := s.Root()
	collectScopeNames(*root, "", defs, &refs)
	wantSet := make(map[string]bool, len(want))
	for _, w := range want {
		wantSet[w] = true
	}
	for w := range wantSet {
		if !defs[w] {
			t.Errorf("fullname %q missing from output definitions (got %v)", w, defs)
		}
	}
	for d := range defs {
		if !wantSet[d] {
			t.Errorf("unexpected definition %q in output (want %v)", d, want)
		}
	}
	for _, r := range refs {
		spelling, scope := r[0], r[1]
		switch {
		case strings.Contains(spelling, "."):
			if !defs[spelling] {
				t.Errorf("dotted reference %q does not bind any definition (%v)", spelling, defs)
			}
		case scope != "" && defs[scope+"."+spelling]:
			// binds in the enclosing namespace
		case defs[spelling]:
			// null-namespace fallback
		default:
			t.Errorf("bare reference %q in scope %q does not bind any definition (%v)", spelling, scope, defs)
		}
	}
}

func scopeCellFields(occurrences int, goType reflect.Type) []reflect.StructField {
	fields := []reflect.StructField{{Name: "F1", Type: goType}}
	if occurrences == 2 {
		fields = append(fields, reflect.StructField{Name: "F2", Type: goType})
	}
	return fields
}

func TestMatrix_SchemaForCustomSchemaScope(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	partner := reflect.TypeFor[scopeMatrixPartner]()

	kindShapes := []struct{ kind, shape string }{
		{"record", "flat"},
		{"record", "recursive"},
		{"record", "nestedforeign"},
		{"enum", "flat"},
		{"fixed", "flat"},
	}

	for _, spelling := range []string{"split", "dotted", "nullns"} {
		for _, ks := range kindShapes {
			for _, occurrences := range []int{1, 2} {
				for _, ns := range []string{"", "b"} {
					name := fmt.Sprintf("%s/%s_%s/occ%d/ns=%q", spelling, ks.kind, ks.shape, occurrences, ns)
					t.Run(name, func(t *testing.T) {
						node, fullnames := buildScopeCustomNode(t, spelling, ks.kind, ks.shape)
						ct := CustomType{GoType: primary, Schema: node}
						s, err := schemaForScopeCell(t, scopeCellFields(occurrences, primary), ns, []CustomType{ct})

						// The one unrepresentable combination: a
						// null-namespace type recurring inside a namespaced
						// scope has no reference spelling.
						if spelling == "nullns" && occurrences == 2 && ns != "" {
							if err == nil {
								t.Fatalf("null-namespace type recurring under WithNamespace must error; got schema %s", s.String())
							}
							want := fmt.Sprintf("the null-namespace type %q recurs inside namespace %q", fullnames[0], ns)
							if !strings.Contains(err.Error(), want) {
								t.Fatalf("error %q does not name the corner (%q)", err, want)
							}
							return
						}
						if err != nil {
							t.Fatalf("cell errored: %v", err)
						}
						if _, err := Parse(s.String()); err != nil {
							t.Fatalf("output does not re-parse: %v", err)
						}
						top := "Top"
						if ns != "" {
							top = ns + ".Top"
						}
						assertScopeFullnames(t, s, append([]string{top}, fullnames...))
					})
				}
			}
		}
	}

	// Coexistence cells: distinct fullnames sharing a short name must
	// coexist; identical definitions supplied by two DIFFERENT customs
	// must dedup to one definition plus a reference.
	for _, ns := range []string{"", "b"} {
		nsName := fmt.Sprintf("ns=%q", ns)
		top := "Top"
		if ns != "" {
			top = ns + ".Top"
		}
		t.Run("coexist/aX_nullX/"+nsName, func(t *testing.T) {
			aNode, _ := buildScopeCustomNode(t, "split", "record", "flat")
			nullNode, _ := buildScopeCustomNode(t, "nullns", "record", "flat")
			fields := []reflect.StructField{
				{Name: "F1", Type: primary},
				{Name: "F2", Type: partner},
			}
			customs := []CustomType{
				{GoType: primary, Schema: aNode},
				{GoType: partner, Schema: nullNode},
			}
			s, err := schemaForScopeCell(t, fields, ns, customs)
			if err != nil {
				t.Fatalf("a.X + null-namespace X must coexist: %v", err)
			}
			assertScopeFullnames(t, s, []string{top, "a.X", "X"})
		})
		t.Run("coexist/aX_bX/"+nsName, func(t *testing.T) {
			aNode, _ := buildScopeCustomNode(t, "split", "record", "flat")
			bSchema, err := Parse(`{"type":"record","name":"X","namespace":"b","fields":[{"name":"n","type":"int"}]}`)
			if err != nil {
				t.Fatal(err)
			}
			bRoot := bSchema.Root()
			fields := []reflect.StructField{
				{Name: "F1", Type: primary},
				{Name: "F2", Type: partner},
			}
			customs := []CustomType{
				{GoType: primary, Schema: aNode},
				{GoType: partner, Schema: bRoot},
			}
			s, err := schemaForScopeCell(t, fields, ns, customs)
			if err != nil {
				t.Fatalf("a.X + b.X must coexist: %v", err)
			}
			assertScopeFullnames(t, s, []string{top, "a.X", "b.X"})
		})
		t.Run("coexist/identical_dedup/"+nsName, func(t *testing.T) {
			n1, _ := buildScopeCustomNode(t, "split", "record", "flat")
			n2, _ := buildScopeCustomNode(t, "dotted", "record", "flat")
			// Two distinct customs carry the SAME definition of a.X — one
			// split-derived, one dotted hand-built — so the dedup must
			// treat the spellings as one name and emit one definition
			// plus a reference, exercising the scope-normalized equality.
			fields := []reflect.StructField{
				{Name: "F1", Type: primary},
				{Name: "F2", Type: partner},
			}
			customs := []CustomType{
				{GoType: primary, Schema: n1},
				{GoType: partner, Schema: n2},
			}
			s, err := schemaForScopeCell(t, fields, ns, customs)
			if err != nil {
				t.Fatalf("identical a.X definitions from two customs must dedup: %v", err)
			}
			assertScopeFullnames(t, s, []string{top, "a.X"})
			// Exactly one inline definition: the second occurrence is a
			// reference, so the schema text contains a single "fields"
			// body for a.X.
			if n := strings.Count(s.String(), `"name":"n"`); n != 1 {
				t.Errorf("want exactly one inline a.X definition, found %d bodies in %s", n, s.String())
			}
		})
	}

	// Wrong-bind decoy: a null-namespace custom X used before AND after a
	// Go-inferred record that owns fullname b.X. The recurrence of the
	// null-namespace X inside scope b must hit the corner error — a bare
	// "X" reference would silently bind the DIFFERENT type b.X.
	t.Run("corner/wrongbind_decoy", func(t *testing.T) {
		nullNode, _ := buildScopeCustomNode(t, "nullns", "record", "flat")
		type X struct{ M int32 }
		fields := []reflect.StructField{
			{Name: "F1", Type: primary},
			{Name: "F2", Type: reflect.TypeFor[X]()},
			{Name: "F3", Type: primary},
		}
		customs := []CustomType{{GoType: primary, Schema: nullNode}}
		_, err := schemaForScopeCell(t, fields, "b", customs)
		if err == nil || !strings.Contains(err.Error(), `the null-namespace type "X" recurs inside namespace "b"`) {
			t.Fatalf("decoy cell must hit the corner error, got: %v", err)
		}
	})

	// Spelling equivalence: the spec makes the split and dotted spellings
	// one name, so for every (kind, shape, occurrences, scope) the two
	// spellings' outputs must agree byte-for-byte on Canonical().
	for _, ks := range kindShapes {
		for _, occurrences := range []int{1, 2} {
			for _, ns := range []string{"", "b"} {
				name := fmt.Sprintf("equiv/%s_%s/occ%d/ns=%q", ks.kind, ks.shape, occurrences, ns)
				t.Run(name, func(t *testing.T) {
					splitNode, _ := buildScopeCustomNode(t, "split", ks.kind, ks.shape)
					dottedNode, _ := buildScopeCustomNode(t, "dotted", ks.kind, ks.shape)
					sSplit, err := schemaForScopeCell(t, scopeCellFields(occurrences, primary), ns, []CustomType{{GoType: primary, Schema: splitNode}})
					if err != nil {
						t.Fatalf("split: %v", err)
					}
					sDotted, err := schemaForScopeCell(t, scopeCellFields(occurrences, primary), ns, []CustomType{{GoType: primary, Schema: dottedNode}})
					if err != nil {
						t.Fatalf("dotted: %v", err)
					}
					if string(sSplit.Canonical()) != string(sDotted.Canonical()) {
						t.Errorf("split and dotted spellings disagree:\n split: %s\ndotted: %s", sSplit.Canonical(), sDotted.Canonical())
					}
				})
			}
		}
	}

	// Props-carried container routes: a Props VALUE shaped like (or
	// containing) a named definition is reachable by the composition
	// walkers through the items/values keys and union slices, and the
	// metadata render hands it over BY REFERENCE when it needs no JSON
	// fixup. Every route × scope must leave the caller's storage untouched
	// — the cell helper's snapshot asserts that — and the direct map check
	// below re-asserts it on the user's own map object, independent of the
	// snapshot machinery.
	for _, route := range []string{"items", "values", "unionslice"} {
		for _, ns := range []string{"", "b"} {
			t.Run(fmt.Sprintf("propscarried/%s/ns=%q", route, ns), func(t *testing.T) {
				userOwned := map[string]any{"type": "fixed", "name": "G", "size": 1}
				want := map[string]any{"type": "fixed", "name": "G", "size": 1}
				var carried any = userOwned
				if route == "unionslice" {
					carried = []any{userOwned}
				}
				key := route
				if route == "unionslice" {
					key = "items"
				}
				node := &SchemaNode{Type: "string", Props: map[string]any{key: carried}}
				_, err := schemaForScopeCell(t, scopeCellFields(1, primary), ns, []CustomType{{GoType: primary, Schema: node}})
				if err != nil {
					t.Fatalf("cell errored: %v", err)
				}
				if !reflect.DeepEqual(userOwned, want) {
					t.Errorf("caller-owned Props map changed: %v, want %v", userOwned, want)
				}
			})
		}
	}
}

// The EXECUTED fastavro arm for this matrix lives in
// matrix_schemafor_scope_differential_test.go (package avro_test, where the
// oracle harness lives), driving representative cells through the public
// SchemaFor entry point.

// ---------- null_spelling_schemafor_test.go ----------

// Avro spells the null type two ways — the bare primitive string "null" and
// the wrapped object {"type":"null"} — and they denote the same type: same
// branch, same wire bytes, same canonical form. Props and a logicalType on a
// wrapped null are inert (Avro defines no null logical type), so a
// carrier-bearing wrapped null is still a null branch.
//
// SchemaFor decides "is this union branch null?" on a PRE-PARSE tree of
// `any` — a representation distinct from the parsed aschema and the compiled
// node — at two points: the pointer collapse (a nullable T inside a nullable
// T must not nest a union inside a union) and the null-first default fill.
// Both decisions must see both spellings, because the tree they decide on is
// handed straight to the very parser that treats the two as one type.
//
// The renderer emits a wrapped null bare when it carries nothing, so only a
// carrier-bearing wrapped null (props, or a logicalType) survives the render
// as an object — those are the spellings these tests use.

// nullSpellUnions returns the union spellings that must behave identically,
// keyed by a subtest-safe name. "bare" is the control: it exercised the
// pre-fix code path, so a test whose control fails is measuring the wrong
// thing.
func nullSpellUnions() []struct{ name, union string } {
	return []struct{ name, union string }{
		{"bare", `["null","string"]`},
		{"wrapped_plain", `[{"type":"null"},"string"]`},
		{"wrapped_props", `[{"type":"null","x":1},"string"]`},
		{"wrapped_logicaltype", `[{"type":"null","logicalType":"nope"},"string"]`},
	}
}

// nullSpellMarker is the Go type the spelling tests' CustomTypes match on.
type nullSpellMarker struct{ A int64 }

// nullSpellCustom builds a CustomType whose Schema is the parsed union.
func nullSpellCustom(t *testing.T, union string) CustomType {
	t.Helper()
	s, err := Parse(union)
	if err != nil {
		t.Fatalf("parse custom union %s: %v", union, err)
	}
	root := s.Root()
	return CustomType{GoType: reflect.TypeFor[nullSpellMarker](), Schema: root}
}

// TestCensus_SchemaForPointerCollapseWrappedNullBranch pins that the
// pointer arm's union collapse recognizes a null first branch in either
// spelling. A *T field whose CustomType supplies a null-first union must
// collapse to that union; keying the collapse on the bare spelling alone
// emits ["null", [<union>]], which Avro forbids — the build then fails on a
// schema whose bare-spelled twin builds fine.
func TestCensus_SchemaForPointerCollapseWrappedNullBranch(t *testing.T) {
	ptrTo := reflect.PointerTo(reflect.TypeFor[nullSpellMarker]())
	fields := []reflect.StructField{{Name: "F", Type: ptrTo}}

	var want string
	for _, tc := range nullSpellUnions() {
		t.Run(tc.name, func(t *testing.T) {
			s, err := schemaForScopeCell(t, fields, "", []CustomType{nullSpellCustom(t, tc.union)})
			if err != nil {
				t.Fatalf("build failed for a null-first union: %v", err)
			}
			if strings.Contains(s.String(), `[["null"`) || strings.Contains(s.String(), `,["null"`) {
				t.Fatalf("emitted a union nested directly inside a union: %s", s.String())
			}
			// Every spelling denotes one type, so the canonical forms —
			// which strip the inert carriers — must be byte-identical.
			if want == "" {
				want = string(s.Canonical())
			} else if got := string(s.Canonical()); got != want {
				t.Fatalf("canonical form differs by null spelling:\n got %s\nwant %s", got, want)
			}
		})
	}
}

// TestCensus_SchemaForNullFirstDefaultWrappedNullBranch pins that the
// null-first default fill recognizes both spellings. The assertion is on the
// EMITTED SCHEMA TEXT, not on twmb's decode behavior: twmb synthesizes an
// implicit null default for a nullable union at parse, so the omission is
// invisible in-process, but the emitted text is what a caller publishes to a
// registry or hands to another implementation — and Java and fastavro do not
// infer the default. Without "default":null those readers cannot read data
// written before the field existed.
func TestCensus_SchemaForNullFirstDefaultWrappedNullBranch(t *testing.T) {
	fields := []reflect.StructField{{Name: "F", Type: reflect.TypeFor[nullSpellMarker]()}}

	for _, tc := range nullSpellUnions() {
		t.Run(tc.name, func(t *testing.T) {
			s, err := schemaForScopeCell(t, fields, "", []CustomType{nullSpellCustom(t, tc.union)})
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			var doc struct {
				Fields []map[string]json.RawMessage `json:"fields"`
			}
			if err := json.Unmarshal([]byte(s.String()), &doc); err != nil {
				t.Fatalf("emitted schema does not unmarshal: %v", err)
			}
			if len(doc.Fields) != 1 {
				t.Fatalf("want 1 field, got %d: %s", len(doc.Fields), s.String())
			}
			raw, ok := doc.Fields[0]["default"]
			if !ok {
				t.Fatalf("emitted schema omits the null-first union's \"default\":null: %s", s.String())
			}
			if string(raw) != "null" {
				t.Fatalf("default is %s, want null: %s", raw, s.String())
			}
			// The metadata surface must agree with the emitted text.
			if f := s.Root().Fields[0]; !f.HasDefault || f.Default != nil {
				t.Fatalf("Root() reports HasDefault=%v Default=%#v, want true/nil", f.HasDefault, f.Default)
			}
		})
	}
}

// TestMatrix_SchemaForNullBranchSpellingParity crosses the null-SPELLING
// axis into the SchemaFor composition space: for every union-bearing cell,
// respelling the null branch must not change the built schema.
//
// Axes: spelling {bare, wrapped-plain, wrapped-props, wrapped-logicalType} ×
// union shape {null-first 2-branch, null-first 3-branch, null-SECOND
// 2-branch} × field shape {value, pointer} × occurrences {1, 2} × SchemaFor
// scope {default, WithNamespace}.
//
// The oracle is per-cell equivalence against the bare spelling, which is the
// control the pre-fix code already handled: identical build verdict (both
// succeed or both fail), identical Canonical() (PCF strips the inert
// carriers, so the four spellings collapse to one form — a calibration-free
// comparison), identical fingerprint, identical per-field default presence,
// and identical wire bytes for a probe value. Cells whose bare form is
// itself an error (a null-SECOND union at a pointer field nests a union in a
// union in every spelling) must fail the same way in every spelling — the
// invariant is agreement, not success.
func TestMatrix_SchemaForNullBranchSpellingParity(t *testing.T) {
	marker := reflect.TypeFor[nullSpellMarker]()
	ptrTo := reflect.PointerTo(marker)

	// Each shape names how to spell its null branch: %s is substituted with
	// the spelling under test.
	shapes := []struct{ name, tmpl string }{
		{"nullfirst2", `[%s,"string"]`},
		{"nullfirst3", `[%s,"string","long"]`},
		{"nullsecond2", `["string",%s]`},
	}
	spellings := []struct{ name, null string }{
		{"bare", `"null"`},
		{"wrapped_plain", `{"type":"null"}`},
		{"wrapped_props", `{"type":"null","x":1}`},
		{"wrapped_logicaltype", `{"type":"null","logicalType":"nope"}`},
	}

	type outcome struct {
		errored     bool
		canonical   string
		fingerprint string
		defaults    string
		emitted     string
	}

	cells := 0
	for _, shape := range shapes {
		for _, fieldShape := range []string{"value", "pointer"} {
			for _, occurrences := range []int{1, 2} {
				for _, ns := range []string{"", "b"} {
					goType := marker
					if fieldShape == "pointer" {
						goType = ptrTo
					}
					var control outcome
					for i, sp := range spellings {
						name := fmt.Sprintf("%s/%s/occ%d/ns=%q/%s", shape.name, fieldShape, occurrences, ns, sp.name)
						t.Run(name, func(t *testing.T) {
							cells++
							union := fmt.Sprintf(shape.tmpl, sp.null)
							s, err := schemaForScopeCell(t, scopeCellFields(occurrences, goType), ns, []CustomType{nullSpellCustom(t, union)})
							got := outcome{errored: err != nil}
							if err == nil {
								got.canonical = string(s.Canonical())
								got.fingerprint = fmt.Sprintf("%x", s.Fingerprint(NewRabin()))
								got.defaults = nullSpellDefaults(t, s)
								got.emitted = s.String()
								if _, perr := Parse(got.emitted); perr != nil {
									t.Fatalf("emitted schema does not re-parse: %v\n%s", perr, got.emitted)
								}
							}
							if i == 0 {
								control = got
								return
							}
							if got.errored != control.errored {
								t.Fatalf("build verdict differs from the bare control: errored=%v (control %v); emitted %s",
									got.errored, control.errored, got.emitted)
							}
							if got.errored {
								return // both spellings reject: agreement is the invariant
							}
							if got.canonical != control.canonical {
								t.Fatalf("canonical differs from the bare control:\n got %s\nwant %s", got.canonical, control.canonical)
							}
							if got.fingerprint != control.fingerprint {
								t.Fatalf("fingerprint differs from the bare control: got %s want %s", got.fingerprint, control.fingerprint)
							}
							if got.defaults != control.defaults {
								t.Fatalf("field defaults differ from the bare control:\n got %s\nwant %s\nemitted %s",
									got.defaults, control.defaults, got.emitted)
							}
						})
					}
				}
			}
		}
	}
	t.Logf("cells=%d", cells)
}

// nullSpellDefaults renders each field's default presence and value from the
// EMITTED text, so the comparison sees exactly what a caller publishes.
func nullSpellDefaults(t *testing.T, s *Schema) string {
	t.Helper()
	var doc struct {
		Fields []map[string]json.RawMessage `json:"fields"`
	}
	if err := json.Unmarshal([]byte(s.String()), &doc); err != nil {
		t.Fatalf("emitted schema does not unmarshal: %v", err)
	}
	var b strings.Builder
	for i, f := range doc.Fields {
		if i > 0 {
			b.WriteByte(';')
		}
		if raw, ok := f["default"]; ok {
			fmt.Fprintf(&b, "default=%s", raw)
		} else {
			b.WriteString("absent")
		}
	}
	return b.String()
}

// ---------- embed_placement_test.go ----------

// Embedded-field name collisions: WHERE the decision is made.
//
// Two implementations answer "which of two same-named promoted fields wins,
// and when is the collision ambiguous?" — collectFields, for SchemaFor, and
// typeFieldMapping, the shared field map for encode and decode. They agree on
// the RULE. What this file guards is that they agree on where the rule RUNS.
//
// The rule ranges over the whole collected field set: shallowest depth wins,
// and only a tie at the winning depth is ambiguous. A resolution step that
// ranges over the whole set but is written as the trailing block of the
// RECURSIVE collector runs once per level instead of once per type, on a
// partial set — so a collision one level below the root is decided before the
// level that resolves it has been read, and any index the step resolves is in
// the root's coordinate space while its receiver is the nested type.
//
// No verdict-comparison net can see that: at the root both placements agree.
// The discriminating observation is the SAME construct at several nesting
// depths, which is the axis this matrix drives.
//
// The oracle is Go itself. reflect.Type.FieldByName implements the language's
// promotion rule and reports an ambiguous promoted name by returning false;
// it is placement-blind by construction, so it decides every untagged cell
// here without reference to anything this package does.

// ---------- the shapes ----------
//
// epLeaf's V is reachable through two sibling embed paths, which is what makes
// epCollide's V a genuine same-depth ambiguity. Everything below places that
// one construct at a different distance from the root.

type epLeaf struct{ V int }

type epWrapA struct{ epLeaf }
type epWrapB struct{ epLeaf }

// epCollide: V promoted from two paths at equal depth — ambiguous.
type epCollide struct {
	epWrapA
	epWrapB
}

type epCollideD1 struct{ epCollide }
type epCollideD2 struct{ epCollideD1 }
type epCollideD3 struct{ epCollideD2 }

// epResolved: the same ambiguity with a shallower V that resolves it. Go
// promotes the shallow one; encoding/json marshals it.
type epResolved struct {
	epCollide
	V int
}

type epResolvedD1 struct{ epResolved }
type epResolvedD2 struct{ epResolvedD1 }
type epResolvedD3 struct{ epResolvedD2 }

// epRootResolves is the sharpest cell: the ambiguity is three levels down and
// the field that resolves it is at the ROOT, so a decision taken at the
// collision's own level cannot possibly see it.
type epRootResolves struct {
	epCollideD2
	V int
}

// Pointer carrier: the promotion path crosses an embedded *struct.
type epWrapPA struct{ *epLeaf }
type epWrapPB struct{ *epLeaf }
type epCollideP struct {
	epWrapPA
	epWrapPB
}
type epCollidePD1 struct{ epCollideP }
type epResolvedP struct {
	epCollideP
	V int
}
type epResolvedPD1 struct{ epResolvedP }

// Tag tier: the collision exists only in AVRO name space (the Go names
// differ), so Go has no opinion and the package's documented tiebreaker
// decides — tagged beats untagged at equal depth. Placement must not change
// that either.
type epTagPlain struct{ Shared int32 }
type epTagNamed struct {
	Renamed int32 `avro:"Shared"`
}
type epTagCollide struct {
	epTagPlain
	epTagNamed
}
type epTagCollideD1 struct{ epTagCollide }
type epTagCollideD2 struct{ epTagCollideD1 }

// ---------- the matrix ----------

type epCell struct {
	name  string
	typ   reflect.Type
	depth int // how far below the root the colliding pair sits
}

// epUntagged are the cells Go's own promotion rule decides.
func epUntagged() []epCell {
	return []epCell{
		{"struct/collide/d0", reflect.TypeFor[epCollide](), 0},
		{"struct/collide/d1", reflect.TypeFor[epCollideD1](), 1},
		{"struct/collide/d2", reflect.TypeFor[epCollideD2](), 2},
		{"struct/collide/d3", reflect.TypeFor[epCollideD3](), 3},
		{"struct/resolved/d0", reflect.TypeFor[epResolved](), 0},
		{"struct/resolved/d1", reflect.TypeFor[epResolvedD1](), 1},
		{"struct/resolved/d2", reflect.TypeFor[epResolvedD2](), 2},
		{"struct/resolved/d3", reflect.TypeFor[epResolvedD3](), 3},
		{"struct/root-resolves-deep-collision", reflect.TypeFor[epRootResolves](), 3},
		{"pointer/collide/d0", reflect.TypeFor[epCollideP](), 0},
		{"pointer/collide/d1", reflect.TypeFor[epCollidePD1](), 1},
		{"pointer/resolved/d0", reflect.TypeFor[epResolvedP](), 0},
		{"pointer/resolved/d1", reflect.TypeFor[epResolvedPD1](), 1},
	}
}

// TestMatrix_EmbedCollisionVerdictIsPlacementInvariant is the class
// elimination. Every cell is the same collision at a different distance from
// the root; Go decides each one, and both of this package's answerers must
// return Go's verdict at every distance.
func TestMatrix_EmbedCollisionVerdictIsPlacementInvariant(t *testing.T) {
	for _, c := range epUntagged() {
		t.Run(c.name, func(t *testing.T) {
			// The oracle: Go's own promotion. false means "ambiguous
			// selector", which is a compile error for a program that writes
			// x.V — the exact condition this package reports as a duplicate
			// field name.
			_, goResolves := c.typ.FieldByName("V")

			cfErr := epCollectErr(t, c.typ)
			tfmErr := epMappingErr(t, c.typ)

			if goResolves {
				if cfErr != nil {
					t.Errorf("SchemaFor's collector rejects a type Go promotes unambiguously (x.V compiles): %v", cfErr)
				}
				if tfmErr != nil {
					t.Errorf("the runtime field map rejects a type Go promotes unambiguously (x.V compiles): %v", tfmErr)
				}
			} else {
				if cfErr == nil {
					t.Errorf("SchemaFor's collector accepts a type whose V is an ambiguous selector in Go")
				}
				if tfmErr == nil {
					t.Errorf("the runtime field map accepts a type whose V is an ambiguous selector in Go")
				}
			}
			// The two answerers must not merely both be right about Go; they
			// must agree with each other, which is what makes a schema built
			// by SchemaFor usable by Encode and Decode.
			if (cfErr == nil) != (tfmErr == nil) {
				t.Errorf("the two answerers disagree: collector err=%v, runtime field map err=%v", cfErr, tfmErr)
			}
		})
	}
}

// TestMatrix_EmbedCollisionErrorNamesTheCollidingFields pins the other half of
// the placement fact: the error is built by resolving field INDEX PATHS, and
// an index path accumulated from the root only denotes a field when it is
// resolved against the root. Reported against any other type it names a
// different field, or steps into a non-struct and panics.
func TestMatrix_EmbedCollisionErrorNamesTheCollidingFields(t *testing.T) {
	for _, c := range epUntagged() {
		if _, ok := c.typ.FieldByName("V"); ok {
			continue // no error to inspect
		}
		t.Run(c.name, func(t *testing.T) {
			err := epCollectErr(t, c.typ)
			if err == nil {
				t.Fatalf("want a duplicate-field error")
			}
			msg := err.Error()
			// The colliding Go fields are both named V, and the type the
			// caller asked about is the one that must be named.
			if !strings.Contains(msg, `"V" and "V"`) {
				t.Errorf("error names the wrong Go fields: %s", msg)
			}
			if !strings.Contains(msg, c.typ.String()) {
				t.Errorf("error blames %s, but the type asked about is %s: %s", "another type", c.typ, msg)
			}
		})
	}
}

// TestMatrix_EmbedTagTierIsPlacementInvariant covers the tier Go has no
// opinion about: the collision is in Avro name space only, so the package's
// documented tiebreaker decides, and it must decide the same way wherever the
// pair sits.
func TestMatrix_EmbedTagTierIsPlacementInvariant(t *testing.T) {
	cells := []epCell{
		{"tag/d0", reflect.TypeFor[epTagCollide](), 0},
		{"tag/d1", reflect.TypeFor[epTagCollideD1](), 1},
		{"tag/d2", reflect.TypeFor[epTagCollideD2](), 2},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			fields, err := collectFields(c.typ, make(map[reflect.Type]bool))
			if err != nil {
				t.Fatalf("tagged beats untagged at equal depth, so this resolves: %v", err)
			}
			var got []string
			for _, f := range fields {
				got = append(got, f.name)
			}
			if len(got) != 1 || got[0] != "Shared" {
				t.Fatalf("want exactly one field %q; got %v", "Shared", got)
			}
			// The winner must be the TAGGED field, at every depth: the
			// runtime map selects by index path, so ask it which Go field it
			// picked rather than trusting the name.
			m, err := typeFieldMapping([]string{"Shared"}, nil, c.typ)
			if err != nil {
				t.Fatalf("runtime field map: %v", err)
			}
			ft := fieldTypeByIndex(c.typ, m.indices[0])
			if ft.Kind() != reflect.Int32 {
				t.Fatalf("runtime map selected a %s field; the tagged winner is int32", ft)
			}
			sf := epFieldByIndexPath(c.typ, m.indices[0])
			if sf.Name != "Renamed" {
				t.Errorf("runtime map selected Go field %q; the tagged field %q wins at equal depth", sf.Name, "Renamed")
			}
		})
	}
}

// TestMatrix_EmbedCollisionBelowRootDoesNotPanic is the public-entry
// pin. SchemaFor is generic, so these are written out rather than generated;
// the panic they lock is a reflect index path resolved against the wrong
// type, and it needs no collision at the root to fire.
func TestMatrix_EmbedCollisionBelowRootDoesNotPanic(t *testing.T) {
	cases := []struct {
		name     string
		fn       func() (*Schema, error)
		wantErr  bool
		goResolv bool
	}{
		{"collide-at-root", func() (*Schema, error) { return SchemaFor[epCollide]() }, true, false},
		{"collide-one-below-root", func() (*Schema, error) { return SchemaFor[epCollideD1]() }, true, false},
		{"collide-three-below-root", func() (*Schema, error) { return SchemaFor[epCollideD3]() }, true, false},
		{"resolved-at-root", func() (*Schema, error) { return SchemaFor[epResolved]() }, false, true},
		{"resolved-one-below-root", func() (*Schema, error) { return SchemaFor[epResolvedD1]() }, false, true},
		{"root-resolves-deep-collision", func() (*Schema, error) { return SchemaFor[epRootResolves]() }, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("SchemaFor panicked: %v", r)
				}
			}()
			s, err := c.fn()
			switch {
			case c.wantErr && err == nil:
				t.Errorf("want a duplicate-field error, got schema %s", s.String())
			case !c.wantErr && err != nil:
				t.Errorf("want a schema, got error: %v", err)
			}
		})
	}
}

// TestRegression_EmbedResolvedBelowRootRoundTrips pins the consequence a
// caller sees: a type whose deep collision is resolved by a shallower field
// is one Go promotes, encoding/json marshals, and this package's own encoder
// already handles — so SchemaFor must produce a schema for it, and that
// schema must round-trip the promoted value.
func TestRegression_EmbedResolvedBelowRootRoundTrips(t *testing.T) {
	// A panic here would take the binary down and hide every other result,
	// and the failure being pinned is reachable as one.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked instead of building a schema: %v", r)
		}
	}()
	var in epRootResolves
	in.V = 7

	// Go and encoding/json both resolve V to the shallow field.
	if in.V != 7 {
		t.Fatal("unreachable: the selector must compile")
	}
	jb, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("encoding/json: %v", err)
	}
	if !strings.Contains(string(jb), `"V":7`) {
		t.Fatalf("encoding/json promoted a different V: %s", jb)
	}

	s, err := SchemaFor[epRootResolves]()
	if err != nil {
		t.Fatalf("SchemaFor rejected a type Go and encoding/json both resolve: %v", err)
	}
	b, err := s.Encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out epRootResolves
	if _, err := s.Decode(b, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.V != 7 {
		t.Errorf("round trip put the value in a different field: got V=%d, want 7", out.V)
	}
}

// ---------- helpers ----------

func epCollectErr(t *testing.T, typ reflect.Type) (err error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("PANIC: %v", r)
			t.Errorf("collectFields panicked on %s: %v", typ, r)
		}
	}()
	_, err = collectFields(typ, make(map[reflect.Type]bool))
	return err
}

func epMappingErr(t *testing.T, typ reflect.Type) (err error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("PANIC: %v", r)
			t.Errorf("typeFieldMapping panicked on %s: %v", typ, r)
		}
	}()
	_, err = typeFieldMapping([]string{"V"}, nil, typ)
	return err
}

// epFieldByIndexPath walks an index path the way the encoders do, so the test
// reads the same coordinate space they do.
func epFieldByIndexPath(t reflect.Type, index []int) reflect.StructField {
	var sf reflect.StructField
	for _, i := range index {
		if t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		sf = t.Field(i)
		t = sf.Type
	}
	return sf
}

// ---------- embed_selection_test.go ----------

// Exported carrier types so reflect.StructOf can embed them anonymously
// (StructOf rejects unexported embedded types). Each EX_k promotes a field
// "X" at a distinct depth, so a struct embedding a SUBSET of them has X
// reachable at several different depths through different paths — the shape
// the repeated-embed bug lived in.
type EmbedX0 struct {
	X int32 `avro:"X"`
}
type EmbedX1 struct{ EmbedX0 }
type EmbedX2 struct{ EmbedX1 }
type EmbedX3 struct{ EmbedX2 }

// TestGenerative_EmbedSelectionMatchesGoPromotion is the GENERATIVE net for
// embedded-field selection. It sweeps the embed lattice — structs embedding
// every ordered subset of the depth carriers above, as value AND pointer
// embeds, with and without a direct field — and for every shape asserts
// twmb's selected field equals Go's OWN field promotion (reflect.FieldByName,
// the resolver Go uses for v.X). That is the narrow, correct oracle for
// doc.go's "shallowest wins": NOT encoding/json (whose tag namespace, tag
// options, and case-insensitive decode differ from avro's), but Go promotion
// itself, which is tag-independent.
//
// Oracle scope: every field's avro name equals its Go field name (no
// rename), so "which field does name N resolve to" is a pure Go-promotion
// question both twmb and reflect answer identically. Out of scope (no
// external oracle — twmb-DEFINED policy, pinned separately): tagged renames
// colliding with promoted names, and equal-depth ties where reflect abstains.
func TestGenerative_EmbedSelectionMatchesGoPromotion(t *testing.T) {
	carriers := []reflect.Type{
		reflect.TypeFor[EmbedX0](), reflect.TypeFor[EmbedX1](),
		reflect.TypeFor[EmbedX2](), reflect.TypeFor[EmbedX3](),
	}
	i32 := reflect.TypeFor[int32]()
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"X","type":"int"}]}`)

	carrierName := func(ct reflect.Type) string {
		if ct.Kind() == reflect.Pointer {
			return ct.Elem().Name()
		}
		return ct.Name()
	}

	var checked int
	check := func(t *testing.T, fields []reflect.StructField) {
		st := reflect.StructOf(fields)
		pv := reflect.New(st)
		setEveryX(pv.Elem()) // distinct value in every physical X occurrence

		of := pv.Elem().FieldByName("X")
		if !of.IsValid() {
			return // equal-depth ambiguity: Go abstains, separate policy pin
		}
		want := of.Int()
		checked++

		// Encode must read the Go-promoted field.
		data, err := s.AppendEncode(nil, pv.Interface())
		if err != nil {
			t.Fatalf("%s: encode: %v", fieldList(st), err)
		}
		var out map[string]any
		if _, err := s.Decode(data, &out); err != nil {
			t.Fatalf("%s: decode: %v", fieldList(st), err)
		}
		if int64(out["X"].(int32)) != want {
			t.Fatalf("%s: encode selected a field disagreeing with Go promotion: twmb X=%v, reflect.FieldByName=%d",
				fieldList(st), out["X"], want)
		}

		// Decode must WRITE the Go-promoted field.
		zero := reflect.New(st)
		allocPointers(zero.Elem())
		wire, _ := s.AppendEncode(nil, map[string]any{"X": int32(12345)})
		if _, err := s.Decode(wire, zero.Interface()); err != nil {
			t.Fatalf("%s: decode into struct: %v", fieldList(st), err)
		}
		if got := zero.Elem().FieldByName("X").Int(); got != 12345 {
			t.Fatalf("%s: decode wrote a field disagreeing with Go promotion: FieldByName=%d, want 12345",
				fieldList(st), got)
		}
	}

	// Depth lattice: every ordered subset (size 1..3) of the value carriers,
	// with and without a direct field, in two orders.
	t.Run("depth-lattice", func(t *testing.T) {
		var combos [][]int
		var gen func(prefix []int, start int)
		gen = func(prefix []int, start int) {
			if len(prefix) >= 1 {
				combos = append(combos, append([]int(nil), prefix...))
			}
			if len(prefix) == 3 {
				return
			}
			for i := start; i < len(carriers); i++ {
				gen(append(prefix, i), i+1)
			}
		}
		gen(nil, 0)
		for _, direct := range []bool{false, true} {
			for _, combo := range combos {
				for _, order := range [][]int{combo, reversed(combo)} {
					var fields []reflect.StructField
					if direct {
						fields = append(fields, reflect.StructField{Name: "X", Type: i32, Tag: `avro:"X"`})
					}
					for _, ci := range order {
						ct := carriers[ci]
						fields = append(fields, reflect.StructField{Name: ct.Name(), Type: ct, Anonymous: true})
					}
					check(t, fields)
				}
			}
		}
	})

	// Pointer dimension: every ordered pair of distinct carriers as value OR
	// pointer embeds (the field-mapper unwraps pointer embeds).
	t.Run("value-and-pointer-embeds", func(t *testing.T) {
		var variants []reflect.Type
		for _, c := range carriers {
			variants = append(variants, c, reflect.PointerTo(c))
		}
		for i := range variants {
			for j := range variants {
				if i == j {
					continue
				}
				vi, vj := variants[i], variants[j]
				if carrierName(vi) == carrierName(vj) {
					continue // two fields can't share the embedded type name
				}
				check(t, []reflect.StructField{
					{Name: carrierName(vi), Type: vi, Anonymous: true},
					{Name: carrierName(vj), Type: vj, Anonymous: true},
				})
			}
		}
	})

	if checked < 40 {
		t.Fatalf("generator covered only %d shapes — generation regressed", checked)
	}
	t.Logf("checked %d generated embed shapes against Go promotion", checked)
}

// setEveryX sets a distinct value in every physical X occurrence, allocating
// pointer embeds along the way.
var embedXSeq int32

func setEveryX(v reflect.Value) {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		setEveryX(v.Elem())
		return
	}
	if v.Kind() != reflect.Struct {
		return
	}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Name == "X" && f.Type.Kind() == reflect.Int32 {
			embedXSeq++
			v.Field(i).SetInt(int64(embedXSeq))
			continue
		}
		if f.Anonymous {
			setEveryX(v.Field(i))
		}
	}
}

// allocPointers pre-allocates pointer embeds so a decode-target struct can
// receive the promoted field (decode does its own allocation, but the
// FieldByName oracle read afterward must not hit a nil pointer).
func allocPointers(v reflect.Value) {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		allocPointers(v.Elem())
		return
	}
	if v.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < v.NumField(); i++ {
		if v.Type().Field(i).Anonymous {
			allocPointers(v.Field(i))
		}
	}
}

func reversed(a []int) []int {
	r := make([]int, len(a))
	for i, x := range a {
		r[len(a)-1-i] = x
	}
	return r
}

func fieldList(t reflect.Type) string {
	s := "struct{"
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		s += " " + f.Name
		if f.Type.Kind() == reflect.Pointer {
			s += "*"
		}
	}
	return s + " }"
}

// TestRegression_EmbedEqualDepthAmbiguity pins twmb's LAZY handling of an
// equal-depth name collision through two embeds. The collision is genuinely
// ambiguous (Go makes the selector a compile error; encoding/json silently
// drops the field). twmb's contract:
//   - SchemaFor REJECTS (eager — it must emit every field, and cannot emit two
//     with the same name).
//   - Runtime encode/decode (shared typeFieldMapping) reject ONLY when the
//     schema actually resolves a field to the ambiguous name. A coincidental
//     collision on a name the schema never references — e.g. two embedded
//     library structs that happen to share a field name — does NOT break the
//     struct; the other fields work. When the schema DOES use the ambiguous
//     name, the error is loud and has encode/decode parity (vs json's silent
//     drop, or the old silent first-win). The runtime is schema-driven, so it
//     errors lazily; SchemaFor sees all fields, so it errors eagerly — a
//     justified scoping difference, not a contradiction.
func TestRegression_EmbedEqualDepthAmbiguity(t *testing.T) {
	type C struct {
		Dup int32 `avro:"dup"`
	}
	type A struct{ C }
	type B struct{ C }
	// R collides on "dup" at equal depth, and also has a clean "keep" field.
	type R struct {
		A
		B
		Keep int32 `avro:"keep"`
	}

	// SchemaFor rejects eagerly (cannot represent two "dup" fields).
	if _, err := SchemaFor[R](); err == nil {
		t.Fatal("SchemaFor[R] must reject the equal-depth collision")
	}

	// Schema that NEVER references the ambiguous "dup": encode + decode work.
	clean := MustParse(`{"type":"record","name":"R","fields":[{"name":"keep","type":"int"}]}`)
	var r R
	r.Keep = 7
	wire, err := clean.AppendEncode(nil, &r)
	if err != nil {
		t.Fatalf("unreferenced collision must NOT break the struct: encode errored: %v", err)
	}
	var into R
	if _, err := clean.Decode(wire, &into); err != nil {
		t.Fatalf("unreferenced collision must NOT break decode: %v", err)
	}
	if into.Keep != 7 {
		t.Fatalf("keep round-trip: got %d want 7", into.Keep)
	}

	// Schema that DOES reference the ambiguous "dup": encode AND decode reject.
	ambig := MustParse(`{"type":"record","name":"R","fields":[{"name":"dup","type":"int"}]}`)
	if _, err := ambig.AppendEncode(nil, &r); err == nil {
		t.Fatal("encode must reject when the schema resolves a field to the ambiguous name")
	}
	dwire, _ := MustParse(`{"type":"record","name":"R","fields":[{"name":"dup","type":"int"}]}`).AppendEncode(nil, map[string]any{"dup": int32(9)})
	var into2 R
	if _, err := ambig.Decode(dwire, &into2); err == nil {
		t.Fatal("decode must reject the ambiguous name too (encode/decode parity)")
	}
}

// A name that a higher-priority field unambiguously OWNS is not an ambiguous
// collision, even when lower-priority fields collide among themselves at a
// deeper-or-equal level. SchemaFor must accept such a struct and infer the
// single winning field, matching the runtime field mapper (typeFieldMapping)
// and Go's own field promotion — both of which resolve the name. The
// resolution is DEFERRED: the resolving field may be declared AFTER the
// colliding pair (the common "embeds first, own fields after" layout), so
// erroring the instant two deep fields collide wrongly rejects a struct whose
// name a shallower or tagged field owns. The encode/decode round-trip is the
// parity oracle: SchemaFor's inferred mapping must match what the codec uses.
func TestRegression_SchemaForResolvableCollisionNotAmbiguous(t *testing.T) {
	t.Run("shallower field declared last resolves a deep collision", func(t *testing.T) {
		type EmbA struct{ Name string } // depth 2, untagged
		type EmbB struct{ Name string } // depth 2, untagged
		type Outer struct {
			EmbA
			EmbB
			Name string // depth 1, declared last: Go resolves Outer.Name here
		}
		s, err := SchemaFor[Outer]()
		if err != nil {
			t.Fatalf("SchemaFor must accept a struct whose name a shallower field owns: %v", err)
		}
		root := s.Root()
		if len(root.Fields) != 1 || root.Fields[0].Name != "Name" {
			t.Fatalf("expected a single inferred field %q, got %s", "Name", s.String())
		}
		// Parity: the codec maps "Name" to the direct (shallowest) field.
		wire, err := s.AppendEncode(nil, Outer{Name: "direct"})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got Outer
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Name != "direct" {
			t.Fatalf("\"Name\" mapped to a shadowed field, not the direct one: %+v", got)
		}
	})

	t.Run("tagged field declared last resolves a same-depth untagged collision", func(t *testing.T) {
		type EmbA struct{ Name string } // depth 2, untagged
		type EmbB struct{ Name string } // depth 2, untagged
		type EmbTagged struct {
			Other string `avro:"Name"`
		} // depth 2, tagged "Name"
		type Outer struct {
			EmbA
			EmbB
			EmbTagged // tag tiebreak wins over the untagged pair at the same depth
		}
		s, err := SchemaFor[Outer]()
		if err != nil {
			t.Fatalf("SchemaFor must accept a struct whose name a tagged field owns: %v", err)
		}
		if len(s.Root().Fields) != 1 {
			t.Fatalf("expected a single inferred field, got %s", s.String())
		}
		// Parity: the codec maps "Name" to the tagged field.
		wire, err := s.AppendEncode(nil, Outer{EmbTagged: EmbTagged{Other: "tagged"}})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got Outer
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.EmbTagged.Other != "tagged" {
			t.Fatalf("\"Name\" mapped to an untagged field, not the tagged one: %+v", got)
		}
	})

	// Boundary the other direction: a same-depth same-tagged collision with NO
	// higher-priority resolver is genuinely ambiguous and must STILL reject —
	// the fix defers the decision, it does not disable it.
	t.Run("unresolved same-depth collision still rejects", func(t *testing.T) {
		type EmbA struct{ Dup int32 }
		type EmbB struct{ Dup int32 }
		type Outer struct {
			EmbA
			EmbB
		}
		if _, err := SchemaFor[Outer](); err == nil {
			t.Fatal("a genuinely ambiguous same-depth collision must still reject")
		}
	})
}

// ---------- embed_shape_generative_test.go ----------

// ===========================================================================
// The generative adversarial-struct-shape net for SchemaFor's field selection.
//
// ONE generator (genStructuralShapes / genTagEdgeShapes), not hand cases. Every
// shape is a reflect.StructOf type built by crossing the axes that the embedded-
// field selection bugs lived in:
//
//   - diamond embeds            (a base reached through two arms at EQUAL depth)
//   - equal-depth collisions    (two DISTINCT types promoting the same name)
//   - repeated-type two-depth   (one type reached directly AND through an embed)
//   - embedded vs named fields  (a direct field colliding with a promoted one)
//   - tagged vs untagged         (a rename colliding with a promoted Go name)
//   - malformed / edge tags      (genTagEdgeShapes: inline-on-non-struct, name+
//                                 inline, decimal trailing junk, dash+options,
//                                 narrow-int default bounds, uuid/plain dedup)
//
// For every shape the net asserts the two field-mapping walkers AGREE:
//
//     SchemaFor's    collectFields  (schema_for.go)   -- the schema builder
//     the runtime's  typeFieldMapping (reflect.go)    -- shared by encode AND
//                                                         decode (ser/deser/
//                                                         json_codec/json_decode/
//                                                         resolve/unsafe)
//
// on (1) WHICH Go field each Avro name resolves to, and (2) the RESOLVED schema
// (exercised end-to-end through the real Encode/Decode path). The two diverging
// is the failure mode Family 5 keeps hitting (commits 692b039, a1c4b25,
// 6ce8257): a silently-picked wrong field, an embed pruned by a marked-forever
// visited map, an ambiguity one walker rejects and the other first-wins.
//
// Non-vacuity is NOT self-asserted: the walkers are cross-checked against an
// INDEPENDENT oracle — Go's own field promotion (reflect.FieldByName) for the
// untagged shapes, and a from-scratch precedence resolver (oracleResolve,
// validated against FieldByName on the untagged shapes) for the tagged ones.
// If the two walkers ever drifted in lockstep, the FieldByName oracle still
// catches it. The neutering record at the bottom of this file documents the
// exact reverts that turn cells red, and what they were measured to do.
//
// The eager/lazy split is part of the contract, not a divergence: SchemaFor
// REJECTS any ambiguous collision (it must emit every field), while the runtime
// defers — it errors only when a schema field actually RESOLVES to an ambiguous
// name, so a coincidental collision the schema never references does not break
// the struct. The net asserts BOTH halves.
// ===========================================================================

// ---- carrier alphabet -----------------------------------------------------
//
// Exported (reflect.StructOf rejects unexported embedded fields) named types,
// each promoting an "N" field at a controlled depth through a controlled type,
// so subsets of them embedded as siblings synthesize every structural family.

type GA struct{ N int32 } // untagged N, depth 1 when embedded
type GB struct{ N int32 } // DISTINCT type, also untagged N
type GTag struct {
	M int32 `avro:"N"`
}                            // TAGGED N (Go field "M")
type GMid struct{ GA }       // N one level deeper
type GDeep struct{ GMid }    // N two levels deeper
type GBase struct{ N int32 } // diamond base
type GL struct{ GBase }      // diamond arm L
type GR struct{ GBase }      // diamond arm R

func structuralCarriers() []reflect.Type {
	return []reflect.Type{
		reflect.TypeFor[GA](), reflect.TypeFor[GB](), reflect.TypeFor[GTag](),
		reflect.TypeFor[GMid](), reflect.TypeFor[GDeep](),
		reflect.TypeFor[GBase](), reflect.TypeFor[GL](), reflect.TypeFor[GR](),
	}
}

// embedName is the field name an anonymous embed of ct must carry (the
// unqualified type name; the element name for a pointer embed).
func embedName(ct reflect.Type) string {
	if ct.Kind() == reflect.Pointer {
		return ct.Elem().Name()
	}
	return ct.Name()
}

func anonEmbed(ct reflect.Type) reflect.StructField {
	return reflect.StructField{Name: embedName(ct), Type: ct, Anonymous: true}
}

// ---- schemaForType: a faithful, reflect.Type-driven replica of SchemaFor ---
//
// SchemaFor is generic (SchemaFor[T]); the generator produces reflect.Type at
// run time, which a generic call cannot take. This mirrors SchemaFor's body
// exactly (the only addition: a synthetic name for the unnamed StructOf top
// type, which the real WithName supplies). TestGenerative_SchemaForReplicaParity
// pins it byte-identical to the real SchemaFor on named anchor types so it
// cannot silently drift from the entry point under test.
func schemaForType(t reflect.Type, opts ...SchemaOpt) (*Schema, error) {
	var o schemaOpts
	var customTypes []CustomType
	for _, opt := range opts {
		switch v := opt.(type) {
		case withNamespace:
			o.namespace = string(v)
		case withName:
			o.name = string(v)
		case CustomType:
			customTypes = append(customTypes, v)
		}
	}
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("avro: SchemaFor requires a struct type, got %s", t)
	}
	name := o.name
	if name == "" {
		name = t.Name()
	}
	if name == "" {
		name = "GenRec"
	}
	seen := make(map[reflect.Type]seenForm)
	s, err := inferRecord(t, name, o.namespace, seen, customTypes, make(appliedTypeAliases))
	if err != nil {
		return nil, err
	}
	s, err = dedupNamedTypes(s, make(map[string]string), "")
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("avro: marshaling inferred schema: %w", err)
	}
	return Parse(string(b), opts...)
}

// ---- the independent oracle ------------------------------------------------
//
// oracleResolve computes, FROM SCRATCH, which Go field each Avro name resolves
// to and which names are ambiguous, using a naive per-path walk (a cycle guard
// on the on-path slice — per-path by construction, so it cannot share the
// marked-forever prune bug) and the DOCUMENTED precedence rule applied to the
// full candidate set at once (gather-all-then-argmin), as opposed to the two
// walkers' single-pass iterative dedup. It calls NEITHER collectFields NOR
// typeFieldMapping. Its structural walk is validated against reflect.FieldByName
// (Go's own promotion) on every untagged shape; the only logic FieldByName does
// not cover — tagged-beats-untagged — is small and additionally pinned by the
// kept hand regressions.

type oracleCand struct {
	index  []int
	tagged bool
}

type oracleResult struct {
	names     []string // every resolvable Avro name, sorted
	winner    map[string][]int
	ambiguous map[string]bool
	cands     map[string][]oracleCand // every physical occurrence per name
}

// minimalTag splits an avro tag the way a reader that only needs name + inline
// would: a plain comma split. The structural family uses only bracket-free
// tags (rename, "-", ",inline", ",omitzero"), so this matches splitTag without
// borrowing its code — keeping the oracle independent.
func minimalTag(tag string) (name string, opts []string) {
	parts := strings.Split(tag, ",")
	return parts[0], parts[1:]
}

func oracleResolve(t reflect.Type) oracleResult {
	cands := map[string][]oracleCand{}
	var walk func(t reflect.Type, index []int, onPath []reflect.Type)
	walk = func(t reflect.Type, index []int, onPath []reflect.Type) {
		if slices.Contains(onPath, t) {
			return // cycle: this type is already on the current path
		}
		onPath = append(onPath, t)
		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)
			idx := append(append([]int(nil), index...), i)
			tag := sf.Tag.Get("avro")
			if sf.Anonymous {
				ft := sf.Type
				if ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				if ft.Kind() == reflect.Struct {
					if tag == "-" {
						continue
					}
					name, _ := minimalTag(tag)
					if name != "" {
						// Explicit name on an embed: a single named field, not flattened.
						cands[name] = append(cands[name], oracleCand{idx, true})
						continue
					}
					walk(ft, idx, onPath)
					continue
				}
				if !sf.IsExported() {
					continue
				}
			} else if !sf.IsExported() {
				continue
			}
			if tag == "-" {
				continue
			}
			name, opts := minimalTag(tag)
			if slices.Contains(opts, "inline") {
				ft := sf.Type
				if ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				if ft.Kind() == reflect.Struct {
					walk(ft, idx, onPath)
					continue
				}
			}
			tagged := name != ""
			if name == "" {
				name = sf.Name
			}
			cands[name] = append(cands[name], oracleCand{idx, tagged})
		}
	}
	walk(t, nil, nil)

	res := oracleResult{
		winner:    map[string][]int{},
		ambiguous: map[string]bool{},
		cands:     cands,
	}
	for name, cs := range cands {
		// Tagged beats untagged at ANY depth: if any candidate is tagged,
		// only tagged candidates remain in contention.
		anyTagged := false
		for _, c := range cs {
			if c.tagged {
				anyTagged = true
				break
			}
		}
		pool := cs[:0:0]
		for _, c := range cs {
			if c.tagged == anyTagged {
				pool = append(pool, c)
			}
		}
		// Among the pool, the shallowest (shortest index) wins; a tie at the
		// shallowest depth is genuinely ambiguous.
		minDepth := len(pool[0].index)
		for _, c := range pool {
			if len(c.index) < minDepth {
				minDepth = len(c.index)
			}
		}
		var atMin []oracleCand
		for _, c := range pool {
			if len(c.index) == minDepth {
				atMin = append(atMin, c)
			}
		}
		if len(atMin) == 1 {
			res.winner[name] = atMin[0].index
		} else {
			res.ambiguous[name] = true
		}
		res.names = append(res.names, name)
	}
	sort.Strings(res.names)
	return res
}

// ---- generated structural shapes -------------------------------------------

type genShape struct {
	label     string
	t         reflect.Type
	hasTag    bool // any avro tag anywhere -> FieldByName oracle applies only when false
	hasInline bool // ,inline-flattened fields have no Go-promotion analog -> skip FieldByName
	hasPtr    bool // carrier 0 is a POINTER embed -> the occupancy axis applies
}

// genStructuralShapes crosses: every ordered subset (size 1..3) of the carrier
// alphabet as the embeds; an optional direct field colliding on "N" (untagged,
// or tagged via a differently-named Go field) placed BEFORE or AFTER the
// embeds; an optional clean non-colliding "Keep" field; and a pointer variant
// (the first embed made a pointer). Names per shape are a subset of {N, Keep}.
func genStructuralShapes() []genShape {
	carriers := structuralCarriers()
	var embedArrangements [][]reflect.Type
	var gen func(prefix []reflect.Type, used map[string]bool)
	gen = func(prefix []reflect.Type, used map[string]bool) {
		if len(prefix) >= 1 {
			embedArrangements = append(embedArrangements, append([]reflect.Type(nil), prefix...))
		}
		if len(prefix) == 3 {
			return
		}
		for _, c := range carriers {
			if used[embedName(c)] {
				continue // two embeds cannot share a Go field name
			}
			used[embedName(c)] = true
			gen(append(prefix, c), used)
			delete(used, embedName(c))
		}
	}
	gen(nil, map[string]bool{})

	type directOpt struct {
		label string
		field *reflect.StructField // nil = none
		tag   bool
	}
	i32 := reflect.TypeFor[int32]()
	directOpts := []directOpt{
		{"noDirect", nil, false},
		{"directN", &reflect.StructField{Name: "N", Type: i32}, false},
		{"directTagN", &reflect.StructField{Name: "Dir", Type: i32, Tag: `avro:"N"`}, true},
	}
	keepField := reflect.StructField{Name: "Keep", Type: i32}

	var shapes []genShape
	for _, arr := range embedArrangements {
		// inl renders the carriers as ,inline-flattened NAMED fields instead of
		// anonymous embeds — the other flattening mechanism. The collision tree
		// is identical (both walk into the carrier at the same index), but inline
		// has no Go-promotion analog, so FieldByName cannot oracle it; oracleResolve
		// (validated against FieldByName on the anonymous-embed shapes) + the
		// two-walker agreement carry it.
		for _, inl := range []bool{false, true} {
			for _, ptr := range []bool{false, true} {
				embeds := make([]reflect.StructField, len(arr))
				for i, c := range arr {
					ct := c
					if ptr && i == 0 {
						ct = reflect.PointerTo(c)
					}
					if inl {
						embeds[i] = reflect.StructField{Name: fmt.Sprintf("Inl%d", i), Type: ct, Tag: `avro:",inline"`}
					} else {
						embeds[i] = anonEmbed(ct)
					}
				}
				for _, d := range directOpts {
					positions := []string{"after"}
					if d.field != nil {
						positions = []string{"before", "after"}
					}
					for _, pos := range positions {
						for _, keep := range []bool{false, true} {
							var fields []reflect.StructField
							addDirect := func() {
								if d.field != nil {
									fields = append(fields, *d.field)
								}
								if keep {
									fields = append(fields, keepField)
								}
							}
							if pos == "before" {
								addDirect()
								fields = append(fields, embeds...)
							} else {
								fields = append(fields, embeds...)
								addDirect()
							}
							st := reflect.StructOf(fields)
							hasTag := d.tag || inl // ,inline is itself a tag
							for _, c := range arr {
								if c == reflect.TypeFor[GTag]() {
									hasTag = true
								}
							}
							names := make([]string, 0, len(arr))
							for _, c := range arr {
								names = append(names, c.Name()[:1])
							}
							label := fmt.Sprintf("carriers=%v inline=%v ptr=%v %s/%s keep=%v",
								names, inl, ptr, d.label, pos, keep)
							shapes = append(shapes, genShape{label: label, t: st, hasTag: hasTag, hasInline: inl, hasPtr: ptr})
						}
					}
				}
			}
		}
	}
	return shapes
}

// ---- value plumbing for the round-trip ------------------------------------

// setLeafInt sets the int32 at index, allocating any nil pointer along the path
// (e.g. a ,inline *struct field, which allocPointers — anonymous-only — skips).
func setLeafInt(structVal reflect.Value, index []int, v int32) {
	fv := structVal
	for _, i := range index {
		for fv.Kind() == reflect.Pointer {
			if fv.IsNil() {
				fv.Set(reflect.New(fv.Type().Elem()))
			}
			fv = fv.Elem()
		}
		fv = fv.Field(i)
	}
	fv.SetInt(int64(v))
}

// readLeafInt reads the int32 at index, returning 0 when a pointer along the
// path is nil (a field the decoder legitimately never allocated).
func readLeafInt(structVal reflect.Value, index []int) int32 {
	fv := structVal
	for _, i := range index {
		for fv.Kind() == reflect.Pointer {
			if fv.IsNil() {
				return 0
			}
			fv = fv.Elem()
		}
		fv = fv.Field(i)
	}
	return int32(fv.Int())
}

func intRecord(names []string) string {
	var b strings.Builder
	b.WriteString(`{"type":"record","name":"R","fields":[`)
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"name":%q,"type":"int"}`, n)
	}
	b.WriteString(`]}`)
	return b.String()
}

// ---- the net ---------------------------------------------------------------

func TestGenerative_EmbedShapeWalkerAgreement(t *testing.T) {
	shapes := genStructuralShapes()
	var checkedWinners, checkedAmbig, roundTripped, fieldByNameChecks int
	var nilEmbedRoundTrips int

	for _, sh := range shapes {
		or := oracleResolve(sh.t)
		anyAmbig := len(or.ambiguous) > 0

		// (A) Validate the oracle against Go's own promotion for every name
		//     with no tagged candidate (pure Go-promotion question). Skipped for
		//     ,inline shapes: inline flattening has no Go-promotion analog (Go
		//     does not promote through a non-anonymous field), so FieldByName
		//     would not find the flattened name — oracleResolve (validated here
		//     on the anonymous-embed shapes) and the two-walker agreement carry
		//     the inline shapes instead.
		for _, n := range or.names {
			if sh.hasInline {
				break
			}
			tagged := false
			for _, c := range or.cands[n] {
				if c.tagged {
					tagged = true
				}
			}
			if tagged {
				continue
			}
			fbn, ok := sh.t.FieldByName(n)
			fieldByNameChecks++
			if or.ambiguous[n] {
				if ok {
					t.Fatalf("%s: oracle says %q ambiguous but reflect.FieldByName resolved it to %v", sh.label, n, fbn.Index)
				}
			} else {
				if !ok {
					t.Fatalf("%s: oracle resolved %q to %v but reflect.FieldByName abstained (ambiguous)", sh.label, n, or.winner[n])
				}
				if !reflect.DeepEqual(fbn.Index, or.winner[n]) {
					t.Fatalf("%s: oracle %q=%v disagrees with reflect.FieldByName=%v", sh.label, n, or.winner[n], fbn.Index)
				}
			}
		}

		// (B) collectFields (SchemaFor's walker): eager-rejects any ambiguity,
		//     else resolves every name to the oracle's winner.
		cf, cfErr := collectFields(sh.t, make(map[reflect.Type]bool))
		if anyAmbig {
			if cfErr == nil {
				t.Fatalf("%s: collectFields accepted an ambiguous shape (oracle ambiguous: %v)", sh.label, ambigNames(or))
			}
		} else {
			if cfErr != nil {
				t.Fatalf("%s: collectFields rejected an unambiguous shape: %v", sh.label, cfErr)
			}
			cfMap := map[string][]int{}
			for _, f := range cf {
				cfMap[f.name] = f.index
			}
			if len(cfMap) != len(or.names) {
				t.Fatalf("%s: collectFields names %v != oracle names %v", sh.label, sortedKeys(cfMap), or.names)
			}
			for _, n := range or.names {
				if !reflect.DeepEqual(cfMap[n], or.winner[n]) {
					t.Fatalf("%s: collectFields %q=%v != oracle %v", sh.label, n, cfMap[n], or.winner[n])
				}
			}
		}

		// (C) typeFieldMapping (the runtime walker): per-name lazy resolution.
		for _, n := range or.names {
			m, err := typeFieldMapping([]string{n}, nil, sh.t)
			if or.ambiguous[n] {
				if err == nil {
					t.Fatalf("%s: typeFieldMapping([%q]) accepted an ambiguous name", sh.label, n)
				}
				checkedAmbig++
			} else {
				if err != nil {
					t.Fatalf("%s: typeFieldMapping([%q]) rejected a resolvable name: %v", sh.label, n, err)
				}
				if !reflect.DeepEqual(m.indices[0], or.winner[n]) {
					t.Fatalf("%s: typeFieldMapping %q=%v != oracle %v", sh.label, n, m.indices[0], or.winner[n])
				}
				checkedWinners++
			}
		}

		// (C2) typeFieldMapping over ALL names at once mirrors collectFields'
		//      verdict: ambiguous -> reject, else resolve every name.
		mAll, errAll := typeFieldMapping(or.names, nil, sh.t)
		if anyAmbig {
			if errAll == nil {
				t.Fatalf("%s: typeFieldMapping(all names) accepted despite an ambiguous name", sh.label)
			}
		} else {
			if errAll != nil {
				t.Fatalf("%s: typeFieldMapping(all names) rejected: %v", sh.label, errAll)
			}
			for i, n := range or.names {
				if !reflect.DeepEqual(mAll.indices[i], or.winner[n]) {
					t.Fatalf("%s: typeFieldMapping(all) %q=%v != oracle %v", sh.label, n, mAll.indices[i], or.winner[n])
				}
			}
		}

		// (D) Resolved schema + end-to-end round trip.
		if !anyAmbig {
			s, err := schemaForType(sh.t, WithName("R"))
			if err != nil {
				t.Fatalf("%s: schemaForType rejected an unambiguous shape: %v", sh.label, err)
			}
			gotNames := map[string]bool{}
			for _, f := range s.Root().Fields {
				gotNames[f.Name] = true
			}
			if len(gotNames) != len(or.names) {
				t.Fatalf("%s: schema field names %v != oracle %v", sh.label, gotNames, or.names)
			}
			roundTripWinners(t, sh, s, or)
			roundTripped++
			if sh.hasPtr {
				roundTripNilEmbed(t, sh, s, or)
				nilEmbedRoundTrips++
			}
		} else {
			// Lazy contract: a schema over only the NON-ambiguous names still
			// round-trips (the coincidental collision does not break the
			// struct); a schema over an ambiguous name rejects on encode AND
			// decode (parity), never silently first-wins.
			var clean []string
			for _, n := range or.names {
				if !or.ambiguous[n] {
					clean = append(clean, n)
				}
			}
			if len(clean) > 0 {
				cs := MustParse(intRecord(clean))
				roundTripWinners(t, sh, cs, restrict(or, clean))
			}
			as := MustParse(intRecord([]string{firstAmbig(or)}))
			src := reflect.New(sh.t)
			allocPointers(src.Elem())
			if _, err := as.AppendEncode(nil, src.Interface()); err == nil {
				t.Fatalf("%s: encode must reject a schema resolving to ambiguous %q", sh.label, firstAmbig(or))
			}
			wire, _ := as.AppendEncode(nil, map[string]any{firstAmbig(or): int32(1)})
			dst := reflect.New(sh.t)
			if _, err := as.Decode(wire, dst.Interface()); err == nil {
				t.Fatalf("%s: decode must reject a schema resolving to ambiguous %q (parity)", sh.label, firstAmbig(or))
			}
		}
	}

	if checkedWinners < 100 || checkedAmbig < 50 || roundTripped < 100 {
		t.Fatalf("generator under-covered: winners=%d ambig=%d roundtrips=%d shapes=%d — generation regressed",
			checkedWinners, checkedAmbig, roundTripped, len(shapes))
	}
	// The occupancy arm is only meaningful if pointer-embed shapes actually
	// reach it: allocPointers made every generated pointer embed non-nil for
	// this net's whole history, so a zero here means the axis went dead again.
	if nilEmbedRoundTrips < 100 {
		t.Fatalf("nil-embed occupancy arm ran %d times — the pointer-embed axis is not being generated",
			nilEmbedRoundTrips)
	}
	t.Logf("structural net: %d shapes | %d winner resolutions | %d ambiguity rejections | %d round trips (%d with a NIL pointer embed) | %d FieldByName cross-checks",
		len(shapes), checkedWinners, checkedAmbig, roundTripped, nilEmbedRoundTrips, fieldByNameChecks)
}

// roundTripNilEmbed is the OCCUPANCY arm of the pointer-embed axis. The shape
// generator wraps carrier 0 in a pointer on half the shapes, but roundTripWinners
// calls allocPointers before encoding, so for this net's whole history every
// generated pointer embed reached the codecs ALLOCATED. A NIL embed takes a
// different arm — fieldByIndexZero returns the zero of fieldTypeByIndex's
// resolved type instead of walking — and that arm is reached from three distinct
// encode sites (ser.go's reflect path, unsafe.go's compiled slow-field arm, and
// json_codec.go), any of which could panic on a nil deref without the net
// noticing.
//
// The expectation is not read off this package: a value whose fields are all
// zero has one image, so the struct with the embed left NIL must encode to
// exactly what the same schema produces for an explicit all-zero MAP — the map
// encoder being a path that never touches fieldByIndexZero at all. Encode then
// implies decode: the wire must read back, allocating the embed on the way in.
func roundTripNilEmbed(t *testing.T, sh genShape, s *Schema, or oracleResult) {
	t.Helper()

	zeros := map[string]any{}
	for _, n := range or.names {
		zeros[n] = int32(0)
	}
	want, err := s.AppendEncode(nil, zeros)
	if err != nil {
		t.Fatalf("%s: encoding the all-zero map twin: %v", sh.label, err)
	}
	wantJSON, err := s.EncodeJSON(zeros)
	if err != nil {
		t.Fatalf("%s: JSON-encoding the all-zero map twin: %v", sh.label, err)
	}

	// nilV's pointer embeds are left exactly as reflect.New made them: nil.
	nilV := reflect.New(sh.t)
	for _, c := range []struct {
		route string
		enc   func() ([]byte, error)
		want  []byte
	}{
		// Addressable: the compiled record, whose promoted field cannot have
		// a fixed offset and so takes the slow fieldByIndexZero arm.
		{"binary/compiled", func() ([]byte, error) { return s.AppendEncode(nil, nilV.Interface()) }, want},
		// Non-addressable: ser.go's reflect path.
		{"binary/reflect", func() ([]byte, error) { return s.AppendEncode(nil, nilV.Elem().Interface()) }, want},
		{"json", func() ([]byte, error) { return s.EncodeJSON(nilV.Interface()) }, wantJSON},
	} {
		got, err := func() (b []byte, err error) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("%s: %s encode PANICKED on a nil embedded pointer: %v", sh.label, c.route, r)
				}
			}()
			return c.enc()
		}()
		if err != nil {
			t.Fatalf("%s: %s encode of a nil embedded pointer: %v", sh.label, c.route, err)
		}
		if !bytes.Equal(got, c.want) {
			t.Fatalf("%s: %s encode of a nil embedded pointer = %x, want the all-zero image %x",
				sh.label, c.route, got, c.want)
		}
	}

	// Encode implies decode: the zero image must read back into the same shape,
	// which is where fieldByIndex allocates the embed it just read as zero.
	dst := reflect.New(sh.t)
	if _, err := s.Decode(want, dst.Interface()); err != nil {
		t.Fatalf("%s: decoding the all-zero image back into the shape: %v", sh.label, err)
	}
	for _, n := range or.names {
		if got := readLeafInt(dst.Elem(), or.winner[n]); got != 0 {
			t.Fatalf("%s: %q read back as %d, want 0", sh.label, n, got)
		}
	}
}

// embedIndexSites is the set of fieldByIndex / fieldByIndexZero call sites,
// keyed "file.go:enclosingFunc" and valued by the number of calls there. It is
// the set TestMatrix_NilEmbedPointerRouteAgreement claims to drive, and
// TestInvariant_EveryFieldByIndexSiteHasARouteCell derives the REAL set from
// source and fails when the two disagree in either direction — a new call site
// landing without a route cell, or a listed one going away.
//
// A promoted field's Go destination is reached only through these two helpers,
// so this table is the route inventory for the whole embedded-pointer class.
var embedIndexSites = map[string]int{
	// decode (fieldByIndex — allocates a nil embed, or refuses cleanly)
	"unsafe.go:deserRecordFast":                     1, // binary: the ONLY binary decode route (struct records always compile)
	"json_decode.go:jsonDecoder.decodeRecordStruct": 2, // JSON: present-key arm + default-fill arm
	"resolve.go:resolvedRecord.deserStruct":         2, // resolved: writer-op arm + reader-default arm
	// encode (fieldByIndexZero — reads a nil embed as zero)
	"ser.go:serRecord.ser":               1, // binary, non-addressable (reflect path)
	"unsafe.go:serRecordFast":            1, // binary, addressable (compiled slow-field arm)
	"json_codec.go:appendAvroJSONRecord": 1, // JSON
}

// A field promoted through an embedded POINTER reaches its Go destination via
// fieldByIndex (decode) and fieldByIndexZero (encode): helpers that allocate a
// nil embed on the way in, refuse cleanly when Go reflection cannot allocate it
// (an embed named through an UNEXPORTED type is unsettable), and read a nil
// embed as zero on the way out.
//
// The verdict is a property of the Go SHAPE, not of the wire, so every route
// reaching those helpers owes the same answer — and the routes are not one path.
// Binary decode reaches fieldByIndex only through the COMPILED record
// (deserRecordFast's slow-field arm); JSON decode has a present-key arm and a
// separate default-fill arm; the RESOLVED decoder has its own writer-op and
// reader-default arms. Five decode sites, three encode sites, all in
// embedIndexSites.
//
// AXES: occupancy {nil, pre-allocated} x embed exportedness {exported,
// unexported} x route {the eight sites above}.
//
// ORACLE: encoding/json, decoded into the SAME Go types. It is an independent
// implementation of the same Go-reflection constraint, and fieldByIndex's own
// comment claims parity with it — so the accept/reject verdict is taken from it
// cell for cell rather than read off this package. Its ENCODE behavior is
// deliberately NOT the oracle: json omits a nil embed's promoted fields, while
// an Avro record has no absent field and writes the zero. The encode arm uses
// the all-zero map twin instead (a path that never touches fieldByIndexZero).
func TestMatrix_NilEmbedPointerRouteAgreement(t *testing.T) {
	full := MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"c","type":"int"}]}`)
	// withDefault carries a default for "a" so the JSON decoder's default-fill
	// arm — a SECOND fieldByIndex site — runs when "a" is absent.
	withDefault := MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":5},{"name":"c","type":"int"}]}`)
	// wideWriter carries a field the reader drops, so resolution is real (a skip
	// op beside the reads) and the resolved decoder's WIRE-OP arm runs.
	wideWriter := MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"x","type":"int"},{"name":"c","type":"int"}]}`)
	// thinWriter lacks "a" entirely, so the reader's own default fills it and the
	// resolved decoder's DEFAULT arm runs.
	thinWriter := MustParse(`{"type":"record","name":"R","fields":[{"name":"c","type":"int"}]}`)

	resolvedWire, err := Resolve(wideWriter, full)
	if err != nil {
		t.Fatalf("resolve (writer-op arm): %v", err)
	}
	resolvedDflt, err := Resolve(thinWriter, withDefault)
	if err != nil {
		t.Fatalf("resolve (reader-default arm): %v", err)
	}
	mustEnc := func(s *Schema, v any) []byte {
		t.Helper()
		b, err := s.AppendEncode(nil, v)
		if err != nil {
			t.Fatalf("encode fixture: %v", err)
		}
		return b
	}
	binFull := mustEnc(full, map[string]any{"a": int32(7), "c": int32(3)})
	binWide := mustEnc(wideWriter, map[string]any{"a": int32(7), "x": int32(9), "c": int32(3)})
	binThin := mustEnc(thinWriter, map[string]any{"c": int32(3)})

	// The two targets differ ONLY in whether the embedded pointer's type is
	// exported. Everything else — field names, tags, promoted depth — is equal,
	// so a verdict that differs between them can only be the exportedness rule.
	type target struct {
		label string
		fresh func(alloc bool) any
		read  func(any) (a, c int32, embedSet bool)
	}
	targets := []target{{
		label: "exported embed type (*EmbeddedInner)",
		fresh: func(alloc bool) any {
			v := &withNilEmbedPtr{}
			if alloc {
				v.EmbeddedInner = &EmbeddedInner{}
			}
			return v
		},
		read: func(p any) (int32, int32, bool) {
			v := p.(*withNilEmbedPtr)
			if v.EmbeddedInner == nil {
				return 0, v.C, false
			}
			return v.A, v.C, true
		},
	}, {
		label: "unexported embed type (*unexportedInner)",
		fresh: func(alloc bool) any {
			v := &withUnexportedEmbedPtr{}
			if alloc {
				v.unexportedInner = &unexportedInner{}
			}
			return v
		},
		read: func(p any) (int32, int32, bool) {
			v := p.(*withUnexportedEmbedPtr)
			if v.unexportedInner == nil {
				return 0, v.C, false
			}
			return v.A, v.C, true
		},
	}}

	type route struct {
		label string
		site  string // must be a key of embedIndexSites
		wantA int32  // 7 from the wire, 5 from a schema default
		run   func(dst any) error
	}
	routes := []route{{
		"binary, compiled record", "unsafe.go:deserRecordFast", 7,
		func(dst any) error { _, e := full.Decode(binFull, dst); return e },
	}, {
		"JSON, key present", "json_decode.go:jsonDecoder.decodeRecordStruct", 7,
		func(dst any) error { return full.DecodeJSON([]byte(`{"a":7,"c":3}`), dst) },
	}, {
		"JSON, key absent (default fill)", "json_decode.go:jsonDecoder.decodeRecordStruct", 5,
		func(dst any) error { return withDefault.DecodeJSON([]byte(`{"c":3}`), dst) },
	}, {
		"resolved, writer op", "resolve.go:resolvedRecord.deserStruct", 7,
		func(dst any) error { _, e := resolvedWire.Decode(binWide, dst); return e },
	}, {
		"resolved, reader default", "resolve.go:resolvedRecord.deserStruct", 5,
		func(dst any) error { _, e := resolvedDflt.Decode(binThin, dst); return e },
	}}
	for _, r := range routes {
		if _, ok := embedIndexSites[r.site]; !ok {
			t.Fatalf("route %q names site %q, which is not in embedIndexSites", r.label, r.site)
		}
	}

	var accepted, rejected int
	for _, tg := range targets {
		for _, alloc := range []bool{false, true} {
			occ := "nil embed"
			if alloc {
				occ = "pre-allocated embed"
			}

			// The oracle runs on the identical Go shape, one document carrying
			// the same two values under encoding/json's own field names.
			oracleDst := tg.fresh(alloc)
			oracleErr := json.Unmarshal([]byte(`{"A":7,"C":3}`), oracleDst)
			wantReject := oracleErr != nil
			if wantReject {
				rejected++
			} else {
				accepted++
			}

			for _, r := range routes {
				t.Run(fmt.Sprintf("%s/%s/%s", tg.label, occ, r.label), func(t *testing.T) {
					dst := tg.fresh(alloc)
					err := func() (err error) {
						defer func() {
							if p := recover(); p != nil {
								t.Fatalf("PANICKED where encoding/json returns %v: %v", oracleErr, p)
							}
						}()
						return r.run(dst)
					}()
					if wantReject {
						if err == nil {
							t.Fatalf("accepted a shape encoding/json refuses (%v)", oracleErr)
						}
						if !strings.Contains(err.Error(), "unexported embedded pointer") {
							t.Errorf("error must name what refused it, got: %v", err)
						}
						return
					}
					if err != nil {
						t.Fatalf("refused a shape encoding/json accepts: %v", err)
					}
					a, c, set := tg.read(dst)
					if !set {
						t.Fatalf("decode left the embedded pointer nil")
					}
					if a != r.wantA || c != 3 {
						t.Fatalf("promoted a=%d c=%d, want a=%d c=3", a, c, r.wantA)
					}
				})
			}
		}
	}
	// A matrix whose oracle answers the same way in every cell is measuring
	// nothing: the exportedness axis must actually split the verdict.
	if accepted == 0 || rejected == 0 {
		t.Fatalf("oracle never split: %d accepting cells, %d rejecting — the exportedness axis is not being exercised", accepted, rejected)
	}

	// ENCODE. fieldByIndexZero never needs to SET the embed, so a nil one is
	// read as zero whatever its exportedness — on all three encode routes, which
	// must agree with the all-zero map twin.
	zeroImage := mustEnc(full, map[string]any{"a": int32(0), "c": int32(0)})
	zeroJSON, err := full.EncodeJSON(map[string]any{"a": int32(0), "c": int32(0)})
	if err != nil {
		t.Fatalf("encoding the all-zero JSON twin: %v", err)
	}
	for _, tg := range targets {
		nilV := tg.fresh(false)
		for _, c := range []struct {
			label, site string
			enc         func() ([]byte, error)
			want        []byte
		}{
			{"binary, compiled record", "unsafe.go:serRecordFast",
				func() ([]byte, error) { return full.AppendEncode(nil, nilV) }, zeroImage},
			{"binary, reflect path (non-addressable)", "ser.go:serRecord.ser",
				func() ([]byte, error) { return full.AppendEncode(nil, reflect.ValueOf(nilV).Elem().Interface()) }, zeroImage},
			{"JSON", "json_codec.go:appendAvroJSONRecord",
				func() ([]byte, error) { return full.EncodeJSON(nilV) }, zeroJSON},
		} {
			t.Run(fmt.Sprintf("%s/nil embed/encode %s", tg.label, c.label), func(t *testing.T) {
				if _, ok := embedIndexSites[c.site]; !ok {
					t.Fatalf("encode route names site %q, which is not in embedIndexSites", c.site)
				}
				got, err := func() (b []byte, err error) {
					defer func() {
						if p := recover(); p != nil {
							t.Fatalf("PANICKED encoding a nil embedded pointer: %v", p)
						}
					}()
					return c.enc()
				}()
				if err != nil {
					t.Fatalf("encoding a nil embedded pointer: %v", err)
				}
				if !bytes.Equal(got, c.want) {
					t.Fatalf("nil embed encoded as %q, want the all-zero image %q", got, c.want)
				}
			})
		}
	}
}

// The route inventory must be DERIVED, not listed: a new fieldByIndex /
// fieldByIndexZero call site is a new route through the embedded-pointer class,
// and one landing without a cell in TestMatrix_NilEmbedPointerRouteAgreement is
// exactly how four of the five decode sites came to be unreachable by the whole
// suite. The guard fails in BOTH directions — an unlisted site appearing, and a
// listed site going away — so it can neither let a new member ship unexercised
// nor go stale after a removal.
func TestInvariant_EveryFieldByIndexSiteHasARouteCell(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	found := map[string]int{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), e.Name(), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			name := fd.Name.Name
			if fd.Recv != nil && len(fd.Recv.List) == 1 {
				rt := fd.Recv.List[0].Type
				if star, ok := rt.(*ast.StarExpr); ok {
					rt = star.X
				}
				if id, ok := rt.(*ast.Ident); ok {
					name = id.Name + "." + name
				}
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				id, ok := call.Fun.(*ast.Ident)
				if !ok {
					return true
				}
				if id.Name == "fieldByIndex" || id.Name == "fieldByIndexZero" {
					found[e.Name()+":"+name]++
				}
				return true
			})
		}
	}

	for site, n := range found {
		switch want, ok := embedIndexSites[site]; {
		case !ok:
			t.Errorf("%s calls fieldByIndex/fieldByIndexZero %d time(s) but has no route cell — "+
				"add it to embedIndexSites and drive it from TestMatrix_NilEmbedPointerRouteAgreement", site, n)
		case want != n:
			t.Errorf("%s has %d call(s), embedIndexSites claims %d — the extra call is a route with no cell", site, n, want)
		}
	}
	for site := range embedIndexSites {
		if _, ok := found[site]; !ok {
			t.Errorf("embedIndexSites lists %s, which no longer calls fieldByIndex/fieldByIndexZero — "+
				"the table is stale and its cell is measuring nothing", site)
		}
	}
}

// roundTripWinners proves, through the REAL Encode/Decode path (which consumes
// typeFieldMapping in ser.go / deser.go), that encode reads the winning field
// and decode writes it — never a shadowed loser. For each name it sets the
// winner to a sentinel and every loser to a distinct decoy; encode-then-decode-
// to-map must yield the sentinel (encode read the winner). Then it encodes a
// known map and decodes into a fresh struct: the winner must hold the value and
// every loser must stay zero (decode wrote the winner).
func roundTripWinners(t *testing.T, sh genShape, s *Schema, or oracleResult) {
	t.Helper()
	src := reflect.New(sh.t)
	allocPointers(src.Elem())
	sentinel := map[string]int32{}
	k := int32(0)
	for _, n := range or.names {
		k++
		sentinel[n] = 1000 + k
		decoy := int32(0)
		for _, c := range or.cands[n] {
			if reflect.DeepEqual(c.index, or.winner[n]) {
				setLeafInt(src.Elem(), c.index, sentinel[n])
			} else {
				decoy--
				setLeafInt(src.Elem(), c.index, -100+decoy)
			}
		}
	}
	data, err := s.AppendEncode(nil, src.Interface())
	if err != nil {
		t.Fatalf("%s: encode: %v", sh.label, err)
	}
	var out map[string]any
	if _, err := s.Decode(data, &out); err != nil {
		t.Fatalf("%s: decode-to-map: %v", sh.label, err)
	}
	for _, n := range or.names {
		if got, _ := out[n].(int32); got != sentinel[n] {
			t.Fatalf("%s: encode read a non-winner for %q: got %v want %d (winner index %v)",
				sh.label, n, out[n], sentinel[n], or.winner[n])
		}
	}

	wireVals := map[string]int32{}
	wm := map[string]any{}
	for i, n := range or.names {
		wireVals[n] = 2000 + int32(i)
		wm[n] = wireVals[n]
	}
	wire, err := s.AppendEncode(nil, wm)
	if err != nil {
		t.Fatalf("%s: encode map: %v", sh.label, err)
	}
	dst := reflect.New(sh.t)
	if _, err := s.Decode(wire, dst.Interface()); err != nil {
		t.Fatalf("%s: decode into struct: %v", sh.label, err)
	}
	for _, n := range or.names {
		for _, c := range or.cands[n] {
			got := readLeafInt(dst.Elem(), c.index)
			if reflect.DeepEqual(c.index, or.winner[n]) {
				if got != wireVals[n] {
					t.Fatalf("%s: decode did not write winner %q@%v: got %d want %d", sh.label, n, c.index, got, wireVals[n])
				}
			} else if got != 0 {
				t.Fatalf("%s: decode wrote a non-winner %q@%v: got %d want 0", sh.label, n, c.index, got)
			}
		}
	}
}

func ambigNames(or oracleResult) []string {
	var a []string
	for n := range or.ambiguous {
		a = append(a, n)
	}
	sort.Strings(a)
	return a
}

func firstAmbig(or oracleResult) string { return ambigNames(or)[0] }

func restrict(or oracleResult, names []string) oracleResult {
	r := oracleResult{winner: map[string][]int{}, ambiguous: map[string]bool{}, cands: map[string][]oracleCand{}}
	for _, n := range names {
		r.names = append(r.names, n)
		r.winner[n] = or.winner[n]
		r.cands[n] = or.cands[n]
	}
	return r
}

func sortedKeys(m map[string][]int) []string {
	var k []string
	for s := range m {
		k = append(k, s)
	}
	sort.Strings(k)
	return k
}

// TestGenerative_SchemaForReplicaParity pins schemaForType byte-identical to the
// real generic SchemaFor on named anchors, so the generator's schema builder
// cannot drift from the entry point under test.
func TestGenerative_SchemaForReplicaParity(t *testing.T) {
	type Inner struct {
		P int32  `avro:"p"`
		Q string `avro:"q"`
	}
	type Anchor struct {
		A int32             `avro:"a"`
		B string            `avro:"b"`
		C Inner             `avro:"c"`
		D []int64           `avro:"d"`
		E map[string]string `avro:"e"`
		F *int32            `avro:"f"`
	}
	real, err := SchemaFor[Anchor]()
	if err != nil {
		t.Fatalf("real SchemaFor: %v", err)
	}
	rep, err := schemaForType(reflect.TypeFor[Anchor]())
	if err != nil {
		t.Fatalf("replica: %v", err)
	}
	if real.String() != rep.String() {
		t.Fatalf("replica drift:\n real=%s\n repl=%s", real.String(), rep.String())
	}
}

// ---- neutering record (non-vacuity proof) ----------------------------------
//
// This net is proven to FAIL when each Family-5 fix is reverted in the
// production walkers. Measured over the 16000 generated structural shapes with a
// temporary count-don't-fatal harness (the live test fatals at the first red
// cell). With both fixes intact all four counts below are 0.
//
//	NEUTER-1  Remove `defer delete(visited, t)` from BOTH walkers
//	          (reflect.go + schema_for.go) — revert 6ce8257, restoring the
//	          marked-forever visited map:
//	            collectFields wrong-winner ......... 200 shapes (100 of them inline)
//	            collectFields accepted-ambiguous ... 304 shapes
//	            typeFieldMapping mirrors both (200 / 304)
//	          A type reached through two embed paths has its SHALLOW occurrence
//	          pruned, so the deeper field wins (caught by the FieldByName oracle
//	          on the embed shapes and by oracleResolve on the inline shapes, where
//	          FieldByName does not apply — hence the 100 inline reds); a diamond's
//	          second arm is pruned, so the collision is silently first-won instead
//	          of flagged ambiguous.
//
//	NEUTER-2  Drop the equal-depth `ambiguous[...]` mark in BOTH walkers
//	          (revert 692b039 + a1c4b25), restoring silent first-win:
//	            collectFields accepted-ambiguous ... 912 shapes
//	            typeFieldMapping accepted-ambiguous  912 names
//	          Every ambiguous shape the net asserts must reject is silently
//	          first-won instead. 912 == the net's own ambiguity-rejection count,
//	          i.e. EVERY ambiguous cell goes red.

// ---------- embed_shape_tagedge_test.go ----------

// ===========================================================================
// The tag-edge half of the generative net: malformed / edge struct tags.
//
// SchemaFor's parser (collectFields -> parseSchemaTag/splitTag, then
// inferField/inferType) is STRICT: it rejects inline-on-non-struct, inline with
// an explicit name, a decimal tag with trailing junk, a "-" skip carrying
// options, an unknown option, a default that overflows the Go field's narrow
// integer kind, a logical type on an incompatible Go type, an empty alias list.
// The runtime field-mapper (typeFieldMapping -> splitFieldTag/parseTagOptions)
// is LENIENT: it needs only the field name, inline, and omitzero, and ignores
// everything else; on an unbalanced-bracket tag splitTag rejects but
// splitFieldTag falls back to a naive split so the runtime never NEWLY errors
// on a tag a hand-written-schema user already relies on.
//
// That strict/lenient split is the tag-dimension analog of the eager/lazy
// ambiguity split, and it is SAFE only as long as it never becomes a
// both-succeed-DISAGREE: the two walkers share splitTag's tokenization and
// extract name/inline/omitzero with identical logic, so whenever SchemaFor
// builds a field the runtime must map the SAME name to the SAME Go field. This
// family proves that across the cross-product (defect x placement): for every
// shape where collectFields succeeds, typeFieldMapping agrees on every name; and
// the documented SchemaFor verdict (accept/reject) is pinned so a regression in
// the strict parser is caught. Where collectFields rejects, the runtime is
// asserted non-corrupting — it errors loudly or maps a syntactically-valid name
// to a real field, never silently picks a contradictory winner.
// ===========================================================================

type GUUID [16]byte

func ratType() reflect.Type { return reflect.TypeFor[*big.Rat]() }

// a valid struct to attach an (invalid) inline+name to.
func innerNamedStruct() reflect.Type {
	return reflect.StructOf([]reflect.StructField{
		{Name: "A", Type: reflect.TypeFor[int32](), Tag: `avro:"a"`},
	})
}

type tagDefect struct {
	label       string
	field       reflect.StructField
	schemaForOK bool     // does the full SchemaFor pipeline accept it?
	probes      []string // names a user might reference; typeFieldMapping must stay non-corrupting
}

func tagDefects() []tagDefect {
	i32 := reflect.TypeFor[int32]()
	i8 := reflect.TypeFor[int8]()
	str := reflect.TypeFor[string]()
	return []tagDefect{
		// --- rejected by the strict tag parser (collectFields errors) ---
		{"inline-on-nonstruct", reflect.StructField{Name: "F", Type: i32, Tag: `avro:",inline"`}, false, []string{"F"}},
		{"inline-with-name", reflect.StructField{Name: "F", Type: innerNamedStruct(), Tag: `avro:"foo,inline"`}, false, []string{"foo", "a"}},
		{"decimal-trailing-junk", reflect.StructField{Name: "F", Type: ratType(), Tag: `avro:"f,decimal(9,2,3)"`}, false, []string{"f"}},
		{"dash-with-options", reflect.StructField{Name: "F", Type: i32, Tag: `avro:"-,omitzero"`}, false, []string{"-", "F"}},
		{"unknown-option", reflect.StructField{Name: "F", Type: i32, Tag: `avro:"f,bogus"`}, false, []string{"f"}},
		{"empty-alias-bracket", reflect.StructField{Name: "F", Type: i32, Tag: `avro:"f,alias=[]"`}, false, []string{"f"}},
		// --- parsed fine, rejected later by inferField/inferType (collectFields succeeds) ---
		{"narrow-int-default-overflow", reflect.StructField{Name: "F", Type: i8, Tag: `avro:"f,default=9999"`}, false, []string{"f"}},
		{"uuid-on-wrong-kind", reflect.StructField{Name: "F", Type: i32, Tag: `avro:",uuid"`}, false, []string{"F"}},
		{"decimal-on-non-bigrat", reflect.StructField{Name: "F", Type: i32, Tag: `avro:"f,decimal(9,2)"`}, false, []string{"f"}},
		// --- valid controls: both walkers succeed and must agree ---
		{"valid-omitzero", reflect.StructField{Name: "F", Type: i32, Tag: `avro:"f,omitzero"`}, true, []string{"f"}},
		{"valid-alias", reflect.StructField{Name: "F", Type: i32, Tag: `avro:"f,alias=old"`}, true, []string{"f"}},
		{"valid-decimal", reflect.StructField{Name: "F", Type: ratType(), Tag: `avro:"f,decimal(9,2)"`}, true, []string{"f"}},
		{"valid-uuid-on-string", reflect.StructField{Name: "F", Type: str, Tag: `avro:"f,uuid"`}, true, []string{"f"}},
		{"valid-narrow-int-default-ok", reflect.StructField{Name: "F", Type: i8, Tag: `avro:"f,default=5"`}, true, []string{"f"}},
	}
}

type tagEdgeShape struct {
	label       string
	t           reflect.Type
	schemaForOK bool
	probes      []string
}

// genTagEdgeShapes crosses every defect with three placements: the defect field
// alone; alongside a clean sibling (the defect must not poison the clean
// field's mapping); and nested one level inside an inlined struct (the parse
// path must behave identically at depth).
func genTagEdgeShapes() []tagEdgeShape {
	keep := reflect.StructField{Name: "Keep", Type: reflect.TypeFor[int32](), Tag: `avro:"keep"`}
	var shapes []tagEdgeShape
	for _, d := range tagDefects() {
		// alone
		shapes = append(shapes, tagEdgeShape{
			label: d.label + "/alone", schemaForOK: d.schemaForOK, probes: d.probes,
			t: reflect.StructOf([]reflect.StructField{d.field}),
		})
		// with a clean sibling
		shapes = append(shapes, tagEdgeShape{
			label: d.label + "/with-keep", schemaForOK: d.schemaForOK, probes: append([]string{"keep"}, d.probes...),
			t: reflect.StructOf([]reflect.StructField{d.field, keep}),
		})
		// nested one level inside an inlined wrapper
		inner := reflect.StructOf([]reflect.StructField{d.field})
		shapes = append(shapes, tagEdgeShape{
			label: d.label + "/nested-inline", schemaForOK: d.schemaForOK, probes: d.probes,
			t: reflect.StructOf([]reflect.StructField{
				{Name: "Wrap", Type: inner, Tag: `avro:",inline"`},
			}),
		})
	}
	return shapes
}

func TestGenerative_TagEdgeWalkerAgreement(t *testing.T) {
	shapes := genTagEdgeShapes()
	var verdictPins, twoWalkerAgreements, nonCorruptionProbes, bothSucceedDisagree int

	for _, sh := range shapes {
		// (1) SchemaFor verdict pin (independent: the documented accept/reject).
		_, sfErr := schemaForType(sh.t, WithName("R"))
		if (sfErr == nil) != sh.schemaForOK {
			t.Fatalf("%s: SchemaFor verdict mismatch: got err=%v, want accept=%v", sh.label, sfErr, sh.schemaForOK)
		}
		verdictPins++

		cf, cfErr := collectFields(sh.t, make(map[reflect.Type]bool))
		if cfErr == nil {
			// (2) Two-walker agreement: every field collectFields produced must
			// map to the SAME Go field under typeFieldMapping. A both-succeed-
			// disagree here is a Family-5 divergence.
			for _, f := range cf {
				m, err := typeFieldMapping([]string{f.name}, nil, sh.t)
				if err != nil {
					bothSucceedDisagree++
					t.Fatalf("%s: collectFields produced field %q@%v but typeFieldMapping rejected it: %v",
						sh.label, f.name, f.index, err)
				}
				if !reflect.DeepEqual(m.indices[0], f.index) {
					bothSucceedDisagree++
					t.Fatalf("%s: BOTH-SUCCEED-DISAGREE on %q: collectFields=%v typeFieldMapping=%v",
						sh.label, f.name, f.index, m.indices[0])
				}
				twoWalkerAgreements++
			}
		}

		// (3) Non-corruption: probing any name on the runtime mapper must either
		// error (loud — missing/ambiguous) or return a valid, in-bounds field
		// index (FieldByIndex lands on a real field; never a panic, never a path
		// that does not exist in the type).
		for _, p := range sh.probes {
			m, err := typeFieldMapping([]string{p}, nil, sh.t)
			if err != nil {
				nonCorruptionProbes++
				continue
			}
			assertValidIndex(t, sh.label, p, sh.t, m.indices[0])
			nonCorruptionProbes++
		}
	}

	if bothSucceedDisagree != 0 {
		t.Fatalf("found %d both-succeed-disagree tag divergences", bothSucceedDisagree)
	}
	t.Logf("tag-edge net: %d shapes | %d verdict pins | %d two-walker agreements | %d non-corruption probes | 0 both-succeed-disagree",
		len(shapes), verdictPins, twoWalkerAgreements, nonCorruptionProbes)
}

// assertValidIndex confirms an index path returned by the runtime mapper points
// at a real field of t (navigating embeds/pointers), so a "successful" mapping
// can never be a fabricated or out-of-bounds path.
func assertValidIndex(t *testing.T, label, name string, typ reflect.Type, index []int) {
	t.Helper()
	cur := typ
	for _, i := range index {
		for cur.Kind() == reflect.Pointer {
			cur = cur.Elem()
		}
		if cur.Kind() != reflect.Struct || i >= cur.NumField() {
			t.Fatalf("%s: typeFieldMapping(%q) returned invalid index %v (overran %s)", label, name, index, cur)
		}
		cur = cur.Field(i).Type
	}
}

// TestGenerative_UUIDPlainDedup pins the resolved-schema corner the task names:
// the SAME [16]byte Go type used once ,uuid-tagged and once plain is two
// distinct Avro fixed types (they differ by logicalType), so SchemaFor must
// emit BOTH definitions (the name-guarded seen[t] dedup must not collapse them),
// the schema must Parse, and the runtime mapper must round-trip both fields to
// their distinct Go fields. Crossed over field order so neither "first
// occurrence defines" path is privileged.
func TestGenerative_UUIDPlainDedup(t *testing.T) {
	u16 := reflect.TypeFor[GUUID]()
	uuidField := reflect.StructField{Name: "U", Type: u16, Tag: `avro:"u,uuid"`}
	plainField := reflect.StructField{Name: "P", Type: u16, Tag: `avro:"p"`}

	for _, order := range [][]reflect.StructField{
		{uuidField, plainField},
		{plainField, uuidField},
	} {
		st := reflect.StructOf(order)
		s, err := schemaForType(st, WithName("R"))
		if err != nil {
			t.Fatalf("order %v: uuid/plain dedup must build a schema: %v", fieldNamesOf(st), err)
		}
		// Two-walker agreement on both names.
		cf, err := collectFields(st, make(map[reflect.Type]bool))
		if err != nil {
			t.Fatalf("order %v: collectFields: %v", fieldNamesOf(st), err)
		}
		for _, f := range cf {
			m, err := typeFieldMapping([]string{f.name}, nil, st)
			if err != nil || !reflect.DeepEqual(m.indices[0], f.index) {
				t.Fatalf("order %v: walker disagree on %q: cf=%v tfm=%v err=%v", fieldNamesOf(st), f.name, f.index, m, err)
			}
		}
		// Round-trip: distinct 16-byte values land in their distinct fields.
		pv := reflect.New(st)
		var a, b GUUID
		for i := range a {
			a[i] = byte(i)
			b[i] = byte(255 - i)
		}
		setUUIDField(pv.Elem(), "u", "U", a)
		setUUIDField(pv.Elem(), "p", "P", b)
		_ = cf
		data, err := s.AppendEncode(nil, pv.Interface())
		if err != nil {
			t.Fatalf("order %v: encode: %v", fieldNamesOf(st), err)
		}
		dst := reflect.New(st)
		if _, err := s.Decode(data, dst.Interface()); err != nil {
			t.Fatalf("order %v: decode: %v", fieldNamesOf(st), err)
		}
		gotU := dst.Elem().FieldByName("U").Interface().(GUUID)
		gotP := dst.Elem().FieldByName("P").Interface().(GUUID)
		if gotU != a {
			t.Fatalf("order %v: uuid field round-trip: got %v want %v", fieldNamesOf(st), gotU, a)
		}
		if gotP != b {
			t.Fatalf("order %v: plain field round-trip: got %v want %v", fieldNamesOf(st), gotP, b)
		}
	}
}

func setUUIDField(structVal reflect.Value, _ string, goName string, v GUUID) {
	structVal.FieldByName(goName).Set(reflect.ValueOf(v))
}

func fieldNamesOf(t reflect.Type) []string {
	var n []string
	for i := 0; i < t.NumField(); i++ {
		n = append(n, fmt.Sprintf("%s(%s)", t.Field(i).Name, t.Field(i).Tag))
	}
	return n
}

// ---------- embed_diamond_cost_test.go ----------

// The reflect collectors' cost is a PRODUCT, and only one of its factors was
// ever driven.
//
// Both collectors — collectFieldsRaw (schema_for.go, behind SchemaFor) and
// typeFieldMapping's collect (reflect.go, behind a record decode/encode) —
// mark the type they are descending PER PATH and unmark on the way out
// (`defer delete(visited, t)`). That is correct for embed CYCLES and
// deliberate: a type reached through two SIBLING embed paths has to be
// collected at each occurrence, so the shallower one reaches the
// shallowest-wins dedup and a type genuinely inlined twice surfaces as the
// duplicate-field collision it is. The consequence is that a Go type graph
// which is a DAG — no cycle at all — is re-descended once per PATH, and a
// diamond of embeds has 2^depth of them.
//
// That is a cost, not a bug: the carrier is a Go type, fixed at compile time,
// so nothing an attacker sends can grow it. What made it worth a permanent
// cell is that the ruling closing it rested on the two collectors being
// equivalent, and they are not. The cost is
//
//	paths-through-the-embed-DAG  x  CALLS
//
// and the second factor differs between them: typeFieldMapping's result is
// memoized per reflect.Type in a sync.Map (deser.go, ser.go), so a decode pays
// the walk once and never again; collectFieldsRaw has no memo at all, so every
// SchemaFor call re-pays it in full. Driving depth alone cannot see that, which
// is why the cell drives both.
// Sibling-embed diamond: T_k embeds A_k and B_k, both of which embed T_{k+1},
// so T1 reaches the leaf by 2^depth distinct paths while the type GRAPH is
// linear in the depth. The leaf is empty, so the type is ACCEPTED and the
// walk runs to completion rather than stopping at a duplicate-field error.
type embedDiamondLeaf struct{}

type T13 = embedDiamondLeaf
type A12 struct{ T13 }
type B12 struct{ T13 }
type T12 struct {
	A12
	B12
}
type A11 struct{ T12 }
type B11 struct{ T12 }
type T11 struct {
	A11
	B11
}
type A10 struct{ T11 }
type B10 struct{ T11 }
type T10 struct {
	A10
	B10
}
type A9 struct{ T10 }
type B9 struct{ T10 }
type T9 struct {
	A9
	B9
}
type A8 struct{ T9 }
type B8 struct{ T9 }
type T8 struct {
	A8
	B8
}
type A7 struct{ T8 }
type B7 struct{ T8 }
type T7 struct {
	A7
	B7
}
type A6 struct{ T7 }
type B6 struct{ T7 }
type T6 struct {
	A6
	B6
}
type A5 struct{ T6 }
type B5 struct{ T6 }
type T5 struct {
	A5
	B5
}
type A4 struct{ T5 }
type B4 struct{ T5 }
type T4 struct {
	A4
	B4
}
type A3 struct{ T4 }
type B3 struct{ T4 }
type T3 struct {
	A3
	B3
}
type A2 struct{ T3 }
type B2 struct{ T3 }
type T2 struct {
	A2
	B2
}
type A1 struct{ T2 }
type B1 struct{ T2 }
type T1 struct {
	A1
	B1
}

// TestInvariant_EmbedDiamondCostFactors drives BOTH factors of the reflect
// collectors' cost.
//
// What it asserts, and what it deliberately does not. It does NOT assert the
// depth factor is flat — it is not, by design, and a cell claiming otherwise
// would be asserting a property the package does not have. It asserts the two
// things that ARE invariants:
//
//   - the DECODE collector is amortized. A second decode into the same Go type
//     must cost a small fraction of the first.
//
//     TWO caches in SERIES produce that, and neither can be discriminated
//     alone: deserRecord.fast holds the compiled unsafe path per Go type and
//     is consulted first, and typeFieldMapping's own sync.Map holds the field
//     mapping behind it. Disabling either one measured 375ns and 583ns on the
//     second decode — unchanged — because the survivor still answers.
//     Disabling BOTH gives 3.9ms against a 4.3ms first decode, i.e. the walk
//     running again. So what this asserts is the COMBINATION, and the naming
//     matters: a comment crediting the mapping cache alone would be a cell
//     named for a bound it does not measure, which is how the last one in this
//     file got renamed.
//
//   - neither collector ACCUMULATES across calls. Call N must cost about what
//     call 1 did, so a per-call cost that grew with the number of calls — a
//     cache keyed on something that is not the type, a leak — reds. This is
//     the form that leaves room for the improvement rather than forbidding it:
//     adding a memo to collectFieldsRaw makes later calls cheaper, which
//     passes.
//
// The depth pair is measured and LOGGED rather than bounded, so the 2^depth
// shape is visible to a reader of the output instead of living only in a
// comment, and the absolute ceiling still catches a regression that made the
// walk worse than exponential in the depth.
func TestInvariant_EmbedDiamondCostFactors(t *testing.T) {
	depths := costFactorValues(t, "TestInvariant_EmbedDiamondCostFactors")
	if len(depths) < 2 {
		t.Fatalf("need two depths, row gives %v", depths)
	}
	// The row's two values are the DEPTHS the two concrete types carry; the
	// types cannot be indexed by a variable, so the mapping is stated here and
	// asserted rather than assumed.
	const shallowDepth, deepDepth = 8, 12
	if depths[0] != shallowDepth || depths[1] != deepDepth {
		t.Fatalf("row drives %v but the declared types carry depths %d and %d — the row and the types disagree",
			depths, shallowDepth, deepDepth)
	}

	ceiling := raceRelaxed(2 * time.Second)
	timeCall := func(fn func()) time.Duration {
		start := time.Now()
		fn()
		return time.Since(start)
	}

	// Factor 1: PATHS. T5 is depth 8, T1 is depth 12 — 16x the paths.
	shallow := timeCall(func() {
		if _, err := SchemaFor[T5](); err != nil {
			t.Errorf("SchemaFor at depth %d: %v", shallowDepth, err)
		}
	})
	deep := timeCall(func() {
		if _, err := SchemaFor[T1](); err != nil {
			t.Errorf("SchemaFor at depth %d: %v", deepDepth, err)
		}
	})
	t.Logf("SchemaFor: depth %d %v, depth %d %v (%.1fx for 16x the paths)",
		shallowDepth, shallow, deepDepth, deep, float64(deep)/float64(shallow))
	if deep > ceiling {
		t.Errorf("SchemaFor at depth %d took %v (> %v) — the per-path descent got worse than the exponential it is known to be", deepDepth, deep, ceiling)
	}

	// Factor 2: CALLS, on the SchemaFor collector. Each call re-pays the walk
	// (no memo), which is the documented cost; what must hold is that no call
	// costs MORE than the first, so nothing accumulates.
	for i := range 3 {
		d := timeCall(func() {
			if _, err := SchemaFor[T1](); err != nil {
				t.Errorf("SchemaFor call %d: %v", i, err)
			}
		})
		if d > ceiling {
			t.Errorf("SchemaFor call %d took %v (> %v) — cost is accumulating across calls", i, d, ceiling)
		}
	}

	// Factor 2 again, on the DECODE collector, where the memo makes it free.
	s, err := SchemaFor[T1]()
	if err != nil {
		t.Fatalf("SchemaFor: %v", err)
	}
	wire, err := s.Encode(T1{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out T1
	first := timeCall(func() {
		if _, err := s.Decode(wire, &out); err != nil {
			t.Errorf("first decode: %v", err)
		}
	})
	second := timeCall(func() {
		if _, err := s.Decode(wire, &out); err != nil {
			t.Errorf("second decode: %v", err)
		}
	})
	t.Logf("Decode into the depth-%d type: first %v, second %v", deepDepth, first, second)
	// The measured gap is enormous (microseconds against milliseconds at this
	// depth, and 3us against 952ms at depth 18); a tenth is a bound nothing but
	// a lost cache can cross, and it does not depend on the host.
	if second > first/10 && second > raceRelaxed(time.Millisecond) {
		t.Errorf("second decode took %v against the first's %v — the embed walk is running again.\n"+
			"Two caches in series prevent that (deserRecord.fast, then typeFieldMapping's own map); losing either one is invisible here, so this failure means both stopped answering.",
			second, first)
	}
}

// ---------- repeated_embed_test.go ----------

// TestRegression_RepeatedEmbedShallowestWins pins doc.go's documented field
// precedence ("among fields with the same tagged status, the shallowest
// wins") for the case where the SAME embedded type is
// reachable through two different embed paths at different depths. The
// field-mapper's cycle-breaking visited map was marked-forever, so the
// depth-first walk collected only the FIRST (deeper) occurrence of the
// repeated type and the shallower occurrence never reached the
// shallowest-wins dedup — encode and decode both silently selected the deep
// field, disagreeing with Go's own promotion (r.X), reflect.FieldByName, and
// encoding/json.
func TestRegression_RepeatedEmbedShallowestWins(t *testing.T) {
	type C struct {
		X int32 `avro:"X"`
	}
	type D struct{ C }
	type R struct {
		D
		C // shallower X — Go's r.X and encoding/json both select this one
	}

	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"X","type":"int"}]}`)

	// Encode must read the SHALLOW field (r.C.X), matching Go promotion.
	var r R
	r.D.C.X = 1 // deeper
	r.C.X = 2   // shallower — the one Go's r.X selects
	if got := r.X; got != 2 {
		t.Fatalf("Go promotion sanity: r.X = %d, want 2", got)
	}
	data, err := s.AppendEncode(nil, &r)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out map[string]any
	if _, err := s.Decode(data, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out["X"] != int32(2) {
		t.Fatalf("encode selected the DEEPER field: X=%v, want 2 (shallowest-wins / Go promotion)", out["X"])
	}

	// Decode must WRITE the shallow field (r.C.X), leaving the deep one zero.
	wire, err := s.AppendEncode(nil, map[string]any{"X": int32(9)})
	if err != nil {
		t.Fatalf("encode map: %v", err)
	}
	var r2 R
	if _, err := s.Decode(wire, &r2); err != nil {
		t.Fatalf("decode into struct: %v", err)
	}
	if r2.C.X != 9 || r2.D.C.X != 0 {
		t.Fatalf("decode wrote the DEEPER field: shallow=%d deep=%d, want shallow=9 deep=0", r2.C.X, r2.D.C.X)
	}
}

// TestRegression_DoubleInlineRejectsDuplicate pins a corollary of the
// per-path fix: a type inlined twice (struct{ A P ",inline"; B P ",inline" })
// is a genuine duplicate-field collision. The old mark-forever prune made
// B's fields vanish before SchemaFor's duplicate-name check, silently
// accepting the declaration and dropping B's data on encode. With per-path
// collection both copies surface, so SchemaFor rejects it as the dup it is.
func TestRegression_DoubleInlineRejectsDuplicate(t *testing.T) {
	type P struct {
		X int32 `avro:"x"`
		Y int32 `avro:"y"`
	}
	type Inl struct {
		A P `avro:",inline"`
		B P `avro:",inline"`
	}
	if _, err := SchemaFor[Inl](); err == nil {
		t.Fatal("SchemaFor accepted a type inlined twice (silent data drop); must reject as a duplicate-field collision")
	}
}

// TestRegression_EmbedCycleStillTerminates confirms the per-path fix did not
// reintroduce infinite recursion: a self-referential embed (a pointer to the
// same type) must still map cleanly — the cycle revisits a type while it is
// ON the current path, which the per-path visited set still prunes.
func TestRegression_EmbedCycleStillTerminates(t *testing.T) {
	type Node struct {
		*Node       // embedded self-pointer (cycle)
		V     int32 `avro:"v"`
	}
	s := MustParse(`{"type":"record","name":"N","fields":[{"name":"v","type":"int"}]}`)
	data, err := s.AppendEncode(nil, &Node{V: 7})
	if err != nil {
		t.Fatalf("encode cyclic-embed type: %v", err)
	}
	var n Node
	if _, err := s.Decode(data, &n); err != nil {
		t.Fatalf("decode cyclic-embed type: %v", err)
	}
	if n.V != 7 {
		t.Fatalf("cyclic-embed round-trip: V=%d, want 7", n.V)
	}
}
