package avro

import (
	"reflect"
	"testing"
)

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

// TestRegression_EmbedSelectionMatchesGoPromotion is the GENERATIVE net for
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
func TestRegression_EmbedSelectionMatchesGoPromotion(t *testing.T) {
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
		type EmbA struct{ Name string }                     // depth 2, untagged
		type EmbB struct{ Name string }                     // depth 2, untagged
		type EmbTagged struct{ Other string `avro:"Name"` } // depth 2, tagged "Name"
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
