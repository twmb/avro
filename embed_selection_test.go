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

// TestRegression_EmbedEqualDepthAmbiguity documents the case the Go-promotion
// oracle CANNOT adjudicate: a type reachable at EQUAL depth through two embeds.
// Go makes v.X a compile error (ambiguous selector); reflect.FieldByName
// returns the zero Value; encoding/json drops the field. doc.go's
// "shallowest wins" is silent on equal-depth ties, so twmb's first-wins is a
// deliberate policy, pinned so a change to it is conscious.
func TestRegression_EmbedEqualDepthAmbiguity(t *testing.T) {
	type C struct {
		X int32 `avro:"X"`
	}
	type A struct{ C }
	type B struct{ C }
	type R struct {
		A
		B
	}
	if _, ok := reflect.TypeFor[R]().FieldByName("X"); ok {
		t.Fatal("precondition: expected Go to treat R.X as ambiguous")
	}
	var r R
	r.A.C.X = 1
	r.B.C.X = 2
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"X","type":"int"}]}`)
	data, err := s.AppendEncode(nil, &r)
	if err != nil {
		t.Fatalf("equal-depth encode unexpectedly errored: %v", err)
	}
	var out map[string]any
	if _, err := s.Decode(data, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out["X"] != int32(1) {
		t.Fatalf("equal-depth first-wins policy changed: X=%v, want 1 (A.C.X). If intentional, update this pin.", out["X"])
	}
}
