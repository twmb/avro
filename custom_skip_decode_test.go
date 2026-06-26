package avro_test

import (
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// Test types for the skip-custom parity matrix. Package-level so reflect.New
// sees named (not anonymous) types, matching real caller targets.
type csInner struct {
	X int32 `avro:"x"`
}
type csStruct struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
}
type csNest struct {
	In csInner `avro:"in"`
	N  int64   `avro:"n"`
}
type csMoney int64
type csPtr struct {
	P *int32 `avro:"p"`
}
type csDec struct {
	D *big.Rat `avro:"d"`
}

// Named scalar types: the canonical Avro value (int32/string/bool/float32/
// float64/[]byte) is NOT assignable to a named type, so a bare value placement
// cannot satisfy these targets — the all-skip fall-through must re-decode the wire
// into them through the base deserializer, exactly as a no-custom decode does. A
// regression mishandling a named target reds the matching row.
type (
	csI32   int32
	csStr   string
	csBool  bool
	csF32   float32
	csF64   float64
	csBytes []byte
)

// namedI32 is a NAMED int32 so that a *namedI32 union target is NOT trivially
// assignable from the canonical int32 — the all-skip fall-through must re-decode
// the union through the base deserializer, which reads the exact wire branch (a
// plain *int32 is the easy case; the named type forces the full per-branch
// decode).
type namedI32 int32
type csUPtr struct {
	P *namedI32 `avro:"p"`
}

// TestRegression_CustomSkipDecodeMatchesNoCustom pins the contract that a custom
// decoder returning ErrSkipCustomType (no decoder matched) falls through to
// built-in decode: the value lands in the typed target EXACTLY as a no-custom
// decode would, on binary, JSON, and resolved paths. A wildcard custom (empty
// criteria) wraps every node and skips, so the all-skip fall-through re-decodes
// every kind through the base deserializer; a wildcard does not suppress logicals
// (suppressLogical=false), so its decode must equal the plain no-custom decode
// value-for-value. Each row decodes the SAME wire with the plain schema and the
// skip-custom schema into the SAME typed target and asserts equality.
//
// This is the net for the fall-through: a faithfulness regression (a zeroed
// record/array/map, a dropped enum-ordinal, a mis-sized fixed) reds a row here.
// Non-vacuity is proven by neutering the re-decode (placing a probe value instead)
// and confirming rows go red.
func TestRegression_CustomSkipDecodeMatchesNoCustom(t *testing.T) {
	skip := avro.CustomType{Decode: func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType }}
	ptr := func(x int32) *int32 { return &x }
	pn := func(x namedI32) *namedI32 { return &x }

	rows := []struct {
		name   string
		schema string
		value  any // typed value; its type is also the decode target
	}{
		{"record-struct", `{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}`, csStruct{A: 1, B: "x"}},
		{"nested-struct", `{"type":"record","name":"R","fields":[{"name":"in","type":{"type":"record","name":"In","fields":[{"name":"x","type":"int"}]}},{"name":"n","type":"long"}]}`, csNest{In: csInner{X: 7}, N: 9}},
		{"record-map", `{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]}`, map[string]int64{"a": 1, "b": 2}},
		{"array-int", `{"type":"array","items":"long"}`, []int64{1, 2, 3}},
		{"array-struct", `{"type":"array","items":{"type":"record","name":"In","fields":[{"name":"x","type":"int"}]}}`, []csInner{{X: 1}, {X: 2}}},
		{"array-fixedlen", `{"type":"array","items":"int"}`, [3]int32{4, 5, 6}},
		{"map-int", `{"type":"map","values":"long"}`, map[string]int64{"k": 5}},
		{"map-struct", `{"type":"map","values":{"type":"record","name":"In","fields":[{"name":"x","type":"int"}]}}`, map[string]csInner{"k": {X: 3}}},
		{"named-long", `"long"`, csMoney(42)},
		{"named-int", `"int"`, csI32(7)},
		{"named-string", `"string"`, csStr("hi")},
		{"named-bool", `"boolean"`, csBool(true)},
		{"named-float", `"float"`, csF32(1.5)},
		{"named-double", `"double"`, csF64(2.5)},
		{"named-bytes", `"bytes"`, csBytes{1, 2, 3}},
		{"ptr-field-set", `{"type":"record","name":"R","fields":[{"name":"p","type":["null","int"]}]}`, csPtr{P: ptr(8)}},
		{"ptr-field-nil", `{"type":"record","name":"R","fields":[{"name":"p","type":["null","int"]}]}`, csPtr{P: nil}},
		// Union into a NAMED-pointer target: int32 is not assignable to namedI32,
		// so setCustomResult's pointer-walk cannot resolve it and the union
		// branches arm runs (recovers the lost branch index). A plain *int32
		// (ptr-field-set above) is resolved before the loop, leaving it un-netted.
		{"union-named-ptr-set", `{"type":"record","name":"R","fields":[{"name":"p","type":["null","int"]}]}`, csUPtr{P: pn(8)}},
		{"union-named-ptr-nil", `{"type":"record","name":"R","fields":[{"name":"p","type":["null","int"]}]}`, csUPtr{P: nil}},
		{"union-named-slice", `{"type":"array","items":["null","int"]}`, []*namedI32{pn(5), nil}},
		{"enum-string", `{"type":"enum","name":"E","symbols":["A","B","C"]}`, "B"},
		{"enum-named-int", `{"type":"enum","name":"E","symbols":["A","B","C"]}`, csMoney(2)},
		{"fixed-array", `{"type":"fixed","name":"F","size":4}`, [4]byte{1, 2, 3, 4}},
		{"bytes", `"bytes"`, []byte{9, 8, 7}},
		{"bool", `"boolean"`, true},
		{"float", `"float"`, float32(1.5)},
		{"double", `"double"`, float64(2.5)},
		{"string", `"string"`, "hi"},
		{"timestamp", `{"type":"long","logicalType":"timestamp-millis"}`, time.UnixMilli(1600000000000).UTC()},
		{"decimal-in-record", `{"type":"record","name":"R","fields":[{"name":"d","type":{"type":"bytes","logicalType":"decimal","precision":5,"scale":2}}]}`, csDec{D: big.NewRat(33, 100)}},
		{"uuid-string", `{"type":"string","logicalType":"uuid"}`, "12345678-1234-1234-1234-123456789abc"},
	}

	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			plain := avro.MustParse(r.schema)
			sskip := avro.MustParse(r.schema, skip)
			tt := reflect.TypeOf(r.value)

			wireBin, err := plain.Encode(r.value)
			if err != nil {
				t.Fatalf("encode binary: %v", err)
			}
			wireJSON, err := plain.EncodeJSON(r.value)
			if err != nil {
				t.Fatalf("encode json: %v", err)
			}

			dec := func(s *avro.Schema, wire []byte, jsonForm bool, opts ...avro.Opt) (any, error) {
				p := reflect.New(tt)
				if jsonForm {
					return p.Elem().Interface(), s.DecodeJSON(wire, p.Interface(), opts...)
				}
				_, err := s.Decode(wire, p.Interface(), opts...)
				return p.Elem().Interface(), err
			}

			resolved, rerr := avro.Resolve(plain, sskip)
			if rerr != nil {
				t.Fatalf("resolve: %v", rerr)
			}

			// Run the matrix untagged AND with TaggedUnions. The all-skip
			// fall-through re-decodes the wire into the target, so a union field
			// lands identically to a no-custom decode under either option — the
			// re-decode reads the exact wire branch, so neither a typed target nor
			// a tagged envelope can be misplaced.
			check := func(opt string, opts ...avro.Opt) {
				// Oracle: plain no-custom decode (binary + JSON).
				binPlain, e := dec(plain, wireBin, false, opts...)
				if e != nil {
					t.Fatalf("[%s] plain binary decode: %v", opt, e)
				}
				jsonPlain, e := dec(plain, wireJSON, true, opts...)
				if e != nil {
					t.Fatalf("[%s] plain json decode: %v", opt, e)
				}
				// Skip-custom and resolved (skip-custom reader) must equal it.
				binSkip, e := dec(sskip, wireBin, false, opts...)
				if e != nil {
					t.Fatalf("[%s] skip-custom binary decode errored where no-custom succeeded: %v", opt, e)
				}
				if !matEqual(binPlain, binSkip) {
					t.Errorf("[%s] binary skip-custom != no-custom:\n no-custom=%#v\n skip     =%#v", opt, binPlain, binSkip)
				}
				jsonSkip, e := dec(sskip, wireJSON, true, opts...)
				if e != nil {
					t.Fatalf("[%s] skip-custom json decode errored where no-custom succeeded: %v", opt, e)
				}
				if !matEqual(jsonPlain, jsonSkip) {
					t.Errorf("[%s] json skip-custom != no-custom:\n no-custom=%#v\n skip     =%#v", opt, jsonPlain, jsonSkip)
				}
				binRes, e := dec(resolved, wireBin, false, opts...)
				if e != nil {
					t.Fatalf("[%s] resolved binary decode errored: %v", opt, e)
				}
				if !matEqual(binPlain, binRes) {
					t.Errorf("[%s] resolved binary skip-custom != no-custom:\n no-custom=%#v\n resolved =%#v", opt, binPlain, binRes)
				}
				jsonRes, e := dec(resolved, wireJSON, true, opts...)
				if e != nil {
					t.Fatalf("[%s] resolved json decode errored: %v", opt, e)
				}
				if !matEqual(jsonPlain, jsonRes) {
					t.Errorf("[%s] resolved json skip-custom != no-custom:\n no-custom=%#v\n resolved =%#v", opt, jsonPlain, jsonRes)
				}
			}
			check("untagged")
			check("tagged", avro.TaggedUnions())
		})
	}
}

type csTransformed struct{ Cents int64 }

// TestRegression_CustomSkipDecodeMatchedTransformSurvives nets the deep-match
// re-decode path — the main net's wildcard custom purely skips, so a
// deeper-matched custom's transform is never carried through. A wildcard custom
// TRANSFORMS one node (a long tagged "domain":"money" -> a domain Go type) and
// SKIPS the rest. The record itself is skipped, but a nested custom matched in its
// subtree, so the all-skip fall-through re-decodes the record with customs ACTIVE
// (not bypassed) — reproducing the money field's transform in the typed field
// while the skipped sibling decodes normally (== no-custom). A bypass here, or a
// placement that dropped the match, would land int64 where csTransformed is
// expected and fail.
func TestRegression_CustomSkipDecodeMatchedTransformSurvives(t *testing.T) {
	ct := avro.CustomType{
		Decode: func(v any, sn *avro.SchemaNode) (any, error) {
			if sn.Props["domain"] == "money" {
				return csTransformed{Cents: v.(int64)}, nil
			}
			return nil, avro.ErrSkipCustomType
		},
	}
	schema := `{"type":"record","name":"R","fields":[{"name":"amt","type":{"type":"long","domain":"money"}},{"name":"name","type":"string"}]}`
	plain := avro.MustParse(schema)
	s := avro.MustParse(schema, ct)

	// Encode the raw wire via a plain (int64) shape.
	type Rw struct {
		Amt  int64  `avro:"amt"`
		Name string `avro:"name"`
	}
	wireBin, err := plain.Encode(Rw{Amt: 500, Name: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	wireJSON, err := plain.EncodeJSON(Rw{Amt: 500, Name: "alice"})
	if err != nil {
		t.Fatal(err)
	}

	type R struct {
		Amt  csTransformed `avro:"amt"`
		Name string        `avro:"name"`
	}
	want := R{Amt: csTransformed{Cents: 500}, Name: "alice"}

	var gb R
	if _, err := s.Decode(wireBin, &gb); err != nil {
		t.Fatalf("binary decode: %v", err)
	}
	if gb != want {
		t.Errorf("binary: matched transform did not survive (or skipped sibling wrong):\n got=%+v\n want=%+v", gb, want)
	}
	var gj R
	if err := s.DecodeJSON(wireJSON, &gj); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	if gj != want {
		t.Errorf("json: matched transform did not survive (or skipped sibling wrong):\n got=%+v\n want=%+v", gj, want)
	}
}

// TestRegression_CustomSkipDecodeReusesTarget pins that a wildcard all-skip custom
// decode REUSES a pre-populated decode target identically to a no-custom decode.
// A non-nil typed map and an interface already wrapping a map[string]any retain
// keys absent from the wire — deserMap's `mapVal = v` reuse and deserRecord's
// reuseOrMakeStringAnyMap stale-key contract. The all-skip fall-through RE-DECODES
// the wire into the same target through the base deserializer, so reuse is
// inherited for free rather than re-implemented.
//
// This axis is invisible to TestRegression_CustomSkipDecodeMatchesNoCustom, which
// decodes only into fresh reflect.New targets. The oracle is a no-custom decode
// into the SAME pre-populated target. The map[string]any subtest is the cell an
// earlier assignable-fast-path placement swallowed (it replaced the whole map,
// dropping stale keys); an Avro map decoded into `any` is the control: it does NOT
// reuse (deserMap's iface arm allocates fresh), so the all-skip path matches that
// too. Non-vacuity is verified by neutering the typed-target re-decode (placing the
// probe value instead), which reds the map[string]any and record-into-any cells.
func TestRegression_CustomSkipDecodeReusesTarget(t *testing.T) {
	skip := avro.CustomType{Decode: func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType }}

	t.Run("typed-map", func(t *testing.T) {
		schema := `{"type":"map","values":"long"}`
		plain := avro.MustParse(schema)
		sskip := avro.MustParse(schema, skip)
		bin, err := plain.Encode(map[string]int64{"k": 5})
		if err != nil {
			t.Fatal(err)
		}
		jsonw, err := plain.EncodeJSON(map[string]int64{"k": 5})
		if err != nil {
			t.Fatal(err)
		}
		nb := map[string]int64{"stale": 99}
		if _, err := plain.Decode(bin, &nb); err != nil {
			t.Fatal(err)
		}
		sb := map[string]int64{"stale": 99}
		if _, err := sskip.Decode(bin, &sb); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(nb, sb) {
			t.Errorf("binary typed-map: skip-custom=%v != no-custom=%v (stale key must be retained)", sb, nb)
		}
		nj := map[string]int64{"stale": 99}
		if err := plain.DecodeJSON(jsonw, &nj); err != nil {
			t.Fatal(err)
		}
		sj := map[string]int64{"stale": 99}
		if err := sskip.DecodeJSON(jsonw, &sj); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(nj, sj) {
			t.Errorf("json typed-map: skip-custom=%v != no-custom=%v", sj, nj)
		}
	})

	t.Run("typed-map-any", func(t *testing.T) {
		// map[string]any (Kind Map, any-valued): the value type an assignable
		// fast-path placement swallowed — it replaced the whole map, dropping
		// stale keys the base decoder retains. Re-decode reuses it like any other
		// non-nil typed map, on binary AND JSON.
		schema := `{"type":"map","values":"long"}`
		plain := avro.MustParse(schema)
		sskip := avro.MustParse(schema, skip)
		bin, err := plain.Encode(map[string]int64{"k": 5})
		if err != nil {
			t.Fatal(err)
		}
		jsonw, err := plain.EncodeJSON(map[string]int64{"k": 5})
		if err != nil {
			t.Fatal(err)
		}
		nb := map[string]any{"stale": int64(99)}
		if _, err := plain.Decode(bin, &nb); err != nil {
			t.Fatal(err)
		}
		sb := map[string]any{"stale": int64(99)}
		if _, err := sskip.Decode(bin, &sb); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(nb, sb) {
			t.Errorf("binary map[string]any: skip-custom=%v != no-custom=%v (stale key must be retained)", sb, nb)
		}
		nj := map[string]any{"stale": int64(99)}
		if err := plain.DecodeJSON(jsonw, &nj); err != nil {
			t.Fatal(err)
		}
		sj := map[string]any{"stale": int64(99)}
		if err := sskip.DecodeJSON(jsonw, &sj); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(nj, sj) {
			t.Errorf("json map[string]any: skip-custom=%v != no-custom=%v", sj, nj)
		}
	})

	t.Run("record-into-any", func(t *testing.T) {
		schema := `{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`
		plain := avro.MustParse(schema)
		sskip := avro.MustParse(schema, skip)
		type Rw struct {
			A int64 `avro:"a"`
		}
		bin, err := plain.Encode(Rw{A: 5})
		if err != nil {
			t.Fatal(err)
		}
		var nb any = map[string]any{"stale": int64(99)}
		if _, err := plain.Decode(bin, &nb); err != nil {
			t.Fatal(err)
		}
		var sb any = map[string]any{"stale": int64(99)}
		if _, err := sskip.Decode(bin, &sb); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(nb, sb) {
			t.Errorf("binary record-into-any: skip-custom=%v != no-custom=%v (stale key must be retained)", sb, nb)
		}
	})

	t.Run("map-into-any-control", func(t *testing.T) {
		// Avro map into `any` must MATCH no-custom — and no-custom does NOT reuse
		// (deserMap's iface arm allocates fresh), so the stale key is dropped on
		// both. Guards against an over-eager reuse fix that retains the stale key
		// where the base decoder would not.
		schema := `{"type":"map","values":"long"}`
		plain := avro.MustParse(schema)
		sskip := avro.MustParse(schema, skip)
		bin, err := plain.Encode(map[string]int64{"k": 5})
		if err != nil {
			t.Fatal(err)
		}
		var nb any = map[string]any{"stale": int64(99)}
		if _, err := plain.Decode(bin, &nb); err != nil {
			t.Fatal(err)
		}
		var sb any = map[string]any{"stale": int64(99)}
		if _, err := sskip.Decode(bin, &sb); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(nb, sb) {
			t.Errorf("binary map-into-any: skip-custom=%v != no-custom=%v", sb, nb)
		}
	})
}

// TestRegression_CustomSkipDecodeLogicalIntoBaseTypedTarget pins that a WILDCARD
// all-skip custom — which does NOT suppress logicals — decoding a logical node
// into a base TYPED target lands the value identically to a no-custom decode. The
// base (logical) deserializer fills the typed target natively (deserDate→int32 raw
// days, deserDuration→[12]byte, decimal→raw []byte); the all-skip fall-through
// RE-DECODES the wire through it. A box-into-any placement could not: the probe
// holds the ENRICHED type (time.Time / avro.Duration / *big.Rat), which no
// base-kind setter accepts, so it ERRORED where no-custom succeeds.
//
// Held constant by TestRegression_CustomSkipDecodeMatchesNoCustom (decode target =
// the encode value's own type, so a timestamp decodes only into time.Time): this
// crosses logical schema × base typed target, the foreclosed cell, on binary AND
// JSON. Non-vacuity: neutering the typed re-decode to place the probe value reds
// every row (the enriched probe value cannot fill the base target).
func TestRegression_CustomSkipDecodeLogicalIntoBaseTypedTarget(t *testing.T) {
	skip := avro.CustomType{Decode: func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType }}
	rows := []struct {
		name   string
		schema string
		enc    any
		mk     func() any
	}{
		{"date->int32", `{"type":"int","logicalType":"date"}`, int32(19000), func() any { return new(int32) }},
		{"timestamp-millis->int64", `{"type":"long","logicalType":"timestamp-millis"}`, int64(1600000000000), func() any { return new(int64) }},
		{"time-micros->int64", `{"type":"long","logicalType":"time-micros"}`, int64(3600000000), func() any { return new(int64) }},
		{"duration->array12", `{"type":"fixed","name":"D","size":12,"logicalType":"duration"}`, avro.Duration{Months: 1, Days: 2, Milliseconds: 3}, func() any { return new([12]byte) }},
		{"decimal->bytes", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, big.NewRat(1234, 100), func() any { return new([]byte) }},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			plain := avro.MustParse(r.schema)
			sskip := avro.MustParse(r.schema, skip)
			bin, err := plain.Encode(r.enc)
			if err != nil {
				t.Fatal(err)
			}
			jsonw, err := plain.EncodeJSON(r.enc)
			if err != nil {
				t.Fatal(err)
			}
			no := r.mk()
			if _, err := plain.Decode(bin, no); err != nil {
				t.Fatalf("no-custom binary: %v", err)
			}
			sk := r.mk()
			if _, err := sskip.Decode(bin, sk); err != nil {
				t.Fatalf("skip-custom binary errored where no-custom succeeded: %v", err)
			}
			if !reflect.DeepEqual(no, sk) {
				t.Errorf("binary: skip-custom=%v != no-custom=%v", sk, no)
			}
			noj := r.mk()
			if err := plain.DecodeJSON(jsonw, noj); err != nil {
				t.Fatalf("no-custom json: %v", err)
			}
			skj := r.mk()
			if err := sskip.DecodeJSON(jsonw, skj); err != nil {
				t.Fatalf("skip-custom json errored where no-custom succeeded: %v", err)
			}
			if !reflect.DeepEqual(noj, skj) {
				t.Errorf("json: skip-custom=%v != no-custom=%v", skj, noj)
			}
		})
	}
}

// TestRegression_CustomSkipDecodeTaggedUnionIntoAny pins that a wildcard all-skip
// custom decode into an interface target under TaggedUnions reproduces the
// {branch: value} envelope a no-custom decode emits (deserUnion.maybeWrap /
// wrapUnion). A fresh interface target is decoded straight through the base
// deserializer with the caller's TaggedUnions option in force, so the envelope is
// produced natively — no re-tag step. The main skip matrix decodes only into typed
// targets, which maybeWrap never tags, so this axis was unnetted.
//
// Non-vacuous: the oracle is a no-custom TaggedUnions decode into the same `any`
// target; a regression that decoded the interface untagged (or boxed an untagged
// value) reds every cell. The rows are distinct-Go-type / single-non-null unions.
func TestRegression_CustomSkipDecodeTaggedUnionIntoAny(t *testing.T) {
	skip := avro.CustomType{Decode: func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType }}
	p := func(x int32) *int32 { return &x }

	rows := []struct {
		name   string
		schema string
		value  any
	}{
		{"null-first", `{"type":"record","name":"R","fields":[{"name":"u","type":["null","int"]}]}`, struct {
			U *int32 `avro:"u"`
		}{U: p(7)}},
		{"null-second", `{"type":"record","name":"R","fields":[{"name":"u","type":["int","null"]}]}`, struct {
			U *int32 `avro:"u"`
		}{U: p(7)}},
		{"multibranch-distinct", `{"type":"record","name":"R","fields":[{"name":"u","type":["int","string"]}]}`, struct {
			U string `avro:"u"`
		}{U: "hi"}},
		{"array-of-nullunion", `{"type":"array","items":["null","int"]}`, []*int32{p(7), nil}},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			plain := avro.MustParse(r.schema)
			sskip := avro.MustParse(r.schema, skip)
			bin, err := plain.Encode(r.value)
			if err != nil {
				t.Fatal(err)
			}
			jsonw, err := plain.EncodeJSON(r.value)
			if err != nil {
				t.Fatal(err)
			}
			var nb any
			if _, err := plain.Decode(bin, &nb, avro.TaggedUnions()); err != nil {
				t.Fatal(err)
			}
			var sb any
			if _, err := sskip.Decode(bin, &sb, avro.TaggedUnions()); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(nb, sb) {
				t.Errorf("binary: skip-custom=%#v != no-custom=%#v", sb, nb)
			}
			var nj any
			if err := plain.DecodeJSON(jsonw, &nj, avro.TaggedUnions()); err != nil {
				t.Fatal(err)
			}
			var sj any
			if err := sskip.DecodeJSON(jsonw, &sj, avro.TaggedUnions()); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(nj, sj) {
				t.Errorf("json: skip-custom=%#v != no-custom=%#v", sj, nj)
			}
		})
	}
}

// TestRegression_CustomSkipDecodeChainInputUntagged pins the custom decoder
// chain's input contract: the chain receives the probe value decoded with the
// caller's options in force — here, with no TaggedUnions, the RAW untagged value a
// no-custom decode into `any` produces. The custom records its input and the test
// compares it to that untagged oracle.
//
// Non-vacuous: a regression that fed the chain a tagged {branch: value} envelope
// (or any transformed shape) reds this — the recorded input would not equal the
// untagged oracle.
func TestRegression_CustomSkipDecodeChainInputUntagged(t *testing.T) {
	var captured any
	rec := avro.CustomType{
		AvroType: "record",
		Decode: func(v any, sn *avro.SchemaNode) (any, error) {
			captured = v
			return nil, avro.ErrSkipCustomType
		},
	}
	schema := `{"type":"record","name":"R","fields":[` +
		`{"name":"u","type":["null","int"]},` +
		`{"name":"s","type":"string"},` +
		`{"name":"arr","type":{"type":"array","items":["null","long"]}}]}`
	plain := avro.MustParse(schema)
	s := avro.MustParse(schema, rec)

	type Rw struct {
		U   *int32   `avro:"u"`
		S   string   `avro:"s"`
		Arr []*int64 `avro:"arr"`
	}
	x32, x64 := int32(7), int64(9)
	in := Rw{U: &x32, S: "hi", Arr: []*int64{&x64, nil}}
	bin, err := plain.Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	jsonw, err := plain.EncodeJSON(in)
	if err != nil {
		t.Fatal(err)
	}

	// Oracle: an UNtagged no-custom decode into any — exactly what the chain sees.
	var oracle any
	if _, err := plain.Decode(bin, &oracle); err != nil {
		t.Fatal(err)
	}

	captured = nil
	var sinkB any
	if _, err := s.Decode(bin, &sinkB); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(captured, oracle) {
		t.Errorf("binary: custom chain saw\n %#v\n want untagged\n %#v", captured, oracle)
	}

	captured = nil
	var sinkJ any
	if err := s.DecodeJSON(jsonw, &sinkJ); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(captured, oracle) {
		t.Errorf("json: custom chain saw\n %#v\n want untagged\n %#v", captured, oracle)
	}
}

// TestRegression_CustomSkipDecodeOverlappingUnion pins that the all-skip path
// recovers the EXACT wire branch of an OVERLAPPING same-symbol union by
// RE-DECODING the wire — the branch index comes from the wire itself, not a guess.
// An enum-union → int-ordinal target gets the wire branch's ordinal, and an
// overlapping enum-union → tagged-any gets the wire branch's name.
//
// Non-vacuous: a fall-through that placed a probe value instead of re-decoding
// reds both cells — the ordinal arm cannot derive the right ordinal from an
// untagged probe (first-match guesses wrong), and the any arm mis-tags.
func TestRegression_CustomSkipDecodeOverlappingUnion(t *testing.T) {
	skip := avro.CustomType{Decode: func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType }}

	t.Run("ordinal-target", func(t *testing.T) {
		// EnumA "X"@0, EnumB "X"@1; the wire selects EnumB.
		schema := `{"type":"array","items":[` +
			`{"type":"enum","name":"EnumA","symbols":["X","Y"]},` +
			`{"type":"enum","name":"EnumB","symbols":["P","X"]}]}`
		plain := avro.MustParse(schema)
		sskip := avro.MustParse(schema, skip)

		// binary: array[1] = union branch 1 (EnumB) + enum idx 1 ("X"), end block.
		bin := []byte{0x02, 0x02, 0x02, 0x00}
		var nb, sb []int32
		if _, err := plain.Decode(bin, &nb); err != nil {
			t.Fatal(err)
		}
		if _, err := sskip.Decode(bin, &sb); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(nb, sb) || len(nb) != 1 || nb[0] != 1 {
			t.Errorf("binary ordinal: skip=%v no-custom=%v (want [1] = EnumB ordinal of X)", sb, nb)
		}

		// json: tagged-union spec form selecting EnumB.
		jsonw := []byte(`[{"EnumB":"X"}]`)
		var nj, sj []int32
		if err := plain.DecodeJSON(jsonw, &nj); err != nil {
			t.Fatal(err)
		}
		if err := sskip.DecodeJSON(jsonw, &sj); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(nj, sj) || len(nj) != 1 || nj[0] != 1 {
			t.Errorf("json ordinal: skip=%v no-custom=%v (want [1])", sj, nj)
		}
	})

	t.Run("tagged-any-target", func(t *testing.T) {
		schema := `{"type":"record","name":"R","fields":[{"name":"e","type":[` +
			`{"type":"enum","name":"EA","symbols":["X","Y"]},` +
			`{"type":"enum","name":"EB","symbols":["P","X"]}]}]}`
		plain := avro.MustParse(schema)
		sskip := avro.MustParse(schema, skip)

		// binary: record -> union branch 1 (EB) -> enum idx 1 ("X").
		bin := []byte{0x02, 0x02}
		var nb, sb any
		if _, err := plain.Decode(bin, &nb, avro.TaggedUnions()); err != nil {
			t.Fatal(err)
		}
		if _, err := sskip.Decode(bin, &sb, avro.TaggedUnions()); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(nb, sb) {
			t.Errorf("binary tagged-any: skip=%#v no-custom=%#v", sb, nb)
		}

		jsonw := []byte(`{"e":{"EB":"X"}}`)
		var nj, sj any
		if err := plain.DecodeJSON(jsonw, &nj, avro.TaggedUnions()); err != nil {
			t.Fatal(err)
		}
		if err := sskip.DecodeJSON(jsonw, &sj, avro.TaggedUnions()); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(nj, sj) {
			t.Errorf("json tagged-any: skip=%#v no-custom=%#v", sj, nj)
		}
	})
}

// TestRegression_CustomSkipDecodeLogicalSuppression crosses the logical-suppression
// axis through the all-skip path. A LogicalType-matching custom SUPPRESSES the
// built-in logical (hasMatchingCustomType), so a skip falls through to the RAW
// Avro-native value — identical to a no-callback (Decode==nil) LogicalType custom,
// which suppresses the same way. A WILDCARD custom does NOT suppress, so a skip
// preserves the enriched value. The all-skip placement must honor each.
func TestRegression_CustomSkipDecodeLogicalSuppression(t *testing.T) {
	schema := `{"type":"long","logicalType":"timestamp-millis"}`
	matchSkip := avro.CustomType{LogicalType: "timestamp-millis", Decode: func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType }}
	matchRaw := avro.CustomType{LogicalType: "timestamp-millis"} // Decode==nil → suppress → raw
	wildSkip := avro.CustomType{Decode: func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType }}

	wire, err := avro.MustParse(schema).Encode(time.UnixMilli(1600000000000).UTC())
	if err != nil {
		t.Fatal(err)
	}

	dec := func(opts ...avro.SchemaOpt) any {
		var got any
		if _, err := avro.MustParse(schema, opts...).Decode(wire, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return got
	}

	matchSkipVal := dec(matchSkip)
	matchRawVal := dec(matchRaw)
	wildSkipVal := dec(wildSkip)
	noCustomVal := func() any {
		var got any
		avro.MustParse(schema).Decode(wire, &got)
		return got
	}()

	// LogicalType-matching skip == no-callback (both suppress → raw int64).
	if !reflect.DeepEqual(matchSkipVal, matchRawVal) {
		t.Errorf("matching skip %T(%v) != no-callback %T(%v)", matchSkipVal, matchSkipVal, matchRawVal, matchRawVal)
	}
	if _, ok := matchSkipVal.(int64); !ok {
		t.Errorf("suppressed logical: want raw int64, got %T", matchSkipVal)
	}
	// Wildcard skip preserves the logical == no-custom (enriched time.Time).
	if !reflect.DeepEqual(wildSkipVal, noCustomVal) {
		t.Errorf("wildcard skip %T(%v) != no-custom %T(%v)", wildSkipVal, wildSkipVal, noCustomVal, noCustomVal)
	}
	if _, ok := wildSkipVal.(time.Time); !ok {
		t.Errorf("non-suppressing wildcard: want enriched time.Time, got %T", wildSkipVal)
	}
}
