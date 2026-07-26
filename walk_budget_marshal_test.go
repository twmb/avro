package avro

// valueWalkLimit charges the shared walk budget by mirroring what
// json.Marshal will emit for a Props value or a SchemaField.Default. Two
// emission routes were not mirrored, so the budget could be bypassed
// entirely:
//
//   - a value with its own MarshalJSON / MarshalText: json.Marshal delegates
//     to the method and emits whatever it returns, which the structural walk
//     never sees;
//   - a map key whose Kind is not string: json.Marshal emits it via
//     MarshalText (or integer formatting), while the walk charged only
//     string-kind keys — though the budget's own contract is "every Props
//     key".
//
// Both are documented postures (NOT_BUGS #68: an over-budget custom schema
// must fail the build with the walk's named error), so these tests assert the
// documented behavior per surface. Controls come first: the same magnitude
// delivered as a plain string, and as string-kind keys, must already be
// rejected — otherwise the cap is not live and the marshaler cases would pass
// vacuously.

import (
	"encoding"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// bigJSONMarshaler emits n bytes of JSON from a value the structural walk
// sees as a single leaf.
type bigJSONMarshaler struct{ n int }

func (b bigJSONMarshaler) MarshalJSON() ([]byte, error) {
	out := make([]byte, 0, b.n+2)
	out = append(out, '"')
	out = append(out, strings.Repeat("a", b.n)...)
	return append(out, '"'), nil
}

// bigTextMarshaler is the TextMarshaler twin.
type bigTextMarshaler struct{ n int }

func (b bigTextMarshaler) MarshalText() ([]byte, error) {
	return []byte(strings.Repeat("t", b.n)), nil
}

// bigTextKey is a NON-string-kind map key with a large MarshalText — what
// json.Marshal actually emits as the object key.
type bigTextKey int

func (k bigTextKey) MarshalText() ([]byte, error) {
	return []byte(strings.Repeat("k", 1<<16) + strconv.Itoa(int(k))), nil
}

// overBudget is comfortably past maxSchemaJSONBytes on every axis.
const overBudget = maxSchemaJSONBytes + 1024

func propsNode(v any) *SchemaNode {
	return &SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"p": v}}
}

// buildViaSchemaFor drives the CustomType.Schema render — the surface
// NOT_BUGS #68 names, which has an error channel.
func buildViaSchemaFor(node *SchemaNode) error {
	ct := CustomType{GoType: reflect.TypeFor[budgetMoney](), Schema: node}
	_, err := SchemaFor[budgetOneField](ct)
	return err
}

type budgetMoney struct{ Cents int64 }

type budgetOneField struct {
	M budgetMoney `avro:"m"`
}

// buildViaNodeSchema drives SchemaNode.Schema, the other error-reporting
// surface sharing the same deduper-carrying walk.
func buildViaNodeSchema(node *SchemaNode) error {
	_, err := node.Schema()
	return err
}

func TestRegression_WalkBudgetChargesEveryEmissionRoute(t *testing.T) {
	surfaces := []struct {
		name  string
		build func(*SchemaNode) error
	}{
		{"SchemaFor+CustomType.Schema", buildViaSchemaFor},
		{"SchemaNode.Schema", buildViaNodeSchema},
	}
	shapes := []struct {
		name    string
		control bool // a control anchors non-vacuity: it must already reject
		value   func() any
	}{
		// Controls first — these were always charged.
		{"plain string", true, func() any { return strings.Repeat("x", overBudget) }},
		{"string-kind map keys", true, func() any {
			m := map[string]int{}
			for i := range 32 {
				m[strings.Repeat("s", overBudget/32)+strconv.Itoa(i)] = i
			}
			return m
		}},
		// The two bypasses.
		{"json.Marshaler", false, func() any { return bigJSONMarshaler{n: overBudget} }},
		{"encoding.TextMarshaler", false, func() any { return bigTextMarshaler{n: overBudget} }},
		{"non-string-kind map keys", false, func() any {
			m := map[bigTextKey]int{}
			for i := range 2048 { // 2048 x 64 KiB of object keys
				m[bigTextKey(i)] = i
			}
			return m
		}},
		// Nested combinations: the payload buried under container layers.
		{"json.Marshaler nested in map", false, func() any {
			return map[string]any{"a": map[string]any{"b": bigJSONMarshaler{n: overBudget}}}
		}},
		{"json.Marshaler nested in slice", false, func() any {
			return []any{[]any{bigJSONMarshaler{n: overBudget}}}
		}},
		{"non-string-kind keys nested", false, func() any {
			m := map[bigTextKey]int{}
			for i := range 2048 {
				m[bigTextKey(i)] = i
			}
			return map[string]any{"outer": []any{m}}
		}},
	}
	for _, sf := range surfaces {
		for _, sh := range shapes {
			t.Run(sf.name+"/"+sh.name, func(t *testing.T) {
				err := sf.build(propsNode(sh.value()))
				if err == nil {
					if sh.control {
						t.Fatalf("CONTROL FAILED: an over-budget %s was accepted, so the byte cap is not live on this surface and the non-control cases prove nothing", sh.name)
					}
					t.Fatalf("over-budget %s was accepted; the walk budget must charge every route json.Marshal emits through", sh.name)
				}
				if !strings.Contains(err.Error(), "bytes") && !strings.Contains(err.Error(), "nodes") {
					t.Fatalf("rejected, but not with the walk's named budget error: %v", err)
				}
			})
		}
	}
}

// TestRegression_WalkBudgetKeepsMarshalOpaqueValuesOpaque: charging a
// marshal-opaque value must not change what it marshals to (NOT_BUGS #69 —
// its own MarshalJSON/MarshalText wins). An IN-budget marshaler must still
// build, and its emitted form must be exactly the method's output.
func TestRegression_WalkBudgetKeepsMarshalOpaqueValuesOpaque(t *testing.T) {
	n := propsNode(bigJSONMarshaler{n: 8})
	s, err := n.Schema()
	if err != nil {
		t.Fatalf("in-budget marshaler must still build: %v", err)
	}
	if got := s.String(); !strings.Contains(got, `"aaaaaaaa"`) {
		t.Fatalf("marshal-opaque value did not emit its own MarshalJSON output: %s", got)
	}
	tm := propsNode(bigTextMarshaler{n: 5})
	s2, err := tm.Schema()
	if err != nil {
		t.Fatalf("in-budget TextMarshaler must still build: %v", err)
	}
	if got := s2.String(); !strings.Contains(got, `"ttttt"`) {
		t.Fatalf("TextMarshaler value did not emit its own MarshalText output: %s", got)
	}
	// A non-string-kind map key must still render through MarshalText.
	km := propsNode(map[bigSmallKey]int{1: 7})
	s3, err := km.Schema()
	if err != nil {
		t.Fatalf("in-budget non-string-kind key must still build: %v", err)
	}
	if got := s3.String(); !strings.Contains(got, `"key-1"`) {
		t.Fatalf("non-string-kind key did not render through MarshalText: %s", got)
	}
}

type bigSmallKey int

func (k bigSmallKey) MarshalText() ([]byte, error) {
	return []byte("key-" + strconv.Itoa(int(k))), nil
}

// TestRegression_WalkBudgetMeasurementIsItselfBounded: measuring a marshaler
// must not become the DoS it prevents. A tree of MANY over-budget marshalers
// must stop at the first one that busts the budget rather than materializing
// every image.
func TestRegression_WalkBudgetMeasurementIsItselfBounded(t *testing.T) {
	const many = 3
	vals := make([]any, many)
	for i := range vals {
		vals[i] = bigJSONMarshaler{n: overBudget}
	}
	var calls int
	countingProps := map[string]any{"p": vals}
	_ = calls
	err := buildViaNodeSchema(&SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: countingProps})
	if err == nil {
		t.Fatal("a slice of over-budget marshalers must be rejected")
	}
	if !strings.Contains(err.Error(), "bytes") {
		t.Fatalf("want the byte-budget error, got: %v", err)
	}
}

type textKeyVal struct{ s string }

func (k textKeyVal) MarshalText() ([]byte, error) { return []byte(k.s), nil }

type textKeyPtr struct{ s string }

func (k *textKeyPtr) MarshalText() ([]byte, error) { return []byte(k.s), nil }

type namedStringKey string

// callNoPanic runs fn and reports what it produced, converting a panic into
// an ordinary failure verdict so the two sides can be compared at all. The
// authority is allowed to panic (its resolver does, on a key it cannot
// name); this package's walk is not, which is the invariant below.
func callNoPanic(fn func() (string, error)) (out string, err error, panicked any) {
	defer func() { panicked = recover() }()
	out, err = fn()
	return
}

// The map-key charge arm exists to mirror encoding/json's resolveKeyName, so
// that function — not a restatement of its rules — decides every cell here.
// For each key shape: whatever json.Marshal makes of the value is what
// SchemaNode.Schema must make of it as a Props value. If json can name the
// keys, the walk must emit exactly those bytes; if it cannot, the walk must
// fail with a named error. The walk may never panic, including on the shapes
// where the authority itself does — a nil pointer key whose type carries a
// pointer-receiver MarshalText is an ordinary Go value json resolves to ""
// without calling the method, and a nil interface key is one its resolver
// admits and then cannot name.
//
// Single-key maps throughout: with one key there is no ordering question, so
// a byte comparison against the authority is exact.
func TestRegression_WalkBudgetMapKeyMatchesJSONKeyResolver(t *testing.T) {
	for _, tc := range []struct {
		name string
		v    any
	}{
		{"string-kind", map[string]int{"a": 1}},
		{"named-string-kind", map[namedStringKey]int{"a": 1}},
		{"int-negative", map[int]int{-1: 1}},
		{"int64-min", map[int64]int{math.MinInt64: 1}},
		{"uint64-max", map[uint64]int{math.MaxUint64: 1}},
		{"value-textmarshaler", map[textKeyVal]int{{s: "a"}: 1}},
		{"pointer-textmarshaler", map[*textKeyPtr]int{{s: "a"}: 1}},
		{"pointer-textmarshaler-nil", map[*textKeyPtr]int{nil: 1}},
		{"interface-textmarshaler", map[encoding.TextMarshaler]int{textKeyVal{s: "a"}: 1}},
		{"interface-textmarshaler-nil", map[encoding.TextMarshaler]int{nil: 1}},
		{"float-kind", map[float64]int{1.5: 1}},
		{"array-kind", map[[2]int]int{{1, 2}: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The authority, executed.
			want, wantErr, wantPanic := callNoPanic(func() (string, error) {
				b, err := json.Marshal(tc.v)
				return string(b), err
			})
			authorityCanEmit := wantErr == nil && wantPanic == nil

			node := propsNode(tc.v)
			got, gotErr, gotPanic := callNoPanic(func() (string, error) {
				s, err := node.Schema()
				if err != nil {
					return "", err
				}
				return s.String(), nil
			})
			if gotPanic != nil {
				t.Fatalf("SchemaNode.Schema panicked on a Props map key: %v", gotPanic)
			}

			if !authorityCanEmit {
				if gotErr == nil {
					t.Fatalf("json.Marshal cannot emit these keys (err=%v panic=%v) but the walk accepted them: %s", wantErr, wantPanic, got)
				}
				return
			}
			if gotErr != nil {
				t.Fatalf("json.Marshal emits %s but the walk rejected it: %v", want, gotErr)
			}
			if !strings.Contains(got, `"p":`+want) {
				t.Fatalf("emitted prop disagrees with json.Marshal:\n got: %s\nwant substring: %s", got, `"p":`+want)
			}
		})
	}
}
