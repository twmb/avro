package avro

// User-supplied callback contract matrix: every point where the encoders
// and decoders do arithmetic, slicing, or a state transition on a value
// returned by USER code — text-out methods (MarshalText / AppendText)
// beyond the plain-string positions text_appender_contract_test.go pins,
// TextUnmarshaler error returns, and CustomType Encode/Decode returns.
// The invariant pinned per cell: a contract-violating return NEVER
// panics through a public API and NEVER silently corrupts sibling data —
// detectable violations yield named errors (*SemanticError with the
// user's error preserved in the chain), and the fall-through / zeroing
// shapes leave every sibling value intact.
//
// The lax-name validator (func(string) error), IsZero() bool, and the
// wire-side use of map keys are structurally immune: the first two
// return no value the library computes with, and map keys are read and
// written as raw strings on every path — pinned by
// TestRegression_MapKeysBypassTextMethods below.

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// symbolTexter's MarshalText names an enum symbol (or violates the
// contract, per mode). The enum encoders look the returned text up in
// the symbol table, so every wrong-content shape is detectable there.
type symbolTexter struct{ mode string }

func (e symbolTexter) MarshalText() ([]byte, error) {
	switch e.mode {
	case "valid":
		return []byte("B"), nil
	case "unknown":
		return []byte("NOPE"), nil
	case "nil-nil":
		return nil, nil
	case "error":
		return nil, errors.New("texter boom")
	}
	panic("bad mode " + e.mode)
}

// uuidTexter's MarshalText yields UUID text (or violations). On a
// fixed(16)+uuid schema the 16 wire bytes are DERIVED from the returned
// text (parseUUID), so wrong content is detectable and must reject; on a
// string+uuid schema the encoder is string-lenient (serUUID delegates
// non-[16]byte sources to the string encoder), so arbitrary text encodes
// verbatim — those cells assert byte-parity with the plain-string twin
// rather than rejection.
type uuidTexter struct{ mode string }

func (u uuidTexter) MarshalText() ([]byte, error) {
	switch u.mode {
	case "valid":
		return []byte("12345678-1234-1234-1234-123456789abc"), nil
	case "garbage":
		return []byte("not-a-uuid"), nil
	case "nil-nil":
		return nil, nil
	case "error":
		return nil, errors.New("uuid boom")
	}
	panic("bad mode " + u.mode)
}

func TestMatrix_TextOutCallbackReturnShapes(t *testing.T) {
	encode := func(s *Schema, wire string, v any) ([]byte, error) {
		if wire == "binary" {
			return s.Encode(v)
		}
		return s.EncodeJSON(v)
	}

	t.Run("enum", func(t *testing.T) {
		s := MustParse(`{"type":"enum","name":"E","symbols":["A","B"]}`)
		for _, wire := range []string{"binary", "json"} {
			for _, mode := range []string{"valid", "unknown", "nil-nil", "error"} {
				t.Run(mode+"/"+wire, func(t *testing.T) {
					out, err := encode(s, wire, symbolTexter{mode})
					if mode == "valid" {
						if err != nil {
							t.Fatalf("valid symbol via MarshalText rejected: %v", err)
						}
						twin, _ := encode(s, wire, "B")
						if !bytes.Equal(out, twin) {
							t.Errorf("text-out enum diverged from plain-string twin: % x vs % x", out, twin)
						}
						return
					}
					// unknown and nil-nil (empty text) miss the symbol table;
					// an error return is surfaced — all as *SemanticError.
					if err == nil {
						t.Fatalf("%s silently encoded: % x", mode, out)
					}
					var se *SemanticError
					if !errors.As(err, &se) {
						t.Errorf("not a SemanticError: %v", err)
					}
					if mode == "error" && !strings.Contains(err.Error(), "texter boom") {
						t.Errorf("user error identity lost: %v", err)
					}
				})
			}
		}
	})

	// Every INPUT ARM of the enum encoders — plain string, named string
	// without text methods, text-out (covered above), int ordinal — must
	// produce the same *SemanticError{AvroType: "enum"} identity on both
	// wires for a value naming no symbol / an out-of-range ordinal. The
	// cells run at TOP LEVEL deliberately: record positions wrap any
	// error in a SemanticError via the field-path wrapper, which would
	// mask a plain-error arm; top level has no wrapper to hide behind.
	t.Run("enum-arm-identity", func(t *testing.T) {
		s := MustParse(`{"type":"enum","name":"E","symbols":["A","B"]}`)
		type bare string // named string kind, no text methods
		arms := []struct {
			name string
			val  any
		}{
			{"plain-string", "NOPE"},
			{"named-string", bare("NOPE")},
			{"int-ordinal", int64(99)},
		}
		for _, arm := range arms {
			for _, wire := range []string{"binary", "json"} {
				t.Run(arm.name+"/"+wire, func(t *testing.T) {
					var err error
					if wire == "binary" {
						_, err = s.Encode(arm.val)
					} else {
						_, err = s.EncodeJSON(arm.val)
					}
					if err == nil {
						t.Fatal("non-symbol value accepted")
					}
					var se *SemanticError
					if !errors.As(err, &se) {
						t.Fatalf("no SemanticError identity: %v", err)
					}
					if se.AvroType != "enum" {
						t.Errorf("AvroType = %q, want enum: %v", se.AvroType, err)
					}
				})
			}
		}
	})

	// Sibling user-value failures with the same two-wire contract: a
	// wrong-length source for fixed, and a source map missing a
	// defaultless record field. Binary rejects both as *SemanticError
	// (serSize's shape check; the record loop's "missing key"
	// construction); JSON encode must carry the identical identity.
	t.Run("fixed-size-mismatch-identity", func(t *testing.T) {
		s := MustParse(`{"type":"fixed","name":"F","size":4}`)
		for _, wire := range []string{"binary", "json"} {
			t.Run(wire, func(t *testing.T) {
				var err error
				if wire == "binary" {
					_, err = s.Encode([]byte("toolongvalue"))
				} else {
					_, err = s.EncodeJSON([]byte("toolongvalue"))
				}
				if err == nil {
					t.Fatal("wrong-length fixed source accepted")
				}
				var se *SemanticError
				if !errors.As(err, &se) {
					t.Fatalf("no SemanticError identity: %v", err)
				}
				if se.AvroType != "fixed" {
					t.Errorf("AvroType = %q, want fixed: %v", se.AvroType, err)
				}
			})
		}
	})
	t.Run("missing-required-field-identity", func(t *testing.T) {
		s := MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`)
		for _, wire := range []string{"binary", "json"} {
			t.Run(wire, func(t *testing.T) {
				var err error
				if wire == "binary" {
					_, err = s.Encode(map[string]any{})
				} else {
					_, err = s.EncodeJSON(map[string]any{})
				}
				if err == nil {
					t.Fatal("missing defaultless field accepted")
				}
				var se *SemanticError
				if !errors.As(err, &se) {
					t.Fatalf("no SemanticError identity: %v", err)
				}
				if se.Field != "a" {
					t.Errorf("Field = %q, want a: %v", se.Field, err)
				}
			})
		}
	})

	// The remaining encode user-value failures already AGREE across the
	// two wires — nil-for-non-nullable is plain on both (its own family),
	// union no-match and numeric-content rejects are SemanticError on
	// both. Pin the agreement (identity equality, not any specific shape)
	// so neither wire drifts alone.
	t.Run("cross-wire-identity-agreement", func(t *testing.T) {
		identityOf := func(err error) string {
			if err == nil {
				return "nil"
			}
			var se *SemanticError
			if errors.As(err, &se) {
				return "semantic:" + se.AvroType
			}
			return "plain"
		}
		rows := []struct {
			name   string
			schema string
			val    any
		}{
			{"nil-non-nullable", `"long"`, (*int64)(nil)},
			{"union-no-match", `["null","long"]`, "zz"},
			{"float-bad-string", `"float"`, "abc"},
		}
		for _, row := range rows {
			t.Run(row.name, func(t *testing.T) {
				s := MustParse(row.schema)
				_, berr := s.Encode(row.val)
				_, jerr := s.EncodeJSON(row.val)
				if berr == nil || jerr == nil {
					t.Fatalf("both wires must reject: bin=%v json=%v", berr, jerr)
				}
				if identityOf(berr) != identityOf(jerr) {
					t.Errorf("error identity diverged: binary=%s (%v) vs json=%s (%v)",
						identityOf(berr), berr, identityOf(jerr), jerr)
				}
			})
		}
	})

	// The encode-side unknown-symbol reject is a USER-VALUE failure and
	// carries *SemanticError identity on both wires (asserted above). The
	// decode-side counterparts — a binary ordinal outside the symbol
	// table, a JSON string naming no symbol — are WIRE-CONTENT failures,
	// plain errors on both wires like the union-index and map-key-length
	// rejects. Pin the two families' boundary so neither side drifts.
	t.Run("enum-decode-content-errors-stay-plain", func(t *testing.T) {
		s := MustParse(`{"type":"enum","name":"E","symbols":["A","B"]}`)
		var got string
		_, err := s.Decode([]byte{0x08}, &got) // ordinal 4: out of range
		if err == nil {
			t.Fatal("out-of-range ordinal accepted")
		}
		var se *SemanticError
		if errors.As(err, &se) {
			t.Errorf("binary wire-content error gained SemanticError identity: %v", err)
		}
		if err := s.DecodeJSON([]byte(`"NOPE"`), &got); err == nil {
			t.Fatal("unknown wire symbol accepted")
		} else if errors.As(err, &se) {
			t.Errorf("JSON wire-content error gained SemanticError identity: %v", err)
		}
	})

	t.Run("fixed-uuid", func(t *testing.T) {
		s := MustParse(`{"type":"fixed","name":"F","size":16,"logicalType":"uuid"}`)
		for _, wire := range []string{"binary", "json"} {
			for _, mode := range []string{"valid", "garbage", "nil-nil", "error"} {
				t.Run(mode+"/"+wire, func(t *testing.T) {
					out, err := encode(s, wire, uuidTexter{mode})
					if mode == "valid" {
						if err != nil {
							t.Fatalf("valid uuid text rejected: %v", err)
						}
						twin, _ := encode(s, wire, "12345678-1234-1234-1234-123456789abc")
						if !bytes.Equal(out, twin) {
							t.Errorf("diverged from plain-string twin: % x vs % x", out, twin)
						}
						return
					}
					if err == nil {
						t.Fatalf("%s silently encoded: % x", mode, out)
					}
					if mode == "error" && !strings.Contains(err.Error(), "uuid boom") {
						t.Errorf("user error identity lost: %v", err)
					}
				})
			}
		}
	})

	t.Run("string-uuid-lenient", func(t *testing.T) {
		s := MustParse(`{"type":"string","logicalType":"uuid"}`)
		twinFor := map[string]string{"garbage": "not-a-uuid", "nil-nil": ""}
		for _, wire := range []string{"binary", "json"} {
			for mode, twinStr := range twinFor {
				t.Run(mode+"/"+wire, func(t *testing.T) {
					out, err := encode(s, wire, uuidTexter{mode})
					if err != nil {
						t.Fatalf("string+uuid is string-lenient for non-[16]byte sources: %v", err)
					}
					twin, _ := encode(s, wire, twinStr)
					if !bytes.Equal(out, twin) {
						t.Errorf("text-out diverged from plain-string twin: % x vs % x", out, twin)
					}
				})
			}
			t.Run("error/"+wire, func(t *testing.T) {
				if _, err := encode(s, wire, uuidTexter{"error"}); err == nil ||
					!strings.Contains(err.Error(), "uuid boom") {
					t.Fatalf("user error not surfaced with identity: %v", err)
				}
			})
		}
	})

	// A MarshalText-only type (no AppendText) on a plain string schema:
	// the returned bytes are materialized and copied, so nil text with a
	// nil error is simply the empty string; an error is surfaced.
	t.Run("string-marshaler-only", func(t *testing.T) {
		s := MustParse(`"string"`)
		for _, wire := range []string{"binary", "json"} {
			out, err := encode(s, wire, symbolTexter{"nil-nil"})
			if err != nil {
				t.Fatalf("nil text + nil error must encode the empty string: %v", err)
			}
			twin, _ := encode(s, wire, "")
			if !bytes.Equal(out, twin) {
				t.Errorf("nil-text encode diverged from empty-string twin: % x vs % x", out, twin)
			}
			if _, err := encode(s, wire, symbolTexter{"error"}); err == nil ||
				!strings.Contains(err.Error(), "texter boom") {
				t.Fatalf("user error not surfaced with identity: %v", err)
			}
		}
	})
}

// failingUnmarshaler errors on any text not prefixed "ok". Its error
// must surface — wrapped so the user's identity is preserved — from
// every text-shaped decode position on both wire formats.
type failingUnmarshaler struct{ S string }

func (f *failingUnmarshaler) UnmarshalText(b []byte) error {
	if strings.HasPrefix(string(b), "ok") {
		f.S = string(b)
		return nil
	}
	return errors.New("unmarshal boom")
}

func TestMatrix_TextUnmarshalerReturnShapes(t *testing.T) {
	strS := MustParse(`"string"`)
	recS := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"string"},{"name":"b","type":"string"},{"name":"c","type":"string"}]}`)
	arrS := MustParse(`{"type":"array","items":"string"}`)
	mapS := MustParse(`{"type":"map","values":"string"}`)
	enumS := MustParse(`{"type":"enum","name":"E","symbols":["A","B"]}`)
	uuidS := MustParse(`{"type":"string","logicalType":"uuid"}`)

	type recTarget struct {
		A string             `avro:"a"`
		B failingUnmarshaler `avro:"b"`
		C string             `avro:"c"`
	}

	t.Run("error-surfaces", func(t *testing.T) {
		cases := []struct {
			name   string
			decode func() error
		}{
			{"top/binary", func() error {
				wire, _ := strS.Encode("bad")
				var f failingUnmarshaler
				_, err := strS.Decode(wire, &f)
				return err
			}},
			{"top/json", func() error {
				var f failingUnmarshaler
				return strS.DecodeJSON([]byte(`"bad"`), &f)
			}},
			{"record-mid/binary", func() error {
				wire, _ := recS.Encode(map[string]any{"a": "okA", "b": "bad", "c": "okC"})
				var tg recTarget
				_, err := recS.Decode(wire, &tg)
				return err
			}},
			{"record-mid/json", func() error {
				var tg recTarget
				return recS.DecodeJSON([]byte(`{"a":"okA","b":"bad","c":"okC"}`), &tg)
			}},
			{"array-item/binary", func() error {
				wire, _ := arrS.Encode([]string{"bad"})
				var tg []failingUnmarshaler
				_, err := arrS.Decode(wire, &tg)
				return err
			}},
			{"map-value/binary", func() error {
				wire, _ := mapS.Encode(map[string]string{"k": "bad"})
				var tg map[string]failingUnmarshaler
				_, err := mapS.Decode(wire, &tg)
				return err
			}},
			{"enum/binary", func() error {
				wire, _ := enumS.Encode("B")
				var f failingUnmarshaler
				_, err := enumS.Decode(wire, &f)
				return err
			}},
			{"uuid-string/binary", func() error {
				wire, _ := uuidS.Encode("12345678-1234-1234-1234-123456789abc")
				var f failingUnmarshaler
				_, err := uuidS.Decode(wire, &f)
				return err
			}},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				err := c.decode()
				if err == nil {
					t.Fatal("UnmarshalText error swallowed")
				}
				if !strings.Contains(err.Error(), "unmarshal boom") {
					t.Errorf("user error identity lost: %v", err)
				}
			})
		}
	})

	// Success control: the method fires and the value lands.
	t.Run("control", func(t *testing.T) {
		wire, _ := strS.Encode("okYes")
		var f failingUnmarshaler
		if _, err := strS.Decode(wire, &f); err != nil || f.S != "okYes" {
			t.Fatalf("control: %v %q", err, f.S)
		}
	})
}

// contractLong is the GoType for the CustomType return-shape matrices.
type contractLong int64

func customEncodeReturning(shape string) CustomType {
	return CustomType{
		AvroType: "long",
		GoType:   reflect.TypeFor[contractLong](),
		Encode: func(v any, sn *SchemaNode) (any, error) {
			switch shape {
			case "ok":
				return int64(v.(contractLong)) * 10, nil
			case "untyped-nil":
				return nil, nil
			case "typed-nil":
				return (*int64)(nil), nil
			case "wrong-type":
				return "zz", nil
			case "err-with-value":
				return int64(42), errors.New("enc boom")
			case "skip-wrapped":
				return nil, fmt.Errorf("wrapped: %w", ErrSkipCustomType)
			}
			panic("bad shape " + shape)
		},
	}
}

// TestMatrix_CustomTypeEncodeReturnShapes crosses CustomType.Encode
// return shapes with encode positions on both wire formats. The
// contract: an untyped nil return is the named "returned nil" reject; a
// typed-nil or wrong-typed return re-enters the underlying serializer,
// whose type validation names it; a non-nil error is fatal with the
// value discarded and the user's identity preserved; an
// ErrSkipCustomType return (wrapped counts — the chain matches with
// errors.Is) falls through to the built-in encode of the original
// value. No shape panics, and sibling values already encoded are
// unaffected (an error aborts the whole Encode; success shapes place
// only their own node).
func TestMatrix_CustomTypeEncodeReturnShapes(t *testing.T) {
	positions := []struct {
		name   string
		schema string
		val    func() any
		twin   func() any // plain value with contractLong(5)'s wire stand-in int64(5)
	}{
		{"top", `"long"`,
			func() any { return contractLong(5) },
			func() any { return int64(5) }},
		{"record-mid", `{"type":"record","name":"R","fields":[
			{"name":"a","type":"string"},{"name":"b","type":"long"},{"name":"c","type":"string"}]}`,
			func() any { return map[string]any{"a": "AA", "b": contractLong(5), "c": "CC"} },
			func() any { return map[string]any{"a": "AA", "b": int64(5), "c": "CC"} }},
		{"array-item", `{"type":"array","items":"long"}`,
			func() any { return []any{contractLong(5)} },
			func() any { return []any{int64(5)} }},
		{"map-value", `{"type":"map","values":"long"}`,
			func() any { return map[string]any{"k": contractLong(5)} },
			func() any { return map[string]any{"k": int64(5)} }},
		{"union-branch", `["null","long"]`,
			func() any { return contractLong(5) },
			func() any { return int64(5) }},
	}
	shapes := []string{"ok", "untyped-nil", "typed-nil", "wrong-type", "err-with-value", "skip-wrapped"}

	for _, pos := range positions {
		plain := MustParse(pos.schema)
		for _, shape := range shapes {
			s, err := Parse(pos.schema, customEncodeReturning(shape))
			if err != nil {
				t.Fatal(err)
			}
			for _, wire := range []string{"binary", "json"} {
				t.Run(pos.name+"/"+shape+"/"+wire, func(t *testing.T) {
					encode := func(sc *Schema, v any) ([]byte, error) {
						if wire == "binary" {
							return sc.Encode(v)
						}
						return sc.EncodeJSON(v)
					}
					out, err := encode(s, pos.val())
					switch shape {
					case "ok":
						if err != nil {
							t.Fatalf("transforming encode rejected: %v", err)
						}
						twin, _ := encode(plain, func() any {
							switch pos.name {
							case "record-mid":
								return map[string]any{"a": "AA", "b": int64(50), "c": "CC"}
							case "array-item":
								return []any{int64(50)}
							case "map-value":
								return map[string]any{"k": int64(50)}
							default:
								return int64(50)
							}
						}())
						if !bytes.Equal(out, twin) {
							t.Errorf("transformed encode != plain x10 twin: % x vs % x", out, twin)
						}
					case "skip-wrapped":
						if err != nil {
							t.Fatalf("wrapped ErrSkipCustomType not honored: %v", err)
						}
						twin, _ := encode(plain, pos.twin())
						if !bytes.Equal(out, twin) {
							t.Errorf("fall-through encode != plain twin: % x vs % x", out, twin)
						}
					case "untyped-nil":
						if err == nil {
							t.Fatalf("untyped-nil return silently encoded: % x", out)
						}
						if !strings.Contains(err.Error(), "custom type encoder returned nil") {
							t.Errorf("want the named returned-nil reject, got: %v", err)
						}
					case "err-with-value":
						if err == nil {
							t.Fatalf("error swallowed, value encoded: % x", out)
						}
						if !strings.Contains(err.Error(), "enc boom") {
							t.Errorf("user error identity lost: %v", err)
						}
					case "typed-nil", "wrong-type":
						// The returned value re-enters the underlying
						// serializer; its type validation names the shape
						// (nil for a non-nullable long / string vs long).
						if err == nil {
							t.Fatalf("%s return silently encoded: % x", shape, out)
						}
					}
				})
			}
		}
	}
}

func customDecodeReturning(shape string) CustomType {
	return CustomType{
		AvroType: "long",
		Decode: func(v any, sn *SchemaNode) (any, error) {
			switch shape {
			case "ok":
				return contractLong(v.(int64) * 10), nil
			case "nil-nil":
				return nil, nil
			case "wrong-type":
				return "zz", nil
			case "err-with-value":
				return contractLong(9), errors.New("dec boom")
			case "skip-wrapped":
				return nil, fmt.Errorf("wrapped: %w", ErrSkipCustomType)
			}
			panic("bad shape " + shape)
		},
	}
}

// TestMatrix_CustomTypeDecodeReturnShapes crosses CustomType.Decode
// return shapes with a typed record target on both wire formats. The
// contract: a nil result zeroes the target field; a result whose type
// is not assignable to the target is the named *SemanticError (never a
// reflect.Set panic); a non-nil error is fatal with the value discarded
// and the user's identity preserved; a wrapped ErrSkipCustomType falls
// through to the value a no-custom decode produces. Whenever Decode
// returns nil error, sibling fields hold their decoded values — a
// violating callback can never corrupt data beside its own node.
func TestMatrix_CustomTypeDecodeReturnShapes(t *testing.T) {
	const recSchema = `{"type":"record","name":"R","fields":[
		{"name":"a","type":"string"},{"name":"b","type":"long"},{"name":"c","type":"string"}]}`
	plain := MustParse(recSchema)
	wire, err := plain.Encode(map[string]any{"a": "AA", "b": int64(5), "c": "CC"})
	if err != nil {
		t.Fatal(err)
	}
	jsonWire := []byte(`{"a":"AA","b":5,"c":"CC"}`)

	type target struct {
		A string       `avro:"a"`
		B contractLong `avro:"b"`
		C string       `avro:"c"`
	}

	for _, shape := range []string{"ok", "nil-nil", "wrong-type", "err-with-value", "skip-wrapped"} {
		s, err := Parse(recSchema, customDecodeReturning(shape))
		if err != nil {
			t.Fatal(err)
		}
		for _, wk := range []string{"binary", "json"} {
			t.Run(shape+"/"+wk, func(t *testing.T) {
				var tg target
				var derr error
				if wk == "binary" {
					_, derr = s.Decode(wire, &tg)
				} else {
					derr = s.DecodeJSON(jsonWire, &tg)
				}
				siblingsIntact := func() {
					if tg.A != "AA" || tg.C != "CC" {
						t.Errorf("sibling fields corrupted: %+v", tg)
					}
				}
				switch shape {
				case "ok":
					if derr != nil || tg.B != 50 {
						t.Fatalf("transforming decode: err=%v B=%d", derr, tg.B)
					}
					siblingsIntact()
				case "nil-nil":
					if derr != nil {
						t.Fatalf("nil result must zero, not error: %v", derr)
					}
					if tg.B != 0 {
						t.Errorf("nil result must zero the field: %d", tg.B)
					}
					siblingsIntact()
				case "wrong-type":
					if derr == nil {
						t.Fatalf("unassignable result silently placed: %+v", tg)
					}
					var se *SemanticError
					if !errors.As(derr, &se) {
						t.Errorf("not a SemanticError: %v", derr)
					}
				case "err-with-value":
					if derr == nil {
						t.Fatalf("error swallowed: %+v", tg)
					}
					if !strings.Contains(derr.Error(), "dec boom") {
						t.Errorf("user error identity lost: %v", derr)
					}
				case "skip-wrapped":
					if derr != nil {
						t.Fatalf("wrapped ErrSkipCustomType not honored: %v", derr)
					}
					if tg.B != 5 {
						t.Errorf("fall-through must match no-custom decode: %d", tg.B)
					}
					siblingsIntact()
				}
			})
		}
	}

	// An interface target accepts any result type — the callback's value
	// is the user's own choice there, placed verbatim.
	t.Run("wrong-type/any-target", func(t *testing.T) {
		s, _ := Parse(`"long"`, customDecodeReturning("wrong-type"))
		w2, _ := MustParse(`"long"`).Encode(int64(5))
		var v any
		if _, err := s.Decode(w2, &v); err != nil {
			t.Fatalf("interface target must accept any result: %v", err)
		}
		if v != "zz" {
			t.Errorf("callback result not placed verbatim: %v", v)
		}
	})
}

// rawKey carries transforming text methods that the map-key paths must
// NOT consult: Avro map keys are already string-kind, and all four
// paths (binary/JSON x encode/decode) read and write them as raw
// strings. If any single path started consulting text methods, the
// transform here would break the raw-key agreement across paths.
type rawKey string

func (k rawKey) MarshalText() ([]byte, error)  { return []byte(strings.ToUpper(string(k))), nil }
func (k *rawKey) UnmarshalText(b []byte) error { *k = rawKey(strings.ToLower(string(b))); return nil }

func TestRegression_MapKeysBypassTextMethods(t *testing.T) {
	s := MustParse(`{"type":"map","values":"long"}`)
	in := map[rawKey]int64{"Key": 7}

	bin, err := s.Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(bin, []byte("Key")) {
		t.Errorf("binary map key not the raw string: % x", bin)
	}
	var back map[rawKey]int64
	if _, err := s.Decode(bin, &back); err != nil {
		t.Fatal(err)
	}
	if _, ok := back["Key"]; !ok {
		t.Errorf("binary decode transformed the key: %v", back)
	}

	jout, err := s.EncodeJSON(in)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(jout, []byte(`"Key"`)) {
		t.Errorf("JSON map key not the raw string: %s", jout)
	}
	var jback map[rawKey]int64
	if err := s.DecodeJSON(jout, &jback); err != nil {
		t.Fatal(err)
	}
	if _, ok := jback["Key"]; !ok {
		t.Errorf("JSON decode transformed the key: %v", jback)
	}
}

// TestRegression_SchemaNodePropsUnmarshalableValueNamedError pins that a
// hand-built SchemaNode whose Props holds a value the schema rebuild
// cannot marshal (a channel) surfaces as a named error from
// SchemaNode.Schema, not a panic.
func TestRegression_SchemaNodePropsUnmarshalableValueNamedError(t *testing.T) {
	sn := &SchemaNode{Type: "record", Name: "R",
		Fields: []SchemaField{{Name: "f", Type: SchemaNode{Type: "int"}}},
		Props:  map[string]any{"x": make(chan int)},
	}
	if _, err := sn.Schema(); err == nil {
		t.Fatal("unmarshalable Props value silently accepted")
	}
}
