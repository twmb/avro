package avro

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

// freshShortAppender violates the encoding.TextAppender contract by
// returning a fresh slice instead of appending to its input — the
// realistic implementation bug `return []byte(s), nil`. Its returned
// slice is SHORTER than the accumulated output the encoder handed it.
type freshShortAppender struct{}

func (freshShortAppender) AppendText(b []byte) ([]byte, error) { return []byte("x"), nil }

// TestRegression_AppendTextShortReturnNamedError pins that a
// contract-violating TextAppender whose returned slice is SHORTER than
// its input surfaces as a named *SemanticError from binary encode —
// never a slice-bounds panic. appendAvroString's inline-write backfill
// derives the text length from the returned slice; without the length
// guard the arithmetic indexes dst[mark:...] past the end of the fresh
// short slice and panics the calling goroutine. The record's first
// field makes the accumulated output longer than the fresh return, the
// shape that drives the arithmetic out of bounds.
func TestRegression_AppendTextShortReturnNamedError(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Encode panicked, want named error: %v", r)
		}
	}()
	s := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"string"},{"name":"b","type":"string"}]}`)
	_, err := s.Encode(map[string]any{"a": "0123456789", "b": freshShortAppender{}})
	if err == nil {
		t.Fatal("want error for AppendText returning a shorter slice, got nil")
	}
	var se *SemanticError
	if !errors.As(err, &se) {
		t.Fatalf("want *SemanticError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "AppendText returned a slice shorter than its input") {
		t.Fatalf("error does not name the AppendText contract violation: %v", err)
	}
}

// contractAppender's mode selects the return shape of AppendText,
// covering the legal contract forms and the violating ones.
type contractAppender struct{ mode string }

func (c contractAppender) AppendText(b []byte) ([]byte, error) {
	switch c.mode {
	case "legal-append":
		return append(b, "hello"...), nil
	case "legal-zero-append":
		return b, nil
	case "fresh-short":
		return []byte("x"), nil
	case "fresh-long":
		return []byte(strings.Repeat("z", 64)), nil
	case "fresh-equal-len":
		return bytes.Repeat([]byte{'q'}, len(b)), nil
	case "error-return":
		return nil, errors.New("appender boom")
	}
	panic("unknown mode " + c.mode)
}

type appenderStruct struct {
	A string           `avro:"a"`
	B contractAppender `avro:"b"`
}

// TestMatrix_AppendTextReturnShapes crosses every AppendText return
// shape with every encode position that reaches appendAvroString's
// inline-write backfill. The contract:
//
//   - No return shape may panic, anywhere.
//   - A legal append (including a zero-length append) produces wire
//     bytes byte-identical to encoding the equivalent plain string —
//     the happy path is untouched by the shrunk-return guard.
//   - A fresh return SHORTER than the input is the detectable
//     violation (the backfill length arithmetic would go negative):
//     it yields the named *SemanticError at every position.
//   - A fresh return >= the input length passes the length guard and
//     is NOT detectable without comparing prefix bytes on every encode
//     (a per-string memcmp of everything encoded so far — a cost the
//     encoder deliberately does not pay for the caller's own contract
//     violation; encoding/json/v2's jsontext.AppendRaw trusts the
//     append contract the same way). Documenting: those shapes return
//     err == nil with the accumulated output replaced by the fresh
//     slice's content and the length header backfilled at the
//     placeholder offset; the exact observed bytes are pinned below so
//     any future change to this posture is a deliberate one.
//   - An error return surfaces the appender's error.
func TestMatrix_AppendTextReturnShapes(t *testing.T) {
	recordSchema := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"string"},{"name":"b","type":"string"}]}`
	positions := []struct {
		name   string
		schema string
		val    func(v contractAppender) any
		twin   func(tw string) any // same shape with a plain string standing in for the appender
	}{
		{"top-level", `"string"`,
			func(v contractAppender) any { return v },
			func(tw string) any { return tw }},
		{"record-second-field", recordSchema,
			func(v contractAppender) any { return map[string]any{"a": "0123456789", "b": v} },
			func(tw string) any { return map[string]any{"a": "0123456789", "b": tw} }},
		// A struct field routes through the reflect record path (the
		// unsafe string fast paths exclude text-method types), reaching
		// the same backfill; must behave identically to the map form.
		{"record-struct-field", recordSchema,
			func(v contractAppender) any { return appenderStruct{A: "0123456789", B: v} },
			func(tw string) any { return map[string]any{"a": "0123456789", "b": tw} }},
		{"array-element", `{"type":"array","items":"string"}`,
			func(v contractAppender) any { return []any{"0123456789", v} },
			func(tw string) any { return []any{"0123456789", tw} }},
		{"map-value", `{"type":"map","values":"string"}`,
			func(v contractAppender) any { return map[string]any{"k": v} },
			func(tw string) any { return map[string]any{"k": tw} }},
		{"union-branch", `["null","string"]`,
			func(v contractAppender) any { return v },
			func(tw string) any { return tw }},
	}

	// Observed outputs for the undetectable fresh-return shapes
	// (Documenting, see the test doc): the fresh slice's bytes with the
	// real length header backfilled at the placeholder offset, plus any
	// container terminator appended after the corrupted element.
	rep := strings.Repeat
	goldens := map[string]string{
		// Position-dependent detectability (Documenting): at top level
		// the 1-byte fresh-short return has the same length as its input
		// (the placeholder is the only accumulated byte), so the
		// shorter-than-input violation is not length-detectable there —
		// the cell lands in the documented undetectable class and
		// encodes the empty string (the backfilled header is the entire
		// output). At every other position the accumulated output
		// exceeds the fresh return and the guard names the violation.
		"top-level/fresh-short":               "00",
		"top-level/fresh-long":                "7e" + rep("7a", 63),
		"top-level/fresh-equal-len":           "00",
		"record-second-field/fresh-long":      rep("7a", 11) + "68" + rep("7a", 52),
		"record-second-field/fresh-equal-len": rep("71", 11) + "00",
		"record-struct-field/fresh-long":      rep("7a", 11) + "68" + rep("7a", 52),
		"record-struct-field/fresh-equal-len": rep("71", 11) + "00",
		"array-element/fresh-long":            rep("7a", 12) + "66" + rep("7a", 51) + "00",
		"array-element/fresh-equal-len":       rep("71", 12) + "0000",
		"map-value/fresh-long":                rep("7a", 3) + "78" + rep("7a", 60) + "00",
		"map-value/fresh-equal-len":           rep("71", 3) + "0000",
		"union-branch/fresh-long":             "7a7c" + rep("7a", 62),
		"union-branch/fresh-equal-len":        "7100",
	}

	shapes := []struct {
		mode  string
		class string // legal | guard | silent | error
		twin  string // legal only: the plain-string equivalent
	}{
		{"legal-append", "legal", "hello"},
		{"legal-zero-append", "legal", ""},
		{"fresh-short", "guard", ""},
		{"fresh-long", "silent", ""},
		{"fresh-equal-len", "silent", ""},
		{"error-return", "error", ""},
	}

	for _, pos := range positions {
		s := MustParse(pos.schema)
		for _, sh := range shapes {
			t.Run(pos.name+"/"+sh.mode, func(t *testing.T) {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("Encode panicked: %v", r)
					}
				}()
				out, err := s.Encode(pos.val(contractAppender{mode: sh.mode}))
				class := sh.class
				if class == "guard" {
					// A guard cell whose observed output is pinned in
					// goldens is a position where the length information
					// does not exist (see the goldens doc) — silent there.
					if _, undetectable := goldens[pos.name+"/"+sh.mode]; undetectable {
						class = "silent"
					}
				}
				switch class {
				case "legal":
					if err != nil {
						t.Fatalf("legal shape errored: %v", err)
					}
					want, werr := s.Encode(pos.twin(sh.twin))
					if werr != nil {
						t.Fatalf("plain-string twin errored: %v", werr)
					}
					if !bytes.Equal(out, want) {
						t.Fatalf("legal shape diverged from plain-string twin:\n got %x\nwant %x", out, want)
					}
				case "guard":
					if err == nil {
						t.Fatalf("want named error, got nil (out=%x)", out)
					}
					var se *SemanticError
					if !errors.As(err, &se) {
						t.Fatalf("want *SemanticError in chain, got %T: %v", err, err)
					}
					if !strings.Contains(err.Error(), "AppendText returned a slice shorter than its input") {
						t.Fatalf("error does not name the violation: %v", err)
					}
				case "silent":
					if err != nil {
						t.Fatalf("documented-silent shape errored: %v", err)
					}
					if got := hex.EncodeToString(out); got != goldens[pos.name+"/"+sh.mode] {
						t.Fatalf("observed silent output changed:\n got %s\nwant %s", got, goldens[pos.name+"/"+sh.mode])
					}
				case "error":
					if err == nil || !strings.Contains(err.Error(), "appender boom") {
						t.Fatalf("want appender's own error, got %v", err)
					}
				}
			})
		}
	}
}

// TestMatrix_AppendTextReturnShapesJSONImmunity pins that the JSON
// encoder cannot be affected by any AppendText return shape: it
// materializes text via AppendText(nil) (textValue), so the returned
// bytes simply ARE the text — there is no backfill arithmetic to
// corrupt and nothing to guard. Documenting: for contract-violating
// appenders the two wire formats legitimately differ — binary rejects
// the shorter-than-input return via the backfill guard while JSON
// emits the fresh slice's content verbatim.
func TestMatrix_AppendTextReturnShapesJSONImmunity(t *testing.T) {
	recordSchema := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"string"},{"name":"b","type":"string"}]}`
	positions := []struct {
		name   string
		schema string
		val    func(v contractAppender) any
		wrap   func(cell string) string // cell JSON → full document
	}{
		{"top-level", `"string"`,
			func(v contractAppender) any { return v },
			func(cell string) string { return cell }},
		{"record-second-field", recordSchema,
			func(v contractAppender) any { return map[string]any{"a": "0123456789", "b": v} },
			func(cell string) string { return `{"a":"0123456789","b":` + cell + `}` }},
	}
	shapes := []struct {
		mode string
		cell string // expected JSON for the appender's field; "" means expect the appender's error
	}{
		{"legal-append", `"hello"`},
		{"legal-zero-append", `""`},
		// AppendText(nil): a fresh return is the text, verbatim.
		{"fresh-short", `"x"`},
		{"fresh-long", `"` + strings.Repeat("z", 64) + `"`},
		// len(nil) == 0, so the equal-len fresh return is empty.
		{"fresh-equal-len", `""`},
		{"error-return", ""},
	}
	for _, pos := range positions {
		s := MustParse(pos.schema)
		for _, sh := range shapes {
			t.Run(pos.name+"/"+sh.mode, func(t *testing.T) {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("EncodeJSON panicked: %v", r)
					}
				}()
				out, err := s.EncodeJSON(pos.val(contractAppender{mode: sh.mode}))
				if sh.cell == "" {
					if err == nil || !strings.Contains(err.Error(), "appender boom") {
						t.Fatalf("want appender's own error, got %v", err)
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if want := pos.wrap(sh.cell); string(out) != want {
					t.Fatalf("json output:\n got %s\nwant %s", out, want)
				}
			})
		}
	}
}
