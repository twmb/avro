package avro_test

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// The enum-LEVEL "default" attribute must be a JSON STRING token naming a
// member symbol. The token-type verdict is decided BEFORE the membership
// check: a non-string body (number, bool, array, object, explicit null)
// can never name a symbol, and deciding by membership alone would let the
// json.Unmarshal zero value "" flow into the membership test — under a
// WithLaxNames validator that accepts empty name components, a schema
// whose symbols legitimately include "" would then silently BIND the
// garbage default to the "" symbol (and schema evolution would fill it
// for unknown writer symbols) while the metadata surface reports no
// default at all.
//
// References: fastavro rejects every non-member (hence every non-string)
// enum default at parse (SchemaParseException, executed 1.12.2); Java
// binds NO default for a non-text token (Schema.java:1921-1925 —
// enumDefault.textValue() is null for non-text nodes, which skips
// EnumSchema's containment check at Schema.java:1100). Neither reference
// ever binds a default from a non-string token.

func laxAllEnumNames(string) error { return nil }

// enumResolutionFill encodes a writer symbol absent from reader r and
// resolved-decodes it, returning the filled reader default. The writer
// shares the reader's fullname (enum resolution matches by name) and
// carries the unknown symbol "UNKNOWN__".
func enumResolutionFill(t *testing.T, r *avro.Schema) (string, error) {
	t.Helper()
	w := avro.MustParse(`{"type":"enum","name":"E","symbols":["A","B","UNKNOWN__"]}`)
	res, err := avro.Resolve(w, r)
	if err != nil {
		return "", err
	}
	wire, err := w.Encode("UNKNOWN__")
	if err != nil {
		t.Fatalf("writer encode: %v", err)
	}
	var got string
	if _, err := res.Decode(wire, &got); err != nil {
		return "", err
	}
	return got, nil
}

// A non-string enum default under a lax-name validator with "" in symbols
// must reject by token type — never parse and bind the "" symbol.
func TestRegression_EnumDefaultLaxPhantomBindRejected(t *testing.T) {
	s, err := avro.Parse(
		`{"type":"enum","name":"E","symbols":["","A"],"default":5}`,
		avro.WithLaxNames(laxAllEnumNames))
	if err != nil {
		if !strings.Contains(err.Error(), "is not a string") {
			t.Fatalf("reject reason must be the token type, got: %v", err)
		}
		return
	}
	// Parse accepted: demonstrate the full phantom bind — metadata denies
	// the default while resolution fills it.
	root := s.Root()
	filled, ferr := enumResolutionFill(t, s)
	t.Fatalf("non-string enum default parsed under lax names: HasEnumDefault=%v EnumDefault=%q, resolution filled %q (err=%v) — must reject (default is not a string)",
		root.HasEnumDefault, root.EnumDefault, filled, ferr)
}

// An explicit null default is a non-string token like any other — it must
// reject in BOTH modes, even though json.Unmarshal(null, *string) is a
// no-error no-op that leaves the zero value in place.
func TestRegression_EnumDefaultExplicitNullRejected(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		opts   []avro.SchemaOpt
	}{
		{"strict", `{"type":"enum","name":"E","symbols":["A"],"default":null}`, nil},
		{"lax-empty-symbol", `{"type":"enum","name":"E","symbols":["","A"],"default":null}`,
			[]avro.SchemaOpt{avro.WithLaxNames(laxAllEnumNames)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := avro.Parse(c.schema, c.opts...)
			if err == nil {
				t.Fatal("null enum default parsed; must reject (not a string)")
			}
			if !strings.Contains(err.Error(), "is not a string") {
				t.Fatalf("reject reason must be the token type, got: %v", err)
			}
		})
	}
}

// The strict-mode reject of a non-string default must name the offending
// token — not echo the empty string the failed Unmarshal left behind.
func TestRegression_EnumDefaultNonStringEchoNamesToken(t *testing.T) {
	_, err := avro.Parse(`{"type":"enum","name":"E","symbols":["A"],"default":5}`)
	if err == nil {
		t.Fatal("non-string enum default parsed; must reject")
	}
	msg := err.Error()
	if !strings.Contains(msg, "is not a string") || !strings.Contains(msg, "5") {
		t.Fatalf("error must name the non-string token, got: %v", err)
	}
	if strings.Contains(msg, `default ""`) {
		t.Fatalf("error echoes the Unmarshal zero value instead of the token: %v", err)
	}
}

// Boundary controls for the token-type check.
func TestEnumDefaultEmptySymbolBoundary(t *testing.T) {
	// An explicit "" default is a legal STRING token; with "" a member
	// (lax names) it binds, both surfaces agree, and resolution fills it.
	t.Run("explicit-empty-binds", func(t *testing.T) {
		s, err := avro.Parse(
			`{"type":"enum","name":"E","symbols":["","A"],"default":""}`,
			avro.WithLaxNames(laxAllEnumNames))
		if err != nil {
			t.Fatalf("explicit \"\" default with \"\" a member must parse: %v", err)
		}
		root := s.Root()
		if !root.HasEnumDefault || root.EnumDefault != "" {
			t.Fatalf("metadata: HasEnumDefault=%v EnumDefault=%q, want true/\"\"", root.HasEnumDefault, root.EnumDefault)
		}
		filled, err := enumResolutionFill(t, s)
		if err != nil || filled != "" {
			t.Fatalf("resolution fill = %q, %v; want \"\"", filled, err)
		}
	})
	// The same "" default WITHOUT "" in symbols is a membership reject —
	// the string token passes the type check and fails containment.
	t.Run("empty-not-member-rejects", func(t *testing.T) {
		for _, c := range []struct {
			name string
			opts []avro.SchemaOpt
		}{
			{"strict", nil},
			{"lax", []avro.SchemaOpt{avro.WithLaxNames(laxAllEnumNames)}},
		} {
			_, err := avro.Parse(`{"type":"enum","name":"E","symbols":["A"],"default":""}`, c.opts...)
			if err == nil || !strings.Contains(err.Error(), "not a member") {
				t.Fatalf("%s: want membership reject, got: %v", c.name, err)
			}
		}
	})
}

// enumDefaultToken is one default-token cell of the class-elimination
// matrix: its JSON spelling, whether it is a JSON string token, and (for
// strings) the symbol it names.
type enumDefaultToken struct {
	name     string
	raw      string // as written in the schema JSON
	isString bool
	strVal   string // meaningful only when isString
}

var enumDefaultTokens = []enumDefaultToken{
	{"member-string", `"A"`, true, "A"},
	{"non-member-string", `"Z"`, true, "Z"},
	{"empty-string", `""`, true, ""},
	{"number", `5`, false, ""},
	{"null", `null`, false, ""},
	{"bool", `true`, false, ""},
	{"array", `["A"]`, false, ""},
	{"object", `{"a":1}`, false, ""},
}

type enumDefaultMode struct {
	name string
	lax  bool
}

var enumDefaultModes = []enumDefaultMode{{"strict", false}, {"lax", true}}

type enumDefaultSymbols struct {
	name          string
	json          string
	members       []string
	containsEmpty bool
}

var enumDefaultSymbolSets = []enumDefaultSymbols{
	{"plain", `["A","B"]`, []string{"A", "B"}, false},
	{"with-empty", `["","A","B"]`, []string{"", "A", "B"}, true},
}

// enumDefaultCellSchema renders the cell's schema JSON.
func enumDefaultCellSchema(sym enumDefaultSymbols, tok enumDefaultToken) string {
	return fmt.Sprintf(`{"type":"enum","name":"E","symbols":%s,"default":%s}`, sym.json, tok.raw)
}

// enumDefaultCellExpect derives the cell's verdict from the RULE, not the
// implementation: the schema parses iff the symbol set is legal in the
// mode AND the default is a string token naming a member. Reject class:
// symbol-set failures reject on the symbol; string non-members reject on
// membership; every non-string token rejects on token type.
func enumDefaultCellExpect(mode enumDefaultMode, sym enumDefaultSymbols, tok enumDefaultToken) (accept bool, rejectContains string) {
	if sym.containsEmpty && !mode.lax {
		return false, "symbol"
	}
	if !tok.isString {
		return false, "is not a string"
	}
	if slices.Contains(sym.members, tok.strVal) {
		return true, ""
	}
	return false, "not a member"
}

// TestMatrix_EnumDefaultTokenClassElimination crosses default token type
// x name mode x symbol set and checks the parse verdict plus, for
// accepted cells: metadata parity (HasEnumDefault/EnumDefault), the
// render round-trip (Root().Schema() re-parse preserves the default),
// canonical stripping (PCF never carries "default"), and the resolution
// fill (an unknown writer symbol resolves to the default).
func TestMatrix_EnumDefaultTokenClassElimination(t *testing.T) {
	for _, mode := range enumDefaultModes {
		for _, sym := range enumDefaultSymbolSets {
			for _, tok := range enumDefaultTokens {
				t.Run(mode.name+"/"+sym.name+"/"+tok.name, func(t *testing.T) {
					var opts []avro.SchemaOpt
					if mode.lax {
						opts = append(opts, avro.WithLaxNames(laxAllEnumNames))
					}
					schema := enumDefaultCellSchema(sym, tok)
					s, err := avro.Parse(schema, opts...)
					accept, rejectContains := enumDefaultCellExpect(mode, sym, tok)
					if !accept {
						if err == nil {
							t.Fatalf("parse accepted, want reject containing %q", rejectContains)
						}
						if !strings.Contains(err.Error(), rejectContains) {
							t.Fatalf("reject %v, want it to contain %q", err, rejectContains)
						}
						return
					}
					if err != nil {
						t.Fatalf("parse rejected an accept cell: %v", err)
					}

					root := s.Root()
					if !root.HasEnumDefault || root.EnumDefault != tok.strVal {
						t.Fatalf("metadata HasEnumDefault=%v EnumDefault=%q, want true/%q",
							root.HasEnumDefault, root.EnumDefault, tok.strVal)
					}
					if strings.Contains(string(s.Canonical()), `"default"`) {
						t.Fatalf("canonical form carries the stripped default: %s", s.Canonical())
					}
					rebuilt, err := root.Schema(opts...)
					if err != nil {
						t.Fatalf("render re-parse: %v", err)
					}
					rr := rebuilt.Root()
					if !rr.HasEnumDefault || rr.EnumDefault != tok.strVal {
						t.Fatalf("render round-trip HasEnumDefault=%v EnumDefault=%q, want true/%q",
							rr.HasEnumDefault, rr.EnumDefault, tok.strVal)
					}
					filled, err := enumResolutionFill(t, s)
					if err != nil {
						t.Fatalf("resolution fill: %v", err)
					}
					if filled != tok.strVal {
						t.Fatalf("resolution filled %q, want %q", filled, tok.strVal)
					}
				})
			}
		}
	}
}

// FIELD-level enum defaults go through the record default pipeline, whose
// enum arm requires a string (validateLeaf) and validates membership at
// parse — deliberately stricter than the references (a non-member default
// can never encode; fastavro parses non-member field defaults outright,
// Java's isValidDefault ENUM arm is isTextual-only). The token classes
// eliminate the same way at this second consumption site.
func TestMatrix_EnumFieldDefaultTokenTypes(t *testing.T) {
	cells := []struct {
		name    string
		raw     string
		wantErr string // "" accepts
	}{
		{"member-string", `"A"`, ""},
		{"non-member-string", `"Z"`, "not a member"},
		{"number", `5`, "expected string"},
		{"null", `null`, "expected string"},
		{"bool", `true`, "expected string"},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			schema := fmt.Sprintf(`{"type":"record","name":"R","fields":[
				{"name":"f","type":{"type":"enum","name":"E","symbols":["A","B"]},"default":%s}]}`, c.raw)
			s, err := avro.Parse(schema)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("parse: %v", err)
				}
				root := s.Root()
				if d, ok := root.Fields[0].Default.(string); !ok || d != "A" {
					t.Fatalf("field default = %#v, want \"A\"", root.Fields[0].Default)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("want reject containing %q, got: %v", c.wantErr, err)
			}
		})
	}
}

// A "default" key riding a WRAPPER around an enum REFERENCE never BINDS:
// the enum-level default is read only at the definition site, and a
// reference wrapper's own kind is the referenced NAME, which binds nothing.
// The key is therefore an ordinary custom property of the usage site for
// EVERY token type — it rides in Props as its only surface, it is preserved
// by the rebuild as written, and the enum it names still declares no
// default. Nothing about the wrapper's token type changes that: binding is
// decided by placement, never by the body.
//
// The SchemaCache cross-parse spelling differs by design and the difference
// is the splice, not the routing: the cache materializes the DEFINITION in
// place of the reference, and the merge is definition-wins on
// consumed-ness — a key the definition's own kind consumes cannot be
// re-supplied by a usage site, so an enum definition swallows the wrapper's
// "default" rather than carrying a second one.
func TestRegression_EnumRefWrapperDefaultInert(t *testing.T) {
	for _, tok := range []struct {
		src  string
		want any
	}{
		{`"B"`, "B"},
		{`5`, int64(5)},
	} {
		t.Run("direct-"+tok.src, func(t *testing.T) {
			s, err := avro.Parse(fmt.Sprintf(`{"type":"record","name":"R","fields":[
				{"name":"a","type":{"type":"enum","name":"E","symbols":["A","B"]}},
				{"name":"b","type":{"type":"E","default":%s}}]}`, tok.src))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			root := s.Root()
			b := root.Fields[1].Type
			// Not bound: the usage site declares no enum default.
			if b.HasEnumDefault || b.EnumDefault != "" {
				t.Fatalf("wrapper default BOUND at a reference: HasEnumDefault=%v EnumDefault=%q",
					b.HasEnumDefault, b.EnumDefault)
			}
			// Preserved: Props is its only surface, as written.
			if got, ok := b.Props["default"]; !ok || !reflect.DeepEqual(got, tok.want) {
				t.Fatalf("wrapper default not preserved as written: Props=%#v, want %#v", b.Props, tok.want)
			}
			// And the definition it names is untouched.
			if a := root.Fields[0].Type; a.HasEnumDefault {
				t.Fatalf("the usage site's default reached the DEFINITION: %+v", a)
			}
			rebuilt, err := root.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if !strings.Contains(rebuilt.String(), "default") {
				t.Fatalf("rebuild dropped the as-written wrapper default: %s", rebuilt.String())
			}
			// Re-parsing the rebuild still binds no enum default anywhere:
			// preservation is not promotion.
			again, err := avro.Parse(rebuilt.String())
			if err != nil {
				t.Fatalf("reparse: %v", err)
			}
			for i, f := range again.Root().Fields {
				if f.Type.HasEnumDefault {
					t.Fatalf("field %d bound an enum default after the round trip: %s", i, rebuilt)
				}
			}
		})
	}
	t.Run("cache-splice", func(t *testing.T) {
		var c avro.SchemaCache
		if _, err := c.Parse(`{"type":"enum","name":"E2","symbols":["A","B"]}`); err != nil {
			t.Fatal(err)
		}
		s, err := c.Parse(`{"type":"record","name":"R2","fields":[{"name":"f","type":{"type":"E2","default":"B"}}]}`)
		if err != nil {
			t.Fatalf("cache parse: %v", err)
		}
		root := s.Root()
		f := root.Fields[0].Type
		// The splice replaced the reference with the enum definition, which
		// CONSUMES "default" — definition-wins, so the usage-site copy does
		// not ride along and does not become the definition's default.
		if f.HasEnumDefault || f.EnumDefault != "" || len(f.Props) != 0 {
			t.Fatalf("spliced wrapper default leaked: HasEnumDefault=%v EnumDefault=%q Props=%v",
				f.HasEnumDefault, f.EnumDefault, f.Props)
		}
		rebuilt, err := root.Schema()
		if err != nil {
			t.Fatalf("splice rebuild: %v", err)
		}
		if strings.Contains(rebuilt.String(), "default") {
			t.Fatalf("splice rebuild carries the inert wrapper default: %s", rebuilt.String())
		}
	})
}

// TestDifferentialFastavroEnumDefaultToken drives every enum-LEVEL matrix
// cell through fastavro's parser and asserts VERDICT parity wherever
// fastavro can parse the cell's schema shape at all (self-calibrated: a
// cell whose no-default twin fastavro rejects — e.g. the ""-symbol sets,
// which fail its symbol name validation — is schema-blocked for fastavro
// and skipped). fastavro rejects every non-member default, which
// subsumes every non-string token (None/True/lists/dicts are not
// members), so parity holds cell-for-cell on the parseable shapes.
//
// The FIELD-level cells pin fastavro's LAXER observed verdict instead:
// fastavro 1.12.2 parses non-member and non-string enum FIELD defaults
// outright (no field-default validation at parse), a calibrated
// divergence — twmb is deliberately stricter. A fastavro release that
// starts rejecting flips these cells and forces recalibration.
func TestDifferentialFastavroEnumDefaultToken(t *testing.T) {
	o := startOracle(t)

	for _, mode := range enumDefaultModes {
		for _, sym := range enumDefaultSymbolSets {
			for _, tok := range enumDefaultTokens {
				t.Run("level/"+mode.name+"/"+sym.name+"/"+tok.name, func(t *testing.T) {
					twin := fmt.Sprintf(`{"type":"enum","name":"E","symbols":%s}`, sym.json)
					if !o.call(oracleJob{Op: "parse", Schema: []byte(twin)}).OK {
						t.Skipf("schema shape blocked for fastavro (no-default twin rejects)")
					}
					schema := enumDefaultCellSchema(sym, tok)
					fast := o.call(oracleJob{Op: "parse", Schema: []byte(schema)})

					var opts []avro.SchemaOpt
					if mode.lax {
						opts = append(opts, avro.WithLaxNames(laxAllEnumNames))
					}
					_, err := avro.Parse(schema, opts...)
					if fast.OK != (err == nil) {
						t.Fatalf("verdict divergence: twmb err=%v, fastavro ok=%v err=%s", err, fast.OK, fast.Err)
					}
				})
			}
		}
	}

	// Field-level calibration, executed 1.12.2: fastavro TYPE-checks a
	// field default against the enum (number/null reject: "Default value
	// <5> must match schema type: enum") but does NOT check MEMBERSHIP
	// (a non-member string parses outright). twmb rejects all three —
	// stricter only on the membership half. A fastavro release changing
	// either half flips its cell and forces recalibration.
	for _, c := range []struct {
		name   string
		raw    string
		fastOK bool
	}{
		{"non-member-string", `"Z"`, true},
		{"number", `5`, false},
		{"null", `null`, false},
	} {
		t.Run("field/"+c.name, func(t *testing.T) {
			schema := fmt.Sprintf(`{"type":"record","name":"R","fields":[
				{"name":"f","type":{"type":"enum","name":"E","symbols":["A","B"]},"default":%s}]}`, c.raw)
			fast := o.call(oracleJob{Op: "parse", Schema: []byte(schema)})
			if fast.OK != c.fastOK {
				t.Fatalf("fastavro verdict flipped for enum FIELD default %s: ok=%v err=%s — recalibrate", c.name, fast.OK, fast.Err)
			}
			if _, err := avro.Parse(schema); err == nil {
				t.Fatal("twmb accepted a non-member/non-string enum field default; the stricter posture pin broke")
			}
		})
	}

	// Wrapper-reference calibration, executed 1.12.2: fastavro rejects
	// the WRAPPED named-reference form itself (UnknownType for
	// {"type":"E"} with or without extra keys), so the inert-default
	// posture is untestable there — the shape is fastavro-blocked. twmb
	// and Java both accept the wrapped form (Java TestUnionSelfReference)
	// and neither consumes the riding "default".
	t.Run("wrapper-ref", func(t *testing.T) {
		clean := `{"type":"record","name":"R","fields":[
			{"name":"a","type":{"type":"enum","name":"E","symbols":["A","B"]}},
			{"name":"b","type":{"type":"E"}}]}`
		withDefault := `{"type":"record","name":"R","fields":[
			{"name":"a","type":{"type":"enum","name":"E","symbols":["A","B"]}},
			{"name":"b","type":{"type":"E","default":5}}]}`
		if o.call(oracleJob{Op: "parse", Schema: []byte(clean)}).OK {
			t.Fatal("fastavro now ACCEPTS the wrapped named-reference form — recalibrate (the default-inert posture becomes testable)")
		}
		if o.call(oracleJob{Op: "parse", Schema: []byte(withDefault)}).OK {
			t.Fatal("fastavro accepts the default-bearing wrapper while rejecting the clean one — recalibrate")
		}
		if _, err := avro.Parse(withDefault); err != nil {
			t.Fatalf("twmb rejects the inert wrapper default: %v", err)
		}
	})
}
