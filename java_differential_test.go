//go:build cisuite

package avro_test

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/avro"
	"github.com/twmb/avro/internal/avrotest"
)

// ---------- java_differential_test.go ----------

// This file is behind the `cisuite` build tag, so it *only* runs in CI: `go
// test -tags=cisuite`. CI is where the JVM + avro-tools jar are provisioned.
// A plain `go test ./...` never compiles or runs it. CI builds
// testdata/oracle/SchemaOracle.java against the avro-tools fat jar and sets
// AVRO_TOOLS_JAR. See .github/workflows/test.yml.

// TestDifferentialJavaFingerprint cross-checks twmb's Parsing Canonical Form
// and CRC-64-AVRO fingerprint against org.apache.avro.SchemaNormalization via
// the SchemaOracle subprocess. The static schema-tests.txt vectors omit named
// types referenced multiple times and forward references. We exercise those
// here, against live Java, for the canonical first-occurrence behavior. When
// Java rejects a schema twmb accepts, that parse-level divergence is logged
// rather than failed. There is no Java fingerprint to compare, and the
// acceptance difference is surfaced for triage rather than asserted away.
func TestDifferentialJavaFingerprint(t *testing.T) {
	in, out := schemaOraclePipes(t, "set AVRO_TOOLS_JAR to the avro-tools fat jar to run the Java differential")

	// ask sends one schema to the Java oracle and returns its response.
	ask := func(t *testing.T, schema string) (ok bool, fp int64, canon, errMsg string) {
		t.Helper()
		if _, err := io.WriteString(in, schema+"\n"); err != nil {
			t.Fatalf("write schema to oracle: %v", err)
		}
		line, err := out.ReadString('\n')
		if err != nil {
			t.Fatalf("read oracle response: %v", err)
		}
		parts := strings.SplitN(strings.TrimRight(line, "\n"), "\t", 3)
		switch parts[0] {
		case "ERR":
			msg := ""
			if len(parts) > 1 {
				msg = parts[1]
			}
			return false, 0, "", msg
		case "OK":
			if len(parts) != 3 {
				t.Fatalf("malformed OK response: %q", line)
			}
			fp, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				t.Fatalf("parse java fingerprint %q: %v", parts[1], err)
			}
			cb, err := base64.StdEncoding.DecodeString(parts[2])
			if err != nil {
				t.Fatalf("decode java canonical b64: %v", err)
			}
			return true, fp, string(cb), ""
		default:
			t.Fatalf("unexpected oracle line: %q", line)
			return false, 0, "", ""
		}
	}

	cases := []struct{ name, schema string }{
		{"int", `"int"`},
		{"record", `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`},
		// Named type defined then referenced twice (backward). Java parses
		// this. The canonical first-occurrence transform must keep twmb's
		// output byte-identical to Java's here: the definition *is* the first
		// occurrence.
		{"define-then-ref-twice", `{"type":"record","name":"o","fields":[{"name":"a","type":{"type":"record","name":"i","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":"i"},{"name":"c","type":"i"}]}`},
		{"namespaced define-then-ref", `{"type":"record","name":"o","namespace":"com.x","fields":[{"name":"a","type":{"type":"record","name":"i","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":"i"}]}`},
		{"recursive", `{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}`},
		// Forward references: Java may or may not accept these in a single
		// parse. If it rejects, the divergence is logged, since twmb is more
		// lenient. If it accepts, twmb's first-occurrence canonical form and
		// fingerprint must match.
		{"forward-ref", `{"type":"record","name":"outer","fields":[{"name":"ref","type":{"type":"inner"}},{"name":"def","type":{"type":"record","name":"inner","fields":[{"name":"x","type":"int"}]}}]}`},
		{"namespaced forward-ref", `{"type":"record","name":"Outer","namespace":"com.x","fields":[{"name":"a","type":"Inner"},{"name":"b","type":{"type":"record","name":"Inner","fields":[{"name":"v","type":"int"}]}}]}`},
	}

	var compared, divergent int
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, twmbErr := avro.Parse(c.schema)
			ok, fp, canon, errMsg := ask(t, c.schema)

			if !ok {
				// Java rejected the schema. Surface (don't fail) the parse-
				// level acceptance difference for triage.
				divergent++
				if twmbErr == nil {
					t.Logf("DIVERGENCE: Java rejects but twmb accepts %s: %q", c.name, errMsg)
				} else {
					t.Logf("both reject %s (twmb: %v; java: %q)", c.name, twmbErr, errMsg)
				}
				return
			}
			if twmbErr != nil {
				t.Errorf("twmb rejects a schema Java accepts (%s): %v", c.name, twmbErr)
				return
			}
			if got := string(s.Canonical()); got != canon {
				t.Errorf("canonical form differs from Java (%s)\n twmb %s\n java %s", c.name, got, canon)
			}
			h := avro.NewRabin()
			h.Write(s.Canonical())
			if got := int64(h.Sum64()); got != fp {
				t.Errorf("fingerprint differs from Java (%s): twmb %d java %d", c.name, got, fp)
			}
			compared++
		})
	}
	t.Logf("java differential: %d schemas fingerprint-matched, %d parse-level divergences logged", compared, divergent)
}

// ---------- java_matrix_differential_test.go ----------

// Behind the `cisuite` build tag: runs only where the JVM + avro-tools jar
// are provisioned, via `go test -tags=cisuite`. See
// .github/workflows/test.yml. Plain `go test ./...` never compiles this file.

// startMatrixJavaOracle launches SchemaOracle and returns two closures over
// one subprocess. rt drives the "RT" command, where Java binary-decodes our
// bytes and re-encodes them. fpCanon drives the bare-schema command, Java's
// parsingFingerprint64 + Parsing Canonical Form.
func startMatrixJavaOracle(t *testing.T) (
	rt func(t *testing.T, schema string, binary []byte) (ok bool, jsonOut, binOut []byte, errMsg string),
	fpCanon func(t *testing.T, schema string) (ok bool, fp int64, canon, errMsg string),
) {
	t.Helper()
	in, out := schemaOraclePipes(t, "set AVRO_TOOLS_JAR to the avro-tools fat jar to run the Java matrix differential")

	// The oracle protocol is one request line per response line. Schemas
	// composed by the matrix contain newlines, so we compact them first.
	compact := func(t *testing.T, schema string) string {
		t.Helper()
		var buf bytes.Buffer
		if err := json.Compact(&buf, []byte(schema)); err != nil {
			t.Fatalf("compact schema: %v\n%s", err, schema)
		}
		return buf.String()
	}

	rt = func(t *testing.T, schema string, binary []byte) (bool, []byte, []byte, string) {
		t.Helper()
		req := "RT\t" + compact(t, schema) + "\t" + base64.StdEncoding.EncodeToString(binary) + "\n"
		if _, err := io.WriteString(in, req); err != nil {
			t.Fatalf("write RT: %v", err)
		}
		line, err := out.ReadString('\n')
		if err != nil {
			t.Fatalf("read RT response: %v", err)
		}
		parts := strings.SplitN(strings.TrimRight(line, "\n"), "\t", 3)
		if parts[0] == "ERR" {
			msg := ""
			if len(parts) > 1 {
				msg = parts[1]
			}
			return false, nil, nil, msg
		}
		if len(parts) != 3 {
			t.Fatalf("malformed RT response: %q", line)
		}
		jsonOut, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			t.Fatalf("decode RT json: %v", err)
		}
		binOut, err := base64.StdEncoding.DecodeString(parts[2])
		if err != nil {
			t.Fatalf("decode RT binary: %v", err)
		}
		return true, jsonOut, binOut, ""
	}

	fpCanon = func(t *testing.T, schema string) (bool, int64, string, string) {
		t.Helper()
		if _, err := io.WriteString(in, compact(t, schema)+"\n"); err != nil {
			t.Fatalf("write FP: %v", err)
		}
		line, err := out.ReadString('\n')
		if err != nil {
			t.Fatalf("read FP response: %v", err)
		}
		parts := strings.SplitN(strings.TrimRight(line, "\n"), "\t", 3)
		if parts[0] == "ERR" {
			msg := ""
			if len(parts) > 1 {
				msg = parts[1]
			}
			return false, 0, "", msg
		}
		if len(parts) != 3 {
			t.Fatalf("malformed FP response: %q", line)
		}
		fp, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			t.Fatalf("parse fingerprint: %v", err)
		}
		canon, err := base64.StdEncoding.DecodeString(parts[2])
		if err != nil {
			t.Fatalf("decode canonical: %v", err)
		}
		return true, fp, string(canon), ""
	}
	return rt, fpCanon
}

// javaMatrixCheck runs the two Java-oracle checks for one composed cell.
// Java's binary re-encode of twmb's wire must be byte-identical. Java's
// canonical form + Rabin fingerprint must match twmb's.
func javaMatrixCheck(t *testing.T, rt func(*testing.T, string, []byte) (bool, []byte, []byte, string),
	fpCanon func(*testing.T, string) (bool, int64, string, string),
	schemaJSON string, vin any,
) {
	t.Helper()
	s := avrotest.MustParse(t, schemaJSON)
	w1 := avrotest.MustAppendEncode(t, s, nil, vin)
	ok, _, binOut, errMsg := rt(t, schemaJSON, w1)
	if !ok {
		t.Fatalf("Java could not round-trip twmb's bytes: %s\nschema: %s\nwire: %x", errMsg, schemaJSON, w1)
	}
	if !bytes.Equal(binOut, w1) {
		t.Fatalf("Java re-encode differs from twmb:\n twmb=%x\n java=%x\nschema: %s", w1, binOut, schemaJSON)
	}
	ok, fp, canon, errMsg := fpCanon(t, schemaJSON)
	if !ok {
		t.Fatalf("Java could not fingerprint: %s\nschema: %s", errMsg, schemaJSON)
	}
	twmbCanon := s.Canonical()
	if canon != string(twmbCanon) {
		t.Fatalf("canonical diverges:\n twmb=%s\n java=%s", twmbCanon, canon)
	}
	h := avro.NewRabin()
	h.Write(twmbCanon)
	if got := int64(h.Sum64()); got != fp {
		t.Fatalf("Rabin fingerprint diverges: twmb=%d java=%d\nschema: %s", got, fp, schemaJSON)
	}
}

// TestDifferentialJavaMatrix drives every (fragment × context) matrix cell
// through the Apache Avro Java reference. Wire-layout and canonical-form
// agreement on arbitrary composed schemas, not just curated vectors.
func TestDifferentialJavaMatrix(t *testing.T) {
	rt, fpCanon := startMatrixJavaOracle(t)
	for _, fr := range matFrags() {
		for _, cx := range matCtxs() {
			if cx.skip != nil && cx.skip(fr.kind) {
				continue
			}
			t.Run(fr.label+"/"+cx.label, func(t *testing.T) {
				u := &uniq{}
				schemaJSON := cx.schema(fr.schema(u), fr.kind, u)
				javaMatrixCheck(t, rt, fpCanon, schemaJSON, cx.wrap(fr.values[0]))
			})
		}
	}
}

// TestDifferentialJavaMatrixRecursion covers the recursive shapes. Java is
// the *one* external oracle that accepts forward references, the twmb+Java
// extension fastavro rejects. So the fwd-ref shapes run here unskipped.
func TestDifferentialJavaMatrixRecursion(t *testing.T) {
	rt, fpCanon := startMatrixJavaOracle(t)
	for _, sh := range recShapes() {
		for _, d := range []int{0, 3} {
			t.Run(fmt.Sprintf("%s/depth%d", sh.label, d), func(t *testing.T) {
				javaMatrixCheck(t, rt, fpCanon, sh.schema, sh.value(d))
			})
		}
	}
}

// TestDifferentialJavaAcceptance: schema-*acceptance* parity with the Java
// reference over every composed cell and every structurally-broken mutant.
// Java enforces the full mutator set: missing fields/symbols/size/name,
// duplicates, nested unions, negative sizes, empty field names. So unlike the
// fastavro twin, we scope nothing out. The bare-schema oracle command doubles
// as the parse probe: OK means Java parsed it, ERR means rejected.
func TestDifferentialJavaAcceptance(t *testing.T) {
	_, fpCanon := startMatrixJavaOracle(t)
	for _, cell := range acceptanceCells() {
		ok, _, _, errMsg := fpCanon(t, cell)
		if !ok {
			t.Fatalf("Java rejected a schema twmb accepts: %s\n%s", errMsg, cell)
		}
		for _, m := range schemaMutants(cell) {
			ok, _, _, _ := fpCanon(t, m.schema)
			if ok {
				t.Errorf("Java accepted mutant %s that twmb rejects:\n%s", m.label, m.schema)
			}
		}
	}
}

// jsonSemanticEqual compares two JSON documents by *value*: objects by key,
// arrays by index, numbers by exact rational value, strings by codepoints.
// Jackson spells float zero "0.0" where twmb writes "0"; both are the same
// number. Jackson writes high bytes raw where twmb escapes \u00XX; both
// decode identically.
func jsonSemanticEqual(a, b []byte) bool {
	var av, bv any
	da := json.NewDecoder(bytes.NewReader(a))
	da.UseNumber()
	db := json.NewDecoder(bytes.NewReader(b))
	db.UseNumber()
	if da.Decode(&av) != nil || db.Decode(&bv) != nil {
		return false
	}
	var eq func(x, y any) bool
	eq = func(x, y any) bool {
		switch xv := x.(type) {
		case json.Number:
			yv, ok := y.(json.Number)
			if !ok {
				return false
			}
			xr, ok1 := new(big.Rat).SetString(xv.String())
			yr, ok2 := new(big.Rat).SetString(yv.String())
			return ok1 && ok2 && xr.Cmp(yr) == 0
		case map[string]any:
			yv, ok := y.(map[string]any)
			if !ok || len(xv) != len(yv) {
				return false
			}
			for k, v := range xv {
				if !eq(v, yv[k]) {
					return false
				}
			}
			return true
		case []any:
			yv, ok := y.([]any)
			if !ok || len(xv) != len(yv) {
				return false
			}
			for i := range xv {
				if !eq(xv[i], yv[i]) {
					return false
				}
			}
			return true
		default:
			return x == y
		}
	}
	return eq(av, bv)
}

// TestDifferentialJavaJSONForm compares twmb's Avro-JSON encoding against
// Java's JsonEncoder per cell, semantically. Scoped to non-logical fragments.
// Java's *generic* datum path writes logical types in underlying form (raw
// longs) while twmb writes the enriched form (RFC 3339 strings). That is a
// documented representation difference, not a parity target. twmb runs with
// TaggedUnions because Java's JsonEncoder always writes the {branch: value}
// union envelope.
func TestDifferentialJavaJSONForm(t *testing.T) {
	rt, _ := startMatrixJavaOracle(t)
	// We exclude rec0, the zero-field record. avro-tools 1.12.0's JsonEncoder
	// emits *zero bytes* for an empty record, and for any document containing
	// one. That is observed empirically via the RT oracle: the generator
	// appears to never complete or flush the empty object. twmb's "{}" is the
	// only valid JSON for an empty record and matches fastavro. Java's empty
	// output is not a parity target.
	eligible := map[string]bool{
		"null": true, "boolean": true, "int": true, "long": true,
		"float": true, "double": true, "string": true, "bytes": true,
		"enum3": true, "enum1": true, "fixed0": true, "fixed1": true,
		"fixed16": true, "rec2": true, "arr-int": true,
		"map-str": true,
	}
	for _, fr := range matFrags() {
		if !eligible[fr.label] {
			continue
		}
		for _, cx := range matCtxs() {
			if cx.skip != nil && cx.skip(fr.kind) {
				continue
			}
			t.Run(fr.label+"/"+cx.label, func(t *testing.T) {
				u := &uniq{}
				schemaJSON := cx.schema(fr.schema(u), fr.kind, u)
				s := avrotest.MustParse(t, schemaJSON)
				vin := cx.wrap(fr.values[0])
				w1 := avrotest.MustAppendEncode(t, s, nil, vin, avro.TaggedUnions())
				var a1 any
				avrotest.MustDecode(t, s, w1, &a1, avro.TaggedUnions())
				j1 := avrotest.MustAppendEncodeJSON(t, s, nil, a1, avro.TaggedUnions())
				ok, javaJSON, _, errMsg := rt(t, schemaJSON, w1)
				if !ok {
					t.Fatalf("Java rt: %s", errMsg)
				}
				if !jsonSemanticEqual(j1, javaJSON) {
					t.Fatalf("JSON form diverges from Java:\n twmb=%s\n java=%s\nschema: %s", j1, javaJSON, schemaJSON)
				}
			})
		}
	}
}

// ---------- java_value_differential_test.go ----------

// This file is behind the `cisuite` build tag, so it *only* runs in CI: `go
// test -tags=cisuite`. CI is where the JVM + avro-tools jar are provisioned.
// A plain `go test ./...` never compiles or runs it. CI builds
// testdata/oracle/SchemaOracle.java against the avro-tools fat jar and sets
// AVRO_TOOLS_JAR. See .github/workflows/test.yml.

// startSchemaOracle launches the SchemaOracle subprocess, skipping when the
// JVM/jar is unavailable. It returns an rt function driving the "RT" command:
// send (schema, avro binary), get back Java's JSON re-encode and binary
// re-encode of the same datum.
func startSchemaOracle(t *testing.T) func(t *testing.T, schema string, binary []byte) (ok bool, jsonOut, binOut []byte, errMsg string) {
	t.Helper()
	in, out := schemaOraclePipes(t, "set AVRO_TOOLS_JAR to the avro-tools fat jar to run the Java value differential")

	return func(t *testing.T, schema string, binary []byte) (ok bool, jsonOut, binOut []byte, errMsg string) {
		t.Helper()
		req := "RT\t" + schema + "\t" + base64.StdEncoding.EncodeToString(binary) + "\n"
		if _, err := io.WriteString(in, req); err != nil {
			t.Fatalf("write RT to oracle: %v", err)
		}
		line, err := out.ReadString('\n')
		if err != nil {
			t.Fatalf("read oracle response: %v", err)
		}
		parts := strings.SplitN(strings.TrimRight(line, "\n"), "\t", 3)
		switch parts[0] {
		case "ERR":
			msg := ""
			if len(parts) > 1 {
				msg = parts[1]
			}
			return false, nil, nil, msg
		case "OK":
			if len(parts) != 3 {
				t.Fatalf("malformed OK response: %q", line)
			}
			jb, err := base64.StdEncoding.DecodeString(parts[1])
			if err != nil {
				t.Fatalf("decode java json b64: %v", err)
			}
			bb, err := base64.StdEncoding.DecodeString(parts[2])
			if err != nil {
				t.Fatalf("decode java binary b64: %v", err)
			}
			return true, jb, bb, ""
		default:
			t.Fatalf("unexpected oracle line: %q", line)
			return false, nil, nil, ""
		}
	}
}

// jsonValueEqual reports whether two JSON documents encode the same value. It
// ignores non-semantic text differences: number formatting ("1" vs "1.0",
// exponent forms), \uXXXX escaping vs raw UTF-8 for the same code points
// (including hex case), and object key order. Numbers compare as float64,
// adequate for the float/bytes/map cases routed here. Precision-critical long
// cases use byte comparison instead.
func jsonValueEqual(a, b []byte) (bool, error) {
	var av, bv any
	da := json.NewDecoder(strings.NewReader(string(a)))
	da.UseNumber()
	if err := da.Decode(&av); err != nil {
		return false, err
	}
	db := json.NewDecoder(strings.NewReader(string(b)))
	db.UseNumber()
	if err := db.Decode(&bv); err != nil {
		return false, err
	}
	var eq func(x, y any) bool
	eq = func(x, y any) bool {
		switch xv := x.(type) {
		case json.Number:
			yv, ok := y.(json.Number)
			if !ok {
				return false
			}
			xf, xe := strconv.ParseFloat(xv.String(), 64)
			yf, ye := strconv.ParseFloat(yv.String(), 64)
			if xe != nil || ye != nil {
				return xv.String() == yv.String()
			}
			return xf == yf
		case map[string]any:
			yv, ok := y.(map[string]any)
			if !ok || len(xv) != len(yv) {
				return false
			}
			for k, xe := range xv {
				ye, ok := yv[k]
				if !ok || !eq(xe, ye) {
					return false
				}
			}
			return true
		case []any:
			yv, ok := y.([]any)
			if !ok || len(xv) != len(yv) {
				return false
			}
			for i := range xv {
				if !eq(xv[i], yv[i]) {
					return false
				}
			}
			return true
		default:
			return reflect.DeepEqual(x, y)
		}
	}
	return eq(av, bv), nil
}

// TestDifferentialJavaValueMatrix sweeps a (schema x value) corpus through
// the SchemaOracle RT command. We assert *value*-level wire parity with Java
// on both formats. On binary, Java decodes twmb's Encode output and
// re-encodes it, and the re-encode must be byte-identical. Multi-entry maps
// compare decoded-back, entry order being unspecified on both sides. On JSON,
// twmb's EncodeJSON (TaggedUnions, the spec form Java emits) must match
// Java's JsonEncoder. cmpJSON chooses byte-identical comparison where the
// text is canonical, value-equal where equally-valid texts differ.
//
// Logical-typed values ride as their *raw* base-type wire values through
// Java's generic datum path, which is exactly what the Avro JSON encoding of
// a logical type is. We report failures with Errorf so one CI run yields the
// complete divergence list.
func TestDifferentialJavaValueMatrix(t *testing.T) {
	rt := startSchemaOracle(t)

	const (
		cmpBytes = "bytes" // JSON wires must be byte-identical
		cmpValue = "value" // JSON wires must encode the same value
	)
	cases := []struct {
		name    string
		schema  string
		value   any
		cmpJSON string
		skipBin bool // multi-entry map: binary entry order unspecified
	}{
		// Primitives: canonical JSON text, byte-exact on both wires.
		{"null", `"null"`, nil, cmpBytes, false},
		{"bool-true", `"boolean"`, true, cmpBytes, false},
		{"bool-false", `"boolean"`, false, cmpBytes, false},
		{"int-zero", `"int"`, int32(0), cmpBytes, false},
		{"int-one", `"int"`, int32(1), cmpBytes, false},
		{"int-neg", `"int"`, int32(-1), cmpBytes, false},
		{"int-max", `"int"`, int32(math.MaxInt32), cmpBytes, false},
		{"int-min", `"int"`, int32(math.MinInt32), cmpBytes, false},
		{"long-max", `"long"`, int64(math.MaxInt64), cmpBytes, false},
		{"long-min", `"long"`, int64(math.MinInt64), cmpBytes, false},
		{"string-empty", `"string"`, "", cmpBytes, false},
		{"string-ascii", `"string"`, "hello", cmpBytes, false},
		// BMP multi-byte is byte-exact: Jackson, like Go, writes BMP
		// non-ASCII as raw UTF-8.
		{"string-unicode-bmp", `"string"`, "héllo 世界", cmpBytes, false},
		// Supplementary-plane (astral) characters: Jackson escapes them as a
		// UTF-16 surrogate pair (U+1F986 -> 🦆). twmb writes raw UTF-8 (f0 9f
		// a6 86). Same string value, both valid JSON texts.
		{"string-unicode-astral", `"string"`, "héllo 世界 🦆", cmpValue, false},
		{"bytes-empty", `"bytes"`, []byte{}, cmpBytes, false},
		{"bytes-ascii", `"bytes"`, []byte("hi"), cmpBytes, false},

		// Floats/doubles: same value, possibly different (equally valid)
		// JSON number text between Go and Jackson.
		{"float-1.5", `"float"`, float32(1.5), cmpValue, false},
		{"float-zero", `"float"`, float32(0), cmpValue, false},
		{"double-one", `"double"`, float64(1), cmpValue, false},
		{"double-pi", `"double"`, 3.14159, cmpValue, false},
		{"double-huge", `"double"`, 1e300, cmpValue, false},
		// Non-finite floats: twmb's default emits the Java-convention quoted
		// strings ("NaN"/"Infinity"/"-Infinity"). This asserts the convention
		// claim against live Java rather than trusting the doc.
		{"float-nan", `"float"`, float32(math.NaN()), cmpValue, false},
		{"double-nan", `"double"`, math.NaN(), cmpValue, false},
		{"double-inf", `"double"`, math.Inf(1), cmpValue, false},
		{"double-neginf", `"double"`, math.Inf(-1), cmpValue, false},

		// bytes/fixed with non-printable content: codepoint-string JSON form.
		// twmb escapes all non-ASCII as \u00XX. Jackson emits raw UTF-8 for
		// the same code points, and uppercase hex when it does escape. Same
		// value, different text.
		{"bytes-binary", `"bytes"`, []byte{0x00, 0x1f, 0x80, 0xff}, cmpValue, false},
		{"fixed-ascii", `{"type":"fixed","name":"F4","size":4}`, []byte("abcd"), cmpBytes, false},
		{"fixed-binary", `{"type":"fixed","name":"F4b","size":4}`, []byte{0x00, 0xff, 0x10, 0x80}, cmpValue, false},
		// U+2028: twmb escapes it for JavaScript safety; Jackson does not.
		{"string-u2028", `"string"`, "a\u2028b", cmpValue, false},

		// Enum.
		{"enum", `{"type":"enum","name":"E","symbols":["A","B","C"]}`, "B", cmpBytes, false},

		// Unions: tagged (spec) form on the JSON side.
		{"union-int-branch", `["null","int"]`, int32(7), cmpBytes, false},
		{"union-null-branch", `["null","int"]`, nil, cmpBytes, false},
		{"union-string-branch", `["null","string"]`, "x", cmpBytes, false},

		// Containers.
		{"array-empty", `{"type":"array","items":"int"}`, []int32{}, cmpBytes, false},
		{"array-int", `{"type":"array","items":"int"}`, []int32{1, 2, 3}, cmpBytes, false},
		{"array-string", `{"type":"array","items":"string"}`, []string{"a", "b"}, cmpBytes, false},
		{"map-empty", `{"type":"map","values":"int"}`, map[string]int32{}, cmpBytes, false},
		{"map-one", `{"type":"map","values":"int"}`, map[string]int32{"k": 7}, cmpBytes, false},
		{"map-multi", `{"type":"map","values":"int"}`, map[string]int32{"a": 1, "b": 2, "c": 3}, cmpValue, true},

		// Records.
		{"record", `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
			map[string]any{"a": int32(1), "b": "v"}, cmpBytes, false},
		{"record-nested", `{"type":"record","name":"O","fields":[{"name":"in","type":{"type":"record","name":"I","fields":[{"name":"x","type":"long"}]}}]}`,
			map[string]any{"in": map[string]any{"x": int64(9)}}, cmpBytes, false},
		{"record-union-field", `{"type":"record","name":"RU","fields":[{"name":"u","type":["null","long"]}]}`,
			map[string]any{"u": int64(5)}, cmpBytes, false},
		{"array-of-record", `{"type":"array","items":{"type":"record","name":"AR","fields":[{"name":"x","type":"int"}]}}`,
			[]any{map[string]any{"x": int32(1)}, map[string]any{"x": int32(2)}}, cmpBytes, false},
		{"recursive-leaf", `{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}`,
			map[string]any{"next": nil}, cmpBytes, false},

		// Int/long-backed logical types: the JSON wire is the base integer,
		// canonical text, byte-exact. Values are the enriched Go types twmb's
		// built-in logical encoders accept.
		{"date", `{"type":"int","logicalType":"date"}`, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), cmpBytes, false},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, 3 * time.Hour, cmpBytes, false},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`, 3 * time.Hour, cmpBytes, false},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, time.UnixMilli(1700000000000).UTC(), cmpBytes, false},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, time.UnixMicro(1700000000123456).UTC(), cmpBytes, false},
		{"timestamp-nanos", `{"type":"long","logicalType":"timestamp-nanos"}`, time.Unix(1700000000, 5).UTC(), cmpBytes, false},
		{"local-timestamp-millis", `{"type":"long","logicalType":"local-timestamp-millis"}`, time.UnixMilli(1700000000000).UTC(), cmpBytes, false},
		{"local-timestamp-micros", `{"type":"long","logicalType":"local-timestamp-micros"}`, time.UnixMilli(1700000000000).UTC(), cmpBytes, false},
		{"local-timestamp-nanos", `{"type":"long","logicalType":"local-timestamp-nanos"}`, time.Unix(1700000000, 5).UTC(), cmpBytes, false},

		// Bytes/fixed-backed logical types: codepoint-string JSON form, so
		// value comparison (escaping style differs, value must not).
		{"decimal-bytes", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, big.NewRat(12345, 100), cmpValue, false},
		{"decimal-fixed", `{"type":"fixed","name":"DF","size":8,"logicalType":"decimal","precision":10,"scale":2}`, big.NewRat(12345, 100), cmpValue, false},
		{"big-decimal", `{"type":"bytes","logicalType":"big-decimal"}`, big.NewRat(33, 100), cmpValue, false},
		{"duration", `{"type":"fixed","name":"DUR","size":12,"logicalType":"duration"}`, avro.Duration{Months: 1, Days: 2, Milliseconds: 3}, cmpValue, false},
		{"uuid-string", `{"type":"string","logicalType":"uuid"}`, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", cmpBytes, false},
		{"uuid-fixed", `{"type":"fixed","name":"UF","size":16,"logicalType":"uuid"}`, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", cmpValue, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := avro.Parse(c.schema)
			if err != nil {
				t.Fatalf("twmb Parse: %v", err)
			}
			twBin, err := s.Encode(c.value)
			if err != nil {
				t.Fatalf("twmb Encode: %v", err)
			}
			// TaggedUnions: the spec union form, which is what Java's
			// JsonEncoder emits. A no-op for non-union schemas.
			twJSON, err := s.EncodeJSON(c.value, avro.TaggedUnions())
			if err != nil {
				t.Fatalf("twmb EncodeJSON: %v", err)
			}

			ok, javaJSON, javaBin, errMsg := rt(t, c.schema, twBin)
			if !ok {
				t.Errorf("Java rejected the round-trip: %q (twmb binary % x)", errMsg, twBin)
				return
			}

			// Binary parity.
			if c.skipBin {
				// Entry order is unspecified: compare decoded-back values.
				var a, b any
				if _, err := s.Decode(twBin, &a); err != nil {
					t.Fatalf("decode own binary: %v", err)
				}
				if _, err := s.Decode(javaBin, &b); err != nil {
					t.Errorf("twmb cannot decode Java's binary re-encode: %v (java % x)", err, javaBin)
				} else if !reflect.DeepEqual(a, b) {
					t.Errorf("binary value divergence after Java re-encode:\n  twmb %#v\n  java %#v", a, b)
				}
			} else if string(javaBin) != string(twBin) {
				t.Errorf("binary wire divergence:\n  twmb % x\n  java % x", twBin, javaBin)
			}

			// JSON parity.
			switch c.cmpJSON {
			case cmpBytes:
				if string(javaJSON) != string(twJSON) {
					t.Errorf("JSON wire divergence (byte mode):\n  twmb %s (% x)\n  java %s (% x)", twJSON, twJSON, javaJSON, javaJSON)
				}
			case cmpValue:
				eq, err := jsonValueEqual(twJSON, javaJSON)
				if err != nil {
					t.Errorf("JSON wire not parseable for value comparison (likely a convention divergence):\n  twmb %s\n  java %s\n  err %v", twJSON, javaJSON, err)
				} else if !eq {
					t.Errorf("JSON value divergence:\n  twmb %s\n  java %s", twJSON, javaJSON)
				} else if string(javaJSON) != string(twJSON) {
					// Informational: same value, different text (expected for
					// number formatting / escaping style).
					t.Logf("JSON text differs, value equal:\n  twmb %s\n  java %s", twJSON, javaJSON)
				}
			}
		})
	}
}

// TestDifferentialJavaInvalidUTF8 cross-checks the JSON encode of
// invalid-UTF-8 string content against Java. twmb writes such bytes
// *verbatim* on the binary wire. On the JSON wire we coerce each invalid byte
// to U+FFFD, since a raw 0xff cannot appear in an RFC 8259 JSON string. That
// split is documented policy precisely because it matches Java byte-for-byte,
// which this verifies live. The per-case parity is asserted, so an avro-tools
// upgrade that changes Java's behavior fails CI and forces the rationale to
// be revisited rather than silently rotting.
func TestDifferentialJavaInvalidUTF8(t *testing.T) {
	rt := startSchemaOracle(t)

	cases := []struct {
		name   string
		schema string
		value  any
	}{
		{"valid-multibyte-control", `"string"`, "héllo"},
		{"invalid-mid-string", `"string"`, "A\xffB"},
		{"invalid-all-bytes", `"string"`, "\xff\xfe"},
		{"invalid-in-record-field", `{"type":"record","name":"R","fields":[{"name":"s","type":"string"}]}`, map[string]any{"s": "A\xffB"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			twBin, err := avro.MustParse(c.schema).Encode(c.value)
			if err != nil {
				t.Fatalf("twmb binary Encode: %v", err)
			}
			twJSON, err := avro.MustParse(c.schema).EncodeJSON(c.value)
			if err != nil {
				t.Fatalf("twmb EncodeJSON: %v", err)
			}

			ok, javaJSON, javaBin, errMsg := rt(t, c.schema, twBin)
			if !ok {
				t.Fatalf("Java rejected the round-trip (harness/decode failure): %q", errMsg)
			}

			jsonMatch := string(javaJSON) == string(twJSON)
			binMatch := string(javaBin) == string(twBin)
			t.Logf("twmb  JSON = %q (% x)", twJSON, twJSON)
			t.Logf("java  JSON = %q (% x)", javaJSON, javaJSON)
			t.Logf("JSON match (twmb U+FFFD coercion == Java): %v", jsonMatch)
			t.Logf("twmb  binary = % x", twBin)
			t.Logf("java  binary re-encode = % x", javaBin)
			t.Logf("binary verbatim match (Java binary preserves bytes): %v", binMatch)

			// A concise verdict line per case for the CI log scanner.
			t.Logf("VERDICT[%s]: jsonMatchesJava=%v javaBinaryVerbatim=%v", c.name, jsonMatch, binMatch)

			// Asserted, not just logged: the documented U+FFFD-on-JSON /
			// verbatim-on-binary policy rests on matching Java, so drift on
			// either wire must fail CI.
			if !jsonMatch {
				t.Errorf("twmb JSON diverged from Java: twmb=%q java=%q", twJSON, javaJSON)
			}
			if !binMatch {
				t.Errorf("Java binary re-encode not verbatim vs twmb: twmb=% x java=% x", twBin, javaBin)
			}
		})
	}
}

// TestDifferentialJavaWireLeniencies executes, against live Java, the
// specific Java wire-level behaviors this package's comments cite as the
// rationale for our own decode leniencies and test exclusions. Each cell
// hand-frames non-canonical (or degenerate) writer bytes and round-trips them
// through Java's decoder via the RT oracle. Each then asserts Java's
// *documented* verdict, so an avro-tools upgrade that changes the behavior
// fails CI instead of silently rotting the citation.
func TestDifferentialJavaWireLeniencies(t *testing.T) {
	rt := startSchemaOracle(t)

	t.Run("boolean non-1 byte decodes false", func(t *testing.T) {
		// BinaryDecoder.readBoolean is `return n == 1`, so byte 2 is false
		// (BinaryDecoder.java:150-151). twmb's Decode matches Java here.
		// fastavro diverges: any non-zero is True there. This cell pins the
		// Java anchor the leniency comment on Schema.Decode cites.
		ok, jsonOut, binOut, errMsg := rt(t, `"boolean"`, []byte{0x02})
		if !ok {
			t.Fatalf("Java rejected boolean wire byte 0x02: %q", errMsg)
		}
		if string(jsonOut) != "false" || string(binOut) != "\x00" {
			t.Errorf("Java boolean(0x02): JSON %q binary % x, want false / 00 — Java's readBoolean semantics changed, update deser.go's boolean-leniency comment",
				jsonOut, binOut)
		}
	})

	t.Run("overlong union index varint accepted", func(t *testing.T) {
		// BinaryDecoder.readIndex is a plain readInt() varint loop that
		// accepts non-minimal encodings within 5 bytes. 0x82 0x00 is the
		// two-byte overlong form of index 1. Java must decode the union's int
		// branch and re-encode the canonical single-byte form. Pins the
		// readNullUnionIndex parity comment in deser.go.
		ok, jsonOut, binOut, errMsg := rt(t, `["null","int"]`, []byte{0x82, 0x00, 0x0e})
		if !ok {
			t.Fatalf("Java rejected overlong union-index varint: %q", errMsg)
		}
		if string(binOut) != "\x02\x0e" {
			t.Errorf("Java re-encode of overlong-index union: % x, want 02 0e", binOut)
		}
		if string(jsonOut) != `{"int":7}` {
			t.Errorf("Java JSON for overlong-index union: %q, want {\"int\":7}", jsonOut)
		}
	})

	t.Run("empty record JsonEncoder emits zero bytes", func(t *testing.T) {
		// avro-tools 1.12.0's JsonEncoder emits *nothing* for a datum that is
		// entirely empty records. The grammar's implicit actions only run
		// when a terminal pulls advance(), and an empty record has no
		// terminals (JsonGrammarGenerator.java:83-90; the flush drain's
		// `while (pos > 1)` guard at Parser.java:108 never fires). twmb's
		// "{}" is the only valid JSON for an empty record and matches
		// fastavro. This cell pins Java's *current* zero-byte output, which
		// is why rec0 is excluded from TestDifferentialJavaJSONForm, and
		// flips when a Java release fixes the bug.
		ok, jsonOut, binOut, errMsg := rt(t, `{"type":"record","name":"E0","fields":[]}`, nil)
		if !ok {
			t.Fatalf("Java rejected the empty-record round-trip: %q", errMsg)
		}
		if len(binOut) != 0 {
			t.Errorf("Java binary re-encode of empty record: % x, want empty", binOut)
		}
		if len(jsonOut) != 0 {
			t.Errorf("Java JsonEncoder now emits %q for an empty record (historically zero bytes) — the upstream bug is fixed; re-include rec0 in TestDifferentialJavaJSONForm", jsonOut)
		}
	})
}

// ---------- attribute_placement_census_java_test.go ----------

// TestDifferentialJavaAcceptanceAttributePlacement drives a representative
// subset of the attribute x placement census through the Java oracle. Java
// accepts every cell. Stray attributes are either reserved-and-ignored via
// SCHEMA_RESERVED or kept as props, including the structural-key cells twmb
// rejects as structural-key exclusivity. For every cell twmb also accepts, Java's Parsing
// Canonical Form must equal twmb's. That proves both strip the stray
// identically, so the Rabin fingerprints agree. The "error" kind is excluded:
// standalone error schemas are a protocol-context type in Java's parser, and
// twmb's record-alias handling for it is pinned against the record twin
// locally.
func TestDifferentialJavaAcceptanceAttributePlacement(t *testing.T) {
	_, fpCanon := startMatrixJavaOracle(t)

	javaKinds := []string{"int", "string", "bytes", "fixed", "enum", "record", "array", "map"}
	for _, attr := range censusAttrs() {
		for _, kind := range javaKinds {
			verdict := attr.verdict(kind)
			if verdict == censusSkip {
				continue
			}
			t.Run("type/"+attr.key+"/"+kind, func(t *testing.T) {
				src := censusTypeSchema(kind, attr.key, attr.val(kind), true)
				ok, _, canon, errMsg := fpCanon(t, src)
				if !ok {
					t.Fatalf("Java rejected the placement (%s): %s", src, errMsg)
				}
				if verdict == censusReject63 {
					return // twmb rejects (documented #63 divergence); nothing to compare
				}
				s, err := avro.Parse(src)
				if err != nil {
					t.Fatalf("twmb Parse(%s): %v", src, err)
				}
				if got := string(s.Canonical()); got != canon {
					t.Errorf("PCF diverges from Java for %s:\n twmb: %s\n java: %s", src, got, canon)
				}
			})
		}
		if !attr.fieldLevel {
			continue
		}
		for _, kind := range []string{"int", "fixed", "record", "array", "union"} {
			t.Run("field/"+attr.key+"/"+kind, func(t *testing.T) {
				src := censusFieldSchema(kind, attr.key, attr.val(kind), true)
				ok, _, canon, errMsg := fpCanon(t, src)
				if !ok {
					t.Fatalf("Java rejected the field placement (%s): %s", src, errMsg)
				}
				s, err := avro.Parse(src)
				if err != nil {
					t.Fatalf("twmb Parse(%s): %v", src, err)
				}
				if got := string(s.Canonical()); got != canon {
					t.Errorf("PCF diverges from Java for %s:\n twmb: %s\n java: %s", src, got, canon)
				}
			})
		}
	}

	// The stray-namespace scoping composition: Java, like twmb and
	// fastavro, resolves a named type defined under a namespace-carrying
	// array in the *enclosing* scope.
	t.Run("namespace-scoping", func(t *testing.T) {
		const def = `{"name":"f","type":{"type":"array","namespace":"x","items":{"type":"record","name":"Inner","fields":[{"name":"i","type":"int"}]}}}`
		src := `{"type":"record","name":"top.R","fields":[` + def + `,{"name":"g","type":"top.Inner"}]}`
		ok, _, canon, errMsg := fpCanon(t, src)
		if !ok {
			t.Fatalf("Java: enclosing-scope reference should resolve: %s", errMsg)
		}
		if got := string(avro.MustParse(src).Canonical()); got != canon {
			t.Errorf("PCF diverges from Java:\n twmb: %s\n java: %s", got, canon)
		}
		if ok, _, _, _ := fpCanon(t, `{"type":"record","name":"top.R","fields":[`+def+`,{"name":"g","type":"x.Inner"}]}`); ok {
			t.Errorf("Java resolved x.Inner — namespace-on-array scopes there; recalibrate")
		}
	})

	// The stray-name-on-container divergence direction, documented: Java
	// ignores the reserved key where twmb keeps its walker-parity reject.
	t.Run("stray-name-on-array-java-accepts", func(t *testing.T) {
		src := `{"type":"array","items":"int","name":"strayName"}`
		if ok, _, _, errMsg := fpCanon(t, src); !ok {
			t.Errorf("Java now rejects a stray name on an array (%s) — recalibrate the documented divergence", errMsg)
		}
		if _, err := avro.Parse(src); err == nil {
			t.Errorf("twmb accepted a stray name on an array; the documented keep-strict posture changed")
		}
	})
}

// schemaOraclePipes launches the Java SchemaOracle subprocess and returns its
// stdin plus a buffered reader over its stdout. It skips when the JVM or the
// avro-tools jar is unavailable. Cleanup closes stdin and reaps the process.
func schemaOraclePipes(t *testing.T, skipMsg string) (io.WriteCloser, *bufio.Reader) {
	t.Helper()
	jar := os.Getenv("AVRO_TOOLS_JAR")
	if jar == "" {
		t.Skip(skipMsg)
	}
	javaBin := os.Getenv("AVRO_JAVA")
	if javaBin == "" {
		javaBin = "java"
	}
	if _, err := exec.LookPath(javaBin); err != nil {
		t.Skipf("java (%q) not found: %v", javaBin, err)
	}
	classDir := os.Getenv("AVRO_ORACLE_CLASSDIR")
	if classDir == "" {
		classDir = "testdata/oracle"
	}

	cmd := exec.Command(javaBin, "-cp", jar+string(os.PathListSeparator)+classDir, "SchemaOracle")
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	outPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start SchemaOracle: %v", err)
	}
	t.Cleanup(func() { _ = in.Close(); _ = cmd.Wait() })
	return in, bufio.NewReader(outPipe)
}
