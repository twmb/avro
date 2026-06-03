//go:build cisuite

// This file is behind the `cisuite` build tag so it ONLY runs in CI (where the
// JVM + avro-tools jar are provisioned): `go test -tags=cisuite`. A plain
// `go test ./...` never compiles or runs it. CI builds testdata/oracle/
// SchemaOracle.java against the avro-tools fat jar and sets AVRO_TOOLS_JAR;
// see .github/workflows/test.yml.

package avro_test

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
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
)

// startSchemaOracle launches the SchemaOracle subprocess (skipping when the
// JVM/jar is unavailable) and returns an rt function that drives its "RT"
// command: send (schema, avro binary), get back Java's JSON re-encode and
// binary re-encode of the same datum.
func startSchemaOracle(t *testing.T) func(t *testing.T, schema string, binary []byte) (ok bool, jsonOut, binOut []byte, errMsg string) {
	t.Helper()
	jar := os.Getenv("AVRO_TOOLS_JAR")
	if jar == "" {
		t.Skip("set AVRO_TOOLS_JAR to the avro-tools fat jar to run the Java value differential")
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
	out := bufio.NewReader(outPipe)
	t.Cleanup(func() { _ = in.Close(); _ = cmd.Wait() })

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

// jsonValueEqual reports whether two JSON documents encode the same value,
// ignoring non-semantic text differences: number formatting ("1" vs "1.0",
// exponent forms), \uXXXX escaping vs raw UTF-8 for the same code points
// (including hex case), and object key order. Numbers compare as float64 —
// adequate for the float/bytes/map cases routed here (precision-critical
// long cases use byte comparison instead).
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

// TestDifferentialJavaValueMatrix sweeps a (schema × value) corpus through the
// SchemaOracle RT command, asserting VALUE-level wire parity with the Java
// reference on BOTH formats:
//
//   - binary: Java binary-decodes twmb's Encode output and re-encodes it; the
//     re-encode must be byte-identical (both impls produce the canonical
//     single-block / varint forms), proving Java reads our binary AND agrees
//     on the bytes. Multi-entry maps are exempt from the byte comparison
//     (entry order is unspecified on both sides) and compare decoded-back.
//   - JSON: twmb's EncodeJSON (TaggedUnions — the spec form Java emits) must
//     match Java's JsonEncoder output. cmpJSON selects byte-identical
//     comparison (where JSON text is canonical: ints, strings, containers,
//     enum, tagged unions) or value-equal comparison (where equally-valid
//     texts differ: float formatting "1" vs "1.0", \u00XX escaping style for
//     the bytes/fixed codepoint string, U+2028 escaping, map key order).
//
// Logical-typed values ride as their RAW base-type wire values through Java's
// generic datum path (no Conversions registered), which is exactly what the
// Avro JSON encoding of a logical type is — the base type's encoding.
//
// Every case failure is reported (Errorf, not Fatalf) so one CI run yields
// the complete divergence list.
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
		// Primitives — canonical JSON text, byte-exact on both wires.
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
		{"string-unicode", `"string"`, "héllo 世界 🦆", cmpBytes, false},
		{"bytes-empty", `"bytes"`, []byte{}, cmpBytes, false},
		{"bytes-ascii", `"bytes"`, []byte("hi"), cmpBytes, false},

		// Floats/doubles — same value, possibly different (equally valid)
		// JSON number text between Go and Jackson.
		{"float-1.5", `"float"`, float32(1.5), cmpValue, false},
		{"float-zero", `"float"`, float32(0), cmpValue, false},
		{"double-one", `"double"`, float64(1), cmpValue, false},
		{"double-pi", `"double"`, 3.14159, cmpValue, false},
		{"double-huge", `"double"`, 1e300, cmpValue, false},
		// Non-finite floats: twmb's default emits the Java-convention quoted
		// strings ("NaN"/"Infinity"/"-Infinity"); this asserts the convention
		// claim against live Java rather than trusting the doc.
		{"float-nan", `"float"`, float32(math.NaN()), cmpValue, false},
		{"double-nan", `"double"`, math.NaN(), cmpValue, false},
		{"double-inf", `"double"`, math.Inf(1), cmpValue, false},
		{"double-neginf", `"double"`, math.Inf(-1), cmpValue, false},

		// bytes/fixed with non-printable content: codepoint-string JSON form;
		// twmb escapes all non-ASCII as \u00XX, Jackson emits raw UTF-8 for
		// the same code points (and uppercase hex when it does escape) — same
		// value, different text.
		{"bytes-binary", `"bytes"`, []byte{0x00, 0x1f, 0x80, 0xff}, cmpValue, false},
		{"fixed-ascii", `{"type":"fixed","name":"F4","size":4}`, []byte("abcd"), cmpBytes, false},
		{"fixed-binary", `{"type":"fixed","name":"F4b","size":4}`, []byte{0x00, 0xff, 0x10, 0x80}, cmpValue, false},
		// U+2028: twmb escapes it for JavaScript safety; Jackson does not.
		{"string-u2028", `"string"`, "a\u2028b", cmpValue, false},

		// Enum.
		{"enum", `{"type":"enum","name":"E","symbols":["A","B","C"]}`, "B", cmpBytes, false},

		// Unions — tagged (spec) form on the JSON side.
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
		// canonical text — byte-exact. Values are the enriched Go types twmb's
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

		// Bytes/fixed-backed logical types: codepoint-string JSON form —
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
			// JsonEncoder emits; a no-op for non-union schemas.
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

// TestDifferentialJavaInvalidUTF8 cross-checks, against the Apache Avro Java
// reference, the JSON encode of invalid-UTF-8 string content: twmb writes such
// bytes VERBATIM on the binary wire but coerces each invalid byte to U+FFFD on
// the JSON wire (appendJSONString, json_codec.go). A raw 0xff cannot appear in
// an RFC 8259 JSON string, so JSON cannot be byte-faithful; this split is
// DOCUMENTED POLICY (Schema.EncodeJSON doc; BUG_AUDIT.md §Known intentional
// divergences; pinned locally by
// TestRegression_InvalidUTF8StringBinaryVerbatimJSONCoercion) precisely
// BECAUSE it matches Java byte-for-byte — verified live by this test.
//
// The per-case parity is ASSERTED: if a future avro-tools upgrade changes
// Java's behavior, or a twmb encode change breaks the match, CI fails loudly
// and the documented rationale must be revisited rather than silently rotting.
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
