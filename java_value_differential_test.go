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
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// TestDifferentialJavaInvalidUTF8 answers, against the Apache Avro Java
// reference, the open design question behind the JSON encode of invalid-UTF-8
// string content (audit U2): twmb writes such bytes VERBATIM on the binary wire
// but coerces each invalid byte to U+FFFD on the JSON wire (appendJSONString,
// json_codec.go). A raw 0xff cannot appear in an RFC 8259 JSON string, so JSON
// cannot be byte-faithful; the question is whether twmb's lossy-on-JSON /
// verbatim-on-binary split MATCHES Java (which would justify documenting it
// rather than changing behavior).
//
// Method: twmb binary-encodes the value (verbatim), hands the bytes to Java via
// the SchemaOracle "RT" command — Java binary-decodes to a datum, then
// re-encodes to BOTH JSON (JsonEncoder) and binary (BinaryEncoder). We then
// compare Java's JSON to twmb's EncodeJSON, and Java's binary re-encode to
// twmb's binary, surfacing exactly what Java does on each wire.
//
// Outcomes are LOGGED for maintainer triage (this is a "what does Java do"
// probe, not a fixed contract); the only hard assertion is that Java accepts
// the round-trip at all (a harness failure, not a behavior finding).
func TestDifferentialJavaInvalidUTF8(t *testing.T) {
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

	// rt sends an "RT" round-trip request and returns Java's JSON re-encode and
	// binary re-encode (or an error message).
	rt := func(t *testing.T, schema string, binary []byte) (ok bool, jsonOut, binOut []byte, errMsg string) {
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
		})
	}
}
