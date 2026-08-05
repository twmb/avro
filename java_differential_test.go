//go:build cisuite

// This file is behind the `cisuite` build tag so it ONLY runs in CI (where
// the JVM + avro-tools jar are provisioned): `go test -tags=cisuite`. A plain
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
	"strconv"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// TestDifferentialJavaFingerprint cross-checks twmb's Parsing Canonical Form
// and CRC-64-AVRO (Rabin) fingerprint against Apache Avro's reference Java
// implementation (org.apache.avro.SchemaNormalization), via the SchemaOracle
// subprocess. Unlike the static schema-tests.txt vectors, this exercises the
// cases those vectors omit — named types referenced multiple times and
// forward references — i.e. the canonical first-occurrence behavior (the F5
// class) against live Java.
//
// When Java rejects a schema twmb accepts (or vice versa), that parse-level
// divergence is logged (not failed): there is no Java fingerprint to compare,
// and the acceptance difference is surfaced for maintainer triage rather than
// asserted away.
func TestDifferentialJavaFingerprint(t *testing.T) {
	jar := os.Getenv("AVRO_TOOLS_JAR")
	if jar == "" {
		t.Skip("set AVRO_TOOLS_JAR to the avro-tools fat jar to run the Java differential")
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
		// this; the canonical first-occurrence transform must keep twmb's
		// output byte-identical to Java's here (definition IS first occurrence).
		{"define-then-ref-twice", `{"type":"record","name":"o","fields":[{"name":"a","type":{"type":"record","name":"i","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":"i"},{"name":"c","type":"i"}]}`},
		{"namespaced define-then-ref", `{"type":"record","name":"o","namespace":"com.x","fields":[{"name":"a","type":{"type":"record","name":"i","fields":[{"name":"x","type":"int"}]}},{"name":"b","type":"i"}]}`},
		{"recursive", `{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}`},
		// Forward references: Java may or may not accept these in a single
		// parse. If it rejects, the divergence is logged (twmb is more
		// lenient); if it accepts, twmb's first-occurrence canonical/
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
