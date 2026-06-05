//go:build cisuite

// Behind the `cisuite` build tag: runs only where the JVM + avro-tools jar
// are provisioned (see .github/workflows/test.yml), via
// `go test -tags=cisuite`. Plain `go test ./...` never compiles this file.

package avro_test

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// startMatrixJavaOracle launches SchemaOracle and returns two closures over
// one subprocess: rt (the "RT" command — Java binary-decodes our bytes and
// re-encodes them) and fpCanon (the bare-schema command — Java's
// parsingFingerprint64 + Parsing Canonical Form).
func startMatrixJavaOracle(t *testing.T) (
	rt func(t *testing.T, schema string, binary []byte) (ok bool, binOut []byte, errMsg string),
	fpCanon func(t *testing.T, schema string) (ok bool, fp int64, canon, errMsg string),
) {
	t.Helper()
	jar := os.Getenv("AVRO_TOOLS_JAR")
	if jar == "" {
		t.Skip("set AVRO_TOOLS_JAR to the avro-tools fat jar to run the Java matrix differential")
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

	// The oracle protocol is one request line per response line; schemas
	// composed by the matrix contain newlines, so compact them first.
	compact := func(t *testing.T, schema string) string {
		t.Helper()
		var buf bytes.Buffer
		if err := json.Compact(&buf, []byte(schema)); err != nil {
			t.Fatalf("compact schema: %v\n%s", err, schema)
		}
		return buf.String()
	}

	rt = func(t *testing.T, schema string, binary []byte) (bool, []byte, string) {
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
			return false, nil, msg
		}
		if len(parts) != 3 {
			t.Fatalf("malformed RT response: %q", line)
		}
		binOut, err := base64.StdEncoding.DecodeString(parts[2])
		if err != nil {
			t.Fatalf("decode RT binary: %v", err)
		}
		return true, binOut, ""
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

// javaMatrixCheck runs the two Java-oracle checks for one composed cell:
// Java's binary re-encode of twmb's wire must be byte-identical, and Java's
// canonical form + Rabin fingerprint must match twmb's.
func javaMatrixCheck(t *testing.T, rt func(*testing.T, string, []byte) (bool, []byte, string),
	fpCanon func(*testing.T, string) (bool, int64, string, string),
	schemaJSON string, vin any,
) {
	t.Helper()
	s, err := avro.Parse(schemaJSON)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	w1, err := s.AppendEncode(nil, vin)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	ok, binOut, errMsg := rt(t, schemaJSON, w1)
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
// through the Apache Avro Java reference: wire-layout and canonical-form
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

// TestDifferentialJavaMatrixRecursion covers the recursive shapes — Java is
// the ONE external oracle that accepts forward references (the twmb+Java
// extension fastavro rejects), so the fwd-ref shapes run here unskipped.
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

// TestDifferentialJavaAcceptance: schema-ACCEPTANCE parity with the Java
// reference over every composed cell and every structurally-broken mutant.
// Java enforces the full mutator set (missing fields/symbols/size/name,
// duplicates, nested unions, negative sizes, empty field names), so unlike
// the fastavro twin nothing is scoped out. The bare-schema oracle command
// doubles as the parse probe: OK means Java parsed it, ERR means rejected.
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
