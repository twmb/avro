// Vectors in testdata/avro-schema-tests.txt are vendored from Apache Avro
// (apache/avro), Apache License 2.0: https://www.apache.org/licenses/LICENSE-2.0

package avro_test

import (
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// schemaTestVec is one case parsed from the Apache schema-tests.txt oracle.
type schemaTestVec struct {
	input       string
	canonical   string
	fingerprint string // empty when the case has no fingerprint line
}

// parseApacheSchemaTests parses the official heredoc-style format:
//
//	// NNN
//	<<INPUT <schema json>            (single line) OR
//	<<INPUT
//	<multi-line schema json>
//	INPUT                            (bare terminator)
//	<<canonical <parsing-canonical-form json>
//	<<fingerprint <signed int64 rabin>   (optional)
func parseApacheSchemaTests(raw string) []schemaTestVec {
	lines := strings.Split(raw, "\n")
	var vecs []schemaTestVec
	var cur *schemaTestVec
	flush := func() {
		if cur != nil && cur.input != "" {
			vecs = append(vecs, *cur)
		}
		cur = nil
	}
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		switch {
		case strings.HasPrefix(line, "<<INPUT"):
			flush()
			rest := strings.TrimSpace(strings.TrimPrefix(line, "<<INPUT"))
			if rest != "" {
				cur = &schemaTestVec{input: rest}
				break
			}
			// Heredoc: accumulate following lines until a bare "INPUT".
			var b strings.Builder
			for i++; i < len(lines) && strings.TrimSpace(lines[i]) != "INPUT"; i++ {
				b.WriteString(lines[i])
				b.WriteByte(' ')
			}
			cur = &schemaTestVec{input: strings.TrimSpace(b.String())}
		case strings.HasPrefix(line, "<<canonical ") && cur != nil:
			cur.canonical = strings.TrimPrefix(line, "<<canonical ")
		case strings.HasPrefix(line, "<<fingerprint ") && cur != nil:
			cur.fingerprint = strings.TrimSpace(strings.TrimPrefix(line, "<<fingerprint "))
		}
	}
	flush()
	return vecs
}

// schemaTestKnownDivergences maps an INPUT (verbatim from the vector file) to
// the reason twmb intentionally does NOT match the Apache oracle. Each entry
// is verified to STILL diverge (Parse must still fail / differ); a stale entry
// — twmb starting to agree with the oracle — fails the test so it gets removed
// and the case re-enabled. Keep this list short and every entry justified.
var schemaTestKnownDivergences = map[string]string{
	// An empty union has no branches: it can encode no value and a decode
	// reads a branch index that is always out of range. Apache Avro parses
	// it (canonical "[]"); twmb rejects it at parse time, consistent with its
	// documented eager-fail stance. Surfaced by this oracle; left as a
	// maintainer policy decision (see CORRECTNESS_PLAN.md counterexamples).
	"[  ]": "twmb rejects the empty union (encodes/decodes nothing); Apache parses to canonical []",
}

// TestApacheSchemaTestsVectors runs the ENTIRE official Apache Avro
// schema-tests.txt cross-implementation oracle (vendored at
// testdata/avro-schema-tests.txt; carries its own Apache-2.0 header). Each
// case gives an INPUT schema, its expected Parsing Canonical Form, and — for
// most — the expected CRC-64-AVRO (Rabin) fingerprint as a signed int64.
// These values are validated by the Java reference implementation, so they
// are a real external oracle, not the author's belief.
//
// This is the Tier-1 canonical/fingerprint differential: a future
// canonical-form or fingerprint divergence (the F5 class) fails here
// automatically instead of waiting for an audit. See CORRECTNESS_PLAN.md §T1a.
func TestApacheSchemaTestsVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/avro-schema-tests.txt")
	if err != nil {
		t.Fatalf("read vendored vectors: %v", err)
	}
	vecs := parseApacheSchemaTests(string(raw))
	if len(vecs) < 30 {
		t.Fatalf("parsed only %d vectors; expected ~35 — parser or file drift", len(vecs))
	}

	var canonChecked, fpChecked, diverged int
	for _, v := range vecs {
		if reason, known := schemaTestKnownDivergences[v.input]; known {
			// Verify the documented divergence still holds (Parse fails or
			// canonical differs); a now-agreeing case means a stale allowlist.
			if s, err := avro.Parse(v.input); err == nil && string(s.Canonical()) == v.canonical {
				t.Errorf("stale known-divergence: twmb now matches the oracle for %q — remove the allowlist entry", v.input)
			} else {
				diverged++
				t.Logf("documented divergence for %q: %s", v.input, reason)
			}
			continue
		}

		s, err := avro.Parse(v.input)
		if err != nil {
			t.Errorf("Parse(%s): %v", v.input, err)
			continue
		}
		if v.canonical != "" {
			if got := string(s.Canonical()); got != v.canonical {
				t.Errorf("Canonical(%s)\n got  %s\n want %s", v.input, got, v.canonical)
			} else {
				canonChecked++
			}
		}
		if v.fingerprint != "" {
			want, perr := strconv.ParseInt(v.fingerprint, 10, 64)
			if perr != nil {
				t.Errorf("bad fingerprint vector %q: %v", v.fingerprint, perr)
				continue
			}
			h := avro.NewRabin()
			h.Write(s.Canonical())
			if got := int64(h.Sum64()); got != want {
				t.Errorf("Rabin fingerprint for %s: got %d want %d", v.input, got, want)
			} else {
				fpChecked++
			}
		}
	}
	t.Logf("Apache oracle: %d canonical forms, %d fingerprints verified, %d documented divergences", canonChecked, fpChecked, diverged)
}
