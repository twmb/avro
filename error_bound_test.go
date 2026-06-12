package avro_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// Tier-2 error-message DoS bound (CORRECTNESS_PLAN.md DoS gap). Every
// fmt.Errorf("...%q...", x) that interpolates wire- or schema-controlled
// content x is a 1:1 amplification vector: a hostile N-byte input rejected
// into an N-byte error message floods logging pipelines, RPC error channels,
// metric labels, and traces. The trunc*ForError helpers exist to cap that
// echo, but the recurring regression is a call site that forgets to use them.
// The invariant pinned here: the error from a rejected hostile input is
// bounded by a small constant, INDEPENDENT of the input size. A call site
// that drops its trunc wrapper makes the message scale with the 1 MiB input
// and trips the cap.
func TestErrorMessageBounded(t *testing.T) {
	const hostileLen = 1 << 20 // 1 MiB
	// Legit messages from these paths are ~100 bytes (an ~80-char truncated
	// echo plus template text). The cap is far below the 1 MiB input so any
	// amplification regression trips it, and far above any legitimate message.
	const cap = 4096

	cases := []struct {
		name    string
		trigger func() error
	}{
		{
			// Schema parse echoing an unknown named-type reference.
			name: "parse unknown type reference",
			trigger: func() error {
				huge := strings.Repeat("A", hostileLen) // valid name chars, unknown type
				_, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"` + huge + `"}]}`)
				return err
			},
		},
		{
			// JSON decode echoing an unknown enum symbol.
			name: "json unknown enum symbol",
			trigger: func() error {
				s := avro.MustParse(`{"type":"enum","name":"E","symbols":["A","B"]}`)
				huge := strings.Repeat("Z", hostileLen)
				var out string
				return s.DecodeJSON([]byte(`"`+huge+`"`), &out)
			},
		},
		{
			// JSON decode echoing an out-of-range integer literal.
			name: "json integer overflow",
			trigger: func() error {
				s := avro.MustParse(`"int"`)
				huge := strings.Repeat("9", hostileLen)
				var out int32
				return s.DecodeJSON([]byte(huge), &out)
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.trigger()
			if err == nil {
				t.Fatalf("expected an error for the hostile input")
			}
			if n := len(err.Error()); n > cap {
				t.Errorf("error message is %d bytes for a %d-byte hostile input (cap %d): the echo is unbounded\nfirst 200: %.200s",
					n, hostileLen, cap, err.Error())
			}
		})
	}
}

// A deeply nested schema that the builder rejects (past the recursion
// limit) must not produce an unbounded error message: each nesting level
// wraps the inner error, so without a cap a 1500-deep array yields a
// ~15 KB message from a ~37 KB input — the same amplification the
// per-value trunc helpers prevent, accumulated over the wrap chain.
func TestRegression_DeepSchemaErrorBounded(t *testing.T) {
	for _, depth := range []int{1100, 1500, 3000} {
		schema := strings.Repeat(`{"type":"array","items":`, depth) + `"int"` + strings.Repeat(`}`, depth)
		_, err := avro.Parse(schema)
		if err == nil {
			t.Fatalf("depth %d: expected rejection", depth)
		}
		if len(err.Error()) > 4096 {
			t.Errorf("depth %d: error %d bytes exceeds 4096 bound", depth, len(err.Error()))
		}
	}
}

// Parse is O(n) in schema size, not O(depth*size): a valid deeply-nested
// schema (under the build's maxDepth) must parse in time linear in its
// bytes. The former json.Unmarshaler front-end re-scanned each node's
// full subtree (O(n^2)); a 999-deep array took ~0.4s, and Parse also fed
// Canonical() whose nested MarshalJSON re-copied each subtree (a second
// O(n^2)). Both are now single-pass.
func TestRegression_DeepValidSchemaParsesLinear(t *testing.T) {
	deep := strings.Repeat(`{"type":"array","items":`, 900) + `"int"` + strings.Repeat(`}`, 900)
	t0 := time.Now()
	s, err := avro.Parse(deep)
	if err != nil {
		t.Fatalf("parse valid deep schema: %v", err)
	}
	if d := time.Since(t0); d > 200*time.Millisecond {
		t.Errorf("valid 900-deep schema parsed in %v; want <200ms (O(n^2) regression?)", d)
	}
	// Canonical()/Fingerprint must also be linear (it is on the hot Parse
	// path for the SOE fingerprint).
	t1 := time.Now()
	_ = s.Canonical()
	if d := time.Since(t1); d > 200*time.Millisecond {
		t.Errorf("Canonical() of 900-deep schema took %v; want <200ms", d)
	}
}

// Canonical() must emit valid JSON (and a sound fingerprint) for a name
// containing a literal backslash, reachable via WithLaxNames. The former
// path HTML-escaped then bytes.ReplaceAll-un-escaped, which collapsed the
// \uXXXX target inside a \\uXXXX escape, producing invalid JSON.
func TestRegression_CanonicalBackslashNameValid(t *testing.T) {
	for _, name := range []string{`a&b`, `x<y`, `p q`, `back\\slash`} {
		schema := `{"type":"record","name":"` + jsonEscapeForTest(name) + `","fields":[]}`
		s, err := avro.Parse(schema, avro.WithLaxNames(nil))
		if err != nil {
			t.Fatalf("parse %q: %v", name, err)
		}
		c := s.Canonical()
		if !json.Valid(c) {
			t.Errorf("Canonical() for name %q is invalid JSON: %s", name, c)
		}
		// The PCF must round-trip-parse (registries re-parse canonical form).
		if _, err := avro.Parse(string(c), avro.WithLaxNames(nil)); err != nil {
			t.Errorf("Parse(Canonical()) for name %q: %v\ncanonical: %s", name, err, c)
		}
	}
}

func jsonEscapeForTest(s string) string {
	b, _ := json.Marshal(s)
	return string(b[1 : len(b)-1]) // strip surrounding quotes
}

// SchemaNode.Schema() (the Root().Schema() metadata round-trip) must be O(n)
// in schema size, not O(depth*subtree). toJSONWalk snapshotted every named
// type's full marshaled body for conflict detection; on a nested record chain
// each enclosing record re-marshaled everything below it (O(n^2)) even though
// the snapshot map is only ever read on a duplicate fullname. Parse() and
// Canonical() of the same schema are already linear (microseconds); this pins
// the metadata emitter to match. A 900-deep, ~318KB record chain that parses
// in ~12ms regressed to >1.3s through Root().Schema().
func TestRegression_RootSchemaEmitterLinearOnDeepNesting(t *testing.T) {
	const depth = 900
	var sb strings.Builder
	for i := 0; i < depth; i++ {
		fmt.Fprintf(&sb, `{"type":"record","name":"R%d","fields":[{"name":"f","type":`, i)
	}
	sb.WriteString(`{"type":"record","name":"Leaf","doc":"` + strings.Repeat("x", 256*1024) + `","fields":[{"name":"v","type":"int"}]}`)
	for i := 0; i < depth; i++ {
		sb.WriteString(`}]}`)
	}
	s, err := avro.Parse(sb.String())
	if err != nil {
		t.Fatalf("parse deep record chain: %v", err)
	}
	root := s.Root()
	t0 := time.Now()
	if _, err := root.Schema(); err != nil {
		t.Fatalf("Root().Schema(): %v", err)
	}
	if d := time.Since(t0); d > 500*time.Millisecond {
		t.Errorf("Root().Schema() of a %d-deep record chain took %v; want <500ms (O(depth*subtree) regression in toJSONWalk)", depth, d)
	}
}

// A schema field name has no length cap at parse (validName is pure grammar;
// WithLaxNames permits any non-empty string), so a registry/remote schema can
// carry a multi-megabyte name. Runtime per-datum errors that echo it amplify
// 1:N — one oversized error string on every Encode/Decode call, flooding logs,
// RPC error channels, and metric labels. The binary type-mismatch path routes
// the name through SemanticError.Field, which Error() render-truncates; these
// four paths (JSON missing-field on encode and decode, JSON alias collision,
// and the binary struct-mapping missing-field whose name rides in .Err rather
// than the truncated .Field) must apply the same bound at construction. The
// control below proves the asymmetry: the SemanticError.Field path is already
// bounded, so a failure here is a missed sibling of that bound, not a property
// of field names being safe to echo.
func TestRegression_FieldNameErrorEchoBounded(t *testing.T) {
	const hostileLen = 1 << 20 // 1 MiB
	const cap = 4096

	hugeNameSchema := func(t *testing.T) *avro.Schema {
		t.Helper()
		huge := strings.Repeat("A", hostileLen)
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"` + huge + `","type":"int"}]}`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		return s
	}
	assertBounded := func(t *testing.T, err error) {
		t.Helper()
		if err == nil {
			t.Fatal("expected an error")
		}
		if n := len(err.Error()); n > cap {
			t.Errorf("error message is %d bytes for a %d-byte hostile name (cap %d): echo unbounded\nfirst 160: %.160s",
				n, hostileLen, cap, err.Error())
		}
	}

	t.Run("json encode missing required field", func(t *testing.T) {
		s := hugeNameSchema(t)
		_, err := s.EncodeJSON(map[string]any{})
		assertBounded(t, err)
	})
	t.Run("json decode missing required field", func(t *testing.T) {
		s := hugeNameSchema(t)
		var out map[string]any
		assertBounded(t, s.DecodeJSON([]byte(`{}`), &out))
	})
	t.Run("binary encode struct mapping missing field", func(t *testing.T) {
		s := hugeNameSchema(t)
		type empty struct{}
		_, err := s.Encode(empty{})
		assertBounded(t, err)
	})
	t.Run("json decode alias collision echoes two wire keys", func(t *testing.T) {
		huge := strings.Repeat("B", hostileLen)
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"f","aliases":["` + huge + `"],"type":"int"}]}`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		var out map[string]any
		assertBounded(t, s.DecodeJSON([]byte(`{"f":1,"`+huge+`":2}`), &out))
	})

	// Control: the binary type-mismatch path routes the field name through
	// SemanticError.Field (render-truncated in errors.go). It is already
	// bounded — present so a future change that breaks the render truncation
	// is caught here, and to document that the four cases above are an
	// asymmetry with this path, not field names being inherently echo-safe.
	t.Run("control: binary type mismatch already bounded", func(t *testing.T) {
		huge := strings.Repeat("A", hostileLen)
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"` + huge + `","type":"int"}]}`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		_, err = s.Encode(map[string]any{huge: "not-an-int"})
		assertBounded(t, err)
	})
}
