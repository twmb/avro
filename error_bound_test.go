package avro_test

import (
	"strings"
	"testing"

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
