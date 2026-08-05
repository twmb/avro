package avro_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Hostile-SIZE rejection axis: megabyte-scale wrong values driven at every
// encode arm. The rejection itself is correctness; the axis asserts the two
// DoS postures around it — the reject is FAST (no superlinear parse work
// before the type check) and the error message is BOUNDED (no echoing the
// hostile input back; the trunc-helper contract).
// ---------------------------------------------------------------------------

func TestMatrix_HostileSizeRejects(t *testing.T) {
	bigStr := strings.Repeat("x", 1<<20)
	bigBytes := []byte(bigStr)
	bigNum := json.Number(strings.Repeat("9", 1<<20))
	bigKeyMap := map[string]any{bigStr: int32(1)}

	cases := []struct {
		label  string
		schema string
		bad    any
	}{
		{"string-into-int", `"int"`, bigStr},
		{"bytes-into-long", `"long"`, bigBytes},
		{"string-into-boolean", `"boolean"`, bigStr},
		{"hugenum-into-int", `"int"`, bigNum},
		{"hugenum-into-long", `"long"`, bigNum},
		{"hugenum-into-float", `"float"`, bigNum},
		{"hugenum-into-timestamp", `{"type":"long","logicalType":"timestamp-millis"}`, bigNum},
		{"string-into-fixed16", `{"type":"fixed","name":"HF","size":16}`, bigStr},
		{"bytes-into-fixed16", `{"type":"fixed","name":"HF","size":16}`, bigBytes},
		{"symbol-into-enum", `{"type":"enum","name":"HE","symbols":["A","B"]}`, bigStr},
		{"hugenum-into-decimal", `{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}`, bigNum},
		{"string-into-array", `{"type":"array","items":"int"}`, bigStr},
		{"hugekey-into-record", `{"type":"record","name":"HR","fields":[{"name":"a","type":"int"}]}`, bigKeyMap},
		{"string-into-nullunion", `["null","int"]`, bigStr},
		{"string-into-uuid-fixed", `{"type":"fixed","name":"HU","size":16,"logicalType":"uuid"}`, bigStr},
	}
	// The reject is locally ~µs; 250ms is generous CI headroom. Under -race,
	// instrumentation inflates the bounded reject past 250ms, so relax to a
	// ~3s ceiling there — a superlinear blowup before the type check is
	// multi-second and still trips it (see raceRelaxed).
	maxDur := raceRelaxed(250 * time.Millisecond)
	const maxErrLen = 2 << 10

	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			s := avro.MustParse(c.schema)

			start := time.Now()
			_, err := s.AppendEncode(nil, c.bad)
			d := time.Since(start)
			if err == nil {
				t.Fatalf("hostile value unexpectedly accepted (binary)")
			}
			if d > maxDur {
				t.Errorf("binary reject took %v (> %v): superlinear work before the type check", d, maxDur)
			}
			if n := len(err.Error()); n > maxErrLen {
				t.Errorf("binary reject error echoes hostile input: %d bytes", n)
			}

			start = time.Now()
			_, jerr := s.AppendEncodeJSON(nil, c.bad)
			d = time.Since(start)
			if jerr == nil {
				t.Fatalf("hostile value unexpectedly accepted (JSON)")
			}
			if d > maxDur {
				t.Errorf("JSON reject took %v (> %v)", d, maxDur)
			}
			if n := len(jerr.Error()); n > maxErrLen {
				t.Errorf("JSON reject error echoes hostile input: %d bytes", n)
			}
		})
	}
}

// Hostile-size DECODE-target rejects: a valid small wire decoded into a
// mismatched target must reject with a bounded message too (the wire side
// of the same posture; the wire itself is small, so only message size and
// promptness are interesting).
func TestMatrix_HostileSizeDecodeMessages(t *testing.T) {
	s := avro.MustParse(`"string"`)
	big := strings.Repeat("y", 1<<20)
	wire, err := s.AppendEncode(nil, big)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Decoding a 1 MiB string wire into an int target: the rejection must
	// not echo the megabyte of wire content.
	var i int32
	start := time.Now()
	_, derr := s.Decode(wire, &i)
	d := time.Since(start)
	if derr == nil {
		t.Fatal("string wire into int target unexpectedly accepted")
	}
	if bound := raceRelaxed(250 * time.Millisecond); d > bound {
		t.Errorf("decode reject took %v (>%v)", d, bound)
	}
	if n := len(derr.Error()); n > 2<<10 {
		t.Errorf("decode reject error echoes wire content: %d bytes", n)
	}
}
