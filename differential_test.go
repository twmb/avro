package avro_test

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// Tier-1 differential testing against fastavro (an independent reference
// implementation). Everything is compared as Avro bytes, so there is no
// fragile cross-language value coercion:
//
//   - encode parity:   twmb.Encode(v) bytes == fastavro encode(v) bytes
//                      (skipped for map-containing schemas: map entry order
//                       is unspecified, so bytes legitimately differ)
//   - fastavro reads twmb: fastavro decodes twmb's bytes without error
//   - twmb reads fastavro:  twmb.Decode succeeds on fastavro's bytes
//
// This retires the "twmb disagrees with the reference" commit class for the
// covered schema shapes. The test SKIPS (does not fail) when fastavro is not
// installed, so `go test ./...` stays green without the toolchain; CI/local
// runs set AVRO_FASTAVRO_PYTHON to a python with fastavro. See
// CORRECTNESS_PLAN.md §T1b/§T1c and testdata/oracle/README.md.
//
// Binary- and logical-typed values (bytes, fixed, decimal, uuid, timestamp)
// are covered by TestDifferentialFastavroBinaryLogical in
// differential_logical_test.go, which carries them to the oracle via the
// Kind-tagged transport. Still NOT covered: ambiguous multi-numeric unions
// (encode branch-selection can legitimately differ by impl).

type oracleJob struct {
	Op     string          `json:"op"`
	Schema json.RawMessage `json:"schema"`
	Value  json.RawMessage `json:"value,omitempty"`
	// Kind tags how the oracle must reconstruct Value into a native Python
	// type before fastavro encodes it: "" passes the JSON value through, while
	// "bytes"/"fixed" base64-decode it, "decimal" builds a decimal.Decimal,
	// and "timestamp-millis"/"timestamp-micros" build a UTC datetime. Lets the
	// differential carry binary- and logical-typed values JSON cannot.
	Kind string `json:"kind,omitempty"`
	// No omitempty: a zero-byte encoding (e.g. the "null" type) has an empty
	// hex string that must still be sent, or the oracle sees no "hex" key.
	Hex string `json:"hex"`
}

type oracleResp struct {
	OK    bool   `json:"ok"`
	Hex   string `json:"hex"`
	Err   string `json:"err"`
	Fatal string `json:"fatal"`
}

type oracle struct {
	cmd *exec.Cmd
	in  io.WriteCloser
	out *bufio.Reader
	t   *testing.T
}

func oraclePython() string {
	if p := os.Getenv("AVRO_FASTAVRO_PYTHON"); p != "" {
		return p
	}
	return "python3"
}

// startOracle launches the fastavro oracle subprocess, or skips the test when
// python / fastavro is unavailable.
func startOracle(t *testing.T) *oracle {
	py := oraclePython()
	if _, err := exec.LookPath(py); err != nil {
		t.Skipf("python interpreter %q not found; set AVRO_FASTAVRO_PYTHON (skip differential)", py)
	}
	if err := exec.Command(py, "-c", "import fastavro").Run(); err != nil {
		t.Skipf("fastavro not importable via %q (%v); `pip install fastavro` to enable the differential", py, err)
	}
	cmd := exec.Command(py, "testdata/oracle/fastavro_oracle.py")
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
		t.Fatalf("start oracle: %v", err)
	}
	o := &oracle{cmd: cmd, in: in, out: bufio.NewReader(outPipe), t: t}
	t.Cleanup(func() { _ = o.in.Close(); _ = o.cmd.Wait() })
	return o
}

func (o *oracle) call(job oracleJob) oracleResp {
	o.t.Helper()
	b, err := json.Marshal(job)
	if err != nil {
		o.t.Fatalf("marshal job: %v", err)
	}
	if _, err := o.in.Write(append(b, '\n')); err != nil {
		o.t.Fatalf("write job: %v", err)
	}
	line, err := o.out.ReadBytes('\n')
	if err != nil {
		o.t.Fatalf("read oracle response: %v", err)
	}
	var resp oracleResp
	if err := json.Unmarshal(line, &resp); err != nil {
		o.t.Fatalf("unmarshal oracle response %q: %v", strings.TrimSpace(string(line)), err)
	}
	if resp.Fatal != "" {
		o.t.Skipf("oracle fatal: %s", resp.Fatal)
	}
	return resp
}

type diffSeed struct {
	name   string
	schema string
	value  any  // typed Go value (precise; not routed through json.Unmarshal-into-any)
	hasMap bool // map entry order is unspecified → skip byte-parity, keep readability checks
}

func diffSeeds() []diffSeed {
	enum := `{"type":"enum","name":"E","symbols":["A","B","C"]}`
	rec := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`
	return []diffSeed{
		{"null", `"null"`, nil, false},
		{"bool true", `"boolean"`, true, false},
		{"bool false", `"boolean"`, false, false},
		{"int 0", `"int"`, int32(0), false},
		{"int 42", `"int"`, int32(42), false},
		{"int min", `"int"`, int32(math.MinInt32), false},
		{"int max", `"int"`, int32(math.MaxInt32), false},
		{"long 0", `"long"`, int64(0), false},
		{"long 2^53+1", `"long"`, int64(9007199254740993), false}, // exact (typed int64, not float64)
		{"long max", `"long"`, int64(math.MaxInt64), false},
		{"long min", `"long"`, int64(math.MinInt64), false},
		{"float 1.5", `"float"`, float32(1.5), false},
		{"float -3.25", `"float"`, float32(-3.25), false},
		{"double 1.5", `"double"`, float64(1.5), false},
		{"double 1e308", `"double"`, float64(1e308), false},
		{"string empty", `"string"`, "", false},
		{"string unicode", `"string"`, "café 日本 🎉", false},
		{"enum B", enum, "B", false},
		{"array int", `{"type":"array","items":"int"}`, []int32{1, 2, 3, -7}, false},
		{"array empty", `{"type":"array","items":"long"}`, []int64{}, false},
		{"array string", `{"type":"array","items":"string"}`, []string{"a", "b", ""}, false},
		{"record", rec, map[string]any{"a": int32(7), "b": "x"}, false},
		{"union null/int -> int", `["null","int"]`, int32(5), false},
		{"union null/int -> null", `["null","int"]`, nil, false},
		{"union null/string -> string", `["null","string"]`, "s", false},
		{"map int (decode-only)", `{"type":"map","values":"int"}`, map[string]any{"k": int32(1), "j": int32(2)}, true},
	}
}

func TestDifferentialFastavro(t *testing.T) {
	o := startOracle(t)
	var encParity, faRead, twmbRead int
	for _, sd := range diffSeeds() {
		t.Run(sd.name, func(t *testing.T) {
			s, err := avro.Parse(sd.schema)
			if err != nil {
				t.Fatalf("twmb Parse: %v", err)
			}
			valJSON, err := json.Marshal(sd.value)
			if err != nil {
				t.Fatalf("marshal seed value: %v", err)
			}

			// twmb encode.
			bTwmb, err := s.Encode(sd.value)
			if err != nil {
				t.Fatalf("twmb Encode(%#v): %v", sd.value, err)
			}

			// fastavro encode.
			enc := o.call(oracleJob{Op: "encode", Schema: json.RawMessage(sd.schema), Value: valJSON})
			if !enc.OK {
				t.Fatalf("fastavro encode failed: %s", enc.Err)
			}

			// (1) encode byte parity (non-map).
			if !sd.hasMap {
				if got := hex.EncodeToString(bTwmb); got != enc.Hex {
					t.Errorf("encode byte mismatch vs fastavro:\n twmb     %s\n fastavro %s", got, enc.Hex)
				} else {
					encParity++
				}
			}

			// (2) fastavro reads twmb's bytes.
			if d := o.call(oracleJob{Op: "decode", Schema: json.RawMessage(sd.schema), Hex: hex.EncodeToString(bTwmb)}); !d.OK {
				t.Errorf("fastavro cannot decode twmb's bytes: %s", d.Err)
			} else {
				faRead++
			}

			// (3) twmb reads fastavro's bytes.
			faBytes, err := hex.DecodeString(enc.Hex)
			if err != nil {
				t.Fatalf("decode fastavro hex: %v", err)
			}
			var out any
			if _, err := s.Decode(faBytes, &out); err != nil {
				t.Errorf("twmb cannot decode fastavro's bytes: %v", err)
			} else {
				twmbRead++
			}
		})
	}
	t.Logf("fastavro differential: %d encode-parity, %d fastavro-reads-twmb, %d twmb-reads-fastavro", encParity, faRead, twmbRead)
}
