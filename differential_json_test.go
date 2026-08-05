package avro_test

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// JSON-wire differential vs fastavro's json_writer / json_reader: executes
// the JSON-encoding behaviors this package documents against the reference
// implementations, instead of citing them. Three cell classes:
//
//   - PARITY cells: twmb's EncodeJSON text must equal fastavro's
//     json_writer text (bytes/fixed codepoint strings, tagged-union
//     envelopes keyed by fullname), and each engine must read the other's
//     text back to the same value.
//   - REJECT-PARITY cells: both engines must reject the same malformed or
//     unrepresentable inputs (bare value against a tagged-union reader,
//     JSON null against a union without a null branch, lowercase "nan",
//     trailing content).
//   - CALIBRATION cells: documented divergences pinned at fastavro's
//     OBSERVED verdict (1.12.2) so an upgrade that changes fastavro's
//     behavior flips the cell and forces a deliberate recalibration —
//     never a silently rotting claim: fastavro emits BARE NaN where twmb
//     emits the quoted Java form, and fastavro's json_reader returns
//     twmb's quoted "NaN" as a plain string (it applies no float
//     validation) where twmb parses NaN.
// ---------------------------------------------------------------------------

func TestDifferentialFastavroJSON(t *testing.T) {
	o := startOracle(t)

	// twmbJSON encodes one value with twmb, failing the test on error.
	twmbJSON := func(t *testing.T, schema string, v any, opts ...avro.Opt) string {
		t.Helper()
		s := avro.MustParse(schema)
		out, err := s.EncodeJSON(v, opts...)
		if err != nil {
			t.Fatalf("twmb EncodeJSON: %v", err)
		}
		return string(out)
	}

	// compact normalizes insignificant JSON whitespace (fastavro's
	// json.dump writes ": " where twmb writes ":") so parity compares
	// content, not formatting. Non-JSON text (bare NaN) passes through.
	compact := func(t *testing.T, s string) string {
		t.Helper()
		var buf strings.Builder
		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err != nil {
			return s
		}
		enc := json.NewEncoder(&buf)
		if err := enc.Encode(v); err != nil {
			return s
		}
		return strings.TrimRight(buf.String(), "\n")
	}

	t.Run("bytes codepoint string parity", func(t *testing.T) {
		// Avro JSON writes bytes as a codepoint-per-byte string (spec;
		// Java's JsonEncoder writeByteArray via ISO_8859_1). The 'A' byte
		// stays literal, 0x00 and 0xE9 become \u escapes in both engines.
		got := twmbJSON(t, `"bytes"`, []byte{0x00, 0xE9, 'A'})
		resp := o.call(oracleJob{Op: "jsonwrite", Schema: json.RawMessage(`"bytes"`),
			Value: json.RawMessage(`"AOlB"`), Kind: "bytes"}) // base64 of 00 e9 41
		if !resp.OK {
			t.Fatalf("fastavro json_writer: %s", resp.Err)
		}
		if resp.JSON != got {
			t.Errorf("bytes JSON text: twmb %s, fastavro %s", got, resp.JSON)
		}
		// twmb reads fastavro's text back to the identical bytes.
		var back []byte
		if err := avro.MustParse(`"bytes"`).DecodeJSON([]byte(resp.JSON), &back); err != nil {
			t.Fatalf("twmb DecodeJSON of fastavro text: %v", err)
		}
		if string(back) != "\x00\xe9A" {
			t.Errorf("round-trip bytes: %x", back)
		}
	})

	t.Run("fixed codepoint string parity", func(t *testing.T) {
		schema := `{"type":"fixed","name":"F","size":3}`
		got := twmbJSON(t, schema, []byte{0x01, 0xFF, 'B'})
		resp := o.call(oracleJob{Op: "jsonwrite", Schema: json.RawMessage(schema),
			Value: json.RawMessage(`"Af9C"`), Kind: "fixed"}) // base64 of 01 ff 42
		if !resp.OK {
			t.Fatalf("fastavro json_writer: %s", resp.Err)
		}
		if resp.JSON != got {
			t.Errorf("fixed JSON text: twmb %s, fastavro %s", got, resp.JSON)
		}
	})

	t.Run("tagged union envelope parity", func(t *testing.T) {
		schema := `["null","int"]`
		got := twmbJSON(t, schema, int32(7), avro.TaggedUnions())
		resp := o.call(oracleJob{Op: "jsonwrite", Schema: json.RawMessage(schema),
			Value: json.RawMessage(`7`)})
		if !resp.OK {
			t.Fatalf("fastavro json_writer: %s", resp.Err)
		}
		if compact(t, resp.JSON) != compact(t, got) {
			t.Errorf("union JSON text: twmb %s, fastavro %s", got, resp.JSON)
		}
		// fastavro reads twmb's tagged output.
		read := o.call(oracleJob{Op: "jsonread", Schema: json.RawMessage(schema), JSON: got})
		if !read.OK || len(read.Values) != 1 || fmt.Sprint(read.Values[0]) != "7" {
			t.Errorf("fastavro json_read of twmb tagged output: ok=%v values=%v err=%s",
				read.OK, read.Values, read.Err)
		}
	})

	t.Run("tagged union envelope keyed by fullname", func(t *testing.T) {
		schema := `["null",{"type":"record","name":"com.ex.User","fields":[{"name":"a","type":"int"}]}]`
		got := twmbJSON(t, schema, map[string]any{"a": int32(1)}, avro.TaggedUnions())
		resp := o.call(oracleJob{Op: "jsonwrite", Schema: json.RawMessage(schema),
			Value: json.RawMessage(`{"a":1}`)})
		if !resp.OK {
			t.Fatalf("fastavro json_writer: %s", resp.Err)
		}
		if compact(t, resp.JSON) != compact(t, got) {
			t.Errorf("named-branch envelope: twmb %s, fastavro %s", got, resp.JSON)
		}
		if !strings.Contains(got, `"com.ex.User"`) {
			t.Errorf("envelope key is not the fullname: %s", got)
		}
	})

	t.Run("bare union output is NOT readable by fastavro", func(t *testing.T) {
		// The documented interop divergence behind the TaggedUnions doc:
		// twmb's default bare-union output is rejected by the references'
		// JSON decoders. Executed here for fastavro (Java's JsonDecoder
		// readIndex throws "Expected start-union" for the same shape).
		schema := `["null","int"]`
		bare := twmbJSON(t, schema, int32(7)) // no TaggedUnions: "7"
		read := o.call(oracleJob{Op: "jsonread", Schema: json.RawMessage(schema), JSON: bare})
		if read.OK {
			t.Errorf("fastavro now READS twmb's bare union output %q (historically rejected) — revisit the TaggedUnions interop note", bare)
		}
	})

	t.Run("json null rejected without a null branch", func(t *testing.T) {
		schema := `["int","string"]`
		read := o.call(oracleJob{Op: "jsonread", Schema: json.RawMessage(schema), JSON: `null`})
		if read.OK {
			t.Errorf("fastavro accepted null against a no-null union")
		}
		var v any
		if err := avro.MustParse(schema).DecodeJSON([]byte(`null`), &v); err == nil {
			t.Errorf("twmb accepted null against a no-null union")
		}
	})

	t.Run("special float spelling calibration", func(t *testing.T) {
		// twmb (default) emits the quoted Java JsonEncoder form; fastavro
		// emits the bare Python json.dumps(allow_nan=True) token. A
		// documented divergence, pinned at both engines' observed output.
		got := twmbJSON(t, `"double"`, math.NaN())
		if got != `"NaN"` {
			t.Errorf("twmb NaN spelling: %s, want quoted \"NaN\"", got)
		}
		resp := o.call(oracleJob{Op: "jsonwrite", Schema: json.RawMessage(`"double"`),
			Value: json.RawMessage(`null`), Kind: "nan"})
		if !resp.OK {
			t.Fatalf("fastavro json_writer NaN: %s", resp.Err)
		}
		if resp.JSON != "NaN" {
			t.Errorf("fastavro NaN spelling: %q, want bare NaN (recalibrate the parseSpecialFloat docstring if fastavro changed)", resp.JSON)
		}
		// twmb reads fastavro's bare token (the lenient accept the bare-token
		// arm of decodeJSONFloat documents).
		var f float64
		if err := avro.MustParse(`"double"`).DecodeJSON([]byte(resp.JSON), &f); err != nil || !math.IsNaN(f) {
			t.Errorf("twmb DecodeJSON of fastavro bare NaN: %v %v", f, err)
		}
		// fastavro reads twmb's QUOTED form as a plain string — it applies
		// no float validation on JSON read (observed 1.12.2; the quoted
		// convention is Java's, not fastavro's). Calibration pin.
		read := o.call(oracleJob{Op: "jsonread", Schema: json.RawMessage(`"double"`), JSON: got})
		if !read.OK || len(read.Values) != 1 || fmt.Sprint(read.Values[0]) != "NaN" {
			t.Errorf("fastavro json_read of quoted NaN: ok=%v values=%v err=%s (recalibrate: fastavro may have added float validation)",
				read.OK, read.Values, read.Err)
		}
	})

	t.Run("lowercase nan rejected by both", func(t *testing.T) {
		read := o.call(oracleJob{Op: "jsonread", Schema: json.RawMessage(`"double"`), JSON: `nan`})
		if read.OK {
			t.Errorf("fastavro accepted lowercase nan: %v", read.Values)
		}
		var f float64
		if err := avro.MustParse(`"double"`).DecodeJSON([]byte(`nan`), &f); err == nil {
			t.Errorf("twmb accepted lowercase nan")
		}
	})

	t.Run("trailing content rejected by both", func(t *testing.T) {
		read := o.call(oracleJob{Op: "jsonread", Schema: json.RawMessage(`"int"`), JSON: `7 garbage`})
		if read.OK {
			t.Errorf("fastavro accepted trailing content: %v", read.Values)
		}
		var v int32
		if err := avro.MustParse(`"int"`).DecodeJSON([]byte(`7 garbage`), &v); err == nil {
			t.Errorf("twmb accepted trailing content")
		}
	})

	t.Run("empty-named branch tagged envelope parity", func(t *testing.T) {
		// A union branch whose short name is empty (lax names) tags by its
		// FULLNAME like any named branch. fastavro is the only reference
		// impl that parses the shape; both engines emit `{"ok.": ...}` and
		// each reads the other's text.
		acceptAll := func(string) error { return nil }
		schema := `["null",{"type":"enum","name":"","namespace":"ok","symbols":["A","B"]}]`
		s, err := avro.Parse(schema, avro.WithLaxNames(acceptAll))
		if err != nil {
			t.Fatalf("twmb parse: %v", err)
		}
		got, err := s.EncodeJSON("A", avro.TaggedUnions())
		if err != nil {
			t.Fatalf("twmb EncodeJSON: %v", err)
		}
		resp := o.call(oracleJob{Op: "jsonwrite", Schema: json.RawMessage(schema),
			Value: json.RawMessage(`"A"`)})
		if !resp.OK {
			t.Fatalf("fastavro json_writer: %s", resp.Err)
		}
		if compact(t, resp.JSON) != compact(t, string(got)) {
			t.Errorf("empty-named union tag: twmb %s, fastavro %s", got, resp.JSON)
		}
		read := o.call(oracleJob{Op: "jsonread", Schema: json.RawMessage(schema), JSON: string(got)})
		if !read.OK || len(read.Values) != 1 || fmt.Sprint(read.Values[0]) != "A" {
			t.Errorf("fastavro json_read of twmb tag: ok=%v values=%v err=%s", read.OK, read.Values, read.Err)
		}
		var back any
		if err := s.DecodeJSON([]byte(resp.JSON), &back); err != nil || back != "A" {
			t.Errorf("twmb DecodeJSON of fastavro tag: %v %v", back, err)
		}

		// Calibration: the BARE empty-name class ("" fullname). twmb emits
		// and round-trips `{"":"A"}`; fastavro's json_writer cannot produce
		// the envelope (observed 1.12.2: "No key was set" — the falsy
		// fullname never becomes the key) while its json_reader accepts the
		// "" key, so twmb's emission stays fastavro-readable. An upgrade
		// that makes the write succeed flips this pin — recalibrate.
		bare := `["null",{"type":"enum","name":"","symbols":["A","B"]}]`
		bs, err := avro.Parse(bare, avro.WithLaxNames(acceptAll))
		if err != nil {
			t.Fatalf("twmb parse bare: %v", err)
		}
		bgot, err := bs.EncodeJSON("A", avro.TaggedUnions())
		if err != nil {
			t.Fatalf("twmb EncodeJSON bare: %v", err)
		}
		if string(bgot) != `{"":"A"}` {
			t.Errorf("twmb bare tag: %s, want {\"\":\"A\"}", bgot)
		}
		bwrite := o.call(oracleJob{Op: "jsonwrite", Schema: json.RawMessage(bare),
			Value: json.RawMessage(`"A"`)})
		if bwrite.OK {
			t.Errorf("fastavro json_writer wrote the bare empty-name envelope %q (recalibrate: 1.12.2 errored)", bwrite.JSON)
		}
		bread := o.call(oracleJob{Op: "jsonread", Schema: json.RawMessage(bare), JSON: string(bgot)})
		if !bread.OK || len(bread.Values) != 1 || fmt.Sprint(bread.Values[0]) != "A" {
			t.Errorf("fastavro json_read of twmb bare tag: ok=%v values=%v err=%s", bread.OK, bread.Values, bread.Err)
		}
	})
}
