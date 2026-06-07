package ocf

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// wideSchema builds a record whose JSON text exceeds n bytes (a wide, shallow
// record — many simple fields — which parses fine but produces large
// avro.schema header metadata).
func wideSchema(t *testing.T, minBytes int) (s *avro.Schema, js string, nFields int) {
	t.Helper()
	var b strings.Builder
	b.WriteString(`{"type":"record","name":"Wide","fields":[`)
	for b.Len() < minBytes {
		if nFields > 0 {
			b.WriteByte(',')
		}
		// A default lets Encode accept a sparse record without building all
		// nFields entries (and without the expensive s.Root() walk).
		fmt.Fprintf(&b, `{"name":"f%d","type":"long","default":0}`, nFields)
		nFields++
	}
	b.WriteString(`]}`)
	js = b.String()
	return avro.MustParse(js), js, nFields
}

// TestRegression_OCFLargeSchemaSelfReadable pins that an OCF file whose
// avro.schema header metadata exceeds the generic 1 MiB metadata cap is still
// readable. A wide record's JSON legitimately exceeds 1 MiB (and Java /
// fastavro read such files), but the reader's decodeMap capped every metadata
// value at ocfMetadataSafetyLimit (1 MiB) — so the writer produced a file
// NewReader (and NewAppendWriter, which re-reads the header) then rejected:
// self-incompatible, and unable to read Java's large-schema files. The
// self-describing avro.schema value now has a dedicated larger bound (its
// parse cost is independently bounded by the schema parser's own guards).
func TestRegression_OCFLargeSchemaSelfReadable(t *testing.T) {
	s, js, _ := wideSchema(t, ocfMetadataSafetyLimit+(64<<10)) // > 1 MiB
	if len(js) <= ocfMetadataSafetyLimit {
		t.Fatalf("test setup: schema JSON %d not over the cap %d", len(js), ocfMetadataSafetyLimit)
	}

	var buf bytes.Buffer
	w, err := NewWriter(&buf, s)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	// Every field has a default, so a sparse record encodes fine — this
	// exercises the HEADER (large avro.schema metadata), which is the point.
	if err := w.Encode(map[string]any{"f0": int64(1)}); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The writer's own output must be readable by the reader.
	if _, err := NewReader(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("SELF-INCOMPATIBILITY: wrote a %d-byte file with a %d-byte schema it cannot read: %v",
			buf.Len(), len(js), err)
	}
}

// TestRegression_OCFLargeUserMetadataProducerCompliance pins producer-side
// compliance for user metadata against the reader's per-entry cap. Arbitrary
// user metadata (WithMetadata) is opaque, unbounded-by-anything-else data, so
// the 1 MiB reader cap is a reasonable DoS limit — but the writer enforced no
// matching bound, so a >1 MiB WithMetadata value produced a file the reader
// rejected. The writer now refuses to write metadata the reader cannot read,
// with a clear error, rather than emitting a self-incompatible file.
func TestRegression_OCFLargeUserMetadataProducerCompliance(t *testing.T) {
	s := avro.MustParse(`"long"`)

	// At the cap: writes and reads back.
	atCap := bytes.Repeat([]byte{'x'}, ocfMetadataSafetyLimit)
	var ok bytes.Buffer
	w, err := NewWriter(&ok, s, WithMetadata(map[string][]byte{"m": atCap}))
	if err != nil {
		t.Fatalf("NewWriter at cap: %v", err)
	}
	if err := w.Encode(int64(1)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close at cap: %v", err)
	}
	if _, err := NewReader(bytes.NewReader(ok.Bytes())); err != nil {
		t.Fatalf("SELF-INCOMPATIBILITY: %d-byte user metadata at the cap unreadable: %v", len(atCap), err)
	}

	// Over the cap: the writer must refuse (producer compliance), surfacing a
	// clear error rather than a file the reader rejects.
	over := bytes.Repeat([]byte{'x'}, ocfMetadataSafetyLimit+100)
	var bad bytes.Buffer
	w2, err := NewWriter(&bad, s, WithMetadata(map[string][]byte{"m": over}))
	werr := err
	if werr == nil {
		werr = w2.Encode(int64(1))
		if werr == nil {
			werr = w2.Close()
		}
	}
	if werr == nil {
		t.Fatal("writer produced a file with >1 MiB user metadata the reader rejects; want a write-time error")
	}
	if !strings.Contains(werr.Error(), "metadata") {
		t.Fatalf("over-cap write rejected, but not with a metadata reason: %v", werr)
	}
}
