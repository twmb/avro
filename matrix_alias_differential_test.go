package avro_test

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// TestDifferentialFastavroAliasResolution executes every cell of the alias
// census (TestMatrix_AliasResolutionCensus's spelling × writer-namespace ×
// kind × site axes) against a real fastavro process: a value is encoded
// with the writer schema and resolved-read with the reader schema
// (schemaless_reader with both schemas — fastavro's match_schemas is the
// matcher under test on their side).
//
// fastavro's verdict table differs from twmb's on exactly one row: the
// leading-dot alias spelling. fastavro compares alias strings as written
// (".Old" matches no writer fullname), while twmb applies Java's Name-
// constructor rule (Schema.java ~1455: lastDot split, empty space → null
// namespace), under which ".Old" is the null-namespace fullname "Old" —
// so twmb and Java accept the null-namespace writer that fastavro rejects.
// Every other row must agree exactly, both accept AND reject sides.
func TestDifferentialFastavroAliasResolution(t *testing.T) {
	o := startOracle(t)

	writerName := map[string]string{"samens": "n1.Old", "foreignns": "n2.Old", "nullns": "Old"}
	aliasSpelling := map[string]string{
		"bare":          "Old",
		"dottedown":     "n1.Old",
		"dottedforeign": "n2.Old",
		"leadingdot":    ".Old",
	}
	twmbAccept := map[string]map[string]bool{
		"bare":          {"samens": true, "foreignns": true, "nullns": true},
		"dottedown":     {"samens": true, "foreignns": false, "nullns": false},
		"dottedforeign": {"samens": false, "foreignns": true, "nullns": false},
		"leadingdot":    {"samens": false, "foreignns": false, "nullns": true},
	}
	fastavroAccept := map[string]map[string]bool{
		"bare":          {"samens": true, "foreignns": true, "nullns": true},
		"dottedown":     {"samens": true, "foreignns": false, "nullns": false},
		"dottedforeign": {"samens": false, "foreignns": true, "nullns": false},
		"leadingdot":    {"samens": false, "foreignns": false, "nullns": false}, // verbatim alias string: matches nothing
	}

	kindSchema := func(kind, name, aliases string) string {
		aliasAttr := ""
		if aliases != "" {
			aliasAttr = fmt.Sprintf(`,"aliases":[%q]`, aliases)
		}
		switch kind {
		case "record":
			return fmt.Sprintf(`{"type":"record","name":%q%s,"fields":[{"name":"a","type":"int"}]}`, name, aliasAttr)
		case "enum":
			return fmt.Sprintf(`{"type":"enum","name":%q%s,"symbols":["A","B"]}`, name, aliasAttr)
		case "fixed":
			return fmt.Sprintf(`{"type":"fixed","name":%q%s,"size":2}`, name, aliasAttr)
		}
		panic("unknown kind")
	}
	value := map[string]any{
		"record": map[string]any{"a": int32(7)},
		"enum":   "A",
		"fixed":  []byte{1, 2},
	}

	for spelling, alias := range aliasSpelling {
		for wns, wname := range writerName {
			for _, kind := range []string{"record", "enum", "fixed"} {
				for _, site := range []string{"top", "union"} {
					wantFast := fastavroAccept[spelling][wns]
					wantTwmb := twmbAccept[spelling][wns]
					name := fmt.Sprintf("%s/%s/%s/%s", spelling, wns, kind, site)
					t.Run(name, func(t *testing.T) {
						writerJSON := kindSchema(kind, wname, "")
						readerJSON := kindSchema(kind, "n1.New", alias)
						if site == "union" {
							readerJSON = `["boolean",` + readerJSON + `]`
						}

						writer := avro.MustParse(writerJSON)
						wire, err := writer.Encode(value[kind])
						if err != nil {
							t.Fatalf("twmb encode: %v", err)
						}

						resp := o.call(oracleJob{
							Op:     "readresolve",
							Schema: json.RawMessage(writerJSON),
							Reader: json.RawMessage(readerJSON),
							Hex:    hex.EncodeToString(wire),
						})
						fastGot := resp.OK
						if !resp.OK && strings.Contains(resp.Err, "not JSON serializable") {
							// A fixed decodes to Python bytes, which the
							// oracle's response json.dumps cannot represent.
							// schemaless_reader RETURNED — a resolution
							// reject raises SchemaResolutionError before any
							// dumps — so the resolution verdict is accept;
							// only the datum transport failed.
							fastGot = true
						}
						if fastGot != wantFast {
							t.Fatalf("fastavro accept=%v, want %v (err: %s)", fastGot, wantFast, resp.Err)
						}
						if !fastGot && !strings.Contains(resp.Err, "chema") {
							// Reject cells must reject on schema RESOLUTION
							// (SchemaResolutionError / "Schema mismatch"),
							// not on some value or transport error.
							t.Fatalf("fastavro rejected for a non-resolution reason: %s", resp.Err)
						}

						// Cross-check twmb's verdict in the same cell, so the
						// two tables cannot drift: they agree everywhere
						// except the documented leading-dot divergence.
						reader := avro.MustParse(readerJSON)
						twmbGot := avro.CheckCompatibility(writer, reader) == nil
						if twmbGot != wantTwmb {
							t.Fatalf("twmb accept=%v, want %v", twmbGot, wantTwmb)
						}
						if spelling == "leadingdot" && wns == "nullns" {
							if !twmbGot || fastGot {
								t.Fatalf("documented divergence inverted: twmb=%v fastavro=%v (want twmb accept per Java's Name-ctor rule, fastavro reject verbatim)", twmbGot, fastGot)
							}
						} else if twmbGot != fastGot {
							t.Fatalf("undocumented twmb/fastavro divergence: twmb=%v fastavro=%v", twmbGot, fastGot)
						}
					})
				}
			}
		}
	}
}
