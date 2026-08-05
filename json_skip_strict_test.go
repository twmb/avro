package avro_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// TestRegression_JSONSkipUnknownFieldRejectsMalformed pins that DecodeJSON
// validates malformed JSON in UNKNOWN (skipped) record fields, matching its
// own value path, Java, fastavro, and encoding/json. The skip path
// (skipValue/skipCompound) was a SECOND hand-rolled parser that delimited but
// did not validate — the framework's "a hand-rolled parser that replaced a
// stdlib parser silently dropped the stdlib's rejections" class: the number
// arm accepted 1.2.3/1e/5., the string arm skipped escapes blindly so \q
// passed, and skipCompound counted only bracket depth so [}]/{"a" 1}/[1,2,]
// balanced. The same bytes in a KNOWN field reject.
func TestRegression_JSONSkipUnknownFieldRejectsMalformed(t *testing.T) {
	reader := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"known","type":"long"}]}`)

	malformed := []struct{ name, frag string }{
		{"invalid_escape", `"\q"`},
		{"hex_escape", `"\x41"`},
		{"double_dot_number", `1.2.3`},
		{"bare_exponent", `1e`},
		{"trailing_dot", `5.`},
		{"bracket_mismatch_array", `[}]`},
		{"bracket_mismatch_object", `{]}`},
		{"missing_commas_array", `[1 2 3]`},
		{"missing_colon_object", `{"a" 1}`},
		{"trailing_comma_array", `[1,2,]`},
		{"trailing_comma_object", `{"a":1,}`},
		{"leading_comma", `[,1]`},
		{"unquoted_key", `{a:1}`},
		{"double_colon", `{"a"::1}`},
	}

	for _, c := range malformed {
		t.Run(c.name, func(t *testing.T) {
			if json.Valid([]byte(c.frag)) {
				t.Fatalf("test bug: %s is valid JSON", c.frag)
			}
			doc := fmt.Sprintf(`{"known":42,"x":%s}`, c.frag)
			var out map[string]any
			if err := reader.DecodeJSON([]byte(doc), &out); err == nil {
				t.Errorf("skip SILENTLY ACCEPTED malformed JSON %s -> out=%v", c.frag, out)
			}
		})
	}
}

// TestRegression_JSONSkipUnknownFieldAcceptsValid is the control: well-formed
// JSON in skipped fields must STILL skip cleanly (the strict validator must
// not reject valid input), including nesting, escapes, and whitespace.
func TestRegression_JSONSkipUnknownFieldAcceptsValid(t *testing.T) {
	reader := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"known","type":"long"}]}`)
	valid := []string{
		`"plain"`, `"with \"escapes\" and é"`, `42`, `-3.14e10`, `0`, `0.5`,
		`true`, `false`, `null`,
		`[]`, `{}`, `[1,2,3]`, `{"a":1,"b":[2,3]}`,
		`{ "a" : 1 , "b" : [ 2 , { "c" : "d" } ] }`,
		`[[[[]]]]`, `{"nested":{"deep":{"v":1}}}`,
	}
	for _, v := range valid {
		t.Run(v, func(t *testing.T) {
			doc := fmt.Sprintf(`{"x":%s,"known":42}`, v)
			var out map[string]any
			if err := reader.DecodeJSON([]byte(doc), &out); err != nil {
				t.Errorf("skip REJECTED valid JSON %s: %v", v, err)
			} else if out["known"] != int64(42) {
				t.Errorf("known field lost after skipping %s: %v", v, out)
			}
		})
	}
}

// TestRegression_JSONSkipDepthBounded confirms the strict (now recursive)
// skip validator keeps the old iterative skipCompound's DoS resistance: a
// pathologically deep skipped value errors rather than overflowing the stack.
func TestRegression_JSONSkipDepthBounded(t *testing.T) {
	reader := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"known","type":"long"}]}`)
	deep := strings.Repeat("[", 100000) + strings.Repeat("]", 100000)
	doc := fmt.Sprintf(`{"known":42,"x":%s}`, deep)
	var out map[string]any
	if err := reader.DecodeJSON([]byte(doc), &out); err == nil {
		t.Fatal("deeply-nested skipped value accepted; expected a depth-limit error")
	}
}
