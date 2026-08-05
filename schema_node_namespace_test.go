package avro

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

// fingerprintRoundTrip parses schema, runs it through the Root().Schema()
// metadata round-trip, and asserts the schema identity (fingerprint) is
// unchanged. The metadata API is the documented way to programmatically
// inspect and re-emit a schema; a fingerprint change means the re-emission
// silently described a different schema.
func fingerprintRoundTrip(t *testing.T, schema string) {
	t.Helper()
	s, err := Parse(schema)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	node := s.Root()
	rt, err := node.Schema()
	if err != nil {
		t.Fatalf("Root().Schema(): %v\ninput: %s", err, schema)
	}
	if want, got := s.Fingerprint(sha256.New()), rt.Fingerprint(sha256.New()); !bytes.Equal(want, got) {
		t.Errorf("fingerprint changed across Root().Schema():\n  orig canonical: %s\n  rt   canonical: %s",
			s.Canonical(), rt.Canonical())
	}
}

// A named child explicitly in the null namespace ("namespace":"") inside a
// namespaced parent is a DIFFERENT type than one inheriting the parent's
// namespace (spec: equality of names is defined on the fullname). The
// re-emission must escape inheritance the way Java's Schema.toString does
// (Name.writeName emits "namespace":"" for a null-namespace name inside a
// non-null enclosing namespace); dropping the escape silently moves the
// child into the parent's namespace.
func TestRegression_SchemaNodeNullNamespaceEscapeRoundTrip(t *testing.T) {
	cases := []struct{ name, schema string }{
		{"record child", `{"type":"record","name":"P","namespace":"x","fields":[{"name":"c","type":{"type":"record","name":"Child","namespace":"","fields":[{"name":"v","type":"int"}]}}]}`},
		{"enum child", `{"type":"record","name":"P","namespace":"x","fields":[{"name":"e","type":{"type":"enum","name":"E","namespace":"","symbols":["A"]}}]}`},
		{"fixed child", `{"type":"record","name":"P","namespace":"x","fields":[{"name":"f","type":{"type":"fixed","name":"F","namespace":"","size":4}}]}`},
		// inheritance-relying shapes must keep round-tripping too.
		{"inheriting child", `{"type":"record","name":"P","namespace":"x","fields":[{"name":"c","type":{"type":"record","name":"Child","fields":[{"name":"v","type":"int"}]}}]}`},
		{"explicit different ns child", `{"type":"record","name":"P","namespace":"x","fields":[{"name":"c","type":{"type":"record","name":"Child","namespace":"y","fields":[{"name":"v","type":"int"}]}}]}`},
		{"null-ns parent namespaced child", `{"type":"record","name":"P","fields":[{"name":"c","type":{"type":"record","name":"Child","namespace":"y","fields":[{"name":"v","type":"int"}]}}]}`},
		{"deep reinheritance", `{"type":"record","name":"P","namespace":"x","fields":[{"name":"c","type":{"type":"record","name":"Mid","namespace":"","fields":[{"name":"d","type":{"type":"record","name":"Leaf","fields":[{"name":"v","type":"int"}]}}]}}]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { fingerprintRoundTrip(t, c.schema) })
	}
}

// Two distinct named types may share a short name across namespaces
// (equality is on the fullname). The re-emission dedup must key on the
// fullname: keying on the short name either reports a false "conflicting
// definitions" error (different bodies) or emits a short name reference
// that re-binds to the wrong type (identical bodies).
func TestRegression_SchemaNodeSameShortNameDistinctNamespaces(t *testing.T) {
	cases := []struct{ name, schema string }{
		{"different bodies", `{"type":"record","name":"P","namespace":"x","fields":[
			{"name":"a","type":{"type":"fixed","name":"T","size":4}},
			{"name":"b","type":{"type":"fixed","name":"T","namespace":"y","size":8}}]}`},
		{"identical bodies", `{"type":"record","name":"P","namespace":"x","fields":[
			{"name":"a","type":{"type":"record","name":"Q","namespace":"y","fields":[
				{"name":"i","type":{"type":"fixed","name":"T","size":4}}]}},
			{"name":"b","type":{"type":"fixed","name":"T","size":4}}]}`},
		{"null-ns vs namespaced", `{"type":"record","name":"P","namespace":"x","fields":[
			{"name":"a","type":{"type":"fixed","name":"T","namespace":"","size":4}},
			{"name":"b","type":{"type":"fixed","name":"T","namespace":"y","size":8}}]}`},
		// genuine same-fullname reuse must still dedup into a reference.
		{"same fullname reused", `{"type":"record","name":"P","namespace":"x","fields":[
			{"name":"a","type":{"type":"fixed","name":"T","size":4}},
			{"name":"b","type":"T"}]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { fingerprintRoundTrip(t, c.schema) })
	}
}
