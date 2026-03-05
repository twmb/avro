package conformance

import (
	"testing"

	"github.com/twmb/avro"
)

// -----------------------------------------------------------------------
// Compatibility Matrix
// Spec: "Schema Resolution" — static compatibility checks before data
// is exchanged, verifying that a reader can process any writer output.
//   - Field addition (with/without default), field removal
//   - Type promotions (8 valid + invalid rejected)
//   - Enum symbol changes (added, removed with/without default)
//   - Union branch additions
//   - Named type matching (record/enum/fixed name, including unqualified)
//   - Recursive compatibility through arrays and maps
// https://avro.apache.org/docs/1.12.0/specification/#schema-resolution
// -----------------------------------------------------------------------

func TestCompatSameSchema(t *testing.T) {
	schema := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`
	w := mustParse(t, schema)
	r := mustParse(t, schema)
	if err := avro.CheckCompatibility(w, r); err != nil {
		t.Fatalf("same schema should be compatible: %v", err)
	}
}

func TestCompatFieldAddedWithDefault(t *testing.T) {
	writer := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`
	reader := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string","default":"x"}
	]}`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader)); err != nil {
		t.Fatalf("field added with default should be compatible: %v", err)
	}
}

func TestCompatFieldAddedNoDefault(t *testing.T) {
	writer := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`
	reader := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`
	err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader))
	if err == nil {
		t.Fatal("field added without default should be incompatible")
	}
}

func TestCompatFieldRemoved(t *testing.T) {
	writer := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`
	reader := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader)); err != nil {
		t.Fatalf("field removed should be compatible: %v", err)
	}
}

func TestCompatNamedTypesMatchByUnqualifiedName(t *testing.T) {
	if err := avro.CheckCompatibility(
		mustParse(t, `{"type":"record","name":"a.Foo","fields":[{"name":"a","type":"int"}]}`),
		mustParse(t, `{"type":"record","name":"b.Foo","fields":[{"name":"a","type":"int"}]}`),
	); err != nil {
		t.Fatalf("record unqualified-name match should be compatible: %v", err)
	}

	if err := avro.CheckCompatibility(
		mustParse(t, `{"type":"enum","name":"a.E","symbols":["A","B"]}`),
		mustParse(t, `{"type":"enum","name":"b.E","symbols":["A","B"]}`),
	); err != nil {
		t.Fatalf("enum unqualified-name match should be compatible: %v", err)
	}

	if err := avro.CheckCompatibility(
		mustParse(t, `{"type":"fixed","name":"a.Id","size":4}`),
		mustParse(t, `{"type":"fixed","name":"b.Id","size":4}`),
	); err != nil {
		t.Fatalf("fixed unqualified-name match should be compatible: %v", err)
	}
}

func TestCompatTypePromotion(t *testing.T) {
	// All 8 valid promotions.
	promotions := []struct {
		name   string
		writer string
		reader string
	}{
		{"int→long", `"int"`, `"long"`},
		{"int→float", `"int"`, `"float"`},
		{"int→double", `"int"`, `"double"`},
		{"long→float", `"long"`, `"float"`},
		{"long→double", `"long"`, `"double"`},
		{"float→double", `"float"`, `"double"`},
		{"string→bytes", `"string"`, `"bytes"`},
		{"bytes→string", `"bytes"`, `"string"`},
	}

	for _, p := range promotions {
		t.Run(p.name, func(t *testing.T) {
			if err := avro.CheckCompatibility(mustParse(t, p.writer), mustParse(t, p.reader)); err != nil {
				t.Fatalf("promotion %s should be compatible: %v", p.name, err)
			}
		})
	}
}

func TestCompatInvalidPromotion(t *testing.T) {
	invalid := []struct {
		name   string
		writer string
		reader string
	}{
		{"long→int", `"long"`, `"int"`},
		{"double→float", `"double"`, `"float"`},
		{"float→int", `"float"`, `"int"`},
		{"double→long", `"double"`, `"long"`},
		{"string→int", `"string"`, `"int"`},
		{"int→string", `"int"`, `"string"`},
		{"boolean→int", `"boolean"`, `"int"`},
	}

	for _, p := range invalid {
		t.Run(p.name, func(t *testing.T) {
			err := avro.CheckCompatibility(mustParse(t, p.writer), mustParse(t, p.reader))
			if err == nil {
				t.Fatalf("promotion %s should be incompatible", p.name)
			}
		})
	}
}

func TestCompatEnumSymbolAdded(t *testing.T) {
	writer := `{"type":"enum","name":"E","symbols":["A","B"]}`
	reader := `{"type":"enum","name":"E","symbols":["A","B","C"]}`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader)); err != nil {
		t.Fatalf("adding enum symbol to reader should be compatible: %v", err)
	}
}

func TestCompatEnumSymbolRemoved(t *testing.T) {
	writer := `{"type":"enum","name":"E","symbols":["A","B","C"]}`
	reader := `{"type":"enum","name":"E","symbols":["A","B"]}`
	err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader))
	if err == nil {
		t.Fatal("removing enum symbol without default should be incompatible")
	}

	// With a default, it should be compatible.
	readerWithDefault := `{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, readerWithDefault)); err != nil {
		t.Fatalf("removing enum symbol with default should be compatible: %v", err)
	}
}

func TestCompatUnionBranchAdded(t *testing.T) {
	writer := `["null","int"]`
	reader := `["null","int","string"]`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader)); err != nil {
		t.Fatalf("adding union branch to reader should be compatible: %v", err)
	}
}

func TestCompatRecordNameMismatch(t *testing.T) {
	writer := `{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]}`
	reader := `{"type":"record","name":"B","fields":[{"name":"x","type":"int"}]}`
	err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader))
	if err == nil {
		t.Fatal("different record names should be incompatible")
	}
}

func TestCompatArrayItemsCompat(t *testing.T) {
	// Compatible array items.
	writer := `{"type":"array","items":"int"}`
	reader := `{"type":"array","items":"long"}`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader)); err != nil {
		t.Fatalf("array int→long should be compatible: %v", err)
	}

	// Incompatible array items.
	reader2 := `{"type":"array","items":"string"}`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader2)); err == nil {
		t.Fatal("array int→string should be incompatible")
	}
}

func TestCompatMapValuesCompat(t *testing.T) {
	// Compatible map values.
	writer := `{"type":"map","values":"int"}`
	reader := `{"type":"map","values":"long"}`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader)); err != nil {
		t.Fatalf("map int→long should be compatible: %v", err)
	}

	// Incompatible map values.
	reader2 := `{"type":"map","values":"string"}`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader2)); err == nil {
		t.Fatal("map int→string should be incompatible")
	}
}
