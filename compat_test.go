package avro

import (
	"errors"
	"testing"
)

func TestNodeKind(t *testing.T) {
	if got := nodeKind(nil); got != "<nil>" {
		t.Fatalf("expected <nil>, got %s", got)
	}
	n := &schemaNode{kind: "int"}
	if got := nodeKind(n); got != "int" {
		t.Fatalf("expected int, got %s", got)
	}
}

func TestCheckCompatNilSchema(t *testing.T) {
	seen := make(map[nodePair]bool)
	err := checkCompat(nil, &schemaNode{kind: "int"}, "", seen)
	if err == nil {
		t.Fatal("expected error for nil reader")
	}
	err = checkCompat(&schemaNode{kind: "int"}, nil, "", seen)
	if err == nil {
		t.Fatal("expected error for nil writer")
	}
}

func TestCheckCompatCycleDetection(t *testing.T) {
	// Pre-populate seen to trigger the cycle return.
	r := &schemaNode{kind: "record", name: "R", fields: nil}
	w := &schemaNode{kind: "record", name: "R", fields: nil}
	seen := make(map[nodePair]bool)
	seen[nodePair{r, w}] = true
	err := checkCompat(r, w, "", seen)
	if err != nil {
		t.Fatalf("expected nil for cycle, got %v", err)
	}
}

func TestCompatibilityErrorFormat(t *testing.T) {
	e := &CompatibilityError{
		Path:       "User.address.zip",
		ReaderType: "string",
		WriterType: "int",
		Detail:     "incompatible types",
	}
	want := "avro: incompatible at User.address.zip: reader string vs writer int: incompatible types"
	if got := e.Error(); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestCheckWriterUnionDeepRecursionFailure(t *testing.T) {
	// Both reader and writer are unions. kindsMatch passes for all branches
	// (records have the same name), but deep compatibility check fails because
	// a reader field has no default and is missing from writer.
	reader, err := Parse(`["null", {"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}]`)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := Parse(`["null", {"type":"record","name":"R","fields":[
		{"name":"a","type":"int"}
	]}]`)
	if err != nil {
		t.Fatal(err)
	}
	err = CheckCompatibility(writer, reader)
	if err == nil {
		t.Fatal("expected error for deep record incompatibility in union")
	}
}

func TestCheckCompatibility(t *testing.T) {
	tests := []struct {
		name    string
		reader  string
		writer  string
		wantErr bool
	}{
		{
			name:   "identical primitives",
			reader: `"int"`,
			writer: `"int"`,
		},
		{
			name:   "int promoted to long",
			reader: `"long"`,
			writer: `"int"`,
		},
		{
			name:   "int promoted to float",
			reader: `"float"`,
			writer: `"int"`,
		},
		{
			name:   "int promoted to double",
			reader: `"double"`,
			writer: `"int"`,
		},
		{
			name:   "long promoted to float",
			reader: `"float"`,
			writer: `"long"`,
		},
		{
			name:   "long promoted to double",
			reader: `"double"`,
			writer: `"long"`,
		},
		{
			name:   "float promoted to double",
			reader: `"double"`,
			writer: `"float"`,
		},
		{
			name:   "string to bytes",
			reader: `"bytes"`,
			writer: `"string"`,
		},
		{
			name:   "bytes to string",
			reader: `"string"`,
			writer: `"bytes"`,
		},
		{
			name:    "incompatible primitives",
			reader:  `"int"`,
			writer:  `"string"`,
			wantErr: true,
		},
		{
			name:    "incompatible long to int (no demotion)",
			reader:  `"int"`,
			writer:  `"long"`,
			wantErr: true,
		},
		{
			name:   "identical records",
			reader: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
			writer: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
		},
		{
			name:   "reader has new field with default",
			reader: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string","default":"hello"}]}`,
			writer: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
		},
		{
			name:    "reader has new field without default",
			reader:  `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
			writer:  `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
			wantErr: true,
		},
		{
			name:   "writer has extra field (removed in reader)",
			reader: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
			writer: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
		},
		{
			name:   "field type promoted",
			reader: `{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`,
			writer: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
		},
		{
			name:    "record name mismatch",
			reader:  `{"type":"record","name":"A","fields":[{"name":"a","type":"int"}]}`,
			writer:  `{"type":"record","name":"B","fields":[{"name":"a","type":"int"}]}`,
			wantErr: true,
		},
		{
			name:   "record matched by alias",
			reader: `{"type":"record","name":"A","aliases":["B"],"fields":[{"name":"a","type":"int"}]}`,
			writer: `{"type":"record","name":"B","fields":[{"name":"a","type":"int"}]}`,
		},
		{
			name:   "identical enums",
			reader: `{"type":"enum","name":"E","symbols":["A","B","C"]}`,
			writer: `{"type":"enum","name":"E","symbols":["A","B","C"]}`,
		},
		{
			name:   "reader enum is superset",
			reader: `{"type":"enum","name":"E","symbols":["A","B","C","D"]}`,
			writer: `{"type":"enum","name":"E","symbols":["A","B","C"]}`,
		},
		{
			name:    "writer enum has unknown symbol, no default",
			reader:  `{"type":"enum","name":"E","symbols":["A","B"]}`,
			writer:  `{"type":"enum","name":"E","symbols":["A","B","C"]}`,
			wantErr: true,
		},
		{
			name:   "writer enum has unknown symbol with default",
			reader: `{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`,
			writer: `{"type":"enum","name":"E","symbols":["A","B","C"]}`,
		},
		{
			name:   "identical arrays",
			reader: `{"type":"array","items":"int"}`,
			writer: `{"type":"array","items":"int"}`,
		},
		{
			name:   "array with promoted items",
			reader: `{"type":"array","items":"long"}`,
			writer: `{"type":"array","items":"int"}`,
		},
		{
			name:   "identical maps",
			reader: `{"type":"map","values":"string"}`,
			writer: `{"type":"map","values":"string"}`,
		},
		{
			name:   "map with promoted values",
			reader: `{"type":"map","values":"double"}`,
			writer: `{"type":"map","values":"float"}`,
		},
		{
			name:   "identical fixed",
			reader: `{"type":"fixed","name":"F","size":4}`,
			writer: `{"type":"fixed","name":"F","size":4}`,
		},
		{
			name:    "fixed size mismatch",
			reader:  `{"type":"fixed","name":"F","size":4}`,
			writer:  `{"type":"fixed","name":"F","size":8}`,
			wantErr: true,
		},
		{
			name:   "writer union, reader matches all branches",
			reader: `["null","int"]`,
			writer: `["null","int"]`,
		},
		{
			name:   "reader union, writer is branch member",
			reader: `["null","int","string"]`,
			writer: `"int"`,
		},
		{
			name:    "reader union, writer not in any branch",
			reader:  `["null","int"]`,
			writer:  `"string"`,
			wantErr: true,
		},
		{
			name:   "nested record compatibility",
			reader: `{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"long"}]}}]}`,
			writer: `{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}}]}`,
		},
		// Additional edge cases:
		{
			name:    "enum name mismatch",
			reader:  `{"type":"enum","name":"E1","symbols":["A","B"]}`,
			writer:  `{"type":"enum","name":"E2","symbols":["A","B"]}`,
			wantErr: true,
		},
		{
			name:    "fixed name mismatch",
			reader:  `{"type":"fixed","name":"F1","size":4}`,
			writer:  `{"type":"fixed","name":"F2","size":4}`,
			wantErr: true,
		},
		{
			name:    "writer union branch not in reader union",
			reader:  `["null","int"]`,
			writer:  `["null","string"]`,
			wantErr: true,
		},
		{
			name:   "self-referencing record compatibility",
			reader: `{"type":"record","name":"Node","fields":[{"name":"v","type":"int"},{"name":"n","type":["null","Node"]}]}`,
			writer: `{"type":"record","name":"Node","fields":[{"name":"v","type":"int"},{"name":"n","type":["null","Node"]}]}`,
		},
		{
			name:   "kindsMatch promotion in union branch",
			reader: `["null","long"]`,
			writer: `"int"`,
		},
		{
			name:    "decimal precision mismatch",
			reader:  `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
			writer:  `{"type":"bytes","logicalType":"decimal","precision":8,"scale":2}`,
			wantErr: true,
		},
		{
			name:    "decimal scale mismatch",
			reader:  `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
			writer:  `{"type":"bytes","logicalType":"decimal","precision":10,"scale":4}`,
			wantErr: true,
		},
		{
			name:   "decimal same precision and scale",
			reader: `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
			writer: `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := Parse(tt.reader)
			if err != nil {
				t.Fatalf("reader schema: %v", err)
			}
			writer, err := Parse(tt.writer)
			if err != nil {
				t.Fatalf("writer schema: %v", err)
			}
			err = CheckCompatibility(writer, reader)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				var ce *CompatibilityError
				if !errors.As(err, &ce) {
					t.Fatalf("expected *CompatibilityError, got %T: %v", err, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestNamesMatchUnqualified(t *testing.T) {
	// Same unqualified name, different namespaces should be compatible.
	reader, err := Parse(`{"type":"record","name":"R","namespace":"com.a","fields":[{"name":"x","type":"int"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := Parse(`{"type":"record","name":"R","namespace":"com.b","fields":[{"name":"x","type":"int"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckCompatibility(writer, reader); err != nil {
		t.Fatalf("expected compatible by unqualified name, got %v", err)
	}
}
