package avro_test

import (
	"encoding"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// Mutually recursive types — must be at package level for Go.
type mutA struct {
	Val int32 `avro:"val"`
	B   *mutB `avro:"b"`
}
type mutB struct {
	Val string `avro:"val"`
	A   *mutA  `avro:"a"`
}

// 3-way cross-referential types.
type crossX struct {
	ID int32  `avro:"id"`
	Y  *crossY `avro:"y"`
}
type crossY struct {
	ID int32  `avro:"id"`
	Z  *crossZ `avro:"z"`
}
type crossZ struct {
	ID int32  `avro:"id"`
	X  *crossX `avro:"x"`
}

// testIP is a minimal TextMarshaler/TextUnmarshaler for testing.
type testIP [4]byte

var (
	_ encoding.TextMarshaler   = testIP{}
	_ encoding.TextUnmarshaler = (*testIP)(nil)
)

func (ip testIP) MarshalText() ([]byte, error) {
	return []byte{
		'0' + ip[0]/100, '0' + (ip[0]/10)%10, '0' + ip[0]%10, '.',
		'0' + ip[1]/100, '0' + (ip[1]/10)%10, '0' + ip[1]%10, '.',
		'0' + ip[2]/100, '0' + (ip[2]/10)%10, '0' + ip[2]%10, '.',
		'0' + ip[3]/100, '0' + (ip[3]/10)%10, '0' + ip[3]%10,
	}, nil
}

func (ip *testIP) UnmarshalText(b []byte) error {
	var parts [4]byte
	p := 0
	for i := 0; i < 4; i++ {
		var v byte
		for p < len(b) && b[p] != '.' {
			v = v*10 + (b[p] - '0')
			p++
		}
		parts[i] = v
		p++ // skip '.'
	}
	*ip = parts
	return nil
}

// -----------------------------------------------------------------------
// Deep Multi-Level Nesting Tests
//
// These tests exercise multi-level type nesting: deep record/array/map
// chains, logical types at depth, odd Go types (TextMarshaler, uint,
// big.Rat, embedded structs, inline tags), recursive and cross-referential
// schemas, schema resolution through nested types, and JSON codec.
// -----------------------------------------------------------------------

// ---------- deep record/collection chains ----------

func TestDeepNestingRecordArrayRecordArrayRecord(t *testing.T) {
	// Company → []Department → []Employee
	schema := `{
		"type": "record", "name": "Company", "fields": [
			{"name": "name", "type": "string"},
			{"name": "departments", "type": {"type": "array", "items": {
				"type": "record", "name": "Department", "fields": [
					{"name": "name", "type": "string"},
					{"name": "employees", "type": {"type": "array", "items": {
						"type": "record", "name": "Employee", "fields": [
							{"name": "name", "type": "string"},
							{"name": "age", "type": "int"}
						]
					}}}
				]
			}}}
		]
	}`
	type Employee struct {
		Name string `avro:"name"`
		Age  int32  `avro:"age"`
	}
	type Department struct {
		Name      string     `avro:"name"`
		Employees []Employee `avro:"employees"`
	}
	type Company struct {
		Name        string       `avro:"name"`
		Departments []Department `avro:"departments"`
	}

	t.Run("typed round-trip", func(t *testing.T) {
		input := Company{
			Name: "Acme",
			Departments: []Department{
				{Name: "Eng", Employees: []Employee{{Name: "Alice", Age: 30}, {Name: "Bob", Age: 25}}},
				{Name: "Sales", Employees: []Employee{{Name: "Charlie", Age: 35}}},
			},
		}
		got := roundTrip(t, schema, input)
		if !reflect.DeepEqual(got, input) {
			t.Fatalf("got %+v, want %+v", got, input)
		}
	})

	t.Run("decode to any", func(t *testing.T) {
		s := mustParse(t, schema)
		input := Company{Name: "Acme", Departments: []Department{
			{Name: "Eng", Employees: []Employee{{Name: "Alice", Age: 30}}},
		}}
		encoded, err := s.AppendEncode(nil, &input)
		if err != nil {
			t.Fatal(err)
		}
		var result any
		rem, err := s.Decode(encoded, &result)
		if err != nil {
			t.Fatal(err)
		}
		if len(rem) != 0 {
			t.Fatalf("unconsumed: %d", len(rem))
		}
		m := result.(map[string]any)
		depts := m["departments"].([]any)
		emp := depts[0].(map[string]any)["employees"].([]any)[0].(map[string]any)
		if emp["name"] != "Alice" || emp["age"] != int32(30) {
			t.Fatalf("employee: got %v", emp)
		}
	})
}

func TestDeepNestingMapOfRecordWithArrayOfRecords(t *testing.T) {
	schema := `{
		"type": "map",
		"values": {
			"type": "record", "name": "Team", "fields": [
				{"name": "lead", "type": "string"},
				{"name": "members", "type": {"type": "array", "items": {
					"type": "record", "name": "Member", "fields": [
						{"name": "name", "type": "string"},
						{"name": "role", "type": "string"}
					]
				}}}
			]
		}
	}`
	type Member struct {
		Name string `avro:"name"`
		Role string `avro:"role"`
	}
	type Team struct {
		Lead    string   `avro:"lead"`
		Members []Member `avro:"members"`
	}
	input := map[string]Team{
		"backend":  {Lead: "Alice", Members: []Member{{Name: "Bob", Role: "dev"}, {Name: "Carol", Role: "dev"}}},
		"frontend": {Lead: "Dave", Members: []Member{{Name: "Eve", Role: "design"}}},
	}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("got %+v, want %+v", got, input)
	}
}

func TestDeepNestingMapMapMapRecord(t *testing.T) {
	schema := `{
		"type": "map", "values": {
			"type": "map", "values": {
				"type": "map", "values": {
					"type": "record", "name": "Cell", "fields": [
						{"name": "v", "type": "double"}
					]
				}
			}
		}
	}`
	type Cell struct {
		V float64 `avro:"v"`
	}
	input := map[string]map[string]map[string]Cell{
		"sheet1": {"row1": {"A": {V: 1.1}, "B": {V: 2.2}}},
		"sheet2": {"row1": {"A": {V: 3.3}}},
	}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("got %+v, want %+v", got, input)
	}
}

func TestDeepNestingArrayOfArrayOfArrayOfString(t *testing.T) {
	schema := `{"type": "array", "items": {"type": "array", "items": {"type": "array", "items": "string"}}}`
	input := [][][]string{{{"a", "b"}, {"c"}}, {{"d"}}}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("got %+v, want %+v", got, input)
	}
}

func TestDeepNesting5LevelArrayOfInt(t *testing.T) {
	schema := `{"type":"array","items":{"type":"array","items":{"type":"array","items":{"type":"array","items":{"type":"array","items":"int"}}}}}`
	input := [][][][][]int32{
		{{{{1, 2}, {3}}}},
		{{{{4}}, {{5, 6, 7}}}},
	}
	got := roundTrip(t, schema, input)
	if got[0][0][0][0][0] != 1 || got[1][0][1][0][2] != 7 {
		t.Fatalf("got %v", got)
	}
}

func TestDeepNestingArrayOfMapOfArrayOfInt(t *testing.T) {
	schema := `{"type": "array", "items": {"type": "map", "values": {"type": "array", "items": "int"}}}`
	input := []map[string][]int32{
		{"evens": {2, 4}, "odds": {1, 3}},
		{"primes": {2, 3, 5, 7}},
	}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("got %+v, want %+v", got, input)
	}
}

// ---------- unions at depth ----------

func TestDeepNestingUnionWithNestedRecord(t *testing.T) {
	// Top-level ["null", Record] union — exercises isNilValue peeling through **T.
	schema := `["null", {
		"type": "record", "name": "Order", "fields": [
			{"name": "id", "type": "int"},
			{"name": "shipping", "type": {
				"type": "record", "name": "Address", "fields": [
					{"name": "street", "type": "string"},
					{"name": "city", "type": "string"}
				]
			}}
		]
	}]`
	type Address struct {
		Street string `avro:"street"`
		City   string `avro:"city"`
	}
	type Order struct {
		ID       int32   `avro:"id"`
		Shipping Address `avro:"shipping"`
	}
	s := mustParse(t, schema)

	input := &Order{ID: 42, Shipping: Address{Street: "123 Main", City: "Springfield"}}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatal(err)
	}
	var got *Order
	rem, err := s.Decode(encoded, &got)
	if err != nil {
		t.Fatal(err)
	}
	if len(rem) != 0 {
		t.Fatalf("unconsumed: %d", len(rem))
	}
	if got.ID != 42 || got.Shipping.Street != "123 Main" {
		t.Fatalf("got %+v", got)
	}

	var nilInput *Order
	encoded, err = s.AppendEncode(nil, &nilInput)
	if err != nil {
		t.Fatal(err)
	}
	var gotNil *Order
	if rem, err = s.Decode(encoded, &gotNil); err != nil {
		t.Fatal(err)
	}
	if len(rem) != 0 || gotNil != nil {
		t.Fatalf("expected nil, got %+v", gotNil)
	}
}

func TestDeepNestingArrayOfRecordsWithNullableNestedRecord(t *testing.T) {
	schema := `{
		"type": "array",
		"items": {
			"type": "record", "name": "Node", "fields": [
				{"name": "value", "type": "int"},
				{"name": "meta", "type": ["null", {
					"type": "record", "name": "Meta", "fields": [
						{"name": "tag", "type": "string"},
						{"name": "score", "type": "float"}
					]
				}]}
			]
		}
	}`
	type Meta struct {
		Tag   string  `avro:"tag"`
		Score float32 `avro:"score"`
	}
	type Node struct {
		Value int32 `avro:"value"`
		Meta  *Meta `avro:"meta"`
	}
	input := []Node{
		{Value: 1, Meta: &Meta{Tag: "first", Score: 0.9}},
		{Value: 2, Meta: nil},
		{Value: 3, Meta: &Meta{Tag: "third", Score: 0.5}},
	}
	got := roundTrip(t, schema, input)
	if got[0].Meta == nil || got[0].Meta.Tag != "first" {
		t.Fatalf("got[0]: %+v", got[0])
	}
	if got[1].Meta != nil {
		t.Fatalf("got[1]: expected nil meta")
	}
	if got[2].Meta == nil || got[2].Meta.Tag != "third" {
		t.Fatalf("got[2]: %+v", got[2])
	}
}

func TestDeepNestingRecordWithMapOfArrayOfUnions(t *testing.T) {
	schema := `{
		"type": "record", "name": "Config", "fields": [
			{"name": "name", "type": "string"},
			{"name": "settings", "type": {
				"type": "map",
				"values": {"type": "array", "items": ["null", "int", "string"]}
			}}
		]
	}`
	type Config struct {
		Name     string           `avro:"name"`
		Settings map[string][]any `avro:"settings"`
	}
	input := Config{
		Name: "myconfig",
		Settings: map[string][]any{
			"ports":  {int32(8080), int32(443), nil},
			"labels": {"web", nil, "api"},
		},
	}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got.Settings, input.Settings) {
		t.Fatalf("got %+v, want %+v", got.Settings, input.Settings)
	}
}

func TestDeepNesting3BranchUnionAtDepth(t *testing.T) {
	schema := `{
		"type": "record", "name": "Outer", "fields": [{
			"name": "inner", "type": {
				"type": "record", "name": "Inner", "fields": [{
					"name": "payload", "type": [
						"null",
						{"type": "record", "name": "Text", "fields": [{"name": "body", "type": "string"}]},
						{"type": "record", "name": "Num", "fields": [{"name": "val", "type": "long"}]}
					]
				}]
			}
		}]
	}`
	type Inner struct{ Payload any `avro:"payload"` }
	type Outer struct{ Inner Inner `avro:"inner"` }
	s := mustParse(t, schema)

	for _, tc := range []struct {
		name string
		in   any
		check func(any)
	}{
		{"text", map[string]any{"body": "hello"}, func(v any) {
			if v.(map[string]any)["body"] != "hello" { panic("text mismatch") }
		}},
		{"num", map[string]any{"val": int64(999)}, func(v any) {
			if v.(map[string]any)["val"] != int64(999) { panic("num mismatch") }
		}},
		{"null", nil, func(v any) {
			if v != nil { panic("null mismatch") }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := Outer{Inner: Inner{Payload: tc.in}}
			encoded, err := s.AppendEncode(nil, &in)
			if err != nil {
				t.Fatal(err)
			}
			var got Outer
			if _, err := s.Decode(encoded, &got); err != nil {
				t.Fatal(err)
			}
			tc.check(got.Inner.Payload)
		})
	}
}

// ---------- named type reuse ----------

func TestDeepNestingNamedTypeReuseInNestedRecords(t *testing.T) {
	cache := &avro.SchemaCache{}
	mustCacheParse := func(schema string) *avro.Schema {
		s, err := cache.Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	mustCacheParse(`{"type":"record","name":"Coord","fields":[{"name":"lat","type":"double"},{"name":"lon","type":"double"}]}`)
	mustCacheParse(`{"type":"record","name":"Location","fields":[{"name":"name","type":"string"},{"name":"coord","type":"Coord"}]}`)
	s := mustCacheParse(`{
		"type": "record", "name": "Trip", "fields": [
			{"name": "origin", "type": "Location"},
			{"name": "destination", "type": "Location"},
			{"name": "waypoints", "type": {"type": "array", "items": "Location"}}
		]
	}`)

	type Coord struct {
		Lat float64 `avro:"lat"`
		Lon float64 `avro:"lon"`
	}
	type Location struct {
		Name  string `avro:"name"`
		Coord Coord  `avro:"coord"`
	}
	type Trip struct {
		Origin      Location   `avro:"origin"`
		Destination Location   `avro:"destination"`
		Waypoints   []Location `avro:"waypoints"`
	}
	input := Trip{
		Origin:      Location{Name: "Home", Coord: Coord{Lat: 40.7, Lon: -74.0}},
		Destination: Location{Name: "Office", Coord: Coord{Lat: 40.8, Lon: -73.9}},
		Waypoints:   []Location{{Name: "Coffee", Coord: Coord{Lat: 40.75, Lon: -73.95}}},
	}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatal(err)
	}
	var got Trip
	if rem, err := s.Decode(encoded, &got); err != nil || len(rem) != 0 {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("got %+v, want %+v", got, input)
	}
}

// ---------- empty collections at various levels ----------

func TestDeepNestingEmptyCollections(t *testing.T) {
	t.Run("record with empty nested array", func(t *testing.T) {
		schema := `{
			"type": "record", "name": "Outer", "fields": [
				{"name": "items", "type": {"type": "array", "items": {
					"type": "record", "name": "Inner", "fields": [{"name": "v", "type": "int"}]
				}}},
				{"name": "label", "type": "string"}
			]
		}`
		type Inner struct{ V int32 `avro:"v"` }
		type Outer struct {
			Items []Inner `avro:"items"`
			Label string  `avro:"label"`
		}
		got := roundTrip(t, schema, Outer{Items: []Inner{}, Label: "empty"})
		if len(got.Items) != 0 || got.Label != "empty" {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("nested empty maps", func(t *testing.T) {
		schema := `{"type": "map", "values": {"type": "map", "values": "int"}}`
		input := map[string]map[string]int32{"filled": {"a": 1}, "empty": {}}
		got := roundTrip(t, schema, input)
		if len(got["empty"]) != 0 || got["filled"]["a"] != 1 {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("deeply nested all empty", func(t *testing.T) {
		schema := `{
			"type": "record", "name": "Root", "fields": [
				{"name": "data", "type": {"type": "map", "values": {
					"type": "array", "items": {
						"type": "record", "name": "Leaf", "fields": [
							{"name": "tags", "type": {"type": "array", "items": "string"}}
						]
					}
				}}}
			]
		}`
		type Leaf struct{ Tags []string `avro:"tags"` }
		type Root struct{ Data map[string][]Leaf `avro:"data"` }
		got := roundTrip(t, schema, Root{Data: map[string][]Leaf{
			"group1": {{Tags: []string{}}, {Tags: []string{}}},
			"group2": {},
		}})
		if len(got.Data["group2"]) != 0 || len(got.Data["group1"]) != 2 {
			t.Fatalf("got %+v", got)
		}
	})
}

// ---------- recursive / self-referential ----------

func TestDeepNestingRecursiveTree(t *testing.T) {
	schema := `{
		"type": "record", "name": "TreeNode", "fields": [
			{"name": "value", "type": "int"},
			{"name": "left", "type": ["null", "TreeNode"]},
			{"name": "right", "type": ["null", "TreeNode"]}
		]
	}`
	type TreeNode struct {
		Value int32     `avro:"value"`
		Left  *TreeNode `avro:"left"`
		Right *TreeNode `avro:"right"`
	}
	input := TreeNode{
		Value: 10,
		Left:  &TreeNode{Value: 5, Left: &TreeNode{Value: 2}, Right: &TreeNode{Value: 7}},
		Right: &TreeNode{Value: 15, Right: &TreeNode{Value: 20}},
	}
	got := roundTrip(t, schema, input)
	if got.Left.Left.Value != 2 || got.Left.Right.Value != 7 || got.Right.Right.Value != 20 {
		t.Fatalf("tree mismatch: %+v", got)
	}
}

func TestDeepNestingRecursiveTreeWithArrayChildren(t *testing.T) {
	schema := `{
		"type": "record", "name": "TreeNode", "fields": [
			{"name": "name", "type": "string"},
			{"name": "children", "type": {"type": "array", "items": "TreeNode"}}
		]
	}`
	type TreeNode struct {
		Name     string      `avro:"name"`
		Children []*TreeNode `avro:"children"`
	}
	input := TreeNode{
		Name: "root",
		Children: []*TreeNode{
			{Name: "a", Children: []*TreeNode{
				{Name: "a1"},
				{Name: "a2", Children: []*TreeNode{{Name: "a2i"}}},
			}},
			{Name: "b"},
		},
	}
	got := roundTrip(t, schema, input)
	if got.Children[0].Children[1].Children[0].Name != "a2i" {
		t.Fatalf("a2i: got %q", got.Children[0].Children[1].Children[0].Name)
	}
}

// ---------- mutually recursive / cross-referential ----------

func TestDeepNestingMutualRecursion(t *testing.T) {
	s := avro.MustParse(`{
		"type": "record", "name": "A", "fields": [
			{"name": "val", "type": "int"},
			{"name": "b", "type": ["null", {
				"type": "record", "name": "B", "fields": [
					{"name": "val", "type": "string"},
					{"name": "a", "type": ["null", "A"]}
				]
			}]}
		]
	}`)

	t.Run("A-B-A-B-A deep", func(t *testing.T) {
		input := mutA{Val: 1, B: &mutB{Val: "2", A: &mutA{Val: 3, B: &mutB{Val: "4", A: &mutA{Val: 5, B: nil}}}}}
		encoded, err := s.AppendEncode(nil, &input)
		if err != nil {
			t.Fatal(err)
		}
		var got mutA
		if _, err := s.Decode(encoded, &got); err != nil {
			t.Fatal(err)
		}
		if got.B.A.B.A.Val != 5 {
			t.Fatalf("got %d, want 5", got.B.A.B.A.Val)
		}
	})

	t.Run("decode to any", func(t *testing.T) {
		input := mutA{Val: 1, B: &mutB{Val: "two", A: &mutA{Val: 3}}}
		encoded, err := s.AppendEncode(nil, &input)
		if err != nil {
			t.Fatal(err)
		}
		var got any
		if _, err := s.Decode(encoded, &got); err != nil {
			t.Fatal(err)
		}
		a2 := got.(map[string]any)["b"].(map[string]any)["a"].(map[string]any)
		if a2["val"] != int32(3) {
			t.Fatalf("b.a.val: got %v", a2["val"])
		}
	})
}

func TestDeepNesting3WayCrossReference(t *testing.T) {
	s := avro.MustParse(`{
		"type": "record", "name": "X", "fields": [
			{"name": "id", "type": "int"},
			{"name": "y", "type": ["null", {
				"type": "record", "name": "Y", "fields": [
					{"name": "id", "type": "int"},
					{"name": "z", "type": ["null", {
						"type": "record", "name": "Z", "fields": [
							{"name": "id", "type": "int"},
							{"name": "x", "type": ["null", "X"]}
						]
					}]}
				]
			}]}
		]
	}`)

	t.Run("double cycle X-Y-Z-X-Y-Z-X", func(t *testing.T) {
		input := crossX{ID: 1, Y: &crossY{ID: 2, Z: &crossZ{ID: 3, X: &crossX{
			ID: 4, Y: &crossY{ID: 5, Z: &crossZ{ID: 6, X: &crossX{ID: 7}}},
		}}}}
		encoded, err := s.AppendEncode(nil, &input)
		if err != nil {
			t.Fatal(err)
		}
		var got crossX
		if _, err := s.Decode(encoded, &got); err != nil {
			t.Fatal(err)
		}
		if got.Y.Z.X.Y.Z.X.ID != 7 {
			t.Fatalf("got %d, want 7", got.Y.Z.X.Y.Z.X.ID)
		}
	})

	t.Run("partial nulls", func(t *testing.T) {
		input := crossX{ID: 1, Y: &crossY{ID: 2, Z: nil}}
		encoded, err := s.AppendEncode(nil, &input)
		if err != nil {
			t.Fatal(err)
		}
		var got crossX
		if _, err := s.Decode(encoded, &got); err != nil {
			t.Fatal(err)
		}
		if got.Y.Z != nil {
			t.Fatalf("expected nil Z")
		}
	})
}

// ---------- schema resolution at depth ----------

func TestDeepNestingResolve3LevelFieldChanges(t *testing.T) {
	// 3-level: add field at L3, remove field at L2, promote int→long at L3.
	writerSchema := `{
		"type": "record", "name": "L1", "fields": [{
			"name": "l2", "type": {
				"type": "record", "name": "L2", "fields": [
					{"name": "removed", "type": "string"},
					{"name": "l3", "type": {
						"type": "record", "name": "L3", "fields": [
							{"name": "x", "type": "int"}
						]
					}}
				]
			}
		}]
	}`
	readerSchema := `{
		"type": "record", "name": "L1", "fields": [{
			"name": "l2", "type": {
				"type": "record", "name": "L2", "fields": [
					{"name": "l3", "type": {
						"type": "record", "name": "L3", "fields": [
							{"name": "x", "type": "long"},
							{"name": "y", "type": "string", "default": "new"}
						]
					}}
				]
			}
		}]
	}`
	type L3W struct{ X int32 `avro:"x"` }
	type L2W struct {
		Removed string `avro:"removed"`
		L3      L3W    `avro:"l3"`
	}
	type L1W struct{ L2 L2W `avro:"l2"` }
	type L3R struct {
		X int64  `avro:"x"`
		Y string `avro:"y"`
	}
	type L2R struct{ L3 L3R `avro:"l3"` }
	type L1R struct{ L2 L2R `avro:"l2"` }

	var got L1R
	resolveEncodeDecode(t, writerSchema, readerSchema,
		&L1W{L2: L2W{Removed: "gone", L3: L3W{X: 7}}}, &got)
	if got.L2.L3.X != 7 || got.L2.L3.Y != "new" {
		t.Fatalf("got %+v", got.L2.L3)
	}
}

// ---------- JSON codec at depth ----------

func TestDeepNestingJSONCodec(t *testing.T) {
	schema := `{
		"type": "record", "name": "Root", "fields": [
			{"name": "child", "type": {
				"type": "record", "name": "Child", "fields": [
					{"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
					{"name": "items", "type": {"type": "array", "items": {
						"type": "record", "name": "Item", "fields": [
							{"name": "id", "type": "int"},
							{"name": "label", "type": "string"}
						]
					}}},
					{"name": "opt", "type": ["null", "string"]}
				]
			}}
		]
	}`
	type Item struct {
		ID    int32  `avro:"id"`
		Label string `avro:"label"`
	}
	type Child struct {
		Ts    time.Time `avro:"ts"`
		Items []Item    `avro:"items"`
		Opt   *string   `avro:"opt"`
	}
	type Root struct{ Child Child `avro:"child"` }
	s := avro.MustParse(schema)
	ts := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	opt := "yes"
	input := Root{Child: Child{Ts: ts, Items: []Item{{1, "a"}, {2, "b"}}, Opt: &opt}}

	jsonBytes, err := s.EncodeJSON(&input)
	if err != nil {
		t.Fatal(err)
	}
	var got Root
	if err := s.DecodeJSON(jsonBytes, &got); err != nil {
		t.Fatal(err)
	}
	if !got.Child.Ts.Equal(ts) || len(got.Child.Items) != 2 || got.Child.Opt == nil || *got.Child.Opt != "yes" {
		t.Fatalf("got %+v", got)
	}

	// Null union case.
	inputNil := Root{Child: Child{Ts: ts, Items: []Item{{1, "a"}}, Opt: nil}}
	jsonBytes, err = s.EncodeJSON(&inputNil)
	if err != nil {
		t.Fatal(err)
	}
	var gotNil Root
	if err := s.DecodeJSON(jsonBytes, &gotNil); err != nil {
		t.Fatal(err)
	}
	if gotNil.Child.Opt != nil {
		t.Fatalf("expected nil opt")
	}
}

// ---------- complex mixed nesting ----------

func TestDeepNestingRecordMapArrayRecordUnion(t *testing.T) {
	schema := `{
		"type": "record", "name": "Catalog", "fields": [
			{"name": "sections", "type": {
				"type": "map", "values": {
					"type": "array", "items": {
						"type": "record", "name": "Product", "fields": [
							{"name": "name", "type": "string"},
							{"name": "desc", "type": ["null", "string"]}
						]
					}
				}
			}}
		]
	}`
	type Product struct {
		Name string  `avro:"name"`
		Desc *string `avro:"desc"`
	}
	type Catalog struct{ Sections map[string][]Product `avro:"sections"` }
	desc := "A widget"
	input := Catalog{Sections: map[string][]Product{
		"widgets": {{Name: "Widget A", Desc: &desc}, {Name: "Widget B", Desc: nil}},
		"gadgets": {{Name: "Gadget X", Desc: nil}},
	}}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("got %+v, want %+v", got, input)
	}
}

func TestDeepNestingRecordWithEnumArrayFixedMapUnion(t *testing.T) {
	schema := `{
		"type": "record", "name": "Complex", "fields": [
			{"name": "statuses", "type": {"type": "array", "items": {
				"type": "enum", "name": "Status", "symbols": ["ACTIVE", "INACTIVE", "PENDING"]
			}}},
			{"name": "checksums", "type": {"type": "map", "values": {
				"type": "fixed", "name": "MD5", "size": 16
			}}},
			{"name": "payload", "type": ["null",
				{"type":"record","name":"TextPayload","fields":[{"name":"body","type":"string"}]},
				{"type":"record","name":"BinaryPayload","fields":[{"name":"data","type":"bytes"},{"name":"mime","type":"string"}]}
			]}
		]
	}`
	type Complex struct {
		Statuses  []string            `avro:"statuses"`
		Checksums map[string][16]byte `avro:"checksums"`
		Payload   any                 `avro:"payload"`
	}
	s := mustParse(t, schema)
	md5 := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	input := Complex{
		Statuses: []string{"ACTIVE", "PENDING"}, Checksums: map[string][16]byte{"file1": md5},
		Payload: map[string]any{"body": "hello"},
	}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatal(err)
	}
	var got Complex
	if _, err := s.Decode(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Statuses, input.Statuses) || !reflect.DeepEqual(got.Checksums, input.Checksums) {
		t.Fatalf("got %+v", got)
	}
	if got.Payload.(map[string]any)["body"] != "hello" {
		t.Fatalf("payload: %v", got.Payload)
	}
}

// ---------- nullable pointer chains ----------

func TestDeepNestingNullableChainWithLogicalTypes(t *testing.T) {
	schema := `{
		"type": "record", "name": "Root", "fields": [
			{"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
			{"name": "mid", "type": ["null", {
				"type": "record", "name": "Mid", "fields": [
					{"name": "day", "type": {"type": "int", "logicalType": "date"}},
					{"name": "leaf", "type": ["null", {
						"type": "record", "name": "Leaf", "fields": [
							{"name": "dur", "type": {"type": "int", "logicalType": "time-millis"}},
							{"name": "hash", "type": {"type": "fixed", "name": "H4", "size": 4}}
						]
					}]}
				]
			}]}
		]
	}`
	type Leaf struct {
		Dur  time.Duration `avro:"dur"`
		Hash [4]byte       `avro:"hash"`
	}
	type Mid struct {
		Day  time.Time `avro:"day"`
		Leaf *Leaf     `avro:"leaf"`
	}
	type Root struct {
		Ts  time.Time `avro:"ts"`
		Mid *Mid      `avro:"mid"`
	}
	ts := time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)
	day := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)

	full := roundTrip(t, schema, Root{Ts: ts, Mid: &Mid{Day: day, Leaf: &Leaf{Dur: 2 * time.Hour, Hash: [4]byte{0xCA, 0xFE, 0xBA, 0xBE}}}})
	if full.Mid.Leaf.Hash != [4]byte{0xCA, 0xFE, 0xBA, 0xBE} || full.Mid.Leaf.Dur != 2*time.Hour {
		t.Fatalf("full: %+v", full)
	}

	partial := roundTrip(t, schema, Root{Ts: ts, Mid: &Mid{Day: day, Leaf: nil}})
	if partial.Mid == nil || partial.Mid.Leaf != nil {
		t.Fatalf("partial: %+v", partial)
	}

	none := roundTrip(t, schema, Root{Ts: ts, Mid: nil})
	if none.Mid != nil {
		t.Fatalf("none: %+v", none)
	}
}

// ---------- logical types at depth ----------

func TestDeepNestingTimestampsAtDepth(t *testing.T) {
	schema := `{
		"type": "record", "name": "EventLog", "fields": [
			{"name": "events", "type": {"type": "array", "items": {
				"type": "record", "name": "Event", "fields": [
					{"name": "name", "type": "string"},
					{"name": "ts_ms", "type": {"type": "long", "logicalType": "timestamp-millis"}},
					{"name": "ts_us", "type": {"type": "long", "logicalType": "timestamp-micros"}},
					{"name": "ts_ns", "type": {"type": "long", "logicalType": "timestamp-nanos"}},
					{"name": "day", "type": {"type": "int", "logicalType": "date"}}
				]
			}}}
		]
	}`
	type Event struct {
		Name string    `avro:"name"`
		TsMs time.Time `avro:"ts_ms"`
		TsUs time.Time `avro:"ts_us"`
		TsNs time.Time `avro:"ts_ns"`
		Day  time.Time `avro:"day"`
	}
	type EventLog struct{ Events []Event `avro:"events"` }
	ts := time.Date(2025, 6, 15, 12, 30, 45, 123456789, time.UTC)
	day := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
	input := EventLog{Events: []Event{
		{Name: "click", TsMs: ts.Truncate(time.Millisecond), TsUs: ts.Truncate(time.Microsecond), TsNs: ts, Day: day},
	}}
	got := roundTrip(t, schema, input)
	e := got.Events[0]
	if !e.TsMs.Equal(input.Events[0].TsMs) || !e.TsUs.Equal(input.Events[0].TsUs) || !e.TsNs.Equal(input.Events[0].TsNs) || !e.Day.Equal(day) {
		t.Fatalf("got %+v", e)
	}
}

func TestDeepNestingTimeDurationAtDepth(t *testing.T) {
	// time-millis, time-micros, and Avro duration all at depth.
	schema := `{
		"type": "record", "name": "Outer", "fields": [{
			"name": "inner", "type": {
				"type": "record", "name": "Inner", "fields": [
					{"name": "t_ms", "type": {"type": "int", "logicalType": "time-millis"}},
					{"name": "t_us", "type": {"type": "long", "logicalType": "time-micros"}},
					{"name": "span", "type": {"type": "fixed", "name": "dur", "size": 12, "logicalType": "duration"}}
				]
			}
		}]
	}`
	type Inner struct {
		TMs  time.Duration `avro:"t_ms"`
		TUs  time.Duration `avro:"t_us"`
		Span avro.Duration `avro:"span"`
	}
	type Outer struct{ Inner Inner `avro:"inner"` }
	input := Outer{Inner: Inner{
		TMs:  8 * time.Hour,
		TUs:  12*time.Hour + 30*time.Minute,
		Span: avro.Duration{Months: 3, Days: 15, Milliseconds: 43200000},
	}}
	got := roundTrip(t, schema, input)
	if got.Inner != input.Inner {
		t.Fatalf("got %+v, want %+v", got.Inner, input.Inner)
	}
}

func TestDeepNestingDecimalAtDepth(t *testing.T) {
	// Bytes-backed and fixed-backed decimals in one nested schema.
	schema := `{
		"type": "record", "name": "Ledger", "fields": [{
			"name": "entries", "type": {"type": "array", "items": {
				"type": "record", "name": "Entry", "fields": [
					{"name": "desc", "type": "string"},
					{"name": "amount_b", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
					{"name": "amount_f", "type": {"type": "fixed", "name": "Amt", "size": 8, "logicalType": "decimal", "precision": 12, "scale": 4}}
				]
			}}
		}]
	}`
	type Entry struct {
		Desc    string   `avro:"desc"`
		AmountB *big.Rat `avro:"amount_b"`
		AmountF *big.Rat `avro:"amount_f"`
	}
	type Ledger struct{ Entries []Entry `avro:"entries"` }
	input := Ledger{Entries: []Entry{
		{Desc: "Credit", AmountB: new(big.Rat).SetFrac64(1999, 100), AmountF: new(big.Rat).SetFrac64(1745000, 10000)},
		{Desc: "Debit", AmountB: new(big.Rat).SetFrac64(-150, 100), AmountF: new(big.Rat).SetFrac64(0, 1)},
	}}
	s := mustParse(t, schema)
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatal(err)
	}
	var got Ledger
	if _, err := s.Decode(encoded, &got); err != nil {
		t.Fatal(err)
	}
	for i, e := range got.Entries {
		if e.AmountB.Cmp(input.Entries[i].AmountB) != 0 || e.AmountF.Cmp(input.Entries[i].AmountF) != 0 {
			t.Fatalf("entries[%d]: got b=%s f=%s", i, e.AmountB.FloatString(2), e.AmountF.FloatString(4))
		}
	}
}

func TestDeepNestingUUIDAtDepth(t *testing.T) {
	schema := `{
		"type": "record", "name": "Cluster", "fields": [
			{"name": "nodes", "type": {"type": "array", "items": {
				"type": "record", "name": "Node", "fields": [
					{"name": "id", "type": {"type": "fixed", "name": "uuid", "size": 16, "logicalType": "uuid"}},
					{"name": "host", "type": "string"}
				]
			}}}
		]
	}`
	type Node struct {
		ID   [16]byte `avro:"id"`
		Host string   `avro:"host"`
	}
	type Cluster struct{ Nodes []Node `avro:"nodes"` }
	id1 := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	input := Cluster{Nodes: []Node{{ID: id1, Host: "node-1.local"}}}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("got %+v, want %+v", got, input)
	}
}

// ---------- enums and fixed at depth ----------

func TestDeepNestingEnumInRecordInArrayInMapInRecord(t *testing.T) {
	schema := `{
		"type": "record", "name": "Dashboard", "fields": [
			{"name": "panels", "type": {"type": "map", "values": {
				"type": "array", "items": {
					"type": "record", "name": "Widget", "fields": [
						{"name": "title", "type": "string"},
						{"name": "severity", "type": {
							"type": "enum", "name": "Severity",
							"symbols": ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]
						}}
					]
				}
			}}}
		]
	}`
	type Widget struct {
		Title    string `avro:"title"`
		Severity string `avro:"severity"`
	}
	type Dashboard struct{ Panels map[string][]Widget `avro:"panels"` }
	input := Dashboard{Panels: map[string][]Widget{
		"alerts": {{Title: "CPU", Severity: "WARN"}, {Title: "Disk", Severity: "FATAL"}},
		"info":   {{Title: "Uptime", Severity: "INFO"}},
	}}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("got %+v, want %+v", got, input)
	}
}

func TestDeepNestingEnumAsOrdinalAtDepth(t *testing.T) {
	schema := `{
		"type": "record", "name": "Outer", "fields": [{
			"name": "inner", "type": {
				"type": "record", "name": "Inner", "fields": [{
					"name": "status", "type": {
						"type": "enum", "name": "Status", "symbols": ["OFF", "ON", "STANDBY"]
					}
				}]
			}
		}]
	}`
	s := mustParse(t, schema)
	type SI struct{ Status string `avro:"status"` }
	type OI struct{ Inner SI `avro:"inner"` }
	encoded, err := s.AppendEncode(nil, &OI{Inner: SI{Status: "STANDBY"}})
	if err != nil {
		t.Fatal(err)
	}
	type II struct{ Status int `avro:"status"` }
	type OO struct{ Inner II `avro:"inner"` }
	var got OO
	if _, err := s.Decode(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got.Inner.Status != 2 {
		t.Fatalf("ordinal: got %d, want 2", got.Inner.Status)
	}
}

func TestDeepNestingFixedVariousSizesAtDepth(t *testing.T) {
	schema := `{
		"type": "record", "name": "Outer", "fields": [{
			"name": "inner", "type": {
				"type": "record", "name": "Inner", "fields": [
					{"name": "flag", "type": {"type": "fixed", "name": "F1", "size": 1}},
					{"name": "tag", "type": {"type": "fixed", "name": "F4", "size": 4}},
					{"name": "hash", "type": {"type": "fixed", "name": "F32", "size": 32}}
				]
			}
		}]
	}`
	type Inner struct {
		Flag [1]byte  `avro:"flag"`
		Tag  [4]byte  `avro:"tag"`
		Hash [32]byte `avro:"hash"`
	}
	type Outer struct{ Inner Inner `avro:"inner"` }
	var hash [32]byte
	for i := range hash { hash[i] = byte(i) }
	input := Outer{Inner: Inner{Flag: [1]byte{0xFF}, Tag: [4]byte{0xDE, 0xAD, 0xBE, 0xEF}, Hash: hash}}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("got %+v, want %+v", got, input)
	}
}

func TestDeepNestingArrayOfMapsOfFixed(t *testing.T) {
	schema := `{"type":"array","items":{"type":"map","values":{"type":"fixed","name":"Tag","size":4}}}`
	input := []map[string][4]byte{{"x": {1, 2, 3, 4}, "y": {5, 6, 7, 8}}, {"z": {9, 10, 11, 12}}}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("got %+v, want %+v", got, input)
	}
}

func TestDeepNestingMapOfArrayOfEnums(t *testing.T) {
	schema := `{"type":"map","values":{"type":"array","items":{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}}}`
	input := map[string][]string{"warm": {"RED"}, "cool": {"GREEN", "BLUE"}, "all": {"RED", "GREEN", "BLUE"}}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("got %+v, want %+v", got, input)
	}
}

// ---------- odd Go types at depth ----------

func TestDeepNestingTextMarshalerAtDepth(t *testing.T) {
	schema := `{
		"type": "record", "name": "Network", "fields": [
			{"name": "hosts", "type": {"type": "array", "items": {
				"type": "record", "name": "Host", "fields": [
					{"name": "name", "type": "string"},
					{"name": "addr", "type": "string"}
				]
			}}}
		]
	}`
	type Host struct {
		Name string `avro:"name"`
		Addr testIP `avro:"addr"`
	}
	type Network struct{ Hosts []Host `avro:"hosts"` }
	input := Network{Hosts: []Host{
		{Name: "gw", Addr: testIP{192, 168, 1, 1}},
		{Name: "dns", Addr: testIP{8, 8, 8, 8}},
	}}
	got := roundTrip(t, schema, input)
	if got.Hosts[0].Addr != input.Hosts[0].Addr || got.Hosts[1].Addr != input.Hosts[1].Addr {
		t.Fatalf("got %+v", got.Hosts)
	}
}

func TestDeepNestingStructCompositionAtDepth(t *testing.T) {
	// Embedded struct + inline tag in nested records.
	schema := `{
		"type": "record", "name": "Wrapper", "fields": [
			{"name": "items", "type": {"type": "array", "items": {
				"type": "record", "name": "Item", "fields": [
					{"name": "id", "type": "int"},
					{"name": "name", "type": "string"},
					{"name": "x", "type": "int"},
					{"name": "y", "type": "int"},
					{"name": "created", "type": {"type": "long", "logicalType": "timestamp-millis"}}
				]
			}}}
		]
	}`
	type Base struct {
		ID      int32     `avro:"id"`
		Created time.Time `avro:"created"`
	}
	type Coords struct {
		X int32 `avro:"x"`
		Y int32 `avro:"y"`
	}
	type Item struct {
		Base
		Name   string `avro:"name"`
		Coords `avro:",inline"`
	}
	type Wrapper struct{ Items []Item `avro:"items"` }
	ts := time.Date(2025, 3, 19, 10, 0, 0, 0, time.UTC)
	input := Wrapper{Items: []Item{
		{Base: Base{ID: 1, Created: ts}, Name: "first", Coords: Coords{X: 10, Y: 20}},
	}}
	got := roundTrip(t, schema, input)
	if got.Items[0].ID != 1 || got.Items[0].X != 10 || got.Items[0].Y != 20 || !got.Items[0].Created.Equal(ts) {
		t.Fatalf("got %+v", got.Items[0])
	}
}

func TestDeepNestingUintTypesAtDepth(t *testing.T) {
	schema := `{
		"type": "record", "name": "Outer", "fields": [{
			"name": "inner", "type": {
				"type": "record", "name": "Inner", "fields": [
					{"name": "a", "type": "int"},
					{"name": "b", "type": "int"},
					{"name": "c", "type": "long"},
					{"name": "d", "type": "long"}
				]
			}
		}]
	}`
	type Inner struct {
		A uint8  `avro:"a"`
		B uint16 `avro:"b"`
		C uint32 `avro:"c"`
		D uint   `avro:"d"`
	}
	type Outer struct{ Inner Inner `avro:"inner"` }
	input := Outer{Inner: Inner{A: 255, B: 65535, C: 4294967295, D: 123456789}}
	got := roundTrip(t, schema, input)
	if got.Inner != input.Inner {
		t.Fatalf("got %+v", got.Inner)
	}
}

func TestDeepNestingBytesStringCoercionAtDepth(t *testing.T) {
	schema := `{
		"type": "record", "name": "Outer", "fields": [{
			"name": "inner", "type": {
				"type": "record", "name": "Inner", "fields": [
					{"name": "data", "type": "bytes"}
				]
			}
		}]
	}`
	s := mustParse(t, schema)
	type IB struct{ Data []byte `avro:"data"` }
	type OB struct{ Inner IB `avro:"inner"` }
	encoded, err := s.AppendEncode(nil, &OB{Inner: IB{Data: []byte("hello bytes")}})
	if err != nil {
		t.Fatal(err)
	}
	type IS struct{ Data string `avro:"data"` }
	type OS struct{ Inner IS `avro:"inner"` }
	var got OS
	if _, err := s.Decode(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got.Inner.Data != "hello bytes" {
		t.Fatalf("got %q", got.Inner.Data)
	}
}

func TestDeepNestingOmitzeroAtDepth(t *testing.T) {
	schema := `{
		"type": "record", "name": "Outer", "fields": [{
			"name": "inner", "type": {
				"type": "record", "name": "Inner", "fields": [
					{"name": "required", "type": "string"},
					{"name": "optional", "type": ["null", "int"]}
				]
			}
		}]
	}`
	type Inner struct {
		Required string `avro:"required"`
		Optional *int32 `avro:"optional,omitzero"`
	}
	type Outer struct{ Inner Inner `avro:"inner"` }

	v := int32(42)
	got := roundTrip(t, schema, Outer{Inner: Inner{Required: "yes", Optional: &v}})
	if *got.Inner.Optional != 42 {
		t.Fatalf("with value: %+v", got)
	}

	gotZero := roundTrip(t, schema, Outer{Inner: Inner{Required: "bare", Optional: nil}})
	if gotZero.Inner.Optional != nil {
		t.Fatalf("omitzero: %+v", gotZero)
	}
}

// ---------- 10-level record chain with mixed odd types ----------

func TestDeepNesting10LevelMixedTypes(t *testing.T) {
	schema := `{
		"type": "record", "name": "L1", "fields": [
			{"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
			{"name": "l2", "type": {
				"type": "record", "name": "L2", "fields": [
					{"name": "day", "type": {"type": "int", "logicalType": "date"}},
					{"name": "l3", "type": {
						"type": "record", "name": "L3", "fields": [
							{"name": "status", "type": {"type": "enum", "name": "S", "symbols": ["A", "B", "C"]}},
							{"name": "l4", "type": {
								"type": "record", "name": "L4", "fields": [
									{"name": "hash", "type": {"type": "fixed", "name": "H8", "size": 8}},
									{"name": "l5", "type": {
										"type": "record", "name": "L5", "fields": [
											{"name": "dur", "type": {"type": "int", "logicalType": "time-millis"}},
											{"name": "l6", "type": {
												"type": "record", "name": "L6", "fields": [
													{"name": "data", "type": "bytes"},
													{"name": "l7", "type": {
														"type": "record", "name": "L7", "fields": [
															{"name": "tags", "type": {"type": "array", "items": "string"}},
															{"name": "l8", "type": {
																"type": "record", "name": "L8", "fields": [
																	{"name": "meta", "type": {"type": "map", "values": "int"}},
																	{"name": "l9", "type": {
																		"type": "record", "name": "L9", "fields": [
																			{"name": "flag", "type": "boolean"},
																			{"name": "l10", "type": {
																				"type": "record", "name": "L10", "fields": [
																					{"name": "value", "type": "double"},
																					{"name": "label", "type": "string"}
																				]
																			}}
																		]
																	}}
																]
															}}
														]
													}}
												]
											}}
										]
									}}
								]
							}}
						]
					}}
				]
			}}
		]
	}`
	type L10 struct{ Value float64 `avro:"value"`; Label string `avro:"label"` }
	type L9 struct{ Flag bool `avro:"flag"`; L10 L10 `avro:"l10"` }
	type L8 struct{ Meta map[string]int32 `avro:"meta"`; L9 L9 `avro:"l9"` }
	type L7 struct{ Tags []string `avro:"tags"`; L8 L8 `avro:"l8"` }
	type L6 struct{ Data []byte `avro:"data"`; L7 L7 `avro:"l7"` }
	type L5 struct{ Dur time.Duration `avro:"dur"`; L6 L6 `avro:"l6"` }
	type L4 struct{ Hash [8]byte `avro:"hash"`; L5 L5 `avro:"l5"` }
	type L3 struct{ Status string `avro:"status"`; L4 L4 `avro:"l4"` }
	type L2 struct{ Day time.Time `avro:"day"`; L3 L3 `avro:"l3"` }
	type L1 struct{ Ts time.Time `avro:"ts"`; L2 L2 `avro:"l2"` }

	ts := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	day := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
	input := L1{Ts: ts, L2: L2{Day: day, L3: L3{Status: "B", L4: L4{
		Hash: [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		L5: L5{Dur: 5*time.Hour + 30*time.Minute, L6: L6{
			Data: []byte{0xFF, 0x00, 0xAB},
			L7: L7{Tags: []string{"deep", "test"}, L8: L8{
				Meta: map[string]int32{"depth": 10},
				L9:   L9{Flag: true, L10: L10{Value: 3.14159, Label: "bottom"}},
			}},
		}},
	}}}}
	got := roundTrip(t, schema, input)
	if !got.Ts.Equal(ts) || !got.L2.Day.Equal(day) || got.L2.L3.Status != "B" {
		t.Fatalf("top levels: %+v", got)
	}
	if got.L2.L3.L4.L5.Dur != input.L2.L3.L4.L5.Dur {
		t.Fatalf("L5.dur: got %v", got.L2.L3.L4.L5.Dur)
	}
	if !got.L2.L3.L4.L5.L6.L7.L8.L9.Flag || got.L2.L3.L4.L5.L6.L7.L8.L9.L10.Label != "bottom" {
		t.Fatalf("bottom: %+v", got.L2.L3.L4.L5.L6.L7.L8.L9)
	}
}

// ---------- large collection within deep nesting ----------

func TestDeepNestingLargeNestedCollections(t *testing.T) {
	schema := `{
		"type": "record", "name": "Batch", "fields": [
			{"name": "items", "type": {"type": "array", "items": {
				"type": "record", "name": "Row", "fields": [
					{"name": "id", "type": "int"},
					{"name": "attrs", "type": {"type": "map", "values": "string"}}
				]
			}}}
		]
	}`
	type Row struct {
		ID    int32             `avro:"id"`
		Attrs map[string]string `avro:"attrs"`
	}
	type Batch struct{ Items []Row `avro:"items"` }
	items := make([]Row, 100)
	for i := range items {
		attrs := make(map[string]string, 10)
		for j := 0; j < 10; j++ {
			attrs["key"+string(rune('0'+j))] = "val"
		}
		items[i] = Row{ID: int32(i), Attrs: attrs}
	}
	got := roundTrip(t, schema, Batch{Items: items})
	if len(got.Items) != 100 || got.Items[99].ID != 99 || len(got.Items[50].Attrs) != 10 {
		t.Fatalf("got %d items, last=%d, attrs=%d", len(got.Items), got.Items[99].ID, len(got.Items[50].Attrs))
	}
}
