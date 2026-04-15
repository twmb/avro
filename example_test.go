package avro_test

import (
	"fmt"
	"log"
	"net"
	"reflect"
	"time"

	"github.com/twmb/avro"
)

func Example() {
	schema := avro.MustParse(`{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age",  "type": "int"}
		]
	}`)

	type User struct {
		Name string `avro:"name"`
		Age  int32  `avro:"age"`
	}

	data, err := schema.Encode(&User{Name: "Alice", Age: 30})
	if err != nil {
		log.Fatal(err)
	}

	var u User
	if _, err := schema.Decode(data, &u); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s is %d\n", u.Name, u.Age)
	// Output: Alice is 30
}

func ExampleResolve() {
	// v1 wrote User with just a name.
	writerSchema := avro.MustParse(`{
		"type": "record", "name": "User",
		"fields": [{"name": "name", "type": "string"}]
	}`)

	// v2 added an email field with a default.
	readerSchema := avro.MustParse(`{
		"type": "record", "name": "User",
		"fields": [
			{"name": "name",  "type": "string"},
			{"name": "email", "type": "string", "default": ""}
		]
	}`)

	resolved, err := avro.Resolve(writerSchema, readerSchema)
	if err != nil {
		log.Fatal(err)
	}

	// Encode a v1 record (name only).
	v1Data, err := writerSchema.Encode(map[string]any{"name": "Alice"})
	if err != nil {
		log.Fatal(err)
	}

	// Decode old data into the new layout; email gets the default.
	type User struct {
		Name  string `avro:"name"`
		Email string `avro:"email"`
	}
	var u User
	if _, err := resolved.Decode(v1Data, &u); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("name=%s email=%q\n", u.Name, u.Email)
	// Output: name=Alice email=""
}

func ExampleSchema_AppendEncode() {
	schema := avro.MustParse(`"string"`)

	// AppendEncode reuses a buffer across calls, avoiding allocation.
	var buf []byte
	var err error
	for _, s := range []string{"hello", "world"} {
		buf, err = schema.AppendEncode(buf[:0], s)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("encoded %q: %d bytes\n", s, len(buf))
	}
	// Output:
	// encoded "hello": 6 bytes
	// encoded "world": 6 bytes
}

func ExampleSchemaCache() {
	cache := new(avro.SchemaCache)

	// Parse the Address type first.
	if _, err := cache.Parse(`{
		"type": "record",
		"name": "Address",
		"fields": [
			{"name": "street", "type": "string"},
			{"name": "city",   "type": "string"}
		]
	}`); err != nil {
		log.Fatal(err)
	}

	// User references Address by name.
	schema, err := cache.Parse(`{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "name",    "type": "string"},
			{"name": "address", "type": "Address"}
		]
	}`)
	if err != nil {
		log.Fatal(err)
	}

	type Address struct {
		Street string `avro:"street"`
		City   string `avro:"city"`
	}
	type User struct {
		Name    string  `avro:"name"`
		Address Address `avro:"address"`
	}

	data, err := schema.Encode(&User{
		Name:    "Alice",
		Address: Address{Street: "123 Main St", City: "Springfield"},
	})
	if err != nil {
		log.Fatal(err)
	}

	var u User
	if _, err := schema.Decode(data, &u); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s lives at %s, %s\n", u.Name, u.Address.Street, u.Address.City)
	// Output: Alice lives at 123 Main St, Springfield
}

func ExampleSchema_AppendSingleObject() {
	schema := avro.MustParse(`{
		"type": "record",
		"name": "Event",
		"fields": [
			{"name": "id",   "type": "long"},
			{"name": "name", "type": "string"}
		]
	}`)

	type Event struct {
		ID   int64  `avro:"id"`
		Name string `avro:"name"`
	}

	// Encode: 2-byte magic + 8-byte fingerprint + Avro payload.
	data, err := schema.AppendSingleObject(nil, &Event{ID: 1, Name: "click"})
	if err != nil {
		log.Fatal(err)
	}

	// Decode.
	var e Event
	if _, err := schema.DecodeSingleObject(data, &e); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("id=%d name=%s\n", e.ID, e.Name)
	// Output: id=1 name=click
}

func ExampleSchema_EncodeJSON() {
	schema := avro.MustParse(`{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "name",  "type": "string"},
			{"name": "email", "type": ["null", "string"]}
		]
	}`)

	type User struct {
		Name  string  `avro:"name"`
		Email *string `avro:"email"`
	}
	email := "alice@example.com"
	u := User{Name: "Alice", Email: &email}

	// Default: bare union values.
	bare, err := schema.EncodeJSON(&u)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(bare))

	// TaggedUnions: wrapped as {"type": value}.
	tagged, err := schema.EncodeJSON(&u, avro.TaggedUnions())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(tagged))
	// Output:
	// {"name":"Alice","email":"alice@example.com"}
	// {"name":"Alice","email":{"string":"alice@example.com"}}
}

func ExampleSchema_DecodeJSON() {
	schema := avro.MustParse(`{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "name",  "type": "string"},
			{"name": "email", "type": ["null", "string"]}
		]
	}`)

	type User struct {
		Name  string  `avro:"name"`
		Email *string `avro:"email"`
	}

	// DecodeJSON accepts both bare and tagged union formats.
	var u1, u2 User
	if err := schema.DecodeJSON([]byte(`{"name":"Alice","email":"a@b.com"}`), &u1); err != nil {
		log.Fatal(err)
	}
	if err := schema.DecodeJSON([]byte(`{"name":"Bob","email":{"string":"b@c.com"}}`), &u2); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s: %s\n", u1.Name, *u1.Email)
	fmt.Printf("%s: %s\n", u2.Name, *u2.Email)
	// Output:
	// Alice: a@b.com
	// Bob: b@c.com
}

func ExampleSchemaFor() {
	type Event struct {
		ID     int64     `avro:"id"`
		Name   string    `avro:"name,default=unnamed"`
		Source string    `avro:"source,default=web"`
		Time   time.Time `avro:"ts"`
		Meta   *string   `avro:"meta"` // *T becomes ["null", T] union
	}

	schema := avro.MustSchemaFor[Event](avro.WithNamespace("com.example"))

	// Encode, then decode back.
	meta := "test"
	data, err := schema.Encode(&Event{
		ID:     1,
		Name:   "click",
		Source: "mobile",
		Time:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Meta:   &meta,
	})
	if err != nil {
		log.Fatal(err)
	}

	var out Event
	if _, err := schema.Decode(data, &out); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("id=%d name=%s source=%s meta=%s\n", out.ID, out.Name, out.Source, *out.Meta)

	// Inspect the inferred schema.
	root := schema.Root()
	for _, f := range root.Fields {
		if f.HasDefault {
			fmt.Printf("field %s: default=%v\n", f.Name, f.Default)
		}
	}
	// Output:
	// id=1 name=click source=mobile meta=test
	// field name: default=unnamed
	// field source: default=web
	// field meta: default=<nil>
}

func ExampleSchema_Encode_textMarshaler() {
	// Types implementing encoding.TextMarshaler are encoded as Avro
	// strings, and encoding.TextUnmarshaler types decode from them.
	schema := avro.MustParse(`{
		"type": "record",
		"name": "Server",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "ip",   "type": "string"}
		]
	}`)

	type Server struct {
		Name string `avro:"name"`
		IP   net.IP `avro:"ip"`
	}

	data, err := schema.Encode(&Server{
		Name: "web-1",
		IP:   net.IPv4(192, 168, 1, 1),
	})
	if err != nil {
		log.Fatal(err)
	}

	var out Server
	if _, err := schema.Decode(data, &out); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s: %s\n", out.Name, out.IP)
	// Output: web-1: 192.168.1.1
}

type ExMoney struct {
	Cents int64
}

func ExampleNewCustomType() {
	// NewCustomType is the easiest way to map a custom Go type to/from a
	// primitive Avro type. The type parameters wire everything up:
	//   G = your Go type, A = the Avro-native Go type it maps to.
	//
	// A is the raw type on the wire:
	//   int32 → Avro int       float32 → Avro float     bool   → Avro boolean
	//   int64 → Avro long      float64 → Avro double    string → Avro string
	//   []byte → Avro bytes
	//
	// The first argument is the logicalType to match. Pass "" to match
	// all schema nodes of the inferred Avro type.
	moneyType := avro.NewCustomType[ExMoney, int64]("money",
		func(m ExMoney, _ *avro.SchemaNode) (int64, error) { return m.Cents, nil },
		func(c int64, _ *avro.SchemaNode) (ExMoney, error) { return ExMoney{Cents: c}, nil },
	)

	schema := avro.MustParse(`{
		"type": "record", "name": "Order",
		"fields": [
			{"name": "price", "type": {"type": "long", "logicalType": "money"}}
		]
	}`, moneyType)

	type Order struct {
		Price ExMoney `avro:"price"`
	}

	data, err := schema.Encode(&Order{Price: ExMoney{Cents: 1999}})
	if err != nil {
		log.Fatal(err)
	}

	var out Order
	if _, err := schema.Decode(data, &out); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%d cents\n", out.Price.Cents)
	// Output: 1999 cents
}

func ExampleCustomType_override() {
	// Use CustomType directly to override a built-in logical type handler.
	// Here we suppress the timestamp-millis → time.Time conversion and
	// keep the raw int64 epoch millis.
	schema := avro.MustParse(`{
		"type": "record", "name": "Event",
		"fields": [
			{"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}}
		]
	}`, avro.CustomType{
		LogicalType: "timestamp-millis",
		Decode: func(v any, _ *avro.SchemaNode) (any, error) {
			return v, nil // pass through raw int64
		},
	})

	data, _ := schema.Encode(map[string]any{"ts": int64(1767225600000)})
	var out any
	schema.Decode(data, &out)
	m := out.(map[string]any)
	fmt.Printf("ts type: %T\n", m["ts"])
	// Output: ts type: int64
}

func ExampleCustomType_schemaFor() {
	// Setting GoType lets SchemaFor infer the Avro schema for struct
	// fields of that type. Without GoType, SchemaFor doesn't know that
	// a Cents field should map to {"type":"long","logicalType":"money"}.
	type Cents int64
	ct := avro.CustomType{
		LogicalType: "money",
		AvroType:    "long",
		GoType:      reflect.TypeFor[Cents](),
		Encode: func(v any, _ *avro.SchemaNode) (any, error) {
			return int64(v.(Cents)), nil
		},
		Decode: func(v any, _ *avro.SchemaNode) (any, error) {
			return Cents(v.(int64)), nil
		},
	}

	type Order struct {
		Price Cents `avro:"price"`
	}
	schema, err := avro.SchemaFor[Order](ct)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(schema.Root().Fields[0].Type.LogicalType)
	// Output: money
}

func ExampleCustomType_propertyDispatch() {
	// CustomType with no LogicalType/AvroType/GoType matches ALL schema
	// nodes. Use ErrSkipCustomType to selectively handle nodes based on
	// schema properties, e.g. Kafka Connect type annotations.
	ct := avro.CustomType{
		Decode: func(v any, node *avro.SchemaNode) (any, error) {
			if node.Props["connect.type"] == "double-it" {
				return v.(int64) * 2, nil
			}
			return nil, avro.ErrSkipCustomType
		},
	}

	// Properties on the type object are available via node.Props in the
	// custom type callback.
	schema := avro.MustParse(`{
		"type": "record", "name": "R",
		"fields": [
			{"name": "x", "type": {"type": "long", "connect.type": "double-it"}},
			{"name": "y", "type": "long"}
		]
	}`, ct)

	data, _ := schema.Encode(map[string]any{"x": int64(5), "y": int64(5)})
	var out any
	schema.Decode(data, &out)
	m := out.(map[string]any)
	fmt.Printf("x=%d y=%d\n", m["x"], m["y"])
	// Output: x=10 y=5
}
