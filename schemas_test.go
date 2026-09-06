package avro

// Schema fixtures used by more than one test. Spelling them once means a
// cell shows what it varies instead of burying it in near-identical JSON.

// Wrapper{vals: array of int}.
const arrayOfIntSchema = `{
			"type": "record",
			"name": "Wrapper",
			"fields": [{"name": "vals", "type": {"type": "array", "items": "int"}}]
		}`

// Outer{items: array of Inner{p:int}}.
const arrayOfPtrInnerSchema = `{
			"type": "record",
			"name": "Outer",
			"fields": [{"name": "items", "type": {"type": "array", "items": {
				"type": "record", "name": "Inner",
				"fields": [{"name": "p", "type": "int"}]
			}}}]
		}`

// Wrapper{items: array of Rec{v:int}}, the ptr-record array path.
const arrayOfPtrRecSchema = `{
			"type": "record",
			"name": "Wrapper",
			"fields": [{"name": "items", "type": {"type": "array", "items": {
				"type": "record", "name": "Rec",
				"fields": [{"name": "v", "type": "int"}]
			}}}]
		}`

// iface{s: Foobar{f:int}}, a record field holding a record.
const ifaceFoobarSchema = `{
		"type": "record",
		"name": "iface",
		"fields" : [
			{
				"name": "s", "type": {
					"type": "record",
					"name": "Foobar",
					"fields": [
						{"name": "f", "type": "int"}
					]
				}
			}
		]
	}`

// LongList aliased as LinkedLongs, recursive through next.
const longListAliasSchema = `{
		"type": "record",
		"name": "LongList",
		"aliases": ["LinkedLongs"],
		"fields" : [
			{"name": "value", "type": "long"},
			{"name": "next", "type": ["null", "LongList"]}
		]
	}`

// LongList{value:long, next:[null,LongList]}.
const longListSchema = `{
		"type": "record",
		"name": "LongList",
		"fields": [
			{"name": "value", "type": "long"},
			{"name": "next", "type": ["null", "LongList"]}
		]
	}`

// Outer{inner: Inner{x:int}}.
const nestedInnerSchema = `{
		"type": "record",
		"name": "Outer",
		"fields": [{"name": "inner", "type": {
			"type": "record", "name": "Inner",
			"fields": [{"name": "x", "type": "int"}]
		}}]
	}`

// Node{value:int, next:[null,Node]}. A self-referential record.
const nodeRecursiveSchema = `{"type":"record","name":"Node","fields":[
		{"name":"value","type":"int"},
		{"name":"next","type":["null","Node"]}
	]}`

// Wrapper{value: [null, int]}.
const nullableIntSchema = `{
		"type": "record",
		"name": "Wrapper",
		"fields": [{"name": "value", "type": ["null", "int"]}]
	}`

// Order{id:long, price: long with logicalType money}.
const orderIDPriceSchema = `{
		"type":"record","name":"Order","fields":[
			{"name":"id","type":"long"},
			{"name":"price","type":{"type":"long","logicalType":"money"}}
		]
	}`

// prims: one field per primitive type.
const primsSchema = `{
		"type":"record","name":"prims","fields":[
			{"name":"b","type":"boolean"},
			{"name":"i","type":"int"},
			{"name":"l","type":"long"},
			{"name":"f","type":"float"},
			{"name":"d","type":"double"},
			{"name":"s","type":"string"},
			{"name":"bs","type":"bytes"}
		]
	}`

// R{a:int, b:string}.
const recABSchema = `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`

// R{a:int}.
const recASchema = `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"}
	]}`

// r{a:int, b:string}. Lowercase record name.
const recIntBSchema = `{"type":"record","name":"r","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`

// r{a:long, b:string}. Lowercase record name.
const recLongBSchema = `{"type":"record","name":"r","fields":[
		{"name":"a","type":"long"},
		{"name":"b","type":"string"}
	]}`

// R{ts: long with logicalType timestamp-millis}.
const recTimestampMillisSchema = `{"type":"record","name":"R","fields":[
				{"name":"ts","type":"long","logicalType":"timestamp-millis"}
			]}`

// R{id: fixed(16) with logicalType uuid}.
const recUUIDFixedSchema = `{"type":"record","name":"R","fields":[
			{"name":"id","type":{"type":"fixed","name":"u","size":16,"logicalType":"uuid"}}
		]}`

// R{id: string with logicalType uuid}.
const recUUIDStringSchema = `{"type":"record","name":"R","fields":[
		{"name":"id","type":{"type":"string","logicalType":"uuid"}}
	]}`

// Record{name:string, email:[null,string]}.
const recordNameEmailSchema = `{"type":"record","name":"Record","fields":[
		{"name":"name","type":"string"},
		{"name":"email","type":["null","string"]}
	]}`

// strings: ten string fields, for wide-record work.
const stringsSchema = `{"type":"record","name":"strings","fields":[
		{"name":"s1","type":"string"},
		{"name":"s2","type":"string"},
		{"name":"s3","type":"string"},
		{"name":"s4","type":"string"},
		{"name":"s5","type":"string"},
		{"name":"s6","type":"string"},
		{"name":"s7","type":"string"},
		{"name":"s8","type":"string"},
		{"name":"s9","type":"string"},
		{"name":"s10","type":"string"}
	]}`

// The Superhero record wrapped in ["null", ...].
const superheroUnionSchema = `
["null",
{
"name": "Superhero",
"type": "record",
"fields": [
	{"name": "id", "type": "int"},
	{"name": "affiliation_id", "type": "int"},
	{"name": "name", "type": "string"},
	{"name": "life", "type": "float"},
	{"name": "energy", "type": "float"},
	{"name": "powers", "type": {
		"type": "array",
		"items": {
			"name": "Superpower",
			"type": "record",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"},
				{"name": "damage", "type": "float"},
				{"name": "energy", "type": "float"},
				{"name": "passive", "type": "boolean"}
			]
		}
	}}
]
}]`
