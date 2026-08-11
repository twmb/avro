package avro_test

// Schema fixtures used by more than one test. Spelling them once means a
// cell shows what it varies instead of burying it in near-identical JSON.

// Node{value:int, next:[null,Node]} — a self-referential record.
const nodeRecursiveSchema = `{
		"type": "record",
		"name": "Node",
		"fields": [
			{"name": "value", "type": "int"},
			{"name": "next", "type": ["null", "Node"]}
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

// R{f: [A{x:int}, "A"]} — a union naming A inline then by reference.
const recUnionInlineThenRefSchema = `{"type":"record","name":"R","fields":[
			{"name":"f","type":[
				{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]},
				"A"
			]}
		]}`

// R{x:int}.
const recXSchema = `{"type":"record","name":"R","fields":[
			{"name":"x","type":"int"}
		]}`

// R{x:int, y:int}.
const recXYSchema = `{"type":"record","name":"R","fields":[
			{"name":"x","type":"int"},
			{"name":"y","type":"int"}
		]}`
