package avro

// The predicate-agreement census.
//
// A schema question — "is this branch the null type", "does this Type name
// this definition", "what will json.Marshal emit for this value" — usually
// has to be answered in more than one place, because the same schema exists
// in several REPRESENTATIONS at once: the as-written parse tree (aschema),
// the compiled wire tree (schemaNode), the metadata tree (SchemaNode), the
// pre-Parse `any` tree SchemaFor composes, and the cache's raw JSON tree.
// Every answerer is written by hand, and a hand-written answer is a snapshot
// of the rule at the moment it was typed.
//
// Two failure modes follow, and both have shipped:
//
//   - two answerers of one question DISAGREE, so semantically identical
//     inputs take different paths depending on which representation the code
//     happened to consult;
//   - an answerer restates an EXTERNAL authority's accept-set — the name
//     resolver's binding spellings, encoding/json's key resolver — and the
//     restatement is narrower than the authority, so valid input is refused
//     or an unguarded case panics.
//
// Neither is reachable by generating more INPUTS, which is what every other
// net in this package does: an input matrix is derived from the bug that
// motivated it, so it holds the set of implementations constant and cannot
// see a new one. This census generates over IMPLEMENTATIONS instead. For
// each question it names the canonical predicate (or the external
// authority), every answerer across every representation, and a corpus
// spanning the question's domain; the driver runs every answerer over the
// whole corpus and requires identical verdicts, and where the authority is
// external it is EXECUTED and compared against, never restated.
//
// The drift guard (TestCensus_NoUnregisteredAnswerers) reads the package
// sources and requires every syntactic occurrence of a question's tell to be
// a registered site, so a new hand-written answerer cannot land unexamined.
//
// Adding a predicate, or editing one, means updating this registry. That is
// the point: the registry is the list of places an answer can drift.

import (
	"bytes"
	"encoding/json"
	"errors"

	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// censusAnswerer is one site that answers a question. note is empty when the
// site routes through the question's canonical predicate; a non-empty note
// is the documented reason this site CANNOT collapse into it. An
// undocumented second answerer is a finding, so "" plus a site that does not
// call the canonical predicate is exactly what review should catch.
type censusAnswerer struct {
	repr string // which representation of the schema this site reads
	site string // function or identifier, for the failure message
	file string
	note string
}

// censusTell is a source substring whose every occurrence in non-test code
// must be a registered site of the question. counts is file → number of
// registered occurrences; both a new file and a new occurrence in a known
// file fail the guard.
type censusTell struct {
	pattern string
	counts  map[string]int
}

type censusQuestion struct {
	id        string
	question  string
	authority string // the canonical predicate, or the external authority
	answerers []censusAnswerer
	tells     []censusTell
}

// censusRegistry is the enumeration. It grows one question per commit; the
// floor in the vacuity check below rises with it, so the registry cannot
// silently shrink back.
var censusRegistry = []censusQuestion{
	{
		id:        "Q1",
		question:  "Is this union branch / type the null type?",
		authority: "one predicate per representation: aschema.isNullBranch (as-written), isNullBranchTree (pre-Parse any tree), and the builder-normalized schemaNode.kind (compiled + metadata)",
		answerers: []censusAnswerer{
			{repr: "as-written aschema", site: "aschema.isNullBranch", file: "schema.go"},
			{repr: "pre-Parse any tree", site: "isNullBranchTree", file: "schema_for.go"},
			{
				repr: "compiled schemaNode", site: "kind == \"null\" (json_codec.go, json_decode.go, resolve.go)", file: "json_codec.go",
				note: "different-by-design: the BUILDER normalizes both spellings into schemaNode.kind, so by the time these sites run there is only one spelling left to compare. A shared predicate here would wrap a field read that is already canonical. The census driver is what proves the normalization actually holds — it asks these sites the same question as the two as-written predicates and requires the same answer.",
			},
		},
		tells: []censusTell{
			{pattern: `== "null"`, counts: map[string]int{
				"json_codec.go":  7,
				"json_decode.go": 2,
				"resolve.go":     1,
				"schema_for.go":  2, // both inside isNullBranchTree
				"schema.go":      2, // both inside isNullBranch
			}},
			{pattern: `!= "null"`, counts: map[string]int{
				"json_codec.go": 1,
				// Not answerers of this question, and registered so the guard
				// does not flag them: json_scan.go matches the four bytes of
				// the JSON literal `null` in a VALUE stream, and ocf.go
				// compares a compression CODEC's name.
				"json_scan.go": 1,
				"ocf/ocf.go":   1,
			}},
		},
	},
	{
		id:        "Q2",
		question:  "What fullname does this named-type DEFINITION occupy?",
		authority: "the wire builder's registration (schema.go, `o.Name = ns + \".\" + o.Name` feeding registerNamed) — every other representation must land on the same string, because the fullname is what a reference binds to",
		answerers: []censusAnswerer{
			{repr: "as-written aschema → compiled schemaNode", site: "builder namespace qualification → schemaNode.name", file: "schema.go"},
			{repr: "metadata SchemaNode", site: "nodeFullname", file: "schema_node.go"},
			{repr: "cache raw JSON tree", site: "nodeFullnameTree", file: "cache.go"},
			{
				repr: "pre-Parse any tree", site: "SchemaFor's namespace joins", file: "schema_for.go",
				note: "not driven by the census: SchemaFor COMPOSES a tree rather than reading one, so it has no definition in hand to ask about. Its output is checked instead by handing the emitted schema to Parse — the authority above — in the SchemaFor round-trip suites.",
			},
		},
		tells: []censusTell{
			{pattern: `+ "." +`, counts: map[string]int{
				"cache.go":       1, // nodeFullnameTree
				"schema_node.go": 1, // nodeFullname
				"schema_for.go":  2, // SchemaFor composition
				"schema.go":      2, // builder qualification (2727); logical key (2165) — not this question
				// Not answerers: an error FIELD PATH join, not a schema name.
				"compat.go": 1,
				"errors.go": 1,
				// Not an answerer: a kind.logicalType lookup key.
				"json_codec.go": 1,
			}},
			{pattern: `+"."+`, counts: map[string]int{
				"schema.go": 1, // scopedRefKeys — the binding side of the same rule
				"cache.go":  1, // dupDefRef — which spelling re-binds after a splice
			}},
		},
	},
	{
		id:       "Q3",
		question: "What does json.Marshal emit as the object key for this Go map key?",
		authority: "EXTERNAL: encoding/json's resolveKeyName. It is executed per corpus cell by " +
			"TestRegression_WalkBudgetMapKeyMatchesJSONKeyResolver, which compares against json.Marshal's " +
			"actual output rather than any restatement of its rules — the whole point, since the two bugs " +
			"in this area were both a restatement that was narrower than the authority",
		answerers: []censusAnswerer{
			{repr: "caller `any` tree, budget walk", site: "mapKeyEmitLen", file: "schema_node.go"},
			{
				repr: "caller `any` tree, fixup + canonicalize walk", site: "canonicalStringKeyMap", file: "schema_node.go",
				note: "different-by-design: this arm asks a NARROWER question — 'do these keys canonicalize to their plain string value', which is true only for the string KIND. A non-string key's object-key form comes from its MarshalText, so such maps stay marshal-opaque and are never rewritten. It must not be collapsed into mapKeyEmitLen, whose question is 'how many bytes does this key cost'; the two agree on the string-kind arm and are deliberately different elsewhere.",
			},
			// SchemaFor's tree canonicalizer (canonicalizeTreeValue,
			// schema_for.go) is NOT a separate answerer: it calls
			// canonicalStringKeyMap. That is the shape this census wants —
			// a second representation consuming the one predicate instead
			// of restating it — and it is recorded here so a later edit
			// that inlines the check reads as the regression it would be.
		},
		tells: []censusTell{
			{pattern: `.Key().Kind()`, counts: map[string]int{
				"schema_node.go": 1, // canonicalStringKeyMap
				// Not answerers of this question: these decide the Go type of
				// an AVRO map's keys for encode/decode/resolve/SchemaFor,
				// where the key form is Avro's own string wire, not a JSON
				// object key.
				"deser.go":       2,
				"json_codec.go":  2,
				"json_decode.go": 2,
				"resolve.go":     1,
				"schema_for.go":  1,
				"ser.go":         3,
			}},
			{pattern: `k.Kind() == reflect.String`, counts: map[string]int{
				"schema_node.go": 1, // mapKeyEmitLen's string-kind arm
			}},
		},
	},
	{
		id:       "Q9",
		question: "What will json.Marshal emit for this caller-supplied value?",
		authority: "EXTERNAL: encoding/json. Executed per cell by TestCensus_Q9_EmissionRouteChargeTracksJSON, " +
			"which compares the walk's charge against json.Marshal's ACTUAL output on the same value. " +
			"This is the authority behind both of the fixes that motivated the census, and the widest " +
			"answerer set in the package: the schema-tree budget models json.Marshal's recursion, and " +
			"anything it fails to model is emitted for free",
		answerers: []censusAnswerer{
			{repr: "caller `any` tree, budget walk", site: "valueWalkLimit + marshalEmitLen + mapKeyEmitLen", file: "schema_node.go"},
			{
				repr: "caller `any` tree, escaped-length scan", site: "jsonEscapedLen / jsonEscapedLenBytes / asciiEscapedLen / avroCodepointEscapedLen / compactedEmitLen", file: "schema_node.go",
				note: "RESTATES the authority rather than delegating, which this census otherwise treats as the bug. Permitted only because delegation is impossible for MEASUREMENT: asking the emitter how long its output is means producing that output, which is the allocation the budget exists to prevent. The licence is conditional on the executed differential over the authority's COMPLETE domain — every one of the 256 single-byte values, the multi-byte runes, invalid UTF-8, the HTML trio and the two-character escapes — derived from marshalSchemaTree, the package's own emitter, so a future SetEscapeHTML(false) moves the expectation and reds this until the restatement follows. Escaping below utf8.RuneSelf is byte-LOCAL, so per-byte totality is a proof over that part of the domain, not a sample of it.",
			},
			{
				repr: "caller `any` tree, fixup detection", site: "treeValueMarshalOpaque / needsJSONFixupKind", file: "schema_node.go",
				note: "different-by-design: asks whether a value's JSON form is SELF-DEFINED (so the fixups must leave it alone), not how many bytes it costs. Same authority, different projection of it — a value can be opaque and cheap, or transparent and huge.",
			},
			{
				repr: "caller `any` tree, canonicalization", site: "canonicalByteSliceKind / sliceElemMarshalPositionDependent", file: "schema_node.go",
				note: "different-by-design: asks whether a value's marshal is INDISTINGUISHABLE from its canonical twin's, so the tree can be rewritten without changing the emission. Mirrors newSliceEncoder's byte-slice rule and json's addressability rule respectively.",
			},
			{
				repr: "SchemaFor pre-Parse tree", site: "deepCopyJSONTree's slice arm", file: "schema_for.go",
				note: "different-by-design: the same canonicalization question on the tree SchemaFor composes. It consults the same jsonMarshalerType/textMarshalerType reflect vars rather than re-deriving the rule.",
			},
		},
		tells: []censusTell{
			{pattern: `jsonMarshalerType`, counts: map[string]int{
				"schema_node.go": 4,
				"schema_for.go":  1,
			}},
			{pattern: `json.Marshaler`, counts: map[string]int{
				"schema_node.go": 6,
				// Not an answerer: a COMMENT recording that aschema is
				// deliberately NOT a json.Marshaler (so the stdlib decoder does
				// not re-scan each nested subtree).
				"schema.go": 1,
			}},
			// Rejected tells, recorded so the next question's design starts
			// from evidence rather than from scratch:
			//   `MarshalText()`        — 4 hits, but reflect.go:161 and
			//     ser.go:1063 are the AVRO ENCODE path (Q13, text-interface
			//     precedence). A tell that spans two questions cannot fail
			//     for one of them.
			//   `encoding.TextMarshaler` — 16 hits across doc.go, reflect.go
			//     and ser.go, dominated by that same encode question. Same
			//     defect, larger.
			//   `reflect.Kind` switches — matches nearly every walker in the
			//     package; a tell that matches everything reports nothing.
			// The usable pair above is narrow because both names exist ONLY
			// to answer "does this type define its own JSON form".
		},
	},
	{
		id:       "Q14",
		question: "Does this registered CustomType match this schema node (kind + logicalType)?",
		authority: "no external authority. hasMatchingCustomTypeCond (schema.go) is the build-time answer that decides " +
			"codec suppression; CustomType.matches is the runtime answer. The two must agree on the BASE rule -- an " +
			"empty field means \"any\", a set field must equal the node's -- and they implement it separately",
		answerers: []censusAnswerer{
			{repr: "build-time suppression", site: "hasMatchingCustomTypeCond", file: "schema.go"},
			{repr: "runtime dispatch", site: "CustomType.matches", file: "custom_type.go"},
		},
		tells: []censusTell{
			{pattern: `.AvroType != "" &&`, counts: map[string]int{
				"schema.go":      1, // hasMatchingCustomTypeCond
				"custom_type.go": 1, // CustomType.matches
			}},
			{pattern: `.LogicalType != "" &&`, counts: map[string]int{
				"schema.go":      1,
				"custom_type.go": 1,
			}},
			// Rejected tell: `matches(` — too generic, and it names only one
			// of the two answerers, so the site most likely to drift away
			// (the hand-rolled clause pair inside the build-time loop) would
			// not be watched at all.
		},
	},
	{
		id:       "Q5",
		question: "Does a field-level decimal lift CONSUME precision/scale — so a malformed body must reject rather than ride to Props?",
		authority: "decimalConsumesPrecisionScale (schema_node.go) owns the carrier test and both sides delegate to it. " +
			"What is answered TWICE is the NAVIGATION to the lift target: fieldDecimalLiftConsumesPrecisionScale " +
			"computes the verdict, liftFieldLogicalIntoType performs the move, and the verdict's comment claims to " +
			"mirror the lift. They drifted before, on the wrapped-null branch",
		answerers: []censusAnswerer{
			{repr: "as-written aschema, SHARED navigation", site: "liftTarget + liftEffectiveLogical", file: "schema.go"},
			{repr: "as-written aschema, verdict", site: "fieldDecimalLiftConsumesPrecisionScale (reads through liftEffectiveLogical)", file: "schema.go"},
			{repr: "as-written aschema, mutation", site: "liftFieldLogicalIntoType (moves through liftTarget)", file: "schema.go"},
			{
				repr: "compiled schemaNode + metadata", site: "decimalConsumesPrecisionScale call sites", file: "schema_node.go",
				note: "not a separate answerer: the shared carrier test, consulted by the render and Props routing. Registered so the guard watches its count — a new hand-rolled bytes/fixed check beside it would be the drift.",
			},
		},
		tells: []censusTell{
			{pattern: `decimalConsumesPrecisionScale`, counts: map[string]int{
				"schema_node.go":  5,
				"schema_parse.go": 2,
				"schema.go":       3,
			}},
			// Rejected tell: `Logical == ""` — 6 hits in schema.go, three of
			// them the lift's closer-to-the-type gates and three unrelated
			// (canonical emission, logical dispatch). It spans two questions,
			// so it can never fail cleanly for either.
		},
	},
	{
		id:       "Q10",
		question: "Is this Go value nil-equivalent — does it encode as Avro null?",
		authority: "isNilValue (ser.go) is the shared predicate, and its own doc enumerates the five dispatch " +
			"sites that must agree. serNull peels SEPARATELY rather than calling it, and the two have drifted: " +
			"a fix once claimed parity but added only Interface peeling, leaving &nilPtr rejected",
		answerers: []censusAnswerer{
			{repr: "reflect value, shared predicate", site: "isNilValue", file: "ser.go"},
			{
				repr: "binary null encoder", site: "serNull's own peel loop", file: "ser.go",
				note: "different-by-design as an IMPLEMENTATION, not as an answer: serNull is the null type's encoder and runs before any union dispatch, so it cannot consult a predicate written for the union arms without inverting the call order. It owes the identical accept set, which the driver checks by routing the same values through the try-each path.",
			},
			{repr: "JSON null arm + 2-branch short-circuit", site: "appendAvroJSON / appendAvroJSONUnion", file: "json_codec.go"},
			{
				repr: "unsafe struct fast path", site: "usNullUnionEnter / usArrayNullUnionPtr", file: "unsafe.go",
				note: "different-by-design: it holds an unsafe.Pointer, not a reflect.Value, so it CANNOT call isNilValue. It tests only the outer pointer — which equals isNilValue exactly when the inner kind is not itself nilable — and its tryCompileFieldSer gate declines every isNilableKind inner to the reflect path. That is lockstep by exclusion rather than by sharing, and the struct-field driver is what proves the exclusion holds.",
			},
		},
		tells: []censusTell{
			{pattern: `isNilValue`, counts: map[string]int{
				"ser.go":        13,
				"json_codec.go": 5,
				"unsafe.go":     4,
				"reflect.go":    1,
			}},
			{pattern: `isNilableKind`, counts: map[string]int{
				"ser.go":    5,
				"unsafe.go": 4,
			}},
			// Rejected tell: `IsNil()` — 41 hits across 11 files, and most
			// answer a different question entirely (is this reflect.Value
			// safe to deref, is this pointer field set). A tell that broad
			// cannot fail for THIS question.
		},
	},
	{
		id:       "Q13",
		question: "Which text route does this type take — its MarshalText, its raw string kind, or raw bytes?",
		authority: "NOT_BUGS #39's precedence order, enforced by two gates: stringFastPathEligibleEncode / " +
			"stringFastPathEligibleDecode (reflect.go) are the single source of truth for which types may ride " +
			"the raw-string fast paths, and the fast paths themselves are the second answerer — they read the " +
			"underlying string directly and bypass the text arm, so the EXCLUSION LIST is the sibling set",
		answerers: []censusAnswerer{
			{repr: "type-level gate", site: "stringFastPathEligibleEncode / Decode", file: "reflect.go"},
			{repr: "value-level dispatch", site: "textOutFor / textValue", file: "reflect.go"},
			{
				repr: "unsafe + container fast paths", site: "usString / usFixedUUIDString / array + map fast loops", file: "unsafe.go",
				note: "different-by-design: these CANNOT consult the value-level dispatch — they exist to skip it — so they consult the type-level gate at compile time instead, once per type rather than per value. Agreement is therefore a property of the gate's exclusion list, which is what the driver checks by encoding the same value at every position and requiring the same bytes.",
			},
			{
				repr: "uuid raw-bytes exception", site: "isUUIDType + the [16]byte arms", file: "ser.go",
				note: "different-by-design and documented (#39 rule 1): a [16]byte-shaped uuid TRUSTS its raw bytes and does not consult the text interface at all, because the 16 bytes ARE the uuid and a round trip through a non-canonical text method would diverge binary from JSON.",
			},
		},
		tells: []censusTell{
			{pattern: `stringFastPathEligible`, counts: map[string]int{
				"reflect.go": 4,
				"unsafe.go":  10,
				"deser.go":   2,
			}},
			{pattern: `implementsTextMarshaler`, counts: map[string]int{
				"reflect.go":    5,
				"schema_for.go": 2,
			}},
			// Rejected tell: `MarshalText()` — it names the CALL, not the
			// routing decision, and spans the schema-tree budget's emission
			// question (Q9) as well. `textOutFor` was also rejected: it is
			// the shared helper, so counting it misses exactly the sites that
			// bypass it, which are the ones that can drift.
		},
	},
	{
		id:       "Q15",
		question: "Is this kind a NAMED type (occupies a fullname others can reference), and is it a RECORD?",
		authority: "isNamedKind / isRecordKind (schema_node.go) are the shared predicates. The same " +
			"classification is ALSO written as literal case sets in four other files, which is why the " +
			"question is here: those copies cannot call the predicates from inside a switch, so they can " +
			"only be kept honest by driving the classification's observable consequence",
		answerers: []censusAnswerer{
			{repr: "shared predicates", site: "isNamedKind / isRecordKind", file: "schema_node.go"},
			{
				repr: "compat + JSON codec literal sets", site: `case "record", "enum", "fixed":`, file: "compat.go",
				note: "different-by-design as a FORM, not as an answer: a switch arm cannot call a predicate and still be a switch arm. They owe the identical classification, which the driver checks through the property that defines it — whether a definition of that kind can be referenced by name.",
			},
			{
				repr: "canonical + parse literal sets", site: `case "record", "error":`, file: "schema_canonical.go",
				note: "same form-vs-answer split for the RECORD half: canonical emission and the parse arm both spell the record kinds literally.",
			},
		},
		tells: []censusTell{
			{pattern: `isNamedKind`, counts: map[string]int{
				"cache.go": 3, "schema_canonical.go": 1, "schema_for.go": 4,
				"schema_node.go": 14, "schema_parse.go": 1, "schema_walk.go": 2, "schema.go": 5,
			}},
			{pattern: `isRecordKind`, counts: map[string]int{
				"schema_for.go": 2, "schema_node.go": 10, "schema_parse.go": 1, "schema_walk.go": 1,
			}},
			{pattern: `"record", "enum", "fixed"`, counts: map[string]int{
				"compat.go": 1, "json_codec.go": 2,
			}},
			{pattern: `"record", "error"`, counts: map[string]int{
				"schema_canonical.go": 1, "schema_node.go": 3, "schema_parse.go": 1, "schema.go": 3,
			}},
			// Rejected tell: `== "record"` — it also matches the RECURSION
			// question (json_decode.go's `kind == "record" || kind == "array"
			// || kind == "map"`, which asks whether a kind nests, not whether
			// it is named). Two questions, one tell.
		},
	},
	{
		id:       "Q8",
		question: "Does this struct tag SKIP the field?",
		authority: "no external authority: `tag == \"-\"` is spelled identically at all four sites and the " +
			"invariant is AGREEMENT between the two subsystems — SchemaFor decides what a generated schema " +
			"CONTAINS, the runtime field mapper decides what an encode/decode BINDS, and a field excluded by " +
			"one must be excluded by the other",
		answerers: []censusAnswerer{
			{repr: "SchemaFor, named-field path", site: `tag == "-"`, file: "schema_for.go"},
			{repr: "SchemaFor, anonymous-embed path", site: `tag == "-"`, file: "schema_for.go"},
			{repr: "runtime mapper, both paths", site: `tag == "-"`, file: "reflect.go"},
			{
				repr: "grammar guard (SchemaFor only)", site: "checkSkipDirectiveExact", file: "schema_for.go",
				note: "different-by-design and SINGLE-answerer by intent: rejecting a tag that begins with the directive without being it is a typo SchemaFor can name while generating. The runtime mapper has never enforced tag grammar — it takes any tag as a field name, which is what it does with every other malformed tag — so extending the guard there would be a behavior change to a documented boundary, not a consistency fix. Both directions are asserted so neither side can drift into the other.",
			},
		},
		tells: []censusTell{
			{pattern: `tag == "-"`, counts: map[string]int{
				// The two paths, plus one occurrence inside the guard's own
				// doc comment describing where it is called from.
				"schema_for.go": 3,
				"reflect.go":    2, // the runtime mapper's two paths
			}},
			{pattern: `checkSkipDirectiveExact`, counts: map[string]int{
				// Definition, both call sites, and two doc references.
				"schema_for.go": 5,
			}},
			// Rejected tell: `HasPrefix(tag` — it also matches the
			// "default=" option scan, a different question entirely, and it
			// misses the exact-match skips that are the agreement invariant.
		},
	},
	{
		id:       "Q7",
		question: "Is this field written in the FLAT (goavro-style) form, needing a lift into a nested type?",
		authority: "flatFieldNeedsLift (schema_parse.go) is the one predicate, and THREE representations call " +
			"it — the parser, the tree walker, the metadata renderer. Sharing makes the agreement structural, " +
			"so what can still drift is the INPUT: a walker that reconstructs the field map differently reaches " +
			"a different verdict from the same predicate",
		answerers: []censusAnswerer{
			{repr: "as-written parse", site: "flatFieldNeedsLift → liftFlatFieldType", file: "schema_parse.go"},
			{repr: "cache / tree walker", site: "flatFieldNeedsLift → flatLiftTypeMap", file: "schema_walk.go"},
			{repr: "metadata renderer", site: "flatFieldNeedsLift", file: "schema_node.go"},
		},
		tells: []censusTell{
			{pattern: `flatFieldNeedsLift`, counts: map[string]int{
				"schema_parse.go": 4, "schema_walk.go": 3, "schema_node.go": 1,
			}},
			{pattern: `flatLiftTypeMap`, counts: map[string]int{
				"schema_parse.go": 5, "schema_walk.go": 2, "schema_node.go": 3, "cache.go": 2,
			}},
			// Rejected tell: `liftFlatFieldType` — it names the MUTATOR, which
			// only the parse path calls, so the walker and renderer sites
			// (the ones that could reconstruct the map differently) would go
			// unwatched.
		},
	},
	{
		id:       "Q4",
		question: "Is this key RESERVED on this kind — consumed into a structural field, or an ordinary custom property?",
		authority: "the RULINGS, not the code: NOT_BUGS #46 (reserved names match only their exact lowercase " +
			"spelling; a variant is an ordinary prop on every surface, body-independent) and #63(b)/(f)/(j) " +
			"(shape-conditional routing on a non-binding kind; placement-conditional, never case-conditional; " +
			"and a reserved key that is neither bound nor surfaceable has Props as its ONLY surface). " +
			"schemaKeyBinds decides binding -- strayKeyBinds for the keys the kind alone settles, plus the two " +
			"whose binding depends on the value or the logical type -- and schemaReservedKeyForObject decides " +
			"routing, as the disjunction of exactly two ways a reserved key stays out of Props: the kind BINDS " +
			"it, or the kind SURFACES it as-written on a structural field. Both are checked against the parse's " +
			"observable rather than against each other",
		answerers: []censusAnswerer{
			{repr: "as-written parse", site: "strayKeyBinds + the per-key shape arms", file: "schema_parse.go"},
			{repr: "metadata Root + render", site: "schemaReservedKeyForObject", file: "schema_node.go"},
			{
				repr: "the shared binding question", site: "schemaKeyBinds", file: "schema_node.go",
				note: "not a fourth answerer but the ONE binding predicate the routing asks, which is what keeps the routing from enumerating the consumed keys as a hand-written list. An enumeration is a subset and a subset can be missing a member: the type-level \"default\" and \"order\" fell through such a list and were dropped, reaching neither a structural field nor Props.",
			},
			{repr: "cache splice merge", site: "schemaReservedKeyForObject (nil shape verdict)", file: "cache.go"},
			{
				repr: "tree walker", site: "strayBodyShapeOK gating stray enumeration", file: "schema_walk.go",
				note: "different-by-design and PINNED as an asymmetry (#63(e)): the binding-kind gate is walkNodeChildren's default, and only the METADATA walker opts into stray enumeration. Collect and inline keep the bound-only view deliberately, so this site answers a deliberately narrower question than the others.",
			},
		},
		tells: []censusTell{
			{pattern: `strayKeyBinds`, counts: map[string]int{
				"schema_parse.go": 11, "schema_node.go": 2,
			}},
			{pattern: `schemaKeyBinds`, counts: map[string]int{
				"schema_node.go": 3, "schema_parse.go": 1,
			}},
			{pattern: `schemaReservedKeyForObject`, counts: map[string]int{
				"schema_node.go": 6, "schema_parse.go": 5, "cache.go": 1,
			}},
			// Rejected tell: `strayBodyShapeOK` — 20 hits across three files,
			// but it answers the SHAPE question (does this body parse as the
			// key's schema form), which is an input to the routing rather
			// than the routing itself. Counting it would make the guard fire
			// on shape-check refactors that do not touch reservedness.
		},
	},
	{
		id:       "Q16",
		question: "Does this node carry anything beyond its Type — i.e. is it structurally empty?",
		authority: "nodeCarriesOnlyType (schema_node.go), DERIVED from SchemaNode's field set rather than " +
			"written as a list. Two sites ask it — the primitive arm and the name-reference arm of toJSONWalk " +
			"— and each previously held its OWN incomplete copy of the list, which is how a stray Symbols, " +
			"Size or Aliases was silently dropped by the rebuild while a stray Name was caught by one copy only. " +
			"The MEMBERS are classified rather than merely counted (bareEmissionFieldRules): Branches and " +
			"EnumDefault exempt (no emitted form — Branches has no JSON key outside a union, EnumDefault has no " +
			"carrier without HasEnumDefault); HasEnumDefault, Precision and Scale block but do not come back on " +
			"their own field (precision/scale ride to Props per #71, \"default\" is dropped by the reserved-name " +
			"routing the attribute-placement census pins); the remaining 11 block and round-trip on their own field",
		answerers: []censusAnswerer{
			{repr: "metadata render, primitive arm", site: "nodeCarriesOnlyType", file: "schema_node.go"},
			{repr: "metadata render, name-reference arm", site: "nodeCarriesOnlyType", file: "schema_node.go"},
			{
				repr: "the shared field-set walk", site: "nodeCarriesNothingBut", file: "schema_node.go",
				note: "not a second answerer but the ONE walk both questions run — Q16 and Q17 differ only in the exemption function they pass. Two structurally identical reflect loops is the shape this pair of questions was already burned by, so there is one loop and the difference is data.",
			},
		},
		tells: []censusTell{
			{pattern: `nodeCarriesOnlyType`, counts: map[string]int{
				// Definition, its two call sites, and three doc references
				// (counted with grep -o; doc comments count, and reasoning
				// about the number has been wrong every time).
				"schema_node.go": 6,
			}},
			{pattern: `bareEmissionExempt`, counts: map[string]int{
				"schema_node.go": 3,
			}},
			// Rejected tell: `len(n.Props) == 0` — the shape the OLD
			// hand-written lists shared. It still appears in unrelated
			// emptiness checks, so it would fire on changes that have nothing
			// to do with bare emission; and after the fix it no longer marks
			// this question's sites at all, which is the point.
			//
			// The durable guard for this question is not a tell but
			// TestInvariant_BareEmissionCoversEverySchemaNodeField, which
			// sets every exported field in turn and requires the predicate to
			// notice BOTH halves — that the field blocks, and that the object
			// form it falls through to actually carries the value through an
			// emit → re-parse round trip. Proving only the blocking half left
			// the emitter free to drop the value with nothing but the render
			// changed, which is exactly what EnumDefault did. A field added
			// later fails there until classified, so the enumeration checks
			// ITSELF rather than trusting the next author — which a tell count
			// cannot do.
		},
	},
	{
		id:       "Q17",
		question: "Is this node a pure NAME-REFERENCE SHAPE — may the definition it names be SPLICED in place of it?",
		authority: "nodeIsNameRefShape (schema_node.go), derived from SchemaNode's field set through the same " +
			"walk as Q16 with a different exemption set. The two questions are siblings, not one question: Q16 asks " +
			"whether a node may collapse to its bare type NAME, Q17 whether a stamped reference may be REPLACED by " +
			"the definition it names — and a splice discards whatever the usage site carried. The exemption set is " +
			"therefore the ADJUDICATED usage-site attributes and nothing else (nameRefSpliceFieldRules): Doc, " +
			"Aliases, Namespace and LogicalType, because a definition cannot carry a second name/namespace/doc for " +
			"its usage site (NOT_BUGS #25) and the parse LANDS those on the structural fields, so blocking them " +
			"would convert an adjudicated silent drop into a hard \"unknown complex type\" error on the extraction " +
			"feature; plus Props, which the splice MERGES onto the definition (#63's splice-merge clause). " +
			"Precision and Scale are NOT exempt even though they too are usage-site attributes, because the parse " +
			"routes an unconsumed pair to Props (#71) — a non-zero value on those FIELDS can only come from a " +
			"caller writing them, and that write must not vanish",
		answerers: []censusAnswerer{
			{repr: "metadata render, splice gate", site: "nodeIsNameRefShape", file: "schema_node.go"},
			{
				repr: "cache splice merge", site: "inlineTreeDefs's wrapper arm", file: "cache.go",
				note: "different-by-design as a SITE: the cache splices a wrapped reference in the RAW JSON tree, before any SchemaNode exists, so it asks the question of a key map rather than of a field set. It answers the same policy — reserved usage-site keys drop, custom props merge definition-wins — and TestMatrix_SpliceWrapperReservedKeyMerge is what holds the two in step.",
			},
		},
		tells: []censusTell{
			{pattern: `nodeIsNameRefShape`, counts: map[string]int{
				// Definition, its one call site, and one doc reference.
				"schema_node.go": 3,
			}},
			{pattern: `nameRefUsageSiteExempt`, counts: map[string]int{
				"schema_node.go": 3,
			}},
			// Rejected tell: `n.refTarget` — it marks the STAMP, which is
			// nodeRefTargetAgrees's question ("does Type still name this
			// target"), asked immediately beside this one at the same call
			// site. Counting it would make this question fire on stamp
			// changes that have nothing to do with what the node may carry.
			//
			// As with Q16 the durable guard is not a tell:
			// TestInvariant_NameRefSpliceCoversEverySchemaNodeField sets each
			// exported field on an EXTRACTED reference and requires the
			// predicate to notice, and TestMatrix_CallerComposedAndEditedNodes
			// crosses that with the recursive, diamond, forward-reference and
			// cache-cross-parse structures. A tell watches where a rule is
			// WRITTEN; this class's failure mode is a member the rule never
			// mentioned, which no count can see.
		},
	},
	{
		id:       "Q18",
		question: "Is this attribute's body PRESENT but unreadable, as opposed to ABSENT?",
		authority: "jsonNullBody (schema_parse.go). The authority under it is encoding/json's documented null " +
			"handling: unmarshaling the null literal into a destination that is not a pointer, interface or map " +
			"\"has no effect on the value and produces no error\". So null is the ONE body a typed decode accepts " +
			"in silence, and any reader that answers this question by asking whether the decode FAILED answers it " +
			"wrong — it reports a written attribute as absent and keeps the destination's zero. That zero is a " +
			"legal setting for several Avro attributes (a fixed of size 0, a decimal of scale 0), so the wrong " +
			"answer substitutes a schema nobody wrote. Java asks the question by TOKEN TYPE and rejects the same " +
			"bodies (Schema.java:1957-1960 for size; LogicalTypes.java:414-421 for the decimal parameters)",
		answerers: []censusAnswerer{
			{repr: "size, all three surfaces", site: "decodeLaxInt", file: "schema_parse.go"},
			{repr: "precision/scale, type and field level", site: "intPtrFrom", file: "schema_parse.go"},
			{
				repr: "enum-level default", site: "the token-type check before json.Unmarshal", file: "schema.go",
				note: "different-by-design as a SITE: the default's body is kept as raw JSON, so the question is asked of the raw TOKEN (its first byte must be a quote) rather than of a decoded any. It is registered here because it is the same question and the same hazard — its own ruling records that the pre-fix membership check let the Unmarshal zero flow through — and because it is the shape the two decoded-value answerers were brought into line with.",
			},
			{
				repr: "every assertion-read key", site: "getString / jsonNumericInt / stringSliceFrom / the m[k].(T) arms", file: "schema_node.go",
				note: "different-by-design as an ANSWER, not a site: a JSON null decodes to a nil any, which satisfies no type assertion and matches no case of a type switch, so these reads decline it exactly as they decline a wrong-typed body. They answer the question structurally and need no guard — which is why the hazard is confined to the two re-marshal-then-decode helpers. Registering them is what keeps a later refactor from turning one of them into a typed decode without noticing it has joined this question.",
			},
		},
		tells: []censusTell{
			{pattern: `jsonNullBody`, counts: map[string]int{
				// The doc heading, the definition, one call in each of the two
				// decode helpers, and one doc reference from intPtrFrom, whose
				// comment records what the guard restores.
				"schema_parse.go": 5,
			}},
			// Rejected tell: `== nil` — it is the most common comparison in
			// the package and answers "is this pointer/error/interface unset"
			// almost everywhere it appears. The question here is about a
			// DECODED JSON BODY specifically, which is exactly what the named
			// predicate marks.
			//
			// The durable guard is not a tell either:
			// TestMatrix_ReservedKeyBodyPresence crosses every reserved key
			// that has a typed destination with {absent, valid, null,
			// wrong-typed, quoted} at both levels and requires the null
			// verdict to equal the wrong-typed one on every surface. A tell
			// watches where the rule is written; this class's failure mode is
			// a new typed read that never asks it.
		},
	},
	{
		id:       "Q11",
		question: "What IDENTITY does a failure carry — is it errors.As-able to *SemanticError, and what Field path does it report?",
		authority: "no external authority: the contract is doc.go's \"# Errors\" section, and the invariant the " +
			"drivers assert is AGREEMENT between the wire formats. Which identity a family carries is policy; a " +
			"caller who only changes format finding errors.As change its answer is not",
		answerers: []censusAnswerer{
			{repr: "binary encode", site: "ser.go's SemanticError construction + semErr", file: "ser.go"},
			{repr: "JSON encode", site: "json_codec.go's SemanticError construction + semErr", file: "json_codec.go"},
			{repr: "binary decode", site: "deser.go's SemanticError construction + semErr", file: "deser.go"},
			{repr: "JSON decode", site: "json_decode.go's SemanticError construction + semErr", file: "json_decode.go"},
			{
				repr: "unsafe struct fast paths", site: "unsafe.go's SemanticError construction", file: "unsafe.go",
				note: "different-by-design as a SITE, not as an answer: the unsafe paths are a compiled specialization of the reflect ones and must produce the IDENTICAL identity. That is asserted by the dual-path suites rather than here, because a census cell cannot choose which path a decode takes — the builder does, by target shape.",
			},
			{
				repr: "resolved decode", site: "resolve.go + promote.go", file: "resolve.go",
				note: "different-by-design as a SITE: a resolved decode adds writer→reader translation, so its failures include families the natural path has no equivalent of. Its parity with the natural path is the resolved-vs-natural suites' question, not this one.",
			},
		},
		tells: []censusTell{
			{pattern: `&SemanticError{`, counts: map[string]int{
				"ser.go":         32,
				"json_codec.go":  10,
				"deser.go":       30,
				"json_decode.go": 5,
				"unsafe.go":      11,
				"resolve.go":     1,
				"promote.go":     1,
				"reflect.go":     6,
				"custom_type.go": 2,
				"errors.go":      4,
			}},
			// Rejected tell: `semErr(` — it is the CONSTRUCTOR most of these
			// sites call, so counting it double-counts the same answerers and
			// misses the ones that build the struct literally. `&SemanticError{`
			// is the shape a NEW hand-written identity decision takes.
		},
	},
}

// censusOutstanding is the enumeration's OPEN end. A question lands here the
// moment it is discovered — usually when a candidate tell has to be REJECTED
// because it answers a different question, which is the census noticing a
// row it has not asked yet. Recording it with the tell that revealed it is
// what stops it being lost between rounds.
//
// The total is not fixed and should not be reported as if it were: say "N
// registered, M outstanding, enumeration open".
var censusOutstanding = []struct {
	question   string
	revealedBy string
}{
	{
		question:   "Does this BODY parse as the key's schema SHAPE?",
		revealedBy: "strayBodyShapeOK / strayBodyShapeOKMemo — REJECTED as a Q4 tell because it answers the shape question that FEEDS routing, not reservedness itself; 20 hits across schema_parse.go, schema_node.go and schema_walk.go, and #63(b)'s capture-implies-verdict clause says the metadata captures must run the parser's own decodes, which is an agreement question of its own",
	},
	{
		question:   "Which union BRANCH NAME does this Go value dispatch to?",
		revealedBy: "unionTypeNameForValue — binary (serUnion.ser) and JSON (appendAvroJSONUnion) dispatch it separately; surfaced while driving Q10, whose nil short-circuit sits beside it",
	},
}

// censusDemoted records questions examined and found NOT to be census
// material, with the evidence. A genuine one-answerer question with no
// external authority has nothing to disagree with, so a driver for it would
// assert a function against itself. Saying so is a result; leaving it
// unexplained invites a later round to re-derive the same enumeration.
//
// The bar is the RULE's shape, not the helper's name — two questions were
// wrongly flagged for demotion before this bar was applied, and both turned
// out to have several hand-written answerers.
var censusDemoted = []struct {
	question string
	evidence string
}{
	{
		question: "Does this kind RECURSE (can a union branch of it nest further nodes)?",
		evidence: `the rule shape kind == "record" || "array" || "map" appears EXACTLY ONCE, in unionBranchRecurses (json_decode.go:1530); its three call sites (1626, 1653, 1786) all consume that one predicate, and no second representation asks the question. The per-kind ` + "`case \"array\":`" + ` switches elsewhere answer "how do I handle this kind", not "does it recurse". No external authority either: the rule is a DoS-motivated backtracking policy internal to the JSON union decoder (scalar branches keep their bounded backtrack; container branches would be 2^depth).`,
	},
}

// TestCensus_DemotedIsJustified requires every demotion to carry its
// evidence, and re-checks the claim that makes it a demotion: a
// single-answerer question must still have exactly one occurrence of its
// rule shape in the sources.
func TestCensus_DemotedIsJustified(t *testing.T) {
	for _, d := range censusDemoted {
		if d.question == "" || d.evidence == "" {
			t.Errorf("demotion is unexplained: %+v", d)
		}
	}
	files := censusSourceFiles(t)
	found := occurrences(t, files, `kind == "record" || kind == "array" || kind == "map"`)
	total := 0
	for _, lines := range found {
		total += len(lines)
	}
	if total != 1 {
		t.Errorf("the recursion rule shape now appears %d times (%v); it was demoted as single-answerer, so a second occurrence means the demotion must be revisited", total, found)
	}
}

// TestCensus_OutstandingIsRecorded keeps the open end honest: every entry
// names the code that revealed it, so a later round can pick it up without
// re-deriving why it exists.
func TestCensus_OutstandingIsRecorded(t *testing.T) {
	for _, q := range censusOutstanding {
		if q.question == "" || q.revealedBy == "" {
			t.Errorf("outstanding entry is incomplete: %+v", q)
		}
	}
	t.Logf("census: %d registered, %d outstanding, %d demoted, enumeration open",
		len(censusRegistry), len(censusOutstanding), len(censusDemoted))
}

// censusSourceFiles returns every non-test .go file the census scans, as
// paths relative to the package root.
func censusSourceFiles(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, dir := range []string{".", "ocf", "atype"} {
		ents, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("reading %s: %v", dir, err)
		}
		for _, e := range ents {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			p := name
			if dir != "." {
				p = filepath.ToSlash(filepath.Join(dir, name))
			}
			out = append(out, p)
		}
	}
	if len(out) < 25 {
		t.Fatalf("only found %d source files; the scan is not seeing the package", len(out))
	}
	return out
}

// occurrences reports, per file, the line numbers where pattern appears.
func occurrences(t *testing.T, files []string, pattern string) map[string][]int {
	t.Helper()
	found := make(map[string][]int)
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("reading %s: %v", f, err)
		}
		for i, line := range strings.Split(string(b), "\n") {
			for rest, off := line, 0; ; {
				j := strings.Index(rest, pattern)
				if j < 0 {
					break
				}
				found[f] = append(found[f], i+1)
				off += j + len(pattern)
				rest = line[off:]
			}
		}
	}
	return found
}

// TestCensus_NoUnregisteredAnswerers fails when a hand-written answerer of a
// registered question appears outside the registry — a new file, or a new
// occurrence in a file already listed. It also fails when a registered site
// VANISHES, which is the vacuity direction: a tell that matches less than it
// claims means the registry is describing code that no longer exists, and
// every driver keyed to it is guarding nothing.
func TestCensus_NoUnregisteredAnswerers(t *testing.T) {
	files := censusSourceFiles(t)
	for _, q := range censusRegistry {
		if len(q.tells) == 0 {
			t.Errorf("%s (%s) registers no tells, so nothing guards it", q.id, q.question)
		}
		for _, tell := range q.tells {
			found := occurrences(t, files, tell.pattern)
			if len(found) == 0 {
				t.Errorf("%s: tell %q matches nothing in the package — the registry has rotted", q.id, tell.pattern)
				continue
			}
			for file, lines := range found {
				want, registered := tell.counts[file]
				if !registered {
					t.Errorf("%s: tell %q appears in UNREGISTERED file %s (lines %v).\n  A new site answering %q must be added to the census registry with its representation, and either routed through %s or given the documented reason it cannot be.",
						q.id, tell.pattern, file, lines, q.question, q.authority)
					continue
				}
				if len(lines) != want {
					t.Errorf("%s: tell %q appears %d times in %s (lines %v), registry says %d.\n  If a site was ADDED it must be registered (see %s); if one was REMOVED the count must come down, or this guard is watching code that is gone.",
						q.id, tell.pattern, len(lines), file, lines, want, q.authority)
				}
			}
			for file, want := range tell.counts {
				if len(found[file]) == 0 && want > 0 {
					t.Errorf("%s: tell %q is registered %d times in %s but matches nothing there — the site moved or was deleted, and the registry no longer describes the code",
						q.id, tell.pattern, want, file)
				}
			}
		}
	}
	// The registry must not silently shrink. Deleting a question is a
	// decision — it means the question stopped being one, or was demoted
	// into censusDemoted with its evidence — so it has to be made here
	// rather than by a row quietly disappearing.
	const registered = 16
	if len(censusRegistry) < registered {
		t.Fatalf("census registry has %d questions, was %d; a question was removed without "+
			"recording why. Demote it into censusDemoted with its evidence, or lower this floor "+
			"deliberately", len(censusRegistry), registered)
	}
}

// ---------------------------------------------------------------------
// Q1 — is this union branch the null type?
// ---------------------------------------------------------------------

// nullSpellingCell is one branch spelling plus the answer every
// representation owes for it.
type nullSpellingCell struct {
	name   string
	branch string // the branch as written inside a union
	isNull bool
}

// The corpus spans the question's domain: both spellings Avro admits for
// null, the wrapped form carrying each kind of inert metadata (Avro defines
// no null logical type, so neither props nor logicalType can make a wrapped
// null stop being null), and near-misses that must answer false — including
// a named type whose name merely contains "null", which a substring-minded
// answerer would get wrong.
var nullSpellingCorpus = []nullSpellingCell{
	{name: "bare", branch: `"null"`, isNull: true},
	{name: "wrapped", branch: `{"type":"null"}`, isNull: true},
	{name: "wrapped-with-props", branch: `{"type":"null","x":1}`, isNull: true},
	{name: "wrapped-with-logicaltype", branch: `{"type":"null","logicalType":"nope"}`, isNull: true},
	{name: "wrapped-with-doc", branch: `{"type":"null","doc":"d"}`, isNull: true},
	{name: "bare-int", branch: `"int"`, isNull: false},
	{name: "wrapped-int", branch: `{"type":"int"}`, isNull: false},
	{name: "bare-string", branch: `"string"`, isNull: false},
	{name: "record", branch: `{"type":"record","name":"R","fields":[]}`, isNull: false},
	{name: "fixed-named-nullish", branch: `{"type":"fixed","name":"nullable","size":1}`, isNull: false},
	{name: "enum-with-null-symbol", branch: `{"type":"enum","name":"E","symbols":["null"]}`, isNull: false},
}

// TestCensus_Q1_NullBranchAgreement runs every representation's answerer
// over the whole corpus and requires identical verdicts. Disagreement here
// is the bug class directly: the wire bytes of the two null spellings are
// IDENTICAL, so nothing in a round-trip net can see the two sides diverge —
// only a derived artifact does (which encoder arm is selected, which branch
// a lift targets, which error identity surfaces).
func TestCensus_Q1_NullBranchAgreement(t *testing.T) {
	for _, cell := range nullSpellingCorpus {
		t.Run(cell.name, func(t *testing.T) {
			// The branch sits second so the union is never null-first,
			// keeping the three representations comparable at a fixed index
			// (a null-first 2-branch union takes a different builder arm).
			// The anchor is boolean because no corpus cell is boolean —
			// a union may not repeat a type.
			text := `["boolean",` + cell.branch + `]`
			const idx = 1

			var tree any
			if err := json.Unmarshal([]byte(text), &tree); err != nil {
				t.Fatalf("corpus cell is not valid JSON: %v", err)
			}
			preParse := isNullBranchTree(tree.([]any)[idx])

			a, err := parseSchemaTree(text)
			if err != nil {
				t.Fatalf("parseSchemaTree: %v", err)
			}
			asWritten := a.union[idx].isNullBranch()

			s, err := Parse(text)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			compiled := s.node.branches[idx].kind == "null"

			got := map[string]bool{
				"pre-Parse any tree":  preParse,
				"as-written aschema":  asWritten,
				"compiled schemaNode": compiled,
			}
			for repr, v := range got {
				if v != cell.isNull {
					t.Errorf("%s answered %v for branch %s, want %v — the representations disagree: %v",
						repr, v, cell.branch, cell.isNull, got)
				}
			}
		})
	}
}

// TestCensus_Q1_CorpusIsNotVacuous proves the corpus actually exercises both
// answers and both spellings; a corpus that drifted to all-true (or all
// bare) would let a broken answerer pass.
func TestCensus_Q1_CorpusIsNotVacuous(t *testing.T) {
	var nulls, nonNulls, wrappedNulls int
	for _, c := range nullSpellingCorpus {
		switch {
		case c.isNull && strings.HasPrefix(c.branch, "{"):
			wrappedNulls++
			nulls++
		case c.isNull:
			nulls++
		default:
			nonNulls++
		}
	}
	if nulls < 2 || nonNulls < 2 || wrappedNulls < 2 {
		t.Fatalf("corpus is too thin to discriminate: %d null (%d wrapped), %d non-null", nulls, wrappedNulls, nonNulls)
	}
}

// ---------------------------------------------------------------------
// Q2 — what fullname does a named-type definition occupy?
// ---------------------------------------------------------------------

// A definition's fullname is the string a reference has to bind to, so
// every representation that computes one has to land on the same answer.
// The name can arrive dotted, split across a "namespace" attribute,
// inherited from the enclosing scope, or explicitly escaped back to the
// null namespace — and the rules interact (a dotted name outranks both the
// attribute and the enclosing scope).
type fullnameCell struct {
	name     string // the definition's "name" as written
	nsAttr   string // its "namespace" attribute; "" means the key is absent
	hasNSKey bool   // whether "namespace" is present at all (present-and-empty differs)
	lax      bool   // needs an accept-all name validator to parse at all
	want     string
}

var fullnameCorpus = []fullnameCell{
	{name: "Foo", want: "ns.Foo"},                                     // inherited from the enclosing scope
	{name: "Foo", nsAttr: "other", hasNSKey: true, want: "other.Foo"}, // attribute overrides the enclosing scope
	{name: "Foo", nsAttr: "", hasNSKey: true, want: "Foo"},            // present-and-empty: the null-namespace escape
	{name: "x.Foo", want: "x.Foo"},                                    // dotted name outranks the enclosing scope
	{name: "x.Foo", nsAttr: "other", hasNSKey: true, want: "x.Foo"},   // dotted name outranks the attribute
	{name: "a.b.c.Foo", want: "a.b.c.Foo"},                            // multi-component
	// The leading-dot escape is normalized at parse into the
	// null-namespace fullname. In a DEFINITION it carries an empty
	// namespace component, which the strict grammar rejects, so these
	// cells only exist under an accept-all validator — which is exactly
	// where a normalization that only one representation performs would
	// go unnoticed.
	{name: ".Foo", lax: true, want: "Foo"},
	{name: ".Foo", nsAttr: "other", hasNSKey: true, lax: true, want: "Foo"},
}

// TestCensus_Q2_DefinitionFullnameAgreement builds one enclosing schema per
// cell and asks each representation what fullname the inner definition
// occupies. The compiled tree is the authority — it is the name the wire
// builder registers, and therefore the name a reference actually binds to —
// so a metadata or cache-tree answer that differs means a reference resolves
// to one type on the wire and another in the surface that re-emits it.
func TestCensus_Q2_DefinitionFullnameAgreement(t *testing.T) {
	for _, cell := range fullnameCorpus {
		label := cell.name
		if cell.hasNSKey {
			label += "+ns=" + strconv.Quote(cell.nsAttr)
		}
		t.Run(label, func(t *testing.T) {
			inner := `{"type":"record","name":` + strconv.Quote(cell.name)
			if cell.hasNSKey {
				inner += `,"namespace":` + strconv.Quote(cell.nsAttr)
			}
			inner += `,"fields":[{"name":"x","type":"int"}]}`
			// The enclosing record establishes the "ns" scope the inner
			// definition may or may not inherit.
			text := `{"type":"record","name":"ns.Top","fields":[{"name":"a","type":` + inner + `}]}`

			var opts []SchemaOpt
			if cell.lax {
				opts = append(opts, WithLaxNames(func(string) error { return nil }))
			}
			s, err := Parse(text, opts...)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			compiled := s.node.fields[0].node.name

			root := s.Root()
			metadata := nodeFullname(&root.Fields[0].Type)

			var tree any
			if err := json.Unmarshal([]byte(inner), &tree); err != nil {
				t.Fatalf("inner is not valid JSON: %v", err)
			}
			cacheTree := nodeFullnameTree(tree.(map[string]any), "ns")

			got := map[string]string{
				"compiled schemaNode (authority)": compiled,
				"metadata SchemaNode":             metadata,
				"cache raw JSON tree":             cacheTree,
			}
			for repr, v := range got {
				if v != cell.want {
					t.Errorf("%s answered %q for definition %s, want %q — the representations disagree: %v",
						repr, v, inner, cell.want, got)
				}
			}
		})
	}
}

// The corpus must exercise every way a namespace can arrive, or a
// representation that mishandles one of them passes by not being asked.
func TestCensus_Q2_CorpusIsNotVacuous(t *testing.T) {
	var inherited, attr, nullEscape, dotted int
	for _, c := range fullnameCorpus {
		switch {
		case strings.HasPrefix(c.name, "."):
			nullEscape++
		case strings.Contains(c.name, "."):
			dotted++
		case c.hasNSKey && c.nsAttr == "":
			nullEscape++
		case c.hasNSKey:
			attr++
		default:
			inherited++
		}
	}
	if inherited < 1 || attr < 1 || nullEscape < 2 || dotted < 2 {
		t.Fatalf("corpus misses a namespace arrival form: inherited=%d attr=%d nullEscape=%d dotted=%d", inherited, attr, nullEscape, dotted)
	}
}

// ---------------------------------------------------------------------
// Q9 — what will json.Marshal emit for this value?
// ---------------------------------------------------------------------

// chargedBytes runs the schema-tree budget walk over v and reports how many
// BYTES it charged — the walk's own answer to "how much will json.Marshal
// emit for this".
func chargedBytes(t *testing.T, v any) int {
	t.Helper()
	b := newWalkBudget()
	before := b.bytes
	if r := valueWalkLimit(reflect.ValueOf(v), maxSchemaJSONDepth, &b); r != valueWalkOK {
		t.Fatalf("walk did not complete over %T (code %d) — the corpus cell must fit the budget", v, r)
	}
	return before - b.bytes
}

// emissionRouteCell is one ROUTE by which content reaches json.Marshal's
// output, given as a small value and a larger twin that differs ONLY in how
// much content travels that route. Comparing the two isolates the route: a
// walk that does not model it charges the same for both while json.Marshal
// emits the difference.
type emissionRouteCell struct {
	name         string
	small, large any
	// openRuling, when set, records that this route is KNOWN to under-charge
	// and the question of what to do about it is with the maintainer. Such a
	// cell asserts the under-charge still holds, so whichever way the ruling
	// goes the cell reds and forces this registry to be updated — the
	// disagreement cannot be silently resolved in either direction.
	openRuling string
}

// The openRuling machinery below stays even though no question currently
// uses it: it is how a disagreement with two defensible resolutions gets
// recorded without either leaving the suite red or letting the question
// close itself silently. Its first use was the escaped-vs-content byte
// charge, now FIXED — those cells are ordinary agreement cells again, which
// is the mechanism working as designed.

func emissionRouteCorpus() []emissionRouteCell {
	const (
		lo = 64
		hi = 4096
	)
	big := func(n int) string { return strings.Repeat("v", n) }
	return []emissionRouteCell{
		{name: "plain-string", small: big(lo), large: big(hi)},
		{name: "named-string-kind", small: namedStringKey(big(lo)), large: namedStringKey(big(hi))},
		// json.Marshal ESCAPES a string's contents, so a byte of content is
		// not a byte of output: a control byte becomes \u00XX (six), and the
		// HTML-escaped set becomes < and friends. An all-printable cell
		// cannot see the difference between charging content and charging
		// emission.
		{name: "string-control-bytes", small: strings.Repeat("\x01", lo), large: strings.Repeat("\x01", hi)},
		{name: "string-html-escaped", small: strings.Repeat("<", lo), large: strings.Repeat("<", hi)},
		{name: "string-map-key-control", small: map[string]int{strings.Repeat("\x01", lo): 1}, large: map[string]int{strings.Repeat("\x01", hi): 1}},
		// []byte reaches json.Marshal as the Avro codepoint STRING, not as a
		// byte slice, so its emitted size depends on the byte VALUES: ASCII
		// costs one byte, 0x80-0xFF two (UTF-8), and a control byte six
		// (\u00XX). The walk charges the raw length, so the three classes
		// are separate cells — a single ASCII cell would never see it.
		{name: "byte-slice-ascii", small: []byte(big(lo)), large: []byte(big(hi))},
		{name: "byte-slice-high", small: bytes.Repeat([]byte{0xff}, lo), large: bytes.Repeat([]byte{0xff}, hi)},
		{name: "byte-slice-control", small: bytes.Repeat([]byte{0x01}, lo), large: bytes.Repeat([]byte{0x01}, hi)},
		{name: "string-kind-map-key", small: map[string]int{big(lo): 1}, large: map[string]int{big(hi): 1}},
		{name: "map-value", small: map[string]any{"k": big(lo)}, large: map[string]any{"k": big(hi)}},
		{name: "slice-element", small: []any{big(lo)}, large: []any{big(hi)}},
		{name: "json-marshaler", small: bigJSONMarshaler{n: lo}, large: bigJSONMarshaler{n: hi}},
		{name: "text-marshaler", small: bigTextMarshaler{n: lo}, large: bigTextMarshaler{n: hi}},
		{name: "json-marshaler-in-map", small: map[string]any{"k": bigJSONMarshaler{n: lo}}, large: map[string]any{"k": bigJSONMarshaler{n: hi}}},
		{name: "json-marshaler-in-slice", small: []any{bigJSONMarshaler{n: lo}}, large: []any{bigJSONMarshaler{n: hi}}},
		{name: "text-marshaler-map-key", small: map[textKeyVal]int{{s: big(lo)}: 1}, large: map[textKeyVal]int{{s: big(hi)}: 1}},
		{name: "struct-field-value", small: struct{ F string }{big(lo)}, large: struct{ F string }{big(hi)}},
		// Routes the escape-aware charge newly has to model: a Marshaler's
		// output is re-scanned by the compactor, which expands the HTML trio
		// one byte into six, and a TextMarshaler's output goes through the
		// same string escaper a plain string does.
		{name: "json-marshaler-html", small: htmlJSONMarshaler{n: lo}, large: htmlJSONMarshaler{n: hi}},
		{name: "text-marshaler-control", small: ctrlTextMarshaler{n: lo}, large: ctrlTextMarshaler{n: hi}},
		{name: "text-marshaler-key-control", small: map[textKeyVal]int{{s: strings.Repeat("\x01", lo)}: 1}, large: map[textKeyVal]int{{s: strings.Repeat("\x01", hi)}: 1}},
		{name: "nested-two-levels", small: map[string]any{"a": []any{big(lo)}}, large: map[string]any{"a": []any{big(hi)}}},
	}
}

// TestCensus_Q9_EmissionRouteChargeTracksJSON asserts the walk's model of
// json.Marshal against json.Marshal itself, per route. The budget exists to
// bound what json.Marshal will emit, so for every route the charge must grow
// at least as fast as the real output does: an under-charge means that route
// is FREE, which is precisely how a value with its own MarshalJSON once
// cost one node and zero bytes while emitting megabytes.
//
// Over-charging is allowed (the walk may be conservative); under-charging is
// the bug. The comparison is a DELTA rather than an absolute, because the
// walk deliberately does not charge for structural punctuation — but it
// cannot decline to charge for content without the delta collapsing.
func TestCensus_Q9_EmissionRouteChargeTracksJSON(t *testing.T) {
	for _, cell := range emissionRouteCorpus() {
		t.Run(cell.name, func(t *testing.T) {
			// The authority is json.Marshal of what the pipeline actually
			// hands it: boundedSerializableValue charges the budget and then
			// returns jsonSerializableValue(v), so the fixups (a []byte
			// becoming the Avro codepoint string, ±Inf becoming a literal)
			// are part of the emission the budget is supposed to bound.
			// Marshaling the raw value instead would compare against bytes
			// this package never emits.
			smallOut, err := json.Marshal(jsonSerializableValue(cell.small))
			if err != nil {
				t.Fatalf("authority could not marshal the small twin: %v", err)
			}
			largeOut, err := json.Marshal(jsonSerializableValue(cell.large))
			if err != nil {
				t.Fatalf("authority could not marshal the large twin: %v", err)
			}
			authorityDelta := len(largeOut) - len(smallOut)
			if authorityDelta <= 0 {
				t.Fatalf("corpus cell is not a growth pair: json.Marshal emitted %d then %d bytes", len(smallOut), len(largeOut))
			}

			chargedDelta := chargedBytes(t, cell.large) - chargedBytes(t, cell.small)

			if cell.openRuling != "" {
				// A recorded disagreement. Assert it STILL holds, so that
				// resolving it either way reds this cell and forces the
				// registry to be updated — an open question must not be able
				// to close itself silently.
				if chargedDelta >= authorityDelta {
					t.Errorf("route %q no longer under-charges (json +%d, charged +%d) — the recorded open ruling has been resolved in the code; update the registry and delete openRuling.\n  Recorded question: %s",
						cell.name, authorityDelta, chargedDelta, cell.openRuling)
				} else {
					t.Logf("route %q under-charges by %.1fx (json +%d, charged +%d) — RECORDED OPEN RULING: %s",
						cell.name, float64(authorityDelta)/float64(chargedDelta), authorityDelta, chargedDelta, cell.openRuling)
				}
				return
			}

			if chargedDelta < authorityDelta {
				t.Errorf("route %q is under-charged: json.Marshal emits %d more bytes for the large twin, the walk charged only %d more.\n  A route the budget does not model is emitted for free, so the cap it advertises does not bound json.Marshal's output.",
					cell.name, authorityDelta, chargedDelta)
			}
		})
	}
}

// TestCensus_Q9_CorpusIsNotVacuous proves the corpus reaches the routes that
// matter: the two DELEGATING routes (a value's own MarshalJSON / MarshalText,
// which the structural walk never descends into) and the container positions
// that carry them, plus a non-string map key. Without those, the corpus only
// exercises the plain structural recursion that was never the bug.
func TestCensus_Q9_CorpusIsNotVacuous(t *testing.T) {
	var delegating, nested, mapKey int
	for _, c := range emissionRouteCorpus() {
		switch {
		case strings.Contains(c.name, "map-key"):
			mapKey++
		case strings.Contains(c.name, "-in-"), strings.Contains(c.name, "nested"):
			nested++
			if strings.Contains(c.name, "marshaler") {
				delegating++
			}
		case strings.Contains(c.name, "marshaler"):
			delegating++
		}
	}
	if delegating < 3 || nested < 3 || mapKey < 2 {
		t.Fatalf("corpus misses a route class: delegating=%d nested=%d mapKey=%d", delegating, nested, mapKey)
	}
	q9NotVacuousTail(t)
}

func q9NotVacuousTail(t *testing.T) {
	t.Helper()
	// And the walk must actually be measuring something: a cell whose charge
	// is zero for both twins would pass the delta test vacuously only if the
	// authority delta were zero, which is separately guarded — but a corpus
	// where NOTHING is charged means chargedBytes is broken.
	if got := chargedBytes(t, strings.Repeat("x", 128)); got < 128 {
		t.Fatalf("chargedBytes reports %d for a 128-byte string; the measurement itself is broken", got)
	}
}

// ---------------------------------------------------------------------
// Q11 — what identity does a failure carry?
// ---------------------------------------------------------------------

// A caller's only programmatic handle on a failure is its IDENTITY: whether
// it is errors.As-able to *SemanticError, and what that error's Field path
// says. The same failure reached through the binary and the JSON decoder
// must present the same handle, or `errors.As` succeeds on one wire and
// fails on the other for a caller who only changed format.
//
// The ENCODE half of this question is already driven by
// TestMatrix_EncodeErrorIdentityCensus (encode_error_identity_census_test.go).
// This is the DECODE half, which had only three spot subtests: the same
// schema and the same VALUE presented on both wires, decoded into a Go
// target that cannot hold it, so both decoders reach a target-type failure
// from equivalent input.
type decodeIdentityCell struct {
	name   string
	schema string
	value  any // encoded to both wires with the schema itself
	target func() any
}

func decodeIdentityCorpus() []decodeIdentityCell {
	return []decodeIdentityCell{
		{"int-into-string", `"int"`, int32(1), func() any { return new(string) }},
		{"string-into-int", `"string"`, "s", func() any { return new(int32) }},
		{"bool-into-string", `"boolean"`, true, func() any { return new(string) }},
		{"double-into-string", `"double"`, 1.5, func() any { return new(string) }},
		{"bytes-into-int", `"bytes"`, []byte{1}, func() any { return new(int32) }},
		{
			"array-into-string", `{"type":"array","items":"int"}`,
			[]any{int32(1)}, func() any { return new(string) },
		},
		{
			"map-into-int", `{"type":"map","values":"int"}`,
			map[string]any{"k": int32(1)}, func() any { return new(int32) },
		},
		{
			"record-into-int",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"int"}]}`,
			map[string]any{"f": int32(1)}, func() any { return new(int32) },
		},
		{
			// NOT *int32: decoding an enum into an integer target is the
			// documented ORDINAL behavior and succeeds on both wires, so it
			// is not a failure family at all. A bool cannot hold a symbol
			// under any rule.
			"enum-into-bool",
			`{"type":"enum","name":"E","symbols":["A","B"]}`,
			"A", func() any { return new(bool) },
		},
		{
			"fixed-into-int",
			`{"type":"fixed","name":"F","size":2}`,
			[]byte{1, 2}, func() any { return new(int32) },
		},
		{
			"record-field-into-string",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"int"}]}`,
			map[string]any{"f": int32(1)},
			func() any {
				return new(struct {
					F string `avro:"f"`
				})
			},
		},
	}
}

// identityOf reduces an error to the handle a caller can act on.
func identityOf(err error) (semantic bool, field string) {
	var se *SemanticError
	if errors.As(err, &se) {
		return true, se.Field
	}
	return false, ""
}

// TestCensus_Q11_DecodeFailureIdentityAgreesAcrossWires presents the same
// value on both wires and decodes it into a target that cannot hold it. The
// invariant is AGREEMENT, not a particular verdict: which identity a family
// carries is policy (doc.go "# Errors"), but a caller who switches format
// must not find errors.As changing its answer.
func TestCensus_Q11_DecodeFailureIdentityAgreesAcrossWires(t *testing.T) {
	for _, cell := range decodeIdentityCorpus() {
		t.Run(cell.name, func(t *testing.T) {
			s, err := Parse(cell.schema)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			bin, err := s.Encode(cell.value)
			if err != nil {
				t.Fatalf("corpus value does not encode to binary: %v", err)
			}
			jsn, err := s.EncodeJSON(cell.value)
			if err != nil {
				t.Fatalf("corpus value does not encode to JSON: %v", err)
			}

			_, errB := s.Decode(bin, cell.target())
			errJ := s.DecodeJSON(jsn, cell.target())
			if errB == nil || errJ == nil {
				t.Fatalf("both wires must reject this target; binary=%v json=%v", errB, errJ)
			}

			semB, fieldB := identityOf(errB)
			semJ, fieldJ := identityOf(errJ)
			if semB != semJ {
				t.Errorf("identity disagrees across wires: binary errors.As(*SemanticError)=%v, json=%v\n  binary: %v\n  json:   %v\n  A caller who only changed wire format finds errors.As changing its answer.",
					semB, semJ, errB, errJ)
			}
			if semB && semJ && fieldB != fieldJ {
				t.Errorf("SemanticError.Field disagrees across wires: binary=%q json=%q\n  binary: %v\n  json:   %v",
					fieldB, fieldJ, errB, errJ)
			}
		})
	}
}

// The corpus must span both scalar and CONTAINER targets, and must include a
// record-FIELD position: the field path is where the two decoders most
// plausibly diverge, since only one of them threads a field name.
func TestCensus_Q11_CorpusIsNotVacuous(t *testing.T) {
	var containers, fieldPos int
	for _, c := range decodeIdentityCorpus() {
		if strings.HasPrefix(c.name, "array") || strings.HasPrefix(c.name, "map") || strings.HasPrefix(c.name, "record") {
			containers++
		}
		if strings.Contains(c.name, "field") {
			fieldPos++
		}
	}
	if containers < 3 || fieldPos < 1 {
		t.Fatalf("corpus misses a position class: containers=%d fieldPos=%d", containers, fieldPos)
	}
}

// ---------------------------------------------------------------------
// Q9 differential: the escape-length restatement vs the real emitter
// ---------------------------------------------------------------------

// jsonEscapedLen restates encoding/json's escape rules instead of delegating
// to them, because delegation is impossible for MEASUREMENT: asking the
// emitter how long its output is means producing that output, which is the
// allocation the budget exists to prevent. A restatement is only allowed
// with an executed differential over the authority's COMPLETE domain, and
// that is what this is.
//
// Expectations come from marshalSchemaTree — the package's own emitter —
// not from json.Marshal named directly, so if this package ever switches to
// an Encoder with SetEscapeHTML(false) the expected values move with it and
// this test fails until the restatement is updated to match.
//
// Escaping below utf8.RuneSelf is byte-LOCAL: a byte's emitted cost never
// depends on its neighbours. Testing all 256 single-byte values is therefore
// a proof over that part of the domain rather than a sample of it; the
// multi-byte cases are enumerated separately below.
func emittedContentLen(t *testing.T, s string) int {
	t.Helper()
	out, err := marshalSchemaTree(s)
	if err != nil {
		t.Fatalf("the package emitter rejected %q: %v", s, err)
	}
	if len(out) < 2 || out[0] != '"' || out[len(out)-1] != '"' {
		t.Fatalf("emitter produced a non-string form for %q: %s", s, out)
	}
	return len(out) - 2 // strip the quotes; the charge is for content
}

func TestCensus_Q9_EscapedLenMatchesEmitterOverEveryByte(t *testing.T) {
	const noLimit = 1 << 30
	for v := 0; v < 256; v++ {
		s := string([]byte{byte(v)})
		want := emittedContentLen(t, s)
		if got := jsonEscapedLen(s, noLimit); got != want {
			t.Errorf("byte 0x%02x: jsonEscapedLen=%d, emitter wrote %d", v, got, want)
		}
		if got := jsonEscapedLenBytes([]byte{byte(v)}, noLimit); got != want {
			t.Errorf("byte 0x%02x: jsonEscapedLenBytes=%d, emitter wrote %d", v, got, want)
		}
	}
}

func TestCensus_Q9_EscapedLenMatchesEmitterOnMultiByte(t *testing.T) {
	const noLimit = 1 << 30
	for _, s := range []string{
		"", "plain ascii",
		"é",          // 2-byte rune
		"€",          // 3-byte rune
		"\U0001d11e", // 4-byte rune
		" ", " ",     // escaped unconditionally
		"a b c",         // interleaved with plain text
		"<>&", `"`, `\`, // the HTML trio and the two-character escapes
		"\x00\x01\x1f", // control run
		"\n\r\t\b\f",   // the named escapes
		"\x80",         // lone continuation byte: invalid UTF-8
		"\xe2\x80",     // truncated 3-byte sequence
		"\xff\xfe",     // never-valid bytes
		"a\x80b",       // invalid byte between valid ones
		"\xf0\x9f\x92", // truncated 4-byte sequence
		"héllo <world> & \"quotes\"\n",
	} {
		want := emittedContentLen(t, s)
		if got := jsonEscapedLen(s, noLimit); got != want {
			t.Errorf("%q: jsonEscapedLen=%d, emitter wrote %d", s, got, want)
		}
		if got := jsonEscapedLenBytes([]byte(s), noLimit); got != want {
			t.Errorf("%q: jsonEscapedLenBytes=%d, emitter wrote %d", s, got, want)
		}
	}
}

// The []byte arm charges the value's json-FACING image — the Avro codepoint
// string the fixup produces — so its differential runs through that fixup.
func TestCensus_Q9_CodepointEscapedLenMatchesEmitterOverEveryByte(t *testing.T) {
	const noLimit = 1 << 30
	for v := 0; v < 256; v++ {
		raw := []byte{byte(v)}
		fixed, ok := jsonSerializableValue(raw).(string)
		if !ok {
			t.Fatalf("byte 0x%02x: the fixup did not produce a string", v)
		}
		want := emittedContentLen(t, fixed)
		if got := avroCodepointEscapedLen(reflect.ValueOf(raw), noLimit); got != want {
			t.Errorf("byte 0x%02x: avroCodepointEscapedLen=%d, emitter wrote %d for the codepoint form", v, got, want)
		}
	}
}

// The early exit is what makes the scan bounded by the BUDGET rather than by
// the input. Proven deterministically rather than by timing: escaping never
// shrinks, so the running total passes the limit within limit+1 input bytes,
// and the returned value is therefore the same for inputs of wildly
// different sizes. A scan without the exit would return a total proportional
// to its input.
func TestCensus_Q9_EscapedLenScanIsBoundedByTheLimit(t *testing.T) {
	const limit = 100
	var prev int
	for _, size := range []int{1 << 10, 1 << 16, 1 << 20, 1 << 23} {
		got := jsonEscapedLen(strings.Repeat("\x01", size), limit)
		if got > limit+6 {
			t.Fatalf("size %d: scan returned %d, past limit %d by more than one byte's cost — it did not stop early", size, got, limit)
		}
		if prev != 0 && got != prev {
			t.Fatalf("size %d returned %d but size before returned %d; the result must not depend on input length once the limit is passed", size, got, prev)
		}
		prev = got
	}
	// And the cost of the whole walk over a value far larger than the budget
	// stays small, since the scan abandons it.
	huge := strings.Repeat("\x01", 32<<20) // 32 MiB of 6x-escaping content
	start := time.Now()
	b := newWalkBudget()
	if r := valueWalkLimit(reflect.ValueOf(huge), maxSchemaJSONDepth, &b); r != valueWalkTooLarge {
		t.Fatalf("a 32 MiB control-byte string must bust the byte budget, got code %d", r)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("rejecting an over-budget string took %v; the scan is not bounded by the budget", el)
	}
}

// htmlJSONMarshaler emits JSON whose string content is the HTML trio, which
// the compactor expands one byte into six.
type htmlJSONMarshaler struct{ n int }

func (h htmlJSONMarshaler) MarshalJSON() ([]byte, error) {
	return []byte(`"` + strings.Repeat("<", h.n) + `"`), nil
}

// ctrlTextMarshaler returns text the string escaper expands six-fold.
type ctrlTextMarshaler struct{ n int }

func (c ctrlTextMarshaler) MarshalText() ([]byte, error) {
	return []byte(strings.Repeat("\x01", c.n)), nil
}

// ---------------------------------------------------------------------
// Q14 — does this CustomType match this node?
// ---------------------------------------------------------------------

// customMatchCell crosses a CustomType's declared shape with a node's actual
// kind and logical type. The base rule is the same on both sides: an empty
// field means "any", a set field must equal the node's.
type customMatchCell struct {
	ctAvro, ctLogical string
	nodeKind, nodeLog string
}

func customMatchCorpus() []customMatchCell {
	shapes := []struct{ avro, logical string }{
		{"", ""},             // wildcard
		{"bytes", ""},        // kind only
		{"", "decimal"},      // logical only
		{"bytes", "decimal"}, // both
		{"long", "timestamp-millis"},
	}
	nodes := []struct{ kind, log string }{
		{"bytes", "decimal"},
		{"bytes", ""},
		{"long", "timestamp-millis"},
		{"long", ""},
		{"string", "uuid"},
	}
	var out []customMatchCell
	for _, sh := range shapes {
		for _, n := range nodes {
			out = append(out, customMatchCell{sh.avro, sh.logical, n.kind, n.log})
		}
	}
	return out
}

// TestCensus_Q14_CustomTypeMatchAgreesAcrossAnswerers runs both answerers
// over the cross. They must agree everywhere except on the WILDCARD, where
// the build-time answerer deliberately declines: a CustomType that names
// neither a kind nor a logical type must not suppress the built-in handlers,
// because it decides per value at runtime via ErrSkipCustomType. That
// exception is asserted explicitly rather than skipped, so if the build-time
// side ever stops excluding wildcards this cell reds.
func TestCensus_Q14_CustomTypeMatchAgreesAcrossAnswerers(t *testing.T) {
	for _, c := range customMatchCorpus() {
		name := "ct(" + c.ctAvro + "/" + c.ctLogical + ")-node(" + c.nodeKind + "/" + c.nodeLog + ")"
		t.Run(name, func(t *testing.T) {
			ct := CustomType{AvroType: c.ctAvro, LogicalType: c.ctLogical}
			runtimeAns := ct.matches(&schemaNode{kind: c.nodeKind, logical: c.nodeLog})

			b := &builder{customTypes: []CustomType{ct}}
			buildAns := b.hasMatchingCustomTypeCond(c.nodeKind, c.nodeLog, false)

			isWildcard := c.ctAvro == "" && c.ctLogical == ""
			if isWildcard {
				if !runtimeAns {
					t.Errorf("a wildcard CustomType must match at RUNTIME (it decides per value via ErrSkipCustomType), got false")
				}
				if buildAns {
					t.Errorf("a wildcard CustomType must NOT suppress built-in handlers at BUILD time, got true")
				}
				return
			}
			if runtimeAns != buildAns {
				t.Errorf("the two answerers disagree: CustomType.matches=%v, hasMatchingCustomTypeCond=%v.\n  The base rule (empty means any, set must equal) is implemented twice; a node reaching one answer at build time and the other at runtime is wired to a codec that does not handle it.",
					runtimeAns, buildAns)
			}
		})
	}
}

// The cross must contain cells that answer BOTH ways on each axis, or the
// agreement is trivial.
func TestCensus_Q14_CorpusIsNotVacuous(t *testing.T) {
	var match, nonMatch, wildcard int
	for _, c := range customMatchCorpus() {
		ct := CustomType{AvroType: c.ctAvro, LogicalType: c.ctLogical}
		switch {
		case c.ctAvro == "" && c.ctLogical == "":
			wildcard++
		case ct.matches(&schemaNode{kind: c.nodeKind, logical: c.nodeLog}):
			match++
		default:
			nonMatch++
		}
	}
	if match < 3 || nonMatch < 3 || wildcard < 3 {
		t.Fatalf("corpus is trivial: match=%d nonMatch=%d wildcard=%d", match, nonMatch, wildcard)
	}
}

// ---------------------------------------------------------------------
// Q5 — does a field-level decimal lift CONSUME precision/scale?
// ---------------------------------------------------------------------

// Two answerers navigate the field's type to find where a field-level
// logicalType lands. fieldDecimalLiftConsumesPrecisionScale decides whether
// the pair is CONSUMED — which makes a malformed body reject loudly instead
// of riding to Props — and liftFieldLogicalIntoType decides where the
// annotation actually goes. The verdict's own comment says it mirrors the
// lift, and that is a claim: they have drifted before, when one skipped a
// wrapped null branch and the other did not.
//
// Both run inside parseSchemaTree, so neither is callable on a pre-lift
// tree. Each is observed through the consequence it owns instead: the
// verdict through whether a MALFORMED pair rejects, the lift through where
// the parsed metadata ended up carrying the decimal annotation.
type liftTargetCell struct {
	name      string
	fieldType string // the field's "type" as written
	// byDesign, when set, is the documented reason the two navigations
	// deliberately disagree here. Such a cell asserts BOTH directions, so
	// either side changing reds it.
	byDesign string
}

func liftTargetCorpus() []liftTargetCell {
	return []liftTargetCell{
		{name: "bare-bytes", fieldType: `"bytes"`},
		{name: "bare-long-not-a-carrier", fieldType: `"long"`},
		{name: "bare-string-not-a-carrier", fieldType: `"string"`},
		{name: "wrapped-bytes", fieldType: `{"type":"bytes"}`},
		{name: "fixed", fieldType: `{"type":"fixed","name":"F","size":4}`},
		{name: "record-not-a-carrier", fieldType: `{"type":"record","name":"R2","fields":[]}`},
		{name: "union-null-first-bytes", fieldType: `["null","bytes"]`},
		{name: "union-wrapped-null-first-bytes", fieldType: `[{"type":"null"},"bytes"]`},
		{name: "union-null-first-long", fieldType: `["null","long"]`},
		{name: "union-bytes-first", fieldType: `["bytes","null"]`},
		{name: "union-null-then-wrapped-bytes", fieldType: `["null",{"type":"bytes"}]`},
		// The suspect shape: the lift's "closer-to-the-type wins" rule
		// declines to overwrite an annotation the target already has, so the
		// field's "decimal" never lands — but the verdict reads the FIELD's
		// logical, not the one that survives.
		{name: "target-already-annotated", fieldType: `{"type":"bytes","logicalType":"uuid"}`},
		{name: "union-target-already-annotated", fieldType: `["null",{"type":"bytes","logicalType":"uuid"}]`},
		// The DISCRIMINATOR: the target's own logical IS decimal, so the
		// field's parameters land on a real decimal carrier and ARE
		// consumed. Without this cell the rule could be loosened into
		// "a target with any annotation of its own is inert".
		{name: "target-own-logical-is-decimal", fieldType: `{"type":"bytes","logicalType":"decimal"}`},
		// The union twin of the cell above. It was once unmeasurable — its
		// VALID control did not parse, because the lift's union arm declined
		// to complete parameters the object arm supplied — and now exists,
		// which is the whole point of aligning the arms.
		{name: "union-target-own-logical-is-decimal", fieldType: `["null",{"type":"bytes","logicalType":"decimal"}]`},
		{name: "fixed-target-own-logical-is-decimal", fieldType: `{"type":"fixed","name":"F","size":4,"logicalType":"decimal"}`},
		// Non-decimal effective logical on a carrier: inert.
		{name: "target-own-logical-big-decimal", fieldType: `{"type":"bytes","logicalType":"big-decimal"}`},
	}
}

// Consumption is decided by what LANDS, not by where the lift points: the
// pair is consumed iff the target's EFFECTIVE logical — its own when it has
// one, else the field's — is "decimal" on a bytes/fixed carrier. The two
// pre-annotated-target cells below were once recorded as different-by-design
// on the opposite reading; wire evidence retired that (see the discriminator
// cells, which prove consumed-ness rather than assuming it).

func liftFieldSchema(fieldType, precision string) string {
	return `{"type":"record","name":"R","fields":[{"name":"f","type":` + fieldType +
		`,"logicalType":"decimal","precision":` + precision + `,"scale":2}]}`
}

// consumedByRejection asks the VERDICT's question: a malformed precision
// body rejects only where the pair is consumed.
//
// "Parse failed" is NOT the signal — it is confounded. When the pair is
// unconsumed the malformed value is dropped, and if the lift still put a
// decimal annotation on a carrier, the TYPE-level decimal validation then
// fails for a missing precision instead. Both paths return non-nil. The
// discriminator is the field gate's own message, which names the key it
// refused; that is the only error the verdict itself produces.
func consumedByRejection(t *testing.T, fieldType string) bool {
	t.Helper()
	if _, err := Parse(liftFieldSchema(fieldType, "4")); err != nil {
		t.Fatalf("the valid control must parse for %s: %v", fieldType, err)
	}
	_, err := Parse(liftFieldSchema(fieldType, "3.7"))
	return err != nil && strings.Contains(err.Error(), `record field "precision"`)
}

// consumedByLift asks the LIFT's question: after parsing, is the field's
// decimal annotation actually sitting on a bytes/fixed carrier — the only
// place precision/scale mean anything?
func consumedByLift(t *testing.T, fieldType string) bool {
	t.Helper()
	s, err := Parse(liftFieldSchema(fieldType, "4"))
	if err != nil {
		t.Fatalf("valid control: %v", err)
	}
	// The COMPILED tree is where the lift's effect lives: the metadata tree
	// preserves the schema as written, keeping the field-level annotation on
	// the FIELD, so reading it would report that no lift ever happened.
	target := s.node.fields[0].node
	// A union's annotation lands on its first non-null branch.
	if len(target.branches) > 0 {
		for _, br := range target.branches {
			if br.kind != "null" {
				target = br
				break
			}
		}
	}
	return decimalConsumesPrecisionScale(target.kind, target.logical)
}

// TestCensus_Q5_DecimalLiftConsumeVerdictMatchesWhereTheLiftLanded requires
// the two navigations to agree. A verdict of "consumed" over a target the
// lift did not annotate as decimal means a malformed pair rejects loudly on
// a schema where the pair is inert metadata — the opposite of the
// unconsumed-is-a-custom-property rule.
func TestCensus_Q5_DecimalLiftConsumeVerdictMatchesWhereTheLiftLanded(t *testing.T) {
	for _, cell := range liftTargetCorpus() {
		t.Run(cell.name, func(t *testing.T) {
			byVerdict := consumedByRejection(t, cell.fieldType)
			byLift := consumedByLift(t, cell.fieldType)

			if cell.byDesign != "" {
				// Both directions asserted, so either side changing reds this.
				if !byVerdict {
					t.Errorf("the verdict must still CONSUME here (a malformed pair must reject): %s", cell.byDesign)
				}
				if byLift {
					t.Errorf("the lift must still DECLINE to overwrite the target's own annotation: %s", cell.byDesign)
				}
				return
			}

			if byVerdict != byLift {
				t.Errorf("the two navigations disagree for field type %s:\n  verdict (malformed pair rejects) = %v\n  lift    (decimal landed on a carrier) = %v\n  One of them is reading a type the other never addressed.",
					cell.fieldType, byVerdict, byLift)
			}
		})
	}
}

// The corpus must contain carriers AND non-carriers, both bare and wrapped,
// unions with both null spellings, and a target that already carries its own
// annotation — otherwise the navigation is never actually exercised.
func TestCensus_Q5_CorpusIsNotVacuous(t *testing.T) {
	var unions, wrappedNull, preAnnotated, nonCarrier int
	for _, c := range liftTargetCorpus() {
		if strings.HasPrefix(c.fieldType, "[") {
			unions++
		}
		if strings.Contains(c.fieldType, `{"type":"null"}`) {
			wrappedNull++
		}
		if strings.Contains(c.fieldType, `"logicalType"`) {
			preAnnotated++
		}
		if strings.Contains(c.name, "not-a-carrier") {
			nonCarrier++
		}
	}
	if unions < 4 || wrappedNull < 1 || preAnnotated < 2 || nonCarrier < 3 {
		t.Fatalf("corpus misses a navigation class: unions=%d wrappedNull=%d preAnnotated=%d nonCarrier=%d",
			unions, wrappedNull, preAnnotated, nonCarrier)
	}
}

// ---------------------------------------------------------------------
// Q10 — is this Go value nil-equivalent (does it encode as Avro null)?
// ---------------------------------------------------------------------

// isNilValue's own doc names five dispatch sites that must agree on what
// counts as nil: the binary 2-branch [null,T] optimization, the binary
// try-each path through serNull, the JSON 2-branch short-circuit, the JSON
// try-each "null" arm, and the unsafe struct fast path. Four of them are
// reachable by choosing the schema shape and the wire; the fifth is chosen
// by the builder from the target's shape, so it gets a struct cell.
//
// serNull peels separately from isNilValue rather than calling it, and the
// two have drifted before — a fix once claimed to bring serNull "into
// parity" but added only Interface peeling, leaving &nilPtr rejected.
type nilShapeCell struct {
	name  string
	value func() any
	isNil bool
}

func nilShapeCorpus() []nilShapeCell {
	return []nilShapeCell{
		{"untyped-nil", func() any { return nil }, true},
		{"typed-nil-pointer", func() any { return (*string)(nil) }, true},
		{"pointer-to-nil-pointer", func() any { p := (*string)(nil); return &p }, true},
		{"any-wrapping-typed-nil", func() any { var a any = (*string)(nil); return a }, true},
		{"nil-map", func() any { return map[string]string(nil) }, true},
		{"nil-slice", func() any { return []string(nil) }, true},
		{"nil-chan", func() any { return (chan int)(nil) }, true},
		{"nil-func", func() any { return (func())(nil) }, true},
		{"pointer-to-nil-map", func() any { m := map[string]string(nil); return &m }, true},
		{"deep-nil-pointer-chain", func() any {
			p := (*string)(nil)
			pp := &p
			ppp := &pp
			return &ppp
		}, true},
		{"plain-string", func() any { return "s" }, false},
		{"empty-string", func() any { return "" }, false},
		{"pointer-to-string", func() any { s := "s"; return &s }, false},
		{"pointer-to-empty-string", func() any { s := ""; return &s }, false},
	}
}

// nilVerdictOf reports whether the schema encoded v as its NULL branch. The
// schemas are chosen so the null branch is index 0, whose binary tag byte is
// 0x00 and whose JSON form is the bare literal null.
func nilVerdictOf(t *testing.T, schema string, v any, jsonWire bool) (tookNull, encoded bool) {
	t.Helper()
	s, err := Parse(schema)
	if err != nil {
		t.Fatalf("Parse(%s): %v", schema, err)
	}
	var out []byte
	if jsonWire {
		out, err = s.EncodeJSON(v)
	} else {
		out, err = s.Encode(v)
	}
	if err != nil {
		return false, false
	}
	if jsonWire {
		return string(out) == "null", true
	}
	return len(out) == 1 && out[0] == 0x00, true
}

// TestCensus_Q10_NilEquivalenceAgreesAcrossDispatchSites requires every site
// to reach the same verdict as the shared predicate. A site that disagrees
// encodes a value as null where another encodes it as data, or rejects it
// outright — the same Go value meaning two different things depending on the
// union's arity or the wire format.
func TestCensus_Q10_NilEquivalenceAgreesAcrossDispatchSites(t *testing.T) {
	const (
		twoBranch   = `["null","string"]`
		threeBranch = `["null","string","long"]`
	)
	for _, cell := range nilShapeCorpus() {
		t.Run(cell.name, func(t *testing.T) {
			// The predicate itself, called directly.
			predicate := isNilValue(reflect.ValueOf(cell.value()))
			if predicate != cell.isNil {
				t.Fatalf("isNilValue = %v, want %v — the corpus and the predicate disagree before any dispatch site is asked", predicate, cell.isNil)
			}

			sites := map[string]bool{}
			for _, site := range []struct {
				name   string
				schema string
				asJSON bool
			}{
				{"binary 2-branch optimization", twoBranch, false},
				{"binary union try-each dispatcher", threeBranch, false},
				{"JSON 2-branch short-circuit", twoBranch, true},
				{"JSON try-each null arm", threeBranch, true},
			} {
				tookNull, encoded := nilVerdictOf(t, site.schema, cell.value(), site.asJSON)
				if !encoded && cell.isNil {
					t.Errorf("%s REJECTED a nil-equivalent value that isNilValue accepts", site.name)
					continue
				}
				sites[site.name] = tookNull
			}
			// serNull itself is NOT reachable through a union: serUnion.ser
			// short-circuits on isNilValue before trying any branch, so a
			// union cell measures the predicate rather than serNull's own
			// peel. The bare "null" schema is the only path that reaches it,
			// and it is the site that has actually drifted.
			bare, err := Parse(`"null"`)
			if err != nil {
				t.Fatalf("Parse null: %v", err)
			}
			out, encErr := bare.Encode(cell.value())
			serNullAccepted := encErr == nil && len(out) == 0
			if serNullAccepted != cell.isNil {
				t.Errorf("serNull (bare \"null\" schema) accepted=%v, isNilValue says %v — the null encoder's own peel disagrees with the shared predicate (err %v)",
					serNullAccepted, cell.isNil, encErr)
			}
			sites["serNull via bare null schema"] = serNullAccepted

			for name, tookNull := range sites {
				if tookNull != cell.isNil {
					t.Errorf("%s encoded null=%v, but isNilValue says %v — the dispatch sites disagree: %v",
						name, tookNull, cell.isNil, sites)
				}
			}
		})
	}
}

// The fifth site is chosen by the BUILDER from the target's shape, not by the
// schema, so it needs a struct whose field is a nullable pointer. Its own
// documented contract is that it cannot call isNilValue (it holds an
// unsafe.Pointer, not a reflect.Value) and instead declines every nilable
// inner kind to the reflect path — so the agreement it owes is that a struct
// field reaches the same verdict as the bare value did above.
func TestCensus_Q10_StructFieldPathAgrees(t *testing.T) {
	type holder struct {
		F *string `avro:"f"`
	}
	type holderMap struct {
		F map[string]string `avro:"f"`
	}
	s, err := Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":["null","string"]}]}`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	sm, err := Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":["null",{"type":"map","values":"string"}]}]}`)
	if err != nil {
		t.Fatalf("Parse map holder: %v", err)
	}

	for _, c := range []struct {
		name  string
		sch   *Schema
		v     any
		isNil bool
	}{
		{"nil-pointer-field", s, holder{}, true},
		{"set-pointer-field", s, holder{F: new(string)}, false},
		{"nil-map-field", sm, holderMap{}, true},
		{"empty-map-field", sm, holderMap{F: map[string]string{}}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			bin, err := c.sch.Encode(c.v)
			if err != nil {
				t.Fatalf("binary: %v", err)
			}
			jsn, err := c.sch.EncodeJSON(c.v)
			if err != nil {
				t.Fatalf("json: %v", err)
			}
			binNull := len(bin) == 1 && bin[0] == 0x00
			jsonNull := string(jsn) == `{"f":null}`
			if binNull != c.isNil || jsonNull != c.isNil {
				t.Errorf("struct-field path disagrees: binary null=%v json null=%v, want %v (binary %x, json %s)",
					binNull, jsonNull, c.isNil, bin, jsn)
			}
		})
	}
}

// The corpus must span every nilable KIND the predicate accepts plus the
// indirection shapes that once broke it, and must contain non-nil controls —
// otherwise "everything is nil" passes.
func TestCensus_Q10_CorpusIsNotVacuous(t *testing.T) {
	var nils, nonNils int
	kinds := map[reflect.Kind]bool{}
	for _, c := range nilShapeCorpus() {
		if c.isNil {
			nils++
		} else {
			nonNils++
		}
		if rv := reflect.ValueOf(c.value()); rv.IsValid() {
			kinds[rv.Kind()] = true
		}
	}
	for _, k := range []reflect.Kind{reflect.Pointer, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func} {
		if !kinds[k] {
			t.Errorf("corpus never exercises the nilable kind %v, which isNilableKind accepts", k)
		}
	}
	if nils < 6 || nonNils < 3 {
		t.Fatalf("corpus is lopsided: %d nil, %d non-nil", nils, nonNils)
	}
}

// ---------------------------------------------------------------------
// Q13 — which text route does this type take on encode?
// ---------------------------------------------------------------------

// A string-kind type with a MarshalText method encodes its MARSHALED form,
// not its raw string. The eligibility gates exist because the unsafe and
// container fast paths read the underlying string directly and bypass
// appendAvroString's text arm entirely, so a type with a text method must be
// kept OFF those paths — the gate's answer and the route actually taken are
// two answers to one question, and the fast-path exclusion list IS the
// sibling set.
//
// The method TRANSFORMS its input, so the two routes are distinguishable: an
// identity method would make a bypassed fast path and a working text arm
// produce the same bytes, and the probe would pass either way.
type censusUpperText string

func (c censusUpperText) MarshalText() ([]byte, error) {
	return []byte(strings.ToUpper(string(c))), nil
}

type censusPlainString string

// TestCensus_Q13_TextRouteAgreesWithTheEligibilityGate crosses the gate's
// verdict with the route actually taken at every position a value can hold:
// scalar, struct field (the unsafe fast path's home), array element, and map
// value. A type the gate calls ineligible must encode its marshaled form
// everywhere; a type it calls eligible must encode its raw string everywhere.
func TestCensus_Q13_TextRouteAgreesWithTheEligibilityGate(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  reflect.Type
		// positions, each producing the encoded STRING content
		scalar func() any
		field  func() any
		array  func() any
		mapv   func() any
		want   string // the content every position must carry
	}{
		{
			name:   "string-kind with a transforming MarshalText",
			typ:    reflect.TypeFor[censusUpperText](),
			scalar: func() any { return censusUpperText("ab") },
			field: func() any {
				return struct {
					F censusUpperText `avro:"f"`
				}{"ab"}
			},
			array: func() any { return []censusUpperText{"ab"} },
			mapv:  func() any { return map[string]censusUpperText{"k": "ab"} },
			want:  "AB",
		},
		{
			name:   "string-kind with no text method",
			typ:    reflect.TypeFor[censusPlainString](),
			scalar: func() any { return censusPlainString("ab") },
			field: func() any {
				return struct {
					F censusPlainString `avro:"f"`
				}{"ab"}
			},
			array: func() any { return []censusPlainString{"ab"} },
			mapv:  func() any { return map[string]censusPlainString{"k": "ab"} },
			want:  "ab",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eligible := stringFastPathEligibleEncode(tc.typ)
			// The gate's contract: a type with a text method is INELIGIBLE
			// for the raw-string fast paths, precisely so its method runs.
			if wantEligible := tc.want == "ab"; eligible != wantEligible {
				t.Fatalf("stringFastPathEligibleEncode = %v, but the type's route produces %q — the gate and the route disagree before any position is encoded", eligible, tc.want)
			}

			for _, pos := range []struct {
				name   string
				schema string
				value  func() any
			}{
				{"scalar", `"string"`, tc.scalar},
				{"struct field", `{"type":"record","name":"R","fields":[{"name":"f","type":"string"}]}`, tc.field},
				{"array element", `{"type":"array","items":"string"}`, tc.array},
				{"map value", `{"type":"map","values":"string"}`, tc.mapv},
			} {
				s, err := Parse(pos.schema)
				if err != nil {
					t.Fatalf("%s: Parse: %v", pos.name, err)
				}
				bin, err := s.Encode(pos.value())
				if err != nil {
					t.Fatalf("%s: encode: %v", pos.name, err)
				}
				if !bytes.Contains(bin, []byte(tc.want)) {
					t.Errorf("%s (binary) does not carry %q: %q — this position took a different text route than the gate promises",
						pos.name, tc.want, bin)
				}
				jsn, err := s.EncodeJSON(pos.value())
				if err != nil {
					t.Fatalf("%s: encodeJSON: %v", pos.name, err)
				}
				if !bytes.Contains(jsn, []byte(tc.want)) {
					t.Errorf("%s (JSON) does not carry %q: %s", pos.name, tc.want, jsn)
				}
			}
		})
	}
}

// The corpus must contain a type on BOTH sides of the gate, and the text
// method must TRANSFORM — an identity method cannot distinguish a bypassed
// fast path from a working text arm, so the whole driver would pass
// vacuously.
func TestCensus_Q13_CorpusIsNotVacuous(t *testing.T) {
	if !implementsTextMarshaler(reflect.TypeFor[censusUpperText]()) {
		t.Fatal("the marked type does not implement TextMarshaler; the driver tests nothing")
	}
	if implementsTextMarshaler(reflect.TypeFor[censusPlainString]()) {
		t.Fatal("the control type implements TextMarshaler; there is no eligible side")
	}
	out, err := censusUpperText("ab").MarshalText()
	if err != nil || string(out) == "ab" {
		t.Fatalf("the text method must TRANSFORM its input, got %q (err %v) — an identity method makes every position look correct", out, err)
	}
	if stringFastPathEligibleEncode(reflect.TypeFor[censusUpperText]()) ==
		stringFastPathEligibleEncode(reflect.TypeFor[censusPlainString]()) {
		t.Fatal("both corpus types land on the same side of the gate")
	}
}

// ---------------------------------------------------------------------
// Q15 — is this kind a NAMED type, and is it a RECORD?
// ---------------------------------------------------------------------

// "Named" is the property that decides whether a kind occupies a fullname
// other schemas can reference. isNamedKind and isRecordKind are the shared
// predicates, but the same classification is also written out as literal
// case sets — `case "record", "enum", "fixed":` in compat.go and
// json_codec.go, `case "record", "error":` in schema_canonical.go and
// schema_node.go — so the rule exists in several hand-written copies.
//
// The observable is exact rather than a proxy: a kind is named iff a
// definition of that kind can be REFERENCED by name from a sibling position.
// On an unnamed kind a "name" key is a stray custom property (it binds
// nothing), so the reference must fail to resolve — which is the same
// statement from the other side.
type kindCell struct {
	kind    string
	def     string // a definition of this kind carrying "name":"N"
	isNamed bool
	isRec   bool
}

func kindCorpus() []kindCell {
	prim := func(k string) string { return `{"type":"` + k + `","name":"N"}` }
	return []kindCell{
		{kind: "null", def: prim("null")},
		{kind: "boolean", def: prim("boolean")},
		{kind: "int", def: prim("int")},
		{kind: "long", def: prim("long")},
		{kind: "float", def: prim("float")},
		{kind: "double", def: prim("double")},
		{kind: "bytes", def: prim("bytes")},
		{kind: "string", def: prim("string")},
		{kind: "array", def: `{"type":"array","items":"int","name":"N"}`},
		{kind: "map", def: `{"type":"map","values":"int","name":"N"}`},
		{kind: "record", def: `{"type":"record","name":"N","fields":[]}`, isNamed: true, isRec: true},
		{kind: "error", def: `{"type":"error","name":"N","fields":[]}`, isNamed: true, isRec: true},
		{kind: "enum", def: `{"type":"enum","name":"N","symbols":["A"]}`, isNamed: true},
		{kind: "fixed", def: `{"type":"fixed","name":"N","size":1}`, isNamed: true},
	}
}

// TestCensus_Q15_NamedKindAgreesWithNameBinding crosses the predicates with
// the binding behavior they describe. A kind the predicate calls named whose
// definition cannot be referenced — or an unnamed kind whose stray "name"
// nonetheless binds a reference — means the name table and the predicate
// disagree about which types exist.
func TestCensus_Q15_NamedKindAgreesWithNameBinding(t *testing.T) {
	for _, cell := range kindCorpus() {
		t.Run(cell.kind, func(t *testing.T) {
			if got := isNamedKind(cell.kind); got != cell.isNamed {
				t.Errorf("isNamedKind(%q) = %v, want %v", cell.kind, got, cell.isNamed)
			}
			if got := isRecordKind(cell.kind); got != cell.isRec {
				t.Errorf("isRecordKind(%q) = %v, want %v", cell.kind, got, cell.isRec)
			}

			// The observable: can a sibling field reference the name?
			src := `{"type":"record","name":"Top","fields":[` +
				`{"name":"a","type":` + cell.def + `},` +
				`{"name":"b","type":"N"}]}`
			_, err := Parse(src)
			bound := err == nil
			if bound != cell.isNamed {
				t.Errorf("a reference to the declared name %s for kind %q, but isNamedKind says %v.\n  schema: %s\n  err: %v",
					map[bool]string{true: "RESOLVED", false: "did NOT resolve"}[bound], cell.kind, cell.isNamed, src, err)
			}
		})
	}
}

// TestCensus_Q15_RecordKindAgreesWithFieldBinding checks the record half
// against the key only a record binds: a "fields" attribute is structural on
// record and error, and on nothing else.
func TestCensus_Q15_RecordKindAgreesWithFieldBinding(t *testing.T) {
	for _, cell := range kindCorpus() {
		if cell.kind == "null" || cell.kind == "record" || cell.kind == "error" {
			continue // null carries no fields anywhere; the record kinds are the positive control
		}
		t.Run(cell.kind, func(t *testing.T) {
			// "fields" on a non-record kind binds nothing, so the compiled
			// node must carry no fields regardless of the attribute.
			var withFields string
			if cell.kind == "array" {
				withFields = `{"type":"array","items":"int","fields":[{"name":"x","type":"int"}]}`
			} else if cell.kind == "map" {
				withFields = `{"type":"map","values":"int","fields":[{"name":"x","type":"int"}]}`
			} else if cell.kind == "enum" {
				withFields = `{"type":"enum","name":"E2","symbols":["A"],"fields":[{"name":"x","type":"int"}]}`
			} else if cell.kind == "fixed" {
				withFields = `{"type":"fixed","name":"F2","size":1,"fields":[{"name":"x","type":"int"}]}`
			} else {
				withFields = `{"type":"` + cell.kind + `","fields":[{"name":"x","type":"int"}]}`
			}
			s, err := Parse(withFields)
			if err != nil {
				t.Logf("kind %q rejects a stray fields attribute (%v) — the exclusivity rule, not this question", cell.kind, err)
				return
			}
			if n := len(s.node.fields); n != 0 {
				t.Errorf("kind %q is not a record kind but the compiled node bound %d fields", cell.kind, n)
			}
		})
	}
}

// The corpus must cover every kind the parser produces on both sides of both
// predicates, or a classification error in an uncovered kind passes.
func TestCensus_Q15_CorpusIsNotVacuous(t *testing.T) {
	var named, unnamed, rec int
	seen := map[string]bool{}
	for _, c := range kindCorpus() {
		seen[c.kind] = true
		if c.isNamed {
			named++
		} else {
			unnamed++
		}
		if c.isRec {
			rec++
		}
	}
	for _, k := range []string{"null", "boolean", "int", "long", "float", "double",
		"bytes", "string", "record", "error", "enum", "fixed", "array", "map"} {
		if !seen[k] {
			t.Errorf("corpus is missing the kind %q", k)
		}
	}
	if named < 4 || unnamed < 8 || rec < 2 {
		t.Fatalf("corpus is lopsided: named=%d unnamed=%d record=%d", named, unnamed, rec)
	}
}

// ---------------------------------------------------------------------
// Q8 — does this struct tag skip the field?
// ---------------------------------------------------------------------

// Two subsystems read avro struct tags and each reads them on two
// structurally distinct paths: SchemaFor's named-field and anonymous-embed
// paths decide what a GENERATED schema contains, and the runtime field
// mapper's two paths decide what an encode/decode BINDS. All four spell the
// exact-match skip as `tag == "-"`, and they must agree — a subsystem that
// stopped skipping would put back a field the caller excluded, on one side
// only.
//
// This was flagged as a possible demotion (one answerer, no external
// authority). Grepping the RULE's shape rather than the helper's name
// disproves that: `tag == "-"` appears at reflect.go 481 and 510 and
// schema_for.go 725 and 784. What IS single-answerer is the GRAMMAR guard
// (checkSkipDirectiveExact), and its scope is deliberate — see the
// different-by-design cell below.
type skipTagCell struct {
	name string
	// schemaFor renders a schema from a type carrying the tag; mapped
	// reports whether the runtime mapper binds a field of that name.
	schemaHas func(t *testing.T) (rendered string, buildErr error)
	mapped    func(t *testing.T) bool
}

type skipNamed struct {
	A int `avro:"a"`
	B int `avro:"-"`
}

type skipInner struct {
	C int `avro:"c"`
}

type skipEmbed struct {
	A         int `avro:"a"`
	skipInner `avro:"-"`
}

// TestCensus_Q8_SkipDirectiveAgreesAcrossSubsystems requires the generated
// schema and the runtime binding to agree that a plain "-" skips, on both
// the named-field and the anonymous-embed path.
func TestCensus_Q8_SkipDirectiveAgreesAcrossSubsystems(t *testing.T) {
	t.Run("named field", func(t *testing.T) {
		s, err := SchemaFor[skipNamed]()
		if err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
		if strings.Contains(s.String(), `"b"`) || strings.Contains(s.String(), `"B"`) {
			t.Errorf("SchemaFor emitted the skipped field: %s", s.String())
		}
		// The runtime mapper registers NO target for a skipped field, which
		// the strict decoder surfaces as "missing field" on a schema that
		// carries one. That error IS the skip: an unskipped sibling binds.
		full := MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"B","type":"int"}]}`)
		wire, err := full.Encode(map[string]any{"a": int32(1), "B": int32(9)})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got skipNamed
		_, err = full.Decode(wire, &got)
		if err == nil {
			t.Errorf("the runtime mapper bound a field the tag skips: B = %d", got.B)
		} else if !strings.Contains(err.Error(), "missing field B") {
			t.Errorf("want the no-target error for the skipped field, got: %v", err)
		}
		// Control: the same struct against the schema SchemaFor generates
		// for it — the two subsystems agreeing is exactly the invariant.
		gen, err := SchemaFor[skipNamed]()
		if err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
		genWire, err := gen.Encode(skipNamed{A: 1, B: 9})
		if err != nil {
			t.Fatalf("encode against the generated schema: %v", err)
		}
		var rt skipNamed
		if _, err := gen.Decode(genWire, &rt); err != nil {
			t.Fatalf("the generated schema must round-trip its own type: %v", err)
		}
		if rt.A != 1 || rt.B != 0 {
			t.Errorf("round trip through the generated schema = %+v; the skipped field must not travel", rt)
		}
	})

	t.Run("anonymous embed", func(t *testing.T) {
		s, err := SchemaFor[skipEmbed]()
		if err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
		if strings.Contains(s.String(), `"c"`) {
			t.Errorf("SchemaFor inlined a skipped embedded struct: %s", s.String())
		}
		full := MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"c","type":"int"}]}`)
		wire, err := full.Encode(map[string]any{"a": int32(1), "c": int32(9)})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got skipEmbed
		_, err = full.Decode(wire, &got)
		if err == nil {
			t.Errorf("the runtime mapper bound a field inside a skipped embed: C = %d", got.C)
		} else if !strings.Contains(err.Error(), "missing field c") {
			t.Errorf("want the no-target error for the skipped embed's field, got: %v", err)
		}
		gen, err := SchemaFor[skipEmbed]()
		if err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
		genWire, err := gen.Encode(skipEmbed{A: 1})
		if err != nil {
			t.Fatalf("encode against the generated schema: %v", err)
		}
		var rt skipEmbed
		if _, err := gen.Decode(genWire, &rt); err != nil {
			t.Fatalf("the generated schema must round-trip its own type: %v", err)
		}
	})
}

type skipSuffixNamed struct {
	A int `avro:"a"`
	B int `avro:"-,omitzero"`
}

// The GRAMMAR guard is deliberately scoped to SchemaFor, and this asserts
// BOTH directions of that split so neither side can drift into the other.
// SchemaFor rejects a tag that starts with the directive without being it,
// because that is a typo it can name; the runtime mapper has never enforced
// tag grammar and treats the tag as a field name, which is what every other
// malformed tag does there. Collapsing either way would be a behavior change
// to a documented boundary, not a consistency fix.
func TestCensus_Q8_GrammarGuardIsSchemaForScoped(t *testing.T) {
	if _, err := SchemaFor[skipSuffixNamed](); err == nil {
		t.Error("SchemaFor must reject a tag that begins with the skip directive without being exactly it")
	} else if !strings.Contains(err.Error(), "exact-match only") {
		t.Errorf("reject does not name the directive rule: %v", err)
	}

	// The runtime mapper takes it as a field name and binds nothing unless
	// the schema happens to carry that name — no grammar error either way.
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	wire, err := s.Encode(map[string]any{"a": int32(1)})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got skipSuffixNamed
	if _, err := s.Decode(wire, &got); err != nil {
		t.Errorf("the runtime mapper must not enforce tag grammar, but it errored: %v", err)
	}
	if got.A != 1 {
		t.Errorf("the sibling field did not bind: %+v", got)
	}
}

// Both paths and both subsystems must actually be exercised, and the
// exact-match boundary needs its near-miss.
func TestCensus_Q8_CorpusIsNotVacuous(t *testing.T) {
	if _, err := SchemaFor[skipNamed](); err != nil {
		t.Fatalf("the named-path control does not build: %v", err)
	}
	if _, err := SchemaFor[skipEmbed](); err != nil {
		t.Fatalf("the embed-path control does not build: %v", err)
	}
	// The near-miss must differ from the directive, or the guard test is
	// asserting nothing.
	if reflect.TypeFor[skipSuffixNamed]().Field(1).Tag.Get("avro") == "-" {
		t.Fatal("the near-miss tag IS the directive; the grammar guard cell tests nothing")
	}
}

// ---------------------------------------------------------------------
// Q7 — is this field written in the FLAT form, needing a lift?
// ---------------------------------------------------------------------

// The flat (goavro-style) field form puts a complex kind's defining key
// beside the field's own keys — {"name":"f","type":"enum","symbols":[...]}
// instead of nesting a type object. Deciding whether to lift is one
// predicate, flatFieldNeedsLift, and three representations call it: the
// parser, the tree walker, and the metadata renderer. Sharing makes the
// agreement structural, so what this drives is that the three consult it on
// the SAME input — a walker that reconstructs the field map differently
// would reach a different verdict from the same predicate.
//
// The discriminator is a MISMATCHED defining key: "symbols" beside
// "type":"array" is not the array's key, so it is a stray custom property
// and no lift happens. A corpus without that cell would pass on a predicate
// that lifted whenever ANY complex key was present.
type flatFieldCell struct {
	name     string
	field    string // the field object as written inside a record's "fields"
	wantLift bool
	wantKind string // the compiled field's kind; "" when the schema must reject
}

func flatFieldCorpus() []flatFieldCell {
	return []flatFieldCell{
		{"flat-enum", `{"name":"f","type":"enum","name2":"","symbols":["A"]}`, true, "enum"},
		{"flat-array", `{"name":"f","type":"array","items":"int"}`, true, "array"},
		{"flat-map", `{"name":"f","type":"map","values":"int"}`, true, "map"},
		{"flat-fixed", `{"name":"f","type":"fixed","size":4}`, true, "fixed"},
		{"flat-record", `{"name":"f","type":"record","fields":[{"name":"x","type":"int"}]}`, true, "record"},
		{"nested-enum-not-flat", `{"name":"f","type":{"type":"enum","name":"E","symbols":["A"]}}`, false, "enum"},
		{"plain-primitive", `{"name":"f","type":"int"}`, false, "int"},
		// The discriminator. The lift still fires — "items" IS array's
		// defining key — and it carries the foreign "symbols" into the
		// lifted array object, where the per-kind exclusivity rule rejects
		// it. So the verdict is LIFT and the outcome is a parse error, which
		// is the documented path (NOT_BUGS #63(a)); a predicate that lifted
		// on any complex key, or one that declined here, would both look
		// fine without this cell.
		{"mismatched-defining-key", `{"name":"f","type":"array","items":"int","symbols":["A"]}`, true, ""},
	}
}

func TestCensus_Q7_FlatLiftVerdictAgreesWithTheParsedShape(t *testing.T) {
	for _, cell := range flatFieldCorpus() {
		t.Run(cell.name, func(t *testing.T) {
			// The predicate, called directly on the field map the parser sees.
			var fm map[string]any
			if err := json.Unmarshal([]byte(cell.field), &fm); err != nil {
				t.Fatalf("corpus cell is not valid JSON: %v", err)
			}
			tp, _ := fm["type"].(string)
			predicate := flatFieldNeedsLift(fm, tp)
			if predicate != cell.wantLift {
				t.Errorf("flatFieldNeedsLift = %v, want %v", predicate, cell.wantLift)
			}

			// The parser's observable: whatever the verdict, the field's
			// compiled kind must be the complex kind, reached either by the
			// lift or by the nested form.
			src := `{"type":"record","name":"R","fields":[` + cell.field + `]}`
			s, err := Parse(src)
			if cell.wantKind == "" {
				if err == nil {
					t.Fatalf("Parse(%s) accepted; the lift carries the foreign defining key into the lifted type, where the exclusivity rule must reject it", src)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%s): %v", src, err)
			}
			if got := s.node.fields[0].node.kind; got != cell.wantKind {
				t.Errorf("compiled field kind = %q, want %q — the lift verdict and the parsed shape disagree", got, cell.wantKind)
			}

			// The walker + metadata observable: rebuilding through the
			// metadata tree must reach the same compiled kind, or one
			// representation reconstructed the field map differently than
			// the parser handed it to the predicate.
			root := s.Root()
			rebuilt, err := root.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if got := rebuilt.node.fields[0].node.kind; got != cell.wantKind {
				t.Errorf("rebuilt field kind = %q, want %q — the metadata walker disagrees with the parser about the flat form", got, cell.wantKind)
			}
			if !bytes.Equal(s.Canonical(), rebuilt.Canonical()) {
				t.Errorf("canonical form changed across the rebuild:\n orig: %s\n  new: %s", s.Canonical(), rebuilt.Canonical())
			}
		})
	}
}

// The corpus must contain a flat form for every complex kind that can take
// one, a non-flat control, and the mismatched-key discriminator — otherwise
// a predicate that lifts on any complex key present would pass.
func TestCensus_Q7_CorpusIsNotVacuous(t *testing.T) {
	kinds := map[string]bool{}
	var flat, notFlat, mismatched int
	for _, c := range flatFieldCorpus() {
		if c.wantLift {
			flat++
			if c.wantKind != "" {
				kinds[c.wantKind] = true
			}
		} else {
			notFlat++
		}
		if strings.Contains(c.name, "mismatched") {
			mismatched++
		}
	}
	for _, k := range []string{"enum", "array", "map", "fixed", "record"} {
		if !kinds[k] {
			t.Errorf("corpus has no flat form for the kind %q", k)
		}
	}
	if flat < 5 || notFlat < 2 || mismatched < 1 {
		t.Fatalf("corpus is thin: flat=%d notFlat=%d mismatched=%d", flat, notFlat, mismatched)
	}
}

// ---------------------------------------------------------------------
// Q4 — is this key RESERVED on this kind, or an ordinary custom property?
// ---------------------------------------------------------------------

// The most heavily adjudicated question in the package, and the corpus is
// defined by the rulings rather than re-derived from the code:
//
//   - NOT_BUGS #46: reserved names match ONLY their exact lowercase
//     spelling. A case-variant is an ordinary custom property on EVERY
//     reading surface, body-independent; exact and variant together means
//     the exact one is consumed and the variant is a prop.
//   - NOT_BUGS #63(b): on a kind that does NOT bind the key, routing is
//     shape-conditional — a schema-shaped body surfaces structurally
//     as-written as its only surface, a malformed body rides in Props
//     verbatim as its only surface, and the structural field stays ZERO.
//   - NOT_BUGS #63(f): routing is placement-conditional, never
//     case-conditional.
//
// The invariant those clauses share is a biconditional, and that is what the
// driver asserts: the structural field is set IFF the key was consumed, and
// Props holds exactly the raw keys that were not. Two surfaces, one rule.
// strayKeyBinds is the binding predicate and schemaReservedKeyForObject the
// routing one; both are callable, so the driver checks them against the
// parse's observable rather than against each other.
//
// The biconditional decomposes into three implications, and only two of them
// are universal:
//
//   - consumed => NOT in Props            (universal)
//   - structural field set => consumed    (universal)
//   - consumed => structural field set    (one documented exception)
//
// The exception is NOT_BUGS #72: "doc" is bound on every kind, but its
// capture is a silently-declining string read, so a NON-STRING doc is
// consumed and yet lands nowhere — neither surface. That is exact Apache
// Avro behavior (parseDoc reads through getOptionalText, which is
// jsonNode.textValue() and null for a non-text node, Schema.java:1996-1998
// and :2039-2042; "doc" is then in SCHEMA_RESERVED :176 and FIELD_RESERVED
// :504, so parseProperties skips it). It is spelled here as a cell OUTCOME
// rather than left to fall through the corpus counters, so the exception is
// counted, cannot widen unnoticed, and cannot close itself silently.
type reservedKeyCell struct {
	name string
	kind string // the type object's kind
	key  string // the attribute spelling as written
	body string // its JSON value
	// Expectations, from the rulings above.
	binds      bool // strayKeyBinds: does this kind bind this key at all?
	structural bool // does the structural field end up populated?
	inProps    bool // does the key survive in Props verbatim?
	rejects    bool // or does the schema fail to parse outright?
	// dropped marks the documented exception to "consumed => structural
	// field set": the key is bound, so it stays out of Props, but the
	// binding read declines this body, so no structural field is set
	// either. See NOT_BUGS #72; it is one key ("doc") with a non-string
	// body, and every other reserved key with a non-conforming body either
	// routes to Props or rejects.
	dropped bool
	// reportedFinding, when set, records that the REBUILD loses this key
	// today, contrary to the documented posture. The cell asserts the loss
	// still happens, so fixing it reds here and forces this registry to be
	// updated — a reported finding must not be able to close itself
	// silently, exactly like an open ruling.
	reportedFinding string
}

// The reportedFinding mechanism is retained though no cell uses it: the
// stray-rebuild loss it recorded is FIXED (the bare-emission sites now ask
// one derived predicate, and a reflect guard keeps its field set complete),
// so those cells are ordinary agreement cells again — the mechanism working
// as designed.

func reservedKeyCorpus() []reservedKeyCell {
	return []reservedKeyCell{
		// Binding kind, exact spelling: consumed. The structural field is
		// set and the key never reaches Props.
		{name: "enum-symbols-exact", kind: "enum", key: "symbols", body: `["A","B"]`,
			binds: true, structural: true},
		{name: "fixed-size-exact", kind: "fixed", key: "size", body: `4`,
			binds: true, structural: true},

		// #46: a case-variant is an ordinary custom property, on a kind that
		// WOULD bind the exact spelling. Body-independent, so the variant
		// rides to Props and the structural field stays zero — and because
		// the exact spelling is then absent, a REQUIRED key's variant means
		// the attribute is missing and the parse rejects loudly.
		{name: "enum-symbols-variant-required-missing", kind: "enum", key: "Symbols", body: `["A","B"]`,
			binds: false, rejects: true},
		{name: "fixed-size-variant-required-missing", kind: "fixed", key: "Size", body: `4`,
			binds: false, rejects: true},

		// #46 on an OPTIONAL reserved key: the variant is inert and
		// preserved in Props verbatim, the exact-spelled attribute absent.
		{name: "record-aliases-variant-optional-inert", kind: "record", key: "Aliases", body: `["x"]`,
			binds: false, inProps: true},
		{name: "record-aliases-exact", kind: "record", key: "aliases", body: `["x"]`,
			binds: true, structural: true},

		// #63(b): a kind that does NOT bind the key. A schema-shaped body
		// surfaces structurally as-written; a malformed body rides in Props
		// and leaves the structural field at zero.
		{name: "int-symbols-shaped-stray", kind: "int", key: "symbols", body: `["A"]`,
			binds: false, structural: true},
		{name: "int-symbols-malformed-stray", kind: "int", key: "symbols", body: `3.7`,
			binds: false, inProps: true},
		{name: "int-size-shaped-stray", kind: "int", key: "size", body: `4`,
			binds: false, structural: true},
		{name: "int-size-malformed-stray", kind: "int", key: "size", body: `["a"]`,
			binds: false, inProps: true},

		// #63(f): the same stray on the same kind in its VARIANT spelling is
		// an ordinary prop whatever its body — placement-conditional routing,
		// never case-conditional.
		{name: "int-symbols-variant-shaped", kind: "int", key: "Symbols", body: `["A"]`,
			binds: false, inProps: true},
		{name: "int-symbols-variant-malformed", kind: "int", key: "Symbols", body: `3.7`,
			binds: false, inProps: true},

		// The two FIELD attributes at the TYPE level. Only an enum binds a
		// schema-level "default" (Java's ENUM_RESERVED is SCHEMA_RESERVED
		// plus that one key, Schema.java:178-180); no kind binds "order"
		// (neither reserved set contains it, :175-180). Where the kind does
		// not bind, there is no structural field for the key to surface on,
		// so Props is its ONLY surface — the biconditional's other arm. The
		// enum pair is the discriminating cell: same kind, one key bound and
		// the other not, so a routing that keyed off the kind alone would
		// get one of them wrong.
		{name: "enum-default-exact", kind: "enum", key: "default", body: `"Z"`,
			binds: true, structural: true},
		{name: "enum-order-stray", kind: "enum", key: "order", body: `"ignore"`,
			binds: false, inProps: true},
		{name: "int-default-stray", kind: "int", key: "default", body: `3`,
			binds: false, inProps: true},
		{name: "int-order-stray", kind: "int", key: "order", body: `"ignore"`,
			binds: false, inProps: true},

		// #46 on the newly routed keys: a case-variant is an ordinary prop
		// even on the kind whose EXACT spelling would bind it, so the enum's
		// own default stays unbound and the variant rides verbatim.
		{name: "enum-default-variant", kind: "enum", key: "Default", body: `"Z"`,
			binds: false, inProps: true},

		// "doc" is bound on EVERY kind, which is what makes it the one place
		// the third implication can fail. With a string body it behaves like
		// any other consumed key; with a non-string body the read declines
		// and the value lands nowhere (NOT_BUGS #72). The variant cell is
		// #46's control: a case-variant binds nothing, so it is an ordinary
		// prop whatever its body, and the drop cannot be reproduced by
		// spelling.
		{name: "int-doc-string", kind: "int", key: "doc", body: `"d"`,
			binds: true, structural: true},
		{name: "int-doc-nonstring", kind: "int", key: "doc", body: `5`,
			binds: true, dropped: true},
		{name: "int-doc-variant", kind: "int", key: "Doc", body: `5`,
			binds: false, inProps: true},

		// A key that is not reserved at all, as the baseline both sides of
		// the rule must agree on.
		{name: "int-plain-custom-key", kind: "int", key: "customThing", body: `7`,
			binds: false, inProps: true},
	}
}

func reservedKeySchema(c reservedKeyCell) string {
	obj := `{"type":"` + c.kind + `"`
	if isNamedKind(c.kind) {
		obj += `,"name":"N"`
	}
	// Named kinds need their own defining key present unless this cell IS
	// that key; otherwise the schema is invalid for an unrelated reason.
	// NOT strings.EqualFold-exempt: a case-VARIANT of the defining key must
	// leave the attribute genuinely absent, so supplying the exact spelling
	// alongside it would defeat the cell (it would become the documented
	// exact-consumed / variant-a-prop case instead).
	switch {
	case c.kind == "enum" && !strings.EqualFold(c.key, "symbols"):
		obj += `,"symbols":["Z"]`
	case c.kind == "fixed" && !strings.EqualFold(c.key, "size"):
		obj += `,"size":1`
	case isRecordKind(c.kind) && !strings.EqualFold(c.key, "fields"):
		obj += `,"fields":[]`
	}
	return obj + `,"` + c.key + `":` + c.body + `}`
}

// structuralFieldFor reads the metadata field the key would populate, so
// "structural field set" is measured rather than inferred.
func structuralFieldFor(n *SchemaNode, key string) bool {
	switch key {
	case "symbols", "Symbols":
		return len(n.Symbols) > 0
	case "size", "Size":
		return n.Size != 0
	case "aliases", "Aliases":
		return len(n.Aliases) > 0
	case "default", "Default":
		return n.HasEnumDefault
	case "doc", "Doc":
		return n.Doc != ""
	case "order", "Order":
		// No type-level kind binds "order", so there is no SchemaNode
		// field for it to reach — which is exactly why Props must be its
		// surface. The absence is the answer, not a gap in this reader.
		return false
	}
	return false
}

// TestCensus_Q4_ReservedKeyRoutingIsOneRuleAcrossSurfaces asserts the
// biconditional the rulings share: consumed keys populate their structural
// field and never appear in Props; unconsumed keys appear in Props verbatim
// and leave the structural field at zero. The two predicates are checked
// against that observable, not against each other.
func TestCensus_Q4_ReservedKeyRoutingIsOneRuleAcrossSurfaces(t *testing.T) {
	for _, cell := range reservedKeyCorpus() {
		t.Run(cell.name, func(t *testing.T) {
			if got := strayKeyBinds(cell.kind, cell.key); got != cell.binds {
				t.Errorf("strayKeyBinds(%q, %q) = %v, want %v", cell.kind, cell.key, got, cell.binds)
			}

			src := reservedKeySchema(cell)
			s, err := Parse(src)
			if cell.rejects {
				if err == nil {
					t.Fatalf("Parse(%s) accepted; a case-variant of a REQUIRED key means the attribute is absent, which must reject loudly", src)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%s): %v", src, err)
			}

			root := s.Root()
			gotStructural := structuralFieldFor(&root, cell.key)
			_, gotProps := root.Props[cell.key]

			if gotStructural != cell.structural {
				t.Errorf("structural field set = %v, want %v (schema %s)", gotStructural, cell.structural, src)
			}
			if gotProps != cell.inProps {
				t.Errorf("key in Props = %v, want %v (schema %s, props %v)", gotProps, cell.inProps, src, root.Props)
			}
			// The biconditional itself. "Never both" is universal. "Never
			// neither" holds for every cell except the documented
			// drop (NOT_BUGS #72), which is why the exception is an
			// expectation the cell states rather than a silence.
			if gotStructural && gotProps {
				t.Errorf("key %q surfaced BOTH structurally and in Props — the routing is meant to pick exactly one", cell.key)
			}
			if cell.dropped {
				if gotStructural || gotProps {
					t.Errorf("key %q reached a surface; the documented exception (NOT_BUGS #72) says a bound key whose read declines this body lands nowhere. If the drop is gone, delete `dropped` and state the new routing",
						cell.key)
				}
			} else if !gotStructural && !gotProps {
				t.Errorf("key %q reached NEITHER surface and is not the documented exception — either the routing lost it, or a new exception needs a ruling and a `dropped` cell",
					cell.key)
			}

			// The rebuild must reach the same routing, or one representation
			// reads the key differently than the parser did.
			rebuilt, err := root.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			rb := rebuilt.Root()
			if cell.reportedFinding != "" {
				if structuralFieldFor(&rb, cell.key) == gotStructural {
					t.Errorf("the rebuild no longer loses %q — the reported finding is fixed; update the registry and delete reportedFinding.\n  %s", cell.key, cell.reportedFinding)
				} else {
					t.Logf("REPORTED FINDING (not fixed in a census round): %s", cell.reportedFinding)
				}
				return
			}
			if structuralFieldFor(&rb, cell.key) != gotStructural {
				t.Errorf("the rebuild changed the structural verdict for %q", cell.key)
			}
			if _, p := rb.Props[cell.key]; p != gotProps {
				t.Errorf("the rebuild changed the Props verdict for %q", cell.key)
			}
		})
	}
}

// The corpus must exercise every axis the rulings distinguish, or a
// case-folding or shape-blind implementation would pass.
func TestCensus_Q4_CorpusIsNotVacuous(t *testing.T) {
	var binding, nonBinding, variant, malformed int
	var consumed, propped, rejected, dropped, unclassified int
	for _, c := range reservedKeyCorpus() {
		if c.binds {
			binding++
		} else {
			nonBinding++
		}
		if c.key != strings.ToLower(c.key) {
			variant++
		}
		if c.body == `3.7` || c.body == `["a"]` {
			malformed++
		}
		switch {
		case c.rejects:
			rejected++
		case c.dropped:
			dropped++
		case c.structural:
			consumed++
		case c.inProps:
			propped++
		default:
			unclassified++
		}
	}
	if binding < 3 || nonBinding < 6 || variant < 4 || malformed < 3 {
		t.Fatalf("corpus misses an axis: binding=%d nonBinding=%d variant=%d malformed=%d",
			binding, nonBinding, variant, malformed)
	}
	if consumed < 3 || propped < 4 || rejected < 2 {
		t.Fatalf("corpus misses an outcome: consumed=%d propped=%d rejected=%d", consumed, propped, rejected)
	}
	// The documented exception must stay exercised, and must stay an
	// exception: a corpus with no drop cell would let the "never neither"
	// assertion pass vacuously, and one where drops outnumbered the ordinary
	// outcomes would mean the rule had quietly become the other way round.
	if dropped != 1 {
		t.Fatalf("the drop exception is meant to be exactly one cell (NOT_BUGS #72), got %d — a new one needs its own ruling", dropped)
	}
	// A cell that declares no outcome at all is a corpus bug: it would run
	// every assertion against zero expectations and report agreement.
	if unclassified != 0 {
		t.Fatalf("%d corpus cells declare no outcome — each must state rejects, dropped, structural or inProps", unclassified)
	}
}

// ---------------------------------------------------------------------------
// Q17 driver: the SPLICE question has two answerers on two representations.
//
// The metadata splice (toJSONWalk, gated by nodeIsNameRefShape) works on a
// SchemaNode tree; the cache splice (inlineTreeDefs's wrapper arm) works on
// the raw JSON tree before any SchemaNode exists. Neither can call the other,
// so the only thing keeping them in step is that they answer the same policy:
// a RESERVED usage-site key cannot survive onto the definition, a CUSTOM
// property merges onto it definition-wins.
//
// This drives both over the same corpus of wrapper keys and requires the same
// verdict per key. The verdict is read off the OBSERVABLE — where the key
// surfaces on the resulting schema — never off either implementation, so the
// test cannot be satisfied by the two sharing a bug.
type spliceWrapperCell struct {
	key  string
	body string
	// merges is the expected verdict, taken from the RULINGS rather than
	// re-derived from either implementation — otherwise agreement between two
	// answerers that share a bug would read as a pass. A key MERGES onto the
	// definition exactly when it is an ordinary custom property there; a key
	// the definition's kind would CONSUME is usage-site metadata and drops,
	// because a definition cannot carry a second one for its usage site.
	merges bool
	ruling string
	// def overrides the plain fixed definition. A wrapper key is only ever
	// SKIPPED by the merge because the DEFINITION's own kind and logical
	// consume it, so a corpus of plain definitions never reaches that arm at
	// all — disabling the skip guard changed nothing until these cells
	// existed.
	def string
}

func spliceWrapperCells() []spliceWrapperCell {
	const usageSite = "#25: a definition cannot carry a second name/namespace/doc for its usage site"
	return []spliceWrapperCell{
		{"doc", `"usage-site"`, false, usageSite, ""},
		{"aliases", `["Other"]`, false, usageSite, ""},
		{"namespace", `"z"`, false, usageSite, ""},
		{"logicalType", `"uuid"`, false, "#70: a STRING logicalType is first-class and consumed, so it is usage-site metadata here", ""},
		{"logicalType", `123`, true, "#70: no value but a string can name a logical, so a non-string spelling is an ordinary prop", ""},
		{"precision", `3`, true, "#71: precision/scale are reserved only on a recognized decimal carrier; unconsumed they ride verbatim", ""},
		{"scale", `1`, true, "#71, same clause", ""},
		{"order", `"ignore"`, true, "no type-level kind BINDS \"order\" — it is a field attribute — so on a definition of any kind it is an ordinary custom property and merges like one", ""},
		{"default", `"D"`, true, "only an enum binds a schema-level \"default\"; on a definition of any other kind it is an ordinary custom property and merges", ""},
		{"my.custom", `"v"`, true, "#63 splice-merge: wrapper custom properties merge onto the definition, definition-wins", ""},
		{"my.custom", `{"nested":[1,2]}`, true, "#63 splice-merge, container body", ""},
		// Definition-consumes cells: the def is a decimal carrier, so its own
		// precision/scale/logicalType are meaningful and a usage-site copy
		// must not overwrite or accompany them.
		{"precision", `9`, false, "#63 splice-merge is definition-wins: a key the DEFINITION consumes cannot be re-supplied by the usage site", decimalCarrierDef},
		{"scale", `7`, false, "#63 splice-merge, definition-wins, same clause", decimalCarrierDef},
		{"logicalType", `"uuid"`, false, "#63 splice-merge, definition-wins: the def's own logical stands", decimalCarrierDef},
		{"my.custom", `"v"`, true, "#63 splice-merge: a custom property merges even onto a def that consumes reserved keys", decimalCarrierDef},
		// The one cell where ONLY the reserved-key skip can decide. The def
		// consumes scale but emits none (spec default 0 is not written), so
		// the definition-wins exact-key check sees no collision and would let
		// a usage-site scale through — silently changing the definition's
		// decimal semantics. Every other cell is decided earlier, by the
		// parse routing or by the key already being present.
		{"scale", `7`, false, "#63 splice-merge is definition-wins on CONSUMED-ness, not merely on keys the def happens to emit: an omitted scale is the spec default 0, not an opening", decimalNoScaleDef},
		// The one kind that BINDS a schema-level "default". Same key, same
		// wrapper spelling, opposite verdict from the plain-def cell above —
		// so the corpus proves the routing reads the DEFINITION's kind and
		// not the key's name.
		{"default", `"D"`, false, "#63 splice-merge, definition-wins: an enum consumes \"default\" as its evolution default, so a usage site cannot supply a second one", enumCarrierDef},
	}
}

// enumCarrierDef is a definition whose kind CONSUMES "default" (the enum
// evolution default), so a wrapper carrying that key reaches the merge's
// reserved-key skip.
const enumCarrierDef = `{"type":"enum","name":"x.y.F","symbols":["D","E"]}`

// decimalNoScaleDef consumes scale but emits none, so only the reserved-key
// skip stands between a usage-site scale and the definition's semantics.
const decimalNoScaleDef = `{"type":"fixed","name":"x.y.F","size":4,"logicalType":"decimal","precision":4}`

// decimalCarrierDef is a definition whose kind and logical CONSUME
// precision/scale, so a wrapper carrying them reaches the merge's skip arm.
const decimalCarrierDef = `{"type":"fixed","name":"x.y.F","size":4,"logicalType":"decimal","precision":4,"scale":2}`

// spliceVerdict is what a splice did with the wrapper's key, phrased so both
// representations can be asked the same way.
type spliceVerdict struct {
	spliced bool // did the definition materialize in place of the reference?
	inProps bool // did the key ride onto the result as a custom property?
	// structural records the key landing on the definition's own structural
	// field. Props alone is a blind observable whenever the DEFINITION's kind
	// BINDS the key: a merged "default" onto an enum is consumed on re-parse
	// and vanishes from Props, so a Props-only reader cannot tell "the merge
	// skipped it" from "the merge supplied the definition's own default".
	structural bool
	propValue  string
}

func TestCensus_Q17_SpliceWrapperKeyVerdictAgreesAcrossRepresentations(t *testing.T) {
	const plainDef = `{"type":"fixed","name":"x.y.F","size":4}`

	for _, c := range spliceWrapperCells() {
		t.Run(c.key+"="+c.body+defLabel(c.def), func(t *testing.T) {
			def := plainDef
			if c.def != "" {
				def = c.def
			}
			// The def's own kind, read off the definition rather than
			// assumed, so "did it splice" is asked of whatever kind the
			// cell's carrier is.
			var defObj struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal([]byte(def), &defObj); err != nil {
				t.Fatalf("cell definition is not a JSON object: %v", err)
			}
			wrapper := `{"type":"x.y.F",` + strconv.Quote(c.key) + `:` + c.body + `}`

			// Answerer 1 — the METADATA splice. A second occurrence inside one
			// self-contained schema, extracted so the walk actually splices
			// (a whole-schema walk never does: the tree defines the name).
			meta := func() (spliceVerdict, error) {
				s, err := Parse(`{"type":"record","name":"x.y.R","fields":[
					{"name":"a","type":` + def + `},
					{"name":"b","type":` + wrapper + `}]}`)
				if err != nil {
					return spliceVerdict{}, err
				}
				sub := s.Root().Fields[1].Type
				out, err := sub.Schema()
				if err != nil {
					return spliceVerdict{}, err
				}
				return verdictFromSpliced(out.Root(), c.key, defObj.Type), nil
			}

			// Answerer 2 — the CACHE splice, on the raw JSON tree. The
			// definition arrives from a prior Parse, so the wrapper is the
			// only occurrence and the cache must inline it.
			cache := func() (spliceVerdict, error) {
				var cc SchemaCache
				if _, err := cc.Parse(def); err != nil {
					return spliceVerdict{}, err
				}
				s, err := cc.Parse(`{"type":"record","name":"x.y.R2","fields":[{"name":"b","type":` + wrapper + `}]}`)
				if err != nil {
					return spliceVerdict{}, err
				}
				return verdictFromSpliced(s.Root().Fields[0].Type, c.key, defObj.Type), nil
			}

			mv, merr := meta()
			cv, cerr := cache()
			if (merr == nil) != (cerr == nil) {
				t.Fatalf("the two representations disagree on ACCEPTANCE: metadata err=%v, cache err=%v", merr, cerr)
			}
			if merr != nil {
				return // both rejected; the rejection parity is the verdict
			}
			if mv != cv {
				t.Fatalf("the two representations disagree on what the splice did with %q:\n metadata %+v\n    cache %+v", c.key, mv, cv)
			}
			// And the policy itself, so agreement on a WRONG answer still
			// fails. The expectation comes from the ruling the cell cites.
			if mv.inProps != c.merges {
				got, want := "dropped", "merge onto the definition"
				if mv.inProps {
					got, want = "merged as a prop ("+mv.propValue+")", "drop as usage-site metadata"
				}
				t.Errorf("wrapper key %q was %s; the ruling says it must %s — %s", c.key, got, want, c.ruling)
			}
			// A dropped key must reach NEITHER surface. Where the
			// definition's own kind BINDS the key, a merged copy is consumed
			// on re-parse and disappears from Props, so Props alone cannot
			// see the drop fail — the structural landing is what makes those
			// cells measure anything.
			if !c.merges && mv.structural {
				t.Errorf("wrapper key %q landed on the definition's structural field; a usage site cannot supply a value the definition binds — %s", c.key, c.ruling)
			}
			if !mv.spliced {
				t.Errorf("the wrapper did not splice at all, so this cell measures nothing")
			}
		})
	}
}

// verdictFromSpliced reads the observable off a spliced result. defKind is
// the definition's own kind, so "did it splice" means the node carries that
// kind's DEFINING content rather than still being a bare name reference —
// every cell's definition is named x.y.F, but not every one is a fixed.
//
// The identity compared is the FULLNAME, not the raw Name: the metadata
// splice preserves the definition's dotted spelling while the cache splice
// re-emits it as name+namespace, and that normalization difference is not
// this question's answer. Comparing the raw field made every cell disagree,
// which is the tell that a driver is measuring the wrong thing — genuine
// divergence is selective.
func verdictFromSpliced(n SchemaNode, key, defKind string) spliceVerdict {
	full := n.Name
	if !strings.Contains(full, ".") && n.Namespace != "" {
		full = n.Namespace + "." + full
	}
	defined := false
	switch defKind {
	case "fixed":
		defined = n.Size == 4
	case "enum":
		defined = len(n.Symbols) > 0
	}
	v := spliceVerdict{
		spliced:    n.Type == defKind && defined && full == "x.y.F",
		structural: structuralFieldFor(&n, key),
	}
	if raw, ok := n.Props[key]; ok {
		v.inProps = true
		b, _ := json.Marshal(raw)
		v.propValue = string(b)
	}
	return v
}

// The corpus must exercise BOTH sides of the policy, or agreement is vacuous.
func TestCensus_Q17_CorpusIsNotVacuous(t *testing.T) {
	var drop, merge int
	for _, c := range spliceWrapperCells() {
		if c.ruling == "" {
			t.Errorf("cell %q cites no ruling; the expectation must be derived from policy, not from the code", c.key)
		}
		if c.merges {
			merge++
		} else {
			drop++
		}
	}
	if drop < 5 || merge < 4 {
		t.Fatalf("corpus expects %d drops and %d merges; it must drive both sides of the policy", drop, merge)
	}
	// The same SPELLING must appear on both sides, or the corpus proves only
	// that the routing reads key names — the logicalType pair is what makes
	// the verdict body-dependent rather than name-dependent.
	bodies := map[string]map[bool]bool{}
	for _, c := range spliceWrapperCells() {
		if bodies[c.key] == nil {
			bodies[c.key] = map[bool]bool{}
		}
		bodies[c.key][c.merges] = true
	}
	var split bool
	for _, v := range bodies {
		if len(v) == 2 {
			split = true
		}
	}
	if !split {
		t.Fatal("no key appears with both verdicts; the corpus cannot tell name-conditional routing from body-conditional routing")
	}
}

// defLabel distinguishes the plain-definition cells from the
// definition-consumes cells in subtest names.
func defLabel(def string) string {
	switch def {
	case "":
		return ""
	case enumCarrierDef:
		return "/on-enum-def"
	default:
		return "/on-decimal-def"
	}
}
