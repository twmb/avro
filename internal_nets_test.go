package avro

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"math"
	"math/big"
	"math/bits"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ---------- predicate_census_test.go ----------

// The predicate-agreement census.
//
// A schema question ("is this branch the null type", "does this Type name
// this definition", "what will json.Marshal emit for this value") usually has
// to be answered in more than one place. The same schema exists in several
// representations at once: the as-written parse tree (aschema), the compiled
// wire tree (schemaNode), the metadata tree (SchemaNode), the pre-Parse `any`
// tree SchemaFor composes, and the cache's raw JSON tree. Every answerer is
// hand-written, and a hand-written answer is a snapshot of the rule at the
// moment it was typed. Two failure modes follow, and both have shipped. Two
// answerers of one question disagree, so identical inputs take different
// paths depending on which representation we consulted. Or an answerer
// restates an *external* authority's accept-set (the name resolver's
// spellings, encoding/json's key resolver) more narrowly than the authority,
// so we refuse valid input or panic on an unguarded case.
//
// We cannot reach either by generating more inputs, which is what every other
// net here does. An input matrix is derived from the bug that motivated it, so
// it holds the set of implementations constant. Here we generate over
// *implementations* instead. For each question we name the canonical predicate
// (or the external authority), every answerer across every representation, and
// a corpus spanning the question's domain. The driver runs every answerer over
// the whole corpus and requires identical verdicts. Where the authority is
// external we execute it, never restate it.
//
// TestCensus_NoUnregisteredAnswerers reads the package sources and requires
// every syntactic occurrence of a question's tell to be a registered site, so
// a new hand-written answerer cannot land unexamined. Adding or editing a
// predicate means updating this registry. The registry is the list of places
// an answer can drift.

// censusAnswerer is one site that answers a question. note is empty when the
// site routes through the question's canonical predicate. A non-empty note is
// the documented reason this site *cannot* collapse into it. An undocumented
// second answerer is a finding, so "" plus a site that does not call the
// canonical predicate is exactly what we want review to catch.
type censusAnswerer struct {
	repr string // which representation of the schema this site reads
	site string // function or identifier, for the failure message
	file string
	note string
	// placement states *where* the answer is computed, for a question whose
	// rule ranges over a whole collected set rather than over one value. Two
	// answerers can agree on the rule and disagree on where it runs. Nothing
	// comparing answers can see that: at the outermost call both give the
	// same verdict, and the divergence appears one level deeper. Empty means
	// the rule is per-value.
	//
	// TestCensus_PlacementFactsMatchSource machine-checks this against source.
	// placementWholeSet requires the site's function to NOT recurse,
	// placementPerLevel requires that it does.
	placement string
	// walk names the recursion whose collected set the rule ranges over. The
	// placement fact is meaningless without it. A rule may sit downstream of
	// several recursions, and running once per level of a *different* one is
	// often exactly right.
	walk string
}

// The placement vocabulary. A whole-set rule evaluated per recursion level
// decides on a *partial* set, which is a different rule wearing the same code.
const (
	placementWholeSet = "once, over the root's complete collected set (the site's function must not recurse)"
	placementPerLevel = "once per recursion level, over that level's own set (the site's function recurses)"
)

// censusTell is a source substring whose every occurrence in non-test code
// must be a registered site of the question. files lists the files it may
// appear in: an occurrence in any other file fails the guard, and a listed
// file with no occurrence has rotted.
type censusTell struct {
	pattern string
	files   []string
}

type censusQuestion struct {
	id        string
	question  string
	authority string // the canonical predicate, or the external authority
	answerers []censusAnswerer
	tells     []censusTell
}

// censusRegistry is the enumeration. It grows one question per commit. The
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
			{
				repr: "compiled schemaNode, by TABLE", site: "unionBranchOfKind(node, \"null\") / serUnion's tags.branchByKind(\"null\")", file: "json_codec.go",
				note: "the JSON and binary encoders' nil-first dispatch asks WHICH branch is the null one, once per value. Both read unionTags.byKind, which schema.go's buildUnion keys off branch.kind — so the comparison still happens, once at parse time, in the case arm that builds the table. Registered as its own answerer because the tell for the comparison form cannot see a map lookup: the question is spelled as an argument here, and a site that changes which kind string it passes would otherwise be invisible to this census.",
			},
		},
		tells: []censusTell{
			{pattern: `unionBranchOfKind(node, "null")`, files: []string{
				"json_codec.go",
			}},
			{pattern: `branchByKind("null")`, files: []string{
				"ser.go",
			}},
			{pattern: `== "null"`, files: []string{
				"json_codec.go",
				"json_decode.go",
				"resolve.go",
				"schema_for.go", // both inside isNullBranchTree
				"schema.go",     // both inside isNullBranch
			}},
			{pattern: `!= "null"`, files: []string{
				"json_codec.go",
				// Not answerers of this question. We register them so the
				// guard does not flag them: json_scan.go matches the four
				// bytes of the JSON literal `null` in a *value* stream, and
				// ocf.go compares a compression codec's name.
				"json_scan.go",
				"ocf/ocf.go",
			}},
			{pattern: `literal("null"`, files: []string{
				// Not an answerer of this question. We register it so the site
				// stays visible: schema_decode.go consumes the four bytes of
				// the JSON literal `null` while tokenizing schema *text*,
				// exactly as json_scan.go does for a value stream. It decides
				// what a byte sequence is, not what a schema type is, so it
				// classifies no union branch. A site spelling the word is still
				// worth a row: the next one to appear here might be classifying
				// rather than tokenizing.
				"schema_decode.go",
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
			{pattern: `+ "." +`, files: []string{
				"cache.go",       // nodeFullnameTree
				"schema_node.go", // nodeFullname
				"schema_for.go",  // SchemaFor composition
				"schema.go",      // builder qualification (2727); logical key (2165), not this question
				// Not answerers: an error field-path join, not a schema name.
				"compat.go",
				"errors.go",
				// Not an answerer: a kind.logicalType lookup key.
				"json_codec.go",
			}},
			{pattern: `+"."+`, files: []string{
				"schema.go", // scopedRefKeys: the binding side of the same rule
				"cache.go",  // dupDefRef: which spelling re-binds after a splice
			}},
		},
	},
	{
		id:       "Q3",
		question: "What is the JSON object name for this Go map key?",
		authority: "INTERNAL: mapKeyName (schema_node.go). encoding/json's key resolver used to answer this, " +
			"executed per cell by TestMatrix_WalkBudgetMapKeyMatchesJSONKeyResolver, until the v2 " +
			"implementation (Go 1.27) changed two of its arms: it names a string-kind key by its MarshalText " +
			"where v1 used the raw string, and it formats a float-kind key where v1 refused it. A name that " +
			"depends on the toolchain is no answer, so the package names every key itself, once, and both " +
			"canonicalizing copies emit that name before json.Marshal sees the map. The matrix still executes " +
			"json.Marshal for every cell where the two implementations agree, and states the package's own " +
			"rule for the two where they do not",
		answerers: []censusAnswerer{
			{repr: "caller `any` tree, budget walk", site: "valueWalkLimit, through mapKeyName", file: "schema_node.go"},
			{repr: "caller `any` tree, fixup walk", site: "applyJSONFixupKind, through mapKeyName", file: "schema_node.go"},
			{repr: "caller `any` tree, SchemaFor canonicalize walk", site: "canonicalizeTreeValue, through mapKeyName", file: "schema_for.go"},
			{
				repr: "caller `any` tree, which maps to rewrite", site: "canonicalStringKeyMap", file: "schema_node.go",
				note: "different-by-design: this predicate asks a NARROWER question, 'is this a map we rewrite into map[string]any', which is true only for the string KIND. A non-string-kind key's name comes from its MarshalText or integer formatting under both encoding/json implementations, so those maps stay marshal-opaque and are never rewritten; mapKeyName still charges their keys.",
			},
		},
		tells: []censusTell{
			{pattern: `.Key().Kind()`, files: []string{
				"schema_node.go", // canonicalStringKeyMap
				// Not answerers of this question: these decide the Go type of
				// an Avro map's keys for encode/decode/resolve/SchemaFor.
				// There the key form is Avro's own string wire, not a JSON
				// object key.
				"deser.go",
				"json_codec.go",
				"json_decode.go",
				"resolve.go",
				"schema_for.go",
				"ser.go",
			}},
			{pattern: `mapKeyName(`, files: []string{
				"schema_node.go", // the definition, the budget walk, the fixup walk
				"schema_for.go",  // the SchemaFor canonicalize walk
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
			{repr: "caller `any` tree, budget walk", site: "valueWalkLimit + marshalEmitLen + mapKeyName", file: "schema_node.go"},
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
			{pattern: `jsonMarshalerType`, files: []string{
				"schema_node.go",
				"schema_for.go",
			}},
			{pattern: `json.Marshaler`, files: []string{
				"schema_node.go",
			}},
			// Rejected tells, recorded so the next question's design starts
			// from evidence. `MarshalText()` has 4 hits, but reflect.go:161
			// and ser.go:1063 are the Avro encode path (Q13), and a tell
			// spanning two questions cannot fail for one of them.
			// `encoding.TextMarshaler` is the same defect at 16 hits.
			// `reflect.Kind` switches match nearly every walker, and a tell
			// that matches everything reports nothing. The usable pair above
			// is narrow because both names exist only to answer "does this
			// type define its own JSON form".
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
			{pattern: `.AvroType != "" &&`, files: []string{
				"schema.go",      // hasMatchingCustomTypeCond
				"custom_type.go", // CustomType.matches
			}},
			{pattern: `.LogicalType != "" &&`, files: []string{
				"schema.go",
				"custom_type.go",
			}},
			// Rejected tell: `matches(` is too generic, and it names only one
			// of the two answerers. The site most likely to drift away, the
			// hand-rolled clause pair inside the build-time loop, would not
			// be watched at all.
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
			{pattern: `decimalConsumesPrecisionScale`, files: []string{
				"schema_node.go",
				"schema_parse.go",
				"schema.go",
			}},
			// Rejected tell: `Logical == ""` has 6 hits in schema.go, three of
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
			{pattern: `isNilValue`, files: []string{
				"ser.go",
				"json_codec.go",
			}},
			{pattern: `isNilableKind`, files: []string{
				"ser.go",
				"unsafe.go",
			}},
			// Rejected tell: `IsNil()` has 41 hits across 11 files, and most
			// answer a different question entirely (is this reflect.Value safe
			// to deref, is this pointer field set). A tell that broad cannot
			// fail for *this* question.
		},
	},
	{
		id:       "Q13",
		question: "Which text route does this type take — its MarshalText, its raw string kind, or raw bytes?",
		authority: "the text-route precedence order, enforced by two gates: stringFastPathEligibleEncode / " +
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
			{pattern: `stringFastPathEligible`, files: []string{
				"reflect.go",
				"unsafe.go",
				"deser.go",
			}},
			{pattern: `implementsTextMarshaler`, files: []string{
				"reflect.go",
				"schema_for.go",
			}},
			// Rejected tell: `MarshalText()` names the call, not the routing
			// decision, and spans the schema-tree budget's emission question
			// (Q9) as well. We also rejected `textOutFor`: it is the shared
			// helper, so counting it misses exactly the sites that bypass it.
			// Those are the ones that can drift.
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
				repr: "node-kind predicate", site: "branchIsNamedKind", file: "compat.go",
				note: "the *schemaNode twin of isNamedKind, classifying three spellings rather than four because the builder has already normalized \"error\" into \"record\" by the time a node exists. The union-tag sites in json_codec.go ask it rather than spelling the set again.",
			},
			{
				repr: "compat literal set", site: `case "record", "enum", "fixed":`, file: "compat.go",
				note: "different-by-design as a FORM, not as an answer: a switch arm cannot call a predicate and still be a switch arm. It owes the identical classification, which the driver checks through the property that defines it — whether a definition of that kind can be referenced by name. This arm is branchIsNamedKind's own body, and is now the last literal copy of the NAMED set.",
			},
			{
				repr: "parse + build literal sets", site: `case "record", "error":`, file: "schema.go",
				note: "same form-vs-answer split for the RECORD half: the build arms and the parse grammar spell the record kinds literally where a switch cannot call isRecordKind.",
			},
			{
				repr: "reference expansion, three passes", site: "isRecordKind (markCycles / sizeOf / copy)", file: "schema_node.go",
				note: "ExpandReferences walks the tree three times — cycles, then sizes, then the copy — and each asks the shared predicate rather than spelling the kinds. The three must agree exactly: a pass that descended a kind the others did not would size a tree it never builds, or build one it never sized.",
			},
		},
		tells: []censusTell{
			{pattern: `isNamedKind`, files: []string{
				"cache.go", "schema_canonical.go", "schema_for.go",
				"schema_node.go", "schema_parse.go", "schema_walk.go", "schema.go",
			}},
			{pattern: `isRecordKind`, files: []string{
				"schema_canonical.go", "schema_for.go", "schema_node.go",
				"schema_parse.go", "schema_walk.go",
			}},
			// branchIsNamedKind is the *schemaNode twin of isNamedKind: by the
			// time a node exists the builder has normalized "error" into
			// "record", so it classifies three spellings where isNamedKind
			// classifies four. It is its own tell because the union-tag sites
			// in json_codec.go route through it rather than spell the set.
			{pattern: `branchIsNamedKind`, files: []string{
				"compat.go", "json_codec.go",
			}},
			{pattern: `"record", "enum", "fixed"`, files: []string{
				"compat.go",
			}},
			{pattern: `"record", "error"`, files: []string{
				"schema_node.go", "schema_parse.go", "schema.go",
			}},
			// Rejected tell: `== "record"` also matches the recursion question
			// (json_decode.go's `kind == "record" || kind == "array" || kind
			// == "map"`, which asks whether a kind nests, not whether it is
			// named). Two questions, one tell.
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
			{pattern: `tag == "-"`, files: []string{
				// The two paths, plus one occurrence inside the guard's own
				// doc comment describing where it is called from.
				"schema_for.go",
				"reflect.go", // the runtime mapper's two paths
			}},
			{pattern: `checkSkipDirectiveExact`, files: []string{
				// Definition, both call sites, and two doc references.
				"schema_for.go",
			}},
			// Rejected tell: `HasPrefix(tag` also matches the "default="
			// option scan, a different question entirely, and it misses the
			// exact-match skips that are the agreement invariant.
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
			{pattern: `flatFieldNeedsLift`, files: []string{
				"schema_parse.go", "schema_walk.go",
			}},
			{pattern: `flatLiftTypeMap`, files: []string{
				"schema_parse.go", "schema_node.go", "cache.go",
			}},
			// Rejected tell: `liftFlatFieldType` names the mutator, which only
			// the parse path calls, so the walker and renderer sites (the ones
			// that could reconstruct the map differently) would go unwatched.
		},
	},
	{
		id:       "Q4",
		question: "Is this key RESERVED on this kind — consumed into a structural field, or an ordinary custom property?",
		authority: "the rulings, not the code: reserved names match only their exact lowercase " +
			"spelling, so a variant is an ordinary prop on every surface, body-independent; " +
			"routing on a non-binding kind is shape-conditional and placement-conditional, never case-conditional; " +
			"and a reserved key that is neither bound nor carried structurally goes only to Props. " +
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
			{pattern: `strayKeyBinds`, files: []string{
				"schema_parse.go", "schema_node.go",
			}},
			{pattern: `schemaKeyBinds`, files: []string{
				"schema_node.go",
			}},
			{pattern: `schemaReservedKeyForObject`, files: []string{
				"schema_node.go", "schema_parse.go", "cache.go",
			}},
			// Rejected tell: `strayBodyShapeOK` has 20 hits across three files,
			// but it answers the *shape* question (does this body parse as the
			// key's schema form), which is an input to the routing rather than
			// the routing itself. Counting it would make the guard fire on
			// shape-check refactors that do not touch reservedness.
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
				repr: "reference expansion, is this a bare reference", site: "nodeCarriesOnlyType", file: "schema_node.go",
				note: "ExpandReferences asks the same question to decide what may be replaced by a definition. A reference carrying anything of its own must NOT be — Schema would collapse the expanded copy back to a reference and lose it — which is exactly this predicate's question, not a looser one.",
			},
			{
				repr: "the shared field-set walk", site: "nodeCarriesNothingBut", file: "schema_node.go",
				note: "not a second answerer but the ONE walk both questions run — Q16 and Q17 differ only in the exemption function they pass. Two structurally identical reflect loops is the shape this pair of questions was already burned by, so there is one loop and the difference is data.",
			},
		},
		tells: []censusTell{
			{pattern: `nodeCarriesOnlyType`, files: []string{
				// Definition, its three call sites, and three doc references
				// (counted with grep -o; doc comments count, and reasoning
				// about the number has been wrong every time).
				"schema_node.go",
			}},
			{pattern: `bareEmissionExempt`, files: []string{
				"schema_node.go",
			}},
			// Rejected tell: `len(n.Props) == 0` is the shape the old
			// hand-written lists shared. It still appears in unrelated emptiness
			// checks, and after the fix it no longer marks this question's sites
			// at all.
			//
			// The durable guard is not a tell but
			// TestInvariant_BareEmissionCoversEverySchemaNodeField. It sets
			// every exported field in turn and requires the predicate to notice
			// both halves: that the field blocks, and that the object form it
			// falls through to carries the value through an emit and re-parse
			// round trip. Proving only the blocking half left the emitter free
			// to drop the value with nothing but the render changed, which is
			// what EnumDefault did.
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
			"its usage site, and the parse puts those on the structural fields, so blocking them " +
			"would convert an adjudicated silent drop into a hard \"unknown complex type\" error on the extraction " +
			"feature; plus Props, which the splice MERGES onto the definition . " +
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
			{pattern: `nodeIsNameRefShape`, files: []string{
				// Definition, its one call site, and one doc reference.
				"schema_node.go",
			}},
			{pattern: `nameRefUsageSiteExempt`, files: []string{
				"schema_node.go",
			}},
			// Rejected tell: `n.refTarget` marks the stamp, which is
			// nodeRefTargetAgrees's question. That one is asked beside this
			// one at the same call site, so counting it would make this
			// question fire on stamp changes.
			//
			// As with Q16 the durable guard is not a tell.
			// TestInvariant_NameRefSpliceCoversEverySchemaNodeField sets each
			// exported field on an extracted reference and requires the
			// predicate to notice. TestMatrix_CallerComposedAndEditedNodes
			// crosses that with the recursive, diamond, forward-reference and
			// cross-parse structures. This class's failure mode is a member the
			// rule never mentioned.
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
			{pattern: `jsonNullBody`, files: []string{
				// The doc heading, the definition, one call in each of the two
				// decode helpers, and one doc reference from intPtrFrom, whose
				// comment records what the guard restores.
				"schema_parse.go",
			}},
			// Rejected tell: `== nil` is the most common comparison in the
			// package, answering "is this pointer/error/interface unset" almost
			// everywhere. The question here is about a decoded JSON body
			// specifically, which is what the named predicate marks.
			//
			// The durable guard is TestMatrix_ReservedKeyBodyPresence. It
			// crosses every reserved key with a typed destination against
			// {absent, valid, null, wrong-typed, quoted} at both levels. We
			// require the null verdict to equal the wrong-typed one on every
			// surface.
		},
	},
	{
		id:       "Q19",
		question: "How many bytes will the DECODER charge against the decimal unscaled-value bound, and does this emit path stay inside it?",
		authority: "checkDecimalUnscaledLen (deser.go) owns the bound, and checkDecimalUnscaledSize is the same " +
			"function for a caller that knows the width before it has the bytes. Asking ONE function on both " +
			"sides is what makes over-rejection impossible by construction: encode refuses exactly the payloads " +
			"decode refuses, so a wire this package produces is a wire it can read. The bound itself is " +
			"twmb-specific DoS defense (Java/fastavro/avro-rs store significand+scale and never base-convert), " +
			"which is precisely why no reference can be consulted about the emit side — the standing rule that " +
			"every reader-side cap needs a producer-side compliance check is the authority there. " +
			"What is genuinely answered more than once is WHICH BYTES, because the three wire shapes differ: on " +
			"a bytes/decimal the payload IS the unscaled value, on a fixed/decimal it is the schema SIZE after " +
			"padding, and on a big-decimal it is the length-prefixed INNER slice",
		answerers: []censusAnswerer{
			{repr: "bytes/fixed decimal, both wires, consume", site: "setDecimalValue", file: "deser.go"},
			{repr: "big-decimal inner unscaled, both wires, consume", site: "parseBigDecimalPayload", file: "deser.go"},
			{repr: "JSON codepoint payload, consume", site: "assignBytes + the into-any arm", file: "json_decode.go"},
			{repr: "numeric carrier -> bytes/decimal, both wires, emit", site: "decimalUnscaledBytes", file: "ser.go"},
			{repr: "numeric carrier -> fixed/decimal, both wires, emit", site: "appendDecimalFixed (charges SIZE, the padded width)", file: "ser.go"},
			{repr: "numeric carrier -> big-decimal, both wires, emit", site: "buildBigDecimalPayload (charges the INNER unscaled)", file: "ser.go"},
			{
				repr: "opaque []byte escape, both logicals, emit", site: "chargeOpaqueDecimalBytes", file: "ser.go",
				note: "the arm that reaches the wire without any of the shared builders. It delegates the bound but must decide WHICH BYTES itself, because the caller-supplied payload is the framing on a big-decimal and the unscaled value on a decimal. A framing it cannot read is left alone deliberately: the decoder then fails on the framing, which is a different question than this bound.",
			},
			{
				repr: "pre-encoded field DEFAULT, emit", site: "chargeDecimalDefault, recorded on serRecordField.defaultErr", file: "ser.go",
				note: "the only answerer whose verdict is RECORDED rather than raised: a default is pre-encoded at parse, and refusing it there would refuse the schema, which a reader that DROPS the field must still be able to parse. Four consumers read the pre-encoded bytes and each surfaces the verdict at the moment they would reach the wire (three splice sites via serRecordField.appendDefault, plus the compiled unsafe path's omitzeroErr). The JSON default arm is deliberately NOT here: it encodes the default VALUE through appendAvroJSON, so it is already charged as an ordinary emit.",
			},
			{
				repr: "opaque escape -> fixed, binary and JSON, emit", site: "the size charge in serFixedDecimal.ser and the JSON fixed decimal arm", file: "ser.go + json_codec.go",
				note: "not a separate rule: the fixed opaque arm writes exactly the schema size, the same quantity appendDecimalFixed charges for the numeric arm. It is a separate SITE only because the opaque path never reaches that builder, and neutering either one alone reds its own cells.",
			},
		},
		tells: []censusTell{
			{pattern: `checkDecimalUnscaled`, files: []string{
				// deser.go 9 over 8 lines: the doc heading, both definitions
				// (one line names both, so it matches twice), the delegation
				// line, two binary consume sites, and the RatFromBytes comment
				// recording why that public entry keeps its own guard.
				// ser.go 7: decimalUnscaledBytes, appendDecimalFixed,
				// chargeOpaqueDecimalBytes, the fixed opaque arm,
				// buildBigDecimalPayload and chargeDecimalDefault, every emit
				// route to the wire, plus decimalChargeLen's doc naming the
				// function whose input it computes.
				"deser.go",
				"ser.go",
				"json_decode.go",
				"json_codec.go",
			}},
			// Rejected tell: `maxDecimalUnscaledBytes` names the constant, not
			// the question. A new emit path that hard-codes 32<<10 instead of
			// asking would keep this count unchanged. That is exactly the
			// drift the question exists to catch. The predicate name is what
			// distinguishes delegating from restating.
			//
			// The durable guard is TestMatrix_SelfReadableAtScale's
			// decimal-unscaled-length axis. It crosses carrier x logical x
			// container x wire x length, and we assert the
			// encode-implies-decode invariant rather than any count.
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
			{pattern: `&SemanticError{`, files: []string{
				// ser.go and json_codec.go carry the decimal unscaled-length
				// producer charges (Q19): the three shared builders and the
				// fixed opaque arm on the binary side, the fixed opaque arm on
				// the JSON side. They name the same identity as every other
				// encode-side user-value reject.
				"ser.go",
				"json_codec.go",
				"deser.go",
				"json_decode.go",
				"unsafe.go",
				"resolve.go",
				"promote.go",
				"reflect.go",
				"custom_type.go",
				"errors.go",
			}},
			// Rejected tell: `semErr(` is the constructor most of these sites
			// call, so counting it double-counts the same answerers and misses
			// the ones that build the struct literally. `&SemanticError{` is
			// the shape a new hand-written identity decision takes.
		},
	},
	{
		id:       "Q20",
		question: "Which tag does this union branch emit, and which branch does a tag resolve to?",
		authority: "findUnionBranch (json_codec.go) is the RESOLVER and therefore the authority: it scans exact " +
			"branch names first and only then tries the \"<kind>.<logicalType>\" qualifier. unionEmitTag is the " +
			"canonical predicate for the emit direction, and it exists to respect that order rather than restate " +
			"it. The two spellings share one namespace — \"bytes.decimal\" is both the qualifier of a " +
			"decimal-on-bytes branch and the fullname of a fixed named \"decimal\" in namespace \"bytes\" — so a " +
			"branch whose qualifier another branch owns exactly must emit its unqualified name instead. Without " +
			"that, a value's own tagged JSON decodes back onto a different branch. The oracle is " +
			"calibration-free and needs no reference: a tag must name exactly one branch, and a value's tagged " +
			"round trip must land where it started",
		answerers: []censusAnswerer{
			{repr: "JSON encode envelope (EMIT)", site: "appendTaggedUnion", file: "json_codec.go"},
			{repr: "JSON decode tagged-map wrap (EMIT)", site: "wrapUnion", file: "json_decode.go"},
			{repr: "resolved union wrap tables (EMIT)", site: "resolveReaderUnion / resolveUnionUnion", file: "resolve.go"},
			{
				repr: "compiled EMIT tables (binary decode wrap)",
				site: "fillUnionTagTables, deser.logicalNames", file: "schema.go",
				note: "different-by-design as a SITE, same rule: a TABLE built once at parse time cannot call unionEmitTag per branch, so it applies the identical degrade. It is the SOLE guard for the binary decode wrap tag, and the round-trip matrix drives that consumer directly rather than inferring it from the JSON one.",
			},
			{
				repr: "the ACCEPT side, both wires", site: "unionTagTiers", file: "json_codec.go",
				note: "the accept question is the INVERSE of the emit question — which branch does a caller-written tag name — and it has its own authority: the tier order in unionTagTiers. There is now ONE walk of it, in fillUnionTagTables, whose result is unionTags.byName; every consumer (the binary tagged-map encoder, the JSON encoder, the JSON decoder) asks that table, and findUnionBranch is the ask. So the accept-sets are equal by IDENTITY rather than by two walks agreeing. scanUnionBranch keeps the walk as the fallback for a node built without a table and as the net's oracle. Registered here because emitting a tag no consumer accepts, or accepting one on a single wire, are the same defect seen from two ends. Guarded by TestInvariant_UnionTagTiersAreDerived (source: the walk stays in one place and findUnionBranch may not re-open it), TestInvariant_EveryUnionTagTierIsReachable (a tier no corpus reaches ships unexercised), TestInvariant_UnionTagTableMatchesTheTierWalk (the table answers what the walk answers) and TestInvariant_EveryUnionNodeCarriesItsTagTable (a node holding branches without the table sends its consumers back to the walk, once per value).",
			},
		},
		tells: []censusTell{
			{pattern: `unionEmitTag`, files: []string{
				// Definition, its doc heading, and the one call in
				// appendTaggedUnion.
				"json_codec.go",
				// wrapUnion's call plus the comment naming why it is shared
				// with the encode side.
				"json_decode.go",
				// Two calls, each with a comment line naming the reader union
				// as the namespace the tag resolves against.
				"resolve.go",
			}},
		},
	},
	{
		id:       "Q21",
		question: "Has this reader field slot already been claimed by an earlier writer field / JSON key?",
		authority: "a dedicated presence flag, never the claiming NAME. The empty string is a legal name " +
			"component under a caller-supplied WithLaxNames validator, so a name-valued slot cannot distinguish " +
			"\"unclaimed\" from \"claimed by the field named \\\"\\\"\" — the guard then skips its own check for " +
			"exactly one input and reports a collision as clean. Presence is a []bool; the claiming name is kept " +
			"only to name the collision in the error message. All three sites answer the same question because " +
			"two writer fields (or two JSON keys) reaching one reader slot is the same misconfiguration on every " +
			"wire, and compat.go must agree with resolve.go in particular: Resolve calls CheckCompatibility " +
			"first, so a disagreement means a caller is told a schema pair is fine and then fails at Resolve",
		answerers: []censusAnswerer{
			{repr: "CheckCompatibility, writer field to reader slot", site: "checkRecordFieldClaimsUnique", file: "compat.go"},
			{repr: "Resolve, writer field to reader slot", site: "resolveRecord", file: "resolve.go"},
			{repr: "JSON object decode, JSON key to reader slot", site: "iterateRecordFields", file: "json_decode.go"},
		},
		tells: []censusTell{
			{pattern: `make([]bool,`, files: []string{
				"compat.go",      // claimed
				"resolve.go",     // readerMatched
				"json_decode.go", // seen
				// Not an answerer, registered so the guard does not flag it:
				// this tracks which struct fields carry an omitzero directive,
				// which is a property of the Go type and claims no slot.
				"reflect.go",
				// Not an answerer either, and a near miss worth naming: this
				// one marks which union *branches* produced a tag claim while
				// the tag tables are built. It is per-branch bookkeeping
				// inside one walk, not a record of who claimed a reader
				// *field* slot, and no second writer can contend for it. The
				// tier's own ambiguity rule (Q20) resolves a duplicate claim
				// there. That is a different question with a different remedy.
				"schema.go",
			}},
		},
	},
	{
		id:       "Q22",
		question: "Is this schema-declared magnitude saturated before it enters arithmetic?",
		authority: "saturateSchemaMagnitude / maxSchemaMagnitude (deser.go) — ONE ceiling for every consumer, " +
			"stated where the reason for its value lives. A `fixed` size is the only parse-time quantity whose " +
			"VALUE is not bounded by the length of the text declaring it (nineteen characters name 2^63, and the " +
			"parser deliberately leaves the upper bound open); precision and scale are capped during validation, " +
			"and field / branch / symbol counts each cost bytes to write. The magnitude PROPAGATES, which is what " +
			"makes this a question rather than a field read: a per-record SUM over field minimums contains no " +
			"size read anywhere in its expression and wraps just as readily, so a set built by grepping the field " +
			"is the wrong set. A second consumer reasoning out its own ceiling is the drift this watches — there " +
			"were two before this question existed, and the second one's comment named the hazard correctly while " +
			"the first stayed open for the arithmetic that actually crashed.",
		answerers: []censusAnswerer{
			{repr: "the accessor itself", site: "saturateSchemaMagnitude", file: "deser.go"},
			{repr: "per-element wire minimum (fixed / union / record arms)", site: "minBytesWalk.minBytesFromChildren", file: "deser.go"},
			{repr: "decimal capacity for a fixed size", site: "maxDecimalDigits", file: "schema.go"},
			{
				repr: "probe-buffer allocation for a fixed logical", site: "jsonDecodeAppliesLogical", file: "json_decode.go",
				note: "different-by-design, and the ONE place a different ceiling is correct: this magnitude becomes " +
					"a make() length, and the arithmetic ceiling is a fine addend but a terrible allocation. It caps " +
					"at the largest length any fixed logical inspects. The accessor's doc states why allocation is a " +
					"separate question, so the reason lives in one place even though the number cannot.",
			},
		},
		// The counts include doc mentions. The reason a ceiling has its value
		// is part of what must not drift. A consumer added with a comment
		// explaining its own bound is exactly the shape this question exists
		// to catch, and it would arrive as a count change here.
		tells: []censusTell{
			{pattern: `saturateSchemaMagnitude(`, files: []string{
				"deser.go",  // the definition plus the fixed / union / record arms
				"schema.go", // maxDecimalDigits
			}},
			{pattern: `maxSchemaMagnitude`, files: []string{
				"deser.go", // the const and the accessor, plus the prose stating the ceiling once
			}},
			{pattern: `magnitudeWidestMultiplier`, files: []string{
				"deser.go",  // the const and the prose tying the ceiling to it
				"schema.go", // the multiply itself
			}},
		},
	},
	{
		id:       "Q23",
		question: "Which promoted struct field owns an Avro name, and when is the collision ambiguous?",
		authority: "Go's own field promotion, EXECUTED via reflect.Type.FieldByName: it returns false for a name " +
			"promoted ambiguously, which is the same condition a program hits as a compile error on x.V. The " +
			"package adds ONE tier the language has no notion of — a tagged field beats an untagged one at equal " +
			"depth, since the collision there is in Avro name space and Go sees two differently-named fields — so " +
			"the untagged cells are decided by Go and the tagged tier by the documented tiebreaker.",
		answerers: []censusAnswerer{
			{
				repr: "Go struct type, for the inferred schema", site: "resolvePromotedFields", file: "schema_for.go",
				placement: placementWholeSet, walk: "collectFieldsRaw",
			},
			{
				repr: "Go struct type, for the shared encode/decode field map", site: "typeFieldMapping", file: "reflect.go",
				placement: placementWholeSet, walk: "collect",
				note: "different-by-design as a FUNCTION — it produces index paths for a schema's field names, not " +
					"schemaFields — but it must agree cell for cell, because a schema SchemaFor built has to be one " +
					"Encode and Decode can use. What the two share is the rule and its placement, and the placement " +
					"is the half no verdict comparison can see: this site keeps its resolution outside its own " +
					"recursive closure, and the other keeps it outside collectFieldsRaw.",
			},
		},
		tells: []censusTell{
			// The report resolves index paths that accumulate from the root, so
			// it only denotes a field when the type it is resolved against is
			// the root. Every occurrence of that resolution is an answerer.
			{pattern: `t.FieldByIndex(existing.index).Name`, files: []string{
				"schema_for.go",
				"reflect.go",
			}},
			{pattern: `duplicate field name`, files: []string{
				"schema_for.go",
				"reflect.go",
			}},
		},
	},
	{
		id:       "Q24",
		question: "Is this caller-supplied ocf.Codec nil — i.e. is there nothing here to call a method on?",
		authority: "ocf.isNilCodec. The external authority is the Go language itself: an interface holding a nil " +
			"pointer is not equal to nil, so `c == nil` answers a NARROWER question than the one every caller " +
			"means. The same distinction is already answered correctly in this package by Schema.Decode's target " +
			"guard (deser.go), which asks reflect for Kind plus IsNil rather than comparing the interface.",
		answerers: []censusAnswerer{
			{
				repr: "the offer chosen BY NAME (both reader-side constructors)",
				site: "resolveCodec's scan", file: "ocf/ocf.go",
				note: "the scan is what DECIDES adoption, so it runs over offers about to be declined; asking " +
					"Name() of a nil there crashed a constructor on an offer it was never going to take.",
			},
			{
				repr: "the offer chosen BY POSITION (NewWriter)",
				site: "NewWriter's last-non-nil adoption loop", file: "ocf/ocf.go",
				note: "the writer has no header to match against, so it answers the same question positionally. " +
					"This is the answerer whose ABSENCE was the whole defect class: the two choosers disagreeing " +
					"about what a nil offer is made one option work on one constructor and SIGSEGV on the others.",
			},
			{
				repr: "the offers NOT chosen, at release", site: "releaseUnadopted's loop", file: "ocf/ocf.go",
			},
			{
				repr: "the adopted offer, at release bookkeeping", site: "releaseUnadopted's pre-marking", file: "ocf/ocf.go",
				note: "unreachable while both choosers filter, and kept deliberately — see the comment at the " +
					"site, which records the measured combination that makes it operative.",
			},
		},
		tells: []censusTell{
			// Every consult of the predicate. A site that answers this question
			// by hand instead shows up as a count that did not rise.
			{pattern: `isNilCodec(`, files: []string{
				"ocf/ocf.go", // 4 consults + the declaration
			}},
		},
	},
}

// censusOutstanding is the enumeration's open end. A question lands here the
// moment we discover it. That usually happens when we reject a candidate tell
// because it answers a different question, which is the census noticing a row
// it has not asked yet. Recording it with the tell that revealed it is what
// stops it being lost between rounds. The total is not fixed. We report it as
// "N registered, M outstanding, enumeration open", never as a final count.
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

// censusDemoted records questions we examined and found NOT to be census
// material, with the evidence. A genuine one-answerer question with no
// external authority has nothing to disagree with, so a driver for it would
// assert a function against itself. Saying so is a result. Leaving it
// unexplained invites a later round to re-derive the same enumeration. The bar
// is the *rule's* shape, not the helper's name. We wrongly flagged two
// questions before applying this bar, and both turned out to have several
// hand-written answerers.
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

// perLevelRanges returns the source ranges in file whose code runs once per
// recursion level: the body of every self-calling function, and the body of
// every func literal called through the variable holding it. The second shape
// is what a name-only scan misses, and it is the one that matters. A function
// can declare a recursive closure and still run a rule outside it, which is
// the correct arrangement. The question is not where a rule is written. It is
// whether a body that repeats per level can reach it.
//
// repeatingBody locates the named walk's body and reports its byte range plus
// the names it calls. We name the walk rather than derive it. Schema inference
// recurses too, and a field collector running once per level of *that* walk is
// correct, each level being a different record. Only the author knows which
// recursion's collected set the rule ranges over.
func repeatingBody(t *testing.T, file, walk string) (lo, hi int, calls map[string]bool) {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("parsing %s: %v", file, err)
	}
	off := func(p token.Pos) int { return fset.Position(p).Offset }
	calleeOf := func(ce *ast.CallExpr) string {
		switch fn := ce.Fun.(type) {
		case *ast.Ident:
			return fn.Name
		case *ast.SelectorExpr:
			return fn.Sel.Name
		}
		return ""
	}
	collect := func(body ast.Node) map[string]bool {
		out := map[string]bool{}
		ast.Inspect(body, func(n ast.Node) bool {
			if ce, ok := n.(*ast.CallExpr); ok {
				if c := calleeOf(ce); c != "" {
					out[c] = true
				}
			}
			return true
		})
		return out
	}

	var body *ast.BlockStmt
	// A func literal bound to `walk`...
	ast.Inspect(f, func(n ast.Node) bool {
		bind := func(names []ast.Expr, vals []ast.Expr) {
			for i, v := range vals {
				fl, ok := v.(*ast.FuncLit)
				if !ok || i >= len(names) {
					continue
				}
				if id, ok := names[i].(*ast.Ident); ok && id.Name == walk {
					body = fl.Body
				}
			}
		}
		switch x := n.(type) {
		case *ast.AssignStmt:
			bind(x.Lhs, x.Rhs)
		case *ast.ValueSpec:
			names := make([]ast.Expr, len(x.Names))
			for i, id := range x.Names {
				names[i] = id
			}
			bind(names, x.Values)
		}
		return true
	})
	// ...or a declared function of that name.
	if body == nil {
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil && fd.Name.Name == walk {
				body = fd.Body
			}
		}
	}
	if body == nil {
		t.Fatalf("%s: no function or bound closure named %q — the registered walk is not there", file, walk)
	}
	calls = collect(body)
	if !calls[walk] {
		t.Fatalf("%s: %q does not call itself, so it is not a repeating body and naming it as the walk is wrong",
			file, walk)
	}
	return off(body.Pos()), off(body.End()), calls
}

// enclosingFunc names the declared function containing the given byte offset.
func enclosingFunc(t *testing.T, file string, offset int) string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("parsing %s: %v", file, err)
	}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		if lo, hi := fset.Position(fd.Pos()).Offset, fset.Position(fd.End()).Offset; offset >= lo && offset < hi {
			return fd.Name.Name
		}
	}
	return ""
}

// TestCensus_PlacementFactsMatchSource turns a registered placement into a
// checked fact.
//
// Some questions are answered by a rule that ranges over a whole collected
// set, not one value. Which promoted field owns a name cannot be decided from
// one embedded struct's own fields, because a shallower field declared
// anywhere above takes it. Such a rule has a second property besides its
// content, namely *where* it runs. Nothing that compares answers can see a
// difference there, because at the outermost call the two placements agree.
//
// So the registry states the placement, and here we assert it against source
// in both directions, at the position of the question's own tell. A rule
// claiming to run over the complete set must not sit inside a body that
// repeats per level, and one claiming to run per level must.
func TestCensus_PlacementFactsMatchSource(t *testing.T) {
	stated := 0
	for _, q := range censusRegistry {
		for _, a := range q.answerers {
			if a.placement == "" {
				continue
			}
			stated++
			if a.walk == "" {
				t.Errorf("%s: %s states a placement but names no walk; the fact is uncheckable without the recursion whose set the rule ranges over",
					q.id, a.site)
				continue
			}
			lo, hi, walkCalls := repeatingBody(t, a.file, a.walk)

			// The question's tell *is* the rule. Find where it sits.
			b, err := os.ReadFile(a.file)
			if err != nil {
				t.Fatalf("reading %s: %v", a.file, err)
			}
			var offsets []int
			for _, tell := range q.tells {
				if i := bytes.Index(b, []byte(tell.pattern)); i >= 0 {
					offsets = append(offsets, i)
				}
			}
			if len(offsets) == 0 {
				t.Errorf("%s: answerer %s (%s) states a placement but none of the question's tells appear in that file — the row has rotted",
					q.id, a.site, a.file)
				continue
			}
			for _, off := range offsets {
				// Two ways the rule can run per level: written inside the
				// walk, or called from it. Extracting it into its own
				// function and calling it from the walk moves the text and
				// changes nothing, so containment alone is not the check.
				inside := off >= lo && off < hi
				fn := enclosingFunc(t, a.file, off)
				called := fn != "" && walkCalls[fn]
				perLevel := inside || called

				switch a.placement {
				case placementWholeSet:
					if perLevel {
						how := "is written inside " + a.walk
						if !inside {
							how = "is in " + fn + ", which " + a.walk + " calls"
						}
						t.Errorf("%s: %s (%s) is registered as running ONCE over the root's complete set, but the rule %s.\n"+
							"  Run per level, a whole-set rule decides on a PARTIAL set — a collision below the root is settled before the level that resolves it has been read — and any index path it resolves is in the root's coordinate space while its receiver is the nested type.\n"+
							"  Either take it back out of the walk, or change the placement fact and say why the rule is now per-level.",
							q.id, a.site, a.file, how)
					}
				case placementPerLevel:
					if !perLevel {
						t.Errorf("%s: %s (%s) is registered as running per level of %s, but %s neither contains nor calls it — the fact describes code that is no longer there",
							q.id, a.site, a.file, a.walk, a.walk)
					}
				default:
					t.Errorf("%s: %s carries an unrecognized placement %q; use placementWholeSet or placementPerLevel so the fact is checkable",
						q.id, a.site, a.placement)
				}
			}
		}
	}
	// Anti-rot in the other direction: with nothing stating a placement this
	// guard is watching an empty set and would pass forever.
	if stated == 0 {
		t.Fatal("no answerer states a placement, so this guard is checking nothing")
	}
	t.Logf("checked %d placement facts against source", stated)
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

// occurrences reports, per file, the line numbers where pattern appears in
// code. Comments are blanked first: the registry counts answerers, and prose
// naming a function is not one, so a tell that matched comments would churn
// on every comment edit.
func occurrences(t *testing.T, files []string, pattern string) map[string][]int {
	t.Helper()
	found := make(map[string][]int)
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("reading %s: %v", f, err)
		}
		for i, line := range strings.Split(blankGoComments(b), "\n") {
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

// blankGoComments replaces every comment in src with spaces, keeping newlines
// so line numbers hold.
func blankGoComments(src []byte) string {
	fset := token.NewFileSet()
	file := fset.AddFile("", fset.Base(), len(src))
	var s scanner.Scanner
	s.Init(file, src, nil, scanner.ScanComments)
	out := append([]byte(nil), src...)
	for {
		pos, tok, lit := s.Scan()
		if tok == token.EOF {
			break
		}
		if tok != token.COMMENT {
			continue
		}
		off := file.Offset(pos)
		for i := off; i < off+len(lit) && i < len(out); i++ {
			if out[i] != '\n' {
				out[i] = ' '
			}
		}
	}
	return string(out)
}

// TestCensus_NoUnregisteredAnswerers fails when a hand-written answerer of a
// registered question appears outside the registry: a new file, or a new
// occurrence in a file already listed. It also fails when a registered site
// vanishes, which is the vacuity direction. A tell that matches less than it
// claims means the registry describes code that no longer exists. Every
// driver keyed to it is then guarding nothing.
func TestCensus_NoUnregisteredAnswerers(t *testing.T) {
	files := censusSourceFiles(t)
	for _, q := range censusRegistry {
		if len(q.tells) == 0 {
			t.Errorf("%s (%s) registers no tells, so nothing guards it", q.id, q.question)
		}
		for _, tell := range q.tells {
			found := occurrences(t, files, tell.pattern)
			if len(found) == 0 {
				t.Errorf("%s: tell %q matches nothing in the package; the registry has rotted", q.id, tell.pattern)
				continue
			}
			for file, lines := range found {
				if !slices.Contains(tell.files, file) {
					t.Errorf("%s: tell %q appears in UNREGISTERED file %s (lines %v).\n  A new site answering %q must be added to the census registry with its representation, and either routed through %s or given the documented reason it cannot be.",
						q.id, tell.pattern, file, lines, q.question, q.authority)
				}
			}
			for _, file := range tell.files {
				if len(found[file]) == 0 {
					t.Errorf("%s: tell %q is registered in %s but matches nothing there; the site moved or was deleted, and the registry no longer describes the code",
						q.id, tell.pattern, file)
				}
			}
		}
	}
	// The registry must not silently shrink. Deleting a question is a
	// decision: the question stopped being one, or we demoted it into
	// censusDemoted with its evidence. We make that decision here, not by
	// letting a row quietly disappear.
	const registered = 17
	if len(censusRegistry) < registered {
		t.Fatalf("census registry has %d questions, was %d; a question was removed without "+
			"recording why. Demote it into censusDemoted with its evidence, or lower this floor "+
			"deliberately", len(censusRegistry), registered)
	}
}

// ---------------------------------------------------------------------
// Q1: is this union branch the null type?
// ---------------------------------------------------------------------

// nullSpellingCell is one branch spelling plus the answer every
// representation owes for it.
type nullSpellingCell struct {
	name   string
	branch string // the branch as written inside a union
	isNull bool
}

// The corpus spans the question's domain. We take both spellings Avro admits
// for null, the wrapped form carrying each kind of inert metadata, and
// near-misses that must answer false. Avro defines no null logical type, so
// neither props nor logicalType can make a wrapped null stop being null. The
// near-misses include a named type whose name merely contains "null", which a
// substring-minded answerer would get wrong.
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

// TestCensus_Q1_NullBranchAgreement runs every representation's answerer over
// the whole corpus and requires identical verdicts. Disagreement here is the
// bug class directly. The wire bytes of the two null spellings are identical,
// so nothing in a round-trip net can see the two sides diverge. Only a derived
// artifact does: which encoder arm we select, which branch a lift targets,
// which error identity surfaces.
func TestCensus_Q1_NullBranchAgreement(t *testing.T) {
	for _, cell := range nullSpellingCorpus {
		t.Run(cell.name, func(t *testing.T) {
			// The branch sits second so the union is never null-first,
			// keeping the three representations comparable at a fixed index
			// (a null-first 2-branch union takes a different builder arm).
			// The anchor is boolean because no corpus cell is boolean, and a
			// union may not repeat a type.
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
// answers and both spellings. A corpus that drifted to all-true (or all bare)
// would let a broken answerer pass.
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
// Q2: what fullname does a named-type definition occupy?
// ---------------------------------------------------------------------

// A definition's fullname is the string a reference has to bind to, so every
// representation that computes one has to land on the same answer. The name
// can arrive dotted, split across a "namespace" attribute, inherited from the
// enclosing scope, or explicitly escaped back to the null namespace. The rules
// interact: a dotted name outranks both the attribute and the enclosing scope.
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
	// The leading-dot escape is normalized at parse into the null-namespace
	// fullname. In a definition it carries an empty namespace component,
	// which the strict grammar rejects, so these cells only exist under an
	// accept-all validator. That is exactly where a normalization only one
	// representation performs would go unnoticed.
	{name: ".Foo", lax: true, want: "Foo"},
	{name: ".Foo", nsAttr: "other", hasNSKey: true, lax: true, want: "Foo"},
}

// TestCensus_Q2_DefinitionFullnameAgreement builds one enclosing schema per
// cell and asks each representation what fullname the inner definition
// occupies. The compiled tree is the authority: it is the name the wire
// builder registers, and therefore the name a reference actually binds to.
// A metadata or cache-tree answer that differs means a reference resolves to
// one type on the wire and another in the surface that re-emits it.
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
			s := mustParse(t, text, opts...)
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
// Q9: what will json.Marshal emit for this value?
// ---------------------------------------------------------------------

// chargedBytes runs the schema-tree budget walk over v and reports how many
// bytes it charged: the walk's own answer to "how much will json.Marshal
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

// emissionRouteCell is one route by which content reaches json.Marshal's
// output, given as a small value and a larger twin that differs only in how
// much content travels that route. Comparing the two isolates the route. A
// walk that does not model it charges the same for both, while json.Marshal
// emits the difference.
type emissionRouteCell struct {
	name         string
	small, large any
	// openRuling, when set, records that this route is known to under-charge
	// and that the question of what to do about it is with the maintainer.
	// Such a cell asserts the under-charge still holds. Whichever way the
	// ruling goes the cell reds and forces this registry to be updated, so
	// the disagreement cannot be silently resolved in either direction.
	openRuling string
}

// We keep the openRuling machinery below even though no question currently
// uses it. It is how we record a disagreement with two defensible resolutions
// without either leaving the suite red or letting the question close itself
// silently. Its first use was the escaped-vs-content byte charge, now fixed.
// Those cells are ordinary agreement cells again, which is the mechanism
// working as designed.

func emissionRouteCorpus() []emissionRouteCell {
	const (
		lo = 64
		hi = 4096
	)
	big := func(n int) string { return strings.Repeat("v", n) }
	return []emissionRouteCell{
		{name: "plain-string", small: big(lo), large: big(hi)},
		{name: "named-string-kind", small: namedStringKey(big(lo)), large: namedStringKey(big(hi))},
		// json.Marshal escapes a string's contents, so a byte of content is
		// not a byte of output: a control byte becomes \u00XX (six), as does
		// each of the HTML-escaped set. An all-printable cell cannot see the
		// difference between charging content and charging emission.
		{name: "string-control-bytes", small: strings.Repeat("\x01", lo), large: strings.Repeat("\x01", hi)},
		{name: "string-html-escaped", small: strings.Repeat("<", lo), large: strings.Repeat("<", hi)},
		{name: "string-map-key-control", small: map[string]int{strings.Repeat("\x01", lo): 1}, large: map[string]int{strings.Repeat("\x01", hi): 1}},
		// []byte reaches json.Marshal as the Avro codepoint string, not as a
		// byte slice, so its emitted size depends on the byte values: ASCII
		// costs one byte, 0x80-0xFF two (UTF-8), and a control byte six
		// (\u00XX). The walk charges the raw length, so we give the three
		// classes separate cells. A single ASCII cell would never see it.
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
// json.Marshal against json.Marshal itself, per route. The budget bounds what
// json.Marshal will emit, so the charge must grow at least as fast as the real
// output. An under-charge means that route is free, which is how a value with
// its own MarshalJSON once cost one node and zero bytes while emitting
// megabytes. We compare a delta, because the walk deliberately does not charge
// for structural punctuation. It cannot decline to charge for content without
// the delta collapsing.
func TestCensus_Q9_EmissionRouteChargeTracksJSON(t *testing.T) {
	for _, cell := range emissionRouteCorpus() {
		t.Run(cell.name, func(t *testing.T) {
			// The authority is json.Marshal of what the pipeline actually
			// hands it. boundedSerializableValue charges the budget and then
			// returns jsonSerializableValue(v), so the fixups (a []byte
			// becoming the Avro codepoint string, ±Inf becoming a literal)
			// are part of the emission the budget is supposed to bound. If we
			// marshaled the raw value instead we would compare against bytes
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
				// A recorded disagreement. We assert it still holds, so
				// resolving it either way reds this cell and forces the
				// registry to be updated. An open question must not be able
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
// matter: the two delegating routes (a value's own MarshalJSON / MarshalText,
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
	// authority delta were zero, which is separately guarded. A corpus where
	// nothing at all is charged means chargedBytes is broken.
	if got := chargedBytes(t, strings.Repeat("x", 128)); got < 128 {
		t.Fatalf("chargedBytes reports %d for a 128-byte string; the measurement itself is broken", got)
	}
}

// ---------------------------------------------------------------------
// Q11: what identity does a failure carry?
// ---------------------------------------------------------------------

// A caller's only programmatic handle on a failure is its identity: whether it
// is errors.As-able to *SemanticError, and what that error's Field path says.
// The same failure reached through the binary and the JSON decoder must
// present the same handle, or errors.As succeeds on one wire and fails on the
// other for a caller who only changed format.
// TestMatrix_EncodeErrorIdentityCensus drives the encode half. Here we drive
// the decode half, which had only three spot subtests. We use the same schema
// and value on both wires, decoded into a Go target that cannot hold it, so
// both decoders reach a target-type failure from equivalent input.
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
			// documented ordinal behavior and succeeds on both wires, so it
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
// value on both wires and decodes it into a target that cannot hold it. We
// require agreement, not a particular verdict. Which identity a family carries
// is policy (doc.go "# Errors"), but a caller who switches format must not
// find errors.As changing its answer.
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

// The corpus must span both scalar and container targets, and must include a
// record-field position: the field path is where the two decoders most
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
// to them, because delegation is impossible for measurement. Asking the
// emitter how long its output is means producing that output, the very
// allocation the budget exists to prevent. We allow a restatement only with an
// executed differential over the authority's complete domain, and that is what
// this is. Expectations come from marshalSchemaTree, the package's own
// emitter. Escaping below utf8.RuneSelf is byte-local, so testing all 256
// single-byte values is a proof over that part of the domain, not a sample.
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

// The []byte arm charges the value's json-facing image, the Avro codepoint
// string the fixup produces, so its differential runs through that fixup.
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

// The early exit is what makes the scan bounded by the budget rather than by
// the input. We prove that deterministically rather than by timing. Escaping
// never shrinks, so the running total passes the limit within limit+1 input
// bytes, and the returned value is therefore the same for inputs of wildly
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
	// And the whole walk over a value far larger than the budget reaches the
	// same verdict, since the scan abandons it rather than measuring it.
	huge := strings.Repeat("\x01", 32<<20) // 32 MiB of 6x-escaping content
	b := newWalkBudget()
	if r := valueWalkLimit(reflect.ValueOf(huge), maxSchemaJSONDepth, &b); r != valueWalkTooLarge {
		t.Fatalf("a 32 MiB control-byte string must bust the byte budget, got code %d", r)
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
// Q14: does this CustomType match this node?
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

// TestCensus_Q14_CustomTypeMatchAgreesAcrossAnswerers runs both answerers over
// the cross. They must agree everywhere except on the wildcard, where the
// build-time answerer deliberately declines. A CustomType that names neither a
// kind nor a logical type must not suppress the built-in handlers, because it
// decides per value at runtime via ErrSkipCustomType. We assert that exception
// explicitly rather than skip it, so if the build-time side ever stops
// excluding wildcards this cell reds.
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

// The cross must contain cells that answer both ways on each axis, or the
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
// Q5: does a field-level decimal lift *consume* precision/scale?
// ---------------------------------------------------------------------

// Two answerers navigate the field's type to find where a field-level
// logicalType lands. fieldDecimalLiftConsumesPrecisionScale decides whether
// the pair is consumed, which makes a malformed body reject loudly instead of
// riding to Props. liftFieldLogicalIntoType decides where the annotation goes.
// The verdict's own comment says it mirrors the lift, and that is a claim.
// They have drifted before, when one skipped a wrapped null branch and the
// other did not. Both run inside parseSchemaTree, so neither is callable on a
// pre-lift tree. We observe each through the consequence it owns.
type liftTargetCell struct {
	name      string
	fieldType string // the field's "type" as written
	// byDesign, when set, is the documented reason the two navigations
	// deliberately disagree here. Such a cell asserts both directions, so
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
		// field's "decimal" never lands, yet the verdict reads the *field's*
		// logical, not the one that survives.
		{name: "target-already-annotated", fieldType: `{"type":"bytes","logicalType":"uuid"}`},
		{name: "union-target-already-annotated", fieldType: `["null",{"type":"bytes","logicalType":"uuid"}]`},
		// The discriminator: the target's own logical *is* decimal, so the
		// field's parameters land on a real decimal carrier and are
		// consumed. Without this cell the rule could be loosened into
		// "a target with any annotation of its own is inert".
		{name: "target-own-logical-is-decimal", fieldType: `{"type":"bytes","logicalType":"decimal"}`},
		// The union twin of the cell above. It was once unmeasurable: its
		// valid control did not parse, because the lift's union arm declined
		// to complete parameters the object arm supplied. It exists now,
		// which is the whole point of aligning the arms.
		{name: "union-target-own-logical-is-decimal", fieldType: `["null",{"type":"bytes","logicalType":"decimal"}]`},
		{name: "fixed-target-own-logical-is-decimal", fieldType: `{"type":"fixed","name":"F","size":4,"logicalType":"decimal"}`},
		// Non-decimal effective logical on a carrier: inert.
		{name: "target-own-logical-big-decimal", fieldType: `{"type":"bytes","logicalType":"big-decimal"}`},
	}
}

// What lands decides consumption, not where the lift points. The pair is
// consumed iff the target's effective logical (its own when it has one, else
// the field's) is "decimal" on a bytes/fixed carrier. We once recorded the two
// pre-annotated-target cells below as different-by-design on the opposite
// reading. Wire evidence retired that; see the discriminator cells, which
// prove consumed-ness rather than assuming it.

func liftFieldSchema(fieldType, precision string) string {
	return `{"type":"record","name":"R","fields":[{"name":"f","type":` + fieldType +
		`,"logicalType":"decimal","precision":` + precision + `,"scale":2}]}`
}

// consumedByRejection asks the verdict's question: a malformed precision body
// rejects only where the pair is consumed. "Parse failed" is NOT the signal,
// because it is confounded. When the pair is unconsumed we drop the malformed
// value, and if the lift still put a decimal annotation on a carrier, the
// type-level decimal validation then fails for a missing precision instead. So
// both paths return non-nil. The discriminator is the field gate's own
// message, naming the key it refused, the only error the verdict produces.
func consumedByRejection(t *testing.T, fieldType string) bool {
	t.Helper()
	if _, err := Parse(liftFieldSchema(fieldType, "4")); err != nil {
		t.Fatalf("the valid control must parse for %s: %v", fieldType, err)
	}
	_, err := Parse(liftFieldSchema(fieldType, "3.7"))
	return err != nil && strings.Contains(err.Error(), `record field "precision"`)
}

// consumedByLift asks the lift's question: after parsing, is the field's
// decimal annotation actually sitting on a bytes/fixed carrier, the only
// place precision/scale mean anything?
func consumedByLift(t *testing.T, fieldType string) bool {
	t.Helper()
	s, err := Parse(liftFieldSchema(fieldType, "4"))
	if err != nil {
		t.Fatalf("valid control: %v", err)
	}
	// The compiled tree is where the lift's effect lives: the metadata tree
	// preserves the schema as written, keeping the field-level annotation on
	// the field, so reading it would report that no lift ever happened.
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
// a schema where the pair is inert metadata: the opposite of the
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

// The corpus must contain carriers and non-carriers, both bare and wrapped,
// unions with both null spellings, and a target that already carries its own
// annotation. Otherwise the navigation is never actually exercised.
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
// Q10: is this Go value nil-equivalent (does it encode as Avro null)?
// ---------------------------------------------------------------------

// isNilValue's own doc names five dispatch sites that must agree on what
// counts as nil: the binary 2-branch [null,T] optimization, the binary
// try-each path through serNull, the JSON 2-branch short-circuit, the JSON
// try-each "null" arm, and the unsafe struct fast path. We reach four of them
// by choosing the schema shape and the wire. The builder picks the fifth from
// the target's shape, so it gets a struct cell.
//
// serNull peels separately from isNilValue rather than calling it, and the
// two have drifted before. A fix once claimed to bring serNull "into parity"
// but added only Interface peeling, leaving &nilPtr rejected.
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

// nilVerdictOf reports whether the schema encoded v as its null branch. The
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
// outright: the same Go value meaning two different things depending on the
// union's arity or the wire format.
func TestCensus_Q10_NilEquivalenceAgreesAcrossDispatchSites(t *testing.T) {
	const (
		twoBranch   = `["null","string"]`
		threeBranch = `["null","string","long"]`
	)
	for _, cell := range nilShapeCorpus() {
		t.Run(cell.name, func(t *testing.T) {
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

// The builder picks the fifth site from the target's shape, not from the
// schema, so it needs a struct whose field is a nullable pointer. Its own
// documented contract is that it cannot call isNilValue: it holds an
// unsafe.Pointer, not a reflect.Value. It instead declines every nilable inner
// kind to the reflect path. The agreement it owes is that a struct field
// reaches the same verdict as the bare value did above.
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

// The corpus must span every nilable kind the predicate accepts plus the
// indirection shapes that once broke it, and must contain non-nil controls.
// Otherwise "everything is nil" passes.
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
// Q13: which text route does this type take on encode?
// ---------------------------------------------------------------------

// A string-kind type with a MarshalText method encodes its marshaled form, not
// its raw string. The eligibility gates exist because the unsafe and container
// fast paths read the underlying string directly and bypass appendAvroString's
// text arm entirely. We must keep a type with a text method off those paths.
// The gate's answer and the route actually taken are two answers to one
// question, and the fast-path exclusion list is the sibling set.
//
// The method transforms its input, so the two routes are distinguishable. An
// identity method would make a bypassed fast path and a working text arm
// produce the same bytes.
type censusUpperText string

func (c censusUpperText) MarshalText() ([]byte, error) {
	return []byte(strings.ToUpper(string(c))), nil
}

type censusPlainString string

// TestCensus_Q13_TextRouteAgreesWithTheEligibilityGate crosses the gate's
// verdict with the route actually taken at every position a value can hold:
// scalar, struct field (the unsafe fast path's home), array element, and map
// value. A type the gate calls ineligible must encode its marshaled form
// everywhere. A type it calls eligible must encode its raw string everywhere.
func TestCensus_Q13_TextRouteAgreesWithTheEligibilityGate(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  reflect.Type
		// positions, each producing the encoded string content
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
			// The gate's contract: a type with a text method is ineligible
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

// The corpus must contain a type on both sides of the gate, and the text
// method must transform its input. An identity method cannot distinguish a
// bypassed fast path from a working text arm, so the whole driver would pass
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
// Q15: is this kind a *named* type, and is it a record?
// ---------------------------------------------------------------------

// "Named" is the property that decides whether a kind occupies a fullname
// other schemas can reference. isNamedKind and isRecordKind are the shared
// predicates, but the same classification is also written out as literal case
// sets: `case "record", "enum", "fixed":` in compat.go and json_codec.go,
// `case "record", "error":` in schema_canonical.go and schema_node.go. The
// observable is exact rather than a proxy. A kind is named iff a definition of
// that kind can be referenced by name from a sibling position. On an unnamed
// kind a "name" key is a stray custom property, so the reference must fail to
// resolve.
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
// definition cannot be referenced, or an unnamed kind whose stray "name"
// nonetheless binds a reference, means the name table and the predicate
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
// Q8: does this struct tag skip the field?
// ---------------------------------------------------------------------

// Two subsystems read avro struct tags, each on two structurally distinct
// paths. SchemaFor's named-field and anonymous-embed paths decide what a
// generated schema contains. The runtime field mapper's two decide what an
// encode/decode binds. All four spell the exact-match skip as `tag == "-"` and
// must agree, or a subsystem that stopped skipping would put back a field the
// caller excluded, on one side only.

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
		// The runtime mapper registers no target for a skipped field, which
		// the strict decoder surfaces as "missing field" on a schema that
		// carries one. That error *is* the skip: an unskipped sibling binds.
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
		// for it. The two subsystems agreeing is exactly the invariant.
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

// The grammar guard is deliberately scoped to SchemaFor. Here we assert both
// directions of that split so neither side can drift into the other. SchemaFor
// rejects a tag that starts with the directive without being it, because that
// is a typo it can name. The runtime mapper has never enforced tag grammar and
// treats the tag as a field name, which is what every other malformed tag does
// there. Collapsing either way would be a behavior change to a documented
// boundary, not a consistency fix.
func TestCensus_Q8_GrammarGuardIsSchemaForScoped(t *testing.T) {
	if _, err := SchemaFor[skipSuffixNamed](); err == nil {
		t.Error("SchemaFor must reject a tag that begins with the skip directive without being exactly it")
	} else if !strings.Contains(err.Error(), "exact-match only") {
		t.Errorf("reject does not name the directive rule: %v", err)
	}

	// The runtime mapper takes it as a field name and binds nothing unless
	// the schema happens to carry that name, with no grammar error either way.
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	wire := mustEncode(t, s, map[string]any{"a": int32(1)})
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
// Q7: is this field written in the flat form, needing a lift?
// ---------------------------------------------------------------------

// The flat (goavro-style) field form puts a complex kind's defining key beside
// the field's own keys instead of nesting a type object. One predicate,
// flatFieldNeedsLift, decides whether to lift, and three representations call
// it: parser, tree walker, metadata renderer. Agreement is therefore
// structural. What we drive here is that the three consult it on the *same*
// input. A walker that reconstructs the field map differently reaches a
// different verdict from the same predicate. The discriminator is a mismatched
// defining key ("symbols" beside "type":"array"), without which the corpus
// would pass on a predicate that lifted whenever any complex key was present.
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
		// The discriminator. The lift still fires, since "items" is array's
		// defining key, and it carries the foreign "symbols" into the lifted
		// array object, where the per-kind exclusivity rule rejects it. So
		// the verdict is lift and the outcome is a parse error, the
		// documented path. A predicate that lifted on any
		// complex key, or one that declined here, would both look fine
		// without this cell.
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
// one, a non-flat control, and the mismatched-key discriminator. Otherwise a
// predicate that lifts on any complex key present would pass.
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
// Q4: is this key reserved on this kind, or an ordinary custom property?
// ---------------------------------------------------------------------

// The most heavily adjudicated question in the package. We define the corpus
// from the rulings rather than re-derive it from the code:
//
//   - Reserved names match only their exact lowercase spelling. A
//     case-variant is an ordinary custom property on every reading surface,
//     body-independent.
//   - On a kind that does not bind the key, routing is
//     shape-conditional: a schema-shaped body surfaces structurally as-written,
//     a malformed body rides in Props verbatim, and the structural field stays
//     zero.
//   - Routing is placement-conditional, never case-conditional.
//
// The invariant those share is a biconditional, and that is what the driver
// asserts. The structural field is set iff the key was consumed, and Props
// holds exactly the raw keys that were not. strayKeyBinds is the binding
// predicate and schemaReservedKeyForObject the routing one. Both are callable,
// so we check them against the parse's observable rather than against each
// other.
//
// Two of the three implications are universal: consumed means NOT in Props, and
// structural field set means consumed. The third, consumed means structural
// field set, has one documented exception. "doc" is bound on
// every kind, but its capture is a silently-declining string read, so a
// non-string doc is consumed and lands on neither surface. That is exact Apache
// Avro behavior: parseDoc reads through getOptionalText, i.e.
// jsonNode.textValue(), null for a non-text node (Schema.java:1996-1998 and
// :2039-2042), and "doc" is in SCHEMA_RESERVED :176 / FIELD_RESERVED :504 so
// parseProperties skips it. We spell it as a cell outcome rather than let it
// fall through the corpus counters, so the exception is counted and cannot
// widen or close unnoticed.
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
	// dropped marks the documented exception to "consumed means structural
	// field set". The key is bound, so it stays out of Props, but the
	// binding read declines this body, so no structural field is set
	// either. It is one key ("doc") with a non-string
	// body, and every other reserved key with a non-conforming body either
	// routes to Props or rejects.
	dropped bool
	// reportedFinding, when set, records that the rebuild loses this key
	// today, contrary to the documented posture. The cell asserts the loss
	// still happens, so fixing it reds here and forces this registry to be
	// updated. A reported finding must not be able to close itself
	// silently, exactly like an open ruling.
	reportedFinding string
}

// We retain the reportedFinding mechanism though no cell uses it. The
// stray-rebuild loss it recorded is fixed: the bare-emission sites now ask
// one derived predicate, and a reflect guard keeps its field set complete.
// Those cells are ordinary agreement cells again, the mechanism working as
// designed.

func reservedKeyCorpus() []reservedKeyCell {
	return []reservedKeyCell{
		// Binding kind, exact spelling: consumed. The structural field is
		// set and the key never reaches Props.
		{name: "enum-symbols-exact", kind: "enum", key: "symbols", body: `["A","B"]`,
			binds: true, structural: true},
		{name: "fixed-size-exact", kind: "fixed", key: "size", body: `4`,
			binds: true, structural: true},

		// #46: a case-variant is an ordinary custom property, on a kind that
		// would bind the exact spelling. Body-independent, so the variant
		// rides to Props and the structural field stays zero. Because the
		// exact spelling is then absent, a *required* key's variant means
		// the attribute is missing and the parse rejects loudly.
		{name: "enum-symbols-variant-required-missing", kind: "enum", key: "Symbols", body: `["A","B"]`,
			binds: false, rejects: true},
		{name: "fixed-size-variant-required-missing", kind: "fixed", key: "Size", body: `4`,
			binds: false, rejects: true},

		// #46 on an optional reserved key: the variant is inert and
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

		// #63(f): the same stray on the same kind in its variant spelling is
		// an ordinary prop whatever its body: placement-conditional routing,
		// never case-conditional.
		{name: "int-symbols-variant-shaped", kind: "int", key: "Symbols", body: `["A"]`,
			binds: false, inProps: true},
		{name: "int-symbols-variant-malformed", kind: "int", key: "Symbols", body: `3.7`,
			binds: false, inProps: true},

		// The two field attributes at the type level. Only an enum binds a
		// schema-level "default" (Java's ENUM_RESERVED is SCHEMA_RESERVED plus
		// that one key, Schema.java:178-180) and no kind binds "order"
		// (:175-180). Where the kind does not bind, there is no structural
		// field for the key to surface on, so Props is its only surface. That
		// is the biconditional's other arm. The enum pair is the
		// discriminating cell, same kind with one key bound and the other
		// not, so a routing that keyed off the kind alone would get one wrong.
		{name: "enum-default-exact", kind: "enum", key: "default", body: `"Z"`,
			binds: true, structural: true},
		{name: "enum-order-stray", kind: "enum", key: "order", body: `"ignore"`,
			binds: false, inProps: true},
		{name: "int-default-stray", kind: "int", key: "default", body: `3`,
			binds: false, inProps: true},
		{name: "int-order-stray", kind: "int", key: "order", body: `"ignore"`,
			binds: false, inProps: true},

		// #46 on the newly routed keys: a case-variant is an ordinary prop
		// even on the kind whose exact spelling would bind it, so the enum's
		// own default stays unbound and the variant rides verbatim.
		{name: "enum-default-variant", kind: "enum", key: "Default", body: `"Z"`,
			binds: false, inProps: true},

		// "doc" is bound on every kind, which is what makes it the one place
		// the third implication can fail. With a string body it behaves like
		// any other consumed key. With a non-string body the read declines
		// and the value goes nowhere. The variant cell is
		// the case control: a case-variant binds nothing, so it is an ordinary
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
	// Named kinds need their own defining key present unless this cell *is*
	// that key. Otherwise the schema is invalid for an unrelated reason.
	// NOT strings.EqualFold-exempt: a case-variant of the defining key must
	// leave the attribute genuinely absent. Supplying the exact spelling
	// alongside it would defeat the cell, turning it into the documented
	// exact-consumed / variant-a-prop case instead.
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
		// field for it to reach, which is exactly why Props must be its
		// surface. The absence is the answer, not a gap in this reader.
		return false
	case "items", "Items":
		return n.Items != nil
	case "values", "Values":
		return n.Values != nil
	case "fields", "Fields":
		return len(n.Fields) > 0
		// "name" and "namespace" are deliberately absent. Every definition has
		// its own, so their fields are populated on any spliced result whether
		// or not the wrapper's copy landed. Presence is not evidence, and
		// reporting it as one would make a correct drop read as a failure.
	}
	return false
}

// TestCensus_Q4_ReservedKeyRoutingIsOneRuleAcrossSurfaces asserts the
// biconditional the rulings share. Consumed keys populate their structural
// field and never appear in Props. Unconsumed keys appear in Props verbatim
// and leave the structural field at zero. We check the two predicates against
// that observable, not against each other.
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
			gotStructural := structuralFieldFor(root, cell.key)
			_, gotProps := root.Props[cell.key]

			if gotStructural != cell.structural {
				t.Errorf("structural field set = %v, want %v (schema %s)", gotStructural, cell.structural, src)
			}
			if gotProps != cell.inProps {
				t.Errorf("key in Props = %v, want %v (schema %s, props %v)", gotProps, cell.inProps, src, root.Props)
			}
			// The biconditional itself. "Never both" is universal. "Never
			// neither" holds for every cell except the documented drop
			// (the non-string doc), which is why the exception is an expectation
			// the cell states rather than a silence.
			if gotStructural && gotProps {
				t.Errorf("key %q surfaced BOTH structurally and in Props — the routing is meant to pick exactly one", cell.key)
			}
			if cell.dropped {
				if gotStructural || gotProps {
					t.Errorf("key %q reached a surface; the documented exception says a bound key whose read declines this body lands nowhere. If the drop is gone, delete `dropped` and state the new routing",
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
				if structuralFieldFor(rb, cell.key) == gotStructural {
					t.Errorf("the rebuild no longer loses %q — the reported finding is fixed; update the registry and delete reportedFinding.\n  %s", cell.key, cell.reportedFinding)
				} else {
					t.Logf("REPORTED FINDING (not fixed in a census round): %s", cell.reportedFinding)
				}
				return
			}
			if structuralFieldFor(rb, cell.key) != gotStructural {
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
		t.Fatalf("the drop exception is meant to be exactly one cell, got %d; a new one needs its own ruling", dropped)
	}
	// A cell that declares no outcome at all is a corpus bug: it would run
	// every assertion against zero expectations and report agreement.
	if unclassified != 0 {
		t.Fatalf("%d corpus cells declare no outcome — each must state rejects, dropped, structural or inProps", unclassified)
	}
}

// ---------------------------------------------------------------------------
// Q17 driver: the splice question has two answerers on two representations.
// The metadata splice (toJSONWalk, gated by nodeIsNameRefShape) works on a
// SchemaNode tree. The cache splice (inlineTreeDefs's wrapper arm) works on
// the raw JSON tree before any SchemaNode exists. Neither can call the other, so
// the only thing keeping them in step is that they answer the same policy: a
// reserved usage-site key cannot survive onto the definition, a custom property
// merges onto it definition-wins. We drive both over the same corpus off the
// observable, so they cannot satisfy it by sharing a bug.
type spliceWrapperCell struct {
	key  string
	body string
	// merges is the expected verdict, taken from the rulings rather than
	// re-derived from either implementation. Otherwise agreement between two
	// answerers that share a bug would read as a pass. A key merges onto the
	// definition exactly when it is an ordinary custom property there. A key
	// the definition's kind would consume is usage-site metadata and drops,
	// because a definition cannot carry a second one for its usage site.
	merges bool
	ruling string
	// def overrides the plain fixed definition. A wrapper key is only ever
	// skipped by the merge because the *definition's* own kind and logical
	// consume it, so a corpus of plain definitions never reaches that arm at
	// all. Disabling the skip guard changed nothing until these cells
	// existed.
	def string
}

func spliceWrapperCells() []spliceWrapperCell {
	const usageSite = "#25: a definition cannot carry a second name/namespace/doc for its usage site"
	return []spliceWrapperCell{
		{"doc", `"usage-site"`, false, usageSite, ""},
		{"aliases", `["Other"]`, false, usageSite, ""},
		{"namespace", `"z"`, false, usageSite, ""},
		// A non-string body, so the cell reaches the splice at all. A
		// shape-OK usage-site name stops the object being a reference and
		// the parse rejects it outright. Binding decides the drop here:
		// every definition a wrapper can reference is named, so "name"
		// never reaches the shape decode however malformed it is.
		{"name", `12`, false, usageSite, ""},
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
		// The one cell where only the reserved-key skip can decide. The def
		// consumes scale but emits none (spec default 0 is not written), so
		// the definition-wins exact-key check sees no collision and would let
		// a usage-site scale through, silently changing the definition's
		// decimal semantics. Every other cell is decided earlier, by the
		// parse routing or by the key already being present.
		{"scale", `7`, false, "#63 splice-merge is definition-wins on CONSUMED-ness, not merely on keys the def happens to emit: an omitted scale is the spec default 0, not an opening", decimalNoScaleDef},
		// The one kind that binds a schema-level "default". Same key, same
		// wrapper spelling, opposite verdict from the plain-def cell above,
		// so the corpus proves the routing reads the *definition's* kind and
		// not the key's name.
		{"default", `"D"`, false, "#63 splice-merge, definition-wins: an enum consumes \"default\" as its evolution default, so a usage site cannot supply a second one", enumCarrierDef},
		// The shape-conditional key class. Every cell above is settled by the
		// key alone or by the definition's logical type, so the routing
		// answers each before it asks the third question: does this body parse
		// as the key's schema shape? These keys are the only ones that reach
		// it. At a splice there is no recorded parse verdict, so we decode the
		// body afresh here and nowhere else. The bodies are deliberately NOT
		// schema-shaped, since a shape-OK body on a reference wrapper stops
		// the object being a bare reference at all.
		{"items", `123`, true, "#63(b): \"items\" is a stray on a fixed and its body does not parse as a schema, so it has no structural surface to take and rides in Props verbatim", ""},
		{"values", `[1]`, true, "#63(b), same clause: a JSON array in schema position is a union, and 1 is not a schema, so the body is not schema-shaped", ""},
		{"fields", `123`, true, "#63(b), same clause: a non-array cannot be a field list", ""},
		{"symbols", `[1]`, true, "#63(b), same clause: the right container with a non-string element is not a symbol list", ""},
		{"symbols", `123`, true, "#63(b), same clause: a non-array is rejected as a symbol list before any element is inspected — the container check and the element check are separate arms", ""},
		{"size", `"x"`, true, "#63(b), same clause, on a definition whose kind does NOT bind size: an enum has no size, so an unreadable one is an ordinary custom property", enumCarrierDef},
	}
}

// enumCarrierDef is a definition whose kind consumes "default" (the enum
// evolution default), so a wrapper carrying that key reaches the merge's
// reserved-key skip.
const enumCarrierDef = `{"type":"enum","name":"x.y.F","symbols":["D","E"]}`

// decimalNoScaleDef consumes scale but emits none, so only the reserved-key
// skip stands between a usage-site scale and the definition's semantics.
const decimalNoScaleDef = `{"type":"fixed","name":"x.y.F","size":4,"logicalType":"decimal","precision":4}`

// decimalCarrierDef is a definition whose kind and logical consume
// precision/scale, so a wrapper carrying them reaches the merge's skip arm.
const decimalCarrierDef = `{"type":"fixed","name":"x.y.F","size":4,"logicalType":"decimal","precision":4,"scale":2}`

// spliceVerdict is what a splice did with the wrapper's key, phrased so both
// representations can be asked the same way.
type spliceVerdict struct {
	spliced bool // did the definition materialize in place of the reference?
	inProps bool // did the key ride onto the result as a custom property?
	// structural records the key landing on the definition's own structural
	// field. Props alone is a blind observable whenever the *definition's*
	// kind binds the key: a merged "default" onto an enum is consumed on
	// re-parse and vanishes from Props, so a Props-only reader cannot tell
	// "the merge skipped it" from "the merge supplied the def's own default".
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

			// Answerer 1, the metadata splice. A second occurrence inside one
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
				return verdictFromSpliced(*out.Root(), c.key, defObj.Type), nil
			}

			// Answerer 2, the cache splice, on the raw JSON tree. The
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
			// And the policy itself, so agreement on a wrong answer still
			// fails. The expectation comes from the ruling the cell cites.
			if mv.inProps != c.merges {
				got, want := "dropped", "merge onto the definition"
				if mv.inProps {
					got, want = "merged as a prop ("+mv.propValue+")", "drop as usage-site metadata"
				}
				t.Errorf("wrapper key %q was %s; the ruling says it must %s — %s", c.key, got, want, c.ruling)
			}
			// A dropped key must reach neither surface. Where the
			// definition's own kind binds the key, a merged copy is consumed
			// on re-parse and disappears from Props, so Props alone cannot
			// see the drop fail. The structural landing is what makes those
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

// verdictFromSpliced reads the observable off a spliced result. defKind is the
// definition's own kind, so "did it splice" means the node carries that kind's
// defining content rather than still being a bare name reference. We compare
// the fullname, not the raw Name. The metadata splice preserves the
// definition's dotted spelling while the cache splice re-emits it as
// name+namespace, and that normalization is not this question's answer.
// Comparing the raw field made every cell disagree. That is the tell that a
// driver is measuring the wrong thing, since genuine divergence is selective.
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

// The corpus must exercise both sides of the policy, or agreement is vacuous.
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
	// The same spelling must appear on both sides, or the corpus proves only
	// that the routing reads key names. The logicalType pair is what makes
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
	// The shape-conditional class is the source's own list, not a sample of
	// it. strayRoutedKeys names every key with a structural field to land on,
	// and a key in that list which no cell drives is a routing arm the two
	// answerers can disagree on unwatched. The guard reds in both directions:
	// a key added to the source with no cell, and a cell whose key the source
	// dropped.
	//
	// name/namespace/aliases carry a permanent exemption. They bind on every
	// named kind, and every definition a wrapper can reference is named, so no
	// cell can carry one of them to the shape decode. We record them here
	// rather than let them fall out of the count, so a fourth cannot join
	// them silently.
	alwaysBoundOnNamedDefs := map[string]bool{"name": true, "namespace": true, "aliases": true}
	driven := map[string]bool{}
	for _, c := range spliceWrapperCells() {
		if canonicalStrayKey(c.key) != "" {
			driven[c.key] = true
		}
	}
	for _, key := range strayRoutedKeys {
		if alwaysBoundOnNamedDefs[key] {
			if !driven[key] {
				t.Errorf("stray-routed key %q is exempt from the shape decode but no cell drives it; the exemption must be exercised, not assumed", key)
			}
			continue
		}
		if !driven[key] {
			t.Errorf("stray-routed key %q has no cell; the splice's shape decode never runs its arm, so the two answerers could route it differently unwatched", key)
		}
	}
	for key := range driven {
		if canonicalStrayKey(key) == "" {
			t.Errorf("cell key %q is no longer stray-routed in the source; the corpus is driving a spelling the routing does not treat as shape-conditional", key)
		}
	}
	var exemptSeen int
	for key := range alwaysBoundOnNamedDefs {
		if canonicalStrayKey(key) != "" {
			exemptSeen++
		}
	}
	if exemptSeen != len(alwaysBoundOnNamedDefs) {
		t.Errorf("%d of the %d exempt keys are still stray-routed in the source; the exemption list has drifted", exemptSeen, len(alwaysBoundOnNamedDefs))
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

// TestCensus_Q22_MagnitudeConsumersAgreeOnTheCeiling is Q22's driver. We do
// not assert that the consumers return the same number, since they compute
// different things. We assert that none of them lets a magnitude leave the
// integer range, which is the whole content of the question. The corpus spans
// the domain from zero to the largest value the grammar admits, with the
// ceiling's own neighbours in it because a clamp is exactly where an
// off-by-one lives.
func TestCensus_Q22_MagnitudeConsumersAgreeOnTheCeiling(t *testing.T) {
	const maxInt = int(^uint(0) >> 1)
	corpus := []int{0, 1, 2, 12, 16, 8192, decimalScaleLimit, decimalScaleLimit + 1,
		maxSchemaMagnitude - 1, maxSchemaMagnitude, maxSchemaMagnitude + 1,
		1 << 40, maxInt - 1, maxInt}

	prevCapacity := -1
	for _, size := range corpus {
		if got := saturateSchemaMagnitude(size); got < 0 || got > maxSchemaMagnitude {
			t.Errorf("saturateSchemaMagnitude(%d) = %d, outside [0, %d]", size, got, maxSchemaMagnitude)
		}
		// The decimal capacity consumer: a digit capacity is a count, so it is
		// never negative, and it never shrinks as the size grows. A wrap shows
		// up as either.
		capacity := maxDecimalDigits(size)
		if capacity < 0 {
			t.Errorf("maxDecimalDigits(%d) = %d; a digit capacity cannot be negative, and a negative one "+
				"falsely rejects every precision", size, capacity)
		}
		if capacity < prevCapacity {
			t.Errorf("maxDecimalDigits(%d) = %d, below the capacity %d reported for a SMALLER size; "+
				"a bigger fixed cannot hold fewer digits", size, capacity, prevCapacity)
		}
		prevCapacity = capacity
		// Every precision the parser can still be holding when it asks must
		// fit, or a valid schema is refused.
		if size > 0 && capacity < 1 {
			t.Errorf("maxDecimalDigits(%d) = %d; a fixed of any positive size holds at least one digit", size, capacity)
		}
	}

	// Non-vacuity: the corpus must actually cross the ceiling, or it is
	// measuring the identity function.
	crossed := false
	for _, size := range corpus {
		if size > maxSchemaMagnitude {
			crossed = true
		}
	}
	if !crossed {
		t.Fatal("the corpus never exceeds the ceiling, so it cannot observe a clamp")
	}
}

// ---------- dos_battery_test.go ----------

// DoS entry-point battery.
//
// The single executable matrix of every public entry point x every
// hostile-input class. It exists to end the one-DoS-fix-per-round dribble. A
// resource-bound finding is correct output at unbounded cost on hostile input,
// and we close those wholesale here.
//
// Rows (entry points): Parse / MustParse / SchemaCache.Parse / SchemaFor /
// Decode / DecodeJSON / DecodeSingleObject (safe + unsafe targets) / Encode /
// EncodeJSON / AppendSingleObject / Root / Canonical / String / Fingerprint /
// SchemaNode.Schema / Resolve / CheckCompatibility / RatFromBytes /
// DurationFromBytes / SingleObjectFingerprint.
//
// Columns (hostile-input classes):
//   C1 deep nesting: schema JSON brackets, wire value, Go encode value, JSON
//      value. Stack overflow / O(depth^2).
//   C2 large count / length: array/map block count, bytes/string/fixed length
//      prefix. Pre-bound allocation, zero-byte-item loops, count wraparound.
//   C3 number CPU amplification: decimal/json.Number/float strings driving
//      big.Rat/big.Int/big.Float. O(n^2) / 10^scale.
//   C4 decompression amplification: OCF codecs (ocf/dos_battery_test.go).
//   C5 error-message echo: hostile input echoed verbatim into an error, a 1:1
//      log/RPC/metric-label amplification.
//   C6 metadata DAG / value: SchemaNode->JSON walk, shared-reference fan-out,
//      deep per-node Props/Default value.
//   C7 cyclic Go type: decode target / SchemaFor field type whose reflect
//      graph is cyclic, so the recursion is unbounded.
//   C9 registration-scaled parse cost: a registered CustomType must not change
//      what Parse can accept. The custom-match subtree walks share one
//      per-parse memo, else a backward-reference chain or a many-refs cache
//      parse is quadratic in a magnitude the text is linear in.
//
// Each cell drives the real public API with a hostile input and asserts the
// bound holds: it returns, never hanging, panicking or crashing the process,
// with the verdict the input deserves. No cell asserts a wall-clock cost. The
// caps this package names are what make an unbounded input error out, and a
// cell that merely got slower while staying under a cap is not what these are
// for. Where a dedicated regression already pins the extreme case, the cell
// cites it.
//
// Nothing here is ever "closed". A later DoS find extends this battery with
// the missed cell and its bound. Add the row or column, never delete one.

// dosBudget is the per-cell ceiling separating a working bound (rejects in
// single-digit milliseconds) from a missing one (seconds-to-forever). It is
// deliberately generous so a loaded host never false-fails a real bound, while
// still catching any unbounded path. The gap between the two is orders of
// magnitude, so the exact value is not the point.
const dosBudget = 4 * time.Second

// dosRun executes fn on hostile input under a watchdog. It fails the test if fn
// hangs past dosBudget (a missing bound on a non-allocating loop) or panics (a
// hostile input must surface as an error, never a panic). It returns fn's error
// and whether fn completed. A genuinely unbounded allocating path will OOM-kill
// the process rather than hang, which is still a loud, correct failure signal.
func dosRun(t *testing.T, name string, fn func() error) (error, bool) {
	t.Helper()
	type result struct {
		err error
		pan any
	}
	ch := make(chan result, 1)
	// Under -race we widen the deadline, because instrumentation multiplies a
	// healthy bounded walk's wall-clock several-fold. The widest metadata cell
	// runs ~0.5s unraced and ~4-5s raced with zero data races and normal
	// completion, so the tight bound false-trips it. The separation these cells
	// rely on survives: an actually-unbounded walk here is tens of seconds
	// unraced (the fresh-walk-per-container neuter is ~30s) and hundreds raced,
	// still far past the raced ceiling. Never relax the *unraced* bound for a
	// -race timeout, which would hide a real hang. This only widens what -race
	// itself inflated.
	budget := raceInflated(dosBudget)
	go func() {
		var r result
		defer func() {
			if p := recover(); p != nil {
				r.pan = p
			}
			ch <- r
		}()
		r.err = fn()
	}()
	select {
	case r := <-ch:
		if r.pan != nil {
			t.Errorf("%s: panicked on hostile input (must return an error, not panic): %v", name, r.pan)
			return nil, false
		}
		return r.err, true
	case <-time.After(budget):
		t.Errorf("%s: did not return within %v — bound missing (hang/unbounded loop on hostile input)", name, budget)
		return nil, false
	}
}

// wantReject asserts fn rejects hostile input fast (non-nil error, no hang/panic).
func wantReject(t *testing.T, name string, fn func() error) {
	t.Helper()
	if err, ok := dosRun(t, name, fn); ok && err == nil {
		t.Errorf("%s: hostile input was accepted (want a fast rejection)", name)
	}
}

// wantRejectIs asserts fn rejects fast with an error matching target.
func wantRejectIs(t *testing.T, name string, target error, fn func() error) {
	t.Helper()
	if err, ok := dosRun(t, name, fn); ok {
		if err == nil {
			t.Errorf("%s: hostile input was accepted (want %v)", name, target)
		} else if !errors.Is(err, target) {
			t.Errorf("%s: got %v, want errors.Is(_, %v)", name, err, target)
		}
	}
}

// wantTerminate asserts fn returns fast without hang/panic. Accept-or-reject
// is not the DoS question here, only that the cost is bounded.
func wantTerminate(t *testing.T, name string, fn func() error) {
	t.Helper()
	dosRun(t, name, fn)
}

// dosMaxErrLen bounds an error string built from a 1 MiB hostile input. The
// content-truncating helpers cap user fragments at 40/80 chars, so even a
// message stitching several of them plus structural framing stays far below
// this, and it is orders of magnitude under a 1:1 (1 MiB) amplification.
const dosMaxErrLen = 4096

// wantBoundedErr asserts fn errors and the error string is bounded (not a 1:1
// echo of the megabyte input).
func wantBoundedErr(t *testing.T, name string, fn func() error) {
	t.Helper()
	if err, ok := dosRun(t, name, fn); ok {
		if err == nil {
			t.Errorf("%s: want a (bounded) error, got nil", name)
		} else if n := len(err.Error()); n > dosMaxErrLen {
			t.Errorf("%s: error message is %d bytes (> %d) — hostile input echoed unbounded", name, n, dosMaxErrLen)
		}
	}
}

// ---- shared hostile wire builders ----------------------------------------

// dosVarlong zigzag-varlong-encodes i exactly as the encoder writes a count /
// length prefix (the package's own appendVarlong, so the battery's wire matches
// real producer wire).
func dosVarlong(i int64) []byte { return appendVarlong(nil, i) }

// avroBytesField length-prefixes b as an Avro `bytes`/`string` field.
func avroBytesField(b []byte) []byte { return append(dosVarlong(int64(len(b))), b...) }

// hugeBlockCount is a block count large enough that no buffer could legitimately
// back it, encoded as the Avro varlong an array/map block carries.
func hugeBlockCount() []byte { return dosVarlong(1 << 40) }

// recursiveNodeSchema is `record Node { value:int, next:["null",Node] }`, the
// canonical self-recursive shape for the deep-wire / cyclic-encode cells.
const recursiveNodeSchema = nodeRecursiveSchema

// deepRecursiveWire is the binary encoding of `depth` nested Node records,
// terminated by a null. Decoding it must trip errTooDeep, not recurse the
// goroutine stack to death.
func deepRecursiveWire(depth int) []byte {
	var src []byte
	for range depth {
		src = append(src, 0)    // value = zigzag(0)
		src = append(src, 0x02) // union idx 1 = "Node"
	}
	return append(src, 0) // innermost union idx 0 = null
}

//////////////////////////////////////////////////////////////////////////////
// C1: deep nesting (stack overflow / O(depth^2))
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_C1_DeepNesting(t *testing.T) {
	// Schema-JSON bracket depth past the parse pre-scan cap. Bound:
	// checkSchemaNestingDepth / maxSchemaJSONDepth (schema.go), an O(input)
	// linear pre-scan run before any build. Extreme case + linear-time proof:
	// TestRegression_DeepSchemaNestingRejectedInBoundedTime,
	// TestRegression_DeepSchemaParseRunsInBoundedTime, _DeepValidSchemaParsesLinear.
	deepArraySchema := strings.Repeat(`{"type":"array","items":`, 6000) + `"int"` + strings.Repeat("}", 6000)
	wantReject(t, "Parse/schema-bracket-depth", func() error {
		_, err := Parse(deepArraySchema)
		return err
	})
	wantReject(t, "SchemaCache.Parse/schema-bracket-depth", func() error {
		var c SchemaCache
		_, err := c.Parse(deepArraySchema)
		return err
	})
	// A deeply-nested default value inflates the same bracket count, so the
	// pre-scan covers the value channel at Parse time too.
	deepDefaultSchema := `{"type":"record","name":"R","fields":[{"name":"f","type":` +
		`{"type":"array","items":"int"},"default":` +
		strings.Repeat("[", 6000) + strings.Repeat("]", 6000) + `}]}`
	wantReject(t, "Parse/deep-default-value", func() error {
		_, err := Parse(deepDefaultSchema)
		return err
	})

	s := MustParse(recursiveNodeSchema)
	wire := deepRecursiveWire(20000)

	// Binary decode of a deeply-nested value. Bound: errTooDeep via the
	// decoder's sl.depth (deser.go). Extreme: TestDecodeDeepInputDoesntPanic.
	wantRejectIs(t, "Decode/recursive-wire", errTooDeep, func() error {
		var n any
		_, err := s.Decode(wire, &n)
		return err
	})
	// Resolved-decode path carries its own depth bump (resolve.go:400).
	resolved := mustResolve(t, s, s)
	wantRejectIs(t, "Decode/resolved/recursive-wire", errTooDeep, func() error {
		var n any
		_, err := resolved.Decode(wire, &n)
		return err
	})
	// Skip path: a reader that drops `next` must still bound the skip of the
	// writer's deep subtree (skipRecord/skipUnion via the same sl.depth).
	reader := MustParse(`{"type":"record","name":"Node","fields":[{"name":"value","type":"int"}]}`)
	skipResolved := mustResolve(t, s, reader)
	wantRejectIs(t, "Decode/skip/recursive-wire", errTooDeep, func() error {
		var n struct {
			Value int32 `avro:"value"`
		}
		_, err := skipResolved.Decode(wire, &n)
		return err
	})

	// JSON decode of a deeply-nested matching value. Bound: decodeValue's
	// sl.depth check + the scanner's skipValueDepth (json_scan.go), both at
	// maxDepth. Extreme: TestDecodeDeepInputDoesntPanic/json_union_trial.
	var jsonDeep []byte
	for range 20000 {
		jsonDeep = append(jsonDeep, []byte(`{"value":0,"next":{"Node":`)...)
	}
	jsonDeep = append(jsonDeep, []byte(`{"value":0,"next":null}`)...)
	for range 20000 {
		jsonDeep = append(jsonDeep, []byte(`}}`)...)
	}
	wantRejectIs(t, "DecodeJSON/recursive-json", errTooDeep, func() error {
		var out any
		return s.DecodeJSON(jsonDeep, &out)
	})
	// Scanner skip of a deeply-nested unknown field value (json_scan.go's
	// skipValueDepth, a separate recursion from decodeValue).
	jsonUnknownDeep := []byte(`{"value":0,"next":null,"x":` + strings.Repeat("[", 20000) + strings.Repeat("]", 20000) + `}`)
	wantTerminate(t, "DecodeJSON/skip-unknown-deep", func() error {
		var out any
		return s.DecodeJSON(jsonUnknownDeep, &out)
	})

	// Cyclic Go value at encode. Bound: errTooDeep via the encoder's depth
	// parameter (ser.go). Extreme: TestEncodeCyclicInput.
	cyc := map[string]any{"value": int32(1)}
	cyc["next"] = cyc
	wantRejectIs(t, "Encode/cyclic-value", errTooDeep, func() error {
		_, err := s.AppendEncode(nil, cyc)
		return err
	})
	wantRejectIs(t, "EncodeJSON/cyclic-value", errTooDeep, func() error {
		_, err := s.AppendEncodeJSON(nil, cyc)
		return err
	})
	// Struct fast-path encode bypasses serRecord.ser; the bound lives on
	// serRecordFastPtr itself (unsafe.go). Extreme: TestEncodeCyclicInput/struct.
	type cyclicStructNode struct {
		Value int32             `avro:"value"`
		Next  *cyclicStructNode `avro:"next"`
	}
	cn := &cyclicStructNode{Value: 1}
	cn.Next = cn
	wantRejectIs(t, "Encode/cyclic-struct-fastpath", errTooDeep, func() error {
		_, err := s.AppendEncode(nil, cn)
		return err
	})

	// Single Object Encoding wraps the same body codec, so both directions
	// inherit the depth bound.
	wantRejectIs(t, "AppendSingleObject/cyclic-value", errTooDeep, func() error {
		_, err := s.AppendSingleObject(nil, cyc)
		return err
	})
	soeHdr := mustAppendSingleObject(t, s, nil, map[string]any{"value": int32(0), "next": nil})
	soeDeep := append(soeHdr[:10:10], wire...) // 2-byte magic + 8-byte fingerprint, then deep body
	wantRejectIs(t, "DecodeSingleObject/recursive-wire", errTooDeep, func() error {
		var n any
		_, err := s.DecodeSingleObject(soeDeep, &n)
		return err
	})
	// The unsafe (struct fast-path) DecodeSingleObject target shares the body
	// codec, so it inherits the same depth bound. The header claims "safe +
	// unsafe targets", and this drives the unsafe arm too.
	wantRejectIs(t, "DecodeSingleObject/recursive-wire(unsafe)", errTooDeep, func() error {
		var n cyclicStructNode
		_, err := s.DecodeSingleObject(soeDeep, &n)
		return err
	})

	// A reserved structural key on a kind that does not bind it is inert
	// metadata the parser accepts, so it never reaches the bracket-depth reject
	// above. We still decode it to decide whether it surfaces as-written or
	// rides in Props. That decode must happen once per level. A routing pass
	// that re-decodes it re-enters the recursive schema decode, and two decodes
	// per level compound to O(2^depth), a sub-KB stray schema that hangs Parse.
	// The bracket-depth arms use the binding form, which never routes a stray,
	// so they cannot see this. Depth 1000 stays under the bracket pre-scan for
	// all three keys, so all three entry points must accept it. A doubling
	// regression makes the input unparseable in practice rather than merely
	// slow.
	const strayDepth = 1000
	for _, key := range []string{"items", "values", "fields"} {
		open, closeStr := `{"type":"int","`+key+`":`, `}`
		if key == "fields" {
			open, closeStr = `{"type":"int","fields":[{"name":"f","type":`, `}]}`
		}
		straySchema := strings.Repeat(open, strayDepth) + `"int"` + strings.Repeat(closeStr, strayDepth)
		wantAccept(t, "Parse/nested-stray-"+key, func() error {
			_, err := Parse(straySchema)
			return err
		})
		wantAccept(t, "SchemaCache.Parse/nested-stray-"+key, func() error {
			var c SchemaCache
			_, err := c.Parse(straySchema)
			return err
		})
		wantAccept(t, "Root().Schema()/nested-stray-"+key, func() error {
			ss, err := Parse(straySchema)
			if err != nil {
				return err
			}
			root := ss.Root()
			_, err = root.Schema()
			return err
		})
	}
}

//////////////////////////////////////////////////////////////////////////////
// C2: large count / length prefix (pre-bound allocation, zero-byte loops)
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_C2_LargeCountLength(t *testing.T) {
	// array<null>: zero-byte elements, so the buffer-relative bound is vacuous
	// and the absolute cap maxZeroByteItems applies (pre-add form, overflow
	// safe). Extreme: TestRegression_DecodeArrayOfNullLargeCount/_Capped,
	// TestRegression_ArrayZeroByteProducerCompliance.
	arrNull := MustParse(`{"type":"array","items":"null"}`)
	arrNullWire := append(hugeBlockCount(), 0x00)
	wantReject(t, "Decode/array<null>-huge-count(any)", func() error {
		var got []any
		_, err := arrNull.Decode(arrNullWire, &got)
		return err
	})
	wantReject(t, "Decode/array<null>-huge-count(typed)", func() error {
		var got []struct{}
		_, err := arrNull.Decode(arrNullWire, &got)
		return err
	})

	// array<int>: minItemBytes=1, so checkArrayBlockBounds rejects any count
	// past the remaining buffer (overflow-safe division form). Extreme:
	// TestRegression_DeserArraySliceBlockCountOverflow and siblings.
	arrInt := MustParse(`{"type":"array","items":"int"}`)
	arrIntWire := append(hugeBlockCount(), 0x02, 0x00)
	for _, tgt := range []struct {
		name string
		dst  func() any
	}{
		{"any", func() any { var v []any; return &v }},
		{"typed", func() any { var v []int32; return &v }},
	} {
		wantReject(t, "Decode/array<int>-huge-count("+tgt.name+")", func() error {
			_, err := arrInt.Decode(arrIntWire, tgt.dst())
			return err
		})
	}
	// Skip path (reader drops a writer array field) routes through the same
	// checkArrayBlockBounds.
	arrRec := MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":"int"}},{"name":"keep","type":"int"}]}`)
	arrRecReader := MustParse(`{"type":"record","name":"R","fields":[{"name":"keep","type":"int"}]}`)
	skipArr := mustResolve(t, arrRec, arrRecReader)
	wantReject(t, "Decode/skip-array-huge-count", func() error {
		var v struct {
			Keep int32 `avro:"keep"`
		}
		_, err := skipArr.Decode(append(hugeBlockCount(), 0x02, 0x00), &v)
		return err
	})

	// map<int>: minEntryBytes>=1 (a key is at least 1 byte), checkMapBlockBounds
	// is always buffer-relative. Extreme: TestRegression_MapDecodeBucketAmplificationDoS.
	mapInt := MustParse(`{"type":"map","values":"int"}`)
	mapWire := append(hugeBlockCount(), 0x02, 0x00)
	for _, tgt := range []struct {
		name string
		dst  func() any
	}{
		{"any", func() any { var v map[string]any; return &v }},
		{"typed", func() any { var v map[string]int32; return &v }},
	} {
		wantReject(t, "Decode/map<int>-huge-count("+tgt.name+")", func() error {
			_, err := mapInt.Decode(mapWire, tgt.dst())
			return err
		})
	}

	// bytes / string length prefix: readLength rejects length > remaining
	// buffer BEFORE make([]byte, length), so the alloc can never exceed the
	// bytes actually supplied (1:1, never amplified).
	wantReject(t, "Decode/bytes-huge-length", func() error {
		var v []byte
		_, err := MustParse(`"bytes"`).Decode(dosVarlong(1<<40), &v)
		return err
	})
	wantReject(t, "Decode/string-huge-length", func() error {
		var v string
		_, err := MustParse(`"string"`).Decode(dosVarlong(1<<40), &v)
		return err
	})

	// fixed: size is a schema integer with no upper bound at parse, and only
	// negatives reject, as avro-rs does (its size parse is as_u64, rejecting
	// negatives with no maximum; fastavro 1.12.2 is laxer still and parses
	// even a negative size, observed). deserFixed.deser calls needLen before
	// make([]byte, size), so a 2e9-size fixed against an empty wire rejects
	// without allocating. Sibling parse-time alloc bounds:
	// TestRegression_DecimalFixedSizeCapacityNoOverflow, _FixedLogicalProbeSizeBounded.
	fixedHuge := MustParse(`{"type":"fixed","name":"F","size":2000000000}`)
	wantReject(t, "Decode/fixed-huge-size-short-wire", func() error {
		var v []byte
		_, err := fixedHuge.Decode(nil, &v)
		return err
	})
}

//////////////////////////////////////////////////////////////////////////////
// C3: number CPU amplification (big.Rat/big.Int/big.Float on compact strings)
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_C3_NumberCPU(t *testing.T) {
	hostile1MiB := strings.Repeat("9", 1<<20)

	// Decimal unscaled value on the wire: bounded by maxDecimalUnscaledBytes
	// before the big.Int materialization / base conversion. Extreme:
	// TestMatrix_DecimalUnscaledLengthDoS, TestCoverage_RatFromBytesHostileScale.
	bytesDec := MustParse(`{"type":"bytes","logicalType":"decimal","precision":65536,"scale":0}`)
	hostileUnscaled := avroBytesField(bytes.Repeat([]byte{0x55}, 1<<20)) // ~1 MiB unscaled
	for _, tgt := range []struct {
		name string
		dst  func() any
	}{
		{"any", func() any { var v any; return &v }},
		{"bigRat", func() any { var v big.Rat; return &v }},
	} {
		wantReject(t, "Decode/bytes-decimal-huge-unscaled("+tgt.name+")", func() error {
			_, err := bytesDec.Decode(hostileUnscaled, tgt.dst())
			return err
		})
	}

	// JSON-decode of a megabyte float number: bounded by maxParseFloatLen /
	// boundedRatFromString. Extreme: TestRegression_DecodeJSONFloatLengthCapDoS,
	// TestRegression_DecimalJSONExpDoS.
	wantTerminate(t, "DecodeJSON/double-megabyte-digits", func() error {
		var v float64
		return MustParse(`"double"`).DecodeJSON([]byte(hostile1MiB), &v)
	})

	// Encode of a megabyte json.Number against numeric/decimal schemas:
	// bounded by boundedRatFromString (maxRatInputLen) / parseInt64Lenient's
	// length cap. Extreme: TestSerJSONNumberOverflowInCollections,
	// TestRegression_FiniteScaleCPUBound, TestRegression_JsonNumberToFloatErrorBounded.
	jn := json.Number(hostile1MiB)
	wantReject(t, "Encode/json.Number-megabyte->decimal", func() error {
		_, err := bytesDec.AppendEncode(nil, jn)
		return err
	})
	wantReject(t, "Encode/json.Number-megabyte->long", func() error {
		_, err := MustParse(`"long"`).AppendEncode(nil, jn)
		return err
	})
	wantTerminate(t, "Encode/json.Number-megabyte->double", func() error {
		_, err := MustParse(`"double"`).AppendEncode(nil, jn)
		return err
	})

	// Parse-time field-default number: a megabyte integer/float default is the
	// third validation axis (defaultAsInt64/defaultAsFloat via the same length
	// caps). Extreme: TestRegression_IntDefaultLengthCapBounded,
	// TestRegression_ParseFloatLengthCapDoS.
	wantTerminate(t, "Parse/long-default-megabyte-int", func() error {
		_, err := Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"long","default":` + hostile1MiB + `}]}`)
		return err
	})
	wantTerminate(t, "Parse/double-default-megabyte-float", func() error {
		_, err := Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"double","default":` + hostile1MiB + `}]}`)
		return err
	})

	// Parse-time decimal scale/precision: 10^scale is materialized at decode,
	// so the schema integer is capped at decimalScaleLimit at parse. Extreme:
	// TestMatrix_DecimalScaleAllocBound, TestMatrix_DecimalExponentOverflowRejectsAcrossArms.
	wantReject(t, "Parse/decimal-scale-over-limit", func() error {
		_, err := Parse(`{"type":"bytes","logicalType":"decimal","precision":2000000000,"scale":1000000000}`)
		return err
	})

	// Metadata-API number observability, the fourth axis: a megabyte Props number
	// survives Parse fast (caps reject the conversion, json.Number is preserved)
	// and Root()/String()/Canonical() serialize it under the maxSchemaJSONBytes
	// budget. Extreme: TestRegression_ParseFloatLengthCapDoS (Props case),
	// TestMatrix_SchemaMetadataExponentOverflowNormalizesToInf.
	wantTerminate(t, "Parse+Root+String/metadata-megabyte-number", func() error {
		s, err := Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"int"}],"x":` + hostile1MiB + `}`)
		if err != nil {
			return err
		}
		_ = s.Root()
		_ = s.String()
		_ = s.Canonical()
		return nil
	})
}

//////////////////////////////////////////////////////////////////////////////
// C5: error-message echo (string-size amplification, not CPU)
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_C5_ErrorEcho(t *testing.T) {
	huge := strings.Repeat("a", 1<<20)

	// Schema-parse error echoing a megabyte type token: bounded by
	// boundJSONErrorEcho + boundErrorLen (maxParseErrorLen). Extreme:
	// TestMatrix_SchemaParseErrorBoundedForHostileInput.
	wantBoundedErr(t, "Parse/unknown-type-megabyte-name", func() error {
		_, err := Parse(`{"type":"` + huge + `"}`)
		return err
	})

	// Decode error echoing megabyte wire content: a json.Number target fed a
	// megabyte string-typed wire value is rejected with truncForError(content).
	// Extreme: TestMatrix_ErrorMessageBoundedForHostileInput.
	wantBoundedErr(t, "Decode/json.Number<-megabyte-string-error", func() error {
		var v json.Number
		_, err := MustParse(`"string"`).Decode(avroBytesField([]byte(huge)), &v)
		return err
	})

	// Encode error echoing a megabyte json.Number against a decimal schema:
	// truncForError / truncRatForError keep the message bounded even though the
	// source value is a megabyte. Extreme: TestRegression_BigDecimalRatErrorMessageBounded.
	bytesDec := MustParse(`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`)
	wantBoundedErr(t, "Encode/decimal<-megabyte-json.Number-error", func() error {
		_, err := bytesDec.AppendEncode(nil, json.Number(strings.Repeat("9", 1<<20)))
		return err
	})

	// CheckCompatibility error echoing a megabyte field name: the dotted path
	// is render-truncated (truncForError on CompatibilityError.Path). The
	// per-datum SemanticError.Error() render path is the same shape, pinned by
	// TestRegression_SemanticErrorFieldRenderBounded; CompatibilityError by
	// TestRegression_CompatibilityErrorRenderingBounded.
	writer := MustParse(`{"type":"record","name":"R","fields":[{"name":"` + huge + `","type":"int"}]}`)
	reader := MustParse(`{"type":"record","name":"R","fields":[{"name":"` + huge + `","type":"string"}]}`)
	wantBoundedErr(t, "CheckCompatibility/megabyte-field-name", func() error {
		return CheckCompatibility(writer, reader)
	})
}

//////////////////////////////////////////////////////////////////////////////
// C6: metadata DAG / deep value (SchemaNode->JSON walk)
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_C6_MetadataWalk(t *testing.T) {
	// A shared-reference DAG needs no hand-built SchemaNode and no deep JSON. A
	// named type referenced twice binds both references to one node, so
	// ordinary schema text expresses the fan-out directly. We can write it
	// flat, every level a sibling field wired by forward reference, which puts
	// it past any bracket pre-scan or nesting bound. So this axis belongs to
	// every walk a schema drives, and the cells below cross it with the
	// entry-point list.
	//
	// The hand-built constructions still matter for the axes a caller cannot
	// reach through Parse at all (deep values inside Props, duplicate named
	// definitions) and stay pinned by the dedicated battery in
	// schema_node_test.go. Bounds: maxSchemaJSONNodes + maxSchemaJSONBytes (one
	// shared walkBudget), valueWalkLimit, toJSONShared. This cell exercises
	// the public round trip.
	s := MustParse(`{"type":"record","name":"R","doc":"d","x":{"a":[1,2,3],"b":"y"},"fields":[
		{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B","C"]},"default":"A"},
		{"name":"f","type":"int","default":7}
	]}`)
	wantTerminate(t, "Root+Schema/round-trip", func() error {
		root := s.Root() // addressable: Schema() has a pointer receiver
		_, _ = root.Schema()
		_ = s.String()
		_ = s.Canonical()
		return nil
	})

	// The shared-reference DAG axis, driven at two depths so the cells can tell
	// a bound from a cost merely linear in it. Without a memo this is 2^depth,
	// so the pair is an 8192x separation, and a flat result is the bound
	// working.
	const cell = "TestDoSBattery_C6_MetadataWalk"
	for _, form := range []struct {
		name  string
		build func(depth int) string
	}{
		{"nested", func(d int) string { return `{"type":"array","items":` + dagNested(d, 2) + `}` }},
		{"flat-forward-ref", func(d int) string { return dagFlat(d, 2) }},
	} {
		wantEveryMagnitudeTerminates(t, cell, "Parse/shared-node-"+form.name, func(d int) func() error {
			s := form.build(d)
			return func() error { _, err := Parse(s); return err }
		})
		wantEveryMagnitudeTerminates(t, cell, "SchemaCache.Parse/shared-node-"+form.name, func(d int) func() error {
			s := form.build(d)
			return func() error {
				var c SchemaCache
				_, err := c.Parse(s)
				return err
			}
		})
		wantEveryMagnitudeTerminates(t, cell, "Root+Schema+String+Canonical/shared-node-"+form.name, func(d int) func() error {
			ds := MustParse(form.build(d))
			return func() error {
				root := ds.Root()
				_, _ = root.Schema()
				_ = ds.String()
				_ = ds.Canonical()
				return nil
			}
		})
		wantEveryMagnitudeTerminates(t, cell, "Resolve/shared-node-"+form.name, func(d int) func() error {
			ds := MustParse(form.build(d))
			return func() error { _, err := Resolve(ds, ds); return err }
		})
		// The writer field is dropped, which compiles a skip: a separate
		// derivation of the same per-element bound.
		wantEveryMagnitudeTerminates(t, cell, "Resolve/shared-node-dropped-field-"+form.name, func(d int) func() error {
			schema := form.build(d)
			w := MustParse(`{"type":"record","name":"T","fields":[{"name":"x","type":` + schema + `},{"name":"y","type":"int"}]}`)
			r := MustParse(`{"type":"record","name":"T","fields":[{"name":"y","type":"int"}]}`)
			return func() error { _, err := Resolve(w, r); return err }
		})
		wantEveryMagnitudeTerminates(t, cell, "CheckCompatibility/shared-node-"+form.name, func(d int) func() error {
			ds := MustParse(form.build(d))
			return func() error { return CheckCompatibility(ds, ds) }
		})
		// A value round-trip, so we cross the axis on the wire paths too, at a
		// small depth. A shared node is shared in the schema and not in the
		// datum: a record whose every level has two fields of the next
		// genuinely contains 2^depth leaves, so the wire cost is the value's
		// own size and there is nothing here to bound. We ask only that the
		// encoder and decoder handle a node reached by several paths at all,
		// so it stays a fixed small depth and is not a cost cell.
		small := MustParse(`{"type":"array","items":` + dagNested(8, 2) + `}`)
		wantTerminate(t, "Encode+Decode/shared-node-"+form.name, func() error {
			var v any
			b, err := small.Encode([]any{dagZeroValue(small.node.items)})
			if err != nil {
				return err
			}
			_, err = small.Decode(b, &v)
			return err
		})
	}
}

// dagZeroValue builds one legal value for a shared-node schema, so the DAG
// axis can be crossed with the wire paths and not only the parse paths.
//
// It memoizes per node for the same reason the walk under test does. A node
// reachable by many paths must be built once, or the harness is the
// exponential thing and the cell measures it instead of the package.
func dagZeroValue(n *schemaNode) any {
	return dagZeroValueMemo(n, map[*schemaNode]any{})
}

func dagZeroValueMemo(n *schemaNode, seen map[*schemaNode]any) any {
	if v, ok := seen[n]; ok {
		return v
	}
	v := dagZeroValueOf(n, seen)
	seen[n] = v
	return v
}

func dagZeroValueOf(n *schemaNode, seen map[*schemaNode]any) any {
	switch n.kind {
	case "null":
		return nil
	case "boolean":
		return false
	case "int", "long":
		return 0
	case "float", "double":
		return 0.0
	case "string", "bytes":
		return ""
	case "array":
		return []any{}
	case "map":
		return map[string]any{}
	case "union":
		return dagZeroValueMemo(n.branches[0], seen)
	case "record", "error":
		m := make(map[string]any, len(n.fields))
		for i := range n.fields {
			m[n.fields[i].name] = dagZeroValueMemo(n.fields[i].node, seen)
		}
		return m
	}
	return nil
}

//////////////////////////////////////////////////////////////////////////////
// C7: cyclic Go type (decode target / SchemaFor field type)
//////////////////////////////////////////////////////////////////////////////

func TestDoSBattery_C7_CyclicGoType(t *testing.T) {
	// The custom-decode and encode-field cyclic-pointer paths are pinned
	// (TestRegression_CustomDecodeBoundsRecursivePointerTarget,
	// _EncodeStructCyclicPointerFieldTerminates,
	// _StructFieldPointerChainMatchesReflect), but the non-custom binary Decode
	// into a cyclic-pointer target had no direct pin, only the shared
	// indirectAlloc bound (reflect.go, maxIndirectDepth) inferred from the
	// custom path's comment. A caller can write `type P *P; s.Decode(wire, &p)`:
	// indirectAlloc peels at most maxIndirectDepth levels, never reaches a
	// concrete kind, and the setter returns a SemanticError.
	long := MustParse(`"long"`)
	wire := dosVarlong(42)

	// type P *P as a decode target: infinitely indirect, must reject fast.
	wantTerminate(t, "Decode/cyclic-pointer-target", func() error {
		type P *P
		var p P
		_, err := long.Decode(wire, &p)
		if err == nil {
			return errors.New("cyclic pointer target unexpectedly accepted")
		}
		return nil
	})

	// A deep-but-finite pointer chain past maxIndirectDepth must also terminate
	// (the bound is depth, not cyclicity) rather than walking/allocating the
	// whole chain.
	wantTerminate(t, "Decode/deep-pointer-chain-target", func() error {
		var p7 *******int64 // 7 levels > maxIndirectDepth
		_, err := long.Decode(wire, &p7)
		_ = err // accept or reject; only the bounded cost is asserted
		return nil
	})

	// SchemaFor over a cyclic non-struct Go field type: inferType's depth bound
	// + inferRecord's seen[] break the type-graph cycle. Extreme:
	// TestRegression_SchemaForRecursiveNonStructTypeErrors,
	// _SchemaForRecursivePtrDefaultTerminates.
	wantTerminate(t, "SchemaFor/cyclic-pointer-field-type", func() error {
		type P *P
		_, err := SchemaFor[struct {
			F P `avro:"f"`
		}]()
		_ = err
		return nil
	})
}

//////////////////////////////////////////////////////////////////////////////
// C8: the direct byte-slice / hash public entry points the row list omitted.
//////////////////////////////////////////////////////////////////////////////

// TestDoSBattery_C8_DirectByteAPIs covers the public entry points that take a
// caller-supplied byte slice (or hostile schema) directly, bypassing Decode's
// length-prefix bounds: RatFromBytes, DurationFromBytes, SingleObjectFingerprint,
// and Fingerprint. Each must bound its cost on a megabyte / over-limit input
// and never panic on a short one. These were missing from the battery's row
// list even though two of them (the number-CPU and the metadata-hash surfaces)
// are exactly the amplification shapes C3/C6 guard elsewhere.
func TestDoSBattery_C8_DirectByteAPIs(t *testing.T) {
	hostile1MiB := bytes.Repeat([]byte{0x55}, 1<<20)

	// RatFromBytes (C3 number-CPU, direct surface): a megabyte unscaled value or
	// an attacker scale would drive an unbounded big.Int base conversion / 10^scale
	// without the public-API guards (maxDecimalUnscaledBytes / decimalScaleLimit),
	// which return a zero *big.Rat instead. Extreme: TestCoverage_RatFromBytesHostileScale.
	wantTerminate(t, "RatFromBytes/megabyte-unscaled", func() error {
		got := RatFromBytes(hostile1MiB, 2)
		if got.Sign() != 0 {
			return errors.New("over-length unscaled not bounded to zero rat")
		}
		return nil
	})
	wantTerminate(t, "RatFromBytes/hostile-scale", func() error {
		got := RatFromBytes([]byte{0x01}, decimalScaleLimit+1)
		if got.Sign() != 0 {
			return errors.New("over-limit scale not bounded to zero rat")
		}
		return nil
	})
	wantTerminate(t, "RatFromBytes/hostile-negative-scale", func() error {
		_ = RatFromBytes([]byte{0x01}, -(decimalScaleLimit + 1))
		return nil
	})

	// DurationFromBytes (C2 length): reads exactly 12 bytes, so a megabyte input
	// is read 12-bounded and a short input returns the zero Duration, never panics.
	wantTerminate(t, "DurationFromBytes/megabyte", func() error {
		_ = DurationFromBytes(hostile1MiB)
		return nil
	})
	wantTerminate(t, "DurationFromBytes/short", func() error {
		_ = DurationFromBytes([]byte{1, 2, 3})
		return nil
	})

	// SingleObjectFingerprint (C2 length): validates the 10-byte header then reads
	// it; a megabyte input is header-bounded and a short input errors, never panics.
	wantTerminate(t, "SingleObjectFingerprint/megabyte", func() error {
		_, _, err := SingleObjectFingerprint(hostile1MiB)
		return err
	})
	wantTerminate(t, "SingleObjectFingerprint/short", func() error {
		_, _, err := SingleObjectFingerprint([]byte{0xC3, 0x01})
		return err
	})

	// Fingerprint (C6 metadata-hash): hashes Canonical(), so it inherits the
	// maxSchemaJSONBytes budget. A megabyte Props number (stripped by PCF) and a
	// recursive (cyclic) schema must both fingerprint fast without re-expansion.
	wantTerminate(t, "Fingerprint/metadata-megabyte-number", func() error {
		s, err := Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"int"}],"x":` + strings.Repeat("9", 1<<20) + `}`)
		if err != nil {
			return err
		}
		_ = s.Fingerprint(NewRabin())
		return nil
	})
	wantTerminate(t, "Fingerprint/recursive-schema", func() error {
		_ = MustParse(recursiveNodeSchema).Fingerprint(NewRabin())
		return nil
	})
}

// wantAccept asserts fn accepts, returning a nil error. The cells that reach
// for it drive a hostile-shaped but perfectly legal input through a public
// entry point: a union of twenty thousand branches, a reference chain thousands
// long, a stray key nested a thousand deep. We want the entry point to take
// it, since a size the spec permits must not become an error.
func wantAccept(t *testing.T, name string, fn func() error) {
	t.Helper()
	if err := fn(); err != nil {
		t.Errorf("%s: %v", name, err)
	}
}

// dosChainSchema builds a backward-reference chain: Top's field i defines
// record Ri whose single field references R(i-1). Every custom-match subtree
// walk from Ri reaches all of R0..R(i-1), so without a shared per-parse memo
// the finalize stamping loop costs O(n^2) node visits over ~60n bytes of
// schema text.
func dosChainSchema(n int) string {
	var sb strings.Builder
	sb.WriteString(`{"type":"record","name":"Top","fields":[`)
	for i := range n {
		if i > 0 {
			sb.WriteString(",")
		}
		if i == 0 {
			sb.WriteString(`{"name":"f0","type":{"type":"record","name":"R0","fields":[{"name":"v","type":"long"}]}}`)
		} else {
			fmt.Fprintf(&sb, `{"name":"f%d","type":{"type":"record","name":"R%d","fields":[{"name":"r","type":"R%d"}]}}`, i, i, i-1)
		}
	}
	sb.WriteString(`]}`)
	return sb.String()
}

func TestDoSBattery_C9_CustomTypeParseCost(t *testing.T) {
	noMatch := CustomType{LogicalType: "no-such-logical", AvroType: "string"}
	match := CustomType{AvroType: "long"} // matches every chain leaf

	// The chain length is the factor, driven at two values read from the
	// registry. Both lengths are well-formed schema text the parser must
	// accept. The memo is what keeps the stamping walk from re-descending the
	// whole chain per link, and its absence is quadratic node visits, not a
	// rejection.
	for _, n := range costFactorValues(t, "TestDoSBattery_C9_CustomTypeParseCost") {
		chain := dosChainSchema(n) // ~60n bytes of well-formed schema text

		// Parse × non-matching CustomType: every stamp walk completes clean,
		// the worst case for a memo (nothing to short-circuit on).
		wantAccept(t, fmt.Sprintf("Parse/chain-noMatch-custom/n=%d", n), func() error {
			_, err := Parse(chain, noMatch)
			return err
		})

		// Parse × matching CustomType: walks short-circuit at the chain's
		// leaf, which without a memo is still O(n) per walk = O(n^2) total.
		// The memo must bound the match case too, not only the clean case.
		wantAccept(t, fmt.Sprintf("Parse/chain-matching-custom/n=%d", n), func() error {
			_, err := Parse(chain, match)
			return err
		})
	}

	// SchemaCache × many references to a large inherited type: the boundary
	// guard and the overlay completion each walk the inherited subtree per
	// reference. Without per-parse sharing that is O(refs × nodes), seconds at
	// 1000 × 5000; with it, one walk total.
	var big strings.Builder
	big.WriteString(`{"type":"record","name":"Big","fields":[`)
	for i := range 5000 {
		if i > 0 {
			big.WriteString(",")
		}
		fmt.Fprintf(&big, `{"name":"f%d","type":"long"}`, i)
	}
	big.WriteString(`]}`)
	var wide strings.Builder
	wide.WriteString(`{"type":"record","name":"Wide","fields":[`)
	for i := range 1000 {
		if i > 0 {
			wide.WriteString(",")
		}
		fmt.Fprintf(&wide, `{"name":"r%d","type":"Big"}`, i)
	}
	wide.WriteString(`]}`)

	c := new(SchemaCache)
	if _, err := c.Parse(big.String(), noMatch); err != nil {
		t.Fatalf("cache parse of the inherited type: %v", err)
	}
	wantAccept(t, "SchemaCache.Parse/many-refs-custom", func() error {
		_, err := c.Parse(wide.String(), noMatch)
		return err
	})
}

//////////////////////////////////////////////////////////////////////////////
// C11: schema-declared magnitude at the top of the integer range.
//////////////////////////////////////////////////////////////////////////////

// TestDoSBattery_C11_SchemaDeclaredMagnitude is the column this battery was
// missing. C2 drives hostile counts and lengths off the wire. Nothing drove a
// magnitude the schema *text* declares. A `fixed` size is the one parse-time
// quantity whose value is not bounded by the length of the text declaring it.
// Nineteen characters name 2^63, the parser deliberately leaving the upper
// bound open to match the lenient majority. So any entry point doing arithmetic
// on it has to survive the top of the range.
//
// The bound is saturation at the producer (saturateSchemaMagnitude). The shapes
// reach the arithmetic in each of the ways it can be: a magnitude standing
// alone, a sum over record fields that carries past the range, and a union
// whose smallest branch is one. dosRun's "never panics" assertion is the
// operative one, since an unsaturated sum reaches a divisor as zero.
func TestDoSBattery_C11_SchemaDeclaredMagnitude(t *testing.T) {
	const huge = `{"type":"fixed","name":"BigF","size":9223372036854775807}`
	// A record whose field minimums sum past the range and land on -1.
	const sumWrap = `{"type":"record","name":"SumWrap","fields":[
		{"name":"lead","type":"long"},
		{"name":"a","type":{"type":"fixed","name":"SWA","size":9223372036854775807}},
		{"name":"b","type":{"type":"fixed","name":"SWB","size":9223372036854775807}}]}`

	shapes := []struct{ name, schema string }{
		{"fixed-alone", huge},
		{"sum-wrap-record", sumWrap},
		{"union-of-huge", `[` + huge + `]`},
		{"map-of-sum-wrap", `{"type":"map","values":` + sumWrap + `}`},
		{"array-of-sum-wrap", `{"type":"array","items":` + sumWrap + `}`},
		{"map-of-huge", `{"type":"map","values":` + huge + `}`},
		{"decimal-on-huge-fixed", `{"type":"fixed","name":"BigD","size":9223372036854775807,"logicalType":"decimal","precision":4,"scale":2}`},
	}

	// One block header claiming a single element, and nothing after it. Every
	// shape above needs more bytes for one element than any wire can hold.
	claimsOne := []byte{0x02}

	for _, sh := range shapes {
		// Parse must accept these: an open upper bound is the documented
		// posture, so rejecting here would be the other kind of bug.
		s, err := Parse(sh.schema)
		if err != nil {
			t.Errorf("%s: parse rejected a schema the open-size posture accepts: %v", sh.name, err)
			continue
		}

		wantTerminate(t, "Decode/"+sh.name, func() error {
			var v any
			_, err := s.Decode(claimsOne, &v)
			return err
		})
		wantTerminate(t, "DecodeJSON/"+sh.name, func() error {
			var v any
			return s.DecodeJSON([]byte(`{}`), &v)
		})
		wantTerminate(t, "Encode/"+sh.name, func() error {
			_, err := s.Encode(map[string]any{})
			return err
		})
		wantTerminate(t, "EncodeJSON/"+sh.name, func() error {
			_, err := s.EncodeJSON(map[string]any{})
			return err
		})
		// The metadata surfaces read the same magnitude back out.
		wantTerminate(t, "Root/"+sh.name, func() error {
			r := s.Root()
			_, err := r.Schema()
			return err
		})
		wantTerminate(t, "Canonical/"+sh.name, func() error {
			_ = s.Canonical()
			_ = s.String()
			_ = s.Fingerprint(NewRabin())
			return nil
		})
		// Resolution derives the same bounds a second and third time: once
		// for the resolved reader tree, once for the skip of a dropped field.
		wantTerminate(t, "Resolve/"+sh.name, func() error {
			_, err := Resolve(s, s)
			return err
		})
		wantTerminate(t, "CheckCompatibility/"+sh.name, func() error {
			return CheckCompatibility(s, s)
		})

		dropW, errW := Parse(`{"type":"record","name":"DropOuter","fields":[
			{"name":"c","type":` + sh.schema + `},{"name":"keep","type":"int"}]}`)
		dropR, errR := Parse(`{"type":"record","name":"DropOuter","fields":[{"name":"keep","type":"int"}]}`)
		if errW == nil && errR == nil {
			wantTerminate(t, "Resolve+Decode/"+sh.name, func() error {
				res, err := Resolve(dropW, dropR)
				if err != nil {
					return err
				}
				var v any
				_, err = res.Decode(append(append([]byte{}, claimsOne...), 0x02), &v)
				return err
			})
		}
	}

	// SchemaCache.Parse derives the same bounds through the cache's own
	// build path, and a second parse re-derives them over inherited nodes.
	wantTerminate(t, "SchemaCache.Parse/sum-wrap", func() error {
		var c SchemaCache
		if _, err := c.Parse(sumWrap); err != nil {
			return err
		}
		s, err := c.Parse(`{"type":"map","values":"SumWrap"}`)
		if err != nil {
			return err
		}
		var v any
		_, err = s.Decode(claimsOne, &v)
		return err
	})
}

// ---------- race_bounds_test.go ----------

// Nothing in this suite asserts a wall-clock budget. What remains that has to
// account for the race detector is the liveness deadlines: the watchdogs and
// hang probes that turn "this never returned" into a failure instead of a
// wedged package. The rule for widening them is stated once here. It used to be
// stated six times, and six statements of one rule agree only until one is
// edited.
//
// A deadline set for unraced work is the one that fails silently under the
// detector: it does not report a hang, it reports a healthy walk that was
// instrumented. So the rule is a multiplier with an absolute floor under it,
// and both numbers live here.

// raceCostMultiplier is how much the detector inflates this suite's own work.
// It is measured, not chosen. Running the DoS battery with and without -race
// gives per-cell ratios from 2.3x (C1 deep nesting) to 8.3x (C10c wide-record
// surfaces). A per-call measurement of the widest parse cell gives 6.2x. Ten
// covers the measured maximum with margin.
//
// It stays far below what it has to. A deadline exists to separate "returned"
// from "did not return". The unbounded walks these watchdogs were written
// against run for minutes or forever, so multiplying the deadline by ten costs
// nothing in detection.
const raceCostMultiplier = 10

// raceCeilingFloor is the headroom a deadline gets under -race no matter how
// short it is. A probe over work that normally takes microseconds needs
// *absolute* headroom, not proportional: ten times a 100ms deadline is still a
// second, and process startup, GC and host load are not proportional to the
// work. The floor serves those probes and the multiplier serves the ones whose
// legitimate work is already large. Taking the larger of the two lets one rule
// serve both, and is why this loosens nothing below a 300ms deadline.
const raceCeilingFloor = 3 * time.Second

// raceRelaxed returns the deadline to enforce for a normal one. It never
// tightens: the result is >= normal in every mode, since the multiplier is
// >= 1 and the floor only ever raises.
func raceRelaxed(normal time.Duration) time.Duration {
	if !raceEnabled {
		return normal
	}
	return max(raceCeilingFloor, raceCostMultiplier*normal)
}

// raceInflated scales an allowance by the same inflation, with no absolute
// floor. The distinction from raceRelaxed is what the number already is. The
// two callers here (the DoS watchdog's 4s budget, the schema-node batteries'
// 30s hang deadline) are already seconds, so the floor would change nothing.
// Stating them without it keeps them proportional to the work they cover
// rather than to an unrelated minimum.
func raceInflated(allowance time.Duration) time.Duration {
	if !raceEnabled {
		return allowance
	}
	return raceCostMultiplier * allowance
}

// hangDeadline is the wall-clock backstop the schema-node budget batteries use
// to turn a hang into a failure. It is a liveness detector, never a performance
// assertion. The property under test is that an over-budget walk rejects. The
// goroutine plus deadline exist only so a regression that stopped bounding the
// walk surfaces as a failure instead of wedging the suite. Those batteries are
// the one place whose work is at the budget by construction, since a cell must
// exceed maxSchemaJSONNodes. So they are the slowest thing here and the
// detector multiplies that: two run 21s and 33s in isolation under -race,
// higher under the full suite's parallelism.
var hangDeadline = raceInflated(30 * time.Second)

//////////////////////////////////////////////////////////////////////////////
// The enumeration guard
//////////////////////////////////////////////////////////////////////////////

// raceRelaxation rows one file that decides something by asking whether the
// race detector is on. Rows are per file with a site count rather than per
// line, so they do not rot on an unrelated edit. They still fail in both
// directions: a new consult raises the count, a removed one lowers it.
type raceRelaxation struct {
	file string
	// sites is how many times the file consults the race predicate.
	sites int
	// kind is what the file does with the answer. "authority" is this file.
	// Everything else has to say why it is not simply asking the authority.
	kind string
	why  string
}

// The set is derived from source below, not from this list. This list is what
// the derivation is checked against, so a consult in no row fails and a row
// naming a file that no longer consults fails. Rows are per file, and the
// test-file consolidation made some files hold several of these sections. The
// `// ---------- x ----------` banner still names which original file a consult
// sits in, so a row covering more than one says so and splits its count.
var raceRelaxations = []raceRelaxation{
	{file: "internal_nets_test.go", sites: 3, kind: "authority",
		why: "the race_bounds section is the whole set: raceRelaxed and raceInflated — the two forms of the rule and the only place either number appears — plus the invariant that asserts neither ever tightens. No cell asserts a wall-clock BUDGET any more, so what the rule now widens is liveness: the DoS watchdog's deadline, the schema-node hang deadline, and the one hang probe in package avro_test that reaches raceRelaxed through the export bridge"},
}

// raceConstrained reports whether src carries a build constraint mentioning
// the race tag. Matched on a word boundary so an unrelated tag containing the
// letters (a "trace" build, say) is not mistaken for one.
var raceTagRE = regexp.MustCompile(`\brace\b`)

// declaredNameRE captures the identifier a line declares, so an occurrence can
// be told from the declaration it names.
var declaredNameRE = regexp.MustCompile(`^\s*(?:const|var|func)\s+([A-Za-z_][A-Za-z0-9_]*)`)

func raceConstrained(src string) bool {
	for line := range strings.SplitSeq(src, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "//go:build") {
			if line != "" && !strings.HasPrefix(line, "//") {
				return false // past the header
			}
			continue
		}
		return raceTagRE.MatchString(line)
	}
	return false
}

// raceAnswerers derives the identifiers that answer "is the detector on".
//
// The predicate is identified by its defining shape, not by which file it sits
// in: a boolean declared `true` under a race build constraint and `false` under
// the negated one. Keying on the file was tried first and was wrong in the
// expensive direction. It swept in every constant of any race-tagged file, so
// an unrelated build-tagged duration became an "answerer" and the guard
// reported five phantom consults in a file that mentions none of this.
//
// The set is closed transitively over the two ways an answer is passed on: an
// identifier declared equal to an answerer, and a niladic bool function
// returning one. Without the closure a consult can hide one alias-hop from the
// declaration, which is exactly where the two test packages put theirs.
func raceAnswerers(t *testing.T, files []string) map[string]bool {
	t.Helper()
	boolDecl := regexp.MustCompile(`(?m)^(?:const|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(true|false)\s*$`)
	sawTrue, sawFalse := map[string]bool{}, map[string]bool{}
	for _, f := range files {
		src := readFile(t, f)
		if !raceConstrained(src) {
			continue
		}
		for _, m := range boolDecl.FindAllStringSubmatch(blankCode(src), -1) {
			if m[2] == "true" {
				sawTrue[m[1]] = true
			} else {
				sawFalse[m[1]] = true
			}
		}
	}
	out := map[string]bool{}
	for name := range sawTrue {
		if sawFalse[name] {
			out[name] = true
		}
	}
	if len(out) == 0 {
		t.Fatal("derived no build-switched boolean predicate — the derivation broke, and a broken derivation reads as full coverage")
	}
	aliasDecl := regexp.MustCompile(`(?m)^(?:const|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?:[A-Za-z_][A-Za-z0-9_]*\.)?([A-Za-z_][A-Za-z0-9_]*)\s*$`)
	wrapDecl := regexp.MustCompile(`(?m)^func ([A-Za-z_][A-Za-z0-9_]*)\(\) bool \{\s*\n\s*return (?:[A-Za-z_][A-Za-z0-9_]*\.)?([A-Za-z_][A-Za-z0-9_]*)\s*\n\}`)
	for grew := true; grew; {
		grew = false
		for _, f := range files {
			code := blankCode(readFile(t, f))
			for _, re := range []*regexp.Regexp{aliasDecl, wrapDecl} {
				for _, m := range re.FindAllStringSubmatch(code, -1) {
					if out[m[2]] && !out[m[1]] {
						out[m[1]] = true
						grew = true
					}
				}
			}
		}
	}
	return out
}

// TestInvariant_EveryRaceRelaxationIsRowed derives every place the suite decides
// something by asking whether the race detector is on, and requires each to be
// rowed.
//
// The scope is stated because it has one. It finds occurrences of the
// identifiers that answer the question, derived from what the build-tagged
// mechanism files declare plus any wrapper returning one, inside every
// *_test.go file the module walk reaches. It therefore cannot see a bound
// relaxed by other means: a bound generous enough that -race never trips it,
// one keyed on GOMAXPROCS or an environment variable, one behind a build tag of
// its own, or a cell that does not assert a time.
func TestInvariant_EveryRaceRelaxationIsRowed(t *testing.T) {
	files := moduleTestFiles(t)
	answerers := raceAnswerers(t, files)

	rowed := map[string]raceRelaxation{}
	for _, r := range raceRelaxations {
		if _, dup := rowed[r.file]; dup {
			t.Errorf("raceRelaxations rows %s twice", r.file)
		}
		if r.why == "" || r.kind == "" {
			t.Errorf("raceRelaxations row for %s states no kind/why — a row that explains nothing is not a classification", r.file)
		}
		rowed[r.file] = r
	}

	counted := map[string]int{}
	for _, f := range files {
		src := readFile(t, f)
		if raceConstrained(src) {
			continue
		}
		code := blankCode(src)
		n := 0
		for id := range answerers {
			// A declaration of an answerer is not a consult of one: the bridge
			// and the wrapper each name one on their own signature line. Only the
			// identifier in the declared position is exempt, not the whole line.
			// Skipping any line beginning const/var/func let a genuine consult
			// hide on a declaration line (`var _ = func() bool { if raceEnabled
			// ... }`). The guard then passed a newly added relaxation, the
			// shape it exists to catch. Attacking it by *adding* a member is
			// the only reason that surfaced. Removing one had always redded.
			for _, loc := range regexp.MustCompile(`\b`+id+`\b`).FindAllStringIndex(code, -1) {
				lineStart := strings.LastIndex(code[:loc[0]], "\n") + 1
				lineEnd := lineStart + strings.IndexByte(code[lineStart:]+"\n", '\n')
				if m := declaredNameRE.FindStringSubmatchIndex(code[lineStart:lineEnd]); m != nil &&
					lineStart+m[2] == loc[0] {
					continue
				}
				n++
			}
		}
		if n > 0 {
			counted[filepath.Base(f)] = n
		}
	}

	for f, n := range counted {
		r, ok := rowed[f]
		if !ok {
			t.Errorf("%s consults the race predicate %d time(s) but is not rowed in raceRelaxations.\nRow it saying what it does with the answer — and if it is a wall-clock ceiling, it should be calling raceRelaxed instead of deciding for itself.", f, n)
			continue
		}
		if r.sites != n {
			t.Errorf("%s consults the race predicate %d time(s), row says %d.\nA changed count is a new decision or a removed one; either way the row has to say which.", f, n, r.sites)
		}
	}
	for f := range rowed {
		if _, ok := counted[f]; !ok {
			t.Errorf("raceRelaxations rows %s, which no longer consults the race predicate — the row rotted", f)
		}
	}
}

// TestInvariant_RaceRelaxationNeverTightens pins the two properties the whole
// scheme rests on, in both build modes, so neither can be lost to a future
// edit of the arithmetic. A relaxation never returns less than the deadline it
// was given. Under -race a probe whose normal deadline is large gets headroom
// proportional to it, rather than a fixed ceiling that shrinks as the deadline
// grows. The second is the property the floor form did not have, which is why
// it is asserted rather than left to the constant's documentation.
func TestInvariant_RaceRelaxationNeverTightens(t *testing.T) {
	for _, normal := range []time.Duration{
		time.Microsecond, time.Millisecond, 100 * time.Millisecond,
		500 * time.Millisecond, 1500 * time.Millisecond, 4 * time.Second, time.Minute,
	} {
		if got := raceRelaxed(normal); got < normal {
			t.Errorf("raceRelaxed(%v) = %v — tightened", normal, got)
		}
		if got := raceInflated(normal); got < normal {
			t.Errorf("raceInflated(%v) = %v — tightened", normal, got)
		}
	}
	if !raceEnabled {
		for _, normal := range []time.Duration{time.Millisecond, time.Minute} {
			if got := raceRelaxed(normal); got != normal {
				t.Errorf("raceRelaxed(%v) = %v without -race — the tight deadline must stay in effect", normal, got)
			}
		}
		return
	}
	// Under -race, headroom is proportional once the bound is past the floor.
	small, large := 100*time.Millisecond, 10*time.Second
	if ratio := raceRelaxed(large) / large; ratio < 2 {
		t.Errorf("raceRelaxed(%v) leaves only %vx headroom — the ceiling stops scaling with the deadline, which is the shape that reds a correct probe", large, ratio)
	}
	if raceRelaxed(small) < raceCeilingFloor {
		t.Errorf("raceRelaxed(%v) = %v — below the absolute floor a short deadline needs", small, raceRelaxed(small))
	}
}

//////////////////////////////////////////////////////////////////////////////
// Shared source derivation
//////////////////////////////////////////////////////////////////////////////

// moduleTestFiles returns every _test.go file in the module, in every package.
//
// The scope is stated once for every guard that uses it. The root is the
// directory holding go.mod, found by walking up from the working directory, and
// the walk takes every subdirectory except testdata, vendor and
// dot-directories. So the set is "test files of this module", derived from the
// filesystem rather than from a list of package directories. That list is what
// it replaced, and why a guard built on it could not see the ocf package at
// all. It still cannot see a test in a *different* module, source generated at
// run time, or anything a non-test file does.
func moduleTestFiles(t *testing.T) []string {
	t.Helper()
	root, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("resolving working directory: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		if parent == root {
			t.Fatal("no go.mod above the working directory — the module root derivation broke")
		}
		root = parent
	}
	var out []string
	err = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if p != root && (name == "testdata" || name == "vendor" || strings.HasPrefix(name, ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(d.Name(), "_test.go") {
			rel, rerr := filepath.Rel(root, p)
			if rerr != nil {
				return rerr
			}
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the module: %v", err)
	}
	// A walk that finds one package is a walk that silently lost the others,
	// which is the exact failure this replaced. Require more than one
	// directory to be represented.
	dirs := map[string]bool{}
	for _, f := range out {
		dirs[filepath.Dir(f)] = true
	}
	// A walk that finds one package is a walk that silently lost the others,
	// which is the exact failure this replaced. The file floor was 25 when the
	// suite was 192 files. It is 15 now that they are consolidated, so the
	// floor is 10. Both halves still fail a walk that returns nearly nothing,
	// which is all the count can honestly promise. The *directory* half is the
	// one that catches the failure that actually happened.
	if len(out) < 10 || len(dirs) < 2 {
		t.Fatalf("module walk found %d test files across %d directories — too few; the walk broke and a broken walk reads as full coverage", len(out), len(dirs))
	}
	return out
}

// ---------- coverage_gaps_test.go ----------

// TestCoverage_JSONNumericIntSizeForms exercises jsonNumericInt via the public
// parse paths that actually reach it: a bare numeric size (int64 arm) and a
// quoted-string size "16" (string arm, the Avro [INTEGERS] rule), both flowing
// through getCIInt during Root() metadata-tree construction. The float64 and
// json.Number arms stay uncovered. They are defensive breadth no current public
// path produces, so covering them would mean constructing the metadata value by
// hand. Left uncovered on purpose rather than with a theater test.
func TestCoverage_JSONNumericIntSizeForms(t *testing.T) {
	for _, sz := range []string{`16`, `"16"`} { // bare (json.Number) and quoted (string)
		s := MustParse(fmt.Sprintf(`{"type":"fixed","name":"F","size":%s}`, sz))
		root := s.Root()
		if root.Size != 16 {
			t.Fatalf("fixed size %s: Root().Size = %d, want 16", sz, root.Size)
		}
		// The quoted and bare forms must produce the same wire behavior too.
		wire, err := s.AppendEncode(nil, make([]byte, 16))
		if err != nil || len(wire) != 16 {
			t.Fatalf("fixed size %s: encode 16 bytes: err=%v len=%d", sz, err, len(wire))
		}
	}
}

// TestCoverage_RatFromBytesHostileScale exercises bytesToRat's public-API
// safety guard: RatFromBytes is exported, so a caller can pass an
// attacker-controlled scale beyond decimalScaleLimit (internal callers pass
// schema-validated bounded scale). The guard must return a zero Rat rather
// than materialize a 10^scale big.Int. This branch had no coverage.
func TestCoverage_RatFromBytesHostileScale(t *testing.T) {
	for _, scale := range []int{decimalScaleLimit + 1, -decimalScaleLimit - 1} {
		r := RatFromBytes([]byte{0x01}, scale)
		if r == nil || r.Sign() != 0 {
			t.Fatalf("RatFromBytes with hostile scale %d: got %v, want zero Rat", scale, r)
		}
	}
	// A within-bounds scale still works (control).
	if r := RatFromBytes([]byte{0x01}, 2); r.Cmp(scaledRat(bytesToBigInt([]byte{0x01}), 2)) != 0 {
		t.Fatalf("RatFromBytes within bounds diverged: %v", r)
	}
}

// ---------- export_test.go ----------

// Test-only bridges for the external avro_test package (compiled only into
// the test binary; not part of the library surface).

// RaceRelaxedForTest is the deadline-widening authority (race_bounds_test.go),
// bridged so package avro_test asks it instead of keeping a second copy of the
// rule. The two packages cannot share an unexported helper, and that is exactly
// why the rule was duplicated. The sharing is explicit here rather than left to
// two comments agreeing with each other.
func RaceRelaxedForTest(normal time.Duration) time.Duration { return raceRelaxed(normal) }

// SlabFreeForTest reports the internal slab-free classification: whether
// Decode bypasses the slab pool and runs this schema's deser on a nil slab.
func (s *Schema) SlabFreeForTest() bool { return s.slabFree }

// DeserNilSlabForTest drives the compiled deser directly with a nil slab,
// exactly as Decode does for slab-free schemas, regardless of
// classification. v must be a non-nil pointer. Used by the slab-free oracle
// net to prove that classification matches actual slab usage.
func (s *Schema) DeserNilSlabForTest(src []byte, v any) ([]byte, error) {
	return s.deser(src, reflect.ValueOf(v).Elem(), nil)
}

// ---------- fuzz_test.go ----------

// fuzzNamedString / fuzzNamedBytes / fuzzNamedFloat are named-type aliases
// used by FuzzSetValueTargets to exercise the set{Float,Bytes,String}Value
// helper arms that branch on Kind (not on concrete *Type), plus the
// TextUnmarshaler-via-Addr path.
type fuzzNamedString string

type fuzzNamedBytes []byte

type fuzzNamedFloat float64

// fuzzTextThing implements encoding.TextUnmarshaler / TextMarshaler so
// setStringValue's TextUnmarshaler-on-Addr branch fires.
type fuzzTextThing struct{ S string }

func (t *fuzzTextThing) UnmarshalText(b []byte) error { t.S = string(b); return nil }
func (t fuzzTextThing) MarshalText() ([]byte, error)  { return []byte(t.S), nil }

// fuzzSchemas contains pre-compiled schemas covering all Avro types for use
// in fuzz targets that exercise decoding.
var fuzzSchemas []*Schema

func init() {
	schemas := []string{
		// 0-7: 8 primitives
		`"null"`,
		`"boolean"`,
		`"int"`,
		`"long"`,
		`"float"`,
		`"double"`,
		`"bytes"`,
		`"string"`,
		// 8: enum
		`{"type":"enum","name":"E","symbols":["A","B","C"]}`,
		// 9: fixed
		`{"type":"fixed","name":"F","size":4}`,
		// 10: array of int
		`{"type":"array","items":"int"}`,
		// 11: map of string
		`{"type":"map","values":"string"}`,
		// 12: null union
		`["null","string"]`,
		// 13: general union
		`["null","int","string","boolean"]`,
		// 14: multi-field record
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"},{"name":"c","type":"boolean"},{"name":"d","type":"double"}]}`,
		// 15: nested record
		`{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"},{"name":"y","type":"string"}]}},{"name":"z","type":"long"}]}`,
		// 16: record with logical types
		`{"type":"record","name":"Logical","fields":[{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"d","type":{"type":"int","logicalType":"date"}},{"name":"id","type":{"type":"string","logicalType":"uuid"}}]}`,

		// 17-21: arrays of all specialized primitive types
		`{"type":"array","items":"boolean"}`,
		`{"type":"array","items":"long"}`,
		`{"type":"array","items":"float"}`,
		`{"type":"array","items":"double"}`,
		`{"type":"array","items":"string"}`,
		// 22-26: maps of all specialized primitive types
		`{"type":"map","values":"int"}`,
		`{"type":"map","values":"boolean"}`,
		`{"type":"map","values":"long"}`,
		`{"type":"map","values":"float"}`,
		`{"type":"map","values":"double"}`,
		// 27: fixed(16) UUID, exercising the deserFixedUUIDReflect path
		`{"type":"fixed","name":"UUID","size":16,"logicalType":"uuid"}`,
		// 28: record with nullable fields (exercises implicit null default)
		`{"type":"record","name":"N","fields":[
			{"name":"a","type":"int"},
			{"name":"b","type":["null","int"]},
			{"name":"c","type":["null","string"]}
		]}`,
		// 29: record with reused named type (exercises dedup path)
		`{"type":"record","name":"D","fields":[
			{"name":"u1","type":{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}},
			{"name":"u2","type":"U"}
		]}`,
		// 30: recursive record (linked list via nullable self-reference)
		nodeRecursiveSchema,
		// 31: multi-level nested records (3 levels deep)
		`{"type":"record","name":"L1","fields":[
			{"name":"a","type":"int"},
			{"name":"l2","type":{"type":"record","name":"L2","fields":[
				{"name":"b","type":"string"},
				{"name":"l3","type":{"type":"record","name":"L3","fields":[
					{"name":"c","type":"double"},
					{"name":"items","type":{"type":"array","items":"long"}}
				]}}
			]}}
		]}`,
	}
	for _, s := range schemas {
		fuzzSchemas = append(fuzzSchemas, MustParse(s))
	}
}

// fuzzSeed encodes v using the given schema and returns the raw bytes.
// It panics on error, so it should only be called from init or seed setup.
func fuzzSeed(s *Schema, v any) []byte {
	b, err := s.Encode(v)
	if err != nil {
		panic(err)
	}
	return b
}

// fuzzEqual is like reflect.DeepEqual but treats NaN == NaN as true,
// recursing into maps, slices, and arrays.
func fuzzEqual(a, b any) bool {
	va := reflect.ValueOf(a)
	vb := reflect.ValueOf(b)
	return fuzzDeepEqual(va, vb)
}

func fuzzDeepEqual(a, b reflect.Value) bool {
	if !a.IsValid() && !b.IsValid() {
		return true
	}
	if !a.IsValid() || !b.IsValid() {
		return false
	}
	if a.Type() != b.Type() {
		return false
	}
	switch a.Kind() {
	case reflect.Float32, reflect.Float64:
		af, bf := a.Float(), b.Float()
		if math.IsNaN(af) && math.IsNaN(bf) {
			return true
		}
		return af == bf
	case reflect.Map:
		if a.Len() != b.Len() {
			return false
		}
		for _, k := range a.MapKeys() {
			va := a.MapIndex(k)
			vb := b.MapIndex(k)
			if !vb.IsValid() || !fuzzDeepEqual(va, vb) {
				return false
			}
		}
		return true
	case reflect.Slice, reflect.Array:
		if a.Len() != b.Len() {
			return false
		}
		for i := range a.Len() {
			if !fuzzDeepEqual(a.Index(i), b.Index(i)) {
				return false
			}
		}
		return true
	case reflect.Interface:
		return fuzzDeepEqual(a.Elem(), b.Elem())
	default:
		return reflect.DeepEqual(a.Interface(), b.Interface())
	}
}

func FuzzParse(f *testing.F) {
	// Primitives.
	for _, s := range []string{
		`"null"`, `"boolean"`, `"int"`, `"long"`,
		`"float"`, `"double"`, `"bytes"`, `"string"`,
	} {
		f.Add(s)
	}

	// Complex types.
	f.Add(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	f.Add(`{"type":"enum","name":"E","symbols":["X","Y"]}`)
	f.Add(`{"type":"array","items":"string"}`)
	f.Add(`{"type":"map","values":"int"}`)
	f.Add(`{"type":"fixed","name":"F","size":8}`)
	f.Add(`["null","string"]`)
	f.Add(`["null","int","string","boolean"]`)

	// Logical types.
	f.Add(`{"type":"long","logicalType":"timestamp-millis"}`)
	f.Add(`{"type":"int","logicalType":"date"}`)
	f.Add(`{"type":"string","logicalType":"uuid"}`)
	f.Add(`{"type":"int","logicalType":"time-millis"}`)

	// Aliases, namespaces, defaults.
	f.Add(`{"type":"record","name":"R","namespace":"com.example","fields":[{"name":"a","type":"int","default":0}]}`)
	f.Add(`{"type":"record","name":"R","fields":[{"name":"a","type":"int","aliases":["b"]}]}`)
	f.Add(`{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`)

	// Nested.
	f.Add(`{"type":"record","name":"O","fields":[{"name":"inner","type":{"type":"record","name":"I","fields":[{"name":"x","type":"int"}]}}]}`)

	// Invalid inputs.
	f.Add(``)
	f.Add(`{}`)
	f.Add(`[]`)
	f.Add(`{"type":"bogus"}`)
	f.Add(`not json at all`)
	f.Add(`{"type":"record"}`)
	f.Add(`{"type":"record","name":"R","fields":[{"name":"a","type":"nonexistent"}]}`)

	f.Fuzz(func(t *testing.T, schema string) {
		Parse(schema)
	})
}

func FuzzDecode(f *testing.F) {
	// Seed: for each schema, add valid encoded bytes and empty bytes.
	seeds := []struct {
		idx  uint8
		data []byte
	}{
		// null (null is zero bytes in Avro, no encoding needed)
		{0, []byte{}},
		{0, nil},
		// boolean
		{1, fuzzSeed(fuzzSchemas[1], true)},
		{1, nil},
		// int
		{2, fuzzSeed(fuzzSchemas[2], int32(42))},
		{2, fuzzSeed(fuzzSchemas[2], int32(-1))},
		{2, nil},
		// long
		{3, fuzzSeed(fuzzSchemas[3], int64(1234567890))},
		{3, nil},
		// float
		{4, fuzzSeed(fuzzSchemas[4], float32(3.14))},
		{4, nil},
		// double
		{5, fuzzSeed(fuzzSchemas[5], float64(2.718281828))},
		{5, nil},
		// bytes
		{6, fuzzSeed(fuzzSchemas[6], []byte("hello"))},
		{6, nil},
		// string
		{7, fuzzSeed(fuzzSchemas[7], "hello world")},
		{7, nil},
		// enum
		{8, fuzzSeed(fuzzSchemas[8], "A")},
		{8, nil},
		// fixed
		{9, fuzzSeed(fuzzSchemas[9], [4]byte{1, 2, 3, 4})},
		{9, nil},
		// array
		{10, fuzzSeed(fuzzSchemas[10], []int32{1, 2, 3})},
		{10, nil},
		// map
		{11, fuzzSeed(fuzzSchemas[11], map[string]string{"k": "v"})},
		{11, nil},
		// null union
		{12, fuzzSeed(fuzzSchemas[12], (*string)(nil))},
		{12, fuzzSeed(fuzzSchemas[12], "test")},
		{12, nil},
		// general union
		{13, fuzzSeed(fuzzSchemas[13], (*int)(nil))},
		{13, fuzzSeed(fuzzSchemas[13], int32(7))},
		{13, nil},
		// multi-field record
		{14, fuzzSeed(fuzzSchemas[14], map[string]any{"a": int32(1), "b": "x", "c": true, "d": 1.5})},
		{14, nil},
		// nested record
		{15, fuzzSeed(fuzzSchemas[15], map[string]any{"inner": map[string]any{"x": int32(1), "y": "s"}, "z": int64(2)})},
		{15, nil},
		// logical types record
		{16, fuzzSeed(fuzzSchemas[16], map[string]any{"ts": int64(1000), "d": int32(19000), "id": "550e8400-e29b-41d4-a716-446655440000"})},
		{16, nil},
		// array of boolean
		{17, fuzzSeed(fuzzSchemas[17], []bool{true, false, true})},
		{17, nil},
		// array of long
		{18, fuzzSeed(fuzzSchemas[18], []int64{100, -200, 300})},
		{18, nil},
		// array of float
		{19, fuzzSeed(fuzzSchemas[19], []float32{1.5, -2.5})},
		{19, nil},
		// array of double
		{20, fuzzSeed(fuzzSchemas[20], []float64{3.14, 2.718})},
		{20, nil},
		// array of string
		{21, fuzzSeed(fuzzSchemas[21], []string{"hello", "world"})},
		{21, nil},
		// map of int
		{22, fuzzSeed(fuzzSchemas[22], map[string]int32{"a": 1, "b": 2})},
		{22, nil},
		// map of boolean
		{23, fuzzSeed(fuzzSchemas[23], map[string]bool{"t": true, "f": false})},
		{23, nil},
		// map of long
		{24, fuzzSeed(fuzzSchemas[24], map[string]int64{"x": 999})},
		{24, nil},
		// map of float
		{25, fuzzSeed(fuzzSchemas[25], map[string]float32{"pi": 3.14})},
		{25, nil},
		// map of double
		{26, fuzzSeed(fuzzSchemas[26], map[string]float64{"e": 2.718})},
		{26, nil},
		// fixed UUID
		{27, fuzzSeed(fuzzSchemas[27], [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})},
		{27, nil},
		// record with nullable fields (implicit null default)
		{28, fuzzSeed(fuzzSchemas[28], map[string]any{"a": int32(1), "b": nil, "c": nil})},
		{28, fuzzSeed(fuzzSchemas[28], map[string]any{"a": int32(1), "b": int32(2), "c": "hi"})},
		{28, nil},
		// record with reused named type
		{29, fuzzSeed(fuzzSchemas[29], map[string]any{
			"u1": [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			"u2": [16]byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		})},
		{29, nil},
		// recursive linked list: 3 nodes
		{30, fuzzSeed(fuzzSchemas[30], map[string]any{
			"value": int32(1),
			"next": map[string]any{
				"value": int32(2),
				"next": map[string]any{
					"value": int32(3),
					"next":  nil,
				},
			},
		})},
		{30, fuzzSeed(fuzzSchemas[30], map[string]any{"value": int32(42), "next": nil})},
		{30, nil},
		// 3-level nested record
		{31, fuzzSeed(fuzzSchemas[31], map[string]any{
			"a": int32(1),
			"l2": map[string]any{
				"b": "x",
				"l3": map[string]any{
					"c":     3.14,
					"items": []int64{10, 20, 30},
				},
			},
		})},
		{31, nil},
	}

	// Adversarial patterns.
	seeds = append(seeds,
		struct {
			idx  uint8
			data []byte
		}{2, bytes.Repeat([]byte{0xFF}, 16)}, // varint overflow for int
		struct {
			idx  uint8
			data []byte
		}{3, bytes.Repeat([]byte{0xFF}, 16)}, // varint overflow for long
		struct {
			idx  uint8
			data []byte
		}{7, []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x01}}, // huge string length
	)

	for _, s := range seeds {
		f.Add(s.idx, s.data)
	}

	f.Fuzz(func(t *testing.T, idx uint8, data []byte) {
		s := fuzzSchemas[int(idx)%len(fuzzSchemas)]
		var v any
		s.Decode(data, &v)
	})
}

func FuzzDecodeEncodeRoundTrip(f *testing.F) {
	// Seed: one valid encoding per schema.
	type seed struct {
		idx  uint8
		data []byte
	}
	seeds := []seed{
		{0, []byte{}}, // null is zero bytes
		{1, fuzzSeed(fuzzSchemas[1], true)},
		{2, fuzzSeed(fuzzSchemas[2], int32(42))},
		{3, fuzzSeed(fuzzSchemas[3], int64(99))},
		{4, fuzzSeed(fuzzSchemas[4], float32(1.5))},
		{5, fuzzSeed(fuzzSchemas[5], float64(2.5))},
		{6, fuzzSeed(fuzzSchemas[6], []byte("abc"))},
		{7, fuzzSeed(fuzzSchemas[7], "hello")},
		{8, fuzzSeed(fuzzSchemas[8], "B")},
		{9, fuzzSeed(fuzzSchemas[9], [4]byte{1, 2, 3, 4})},
		{10, fuzzSeed(fuzzSchemas[10], []int32{10, 20})},
		{11, fuzzSeed(fuzzSchemas[11], map[string]string{"key": "val"})},
		{12, fuzzSeed(fuzzSchemas[12], "test")},
		{13, fuzzSeed(fuzzSchemas[13], int32(5))},
		{14, fuzzSeed(fuzzSchemas[14], map[string]any{"a": int32(1), "b": "x", "c": false, "d": 3.14})},
		{15, fuzzSeed(fuzzSchemas[15], map[string]any{"inner": map[string]any{"x": int32(9), "y": "z"}, "z": int64(8)})},
		{16, fuzzSeed(fuzzSchemas[16], map[string]any{"ts": int64(0), "d": int32(0), "id": "550e8400-e29b-41d4-a716-446655440000"})},
		{17, fuzzSeed(fuzzSchemas[17], []bool{true, false})},
		{18, fuzzSeed(fuzzSchemas[18], []int64{100, -200})},
		{19, fuzzSeed(fuzzSchemas[19], []float32{1.5})},
		{20, fuzzSeed(fuzzSchemas[20], []float64{3.14})},
		{21, fuzzSeed(fuzzSchemas[21], []string{"hello"})},
		{22, fuzzSeed(fuzzSchemas[22], map[string]int32{"a": 1})},
		{23, fuzzSeed(fuzzSchemas[23], map[string]bool{"t": true})},
		{24, fuzzSeed(fuzzSchemas[24], map[string]int64{"x": 99})},
		{25, fuzzSeed(fuzzSchemas[25], map[string]float32{"pi": 3.14})},
		{26, fuzzSeed(fuzzSchemas[26], map[string]float64{"e": 2.718})},
		{27, fuzzSeed(fuzzSchemas[27], [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})},
		{28, fuzzSeed(fuzzSchemas[28], map[string]any{"a": int32(1), "b": int32(2), "c": "x"})},
		{29, fuzzSeed(fuzzSchemas[29], map[string]any{
			"u1": [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			"u2": [16]byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		})},
		{30, fuzzSeed(fuzzSchemas[30], map[string]any{
			"value": int32(1),
			"next": map[string]any{
				"value": int32(2),
				"next":  nil,
			},
		})},
		{31, fuzzSeed(fuzzSchemas[31], map[string]any{
			"a": int32(1),
			"l2": map[string]any{
				"b": "x",
				"l3": map[string]any{
					"c":     3.14,
					"items": []int64{10, 20, 30},
				},
			},
		})},
	}

	for _, s := range seeds {
		f.Add(s.idx, s.data)
	}

	f.Fuzz(func(t *testing.T, idx uint8, data []byte) {
		s := fuzzSchemas[int(idx)%len(fuzzSchemas)]

		var v1 any
		rem, err := s.Decode(data, &v1)
		if err != nil || len(rem) != 0 {
			return
		}

		encoded, err := s.Encode(v1)
		if err != nil {
			return // some decoded-into-any types can't re-encode (null, fixed)
		}

		var v2 any
		rem, err = s.Decode(encoded, &v2)
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}
		if len(rem) != 0 {
			t.Fatalf("re-decode left %d trailing bytes", len(rem))
		}

		if !fuzzEqual(v1, v2) {
			t.Fatalf("round-trip mismatch:\n  v1: %#v\n  v2: %#v", v1, v2)
		}
	})
}

func FuzzSingleObject(f *testing.F) {
	// Valid single-object encoded values for several schemas.
	for i, s := range fuzzSchemas {
		var val any
		switch i {
		case 0: // null
			val = nil
		case 1: // boolean
			val = true
		case 2: // int
			val = int32(42)
		case 3: // long
			val = int64(99)
		case 4: // float
			val = float32(1.5)
		case 5: // double
			val = float64(2.5)
		case 6: // bytes
			val = []byte("abc")
		case 7: // string
			val = "hello"
		case 8: // enum
			val = "A"
		case 9: // fixed
			val = [4]byte{1, 2, 3, 4}
		case 10: // array
			val = []int32{1, 2}
		case 11: // map
			val = map[string]string{"k": "v"}
		case 12: // null union
			val = "test"
		case 13: // general union
			val = int32(5)
		case 14: // multi-field record
			val = map[string]any{"a": int32(1), "b": "x", "c": true, "d": 1.5}
		case 15: // nested record
			val = map[string]any{"inner": map[string]any{"x": int32(1), "y": "s"}, "z": int64(2)}
		case 16: // logical types record
			val = map[string]any{"ts": int64(0), "d": int32(0), "id": "550e8400-e29b-41d4-a716-446655440000"}
		case 17: // array of boolean
			val = []bool{true, false}
		case 18: // array of long
			val = []int64{100, -200}
		case 19: // array of float
			val = []float32{1.5}
		case 20: // array of double
			val = []float64{3.14}
		case 21: // array of string
			val = []string{"hello"}
		case 22: // map of int
			val = map[string]int32{"a": 1}
		case 23: // map of boolean
			val = map[string]bool{"t": true}
		case 24: // map of long
			val = map[string]int64{"x": 99}
		case 25: // map of float
			val = map[string]float32{"pi": 3.14}
		case 26: // map of double
			val = map[string]float64{"e": 2.718}
		case 27: // fixed(16) UUID
			val = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		case 28: // nullable record
			val = map[string]any{"a": int32(1), "b": int32(2), "c": "x"}
		case 29: // reused named type
			val = map[string]any{
				"u1": [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
				"u2": [16]byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
			}
		case 30: // recursive linked list
			val = map[string]any{
				"value": int32(1),
				"next":  map[string]any{"value": int32(2), "next": nil},
			}
		case 31: // 3-level nested record
			val = map[string]any{
				"a": int32(1),
				"l2": map[string]any{
					"b": "x",
					"l3": map[string]any{
						"c":     3.14,
						"items": []int64{10, 20, 30},
					},
				},
			}
		}
		soe, err := s.AppendSingleObject(nil, val)
		if err != nil {
			continue
		}
		f.Add(soe)
	}

	// Truncated: just the magic.
	f.Add([]byte{0xC3, 0x01})
	// Too short for fingerprint.
	f.Add([]byte{0xC3, 0x01, 0x00, 0x00})
	// Wrong magic bytes.
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	// Empty.
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		SingleObjectFingerprint(data)
		for _, s := range fuzzSchemas {
			var v any
			s.DecodeSingleObject(data, &v)
		}
	})
}

// FuzzDecodeJSON feeds random JSON strings into the streaming JSON decoder.
// It exercises the byte scanner, schema-guided parsing, and error paths.
func FuzzDecodeJSON(f *testing.F) {
	seeds := []struct {
		idx   uint8
		input string
	}{
		// Primitives.
		{0, `null`},
		{1, `true`}, {1, `false`},
		{2, `42`}, {2, `-1`}, {2, `0`},
		{3, `1234567890`}, {3, `-9999`},
		{4, `3.14`}, {4, `"NaN"`}, {4, `"Infinity"`}, {4, `null`},
		{5, `2.718`}, {5, `"NaN"`}, {5, `"-Infinity"`}, {5, `null`},
		{6, `"hello"`}, {6, `""`},
		{7, `"world"`}, {7, `"line1\nline2"`},
		// Enum.
		{8, `"A"`}, {8, `"B"`}, {8, `"C"`},
		// Fixed.
		{9, `"abcd"`},
		// Array.
		{10, `[1,2,3]`}, {10, `[]`},
		// Map.
		{11, `{"k":"v"}`}, {11, `{}`},
		// Null union.
		{12, `null`}, {12, `{"string":"hello"}`}, {12, `"bare"`},
		// General union.
		{13, `null`}, {13, `42`}, {13, `"hello"`}, {13, `true`},
		{13, `{"int":42}`}, {13, `{"string":"tagged"}`},
		// Multi-field record.
		{14, `{"a":1,"b":"x","c":true,"d":3.14}`},
		{14, `{"a":1,"b":"x","c":true,"d":3.14,"extra":"skip"}`},
		// Nested record.
		{15, `{"inner":{"x":1,"y":"s"},"z":2}`},
		// Logical types.
		{16, `{"ts":1700000000000,"d":19700,"id":"550e8400-e29b-41d4-a716-446655440000"}`},
		// Invalid inputs.
		{2, `"notanumber"`}, {2, ``}, {2, `{}`},
		{7, `42`}, {1, `42`},
		{14, `{"a":"wrong"}`}, {14, `{}`},
		{10, `"notarray"`},
		{11, `"notmap"`},
	}
	for _, s := range seeds {
		f.Add(s.idx, s.input)
	}

	f.Fuzz(func(t *testing.T, idx uint8, input string) {
		s := fuzzSchemas[int(idx)%len(fuzzSchemas)]
		var v any
		s.DecodeJSON([]byte(input), &v)
	})
}

// FuzzDecodeJSONRoundTrip verifies that valid JSON run through DecodeJSON then
// EncodeJSON produces output that re-decodes to the same value.
func FuzzDecodeJSONRoundTrip(f *testing.F) {
	seeds := []struct {
		idx   uint8
		input string
	}{
		{2, `42`},
		{3, `99`},
		{4, `3.14`},
		{5, `2.718`},
		{7, `"hello"`},
		{1, `true`},
		{10, `[1,2,3]`},
		{11, `{"k":"v"}`},
		{12, `{"string":"test"}`},
		{12, `null`},
		{14, `{"a":1,"b":"x","c":true,"d":3.14}`},
	}
	for _, s := range seeds {
		f.Add(s.idx, s.input)
	}

	f.Fuzz(func(t *testing.T, idx uint8, input string) {
		s := fuzzSchemas[int(idx)%len(fuzzSchemas)]
		// Decode tolerates non-canonical-but-valid input (any boolean
		// byte, bare unions, whole-number floats, etc.). Invalid input
		// is skipped via the return below. Re-encoding produces canonical
		// output, so test canonical idempotence (encode, decode, encode is
		// stable) rather than bit-exact equality with the original.
		var v1 any
		if err := s.DecodeJSON([]byte(input), &v1); err != nil {
			return
		}
		encoded1, err := s.EncodeJSON(v1)
		if err != nil {
			return
		}
		var v2 any
		if err := s.DecodeJSON(encoded1, &v2); err != nil {
			t.Fatalf("re-decode of canonical encoded failed: %v\n  input: %s\n  encoded: %s", err, input, encoded1)
		}
		encoded2, err := s.EncodeJSON(v2)
		if err != nil {
			t.Fatalf("re-encode of canonical value failed: %v", err)
		}
		// Value-level fixpoint, NOT byte-level. Avro map encoding (binary and
		// JSON) iterates Go map keys in randomized, spec-legal order. So a
		// multi-key map re-encodes to a different byte ordering even though
		// the value is identical. fuzzEqual is order-robust (maps) and
		// NaN-robust, still catching real round-trip drift.
		if !fuzzEqual(v1, v2) {
			t.Fatalf("decode∘encode is not a value fixpoint:\n  v1: %#v\n  v2: %#v\n  input: %s\n  encoded1: %s\n  encoded2: %s", v1, v2, input, encoded1, encoded2)
		}
	})
}

// FuzzEncodeTaggedUnion verifies that Encode accepts tagged union maps from
// Decode(TaggedUnions) and produces canonical binary that is stable across
// additional encode/decode passes. That is canonical idempotence, not
// bit-exactness with the input: Postel's lenient decode plus strict canonical
// encode means non-canonical input legitimately canonicalizes on the first
// encode.
func FuzzEncodeTaggedUnion(f *testing.F) {
	seeds := []struct {
		idx  uint8
		data []byte
	}{
		{12, fuzzSeed(fuzzSchemas[12], "hello")},
		{12, fuzzSeed(fuzzSchemas[12], (*string)(nil))},
		{13, fuzzSeed(fuzzSchemas[13], int32(7))},
		{13, fuzzSeed(fuzzSchemas[13], "test")},
		{13, fuzzSeed(fuzzSchemas[13], true)},
		{13, fuzzSeed(fuzzSchemas[13], (*int)(nil))},
		{14, fuzzSeed(fuzzSchemas[14], map[string]any{"a": int32(1), "b": "x", "c": true, "d": 1.5})},
	}
	for _, s := range seeds {
		f.Add(s.idx, s.data)
	}

	f.Fuzz(func(t *testing.T, idx uint8, data []byte) {
		s := fuzzSchemas[int(idx)%len(fuzzSchemas)]
		var tagged1 any
		rem, err := s.Decode(data, &tagged1, TaggedUnions())
		if err != nil || len(rem) != 0 {
			return
		}
		encoded1, err := s.Encode(tagged1)
		if err != nil {
			return
		}
		// Canonical idempotence: re-decoding the encoded bytes and
		// re-encoding must produce the same bytes. Comparing to the
		// original data is wrong under Postel: boolean byte 0x30
		// decodes to true and encodes to 0x01.
		var tagged2 any
		if _, err := s.Decode(encoded1, &tagged2, TaggedUnions()); err != nil {
			t.Fatalf("re-decode of canonical encoded failed: %v\n  encoded1: %x", err, encoded1)
		}
		encoded2, err := s.Encode(tagged2)
		if err != nil {
			t.Fatalf("re-encode of canonical value failed: %v", err)
		}
		// Value-level fixpoint, NOT byte-level. Avro map encoding iterates Go
		// map keys in randomized, spec-legal order, so a multi-key map
		// re-encodes to a different byte ordering even though the value is
		// identical. fuzzEqual is order-robust (maps) and NaN-robust (a
		// decoded float NaN round-trips to an identical NaN that
		// reflect.DeepEqual would wrongly call unequal).
		if !fuzzEqual(tagged1, tagged2) {
			t.Fatalf("decode∘encode is not a value fixpoint:\n  tagged1: %#v\n  tagged2: %#v\n  encoded1: %x\n  encoded2: %x", tagged1, tagged2, encoded1, encoded2)
		}
	})
}

// FuzzDecodeJSONTyped decodes random JSON into typed Go targets.
func FuzzDecodeJSONTyped(f *testing.F) {
	type Record struct {
		A int32   `avro:"a"`
		B string  `avro:"b"`
		C bool    `avro:"c"`
		D float64 `avro:"d"`
	}
	recordSchema := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"},
		{"name":"c","type":"boolean"},
		{"name":"d","type":"double"}
	]}`)

	f.Add(`{"a":1,"b":"x","c":true,"d":3.14}`)
	f.Add(`{"a":0,"b":"","c":false,"d":0}`)
	f.Add(`{}`)
	f.Add(`{"a":"wrong"}`)
	f.Add(`not json`)
	f.Add(`{"a":1,"b":"x","c":true,"d":3.14,"extra":{"nested":true}}`)

	f.Fuzz(func(t *testing.T, input string) {
		var r Record
		recordSchema.DecodeJSON([]byte(input), &r)
	})
}

// FuzzDecodeTyped decodes random bytes into typed Go targets, exercising
// the unsafe fast path and fixed-size array decoding.
func FuzzDecodeTyped(f *testing.F) {
	type Record struct {
		A int32   `avro:"a"`
		B string  `avro:"b"`
		C bool    `avro:"c"`
		D float64 `avro:"d"`
	}
	recordSchema := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"},
		{"name":"c","type":"boolean"},
		{"name":"d","type":"double"}
	]}`)

	arraySchema := MustParse(`{"type":"array","items":"int"}`)

	// Seeds: valid encodings.
	f.Add(uint8(0), fuzzSeed(recordSchema, &Record{A: 1, B: "x", C: true, D: 3.14}))
	f.Add(uint8(1), fuzzSeed(arraySchema, []int32{1, 2, 3}))
	f.Add(uint8(0), []byte{})
	f.Add(uint8(1), []byte{})
	f.Add(uint8(0), bytes.Repeat([]byte{0xFF}, 32))
	f.Add(uint8(1), bytes.Repeat([]byte{0xFF}, 32))

	f.Fuzz(func(t *testing.T, mode uint8, data []byte) {
		switch mode % 3 {
		case 0:
			var r Record
			recordSchema.Decode(data, &r)
		case 1:
			var sl []int32
			arraySchema.Decode(data, &sl)
		case 2:
			var arr [4]int32
			arraySchema.Decode(data, &arr)
		}
	})
}

// FuzzEncodeMap exercises encoding from map[string]any with defaults,
// timestamp strings, json.Number, and decimal coercion.
func FuzzEncodeMap(f *testing.F) {
	schema := MustParse(`{
		"type":"record","name":"R",
		"fields":[
			{"name":"a","type":"int","default":0},
			{"name":"b","type":"string","default":""},
			{"name":"c","type":{"type":"long","logicalType":"timestamp-millis"},"default":0},
			{"name":"d","type":"double","default":0}
		]
	}`)

	f.Add(`{}`)
	f.Add(`{"a":42}`)
	f.Add(`{"a":1,"b":"hello","c":"2026-03-19T10:00:00Z","d":3.14}`)
	f.Add(`{"a":1,"b":"hello","c":1742385600000,"d":3.14}`)
	f.Add(`{"a":1,"b":"hello","c":"not-a-timestamp","d":3.14}`)
	f.Add(`{"extra":"ignored","a":1,"b":"x","c":0,"d":0}`)
	f.Add(`not json`)

	f.Fuzz(func(t *testing.T, input string) {
		var m any
		if err := json.Unmarshal([]byte(input), &m); err != nil {
			return
		}
		schema.Encode(m)
	})
}

// FuzzSchemaNode exercises [SchemaNode.Schema] by feeding random JSON
// through Root(), a mutation, then Schema(). This is the closest we can
// get to fuzzing programmatic construction without hand-rolling a
// SchemaNode generator. Exercises toJSONDedup, cycle detection, named
// type dedup, and implicit null default wiring.
func FuzzSchemaNode(f *testing.F) {
	seeds := []string{
		`"int"`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
		// Empty record. The spec requires "fields":[] but twmb/avro's
		// parser is lenient and accepts the missing attribute too; both
		// variants must round-trip identically through Canonical().
		`{"type":"record","name":"Empty","fields":[]}`,
		`{"type":"record","name":"Empty"}`,
		// Nested empty records at various positions.
		`{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"I","fields":[]}}]}`,
		`{"type":"array","items":{"type":"record","name":"E","fields":[]}}`,
		`{"type":"map","values":{"type":"record","name":"E","fields":[]}}`,
		`["null",{"type":"record","name":"E","fields":[]}]`,
		`{"type":"record","name":"R","fields":[
			{"name":"a","type":{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}},
			{"name":"b","type":"U"}
		]}`,
		`{"type":"record","name":"R","fields":[
			{"name":"a","type":["null","int"]},
			{"name":"b","type":["null","string"]}
		]}`,
		`{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}}`,
		`{"type":"map","values":{"type":"enum","name":"E","symbols":["A","B"]}}`,
		`["null","int","string",{"type":"fixed","name":"F","size":4}]`,
		// Recursive linked list via self-reference.
		nodeRecursiveSchema,
		// 3-level nested records.
		`{"type":"record","name":"L1","fields":[
			{"name":"l2","type":{"type":"record","name":"L2","fields":[
				{"name":"l3","type":{"type":"record","name":"L3","fields":[
					{"name":"x","type":"int"}
				]}}
			]}}
		]}`,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		s, err := Parse(input)
		if err != nil {
			return
		}
		root := s.Root()
		// Round-trip: Root().Schema() must succeed for any schema Parse
		// accepted, and produce the same canonical form.
		s2, err := root.Schema()
		if err != nil {
			t.Fatalf("Root().Schema() failed for valid schema %q: %v", input, err)
		}
		if !bytes.Equal(s.Canonical(), s2.Canonical()) {
			t.Fatalf("canonical form changed through Root()/Schema() round-trip:\n  orig: %s\n  new:  %s",
				s.Canonical(), s2.Canonical())
		}
	})
}

// FuzzEncodeMapMissingKeys exercises the implicit null default path by
// encoding random map subsets of a record with nullable fields.
func FuzzEncodeMapMissingKeys(f *testing.F) {
	schema := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":["null","int"]},
		{"name":"c","type":["null","string"]},
		{"name":"d","type":"string","default":"hi"}
	]}`)

	// Seeds: various combinations of present/missing keys.
	seeds := []string{
		`{"a":1,"b":2,"c":"x","d":"y"}`,
		`{"a":1}`,                   // b, c, d all missing
		`{"a":1,"b":5}`,             // c, d missing
		`{"a":1,"c":"only c"}`,      // b, d missing
		`{"a":1,"b":null,"c":null}`, // explicit nulls
		`{"b":1,"c":"x"}`,           // missing required 'a'
		`{"a":"wrong type"}`,        // wrong type
		`{"a":1,"extra":"ignored"}`, // extra key
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		var m any
		if err := json.Unmarshal([]byte(input), &m); err != nil {
			return
		}
		mm, ok := m.(map[string]any)
		if !ok {
			return
		}
		// Coerce float64 (from json.Unmarshal) to int32 for field "a".
		if v, ok := mm["a"]; ok {
			if f, ok := v.(float64); ok {
				mm["a"] = int32(f)
			}
		}
		if v, ok := mm["b"]; ok {
			if f, ok := v.(float64); ok {
				mm["b"] = int32(f)
			}
		}
		schema.Encode(mm)
	})
}

func FuzzResolve(f *testing.F) {
	type seed struct {
		reader string
		writer string
		data   []byte
	}

	// Identity: same record schema.
	recSchema := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`
	writerS := MustParse(recSchema)
	identityData := fuzzSeed(writerS, map[string]any{"a": int32(1), "b": "x"})

	// Field addition with default.
	writerAdd := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`
	readerAdd := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string","default":"hi"}]}`
	writerAddS := MustParse(writerAdd)
	addData := fuzzSeed(writerAddS, map[string]any{"a": int32(7)})

	// Type promotion: int -> long.
	writerProm := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`
	readerProm := `{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`
	writerPromS := MustParse(writerProm)
	promData := fuzzSeed(writerPromS, map[string]any{"a": int32(100)})

	// Field removal.
	writerRem := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`
	readerRem := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`
	writerRemS := MustParse(writerRem)
	remData := fuzzSeed(writerRemS, map[string]any{"a": int32(3), "b": "drop"})

	// Incompatible: int vs string.
	writerIncompat := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`
	readerIncompat := `{"type":"record","name":"R","fields":[{"name":"a","type":"string"}]}`

	seeds := []seed{
		{recSchema, recSchema, identityData},
		{readerAdd, writerAdd, addData},
		{readerProm, writerProm, promData},
		{readerRem, writerRem, remData},
		{readerIncompat, writerIncompat, nil},
		// Primitives.
		{`"int"`, `"int"`, fuzzSeed(MustParse(`"int"`), int32(42))},

		// Enum: writer adds new symbol, reader has default.
		{
			`{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`,
			`{"type":"enum","name":"E","symbols":["A","B","C"]}`,
			fuzzSeed(MustParse(`{"type":"enum","name":"E","symbols":["A","B","C"]}`), "C"),
		},
		// Array: item type promotion int -> long.
		{
			`{"type":"array","items":"long"}`,
			`{"type":"array","items":"int"}`,
			fuzzSeed(MustParse(`{"type":"array","items":"int"}`), []int32{1, 2, 3}),
		},
		// Map: value type promotion int -> long.
		{
			`{"type":"map","values":"long"}`,
			`{"type":"map","values":"int"}`,
			fuzzSeed(MustParse(`{"type":"map","values":"int"}`), map[string]int32{"k": 10}),
		},
		// Union: writer has subset of reader branches.
		{
			`["null","int","string"]`,
			`["null","int"]`,
			fuzzSeed(MustParse(`["null","int"]`), int32(7)),
		},
		// Primitive promotions.
		{`"float"`, `"int"`, fuzzSeed(MustParse(`"int"`), int32(5))},
		{`"double"`, `"long"`, fuzzSeed(MustParse(`"long"`), int64(99))},
		{`"long"`, `"int"`, fuzzSeed(MustParse(`"int"`), int32(42))},
		{`"double"`, `"float"`, fuzzSeed(MustParse(`"float"`), float32(1.5))},
	}

	for _, s := range seeds {
		f.Add(s.reader, s.writer, s.data)
	}

	f.Fuzz(func(t *testing.T, readerJSON, writerJSON string, data []byte) {
		reader, err := Parse(readerJSON)
		if err != nil {
			return
		}
		writer, err := Parse(writerJSON)
		if err != nil {
			return
		}
		resolved, err := Resolve(writer, reader)
		if err != nil {
			return
		}
		var v any
		resolved.Decode(data, &v)
	})
}

// FuzzDecodeVariedTargets fuzzes binary decode against many target shapes, not
// just *any. The pre-existing FuzzDecode used only `var v any`. It missed panics
// when decoding into *interface{Foo()} / *error, into a struct with non-empty-
// interface fields, and on re-decode into a populated *any, where the inner
// unwraps to an unaddressable Value. The driver's `mode` byte selects the target
// shape and data bytes are the wire input. No panic is ever expected, and every
// combination must surface as a returned error.
func FuzzDecodeVariedTargets(f *testing.F) {
	type IfaceField struct {
		X interface{ Foo() } `avro:"x"`
	}
	type ErrorField struct {
		X error `avro:"x"`
	}

	makeTarget := func(mode uint8) any {
		switch mode % 12 {
		case 0:
			var v any
			return &v
		case 1:
			var v interface{ Foo() }
			return &v
		case 2:
			var v error
			return &v
		case 3:
			var v map[string]any
			return &v
		case 4:
			var v []any
			return &v
		case 5:
			var v IfaceField
			return &v
		case 6:
			var v ErrorField
			return &v
		case 7:
			var v int32
			return &v
		case 8:
			var v *int32
			return &v
		case 9:
			var v string
			return &v
		case 10:
			// Pre-populated *any so the inner-Value path runs.
			v := any(int32(99))
			return &v
		case 11:
			// Pre-populated *any holding a slice, not a map, which
			// exercises the unwrap-only-Map rule.
			v := any([]any{int32(1)})
			return &v
		}
		var v any
		return &v
	}

	// Seed every (schema, target_kind) pair with a valid binary encoding
	// plus an empty buffer.
	for i := range fuzzSchemas {
		for mode := range uint8(12) {
			f.Add(uint8(i), mode, []byte{})
			f.Add(uint8(i), mode, []byte{0})
			f.Add(uint8(i), mode, []byte{2, 'x'})
		}
	}

	f.Fuzz(func(t *testing.T, schemaIdx, mode uint8, data []byte) {
		s := fuzzSchemas[int(schemaIdx)%len(fuzzSchemas)]
		tgt := makeTarget(mode)
		// Decode in either tagged or untagged mode based on a low bit of
		// mode; every option combo must be panic-free.
		if mode&0x80 != 0 {
			s.Decode(data, tgt, TaggedUnions())
		} else {
			s.Decode(data, tgt)
		}
	})
}

// FuzzDecodeJSONVariedTargets is the JSON-decode counterpart to
// FuzzDecodeVariedTargets.
func FuzzDecodeJSONVariedTargets(f *testing.F) {
	type IfaceField struct {
		X interface{ Foo() } `avro:"x"`
	}

	makeTarget := func(mode uint8) any {
		switch mode % 9 {
		case 0:
			var v any
			return &v
		case 1:
			var v interface{ Foo() }
			return &v
		case 2:
			var v error
			return &v
		case 3:
			var v map[string]any
			return &v
		case 4:
			var v IfaceField
			return &v
		case 5:
			var v int32
			return &v
		case 6:
			var v string
			return &v
		case 7:
			v := any(int32(0))
			return &v
		case 8:
			v := any(map[string]any{})
			return &v
		}
		var v any
		return &v
	}

	for i := range fuzzSchemas {
		for mode := range uint8(9) {
			for _, src := range []string{
				`null`, `42`, `"x"`, `true`, `[]`, `{}`,
				`{"int":1}`, `{"null":null}`, `{"x":1}`,
			} {
				f.Add(uint8(i), mode, src)
			}
		}
	}

	f.Fuzz(func(t *testing.T, schemaIdx, mode uint8, src string) {
		s := fuzzSchemas[int(schemaIdx)%len(fuzzSchemas)]
		tgt := makeTarget(mode)
		if mode&0x80 != 0 {
			s.DecodeJSON([]byte(src), tgt, TaggedUnions())
		} else {
			s.DecodeJSON([]byte(src), tgt)
		}
	})
}

// FuzzDecodeReuse repeatedly decodes into the same *any target.
// This is the common streaming pattern (OCF reader, batch consumer) and
// the pre-existing fuzzers all created a fresh target per iteration.
// That blind spot hid the indirectAlloc panic where the *any's inner
// becomes unaddressable on the second decode.
func FuzzDecodeReuse(f *testing.F) {
	for i := range fuzzSchemas {
		f.Add(uint8(i), []byte{0}, []byte{0})
		f.Add(uint8(i), []byte{}, []byte{})
	}

	f.Fuzz(func(t *testing.T, schemaIdx uint8, data1, data2 []byte) {
		s := fuzzSchemas[int(schemaIdx)%len(fuzzSchemas)]
		var v any
		s.Decode(data1, &v)
		// Second call into the same target, where the bug manifests.
		s.Decode(data2, &v)
		// Third for good measure.
		s.Decode(data1, &v)
	})
}

// FuzzDecodeJSONReuse: JSON counterpart to FuzzDecodeReuse.
func FuzzDecodeJSONReuse(f *testing.F) {
	for i := range fuzzSchemas {
		f.Add(uint8(i), `null`, `null`)
		f.Add(uint8(i), `42`, `43`)
		f.Add(uint8(i), `{}`, `{}`)
	}

	f.Fuzz(func(t *testing.T, schemaIdx uint8, src1, src2 string) {
		s := fuzzSchemas[int(schemaIdx)%len(fuzzSchemas)]
		var v any
		s.DecodeJSON([]byte(src1), &v)
		s.DecodeJSON([]byte(src2), &v)
	})
}

// FuzzEncodeHostile fuzzes the encoder with values that mix nils, weird
// types, and tagged-union maps with bogus branch keys against every
// schema. None should panic; each must return an error.
func FuzzEncodeHostile(f *testing.F) {
	type S struct {
		X any            `avro:"x"`
		A []any          `avro:"a"`
		M map[string]any `avro:"m"`
	}

	makeValue := func(mode uint8) any {
		switch mode % 16 {
		case 0:
			return nil
		case 1:
			return any(nil)
		case 2:
			return (*int)(nil)
		case 3:
			return map[string]any{"x": nil}
		case 4:
			return []any{nil, int32(1), nil}
		case 5:
			return map[string]any{"int": nil}
		case 6:
			return map[string]any{"null": nil}
		case 7:
			return map[string]any{"unknown_branch": int32(1)}
		case 8:
			return map[string]any{"x": []any{nil}}
		case 9:
			return map[string]any{"x": map[string]any{"k": nil}}
		case 10:
			return S{X: nil, A: []any{nil}, M: map[string]any{"k": nil}}
		case 11:
			return map[int]int{1: 1} // non-string-keyed map
		case 12:
			return map[any]any{1: 1}
		case 13:
			return json.Number("garbage")
		case 14:
			return map[string]any{"x": json.Number("not-a-number")}
		case 15:
			return []any{int32(1), "string", nil, true, 3.14}
		}
		return nil
	}

	for i := range fuzzSchemas {
		for mode := range uint8(16) {
			f.Add(uint8(i), mode)
		}
	}

	f.Fuzz(func(t *testing.T, schemaIdx, mode uint8) {
		s := fuzzSchemas[int(schemaIdx)%len(fuzzSchemas)]
		v := makeValue(mode)
		// Both binary and JSON, both option modes.
		s.AppendEncode(nil, v)
		s.AppendEncode(nil, v, TaggedUnions())
		s.AppendEncodeJSON(nil, v)
		s.AppendEncodeJSON(nil, v, TaggedUnions())
	})
}

// FuzzResolveBroad fuzzes Resolve across many reader/writer pairs.
// FuzzResolve already exists but its seed corpus is narrow. This one
// pairs every fuzzSchemas entry with every other entry to surface
// resolution edge cases (alias mismatches, recursive cycle handling,
// promote chain panics).
func FuzzResolveBroad(f *testing.F) {
	for i := range fuzzSchemas {
		for j := range fuzzSchemas {
			f.Add(uint8(i), uint8(j), []byte{})
			f.Add(uint8(i), uint8(j), []byte{0})
			f.Add(uint8(i), uint8(j), []byte{2, 84})
		}
	}
	f.Fuzz(func(t *testing.T, wIdx, rIdx uint8, data []byte) {
		w := fuzzSchemas[int(wIdx)%len(fuzzSchemas)]
		r := fuzzSchemas[int(rIdx)%len(fuzzSchemas)]
		res, err := Resolve(w, r)
		if err != nil {
			return
		}
		var v any
		res.Decode(data, &v)
		// Vary target shape too.
		var v2 interface{ Foo() }
		res.Decode(data, &v2)
	})
}

// FuzzCustomTypeRoundTrip wires up a custom type and exercises
// encode/decode round-trip with arbitrary value bytes. The custom-type
// path goes through wrapDeserWithCustomDecoders / setCustomResult,
// which had a panic earlier; this fuzz keeps that path under coverage.
func FuzzCustomTypeRoundTrip(f *testing.F) {
	type Wrapped struct{ V int }
	ct := NewCustomType[Wrapped, int32](
		"",
		func(w Wrapped, _ *SchemaNode) (int32, error) { return int32(w.V), nil },
		func(v int32, _ *SchemaNode) (Wrapped, error) { return Wrapped{V: int(v)}, nil },
	)
	s := mustParse(f, `"int"`, WithCustomType(ct))

	f.Add(int32(0))
	f.Add(int32(-1))
	f.Add(int32(1 << 30))
	f.Add(int32(-1 << 30))

	f.Fuzz(func(t *testing.T, val int32) {
		w := Wrapped{V: int(val)}
		encoded, err := s.AppendEncode(nil, w)
		if err != nil {
			return
		}
		var got Wrapped
		if _, err := s.Decode(encoded, &got); err != nil {
			t.Fatalf("decode after encode failed: %v\n  data: %x", err, encoded)
		}
		if got.V != w.V {
			t.Fatalf("custom type round-trip mismatch: got %v, want %v", got, w)
		}
		// And decode-into-interface variants.
		var anyV any
		if _, err := s.Decode(encoded, &anyV); err != nil {
			t.Fatalf("decode into *any failed: %v", err)
		}
	})
}

// FuzzConcurrentEncodeDecode hammers a shared *Schema from multiple
// goroutines with arbitrary inputs. The unsafe fast-path init uses
// atomic.Pointer; the per-type cache uses sync.Map. Concurrent
// fuzz exercise stresses these paths in a way single-threaded fuzz
// can't.
func FuzzConcurrentEncodeDecode(f *testing.F) {
	type Record struct {
		A int32  `avro:"a"`
		B string `avro:"b"`
	}
	s := mustParse(f, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`)

	f.Add(int32(1), "x", uint8(4))
	f.Add(int32(0), "", uint8(8))
	f.Add(int32(-1), "ababab", uint8(2))

	f.Fuzz(func(t *testing.T, a int32, b string, n uint8) {
		// The concurrency surface is shared-schema state across goroutines,
		// not payload size. So cap the fuzz-grown string: a corpus-mutated
		// multi-megabyte b would otherwise turn one execution into seconds
		// of memcpy across workers×iterations. That showed up as the
		// fuzzer's exec counter freezing for whole intervals, risking the
		// -fuzztime shutdown deadline.
		if len(b) > 1024 {
			b = b[:1024]
		}
		workers := 1 + int(n%8)
		// Collect panics from worker goroutines via channel rather than
		// calling t.Errorf directly: testing.T methods other than Log
		// aren't safe for concurrent use from non-test goroutines.
		panicCh := make(chan any, workers)
		done := make(chan struct{}, workers)
		for range workers {
			go func() {
				defer func() {
					if r := recover(); r != nil {
						panicCh <- r
					}
					done <- struct{}{}
				}()
				for j := range 20 {
					rec := Record{A: a + int32(j), B: b}
					data, err := s.AppendEncode(nil, &rec)
					if err != nil {
						continue
					}
					var got Record
					s.Decode(data, &got)
					var anyV any
					s.Decode(data, &anyV)
				}
			}()
		}
		for range workers {
			<-done
		}
		close(panicCh)
		for p := range panicCh {
			t.Errorf("panic in concurrent worker: %v", p)
		}
	})
}

// FuzzTimeDateEdgeCases fuzzes time/date logical types around boundary
// values: pre-epoch, far future, leap seconds, NaN/Infinity for floats
// in time-millis representations.
func FuzzTimeDateEdgeCases(f *testing.F) {
	schemas := []string{
		`{"type":"long","logicalType":"timestamp-millis"}`,
		`{"type":"long","logicalType":"timestamp-micros"}`,
		`{"type":"long","logicalType":"timestamp-nanos"}`,
		`{"type":"int","logicalType":"date"}`,
		`{"type":"int","logicalType":"time-millis"}`,
		`{"type":"long","logicalType":"time-micros"}`,
	}
	parsed := make([]*Schema, len(schemas))
	for i, s := range schemas {
		p := mustParse(f, s)
		parsed[i] = p
	}

	// Adversarial values.
	f.Add(uint8(0), int64(0))
	f.Add(uint8(0), int64(-62135596800000)) // 0001-01-01
	f.Add(uint8(0), int64(253402300800000)) // 9999 AD
	f.Add(uint8(0), int64(1<<62))           // overflow risk
	f.Add(uint8(0), int64(-1<<62))
	f.Add(uint8(2), int64(1))
	f.Add(uint8(3), int64(0))  // epoch
	f.Add(uint8(3), int64(-1)) // pre-epoch date
	f.Add(uint8(4), int64(0))  // midnight
	f.Add(uint8(4), int64(86400000))
	f.Add(uint8(5), int64(86400000000))

	// Track which schemas use "int" wire type vs "long" so the
	// fuzz body can pick the matching Go type without inspecting
	// the *Schema (no public accessor for the underlying kind).
	isInt := []bool{false, false, false, true, true, false}

	f.Fuzz(func(t *testing.T, schemaIdx uint8, val int64) {
		idx := int(schemaIdx) % len(parsed)
		s := parsed[idx]
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic on schema=%s val=%d: %v", s.String(), val, r)
			}
		}()
		var encoded []byte
		var err error
		if isInt[idx] {
			if val < -1<<31 || val > 1<<31-1 {
				return
			}
			encoded, err = s.AppendEncode(nil, int32(val))
		} else {
			encoded, err = s.AppendEncode(nil, val)
		}
		if err != nil {
			return
		}
		var v any
		s.Decode(encoded, &v)
		jsonEncoded, err := s.EncodeJSON(v)
		if err != nil {
			return
		}
		var v2 any
		s.DecodeJSON(jsonEncoded, &v2)
	})
}

// FuzzDepthBounds drives every encode/decode/skip/parse path with
// pathologically deep or cyclic inputs and asserts the library
// terminates with an error rather than panicking, hanging, or
// stack-overflowing. Specifically targets the depth-bound work in
// commits a302d51 and 21006ca: cyclic Go inputs, deeply nested wire
// data, deeply nested schemas, self-referential interfaces.
func FuzzDepthBounds(f *testing.F) {
	// nesting: how many record levels deep to recurse (binary input).
	// arrayCount: how many array blocks to chain (each with one item).
	// schemaDepth: nesting depth for the auto-generated array<array<...>> schema.
	// mode: which subtest to run (0..7).
	f.Add(uint16(2000), uint16(100), uint16(50), uint8(0))
	f.Add(uint16(5000), uint16(500), uint16(2000), uint8(1))
	f.Add(uint16(100), uint16(10), uint16(maxDepth+10), uint8(2))
	f.Add(uint16(50), uint16(50), uint16(10), uint8(3))
	f.Add(uint16(0), uint16(0), uint16(0), uint8(4))
	f.Add(uint16(maxDepth+50), uint16(0), uint16(0), uint8(5))
	f.Add(uint16(10), uint16(maxDepth+50), uint16(0), uint8(6))
	f.Add(uint16(0), uint16(0), uint16(0), uint8(7))

	recursiveSchema := nodeRecursiveSchema
	rs := mustParse(f, recursiveSchema)
	type node struct {
		Value int32 `avro:"value"`
		Next  *node `avro:"next"`
	}
	// Input-independent schemas and resolutions are built once here.
	// Re-parsing and re-resolving them per execution added constant fixture
	// cost to every iteration without exercising anything the first iteration
	// didn't: the per-exec-cost class that starves fuzz workers into missing
	// the coordinator's -fuzztime shutdown deadline.
	resolvedSame := mustResolve(f, rs, rs)
	rdrSchema := mustParse(f, `{"type":"record","name":"Node","fields":[{"name":"value","type":"int"}]}`)
	resolvedDrop := mustResolve(f, rs, rdrSchema)
	arrSchema := mustParse(f, `{"type":"array","items":"int"}`)
	intS := mustParse(f, `"int"`)
	nullableS := mustParse(f, `["null","int"]`)

	f.Fuzz(func(t *testing.T, nesting, arrayCount, schemaDepth uint16, mode uint8) {
		// Hard caps to keep individual fuzz iterations bounded. The depth
		// guard trips at maxDepth, so nesting past maxDepth+margin buys no
		// new coverage. It only linearly burns time building and walking
		// input. At a 20000 cap a single execution averaged tens of
		// milliseconds, sliding the exec rate low enough that a worker
		// could miss the -fuzztime shutdown deadline. The schemaDepth cap
		// is tight because encoding/json's recursive parser is O(N²) on
		// nested-array JSON, so well past maxDepth we only burn time in the
		// stdlib without exercising more of our code.
		if nesting > maxDepth+200 {
			nesting = maxDepth + 200
		}
		if arrayCount > 2000 {
			arrayCount = 2000
		}
		if schemaDepth > maxDepth+10 {
			schemaDepth = maxDepth + 10
		}
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panicked (mode=%d, n=%d, a=%d, sd=%d): %v",
					mode, nesting, arrayCount, schemaDepth, r)
			}
		}()

		switch mode % 8 {
		case 0:
			// Deeply nested binary into recursive struct.
			var src []byte
			for range int(nesting) {
				src = append(src, 0, 0x02)
			}
			src = append(src, 0)
			var n node
			rs.Decode(src, &n)
		case 1:
			// Deeply nested binary into resolved decode (writer == reader).
			var src []byte
			for range int(nesting) {
				src = append(src, 0, 0x02)
			}
			src = append(src, 0)
			var n node
			resolvedSame.Decode(src, &n)
		case 2:
			// Deeply nested binary skipped via resolve (reader drops "next").
			var src []byte
			for range int(nesting) {
				src = append(src, 0, 0x02)
			}
			src = append(src, 0)
			type rR struct {
				Value int32 `avro:"value"`
			}
			var rv rR
			resolvedDrop.Decode(src, &rv)
		case 3:
			// Deeply nested JSON into recursive struct.
			var src []byte
			for range int(nesting) {
				src = append(src, []byte(`{"value":0,"next":{"Node":`)...)
			}
			src = append(src, []byte(`{"value":0,"next":null}`)...)
			for range int(nesting) {
				src = append(src, []byte(`}}`)...)
			}
			var n node
			rs.DecodeJSON(src, &n)
		case 4:
			// Cyclic struct encode (binary + JSON) through unsafe fast path.
			n := &node{Value: 1}
			n.Next = n
			rs.AppendEncode(nil, n)
			rs.AppendEncodeJSON(nil, n)
		case 5:
			// Schema-parse depth bound.
			var b strings.Builder
			d := int(schemaDepth)
			if d == 0 {
				d = maxDepth + 50
			}
			for range d {
				b.WriteString(`{"type":"array","items":`)
			}
			b.WriteString(`"int"`)
			for range d {
				b.WriteString(`}`)
			}
			Parse(b.String())
		case 6:
			// Long array-block chain (count > buffer is rejected; this
			// makes many small blocks each terminating with count=0).
			var src []byte
			for range int(arrayCount) {
				src = append(src, 0x02) // count=1
				src = append(src, 0)    // single item: int(0)
			}
			src = append(src, 0) // terminator
			var out []int32
			arrSchema.Decode(src, &out)
		case 7:
			// Self-referential `any` against various schemas.
			var p any
			p = &p
			intS.AppendEncode(nil, p)
			intS.AppendEncodeJSON(nil, p)
			nullableS.AppendEncode(nil, p)
			nullableS.AppendEncodeJSON(nil, p)
		}
	})
}

// fuzzPromoteLogicalPairs enumerates the (writer wire kind, reader logical-typed
// schema) cells that promotionDeserForLogical wraps. The driver picks one by
// index, encodes arbitrary input against the writer, then resolves
// writer-to-reader and decodes into several Go target shapes. That locks the
// int-to-long+timestamp-*, int-to-long+time-micros, string-to-bytes+decimal,
// string-to-bytes+big-decimal and bytes-to-string+uuid paths under fuzz input,
// where the regression tests pin specific values. Without the wrap the decode
// produces the raw wire type instead of the logical-typed result.
var fuzzPromoteLogicalPairs = []struct {
	writer    string
	reader    string
	encodeInt bool // writer is "int" (encode int32) vs string/bytes (encode []byte)
}{
	{`"int"`, `{"type":"long","logicalType":"timestamp-millis"}`, true},
	{`"int"`, `{"type":"long","logicalType":"timestamp-micros"}`, true},
	{`"int"`, `{"type":"long","logicalType":"timestamp-nanos"}`, true},
	{`"int"`, `{"type":"long","logicalType":"local-timestamp-millis"}`, true},
	{`"int"`, `{"type":"long","logicalType":"local-timestamp-micros"}`, true},
	{`"int"`, `{"type":"long","logicalType":"local-timestamp-nanos"}`, true},
	{`"int"`, `{"type":"long","logicalType":"time-micros"}`, true},
	{`"string"`, `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, false},
	{`"string"`, `{"type":"bytes","logicalType":"big-decimal"}`, false},
	{`"bytes"`, `{"type":"string","logicalType":"uuid"}`, false},
}

// fuzzPromoteLogicalNesting wraps a primitive (writer, reader) pair in
// each container the resolver dispatches through: top-level, record
// field, array items, map values, and reader-side union branch. The
// logical-conversion wrap must apply uniformly across every nesting,
// so the fuzz covers each container axis.
func fuzzPromoteLogicalNesting(writer, reader string, nesting uint8) (string, string) {
	switch nesting % 5 {
	case 0:
		return writer, reader
	case 1:
		return `{"type":"record","name":"R","fields":[{"name":"x","type":` + writer + `}]}`,
			`{"type":"record","name":"R","fields":[{"name":"x","type":` + reader + `}]}`
	case 2:
		return `{"type":"array","items":` + writer + `}`,
			`{"type":"array","items":` + reader + `}`
	case 3:
		return `{"type":"map","values":` + writer + `}`,
			`{"type":"map","values":` + reader + `}`
	case 4:
		return writer, `["null",` + reader + `]`
	}
	return writer, reader
}

func FuzzPromoteLogical(f *testing.F) {
	// Seeds: one canonical per pair × nesting combo, plus a couple of
	// adversarial wire payloads (varint overflow, length > buffer).
	for idx := uint8(0); idx < uint8(len(fuzzPromoteLogicalPairs)); idx++ {
		for n := range uint8(5) {
			pair := fuzzPromoteLogicalPairs[idx]
			w, _ := fuzzPromoteLogicalNesting(pair.writer, pair.reader, n)
			ws, err := Parse(w)
			if err != nil {
				continue
			}
			var v any
			switch {
			case strings.HasPrefix(w, `"int"`):
				v = int32(1742385600)
			case strings.HasPrefix(w, `"string"`):
				v = "12.34"
			case strings.HasPrefix(w, `"bytes"`):
				v = []byte("550e8400-e29b-41d4-a716-446655440000")
			case strings.Contains(w, `"type":"record"`):
				v = map[string]any{"x": canonicalInputFor(pair)}
			case strings.Contains(w, `"type":"array"`):
				v = []any{canonicalInputFor(pair)}
			case strings.Contains(w, `"type":"map"`):
				v = map[string]any{"k": canonicalInputFor(pair)}
			}
			if v == nil {
				continue
			}
			data, err := ws.AppendEncode(nil, v)
			if err != nil {
				continue
			}
			f.Add(idx, n, data)
		}
	}
	// Adversarial inputs: empty, single byte, varint overflow.
	f.Add(uint8(0), uint8(0), []byte{})
	f.Add(uint8(0), uint8(0), []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01})
	f.Add(uint8(7), uint8(0), []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01})

	f.Fuzz(func(t *testing.T, pairIdx, nestIdx uint8, data []byte) {
		pair := fuzzPromoteLogicalPairs[int(pairIdx)%len(fuzzPromoteLogicalPairs)]
		w, r := fuzzPromoteLogicalNesting(pair.writer, pair.reader, nestIdx)
		writer, err := Parse(w)
		if err != nil {
			return
		}
		reader, err := Parse(r)
		if err != nil {
			return
		}
		resolved, err := Resolve(writer, reader)
		if err != nil {
			return
		}
		// Decode against multiple target shapes: *any (the natural
		// promotion target), a typed container that should accept the
		// logical-typed value, and a deliberately-wrong type that should
		// error rather than panic.
		var anyV any
		resolved.Decode(data, &anyV)
		// Typed container: a time.Time field for the timestamp cells,
		// *big.Rat for decimal cells, [16]byte for uuid. The fuzz only
		// cares that this never panics; mismatched promotions produce
		// errors but those are fine.
		switch nestIdx % 5 {
		case 1: // record
			switch pair.reader {
			case fuzzPromoteLogicalPairs[0].reader, fuzzPromoteLogicalPairs[1].reader, fuzzPromoteLogicalPairs[2].reader:
				var typed struct {
					X time.Time `avro:"x"`
				}
				resolved.Decode(data, &typed)
			case fuzzPromoteLogicalPairs[7].reader, fuzzPromoteLogicalPairs[8].reader:
				var typed struct {
					X *big.Rat `avro:"x"`
				}
				resolved.Decode(data, &typed)
			case fuzzPromoteLogicalPairs[9].reader:
				var typed struct {
					X [16]byte `avro:"x"`
				}
				resolved.Decode(data, &typed)
			}
		case 0: // top-level scalar
			var typedTime time.Time
			resolved.Decode(data, &typedTime)
			var typedRat *big.Rat
			resolved.Decode(data, &typedRat)
			var typedUUID [16]byte
			resolved.Decode(data, &typedUUID)
		}
	})
}

func canonicalInputFor(p struct {
	writer    string
	reader    string
	encodeInt bool
}) any {
	if p.encodeInt {
		return int32(1742385600)
	}
	if strings.HasPrefix(p.writer, `"bytes"`) {
		return []byte("550e8400-e29b-41d4-a716-446655440000")
	}
	return "12.34"
}

// FuzzBareSpecialFloat exercises the JSON decoder's bare-token path
// for NaN/Infinity/-Infinity (the unquoted form fastavro and
// python's json.dumps(..., allow_nan=True) emit). consumeBareSpecial-
// Float is reached only when the decoder hits a non-quote/-digit/-null
// token at a float/double position. The existing FuzzDecodeJSON seeds
// only have the quoted form. Coverage includes top-level + nested
// (record field, array element, map value, union branch) so the
// recursive descent's "peek a non-quote at a float position" arm is
// hit from every context.
func FuzzBareSpecialFloat(f *testing.F) {
	floatSchema := MustParse(`"float"`)
	doubleSchema := MustParse(`"double"`)
	recordSchema := MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":"float"},{"name":"d","type":"double"}]}`)
	arrayFloat := MustParse(`{"type":"array","items":"float"}`)
	mapDouble := MustParse(`{"type":"map","values":"double"}`)
	unionFloat := MustParse(`["null","float"]`)
	unionDouble := MustParse(`["null","double"]`)

	schemas := []*Schema{floatSchema, doubleSchema, recordSchema, arrayFloat, mapDouble, unionFloat, unionDouble}

	// Tokens: every casing + sign + word the lenient path must accept,
	// plus things that look almost-right but should error cleanly.
	tokens := []string{
		"NaN", "nan", "NAN", "Nan",
		"Infinity", "infinity", "INFINITY", "Inf", "inf",
		"-Infinity", "-infinity", "-Inf", "-inf",
		"+Infinity", "Inf inity", "InfX", "nul", "-",
		"NaNNaN",
	}

	// Seed each schema with bare tokens at the appropriate position.
	for tIdx := range tokens {
		f.Add(uint8(0), uint8(tIdx), uint8(0)) // top-level float
		f.Add(uint8(1), uint8(tIdx), uint8(0)) // top-level double
		f.Add(uint8(2), uint8(tIdx), uint8(0)) // record (both fields)
		f.Add(uint8(2), uint8(tIdx), uint8(1)) // record (just f)
		f.Add(uint8(3), uint8(tIdx), uint8(0)) // array of float
		f.Add(uint8(4), uint8(tIdx), uint8(0)) // map of double
		f.Add(uint8(5), uint8(tIdx), uint8(0)) // union float
		f.Add(uint8(6), uint8(tIdx), uint8(0)) // union double
	}

	f.Fuzz(func(t *testing.T, schemaIdx, tokenIdx, variant uint8) {
		s := schemas[int(schemaIdx)%len(schemas)]
		tok := tokens[int(tokenIdx)%len(tokens)]
		var input string
		switch schemaIdx % uint8(len(schemas)) {
		case 0, 1: // bare float / double
			input = tok
		case 2: // record
			if variant&1 == 0 {
				input = `{"f":` + tok + `,"d":` + tok + `}`
			} else {
				input = `{"f":` + tok + `,"d":1.0}`
			}
		case 3: // array of float
			input = `[` + tok + `,1.0,` + tok + `]`
		case 4: // map of double
			input = `{"a":` + tok + `,"b":2.0}`
		case 5, 6: // union (bare branch, the new path)
			input = tok
		}
		var v any
		s.DecodeJSON([]byte(input), &v)
		// Sanity: if decode succeeded against a top-level float/double,
		// EncodeJSON must round-trip without panicking. The encoder's
		// canonical form is the quoted variant, so re-decode of the
		// canonical form should land on the same value (NaN-aware).
		if schemaIdx <= 1 {
			if v == nil {
				return
			}
			out, err := s.EncodeJSON(v)
			if err != nil {
				return
			}
			var v2 any
			if err := s.DecodeJSON(out, &v2); err != nil {
				t.Fatalf("re-decode of canonical encoded failed: %v\n  in: %q\n  enc: %q", err, input, out)
			}
			if !fuzzEqual(v, v2) {
				t.Fatalf("round-trip mismatch:\n  v1: %#v\n  v2: %#v\n  input: %q\n  enc: %q", v, v2, input, out)
			}
		}
	})
}

// FuzzBytesFixedUTF8RoundTrip exercises the JSON encoder bytes/fixed arms that
// take Go strings as input. Those arms route through avroStringValue so the wire
// form is codepoint-per-byte and round-trips. Without it, Encode("é") against
// avro "bytes" serializes the UTF-8 bytes c3 a9 on binary but emits the
// pre-mapping codepoint string on JSON. That produces JSON byte strings which
// re-decode to two-codepoint garbage. The seeds cover multibyte runes inside
// arrays, maps, unions and records, and verify binary/JSON parity.
func FuzzBytesFixedUTF8RoundTrip(f *testing.F) {
	// Fixed sizes that fit common rune lengths.
	fixed2 := MustParse(`{"type":"fixed","name":"F2","size":2}`)
	fixed3 := MustParse(`{"type":"fixed","name":"F3","size":3}`)
	bytesSchema := MustParse(`"bytes"`)
	arrayBytes := MustParse(`{"type":"array","items":"bytes"}`)
	mapBytes := MustParse(`{"type":"map","values":"bytes"}`)
	unionBytes := MustParse(`["null","bytes"]`)
	recordBytesFixed := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"b","type":"bytes"},
		{"name":"f","type":{"type":"fixed","name":"FF","size":3}}
	]}`)

	schemas := []*Schema{bytesSchema, fixed2, fixed3, arrayBytes, mapBytes, unionBytes, recordBytesFixed}

	// Multibyte fragments: every encoded byte must survive the JSON
	// pipeline as a code-point character. The encoded UTF-8 byte length
	// is exactly the size baked into the fixed schemas.
	frags := []string{
		"é",     // 'é' (2 bytes: c3 a9), fits fixed2
		"€",     // '€' (3 bytes: e2 82 ac), fits fixed3
		"ñ",     // 'ñ' (2 bytes), fits fixed2
		"ÿ",     // 'ÿ' (2 bytes), fits fixed2
		"À\xa9", // raw 0xc0 0xa9, invalid UTF-8 but the bytes path
		// must still survive the round-trip (encoder canonicalizes via
		// replacement char if necessary)
		"abc", // ASCII (3 bytes), fits fixed3
	}

	for sIdx := range schemas {
		for fIdx := range frags {
			f.Add(uint8(sIdx), uint8(fIdx))
		}
	}

	f.Fuzz(func(t *testing.T, schemaIdx, fragIdx uint8) {
		idx := int(schemaIdx) % len(schemas)
		s := schemas[idx]
		frag := frags[int(fragIdx)%len(frags)]
		// Build an input shaped for the chosen schema.
		var v any
		switch idx {
		case 0: // "bytes"
			v = frag
		case 1: // fixed(2)
			if len(frag) != 2 {
				return
			}
			v = frag
		case 2: // fixed(3)
			if len(frag) != 3 {
				return
			}
			v = frag
		case 3: // array of bytes
			v = []string{frag, frag}
		case 4: // map of bytes
			v = map[string]string{"k": frag}
		case 5: // union [null, bytes]
			v = frag
		case 6: // record with bytes + fixed(3)
			if len(frag) != 3 {
				return
			}
			v = map[string]any{"b": frag, "f": frag}
		}
		binWire, binErr := s.AppendEncode(nil, v)
		jsonWire, jsonErr := s.AppendEncodeJSON(nil, v)
		if binErr != nil || jsonErr != nil {
			return
		}
		var binDec, jsonDec any
		if _, err := s.Decode(binWire, &binDec); err != nil {
			t.Fatalf("Decode after AppendEncode failed: %v\n  v=%#v frag=%q", err, v, frag)
		}
		if err := s.DecodeJSON(jsonWire, &jsonDec); err != nil {
			t.Fatalf("DecodeJSON after AppendEncodeJSON failed: %v\n  v=%#v frag=%q jsonWire=%q", err, v, frag, jsonWire)
		}
		// Parity claim: binary-decoded and JSON-decoded results must
		// match for the bytes/fixed inputs. If JSON drops or munges the
		// multibyte sequence, fuzzEqual fails. A NaN-aware comparator
		// suffices, there being no floats in this fuzz.
		if !fuzzEqual(binDec, jsonDec) {
			t.Fatalf("binary/JSON decode mismatch:\n  bin:  %#v\n  json: %#v\n  v:    %#v\n  frag: %q\n  binWire=%x\n  jsonWire=%q",
				binDec, jsonDec, v, frag, binWire, jsonWire)
		}
	})
}

// FuzzOCFBlockEnvelope is an ocf-package counterpart that lives here for
// proximity to FuzzOCFReader. The real target is in ocf/fuzz_test.go. This one
// exercises the avro.Schema decode path through OCF-style block framing, a
// (count, size, data, sync) envelope, reaching readBlock's count=0
// sync-validation path indirectly by encoding arbitrary count/size pairs.
// Kept here so all fuzz coverage is visible in one place.

// FuzzSetValueTargets fuzzes Decode/DecodeJSON across the new
// set{Float,Bytes,String}Value target arms with adversarial Go target
// types: named types, TextUnmarshaler-via-Addr, json.Number, big.Rat,
// and *big.Rat. The pre-existing FuzzDecodeVariedTargets only exercises
// shapes (*any / *map[string]any / interface{Foo()}), not named types.
// Bugs in the helper arms (e.g. forgetting to handle a named uint8-slice
// or a TextUnmarshaler value-receiver vs pointer-receiver) would not
// surface in the existing fuzzer.
func FuzzSetValueTargets(f *testing.F) {
	// Schemas where the new helpers fire.
	floatS := MustParse(`"float"`)
	doubleS := MustParse(`"double"`)
	bytesS := MustParse(`"bytes"`)
	fixedS := MustParse(`{"type":"fixed","name":"F","size":4}`)
	stringS := MustParse(`"string"`)

	// makeTarget: pick a Go target by mode. Every target is a
	// pointer-to-T so Decode/DecodeJSON can write through.
	makeTarget := func(mode uint8) any {
		switch mode % 14 {
		case 0:
			var v fuzzNamedFloat
			return &v
		case 1:
			var v fuzzNamedBytes
			return &v
		case 2:
			var v fuzzNamedString
			return &v
		case 3:
			var v fuzzTextThing
			return &v
		case 4:
			var v json.Number
			return &v
		case 5:
			var v big.Rat
			return &v
		case 6:
			var v *big.Rat
			return &v
		case 7:
			// Pointer to named alias of a float, exercising the
			// pointer-indirect arm of setFloatValue against named types.
			var v *fuzzNamedFloat
			return &v
		case 8:
			// [4]uint8, exercising the fixed-array arm of setBytesValue.
			var v [4]byte
			return &v
		case 9:
			// [16]byte, a UUID-style fixed target for bytes-as-string-uuid
			// promotions when the reader is uuid.
			var v [16]byte
			return &v
		case 10:
			// Interface targets that should fall through to v.Set on the
			// kind=Interface arm.
			var v any
			return &v
		case 11:
			// Pointer-to-pointer, the indirectAlloc chain.
			var v **string
			return &v
		case 12:
			// uint64, exercising setLongValue overflow / setFloatValue's
			// CanUint arm.
			var v uint64
			return &v
		case 13:
			// Map keyed by a named string, exercising mapKeyAs.
			var v map[fuzzNamedString]string
			return &v
		}
		var v any
		return &v
	}

	schemas := []*Schema{floatS, doubleS, bytesS, fixedS, stringS}

	// Seed every (schema, target) combo with valid + empty wire.
	for sIdx := range schemas {
		for m := range uint8(14) {
			f.Add(uint8(sIdx), m, []byte{})
			f.Add(uint8(sIdx), m, []byte{0})
			// A 4-byte fixed seed, legal for fixedS and harmless for
			// the rest; the fuzz body only cares about no-panic.
			f.Add(uint8(sIdx), m, []byte{1, 2, 3, 4})
		}
	}
	// One canonical encoded value per schema.
	f.Add(uint8(0), uint8(0), fuzzSeed(floatS, float32(1.5)))
	f.Add(uint8(1), uint8(0), fuzzSeed(doubleS, float64(2.5)))
	f.Add(uint8(2), uint8(1), fuzzSeed(bytesS, []byte{1, 2, 3, 4}))
	f.Add(uint8(3), uint8(8), fuzzSeed(fixedS, [4]byte{9, 8, 7, 6}))
	f.Add(uint8(4), uint8(3), fuzzSeed(stringS, "hello"))

	f.Fuzz(func(t *testing.T, schemaIdx, mode uint8, data []byte) {
		s := schemas[int(schemaIdx)%len(schemas)]
		tgt := makeTarget(mode)
		s.Decode(data, tgt)
		tgt2 := makeTarget(mode)
		s.DecodeJSON(data, tgt2)
	})
}

// FuzzFindUnionBranch fuzzes the (kind, logical) pair-match fallback in
// findUnionBranch via DecodeJSON inputs against unions with ambiguous shapes: two
// same-kind branches differing only by logical type. Pre-tightening the fallback
// matched on kind alone and routed the tag to the first kind-match, silently
// dropping the logical conversion. Seeds cover positive matches, negative matches
// (no branch should match, error not panic), and ambiguity (two branches
// differing only by namespace short-name).
func FuzzFindUnionBranch(f *testing.F) {
	// Schemas exercising every fallback class. Per spec a union may not contain
	// two schemas with the same primitive type even if their logical types
	// differ. So single-branch unions paired with adversarial tag inputs
	// exercise the same-kind disambiguation surface. Fixed branches differ by
	// named type, so the same-kind different-logical case is reachable for
	// "fixed" only.
	// 0: single long+timestamp-millis (logical-tag match positive)
	// 1: plain long (logical-tag-on-plain miss case)
	// 2: two fixed branches differing by logical type (same-kind pair-match)
	// 3: two records, same short name, different namespaces (ambiguity guard)
	// 4: enum + record (short-name fallback)
	unions := []string{
		`[{"type":"long","logicalType":"timestamp-millis"}]`,
		`["long"]`,
		`[{"type":"fixed","name":"F","size":16,"logicalType":"uuid"},{"type":"fixed","name":"F2","size":12,"logicalType":"duration"}]`,
		`[{"type":"record","name":"a.R","fields":[{"name":"v","type":"int"}]},{"type":"record","name":"b.R","fields":[{"name":"v","type":"string"}]}]`,
		`[{"type":"enum","name":"E","symbols":["A","B"]},{"type":"record","name":"R","fields":[{"name":"v","type":"int"}]}]`,
	}
	parsed := make([]*Schema, len(unions))
	for i, u := range unions {
		parsed[i] = MustParse(u)
	}

	tags := []string{
		"long",
		"long.timestamp-millis",
		"long.timestamp-micros",
		"long.timestamp-nanos",
		"F.uuid",
		"F.duration",
		"F2.uuid",
		"F2.duration",
		"F",
		"F2",
		"R",
		"a.R",
		"b.R",
		"E",
		"null",
		"bogus",
		"long.",
		".timestamp-millis",
		"...",
	}
	values := []string{
		`1700000000000`,
		`"550e8400-e29b-41d4-a716-446655440000"`,
		`"AAAAAAAAAAAAAAAAAAAA"`, // 16 bytes codepoint-mapped
		`{"v":1}`,
		`{"v":"x"}`,
		`"A"`,
		`null`,
	}

	for u := uint8(0); u < uint8(len(parsed)); u++ {
		for tIdx := range tags {
			for vIdx := range values {
				f.Add(u, uint8(tIdx), uint8(vIdx))
			}
		}
	}

	f.Fuzz(func(t *testing.T, unionIdx, tagIdx, valIdx uint8) {
		s := parsed[int(unionIdx)%len(parsed)]
		tag := tags[int(tagIdx)%len(tags)]
		val := values[int(valIdx)%len(values)]
		// Tagged-union input: {"tag": val}
		// Use json.Marshal on the tag to ensure quoting/escaping is valid.
		tagBytes, err := json.Marshal(tag)
		if err != nil {
			return
		}
		input := []byte(`{` + string(tagBytes) + `:` + val + `}`)
		var v any
		s.DecodeJSON(input, &v)
		// Also the wrapped TaggedUnions form should never panic on the
		// re-decode of the same payload.
		s.DecodeJSON(input, &v, TaggedUnions())
	})
}

// FuzzUnionBranchErrorWrapping locks the decodeUnionObject / decodeUnionBare
// error wrapping: a target-type mismatch inside a matched tagged-union branch
// must preserve the underlying error via errors.Is/Unwrap rather than surface
// the generic "no union branch matched at offset N" that hides the real cause.
// The fuzz only asserts no panics, the error-message check belonging to a
// regression test. It exercises every (union shape, tagged/bare input, target
// shape) combination to surface any panic path.
func FuzzUnionBranchErrorWrapping(f *testing.F) {
	unions := []*Schema{
		MustParse(`["null","int"]`),
		MustParse(`["null","int","string"]`),
		MustParse(`[{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]},"string"]`),
		MustParse(`[{"type":"long","logicalType":"timestamp-millis"},"string"]`),
	}
	inputs := []string{
		`null`, `42`, `"x"`, `true`, `[]`, `{}`,
		`{"int":1}`, `{"int":"x"}`, `{"null":null}`,
		`{"long.timestamp-millis":"not-a-number"}`,
		`{"R":{"x":1}}`, `{"R":{"x":"wrong"}}`,
		`{"unknown":1}`, `{"a.b.c":1}`,
	}
	type stringErr struct {
		V string `avro:"v"`
	}

	makeTarget := func(mode uint8) any {
		switch mode % 6 {
		case 0:
			var v any
			return &v
		case 1:
			var v int32
			return &v
		case 2:
			var v string
			return &v
		case 3:
			var v stringErr
			return &v
		case 4:
			var v map[string]any
			return &v
		case 5:
			var v *time.Time
			return &v
		}
		var v any
		return &v
	}

	for uIdx := range unions {
		for iIdx := range inputs {
			for m := range uint8(6) {
				f.Add(uint8(uIdx), uint8(iIdx), m)
			}
		}
	}

	f.Fuzz(func(t *testing.T, uIdx, iIdx, mode uint8) {
		s := unions[int(uIdx)%len(unions)]
		input := inputs[int(iIdx)%len(inputs)]
		tgt := makeTarget(mode)
		s.DecodeJSON([]byte(input), tgt)
		// And TaggedUnions mode.
		tgt2 := makeTarget(mode)
		s.DecodeJSON([]byte(input), tgt2, TaggedUnions())
	})
}

// FuzzResolveUnionUnionTags exercises resolveUnionUnion's reader-side
// branch-name path under TaggedUnions: Resolve(["null","int"] ->
// ["null","long"]) decoded into *any must emit {"long":42}, the reader-side
// branch name, not {"int":42}. The fuzz drives Resolve across writer×reader
// union pairs, encodes against the writer, resolves and decodes with
// TaggedUnions, and verifies the tagged map's key names a reader-side branch
// (or one of the documented short-name fallbacks). Bug surfaces: a tag key
// matching no reader branch, or a non-map return.
func FuzzResolveUnionUnionTags(f *testing.F) {
	type seed struct {
		writer, reader string
		val            any
	}
	seeds := []seed{
		{`["null","int"]`, `["null","long"]`, int32(42)},
		{`["null","int","string"]`, `["null","long","string"]`, int32(7)},
		{`["null","int","string"]`, `["null","long","string"]`, "hi"},
		{`["int","long"]`, `["long","float"]`, int32(1)},
		{`["int","string"]`, `["long","string"]`, "x"},
		{`["null",{"type":"int","logicalType":"date"}]`, `["null",{"type":"long","logicalType":"timestamp-millis"}]`, int32(19000)},
		{`["int"]`, `["long"]`, int32(99)},
	}
	for _, s := range seeds {
		ws, err := Parse(s.writer)
		if err != nil {
			continue
		}
		data, err := ws.AppendEncode(nil, s.val)
		if err != nil {
			continue
		}
		f.Add(s.writer, s.reader, data)
	}
	// Adversarial: empty + huge varint.
	f.Add(`["null","int"]`, `["null","long"]`, []byte{})
	f.Add(`["null","int"]`, `["null","long"]`, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F})

	f.Fuzz(func(t *testing.T, writerJSON, readerJSON string, data []byte) {
		w, err := Parse(writerJSON)
		if err != nil {
			return
		}
		r, err := Parse(readerJSON)
		if err != nil {
			return
		}
		resolved, err := Resolve(w, r)
		if err != nil {
			return
		}
		var got any
		if _, err := resolved.Decode(data, &got, TaggedUnions()); err != nil {
			return
		}
		// got is either nil (null branch) or map[string]any{<tag>: value}.
		// The tag MUST name a reader-side branch; if not, a regression
		// has been re-introduced.
		if got == nil {
			return
		}
		m, ok := got.(map[string]any)
		if !ok {
			t.Fatalf("TaggedUnions decode returned non-map: %T (%v)", got, got)
		}
		if len(m) != 1 {
			t.Fatalf("TaggedUnions map has %d keys, expected 1: %v", len(m), m)
		}
		var key string
		for k := range m {
			key = k
		}
		ok = slices.Contains(readerBranchTags(readerJSON), key)
		if !ok {
			t.Fatalf("TaggedUnions key %q not found in reader schema %s", key, readerJSON)
		}
	})
}

// readerBranchTags returns the legal tagged-union key forms for each
// branch in unionJSON. Used by FuzzResolveUnionUnionTags to validate
// the reader-side tag claim without re-implementing the encoder's
// naming rules. Returns nil if the schema isn't a union.
func readerBranchTags(unionJSON string) []string {
	s, err := Parse(unionJSON)
	if err != nil {
		return nil
	}
	root := s.Root()
	if len(root.Branches) == 0 {
		return []string{branchTagFor(*root)}
	}
	tags := make([]string, 0, len(root.Branches))
	for i := range root.Branches {
		tags = append(tags, branchTagFor(root.Branches[i]))
	}
	return tags
}

// branchTagFor returns the standard binary-TaggedUnions key form. This
// matches unionBranchName in the codec: primitives use the kind alone,
// without the logical-type qualifier, which only applies to the JSON-side
// TagLogicalTypes form. Named types use their short name.
func branchTagFor(n SchemaNode) string {
	switch n.Type {
	case "null":
		return "null"
	case "boolean", "int", "long", "float", "double", "bytes", "string":
		return n.Type
	case "record", "enum", "fixed":
		return n.Name
	default:
		return n.Type
	}
}

// FuzzDecodeUnionObjectDeep stresses the depth-tracked recursive
// descent through decodeUnionObject / decodeUnionBare with cyclic
// JSON inputs. The errTooDeep propagation must not be masked by the
// "try tagged then bare" fallback: errors.Is(err, errTooDeep)
// short-circuits before the bare retry, otherwise the tagged-side
// errTooDeep would be caught and the bare-side retry would burn
// more depth. Fuzz over deeply nested {"tag":{"tag":{...}}} sequences
// and assert the library terminates, with no panic or stack overflow.
func FuzzDecodeUnionObjectDeep(f *testing.F) {
	recursiveSchema := MustParse(nodeRecursiveSchema)
	// Seeds: short, medium, deeper-than-maxDepth.
	f.Add(uint16(10))
	f.Add(uint16(100))
	f.Add(uint16(maxDepth - 2))
	f.Add(uint16(maxDepth + 5))
	f.Add(uint16(maxDepth + 100))

	f.Fuzz(func(t *testing.T, depth uint16) {
		if depth > maxDepth+200 {
			depth = maxDepth + 200
		}
		// Build {"value":0,"next":{"Node":{"value":0,"next":{"Node":...}}}}
		var b strings.Builder
		for range int(depth) {
			b.WriteString(`{"value":0,"next":{"Node":`)
		}
		b.WriteString(`{"value":0,"next":null}`)
		for range int(depth) {
			b.WriteString(`}}`)
		}
		var n struct {
			Value int32 `avro:"value"`
			Next  any   `avro:"next"`
		}
		recursiveSchema.DecodeJSON([]byte(b.String()), &n)
	})
}

// FuzzNumberCarriers fuzzes the json.Number / *big.Rat / *big.Int / *big.Float
// carrier surface across primitive Avro types, reachable via setFloatValue's
// jsonNumberType branch, setDecimalRat, and the big-decimal payload path. The
// seeds are adversarial numeric strings ("1e1000", "NaN", forty nines) and the
// assertion is no panic on encode or decode.
//
// safeForBigNum reports whether s is small enough to hand to the stdlib
// big.Rat.SetString / big.ParseFloat parsers without risking a multi-minute or
// multi-gigabyte materialization: they eagerly build the full mantissa and
// 10^exponent, so a 20-million-digit mantissa costs big.Rat ~8 minutes and a
// short "1e2000000000" allocates gigabytes. twmb's own entry points are bounded,
// and this mirrors that bound for the fuzzer's direct stdlib construction so
// the harness cannot DoS itself.
func safeForBigNum(s string) bool {
	if len(s) > 1024 {
		return false
	}
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		exp := strings.TrimLeft(s[i+1:], "+-")
		if len(exp) > 4 { // |exponent| could exceed 9999, so 10^exp is huge
			return false
		}
	}
	return true
}

func FuzzNumberCarriers(f *testing.F) {
	floatS := MustParse(`"float"`)
	doubleS := MustParse(`"double"`)
	longS := MustParse(`"long"`)
	intS := MustParse(`"int"`)
	decimalS := MustParse(`{"type":"bytes","logicalType":"decimal","precision":20,"scale":4}`)
	bigDecimalS := MustParse(`{"type":"bytes","logicalType":"big-decimal"}`)

	schemas := []*Schema{floatS, doubleS, longS, intS, decimalS, bigDecimalS}

	for sIdx := range schemas {
		f.Add(uint8(sIdx), "1")
		f.Add(uint8(sIdx), "0")
		f.Add(uint8(sIdx), "-1")
		f.Add(uint8(sIdx), "1.5")
		f.Add(uint8(sIdx), "1e10")
		f.Add(uint8(sIdx), "1e1000")
		f.Add(uint8(sIdx), "-1e-1000")
		f.Add(uint8(sIdx), "NaN")
		f.Add(uint8(sIdx), "Infinity")
		f.Add(uint8(sIdx), strings.Repeat("9", 40))
		f.Add(uint8(sIdx), "0."+strings.Repeat("0", 100)+"1")
		f.Add(uint8(sIdx), "")
	}

	f.Fuzz(func(t *testing.T, schemaIdx uint8, numStr string) {
		s := schemas[int(schemaIdx)%len(schemas)]
		// As json.Number: twmb's own numeric entry points are length- and
		// magnitude-bounded (maxRatInputLen / maxParseFloatLen /
		// decimalScaleLimit), so any input is safe to hand to twmb here.
		s.AppendEncode(nil, json.Number(numStr))
		s.AppendEncodeJSON(nil, json.Number(numStr))
		// As *big.Rat / *big.Float the fuzzer builds the value directly via
		// stdlib parsers, which, unlike twmb, eagerly materialize the full
		// mantissa and 10^exp. A 20-million-digit mantissa takes big.Rat
		// ~8 minutes, and "1e2000000000" allocates gigabytes, OOM-ing the
		// worker. Bound the input the way twmb itself does so the fuzzer
		// exercises twmb's big.Rat/big.Float handling without DoSing its own
		// harness (a 13-byte input could otherwise hang the whole run).
		if !safeForBigNum(numStr) {
			return
		}
		r := new(big.Rat)
		if _, ok := r.SetString(numStr); ok {
			s.AppendEncode(nil, r)
			s.AppendEncodeJSON(nil, r)
		}
		bf, _, err := big.ParseFloat(numStr, 10, 100, big.ToNearestEven)
		if err == nil {
			s.AppendEncode(nil, bf)
		}
	})
}

// emit binary into the silence-the-unused-import floor; only used by
// FuzzOCFBlockEnvelope's avro-side helpers, kept here so the import
// is referenced unconditionally.
var _ = errors.New
var _ = binary.AppendVarint

// ---------- bench_test.go ----------

// benchSuperheroSchema is the Superhero record schema without a union wrapper.
const benchSuperheroSchema = `{
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
}`

func benchNewSuperhero() *Superhero {
	return &Superhero{
		ID:            234765,
		AffiliationID: 9867,
		Name:          "Wolverine",
		Life:          85.25,
		Energy:        32.75,
		Powers: []*Superpower{
			{ID: 2345, Name: "Bone Claws", Damage: 5, Energy: 1.15, Passive: false},
			{ID: 2346, Name: "Regeneration", Damage: -2, Energy: 0.55, Passive: true},
			{ID: 2347, Name: "Adamant skeleton", Damage: -10, Energy: 0, Passive: true},
		},
	}
}

var benchSuperheroValue = map[string]any{
	"id":             int32(234765),
	"affiliation_id": int32(9867),
	"name":           "Wolverine",
	"life":           float32(85.25),
	"energy":         float32(32.75),
	"powers": []map[string]any{
		{"id": int32(2345), "name": "Bone Claws", "damage": float32(5), "energy": float32(1.15), "passive": false},
		{"id": int32(2346), "name": "Regeneration", "damage": float32(-2), "energy": float32(0.55), "passive": true},
		{"id": int32(2347), "name": "Adamant skeleton", "damage": float32(-10), "energy": float32(0), "passive": true},
	},
}

func BenchmarkSerializeGeneric(b *testing.B) {
	super := map[string]any{
		"id":             int32(234765),
		"affiliation_id": int32(9867),
		"name":           "Wolverine",
		"life":           float32(85.25),
		"energy":         float32(32.75),
		"powers": []map[string]any{
			{"id": int32(2345), "name": "Bone Claws", "damage": float32(5), "energy": float32(1.15), "passive": false},
			{"id": int32(2346), "name": "Regeneration", "damage": float32(-2), "energy": float32(0.55), "passive": true},
			{"id": int32(2347), "name": "Adamant skeleton", "damage": float32(-10), "energy": float32(0), "passive": true},
		},
	}

	s := mustParse(b, benchSuperheroSchema)
	dst := mustAppendEncode(b, s, nil, super)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = mustAppendEncode(b, s, dst[:0], super)
	}
}

func BenchmarkParseSchema(b *testing.B) {
	b.Run("Primitives", func(b *testing.B) {
		schema := primsSchema
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			mustParse(b, schema)
		}
	})
	b.Run("Complex", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			mustParse(b, benchSuperheroSchema)
		}
	})
}

func BenchmarkMapEncode(b *testing.B) {
	s := mustParse(b, `{"type":"map","values":"string"}`)
	m := map[string]string{
		"key1": "value1", "key2": "value2", "key3": "value3",
		"key4": "value4", "key5": "value5",
	}
	dst := mustAppendEncode(b, s, nil, m)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = mustAppendEncode(b, s, dst[:0], m)
	}
}

func BenchmarkMapDecode(b *testing.B) {
	s := mustParse(b, `{"type":"map","values":"string"}`)
	m := map[string]string{
		"key1": "value1", "key2": "value2", "key3": "value3",
		"key4": "value4", "key5": "value5",
	}
	encoded := mustAppendEncode(b, s, nil, m)
	var out map[string]string
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = nil
		mustDecode(b, s, encoded, &out)
	}
	_ = out
}

func BenchmarkEnumEncode(b *testing.B) {
	s := mustParse(b, `{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE","YELLOW"]}`)
	val := "GREEN"
	dst := mustAppendEncode(b, s, nil, val)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = mustAppendEncode(b, s, dst[:0], val)
	}
}

func BenchmarkEnumDecode(b *testing.B) {
	s := mustParse(b, `{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE","YELLOW"]}`)
	encoded := mustAppendEncode(b, s, nil, "GREEN")
	var out string
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = ""
		mustDecode(b, s, encoded, &out)
	}
	_ = out
}

func BenchmarkLargeArrayEncode(b *testing.B) {
	s := mustParse(b, benchSuperheroSchema)
	hero := benchNewSuperhero()
	powers := make([]*Superpower, 100)
	for i := range powers {
		powers[i] = &Superpower{
			ID:      int32(i),
			Name:    fmt.Sprintf("Power-%d", i),
			Damage:  float32(i) * 1.5,
			Energy:  float32(i) * 0.3,
			Passive: i%2 == 0,
		}
	}
	hero.Powers = powers
	dst := mustAppendEncode(b, s, nil, hero)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = mustAppendEncode(b, s, dst[:0], hero)
	}
}

func BenchmarkLargeArrayDecode(b *testing.B) {
	s := mustParse(b, benchSuperheroSchema)
	hero := benchNewSuperhero()
	powers := make([]*Superpower, 100)
	for i := range powers {
		powers[i] = &Superpower{
			ID:      int32(i),
			Name:    fmt.Sprintf("Power-%d", i),
			Damage:  float32(i) * 1.5,
			Energy:  float32(i) * 0.3,
			Passive: i%2 == 0,
		}
	}
	hero.Powers = powers
	encoded := mustAppendEncode(b, s, nil, hero)
	var out Superhero
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = Superhero{}
		mustDecode(b, s, encoded, &out)
	}
}

func BenchmarkStringHeavyEncode(b *testing.B) {
	type StringRecord struct {
		S1  string `avro:"s1"`
		S2  string `avro:"s2"`
		S3  string `avro:"s3"`
		S4  string `avro:"s4"`
		S5  string `avro:"s5"`
		S6  string `avro:"s6"`
		S7  string `avro:"s7"`
		S8  string `avro:"s8"`
		S9  string `avro:"s9"`
		S10 string `avro:"s10"`
	}
	s := mustParse(b, stringsSchema)
	input := &StringRecord{
		S1: strings.Repeat("hello ", 20), S2: strings.Repeat("world ", 20),
		S3: strings.Repeat("avro ", 20), S4: strings.Repeat("bench ", 20),
		S5: strings.Repeat("test ", 20), S6: strings.Repeat("data ", 20),
		S7: strings.Repeat("schema ", 20), S8: strings.Repeat("encode ", 20),
		S9: strings.Repeat("decode ", 20), S10: strings.Repeat("string ", 20),
	}
	dst := mustAppendEncode(b, s, nil, input)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = mustAppendEncode(b, s, dst[:0], input)
	}
}

func BenchmarkStringHeavyDecode(b *testing.B) {
	type StringRecord struct {
		S1  string `avro:"s1"`
		S2  string `avro:"s2"`
		S3  string `avro:"s3"`
		S4  string `avro:"s4"`
		S5  string `avro:"s5"`
		S6  string `avro:"s6"`
		S7  string `avro:"s7"`
		S8  string `avro:"s8"`
		S9  string `avro:"s9"`
		S10 string `avro:"s10"`
	}
	s := mustParse(b, stringsSchema)
	input := &StringRecord{
		S1: strings.Repeat("hello ", 20), S2: strings.Repeat("world ", 20),
		S3: strings.Repeat("avro ", 20), S4: strings.Repeat("bench ", 20),
		S5: strings.Repeat("test ", 20), S6: strings.Repeat("data ", 20),
		S7: strings.Repeat("schema ", 20), S8: strings.Repeat("encode ", 20),
		S9: strings.Repeat("decode ", 20), S10: strings.Repeat("string ", 20),
	}
	encoded := mustAppendEncode(b, s, nil, input)
	var out StringRecord
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = StringRecord{}
		mustDecode(b, s, encoded, &out)
	}
}

func BenchmarkDecodeAny(b *testing.B) {
	s := mustParse(b, benchSuperheroSchema)
	encoded := mustEncode(b, s, benchSuperheroValue)
	var out any
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		mustDecode(b, s, encoded, &out)
	}
}

func BenchmarkDecodeAnyTaggedUnions(b *testing.B) {
	s := mustParse(b, benchSuperheroSchema)
	encoded := mustEncode(b, s, benchSuperheroValue)
	var out any
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		mustDecode(b, s, encoded, &out, TaggedUnions())
	}
}

func BenchmarkEncodeJSON(b *testing.B) {
	s := mustParse(b, benchSuperheroSchema)
	encoded := mustEncode(b, s, benchSuperheroValue)
	var native any
	mustDecode(b, s, encoded, &native)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		mustEncodeJSON(b, s, native)
	}
}

func BenchmarkEncodeJSONTagged(b *testing.B) {
	s := mustParse(b, benchSuperheroSchema)
	encoded := mustEncode(b, s, benchSuperheroValue)
	var native any
	mustDecode(b, s, encoded, &native)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		mustEncodeJSON(b, s, native, TaggedUnions())
	}
}

type benchMoney struct{ Cents int64 }

func BenchmarkCustomTypeEncode(b *testing.B) {
	type Order struct {
		ID    int64      `avro:"id"`
		Price benchMoney `avro:"price"`
	}
	s := mustParse(b, orderIDPriceSchema, NewCustomType[benchMoney, int64]("money",
		func(m benchMoney, _ *SchemaNode) (int64, error) { return m.Cents, nil },
		func(c int64, _ *SchemaNode) (benchMoney, error) { return benchMoney{Cents: c}, nil },
	))
	v := Order{ID: 1, Price: benchMoney{Cents: 1999}}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		mustEncode(b, s, &v)
	}
}

func BenchmarkCustomTypeDecode(b *testing.B) {
	type Order struct {
		ID    int64      `avro:"id"`
		Price benchMoney `avro:"price"`
	}
	s := mustParse(b, orderIDPriceSchema, NewCustomType[benchMoney, int64]("money",
		func(m benchMoney, _ *SchemaNode) (int64, error) { return m.Cents, nil },
		func(c int64, _ *SchemaNode) (benchMoney, error) { return benchMoney{Cents: c}, nil },
	))
	data, _ := s.Encode(&Order{ID: 1, Price: benchMoney{Cents: 1999}})
	var out Order
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		mustDecode(b, s, data, &out)
	}
}

func BenchmarkCustomTypeDecodeAny(b *testing.B) {
	s := mustParse(b, `{"type":"long","logicalType":"money"}`, NewCustomType[benchMoney, int64]("money",
		func(m benchMoney, _ *SchemaNode) (int64, error) { return m.Cents, nil },
		func(c int64, _ *SchemaNode) (benchMoney, error) { return benchMoney{Cents: c}, nil },
	))
	data, _ := s.Encode(int64(1999))
	var out any
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		mustDecode(b, s, data, &out)
	}
}

const benchDecodeJSONSchema = `{
	"name": "Event",
	"type": "record",
	"fields": [
		{"name": "id", "type": "long"},
		{"name": "name", "type": "string"},
		{"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
		{"name": "updated_at", "type": {"type": "long", "logicalType": "timestamp-micros"}},
		{"name": "date", "type": {"type": "int", "logicalType": "date"}},
		{"name": "amount", "type": "double"},
		{"name": "active", "type": "boolean"},
		{"name": "tag", "type": ["null", "string"]}
	]
}`

var benchDecodeJSONInput = []byte(`{"id":12345,"name":"test-event","created_at":1700000000000,"updated_at":1700000000000000,"date":19700,"amount":3.14,"active":true,"tag":{"string":"hello"}}`)

func BenchmarkDecodeJSON_Any(b *testing.B) {
	s := mustParse(b, benchDecodeJSONSchema)
	var out any
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		mustDecodeJSON(b, s, benchDecodeJSONInput, &out)
	}
}

func BenchmarkDecodeJSON_Struct(b *testing.B) {
	type Event struct {
		ID        int64     `avro:"id"`
		Name      string    `avro:"name"`
		CreatedAt time.Time `avro:"created_at"`
		UpdatedAt time.Time `avro:"updated_at"`
		Date      time.Time `avro:"date"`
		Amount    float64   `avro:"amount"`
		Active    bool      `avro:"active"`
		Tag       *string   `avro:"tag"`
	}
	s := mustParse(b, benchDecodeJSONSchema)
	var out Event
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		mustDecodeJSON(b, s, benchDecodeJSONInput, &out)
	}
}

const benchUnionTryEachSchema = `{
	"name": "U",
	"type": "record",
	"fields": [
		{"name": "id", "type": ["null","int"], "default": null},
		{"name": "name", "type": ["null","string"], "default": null},
		{"name": "tags", "type": ["null",{"type":"array","items":"string"}], "default": null},
		{"name": "meta", "type": ["null",{"type":"map","values":"int"}], "default": null},
		{"name": "kv", "type": ["null","int","string","double"], "default": null}
	]
}`

// BenchmarkEncodeJSON_UnionTryEach exercises appendAvroJSONUnion's
// try-each loop. json.Number forces unionTypeNameForValue to "" so the
// value falls into try-each (the loop changed by the bare-nil parity
// fix). map[string]int and []string also miss type-name dispatch.
func BenchmarkEncodeJSON_UnionTryEach(b *testing.B) {
	s := mustParse(b, benchUnionTryEachSchema)
	val := map[string]any{
		"id":   json.Number("12345"),
		"name": "hello",
		"tags": []string{"a", "b", "c"},
		"meta": map[string]int{"x": 1, "y": 2},
		"kv":   json.Number("4567"),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		mustEncodeJSON(b, s, val)
	}
}

// ---------- bench_perf_test.go ----------

// Benchmarks targeting the deserString TextUnmarshaler/[]byte/UUID
// allocation paths.

type benchTextUnmarshaler struct{ s string }

func (b *benchTextUnmarshaler) UnmarshalText(text []byte) error {
	b.s = string(text)
	return nil
}

func BenchmarkDecodeStringTextUnmarshaler(b *testing.B) {
	type Encoded struct {
		V string `avro:"v"`
	}
	type Decoded struct {
		V benchTextUnmarshaler `avro:"v"`
	}
	schema := `{"type":"record","name":"r","fields":[{"name":"v","type":"string"}]}`
	s := mustParse(b, schema)
	in := Encoded{V: "hello world this is a test"}
	enc := mustAppendEncode(b, s, nil, &in)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var out Decoded
		mustDecode(b, s, enc, &out)
	}
}

func BenchmarkDecodeStringBytes(b *testing.B) {
	type R struct {
		V []byte `avro:"v"`
	}
	schema := `{"type":"record","name":"r","fields":[{"name":"v","type":"string"}]}`
	s := mustParse(b, schema)
	in := R{V: bytes.Repeat([]byte("x"), 32)}
	enc := mustAppendEncode(b, s, nil, &in)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var out R
		mustDecode(b, s, enc, &out)
	}
}

func BenchmarkDecodeUUIDIntoFixed(b *testing.B) {
	type R struct {
		V [16]byte `avro:"v"`
	}
	schema := `{"type":"record","name":"r","fields":[{"name":"v","type":{"type":"string","logicalType":"uuid"}}]}`
	s := mustParse(b, schema)
	in := R{V: [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}}
	enc := mustAppendEncode(b, s, nil, &in)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var out R
		mustDecode(b, s, enc, &out)
	}
}

func BenchmarkParseUUID(b *testing.B) {
	const s = "550e8400-e29b-41d4-a716-446655440000"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = parseUUID(s)
	}
}

// Map decode benchmarks targeting the size-hint optimization. We use a
// large map with mixed key/value sizes so any rehash cost shows up.

func BenchmarkDecodeMap_String_Small(b *testing.B) {
	benchDecodeMapStringValue(b, 8)
}

func BenchmarkDecodeMap_String_Medium(b *testing.B) {
	benchDecodeMapStringValue(b, 64)
}

func BenchmarkDecodeMap_String_Large(b *testing.B) {
	benchDecodeMapStringValue(b, 512)
}

func benchDecodeMapStringValue(b *testing.B, n int) {
	schema := `{"type":"map","values":"string"}`
	s := mustParse(b, schema)
	in := make(map[string]string, n)
	for i := range n {
		in[fmt.Sprintf("key-%05d", i)] = fmt.Sprintf("value-%05d", i)
	}
	enc := mustAppendEncode(b, s, nil, &in)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var out map[string]string
		mustDecode(b, s, enc, &out)
	}
}

func BenchmarkEncodeEnum_LargeAlphabet(b *testing.B) {
	// 32 symbols exceeds the linear-scan threshold, so the map index path
	// is exercised.
	syms := make([]string, 32)
	for i := range syms {
		syms[i] = fmt.Sprintf("SYM_%d", i)
	}
	enc, _ := json.Marshal(syms)
	schema := fmt.Sprintf(`{"type":"enum","name":"E","symbols":%s}`, enc)
	s := mustParse(b, schema)
	val := "SYM_31" // worst case for linear scan
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		mustAppendEncode(b, s, nil, &val)
	}
}

func BenchmarkResolveDecodeWithDefaults(b *testing.B) {
	writer := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"string"},
		{"name":"b","type":"int"}
	]}`
	reader := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"string"},
		{"name":"b","type":"int"},
		{"name":"c","type":"string","default":"default-c"},
		{"name":"d","type":"int","default":42},
		{"name":"e","type":["null","string"],"default":null}
	]}`
	w := mustParse(b, writer)
	r := mustParse(b, reader)
	resolved := mustResolve(b, w, r)
	type WIn struct {
		A string `avro:"a"`
		B int32  `avro:"b"`
	}
	in := WIn{A: "hello", B: 7}
	enc := mustAppendEncode(b, w, nil, &in)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var out map[string]any
		mustDecode(b, resolved, enc, &out)
	}
}

func BenchmarkDecodeMapInto_Any_Medium(b *testing.B) {
	schema := `{"type":"map","values":"string"}`
	s := mustParse(b, schema)
	in := make(map[string]string, 64)
	for i := range 64 {
		in[fmt.Sprintf("key-%05d", i)] = fmt.Sprintf("value-%05d", i)
	}
	enc := mustAppendEncode(b, s, nil, &in)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var out any
		mustDecode(b, s, enc, &out)
	}
}

func BenchmarkDecodeArrayStringInto_Any_Medium(b *testing.B) {
	schema := `{"type":"array","items":"string"}`
	s := mustParse(b, schema)
	in := make([]string, 64)
	for i := range 64 {
		in[i] = fmt.Sprintf("value-%05d", i)
	}
	enc := mustAppendEncode(b, s, nil, &in)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var out any
		mustDecode(b, s, enc, &out)
	}
}

func BenchmarkDecodeArrayIntInto_Any_Medium(b *testing.B) {
	schema := `{"type":"array","items":"int"}`
	s := mustParse(b, schema)
	in := make([]int32, 64)
	for i := range 64 {
		in[i] = int32(i * 1000)
	}
	enc := mustAppendEncode(b, s, nil, &in)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var out any
		mustDecode(b, s, enc, &out)
	}
}

// Multi-level pointer specialization benchmarks. Each measures serArray.serFoo
// or serMap.serFoo encoding a slice/map whose element type has zero, one, two,
// or three pointer-indirection levels. That quantifies the cost of supporting
// deeper pointer chains in the per-primitive specializations, against the
// existing single-level unwrap and the fully direct path. Pre-multi-level-fix,
// ptr2/ptr3 cases b.Skip because the encoder rejects them.

func BenchmarkSpecArrayMultiLevelPointer(b *testing.B) {
	const N = 1024
	intS := MustParse(`{"type":"array","items":"int"}`)
	longS := MustParse(`{"type":"array","items":"long"}`)
	floatS := MustParse(`{"type":"array","items":"float"}`)
	doubleS := MustParse(`{"type":"array","items":"double"}`)
	stringS := MustParse(`{"type":"array","items":"string"}`)
	boolS := MustParse(`{"type":"array","items":"boolean"}`)

	int32Direct := make([]int32, N)
	int32Ptr1 := make([]*int32, N)
	int32Ptr2 := make([]**int32, N)
	int32Ptr3 := make([]***int32, N)
	int64Direct := make([]int64, N)
	int64Ptr1 := make([]*int64, N)
	int64Ptr2 := make([]**int64, N)
	int64Ptr3 := make([]***int64, N)
	float32Direct := make([]float32, N)
	float32Ptr1 := make([]*float32, N)
	float32Ptr2 := make([]**float32, N)
	float32Ptr3 := make([]***float32, N)
	float64Direct := make([]float64, N)
	float64Ptr1 := make([]*float64, N)
	float64Ptr2 := make([]**float64, N)
	float64Ptr3 := make([]***float64, N)
	stringDirect := make([]string, N)
	stringPtr1 := make([]*string, N)
	stringPtr2 := make([]**string, N)
	stringPtr3 := make([]***string, N)
	boolDirect := make([]bool, N)
	boolPtr1 := make([]*bool, N)
	boolPtr2 := make([]**bool, N)
	boolPtr3 := make([]***bool, N)
	for i := range N {
		i32 := int32(i)
		i32p1 := &i32
		i32p2 := &i32p1
		i64 := int64(i)
		i64p1 := &i64
		i64p2 := &i64p1
		f32 := float32(i)
		f32p1 := &f32
		f32p2 := &f32p1
		f64 := float64(i)
		f64p1 := &f64
		f64p2 := &f64p1
		s := fmt.Sprintf("v%d", i)
		sp1 := &s
		sp2 := &sp1
		bv := i&1 == 1
		bp1 := &bv
		bp2 := &bp1
		int32Direct[i] = i32
		int32Ptr1[i] = i32p1
		int32Ptr2[i] = i32p2
		int32Ptr3[i] = &i32p2
		int64Direct[i] = i64
		int64Ptr1[i] = i64p1
		int64Ptr2[i] = i64p2
		int64Ptr3[i] = &i64p2
		float32Direct[i] = f32
		float32Ptr1[i] = f32p1
		float32Ptr2[i] = f32p2
		float32Ptr3[i] = &f32p2
		float64Direct[i] = f64
		float64Ptr1[i] = f64p1
		float64Ptr2[i] = f64p2
		float64Ptr3[i] = &f64p2
		stringDirect[i] = s
		stringPtr1[i] = sp1
		stringPtr2[i] = sp2
		stringPtr3[i] = &sp2
		boolDirect[i] = bv
		boolPtr1[i] = bp1
		boolPtr2[i] = bp2
		boolPtr3[i] = &bp2
	}

	cases := []struct {
		name string
		s    *Schema
		v    any
	}{
		{"int32/direct", intS, int32Direct},
		{"int32/ptr1", intS, int32Ptr1},
		{"int32/ptr2", intS, int32Ptr2},
		{"int32/ptr3", intS, int32Ptr3},
		{"int64/direct", longS, int64Direct},
		{"int64/ptr1", longS, int64Ptr1},
		{"int64/ptr2", longS, int64Ptr2},
		{"int64/ptr3", longS, int64Ptr3},
		{"float32/direct", floatS, float32Direct},
		{"float32/ptr1", floatS, float32Ptr1},
		{"float32/ptr2", floatS, float32Ptr2},
		{"float32/ptr3", floatS, float32Ptr3},
		{"float64/direct", doubleS, float64Direct},
		{"float64/ptr1", doubleS, float64Ptr1},
		{"float64/ptr2", doubleS, float64Ptr2},
		{"float64/ptr3", doubleS, float64Ptr3},
		{"string/direct", stringS, stringDirect},
		{"string/ptr1", stringS, stringPtr1},
		{"string/ptr2", stringS, stringPtr2},
		{"string/ptr3", stringS, stringPtr3},
		{"bool/direct", boolS, boolDirect},
		{"bool/ptr1", boolS, boolPtr1},
		{"bool/ptr2", boolS, boolPtr2},
		{"bool/ptr3", boolS, boolPtr3},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			buf := make([]byte, 0, 16<<10)
			if _, err := tc.s.AppendEncode(buf, tc.v); err != nil {
				b.Skipf("unsupported pre-patch: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				buf, _ = tc.s.AppendEncode(buf[:0], tc.v)
			}
			_ = buf
		})
	}
}

func BenchmarkSpecMapMultiLevelPointer(b *testing.B) {
	const N = 1024
	intS := MustParse(`{"type":"map","values":"int"}`)
	stringS := MustParse(`{"type":"map","values":"string"}`)

	int32Direct := make(map[string]int32, N)
	int32Ptr1 := make(map[string]*int32, N)
	int32Ptr2 := make(map[string]**int32, N)
	int32Ptr3 := make(map[string]***int32, N)
	stringDirect := make(map[string]string, N)
	stringPtr1 := make(map[string]*string, N)
	stringPtr2 := make(map[string]**string, N)
	stringPtr3 := make(map[string]***string, N)
	for i := range N {
		key := fmt.Sprintf("k%04d", i)
		i32 := int32(i)
		i32p1 := &i32
		i32p2 := &i32p1
		s := fmt.Sprintf("v%d", i)
		sp1 := &s
		sp2 := &sp1
		int32Direct[key] = i32
		int32Ptr1[key] = i32p1
		int32Ptr2[key] = i32p2
		int32Ptr3[key] = &i32p2
		stringDirect[key] = s
		stringPtr1[key] = sp1
		stringPtr2[key] = sp2
		stringPtr3[key] = &sp2
	}

	cases := []struct {
		name string
		s    *Schema
		v    any
	}{
		{"int32/direct", intS, int32Direct},
		{"int32/ptr1", intS, int32Ptr1},
		{"int32/ptr2", intS, int32Ptr2},
		{"int32/ptr3", intS, int32Ptr3},
		{"string/direct", stringS, stringDirect},
		{"string/ptr1", stringS, stringPtr1},
		{"string/ptr2", stringS, stringPtr2},
		{"string/ptr3", stringS, stringPtr3},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			buf := make([]byte, 0, 32<<10)
			if _, err := tc.s.AppendEncode(buf, tc.v); err != nil {
				b.Skipf("unsupported pre-patch: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				buf, _ = tc.s.AppendEncode(buf[:0], tc.v)
			}
			_ = buf
		})
	}
}

// ---------- varint_bench_test.go ----------

// Candidate implementations of varlong (signed zigzag) for benchmarking.
//
//   - appendVarlongLoop: current production shape (simple loop).
//   - appendVarlongSwitch: hand-unrolled switch keyed on bits.Len64.
//   - binary.AppendVarint: encoding/binary stdlib reference (same loop,
//     different package).
//
// Avro and encoding/binary use the same zigzag-varlong wire format
// (PutVarint and twmb's appendVarlong both flip via ^(x<<1) on x<0).

// appendVarlongLoop matches the current appendVarlong.
func appendVarlongLoop(dst []byte, i int64) []byte {
	u := uint64(i)<<1 ^ uint64(i>>63)
	for u&0x7f != u {
		dst = append(dst, byte(u&0x7f|0x80))
		u >>= 7
	}
	return append(dst, byte(u))
}

// appendVarlongSwitch unrolls the varint write into a switch on the
// encoded byte length. Length = ceil(bits.Len64(u)/7), clamped to 1
// (for u == 0).
func appendVarlongSwitch(dst []byte, i int64) []byte {
	u := uint64(i)<<1 ^ uint64(i>>63)
	switch {
	case u < 1<<7:
		return append(dst, byte(u))
	case u < 1<<14:
		return append(dst, byte(u)|0x80, byte(u>>7))
	case u < 1<<21:
		return append(dst, byte(u)|0x80, byte(u>>7)|0x80, byte(u>>14))
	case u < 1<<28:
		return append(dst, byte(u)|0x80, byte(u>>7)|0x80, byte(u>>14)|0x80, byte(u>>21))
	case u < 1<<35:
		return append(dst, byte(u)|0x80, byte(u>>7)|0x80, byte(u>>14)|0x80, byte(u>>21)|0x80, byte(u>>28))
	case u < 1<<42:
		return append(dst, byte(u)|0x80, byte(u>>7)|0x80, byte(u>>14)|0x80, byte(u>>21)|0x80, byte(u>>28)|0x80, byte(u>>35))
	case u < 1<<49:
		return append(dst, byte(u)|0x80, byte(u>>7)|0x80, byte(u>>14)|0x80, byte(u>>21)|0x80, byte(u>>28)|0x80, byte(u>>35)|0x80, byte(u>>42))
	case u < 1<<56:
		return append(dst, byte(u)|0x80, byte(u>>7)|0x80, byte(u>>14)|0x80, byte(u>>21)|0x80, byte(u>>28)|0x80, byte(u>>35)|0x80, byte(u>>42)|0x80, byte(u>>49))
	case u < 1<<63:
		return append(dst, byte(u)|0x80, byte(u>>7)|0x80, byte(u>>14)|0x80, byte(u>>21)|0x80, byte(u>>28)|0x80, byte(u>>35)|0x80, byte(u>>42)|0x80, byte(u>>49)|0x80, byte(u>>56))
	default:
		return append(dst, byte(u)|0x80, byte(u>>7)|0x80, byte(u>>14)|0x80, byte(u>>21)|0x80, byte(u>>28)|0x80, byte(u>>35)|0x80, byte(u>>42)|0x80, byte(u>>49)|0x80, byte(u>>56)|0x80, byte(u>>63))
	}
}

// Avoid bench tear-down dominating the measurement: write into a reused
// buffer that's reset each iteration.

func benchVarlongOver(b *testing.B, samples []int64, fn func(dst []byte, x int64) []byte) {
	var buf [16]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(buf[:0], samples[i%len(samples)])
	}
}

// Representative magnitude buckets, one per varlong byte length.
var varlongSamples = []int64{
	0, 1, 63, 64, // 1-2 bytes
	1 << 10,                       // 3 bytes
	1 << 17,                       // 4 bytes
	1 << 24,                       // 5 bytes
	1 << 31,                       // 6 bytes
	1 << 38,                       // 7 bytes
	1 << 45,                       // 8 bytes
	1 << 52,                       // 9 bytes
	1 << 60,                       // 10 bytes
	-1, -64, -1 << 31, -(1 << 62), // negatives across magnitudes
}

func BenchmarkVarlong_Loop(b *testing.B)    { benchVarlongOver(b, varlongSamples, appendVarlongLoop) }
func BenchmarkVarlong_Switch(b *testing.B)  { benchVarlongOver(b, varlongSamples, appendVarlongSwitch) }
func BenchmarkVarlong_StdLib(b *testing.B)  { benchVarlongOver(b, varlongSamples, binary.AppendVarint) }
func BenchmarkVarlong_Current(b *testing.B) { benchVarlongOver(b, varlongSamples, appendVarlong) }

// Per-length micro-benchmarks let us see whether the switch's advantage
// is mostly on small values (where the loop's branch predicts poorly)
// or applies uniformly. Each sub-benchmark uses a single sample of the
// given length to make the loop perfectly predictable for both impls.

var varlongPerLength = []int64{
	0,       // 1 byte
	1 << 7,  // 2 bytes
	1 << 14, // 3 bytes
	1 << 21, // 4 bytes
	1 << 28, // 5 bytes
	1 << 35, // 6 bytes
	1 << 42, // 7 bytes
	1 << 49, // 8 bytes
	1 << 56, // 9 bytes
	1 << 62, // 10 bytes (max with sign bit)
}

func benchPerLen(b *testing.B, fn func(dst []byte, x int64) []byte) {
	var buf [16]byte
	for _, x := range varlongPerLength {
		n := bytesForVarlong(x)
		b.Run(itoaPad(n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fn(buf[:0], x)
			}
		})
	}
}

func BenchmarkVarlongPerLen_Loop(b *testing.B)   { benchPerLen(b, appendVarlongLoop) }
func BenchmarkVarlongPerLen_Switch(b *testing.B) { benchPerLen(b, appendVarlongSwitch) }
func BenchmarkVarlongPerLen_StdLib(b *testing.B) { benchPerLen(b, binary.AppendVarint) }

// bytesForVarlong returns the number of bytes the zigzag varlong
// encoding of x will occupy (1..10). Used by the per-length labels.
func bytesForVarlong(x int64) int {
	u := uint64(x)<<1 ^ uint64(x>>63)
	if u == 0 {
		return 1
	}
	return (bits.Len64(u) + 6) / 7
}

func itoaPad(n int) string {
	if n < 10 {
		return string([]byte{byte('0' + n)})
	}
	return string([]byte{byte('0' + n/10), byte('0' + n%10)})
}

// Correctness check across all 10 byte-length buckets.
func TestVarlongShapesAgree(t *testing.T) {
	cases := append([]int64{}, varlongSamples...)
	cases = append(cases, varlongPerLength...)
	cases = append(cases, -(1 << 62), 1<<62, -1<<63, 1<<63-1)
	for _, x := range cases {
		want := appendVarlongLoop(nil, x)
		gotSwitch := appendVarlongSwitch(nil, x)
		gotStd := binary.AppendVarint(nil, x)
		if string(want) != string(gotSwitch) {
			t.Fatalf("switch differs from loop for %d: loop=%x switch=%x", x, want, gotSwitch)
		}
		if string(want) != string(gotStd) {
			t.Fatalf("stdlib differs from loop for %d: loop=%x stdlib=%x", x, want, gotStd)
		}
	}
}
