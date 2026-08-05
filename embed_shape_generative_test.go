package avro

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
)

// ===========================================================================
// The generative adversarial-struct-shape net for SchemaFor's field selection.
//
// ONE generator (genStructuralShapes / genTagEdgeShapes), not hand cases. Every
// shape is a reflect.StructOf type built by crossing the axes that the embedded-
// field selection bugs lived in:
//
//   - diamond embeds            (a base reached through two arms at EQUAL depth)
//   - equal-depth collisions    (two DISTINCT types promoting the same name)
//   - repeated-type two-depth   (one type reached directly AND through an embed)
//   - embedded vs named fields  (a direct field colliding with a promoted one)
//   - tagged vs untagged         (a rename colliding with a promoted Go name)
//   - malformed / edge tags      (genTagEdgeShapes: inline-on-non-struct, name+
//                                 inline, decimal trailing junk, dash+options,
//                                 narrow-int default bounds, uuid/plain dedup)
//
// For every shape the net asserts the two field-mapping walkers AGREE:
//
//     SchemaFor's    collectFields  (schema_for.go)   -- the schema builder
//     the runtime's  typeFieldMapping (reflect.go)    -- shared by encode AND
//                                                         decode (ser/deser/
//                                                         json_codec/json_decode/
//                                                         resolve/unsafe)
//
// on (1) WHICH Go field each Avro name resolves to, and (2) the RESOLVED schema
// (exercised end-to-end through the real Encode/Decode path). The two diverging
// is the failure mode Family 5 keeps hitting (commits 692b039, a1c4b25,
// 6ce8257): a silently-picked wrong field, an embed pruned by a marked-forever
// visited map, an ambiguity one walker rejects and the other first-wins.
//
// Non-vacuity is NOT self-asserted: the walkers are cross-checked against an
// INDEPENDENT oracle — Go's own field promotion (reflect.FieldByName) for the
// untagged shapes, and a from-scratch precedence resolver (oracleResolve,
// validated against FieldByName on the untagged shapes) for the tagged ones.
// If the two walkers ever drifted in lockstep, the FieldByName oracle still
// catches it. The neutering record at the bottom of this file documents the
// exact reverts that turn cells red, and what they were measured to do.
//
// The eager/lazy split is part of the contract, not a divergence: SchemaFor
// REJECTS any ambiguous collision (it must emit every field), while the runtime
// defers — it errors only when a schema field actually RESOLVES to an ambiguous
// name, so a coincidental collision the schema never references does not break
// the struct. The net asserts BOTH halves.
// ===========================================================================

// ---- carrier alphabet -----------------------------------------------------
//
// Exported (reflect.StructOf rejects unexported embedded fields) named types,
// each promoting an "N" field at a controlled depth through a controlled type,
// so subsets of them embedded as siblings synthesize every structural family.

type GA struct{ N int32 } // untagged N, depth 1 when embedded
type GB struct{ N int32 } // DISTINCT type, also untagged N
type GTag struct {
	M int32 `avro:"N"`
}                            // TAGGED N (Go field "M")
type GMid struct{ GA }       // N one level deeper
type GDeep struct{ GMid }    // N two levels deeper
type GBase struct{ N int32 } // diamond base
type GL struct{ GBase }      // diamond arm L
type GR struct{ GBase }      // diamond arm R

func structuralCarriers() []reflect.Type {
	return []reflect.Type{
		reflect.TypeFor[GA](), reflect.TypeFor[GB](), reflect.TypeFor[GTag](),
		reflect.TypeFor[GMid](), reflect.TypeFor[GDeep](),
		reflect.TypeFor[GBase](), reflect.TypeFor[GL](), reflect.TypeFor[GR](),
	}
}

// embedName is the field name an anonymous embed of ct must carry (the
// unqualified type name; the element name for a pointer embed).
func embedName(ct reflect.Type) string {
	if ct.Kind() == reflect.Pointer {
		return ct.Elem().Name()
	}
	return ct.Name()
}

func anonEmbed(ct reflect.Type) reflect.StructField {
	return reflect.StructField{Name: embedName(ct), Type: ct, Anonymous: true}
}

// ---- schemaForType: a faithful, reflect.Type-driven replica of SchemaFor ---
//
// SchemaFor is generic (SchemaFor[T]); the generator produces reflect.Type at
// run time, which a generic call cannot take. This mirrors SchemaFor's body
// exactly (the only addition: a synthetic name for the unnamed StructOf top
// type, which the real WithName supplies). TestGenerative_SchemaForReplicaParity
// pins it byte-identical to the real SchemaFor on named anchor types so it
// cannot silently drift from the entry point under test.
func schemaForType(t reflect.Type, opts ...SchemaOpt) (*Schema, error) {
	var o schemaOpts
	var customTypes []CustomType
	for _, opt := range opts {
		switch v := opt.(type) {
		case withNamespace:
			o.namespace = string(v)
		case withName:
			o.name = string(v)
		case CustomType:
			customTypes = append(customTypes, v)
		}
	}
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("avro: SchemaFor requires a struct type, got %s", t)
	}
	name := o.name
	if name == "" {
		name = t.Name()
	}
	if name == "" {
		name = "GenRec"
	}
	seen := make(map[reflect.Type]seenForm)
	s, err := inferRecord(t, name, o.namespace, seen, customTypes, make(appliedTypeAliases))
	if err != nil {
		return nil, err
	}
	s, err = dedupNamedTypes(s, make(map[string]string), "")
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("avro: marshaling inferred schema: %w", err)
	}
	return Parse(string(b), opts...)
}

// ---- the independent oracle ------------------------------------------------
//
// oracleResolve computes, FROM SCRATCH, which Go field each Avro name resolves
// to and which names are ambiguous, using a naive per-path walk (a cycle guard
// on the on-path slice — per-path by construction, so it cannot share the
// marked-forever prune bug) and the DOCUMENTED precedence rule applied to the
// full candidate set at once (gather-all-then-argmin), as opposed to the two
// walkers' single-pass iterative dedup. It calls NEITHER collectFields NOR
// typeFieldMapping. Its structural walk is validated against reflect.FieldByName
// (Go's own promotion) on every untagged shape; the only logic FieldByName does
// not cover — tagged-beats-untagged — is small and additionally pinned by the
// kept hand regressions.

type oracleCand struct {
	index  []int
	tagged bool
}

type oracleResult struct {
	names     []string // every resolvable Avro name, sorted
	winner    map[string][]int
	ambiguous map[string]bool
	cands     map[string][]oracleCand // every physical occurrence per name
}

// minimalTag splits an avro tag the way a reader that only needs name + inline
// would: a plain comma split. The structural family uses only bracket-free
// tags (rename, "-", ",inline", ",omitzero"), so this matches splitTag without
// borrowing its code — keeping the oracle independent.
func minimalTag(tag string) (name string, opts []string) {
	parts := strings.Split(tag, ",")
	return parts[0], parts[1:]
}

func oracleResolve(t reflect.Type) oracleResult {
	cands := map[string][]oracleCand{}
	var walk func(t reflect.Type, index []int, onPath []reflect.Type)
	walk = func(t reflect.Type, index []int, onPath []reflect.Type) {
		if slices.Contains(onPath, t) {
			return // cycle: this type is already on the current path
		}
		onPath = append(onPath, t)
		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)
			idx := append(append([]int(nil), index...), i)
			tag := sf.Tag.Get("avro")
			if sf.Anonymous {
				ft := sf.Type
				if ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				if ft.Kind() == reflect.Struct {
					if tag == "-" {
						continue
					}
					name, _ := minimalTag(tag)
					if name != "" {
						// Explicit name on an embed: a single named field, not flattened.
						cands[name] = append(cands[name], oracleCand{idx, true})
						continue
					}
					walk(ft, idx, onPath)
					continue
				}
				if !sf.IsExported() {
					continue
				}
			} else if !sf.IsExported() {
				continue
			}
			if tag == "-" {
				continue
			}
			name, opts := minimalTag(tag)
			if slices.Contains(opts, "inline") {
				ft := sf.Type
				if ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				if ft.Kind() == reflect.Struct {
					walk(ft, idx, onPath)
					continue
				}
			}
			tagged := name != ""
			if name == "" {
				name = sf.Name
			}
			cands[name] = append(cands[name], oracleCand{idx, tagged})
		}
	}
	walk(t, nil, nil)

	res := oracleResult{
		winner:    map[string][]int{},
		ambiguous: map[string]bool{},
		cands:     cands,
	}
	for name, cs := range cands {
		// Tagged beats untagged at ANY depth: if any candidate is tagged,
		// only tagged candidates remain in contention.
		anyTagged := false
		for _, c := range cs {
			if c.tagged {
				anyTagged = true
				break
			}
		}
		pool := cs[:0:0]
		for _, c := range cs {
			if c.tagged == anyTagged {
				pool = append(pool, c)
			}
		}
		// Among the pool, the shallowest (shortest index) wins; a tie at the
		// shallowest depth is genuinely ambiguous.
		minDepth := len(pool[0].index)
		for _, c := range pool {
			if len(c.index) < minDepth {
				minDepth = len(c.index)
			}
		}
		var atMin []oracleCand
		for _, c := range pool {
			if len(c.index) == minDepth {
				atMin = append(atMin, c)
			}
		}
		if len(atMin) == 1 {
			res.winner[name] = atMin[0].index
		} else {
			res.ambiguous[name] = true
		}
		res.names = append(res.names, name)
	}
	sort.Strings(res.names)
	return res
}

// ---- generated structural shapes -------------------------------------------

type genShape struct {
	label     string
	t         reflect.Type
	hasTag    bool // any avro tag anywhere -> FieldByName oracle applies only when false
	hasInline bool // ,inline-flattened fields have no Go-promotion analog -> skip FieldByName
}

// genStructuralShapes crosses: every ordered subset (size 1..3) of the carrier
// alphabet as the embeds; an optional direct field colliding on "N" (untagged,
// or tagged via a differently-named Go field) placed BEFORE or AFTER the
// embeds; an optional clean non-colliding "Keep" field; and a pointer variant
// (the first embed made a pointer). Names per shape are a subset of {N, Keep}.
func genStructuralShapes() []genShape {
	carriers := structuralCarriers()
	var embedArrangements [][]reflect.Type
	var gen func(prefix []reflect.Type, used map[string]bool)
	gen = func(prefix []reflect.Type, used map[string]bool) {
		if len(prefix) >= 1 {
			embedArrangements = append(embedArrangements, append([]reflect.Type(nil), prefix...))
		}
		if len(prefix) == 3 {
			return
		}
		for _, c := range carriers {
			if used[embedName(c)] {
				continue // two embeds cannot share a Go field name
			}
			used[embedName(c)] = true
			gen(append(prefix, c), used)
			delete(used, embedName(c))
		}
	}
	gen(nil, map[string]bool{})

	type directOpt struct {
		label string
		field *reflect.StructField // nil = none
		tag   bool
	}
	i32 := reflect.TypeFor[int32]()
	directOpts := []directOpt{
		{"noDirect", nil, false},
		{"directN", &reflect.StructField{Name: "N", Type: i32}, false},
		{"directTagN", &reflect.StructField{Name: "Dir", Type: i32, Tag: `avro:"N"`}, true},
	}
	keepField := reflect.StructField{Name: "Keep", Type: i32}

	var shapes []genShape
	for _, arr := range embedArrangements {
		// inl renders the carriers as ,inline-flattened NAMED fields instead of
		// anonymous embeds — the other flattening mechanism. The collision tree
		// is identical (both walk into the carrier at the same index), but inline
		// has no Go-promotion analog, so FieldByName cannot oracle it; oracleResolve
		// (validated against FieldByName on the anonymous-embed shapes) + the
		// two-walker agreement carry it.
		for _, inl := range []bool{false, true} {
			for _, ptr := range []bool{false, true} {
				embeds := make([]reflect.StructField, len(arr))
				for i, c := range arr {
					ct := c
					if ptr && i == 0 {
						ct = reflect.PointerTo(c)
					}
					if inl {
						embeds[i] = reflect.StructField{Name: fmt.Sprintf("Inl%d", i), Type: ct, Tag: `avro:",inline"`}
					} else {
						embeds[i] = anonEmbed(ct)
					}
				}
				for _, d := range directOpts {
					positions := []string{"after"}
					if d.field != nil {
						positions = []string{"before", "after"}
					}
					for _, pos := range positions {
						for _, keep := range []bool{false, true} {
							var fields []reflect.StructField
							addDirect := func() {
								if d.field != nil {
									fields = append(fields, *d.field)
								}
								if keep {
									fields = append(fields, keepField)
								}
							}
							if pos == "before" {
								addDirect()
								fields = append(fields, embeds...)
							} else {
								fields = append(fields, embeds...)
								addDirect()
							}
							st := reflect.StructOf(fields)
							hasTag := d.tag || inl // ,inline is itself a tag
							for _, c := range arr {
								if c == reflect.TypeFor[GTag]() {
									hasTag = true
								}
							}
							names := make([]string, 0, len(arr))
							for _, c := range arr {
								names = append(names, c.Name()[:1])
							}
							label := fmt.Sprintf("carriers=%v inline=%v ptr=%v %s/%s keep=%v",
								names, inl, ptr, d.label, pos, keep)
							shapes = append(shapes, genShape{label: label, t: st, hasTag: hasTag, hasInline: inl})
						}
					}
				}
			}
		}
	}
	return shapes
}

// ---- value plumbing for the round-trip ------------------------------------

// setLeafInt sets the int32 at index, allocating any nil pointer along the path
// (e.g. a ,inline *struct field, which allocPointers — anonymous-only — skips).
func setLeafInt(structVal reflect.Value, index []int, v int32) {
	fv := structVal
	for _, i := range index {
		for fv.Kind() == reflect.Pointer {
			if fv.IsNil() {
				fv.Set(reflect.New(fv.Type().Elem()))
			}
			fv = fv.Elem()
		}
		fv = fv.Field(i)
	}
	fv.SetInt(int64(v))
}

// readLeafInt reads the int32 at index, returning 0 when a pointer along the
// path is nil (a field the decoder legitimately never allocated).
func readLeafInt(structVal reflect.Value, index []int) int32 {
	fv := structVal
	for _, i := range index {
		for fv.Kind() == reflect.Pointer {
			if fv.IsNil() {
				return 0
			}
			fv = fv.Elem()
		}
		fv = fv.Field(i)
	}
	return int32(fv.Int())
}

func intRecord(names []string) string {
	var b strings.Builder
	b.WriteString(`{"type":"record","name":"R","fields":[`)
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"name":%q,"type":"int"}`, n)
	}
	b.WriteString(`]}`)
	return b.String()
}

// ---- the net ---------------------------------------------------------------

func TestGenerative_EmbedShapeWalkerAgreement(t *testing.T) {
	shapes := genStructuralShapes()
	var checkedWinners, checkedAmbig, roundTripped, fieldByNameChecks int

	for _, sh := range shapes {
		or := oracleResolve(sh.t)
		anyAmbig := len(or.ambiguous) > 0

		// (A) Validate the oracle against Go's own promotion for every name
		//     with no tagged candidate (pure Go-promotion question). Skipped for
		//     ,inline shapes: inline flattening has no Go-promotion analog (Go
		//     does not promote through a non-anonymous field), so FieldByName
		//     would not find the flattened name — oracleResolve (validated here
		//     on the anonymous-embed shapes) and the two-walker agreement carry
		//     the inline shapes instead.
		for _, n := range or.names {
			if sh.hasInline {
				break
			}
			tagged := false
			for _, c := range or.cands[n] {
				if c.tagged {
					tagged = true
				}
			}
			if tagged {
				continue
			}
			fbn, ok := sh.t.FieldByName(n)
			fieldByNameChecks++
			if or.ambiguous[n] {
				if ok {
					t.Fatalf("%s: oracle says %q ambiguous but reflect.FieldByName resolved it to %v", sh.label, n, fbn.Index)
				}
			} else {
				if !ok {
					t.Fatalf("%s: oracle resolved %q to %v but reflect.FieldByName abstained (ambiguous)", sh.label, n, or.winner[n])
				}
				if !reflect.DeepEqual(fbn.Index, or.winner[n]) {
					t.Fatalf("%s: oracle %q=%v disagrees with reflect.FieldByName=%v", sh.label, n, or.winner[n], fbn.Index)
				}
			}
		}

		// (B) collectFields (SchemaFor's walker): eager-rejects any ambiguity,
		//     else resolves every name to the oracle's winner.
		cf, cfErr := collectFields(sh.t, make(map[reflect.Type]bool))
		if anyAmbig {
			if cfErr == nil {
				t.Fatalf("%s: collectFields accepted an ambiguous shape (oracle ambiguous: %v)", sh.label, ambigNames(or))
			}
		} else {
			if cfErr != nil {
				t.Fatalf("%s: collectFields rejected an unambiguous shape: %v", sh.label, cfErr)
			}
			cfMap := map[string][]int{}
			for _, f := range cf {
				cfMap[f.name] = f.index
			}
			if len(cfMap) != len(or.names) {
				t.Fatalf("%s: collectFields names %v != oracle names %v", sh.label, sortedKeys(cfMap), or.names)
			}
			for _, n := range or.names {
				if !reflect.DeepEqual(cfMap[n], or.winner[n]) {
					t.Fatalf("%s: collectFields %q=%v != oracle %v", sh.label, n, cfMap[n], or.winner[n])
				}
			}
		}

		// (C) typeFieldMapping (the runtime walker): per-name lazy resolution.
		for _, n := range or.names {
			m, err := typeFieldMapping([]string{n}, nil, sh.t)
			if or.ambiguous[n] {
				if err == nil {
					t.Fatalf("%s: typeFieldMapping([%q]) accepted an ambiguous name", sh.label, n)
				}
				checkedAmbig++
			} else {
				if err != nil {
					t.Fatalf("%s: typeFieldMapping([%q]) rejected a resolvable name: %v", sh.label, n, err)
				}
				if !reflect.DeepEqual(m.indices[0], or.winner[n]) {
					t.Fatalf("%s: typeFieldMapping %q=%v != oracle %v", sh.label, n, m.indices[0], or.winner[n])
				}
				checkedWinners++
			}
		}

		// (C2) typeFieldMapping over ALL names at once mirrors collectFields'
		//      verdict: ambiguous -> reject, else resolve every name.
		mAll, errAll := typeFieldMapping(or.names, nil, sh.t)
		if anyAmbig {
			if errAll == nil {
				t.Fatalf("%s: typeFieldMapping(all names) accepted despite an ambiguous name", sh.label)
			}
		} else {
			if errAll != nil {
				t.Fatalf("%s: typeFieldMapping(all names) rejected: %v", sh.label, errAll)
			}
			for i, n := range or.names {
				if !reflect.DeepEqual(mAll.indices[i], or.winner[n]) {
					t.Fatalf("%s: typeFieldMapping(all) %q=%v != oracle %v", sh.label, n, mAll.indices[i], or.winner[n])
				}
			}
		}

		// (D) Resolved schema + end-to-end round trip.
		if !anyAmbig {
			s, err := schemaForType(sh.t, WithName("R"))
			if err != nil {
				t.Fatalf("%s: schemaForType rejected an unambiguous shape: %v", sh.label, err)
			}
			gotNames := map[string]bool{}
			for _, f := range s.Root().Fields {
				gotNames[f.Name] = true
			}
			if len(gotNames) != len(or.names) {
				t.Fatalf("%s: schema field names %v != oracle %v", sh.label, gotNames, or.names)
			}
			roundTripWinners(t, sh, s, or)
			roundTripped++
		} else {
			// Lazy contract: a schema over only the NON-ambiguous names still
			// round-trips (the coincidental collision does not break the
			// struct); a schema over an ambiguous name rejects on encode AND
			// decode (parity), never silently first-wins.
			var clean []string
			for _, n := range or.names {
				if !or.ambiguous[n] {
					clean = append(clean, n)
				}
			}
			if len(clean) > 0 {
				cs := MustParse(intRecord(clean))
				roundTripWinners(t, sh, cs, restrict(or, clean))
			}
			as := MustParse(intRecord([]string{firstAmbig(or)}))
			src := reflect.New(sh.t)
			allocPointers(src.Elem())
			if _, err := as.AppendEncode(nil, src.Interface()); err == nil {
				t.Fatalf("%s: encode must reject a schema resolving to ambiguous %q", sh.label, firstAmbig(or))
			}
			wire, _ := as.AppendEncode(nil, map[string]any{firstAmbig(or): int32(1)})
			dst := reflect.New(sh.t)
			if _, err := as.Decode(wire, dst.Interface()); err == nil {
				t.Fatalf("%s: decode must reject a schema resolving to ambiguous %q (parity)", sh.label, firstAmbig(or))
			}
		}
	}

	if checkedWinners < 100 || checkedAmbig < 50 || roundTripped < 100 {
		t.Fatalf("generator under-covered: winners=%d ambig=%d roundtrips=%d shapes=%d — generation regressed",
			checkedWinners, checkedAmbig, roundTripped, len(shapes))
	}
	t.Logf("structural net: %d shapes | %d winner resolutions | %d ambiguity rejections | %d round trips | %d FieldByName cross-checks",
		len(shapes), checkedWinners, checkedAmbig, roundTripped, fieldByNameChecks)
}

// roundTripWinners proves, through the REAL Encode/Decode path (which consumes
// typeFieldMapping in ser.go / deser.go), that encode reads the winning field
// and decode writes it — never a shadowed loser. For each name it sets the
// winner to a sentinel and every loser to a distinct decoy; encode-then-decode-
// to-map must yield the sentinel (encode read the winner). Then it encodes a
// known map and decodes into a fresh struct: the winner must hold the value and
// every loser must stay zero (decode wrote the winner).
func roundTripWinners(t *testing.T, sh genShape, s *Schema, or oracleResult) {
	t.Helper()
	src := reflect.New(sh.t)
	allocPointers(src.Elem())
	sentinel := map[string]int32{}
	k := int32(0)
	for _, n := range or.names {
		k++
		sentinel[n] = 1000 + k
		decoy := int32(0)
		for _, c := range or.cands[n] {
			if reflect.DeepEqual(c.index, or.winner[n]) {
				setLeafInt(src.Elem(), c.index, sentinel[n])
			} else {
				decoy--
				setLeafInt(src.Elem(), c.index, -100+decoy)
			}
		}
	}
	data, err := s.AppendEncode(nil, src.Interface())
	if err != nil {
		t.Fatalf("%s: encode: %v", sh.label, err)
	}
	var out map[string]any
	if _, err := s.Decode(data, &out); err != nil {
		t.Fatalf("%s: decode-to-map: %v", sh.label, err)
	}
	for _, n := range or.names {
		if got, _ := out[n].(int32); got != sentinel[n] {
			t.Fatalf("%s: encode read a non-winner for %q: got %v want %d (winner index %v)",
				sh.label, n, out[n], sentinel[n], or.winner[n])
		}
	}

	wireVals := map[string]int32{}
	wm := map[string]any{}
	for i, n := range or.names {
		wireVals[n] = 2000 + int32(i)
		wm[n] = wireVals[n]
	}
	wire, err := s.AppendEncode(nil, wm)
	if err != nil {
		t.Fatalf("%s: encode map: %v", sh.label, err)
	}
	dst := reflect.New(sh.t)
	if _, err := s.Decode(wire, dst.Interface()); err != nil {
		t.Fatalf("%s: decode into struct: %v", sh.label, err)
	}
	for _, n := range or.names {
		for _, c := range or.cands[n] {
			got := readLeafInt(dst.Elem(), c.index)
			if reflect.DeepEqual(c.index, or.winner[n]) {
				if got != wireVals[n] {
					t.Fatalf("%s: decode did not write winner %q@%v: got %d want %d", sh.label, n, c.index, got, wireVals[n])
				}
			} else if got != 0 {
				t.Fatalf("%s: decode wrote a non-winner %q@%v: got %d want 0", sh.label, n, c.index, got)
			}
		}
	}
}

func ambigNames(or oracleResult) []string {
	var a []string
	for n := range or.ambiguous {
		a = append(a, n)
	}
	sort.Strings(a)
	return a
}

func firstAmbig(or oracleResult) string { return ambigNames(or)[0] }

func restrict(or oracleResult, names []string) oracleResult {
	r := oracleResult{winner: map[string][]int{}, ambiguous: map[string]bool{}, cands: map[string][]oracleCand{}}
	for _, n := range names {
		r.names = append(r.names, n)
		r.winner[n] = or.winner[n]
		r.cands[n] = or.cands[n]
	}
	return r
}

func sortedKeys(m map[string][]int) []string {
	var k []string
	for s := range m {
		k = append(k, s)
	}
	sort.Strings(k)
	return k
}

// TestGenerative_SchemaForReplicaParity pins schemaForType byte-identical to the
// real generic SchemaFor on named anchors, so the generator's schema builder
// cannot drift from the entry point under test.
func TestGenerative_SchemaForReplicaParity(t *testing.T) {
	type Inner struct {
		P int32  `avro:"p"`
		Q string `avro:"q"`
	}
	type Anchor struct {
		A int32             `avro:"a"`
		B string            `avro:"b"`
		C Inner             `avro:"c"`
		D []int64           `avro:"d"`
		E map[string]string `avro:"e"`
		F *int32            `avro:"f"`
	}
	real, err := SchemaFor[Anchor]()
	if err != nil {
		t.Fatalf("real SchemaFor: %v", err)
	}
	rep, err := schemaForType(reflect.TypeFor[Anchor]())
	if err != nil {
		t.Fatalf("replica: %v", err)
	}
	if real.String() != rep.String() {
		t.Fatalf("replica drift:\n real=%s\n repl=%s", real.String(), rep.String())
	}
}

// ---- neutering record (non-vacuity proof) ----------------------------------
//
// This net is proven to FAIL when each Family-5 fix is reverted in the
// production walkers. Measured over the 16000 generated structural shapes with a
// temporary count-don't-fatal harness (the live test fatals at the first red
// cell). With both fixes intact all four counts below are 0.
//
//	NEUTER-1  Remove `defer delete(visited, t)` from BOTH walkers
//	          (reflect.go + schema_for.go) — revert 6ce8257, restoring the
//	          marked-forever visited map:
//	            collectFields wrong-winner ......... 200 shapes (100 of them inline)
//	            collectFields accepted-ambiguous ... 304 shapes
//	            typeFieldMapping mirrors both (200 / 304)
//	          A type reached through two embed paths has its SHALLOW occurrence
//	          pruned, so the deeper field wins (caught by the FieldByName oracle
//	          on the embed shapes and by oracleResolve on the inline shapes, where
//	          FieldByName does not apply — hence the 100 inline reds); a diamond's
//	          second arm is pruned, so the collision is silently first-won instead
//	          of flagged ambiguous.
//
//	NEUTER-2  Drop the equal-depth `ambiguous[...]` mark in BOTH walkers
//	          (revert 692b039 + a1c4b25), restoring silent first-win:
//	            collectFields accepted-ambiguous ... 912 shapes
//	            typeFieldMapping accepted-ambiguous  912 names
//	          Every ambiguous shape the net asserts must reject is silently
//	          first-won instead. 912 == the net's own ambiguity-rejection count,
//	          i.e. EVERY ambiguous cell goes red.
