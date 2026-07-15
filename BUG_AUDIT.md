# Audit framework and findings record — twmb/avro

This document records the audit framework, recurring bug patterns, structural blind spots, and intentional divergences for the Go Avro library at `/Users/travisbischel/src/twmb/avro`. It is a reference: the patterns and lessons are descriptive of what audit rounds have found and how the framework operates.

The library's correctness target is unambiguous best-in-class: byte-for-byte compatibility with the Apache Avro reference (Java) where the spec is precise, sane behavior where the spec is silent, and no observable bugs.

## Before changing behavior — required pre-action gate

**Read this section every time you are about to propose or apply a behavior change. The checks below are not advisory; skipping them is a process failure regardless of whether the proposed change happens to be correct.**

A "behavior change" is any code edit that tightens, loosens, or otherwise alters what the encoder accepts, what the decoder accepts, what gets rejected, what gets coerced, what the metadata API surfaces, or what wire bytes get produced. Anything that would invalidate an existing user's call site is a behavior change. Pure DRY refactors that preserve identical behavior are not.

For every proposed behavior change, produce — *before* writing the edit — the following artifact:

```
Pre-tightening gate:
  Site(s): <file:line — every site this change touches>
  Pre-change behavior: <what the code does today, quoted>
  Proposed post-change behavior: <what the code will do after, quoted>
  Direction: <tighten | loosen | symmetric | new path>

  Documented-intentional search (REQUIRED — quote verbatim, do not paraphrase):
    grep ran: <the exact grep commands you ran>
    BUG_AUDIT.md §Known intentional divergences matches:
      - <quoted entry, or "none" with the search patterns you used>
    BUG_AUDIT.md other rationale entries (DRY history, lesson bullets, structural-blind-spots):
      - <quoted, or "none">
    doc.go / package docs matches:
      - <quoted, or "none">
    In-test pins with "Documenting" / "Intentional" / "Asymmetry" comments:
      - <quoted, or "none">
    Recent commit messages for the touched files (`git log --oneline -- <file>` last 30):
      - <SHA + subject for any commit naming the touched area; or "none">
    Recent-commit pickaxe since the latest landed patch-set on main (REQUIRED — anti-ping-pong check):
      base: <the most recent large patch/merge commit on main (`git log --oneline --shortstat main` to spot it), SHA + subject>
      pickaxes ran: <`git log --oneline -S '<exact identifier / predicate / error string being changed>' <base>^..HEAD -- <file>` and/or `-G '<regex>'` — pickaxe the EXACT code being changed, not the general area; one pickaxe per identifier the edit touches>
      hits: <for every hit, SHA + subject + one line classifying what that commit did to this code: behavior-introduction | refactor/DRY | error-echo bounding | comment-only; or "none — predates the patch-set">
      verdict: <introduced-deliberately-in-range | only-refactored-in-range | untouched-in-range>

      If "introduced-deliberately-in-range": do not silently apply the change, and do not silently drop it either — open a correct-behavior discussion with the maintainer before any edit. Quote the introducing commit's rationale, state what the proposed change would do differently and the evidence for it (spec text, Java/fastavro behavior, observable breakage), and lay out the trade-off between the two behaviors. The recent commit is evidence of intent, not proof of correctness; the proposed fix is evidence of a problem, not license to revert. The discussion decides which behavior is right — what the gate prevents is the silent ping-pong, not the reversal itself.

  Verdict: <documented as intentional | not documented | documented but contradicted by new evidence>

  If "documented as intentional": STOP. Do not apply the change. Surface the policy to the user and ask whether to propose a rationale change.
  If "not documented": proceed; treat the change as a new policy decision and note the new pin/doc entry it requires.
  If "documented but contradicted": cite the new evidence (deployed Java/fastavro divergence, real-world user report, cross-impl source) and ask the user before proceeding.
```

This applies *recursively*: any time the gate's "verdict" line would be "documented as intentional," the change is blocked. No second-guessing, no "but symmetry would be cleaner," no "the documented behavior looks like a bug." Documented intentional behavior is the maintainer's policy decision; the gate's job is to surface it, not override it.

The gate travels with DELEGATION: any sub-agent prompt that can produce findings must point the agent at §Known intentional divergences (and the in-test "Documenting"/"Intentional" pins) and require the quoted-search verdict per finding. An agent that isn't handed the gate re-files documented behavior as its headline finding — observed in a parallel fan-out where one agent filed the documented bare-union first-match dispatch as a new divergence while a sibling agent, which had read this file on its own initiative, correctly gated the identical finding as documented-intentional and declined to file it.

**Why this section exists.** Audit rounds have repeatedly proposed (and sometimes applied) tightenings that contradicted explicitly-documented policy. The framework's "Findings that don't count" section *describes* the policy in passive terms; auditors read the section, agree with it, then skip the check at the actual decision point. The pre-action gate is the same content reframed as a mandatory artifact you produce *before* writing the edit — if you cannot produce the quoted documented-intentional search, you have not yet earned the right to apply the change.

### Known cross-cutting policies — search FIRST, change LAST

These policies cross many sites and are exactly the ones audit rounds have re-litigated. Before changing *any* behavior touching these areas, grep the named entries below and quote them.

- **json.Number** is a cross-cutting policy area. The deliberate design:
  - **The Avro schema is the contract.** For numeric schemas (int/long/float/double, including their logical-type variants like date / timestamp-* / time-*), json.Number carries the raw numeric wire value as a JSON-number-valid string — logical-type formatting is bypassed for json.Number targets so the round-trip preserves the numeric content. **json.Number is numeric-only: it is rejected for stringy schemas (string/bytes/fixed/enum) on BOTH encode and decode** — it is a numeric carrier (RFC 8259), so a text/binary target is a type mismatch (use a Go string or []byte there). The reject lives in one shared encode guard (`rejectJSONNumberRawTarget`, mirroring `appendAvroString`'s string-schema reject) and one shared decode guard (`rejectJSONNumberStringTarget`). Map keys are the one content-aware exception: validated per-key on both sides (encode and decode) so `map[json.Number]V` round-trips when all keys are valid JSON numbers — because Avro map keys are strings whose json.Number form must itself be numeric.
  - **json.Number's content must always be a valid JSON number literal.** Per the type's stdlib contract: `json.Marshal(json.Number(x))` rejects any underlying string that isn't a valid RFC 8259 number. Decode rejects when the wire content has no valid JSON-number representation (non-finite floats, arbitrary text from stringy wires that doesn't parse as a number, non-numeric map keys); encode rejects when json.Number's source content doesn't parse as the schema's numeric type or doesn't satisfy the map-key invariant.

  Required entries to read and quote before any json.Number change (titles are stable; grep by these exact strings):
  - doc.go: `Encoding from JSON input` section.
  - BUG_AUDIT.md §Known intentional divergences:
    - `Whole-number floats encode against int / long schemas`
    - `Encode into float / double is lossy by destination`
    - `json.Number content is validated against the schema's contract`
  - In-test: `TestRegression_JSONNumberTargetRejectedForStringLikeWire` (decode reject, all stringy), `TestRegression_JSONNumberStringSourceRejectedOnEncode` (encode reject, all stringy incl bytes/fixed), `TestRegression_JSONNumberMapKeyRejected`, `TestJSONNumberMapKeyContentValidated`, `TestLogicalNumericTargetJSONNumberWritesRawWire`, `TestInvariant_EncodeDecodeTargetParity` (executable encode/decode parity matrix), `TestRegression_UnionDispatchMatrix` (the hex-json.Number case now rejects: numeric-only, no branch accepts it).

- **DOS-resistance caps** (`maxZeroByteItems`, `maxMapPreAllocSize`, `maxIndirectDepth`, `decimalScaleLimit`, `maxRatInputLen`, `WithMaxBlockBytes`, `errTooDeep`) are documented as intentional defense-in-depth. Don't propose relaxing them without quoting the entry.

- **Resolution fail-fast** (writer-union → reader, schema-resolution divergences vs Java's per-branch ErrorAction): documented intentional. Don't propose deferring to decode-time.

- **Decimal/big-decimal canonical handling, big.Rat scale derivation, BigDecimal trailing-zero collapse**: documented intentional. Don't propose changing without reading the entry.

- **Local-timestamp wall-clock-as-UTC, logical-type mismatch under resolution**: documented intentional Java parity. Don't propose changing.

- **Schema metadata numerics normalize (Props value-based dispatch; Fields[].Default schema-width-faithful narrowing), encode into float / double is lossy by destination, fixed/bytes encode-side leniency on string sources**: documented intentional. Don't propose changing.

If you find a documented entry that addresses your proposed change, your job is to surface the policy and ask the user, not to override it. If you cannot find an entry, the gate's "verdict" is "not documented" — proceed as a new policy decision, and note that the change requires a new pin or doc entry.

## Scope — read this first

**The audit ALWAYS targets the entire current state of the codebase, never a branch, never a diff, never a recent fix.** The current git branch is irrelevant. The branch name is irrelevant. The set of files modified since `main` is irrelevant. The set of commits on the working branch is irrelevant. The audit walks `*.go` as it sits on disk *right now* and asks of every line "is this correct?" — not "did this change recently?"

Why this is non-negotiable: bugs do not correlate with recency. The 80-rounds-and-counting history of this codebase is overwhelmingly bugs that survived multiple rounds *until* an audit walked code that nobody had touched in months. Branch-focused audits miss them by construction. A bug introduced in a 2022 commit and a bug introduced in the most recent commit are equally in scope — *because they are equally bugs*. Recency is not evidence of risk; lack of recency is not evidence of safety.

**Do not start an audit with any of:**

- `git log` / `git log --oneline` / `git log -p` to see "what's recent"
- `git diff main` / `git diff main --stat` to see "what the branch changed"
- `git blame` to find "when this was written"
- Reading the branch name to infer "what to focus on"
- Quoting recent commit messages or commit SHAs as evidence of where bugs live
- Phrases like "the recent fix at X" / "what just landed" / "since a re-audit"

Each of these silently shrinks the audit's blast radius from the whole codebase to a few hundred lines. A round that starts there *ends* there — even when the auditor knows the rule, the framing biases what they probe. Refuse the framing at step zero.

**Git is allowed for:**

- Understanding *why* a piece of code is the way it is (after you've already identified the line as suspect).
- Reading a commit message's claim and verifying it against the current code (per hard rule 5 — comments and commit messages are claims, not evidence).
- Finding the original landing of a code shape to understand the surrounding context, *after* it's been flagged on its own merits.

The Convergence section below names recent rounds' findings only as a *catalog of bug shapes* — not as a map of where to look. Re-derive the candidate sites each round from the structural patterns; never from "what changed."

## Convergence

**An audit round converges when it comes back clean.** Every prior round has found something; the recurring bug *shapes* are catalogued as structural questions in §"Patterns that have produced real findings" and §"Structural blind spots." Use those as the method — never as a map of where to look (recency is not a signal; see §Scope). The convergence rule: bugs persist until checked.

Audit rounds bias toward finding real bugs over filler. "Verified" entries are valuable when they document a specific area exhaustively checked against the spec + at least one reference impl, with concrete inputs that could have failed. Empty verifications ("checked X, looks fine") are noise — a short clean report is preferable to padding.

The trap goes both ways: pattern-matching a comment or shape without proving the bug is one failure mode (see convention 5 on comment-as-evidence); assuming cleanness without verification is the other. Every `// matches Java's X` comment, every "the test name implies it's covered" assumption, and every "stdlib handles this" feeling is a hypothesis that needs testing before it counts as verified.

## The executable net runs first — but its flags are candidates, not findings

**Why this net exists.** The treadmill — N audit rounds each finding new things — was not bad luck. Example tests + self-consistency fuzzing (round-trip / no-panic) both encode the *author's belief* about correctness: the test that should catch a bug is drawn from the same mind that wrote it, and every reference to Java/fastavro in the code was a *comment*, never an executed oracle. So audits were a lottery over input *intersections* (decimal × CustomType × wrong-underlying; forward-ref × namespace × position; lax-names × cache × call-order) with no ground truth — variance, not convergence. Every real finding is one of two classes: **"disagrees with the reference"** (catchable only by a real-impl oracle) or **"path X drifted from path Y / edge input"** (catchable by cross-path differential + property fuzzing). Both are now automated — run that automation first.

`go test ./...`, plus — with `AVRO_FASTAVRO_PYTHON` set — the fastavro differential, and under `-tags=cisuite` the Java fingerprint differential. It machine-checks whole classes against *independent* oracles:

- **Encode/decode target-type parity (pattern 12)** — `TestInvariant_EncodeDecodeTargetParity` drives the real paths across a schema × Go-type matrix; any *undocumented* asymmetry fails the build.
- **Wire-format parity vs a foreign impl** — `TestDifferentialFastavro` / `TestDifferentialFastavroBinaryLogical` (fastavro) across primitives + bytes/fixed/decimal/uuid/timestamp.
- **Canonical form + Rabin fingerprint** — `TestApacheSchemaTestsVectors` (vendored Apache vectors) and `TestDifferentialJavaFingerprint` (Java `SchemaNormalization`).
- **Numeric boundaries, reflect-vs-unsafe byte identity, metadata↔wire, resolution promotion, SchemaFor round-trip, decimal round-trip, error-message DoS bounds** — the Tier-2 nets.
- **Combinatorial matrix (`TestMatrix_*`, matrix_*_test.go)** — ~4,000 cells crossing (29 schema fragments incl. every logical type, degenerate-cardinality types, and boundary values) × (14 composition contexts: record fields at 3 depths, arrays, maps, null-first/null-second/multibranch unions, containers-of-unions) × (two-level context pairs) × (9 recursive shapes × depths 0/1/3/17 × plain+tagged × typed-struct × promotion-inside-recursion) × (per-kind defaulted fields: JSON fill ≡ binary auto-fill ≡ explicit encode ≡ rebuilt-schema fill) × (same-token-class tagged unions) × (SchemaCache cross-parse refs) × (StructOf-assembled typed struct fields through the unsafe path × 4 positions). The core invariant is calibration-free: the binary decoder's output is the canonical form, and binary re-encode, JSON round-trip, JSON re-encode, `Root().Schema()` rebuild (fingerprint + both wires), and identity-resolve must all land byte-identically — so a new divergence anywhere in the cross-product fails without anyone having hand-written that cell. Found on landing: the `toJSONWalk` size-0 omission and the `setBytesValue` empty-bytes-nil branch flip (plus its string→bytes-promotion twin, transitively). Constraints the harness honors (they are documented behavior, not gaps): multi-entry maps assert value-equality not byte-identity (Go map iteration order); same-token-class unions round-trip only in tagged form (documented untagged first-match loss); union-padding branches are chosen token-class-distinct.

- **Mutation testing measures the NET, not the code — run it to find assertions the matrix is missing, not bugs.** Gremlins (`go-gremlins`) was run over the whole module (2,813 runnable mutants): it injects one operator/boundary/arithmetic mutation per build and reports which mutants the suite fails to kill. A SURVIVOR is a line you can break with every test still green — either a missing assertion (real gap → pin it) or an equivalent mutant (no observable behavior change → document, don't pin). The exercise found FOUR real assertion gaps the ~6,000-cell matrix + both oracles + fuzzers all missed, every one a **bounds/size-arithmetic edge observable only in a specific runtime state** (the class nothing relational can reach): (1) `boundedRatFromString` decimal-magnitude DoS gate shiftable a few units (deser.go); (2) `deserFixedArray` remaining-capacity bound — a multi-block array wire into a `[N]T` target panics under the mutant, invisible because the bound is inert at idx=0 so every single-block test passes it; (3) the OCF `zeroRun` zero-byte-record DoS cap **invertible** (`++`→`--` defeats the amplification guard entirely — the security-relevant one); (4) `appendAvroString`'s AppendText header-growth slow path (text ≥64 bytes crosses the 1→2-byte varint header boundary and grows/shifts `dst`; every AppendText test used short values so the grow branch had zero wire coverage — the mutant corrupts the next field). All four are pinned with neuter-proofed regression tests (`TestMatrix_DecimalStringMagnitudeBoundary`, `TestMatrix_MultiBlockIntoFixedArray`, `TestReaderZeroRunCapIndependentOfBlockLength` + `TestWriterBlockFramingContract`, `TestRegression_TextAppenderHeaderGrowth`). **The bug density was extreme in `ocf.go`** — 29 of 31 phase-2 survivors — because the package is tested almost entirely through `Encode→Decode` round-trips, which by construction cannot observe block STRUCTURE or a DEFEATED bound (every valid split is a readable file). The remaining ~22 OCF survivors after pinning the substantive gaps are `CONDITIONALS_BOUNDARY` mutants on DoS safety caps (block size, decompression, metadata limits — all `>`→`>=`, a ±1 shift on a 64 MiB margin that makes the guard one byte STRICTER, never defeats it; verified on a scratch copy that the zip-bomb test still passes the mutant) and option-normalization defaults — equivalent/negligible, NOT worth brittle exact-byte-boundary fixtures. **Operational lessons (the run is hostile to a laptop, see [[no-local-mutation-testing]]):** the stock binary leaks the per-mutant `go test` toolchain children on timeout (orphaned `compile`/`.test` under PID 1) AND fills disk via `GOTMPDIR`-into-sandbox build-temp accumulation (~44 G over a run); the durable fixes are a patched gremlins with process-GROUP kill on timeout (`Setpgid` + `cmd.Cancel = kill(-pgid)`) plus a watchdog that reaps stale `go-build*` dirs older than the per-mutant deadline. **Deadline calibration is the measurement's main distortion:** gremlins derives the per-mutant timeout from a WARM baseline, but cache trims force COLD recompiles that blow it — so a large fraction of "TIMED OUT" is a harness artifact, not a genuine hang, and the efficacy % over a timeout-heavy run is unreliable; the trustworthy signal is the confirmed KILLED and LIVED buckets. A SCOPED re-run (one file, `-E`-excluding the rest) with a high timeout-coefficient (40) and orphan cleanup produces 0 false timeouts and a clean per-file efficacy — the right way to re-measure a file after adding pins (the OCF re-run: 127 killed / 22 equivalent-survivors / 0 timed-out / 98.68% coverage). Re-run mutation testing after a round of new test axes to find what they still miss; treat survivors as the to-pin list, not as bugs.

  The commit-history-derived extension axes (each maps a historical fix CLASS to a generative axis): **CustomType parity** (matrix_custom_test.go — every logical fragment × 5 positions × {suppress, box, invocation-count} configs, raw forms CALIBRATED by a suppressing decode of the plain wire, never hand-computed; counts catch side-effect divergence value asserts can't see; the enriched-input reject under matching-Encode suppression on fixed/decimal builds is the documented per-build behavior, so count configs drive the raw tree); **evolution** (matrix_evolution_test.go — all 8 promotion pairs × every context; per-kind field DROP exercising every skipfn incl. recursive, also as array items; per-kind field ADD exercising the resolution default-fill against the reader's own JSON fill; field reorder; union reorder/widening/fail-fast-narrowing/two-pass-exact-beats-promotion; enum reader-default × positions; type/field/enum/fixed aliases incl. namespaced × positions; `resolveBoth` asserts Resolve ⇔ CheckCompatibility agree on every pair); **names** (matrix_names_test.go — namespace inheritance/short-vs-full refs/same-shortname-two-ns/dotted fullnames/namespaced recursion; forward refs at union/field/array/map fixup positions; the three documented field-level logicalType lift shapes incl. rebuild re-lift); **hostile wire** (matrix_hostile_test.go — DETERMINISTIC exhaustive truncation + per-byte corruption of every composed schema's wire, plus through a resolving schema with a dropped recursive-union-array field; pure no-panic invariant — unlike fuzzing this runs every prefix/mutant of every cell on every execution); **lenient input forms** (matrix_extensions_test.go — every accepted Go form of one value → byte-identical wire, per position); **metadata preservation** (doc/aliases/order/props/defaults through two generations of `Root().Schema()`); **lax names** (both wires + canonical validity + rebuild via `Schema(WithLaxNames(nil))`); **nil-equivalent encode shapes** (typed-nil/iface-nil/ptr-to-nil/nil-slice/nil-map → null branch across union arities/positions/wires); and an **SOE round-trip edge inside runCore** itself. Found on extension: `SchemaNode.Schema()` took no SchemaOpts, so lax-named and CustomType-bearing schemas could not rebuild from metadata at all — fixed with backward-compatible variadic opts threaded to the internal Parse, pinned by the lax-rebuild and custom-rebuild matrix cells.

  Second-wave axes (closing the remaining historical classes): **rejection parity** (matrix_reject_test.go — per-kind WRONG-value sets × positions assert the binary and JSON encoders agree on rejection, and per-wire mismatched-target decode rejection agrees across both decoders; the generative form of the encode-accepts/decode-rejects class; two documented leniencies are encoded as expectations: non-numeric strings against bytes+decimal are the raw-bytes string-source leniency, and `[2]byte`/`[]uint8` are legal exact-length/typed-slice targets); **foreign container framing** (matrix_framing_test.go — every fragment's array/map wire re-framed as block-per-item, split blocks, negative-count size-prefixed blocks, and overlong varint counts must decode to the same value and re-encode canonically, including through typed-slice targets and the resolution SKIP path; twmb's encoder never produces these framings, Java/fastavro do); **target reuse** (matrix_reuse_test.go — decode-twice into the same `*any`/typed containers asserting the documented stale-key map retention, slice shrink/regrow, pointer-slice reuse, struct-field nil↔value cycling); **typed extras + per-fragment typed containers** (json.Number numeric-carrier targets, TextMarshaler string-kind + enum name-matching targets, and `[]T`/`map[string]T` for EVERY typed fragment — the per-element fast-path-gate class); **OCF matrix** (ocf/matrix_ocf_test.go — fragments × deflate/snappy/zstd × forced multi-block files × NewAppendWriter continuation × WithReaderSchema/Func evolution; codec instances are never shared writer↔reader because Close closes the codec); **runCore edges** (Parse(String()) fingerprint+wire idempotence, append-to-nonempty-dst, three-value concatenated-stream sequential decode, SOE round-trip per cell); and the **external-oracle matrix** (matrix_differential_test.go + the oracle's new `rt`/`canonical` ops — every fragment×context cell's wire is decoded AND re-encoded by a real fastavro process and must come back byte-identical, and fastavro's Parsing Canonical Form must equal `Canonical()` for every composed schema; this kills the symmetric-encode+decode-bug class the relational core is structurally blind to; fwd-ref schemas are excluded because fastavro rejects the twmb+Java extension outright). The JAVA twin (`java_matrix_differential_test.go`, cisuite-tagged, wired into the GHA java-differential job) drives the same cells plus ALL recursion shapes through `SchemaOracle`'s existing RT and fingerprint commands — Java binary decode→re-encode must be byte-identical and Java's `toParsingForm` + `parsingFingerprint64` must match `Canonical()`/Rabin for every composed schema; Java is the one external oracle that accepts forward references, so the fwd-ref shapes are externally validated there (schemas are `json.Compact`ed for the line protocol; no Java-side changes were needed). The harness honors: NaN payload bits differ between Go (0x…01) and Python — oracle cells drive non-NaN values; the per-build custom-Encode suppression means count configs drive calibrated raw trees.

  Third-wave axes: **schema-ACCEPTANCE parity** (matrix_acceptance_test.go + the oracle `parse` op + `TestDifferentialJavaAcceptance` — every composed cell must parse in twmb AND fastavro AND Java, and every structurally-broken mutant (missing/duplicated/empty required attributes, negative sizes, nested unions, non-member enum defaults) must reject in all three; mutator classes deliberately avoid documented-policy territory (quoted size, logical-param soft-drop-vs-reject, alias grammar, fwd-refs); fastavro is scoped out of the four classes its parser is lax about — missing/duplicate/empty-named record fields, negative fixed size — which Java enforces; this axis is the generative form of the acceptance-divergence class that produced the size-0/empty-enum/empty-union findings, and ON ITS FIRST RUN caught twmb accepting `{"type":"record","name":"R"}` with NO fields attribute — the missing-vs-empty sibling of the enum-symbols fix that the manual sibling sweep missed; fixed via `o.Fields == nil` reject, mirroring symbols, with the stale lenient-form pin in TestSchemaNodeCanonicalIdempotent updated); **the fuzz bridge** (`FuzzMatrixCore` — fuzz input selects a matrix cell and XORs mutations over its valid wire, so CI's auto-discovering fuzz loop explores cell × hostile-byte combinations beyond the curated sweeps); **second-order CustomType crosses** (matrix_custom_cross_test.go — customs × evolution with drop+default-fill around a boxed field incl. writer-shaped resolved DecodeJSON parity, customs × SchemaCache consistent-registration refs, customs × OCF via WithSchemaOpts); and the **options cube** (all 8 combinations of TaggedUnions × TagLogicalTypes × LinkedinFloats through the relational core on opt-active fragments).

  Fourth-wave axes: **hostile-size rejects** (matrix_hostile_size_test.go — 1 MiB wrong-typed values at every encode arm and a 1 MiB wire into a mismatched decode target must reject FAST (<250ms CI headroom; the type check runs before any superlinear parse work) with BOUNDED error messages (<2KB; the trunc-helper contract), both wire formats); **logical boundary tables** (matrix_logical_bounds_test.go — typed extremes per time logical round-trip exactly through the relational core, including timestamp-nanos at the int64-ns instant bounds and year-9999/year-1 edges; raw MaxInt64/MinInt64±1 boundary WIRES through every long/int-backed logical must either reject or re-encode byte-identically — silent boundary corruption is the only forbidden outcome; duration at uint32-max fields; decimal at the precision boundary ±1); **the concurrency hammer** (matrix_concurrent_test.go — 8 goroutines × all five operations on one shared *Schema per fragment, every result byte-compared to the single-threaded reference, plus concurrent SchemaCache parse/cross-ref and concurrent FIRST-use racing the lazily-built once-init skip/field tables on fresh resolved schemas; runs under CI's `-race`); **the OCF stateful model** (ocf/matrix_stateful_test.go — seeded random programs of Encode / value-error-Encode (documented to discard only the failed datum) / raw Write / Flush / Close, with the model tracking accepted datums and the reader required to observe exactly that sequence; the same model across a NewAppendWriter boundary; and I/O-error poisoning stickiness); and the **Java JSON-form differential** (`TestDifferentialJavaJSONForm`, cisuite — twmb's Avro-JSON per cell compared SEMANTICALLY against Java's JsonEncoder output via the RT command's JSON half: numbers by rational value (Jackson spells float zero `0.0`, twmb `0`), strings by codepoints (Jackson writes high bytes raw, twmb escapes `\u00XX`); scoped to non-logical fragments because Java's generic datum path writes logicals in underlying form where twmb writes enriched forms — a documented representation difference; twmb runs with TaggedUnions since Java always writes the union envelope; the zero-field record is ALSO excluded because avro-tools 1.12.0's JsonEncoder emits ZERO bytes for an empty record — and for any document containing one — observed empirically via the RT oracle on the axis's first CI run; twmb's `{}` matches fastavro and is the only valid JSON, so do not re-flag it against Java's empty output).

What this buys the audit: these classes are caught at commit time, so manual effort should concentrate on the *uncovered intersections* (the lottery described above) instead of re-deriving what the suite already guards.

**But a suite flag is a CANDIDATE, not a finding — exactly like the greps below.** When an invariant fails or a differential mismatches, you still owe the §"Before changing behavior" documented-intentional gate before calling it a bug. Real instance: the parity invariant flagged `json.Number`→bytes encode/decode as an asymmetry; it was documented-and-pinned *intentional* (later changed by a deliberate maintainer decision, not as a bugfix). Declaring it a "bug" before running the gate was the exact error the gate exists to prevent. The suite tells you WHERE to look; the gate tells you whether what you found is a bug or a policy.

**Fuzz-step failures triage into FOUR classes; identify the class BEFORE choosing an investigation direction — and the discriminator between the last two is the EXEC-RATE TREND in the elapsed log.** (a) A crasher input was written under `testdata/fuzz/` → a real input-triggered bug; minimize and file. (b) "fuzzing process hung or terminated unexpectedly" with an input recorded → a real hang; file. (c) NO artifact, "context deadline exceeded" at the `-fuzztime` boundary, and the elapsed log shows a SLIDING / DECLINING exec rate (and "new interesting" entries clustering on one arm) → there is no hostile input to hunt: the fuzz body's PER-EXECUTION fixture cost is saturating the GC until a starved worker misses the coordinator's shutdown deadline, and the corpus keeps drifting toward the expensive arm, so it recurs until the cost is fixed. For (c): benchmark the fuzz body per arm (extract it into a `-benchtime=200x` benchmark), find the heavyweight fixture, and shrink it WITHOUT changing the fuzzed surface — concrete record: per-exec default-option zstd codec construction cost 573µs + 1.64MB of garbage per cycle vs 126µs + 0.30MB with min-window/fastest/low-mem options; the construct→use→Close lifecycle under test was unchanged. Also hoist input-independent fixtures (re-Parsed schemas, re-Resolved writer→reader pairs) out of the fuzz body to the `func FuzzX` scope, and cap any corpus-grown payload whose SIZE isn't the surface under test (a multi-MB string in a concurrency fuzz). (d) NO artifact, "context deadline exceeded" at the boundary, but the exec rate is HEALTHY and STEADY (e.g. 16k/sec, not declining), tiny bounded inputs, and the failing fuzzer DOESN'T reproduce locally even at 3× the fuzztime under `GOMAXPROCS=<runner cores>` → NOT a code/input bug: the runner is oversubscribed (`go test -fuzz` defaults its worker count to GOMAXPROCS, so N workers + the coordinator contend for N cores) and at shutdown the coordinator misses its worker-RPC deadline. The discriminator from (c) is the rate trend (healthy vs sliding) plus a clean local repro; the fix is `-parallel=1` in the CI fuzz invocation (one worker leaves a core for the coordinator), not a code change. **The trap: (c) and (d) present with the identical error string — only the exec-rate trend and local reproducibility separate "fix the fuzz body" from "fix the runner contention." Classify before acting; the same string is two different root causes.** Re-running until green is dismissal-by-retry, not triage, for any of (a)-(d). (e) The exec COUNTER freezes (same value for many intervals) but a per-execution watchdog (a goroutine that `panic`s if one fuzz-function call exceeds N seconds) NEVER fires, and the freeze does not reproduce under the watchdog: the time is in the COORDINATOR, not your fuzz function — it is MINIMIZING a large "interesting" input by re-running dozens of shrink-trials, each individually fast. This is not a code/reader bug (the per-call work is proportional and bounded); fix it by bounding the fuzz INPUT SIZE (`if len(data) > N { return }`) so minimization stays cheap, exactly as for a corpus-grown payload in (c). The watchdog is the discriminator: a real slow single execution trips it; coordinator minimization does not. Before concluding "DoS in the reader," confirm a single execution is actually slow — a frozen counter alone does not prove it.

## Patterns that have produced real findings

These are the bug shapes prior audit rounds have caught. Most novel findings come from one of them.

1. **Stdlib functions that signal "bad input" via nil / zero / sentinel / lossy-type rather than error.** Examples: `(*big.Rat).SetFloat64` returns `nil` for NaN/Inf — caller must check; `time.UnixMilli` / `time.UnixMicro` are documented "undefined" for out-of-range times and silently wrap (Java throws ArithmeticException); `json.Unmarshal` into `any` rounds long > 2^53 to float64 silently; `reflect.MakeSlice(0,0)` returns an unaddressable Value.

   **The structural question, applied to every stdlib-parser call site:** Take the call's input domain (user data, wire bytes, hostile-crafted JSON) and ask three things — (a) what's the largest legitimate value the user could pass? (b) does the chosen parser API preserve that value losslessly, return an error on overflow, or silently coerce? (c) if it silently coerces, does the caller's own code catch the bad output? The examples above are *candidates* for this question; they are not an exhaustive list. New stdlib idioms (golang.org/x/exp/constraints math, generic numeric parsers, json/v2 if it ships) are equally susceptible the moment they're added. Treat "this list looks short, so this class is covered" as the failure mode: a named example in this list does not mean every site of the same shape has been swept — the named example *was* the surviving bug for a long time.

   **Sub-pattern 1b: stdlib returns a *usable value* alongside an error sentinel — caller must NOT blindly propagate `err`.** Dual of the main pattern. The main pattern says "`err == nil` isn't enough; check the value." 1b says "`err != nil` isn't always reject; check the value." Concrete: `strconv.ParseFloat("1e1000", 64)` returns `(+Inf, strconv.ErrRange)` — the +Inf result IS the correct IEEE 754 / Avro wire encoding for an overflowing finite input. Blindly propagating the err rejects valid output. Java's `BigDecimal.doubleValue()` returns `Double.POSITIVE_INFINITY` for the same input, no exception; fastavro's `float()` returns inf. Other candidates with the same shape: `(*big.Float).Float64()` returns `(Inf, big.Above|Below)` for overflow (the value is usable); `strconv.ParseFloat` *underflow* returns `(0 or denormal, ErrRange)` (the small value is usable up to the limit-of-representation policy). The audit angle: for every `strconv.Parse*` / `big.*.Set*` callsite that *propagates* err, ask — is the value alongside the err one the typed-input arm would have accepted? If yes, the propagation is the bug.

   The most common smoking gun: the same encode path's *decode* counterpart already has the right "accept the Inf-from-overflow" arm (e.g. `decodeFloat`/`decodeDouble` at json_decode.go/:509 both explicitly accept `±Inf` from `ParseFloat` ErrRange). The encode side just hadn't caught up. Whenever you find this asymmetry on a *new* stdlib-parser callsite, grep the parallel decoder for "Accept ±Inf" / "Accept overflow" / "ErrRange" — if the decoder has it and the encoder doesn't, that's the finding.

   **Four-axis rule (originally "three-axis," added after the `normalizeJSONNumber` finding):** the same stdlib parser appears on at least FOUR code-path axes whenever the schema can carry the same value as both a runtime input AND a literal default AND a metadata-API observable:

   1. **Encode-time** — `s.Encode(json.Number("1e1000"), "double")` routes through e.g. `jsonNumberToFloat` / `jsonCoerceToFloat64`.
   2. **Decode-time** — `s.Decode(+Inf wire bytes, &f64)` routes through e.g. `decodeFloat` / `decodeDouble`.
   3. **Schema-parse-time validation** — `avro.Parse({type:record,...,default:1e1000})` routes through e.g. `defaultAsFloat64` / `coerceDefault` / `validateDefault` / `defaultAsInt32` / `defaultAsInt64`.
   4. **Metadata-API observability** — `s.Root().Props["x"]`, `s.Root().Fields[].Default`, `s.Root().Fields[].Props`, `CustomType` callback `*SchemaNode.Props` route through `normalizeJSONNumber` (`schema.go`). This is a SEPARATE `json.Number.Float64()` callsite from axes 1-3, and was invisible to every prior round.

   Axes 3 and 4 are structurally invisible to encode↔decode-parity sweeps and to the "encoder accepts X, decoder rejects X" round-trip-test angle. A commit that fixes the encode-side and names "now matches the decode-side" is a claim about TWO axes; the audit owes a third probe (`avro.Parse(schemaWithFieldDefaultEqualToTheNewlyAcceptedValue)`) AND a fourth probe (`s.Root().Fields[0].Default.(float64) == expected` — note the type assertion, not just the value). If either probe fails the fix is partial.

   **Grep playbook for any stdlib-parser callsite fix:** `grep -n '<stdlib-parser>(' *.go` → for each hit, classify (a) does it run at encode time, (b) at decode time, (c) at schema parse / validate / coerce time, or (d) at metadata-API normalization time (`normalizeJSONNumber` / `normalizeJSONValue` / `unmarshalAnyPreservePrecision`)? If (c) or (d) hits exist and weren't touched by the commit, write `avro.Parse(...)` against the typed-value the encode fix newly accepts and confirm both `Parse` succeeds AND `s.Root().Fields[0].Default` has the expected normalized Go type. The schema-parse and metadata axes are the most commonly missed because the audit's primary lens is "wire-format parity" and neither parse-validate nor metadata-read shows up in a wire test.

   Concrete record (three-axis stage): a prior fix (encode-side ParseFloat ErrRange-with-Inf acceptance) named `jsonNumberToFloat` and `jsonCoerceToFloat64` — the two encode-time sites. The decode-time sites at `json_decode.go/:506` were already correct. The schema-parse-time sites at `schema.go/:2511/:2567` (`defaultAsFloat64` json.Number arm + string arm, `coerceDefault` ParseFloat call) were missed — they were three more callsites of the same stdlib function on the same user-controllable input (the JSON-encoded `"default":` literal), and they continued to reject `{"type":"record",...,"default":1e1000}` while the encoder accepted the exact same `json.Number("1e1000")`. The fix at that round patched all three; the lesson was added as the three-axis corollary.

   Concrete record : even with axes 1-3 in agreement on ±Inf-from-overflow, `s.Root().Fields[0].Default` continued to return `json.Number("1e1000")` for the same input — a 4th asymmetric surface. `normalizeJSONNumber` (`schema.go` pre-fix) called `(json.Number).Float64()` directly and returned the json.Number unchanged when the call returned `(±Inf, strconv.ErrRange)`. The metadata-API path through `normalizeJSONValue` is structurally invisible to wire-format probes (no encode), schema-parse-validate probes (parse succeeds, the divergence is in Root() observability), and encode/decode-parity sweeps. Java's Jackson DoubleNode and fastavro's Python `float("1e1000")` both expose +Inf in their metadata layers — twmb was the outlier. Fix: route `normalizeJSONNumber`'s exponent-form arm through the same `parseFloatAcceptOverflow` helper used by axes 1-3, and add `jsonSerializableValue` (`schema_node.go`) to convert ±Inf back to a `json.Number` literal during `toJSONWalk` so `encoding/json.Marshal` (which rejects ±Inf unconditionally) doesn't break the `SchemaNode.Schema()` round trip. The four-axis-rule corollary is the lesson — every stdlib-parser callsite fix going forward must probe axis (d) explicitly: `s.Root().Fields[0].Default.(float64) == expected` AND `s.Root().Props["x"].(float64) == expected` AND `s.Root().Fields[0].Props["x"].(float64) == expected` AND the CustomType-callback equivalent.

   **Boundary values to probe** (for every numeric public API surface): `0`, `1`, `-1`, `math.MaxInt32`, `math.MinInt32`, `1<<24` (float32 mantissa boundary), `(1<<24)+1`, `1<<53` (float64 mantissa boundary), `(1<<53)+1` (canonical "silently rounds via float64" probe), `math.MaxInt64`, `math.MinInt64`, `math.NaN()`, `math.Inf(±1)`, empty / zero-length, length cap. Hit at the entry point AND at any intermediate API surface — `Schema.Decode`, `Schema.Root().Props["x"]`, `CustomType` callback receivers, etc. The 2^53+1 probe is the canonical precision-loss test and should appear in the test suite for every numeric-returning API.

2. **Path divergence — same logical contract, multiple implementations.** The library has reflect-based safe paths, unsafe-pointer fast paths on addressable struct fields, JSON encode/decode paths, and per-primitive specializations on array/map containers. When any one of these paths enforces a rule (overflow check, type coercion, logical-type conversion, pointer indirection, length bound), all the others must enforce the same rule for the same input. Look at every `serFoo` / `usFoo` pair, every `deserFoo` / `udFoo` pair, every JSON encode/decode that should mirror its binary counterpart, and every per-primitive container specialization (`serArray.serFoo` etc.) for whether it bypasses the per-element function the schema build assigned.

3. **Wire-format multiple-encoding tolerance.** Avro varints have multiple valid encodings (canonical 1-byte vs non-canonical multi-byte). Whenever code peeks at `src[0]` and switches on byte values, it's at risk of rejecting valid non-canonical input. Java's `BinaryDecoder.readIndex` always uses the full varint loop. Look for any `switch src[0]`, `case 0x00 / 0x02`, etc.

4. **Default value precision and type round-trips.** JSON-encoded defaults round-trip through `json.Unmarshal`/`json.Marshal` losing precision (long > 2^53), changing type (int → float64), or mishandling type coercion (string-defaulted floats per Java's permissive parser). Walk every default-value flow.

5. **Resolution between writer and reader unions.** Per-branch ErrorAction (Java's behavior, mirrored in fastavro): incompatible writer branches should defer the error to wire-decode time, not abort `Resolve`. Two-pass (exact-then-promotion) selection: same-kind matches must beat promotion matches. Both have produced findings.

6. **Spec relaxation that the implementation didn't pick up.** Avro 1.12 relaxed union-default rules (AVRO-3649). The PCF [STRINGS] rule requires raw UTF-8 (Go's encoder hardcodes U+2028/U+2029 escapes). Java's behavior is the reference; check that we match `apache/avro:main` (not just a release tag).

7. **Validation that "silently falls back" instead of erroring.** When a known logical type has invalid params, Java/fastavro reject; the lenient "ignore the logical type" approach is a parity hazard. Same pattern for any "we accept X to be forward-compatible with future Avro versions" sentiment — usually it should be a strict reject.

8. **Bounds checks that miss zero-byte items.** `count > len(src)` is sound for non-zero-byte items but rejects valid `array<null>` and admits 10B-element zero-byte attacks. Generalize to `count > len(src)/minItemBytes` plus an absolute cap when `minItemBytes == 0`. Watch for the **post-add wraparound** sibling: `totalItems += count; if totalItems > cap` wraps to a negative int64 when count is near MaxInt64, bypassing the cap. The pre-add form `count > cap - totalItems` is overflow-safe. A finding: 2 of 4 array-decode sites still had the post-add form, caught only incidentally by a downstream `start > MaxInt - n` guard.

9. **Stdlib API amplification.** A stdlib function takes a compact input and materializes a large internal representation. `(*big.Rat).SetString("1e1000000")` is 9 bytes in, ~3 MB out (360,000× amplification); `(*big.Int).Exp(10, scale, nil)` is bounded by scale (cap it before calling); `(*big.Float).SetString` similarly accepts huge exponents. The Java analogs of these never eagerly materialize the magnitude — they store significand + scale separately and defer until a stringify call. twmb materializes during decode, so every wire- or JSON-controlled input that flows into one of these has to be bounded. Look for: `big.Int.Exp`, `big.Rat.SetString`, `big.Float.Set*`, `big.Int.SetString` (less risky — no exponent materialization, but huge inputs are still O(n²)), and any custom-codec path that calls into a stdlib parser on attacker-controlled bytes.

10. **Auto-fill-default asymmetry across encode arms.** twmb is the only Go Avro impl that auto-fills schema-declared defaults during `EncodeJSON` for absent record fields (see the "JSON DecodeJSON fills defaults" entry — same fill-on-encode logic for emit). Defaults are stored in their *parsed* form (JSON form for `defaultVal`): a fixed-uuid default is a 16-codepoint string per the Avro JSON spec, NOT a 36-char hex-dash UUID. When `appendAvroJSON` routes that default through a logical-type-aware arm, the arm must accept the stored form. A finding: `case "fixed"` → `case "uuid":` arm hard-returned `parseUUID`'s "invalid UUID" error on the 16-codepoint default form. Audit every logical-type arm in `appendAvroJSON` (json_codec.go's "bytes", "fixed", "string", "int", "long" cases) for whether the arm's input form matches the parsed-default form for that logical type.

11. **Dispatch-arm hard-fail vs fall-through.** When a `switch` / `case` arm's strict parser rejects (`parseUUID`, `time.Parse`, `big.Rat.SetString`), the code either returns the error (locking out other interpretations) or falls through to the next arm (the generic path). The right behavior depends on whether other arms could legitimately handle the same input. UUID-fixed had the wrong shape: the strict UUID parser hard-returned, blocking the codepoint-string default form that the comment immediately above said should fall through. Look for: `return nil, err` immediately after a strict-parser call inside a logical-type arm where the surrounding type's generic arm could handle a different input form for the same Go kind. Run `grep -B 3 -A 6 'parseUUID\|time.Parse\|SetString' json_codec.go` and read every hit.

12. **Encode accepts type X, decode rejects type X (round-trip break).** The most common parity bug shape in this codebase. For every Go type the encoder accepts as input to an Avro schema, the decoder MUST accept it as a target — otherwise a struct round-trips through binary OR JSON but not the other, or the user can `Encode(v)` what `Decode(...)` can't read back into `v`'s type. Recent instances: `serTimeMillis` accepts `time.Time` (extracts time-of-day) but `deserTimeMillis` rejected `time.Time`; binary `deserEnum` accepts `int`/`uint` (writes ordinal) but JSON `decodeEnum` rejected; `serSize` accepts `reflect.String` (writes raw bytes) but binary `deserFixed` rejected. The four target-type dispatch sites for any non-trivial Avro kind are (binary safe `deserFoo`), (binary unsafe `udFoo`), (JSON `decodeFoo`/`assignFoo`), (resolved `resolvedFoo`); they must agree on the *set of accepted target types* even if the canonical type differs. Grep target: every `serFoo`/`appendAvroJSON case "Foo"` accepted type that doesn't appear as a target case in the corresponding decoder.

13. **The pin test that pins the bug.** Tests that lock current behavior accidentally lock bugs — and the fact that the test passes hides the bug from every future audit. Two shapes have surfaced:

    **13a. Rejection pins on the wrong rejection.** Some `TestRegression_*`/`TestErrorPaths` tests assert that a target-type rejection IS the documented behavior. They look correct (they document an existing error path) but the documented behavior is itself the bug — encode accepts, decode rejects, a test pins the rejection. Concrete cases: `TestDecodeJSONErrorPaths` (json_decode_test.go) pinned `enum.DecodeJSON("A", new(int))` as an error — the int target should have been a non-error per binary parity, but the pin made every subsequent audit assume coverage. Before reporting "no parity bug here" for a path-pair, grep `TestRegression_*` / `TestError*` for the path's rejection patterns and ask "is this pin's documented behavior actually correct given the other path's behavior?"

    **13b. Type pins on success paths (the silent-precision-loss class).** Pins on the *return type* of a user-facing value can lock in lossy types just as effectively as error pins lock in wrong rejections — and they're harder to spot because the test PASSES. Concrete instance: `TestSchemaNodeRoundTrip` asserted `got.Fields[1].Default != float64(18)` and `TestSchemaNodeCustomPropsExtended` asserted `root.Props["custom.num"] != float64(42)`. Both pins fixed the Go type a numeric JSON literal materializes as — and that type was `float64`, which silently rounded values > 2^53. The pinned VALUES (18, 42) are too small to trigger the bug, so the type pin masked the precision-loss class. Whenever an existing test asserts `v.(T)` or `v == T(x)` on a user-facing return value with `T` numeric, ask: *what would happen if `x` exceeded `T`'s representable range?* If the answer is "silently wrong," the pin IS the bug. Grep target: `grep -rn 'Props\[.*\].*float64\|\.Default .*float64\|\.(float64)' *_test.go` is one example — but the structural question generalizes: every type-pin on a user-facing numeric value is a candidate.

    **13b-union. Type pins on union-default values can mask branch-selection asymmetry.** Sub-case of 13b that's not about precision loss — about which BRANCH the value implies. A user reads `SchemaField.Default.(int64) == 42` for `["float","int"]` default `42`: the test passes (the Go type is int64 because the metadata path normalized the integer literal that way), and the user reasonably infers "int branch chosen." But the wire path's `validateDefault` picks the FLOAT branch via `integerFormFitsFloat` — so the wire encodes float32(42.0) while the metadata says int. The type pin on `int64(42)` doesn't look like a bug-locker, but it implicitly *defines* the chosen branch as int — and the wire silently picks a different one. The structural question that finds this class: for every type assertion on a `SchemaField.Default` that's part of a union, ask "if branchAcceptsDefault iterated branches in declaration order with the wire-side per-value validator, which branch would it pick? Does that match the type pinned here?" If the type pin's implied branch differs from the wire-validator's chosen branch, the pin masks a sibling of the asymmetry. No new grep target — the same `\.Default .*float64\|\.(float64)` / `\.(int64)` greps surface candidates; the question to ask each hit is whether the field type is a union and, if so, whether the pinned Go type matches the wire's chosen branch.

14. **Helper used by the fast path but not the slow path.** When a helper exists specifically to handle an edge case (`isNilValue`, `unwrapElemPtr`, `extractTime`, `decimalRatFor`, `boundedRatFromString`), grep its callers. If only the fast/optimized variant calls it and the generic/fallback path open-codes the simpler-but-narrower check, that's a candidate divergence. The fast path was almost certainly introduced *to fix* a specific class of input; the same class falls through to the generic path whenever the fast path's narrow precondition doesn't match (different arity, different schema shape, slow-path dispatch, etc.), and the generic path now silently mishandles inputs the fast path handles correctly. Concrete instance: the 2-branch `[null,T]` optimization (`serNullUnionAt`) calls `isNilValue` to peel through Pointer/Interface layers; the generic `serUnion.ser` path for 3+ branch unions / `serArray.serItem` over `array<null>` / `serMap` over `map<null>` invoked `serNull` directly, and `serNull`'s kind switch wasn't indirect-aware — so `{"null": (*int)(nil)}` in a 3-branch union failed binary encode but succeeded JSON encode (the JSON side's `appendAvroJSON` indirect loop pre-unwrapped the interface before reaching the `case "null"` arm). Every null-union *test* in the suite happened to be 2-branch, and the helper's docstring named one specific shape ("`AppendEncode` receives `&nilPtr`") that didn't suggest the broader applicability. Grep target: `grep -n 'isNilValue\|extractTime\|decimalRatFor\|unwrapElemPtr\|boundedRatFromString' *.go` — for each helper, every caller should be listed, and the question is "what does the generic path that doesn't call this helper do for the same input?" If the answer differs, that's a finding.

    **14a. Partial fix sub-pattern: the fix matched the bug report's inputs, not the helper's full coverage.** When a recent commit (within the last few audit rounds) added an indirect-unwrap / nil-peel / overflow-clamp loop to bring a slow path "into parity" with a helper, the fix may have copy-pasted only the loop the bug report exercised — not all the shapes the helper handles. Concrete instance: a prior fix added an Interface peel to `serNull` to fix `any((*int)(nil))` in a 3-branch union (the reported input). `isNilValue`'s docstring also names `&nilPtr` (a `**T` with non-nil outer pointer) as a supported shape, but the new loop only checked `v.Kind() != reflect.Interface`, not Pointer. The next audit (this one) found that `serNull` rejected `&nilPtr` and `any(&nilPtr)` at every site where the 2-branch optimization didn't apply — top-level `"null"`, 3-branch unions, `array<null>`, `map<null>`, and the corresponding record-field positions — while the JSON path accepted because its entry loop peels both. Audit playbook for any recent commit whose message says "matches / mirrors / parity with helper X": (a) read `X`'s docstring, list every input shape it explicitly names; (b) for each named shape, write a failing test against the patched path; (c) if any shape fails, the previous fix is partial. The commit message is a claim, not evidence — see hard rule 5.

    **14b. Safety helpers, not just optimization helpers.** The original pattern 14 framing focuses on performance helpers (fast-path peel loops). The same shape applies to **safety helpers** — `UseNumber`-decoding, `boundedRatFromString`, `timeMicrosToDuration`'s overflow check, `checkDecimalPrecision`. When a safety helper exists *somewhere* in the codebase, every site with the same input shape is a candidate for needing it. Concrete instance: `unmarshalDefault` (schema.go) uses `UseNumber` for parsing record field defaults — but its near-twin sites (record extras parsing at schema.go, `Schema.Root()`'s re-parse at schema_node.go) called bare `json.Unmarshal(&v any)` and silently rounded JSON ints > 2^53. The helper existed; the symmetry-twin sites bypassed it. **Sweep playbook for safety helpers**: list every site in the codebase that takes the same kind of input the helper guards against. For each site, either (a) the helper is called, or (b) the input is structurally bounded such that the helper's concern can't fire (document why), or (c) the site is buggy. There is no "the helper is over there, this site doesn't need it" without a structural reason.

    **14c. Helper bypassed by higher-order function-value capture of the underlying operation.** Pattern 14 / 14a / 14b address helpers bypassed by parallel callers that open-code a narrower check. This pattern is their higher-order dual: even when every named-function call site consults the helper, the underlying low-level operation (`reflect.Value.SetString`, `reflect.Value.SetBytes`, `reflect.Value.SetInt`, `(*T).MethodName`-as-function-value) can be captured AS A FUNCTION VALUE and invoked later from a generic loop that knows nothing about the helper. The helper's predicate never runs because the closure that performs the equivalent operation doesn't reference the helper.

    The structural angle: when a safety helper guards an operation `Op`, two grep questions instead of one — (a) every `Op(args)` call site: does it consult the guard? (the existing Pattern 14 sweep); (b) every `Op`-as-function-value reference — closure capture, method expression, higher-order argument: does the *invocation site* (where the closure is called from generic code) consult the guard? The two forms appear in completely different grep results — `\.SetString(` finds direct calls; `reflect\.Value\.SetString[^(]` finds method expressions; `, reflect\.Value\.SetString[,)]` finds it as a function argument. A `grep '<helper>'` sibling sweep on the guard's NAME catches (a) but is structurally blind to (b).

    Concrete instance: `rejectJSONNumberStringTarget` shipped with 12 setter call sites consuming it via the wrapper `setStringTarget`. Two siblings — `deserArrayStringLoop` (deser.go) and `deserMapStringBlock` (deser.go) — captured `reflect.Value.SetString` as a method expression passed to the higher-order `deserArrayLoop` / `deserMapBlock` constructors. The closure body invoked the captured `set(v, val)` without consulting the guard. The natural sibling-sweep grep `'\.SetString('` returned 0 hits for these sites (they have no `.SetString(` token — the method-expression form has no parens). The decision point for the guard is the *invocation site* — the `useFast` predicate at deser.go — which is structurally one level removed from where the wire-write happens. Fix: extend the `useFast` predicate to exclude the target type the helper guards (`sliceType.Elem() != jsonNumberType`), routing those targets through the generic per-element path that calls the wrapper. The closure body itself is unchanged; the dispatch gate is where the guard lives.

    Grep target on any new helper that guards a `reflect.Value.SetX` / `(*T).Method` operation:
    ```
    grep -nE '\.SetString\(|\.SetBytes\(|\.SetInt\(|\.SetFloat\(' *.go        # direct call sites (Pattern 14 primary)
    grep -nE 'reflect\.Value\.SetString[^(]|reflect\.Value\.SetBytes[^(]' *.go # method expressions (Pattern 14c)
    grep -nE ', reflect\.Value\.SetString[,)]|, reflect\.Value\.SetBytes[,)]' *.go # function-value-as-argument (Pattern 14c)
    ```
    Every hit in the second / third greps is a candidate site whose *invocation gate* (where the closure is called from generic code) must include the helper's input-type check. The closure body cannot — by design, generic loops have no knowledge of the typed element they operate on.

    **Type-class generalization: a fast-path gate's exclusion list IS the sibling set when the slow path gains type-class-specific behavior.** Every `goType == jsonNumberType` / `elemType != jsonNumberType` check at a string fast-path gate is excluding ONE type-class (json.Number) that must take the slow path. When a *second* type-class later gains slow-path-only behavior, every such gate is a candidate for a parallel exclusion — and the gates are scattered (unsafe struct-field gates in `unsafe.go`, container-loop gates via `fastPathSafeForElem` in `deser.go`), so the sweep must enumerate all of them, not just the one the change first touched. Concrete instance: the round that made text interfaces win over the `reflect.String` fast path (so a string-kind type with `MarshalText`/`UnmarshalText` routes through `appendAvroString`/`setStringValue`'s text arm) had to add a text-interface exclusion at *every* gate that already excluded json.Number — 6 unsafe gates + `fastPathSafeForElem` — or a struct field / container element would silently diverge from the scalar (the fast path writing the raw string while the scalar used the text method). The fix's own post-audit then consolidated both type-classes into one predicate per direction (`stringFastPathEligibleEncode` / `stringFastPathEligibleDecode` in reflect.go = `t != jsonNumberType && !implementsText{Marshaler,Unmarshaler}(t)`), so the *next* slow-path-only string concern is a one-line edit in one place rather than another N-gate sweep. **Lesson: when a fix broadens what the slow path does for a type-class, the unit of analysis is the set of all fast-path gates that bypass the slow path for that kind — and the durable fix is a single shared eligibility predicate, not inline exclusions duplicated across the gates.** A cross-wire-format instance of the same shape: the BINARY array/map fast-path gate (`schema.go`) excludes `{isFwdRef, meta.hasCustomType, logical != ""}`, but the four JSON container fast-path gates (`json_codec.go` encode ×2, `json_decode.go` decode ×2) excluded only `logical == ""`. So an `AvroType`-only `CustomType` (no logicalType) on array items / map values fired on binary but was silently bypassed on JSON — the native loop emitted/parsed the raw element, a binary↔JSON wire divergence. The existing `TestCustomType{Array,Map}FastPathDisabled` used a logicalType-bearing custom type, so `logical != ""` *also* tripped the JSON gate and masked the gap (the gate had two exclusion reasons collapsed into one test input). Reaffirmed lesson: when the slow path applies per-element behavior (a custom codec), enumerate ALL fast-path gates that bypass it across BOTH wire formats, and test the exclusion reasons *independently* (an AvroType-only custom type isolates the `hasCustomType` reason from the `logical` reason). Encode gate consults `custom[node.items]`; decode gate consults `node.items.decodeJSON != nil`.

15. **Dispatcher short-circuit bypasses the per-branch handler.** Pattern 14 says "grep the helper's callers." This pattern is its dual: even when *every* parallel path calls the helper, a dispatcher's `continue` / skip-branch / pre-filter can route around the helper for one specific input class. The helper's predicates are correct; the dispatcher decides not to ask. The bug is one cheap-looking line of "obviously this case can't fire here" — except it can, when the upstream filter's exclusion set is narrower than the helper's acceptance set.

    The structural angle: a dispatcher has a *skip predicate* (`if branch.kind == "null" continue`, `if !flag.set continue`, a switch case that falls through). The helper downstream has an *acceptance predicate* (the kinds / shapes / types the handler arm accepts). The skip predicate is sound only if the upstream filtering guarantees that every input the helper would have *uniquely* handled has already been routed elsewhere. List both predicates explicitly; compute the set difference; that's the bug surface.

    Concrete instance: `appendAvroJSONUnion`'s try-each loop at `json_codec.go` had `if branch.kind == "null" continue`. The `case "null"` arm of `appendAvroJSON` (the per-branch handler) accepts nil for `{Pointer, Interface, Map, Slice, Chan, Func}`. The upstream filter — `appendAvroJSON`'s peel loop plus `unionTypeNameForValue` — only converts `{Pointer, Interface}` nils to "go to null" outcomes and only names `Slice<uint8>` (as `"bytes"`); `{Map, Slice<non-byte>, Chan, Func}` nils flowed through to try-each and the skip blocked them from reaching the `case "null"` arm that would have accepted them. Binary `serUnion.ser` does *not* skip null in its try-each (ser.go), so the binary path picked the null branch correctly; the JSON path returned "no union branch matched" or silently picked the wrong branch (e.g. `[]byte(nil)` → string branch → `""`). The bug was *created* by a past audit that broadened `serNull`'s peel loop to handle nil Map/Slice via the kind switch — that fix made the binary handler more accepting, which silently widened the gap with the JSON dispatcher's stale skip. Every existing nil-union *test* either (a) used the tagged-union form `{"null": <nil>}` (which flowed through `tryUnwrapTagged` → `case "null"` directly, bypassing try-each), or (b) used a value whose Kind was Pointer/Interface (which the peel loop did handle). The bare `map[string]any(nil)` against `["null","int","string"]` shape exercised neither route.

    **Audit playbook for any `continue` / skip / case-fallthrough inside a dispatch loop:** (a) read the per-branch handler's acceptance predicate — list every input shape it accepts. (b) read the upstream filter's exclusion predicate — list every input shape it rejects or pre-routes. (c) compute the set difference. (d) for each input in the difference, write a failing test. The `continue` is sound iff the difference is empty. Cheap-looking skips that lack a documented invariant ("we know X can't reach here because Y") are the highest-yield sites — undocumented skips encode the original author's mental model at the time the code was written, and that model decays as the helper's acceptance set broadens.

    Grep target: `grep -nE 'if .*\.(kind|Kind|type|Type) == .* continue\|^[[:space:]]*continue$' *.go` plus visual inspection of every `switch` / `for` over branches/cases for short-circuits. Pair every hit with the helper it bypasses.

16. **The precision fix that introduces its own DoS.** A correctness fix that replaces a fast bounded check (per-digit overflow loop, fixed-precision float check, fixed-size buffer scan) with an arbitrary-precision helper (`big.Rat`/`big.Float`/`big.Int.SetString`) is correct on the bug-class the original finding probed but commonly introduces a different attack class: slow-loris-style CPU exhaustion on hostile inputs the old fast-bounded check rejected in O(small). The new helper has a worst-case cost (`big.Rat.SetString` is O(n²) on n-digit input); without an explicit length cap at the helper's entry, a 1 MiB JSON number can drive 10^12+ ops where the old code took 10^1 ops.

    The structural angle: a fix that swaps a *bounded-cost* check for an *unbounded-cost-but-precise* one always changes the perf posture of the entry point. The new cost must be re-bounded — either by an explicit length cap, by an upstream input cap, or by a structural reason hostile inputs can't reach the slow path. The audit playbook from pattern 9 ("stdlib API amplification") applies *to the NEW code path the fix introduces*, not just to existing code.

    Concrete instance: the `parseInt64Lenient` fix replaced the prior `Int64()` → `Float64()` → `floatFitsInt64()` chain (O(n) for n-digit ParseInt + ParseFloat, O(1) for the bound check) with `Int64()` → `boundedRatFromString()` → `big.Rat.SetString` (O(n²) for n-digit SetString). Without the additional 64-char cap before reaching `big.Rat.SetString`, a 1 MiB attacker JSON number like `"1.0...0e3"` would have spent ~1000 sec in `big.Rat.SetString`. The cap on `boundedRatFromString` itself (1 MiB) was inherited from the decimal-logical-type DoS posture, which is *looser than int64 needs* — the helper was designed for a domain that legitimately accepts megabyte-scale numbers; int64 doesn't.

    **Audit playbook for any "swap fast-bound for precise" fix:** (a) identify the new slow-path helper. (b) compute its worst-case complexity (`big.Rat.SetString` is O(n²); `big.Int.Exp(10, scale)` is bounded by `scale`; `big.Float.Parse` is O(n) but `big.Float.Int(nil)` materializes a big.Int proportional to mantissa-bits + exponent). (c) write a 1 MiB hostile input against every entry point the fix touched and time the rejection (`time.Since(start)`); reject takes > 100ms ⇒ the bound is missing. (d) if a tighter bound than the helper's default exists for the new caller's domain (e.g., int64 fits in 30 chars), add it at the caller's entry. (e) re-run benchstat against the pre-fix version to confirm the legitimate fast path doesn't regress.

    Grep target on a freshly-applied fix: `grep -n 'boundedRatFromString\|big.Rat.SetString\|big.Int.SetString\|big.Float.Parse\|big.Float.SetString' <changed-files>` — for every new caller of these helpers added by the fix, verify a tight length cap is in place before the call.

## DRY is bug prevention, not elegance

DRY findings are in scope and **as valuable as bug findings** when they prevent the parity bugs we keep catching. The pattern:

- A logic rule (overflow check, varint decode, default extraction) is duplicated across N sites.
- One site enforces the rule, others don't — or all enforce it now but they'll drift the next time the rule changes.
- The auditor finds the parity gap; the maintainer fixes the immediate divergence and factors the helper, eliminating the entire class.

This loop has fired repeatedly. The detailed lessons live in §Patterns and §Structural blind spots; the representative *drift shapes* (no history needed to apply them):

- A rule enforced at one `serFoo`/`deserFoo` but not its fast/slow twin or its JSON counterpart — Inf-clamp, zero-byte cap (pre- vs post-add form), varint canonical-vs-multibyte handling, nil-peel through Pointer/Interface — factored into one shared helper (`appendAvroFloat32`, `checkArrayBlockBounds`, `readNullUnionIndex`, `isNilValue`). Watch the *re-creation* trap: a later fast path that re-inlines the emit (e.g. `math.Float32bits` for float32) must match the helper on every input, including sNaN payloads — `reflect.Value.Float()` quiets signaling NaNs via a float32→float64→float32 round-trip while `unsafe`/native read raw, and preserve is canonical (Java's `floatToRawIntBits`).
- A stdlib-parser predicate (ParseFloat ErrRange-with-Inf; integer-form precision) applied at the encode arm but not the schema-parse or metadata arm — the four-axis rule (§Patterns 1b); fixed via one shared parser (`parseFloatAcceptOverflow`, `integerFormFitsFloat`) in BOTH accept and reject directions and across every arm of a multi-arm helper.
- A safety guard present at one DRY-extracted sibling but not the others — `truncForError` at some `fmt.Errorf` sites but not all; `rejectJSONNumberStringTarget` at `setFloatValue` but not the string-kind setters (`json.Number`'s `Kind()` is `reflect.String`) — §pattern 14b. Probe every DRY commit: "did one sibling gain a guard the others didn't?"

The throughline: when a rule lives in N copies, one drifts; the durable fix is one shared helper, not N synchronized copies.

**Find the next one.** Look for repeated-near-but-not-quite patterns — search by structure, not by name.

⚠ **The greps below are CANDIDATE GENERATORS, not bug detectors.** A grep hit is the start of an investigation, not the end. Running every grep and finding "no obvious bugs" does NOT mean the underlying pattern is covered. The precision-loss finding for `json.Unmarshal`-into-any was a site that *was* in this list's `json.Unmarshal\|json.Decoder` grep — auditors ran the grep, saw the hits, and stopped at "looks fine" without applying the precision-question from pattern 1 to each hit. For each hit, you owe the structural question from the named pattern, and you owe a test of the boundary case. "I scanned the greps" is not an audit; "I asked the pattern's structural question of every hit, and here's what each hit's answer is" is.

The list is also **non-exhaustive**. New stdlib idioms, new helper sites, new logical types arrive over time. If a grep target here has stopped surfacing findings, that means the pattern has been swept once — not that it's permanently safe. Re-derive the candidate-generators from the pattern descriptions each round; don't treat this list as a fixed checklist.

- `grep -n 'math.IsInf' *.go` → Inf-clamp logic.
- `grep -n 'OverflowInt\|OverflowUint\|MaxInt32\|MaxInt8' *.go` → integer-narrowing checks.
- `grep -n 'readVarlong(src)' *.go | grep -A 4 -B 1` → length/index varint patterns.
- `grep -n 'count > int64(len(src))' *.go` → bounds checks.
- `grep -n 'totalItems += count\|count > .* - totalItems' *.go` → pre-add vs post-add overflow form (pattern 8).
- `grep -n '*big.Rat\|big.NewRat\|SetFloat64\|\.SetString\|big.Int.*Exp' *.go` → decimal coercion + amplification (patterns 9, 1).
- `grep -n 'strconv.Parse\|json.Number\.\(Float64\|Int64\)\|json.Number(' *.go` → stdlib numeric parsers (pattern 1b). For every hit, classify by code-path axis: (a) encode-time `serFoo` / `appendAvroJSON case`, (b) decode-time `deserFoo` / `decodeFoo`, (c) schema-parse-time `defaultAs<Type>` / `validateDefault` / `coerceDefault` / `convertDefaultBytes` / `encodeDefault`, OR (d) metadata-API observability `normalizeJSONNumber` / `normalizeJSONValue` / `unmarshalAnyPreservePrecision`. A fix at ONE axis is a claim about that axis only; the audit owes a probe at each of the other THREE axes. The four-axis rule (pattern 1b corollary) is the structural test — for every newly-accepted (or newly-rejected) input X: (a) does `s.AppendEncode(X)` agree?, (b) does `s.Decode(<wire bytes>)` agree?, (c) does `avro.Parse({type:record,...,default:X})` agree?, (d) does `s.Root().Fields[0].Default` have the expected normalized Go type? If any of the four disagrees, the fix is partial. a prior fix's encode-time ParseFloat ErrRange-with-Inf fix left three schema-parse-time sites diverging AND one metadata-API site (`normalizeJSONNumber`) diverging — the four-axis rule applies to every future stdlib-parser fix.
- `grep -n 'json.Unmarshal\|json.Decoder' *.go` → JSON-number precision risk (pattern 1). For each hit ask: does the call decode into a bare `any` / `map[string]any` / `[]any`? If yes, is `UseNumber` set on the Decoder? If no UseNumber and the input could carry JSON ints > 2^53, the site silently rounds. Don't stop at "looks fine"; the hits that *don't* look fine are the ones missing UseNumber on the json.Decoder, or using `json.Unmarshal(b, &v)` with v of `any` type. The precision-loss finding here came from a hit that had been in this grep for a long time before anyone asked the structural question.
- `grep -B 3 -A 6 'parseUUID\|time.Parse\|boundedRatFromString' json_codec.go` → dispatch-arm hard-fail (pattern 11).
- `grep -n 'f.defaultVal\|f.hasDefault\|fillDefault\|encodeDefault' *.go` → auto-fill-default paths (pattern 10); follow each to whichever encode/decode arm it eventually hits and check the input-form match.
- `grep -n 'isNilValue\|extractTime\|decimalRatFor\|unwrapElemPtr\|boundedRatFromString\|unmarshalDefault\|indirectAlloc' *.go` → helpers that exist for a reason (pattern 14). For each, list callers and the parallel generic paths that don't call them; the open-coded check is usually narrower than the helper. **Then read each helper's docstring** and list every input shape it explicitly names — for each named shape, write a failing test against any parallel path that open-codes a narrower check (pattern 14a). A recently-patched "mirrors helper X" loop may have copy-pasted only the input the bug report exercised; the helper's other supported shapes are still failing on the patched path. **Helpers with safety semantics (UseNumber-using `unmarshalDefault`, bounded parsers like `boundedRatFromString`, overflow-guarded converters like `timeMicrosToDuration`) deserve special attention** — when *one* call site is protected, every site with the same input shape needs the same protection (pattern 14b).
- `grep -rn 'Props\[.*\].*float64\|Default .*float64\|\.(float64)' *_test.go` → type-pinned numeric values in tests (pattern 13b). For each hit, ask: could the pinned VALUE exceed the pinned TYPE's representable range? If yes, the type is wrong on the precision-edge case, and the test is silently locking the bug. Two passing pins (`Default != float64(18)` and `Props["custom.num"] != float64(42)`) hid the precision-loss class before this sweep was added.
- `grep -nE 'fmt\.Errorf\([^)]*%[qsv][^)]*\)' *.go | grep -v _test | grep -v truncForError | grep -v truncBytesForError | grep -v truncValueForError` → error-message amplification (structural blind spot "Error messages echo unbounded user-controllable input"). For each hit, classify the interpolated argument as schema-bounded (safe — echoing schema bytes is 1:1 with the schema input the user already accepted), wire-controlled (needs `truncForError` / `truncBytesForError` wrap — file as finding with a runnable failing test that measures `len(err.Error())` against a 1 MiB hostile input), or mixed (default-value content reachable via record/map-key wrap — also needs trunc). The grep noise is high (most hits are schema-bounded names that don't need wrapping); the structural question per-hit is "what's the largest legitimate version of this argument?" When the answer is "len(src) bytes" or "len(default-literal) bytes" or "unbounded via wire-string", the site needs a trunc wrap. Caveat: the three `grep -v trunc*Error` filters HIDE the deferred-render sub-pattern (a site that wraps `truncForError(r.RatString())` — the wrap is present but the render that precedes it is unbounded); run the separate `trunc(For|Bytes|Value)?Error\(…\.(RatString|FloatString|String|Text|Marshal)` grep from the structural-blind-spot entry for those.

For each pattern with N ≥ 3 sites:

1. List every site (file:line).
2. Note the specific differences between sites (which are intentional, which are drift).
3. Propose a helper signature and call shape.
4. **Perf-impact assessment** — required, not optional. State whether the helper inlines (Go's mid-stack inliner is ~80 nodes), and propose how to verify with benchstat. Helpers that don't inline kill hot paths and shouldn't be factored.

**DRY anti-patterns** — don't propose:

- Factoring two-site repetition. Two near-duplicates aren't drift-prone enough to justify the indirection.
- Factoring code that has *intentional* variation. If `deserBytes` and `deserString` look similar but the differences (interface vs string target, slab vs make+copy) are deliberate per the existing comments, leave them.
- Factoring closure factories that capture different state at construction. The boilerplate is irreducible.

DRY findings ship in the same format as bug findings, with one extra required field:

```
DRY: <one-line title>
Sites:
  - <file:line>: <one-line summary of this site's logic>
  - <file:line>: ...
  - <file:line>: ...
Drift risk: <which sites differ today, or which would diverge under what change>
Proposed helper: <signature + call shape>
Perf impact: <inline-able / measured neutral via benchstat / unknown — needs benchmark>
Pinning test: <a test that locks current behavior so the refactor can be verified equivalent>
```

## Audit conventions

These are the operating rules of the audit framework — how rounds proceed, what counts as a finding, and how claims are validated. They describe the framework's contract, not commands to a specific reader.

1. **Fix-application mode is opt-in.** Audit reports are useful even without code changes, so read-only is the default; the maintainer requests fixes explicitly ("fix" / "fix N" / "apply the patch"). When fixes are applied, the round isn't done until: (a) the full test suite (`go test -count=1 ./...`) passes after every edit, (b) the FIX.md after-fix sweep playbook has been worked through, and (c) every fix is paired with a positive regression test in the in-repo test suite (not just the sandbox), pinning both the rejected boundary (or new acceptance) and the boundary-1 cases that must still pass. Sandbox tests prove a bug exists; in-repo tests lock the fix.

   **🚫 DO NOT REFERENCE AUDIT ROUNDS, FIX NUMBERS, COMMIT SHAs, OR ANY AUDIT-INTERNAL LABELS IN REGRESSION TEST NAMES OR COMMENTS. EVER.** No `F1` / `F2` / `F11` / `audit round 85` / `F-round` / `the F2-round` / `pre-F1-fix` / `post-fix re-audit of F11` / `commit ae99f46` / `the audit's 100ms threshold` / `across the audit rounds` / `next-round audit caught` / `prior round's working-tree fix` / `round-85 fix added`. These labels rot the moment another round happens and they bias future auditors toward "this area was already covered." Regression-test comments document the **structural reason the behavior is correct**: the invariant being pinned, the boundary value being tested, the reference impl being matched, the failure mode being prevented. Never the audit-round narrative that produced the test. If you find yourself writing "this locks the Fn fix" — delete it; the test name and the structural explanation are the only legitimate documentation. A reader who has never heard of audit rounds, fix numbers, or commits should be able to read the comment and understand exactly what behavior is being pinned and why.

2. **Every claimed bug requires a runnable Go test that has been verified failing against the codebase as it currently sits on disk.** Verification means: a sandbox module (typically `/tmp/avro_audit_verify/`) that imports `github.com/twmb/avro`, the test file go-vetted, `go test -run TestRegression_<name>` run, and the failure output observed and quoted in the report. A test written without running is not a confirmed finding. The current git branch is not relevant — what matters is that the test fails against the source on disk.

   This applies **per site, not per bug shape**. Multiple sites with the same shape (e.g. "the same accumulator pattern in `foo.go`, `bar.go`, and `baz.go`") each need their own failing test. Phrases like "the same shape applies for defense in depth" or "this likely has the same gap" without a failing test for that specific site are not findings — those sites might have compensating downstream guards. Either write the failing test for the site (and it becomes a real finding) or drop the site from the report. Prior-round experience: an auditor flagged two siblings as "defense in depth"; both turned out to be already protected by an unrelated downstream check, and the speculative fix had to be reverted.

   **Sibling sweep is required, not optional.** After confirming a finding at site X, grep for the same shape (the dispatch arm, the stdlib call, the accumulator form) across the rest of the codebase. Either (a) confirm a second site fails with its own runnable test (now N findings, not 1), or (b) state explicitly which sites were grepped and why they are not affected — name the compensating guard or the structural reason. "Only looked at site X" is incomplete. Past audits: the array zero-byte cap drift affected 4 sites (2 buggy + 2 correct); the `big.Rat.SetString` DoS affected 4 sites across decode + encode. The first auditor of each found 1; the maintainer's sweep then found the rest. Audit-time grep is much cheaper than a separate next round.

   **Callback-firing claims need a value-TRANSFORMING callback.** An identity / value-preserving CustomType callback cannot distinguish "the callback fired" from "the raw value was coerced into the target" — the values coincide, so the probe passes either way. (Observed: an identity custom "confirmed" that a union-branch custom fires for concrete typed targets on bare-JSON decode; a ×10 transform showed binary=70 vs JSON=7 — the custom was being skipped and plain coercion produced the same number.) Any probe or pin whose claim is "this callback runs here" or "this callback is suppressed here" must use a callback whose output is distinguishable from its input — a marker recording `%T`/`%v` of what it received, or an arithmetic transform — so firing and non-firing produce different observable values.

3. **Spec and cross-implementation claims validate against `apache/avro:main`** — not 1.11.5, not 1.12.0, the current `main` branch. Authoritative sources, in priority order:
   1. Spec markdown source: `doc/content/en/docs/++version++/Specification/_index.md`. Quote verbatim; the rendered website is a paraphrase.
   2. Java implementation: `lang/java/avro/src/main/java/org/apache/avro/`. Cite specific file:line. When the spec is silent or ambiguous, Java's behavior is the de-facto contract.
   3. fastavro: cross-check.
   4. avro-rs (Rust): tertiary; flagged when it diverges from Java + fastavro consensus, but not treated as authoritative alone.

   Claims that depend on Java's behavior need the specific line that produces it.

   For VALUE-level Java behavior ("what bytes / what JSON does Java produce for input X"), the live oracle beats source archaeology: `testdata/oracle/SchemaOracle.java` accepts an `RT` command (binary-decode a value → re-encode to BOTH JSON and binary, base64 over the line protocol) driven from a `cisuite`-tagged test — `java_value_differential_test.go` is the pattern to copy. CI runs it against the real avro-tools jar, so the claim is verified against *executing* Java; and once a behavior becomes documented policy, ASSERT the parity in that test (not just log it) so an avro-tools upgrade that changes Java's behavior fails CI instead of silently rotting the documented rationale.

4. **Spec-divergence claims cite at least two implementations.** Java is mandatory; one of fastavro / goavro / avro-tools is the corroborating second. Prior auditors have made wrong claims about Java's behavior more than once — independent corroboration is what catches that. The rule applies in reverse too: asserting "library X does Y" requires reading X's source, not reasoning from API similarity. (Prior false claim: twmb/avro is a hamba fork. It is not — `git log --reverse` is the only authoritative source for lineage.)

5. **Source comments, commit messages, AND public-API doc strings in twmb/avro are not authoritative evidence.** Comments like "matches Java's X" or "mirrors fastavro's Y" or "unreachable: ..." are claims the maintainer made when the comment was written; they may have rotted, been wrong from the start, or describe an outdated state. The same applies to commit messages — a recent commit saying "all four dispatch sites agree on the nil-equivalence definition" is a claim about what the patch achieves, not proof. **Public-API doc strings are testable contracts**, not free-form description: a comment like "Root preserves all metadata including doc strings, namespaces, and custom properties" makes a claim about value-preservation that needs to be tested at the boundaries (precision-edge ints, large strings, unusual unicode, etc.) — reading the doc as truth instead of as a claim skips the verification, which is how the precision-loss bug at `Schema.Root` survived even though the doc string was effectively a regression-test specification. When a finding turns on whether a comment's, message's, or doc-string's claim is true, the verification goes against the cited source (`apache/avro` / `fastavro` / `hamba/avro` clones, or — for in-repo claims — the actual implementation). Specifically: a function comment saying "matches Java's coerceDefault" is not proof Java's coerceDefault behaves the way our code does; an inline comment marking a branch "unreachable" is not proof the branch is unreachable; a TestRegression name implying a behavior is locked in is not proof the test actually exercises that behavior; a commit message saying "mirrors helper X" is not proof the patch covers every shape `X` handles (verifying involves listing `X`'s docstring's named input shapes and writing a failing test against each); a doc-string saying "preserves all metadata" is not proof that >2^53 integer extras survive intact. Maintainer experience: (i) the union-default coercion comment claimed Java parity but the code diverged — three reference impls disagreed with the comment; (ii) a prior fix's message said `serNull` was brought "into parity" with `isNilValue` but only Interface peeling was added, not Pointer — a subsequent audit found `&nilPtr` still rejected; (iii) `Schema.Root`'s doc string promised "preserves all metadata" while two `json.Unmarshal`-into-any sites silently rounded ints > 2^53 — the doc string was a claim, the metadata path itself was lossy. Comments, commit messages, and doc strings are starting points for understanding intent; the source code (here and at the cited reference) is what's actually true.

6. **Findings cite exact `file:line` in `twmb/avro`.** Vague pointers don't qualify.

7. **Behavior bugs only.** Style nits, naming, "could be cleaner", linter modernizations, `b.Loop()` / `slices.Contains` suggestions are out of scope. A finding produces wrong output, a panic, a DoS, or measurable interop divergence.

8. **Reference-implementation clones live under `~/src/{org}/{repo}`** — preferred over WebFetch because `grep -r` and whole-file reads are faster and more thorough than `raw.githubusercontent.com` one-URL-at-a-time. WebFetch is the fallback when a clone is missing:
   - `~/src/apache/avro` — Java + Python + C + C++ in one repo (Java is the spec reference; `lang/java/avro/src/main/java/org/apache/avro/` is the path)
   - `~/src/apache/avro-rs` — Rust (separate repo)
   - `~/src/fastavro/fastavro` — most-used non-Java Python; the de-facto second-implementation cross-check
   - `~/src/linkedin/goavro` — Go-ecosystem reference, prior to twmb/avro
   - `~/src/hamba/avro` — Go-ecosystem reference, separate lineage; widely deployed
   - `~/src/iskorotkov/avro` — fork of hamba/avro with downstream changes worth diffing against hamba when investigating Go-ecosystem behavior

   twmb/avro's lineage: **not** a fork of hamba or goavro. `git log --reverse` shows `ab1f036 "initial avro code"` (2022) as the root with no inherited history; the code is original, even though the public API is naturally similar to other Go Avro libraries because Avro itself is the shared reference.

## Known intentional divergences (not findings)

These are deliberate choices in this codebase. Findings against them are not bugs; the rationale is documented at the cited source location.

- **Writer-union resolution is fail-fast at `Resolve` / `CheckCompatibility`** — every writer branch must be compatible with the reader; the first incompatible branch returns eagerly. Java's `Resolver.WriterUnion` defers via per-branch `ErrorAction` to decode time; we don't. Rationale: internal consistency with the rest of the package (`resolveEnum`, `resolveReaderUnion`, `resolveUnionUnion`, `resolveNode`, `validateDefault` are all fail-fast). See the doc comment on `checkWriterUnion` (compat.go) and `resolveWriterUnion` (resolve.go).

- **`DecodeJSON` on a resolved schema consumes WRITER-shaped JSON and applies full writer→reader resolution.** A schema from `Resolve(writer, reader)` decodes binary via the resolving `s.deser` (`Schema.Decode`); its `DecodeJSON` mirrors that for JSON by composing already-validated paths — decode the writer-shaped JSON with the writer schema into a faithful intermediate, re-encode to writer binary, then run the resolving binary decode (`decodeJSONResolved` in json_codec.go; `s.resolveWriter` holds the writer, set in `Resolve` only when writer≠reader — an identity resolve returns the reader schema directly, so `resolveWriter` is nil and `DecodeJSON` decodes against the reader node normally). This matches Java's `ResolvingDecoder` wrapping a `JsonDecoder` constructed with the WRITER schema (`Resolver.EnumAdjust.resolve` at `lang/java/avro/src/main/java/org/apache/avro/Resolver.java:388-399` maps a writer enum symbol absent from the reader to the reader's enum default; fastavro `_read_py.py:281-288` does the same — both read the WRITER symbol first) and produces results byte-identical to `resolved.Decode` of the corresponding writer binary: promotion, enum-symbol→reader-default, field add/drop, aliases, and custom-type suppression all resolve. Two deliberate consequences future audits should NOT re-flag: (1) the input is writer-shaped (feeding reader-shaped JSON to a resolved schema may error on writer fields the reader dropped — that is correct, resolution consumes writer data); (2) the JSON path is a decode→re-encode→resolve round-trip, NOT a single-pass resolving JSON decoder — resolution is not throughput-critical, and reusing the tested binary resolver keeps the surface small and correct by construction (a single-pass resolving JSON decoder would be a large new-bug surface for no hot-path benefit). Pinned by `TestRegression_ResolvedDecodeJSONMatchesBinary` (binary-is-oracle across promotion / enum-default / structural / alias / custom-suppression shapes, plus the explicit enum→reader-default value).

- **Decimal JSON decode accepts the spec form (codepoint-mapped string) and bare numbers (`0.33` unquoted).** Encode emits the spec form only. Java is strict (codepoint-only). The bare-number leniency handles hand-edited JSON and twmb/avro's pre-fix output that emitted bare numbers. Quoted-numeric strings (e.g. `"0.33"`, the form linkedin/goavro produces with `EnableDecimalBinarySpecCompliantEncoding`) are NOT accepted as numeric — the string is interpreted via codepoint mapping per the spec, since there's no way to disambiguate a quoted-numeric input from a spec-form byte sequence that happens to contain ASCII digits without ambiguity. Producers targeting twmb/avro should emit the spec form or the bare-number form. The codepoint-mapped string is accepted only as *valid JSON* — control chars / high bytes escaped as `\u00XX` (what the encoder emits); a raw unescaped codepoint form rejects per the JSON-string-strict entry below. See `decodeBytes` and `decodeFixed` in json_decode.go and `TestRegression_DecimalBytesJSONStringIsCodepointForm`.

- **JSON decoder accepts non-canonical multi-byte varints** for null-union branch indices (e.g. `0x80 0x00` for index 0). Java's `BinaryDecoder.readIndex` does the same. Not a divergence — listed here to prevent re-finding it as one.

- **Bare (untagged) JSON union decode commits to the FIRST declaration-order branch of the matching JSON token class — the writer's branch (and any CustomType on a later same-class branch) is NOT recoverable.** The untagged wire is the bare value only: encoding `int32(7)` (int branch) and `int64(7)` (long branch) into `["long","int"]` produces the IDENTICAL wire byte `7`, so no decoder logic can recover the writer's branch — two distinct (branch, value) pairs map to one wire; the encode is non-injective and the loss is information-theoretic, not a dispatch bug. Verified consequences: decode-into-`any` yields the first token-class branch's Go type (`int64` where binary's index dispatch yields `int32`); and a TRANSFORMING CustomType on a non-first same-class branch is silently skipped on EVERY decode-target shape — including a CONCRETE typed target, which is filled by plain coercion from the first branch's raw value (×10 custom Decode: binary=70, untagged JSON=7; an identity custom masks this as value coincidence — Pattern 13b, compare transformed values, not just success). `TaggedUnions` on encode+decode fully recovers the branch (tagged wires `{"int":7}` / `{"long":7}` differ; verified 70 with the transforming custom; into `any` it yields the documented `{branch: value}` envelope, which is itself a different shape than binary's bare value — also intentional, see the TaggedUnions doc). Java has no bare-union JSON decode at all (its JsonDecoder rejects bare unions with "Expected start-union"); the bare default is twmb's documented goavro/hand-edited-JSON leniency (TaggedUnions doc, AVRO-2899), so the recovery bound is inherited from that design choice and is not fixable inside it. Do NOT add branch-guessing heuristics (prefer-the-custom-branch, mimic-the-encoder's-Go-type-choice): the bare wire carries no type information, so every heuristic rests on an input-frequency assumption and mis-decodes values legitimately written via the other branch. Documented on TaggedUnions + DecodeJSON. Pinned by `TestRegression_UntaggedUnionBranchClassFirstMatch`.

- **JSON encode of an Avro string containing invalid UTF-8 coerces each invalid byte to U+FFFD; binary preserves the bytes verbatim.** An RFC 8259 JSON string cannot carry a raw non-UTF-8 byte, so byte-faithful binary↔JSON parity is IMPOSSIBLE for this input class — the divergence is inherent to the wire formats, not a codec bug. Verified byte-for-byte against the Java reference in CI (`TestDifferentialJavaInvalidUTF8`, driving `SchemaOracle`'s `RT` binary-decode→re-encode round-trip): Java's `JsonEncoder` emits the IDENTICAL U+FFFD bytes (`ef bf bd` per invalid byte — `"A\xffB"` → `22 41 ef bf bd 42 22` on both impls) and Java's `BinaryEncoder` re-encodes the raw bytes verbatim, for top-level strings and record fields alike. twmb matches Java on BOTH wires (`appendJSONString` json_codec.go — its in-code comment covers only JSON-internal idempotency; the cross-format contract lives here — vs verbatim `doSerString` ser.go), and the behavior spans string values and map keys at every nesting depth. Do NOT "fix" either side toward the other: making binary lossy would corrupt the byte-faithful wire (and diverge from Java's `BinaryEncoder`); making `EncodeJSON` reject would diverge from Java's lenient coercion. Documented on `Schema.EncodeJSON`. Pinned by `TestRegression_InvalidUTF8StringBinaryVerbatimJSONCoercion`.

- **Local-timestamp encode uses wall-clock-as-UTC**, mirroring Java's `TimeConversions.LocalTimestampMillisConversion`. Some auditors have flagged this as "should encode the absolute moment"; it shouldn't.

- **OCF bzip2 and xz codecs are not supported.** Java and fastavro support both. We deliberately don't carry the third-party deps (`dsnet/compress` for bzip2 write; `ulikunitz/xz` for xz). Users can register a custom codec via `ocf.WithCodec`. If a real interop case lands (a user explicitly needs to read OCF files compressed with these), revisit; the built-in deps stay zstd/snappy/deflate.

- **Whole-number floats encode against `int` / `long` schemas.** `s.Encode(&v)` for `v float64 = 42.0` against `"int"` produces wire bytes equivalent to `int32(42)`; fractional floats (`42.5`) error with "not a whole number". Java's `GenericDatumWriter` rejects `Float`-as-Integer at the type system; fastavro rejects float-as-int. We accept whole-number floats deliberately because `encoding/json.Unmarshal` produces `float64` for every JSON number (the common dynamic-schema flow). Rejecting would force every `json.Number` round-trip user to insert an explicit conversion. Wire format is unchanged — interop is preserved. See `serInt`'s `elem.CanFloat()` arm at ser.go and `TestRegression_WholeFloatEncodesAsInt`.

- **String-form float defaults at single-field outer type only (`{"type":"float","default":"1.5"}`)** are accepted at parse and materialize as `float32(1.5)` / `float64(2.718)`. Java parity with `parseField`'s text→DoubleNode coercion at `Schema.java:1899-1902` — the coercion fires only when the OUTER `fieldSchema.getType()` is FLOAT or DOUBLE directly. avro-rs's `resolve_float` / `resolve_double` (types.rs:960-986) reject regular numeric strings but accept the IEEE 754 special tokens (NaN, INF, Infinity, -INF, -Infinity); twmb's `parseFloatAcceptOverflow` accepts both (regular numeric strings and special tokens), preserving the Java single-field surface. Per Avro 1.12 §"Record" default-values table, JSON string is invalid for float/double defaults; the Java coercion is a deployed-Java extension preserved for interop with Java-generated schemas. **Union-shape string-numeric defaults reject** at parse, matching Java's `isValidDefault` UNION arm (the text→DoubleNode coercion does NOT fire for union outer types), avro-rs's `resolve_internal` for union (no branch accepts a `Value::String` for numeric branches), and goavro's record-field default validator (rejects Go-string against float/double type-assertions). See `defaultAsFloat`'s strict body at schema.go (no string arm — strings handled UPSTREAM by `coerceDefault`'s single-field arm via `parseFloatAcceptOverflow`), and `TestParseFloatDefaultFromString` + `TestRegression_UnionDefaultStringMatchesOnlyStringAcceptingBranches` for the full matrix.

- **DOS-resistance defense-in-depth.** twmb caps several values that Java and fastavro leave unbounded: `errTooDeep` schema/encode/decode recursion at 1000 levels (Java/fastavro rely on language stack); `maxZeroByteItems=4096` for `array<null>`/`array<EmptyRecord>` block-counts (Java has only a 2 GiB cap; fastavro none); `maxMapPreAllocSize=4096` (Java pre-allocates `HashMap((int)l)` from wire-controlled count; fastavro no pre-alloc but also no early bound); `WithMaxBlockBytes` OCF compressed-block ceiling (default 64 MiB; Java only rejects >2 GiB; fastavro no check); `WithMaxDecompressedBlockBytes` OCF DECOMPRESSED-block ceiling (default 64 MiB; enforced inside each built-in codec's Decompress + a post-decompress backstop, bounding decompression-amplification "zip bombs" — an ~89-byte snappy frame would otherwise demand ~200 MiB — and transitively the per-block decode loop; Java/fastavro leave the decompressed side unbounded) plus a per-block consecutive-zero-byte-record cap (`zeroRun ≤ maxOCFZeroByteSlack` in `Reader.Decode`, the dynamic schema-agnostic form of maxZeroByteItems for OCF data blocks; the WRITER enforces the same bound — `shouldFlush`'s `count >= len(buf)+maxOCFZeroByteSlack` clause — so twmb-written files of zero-byte datums are always twmb-readable; the general lesson: every reader-side cap needs a producer-side compliance check, because a cap our own writer can exceed is a self-incompatibility (with ONE deliberate exception — the OCF block-size caps `WithMaxBlockBytes`/`WithMaxDecompressedBlockBytes`, which are reader-only BY DESIGN; see the dedicated "OCF block-size caps are reader-only" entry below before filing any block-size self-incompatibility) — found exactly that way for top-level `"null"`/all-null-record/size-0-fixed OCFs, pinned by `TestWriterZeroByteDatumsSelfReadable`, then TWICE MORE for the same class: the CORE array `serArray.ser` (`maxZeroByteItems` applied to `array<null>`/`array<EmptyRecord>`/`array<size-0-fixed>` only on decode — the encoder now measures the body and rejects an over-cap all-zero-byte array, `TestRegression_ArrayZeroByteProducerCompliance`), and the OCF METADATA caps (`ocfMetadataSafetyLimit` on every header value vs an unbounded writer — the self-describing `avro.schema` value got a dedicated larger bound `ocfSchemaSafetyLimit=64 MiB` since wide-record JSON legitimately exceeds 1 MiB and Java/fastavro read it; user metadata kept the 1 MiB cap with a matching writer check, `TestRegression_OCFLargeSchemaSelfReadable` + `TestRegression_OCFLargeUserMetadataProducerCompliance`). **Why all three survived the matrix until an external audit: the matrix sweeps SHAPE at SMALL scale (collections of 0..4, small schemas), but every cap lives at LARGE scale (4096, 1 MiB, 65536, 1000 levels) — a small-value generator structurally never reaches a cap boundary.** The standing generative guard for this class is now `TestMatrix_SelfReadableAtScale` (the SCALE axis): drive every degenerate shape ACROSS its cap boundary and assert the calibration-free inverse of the bug — Encode-succeeds ⟹ Decode-of-that-wire-succeeds on both wires (a clean encode-time rejection is fine; a wire the producer emits and the consumer refuses is the only failure). Any NEW cap added to the codec MUST add a generator there); `maxIndirectDepth=5` reflect-unwrap chase; `decimalScaleLimit=65536` caps decimal/big-decimal scale + precision at schema parse and wire decode, preventing attacker-controlled `10^scale` `big.Int` allocations (Java's `BigDecimal.fromBytes` accepts int32 scale but defers `10^scale` until `setScale()`/`toString()`; avro-rs same; only twmb materializes the magnitude during decode, so the cap has to live here). Marketing claim and concrete CVE-class hardening; future audits may flag these as "too restrictive" — they are deliberate.

- **OCF block-size caps (`WithMaxBlockBytes` / `WithMaxDecompressedBlockBytes`) are READER-ONLY by design — the deliberate EXCEPTION to the producer-compliance lesson above.** Unlike the zero-byte and metadata caps, the writer has NO producer-side block-size compliance check, intentionally. A default writer CAN emit a single block — e.g. an 80 MiB datum, which by spec cannot be split across blocks — that a default reader then refuses; **this is EXPECTED, not the self-incompatibility bug class.** Rationale: (1) the block-size caps are a reader-side DoS defense against UNTRUSTED input — a file the writer produced itself is not untrusted, so enforcing the reader's DoS cap on the writer is conceptually wrong; (2) it matches Java's `DataFileWriter` and fastavro, which write freely with no writer-side block cap; (3) a single large datum is LEGITIMATE (Java/fastavro read it) and cannot be split, so producer enforcement could only ERROR on a valid value, not fix anything; (4) the reader already errors ACTIONABLY — both the compressed (`WithMaxBlockBytes`) and decompressed (`WithMaxDecompressedBlockBytes`) reader errors name their option — so the user raises the matching reader cap to read large blocks. Producer-side enforcement WAS implemented and reverted (3 commits, reset away): it carried a data-loss trap (rejecting an oversized datum at `flush` discarded the prior buffered datums `Encode` had already accepted) and an unclosable compressed-cap residual (compressed block size is unknowable without compressing). Contract lives in the `ocf` package doc's "Block size limits" section; pinned by `TestRegression_OCFLargeDatumReaderCap` (writer accepts a large datum, default reader refuses with an actionable error, raised reader reads it back). **AUDITORS: do not re-file "default writer emits a block the default reader rejects" as a self-incompatibility, and do not re-add producer-side block-size enforcement.** The producer-compliance lesson explicitly EXCLUDES the block-size caps for the reasons above; it still applies to the zero-byte and metadata caps, which DO have producer checks. (Note: `TestMatrix_SelfReadableAtScale`'s "Encode-succeeds ⟹ Decode-succeeds" invariant is unaffected — it asserts that a wire the producer EMITS the consumer reads; here the producer emits a large datum and the same-cap default consumer's refusal is by-design, so any scale generator for OCF block size must raise the reader caps to match, exactly as the pinned test does.)

- **Degenerate-cardinality types parse: size-0 fixed, zero-symbol enums, zero-branch unions.** All three are reference-legal and twmb accepts them. Evidence (executable where possible): size-0 fixed — Java's own CI constructs and DECODES it (`TestGenericDatumReader.arrayOfZeroLengthFixedAcceptsLargeCount`, `minBytesPerElement(createFixed(…,0))==0`; `SystemLimitException.checkMaxBytesLength` rejects only negatives), fastavro and the official Apache Python both parse + 0-byte wire round-trip (executed), avro-rs `as_u64` accepts 0; empty union — official Apache `share/test/data/schema-tests.txt` vector 016 (`[  ]` → canonical `[]`, Rabin `-1241056759729112623`, Java-validated; the vendored copy is byte-identical and the vector is live in `TestApacheSchemaTestsVectors`), fastavro + Apache Python parse it (executed), Java `UnionSchema`'s constructor no-ops on empty, avro-rs logs but accepts; empty enum — Java's parse path (`parseEnum` → `EnumSchema` ctor, `Schema.java:1910-1933`/`1089-1104`) has no count check, fastavro + Apache Python parse it (executed), avro-rs checks dups/default only. hamba REJECTS empty enums (hamba#295) — twmb sides with the four references against the Go ecosystem here. Semantics: a size-0 fixed is USABLE (every value is the empty byte string, 0 wire bytes, `""` JSON form); empty enums/unions are unusable-but-parseable (every encode/decode of the node errors — never panics — and any DEFAULT against them rejects: no symbol/branch can accept one, so union-default branch selection skips them, Java's `anyMatch` parity). The acceptance matters for schema passthrough: foreign schemas carrying a degenerate type in a position the data never exercises (e.g. a never-selected union branch) must stay readable. Nested-union rejection covers the empty union too (`[[]]` rejects — Java's "Nested union" fires on the UNION type, branch count irrelevant). A MISSING required attribute still rejects (`{"type":"enum"}` with no symbols, `{"type":"fixed"}` with no size) — absence ≠ emptiness. Union-ness is discriminated by `s.union != nil` (the parser materializes `[]` as a non-nil empty slice); do not "simplify" those checks back to `len() > 0`. Pinned by `TestRegression_FixedSizeZero*`, `TestRegression_EmptyEnum*`, `TestRegression_EmptyUnion*`, `TestHambaEmptyEnumSymbolsAccepted`, and oracle vector 016. Do NOT re-tighten any of the three for "eager-fail consistency" — that was the pre-fix state, and it rejected schemas every reference accepts.

- **Valid JSON for special floats.** EncodeJSON emits `"NaN"` / `"Infinity"` / `"-Infinity"` as quoted strings by default (RFC 8259 valid). Java's JsonEncoder emits bare tokens (RFC 8259 invalid); fastavro defers to Python's `json.dumps(allow_nan=True)` (also bare). twmb is the only impl whose default output round-trips through strict JSON consumers (jq, Postgres jsonb, browser parsers). `LinkedinFloats` option swaps to goavro convention (NaN→null, ±Inf→±1e999). DecodeJSON accepts all four forms (Java string-form, goavro 1e999, fastavro bare-tokens, twmb quoted strings).

- **Magnitude-based decimal precision count.** `checkDecimalPrecision` counts digits of the unscaled big.Int magnitude. Java's `BigDecimal.precision()` and fastavro's `len(digits)` count *significand* digits (e.g., `BigDecimal("1E10").precision() == 1`, so they'd accept precision=2 for the wire value `10000000000` despite 11 magnitude digits). Go's `big.Rat` has no significand/exponent split, so the magnitude count is structurally forced — and is the safer direction (round-trip-safe regardless of how the receiver counts).

- **Implicit null default for `["null", T]` unions.** When the schema declares `["null", T]` without an explicit default, twmb infers `null` as the default. Java and fastavro require it explicit (no inference). Improves schema-evolution ergonomics for the canonical nullable pattern. See schema.go's union-default synthesis and `TestParseImplicitNullDefault`.

- **Per-schema custom-type registration scope.** `CustomType` passed via `SchemaOpt` to `Parse`; the decoder map is frozen on the resulting `Schema`. Java's `LogicalTypes.REGISTERED_TYPES` (LogicalTypes.java:58) is a process-global ConcurrentHashMap; fastavro's `LOGICAL_READERS`/`LOGICAL_WRITERS` are module-level dicts. twmb's per-Schema scope avoids global-state hazard — a buggy custom type registered for one Schema can't affect Schemas that didn't opt in. Also: re-registration is order-stable with `ErrSkipCustomType` fall-through, vs Java/fastavro last-write-wins.

- **Eager schema-resolution fail.** `Resolve` and `CheckCompatibility` reject incompatible writer-union → reader-non-union pairs at config time; Java (`Resolver.WriterUnion`) and fastavro (`read_union`) defer to decode time via per-branch ErrorAction. A producer that narrowed during evolution but never emits the dropped branch must update its schema before `Resolve` accepts; in Java/fastavro decode-time errors can fire months later on the first record using the affected branch. Deliberately stricter (already documented above for writer-union case; this entry generalizes).

- **Strict UUID format on decode — for the `[16]byte` (and `*uuid`-typed) target.** `parseUUID` requires the 36-char hex-dash canonical form per RFC 4122. Older JDKs (<15) accepted variable-length hex segments (AVRO-2497); modern JDK matches twmb. fastavro's `uuid.UUID(data)` accepts five formats including 32-char no-dash and 38-char braced. twmb aligns with the modern strict position. NOTE: this applies only to the parse-into-`[16]byte` path; a plain Go `string` target for a `uuid`-logical schema is decoded as a raw Avro string with NO UUID-format validation (deser.go — "UUID-on-string is wire-equivalent to plain string"), symmetric on encode. So "strict UUID" is a property of the `[16]byte` target, not of every uuid-schema decode.

- **Snappy CRC verification on OCF read.** twmb explicitly verifies the trailing CRC32 after Snappy decompression (`ocf.go`); Java's `SnappyCodec.decompress` also verifies; fastavro reads the 4 CRC bytes but never compares (`_read_py.py:729-733`) — silent corruption pass-through.

- **JSON DecodeJSON fills schema-declared defaults for absent record fields.** A JSON object missing a field that has a schema default materializes the default into the target (map[string]any, typed map, struct). Matches fastavro (`io/json_decoder.py:55-78`); Java's JsonDecoder rejects missing fields outright (`JsonDecoder.java:498-530`). Aligned with the project's lenient-acceptance + least-surprise principle. See `iterateRecordFields`'s `fillDefault` callback and `TestSpecJSONDecodeFillsDefaultForMissingField`.

- **`CustomType.Encode` is bypassed for default-filled record fields; `CustomType.Decode` is applied.** The two callbacks have directional contracts (encoder: Go-domain-type → Avro-native; decoder: Avro-native → Go-domain-type) and defaults live on the Avro-native side of that direction. The default value is stored in parsed Avro-native form (`json.Number` / `[]byte` / `string` per the schema's type) — there is no user-Go-type representation for a library-inserted default — so the encoder has nothing to convert; the decoder still has work to do (Avro-native → user's typed struct field). Binary has had this behavior since the original CustomType design: `encodeDefault` (resolve.go) is a self-contained switch that never reaches `custom[node]`, and pre-encoded `f.defaultBytes` roundtrip through the wrapped `deserRecord.fields[idx].fn` on decode (which fires the chain). JSON matches: `appendJSONFieldDefault` (json_codec.go) recurses into `appendAvroJSON` with `custom=nil`, and `applyFieldDefault` (json_decode.go) dispatches through `node.deserRecord.fields[idx].fn` (the wrapped deser). The encode-side bypass is invisible for the typical GoType-typed encoder pattern (`v.(MyType) → ErrSkipCustomType` fallthrough) but matters for GoType=nil patterns used for logging / validation / property-based dispatch: those fire only on user-supplied values, not on library-inserted defaults. See `TestRegression_DecodeJSONFillsDefaultThroughCustomDecoder` and `TestRegression_EncodeJSONBypassesCustomEncoderForDefaultFill`.

- **Wrapped-form name references accepted, including forward refs.** `{"type":"Node"}` is accepted as a name reference both when `Node` is already declared and when it's a forward reference (will be declared later in the schema). Matches Java's parser (`TestUnionSelfReference.java:50`). fastavro and hamba reject the wrapped form. See `buildComplex`'s named-table lookup + `unknownPrimitiveError` fwd-ref signal at schema.go and `TestSpecBareTypeNameInObjectAccepted`.

- **Name references bind EAGERLY at the point of reference, in-scope-first.** A bare (dot-free) reference resolves against the types defined SO FAR: the enclosing-namespace-qualified name first, then the bare (null-namespace) name — Java's `Names.get` order (`Schema.java`: `new Name(o, space)` looked up before the null-space retry; fastavro qualifies unconditionally). A reference that resolves at reference time KEEPS that binding — a type defined later does not retroactively rebind it (old-Java eager-parse semantics; Java's newer `ParseContext` defers all resolution to end-of-parse and can bind differently for the pathological "null-ns type defined before, in-scope type defined after" shape — twmb deliberately keeps the deterministic eager model). Only a reference with NO match at reference time defers to finalize, where the same in-scope-first rule runs over the complete table. The three parallel resolvers — `resolveNamedRef` (wire build + finalize), `lookupCanonDef` (canonical), `lookupNameRef` (metadata default coercion, ns-threaded) — derive their key order from ONE helper (`scopedRefKeys`, schema.go), so the precedence cannot drift between them. The table REGISTRATION side carries the same lockstep duty: every resolver's table registers types under exactly the keys the wire builder registers — the fullname ONLY (a null-namespace type's fullname IS its bare name, so it owns the bare key). A convenience short-name registration for namespaced types makes the bare key last-walked-wins and bound a bare reference at null-namespace scope to a different type than the wire whenever short names collided. Pinned by `TestRegression_BareNameRefBindsInScopeBeforeNullNamespace`, `TestRegression_MetadataDefaultShortNameCollisionWalkOrder`, and the eager/deferred sibling cases in `TestRegression_UnionForwardRefDuplicateOrderIndependent`.

- **Parsing Canonical Form is a fingerprint surface, not a round-trip surface — `Parse(s.Canonical())` can fail for null-namespace types nested in a namespaced scope.** The PCF [FULLNAMES] transform writes every name as its fullname and drops namespace attributes; a null-namespace type's fullname is its bare name, which re-READS inside a namespaced scope as inheriting that scope — so the canonical form of such a schema is ambiguous (re-parse either rebinds or reports a duplicate definition). Java's `SchemaNormalization.build` emits the byte-identical ambiguity (`name.getFullName()` with no escape syntax for refs), and matching Java's bytes is the entire point of PCF (fingerprint interop). Do NOT "fix" the canonical emitter to re-parse at the cost of diverging fingerprints. The same inherent limit applies to `SchemaNode.Schema()` dedup REFERENCES to a null-ns type from inside a namespaced scope (references have no `"namespace":""` escape syntax — only definitions do; Java's `getQualified` has the identical gap). Noted in `TestRegression_UnionForwardRefDuplicateOrderIndependent`'s lossyCanonical cases and `TestRegression_BareNameRefBindsInScopeBeforeNullNamespace`.

- **SchemaNode.Namespace is the RESOLVED namespace; re-emission uses Java's writeName relative-namespace rule.** `Root()` populates `Namespace` for every named type — an inheriting child surfaces the inherited namespace, and `""` always means the null namespace, never "inherit" (matching Java's `getNamespace()`, which is likewise resolved). `SchemaNode.Schema()`'s walker emits the namespace relative to the enclosing scope: omitted when equal, `"namespace":""` to escape inheritance for a null-namespace type inside a namespaced scope (Java `Name.writeName`'s "null within non-null" arm), the value otherwise; dedup and references key on the FULLNAME. Hand-built trees follow the same contract (`""` = null namespace). The as-written placement of the namespace attribute (inherited-vs-explicit spelling) is intentionally NOT preserved — schema identity (fingerprint) is what round-trips, not attribute spelling. See the `SchemaNode.Namespace` field doc and `TestRegression_SchemaNodeNullNamespaceEscapeRoundTrip` / `TestRegression_SchemaNodeSameShortNameDistinctNamespaces`.

- **Aliases (type AND field) accept ANY string — deliberately NOT name-validated. Do not re-add validation.** The Avro spec §Aliases states "any string is accepted as an alias," specifically so schema evolution can alias a reader's valid name to a writer's illegal/legacy name; Java (`Field.addAlias` / `Name`) and fastavro do no alias-name validation. twmb formerly grammar-checked aliases (type aliases via fullname grammar, field aliases via simple-name grammar), making it the lone impl that rejected conformant Java/fastavro schemas using a name-correcting alias — a real interop break. The validation was relaxed: the old strict behavior was the divergence, and this now MATCHES the spec. A future audit comparing twmb to "strict name validation" will see aliases accept `1stField`, `com.example.legacy_x`, `weird!`, etc. — that is intentional, not under-validation. Type *names*, field *names*, and enum *symbols* stay strictly validated; only aliases relax. The leading-dot null-namespace escape survives as a *qualification* rule (not a grammar gate): `qualifyAliases` (schema.go) strips exactly one leading dot, so `.Name` aliases the null-namespace fullname `Name` (the only way to alias a null-ns name from inside a namespaced type, since a bare alias qualifies into the type's own namespace). Resolution matches aliases as plain strings; PCF strips aliases so fingerprints are unaffected. See `qualifyAliases` (schema.go), `TestRegression_AliasAcceptsAnyString`, `TestRegression_LeadingDotAliasNullNamespace`.

- **OCF Writer: value errors recover; I/O and compression errors poison.** A failed `Encode` whose error is a value error (the datum doesn't fit the schema) discards only that datum — `AppendEncode` is append-only into the block buffer, so not adopting the returned slice restores the pre-call state exactly; previously accepted datums survive and flush, and the Writer stays usable. Mirrors Java `DataFileWriter.append`'s buffer truncation. I/O, compression, and flush errors still poison (the sink's state is unknowable), and `Close` still closes the codec in the poisoned state. See `Writer.Encode`'s doc (ocf.go) and `TestRegression_OCFWriterValueErrorRecovers` / `TestEncodeError` / `TestCompressError`.

- **big-decimal canonical-scale (no trailing-zero preservation).** AVRO-4124 wire layout: outer bytes = inner-bytes-framed unscaled (two's-complement big-endian) + zigzag varint scale; only Java and avro-rs implement it (fastavro and hamba do not). twmb encodes `*big.Rat` natively via scale derivation: for `n/d` with `d = 2^a · 5^b`, the canonical scale is `max(a, b)`; non-terminating rationals (`1/3`, denominator with prime factor != 2, 5) are rejected at encode rather than silently rounding (mirrors the regular decimal type's `ratToUnscaled` policy). Java's `BigDecimal` preserves user-declared scale: `BigDecimal("3.14")` emits scale=2 while `BigDecimal("3.140")` emits scale=3 — twmb cannot reproduce that distinction because `big.Rat` reduces to lowest terms (both inputs become `157/50`, both emit canonical scale=2). Equivalent to running `BigDecimal.stripTrailingZeros()` before Java's encoder. Decoder tolerates negative scale (Java/avro-rs may emit via `new BigDecimal(unscaled, -3)` representing `unscaled * 10^3`) by computing the integral-valued `*big.Rat`. See `serBigDecimal` / `deserBigDecimal` / `finiteScale` / `parseBigDecimalPayload` and `TestSpecBigDecimalWireFormat` (Java ground-truth byte match against `bigdec.avro`'s 2.24 wire form).

- **Schema metadata numerics normalize by VALUE, not by syntax or Go type — `Props` uses value-based dispatch; `Fields[].Default` adds schema-width-faithful narrowing on top.**

  **`*.Props` (record/field/CustomType-receiver, no schema attached to the value):** the literal's mathematical value drives dispatch:

  | JSON literal | Mathematical value | Returned Go type |
  | --- | --- | --- |
  | `42`, `1e3`, `9.5e17`, `9.2233720368547758e18` | Exact integer fitting int64 | `int64` |
  | `99999999999999999999` (integer-syntax beyond int64) | Exact integer exceeding int64, written without `.`/`e` | `json.Number` (preserved) |
  | `9.223372036854775808e18` (fractional/exp syntax beyond int64) | Exact integer exceeding int64, written with `.`/`e` | `float64` |
  | `1e308` | Exact integer exceeding int64, fits float64 | `float64` |
  | `1e1000`, `-1e1000` | Overflows float64 | `float64(±Inf)` |
  | `1.5`, `3.14e0`, `1e-3` | Non-integer | `float64` |

  The literal's syntactic shape (`.`/`e` vs pure digits) does not affect the returned Go type — `1e3` and `1000` both return `int64(1000)`. Bare `json.Unmarshal`-into-any (Go's stdlib default) forces every JSON number to `float64` with silent precision loss > 2^53; we trade Go-type round-trip preservation for precision (matches Java's `LongNode` + fastavro's Python int — both preserve precision via their JSON parser's native types, which Go's stdlib does not). Value-based dispatch (vs syntax-based) further eliminates a metadata-vs-wire divergence: under syntax-based dispatch (the prior contract), a `"default":9.2233720368547758e18` against a long field had wire = `int64(9223372036854775800)` but metadata = `float64(~2^63)`; the two surfaces disagreed about the default value. Value-based dispatch normalizes both to `int64(9223372036854775800)`.

  **`Fields[].Default` (schema-typed):** the parsed value is then narrowed to the schema's wire width, so `Default`'s Go type matches both the wire bytes and the user's natural Go field type:

  | Schema | Default Go type | Wire |
  | --- | --- | --- |
  | `int` | `int32` | `int32` (zig-zag varint) |
  | `long` | `int64` | `int64` (zig-zag varint) |
  | `float` | `float32` | `float32` (LE IEEE 754) |
  | `double` | `float64` | `float64` (LE IEEE 754) |

  Out-of-range integer defaults are rejected at schema parse, so int32/int64 `Default` always carry the exact wire value. float32 narrowing surfaces overflow as ±Inf (`{"default":1e100,"type":"float"}` → `float32(+Inf)`), matching the wire bytes. Matches Java's `JacksonUtils.toObject(jsonNode, schema)` (`lang/java/avro/src/main/java/org/apache/avro/util/internal/JacksonUtils.java:150-155`) which narrows `DoubleNode → Float` for FLOAT schemas. **Unlike `Props`, `Default` for numeric fields is NEVER `json.Number`** — schema-parse-time validation guarantees the value fits the target.

  Programmatic `SchemaNode` round-trip via `.Schema().Root()` therefore narrows typed numerics (`float64`-into-float schema → `float32`, etc.) to the schema's wire width — the wire format is unaffected. Users must type-switch rather than asserting `.(float64)` for narrow schemas. See `normalizeJSONNumber` + `coerceMetadataDefault` in `schema_node.go` and `TestRegression_SchemaMetadataNumericPrecisionPreserved`.

- **Precision policy: the READER schema is the user's contract.** The reader schema is what the consumer's code sees and what the encoder writes against — the user's explicit choice of representation. Two sub-rules:

  **(a) When the reader schema is lossy (`float` / `double`), encode AND decode both silently IEEE-round.** By choosing a lossy schema the user opted into IEEE-precision semantics; loss in either direction is acceptable. Concretely: `s.AppendEncode(int64(9007199254740993), "double")`, `s.AppendEncode(json.Number("9007199254740993"), "double")`, `s.AppendEncode(float64(1e300), "float")`, and their JSON-encode counterparts all SUCCEED and silently IEEE-round to the destination's representable range. Wire bytes are the same as if the caller had written `s.AppendEncode(float64(int64(9007199254740993)), "double")` themselves. Matches Java's `GenericDatumWriter.writeDouble(((Number) datum).doubleValue())` (`lang/java/avro/src/main/java/org/apache/avro/generic/GenericDatumWriter.java:180`) — `Long.doubleValue()` is silent IEEE narrowing — and fastavro's `struct.pack("<d", datum)` (`fastavro/io/binary_encoder.py:42-43`) which calls Python's silent `float()` coercion. Hamba is structurally type-strict (`int64` → `Double` returns `errorEncoder{"avro: int64 is unsupported for Avro double"}` at `~/src/hamba/avro/codec_native.go`) — twmb sides with the wire-format reality and the JVM/Python ecosystem majority over Hamba's stricter position. The lossy-destination principle removes a path-divergence bug class: `int64(big)` and `json.Number("big")` and `float64(big)` of the same magnitude now agree on wire output.

  **The same rule applies to RESOLVED promotion.** When the reader schema is `double` (or `float`) and the writer was an int / long, the reader-schema's lossy semantics govern decode: the wire value silently IEEE-rounds at the long→double cast. Matches Java's `ResolvingDecoder.readDouble`'s `(double) in.readLong()` (`lang/java/avro/src/main/java/org/apache/avro/io/ResolvingDecoder.java:192`), fastavro's `maybe_promote` returning `float(data)` (`fastavro/_read_py.py:619-621`), and hamba's `createDoubleConverter` doing `float64(r.ReadLong())` (`hamba/avro/converter.go`). The principle: the user evolved the schema to a lossy type; the wire-was-once-long is a backward-compat detail, not a contract.

  **(b) When the reader schema is exact (`int` / `long` / `bytes` / `string`) and the Go target is a lossy type, decode is allowed ONLY IF the wire value can be exactly represented in that Go type.** The user did NOT opt into a lossy reader schema; they chose a Go type that happens not to fit. The wire faithfully preserved an exact value; silently losing it would mean downstream code sees a value different from what was written. Concretely: `s.Decode(longWire(9007199254740993), &f float64)` against `s = MustParse("long")` REJECTS via `setLongValue`'s `intFitsFloat` arm; values ≤ 2^53 succeed exactly. `s.Decode(doubleWire(1e300), &f float32)` REJECTS via `setFloatValue`'s `finiteFloat32Overflows` arm because here the reader schema is double (lossy at the schema layer) but the Go target is narrower than the schema — Go-type loss BEYOND what the reader schema permits.

  Encode-side rejects only on inputs the target can't represent AT ALL (type mismatches, wrong primitive families, non-whole floats for exact-schema int/long targets); precision loss inside the family is silent for lossy schemas only. Users who need precise large-integer round-trip should use `"long"`, not `"double"`. See `appendAvroFloat32` / `appendAvroFloat64` / `jsonCoerceToFloat64` / `jsonNumberToFloat` / `defaultAsFloat` (encode side); `setLongValue` / `setFloatValue` (natural-decode side); `promoteIntFloatMantissa` (resolved-decode side, now lossy per the reader-schema rule). Pinned by `TestRegression_DefaultFloatIntegerLossyRound`, `TestRegression_Float32NarrowingEncodeIsLossy`, `TestRegression_Float32NarrowingDecodeParity`, `TestRegression_ResolvedLongToDoubleLossy`, and `TestPromotionBoundaryValues`.

- **Logical-type mismatch under schema resolution: reader's logical wins.** When writer and reader carry *different* (known) logical types — e.g. `int+date` writer + `long+timestamp-millis` reader — twmb applies the *reader's* logical-type conversion to the writer's wire value after physical promotion. Concretely: writer encodes `date(2024-01-01)` as wire `int(19723)` (days since epoch); reader decodes as `Instant.ofEpochMilli(19723)` ≈ `1970-01-01T00:00:19.723Z`. The wire bytes are *reinterpreted*, not *converted*. Three-way comparison:
   - **Java** (`Resolver.java:162` stores `r.getLogicalType()`; `GenericDatumReader.java:165-175` applies reader's `Conversion` after physical promotion): same as twmb — reader's logical wins, 1970-01-01.
   - **fastavro** (`_read_py.py:660-664`): applies the *writer's* logical type, ignores reader's — returns `datetime.date(2024, 1, 1)`. Semantic preservation.
   - **twmb**: matches Java.

   The spec is silent — the Schema Resolution section (`doc/.../Specification/_index.md:681-725`) enumerates only physical promotion (int→long→float→double) and never mentions logical types. The Logical Types section (783-787) says only that *unknown* logical types must be ignored. The case of known-but-different logical types on writer vs reader is undefined.

   twmb's choice is intentional Java alignment — Java is the de-facto majority for JVM-ecosystem interop, and matching its wire-decode semantics avoids the "Java reader gets value X, twmb reader gets value Y" interop divergence. The downside is that users evolving date→timestamp-millis (or any logical-type rename pair within the same physical type) silently get semantic confusion at decode rather than an error or a writer-logical-respecting conversion. Workarounds: keep the writer and reader logical types matched, or use the same physical type without logicals if you only need raw promotion.

   Boundary cases probed and pinned (sandbox `TestProbe_LogicalType*` in `/tmp/avro_audit_verify/probe_logical_mismatch_test.go`): `int+date` ↔ `int+date` (identity — works), `int+date` → raw `int` (raw promoted output, no logical applied — Java parity), raw `int` → `int+date` (reader's logical applied to wire value — Java parity), and `int+date` → `long+timestamp-millis` (the agent-flagged case — Java parity, semantic confusion preserved). Future audits should NOT re-flag this as a bug: it's documented spec-silent behavior with Java parity. A genuine policy challenge would need cross-impl evidence Java has changed direction, or a real-world interop break a Java user reports.

- **Field-level `logicalType` annotation on unions is lifted onto the first non-null branch.** Schemas like `{"name":"ts","type":["null","long"],"logicalType":"timestamp-millis"}` (documented as a common user error in AVRO-2015 / AVRO-3014, widely emitted by hand-written .avsc files, older Java tooling, and tutorial code) parse with `timestamp-millis` applied to the `long` branch, so `s.AppendEncode(time.Time{…})` and `s.Decode(wire, &time.Time{})` work directly. Apache Avro's official parser detects and warns but does not lift (`Schema.java:1871-1877`), leaving the union bare; fastavro / hamba / goavro preserve it as a field property only without applying to any branch. Confluent's production kafka-connect-avro-converter does NOT emit this shape (it produces canonical nested form). Twmb-unique lift among Go impls; wire format is identical (raw long varint), only Go-type interpretation differs. Edge case `["null","string","long"]` + `logicalType:"timestamp-millis"` (mismatched first non-null): the lift targets `string`, validateLogical detects the mismatch, and the soft-drop path (`o.Logical = ""`) silently clears — observable end state matches reference impls (no logical effective on any branch). Metadata API does NOT reflect the lift: `Root().Fields[0].Type.LogicalType` is empty on the field-level form, and the annotation surfaces in `Field.Props["logicalType"]` instead — matching Java/fastavro/hamba/goavro metadata behavior (none of which lift either). The wire-layer lift is the entire extent of support; do not propose extending it to the metadata API or to other paths. See `liftFieldLogicalIntoType` in schema.go.

  **The lift is a per-problem-shape concession, NOT a "make every buggy schema work" policy.** The shapes the lift handles today (string-form `["null","T"]+field-level-logical`, primitive-form `"T"+field-level-logical`, single-object-form `{"type":"T"}+field-level-logical` with no own annotation) were each added in response to a specific deployed-buggy-schema or interop-driven need. Other spec-equivalent shapes of the same conceptual mistake — such as object-form null in union (`[{"type":"null"},"T"]+field-level-logical`), `[{"type":"null"},{"type":"null"},"T"]`, lift onto a non-first non-null branch, etc. — are INTENTIONALLY NOT supported. They produce different parsed metadata or fail at encode time, and that is the documented end state. **Auditors: do not file "spec-equivalent variants of supported shapes are unsupported" as findings.** Each new lifted shape ships only with a concrete user-reported breakage that the existing shapes don't cover; symmetry-completeness is not a goal here. The audit cost of "this Avro-equivalent schema produces a different parsed result" is acceptable because field-level logicalType in any form is a deployed-buggy-schema concession, and we don't owe its variants the same parity guarantee that well-formed schemas get. If a real-world producer emits a currently-unsupported lift shape, that is the bar for adding it — surface it as a Suspected with concrete evidence and ask, do not unilaterally extend.

- **Decimal logical type accepts `float64` / `float32` input.** `s.AppendEncode(float64(3.14), {"type":"bytes","logicalType":"decimal",…})` succeeds via `tryCoerceToRat`'s float arm using `strconv.FormatFloat(f, 'f', -1, v.Type().Bits())` → `big.Rat.SetString`. Matches Java's `BigDecimal.valueOf(double)` (which uses `Double.toString(d)` shortest-decimal). fastavro requires `decimal.Decimal`, hamba requires `*big.Rat`, goavro requires textual string — twmb is the only Go impl accepting native float for decimal input. The source-bitsize formatting (`v.Type().Bits()`) is load-bearing: `float32(0.33)` formats as `"0.33"` (not the float64-noise `"0.33000001311302185"` a float64 widening would produce), avoiding non-terminating-rational rejection at schema scale 2. Float64's bounded exponent (~±308) keeps the magnitude well under `decimalScaleLimit` (65536), and `FormatFloat`'s 'f' output is JSON-valid by construction (≤310 chars, no hex/underscore forms) — so the float arm's bypass of `boundedRatFromString`'s `isJSONNumber` / magnitude gates is safe by construction, not a parity hazard. See `tryCoerceToRat` (ser.go).

- **`json.Number` content is validated against the schema's contract.** The unifying principle: the Avro schema is the contract for what wire content represents, and json.Number's underlying string must be a valid RFC 8259 number literal. The two combine as follows:
  - **Numeric schemas (int, long, float, double — and their logical-type variants date, time-millis, time-micros, timestamp-millis/micros/nanos, local-timestamp-*)**: decode writes the raw numeric wire value into json.Number as a JSON-number-valid string, bypassing logical-type formatting (so date wire `19723` → `json.Number("19723")`, not the date string `"2024-01-01"`). Encode parses json.Number's content as a number and validates it fits the schema. Non-finite floats (±Inf, NaN) reject on both sides — no valid JSON-number representation. The bypass is centralized at `formatToStringKindTarget` (deser.go) which excludes `json.Number` and routes through `setIntegerWire`'s json.Number arm.
  - **Stringy schemas (string, bytes, fixed, enum)**: `json.Number` is REJECTED on BOTH encode and decode, regardless of content (no `isJSONNumber` exception — `"1.5"` rejects just like `"foo"`). It is a numeric carrier (RFC 8259), so a text/binary target is a type mismatch — use a Go `string`/`[]byte` there. `rejectJSONNumberStringTarget` (deser.go) rejects unconditionally on decode; `appendAvroString` / `rejectJSONNumberRawTarget` reject on encode. (This is the symmetric reject documented at the top of §"What json.Number…" — an earlier draft of this sub-bullet wrongly described content-validation + lenient verbatim encode; the code does neither.)
  - **Map keys**: per-key validation. `validateJSONNumberMapKey` is called on each wire key during decode and each source key during encode; all-numeric keys round-trip cleanly through `map[json.Number]V`, non-numeric content rejects with the specific key in the error message. Record-as-map decode always rejects on the first field name (Avro field names follow `[A-Za-z_][A-Za-z0-9_]*` and never satisfy the JSON-number grammar).
  - **Decimal logical type is exempt** — `setDecimalRat` writes `big.Rat.FloatString` which is RFC 8259-valid by construction.

  Go stdlib's `json.Marshal(json.Number(x))` fails with "invalid number literal" for any underlying string that isn't a valid RFC 8259 number, so this matches what the type's own marshaler enforces. Twmb is the only Go Avro impl exposing `json.Number` as a decode target (Java returns `Double`, fastavro returns Python `float`, hamba and goavro return `interface{}` wrapping `float64`); none of those have the stdlib contract constraint. See `validateJSONNumberMapKey`, `rejectJSONNumberStringTarget`, `formatToStringKindTarget`, and `setFloatValue` in deser.go.

- **Bare (untagged) union values accepted on JSON decode.** `s.DecodeJSON([]byte("42"), &out)` against union `["null","int"]` decodes as the int branch; the spec form is the tagged map `{"int": 42}`. Java's `JsonDecoder.readIndex` and fastavro / goavro all require the tagged-map form for non-null union values (rejecting bare numerics, booleans, and strings); hamba has no JSON union decode. Twmb-unique leniency at `decodeUnionBare` for round-trip compatibility with goavro-style output and hand-edited JSON. Dispatch matches by JSON token type (string `"` → string/bytes/fixed/float/double; `[` → array; `{` → record/map; digit/`-` → int/long/float/double + decimal-bare-number bytes/fixed); the `if branch.kind == "null" continue` skip is correct *because* `decodeUnion`'s upstream `isJSONNullStart` filter pre-handles actual null tokens before this loop runs. See `decodeUnionBare` + `jsonTokenMatchesBranch` in json_decode.go.

- **JSON *string* decode is strict — matches encoding/json / Java (Jackson) / fastavro / RFC 8259, deliberately STRICTER than goavro.** Unescaped control characters (U+0000–U+001F), invalid UTF-8 byte sequences, and unrecognized escape sequences (`\x`, `\q` — only the eight JSON escapes + `\uXXXX` are valid) all reject in `consumeStringRaw` / `walkJSONEscapes`. `decodeJSONFloat` rejects non-JSON number grammar (trailing-dot `5.` / `5.e3`) via the shared `isJSONNumber` gate. `DecodeJSON` rejects any trailing non-whitespace after the decoded value: it decodes exactly one value and returns no offset, so — unlike Java's streaming JsonDecoder or goavro's `NativeFromTextual`, which return the remainder — it cannot stream concatenated values. The encoder escapes every control/high byte, so the bytes/fixed/decimal codepoint-mapped form round-trips in its *escaped* `\u00XX` representation; only raw/invalid hand-written forms reject (the consumer flows that matter — rpk produce, console Avro viewer — use the escaped form). goavro's scanner accepts raw control chars, drops the backslash on a bad escape (`\x41`→`x41`), and returns trailing bytes; twmb rejects all three. This strictness is the maintainer's deliberate post-ecosystem-sweep choice — do NOT re-flag the rejections as bugs or propose re-loosening toward goavro. Retained JSON-decode leniencies (still intentional): bare untagged unions (above), special-float tokens (NaN/Infinity), whole-number-float-as-int. Pinned by `TestDecodeJSON{InvalidEscape,RawControlChar,InvalidUTF8,TrailingContent,FloatGrammar}Rejected` and `TestSpecJSONTrailingContentRejected`.

- **`SchemaFor` rejects logical-type tags on incompatible Go types.** SchemaFor enforces strict Go-type compatibility for every logical-type tag at build time: `,uuid` requires Go string / `[16]byte` / a text-marshaler type; `,decimal(p,s)` requires `*big.Rat` / `big.Rat`; `,date` and `,time-millis` (int wire) require `time.Time` / `time.Duration` / a Go integer naturally mapping to Avro int (int8/16/32, uint8/16); `,time-micros` / `,timestamp-*` / `,local-timestamp-*` (long wire) require `time.Time` / `time.Duration` / a Go integer naturally mapping to long (int, int64, uint32/64, uint); `time.Time` and `time.Duration` reject non-time/date logicals (uuid, decimal). The `,inline` directive (and anonymous embeds with empty parts[0]) reject any other tag option — the flattened embed has no field at this position for `default=`, `alias=`, logical-type tags, etc. to apply to. Prior behavior across all of these cases was silent drop, producing either a schema that didn't reflect the user's intent (logical-type tag silently lost) or a schema that lied about the Go field's type (e.g., `,uuid` on int32 → `{string, uuid}` schema; Encode then failed at runtime far from SchemaFor). Java/fastavro have no `SchemaFor`-equivalent so there's no cross-impl precedent — the rule is internal-consistency-only. The specific accept/reject boundaries are pinned by `TestSchemaForUUIDRejectsUnsupportedKind`, `TestSchemaForDecimalRejectsNonBigRat`, `TestSchemaForLogicalOnNumericKind`, `TestSchemaForTimeTypesRejectNonTimeLogicals`, and `TestSchemaForInlineRejectsOtherOptions` in `schema_for_test.go` — each test's docstring documents the rule and the cases it covers; future audits should consult those for the exact accept/reject matrix.

- **Text interfaces (`TextMarshaler` / `AppendText` / `TextUnmarshaler`) take precedence over the `reflect.String` fast path and the enum int-ordinal arm — uniformly across binary and JSON, scalar and container.** Resolution order at every text-shaped site (`appendAvroString` / `avroStringValue` / `serEnum` / JSON enum encode; `setStringValue` / `setEnumTarget` / `decodeString` and their JSON twins; `serFixedUUIDReflect` / `deserFixedUUIDReflect` and the JSON fixed+uuid arms): (1) for uuid logical types a `[16]byte`-shaped Go type **trusts its raw bytes** — the text interface is NOT consulted, because the 16 bytes ARE the UUID and a `MarshalText`→`parseUUID` round trip would be redundant AND would let a non-canonical text method diverge binary from JSON; (2) `TextMarshaler`/`TextUnmarshaler`; (3) `reflect.String`; (4) raw bytes / enum int-ordinal. `json.Number` is the one string-kind type excluded from text dispatch (it implements no text method and carries its own RFC 8259 reject/guard). Rationale: prefer the representation native to the wire shape (text-shaped wires honor text methods; byte-shaped uuid wires trust the bytes) and honor an explicitly-implemented text method over the raw Go string — matching `encoding/json` (which prefers `TextMarshaler` over the default string encoding) and Java (`GenericDatumWriter.writeEnum` → `getEnumOrdinal(datum.toString())` name-based enum matching; `Conversions.UUIDConversion.toFixed` extracts raw msb/lsb bytes, never `toString()`). The enum consequence is **name-based matching**: an int-kind enum carrier with a symbol-returning `MarshalText` matches by symbol name (robust to the Go int values not lining up with the Avro symbol order) rather than trusting the Go int as the ordinal; a plain int with no text method still uses the ordinal arm. Two intentional sharp edges: (a) a string-kind type with a *transforming* `MarshalText` (e.g. uppercasing) now encodes its marshaled form on the Avro wire — a behavior change from the prior raw-string encoding, accepted for `encoding/json` parity; (b) a type with `MarshalText` but no `UnmarshalText` encodes via the text method but decodes raw (one-way, the Go-stdlib idiom — see the `textUnmarshalerOnly` pin). Fast-path eligibility is concentrated in two predicates so a struct field / container element behaves identically to the scalar and so a future slow-path-only string concern is added in one place (not re-swept across every gate — the pattern-14c trap): `stringFastPathEligibleEncode` / `stringFastPathEligibleDecode` (reflect.go), consulted by the unsafe struct gates (`usString` / `udStringDeser` / `usFixedUUIDString` / `udFixedUUIDString`) and the array/map fast loops (`fastPathSafeForElem`). `[16]byte` uuid types stay on the raw fast paths (`usUUID` / `udUUID` / `udFixedUUID`) precisely because they trust bytes. Pinned by `TestRegression_FixedUUIDByteArrayTrustsRawBytes`, `TestRegression_StringKindPrefersTextMarshaler`, `TestRegression_EnumTextMarshalerNameMatchOverOrdinal`, and `TestRegression_StringKindTextMarshalerConsistentAcrossContexts` in `text_interface_precedence_test.go`.

- **A nil `*Schema` is a programming error and methods PANIC on it — not guarded; do NOT propose returning errors for nil receivers.** Every exported `*Schema` method dereferences the receiver and panics (nil-pointer deref) when called on a nil `*Schema` with otherwise-valid arguments: `AppendEncode`, `Encode`, `EncodeJSON`, `AppendEncodeJSON`, `Decode`, `DecodeJSON`, `AppendSingleObject`, `DecodeSingleObject`, `Canonical`, `Fingerprint`, `String`, `Root`. `Resolve` and `CheckCompatibility` panic when EITHER `*Schema` argument is nil (each dereferences `writer.node` / `reader.node` before any guard). `*SchemaNode.Schema` and `*SchemaCache.Parse` panic on a nil receiver too. This is idiomatic Go: a `*Schema` is only ever obtained from `Parse` / `MustParse` / `Resolve` / `SchemaFor` / `SchemaNode.Schema`, all of which return a non-nil pointer or an error, so a nil `*Schema` is a caller bug, surfaced loudly. The behavior LOOKS inconsistent only because three methods validate an ARGUMENT before reaching the receiver deref — `Decode` and `DecodeJSON` reject a nil decode TARGET ("decode requires a non-nil pointer" / "DecodeJSON requires a non-nil pointer") and `DecodeSingleObject` rejects a malformed/short HEADER — so a nil-receiver call with ALSO-bad arguments surfaces the arg error first. Those arg guards are correct and must stay (they are about the argument, not the receiver); the SAME three methods panic when handed a VALID argument. Do NOT add error-returning nil-RECEIVER guards to make the surface "consistent" — it already is consistent (always panic on the nil receiver); a returning guard would be un-idiomatic and would mask the caller's bug. The contract is stated on the `Schema` type doc (schema.go). Pinned by `TestRegression_NilSchemaPanicsConsistently` (recover() per method; panic asserted for every receiver-deref path, arg-validation error asserted for the three nil-ARG guards).

- **Quoted `"size"` is accepted at parse (more lenient than Java); quoted `"precision"` / `"scale"` are REJECTED (mirrors Java).** A `"size":"16"` (quoted integer) parses, via `laxInt`, per the Avro spec's [INTEGERS] Parsing-Canonical-Form rule — "Eliminate quotes around ... JSON integer literals (which appear in the _size_ attributes of _fixed_ schemas)" (`doc/.../Specification/_index.md`, 1.11.1 line 716). Apache Avro (Java) is STRICTER here and REJECTS a quoted size: `Schema.parseFixed` requires `sizeNode.isInt()` (`lang/java/avro/src/main/java/org/apache/avro/Schema.java:1958-1960`), and a Jackson `TextNode` (quoted string) returns `isInt() == false`. twmb keeps the quoted-size leniency deliberately (it's a spec-PCF-blessed shape, emitted by some tooling — the very reason [INTEGERS] normalizes it) — this is the one axis where twmb is more lenient than Java. For decimal `"precision"` / `"scale"`, twmb and Java AGREE: both REJECT the quoted form. Java reads them via `LogicalTypes.Decimal.getInt`, which requires `obj instanceof Integer` and throws otherwise (`lang/java/avro/src/main/java/org/apache/avro/LogicalTypes.java:414-420`); a quoted `"10"` deserializes to a Java `String` through `JacksonUtils.toObject`'s `isTextual()` arm (`lang/java/avro/src/main/java/org/apache/avro/util/internal/JacksonUtils.java:156-158`), never an `Integer`. twmb rejects them because precision/scale are plain `*int` (no `laxInt`). The metadata-read helper `jsonNumericInt` (schema_node.go) has a string arm for all three, but it is UNREACHABLE for precision/scale from a parsed schema — parse rejects quoted precision/scale before `Root()` can read them; for size, parse and `Root()` agree (both accept quoted). Do NOT "fix" the size axis by rejecting quoted size to match Java exactly: that would delete a deliberate spec-[INTEGERS] feature (and break `TestParseFixedStringSizeINTEGERS`). Pinned by `TestRegression_QuotedSizePrecisionScaleMirrorsJava` (accept/reject for each of size/precision/scale, quoted + numeric, at parse and `Root()`).

- **Named-fixed-backed logical tagged-union branch name is the fixed's (full)NAME, not `fixed.<logicalType>`.** Under `TagLogicalTypes` (a goavro-interop JSON option), a union branch that is a NAMED fixed carrying a logical type (e.g. `{"type":"fixed","name":"F","size":16,"logicalType":"uuid"}`) is tagged under the fixed's fullname (`F`, or `n.F` when namespaced), NOT `fixed.uuid`. The `<kind>.<logicalType>` qualifier (`long.timestamp-millis`) is retained ONLY for primitive-backed logicals — a branch whose standard name IS its kind. This matches BOTH reference impls that emit tagged-union JSON envelopes: linkedin/goavro keys the envelope by the branch codec's `typeName.fullName` (`union.go:71,75` — `allowedTypes[i] = unionMemberCodec.typeName.fullName`); a named fixed's codec keeps the fixed's name because `makeDecimalFixedCodec` (`logical_type.go:366-399`) calls `makeFixedCodec` (which sets `typeName` from the schema `name`) and only swaps the conversion functions, and goavro has NO uuid/duration logical type at all (`codec.go:746-769` recognizes only `bytes.decimal` / `fixed.decimal` / `string.validated-string`), so a `uuid`/`duration` fixed hits the `default` arm that strips `logicalType` to a plain named fixed — `F` in every case; and goavro's primitive-backed logicals DO use `kind.logical` (`codec.go:358-402`, `typeName = "long.timestamp-millis"` etc.), which is the source of twmb's `long.timestamp-millis` form. Apache Avro's `JsonEncoder` likewise labels a union alternative with the branch's `getFullName()` (`ValidatingGrammarGenerator.java:104` — `labels[i] = b.getFullName()`; Java has no `kind.logical` at all). twmb previously emitted `fixed.uuid` / `fixed.decimal` / `fixed.duration`, dropping the name — a divergence from both references; it now emits the name. The single source of truth is `unionBranchNames` (json_codec.go), consumed by the JSON encoder, the binary-decode TaggedUnions wrap (via `deser.logicalNames`, schema.go's `buildUnion` / `finalizeUnionNames`), and the JSON-decode tagged-map wrap (`wrapUnion`, json_decode.go) — so binary↔JSON stays uniform. The decoder still ACCEPTS the legacy `fixed.<logicalType>` form (`findUnionBranch`'s "fixed" logical-tag fallback, json_codec.go) for backward compatibility. Pinned by `TestRegression_NamedFixedLogicalTaggedUnionName` (exact emitted key for named-fixed uuid/decimal/duration vs unnamed long.timestamp-millis, binary↔JSON wrap uniformity, JSON round-trip, and legacy-form decode-accept) and the forward-reference path by `TestRegression_UnionForwardRefTagLogicalNamesResolved`.

Java-divergence findings require checking this list first. When the claim is about an item here, the path is either (a) a rationale-change proposal with strong evidence the current direction is wrong, or (b) dropping the finding.

## Structural blind spots — categories prior audits have under-covered

These are recurring shapes that past audits missed not because the pattern wasn't named, but because the audit's *angle of attack* didn't reach them. Each entry names a category, names the root cause, and prescribes the kind of probe that surfaces bugs there. Re-read this list before starting each round — the question for each is "which sites in this category have I actually probed *as this category*, not as a side-effect of another sweep?"

- **Public-API surfaces with no encode/decode parity counterpart.** The yield-map's high-yield bullets are dominated by encode/decode parity (patterns 2, 12, 14). Standalone non-data-path methods — `Schema.Root`, `Schema.Canonical`, `Schema.Fingerprint`, `Schema.String`, `CustomType` callback receivers — don't surface from those sweeps. They have independent correctness contracts (precision, type, doc-string promise) that need independent probes. Audit-80's precision-loss bug landed here; the bug was syntactically named in pattern 1 but structurally invisible to past audits because the audit's primary sweep was encode/decode-parity.

- **Type pins on user-facing return values.** Pattern 13b. Existing tests assert `v.(float64)` / `v == float64(N)` and pass because N is small enough that the type is "correct"; the type is wrong for the precision-edge case but no test exercises that case. Distinct from pattern 13a (rejection pins) because the test PASSES — it doesn't look like a bug-locking test.

- **Comment / doc-string contracts that aren't tested.** Hard rule 5 extended to public-API doc strings. A doc-string saying "preserves all metadata" is a testable claim. Audit-80's bug survived in part because no round read `Schema.Root`'s doc string as a claim that needed verification at the precision boundary.

- **Same-input-shape sites that bypass a safety helper.** Pattern 14b. When a safety helper exists (`unmarshalDefault` uses `UseNumber`, `boundedRatFromString` caps exponents, `timeMicrosToDuration` overflow-checks), every site in the codebase that takes the same input shape needs either to call the helper or to have a structural reason it can't trigger the helper's concern. "The helper is over there, this site is different" is not a structural reason — it's an unsupported assumption.

- **Dispatcher skips that bypass a per-branch handler.** Pattern 15. A `continue` / `break` / skip-case inside a dispatch loop encodes an *unwritten* invariant ("the input we'd route to this branch can't reach here"). When the per-branch handler's acceptance set later broadens — usually via a helper fix on a sibling path — the dispatcher's skip silently leaks the new shapes. Concrete instance: the binary `serNull` peel broadened in a prior fix; the matching JSON union dispatcher's `if branch.kind == "null" continue` was not updated; bare nil Map/Slice/[]byte against multi-branch unions started silently mis-dispatching. The angle of attack: every undocumented `continue` inside a try-each / for-branch loop is a candidate; pair it with the per-branch handler and ask whether the handler's acceptance set is still a subset of the dispatcher's exclusion set.

- **Lenient walker skips a nil child that its strict sibling dereferences.** A family of walkers over the same node tree (e.g. `walkDefault` driving `validateDefault`/`convertDefaultBytes`, vs `encodeDefault`) must agree on nil-child handling. When the lenient walkers guard `node == nil` / `child != nil` and SKIP, while the strict walker dereferences `node.kind` unguarded, an input that reaches a transiently-nil child (a not-yet-wired forward-ref descendant during schema build/finalize) passes the lenient gates VACUOUSLY (they validated nothing) and then nil-panics in the strict walker — `Parse` crashes on valid input. Structurally invisible to: the lenient-walker tests (they pass — vacuously), and wire round-trip tests (the panic is at parse, before any encode). Angle: for any tree-walk family, list which walkers guard nil children and which deref unguarded; the asymmetry IS the bug. The fix is usually ordering (resolve/wire all children before the strict walk runs), not a nil-guard in the strict walker (which would silently skip the child and emit wrong output). Concrete instance: forward-ref array-items/map-values/nested-record defaults panicked in `encodeDefault` while `validateDefault` skipped the nil child; fixed by deferring default resolution+encode to finalize after every node is wired (two phases: resolve all default VALUES, then encode all default BYTES, since `encodeDefault` fills absent nested fields from siblings' resolved defaults). A later instance widened this into a **dual-representation** rule: forward-ref UNION branches left `node.branches[i]` nil even *after* finalize, because the union fixup patched only the wire ser/deser dispatch tables (`m.ser.fns[idx]`/`m.deser.fns[idx]`) and never the structure node. So the binary path (dispatches through the fn tables) worked, while every path that walks `node.branches` directly dereferenced nil — JSON encode/decode and `resolveWriterUnion`/`CheckCompatibility` nil-panicked at runtime, and the union-default validator (`firstUnionBranchAcceptingDefault`, which walks `node.branches`) matched a nil branch → reported "no branch matched" → rejected a schema that is byte-equivalent to a backward-ordered one that parses. The angle: a forward-ref fixup must update EVERY representation a path may consume — the wire dispatch (fn tables) AND the structure node-tree (`node.branches`/`node.fields`/`node.items`). Record-field and container fixups already patched the node (`m.nd.fields[idx].node = nt.node`, `*m.nodeChild = nt.node`); unions patched only the fn table. Fix: also `branches[idx] = nt.node` in finalize's missing-branch loop. Invisible to: binary round-trip tests (fn-table path) and the existing union forward-ref test (binary + typed-struct only — never JSON, Resolve, or a union default). A still-later instance added the THIRD representation: **name-derived artifacts captured at parse time** — anything buildUnion (or any build site) computes FROM a branch's name (the duplicate-detection key, the TaggedUnions `branchNames`/`logicalNames` tables, the tagged-map `ser.branchNames` acceptance map) holds the UNRESOLVED as-written name for a fwd-ref branch even after the fn tables and node tree are patched. Consequences: order-dependent duplicate detection (a short-name fwd ref + inline definition of the same type escaped the spec's unions rule), order-dependent + binary↔JSON-divergent TaggedUnions envelopes (`{"Inner":…}` binary vs `{"n.Inner":…}` JSON on the SAME schema), and order-dependent tagged-map encode acceptance (the full name rejected on fwd schemas). Fix: `finalizeUnionNames` re-derives the dup check + all name tables from the RESOLVED nodes for every fwd-bearing union (the parse-time dup check skips fwd-ref branches entirely — their as-written spelling is not a binding). The general rule: enumerate every artifact a build site derives from a name and either compute it from the resolved node or re-derive it in finalize; the fn-table/node-tree fixup checklist is not complete without the name-derived artifacts.

- **A non-wire path re-derives a resolution the parser already computed — and must reproduce it EXACTLY, including position-dependence.** The wire codec's name bindings, branch selections, and default-branch choices are computed once by the parser (eager, in-scope-first, POSITIONAL). Any OTHER path that needs the same answer — the SchemaCache self-containment splice, a canonical-form emitter, a metadata-API walker — either consumes the parser's binding or re-derives it; re-derivation is the trap, correct only if it reproduces the parser's rule in FULL. Audit history shows the rule has more dimensions than the re-deriver remembers. Concrete instance: the cache splice (`inlineTreeDefs`) re-derives how a bare name reference binds. A first fix matched the parser's *precedence* (`scopedRefKeys`: enclosing-namespace-qualified before null-namespace, local before cache) but STILL diverged, because the parser's binding is also POSITIONAL — a reference binds to a local definition only if that definition appears BEFORE the reference in DFS pre-order (eager binding; a later definition does not retroactively rebind). A position-INDEPENDENT "is this name defined locally anywhere in the tree" check kept a before-the-definition reference bare when the parser had bound it to the *cached* type, so `String()`/`Canonical()`/`Fingerprint()`/`Root()` described a different schema than the wire codec — silent decode corruption for any consumer that re-parses the schema text. Fix: track local names positionally as the walk proceeds, mirroring `registerNamed`'s timing. The angle: for any path that re-derives a binding/selection the parser computed, enumerate EVERY dimension of the parser's rule — precedence, namespace scoping, AND position/order — and reproduce all of them, or (better) consume the parser's binding directly. A re-derivation that matches on a define-before-reference battery silently breaks on reference-before-define, so the matrix must cross BOTH orders. This is the higher-order form of the "name-derived artifacts must be computed from resolved nodes" lesson above: there the artifact lived in the wrong representation; here it is re-computed by an algorithm that drifted from the parser's.

- **Cross-parse boundary guards are per-option, and a self-containment fallback must stay coherent.** A `SchemaCache` spans multiple Parses that may pass different SchemaOpts. Every guard protecting a cross-parse boundary for ONE option (the CustomType custom-presence agreement guard, `rejectCachedRefIfCustomTypeWouldMatch`) is a candidate for every OTHER acceptance-changing option. `WithLaxNames` is the sibling: a type defined with `WithLaxNames` and referenced by a strict parse yields a schema whose metadata forms carry a lax name no strict parser accepts — sticky, exactly like a standalone lax schema, now documented on `SchemaCache`. The related trap: a best-effort self-containment step (re-parse the spliced form to rebuild metadata) that SILENTLY falls back on failure can produce a WORSE artifact (a dangling reference no parser resolves) than the input it was improving. A fallback must degrade to a still-coherent form (here: retry the re-parse permissively so the spliced body is present and re-parseable with `WithLaxNames`), never to a silently-broken one. Probe: for every cross-parse opt, parse a type with it then reference that type from a parse WITHOUT it, and assert `Parse(s.String(), <the opt>)` and `Parse(string(s.Canonical()), <the opt>)` both succeed and that `s.Encode`/`s.Decode` are unaffected.

- **Error shape and identity are a parity surface across wire formats — invisible to wire-byte and value sweeps.** `doc.go` promises type mismatches are `errors.As`-able to `*SemanticError` with a dotted record-field path. The binary encoder/decoder build that via `recordFieldError` at every record level; the JSON paths did not — JSON encode returned bare `fmt.Errorf` leaves (not `errors.As`-able, no field), and JSON decode wrapped the field name into message TEXT only (`SemanticError.Field` stayed empty). So a caller's `errors.As(err, &se); se.Field` worked for `Encode`/`Decode` and silently broke for `EncodeJSON`/`DecodeJSON` on the same value and schema. The class is structurally invisible to: wire-byte parity (errors carry no wire bytes), value-parity round-trips (they assert success, not failure shape), and the differential oracles (they compare accepted values, not rejection identity). The angle: error construction is user-visible behavior with its OWN four-path matrix (binary/JSON × encode/decode) and a binary↔JSON contract; when a fix touches one path's error, the other three are siblings. Net: `TestMatrix_JSONEncodeErrorSemanticParity` (every fragment × both wire formats agree on `*SemanticError` + field path). Added to the yield map under error construction.

- **Best-effort first pass that destructively transforms under incomplete context.** A two-pass coercion where pass 1 runs with partial information (e.g. a nil name-table so name-refs can't resolve) must NOT irreversibly mutate a value pass 2 would re-route. If pass 1 greedily converts (string→[]byte for a bytes branch) and pass 2's correct branch (a name-ref enum, only resolvable with the table) accepts only the original type, the value is locked onto the wrong branch forever — pass 2 can never reclaim it. Fix: pass 1 leaves ambiguous values (union branch selection) untouched, deferring all destructive transforms to the full-context pass. Sibling of the dual-namespace two-pass note; concrete instance: `coerceMetadataDefault` first pass (table==nil) converted a valid enum-symbol string default to []byte on a later bytes branch, so `Root().Fields[].Default` reported []byte while the wire picked the name-ref enum branch.

- **Boundary values that escape "happy path" coverage.** Pattern 1's boundary-value list. Most existing tests cover the typical case (small ints, ASCII strings, well-formed JSON); the edge cases (2^53+1, MaxInt64, MinInt64, NaN, ±Inf, empty, max-size) are tested incidentally if at all. New audits should ensure every numeric-touching public API has explicit coverage at each boundary.

- **Float64 fallback as a precision recovery path.** Sites that try a precise parser first (`json.Number.Int64()`, `strconv.ParseInt`) and fall back to `Float64()` + `floatFitsInt64` for "exponent / fractional / overflow" inputs assume the float64 fallback is precise enough to bound the result. It is NOT, at the int64 boundary: float64's 2048-unit precision at magnitude 2^63 means values in `(int64.Min − 1024, int64.Min]` and `[int64.Max + 1, int64.Max + 1024)` all round to the same float64 as int64.{Min,Max} themselves, so the boundary check `n < -(1<<63) || n >= 1<<63` cannot distinguish "exactly int64.Min" from "int64.Min − 1" and silently accepts the overflow. Symmetric on the positive side: valid int64s in exponent form whose mantissa needs more than 53 bits round to int64.Max + 1 and are FALSELY rejected. Fix shape: use an arbitrary-precision parser (`big.Rat.SetString` via `boundedRatFromString`) gated by a length cap, not the float64 fallback. Every site that takes a *string* numeric input destined for int64 needs the precise parser; sites that take a Go float (`CanFloat()` arm) are inherently float-precision and unfixable at that layer. Pattern 1 named "json.Unmarshal rounds long > 2^53 to float64" as a candidate; this is the same class via a different stdlib path (`json.Number.Float64()`).

- **The new helper introduced by a fix.** Pattern 16. Every fix that swaps a bounded check for an arbitrary-precision helper changes the perf posture at the entry point. Before declaring the fix done, write a hostile 1 MiB input against every affected entry point, time the rejection (`time.Since`), and assert < 100ms. If the bound isn't there, add it at the new caller's entry — the helper's own DoS posture is calibrated for its original domain, which is usually *looser* than the new caller's domain needs.

- **Schema-parse-time validation is a third code-path axis.** The audit's primary lenses are encode-time and decode-time. A schema with a field default carries the SAME stdlib-parser-callsite pattern as encode/decode but at `avro.Parse` time, routed through `defaultAsFloat64` / `defaultAsInt32` / `defaultAsInt64` / `validateDefault` / `coerceDefault` / `convertDefaultBytes` / `encodeDefault`. These functions are structurally invisible to round-trip wire tests (the parse-time arm rejects before the wire path is even exercised), invisible to encode/decode-parity sweeps, and invisible to the "decoder counterpart already has the right arm" smoking-gun test. The angle of attack: for every fix at a stdlib-parser callsite, ask "is there a `defaultAs<Type>` or `validate<Type>` or `coerce<Type>` function that calls the same parser?" If yes, probe `avro.Parse({type:record,...,default:<the-input-the-fix-newly-accepts>})` — if the parse rejects what the encode now accepts, the fix is partial. The pattern 1b "three-axis rule" corollary names this for stdlib-parser fixes specifically; the structural lesson generalizes — any time a fix changes input acceptance, all three axes need verification. Concrete instance: a prior fix (encode-side ParseFloat ErrRange-with-Inf) named only encode-time sites; the next round caught three schema-parse-time `defaultAsFloat64` / `coerceDefault` sites that still rejected the same input the encoder newly accepted.

- **Metadata-API observability is a FOURTH code-path axis.** Beyond encode-time, decode-time, and schema-parse-time-validate, there is a fourth user-visible surface that consumes the same stdlib parser outputs through its OWN call chain: the schema metadata API (`Schema.Root().Props`, `Schema.Root().Fields[].Default`, `Schema.Root().Fields[].Props`, `CustomType` callback `*SchemaNode.Props`). All four surfaces route through `normalizeJSONNumber` (`schema.go`) — a SEPARATE `json.Number.Float64()` callsite from the encode/decode/parse arms. The metadata axis is structurally invisible to (a) wire-format round-trip tests (no encode happens), (b) encode/decode-parity sweeps (no encode arm exposed), (c) the schema-parse-validate probe `avro.Parse({...default:X})` (parse accepts but Root() is checked SEPARATELY). The angle of attack: for every fix at a stdlib-parser callsite that has a corresponding metadata-API surface (any JSON numeric literal in a schema reachable via `Root()`), the audit owes a `Root().Default.(float64) == expected` assertion, NOT just a wire-byte assertion. Concrete instance: a prior fix added ErrRange-with-Inf acceptance to `parseFloatAcceptOverflow` and propagated it to four arms (binary encode `jsonNumberToFloat`, JSON encode `jsonCoerceToFloat64`, schema-parse `defaultAsFloat` json.Number + string arms). `normalizeJSONNumber` — the 5th caller of the same `strconv.ParseFloat`-equivalent — was never identified as a sibling because it lives in the metadata path (`schema.go:normalizeJSONValue` → `normalizeJSONNumber`), not in the encode/decode/parse-validate chain. Pre-fix `s.Root().Fields[0].Default` returned `json.Number("1e1000")` while the wire bytes (visible only via `s.AppendEncode` + `s.Decode`) were already +Inf — a single schema with two observable surfaces and two values. The fix routes `normalizeJSONNumber` through the same `parseFloatAcceptOverflow` helper; the four-axis rule generalizes: every stdlib-parser callsite fix must also probe `Root()`-exposed metadata for the newly-accepted input. Cross-impl evidence: Java's Jackson `DoubleNode(Double.parseDouble("1e1000"))` returns +Inf at the metadata layer (`Schema.java:1899-1902`); fastavro's `float("1e1000")` via Python json returns inf — both Java and fastavro expose +Inf in their metadata APIs. The fix must also handle `toJSONWalk` re-serialization (`schema_node.go`), since `encoding/json.Marshal` rejects ±Inf unconditionally — a `jsonSerializableValue` walker converts back to `json.Number` literals so `SchemaNode.Schema()` round-trips, completing the four-axis loop.

- **The 3-axis rule applies to reject-direction predicate changes, not just accept-direction.** Prior framing emphasized "the encode-side fix accepts new shapes — does the schema-parse-time arm also accept?" A re-audit caught the inverse: a prior fix made the encode arms REJECT integer-form > 2^53 (tightened a previously-silent rounding behavior); the schema-parse-time arm was never updated. The "did the schema-parse arm get the matching change?" probe must run for both directions: (a) "newly accepted at encode — does parse accept?" AND (b) "newly rejected at encode — does parse also reject?" The audit angle: for every encode-arm predicate-change commit, identify the predicate's direction and probe the corresponding schema-parse-time arm. The same applies in the reverse — every schema-parse-time predicate-change commit owes a probe at the encode-time arm. Both directions are observable as wire-format inconsistencies.

- **Asymmetric arms within a single helper.** Patterns 14 / 14a / 14b address helpers vs sibling sites in DIFFERENT functions. That fix's self-introduced bug exposed the missing pattern: when a helper has multiple INPUT-TYPE arms (e.g. `defaultAsFloat`'s json.Number arm + float64 arm + string arm), each arm independently implements the predicate, and asymmetry between arms is just as easy to introduce as asymmetry between helpers. The audit angle: for every multi-arm helper that applies the same predicate at each arm, write a SAME-INPUT same-magnitude probe at every arm AND every adjacent helper that should agree (e.g. json.Number("X") vs string-form("X") vs typed-value(X) all routed to the same target type). If any two arms diverge on the same magnitude, the helper has internal drift. Fix shape: extract the predicate into a helper that the arms call uniformly, so the predicate exists in ONE place; the arms become input-conversion boilerplate that hands off to the shared check. Concrete: the fix extracted `integerFormFitsFloat(s, bitSize)` so the json.Number arm and string arm of `defaultAsFloat`, plus the json.Number arm of `jsonCoerceToFloat64`, all share the same precision predicate — drift impossible by construction.

- **Dual type-name namespaces (normalized internal vs preserved-as-written public-API).** Some logical concepts carry two name forms across the codebase: the internal `node.kind` field is normalized by the schema builder (e.g., Avro's `"error"` → `"record"` at schema.go's `case "record", "error":` arm), while the public `SchemaNode.Type` field preserves the JSON-as-written name. Wire-pipeline helpers dispatch on the internal normalized field and need only the canonical case; metadata-API helpers that mirror them dispatch on the public preserved field and need the alias set listed explicitly. The pattern is structurally invisible to (a) "find every site that calls this helper" sweeps (pattern 14) because the helper IS the site — the bug is that the helper's own case list missed an alias, not that a sibling helper isn't called; (b) "find every site that dispatches on the same field" sweeps because the wire-pipeline parallel dispatches on a DIFFERENT field (`node.kind` vs `SchemaNode.Type`); (c) encode/decode-parity sweeps because the wire format agrees by virtue of normalization happening upstream. Concrete instance: a past audit's `coerceMetadataDefault` / `branchAcceptsDefault` / `lookupNameRef` triple — three sites in `schema_node.go` dispatching on `SchemaNode.Type` and missing `"error"` alongside `"record"`. The wire parallels (`coerceDefault`, `validateDefault`, `walkDefault`, the schema builder's `case "record", "error":` at schema.go) all dispatched on `node.kind` and were correct because the builder collapsed the alias at parse time. Audit angle: for every fix to a metadata-API helper (or any helper that dispatches on a public-API preserved-as-written field), enumerate the normalizer's collapse set — the dispatcher must include every alias the normalizer collapses. Concentrate the predicate in a single named helper (e.g., `isRecordKind`) so the alias rule lives in one place; otherwise three near-identical sites are three drift points. Known dual-namespace pairs in twmb (each generates a sibling-sweep candidate when one side is modified): `aobject.Type` (parse-intermediate, preserved) ↔ `schemaNode.kind` (internal, normalized); `SchemaNode.Type` (public, preserved) ↔ `schemaNode.kind` (internal, normalized). If a new alias gets added to the normalizer in the future, this entry is the place to record it so all dispatchers can be re-swept.

    **Per-value predicate drift between dual-namespace helpers** (broader than alias-set drift). The dual-namespace pattern above scopes to alias-list completeness. A second class of drift hides in the same shape: paired helpers that implement the SAME CONCEPTUAL CONTRACT but live in different namespaces (`*schemaNode` internal vs `*SchemaNode` public) can diverge on the *per-value predicates* they apply, not just on the *type-name aliases* they accept. The wire-side helper does full value-level validation (range checks, whole-number checks, codepoint-range checks, size-match checks); the metadata-side helper is a structural type-only matcher (`case string, []byte: return true`). Both dispatch correctly on type-name; both miss the value-level rejections the wire helper applies. The audit angle that finds this class: when a wire-side validator is updated (e.g., `convertDefaultBytes` swapped to use `validateDefault` as its branch selector), the metadata-side parallel is a sibling-sweep candidate even if no alias was added. Concrete instance: the `branchAcceptsDefault` numeric arms accepted `{int64, int32, json.Number, float64}` and bytes/fixed arm accepted `{string, []byte}` purely on type, while wire-side `validateDefault` (via `defaultAsInt32`/`defaultAsInt64`/`defaultAsFloat` for numeric, `validateAvroByteString` + size-check for bytes/fixed) applied range/precision/codepoint/size predicates. Same input traversed both helpers, picked different branches. Fix shape: extend the wire-side helper to accept the metadata-side normalized input types (e.g., `numericDefault` and `defaultAsFloat` now accept int64/int32 as well as json.Number/float64), then have the metadata-side helper *delegate* to the wire helper — drift impossible by construction. The four-axis-rule corollary covers stdlib-parser callsites; this corollary covers wire-vs-metadata predicate helpers more generally. Probe playbook for any wire-side validator change: grep the metadata-side namespace (`*SchemaNode`) for parallel helpers implementing the same conceptual contract; if a parallel exists and does NOT call the updated wire-side validator, file as `[sibling-of-fix]`.

    **Within-helper arm asymmetry: when one arm of a multi-arm dispatch helper is updated to apply a wire-side predicate, every OTHER arm in the same switch is a candidate.** Sub-pattern of the drift class above, scoping inside one function rather than across helper pairs. When a fix updates a metadata-side `branchAcceptsDefault`-style switch to delegate one `case` (or a few cases) to the wire-side per-value predicate, every other arm of the SAME switch is a sibling — even though they live in the same function and the diff didn't touch them. The grep `grep -n '<helper-name>' *.go` finds the helper but doesn't enumerate its arms; the structural angle is "for each `case` in the helper's switch, ask: does this arm need to apply a wire-side predicate the analogous wire-side arm enforces?" The numeric and bytes/fixed cases are easy to delegate because the wire-side has standalone helpers (`defaultAsInt32` / `defaultAsFloat` etc.). The structural cases (record/error, array, map) appear "self-contained" — they look like a simple `_, ok := val.(map[string]any); return ok` — and are easy to leave behind because the wire-side does its per-element validation via a *recursive walker* (`walkDefault` calling `validateLeaf`), not via a single delegatable helper. The fix is to mirror the wire-side recursion locally in the metadata helper.

    **The mandatory artifact: an exhaustive wire-vs-metadata per-arm table.** "Verified per-arm" as a verbal claim is the trap that lets siblings slip. The actual probe is: for every `case` in the helper's switch, write a one-line entry naming the kind, the wire-side predicate, the metadata-side predicate, and whether they agree. If the metadata side has fewer arms than the wire side because of `case A, B:` lumping, EACH LUMPED KIND IS A SEPARATE ROW. The row that doesn't agree is the bug.

    **Combined `case A, B:` clauses are a specific hiding pattern.** When the metadata side writes `case "string", "enum":` and the wire side writes separate `case "string":` / `case "enum":` with different per-value predicates, the combined-case body silently applies whichever predicate is correct for ONE of the kinds — most often the "easier" one — to all kinds in the case list. The eye reads the body as covering both kinds; the actual predicate covers only one. The grep angle: `grep -nE 'case "[a-z]+", "[a-z]+"' <helper-file>` enumerates every combined case in the metadata-side dispatcher; for each, ask "do the wire-side equivalents share this body, or does ONE of them have a per-kind predicate the body doesn't apply?" If yes, split the case.

    **Concrete instances:**
    - Round N: structural arms of `branchAcceptsDefault` were caught as still type-only (`_, ok := val.(map[string]any); return ok` for record/map; `_, ok := val.([]any); return ok` for array). A union like `[{record needing field X}, {record needing nothing}]` default `{}` metadata-matched the first branch (any map matches) while the wire path picked the second; user-observable as `s.Root().Fields[0].Default["shared"]` coming back as `float64(42)` while the wire-decoded auto-fill returned `int64(42)`. Fix: mirror the wire-side recursion locally in the metadata helper.
    - Round N (same round, post-fix re-audit caught immediately): `branchAcceptsDefault`'s `case "string", "enum":` was combined — the body only checked `val.(string)` for both, but the wire-side enum arm requires `slices.Contains(node.symbols, sym)`. Union `[enum:{A,B}, bytes]` default `"Z"` metadata-matched enum (any string accepted) while wire picked bytes (Z not a symbol); user-observable as `Default[u] = string("Z")` vs wire-decoded `u = []byte("Z")`. The combined-case clause was the hiding pattern — the structural-arms diff above didn't touch enum because it was visually grouped with `string`. Fix: split `case "enum":` into its own arm with `slices.Contains(t.Symbols, sym)` (guarded by `len(t.Symbols) > 0` for fwd-ref tolerance). Lesson: writing the per-arm table BEFORE believing the sweep is done would have caught both arms in one pass.

    The audit angle generalizes: any helper that pattern-matches on a public field with N cases, where the fix updated K < N cases (especially when "N cases" includes combined `case A, B:` clauses that visually look like "1 case"), owes a per-arm audit of the remaining N-K cases asking "does this arm need the same treatment?" The probe is the table.

- **Stdlib-type targets carry stdlib-defined invariants.** When the decoder writes into a stdlib type (`json.Number`, `*big.Rat`, `time.Time`, `time.Duration`, `*big.Float`, etc.), that type's documented invariant becomes the decoder's contract too. `json.Number`'s "underlying string is a valid RFC 8259 number literal" is the canonical example; `time.Duration`'s int64 nanosecond bound is similar. The structural failure mode: `reflect.Kind`-based target dispatch collapses distinct types with the same Kind (e.g. `json.Number` and a plain `*string` both have `Kind()==reflect.String`), so a setter's `if v.Kind() == reflect.String { v.SetString(content) }` arm silently accepts arbitrary wire content into the stdlib type, producing values that violate the invariant. The downstream failure (e.g. `encoding/json.Marshal` rejecting the invalid `json.Number`) is far from the decode site, so debug-bisection bounces between "the decoder produced this" and "the marshaler rejects this." The bug class is structurally invisible to encode/decode-parity sweeps when the symmetric encoder *correctly* rejects the stdlib type as input — a round-trip test from a typed input passes (encoder rejects → user never gets there), masking the decoder's broken acceptance of the stdlib-type target. Concrete instances: a past audit caught `setFloatValue` writing `strconv.FormatFloat(±Inf)` ("+Inf"/"NaN") into json.Number; a later round caught `setStringValue` / `setBytesValue` / `setEnumTarget` / `deserFixed` / `deserUUID` / `setTimeAsLongTarget` / `deserDate` / JSON `decodeString` / JSON-date / JSON-time / JSON-uuid arms all writing arbitrary text into json.Number — same shape across 11 sites, none of which had a `v.Type() == jsonNumberType` arm because the original setters predated json.Number as a documented target. **Probe**: for every stdlib type added to the target matrix, identify the type's documented invariant (godoc / godoc.org) and write a boundary test that exercises wire content violating it; if the decoder accepts, file a finding. Then sweep every existing `reflect.Kind`-based setter that could receive that stdlib type and audit whether the invariant is enforced.

- **Error messages echo unbounded user-controllable input — log / RPC / metric-label amplification.** Distinct from pattern 9 (parser-CPU amplification) and pattern 16 (precision-fix introduces DoS): the parser itself rejects in O(1) via a length cap, BUT the rejection's error message interpolates the rejected input verbatim via `fmt.Errorf("…%q…", userInput)`. A 10 MiB hostile JSON.Number → 10 MiB error message → 1:1 amplification through logging pipelines, RPC error channels (Kafka consumer error returns), Prometheus metric labels (the error string ends up as a label value), tracing systems. The CPU posture is fine; the *string* posture is the DoS. Structurally invisible to: (a) timing-based DoS probes (parse takes ~0.1 ms, not slow), (b) wire-correctness round-trip probes (the error is correct, just enormous), (c) the unit tests that pre-existed the bug (every existing test passes a short hostile-looking input and asserts the error exists, not its size). The bug surfaces only when an auditor explicitly measures `len(err.Error())` against a multi-MB hostile input.

  **The structural angle that catches it:** the codebase has safety helpers (`truncForError`, `truncBytesForError`, and the `%T(%v)`-aware `truncValueForError`) whose docstrings explicitly state "prevent 1 MiB hostile inputs from producing 1 MiB error strings." This is pattern 14b applied at the error-construction layer — the helper exists, the helper is used at a few sites, but it's silently absent at most. The bypass shape: every `fmt.Errorf("…%q…", x)` / `fmt.Errorf("…%v…", x)` where `x` is wire-controlled (wire-decoded string content, parsed JSON-Number bytes, UUID wire payload, enum symbol from wire, schema-default value, map-key from parsed default) is a candidate. Per-helper input classification:
  - `truncForError(s string)` — 80-char cap; for arbitrary user-controllable string default literals, JSON-Number strings, decimal RatString, enum symbols.
  - `truncBytesForError(b []byte)` — 40-char cap (sized for fixed-format diagnostic values like UUIDs and int literals); for JSON-scanner number-bytes and UUID wire bytes.
  - `truncValueForError(v any)` — `%v`-style format with type-aware fast paths (string / []byte / json.Number skip the format step); for the `%T(%v)` shape at union-default rejection sites (`walkDefault` / `encodeDefault` for unresolved unions).

  **Sweep playbook:** `grep -nE 'fmt\.Errorf\([^)]*%[qsv][^)]*\)' *.go | grep -v _test`. For each hit, classify the interpolated argument:
  1. **Schema-bounded** (`node.kind`, `f.name`, schema-defined enum symbols, fixed-format internal tokens): safe — the schema-parse step already accepted those bytes; echoing them is 1:1 with the schema input.
  2. **Wire-controlled** (wire-decoded bytes, JSON-scanner output, json.Number content, UUID wire payload, default literal from schema-parse-time validation): every such site needs a `truncForError` / `truncBytesForError` / `truncValueForError` wrap. Filing a finding requires a runnable failing test that measures `len(err.Error())` against a 1+ MiB hostile input.
  3. **Mixed** (record/map-key error wraps where the key comes from a parsed-JSON default with no upstream length cap): also needs wrapping; the schema acceptance step bounded the schema as a whole, but a giant key inside a small schema is still possible.

  Concrete instance: ~15+ sites across `tryCoerceToRat` (3), `parseUUIDBytes` (6 echo sites), enum encoders (`serEnum.ser` + JSON `appendAvroJSON case "enum"` + `decodeEnum`), enum-default validation (schema-parse-time `validateLeaf` + parse-time `parseField`), `walkDefault`'s union no-match (`%T(%v)`), `encodeDefault`'s union no-match (same shape), `setDecimalRat` overflow, `buildBigDecimalPayload` no-finite-expansion, `ratToUnscaled` scale-mismatch, big-decimal JSON decode, `rejectJSONNumberStringTarget`, `parseSpecialFloat`, JSON decode logical-bytes `boundedRatFromString` wrap. Each rejected its 10 MiB hostile input in < 1 ms (CPU posture fine) but produced 10 MiB error message. Fix: wrap each user-controllable interpolated argument with the appropriate trunc helper.

  **Stdlib-parser-error sub-pattern: errors from stdlib `json.Unmarshal` / `strconv.Atoi` / `strconv.Parse*` embed the failing input VERBATIM in their typed error structs.** Distinct from the `fmt.Errorf("%q", x)` shape the sweep grep catches: the call site is `json.Unmarshal(data, &targetIntField)` or `strconv.Atoi(s)`, no `%q` interpolation visible — but the returned `*json.UnmarshalTypeError.Value` and `*strconv.NumError.Num` carry the full input bytes. A subsequent `fmt.Errorf("...: %w", stdlibErr)` then FORMATS the message string at construction (Go's `*fmt.wrapError.Error()` returns a precomputed `msg` field) and caches the multi-MB content. Mutating `ute.Value` / `ne.Num` via `errors.As` AFTER the `%w` wrap is too late — the cached string is already set in stone.

  Two-layered defense required:
  - **Length-cap at the custom `UnmarshalJSON` entry** (for `laxInt`-style types whose body wraps via `fmt.Errorf("%w", strconvErr)`): rejects hostile input before the stdlib parser builds a multi-MB error. `maxLaxIntDataLen` at `schema.go` is the pattern — see `laxInt.UnmarshalJSON`.
  - **Post-hoc truncation of stdlib error structs** at the OUTERMOST wrap site (for bare `*int` / `*int32` / etc. fields with no custom UnmarshalJSON): `errors.As(err, &ute)` / `errors.As(err, &ne)` walks the chain and mutates `ute.Value` / `ne.Num` BEFORE the final `fmt.Errorf("...: %w", err)` formats. The mutation reaches uncached descendants. `boundJSONErrorEcho` at `schema.go` is the pattern; applied at `parse()`'s top-level json.Unmarshal wrap.

  Sweep target on any json.Unmarshal call: classify the target type. `string` / `bool` targets don't echo; `int` / `float64` / typed-numeric targets and custom-`UnmarshalJSON` types whose body wraps via `%w` both echo. Concrete instance: 7 schema-parse-time fields amplified — `aobject.Size *laxInt` (both arms), `aobject.Scale *int`, `aobject.Precision *int`, `afield.Scale *int`, `afield.Precision *int`. All five `*int` / `*laxInt` schema fields were silently amplifying before the two-layered defense landed.

  **Deferred-render sub-pattern: the trunc helper trims the RESULT, but the render that PRODUCES the result is the unbounded work.** The most insidious shape, because the call site DOES wrap with a trunc helper — so the primary sweep grep (which filters out `truncForError` / `truncBytesForError` / `truncValueForError`) skips it as already-defended. But the wrapped argument is `bigRat.RatString()` / `bigRat.FloatString(scale)` / any base-conversion or stringify that materializes the FULL value before truncation runs. `truncForError("…multi-MB string…")` only trims a string that `RatString()` already spent superlinear time and O(value) allocation building — the truncation is too late to bound anything. A `5^(2^24)`-denominator `*big.Rat` took ~20 s and produced a multi-MB error even though the rejection decision itself was O(1). Fix: bound at the VALUE, not the string — check the magnitude (`r.Num().BitLen()` / `r.Denom().BitLen()`) and skip the expensive render when oversized, reporting bit sizes instead (`truncRatForError`, `ser.go`). Sweep target the primary grep cannot see: `grep -nE 'trunc(For|Bytes|Value)?Error\([a-zA-Z0-9_.]+\.(RatString|FloatString|String|Text|Marshal)' *.go` — every `truncX(value.ExpensiveRender())` is a candidate where the render's cost grows with the value's size. The catch is run by FIX.md item 5's hostile-input probe TIMING THE REJECT: a megabit input that rejects in >100 ms means the cost is pre-truncation, even though `len(err.Error())` is small (so the `len`-based check from the parent entry would pass). Concrete instance: a prior fix — 4 sites (`buildBigDecimalPayload` no-finite-expansion, `ratToUnscaled` scale-mismatch, `setDecimalRat` overflow, big-decimal JSON decode) all did `truncForError(r.RatString())`, all pre-existing, surfaced by the item-5 timing probe during the `finiteScale` round.

  **Why this entry is necessary even though pattern 14b covers it abstractly.** Pattern 14b's framing is "safety helper bypass" — auditors who sweep pattern 14b focus on helpers gating *acceptance/rejection* operations (`v.SetString`, `v.SetInt`, etc.). The trunc helpers gate a *message-construction* operation that doesn't change acceptance. Treating it as a separate axis names the structural blind spot: error messages are user-visible behavior, not just diagnostic text. Without this entry the next round will re-derive "but error messages aren't behavior" and skip the sweep again.

- **`SchemaFor` (Go-type → schema) has no wire-format counterpart, so the wire-parity and four-axis lenses never reach it — now netted generatively by `TestSchemaForEncodeParity` (schema_for_test.go).** The audit's dominant angles (encode/decode parity, stdlib-parser axes, default-value pipeline) all assume there's wire data flowing. `SchemaFor` is a pure Go-reflection→schema-JSON transform: its correctness contract is "every valid Go type produces a valid, round-trippable, internally-consistent schema, and its accept/reject strictness is uniform." That contract is invisible to every wire probe. **The build-accepts/encode-rejects sub-class is the highest-yield SchemaFor shape and sat in the one intersection EVERY other net is blind to simultaneously** (recorded so no future round assumes it covered): (a) mutation testing finds weak ASSERTIONS over EXERCISED code paths, never a MISSING input class — no test fed `json.Number` to SchemaFor, the `case reflect.String` line is correct-for-strings and scores killed/green, and there is no AST node for "the absent json.Number case" to mutate; (b) the combinatorial matrix is schema→value (Parse-driven) and never traverses Go-type→schema; (c) Java/fastavro have no SchemaFor to differential against; (d) `SchemaFor[T]` is generic over a compile-time type, so a byte-fuzzer cannot synthesize the field types. The durable net that DOES reach it: because `schema_for_test.go` is `package avro` (internal), it mirrors SchemaFor's body (`inferRecord`→`dedupNamedTypes`→Marshal→Parse) over a `reflect.StructOf`-built struct, enumerating field types the generic `SchemaFor[T]` cannot at runtime, and asserts the invariant **SchemaFor-accepts ⟹ Encode-accepts** (a reject is always safe — build-time strictness cannot defer a failure). Two construction rules the net must keep: (1) cross every codec-special-cased / `reflect.Kind`-misleading Go type (`json.Number`, `time.Time`, `time.Duration`, `big.Rat`, byte containers) plus named aliases / pointers / slices / maps / nesting — the high-risk surface is "stdlib types whose Kind doesn't match the Avro type the codec wants"; (2) drive a NON-EMPTY recursive sample value (`sampleValue`), NOT the zero value — a nil pointer / empty slice / empty map never materializes its leaf type, so a `json.Number` buried in `*T`/`[]T`/`map[K]V` would slip (a first version using zero values caught only top-level `json.Number`; the three container siblings stayed nil/empty — revealed by neutering the fix, the same blind-spot class the net itself targets). The concrete instance: `inferType`'s `reflect.String` arm emitted `"string"` for `json.Number` (Kind String, no text interface), but the codec's documented numeric-only policy rejects `json.Number` for every stringy schema on both encode and decode — so SchemaFor built a schema its own codec rejects for that field, failing at Encode far from the call. Fixed by an exact-type `case jsonNumberType:` reject in `inferType`'s `switch t` (beside the `big.Rat` guard, after the CustomType loop so a registered custom still works; named aliases and the `map[json.Number]V` KEY exception stay accepted). Pinned by `TestSchemaForRejectsJSONNumber` (targeted) + `TestSchemaForEncodeParity` (generative). **Meta-lesson: mutation testing measures the net's assertion quality, it does not enumerate inputs — a missing-input-class bug needs a generative input axis, and for SchemaFor that axis must be reflect-driven (internal `package avro` test) because the public entry is compile-time generic.** Other Go-type-shape sub-shapes found earlier, each a distinct gap:
  - **Memoization keyed coarser than the value it caches.** The `seen map[reflect.Type]string` dedup cached *one* emitted name per Go type, but the name a `[N]byte` gets depends on `(type, logical)` — `"uuid"` when `,uuid`-tagged, `t.Name()` plain. Same type used both ways → the second occurrence emitted a name reference under the *other* form's name → dangling reference Parse rejects. General rule: any cache/dedup whose KEY is coarser than the inputs that determine the cached VALUE returns the wrong value when two inputs collide on the key. Audit every memoization (`seen`, name tables, custom-type wiring maps) by asking "is the key as fine-grained as everything that varies the output?"
  - **A generic pre-tokenizer over-applies to a take-the-rest field.** `splitTag`'s bracket-balance scan (for `alias=[...]` / `decimal(...)`) ran over the *whole* tag including a `default=` value documented to "take the rest verbatim," so a string default containing an unbalanced `(`/`[` was rejected. General rule: when one option's grammar is "the literal remainder," the tokenizer's structural checks must stop at that option's boundary.
  - **Asymmetric strictness: validates one compatibility axis, silently defers another.** `SchemaFor` rejected Go-type/logical-tag mismatches at build time but let a `default=` that fits the Avro type yet overflows the narrower Go field through, deferring the failure to a decode-time error far from the call. General rule: when a builder validates compatibility axis A up-front, axis B (same "does this Go field accept this value" question, different value source) should validate up-front too, or the strictness is a coin-flip from the user's view.

- **A hand-rolled parser that replaced a stdlib parser silently dropped the stdlib's rejections.** A perf rewrite that swaps `encoding/json` (or any stdlib parser) for a hand-rolled scanner re-implements the happy path but omits the stdlib's malformed-input checks unless they are re-added by hand. twmb's streaming JSON scanner (`json_scan.go`, which replaced a `json.Unmarshal → fromAvroJSON → Encode → Decode` pipeline) shipped accepting raw control chars, invalid UTF-8, unrecognized escapes, trailing content, and trailing-dot float literals — every one of which `encoding/json` rejects. The angle that finds this whole class at once: **differential-test the hand-rolled parser against the stdlib.** Feed a battery of edge/malformed inputs (leading zeros, hex/underscore/bare-exponent/double-dot/trailing-dot numbers; `\x` escapes, raw control chars, and invalid-UTF-8 strings; `truex`/`nullx` literal prefixes; trailing content; duplicate keys) to BOTH `s.DecodeJSON(in, &any)` and `encoding/json.Unmarshal(in, &any)`, and triage every acceptance divergence. A lenient accept that recovers obvious intent (`5.`→5) can be intentional; *silent corruption* (`\x41`→`x41`, dropping a byte) or *accepting what the encoder never emits* is an incidental gap. Before tightening, confirm direction against the ecosystem (Java/Jackson, fastavro, goavro, avro-rs, the spec) — and note (a) every impl that *allows* a permissive form (e.g. trailing content) also *returns* it for the caller to use, so an API that allows-but-discards is uniquely a footgun, and (b) the maintainer's downstream real-world consumers (rpk serde, console Avro viewer) may rely on a specific form — check their PRs/issues. Provenance tell: `git log -S '<the lenient line>'` to the rewrite commit; a behavior that predated the rewrite via the stdlib is an incidental regression, not a feature. Add to the yield-map under "JSON edge cases."

- **A lenient validator's early soft-drop leaves a field unvalidated, and a downstream "re-enable" path then assumes the validation ran.** A validator that *soft-drops* (silently clears + `return nil`) on one mismatch BEFORE a later required-field check leaves that field in its unvalidated state (often a nil pointer). Harmless while the drop is terminal — but when a SEPARATE path conditionally *resurrects* the dropped thing (e.g. to let a registered handler take over) and routes it into the code path that ASSUMES the validator's later checks ran, the unvalidated field is dereferenced → panic on otherwise-valid input. The angle: for every validator with multiple early `return`/soft-drop exits, list which exits skip which later checks; then find every site that re-enables a soft-dropped value and ask "does it route into a consumer that assumes the skipped check ran?" The consumer's pointer derefs of validator-owned fields (`*o.Precision`, `*o.Size`, …) are the panic surface. Fix shape: gate the assuming-consumer on the precondition the skipped check would have established (e.g. `o.Type == "bytes"`, the only underlying for which decimal precision is required-and-present), so a resurrected value on the wrong shape falls through to a path that does not assume validation. Concrete instance: `validateLogical`'s decimal arm soft-dropped the logical for a non-bytes/fixed underlying BEFORE its `Precision == nil` check; `buildComplex` resurrected the dropped `"decimal"` for a matching CustomType and entered the bytes-decimal branch, dereferencing nil `*o.Precision` on `Parse({"type":"int","logicalType":"decimal"}, WithCustomType{decimal})`. Distinct from the "lenient walker skips a nil child its strict sibling derefs" entry (that's two walkers over one tree; this is one validator's drop composed with a separate resurrect path).

- **CustomType logical-codec suppression conditions must be mirrored on BOTH wire formats AND BOTH directions — and the conditions differ per build.** When a custom type matches a logical node, the binary builds SUPPRESS the built-in logical codec so the custom callback (or, for a nil callback, the user) sees the RAW Avro-native value. The suppression conditions are per-direction and per-build, and the JSON encode (`appendAvroJSON`) and decode (`decodeKind` via `wrapDecodeJSONWithCustomDecoders`) paths must replicate each one or a binary↔JSON divergence appears. The conditions: **deser** suppressed for ANY match (`hasMatchingCustomType`); **ser** suppressed only when a custom Encode exists (`hasMatchingCustomTypeWithEncode`); the **fixed build** (`schema.go` fixed case) suppresses the ser for ALL fixed logicals (decimal/duration/uuid → base `serSize`) while the **primitive/bytes builds** suppress only decimal/big-decimal (the general primitive path keeps the logical ser, wrapped by the custom encoder). The angle of attack — invisible to round-trip tests that use Decode!=nil customs and matching-GoType values: enumerate by `(build site × direction × logical kind)`, not by the one shape a bug report exercised. For every `hasMatchingCustomType` / `hasMatchingCustomTypeWithEncode` gate in the binary builds, confirm the JSON encode arm (gate on the THREADED `encodeSuppresses` = `hasMatchingCustomTypeWithEncode`, stored on `customWiring` — NOT the runtime proxy `custom[node].encode != nil`) and the JSON decode wrapper (gate on the THREADED `suppressLogical` = `hasMatchingCustomType`, plus `jsonDecodeAppliesLogical` to scope which logicals — NOT the proxy `len(decoders) > 0`) apply the SAME condition for the SAME node shape. The seven divergence shapes this missed: (a) `Decode==nil` custom on a logical node — JSON returned the transformed Go type (time.Time / *big.Rat), binary returned raw; (b) `Encode!=nil` custom with a non-matching pass-through value (e.g. `*big.Rat` into a custom decimal, or a non-UUID string into a custom fixed+uuid) — JSON ran the logical coercion arm the binary suppressed; (c) pointer/interface-GoType custom at a UNION BRANCH — JSON peeled the pointer before union dispatch so the branch's GoType filter never matched (binary serUnion passes the un-peeled value to branch sers); (d) **`[introduced-by-fix]`: WILDCARD custom (empty LogicalType AND AvroType) over-suppressed — on BOTH directions, missed across multiple passes because each pass fixed only one direction.** The binary suppression gates EXCLUDE the both-empty wildcard (`hasMatchingCustomTypeCond` skips it at schema.go — wildcards use `ErrSkipCustomType` at runtime, so the binary leaves the built-in logical codec in place and feeds the callback/user the ENRICHED value). Both JSON fixes initially gated on a RUNTIME PROXY that INCLUDES wildcards: the decode fix used `len(decoders) > 0` (a wildcard has a Decode in its chain → JSON wrongly suppressed → raw while binary stayed enriched); the encode fix used `custom[node].encode != nil` (a wildcard has an Encode wrapper → JSON wrongly skipped the decimal/fixed arm → rejected `*big.Rat` while binary's `serBytesDecimal` accepted it). **The trap: a runtime proxy (`len(decoders) > 0`, `custom[node].encode != nil`, "node has a decodeJSON wrapper") is NOT the binary predicate — proxies include wildcards, the binary gates exclude them. Gate on the EXACT predicate, threaded.** **And the meta-trap: when a fix replaces a proxy with the threaded predicate, sweep BOTH directions (encode AND decode) in the SAME pass — fixing decode and leaving encode on the proxy is exactly how this regenerated for three passes.** Fix shapes: thread `suppressLogical` = `hasMatchingCustomType` AND `encodeSuppresses` = `hasMatchingCustomTypeWithEncode`, BOTH computed once at parse and stored on `customWiring` (so resolved nodes via resolve.go reuse them); gate the JSON decode raw-wrapper install + its `suppressLogical` flag on `suppressLogical` (scoped by `jsonDecodeAppliesLogical`, which reports which logicals `decodeKind` transforms); gate the JSON encode decimal/big-decimal/fixed arms on `!encodeSuppresses`; **`jsonDecodeAppliesLogical` DERIVES its answer by probing the `decodeLogical*` functions at parse time (returns true iff a probe value comes back as a non-raw type), so it is correct by construction and can never drift from what `decodeKind` actually does — even for a future logical, no parallel hand-list to update. The probe boxes into `any`, costing a few PARSE-time allocs per custom-typed logical node (one-time, schema is cached); it does NOT touch the encode/decode hot path (decodeKind and the decodeLogical* functions are unchanged), so enc/dec throughput is unaffected — and that hot-path axis is the only perf constraint here (parse-time allocs are acceptable; benchstat confirmed enc/dec allocs identical, sec/op unchanged). `TestRegression_JSONDecodeAppliesLogicalMatchesDecode` pins the probe's output against the human-known transform set. Lesson: when a parse-time predicate must equal a hot-path function's case set, DERIVE it from that function (probe at parse) instead of a parallel hand-maintained list — drift becomes structurally impossible, and the only cost (parse-time boxing) is off the hot path;** dispatch union before the peel loop so branch encoders see the un-peeled value, with a local peel for the tagged-map / type-name decisions inside `appendAvroJSONUnion`. (e) **`[introduced-by-fix]`/fast-path asymmetry: wildcard Encode callback DOUBLE-FIRES on `EncodeJSON` for a 2-branch `["null", T]` (null-first) union with a non-nil value** — not a value/wire divergence (output is identical) but a side-effect divergence in callback INVOCATION COUNT. The binary 2-branch `["null", T]` shape dispatches via the compiled fast path `serNullUnionAt` (ser.go), which for a non-nil value goes straight to the non-null branch and NEVER trials the null branch; the JSON `appendAvroJSONUnion` has no 2-branch fast path, so its generic try-each trialed the null branch first, firing the wildcard custom encode hook (installed on the null node) spuriously before `case "null"` rejects the non-nil value. For a side-effecting wildcard (logging / metrics / property dispatch via `ErrSkipCustomType`), the Encode runs twice on `EncodeJSON` vs once on `Encode`. Precisely bounded: ONLY 2-branch null-FIRST (null-second `["T","null"]` is clean — try-each hits T at index 0 first; N≥3 is clean — binary's `serUnion.ser` try-each ALSO trials null, so both fire). The fix: skip the null branch in the JSON try-each for 2-branch unions (`if len(node.branches) == 2 && branch.kind == "null" { continue }`), mirroring the binary fast path's branch-trial set per arity. **Lesson: a fast/slow-path optimization on ONE wire format (the 2-branch union fast path) changes the set of branches trialed, and any per-branch SIDE EFFECT (a custom encode/decode hook) must fire the same number of times on the other format — test wildcard callback INVOCATION COUNTS across union arities, not just values/errors (the prior wildcard tests checked values/errors but never unions or counts, so this hid).** (f) **`[introduced-by-fix-coverage-gap]`: NO-CALLBACK custom (Encode==nil AND Decode==nil) but NON-wildcard (LogicalType or AvroType set) — binary suppressed via `hasMatchingCustomType` (which counts callback-less matchers — only the both-empty wildcard is excluded), but the JSON decode wrapper was never installed.** The parse-time wiring (`applyCustomTypes`, schema.go) bailed at `if len(encoders) == 0 && len(decoders) == 0 { return nil }` BEFORE reaching the `suppressLogical && jsonDecodeAppliesLogical(node)` wrapper-install branch. So a `CustomType{LogicalType:"timestamp-millis"}` (the documented way to opt OUT of logical enrichment per the Decode==nil contract) gave raw `int64` from `Decode` but enriched `time.Time` from `DecodeJSON`, on the SAME schema, across EVERY logical type. The shape (a) Encode-only pin (`TestRegression_CustomDecodeNilRawValueBinaryJSONParity`) gives its custom an `Encode` callback (`len(encoders) > 0`), so it sidesteps the early return — the no-callback case was structurally outside its coverage (the fix matched the bug report's input class, not the helper's full set — Pattern 14a). Fix: compute `suppressLogical`/`jsonAppliesLogical` BEFORE the early return, and only bail when `len(encoders)==0 && len(decoders)==0 && !jsonAppliesLogical` (a wildcard, or a no-callback match on a non-logical node, still bails — nothing to mirror; a no-callback match on a LOGICAL node falls through and installs the raw-decode wrapper). (g) **`[introduced-by-fix-coverage-gap]`: schema-resolution PROMOTION re-applied the reader's logical UNCONDITIONALLY, ignoring custom suppression — across ALL callback configs, and a TYPE-only check masked half of it (Pattern 13b).** `doResolve`'s promotion branch (resolve.go) wraps the widening deser with `promotionDeserForLogical(w.kind, r)` to re-apply the reader's logical (so `int`→`long+timestamp-millis` yields time.Time). That wrap fired even when a matching CustomType suppressed the reader's built-in logical decoder, so for the SAME reader+custom a DIRECT long wire fed the user/custom-decoder the RAW int64 (suppressed at parse build) while a PROMOTED int wire fed the ENRICHED time.Time. For no-callbacks/encode-only the Go RESULT type differs (int64 vs time.Time); for decode-only/both the result type is always the custom's, but the custom Decode RECEIVES a different raw type (int64 vs time.Time) — invisible to a `%T`-only promoted-vs-direct check, caught only by comparing the value the Decode records. Fix: gate the wrap on `ctx.custom[r] == nil || !ctx.custom[r].suppressLogical` (composes with (f), which now stores the `customWiring` with `suppressLogical` for no-callback matchers so resolution can consult it). Lesson reaffirmed: when verifying promoted↔direct (or any) parity, compare VALUES the callback was fed, not just result TYPES — a marker-returning Decode that records `%T` of its input is the oracle; a result-type check passes vacuously when the transform output type is config-fixed. (h) **DIFFERENT MECHANISM (not suppression, a PARSE-time failure): a self-/mutually-recursive or forward-referenced named type whose subtree contains a logical a registered CustomType matches FAILED to Parse.** `rejectCachedRefIfCustomTypeWouldMatch` (schema.go) exists to reject a named type inherited from a *SchemaCache* across Parses whose baked ser/deser predates this Parse's CustomTypes (a real silent-drop hazard, pinned by `TestFieldLevelLogicalType_CacheRejectionAcrossFlatForm`). It consults `namedType.hadCustomType`, stamped in `registerNamed` from `hasCustomTypeWired()` (`len(b.custom) > 0`) — but a record's named entry is registered EARLY (before its fields build, so before any CT wires) to support self-reference, and a self-reference (`{"name":"next","type":["null","Node"]}`) resolves DURING field-build, before the post-build `hadCustomType` re-stamp. So the guard saw `hadCustomType==false`, found the CT-matched logical in the subtree, and rejected a VALID schema (`Parse` returned "cached type Node contains … which would match a CustomType; re-parse Node with the CustomType first"). The guard's own docstring says it is about the *SchemaCache* — a current-Parse definition has the CTs in scope and applies them to its single node, so it is never a stale cache. Fix: gate the guard on `b.cachedNames[refName]` (the set of names inherited from the SchemaCache, populated in cache.go) — fire ONLY for cross-Parse names, never for a current-Parse self/forward reference. Invisible to: the existing cache-rejection test (uses two `SchemaCache.Parse` calls, so the name IS cached); every non-recursive custom test; and all binary↔JSON parity tests (the failure is at Parse, before any encode). The angle that found it: combine recursion × custom × logical — a paranoid sweep that crosses "named-type recursion" with "CustomType matching" rather than testing each alone. Lesson: a guard keyed on a flag stamped at one build phase but consulted at an earlier phase (mid-build forward/self reference) reads a stale value; gate such guards on a phase-independent fact (here: provenance — cache vs current parse), not on a flag that is only correct post-build. Pinned by `TestRegression_RecursiveCustomTypeParsesAndParity` (self-nested / self-wrapped / mutual / shared-multiref, Parse-success + binary↔JSON parity). (i) **FORWARD-REFERENCED named type drops the binary custom Encode/Decode wrap — JSON applies it (silent binary↔JSON divergence).** A named type used BEFORE its definition (`{"name":"a","type":["null","E"]}` then `E` defined later) is wired by `finalize`'s fixups (schema.go union-branch / record-field / array-map-item sites) to the UNWRAPPED `namedType.ser`/`.deser`; those sites did NOT re-apply the custom wrap that the IN-ORDER reference gets (tryAssignNamedRef → applyCustomTypes re-wraps). But `finalize` patches the shared `nt.node` into the parent (`m.branches[idx]`/`fields[idx].node`/`*m.nodeChild`), and the JSON encoder/decoder look up `custom[node]`/`node.decodeJSON` by that shared node — so JSON applied the custom while binary used raw. For an enum with a reorder Encode the forward-ref field wrote a DIFFERENT ordinal on each format (silent corruption); for a custom Decode it decoded raw on binary, enriched on JSON. A named reference is position-independent in Avro (Java's parser resolves every reference through ONE `Names` map at `Schema.java:1634` to the same Schema object — forward refs are a twmb+Java extension beyond the spec's "forward references are not permitted", but resolve identically), so encoding cannot depend on reference position. DRY fix: ONE shared wrap point — `makeCustomSer`/`customWrappedSer` (encode) + `customWrappedDeser` (decode) consulted by BOTH the in-order path and all three forward-ref fixup sites, so a future fixup can't forget the wrap. Pinned by `TestRegression_ForwardRefCustomTypeBinaryJSONParity` (union-branch + array-item × encode + decode). (j) **no-Decode CustomType suppression into a fixed-size byte-ARRAY (`[N]byte`) target diverged.** Under suppression (`Decode==nil`) the user gets the RAW Avro-native value; the JSON suppression wrapper boxed it into `any` then `setCustomResult`, which (unlike binary's raw `deserFixed` `reflect.Copy`) cannot land a `[]byte` into a `[N]byte` array — so a suppressed `fixed`+uuid/duration/decimal into `[16]byte`/`[12]byte`/`[8]byte` SUCCEEDED on binary, ERRORED on JSON; and uuid-on-`string` into `[16]byte` did the OPPOSITE (binary `deserString` has no array arm → error; JSON's `decodeString` uuid arm parsed it → success). DRY fix: the no-decoder suppression wrapper decodes STRAIGHT INTO THE TARGET via `decodeKind` (the same raw arms the binary deser uses: `assignBytes`/`setBytesValue` for fixed, `setStringValue` for string) instead of box-into-`any`+`setCustomResult` — parity by construction. uuid-on-string needed two more: thread `raw` into `decodeString` (gate its uuid arm `!raw`) and make `jsonDecodeAppliesLogical` report uuid-on-string as transforming (its TYPED-target arm is invisible to the `*any` probe — a pin that asserted `false` had LOCKED the bug, Pattern 13). Reference is twmb's own binary raw deser, which mirrors Java's GenericDatumReader "no Conversion → raw underlying" (String for string, byte[]/GenericFixed for fixed). Pinned by `TestRegression_CustomSuppressionByteArrayTargetParity`. (k) **SchemaCache: a type cached WITH a CustomType, referenced from a Parse WITHOUT it, silently INHERITED the custom on BOTH wire formats.** The cache shares the built `*namedType.node`, and the custom's effect is baked onto that shared node (binary suppression on `node.ser`/`.deser`, JSON `node.decodeJSON`) — there is NO per-Schema overlay for a named-type reference. So a plain (no-CustomType) Parse referencing a custom-built cached type decoded the SUPPRESSED/raw value instead of the built-in logical type. The cache.go:119 comment ("applyCustomTypes wraps b.ser/b.deser without mutating the node") was about the WRAP only; the SUPPRESSION (raw-vs-logical deser choice) IS baked onto the node at the build site. Per the documented per-Schema CustomType scope, a custom must not leak across the cache. Fix (chosen over a clean-isolation node-rebuild, which records bake field desers into deserRecord so there is no cheap node-level swap): a SYMMETRIC cache-boundary error guard — a cached type and the Parse referencing it must AGREE on custom-presence (forward: clean cached + matching custom now → "re-parse with the CustomType"; reverse: custom cached + no matching custom now → "register it or parse without one"); a consistent registration resolves; a current-Parse self/forward reference is exempt (cachedNames, now populated for every parse with inherited names, with re-registration gated on a separate `allowReRegister`). Coarse edge documented: ALL named types defined by a custom Parse are treated as custom-affected — stamped at FINALIZE over the parse's definedNamed list, never at registration (a registration-time stamp predates applyCustomTypes and permanently missed types whose OWN node matches the custom: fixed/enum self-matches kept `hadCustomType=false`, so the forward arm rejected even CONSISTENT registrations that the allow arm is documented to accept). The allow arm's "consistent" is trust-by-declaration, NOT comparison: Go funcs cannot be compared (and func code pointers collide across closures), so registering a matching CustomType on the referencing parse IS the opt-in, and the cached node's baked callbacks (the defining parse's funcs) are what run — a user registering a behaviorally-different func with the same match gets the defining parse's behavior, which is undetectable and documented. Forward-compatible: the error can be relaxed to auto-apply later without breaking callers. Pinned by `TestRegression_SchemaCacheCustomBoundaryGuard` (both directions + both consistent cases) and `TestRegression_SchemaCacheConsistentCustomSelfMatch` (fixed/enum self-match + record/namespaced subtree, consistent registration resolves). (l) **`[introduced-by-fix-coverage-gap]`: a custom Decode returning a POINTER (`*T`) into a POINTER target (`var x *T; DecodeJSON(…,&x)`) diverged — the JSON decoder-chain wrap pre-dereferenced the target.** The binary decoder-chain / all-skip fall-through (`wrapDeserWithCustomDecoders`) passes the UN-indirected `v` to `setCustomResult`, which itself walks/allocates pointer levels to find the assignable one, so a `*T` Decode result lands in a `*T` target. The JSON sibling (`wrapDecodeJSONWithCustomDecoders`) wrapped the SAME `setCustomResult` call in `indirectAlloc(v)`, peeling one pointer level off the target BEFORE `setCustomResult` ran; the `*T` result then no longer matched the now-`T`-level target ("cannot use T with Avro type string"). So a pointer-returning Decode into a pointer target SUCCEEDED on binary, ERRORED on JSON — same schema, same value. Invisible to shape (a)'s pin and every other custom test because they decode into `any` or a VALUE target, where the extra `indirectAlloc` is harmless (it pre-allocates a level `setCustomResult` would have allocated anyway). Fix: drop the `indirectAlloc` at both decoder-chain `setCustomResult` sites (json_decode.go) so JSON mirrors binary's un-indirected `setCustomResult(v, …)` — `setCustomResult` is the single shared pointer-walker for BOTH formats. Lesson: when two paths call the SAME assignment helper, any pre-processing wrapped around ONE call (here `indirectAlloc`) is a divergence vector — the helper already handles indirection, so the wrap double-applied it; the durable form is "call the shared helper identically," not "pre-shape the target on one side." Pinned by `TestRegression_CustomDecodePointerResultPointerTargetParity`. (m) **`[introduced-by-fix-coverage-gap]`: no-Decode suppression into a SCALAR typed target (`*string`, `*big.Rat`, `avro.Duration`) still applied the logical arm on JSON.** Shape (j) routed the no-decoder suppression wrapper STRAIGHT INTO THE TARGET via `decodeKind` (so `[N]byte` lands by `reflect.Copy`), but `decodeKind`→`decodeBytes`/`decodeFixed`→`assignBytes` still ran the decimal/big-decimal/duration logical switch UNCONDITIONALLY for a typed (non-`any`, non-`[N]byte`) target. So a suppressed `bytes+decimal` into `*string` decoded the logical-formatted `"123.45"` on JSON while binary's raw `deserBytes` handed back the raw 2-byte payload `"09"`; into `*big.Rat` JSON SUCCEEDED (the decimal arm coerced) while binary's raw deser REJECTED `*big.Rat` (no slice/array/string target). Same for `big-decimal`→`*big.Rat` and `duration`→`avro.Duration`. Shape (j)'s `[N]byte` pin only exercised the array arm, which `assignBytes` reaches via the `setBytesValue` FALL-THROUGH *after* the logical switch — so it accidentally bypassed the bug for arrays; the scalar typed targets sit ON the logical arms. Fix: thread the suppression `raw` flag into `assignBytes` and, when set, `return setBytesValue(v, b, node.kind)` BEFORE the logical switch — mirroring binary's raw `deserBytes`/`deserFixed`, which build NO logical deser under suppression (the `raw` flag is the same suppression signal `decodeString`'s `!raw` uuid gate already consumes, shape (j)). Lesson reaffirmed (Pattern 14a): shape (j)'s fix matched its bug-report input class (`[N]byte`) while the helper (`assignBytes`) had a WHOLE sibling target-set (`*string`/`*big.Rat`/typed) still on the un-suppressed path — the unit of analysis is every target `assignBytes` can land, not the one the report used. **AND the SIBLING-DECODER sweep this triggered (mandatory after any fix — grep the same shape across siblings BEFORE declaring done) found the SAME gap in `decodeInt`/`decodeLong`: each per-kind JSON decoder (`decodeInt`/`decodeLong`/`decodeString`/`decodeBytes`/`decodeFixed`) honored `raw` in its decode-into-`any` branch but applied the logical transform UNCONDITIONALLY for a TYPED target.** So a suppressed `date` into `time.Time` succeeded on JSON (enriched) while binary rejected the raw `int32`, and — the worst case — `time-millis`/`time-micros` into a `time.Duration` SILENTLY produced a DIFFERENT VALUE on each format (binary's raw nanoseconds `10800000` = 10.8ms vs JSON's logical `timeMillisToDuration` = 3h): both succeed, no error, corrupt data. `decodeString` was already correct from shape (j) (its only logical, uuid→`[16]byte`, is `!raw`-gated; its TextUnmarshaler/string/`[]byte` arms are raw-target behavior binary's `deserString` shares). Fix (uniform across the three remaining decoders): thread the suppression `raw` flag into `assignBytes`/`decodeInt`/`decodeLong` and, when set, `return setBytesValue`/`setIntValue`/`setLongValue` BEFORE the logical switch — mirroring binary's raw `deserBytes`/`deserFixed`/`deserInt`/`deserLong`. The DECODER-CHAIN path (`len(decoders) > 0`) was never affected: it decodes into an `any` temp (`toAny=true`, where `raw` is honored) and then `setCustomResult`s into the typed target (shape l) — only the NO-decoder straight-into-target path (shape j) ever reaches the typed-target logical arms, so the complete suppression surface is "(every per-kind decoder × typed-target arm) on the no-decoder path." Lesson: when a fix gates ONE per-kind decoder's typed-target transform on a suppression flag, ALL sibling per-kind decoders with a typed-target transform need the same gate — enumerate by `(per-kind decoder × has-typed-target-logical-arm)`, and remember a both-succeed VALUE divergence (Duration) hides from an error-parity-only check. Pinned by `TestRegression_CustomSuppressionScalarTargetParity` (bytes/fixed decimal·big-decimal·duration AND int/long date·time·timestamp, × string / time.Time / time.Duration / *big.Rat / avro.Duration targets; raw-value + binary↔JSON parity, including the silent-Duration-value case). Pinned by `TestRegression_Custom{Decode,Encode}*BinaryJSONParity`, `TestRegression_CustomEncodeFixedLogicalBaseBytesParity`, `TestRegression_WildcardCustom{Decode,Encode}BinaryJSONParity`, `TestRegression_WildcardEncodeCallbackCountUnionParity`, `TestRegression_JSONDecodeAppliesLogicalMatchesDecode` (the switch↔decodeLogical* cross-check), `TestRegression_CustomNoCallbackSuppressionBinaryJSONParity` (shape f, every logical × both matcher forms), and `TestRegression_CustomPromotionHonorsLogicalSuppression` (shape g, every long-backed logical × all four callback configs, value-level). Add to the yield-map under custom types.

- **Bug density marks where auditors LOOKED, not where bugs ARE (the streetlight effect).** The yield-map and "high-bug-density intersection" framing bias every round toward the same well-lit areas (CustomType×wire-format, unions, defaults, precision, names). That bias is self-reinforcing: the lit areas get more probes → more findings → look even higher-risk → more probes. The corollary the framework kept missing: areas with FEW historical findings may be under-audited, not actually clean — "clean" can mean "unexamined." A round that ONLY deepens the high-density intersection will keep finding shallow variants there while real bugs sit unprobed in the plumbing. Counter-measure: every paranoid round should spend at least one front hunting the INVERSE of bug density — the utility/serialization code the yield-map doesn't list (rabin/fingerprint, varint ENCODE, canonical-form marshal, OCF codec internals, the slab allocator, SchemaFor reflection, embedded-struct field mapping). Concrete record: a round that fanned out three fronts — (1) the highest-density intersection (custom×union×promotion), (2) a structurally-un-fuzzable surface (unsafe struct fast-path, untestable by generative harness because Go can't synthesize named struct types at runtime), and (3) the inverse-density plumbing — found ZERO new bugs on front 1 (the lit area was genuinely hardened by prior rounds) but real bugs on fronts 2 (three panics on valid Go shapes: nil embedded `*struct` encode, unexported embedded `*struct` decode, `omitzero` nil-`*time.Time` IsZero) and 3 (a `Canonical()` escape-then-ReplaceAll corruption on backslash-bearing lax names, AND the second O(n²) in the canonical marshal). The density-derived front was the LEAST productive; the two anti-density fronts paid. Treat "we've never found a bug in X" as a reason to probe X, not a reason to skip it.

- **Structurally-un-fuzzable surfaces need hand-written adversarial enumeration.** Generative/property harnesses can't reach some surfaces by construction — the canonical example is the unsafe struct fast-path, selected per concrete addressable Go struct type, which Go cannot synthesize at runtime, so every type-matrix harness silently falls back to `any`-shaped targets and the unsafe path goes unprobed. When a harness reports "typed targets were a fixed representative set" or "couldn't generate the type," that surface is UNDER-covered, not covered. Counter-measure: hand-write a deliberately-awkward type battery (embedded value/pointer structs, `[N]T` of structs, `**T` fields, unexported embeds, named-string-over-fixed, omitzero×nullunion, text-method types) and force BOTH dispatch arms (addressable vs non-addressable to split unsafe vs reflect). The invariant: unsafe path ≡ reflect path byte-for-byte AND neither panics on any valid Go shape.

- **The yield-map itself.** If a category isn't in the yield-map, it isn't getting attention proportional to its risk. When a finding lands in a category not in the yield-map, append the category here AND add it to the yield-map (or amend the existing entry to subsume it). Otherwise the same blind spot regenerates next round.

- **Two-mechanism recursion-depth accounting must charge each schema edge once on BOTH mechanisms.** The `errTooDeep`/`maxDepth` bound is enforced by TWO different mechanisms that must stay in lockstep: the encoder threads a `depth int` PARAMETER incremented at each recursive call site (`fn(..., depth+1)`), while the decoder/JSON-decoder carry a STATEFUL `sl.depth` that is bumped on container/record NODE ENTRY (`sl.depth++; defer sl.depth--`). The invariant — one increment per parent→child schema edge, identical on every path — is easy to violate at the seams where a node has more than one entry function: a reflect body that dispatches to an unsafe fast body (`serRecord.ser`→`serRecordFast`, `deserRecord.deser`→`deserRecordFast`), or a compiled field fn that re-enters the record via a `*Via` helper (`tryCompileFieldSer`'s record/pointer arms → `serRecordVia`). Each such seam is a place where the edge can be counted twice (dispatch hop +1 AND the body's own +1/bump) or zero times. Symptom: a recursive schema trips `errTooDeep` at a DIFFERENT depth on one path than another — `min(encode,decode)` round-trips break (encode rejects what decode produced, or vice versa), and the effective bound silently halves (or thirds) for the affected shape. Structurally invisible to: round-trip tests (they feed decode only the depth encode produced, masking a decode-accepts-deeper gap), and to any oracle whose shapes don't reach the seam. The directly-nested struct-record edge (a non-pointer struct field mapped to a record, no intervening union/array/map) is the seam the container/union oracle structurally misses — and the unsafe struct-fast encode path double-counted it while the reflect and decode paths counted once. A SECOND drift family lives at the **container-of-union / array-element-union seam**: a union schema node interposed between a container and its recursive child (`array<["null",Self]>`, `map<["null",Self]>`), or a container reached through a nullable field union (`["null", array<Self>]`, `["null", map<Self>]`). The union is its OWN node and must cost one depth unit, but the encode-side null-union helpers got it wrong two ways: (1) the array-element fast paths (`usArrayNullUnionRecord` / `usArrayNullUnionPtr`, unsafe.go) entered the inner record/primitive STRAIGHT from the array's depth (`depth+1`), collapsing array→union→record to array→record and SKIPPING the union node entirely — binary encode accepted ~1.5× the depth its own decoder (which charges the union via `udNullUnionRecord`) could read, a literal encode-produces-unreadable-wire break; (2) the 2-branch null-union encode optimizers (`serNullUnionAt` ser.go, `usNullUnionPtr` / `usNullUnionRecord` unsafe.go) CHARGED the union edge (entered the branch at `depth+1`) but OMITTED the union node's own `if depth >= maxDepth` guard, relying on the child's guard — a fence-post that tripped one level DEEPER than decode's `deserNullUnionAt` (which both bumps AND guards at the union node). Fix: every encode-side null-union helper now mirrors `deserNullUnionAt` — guard at the union node AND charge its edge; the array-element paths guard the per-element union (loop-invariant, hoisted before the loop) and enter the inner at `depth+2`. The map carrier was already uniform (maps have no unsafe path; the reflect `serMap` serItem is `serNullUnionAt`, fixed by the same guard); multibranch unions (`array<["null","int",Self]>`) were already uniform (they route through the guarded general `serUnion.ser`). **Probe**: build a depth-uniformity oracle that hand-assembles wire INDEPENDENT of the encoder (so each direction's true trip depth is observed, not `min`), and run EVERY recursive shape × EVERY path (encode, typed/any decode, JSON encode/decode, resolved decode) asserting all trip at the SAME depth — see `TestDepthUniformityOracle` / `TestDepthUniformityMutual`. The oracle now spans the full recursion-carrier cross product, including the seam shapes: `array-of-nullunion` (`[]*N`, unsafe) + `array-of-nullunion-reflect` (`[]**N`, reflect), `array-of-nullsecond-union` (`[T,"null"]` ordering), `map-of-nullunion`, `field-nullunion-of-array`, `field-nullunion-of-map`, `array-of-multibranch-union`, and the nested combos `array-of-map-of-nullunion` / `map-of-array-of-nullunion`. For the directly-nested struct-record seam that static Go types can't express recursively, build deep distinct-named nesting with `reflect.StructOf` and probe a depth above the half-budget collapse point (`TestDepthUniformityNestedStructRecord`). Cyclic-value safety on every container carrier (each must trip `errTooDeep`, never OOM/infinite-loop, on binary AND JSON encode, plus a `map[string]any` value-graph self-reference) is pinned by `TestDepthBoundCyclicContainers`. When a fix touches any increment site, sweep every `depth+1` / `sl.depth++` / `depth >= maxDepth` across ser.go/deser.go/unsafe.go/json_codec.go/json_decode.go/resolve.go/skip.go and classify each as the node's sole entry (counts once) or a dispatch hop into a body that also counts (must NOT add its own +1/bump). Two recurring drift shapes: a node reached by both a reflect entry that bumps and a fast body that bumps (the fast body must defer to the entry's bump), AND a null-union/container-of-union helper that charges the edge but omits its node guard, or skips the union node altogether (encode must mirror the decode side — guard AND charge at the union node).

## Findings that don't count

Reminder: the §"Before changing behavior — required pre-action gate" section at the top of this document is the imperative form. Run it before proposing anything. The bullets here describe what gets rejected after the fact — but the work to *avoid* the rejection is the pre-action gate.

These submissions don't qualify as findings:

- **"This area looks suspicious"** — without a concrete failing test, it's not a bug.
- **"Could be more efficient"** — perf regressions count only with benchstat showing the regression.
- **"This doesn't match the spec letter"** — where Java and fastavro agree on the implementation behavior even on spec-ambiguous points, the implementation IS the spec for interop purposes; what's deployed is the contract.
- **"Convenience" form for spec-defined output** — the spec sometimes mandates non-intuitive output (e.g. decimal-on-bytes JSON encoding as a codepoint-mapped string `"!"` rather than a number `0.33`). "It would be more readable to emit a number" is not a finding — that's a deliberate spec violation, the kind the audit targets in the REVERSE direction (places that emit the convenience form when Java/spec mandates the structural form).
- **Re-finding something already pinned by a `TestRegression_*` test** — UNLESS the pin is itself the bug (pattern 13). The grep `TestRegression_` is the pre-report check; with 110+ regression tests already pinning specific bugs, the not-a-finding case is common. The exception is pattern 12: a pinned rejection that violates round-trip parity IS the finding (quote the pin and propose its replacement).
- **Re-litigating documented intentional behavior** — *this is what the §"Before changing behavior" pre-action gate exists to prevent.* If the gate's verdict for your proposed change is "documented as intentional," the change does not qualify as a finding regardless of how clean the asymmetry-fix looks. A genuine policy challenge requires concrete new evidence (deployed Java/fastavro producer that emits data twmb can't read, a real-world user reporting wire-format divergence) that the documented rationale doesn't address — surfaced as a Suspected/Finding asking the user whether to revisit the policy, not as a unilateral code change. Conformance-test pins with comments containing `"Documenting"`, `"Intentional"`, `"Asymmetry: X (intentional)"`, `"Pre-fix this …; post-fix the two are now uniform"`, or names referencing this document's §"Known intentional divergences" are the in-test form of documented intentional. Same treatment.
- **Encode/decode "asymmetry → fix" reflex on json.Number for documented-lenient cases.** Encode-side leniency is documented for whole-number floats against int/long and for float/double's lossy-by-destination behavior (see the named entries in §Known intentional divergences). Don't propose tightening those. The schema-contract design for json.Number content (numeric schemas validate via parse-back, stringy schemas reject json.Number on both sides, map keys validate per-key on both sides) is also documented — proposed changes there should quote the `json.Number content is validated against the schema's contract` entry and explain what new evidence motivates the change.
- **Tests that don't compile.** `go vet` is the gate; drafts that fail it are not submittable.
- **Symptom fix that introduces a new asymmetry.** When the obvious fix is to relax a strict check in one path so a specific input passes, the implicit question is: does this also change behavior for other inputs the strict check was deliberately rejecting? Recent instance: relaxing `parseUUID` to fall through on rejection fixed the default-fill symptom but silently accepted 16-char non-UUID runtime strings, breaking binary↔JSON parity in a different way. Architectural (root-cause) fixes beat local relaxations, even when the local relaxation is mechanically smaller — the audit framework's bias toward finding bugs requires the suggested-fix-sketch's bias to be toward fixes that don't create them.

## Re-auditing the patched function after every fix

The detailed playbook lives in FIX.md. The premise: once a fix is in place, the function in its new shape is itself audit territory. Patterns 14a (a recent fix that covered the reported input but not the helper's full set), 15 (a stale dispatcher skip widened by a sibling fix), and 16 (a precision fix that swapped a bounded check for an O(n²) helper without bounding the new caller) all arose this way — each was created by a prior fix that wasn't audited against its post-patch shape.

The high-level checks the patched function compares against:

- its fast/slow twin (the 2-branch optimization, the unsafe-pointer struct fast-path, the per-primitive container specialization),
- its JSON/binary counterpart (`serFoo` ↔ `appendAvroJSON` case, `deserFoo` ↔ `decodeFoo`/`assignFoo`),
- the helper docstring that the fix says it "mirrors / matches" (does the fix cover *every* shape the helper handles, or just the bug report's shape? — pattern 14a),
- the dispatchers that route inputs to the patched per-branch handler (do any `continue` / skip predicates still pre-filter inputs the patched handler would now accept? — pattern 15),
- **the cost of the new code path on a hostile 1 MiB input** (does the precision fix's new helper have a length cap before the O(n²) operation? — pattern 16). Required: time a 1 MiB hostile input through each entry point the fix touched and confirm < 100ms rejection.
- **the schema-parse-time validation arm for the same input** (does `avro.Parse({type:record,...,default:<the-newly-accepted-input>})` accept? — pattern 1b three-axis rule). When the encode-side fix newly accepts e.g. `json.Number("1e1000")` against `"double"`, the equivalent schema with `"default":1e1000` must parse. The pattern 1b corollary: every encode-arm predicate change needs the corresponding schema-parse-time arm probed, in both reject and accept directions.
- **the metadata-API observability arm for the same input** (does `s.Root().Fields[0].Default.(<expected-go-type>) == <value>` hold? — pattern 1b four-axis rule). The metadata-API surfaces (`Schema.Root().Props`, `Fields[].Default`, `Fields[].Props`, `CustomType` callback `*SchemaNode.Props`) route through `normalizeJSONNumber` (`schema.go`) — a SEPARATE parser callsite from the encode/decode/schema-parse-validate arms. When the encode-side fix newly accepts e.g. `json.Number("1e1000")` and produces +Inf wire bytes, the equivalent schema's `Default` must surface as `float64(+Inf)` (not `json.Number("1e1000")`). Type-assert against the documented contract type, NOT just compare values — a value comparison hides the type-mismatch bug pattern 13b warns about. If the fix touches a stdlib parser, AND any metadata API exposes that parser's output, BOTH axes need updating.

Findings surfaced by re-audit ship in the same round; the loop converges only when the fix and its consequences are both clean.

## Scope — public-API entry-point coverage

The top-of-document "Scope — read this first" section is the operational rule: whole codebase, never a branch or diff. This section enumerates the public-API entry points that get a walk every round, regardless of when (or whether) they were last touched. The list is the *minimum* coverage — anything else in the codebase is equally in scope.

**Recency is not a signal.** A function untouched since 2022 is exactly as likely to harbor a bug as one rewritten last week. The 2022-era code has had more rounds of "looks fine" sweeps than the recent code — and "looks fine" is precisely what hides the structurally-invisible bug. Don't let antiquity become camouflage. Don't let recency become a magnet.

Public-API entry points each get a walk regardless of when last touched:

- **Decoding**: `Schema.Decode`, `Schema.DecodeJSON`, `Schema.DecodeSingleObject`. Safe (reflect) and unsafe (struct fast-path) variants for each Go target type.
- **Encoding**: `Schema.Encode`, `Schema.AppendEncode`, `Schema.EncodeJSON`, `Schema.AppendEncodeJSON`, `Schema.EncodeSingleObject`, `Schema.AppendSingleObject`. Same safe/unsafe split.
- **Schema parsing**: `Parse`, `MustParse`, `SchemaCache`, `SchemaFor`, options (`WithLaxNames`, `WithCustomTypes`, etc.).
- **Schema introspection**: `Schema.Root`, `Schema.Canonical`, `Schema.Fingerprint`, `Schema.JSON`, `Schema.String`.
- **Schema resolution / compatibility**: `Resolve`, `CheckCompatibility`.
- **OCF**: `ocf.NewWriter`, `ocf.NewReader`, codec selection (snappy, deflate, zstd), `WithMaxBlockBytes`, `WithCodec`, `WithReaderSchema`, `WithReaderSchemaFunc`, `WithSchemaOpts`.
- **Single Object Encoding**: magic-byte framing, fingerprint endianness.
- **Custom types**: `CustomType`, `WithCustomTypes`, all combinations of nested positions.
- **Logical types**: `decimal`, `uuid`, `date`, `time-millis`, `time-micros`, `timestamp-millis/micros/nanos`, `local-timestamp-*`, `duration`. Both regular and via custom types.
- **Decoder options**: `TaggedUnions`, `TagLogicalTypes`, `LinkedinFloats`.

## Yield map (where bugs tend to cluster)

Updated based on where past audits found bugs. Hints, not boundaries — the audit covers the entire codebase, so anything not listed here is a candidate too. The clustering history:

**High yield:**
- **Encode-decode target-type parity (pattern 12 / 13).** The single most productive audit angle. For every encoder's accepted-type set (the `case v.Type() == X` / `case v.Kind() == X` arms), grep the corresponding decoder for the parallel arm. If missing, write a round-trip test through `s.Encode(in)` + `s.Decode(&out)` where `out` has the same shape as `in`. Same for `EncodeJSON`/`DecodeJSON`. Same for unsafe-path `usFoo`/`udFoo`. Also: read existing `TestRegression_*Reject*` and `TestErrorPaths` pins critically — a pinned rejection that violates round-trip parity is itself the bug.
- **CustomType × wire-format parity.** The largest single finding cluster in audit history — thirteen distinct shapes, (a)–(m) in the structural-blind-spots entry "CustomType logical-codec suppression conditions must be mirrored…". Enumerate by (build site × direction × wire format × logical kind × decode-target shape), never by the one shape a report exercised. Recurring sub-traps: suppression gated on a runtime proxy instead of the threaded predicate (wildcards differ between them); a per-kind JSON decoder honoring suppression in its decode-into-`any` branch but not its TYPED-target arms; finalize/forward-ref fixups dropping the custom wrap; cache-shared nodes baking custom effects across Parses; resolution/promotion re-applying a suppressed logical. Probes asserting a callback fires must use value-transforming callbacks (§Audit conventions item 2).
- **Encode-side correctness.** Encode-side validation has historically been thinner than decode-side (overflow checks, range bounds, type coercion). Walk every encode path. A finding: the UUID-fixed JSON encode arm hard-failed on the default's codepoint-string form because `parseUUID` required hex-dash — a logical-type-arm bug that the binary path didn't have because its default path skipped the strict parser entirely.
- **Auto-fill-defaults during EncodeJSON / DecodeJSON.** twmb-unique feature: when a record is encoded/decoded against JSON and a field is absent, the schema's default value materializes. Defaults are stored in their parsed JSON form (codepoint-mapped string for fixed, JSON-Number for int/long, etc.). Each logical-type-aware encode/decode arm must accept the stored form. Audit every arm in `appendAvroJSON` (json_codec.go) and `decodeKind`-equivalent (json_decode.go) for whether it handles the parsed-default's actual Go type.
- **Default-value pipeline.** The full chain is `unmarshalDefault` (parse JSON) → `coerceDefault` (float-from-string) → `validateDefault` (structural check + recursive coerce) → `convertDefaultBytes` (bytes/fixed string → []byte for JSON-encoder parity) → `encodeDefault` (binary pre-encode) → both encoders' default-fill paths (`appendAvroJSON` + `serRecord.ser`). Bugs hide between sites — e.g. validateDefault accepts but convertDefaultBytes mis-routes; coerceDefault doesn't recurse through name-refs but encodeDefault works on strings anyway. Audit each transition for "what value shape does the next site expect?" and confirm every named-type reference (forward or backward) is followed all the way through.
- **Name-reference resolution (`aschema` canon vs `schemaNode`).** A field's parsed canonical form is `aschema{primitive: "MyType"}` for a name-ref to a record/enum/fixed. Code that walks the aschema's `object` / `union` / `primitive` will fall through silently for name-refs (no case for arbitrary names in any primitive-typed switch). Code that walks the resolved `*schemaNode` follows name-refs naturally because the builder pre-resolved them. Any function consuming default values should walk `schemaNode`, not aschema. Past audits repeatedly found code that walked aschema and silently mis-handled name-refs (validateDefault, convertDefaultBytes, coerceDefault).
- **Resolution edge cases.** Promotion chains, alias resolution, missing-field-with-complex-default, two-pass selection rules.
- **Stdlib API call sites — both silent-fail and amplification.** Greppable: `SetFloat64`, `SetString` (pattern 9), `(*big.Int).Exp`, `json.Unmarshal`, `reflect.MakeSlice`, `time.Unix*`, `strconv.Parse*`, `big.Int.Div` vs `Quo`. Each is a candidate for either the "stdlib returns nil/zero on bad input, caller doesn't check" (pattern 1) or "stdlib materializes attacker-controlled magnitude from compact input" (pattern 9) shapes. For each, ask: "what's the largest big.Int / slice / map / Duration this could produce from N bytes of attacker-controlled input?" **The grep is a candidate-generator, not a sufficiency check.** Don't stop at "I ran the grep, every hit looks fine." A hit looks fine until you apply the structural question from pattern 1 / pattern 9 to it. The 80th-audit json-Unmarshal finding came from a hit that *was* in the grep but slipped because no auditor asked the precision question of the specific site.

- **User-facing metadata APIs (no encode/decode parity counterpart).** `Schema.Root()`, `Schema.Canonical()`, `Schema.Fingerprint()`, `Schema.String()`, `SchemaNode.Props` / `SchemaField.Default` / `SchemaField.Props` reads, `CustomType` callback receivers. These surfaces have no wire-format parity counterpart, so they're invisible to the encode/decode-parity sweeps that dominate the audit. They have their own correctness contract — what types come out, what precision survives, what custom properties round-trip — and that contract is testable independently. Boundary-value-probe each numeric path (see pattern 1's boundary list); type-pin-audit each (pattern 13b); doc-string-claim-audit each (hard rule 5). The precision-loss bug landed in this category and was structurally invisible to encode/decode-focused sweeps.

- **Error-construction sites — every `fmt.Errorf("…%q…", x)` where x is user-controllable.** Distinct from the parser-DoS sweeps (patterns 9, 16) that target rejection-CPU-time: the rejection is fine but the *error message* is hostile-size. `truncForError` / `truncBytesForError` / `truncValueForError` exist precisely for this concern; bypass is a real DoS (10 MiB hostile JSON.Number → 10 MiB error message → 1:1 log/RPC/metric-label amplification). The grep target is in §DRY; see the structural-blind-spots entry "Error messages echo unbounded user-controllable input" for the classification rules. ~15 sites fixed in the round that codified this entry; the angle generalizes to every future fix that adds a new error path. The `ocf` package has its OWN `truncForError` (the root helper is unexported); the OCF write-side metadata-key echoes (`NewWriter`'s reserved-key and over-cap-key errors) bypassed it while the read-side codec-name error already used it — a caller-supplied `WithMetadata` key is wire-equivalent user input, so every package's error sweep must cover BOTH directions (read AND write) and BOTH packages. **The sweep is per-FILE within a package, and per-ARM within a function — not just per-package.** `compat.go` used `truncForError` zero times while its cross-path twin `resolve.go` used it five (both build the same `CompatibilityError` from the same user names); and INSIDE one function `resolveEnum`, the enum-default echo (`r.enumDef`) was wrapped while the enum-symbol echo (`ws`) two lines away was not. A whole-file grep of `truncForError` *count* per file is the cheap tell — a cold-path file with zero uses next to a sibling with many is the smell. **Rendering-truncation vs construction-truncation is a real distinction for composed messages.** A single-value field (`CompatibilityError.Path` / `ReaderType` / `WriterType`) can be bounded once at the RENDER point (`Error()`), which covers every construction site across both files AND preserves the public field's full value for callers that inspect the struct. A COMPOSED field (`Detail`, any `fmt.Sprintf("…%q…", name)` sentence) must NOT be blanket-truncated at render (it would chop the sentence) — its embedded user values are truncated at CONSTRUCTION instead. So the two layers coexist: render-truncate the single-name fields, construction-truncate the names embedded in composed sentences.

- **Error SHAPE/identity parity across wire formats (distinct from error SIZE).** The size axis above is "the error is correct but too big." This axis is "the error has the wrong TYPE / missing structured field." `doc.go`'s "# Errors" contract is testable: a type mismatch must be `errors.As`-able to `*SemanticError` with a dotted record-field path, identically on binary AND JSON, encode AND decode. The binary paths wrap every record field via `recordFieldError`; the JSON paths historically did not (encode returned bare `fmt.Errorf`; decode put the field name in message text, leaving `SemanticError.Field` empty). The sweep: for each wire format × direction, feed a wrong-typed value at a nested record field and assert `errors.As(&se)` true and `se.Field` carries the path — the four paths must agree. Structurally invisible to wire-byte parity (errors have no wire bytes), value round-trips (they assert success), and the foreign oracles (they compare accepted values). Net: `TestMatrix_JSONEncodeErrorSemanticParity`.

- **Schema-parse-time validation paths (third axis beyond encode and decode).** `defaultAsFloat64`, `defaultAsInt32`, `defaultAsInt64`, `validateDefault`, `coerceDefault`, `convertDefaultBytes`, `encodeDefault`. These run at `avro.Parse` time when a schema has a record field default. They call many of the same stdlib parsers (`strconv.ParseFloat`, `strconv.ParseInt`, `json.Number.Float64`, `big.Rat.SetString`) as the encode/decode arms — but on the JSON-encoded `"default":` literal, not on runtime wire bytes or runtime Go values. Two structural traps:
  1. A fix at the encode/decode arm naming "now matches" leaves the schema-parse-time arm diverging. The arm rejects schemas whose field defaults equal an input the encoder newly accepts. The audit owes a `avro.Parse({type:record,...,default:<input>})` probe for every encode-fix's newly-accepted input.
  2. The schema-parse-time arm is the only path that runs at config-load time; bugs here surface as "schema parse fails on the new behavior the docs promised" rather than as wire-format issues. Users notice the regression at deploy time, not at data-corruption time.

  A finding: a prior fix fixed encode-side ParseFloat ErrRange-with-Inf acceptance; three schema-parse-time sites (schema.go/:2511/:2567) silently kept rejecting the same input. Java's `Schema.parseField` accepts via `Double.parseDouble("1e1000") → POSITIVE_INFINITY`; fastavro's `_default_matches_schema` accepts via `float("1e1000") → inf`. Both upstream impls accept the parse; twmb only the encode. See pattern 1b's three-axis-rule corollary.

**Medium yield:**
- **OCF** — sync-marker collisions in compressed data, codec selection, truncated files, concurrent writer. **Decompression amplification is a distinct DoS axis from the compressed-size cap:** `WithMaxBlockBytes` bounds the COMPRESSED block read off the wire, but each codec's `Decompress` allocates the DECOMPRESSED size from a length declared inside the payload (snappy `DecodedLen` pre-allocates ~4 GiB from a few bytes; deflate's `io.ReadAll(flate.NewReader)` is unbounded; zstd default permits multi-GiB) — so an ~89-byte block can demand 200 MiB. Bounded by `WithMaxDecompressedBlockBytes` (default 64 MiB) enforced INSIDE each built-in codec (the alloc must be prevented, not caught after) plus a post-`Decompress` length backstop for custom codecs. Sibling axis: the BLOCK COUNT drives the decode loop — `count ≤ len(block)+zeroSlack` lets a garbage-padded or large-decompressed block run hundreds of millions of zero-byte-record iterations; bound it with a per-block consecutive-zero-consumption cap (`Decode` tracks `len(rest)==len(block)`), the dynamic schema-agnostic form of `maxZeroByteItems`. Pinned by `TestRegression_OCFDecompressionAmplificationBounded`. The general lesson: any "read compressed, then inflate" or "read count, then loop" boundary has TWO limits to set — the wire-side and the materialized-side — and a cap on one is silently not a cap on the other.
- **JSON edge cases** — surrogate pairs, duplicate keys, U+2028/U+2029, BOM handling, numbers > 2^53. The hand-rolled scanner (`json_scan.go`) is not `encoding/json`; differential-test `s.DecodeJSON(in,&any)` vs `json.Unmarshal(in,&any)` across a malformed-input battery and triage every divergence (see the structural blind spot "A hand-rolled parser that replaced a stdlib parser silently dropped the stdlib's rejections").
- **Logical types in unusual positions** — as union branches, as record default values, custom-overridden, in nested arrays/maps.
- **Dispatch tables** — any switch on a wire byte / token / kind that decides which inner function runs. A missing case can mean valid input gets rejected or wrong-typed input gets silently accepted.

- **Superlinear cost in the parse/unmarshal pass itself, separate from the values it produces.** The encode/decode-parity and stdlib-call sweeps target the VALUE pipeline; the cost of PARSING the schema (or any container framing) before values exist is a distinct axis. The structural question: for a parse step that recurses per nesting level, does any per-level operation re-scan content proportional to the REMAINING input (not just the current level)? If so the parse is O(depth × size) = O(n²) over nesting, and a result-size/semantic guard (e.g. a maxDepth that fires during the BUILD that consumes the parsed tree) does NOT bound it — the quadratic ran during the earlier unmarshal. Concrete instance: `aschema.UnmarshalJSON`'s object case did a second full `json.Unmarshal(data, &raw)` into `map[string]json.RawMessage` at every level to capture extra properties, re-scanning each node's entire subtree; a ~250 KB deeply-nested schema took ~22 s to parse-then-reject (the build-time `maxDepth` guard fires only after the unmarshal completes). The practical ceiling was `encoding/json`'s own 10000-deep nesting cap (verified: it rejects depth 10001 with "exceeded max depth"), so the worst case was tens of seconds, not unbounded — but the angle generalizes to any hand-rolled or stdlib parse with a per-level full-subtree scan. Backstop fix (`checkSchemaNestingDepth`, schema.go): a single O(input) bracket-depth pre-scan rejecting input past `maxSchemaJSONDepth` (4×maxDepth) BEFORE the build — but the prior claim that this made "parse cost independent of nesting depth" was FALSE: the pre-scan caps the *unbounded* case (depth→json's 10000 limit) but within the cap parse was still O(depth²), so a build-*acceptable* 999-deep schema (24 KB) burned ~0.4 s, and a hostile sub-cap one ~3.5 s. The real fix was the tight O(n) one: replace the per-node `json.Unmarshaler` (whose stdlib `d.skip` re-scans each subtree to delimit it) with a SINGLE generic decode + a pure-Go tree walk (`parseSchemaTree`, schema_parse.go). **Two structural lessons.** (1) **A nested-custom-method quadratic has a MIRROR in the opposite direction via the same mechanism.** Fixing the unmarshal exposed a SECOND O(n²): `aobject`/`aschema` `MarshalJSON` (used only by `Canonical()`, which `Parse` calls for the SOE fingerprint) had each level return its full subtree bytes that the parent then COPIED into its buffer — O(depth×size) on the marshal side. Both custom `Unmarshaler` (d.skip re-scan) and nested custom `Marshaler` (return-and-copy-up) are inherently O(n²) over nesting; a single-pass writer to ONE shared buffer (`canonicalBytes`, schema_canonical.go) is the cure for the marshal half. When you kill a parse quadratic, immediately time the SERIALIZE path on the same depth matrix — the symmetric one is usually right behind it. (2) **The escape-then-string-replace round trip is unsound.** The old `Canonical()` HTML-escaped via `json.Marshal` then `bytes.ReplaceAll`-un-escaped `<`/`>`/`&`/U+2028/U+2029 — but the 6-byte `\uXXXX` target appears INSIDE the `\\uXXXX` escape of a name containing a literal backslash, so ReplaceAll collapsed it to invalid JSON and a corrupt fingerprint (reachable via `WithLaxNames`). The single-pass writer emits raw UTF-8 directly (PCF [STRINGS]), so no un-escape is needed — killing the corruption and the quadratic together. The angle to sweep: time-box `Parse`, `Canonical`, `DecodeJSON`/`Decode` on a depth-vs-width matrix (a deep chain and a wide-but-shallow blob of equal byte size) — a large gap localizes a depth-quadratic on whichever path. Pinned by `TestRegression_DeepSchemaNestingRejectedInBoundedTime`, `TestRegression_DeepValidSchemaParsesLinear`, `TestRegression_CanonicalBackslashNameValid`.

**Lower yield (extensively reviewed):**
- Recursion depth tracking — every branching path bumps depth.
- Unsafe pointer arithmetic — well-trodden, multiple rounds.

**Narrow-before-check (platform-width truncation defeats the guard).** A range/bounds check is sound only if the narrowing to `int` happens AFTER it, not before. `n := int(v.Uint()); if n >= len(x)` truncates a carrier ≥ 2^32 to its low 32 bits on a 32-bit build (`int`==int32) *before* the comparison, so an out-of-range value wraps into range and silently passes — while the same program on 64-bit rejects it (a platform-dependent silent-wrong-output divergence, not a crash). The codebase principle is "compare in the carrier's own width first" (length narrowings already do this); the trap is every OTHER `int(v.Uint())`/`int(v.Int())` that precedes its own guard. Concrete instance: `serEnum.ser` + JSON enum encode did `int(v.Uint())` then range-checked, so `uint64(1<<32+5)` against a 10-symbol enum encoded ordinal 5 on 32-bit; fixed via `enumOrdinalIndex` comparing in int64/uint64 first. Grep: `grep -nE 'int\((v|val|[a-z]+)\.(U?int)\(\)\)' *.go` — for each hit, is the `< len`/`< cap`/range check on the post-narrow `int`? If so the guard is platform-dependent. Probe on 64-bit via the error-message proxy (truncated/sign-wrapped value shows in the reject message) or `GOARCH=386 go build`.

**The trap is not limited to 32-bit `int` truncation — it generalizes to any numeric conversion whose overflow behavior the Go spec leaves *implementation-defined*, used as or before a bound check. The float→integer conversion is the second member of this class.** Go's float→`int64`/`uint64` conversion on overflow is implementation-dependent (spec, *Conversions*: "if the result type cannot represent the value the conversion succeeds but the result value is implementation-dependent") — arm64 saturates (`FCVTZS` → `MaxInt64`/`MinInt64`), amd64 wraps to the indefinite integer (`0x8000000000000000`). So a guard that consults the *converted* value is platform-dependent for inputs outside the destination's range, observable on a 64-bit arm64 host with **no 32-bit build required**. Two concrete shapes: (a) a round-trip whole-number check `f != float64(int64(f))` *passes* on arm64 for `f == 2^63` (saturates to `MaxInt64`, which rounds back up to `2^63 == f`) but fails on amd64; (b) `n := int64(f)` then `v.OverflowInt(n)` checks the post-conversion `n`, so a saturated `n` is already in-range and the guard is a no-op. Both must bound `f` in **float space before converting** — `if f < -(1<<63) || f >= (1<<63)` for int64, `if f < 0 || f >= (1<<64)` for uint64 (the constant converts to the exact power-of-two float64) — exactly as the encode-side `floatFitsInt64`/`floatFitsInt32` already do (they range-check `math.Trunc(f)` against `1<<63` *before* the `int64(...)` cast). Concrete instance: `setFloatValue`'s integer-target arm (deser.go, the decode reverse of the whole-number-float-encodes-as-int leniency) silently decoded the wire double/float `2^63` into `int64(2^63-1)` on arm64 — an off-by-one corruption of an out-of-int64-range value that the encode side rejects on every platform. This is the **same structural class** as the enum `int(v.Uint())` fix and survived that round's sibling sweep purely because the grep `int\((v|val|…)\.(U?int)\(\)\)` does not match `int64(f)` / `float64(int64(f))`. Extra grep targets for the float→int sub-case: `grep -nE 'float64\(int(64|32)\(|int64\(.*[fF]\)|uint64\(.*[fF]\)' *.go`, plus inspect every `\.Float\(\)` (and `math.Float(32|64)frombits`) whose result reaches an `int*(...)` cast or an `OverflowInt`/`OverflowUint` guard. **Lesson: the sibling sweep for any "narrow/convert before check" fix must run BOTH grep patterns — int→int (`int(v.Uint())`) AND float→int (`int64(f)`) — because they are the same bug class under two different conversion idioms, and a grep on one idiom is structurally blind to the other.**

**A THIRD member: intermediate ARITHMETIC overflows the platform int before the comparison that consumes it.** Not a conversion at all — a plain multiply/add whose result wraps `int` before a `>`/`<` bound check reads it. Concrete instance: `maxDecimalDigits` computed `bits := 8*size - 1` then derived a digit capacity from it; a fixed `size` is a Go `int` that can exceed 2^60 on a 64-bit build (twmb accepts fixed sizes Java's int32 size field cannot represent), so `8*size` wrapped the platform int negative, the capacity came back negative, and a precision-1 decimal was FALSELY REJECTED while the same fixed without the decimal logical parsed. The defect is reject-only (a real reject needs a tiny size where no overflow occurs) and platform-divergent (the wrap point is 2^29 on 32-bit, 2^61 on 64-bit). Fix: clamp `size` before the multiply, exploiting that any size past the precision ceiling (`decimalScaleLimit`) yields a capacity that can never be exceeded — so the exact digit count is irrelevant there. Grep extension for the whole class: also scan `grep -nE '[0-9]+\s*\*\s*[a-z]' *.go` for a small-constant multiply of a wire-/schema-controlled `int` feeding a comparison; ask "what is the largest legitimate operand, and does the product overflow `int` before the compare?" The three idioms — `int(v.Uint())`, `int64(f)`, and `k*size` — are the same "the guard reads a value the platform already corrupted" class under three different arithmetic surfaces; a grep on any one is blind to the other two.

## Finding format

**Do NOT write an audit-report markdown file (e.g. `AUDIT_REPORT.md`, `ROUND_85.md`).** Findings go in the conversation reply, not to disk. The maintainer reads the findings in the chat, decides which to apply, and the fix lands as code + in-repo regression tests in the appropriate `_test.go` file — not as a separate `.md` artifact. If the audit produces zero findings, say so in the reply; don't leave a trail of report files. (The exception is BUG_AUDIT.md / FIX.md themselves, which are the *framework* documents, updated when a new lesson or playbook step needs codifying.)

Findings follow this template:

```
Finding: <one-line title>
File:line: <path>:<lineno>
Severity: correctness | spec violation | DoS | parity divergence | interop divergence
Why it's wrong: <2-5 sentences>

Broken code (verbatim from disk):
```go
// <path>:<lineno-start>-<lineno-end>
<the actual lines as they currently sit on disk, with line numbers preserved
where useful. Annotate the offending line(s) with a ← marker or short comment
so the reader sees WHICH lines produce the bug, not just the function
containing them.>
```

Failing test (verified failing against the codebase as it currently sits on disk):
```go
func TestRegression_<short_name>(t *testing.T) {
    // ... ran via /tmp/avro_audit_verify, output captured below ...
}
```

Verification output (the actual failure pasted):
```
--- FAIL: TestRegression_<short_name> (0.00s)
    avro_test.go:NN: ...
```

Validation against apache/avro:main:
- Spec section: <heading + quoted text>
- Java: <file:line + snippet>
- fastavro / goavro: <file:line + snippet OR "no support, irrelevant">

Suggested fix sketch: <1-3 sentences>
```

**The "Broken code" block is required, not optional.** A `file:line` pointer alone forces the maintainer to open the file to see what's actually wrong; an audit that has read the code and is asking the maintainer to act on it should show the code. Quote the lines verbatim — do not paraphrase, do not elide, do not summarize "the function does X". If the broken behavior spans multiple non-adjacent sites (e.g. four dispatcher arms diverging on one rule), show each broken excerpt with its own `file:line` header so the maintainer can see the divergence side-by-side. For "missing code" bugs (a case that should exist in a switch, a check that should run before an operation), show the switch/function as it sits AND annotate with `// ← missing: <what>` at the spot the new code belongs. For policy-call findings ("could be a feature, could be a bug, maintainer decides"), show the code anyway — the reader needs to see what they're deciding about.

**Every finding also requires a "User-visible breakage" block** — a runnable Go program (or near-runnable snippet, with `import "github.com/twmb/avro"`) showing what a USER of the package writes that doesn't work. NOT internal `serFoo`/`deserFoo` machinery — what the caller types at their keyboard. Format:

```
User-visible breakage (what a caller of the package writes that doesn't work):
```go
package main

import (
    "fmt"
    "github.com/twmb/avro"
)

func main() {
    // <minimal program a user would write>
    s, _ := avro.Parse(`...`)
    // <use the API the way a user would>
    fmt.Printf("%T %v\n", got, got)
}
```

Expected (what Java / fastavro produce, or what the doc-string promises): <one line>
Actual (what twmb produces today):                                       <one line>
```

The contract: a maintainer should be able to read the User-visible breakage block alone, without scrolling to the internal-code block, and immediately understand "this is something a user of my package would file an issue about." If the only thing you can write for User-visible breakage is "the internal `deserFoo` arm doesn't accept `time.Duration` as a target," the finding either (a) is actually a user-facing bug — find the public API call that surfaces it (probably `s.Decode(buf, &x)` for some user-typed `x`), or (b) is internal cleanup not a finding. Reflect-only / fast-path-only / unsafe-pointer-only internal divergences that no public API exposes are out of scope.

Suspected (no failing test):

```
Suspected: <one-line title>
File:line: <path>:<lineno>
Broken code (verbatim from disk):
```go
// <path>:<lineno-start>-<lineno-end>
<the suspicious code, annotated with ← markers at the questionable lines>
```
Why suspected: <evidence — what tipped off, what input would prove it>
What input would prove it: <specific>
```

**Suspected → Finding promotion bar is low.** When the "specific input that would prove it" fits in <30 lines of Go, writing the test and running it converts Suspected to Finding. The audit framework gates Findings on a failing test (convention 2) to filter false positives — but the same gate creates a trap: real bugs filed as Suspecteds because writing the test felt like extra work. Default conversion: Suspected→Finding whenever the failing-test friction is low. Reserved for Suspected: cases that genuinely need a maintainer call on policy (e.g. "should this lenient acceptance exist at all?") where no observable behavior question can be settled by a test. Concrete latency cost: a Suspected finding that sat for one audit round, then the next audit confirmed it as a real bug — one round of latency the test would have eliminated.

**A *passing* test that locks an unambiguous observable divergence is Finding-grade, not Suspected.** The subtle case the bar above doesn't cover directly. A test passes, but the *behavior it exercises* diverges from Java/fastavro in a way precisely describable (e.g. "twmb rejects, Java accepts and produces +Inf bits"; "twmb's encoder rejects this input, but twmb's *own decoder* on the corresponding wire bytes already accepts the equivalent symbol — internal route divergence"). The maintainer's choice about *which way to resolve the divergence* (change twmb to match Java, keep current, document as intentional) is what the Finding *asks* — not a precondition for filing. Demotion to Suspected only when the evidence is too thin to write any test at all (e.g. "this grep hit looks suspicious but I haven't traced what input would trigger it"), OR when the question is genuinely lenient-acceptance policy with no observable wire-format consequence either way (e.g. "should we accept this stringly-typed default form at all?"). Audit-83 had this exact shape: probed `s.AppendEncode(json.Number("1e1000"), "double")` vs `s.AppendEncode(math.Inf(1), "double")`, found the route divergence, filed as Suspected on the grounds that "it's a policy question" — but the *internal* asymmetry (encoder rejects an input the matching decoder accepts) is observable, reproducible, and a Java/fastavro divergence, so it was Finding-grade and got fixed the same round when reframed.

## Verified format (the clean case)

Each cleanly-checked area:

```
Verified: <area>
Checked:
  - <specific function or file:line>
  - <specific input or test scenario>
  - <comparison against Java/spec>
Conclusion: matches Java behavior / spec / regression-test set; no bug.
```

A clean audit round looks like a series of these entries with no `Finding:` block.

## Pre-submission cross-checks

The checks before a report ships:

1. **Every claimed failing test was actually run.** Five `TestRegression_*` tests means five runs and five pasted failure outputs. Unran tests get demoted to Suspected.

2. **Every spec claim cites a specific file:line in `apache/avro:main`.** Not the spec website, not a release tag — the `main` branch. The spec text and the Java implementation both quoted with paths.

3. **`git log` and the `TestRegression_*` set checked for prior coverage.** When grep finds a regression test for the same area, the finding may be a misread of an existing fix.

4. **The failing test is copy-pasteable.** If a maintainer can't drop it into the repo and see it fail today, the finding isn't reproducible.

5. **The finding is a behavior bug, not "this could be cleaner".** Style and ergonomics get dropped.

6. **Every reported clean area names the structural angle, not just the syntactic sweep.** "I ran `grep -n 'json.Unmarshal'` and the hits look fine" is not verified-clean. "I ran the grep, listed every hit, and for each hit applied pattern 1's structural question — does this site take user-controllable input that could exceed the parser's precision domain? Here are the answers" is. The candidate-generator vs bug-detector distinction matters: the greps in this document are the former.

7. **Every "no parity bug" conclusion checked the structural blind spots.** Pattern 13b: a passing test that pins a Go type for a user-facing numeric value may be locking the bug. Cross-check: what would the type-asserted value be if the input exceeded that type's representable range? When the cleared area falls into a named blind spot category, the corresponding probe is required even if encode/decode sweeps didn't surface anything.

The bar is high because the loop is expensive — every false positive wastes a maintainer round-trip; every confirmed bug is genuinely valuable.

## Feedback loop — keeping this document current

After each round, before closing it out:

1. **Findings in previously-named patterns prompt the question: why didn't a past audit catch it?** The answer is rarely "this audit was sharper" — it's almost always a missing structural angle, a passing test that masked the bug, a doc-string read as truth, or a category the yield-map didn't list. The lesson goes into the relevant pattern, structural-blind-spot entry, or cross-check item — as a structural question, not as a more-specific grep (overfitting one bug creates false confidence on the next class).

2. **Findings in categories not yet named in the structural blind spots inventory expand the inventory.** The point of the inventory is to record categories that *the audit angle* misses, not bug shapes. A new entry describes the angle-of-attack gap that allowed the bug to hide.

3. **The greps in §DRY are a working starter set; the document does not grow by grep accretion.** Every new grep needs to be either (a) the only realistic way to enumerate a class of sites, or (b) replacing an obsolete grep. Structural questions ("for each hit, ask: …") are preferred over more greps. The 80-audit lesson is that grep-as-checklist creates false confidence; the document should teach the question, not the recipe.

## Distillation archive (2026-07-01)

Verbatim originals of AUDIT_CORE.md entries distilled or tombstoned when the working set split into AUDIT_CORE.md (driver) + AUDIT_PATTERNS.md (indexed compendium). The distilled forms are operative; these are the full narratives, kept so distillation loses nothing.

### Superseded: original file-system header (pre-split)

# Audit core — twmb/avro

This is the always-loaded steering document for audit rounds. It is the distilled
working set of the audit program; it replaces BUG_AUDIT.md as the round driver.

**The file system:**

- **AUDIT_CORE.md** (this file) — load at the start of every round. Gates,
  conventions, patterns, blind spots, yield map, formats.
- **NOT_BUGS.md** — the filing-time filter. Do NOT pre-load. Before filing ANY
  finding, read its `## Index` and check the candidate against matching entries.
  A finding that matches a NOT_BUGS entry is not a finding.
- **BUG_AUDIT.md** — frozen archive (full narrative history of every pattern,
  divergence, and round). Do not load it during rounds; do not update it. Consult
  it only when a distilled entry here is too terse to act on.

The correctness target: byte-for-byte compatibility with the Apache Avro
reference (Java) where the spec is precise, sane behavior where the spec is
silent, and no observable bugs.



### Superseded: original Convergence section (pre-ledger; contains the 2026-06-12 fix-the-fix measurement)

## Convergence

**Measured 2026-06-12: 60% of the last 30 fix commits modified lines this
walk itself wrote** (74/162 across the whole walk). The original codebase is
largely converged; the walk's own output is now the primary bug source. The
machinery below exists to drain that loop instead of feeding it.

**Finding classes — counted differently:**
- **Behavioral**: wrong bytes/values/accepts/rejects observable by a user;
  requires a failing repro test against pre-fix code. The convergence
  signal.
- **Resource-bound (DoS)**: correct output, unbounded cost on hostile input.
  Real, but do NOT drip these one per round: they are closed WHOLESALE by
  the dedicated entry-point sweep (one round: every public entry point ×
  the hostile-input battery), after which a new DoS finding counts only if
  the sweep's battery missed an entry point — fix the battery too.
- **Doc/test pins**: a doc sentence provably false, or a missing pin for
  behavior already correct. Allowed with the contradiction cited; never
  extends the loop. Wording preferences are not findings at all.

**Convergence rule**: an area converges when a round files zero BEHAVIORAL
findings in it; the walk converges after two consecutive full rounds with
zero behavioral findings anywhere. Doc pins and battery-covered DoS bounds
do not reset the counter.

**Generation quarantine — the fix-the-fix tail is the active bug source:**
- Every fix lands UNCONVERGED. The next round's FIRST task, before any new
  ground: re-audit the previous round's diffs with the full current pattern
  list plus the hostile-input battery. A fix is converged only after a
  later round passes it clean.
- **The quarantine is a bounded prefix, not the round.** It is scoped to
  the prior round's diffs and should be a small fraction of the session;
  the round's primary deliverable remains the full-codebase audit per
  §Scope, every round, regardless of what the quarantine finds. If the
  quarantine alone fills a session, the prior round's fixes were defective
  wholesale — file THAT as a finding and stop, rather than silently
  spending the walk on rework.
- **Feature freeze**: no new features, APIs, or behavior-contract changes
  on this branch until the walk converges. Every feature added mid-walk
  restarts its area's clock (measured: the cache self-containment chain
  took four fix generations). If a finding's fix wants to grow into a
  feature, file the feature for after the walk.
- A new pattern distilled mid-walk must name which axes/areas it plausibly
  re-opens (bounded re-opening); "re-audit everything" is not a valid
  scope.

**An audit round converges when it comes back clean.** Every prior round has found something; the recurring bug *shapes* are catalogued as structural questions in §"Patterns that have produced real findings" and §"Structural blind spots." Use those as the method — never as a map of where to look (recency is not a signal; see §Scope). The rule: bugs persist until checked.

Audit rounds bias toward finding real bugs over filler. "Verified" entries are valuable when they document a specific area exhaustively checked against the spec + at least one reference impl, with concrete inputs that could have failed. Empty verifications ("checked X, looks fine") are noise — a short clean report beats padding.

The trap goes both ways: pattern-matching a comment or shape without proving the bug is one failure mode (see convention 5 on comment-as-evidence); assuming cleanness without verification is the other. Every `// matches Java's X` comment, every "the test name implies it's covered" assumption, and every "stdlib handles this" feeling is a hypothesis that needs testing before it counts as verified.


### Superseded: original Feedback loop section (pre-split)

## Feedback loop — keeping these documents current

After every round, fold what the round taught back into the working set — in
distilled form only:

- **A new bug shape that produced a real finding** → add a numbered entry to
  "Patterns that have produced real findings" (or a blind-spot entry if the
  miss was an angle-of-attack failure). Entry format is fixed: bold name,
  1–3 sentences of root cause, `Probe:` recipe, `Instance:` one pointer.
  No war stories — if the history matters, it lives in git.
- **A candidate finding rejected as deliberate design** → append a numbered
  entry to NOT_BUGS.md (`## Entries`) with the bold claim, ≤3 sentences of
  rationale with the comparison point, and the evidence citations; add its
  one-line title to the `## Index`.
- **A pattern that has stopped yielding** (several consecutive rounds, no
  findings, category fully swept) → move its entry to BUG_AUDIT.md's section
  and leave a one-line tombstone here.

Never grow an entry past its format. The previous framework document died of
accumulation (274KB); the cap is the format, not discipline.

### Pattern 3 (tombstoned): Wire-format multiple-encoding tolerance

3. **Wire-format multiple-encoding tolerance.** Avro varints have multiple valid encodings (canonical 1-byte vs non-canonical multi-byte). Code that peeks at `src[0]` and switches on byte values risks rejecting valid non-canonical input. Java's `BinaryDecoder.readIndex` always uses the full varint loop.
   Probe: look for `switch src[0]`, `case 0x00 / 0x02`.

### Pattern 8 (tombstoned): Bounds checks that miss zero-byte items

8. **Bounds checks that miss zero-byte items.** `count > len(src)` is sound for non-zero-byte items but rejects valid `array<null>` and admits 10B-element zero-byte attacks. Generalize to `count > len(src)/minItemBytes` plus an absolute cap when `minItemBytes == 0`. Sibling **post-add wraparound**: `totalItems += count; if totalItems > cap` wraps negative when count near MaxInt64; the pre-add form `count > cap - totalItems` is overflow-safe.
   Instance: 2 of 4 array-decode sites still had the post-add form, caught only by a downstream `start > MaxInt - n` guard.

### Blind spot (merged): Schema-parse-time validation is a third code-path axis

- **Schema-parse-time validation is a third code-path axis.** Primary lenses are encode-time and decode-time; a field default carries the same stdlib-parser-callsite pattern at `avro.Parse` time, via `defaultAsFloat64`/`defaultAsInt32`/`defaultAsInt64`/`validateDefault`/`coerceDefault`/`convertDefaultBytes`/`encodeDefault`. Invisible to round-trip wire tests (parse-arm rejects before wire), encode/decode-parity sweeps, and "decoder counterpart has the right arm" tests. Probe: for every stdlib-parser-callsite fix, ask "is there a `defaultAs<Type>`/`validate<Type>`/`coerce<Type>` calling the same parser?" If yes, probe `avro.Parse({type:record,...,default:<newly-accepted-input>})`; a reject there means a partial fix (pattern 1b "three-axis rule"). Instance: `defaultAsFloat64`/`coerceDefault`.

### Blind spot (merged): Metadata-API observability is a FOURTH code-path axis

- **Metadata-API observability is a FOURTH code-path axis.** Beyond encode/decode/parse-validate, the schema metadata API (`Schema.Root().Props`, `.Fields[].Default`, `.Fields[].Props`, `CustomType` callback `*SchemaNode.Props`) consumes the same stdlib parser outputs through its OWN chain, all routing through `normalizeJSONNumber` (`schema.go`) — a SEPARATE `json.Number.Float64()` callsite. Invisible to wire round-trips, encode/decode-parity, and the `avro.Parse({...default:X})` probe (parse accepts but `Root()` checked separately). Probe: every stdlib-parser-callsite fix with a metadata surface owes a `Root().Default.(float64) == expected` assertion, not just a wire-byte one. Must also handle `toJSONWalk` re-serialization (`schema_node.go`) — `encoding/json.Marshal` rejects ±Inf, so a `jsonSerializableValue` walker converts back to `json.Number`. Instance: `normalizeJSONNumber` (5th caller of `strconv.ParseFloat`-equiv) returned `json.Number("1e1000")` from `Root().Fields[0].Default` while wire was already +Inf.

### Blind spot (merged): The 3-axis rule applies to reject-direction predicate changes

- **The 3-axis rule applies to reject-direction predicate changes, not just accept-direction.** A prior fix made encode arms REJECT integer-form > 2^53 (tightened rounding); the schema-parse arm was never updated. The "did the schema-parse arm get the matching change?" probe runs BOTH directions: (a) newly accepted at encode — does parse accept? AND (b) newly rejected at encode — does parse also reject? And the reverse — every schema-parse predicate-change owes an encode-time probe. Both directions show as wire-format inconsistencies. Instance: encode arms rejecting integer-form > 2^53.

### Blind spot (distilled): CustomType logical-codec suppression, shapes (a)-(r)

- **CustomType logical-codec suppression conditions must be mirrored on BOTH wire formats AND BOTH directions — and the conditions differ per build.** When a custom type matches a logical node, the binary builds SUPPRESS the built-in logical codec so the callback (or, for a nil callback, the user) sees the RAW Avro-native value. JSON encode (`appendAvroJSON`) and decode (`decodeKind` via `wrapDecodeJSONWithCustomDecoders`) must replicate each suppression condition or a binary↔JSON divergence appears. The conditions: **deser** suppressed for ANY match (`hasMatchingCustomType`); **ser** suppressed only when a custom Encode exists (`hasMatchingCustomTypeWithEncode`); the **fixed build** suppresses ser for ALL fixed logicals (decimal/duration/uuid → `serSize`) while **primitive/bytes builds** suppress only decimal/big-decimal. Probe: enumerate by `(build site × direction × logical kind)`, not the one shape a bug report exercised; for every `hasMatchingCustomType`/`hasMatchingCustomTypeWithEncode` gate, confirm the JSON encode arm gates on THREADED `encodeSuppresses` (= `hasMatchingCustomTypeWithEncode`, stored on `customWiring` — NOT the runtime proxy `custom[node].encode != nil`) and the JSON decode wrapper on THREADED `suppressLogical` (= `hasMatchingCustomType`, plus `jsonDecodeAppliesLogical` for scope — NOT the proxy `len(decoders) > 0`). **The trap: a runtime proxy includes wildcards; the binary gates exclude them. Gate on the EXACT threaded predicate. Meta-trap: sweep BOTH directions in the SAME pass.** `jsonDecodeAppliesLogical` DERIVES its answer by probing the `decodeLogical*` functions at parse time (correct by construction, can't drift; parse-time boxing cost only, off the hot path); pinned by `TestRegression_JSONDecodeAppliesLogicalMatchesDecode`. Twelve divergence shapes: (a) `Decode==nil` custom on a logical node — JSON returned transformed Go type, binary raw; (b) `Encode!=nil` custom with non-matching pass-through value — JSON ran the logical coercion binary suppressed; (c) pointer/interface-GoType custom at a UNION BRANCH — JSON peeled the pointer before union dispatch; fix dispatches union before the peel loop; (d) WILDCARD custom (empty LogicalType AND AvroType) over-suppressed on BOTH directions — binary gates EXCLUDE it (`ErrSkipCustomType`), JSON fixes used runtime proxies that INCLUDE it; (e) wildcard Encode DOUBLE-FIRES on `EncodeJSON` for 2-branch `["null", T]` null-first union — fix `if len(node.branches)==2 && branch.kind=="null" { continue }`; test callback INVOCATION COUNTS across union arities; (f) NO-CALLBACK non-wildcard custom — `applyCustomTypes` bailed at `if len(encoders)==0 && len(decoders)==0` BEFORE the wrapper-install; fix computes `suppressLogical`/`jsonAppliesLogical` BEFORE the early return; (g) schema-resolution PROMOTION re-applied the reader's logical unconditionally — `doResolve`'s `promotionDeserForLogical` fired under suppression; fix gates on `ctx.custom[r]==nil || !ctx.custom[r].suppressLogical`; compare VALUES the callback was fed, not result TYPES; (h) self-/forward-ref named type with a CT-matched logical in its subtree FAILED to Parse — `rejectCachedRefIfCustomTypeWouldMatch` read `hadCustomType` stamped at the wrong build phase; fix gates on `b.cachedNames[refName]` (cross-Parse names only); (i) FORWARD-REFERENCED named type dropped the binary custom wrap (JSON applied it) — `finalize`'s fixups wired the UNWRAPPED ser/deser; DRY fix: one shared `makeCustomSer`/`customWrappedSer`/`customWrappedDeser`; (j) no-Decode suppression into `[N]byte` diverged — fix decodes STRAIGHT INTO THE TARGET via `decodeKind`; (k) SchemaCache type cached WITH a CustomType, referenced WITHOUT it, silently inherited it on both formats — fix: symmetric cache-boundary custom-presence agreement guard; (l) custom Decode returning `*T` into a `*T` target diverged — JSON wrapped `setCustomResult` in `indirectAlloc(v)`, peeling a pointer level; fix drops `indirectAlloc` (call the shared helper identically); (m) no-Decode suppression into a SCALAR typed target (`*string`, `*big.Rat`, `avro.Duration`) still applied the logical arm on JSON — `assignBytes`/`decodeInt`/`decodeLong` ran the logical switch unconditionally for typed targets; worst case `time-millis` into `time.Duration` SILENTLY produced different values (both succeed, no error). Fix: thread the suppression `raw` flag and `return setBytesValue`/`setIntValue`/`setLongValue` BEFORE the logical switch; the SIBLING-DECODER sweep (mandatory after any fix) found the same gap across `decodeInt`/`decodeLong`. Lesson (Pattern 14a): the unit of analysis is every target the helper can land, not the report's input class; a both-succeed VALUE divergence hides from an error-parity-only check. (n) a RECORD-level custom (`AvroType:"record"`) was DROPPED by schema resolution — `resolveRecord` built its resolved node without re-applying the reader's custom wiring, while EVERY other resolve arm (enum/array/map/fixed/primitive/promotion) wrapped via `maybeWrapResolvedNode`/`applyCustomToNode`; so a record `Decode` callback fired on a direct decode but silently returned the raw `map[string]any` through any real evolution (the canonical-equality fast path masked it — reorder/add/drop bypasses it). Fix: `ctx.applyCustomToNode(nd, r)` in `resolveRecord`, mirroring the siblings; the resolved `DecodeJSON` funnels through the same deser, so both wire formats gain it. The sibling-sweep angle: when ANY resolve arm builds a node, audit whether EVERY arm re-applies custom — the unit is the arm set, and unions are the one deliberate non-wrap (customs don't match the union container; branches wrap during their own `resolveNode`). (o) a logical RESURRECTED on a kind it is NOT spec-valid for (`{"type":"bytes","logicalType":"uuid"}`/`"duration"`, `{"type":"fixed",...,"logicalType":"big-decimal"}`) decoded raw on binary but TRANSFORMED on JSON. `jsonDecodeAppliesLogical`'s *any-probe correctly returns false for the wrong-kind logical (so NO suppression wrapper installs and `raw` stays false — distinct from shape (m), which is the `raw==true` path), BUT the SHARED `assignBytes` typed-target helper (used by BOTH `decodeBytes` and `decodeFixed`) fired its uuid/duration/big-decimal arm regardless of `node.kind`; the per-kind `*any` `decodeLogical{Bytes,Fixed}` omit the wrong-kind arm but the one shared typed helper included all four. The trap: a typed-assign helper shared across two byte-container kinds breaks the probe's correct-by-construction guarantee, which only holds when the typed transform set per kind equals the `*any` set; `decodeInt`/`decodeLong` are immune (separate functions, kind-disjoint logicals). Fix: gate each `assignBytes` arm to its spec-valid kind (big-decimal→bytes; duration/uuid→fixed) so the typed set matches the `*any` set; the JSON encode side was already immune (`appendAvroJSON`'s bytes and fixed arms are separate per-kind switches) — but the BINARY encode side was NOT (shape (p)): "the encode side was already immune" verified only the JSON encoder. **The item-10 re-grep (`node.logical ==`/`switch .logical` across decode) caught the `[sibling-of-fix]` the assignBytes gate alone missed:** `hasDecimalBareNumberArm` (the lenient bare-number JSON arm, shared by `decodeBytes` AND `decodeFixed`) was kind-AGNOSTIC, so `fixed`+`big-decimal`+custom decoding a bare `123.45` still transformed to `*big.Rat` (the codepoint-string form went through the now-gated `assignBytes`, but the bare-number form bypassed it) — TWO decode FORMS per kind, both needing the gate; two call-site comments ("never reaches here"/"not eligible on a fixed branch") asserted the invariant the predicate didn't enforce. Fixed by making `hasDecimalBareNumberArm` kind-aware (big-decimal→bytes only). Lesson: a shared typed-assign helper has a shared LENIENT-FORM sibling (bare-number, alternate-encoding) reached through a DIFFERENT predicate; gate both, and re-grep `.logical` after the first gate. The metadata/parse-validate axes were checked CLEAN (a wrong-kind logical field default surfaces raw `[]byte` via `Root().Fields[].Default` and validates as raw, already consistent with the now-raw decode). Pinned by the extended `TestRegression_CustomSuppressionScalarTargetParity` (uuid-on-bytes/duration-on-bytes/big-decimal-on-fixed rows), `TestRegression_DecimalBareNumberArmHonorsKindValidity`, + the `{bytes,uuid}`/`{bytes,duration}`/`{fixed,big-decimal}` false rows in `TestRegression_JSONDecodeAppliesLogicalMatchesDecode`. (p) the BINARY-encode mirror of (o): a CustomType-RESURRECTED wrong-kind logical (uuid on bytes, a date/time/timestamp logical on string) encoded via the logical serializer on binary while JSON encoded raw and both decoders stayed raw — the general primitive build applied `logicalSer(o.Logical)` keyed only on the logical NAME (`schema.go`), kind-blind, whereas `appendAvroJSON` is per-kind. Binary thus disagreed with JSON, and a string-backed time logical produced a bare-varint wire its own `deserString` rejected (self-incompatible — missed by `TestMatrix_SelfReadableAtScale`, which has no wrong-kind-logical-with-custom generator). Fix: gate the `logicalSer` application on `logicalUnderlyingAccept[o.Logical](o)` — the same predicate `validateLogical` soft-drops a wrong-kind logical with — so a resurrected wrong-kind logical keeps the base (raw) serializer; `logicalSer` (schema.go) is the lone kind-blind logical-codec selector, the fixed build's per-logical `switch` and `appendAvroJSON`'s per-kind arms being already immune. Lesson: an "the other format/direction was already immune" claim is a claim (hard rule 5) — verify EACH of the four (format × direction) cells independently; the binary encoder selects its logical codec by name, the JSON encoder by kind, so they have different immunity. Pinned by `TestRegression_CustomSuppressionWrongKindLogicalEncodeParity` (all 10 `logicalSers` entries on a wrong kind, encode parity + self-readability) + `TestRegression_CustomSuppressionSpecValidLogicalStillApplied` (spec-valid placements not regressed). (q) the WRONG-SIZE sibling of (p) on the FIXED build — and proof that shape (p)'s OWN "the fixed build's per-logical `switch` and `appendAvroJSON`'s per-kind arms were already immune" parenthetical was the exact unverified "already immune" claim (p)'s own lesson warns against. uuid is fixed-valid only at size 16 and duration only at size 12 (`logicalUnderlyingAccept`); a no-Encode CustomType resurrects the soft-dropped wrong-size logical, and the fixed-build `case "uuid"`/`case "duration"` applied `serFixedUUIDReflect` (always 16 bytes) / `serDuration` (always 12) while the JSON fixed arms wrote the same — SIZE-blind — yet the suppressed decoder reads `deserFixed{size}`. So BOTH wires emitted a wire their own decoders rejected (16/12 bytes where `size` was declared), and registering a passive custom silently broke a round trip the plain (no-custom, soft-dropped-to-raw) fixed completes — the unit is `(format × logical) on the fixed build`, NOT just the primitive build (p) fixed. Fix: gate the fixed-switch logical-ser on `logicalUnderlyingAccept[logical](o)` (binary, schema.go) and the JSON uuid/duration arms on `node.size == 16/12` with raw fall-through (json_codec.go), mirroring (p) on the fixed path. decimal-on-fixed is immune (hard-errors on nil precision before resurrection; `serFixedDecimal` is size-aware); the unsafe fast path is immune (declined when `hasCustomType`); encodeDefault's fixed arm and the decode/metadata axes were already raw+size-checked. Meta-lesson: when shape (p)'s fix PARENTHETICALLY asserts a sibling path is immune, that parenthetical is a finding-in-waiting until a test exercises it — the next round's quarantine of (p) is where (q) surfaced. Pinned by `TestRegression_CustomSuppressionWrongSizeFixedLogicalEncodeParity` (passive custom must match the plain fixed for both a raw `[size]byte` AND the size-blind serializer's own logical-shaped input, on both wires) + the uuid-on-fixed16/duration-on-fixed12 boundary-1 rows in `TestRegression_CustomSuppressionSpecValidLogicalStillApplied`. Pinned by `TestRegression_Custom{Decode,Encode}*BinaryJSONParity`, `TestRegression_Wildcard*`, `TestRegression_CustomNoCallbackSuppressionBinaryJSONParity`, `TestRegression_CustomPromotionHonorsLogicalSuppression`, `TestRegression_RecursiveCustomTypeParsesAndParity`, `TestRegression_SchemaCacheCustomBoundaryGuard`, `TestRegression_CustomSuppressionScalarTargetParity`, `TestRegression_RecordCustomTypeThroughResolve`. (r) the all-decoders-skip (`ErrSkipCustomType`) fall-through DECODED the value into a probe `any` then gated on `AssignableTo`, so a skipped custom could NOT decode into a typed CONTAINER (struct / []T / map[string]T) or a NAMED scalar (`type Money int64`) — diverging from no-custom decode AND from the `Decode==nil` path (which decodes straight into the target), identically broken on binary, JSON, AND resolved (the shared `wrapDeserWithCustomDecoders` / `wrapDecodeJSONWithCustomDecoders`). `any` / `map[string]any` / exactly-assignable targets worked (assignable), masking it; the whole shape sits OUTSIDE (a)–(q) — it is neither a logical raw-vs-enriched nor a binary↔JSON divergence (both wires fail the same). The naive fix (re-decode the wire straight into the target) is O(depth²) for a CONTAINER-matching (wildcard) custom into a nested typed target — every level re-decodes its subtree into the probe AND into the target (measured ~356 ms at depth 500, vs ~3 ms after) — so the all-skip fall-through RE-DECODES the original wire into the typed target through the base deserializer (`wrapDeserWithCustomDecoders`/`wrapDecodeJSONWithCustomDecoders`; NOT_BUGS #48) — byte-identical to a no-custom decode, bounded by `maxDepth`, with a no-match `bypassCustom` fast path keeping the common case single-pass. An EARLIER attempt placed the already-decoded canonical value into the typed target via a shared recursive converter (`assignCanonical`, since REMOVED); it was abandoned because PLACING A VALUE diverged from a no-custom decode on four axes — a REUSED map kept fresh-map keys instead of its existing ones (reuse), a logical node was enriched instead of landing RAW in a base typed target (logical-into-base), an overlapping union lost its exact wire BRANCH-INDEX (branch-index), and under `TaggedUnions()` the probe `any` (which `maybeWrap` tags) leaked a `{branch:value}` envelope into the TYPED placement (which `maybeWrap` never tags), zero-valuing the branch — silently WRONG, worse than the pre-fix error. Re-decode is correct on all four because it re-runs the base deser straight into the target and DISCARDS the probe entirely (so the tagged-probe issue no longer needs the old "clear `sl.taggedUnions` around the probe" workaround — the tagged value is never placed). **Census lesson that survives the converter's removal: when enumerating a recursion's targets, count the reachable KINDS, not just the wrapped node's — a record/array/map field can be a union, the case the converter's "no union arm" assumption missed.** **Option-dimension pin: run the parity matrix under BOTH default and `TaggedUnions()` — the tagged-vs-untagged divergence is invisible in the default mode.** Probe: decode a wildcard-skip custom into every typed target the base decoder fills (struct / []T / map / named scalar / pointer / [N]byte) and assert == no-custom on binary, JSON, AND resolved; a custom-decoder fall-through that boxes into `any`+`AssignableTo` silently restricts the target set vs no-custom. Pinned by `TestRegression_CustomSkipDecodeMatchesNoCustom` (wildcard-skip == no-custom across the type matrix × binary/JSON/resolved, neuter-verified) + the flipped `TestRegression_DecodeJSONCustomDecoderConcreteTargetErrors` (compatible named-scalar now succeeds == no-custom; incompatible still errors-not-panics). Instance: shape (a) `Decode==nil` custom on a logical node; shape (n) record custom dropped through `Resolve`; shape (r) all-skip into a typed container/named-scalar boxed into `any`. Add to the yield-map under custom types.

### Blind spot (distilled): Two-mechanism recursion-depth accounting

- **Two-mechanism recursion-depth accounting must charge each schema edge once on BOTH mechanisms.** The `errTooDeep`/`maxDepth` bound is enforced by TWO mechanisms that must stay in lockstep: the encoder threads a `depth int` PARAMETER incremented at each recursive call (`fn(..., depth+1)`), while the decoder/JSON-decoder carry a STATEFUL `sl.depth` bumped on container/record NODE ENTRY (`sl.depth++; defer sl.depth--`). The invariant — one increment per parent→child schema edge, identical on every path — breaks at seams where a node has more than one entry function: a reflect body dispatching to an unsafe fast body (`serRecord.ser`→`serRecordFast`, `deserRecord.deser`→`deserRecordFast`), or a compiled field fn re-entering the record via `*Via` (`tryCompileFieldSer`'s record/pointer arms → `serRecordVia`). Symptom: a recursive schema trips `errTooDeep` at a DIFFERENT depth on one path than another; `min(encode,decode)` round-trips break and the effective bound silently halves/thirds. Invisible to round-trip tests (feed decode only the depth encode produced) and oracles whose shapes don't reach the seam. The directly-nested struct-record edge is the seam the container/union oracle structurally misses — the unsafe struct-fast encode path double-counted it. A SECOND drift family lives at the **container-of-union / array-element-union seam** (`array<["null",Self]>`, `map<["null",Self]>`, `["null", array<Self>]`): the union is its own node and must cost one depth unit, but encode-side null-union helpers got it wrong two ways — (1) array-element fast paths (`usArrayNullUnionRecord`/`usArrayNullUnionPtr`) entered the inner record from the array's depth (`depth+1`), SKIPPING the union node (binary accepted ~1.5× the depth its decoder via `udNullUnionRecord` could read); (2) the 2-branch null-union encode optimizers (`serNullUnionAt`, `usNullUnionPtr`, `usNullUnionRecord`) CHARGED the edge but OMITTED the union node's own `if depth >= maxDepth` guard, tripping one level deeper than decode's `deserNullUnionAt`. Fix: every encode-side null-union helper mirrors `deserNullUnionAt` — guard at the union node AND charge its edge; array-element paths guard the per-element union (hoisted before the loop) and enter the inner at `depth+2`. Probe: build a depth-uniformity oracle that hand-assembles wire INDEPENDENT of the encoder (so each direction's true trip depth is observed, not `min`), run EVERY recursive shape × EVERY path (encode, typed/any decode, JSON encode/decode, resolved decode) asserting all trip at the SAME depth — `TestDepthUniformityOracle`/`TestDepthUniformityMutual`, spanning `array-of-nullunion`, `array-of-nullunion-reflect`, `array-of-nullsecond-union`, `map-of-nullunion`, `field-nullunion-of-array`, `field-nullunion-of-map`, `array-of-multibranch-union`, `array-of-map-of-nullunion`, `map-of-array-of-nullunion`. For the directly-nested struct-record seam use `reflect.StructOf` deep nesting probed above the half-budget collapse (`TestDepthUniformityNestedStructRecord`). Cyclic-value safety pinned by `TestDepthBoundCyclicContainers`. When a fix touches any increment site, sweep every `depth+1`/`sl.depth++`/`depth >= maxDepth` across ser.go/deser.go/unsafe.go/json_codec.go/json_decode.go/resolve.go/skip.go and classify each as sole entry (counts once) or a dispatch hop into a body that also counts (must NOT add its own +1/bump). Instance: unsafe struct-fast encode double-counting the directly-nested struct-record edge.

### Blind spot (distilled): Structural-tree depth bound leaves per-node values / expansion / nested budgets unbounded

- **A structural-tree depth bound leaves the per-node VALUES unbounded — they reach the same marshaler through a separate channel.** When a recursive tree walk is depth-bounded, the per-node PAYLOAD values it embeds (Props, field defaults — arbitrary user `any` trees) are still handed to the same downstream `json.Marshal` (and to fixup walkers like `needsJSONFixup`/`applyJSONFixup`) WITHOUT the bound, because they descend a separate recursion the structural counter never enters. A hand-built node one level deep with a million-deep Props value overflows the goroutine stack uncatchably even though the node tree is shallow — `recover` cannot catch it, and the entry point's eventual `Parse` (which would reject the JSON) never runs because the crash is in the pre-Parse marshal. Probe: after bounding any tree walk that later serializes the whole tree, enumerate every per-node user-supplied `any` payload (Props, defaults, metadata) embedded into the marshaled output and bound ITS nesting too, short-circuiting at the same ceiling so a hostile value can't overflow the check itself. Instance: `46d4dde` bounded `toJSONWalk`'s structural items/values/branches/fields, but `SchemaNode.Schema()` (and `SchemaFor` via a hand-built `CustomType.Schema`) still crashed on a deep `Props` value or `SchemaField.Default` reaching `json.Marshal` — fixed by `boundedSerializableValue`/`valueNestsTooDeep`, pinned by `TestRegression_SchemaNodeSchemaDeepValueBounded`. **Follow-up (the value-bound helper's own coverage was narrower than the marshaler it guards):** `valueNestsTooDeep` matched only `map[string]any`/`[]any` (the shapes `Root()` produces from parse), but the `map[string]any` Props/Default field accepts ANY Go value, and `json.Marshal` recurses into every container kind — so a hand-built TYPED container (`[]map[string]any`, a struct, a `[]*T` chain) bypassed the type-switch bound and reached `json.Marshal` unbounded (2M-deep → uncatchable stack overflow via the public `SchemaNode.Schema()`). Broadened to a reflect-walk over map/slice/array/struct/pointer/interface, decrementing on EVERY descent so it terminates at the budget rather than hanging on a cyclic Go type (`type P *P`), with `[]byte`/`[N]byte` short-circuited (a base64/number scalar, not nesting). Lesson: a value-channel depth bound must mirror `json.Marshal`'s FULL recursion, not the container subset the parser happens to emit; a type-switch on the parser's shapes silently omits every typed container a hand-built node can carry. The existing `SchemaFor` pin was VACUOUS (`SchemaFor[int32]` errors on "requires a struct type" BEFORE walking the embedded `CustomType.Schema`) — a `CustomType.Schema` value walk runs only when a struct FIELD matches the custom GoType; fixed to `SchemaFor[struct{...}]`. **Second follow-up (a depth bound caps path LENGTH, not tree EXPANSION — a different axis on EVERY channel):** three commits (`46d4dde` structural, `7f13cf9` value, `01b0b32` typed-container) all bounded DEPTH, but depth is orthogonal to a shared-reference DAG: the same `*SchemaNode` reached via a node's Items AND Values pointer, or the same sub-value reached via two map keys (`{"a":x,"b":x}` repeated per level), is tiny in memory yet fans out into a 2^depth TREE when serialized — neither `toJSONWalk` nor `valueNestsTooDeep` nor `json.Marshal` memoizes shared references, and `toJSONWalk`'s `visited` map is PATH-scoped (`defer delete`), so off-path sharing is not a cycle. A ~40-node DAG (depth 40 < the 4000 depth cap, so the depth bound never fires) demands 2^40 emitted nodes and hangs/OOMs before `Parse` runs — on BOTH the structural walk and the value walk + marshal. Probe: after bounding a recursive walk's DEPTH, ask the orthogonal question — can a SHARED sub-node/sub-value reached by two sibling paths re-expand when the walk emits a tree? If the walk (or the marshaler it feeds) doesn't memoize, depth-N sharing is exponential. Fix: a single node-count budget shared across the whole walk (structural + every value), decremented per emitted node and checked before descent so the fan-out is pruned at the frontier (~ms reject) — it bounds `json.Marshal`'s cost too (same expanded tree). It allows benign shallow reuse (low expansion) and named-type dedup, rejecting only compounding fan-out. Lesson: a "no stack overflow" depth bound is not a "bounded total work" bound; enumerate both axes (path length AND emitted-node count) for any walk that serializes a possibly-shared graph. Instance: `maxSchemaJSONNodes` + `valueWalkLimit` in schema_node.go, pinned by `TestRegression_SchemaNodeWalkDepthAllChannels` (depth on all four structural channels + three value sites + every container kind) and `TestRegression_SchemaNodeSharedDAGExpansionBounded` (the expansion axis). **Third follow-up (a NESTED serialization inside the bounded walk that allocates a FRESH budget escapes the shared bound):** the shared budget bounded the main `toJSONWalk`, but the dedup conflict-comparison (the named-redefinition check) marshalled both bodies via `toJSON()`, which allocates a FRESH `maxSchemaJSONNodes` — so a named type re-occurring as a DISTINCT pointer with an IDENTICAL body (a record with k fields each a hand-built copy of one w-node named def) drove `2·k·w` full-subtree re-marshals OUTSIDE the shared budget: the outer walk charges only 1 per re-occurrence (it emits a bare ref, not the body), so k+w stays inside the budget while k·w → budget² (~2^39 at k=w=2^19), from a tiny output (one def + k−1 refs) — only the verification amplifies. Reachable via public `SchemaNode.Schema()` on a hand-built node, NOT via `Parse→Root()→Schema()` (parsed re-references are bare `{Type:"Name"}` nodes that fail `isNamedKind`, so they never reach the conflict marshal). Fix: the comparison shares the walk's budget (`toJSONShared`, not `toJSON`); over-budget reports the expansion error (asymmetric truncation means truncated bodies can't be compared, so it must NOT report a spurious conflict). The SchemaFor `dedupNamedTypes` twin is immune (it CACHES prev's marshaled bytes, and its tree is `inferRecord`-bounded by distinct Go types, not a runtime `make([]SchemaField,k)`). Probe: after introducing any shared-budget/shared-depth walk, grep every NESTED `json.Marshal`/`toJSON`/re-walk reachable from inside it and confirm each threads the SAME budget pointer, never a fresh one — a budget that resets per nested call bounds each call but not their product. Instance: `toJSONShared` in schema_node.go, pinned by `TestRegression_SchemaNodeDuplicateNamedDefinitionBounded`.


## Distillation archive (2026-07-14)

Verbatim originals of entries distilled or tombstoned on 2026-07-14, when
AUDIT_CORE.md had grown to ~163KB against its ~55KB bound (the round ledger
had accumulated full narratives in place of its one-entry-per-round format).
The compressed forms in AUDIT_CORE.md / AUDIT_PATTERNS.md are operative;
these are the full texts, kept so distillation loses nothing.

### Round-ledger narratives (AUDIT_CORE.md §Round ledger, 2026-07-01 through 2026-07-14)

The ledger section header and preamble at distillation time, followed by
every round entry, all verbatim:

## Round ledger

One line per round, appended at round end (clean or not). This is the
convergence record and the quarantine boundary: the next round's quarantine
scope is code commits AFTER the newest line's HEAD, and two consecutive
lines with zero behavioral findings on full rounds = the walk is converged
and the feature freeze lifts.

Format: `date · HEAD at round time · quarantine scope cleared · behavioral
findings · oracles not run`.

- 2026-07-01 · ea9a2ce · 9944fad..ea9a2ce (5 fix commits) — clean ·
  0 behavioral (1 NOT_BUGS policy record, #52) · fastavro + Java oracles not
  run (local round). Fronts: skip↔deser bounds/depth parity, slab, SOE,
  temporal encode overflow, narrow-before-check greps, varint boundary
  differential, promotion × typed targets, atype, Root() aliasing.
- 2026-07-02 · ea9a2ce · empty (no code commits since ea9a2ce) ·
  0 behavioral · fastavro RAN (scratch venv, full differential incl.
  recursion cells — pass); Java oracle not run (no local JRE; covered in
  CI). Fronts: inverse-density walk (rabin, varint, compat, cache, errors,
  promote — clean), json_scan↔encoding/json differential battery + skip↔
  value parity (40 rows, clean), duration logical full typed cross +
  logicals×default-fill (clean), P18/error-echo grep refresh (clean).
  Net gaps closed: promotion×TYPED-targets axis
  (`TestMatrix_PromotionTypedTargets`, neuter-verified ×2) and decimal
  VALUE-precision boundary pin (neuter-verified); the pointer
  FIELD-OF-CONTAINER gap line was stale — already netted by
  `TestMatrix_GenerativePointerIndirectionUnsafeContainers` — deleted.
  SECOND consecutive clean full round: per §Convergence the walk is
  CONVERGED and the feature freeze lifts (modulo Java-oracle areas,
  which run in CI). CI ran at fdc6df5: java-differential + cisuite
  green — convergence unqualified.

- 2026-07-02 · fdc6df5 · ea9a2ce..fdc6df5 (2 test-net commits + 1 doc
  commit) — clear · 1 behavioral, FIXED SAME ROUND (OCF reader treated a
  validated mid-stream count-0 block as end-of-stream, silently truncating
  spec-valid files fastavro reads fully; the in-test pin's "fastavro both"
  claim was false — gate verdict documented-but-contradicted; maintainer
  ruled SKIP-AND-CONTINUE: red-then-green pin
  `TestRegression_EmptyBlockMidStreamSkipped`, foreign-framing matrix
  `TestReaderForeignEmptyBlockFraming` (position × codec × payload,
  fastavro-calibrated, corrupt-sync + 10k-empty-blocks cells),
  neuter-verified (pin + all first/mid/consecutive cells red on revert),
  FIX.md sweep clean, NOT_BUGS #53 records the no-decompress-on-skip
  leniency). CONVERGENCE COUNTER RESET; feature freeze back on until two
  consecutive clean full rounds. · fastavro RAN (venv 1.12.2, five
  differential tests + the new framing matrix); Java oracle not run (no
  local JRE; CI covers). Fronts: ocf.go line walk (the finding),
  reflect.go + schema_parse.go + logical.go inverse-density walk (clean),
  P1/P9/P18/Y4/B20 grep refresh (clean), JSON high/control-byte form
  differential vs fastavro (clean — byte-identical `\u00XX` escaping).
  §Open net gaps foreign-OCF-framing line: added and closed same round by
  the matrix.

- 2026-07-02 · bcb7fa9 · fdc6df5..bcb7fa9 (1 fix + 1 matrix commit) —
  clear · DEDICATED CLAIMS-HARDENING ROUND, not a full walk (neither
  extends nor resets the convergence streak; 0 behavioral findings) ·
  fastavro RAN (recreated venv 1.12.2 — tmp cleanup had emptied the old
  ones; ~45 probe executions + all differentials incl. two NEW suites);
  Java oracle not run locally (no JRE; 3 new cisuite cells run in CI).
  Deliverables: (1) quarantine of the empty-block fix commits — clear;
  (2) net extensions: OCF block-COUNT-value cells (negative count,
  negative size on count-0, 2^40-count-vs-tiny-block, overlong-varint
  count-0; each neuter-verified red against exactly its guard) +
  FIX.md item 14 (new reference-impl claims need same-round execution
  or source quote); (3) THE SWEEP: censused ~930 reference-impl
  mention lines / ~300 distinct behavior assertions across *.go,
  classified execution-backed / executed-now / source-verified /
  false; ~95 source verifications with file:line quotes (Java,
  fastavro, goavro, avro-rs, hamba), ~45 fresh executions, and 21
  corrected false-or-imprecise claims (headline: "fastavro treats
  non-1 boolean as false" — it is `!= 0`, byte 2 → True observed;
  fastavro short-name union tags don't exist — fullname-only at every
  layer; Java soft-drops bad decimal params rather than rejecting;
  Java validates only enum-LEVEL defaults, never field defaults —
  twmb's membership check is deliberately stricter; DataFileWriter.
  close has no try/finally; goavro decimal wants *big.Rat not a
  string; Java validates TYPE aliases via NameValidator). All
  corrections are comments/pins — zero wire-behavior changes; the two
  twmb-stricter postures they had shielded (enum field-default
  membership, decimal-param hard-reject) are now honestly documented
  and stand on fail-fast + anti-silent-drop rationale. New permanent
  nets: TestDifferentialFastavroJSON (oracle jsonwrite/jsonread ops;
  bytes/fixed codepoint parity, tagged-envelope parity, bare-union
  reject, NaN-spelling calibrations), lax-mutant accept-WITNESS
  calibration replacing the blind skip in TestDifferentialAcceptance
  (execution immediately sharpened it: fastavro's laxness is
  class-level — named-type-redefining duplicates and decimal-capacity
  collisions still reject), and cisuite TestDifferentialJavaWireLeniencies
  (boolean 0x02 → false, overlong union-index accepted, empty-record
  JsonEncoder zero-byte pin that flips when upstream fixes the
  JAVA.md bug).

- 2026-07-02 · 2609823 · bcb7fa9..2609823 (2 matrix commits + 1 claims
  commit) — clear (comment/test-only; one NET-INFRASTRUCTURE finding: the
  3 new cisuite cells' `TestDifferentialJavaWireLeniencies` matched NO
  java-differential job `-run` filter, so its CI "pass" was vacuous —
  workflow filter fixed this round, verdict quoted from the next run) ·
  DEDICATED READER-GRAMMAR CENSUS round, not a full walk (streak-neutral;
  0 behavioral findings) · fastavro RAN (fresh venv 1.12.2 — all
  differentials incl. the two NEW suites); Java oracle not run locally
  (no JRE; CI). Deliverables: (1) CI green at 2609823 (test 9m37s /
  differential / java-differential, per-job from the recorded run);
  (2) quarantine clear; (3) NOT_BUGS #54 (enum field-default membership
  keep-strict) + #55 (decimal logical-param hard-reject keep-strict);
  (4) THE CENSUS: production × variant × generating-net map over every
  wire-consuming surface (core binary varint/lengths/blocks/indices/
  boolean, JSON tokens, OCF header+meta+blocks, SOE) — 7 gap families
  found and closed same round: enum-index value space natural+resolved+
  skip (out-of-range/negative/overlong/width-overflow; skip is
  consistent-skip like Java readInt + fastavro read_long),
  general-union-index value space ×3 paths (skip validates like the
  value path), skip-path hostile block headers (MinInt64 count,
  count-over-buffer, negative/over-buffer byteSize, zero-byte-cap
  cumulative, negative item length), byteSize-lie AUTHORITY pin (value
  path item-driven, skip size-driven — Java-parity both;
  fastavro COMPILED .pyx skip is size-driven too, its pure-Python
  fallback item-driven: executing the cell corrected the census's own
  _read_py-based misquote), nested-container framings through skip, OCF
  meta-map dup-keys last-wins (Java HashMap/fastavro dict parity) +
  multi-block + size-prefixed + MinInt64-count, oracle `readresolve` op
  (fastavro skip_* twins now EXECUTABLE) + 6 executed calibration cells
  (headline: fastavro's value path silently WRAPS negative enum indices
  via Python list indexing — twmb/Java reject). Every new cell
  neuter-verified in 5 cycles (index guards, validateByteSize,
  totalGuard, double-negation, canonical-only-varint, decodeMap
  first-wins, skipEnum reach, checkArrayBlockBounds), each red at
  exactly its cells. One message-only source edit: skip.go block labels
  "array block"/"map block" → "array"/"map" so skip and value paths
  report identical text ("invalid array block block count" doubling
  fixed). Grammar rows verified already-netted (no new cells needed):
  overlong/width-overflow varint values, negative/lying string+bytes+
  map-key lengths (natural), null-union index bytes, boolean non-1,
  JSON token grammar + dup-key last-wins + alias-collision, container
  legal framings natural+skip+typed, OCF magic/truncation/unknown-codec/
  meta caps/sync/block-count-values, SOE magic/fingerprint/writerSoe,
  skip depth bound.

- 2026-07-06 · 150b688 · 2609823..150b688 (1 CI-workflow commit + 1 census
  test commit; only production edit = skip.go message-label text, verified
  behavior-identical) — clear · 0 behavioral · fastavro RAN (venv 1.12.2,
  full differential suite); Java oracle not run (no local JRE; CI covers);
  -race concurrency hammer + 2×45s fuzz spot-checks (FuzzMatrixCore,
  FuzzDecodeEncodeRoundTrip) clean. FULL round. Fronts: encode-side
  production walk, inverse-density (serEnum ordinal/textValue arms,
  serSize length checks, serRecord map arms, maxDepth threading across
  reflect+unsafe+JSON — clean); metadata/doc-contract census (Canonical
  strips order/aliases/doc/default, Root()→Schema() preserves order +
  field aliases + docs, doc.go promises each pinned or probe-verified —
  clean); resolution default-fill × named-ref/recursive/diamond shapes +
  logical-typed added fields (fill == natural decode, four paths, probes
  pass — clean); OCF option plumbing (WithSchemaOpts/WithReaderSchemaFunc/
  NewAppendWriter — clean); P15/B17 skip-sweep (every dispatch skip
  documented; resolved-default deser src-immutability backed by
  setBytesValue's never-alias invariant — clean); SchemaCache-shared-nodes
  × Resolve incl. concurrent -race (clean). FIRST consecutive clean full
  round since the counter reset; one more clean full round re-converges
  the walk.

- 2026-07-06 · 150b688 (second round at this HEAD) · empty (no code
  commits since 150b688) · 1 behavioral FILED, NOT FIXED (read-only round):
  flat-format (goavro-style) fields — Parse deliberately accepts them
  (liftFlatFieldType, schema_parse.go) but the metadata twin never mirrors
  the lift: Root() surfaces a half-resolved node (Type="enum"/"fixed"/
  "array"/"map"/"record" with no name/symbols/items/values/size/fields —
  the defining keys sit in SchemaField.Props), Root().Schema() fails for
  all five kinds, and a sibling field name-referencing a flat-defined
  fixed keeps its default as string where SchemaField.Default's doc
  promises []byte (the flat-defined type never enters collectNamedTypes'
  table). Verified failing: TestRegression_FlatFieldRootSchemaRoundTrip
  (5/5 kinds), TestRegression_FlatFixedNameRefDefaultCoerced; control
  TestControl_FieldLogicalLiftRoundTrips PASSES (the field-logicalType
  lift round-trips because the rebuild preserves its flat shape and
  re-parse re-lifts — NOT_BUGS #33 family; the flat rebuild destroys its
  shape, so the same Props-carrying posture cannot round-trip). Gap line
  added to §Open net gaps; fix direction is a maintainer call (metadata-
  side lift vs rebuild-side flat preservation). · fastavro RAN (venv
  1.12.2, 549 differential runs incl. both JSON suites, 0 fail/skip);
  Java oracle not run (no local JRE; CI covers). FULL round. Fronts:
  compat.go line-walk + Java SchemaCompatibility differential
  (schemaNameEquals :96, lookupWriterField ambiguity-throw :116, writer-
  union all-branches :299, reader-union try-all :372, enum-default :428
  all cited; twmb's reader-union commit-to-best-tier matches its own
  Resolve AND Java's decoder Resolver.firstMatchingBranch, diverging only
  from Java's SchemaCompatibility class, which is more lenient than
  Java's own decoder — clean); compat-predicts-resolve seam probed (parse
  runs encodeDefault eagerly, so unfillable recursive defaults reject at
  parse; self/array/mutual shapes all probed — clean); recursive+diamond
  compat/resolve/decode probes incl. cycle-external incompatibility
  (clean); schema_node.go line-walk (the finding; otherwise clean —
  lookupCI is shared wire↔metadata so case-variant keys bind identically,
  stringSliceFrom parse-rejects non-string aliases/symbols, walk budgets
  sound, double-coercion idempotent); json_codec.go line-walk (clean —
  every seam pinned, JSON differentials green; float -0.0/exponent byte
  forms differ from Java/fastavro but re-parse equal); P1/P9/Y4/B20/P18
  grep refresh (clean; P16 vacuous — empty quarantine). CONVERGENCE
  COUNTER RESET by the filed finding; two consecutive clean full rounds
  needed again once the fix lands.

- 2026-07-07 · 4719830 (fix round for the 2026-07-06 flat-format finding:
  98ab1dd fix+pins, 4719830 matrix; maintainer ruled METADATA-SIDE LIFT
  TWIN; #33's logicalType posture untouched) · quarantine n/a (fix round)
  · the 1 filed behavioral FIXED:
  extracted the wire lift's WHEN + WHAT into shared helpers
  (flatFieldNeedsLift / flatLiftTypeMap, schema_parse.go — pure refactor
  of afieldFromAny/liftFlatFieldType, wire-behavior-identical) and applied
  the same lift in nodeFromJSONObject's field loop (schema_node.go): the
  lifted type node carries the field's name + defining content, routed
  keys (defining key, doc, logicalType/precision/scale, custom props,
  name/namespace-for-named) are excluded from SchemaField.Props, and the
  lifted named type registers in collectNamedTypes' table so name-ref
  defaults coerce. Red-then-green in-repo: TestRegression_FlatFieldRoot
  SchemaRoundTrip (6 kinds incl. "error") + TestRegression_FlatFixed
  NameRefDefaultCoerced, both verified failing pre-fix. Class matrix
  (flat_field_lift_test.go, 30 cells): kind{enum,fixed,array,map,record,
  error} × ns{absent,inherited,explicit} with per-cell Canonical+Rabin
  flat==nested-twin guard (wire tree untouched), Root content, Props
  exclusion, rebuild-canonical equality, wire-byte equality; logicals
  (duration fixed-12, decimal fixed-8 p4/s2); name-ref defaults (sibling
  fixed→[]byte, sibling enum control, cross-record diamond, recursive
  self-ref, SchemaCache cross-parse — splice delivers nested form, both
  docs rebuild); no-lift parity boundary (defining-key-absent → parse
  reject, wrong-kind key → parse reject, object-type / name-ref-type /
  primitive never lift with stray keys as-written in Props, unnamed
  explicit-namespace stays field Props); degenerate empty-symbols (#13).
  NEUTER-VERIFIED: gating the metadata twin false reproduced empty-node /
  rebuild-error / uncoerced-default red on every lift-dependent cell (pins
  + 16 kind×ns + logicals + 4 name-ref shapes + degenerate + the
  unnamed-ns cell's lifted half); no-lift parity cells + sibling-enum
  control stayed green as documented. Reference claims executed/quoted
  same round (FIX.md item 14): fastavro 1.12.2 flat-reject EXECUTED
  (UnknownType: enum/array/fixed; venv rebuilt after tmp cleaner gutted
  the old one's .py files); Java flat-reject SOURCE-QUOTED
  (Schema.java:1828-1829 isTextual→context.find; kind dispatch object-
  only at :1830-1844). NOT_BUGS #56 records the post-lift posture (+#33
  contrast); Root doc gains the flat-format paragraph; §Open net gaps
  flat line CLOSED by the matrix. FIX.md sweep walked (items 0-14; item
  13: sibling-enum value-identity claim verified by the neuter run;
  cache-splice-nested claim evidenced by v.String() output). Full suite +
  -race green. · fastavro differential re-run on the rebuilt venv —
  green; Java oracle not run locally (no JRE; CI covers). Fix lands
  UNCONVERGED per §Convergence: next round quarantines these commits.

- 2026-07-07 · 4719830 (second round at this HEAD) · 150b688..4719830
  quarantined (98ab1dd fix+pins, 4719830 matrix) — clear: flatFieldNeedsLift/
  flatLiftTypeMap verified routing-identical to pre-fix wire code by direct
  old-vs-new comparison; walker seams line-audited (lift gates equivalent:
  `primitive != ""` ⇔ bare-string type, since aschemaFromAny sets primitive
  only for `case string:`; default/doc/aliases/order/Props routing agrees
  with the wire side; Props exclusion via exact-key flatType lookup is
  complete because flatLiftTypeMap copies keys verbatim); hostile battery
  bounded (1.1 MiB flat schemas: Parse 52ms, Root+Schema 114ms, linear;
  4000-bracket depth cap rejects in µs; 1 MiB wrong-kind key rejects 10ms).
  · 1 behavioral FILED, NOT FIXED (read-only round): OCF Reader.Decode's
  truncation errors at the zero-bytes-available cuts (after a complete block
  count varint / after the size varint / ReadFull-data and CopyN-data arms /
  at sync start — ocf.go readBlock ~:1014/:1044/:1050/:1056) wrap bare
  io.EOF with %w, so `errors.Is(err, io.EOF)` — Decode's documented
  end-of-file sentinel and the idiom the package's own nets use in 8+
  sites — reads mid-block truncation as clean end-of-stream (silent tail
  loss). In-repo pins TestTruncatedBlockSize/Data/SyncMarker assert only
  err != nil, one predicate too weak; mid-varint/partial-read cuts already
  error loudly (ErrUnexpectedEOF), so strictness is the established posture
  and the sentinel leak is accidental, not a chosen Java-leniency. Verified
  failing: TestRegression_TruncatedBlockHeaderNotEOF (3 cuts red) +
  TestRegression_TruncatedLargeBlockDataNotEOF (CopyN arm, raised cap, red);
  true-EOF control green. fastavro 1.12.2 EXECUTED at every cut: strict
  (EOFError / "expected sync marker not found"); Java SOURCE-QUOTED lenient
  at ALL cuts incl. mid-varint (DataFileStream.hasNext + hasNextBlock catch
  EOFException → false, DataFileStream.java:215-315) — twmb already
  diverges strict from Java at mid-varint cuts. Gate verdict: not
  documented as intentional (pins + foreign-framing anti-truncation
  invariant point strict; pickaxe: wraps land in c2e5170, the original OCF
  commit, untouched since). Sibling sweep: readHeader/decodeMap same wrap
  shape but NewReader has no documented EOF sentinel (uniformity note
  only); root package io.EOF uses are internal JSON-trailing checks. Fix
  sketch: normalize bare io.EOF to a non-Is-EOF error (io.ErrUnexpectedEOF)
  at the four readBlock sites before wrapping. Gap line added to §Open net
  gaps. · FULL round. fastavro RAN (venv 1.12.2: 59+59 differential runs
  0-skip, truncation probes); Java oracle not run (no local JRE; CI
  covers); -race full suite green (102s + 31s); fuzz spot-checks not run
  this round (CPU pacing). Fronts: struct-tag grammar (4 tokenizer sites,
  splitTag/splitFieldTag/parseSchemaTag/parseBracketedValues line-walk +
  tag-edge/tag-grammar net review — clean; the illegal name+inline combo
  resolves position-dependently, embeds name-wins vs regular fields
  inline-wins, but SchemaFor rejects the combo and both resolutions sit
  inside the tag-edge net's documented non-corruption envelope — not
  filed); schema-integer value space (laxInt/intPtrFrom/jsonNumericInt/
  maxDecimalDigits walk + probes: size {-1, 0, 2^62, 2.5, "16"},
  decimal(-1,2)/(3,-1) via SchemaFor, precision 65537, fixed(0) e2e,
  1M-count array<fixed(0)> — every cell a documented posture with µs
  value-time rejects; SemanticError renders "unsupported Avro type" when
  GoType is nil, wording only); OCF io.Reader fault seam (OneByte/DataErr/
  Half wrapper equivalence × 4 codecs green; exhaustive per-prefix
  truncation sweep ×4 codecs → THE FINDING); P18/Y4/P9/B20/P1 grep refresh
  (delegations conform, float-bounds precede converts, Exp capped behind
  decimalScaleLimit, echoes bounded, no new P1 callsites outside
  classified files). CONVERGENCE: counter remains reset (new behavioral
  finding); two consecutive clean full rounds needed once the fix lands.

- 2026-07-07 · 4719830 (fix round for the same-day truncation-sentinel
  finding: 9f2c89c fix+pins, 29a65fb sweep matrix; maintainer ruled
  NORMALIZE TO STRICT) · quarantine n/a (fix round) · the 1 filed
  behavioral FIXED: noEOF (ocf.go) converts a bare
  io.EOF from every mid-structure stream read to io.ErrUnexpectedEOF
  before the %w wrap — the four readBlock sites (size varint, ReadFull
  data, CopyN data incl. partial shortfall, sync) and readHeader's three
  wrap points (magic, metadata chokepoint covering all six decodeMap
  reads, header sync; NewReader has no EOF sentinel — uniformity). The
  count-read clean-end path is the sole bare-io.EOF source, unchanged.
  Red-then-green in-repo: TestRegression_TruncatedBlockHeaderNotEOF
  (3 cuts) + TestRegression_TruncatedLargeBlockDataNotEOF (CopyN zero +
  partial shortfall — the partial cell added on overseer note that CopyN
  returns bare io.EOF on ANY shortfall, unlike ReadFull), all 5 cells
  verified red pre-fix. Existing pins strengthened:
  TestTruncatedBlockSize/Data/SyncMarker now assert !Is(io.EOF) ∧
  Is(io.ErrUnexpectedEOF). Class net: TestMatrix_TruncationTerminalError
  Identity — per-prefix sweep, every byte offset from end-of-header,
  {null, deflate} × spliced count-0 block (skip-arm reads in the sweep),
  boundary cuts must be BARE io.EOF with exact record counts, every
  other cut non-nil ∧ !Is(io.EOF), records never exceed complete blocks.
  NEUTER-VERIFIED: identity noEOF → all 5 regression cells red with the
  exact Is-EOF leak, TestTruncatedBlockSize red, sweep red on both
  codecs at the first zero-bytes cut (null L=42, deflate L=61, "reading
  block size: EOF"); TestTruncatedBlockData/SyncMarker stayed green
  under neuter as documented (partial reads were already
  ErrUnexpectedEOF). Wrap-site census: 12 error-producing stream reads,
  7 noEOF-normalized wrap points, 1 sentinel path, codecs/writer read no
  streams; no function-value captures. Decode doc gains the contract
  sentence; NOT_BUGS #57 records the strict posture (Java
  DataFileStream.java:234/:311 catch source-quoted; fastavro EOFError
  executed; spec _index.md:483-488); §Open net gaps line CLOSED by the
  sweep + strengthened pins. FIX.md walked (items 0-14; item 13's
  "partial reads already safe" claim proven by the neuter run's green
  Data/SyncMarker pins; item 14's reference claims all
  executed-or-quoted this round). Full suite + -race green, fastavro
  differentials 0-skip. Fix lands UNCONVERGED per §Convergence: next
  round quarantines these commits; the two-clean-round rebuild starts
  with the next full round.

- 2026-07-07 · 29a65fb · 4719830..29a65fb quarantined (9f2c89c noEOF fix+
  pins, 29a65fb sweep matrix) — clear: noEOF re-neutered to identity →
  exactly the documented 11-cell red set (5 regression cells,
  TestTruncatedBlockSize, both sweep codecs; Data/SyncMarker pins stayed
  green as documented), wrap-site census re-verified on disk (7 noEOF wrap
  points, count-read the sole bare-EOF sentinel, decodeMap single-caller
  chokepoint, NewAppendWriter reads only via readHeader), no `== io.EOF`
  callers on truncation paths, sweep matrix drives NewReader+Decode and
  asserts both directions plus records-never-exceed. · 1 behavioral FILED,
  NOT FIXED (read-only round): resolved-schema DecodeJSON flips TAG-NAMED
  union branches — decodeJSONResolved's intermediate (json_codec.go:247)
  unwraps the spec's {"branch": value} envelope and w.Encode re-derives the
  branch by first-match, so tagged writer JSON naming a branch that
  type-collides with an earlier sibling silently rewrites branch identity
  (enum-vs-string, two-records, two-enums-shared-symbol, two-fixed-same-
  size, map-vs-record — all verified failing) and changes decoded VALUES
  where reader resolution differs per branch (writer {"E2":"A"}, reader E2
  drops A with default Y: resolved binary + fastavro json_reader+migration
  (EXECUTED) → "Y"; resolved JSON → "A" via flipped E1). Contradicts
  NOT_BUGS #2's "byte-identical to resolved.Decode of the writer binary"
  claim — gate verdict documented-but-contradicted; Java JsonDecoder.
  readIndex (JsonDecoder.java:475) reads the label → exact branch index,
  unknown label throws; spec _index.md:402 makes the envelope name
  normative. Existing pin TestRegression_ResolvedDecodeJSONMatchesBinary
  never catches it: feeds only twmb's own BARE EncodeJSON output (:1515)
  and its one union cell has no colliding branches (B32 held-constant
  axis). Fix sketch VALIDATED then REVERTED: decode the intermediate with
  TaggedUnions (envelope-preserving; binary Encode routes every wrap-key
  shape incl. kind names "string"/"map"/"int"/"bytes" — probed); full
  suite + 490-run fastavro differential green under the trial. Gap line
  added to §Open net gaps. Policy Suspected filed (not counted, reference-
  parity): NewAppendWriter accepts-and-ignores WithMetadata/WithSyncMarker/
  WithSchema — probe-verified silent drop; Java pre-appendTo setMeta also
  never lands (header never rewritten; post-open setMeta throws "already
  open", DataFileWriter.java:93-103), fastavro append-mode metadata kwarg
  EXECUTED dropped; pure reject-vs-document posture call. · FULL round.
  fastavro RAN (venv 1.12.2: 490-run differential green twice — pre- and
  under-trial — plus append/migration probes); Java oracle not run (no
  local JRE; CI covers); -race full suite green (95s+29s); fuzz
  spot-checks not run (CPU pacing). Fronts: OCF writer-side line walk
  (shouldFlush zero-byte third clause seals exactly at the reader's
  count/zeroRun boundaries, Encode no-reassign discard recovery, flush
  framing, Close/Reset poison ordering, writeHeader metadata-cap mirror,
  encodeMap single-block+terminator, deflate Compress never-errors claim
  (bytes.Buffer sink), snappy CRC-of-uncompressed, zstd lazy-decoder
  cap, crypto/rand sync markers, resolveCodec custom-first — clean
  modulo the Suspected); decoder-option cross (LinkedinFloats is
  encode-only 3-arm appendJSONFloat, round-trips green incl. null→NaN
  re-read; TagLogicalTypes inert without TaggedUnions per the deser:234
  gate; DecodeJSON sets slab flags for default-fill parity; resolved
  noWrap/reader-union wrap seams as documented — clean except THE
  finding); errors.go streetlight line-walk (Field/Path render-truncated,
  recordFieldError As-peel drops only wrapper text never sentinel
  identity, ShortBufferError internal vocabulary — clean); atype
  constants vs spec strings (clean); P1/P9/Y4/B20/P18 grep refresh
  (per-file counts match the classified sets, no new files; both P18
  copy-then-delegate sites still fresh-copy — clean). CONVERGENCE
  COUNTER RESET by the filed finding; two consecutive clean full rounds
  needed once the fix lands.

- 2026-07-07 · 29a65fb (fix round for the same-day resolved-JSON
  tagged-union finding: ce13d03 fix+pins, 509010b matrix+policy;
  overseer-verified two-sided repro, ruled TAGGED-PRESERVING INTERMEDIATE,
  symmetric) · quarantine n/a (fix round) · the 1 filed behavioral FIXED:
  decodeJSONResolved decodes its intermediate with TaggedUnions on the
  existing raw custom-free writer view, so the {"branch": value} envelope
  survives the decode AND drives the re-encode's tagged-map dispatch
  (serUnion.tryUnwrapTagged / serNullUnionAt route by exact index) —
  tagged writer JSON keeps the named branch (Java JsonDecoder.readIndex
  parity, SOURCE-QUOTED :475-491 label→findLabel→exact index, unknown
  label throws); bare writer JSON commits to the documented #5/#36
  first-match branch which the envelope then pins, ALIGNING resolved-bare
  with unresolved DecodeJSON where an enum/fixed branch precedes a
  string/bytes sibling (the old Go-type-name re-derivation jumped the
  first-declared branch there — the one observable bare change, toward
  the documented rule). Red-then-green in-repo:
  TestRegression_ResolvedJSONTaggedUnionValueMatchesBinary (binary "Y"
  vs JSON "A" pre-fix) + TestRegression_ResolvedJSONTaggedUnionBranch
  Identity (5 collision shapes, all flipped pre-fix: E→string, R2→R1,
  E2→E1, F2→F1, R→map). Class net closing the §Open net gaps line:
  TestMatrix_ResolvedJSONUnionInputForms — input form {tagged both
  branches, bare} × 11 shapes (the 5 collision shapes + enum-before-
  string + fixed-vs-bytes first-match-restoration + namespaced-fullname
  + two-records-recursive + diamond-shared-enum) × resolution
  {identical-branch, per-branch-divergent (enum-default drop, added
  defaulted field, reordered reader)}, every cell asserting plain +
  TaggedUnions parity with resolved.Decode of the equivalent tagged
  writer wire (NOT_BUGS #2's claim, executable);
  TestMatrix_ResolvedJSONUnionEnvelopeShapedMapValue (schema position
  disambiguates {"map":{"int":3}} — envelope at the union node, map
  content below);
  TestMatrix_ResolvedJSONTaggedUnionWriterDecodeOnlyCustom (raw-view
  property survives the tagged intermediate; reader custom fires,
  domain-typed non-vacuous). NEUTER-VERIFIED (untagged intermediate):
  28 cells red — both pins, every tagged later-branch cell across all
  shapes × resolutions, the map-envelope cell, plus exactly the 4 bare
  first-match-restoration cells (enum-before-string, fixed-vs-bytes ×
  2 resolutions) where the old path violated #5's rule — while every
  canonical bare cell, tagged first-branch cell, and control stayed
  green as documented. fastavro 1.12.2 EXECUTED: json_reader+migration
  "Y" probe re-run + 4 permanent calibration cells
  (TestDifferentialFastavroResolvedJSONUnion; the jsonread oracle op
  gains an optional reader schema — the JSON twin of readresolve).
  NOT_BUGS #2 parity sentence rewritten (tagged-preserving
  intermediate, new pins named); AUDIT_PATTERNS B32 gains the
  held-constant-input-form instance; §Open net gaps line CLOSED.
  Policy (maintainer-adjudicated DOCUMENT): NewAppendWriter doc names
  the ignored WithSchema/WithSyncMarker/WithMetadata (header never
  rewritten); NOT_BUGS #58 records it with Java appendTo/setMeta
  SOURCE-QUOTED (DataFileWriter.java:230-246 / :285-289 / :93-96) and
  the fastavro append-metadata drop EXECUTED (metadata kwarg dropped,
  records readable); pinned by TestAppendWriterIgnoresHeaderOptions.
  FIX.md walked (items 0-14; item 0 pickaxe: the intermediate call was
  last touched by ed7c6c3's raw-view fix — untagged was never a
  deliberate choice; verdict documented-but-contradicted, adjudicated;
  item 3: resolveWriterRaw/Encode(inter) single-site, no siblings;
  item 5: 1 MiB deep tagged nest rejects in 2ms via the recursion cap,
  1 MiB wide accepts in 3ms; item 13's bare-unaffected claim proven by
  the neuter's green canonical bare cells; item 14's reference claims
  all executed-or-quoted this round). Full suite green (with fastavro:
  502 differential runs, 0 skips), -race green (113s+41s), vet clean.
  Fix lands UNCONVERGED per §Convergence: next round quarantines
  ce13d03+509010b; the two-clean-round rebuild starts with the next
  full round.

- 2026-07-07 · 509010b · 29a65fb..509010b quarantined (ce13d03 fix+pins,
  509010b matrix+policy) — clear: decodeJSONResolved's TaggedUnions
  intermediate line-audited (tryUnwrapTagged / serNullUnionAt / serUnion.ser
  fall-through chain; {"null":non-nil} errors loudly on both arities, serNull
  rejects non-nil), position axis probed (tagged envelopes at array-item +
  map-value through resolved DecodeJSON: binary==JSON, no flip), hostile
  battery independently re-run (100k-deep tagged nest rejects 3.6ms, 1 MiB
  wide tagged array accepts 28ms); 509010b doc/policy/pins verified on disk,
  new fastavro differential suite green. · 1 behavioral FILED, NOT FIXED
  (read-only round) — one root cause, TWO sites: internal re-parses assume
  WithLaxNames(nil) subsumes any user lax validator, false for empty name
  components (the only class lax(nil) rejects that a user fn can accept).
  Site 1 resolve.go:68 (3333e9b's custom-free writer view): Resolve()
  HARD-FAILS an already-parsed, wire-valid custom-typed writer whose name
  tree carries an empty component (ns "a..b") — blocks binary resolution
  too; no-custom control resolves. Site 2 cache.go:206-209 (splice-rebuild
  lax retry): both rebuild attempts reject, metadata forms silently degrade
  to the dangling-reference artifact (String()/Canonical() unresolvable
  under ANY opts; s.c/s.full/s.soe stay as-written), reachable TRANSITIVELY
  (parse-2 needs no lax opt). Verified failing:
  TestResolveCustomTypedLaxWriterRealPath (first probe was vacuous — the
  identical-schema pair hit Resolve's canonical fast path; reader must
  differ), TestCacheSpliceLaxRetryInsufficient; controls green (no-custom
  Resolve; lax-nonempty splice re-parses via the documented retry). Gate:
  pickaxe → 3333e9b introduced-in-range, filed as discussion quoting its
  rationale ("a lax-named writer (which already parsed once) re-parses" —
  the counterexample violates the premise, not the goal); NOT_BUGS
  #27/#41/#50 read, none cover; the only custom-free pin locks routing,
  not the validator. Fix sketch: accept-everything validator for internal
  re-parses at both sites (library-produced text from an already-validated
  parse; the validator has no safety role there). Gap line added to §Open
  net gaps. · FULL round. fastavro RAN (venv 1.12.2: 495 differential runs,
  0 fail/skip); Java oracle not run (no local JRE; CI covers); fuzz
  spot-check FuzzMatrixCore 40s/207k execs clean; -race full suite green
  (139s+51s). Fronts: schema.go name-system inverse-density walk
  (validName/validFullnameErr strict+lax, dotted-name/namespace-escape/
  inheritance/post-qualification primitive reject — Java Name parity;
  scopedRefKeys shared by all three resolvers verified in code;
  qualifyAliases post-qualification at all 3 sites; buildUnion fwd-ref
  bookkeeping + finalizeUnionNames order-independence incl. the 2-branch
  fast path (unionMissing always carries ser+deser); short-name fallback
  ambiguity guards; finalize phase-2a lenient continues proven unreachable;
  fwd-vs-backward nullunion meta avroType asymmetry proven unobservable via
  consumer analysis — clean except THE finding); resolve-side alias
  matching (findReaderFieldIndex name-first vs Java applyAliases
  alias-first (Schema.java:2154-2187 source-quoted) vs fastavro name-first
  (_read_py.py:533-542 source-quoted) — the divergent shape is
  parse-REJECTED per NOT_BUGS #50, independently re-derived; natural-JSON
  field-alias leniency pinned both directions — clean); P1/P9/Y4/B20/P18
  refresh (production text since 29a65fb = one line + comments ⇒ hit sets
  identical to last round's verified-clean; per-file snapshot recorded).
  CONVERGENCE COUNTER RESET by the filed finding; two consecutive clean
  full rounds needed once the fix lands.

- 2026-07-08 · 9283a9d · fix round for the 2026-07-07 finding (overseer
  ruling: NORMALIZE INTERNAL RE-PARSES TO ACCEPT-ALL); next round
  quarantines 509010b..9283a9d — resolve.go custom-free view + cache.go
  splice retry now parse with shared internalReparseNames (rationale on
  the helper; the retry appends it LAST so it wins over a user lax fn —
  broadens only). Census closed: zero WithLaxNames(nil) call sites in
  non-test code; remaining internal Parse sites classified (SchemaFor /
  SchemaNode.Schema / ocf = documented user-opts boundaries; splice
  first-attempt = by-design caller opts). Pins red-then-green:
  TestRegression_ResolveCustomTypedLaxWriterView (pre-fix red: "building
  custom-free writer view ... invalid record namespace "a..b": name
  must be non-empty"), TestRegression_CacheSpliceTransitiveLaxNames
  (pre-fix red: String() dangling "ok.Wrapper", 3-parse chain, no lax
  opt after parse-1). Same-round sweep findings, all FIXED:
  [sibling-of-fix] splice walkers' name!="" def guards conflated "no
  name key" with "empty name" — fullname "ok." (empty short name,
  referenceable by exact dotted lookup) never collected, splice never
  fired even under accept-all; TestRegression_CacheSpliceEmptyShortName
  red-then-green; shared nodeHasStringName predicate across
  collectTreeDefs/inlineTreeDefs/inlineNodeContainers.
  [sibling-of-fix] Resolve raw-view trigger len(writer.custom)==0 for
  cache parses whose customs match only inherited subtrees (wraps live
  in the inherited ser/deser composition; applyCustomTypes visits only
  new nodes) — 3333e9b decode-only re-encode failure resurrected through
  the cache door; Schema.customBaked trigger (overlay OR inherited
  hadCustomType ref, set in tryAssignNamedRef, OR-merged in unnest).
  [exposed-by-fix] hadCustomType stamped only when wired-this-parse —
  transitive custom chains (define+ct → wrap+ct → reference+ct) rejected
  with the unsatisfiable "re-parse with the CustomType first"; finalize
  now also stamps per defined type with the guard's own
  findCustomTypeMatchInSubtree (wildcard exclusion preserved; wildcard
  consistency pin green). Class net PERMANENT:
  TestMatrix_InternalReparseLaxNames 49 cells — {resolve-view,
  cache-splice} × {strict, lax-nonempty, empty-component, empty-name
  "ok."} × {custom none/decode-only/enc+dec} × {direct+recursive,
  transitive+diamond} + outer-lax cell; every cell = parity with the
  original parse (Resolve, String()/Canonical() re-parse under the
  user's validator, resolved DecodeJSON == binary == exact want, wire +
  Rabin == no-custom/directly-parsed twin, fullname verbatim in
  canonical); TestMatrix_InternalReparseBareEmptyName pins bare-""
  verdicts (definable, structurally unreferenceable "not a primitive" —
  original-parse behavior upstream of validators) + the OBSERVED
  pre-existing canonical name-key omission for a bare-empty-name root
  (pinned as-is, surfaced for adjudication; only reachable under a user
  lax fn). NEUTER-VERIFIED ×4, disjoint site-matched red sets:
  validator→20 cells+3 pins (resolve custom-none + outer-lax +
  strict/lax-nonempty all green); guards→6 cache emptyname cells+1 pin;
  trigger→8 cache decodeonly cells (encdec green via invertibility);
  stamp→8 cache transitive custom cells. FIX.md walked items 0-14
  (item 0: not-documented — the fix implements 3333e9b's quoted intent,
  gate artifact in-conversation; item 3 name-walk: splice-walker family
  + customBaked consumers enumerated; item 5: stamp walk bounded by the
  build depth cap, no new big-number paths; item 12: internalReparseNames
  + nodeHasStringName extractions; item 13: "encdec invertible" and
  "custom-none unaffected" claims PROVEN by neuter greens; item 14: the
  one reference-impl claim in a new comment rewritten to the library's
  own contract — no reference claims shipped). §Open net gaps: lax-view
  line CLOSED; NEW line FILED with executed evidence — resolved decode
  drops the READER's custom on cache-inherited subtrees under evolution
  (probe: direct ctLong{7} vs resolved raw int64(7); resolveCtx.custom
  overlay incomplete — reader-side twin of the fixed trigger), FILED NOT
  FIXED (scope-ruled); bare-ref-as-schema writer hypothesis probed and
  REFUTED (safe control). NOT_BUGS #59 records accept-all-by-design
  (WithLaxNames contract + 3333e9b rationale quoted). Full suite green,
  -race green (97s+30s), vet clean, goimports clean. fastavro RAN (venv
  1.12.2: differential battery 8 top-level tests all green, 0 skips) —
  wire bytes additionally twin-asserted in every matrix cell; Java
  oracle not run (no local JRE; CI covers). · 1 behavioral FILED, NOT
  FIXED (reader-side custom drop, above) · CONVERGENCE COUNTER stays
  reset; the two-clean-round rebuild starts with the next full round.
  ADDENDUM (2026-07-09, same round, overseer-ruled): the reader-side
  finding FIXED — tryAssignNamedRef completes the per-parse custom
  overlay for cross-parse inherited subtrees (overlayInheritedCustom,
  walking exactly like findCustomTypeMatchInSubtree and inserting the
  pure wiring via buildCustomWiring, the extracted no-mutation half of
  applyCustomTypes — one detection rule, no fork), so resolveCtx.custom
  re-applies reader customs on rebuilt inherited nodes. Pin
  red-then-green: TestRegression_ResolvedDecodeCacheInheritedReaderCustom
  (pre-fix red on BOTH wire formats: resolved {"f":7} raw vs want
  ctLong{7}; direct-decode control green). Bare-ref-writer control
  PINNED green (TestRegression_BareRefWriterCustomControl). Matrix
  extended permanent: TestMatrix_CacheReaderInheritedCustomResolve, 12
  cells — {direct, transitive} × {decode-only, enc+dec} × {evolution:
  added-field (custom-matched long DEFAULT through defaultOp's wrap),
  promotion int→long (promoted-node wrap + suppression gate), reorder
  (record rebuild + direct-reuse wrap)} — each asserting resolved ==
  direct on value AND type plus twin wire-byte identity. CONSOLIDATION:
  hadCustomType stamping unified to the per-type guard predicate
  (findCustomTypeMatchInSubtree) unconditionally; hasCustomTypeWired
  DELETED — the overlay completion made the wired-based coarse arm both
  redundant (completion wires the referencing parse) and NEWLY
  over-broad (it would have stamped self-clean sibling types whenever
  completion wired inherited entries, false-rejecting later no-custom
  references); per-type stamping is precise in both directions and keeps
  the wildcard exclusion (wildcard pin green). NEUTER ×2 on the final
  shape: completion-off → exactly the 12 reader cells + reader pin red
  (raw-value shapes), writer-side matrix + controls green (mandated
  set); stamp-off → exactly the 29 hadCustomType-dependent cells red
  (16 writer cache-custom + 12 reader + the wildcard test's non-wildcard
  control), wildcard-consistent subtest green. NOT_BUGS #60 written for
  the bare-empty-name Canonical() omission — with the ruling's premise
  CORRECTED BY EXECUTION (fastavro 1.12.2 PARSES the shape and its PCF
  keeps "name":"" — bare-class rabin diverges 3d741707ff4bfa45 vs twmb;
  the "ok." and "a..b" classes are fingerprint-IDENTICAL with fastavro,
  executed) — divergence recorded-not-defended, flagged for
  re-adjudication. Reader-side §Open net gaps line CLOSED. FIX.md walked
  for the new fix (item 0 not-documented — resolve.go's own comment
  declares direct-vs-resolved divergence a bug; item 6: the stamp
  unification LOOSENS only the coarse over-stamp direction, no test
  pinned it; item 13: "wraps ride the inherited composition" proven by
  the direct-decode controls; item 14: fastavro claims executed, Java
  marked unverified). Full suite green, -race green (95s+29s), vet +
  goimports clean; fastavro differential re-run green on the final
  state. Round commits: 9283a9d (names + writer-side custom fixes),
  642f53f (reader-side overlay completion + stamp unification). Next
  round quarantines 509010b..642f53f.
  ADDENDUM 2 (2026-07-09, same round, overseer re-adjudication of #60 on
  the executed evidence): RULING FLIPPED to EMIT "name":"" for the
  bare-empty-name class — landed as 27c8781. appendCanonObject emits the
  name key for named KINDS regardless of value (the omission conflated
  structurally-unnamed with empty-named and fingerprinted like nothing
  else); pin flipped red-then-green to fastavro's EXECUTED bytes+rabin
  (compare BYTES — fastavro prints big-endian hex, twmb returns LE).
  Emission-idiom census (mandated): FIXED — schema_canonical.go
  appendCanonObject (the ruled site), toJSONWalk name-key + namespace +
  cycle-ref + dedup arms (fullname-expressible keying: "ns." is a valid
  recursive/diamond ref target, fullname "" keeps the cycle error),
  nsForChildren, collectNamedTypes, nodeFromJSONObject namespace
  inheritance — pre-fix Root().Schema() of the "ok." class silently
  rebuilt the WRONG schema, recursive/diamond "ok." hard-failed, and a
  named child inside an empty-named parent lost its scope
  (TestRegression_SchemaNodeRebuildEmptyNames, red-then-green).
  INERT-classified (reasoning recorded): canon first-occurrence
  machinery (schema.go collectCanonDefs/rewriteCanonFirstOcc — operates
  on canon-tree FULLNAMES, "ok." nonempty there; fullname-"" defs are
  unreferenceable), findCustomTypeMatchInSubtree location string
  (cosmetic), union duplicate-branch keys (node.name is the fullname;
  bare-"" falls to kind, collision-free — duplicate-"" already
  parse-errors), wrapped-ref dispatch (defs carry structural keys),
  toJSONWalk bare-primitive collapse (!isNamedKind). CENSUS-NOTED, NOT
  FIXED (no ruling; separate surface): TaggedUnions branch-name fallback
  for an empty-named union branch (schema.go buildUnion bn/ln fallback)
  — JSON tagged-form naming edge; FILED as a §Open net gaps line
  (overseer-ruled: future round characterizes with executed evidence
  incl. fastavro's tagged form on an empty fullname, then adjudicates). nodeHasStringName applies to JSON-map
  trees only; the struct-side equivalent is the named-KIND predicate
  (stated in code comments). Matrix permanent:
  TestMatrix_CanonicalEmptyNameFastavroParity — {bare, ok., a..b} ×
  {root, nested, reference} + recursive, twmb Canonical() byte-equal to
  fastavro's EXECUTED PCF + rabin byte-parity + accept-all re-parse
  idempotence (9 parity cells green pre-fix except the 2 bare cells —
  the executed values doubled as the red discriminator); bare-REFERENCE
  divergence pinned (twmb structurally rejects the "" spelling, fastavro
  accepts+resolves it, executed rabin f9afa0dabf6cd566; re-open on
  concrete user need); fwd-ref pinned to the Java first-occurrence rule
  (fastavro rejects ALL fwd refs — executed UnknownType). Discovered en
  route and folded into #60's record: missing name ≡ empty name for the
  parser (accept-all parses a name-less record), so the OLD omission
  form did re-parse — the divergence was the FINGERPRINT, not
  re-parseability (corrects both prior rounds' wording). NOT_BUGS #60
  rewritten to the final adjudication (all fastavro claims executed;
  Java marked unverified throughout). NEUTER: omission restored →
  exactly the flipped pin + root/bare + nested/bare cells red on
  exact-bytes, all other classes/pins green; restored. Full suite green,
  -race green, vet + goimports clean, fastavro differential green.
  Round commits now: 9283a9d, 642f53f, 27c8781 (canonical empty-name
  emission). Next round quarantines 509010b..27c8781.

- 2026-07-10 · 27c8781 · 509010b..27c8781 quarantined (9283a9d, 642f53f,
  27c8781) — correctness CLEAR: wrap architecture line-audited (build's
  post-build applyCustomTypes re-wraps at reference sites; forward-ref
  fixups via customWrappedSer; overlayInheritedCustom walks exactly the
  guard's four child kinds — fields/items/values/branches is the complete
  schemaNode child set, so no resurrection door), opts-fold last-wins
  verified in applySchemaOpts (splice retry's append-last claim holds),
  isNamedKind covers "error" at every new emission arm, suppression-only
  no-callback custom × cache-inherited × logical promotion EXECUTED green
  (the 12-cell matrix's uncovered intersection: direct == resolved ==
  raw int64 on both wires). · 1 RESOURCE-BOUND (DoS) finding FILED, NOT
  FIXED (read-only round; does NOT reset the behavioral counter per
  §Convergence): parse with a registered CustomType is superlinear —
  (a) finalize's per-defined-type stamp walk (findCustomTypeMatchInSubtree
  per definedNamed entry, schema.go finalize) is O(defs × reachable
  subtree) with DAG amplification: a 694KB backward-ref DAG parses 72ms
  plain vs 636ms with ONE non-matching CustomType, ratio doubling with N
  (2.3x/4.7x/8.8x at 170/342/694KB), profiled 59.7% cumulative in
  findCustomTypeMatchInSubtree; (b) cache path: the boundary guard AND
  overlayInheritedCustom each re-walk the inherited subtree PER REFERENCE
  with fresh visited maps (tryAssignNamedRef) — 20k-field cached type ×
  2k refs (629KB+59KB texts) = 8.6s. The guard half predates the
  quarantined commits; the overlay walk doubled it; the single-parse
  stamp cost class is NEW (pre-fix: hasCustomTypeWired O(defs) flat).
  The prior round's "stamp walk bounded by the build depth cap" immunity
  claim is executed-FALSE — the DAG probe nests 2 levels. Battery gap
  named: no {Parse × WithCustomType × many-defs/many-refs} hostile cells;
  extend the battery with the fix. Fix sketch: share one memo/visited
  across the finalize defs loop and across per-ref walks (registrations
  are fixed within a parse, match-reachability is per-node; overlay
  insertion is already idempotent). NOT_BUGS #48 covers a different
  (maxDepth-bounded, decode-side) cost shape. · §Open net gaps line
  CLOSED (list now empty): empty-named tagged-union branch characterized
  with EXECUTED evidence, adjudicated CLEAN — twmb tags by FULLNAME
  ("ok." / bare ""), byte-identical to fastavro 1.12.2's json_writer
  where it functions (fastavro CANNOT write the bare class: "No key was
  set", falsy-fullname key selection; its json_reader ACCEPTS twmb's ""
  emission; reads "ok." exact-only, rejecting "" and kind tags),
  round-trips both modes, resolved tagged intermediate keeps branch
  identity under per-branch-divergent resolution (enum-default drop),
  the "" input-key fallback for "ok." is the documented unique-short-name
  leniency (unqualified("ok.") = ""), and buildUnion's typ,typ fallback
  arm is unreachable for the class (inline empties carry nodes; ""
  references parse-reject). Permanent nets landed (TEST-ONLY):
  TestMatrix_EmptyNameTaggedUnion — {bare, ok.} × {emission exact bytes,
  plain+tagged decode of own emission, tagged-map encode routing incl.
  fallback + kind-reject, resolved routing}, NEUTER-VERIFIED ×2 with
  disjoint red sets (empty-name→kind fallback reddens exactly the bare
  cells; fullname→short-name reddens exactly ok. + resolved-routing);
  TestDifferentialFastavroJSON gains the live empty-named envelope
  parity + bare-class write-fails/read-accepts CALIBRATION pins. · FULL
  round. Fronts: json_decode.go line-walk (1856 lines, never
  line-walked — inverse density; CLEAN: escaped keys resolve before
  every lookup, EXECUTED with literal-escape probes across union tag +
  field key + map key + non-symbol reject + escaped/plain dup last-wins;
  map-reuse semantics shared across binary/JSON/resolve via
  reuseOrMakeStringAnyMap; map keys are raw strings on BOTH encode and
  decode — no text-interface asymmetry; union dispatch commit rules,
  32-bit native-arm narrowing gates, and the alias-collision guard on
  resolved spellings all verified as documented); maybeWrapResolvedNode
  shallow-copy immunity EXECUTED (fixed+enum decode-only customs ×
  resolution × both wires == direct decode — resolved nodes are deser
  carriers; size/symbols/logical never consulted); P1/P9/Y4/B20/P18
  grep refresh (hit sets confined to the classified files; the
  quarantined diffs add no pattern callsites — read line-by-line). ·
  fastavro RAN (venv 1.12.2: full suite + all differentials incl. the
  new cells; ~10 characterization executions via the oracle). Java
  oracle not run (no local JRE; CI covers). -race green (96s+29s).
  gofmt flags two pre-existing files (decimal_roundtrip_test.go,
  schema_for_test.go), untouched — formatting-only, predates the round.
  Round repo changes: two test nets + this ledger/gap edit; ZERO
  production edits; nothing committed (working tree, maintainer to
  commit). CONVERGENCE: 0 behavioral findings — FIRST clean full round
  toward re-convergence; one more clean full round converges the walk.
  The filed DoS fix, when applied, quarantines its own commits without
  resetting the behavioral counter.
  ADDENDUM (2026-07-10, same round, overseer-ruled: MEMOIZE PER SKETCH;
  overseer independently reproduced the quadratic — 14.2x at N=2000 on a
  chain shape): the DoS finding FIXED same round as 8ee2b1b (fix + C9
  battery cells); the round's two characterization nets committed
  separately as 5b8ce9d. COST PINS FIRST, red-then-green with absolute
  bounds (battery convention, no ratios) — dos_battery_test.go gains the
  C9 registration-scaled-parse-cost class:
  Parse/chain-noMatch-custom (3000-type backward-reference chain, 189KB,
  bound 200ms: pre-fix 923ms RED, post-fix ~20ms),
  Parse/chain-matching-custom (same chain, matching CustomType, bound
  200ms: pre-fix 702ms RED — per-type walks short-circuited only at the
  chain bottom, post-fix ~21ms), SchemaCache.Parse/many-refs-custom
  (1000 refs × 5000-field cached type, bound 400ms: pre-fix 1.48s RED,
  post-fix ~33ms). The battery-gap closure the finding named. THE FIX:
  b.customMatch (node → matched-custom verdict string; presence marks
  computed, "" valid; string kept so the guard error still names a
  matched type) allocated by applySchemaOpts iff CustomTypes registered
  — after which b.customTypes is never appended (the memo's correctness
  invariant, stated in code) — and shared by reference across nest();
  consulted by the finalize stamping loop and the cache boundary guard
  through customMatchInSubtree / the findCustomTypeMatchInSubtree shim
  (walk body renamed ...Walk, internals untouched). Exactness on cyclic
  graphs via two write rules (in-code): clean top-level completion
  writes "" for the whole visited set (transitive reachability ⇒ every
  visited node's reachable set ⊆ the proven-match-free root set); a
  match writes exactly the unwind stack (each stack node reaches the
  match); mid-walk bubbled "" is NEVER written per-node (back-edge to a
  stack ancestor could still reach a later match). Linearity for shared
  regions: sharing requires NAMED types, and every named type gets its
  own definedNamed walk whose clean completion memoizes the region.
  overlayInheritedCustom's visited becomes per-parse b.overlayDone
  shared across references (idempotent + order-independent within a
  parse: customTypes fixed ⇒ buildCustomWiring deterministic, existing
  entries kept — soundness stated in the helper doc). FIX.md items 0-14
  walked (0: not-documented — pure cost class, one unpinned variance:
  WHICH reachable matched-type name a guard error carries in
  multi-match diamonds, string stays truthful; 3: call-site census —
  finalize + guard on the wrapper, walk recursion through the shim,
  single overlay call site on the shared set; 5 BOTH AXES: no-match
  19.8ms / matching 20.7ms / DAG ratios 1.0-1.1x at N=500/1000/2000
  (were 2.3/4.7/8.8x) / 20k×2k cache 8.6s→124ms; 13: every immunity
  claim in the new comments executed or code-verified — customTypes
  append census (:258 only), stamp-runs-after-fixups order, overlay
  idempotence guard in code, full-suite green + neuter equivalence for
  memo-vs-fresh verdicts; 14: no reference-impl claims added).
  NEUTER-VERIFIED: both sites reverted to fresh per-call maps → exactly
  the 3 C9 cells red (515ms/629ms/1.26s vs bounds), custom/cache
  correctness suites (internal-reparse 49-cell matrix, reader-inherited
  12-cell matrix, splice + guard + wildcard pins) all green — the memo
  changes cost only; restored. Same-round quarantine of the patched
  functions: final shape re-read end-to-end; memo-hit short-circuit
  cannot break cycle detection (memoized nodes never recurse); clean-
  write loop covers only walk-marked nodes (memo-hit nodes already have
  entries); diamond/recursive/transitive/re-register custom cells green
  via the suite. Full suite + fastavro (502+ differential runs, 0 skip)
  green, -race green (133s+38s), vet + goimports clean. Round commits:
  5b8ce9d (nets), 8ee2b1b (fix + C9). NOT pushed. CONVERGENCE UNCHANGED:
  resource-bound fix, behavioral counter stays at clean round #1 of 2;
  next round quarantines 27c8781..8ee2b1b.

- 2026-07-11 · 8ee2b1b · 27c8781..8ee2b1b quarantined (5b8ce9d
  characterization nets, 8ee2b1b memo fix + C9 cells) — clear:
  customMatchInSubtree / findCustomTypeMatchInSubtree / Walk line-audited
  on disk with the write-rule exactness re-derived by induction (children
  route through the memo-checking step so visited ∩ memoized = ∅ at walk
  time; match writes and the clean-completion write are mutually exclusive
  per walk since any match aborts cleanness; mid-walk "" never written —
  the back-edge-to-stack-ancestor hazard; verdicts immutable because
  b.customTypes append census = applySchemaOpts :258 only and inherited
  nodes never mutate in-parse, wiring goes into b.custom not the node);
  stamp-after-fixups order re-verified (finalize's stamp loop runs after
  phases 1/1b/2); overlayInheritedCustom marks-then-wires with no early
  exit so shared-overlayDone skipping is sound; walk-consumer census
  re-run (finalize + boundary guard + recursion shim, no other callers);
  C9 battery cells green in-suite (0.13s). 5b8ce9d test-only, its live
  fastavro parity cells executed this round. · 0 behavioral · fastavro
  RAN (venv 1.12.2, full suite green with 984 differential RUN/PASS
  lines, 0 skips); Java oracle not run (no local JRE; CI covers — note
  these commits are NOT pushed, so CI has not yet run at this HEAD);
  -race full suite green (97s+29s); fuzz spot-checks not run (CPU
  pacing). FULL round. Fronts: deser.go line-walk (2450 lines, first
  dedicated walk — inverse density: readLength/readBlockHeader int64
  bounds before narrowing, checkArray/MapBlockBounds shared-guard family,
  slab aliasing, null-union varint tolerance, enum/fixed/decimal/
  big-decimal/UUID target arms against the shared setter-helper
  contracts, setFloatValue's float-space bound pre-conversion, P18
  fixed-decimal fresh-copy sites — clean; the deserRecord (nil,nil) tail
  proven unreachable: compileFastDeser returns nil only on
  typeFieldMapping error, and every struct target reaching the arm is
  addressable via the pointer-rooted decode chain); unsafe.go full walk
  (compile-gate exclusion sets per P14 sibling analysis — encode's
  nilable-inner decline vs decode's pointer-cap decline each justified by
  that side's semantics with reflect fallback correct in all declined
  cells; stringFastPathEligible predicates the single source of truth,
  pointer-receiver TextUnmarshaler caught via PointerTo; depth-uniformity
  invariants verified at every union/record/array node incl. the hoisted
  loop-invariant union guards and deserRecordFast's documented non-bump;
  udArrayBlocks int narrowing pre-bounded by the block-bounds guard;
  integer width dispatch bounds correct per width/sign/platform;
  time-logical check asymmetries justified by representability —
  usTimeMicros matches serTimeMicros, both no-range-check — clean);
  schema_for.go B21 walk (uuidForm memo-bit has no decimal sibling gap —
  decimal is big.Rat-only, rejected on every other Go type; seen[]
  fullname-for-records / short-name-for-fixeds registration is internally
  consistent pairs; dedupNamedTypes is the single name-collision net;
  UseNumber + dec.More() default guard) + 4 executed sandbox probes
  (uuid form-flip re-definition round-trips, same-Go-name both-forms
  collision rejects, odd-corners struct round-trips incl.
  uint64>MaxInt64 encode-reject, fixed short-name reference resolves
  under WithNamespace) — clean; P1/P9/Y4/B20/P18 grep refresh (hit sets
  confined to the classified files; both P18 synthesized-buffer sites
  still fresh-copy with discarded remainder). CONVERGENCE: 0 behavioral —
  SECOND consecutive clean full round; per §Convergence the walk
  RE-CONVERGES and the feature freeze lifts (modulo Java-oracle areas,
  which run in CI once the pending commits push).

- 2026-07-12 · 8ee2b1b · empty (no code commits since 8ee2b1b; untracked
  framework docs only) · 1 behavioral FILED, NOT FIXED (read-only round):
  appendAvroString's AppendText inline-write backfill (ser.go:1015-1038)
  trusts the user appender's returned slice — a contract-violating
  AppendText returning a fresh short slice PANICS Schema.Encode (slice
  bounds [11:1], verified failing TestRegression_AppendTextContract
  ViolationNoPanic via a record's second string field), and a fresh long
  slice silently REPLACES previously-encoded sibling-field bytes (err=nil,
  corrupt wire — decode short-buffer; executed). Gate: not documented
  (BUG_AUDIT text-interface entry covers precedence/fast-path eligibility
  only; TestRegression_TextAppenderHeaderGrowth pins well-behaved growth;
  pickaxe: backfill landed in 01ea87a, untouched since). Reference
  EXECUTED both ways: Go encoding/json/v2 (go1.26.2 GOEXPERIMENT=jsonv2)
  panics IDENTICALLY on the same appender ([33:1] in jsontext.AppendRaw);
  json v1 immune (never calls AppendText); decode-side TextUnmarshaler +
  textValue's AppendText(nil) immune (no arithmetic on user-returned
  slices — sibling sweep: ser.go:1024 is the only backfill site). Policy
  fork for adjudication: minimal len(dst)<mark+hdrLen guard → SemanticError
  (kills the panic class; the fresh-long silent-replace residual needs a
  per-encode prefix memcmp nobody pays — document it) vs document whole
  posture as #40-family stdlib-parity trust. · FULL round (first of the
  fresh post-convergence cycle). Fronts: ser.go dedicated full line-walk
  (2786 lines, first dedicated walk — inverse density; THE finding;
  otherwise clean: union tagged/nil-first/name/try-each dispatch, serNull
  UnionAt depth+tagged arms, isNilValue/isNilableKind lockstep, floatFits
  *From mantissa bounds, appendAvro{Int,Long,Float32,Float64} arm parity,
  enum wide-compare + text precedence, array/map primitive hoists byte-
  identical to reflect arms, zero-byte compliance shared, serSize/
  serDuration/UUID trust rules, decimal pipeline incl. finiteScale
  power-of-5 derivation + bigIntToBytes boundary values -128/-255/-256);
  resolve.go dedicated full line-walk (990 lines, first dedicated walk:
  fast-path ordering, custom-free writer view + customBaked trigger,
  cycle-placeholder trampoline copy, resolveRecord alias-collision
  fail-fast + default-fill custom wrap, resolveEnum mapping/identity,
  array/map minItemBytes-from-WRITER, writer/reader/union-union
  wrap+noWrap seams, encodeDefaultDepth all arms incl. union declaration-
  order-picker agreement + recursion bound — clean; deserStruct's
  default-bytes copy vs interface/map direct-pass asymmetry noted
  harmless under the established never-alias invariant); custom_type.go
  dedicated line-walk (361 lines: NewCustomType convert-not-assert,
  setCustomResult indirect cap, wrapDeserWithCustomDecoders probe/bypass/
  re-decode discipline — clean); findMatchingBranch tier table re-read vs
  NOT_BUGS #44 (clean); P1/P9/Y4/B20/P18 grep refresh (tree byte-identical
  to the 2026-07-11 clean HEAD ⇒ hit sets identical; Y4a's sole hit is a
  comment, Y4b's are the fixed guard shape, both P18 sites the known
  copy pair). §Open net gaps was empty at round start; the finding adds
  none (single-site, sibling-swept). · fastavro RAN (venv rebuilt 1.12.2
  after tmp-cleaner gutting; full suite -v: 515 differential RUN lines,
  0 fastavro-gated skips, 0 fails); Java oracle not run (no local JRE;
  CI covers — NOTE 5b8ce9d/8ee2b1b still unpushed, so CI has NOT
  validated this HEAD); -race full suite green (126s+46s); fuzz
  spot-checks not run (CPU pacing). CONVERGENCE: counter reset by the
  filed behavioral finding PENDING ADJUDICATION — if ruled documented-
  posture (NOT_BUGS entry, stdlib-parity trust), it reclassifies as a
  policy record and the pre-round converged streak stands.
  ADDENDUM (2026-07-12, same round, overseer-ruled: MINIMAL GUARD —
  overseer independently reproduced all three shapes incl. the
  top-level silent-empty): the finding FIXED. appendAvroString gains
  the shrunk-return guard (`len(dst) < mark+hdrLen` after the
  AppendText call → SemanticError wrapping errAppendTextShrunk,
  "AppendText returned a slice shorter than its input"; the error is a
  saved var so union try-each doesn't allocate per attempt). Pin
  red-then-green: TestRegression_AppendTextShortReturnNamedError
  (pre-fix red via recover harness: "Encode panicked, want named
  error: runtime error: slice bounds out of range [11:1]"). Class
  matrix PERMANENT (text_appender_contract_test.go): TestMatrix_
  AppendTextReturnShapes — return shape {legal-append,
  legal-zero-append, fresh-short, fresh-long, fresh-equal-len,
  error-return} × position {top-level, record-second-field,
  record-STRUCT-field (reflect path via the text-method fast-path
  exclusion), array-element, map-value, union-branch}, recover harness
  on every cell (never panic), legal shapes byte-identical to
  plain-string twins (happy path untouched), fresh-short → the named
  SemanticError at the 5 detectable positions, the undetectable silent
  shapes (fresh-long, fresh-equal-len everywhere; fresh-short at
  top-level ONLY, where the 1-byte return equals the placeholder-only
  input and the length information does not exist — the overseer's
  silent-empty shape, golden "00") pinned to exact observed bytes with
  Documenting comments; TestMatrix_AppendTextReturnShapesJSONImmunity —
  same shapes through EncodeJSON at top+record positions: textValue's
  AppendText(nil) has no backfill arithmetic, nothing panics, fresh
  returns emit verbatim (binary↔JSON divergence for violators
  documented). NEUTER-VERIFIED (guard → `false &&`): exactly the pin +
  5 detectable fresh-short cells red — record-second-field [11:1] /
  record-struct-field [11:1] / array-element [12:1] / map-value [3:1]
  recovered panics, union-branch silent x-wire (out=78, err nil) — all
  44 other cells green (legal twins, silent goldens, JSON, top-level
  fresh-short), proving the guard changes ONLY the detectable-short
  class; restored. Happy path free: guard is one compare inside the
  a != nil branch (plain string exits at the stringType fast-out
  before textOutFor); AllocsPerRun executed pre- and post-fix:
  plain-string 0→0, legal-appender 0→0. NOT_BUGS #61 records the
  posture (json/v2 executed panic quoted; memcmp rejected as hot-path
  cost; JSON-emits-verbatim divergence; do-not-extend + do-not-re-file
  guidance). FIX.md walked (0: retrospective gate = the round's
  pre-fix search, not documented; 1: single-arm guard, MarshalText arm
  has no backfill; 2: no schema-parse surface takes a TextAppender,
  decode side takes bytes in — structurally immune both axes; 3:
  AppendText call-site census = ser.go:1030 guarded + reflect.go:159
  AppendText(nil) immune, no method-value captures (grep
  'AppendText[^(]' hits are error-message text only), errAppendTextShrunk
  single-caller by design; 4: unsafe gates exclude text-method types →
  all routes reach the guard, evidenced by the struct-field cells; 5:
  no new unbounded work — one compare, returned size is the caller's
  own allocation; 6: full suite green, TextAppenderHeaderGrowth
  untouched; 7: no doc-string change — conforming-appender behavior
  identical, violation posture lives in NOT_BUGS + Documenting pins;
  8-9: vet/goimports/gofmt clean, -race green; 10: B20 refresh —
  errAppendTextShrunk is static text, no echo; 11: n/a no kind
  dispatch; 12: single-site guard, error extracted to a var; 13:
  immunity claims all pinned — JSON immunity by the matrix, json v1
  never calls AppendText, decode-side no returned-slice arithmetic;
  14: the one reference claim (json/v2 parity) EXECUTED this round,
  panic text quoted in NOT_BUGS #61). Full suite + fastavro
  differential green (515 RUN lines, 0 fastavro-gated skips), -race
  green (143s+51s), vet + gofmt clean. Fix commit: 9733042 (NOT
  pushed; joins 5b8ce9d/8ee2b1b awaiting CI). CONVERGENCE: counter
  reset STANDS (behavioral finding, fixed same round); the rebuild
  needs two consecutive clean full rounds; next round quarantines
  8ee2b1b..9733042.

- 2026-07-12 · 9733042 (second round today, at the fix HEAD) ·
  8ee2b1b..9733042 quarantined (9733042 AppendText shrunk-return guard) —
  clear: guard final shape line-audited (placed after the err return and
  before all backfill arithmetic; hdrLen always 1; post-guard textLen ≥ 0
  and the grow-shift copy bounds re-derived), AppendText call-site census
  re-verified on disk (ser.go:1030 the only backfill site; reflect.go:159
  AppendText(nil) has no returned-slice arithmetic; every other hit is
  comment/error text), neuter re-run reproduced exactly the documented red
  set (pin + the 5 detectable fresh-short cells; JSON-immunity matrix
  green), restored byte-identical. · 1 behavioral FILED, NOT FIXED
  (read-only round): SchemaCache's self-containment walkers never walk
  flat (goavro-style) array/map fields' items/values subtrees —
  flatFieldNamedDef covers named kinds only and the fields-arm fallback
  recurses fo["type"] (the string "array"/"map", a no-op) — while the wire
  parser lifts them (flatFieldNeedsLift covers all six kinds). A
  cross-parse reference inside flat items/values never splices (String()
  dangles "references unknown named type", Canonical()/Rabin diverge from
  the nested-twin spelling → SOE fingerprint divergence), and a named type
  DEFINED inside flat items is parser-registered but never collected into
  c.defs (later parses referencing it never splice). Wire codec fine both
  ways (encode/decode round-trip probe green — parse succeeded, so node
  resolution worked). Verified failing ×3 (sandbox):
  TestRegression_FlatArrayFieldCrossParseRefSplices,
  TestRegression_FlatArrayFieldInlineDefCollected,
  TestRegression_FlatMapFieldCrossParseRefSplices; nested-twin controls
  green. Gate: not documented (#56 covers the Root()/Props posture only;
  the flat matrix's flat==nested canonical guard is single-parse — the
  cross-parse axis was held constant, B32; pickaxe: flatFieldNamedDef
  landed in 26dbe90, the original cache self-containment commit — a 14a
  coverage hole in that commit's own stated invariant, not a chosen
  posture). Sibling sweep: schema_node.go metadata twin unaffected (shared
  flatFieldNeedsLift/flatLiftTypeMap, matrix-pinned); canon walkers operate
  post-lift (probe: flat canonical shows the lifted nested form);
  fixed/enum flat kinds carry no subtree; wrapped-form collapse exact
  (double-wrap parser-rejected — probed); defWithExplicitNamespace
  CI-variant "Name" keys splice consistently (probed). Fix sketch: gate the
  walkers' flat-field arm on flatFieldNeedsLift (the parser's WHEN);
  array/map flat fields recurse lookupCI items/values in the RECORD's
  namespace scope (the lift drops name/namespace for unnamed kinds); named
  kinds keep the existing registration + dup-def handling. §Open net gaps
  line ADDED ({array,map} × {ref,def} × {items,values} cells with the
  fix). · FULL round (first of the post-reset rebuild). Fronts: json_scan.go
  dedicated line-walk (600 lines, first walk — escape delimit/validate
  split proven no-unvalidated-accept-path by sawHighByte × escape-internal
  high-byte case analysis; surrogate-pair bounds arithmetic exact;
  parseJSONInt64 pre-multiply cutoff exact at both int64 boundaries incl.
  the MinInt64 two's-complement wrap; skip↔value number grammar shares the
  one isJSONNumber gate — parseInt64Lenient gates BEFORE strconv, so
  "5."/"1e"/"01"/"+5" reject uniformly; bare special-float dispatch parity
  via shared parseSpecialFloat; caller census: every raw scanner output
  validated downstream — clean); promote.go dedicated line-walk (296
  lines, first walk — promotion table complete vs spec; int→float/double
  mantissa policy = reader-schema contract with natural-decode parity
  (netted); readBytesPrefix bounds-before-narrow Y4-clean; logical-reader
  dispatch complete for every reachable reader logical;
  decimal/big-decimal/uuid promotion arms share the natural setters incl.
  checkDecimalUnscaledLen inside setDecimalValue and its (true,err)
  no-opaque-fallthrough posture — clean); cache.go dedicated full
  line-walk (689 lines, first end-to-end walk — THE finding; otherwise
  clean: normalize-then-dedup UseNumber precision, allowReRegister
  same-string premise, defs first-wins soundness, splice trigger/fallback
  lanes, "ok."/"." edge fullnames, stray-container-key hazard proven
  UNREACHABLE — the per-kind build rejects "invalid X has schema for other
  types", so the walkers' kind-blind container recursion never sees such
  input); schema_canonical.go + varint.go + rabin.go + soe.go small-file
  sweep (PCF key order exact; the "error" kind canonicalizes as "record" —
  probe green + Java SchemaNormalization st.getName() source-quoted,
  fingerprint byte-identical to the record twin; uvarint length-table
  exact for all reachable Len32 indices, tail padding unreachable;
  width-overflow guards per #47; rabin table/update match the spec
  pseudocode; Sum() big-endian is the hash.Hash convention while SOE uses
  the LE s.soe bytes — clean); P1/P9/Y4/B20/P18 grep refresh (hit sets
  identical to the classified sets; the sole production delta since the
  2026-07-11 clean HEAD is the quarantined guard — static error text, no
  echo). · fastavro RAN (venv 1.12.2 at /private/tmp/avro_fastavro_venv:
  567 differential RUN lines, 0 skips, 0 fails); Java oracle not run (no
  local JRE; CI covers — NOTE 5b8ce9d/8ee2b1b/9733042 remain unpushed, so
  CI has NOT validated this HEAD); -race full suite green (141s+51s);
  fuzz spot-checks not run (CPU pacing). CONVERGENCE: counter RESET by
  the filed behavioral finding; two consecutive clean full rounds needed
  once the fix lands.
  ADDENDUM (2026-07-12, same round, overseer-ruled: FIX PER THE FILED
  SKETCH + build the schema-feature × walker parity harness): the
  finding FIXED as b25d878. Both cache walkers' flat-field arms now
  gate on flatFieldNeedsLift — the parser's shared WHEN; the third
  predicate (flatFieldNamedDef) DELETED, zero remaining refs.
  collectTreeDefs recurses flatLiftTypeMap(fo, ts) so named flat kinds
  visit identically and flat array/map items/values are walked;
  inlineNodeContainers keeps the named-kind registration/dup-def arm
  unchanged and recurses unnamed flat fields' items/values in the
  RECORD's namespace scope (the lift drops name/namespace for unnamed
  kinds — stray-"namespace" field prop parses and binds in record
  scope, executed probe). PINS red-then-green
  (cache_canonical_test.go): the three filed tests — FlatArrayField
  CrossParseRefSplices (dotted ref), FlatMapFieldCrossParseRefSplices
  (short ref in record scope), FlatArrayFieldInlineDefCollected — each
  with a nested-twin control subtest green throughout; pre-fix red =
  exactly the filed divergences (Canonical/String dangle, fingerprint
  divergence, Root().Schema() rebuild failure; wire control green).
  THE HARNESS (permanent; the round's real deliverable):
  matrix_feature_walker_test.go TestMatrix_FeatureWalkerParity —
  feature-fragment TABLE (8 rows: six flat-lift kinds +
  flat-array-in-flat-record composition + flat-array-ns-decoy trap,
  decoy.Elem registered as a DIFFERENT shape so a wrong-scope splice
  binds the wrong type instead of merely dangling) × walker-driver
  TABLE (11: wire-parity control, string-reparse, canonical-rabin,
  root-rebuild, cache-ref-into, cache-def-inside,
  resolve-both-directions, resolved-decode-json, resolve-custom-views
  — custom-baked writer forces the custom-free reparse view of the
  FEATURE spelling, custom reader applies a ×10 value-transforming
  decode — compat, soe-roundtrip). 88 cells = 86 run + 2 structural
  n/a (flat-enum/flat-fixed carry no ref subtree, explicit t.Skip).
  Per-cell invariant: parity with the vanilla twin, no hardcoded
  expectations — adding a feature = adding a ROW; resolve rows defeat
  Resolve's identical-canonical fast path via an added defaulted
  field so resolution recurses the feature subtree both directions.
  Landed GREEN post-fix: ZERO additional red cells (no new findings).
  NEUTER-VERIFIED ×2: (a) named-only gate restored (…&& isNamedKind at
  both walker arms) → exactly the 3 pin flat subtests + 8 harness
  cells red ({flat-array, flat-map, flat-array-in-flat-record,
  flat-array-ns-decoy} × {cache-ref-into, cache-def-inside}); all
  named-kind cells, twin controls, and every non-cache driver green
  (85 passes, 2 skips); (b) wrong-scope mutation (nodeNamespace(fo,…)
  at both new arms) → EXACTLY the decoy row's 2 cache cells red,
  splice bound decoy.Elem (y long) where the twin bound ns.Elem
  (x int) and collect registered decoy.D8 so the follow-up dangled —
  the trap row discriminates scope, not just presence; restored
  byte-identical, suite green. FIX.md walked (0: not documented — #56
  is the Root()/Props posture only, the deleted flatFieldNamedDef
  comment ("flat array/map fields define no name") was the
  mis-rationale — true of the field's own name, silent on subtree
  contents; 1/2: the WHEN is the parser's own predicate at every arm,
  wire/metadata/cache axes crossed by the driver table; 3: caller
  census — flatFieldNeedsLift = parser + metadata walker + both cache
  walkers, flatLiftTypeMap = parser + metadata + collectTreeDefs
  (inlineNodeContainers mutates fo in place BY DESIGN — the splice
  writes back into the original tree), no method-value captures; 4:
  the fields-loop fallback arm sees only non-lift fields — stray
  container keys parse-reject, executed ×3 ("invalid array has schema
  for other types"); 5: no new unbounded work — walk coverage over
  already-parse-accepted subtrees, dos battery in-suite green; 6:
  full suite green with no assertion updates needed; 7: SchemaCache's
  "independent of the cache" doc now MORE true, walker docs updated
  in place; 8/9: vet green, touched files gofmt-clean
  (decimal_roundtrip_test.go / schema_for_test.go carry pre-existing
  cosmetic comment-alignment drift, untouched, out of scope); 10:
  diff adds no narrowing/echo/buffer sites; 11: isNamedKind covers
  "error", flatFieldNeedsLift's error→fields alias arm exercised by
  the flat-error row across all 11 drivers; 12: net DRY win — the fix
  DELETES a predicate copy; 13: every immunity claim executed —
  stray-key rejection probed, decoy-namespace binding probed AND
  neuter-(b)-pinned, enum/fixed no-subtree encoded as structural
  skips; 14: no reference-impl behavior claims added —
  "linkedin/goavro" names the format, not a behavior). Full suite +
  fastavro green (503 differential RUN lines, 0 fastavro-gated
  skips), -race green (127s+49s), vet clean. §Open net gaps: the
  finding's line REPLACED by the FEATURE × WALKER census line (the
  remaining feature rows enumerated there). Fix commit: b25d878 (NOT
  pushed; joins 5b8ce9d/8ee2b1b/9733042 awaiting CI). CONVERGENCE:
  counter reset STANDS (behavioral finding, fixed same round); the
  rebuild still needs two consecutive clean full rounds; next round
  quarantines 9733042..b25d878.

- 2026-07-12 · b25d878 (third round today) · empty (no code commits since
  b25d878) · 0 behavioral · DEDICATED CENSUS ROUND (streak-NEUTRAL: not a
  full audit round, does not advance or reset the convergence counter):
  populated the FEATURE × WALKER harness's eight remaining feature
  families, one commit per family (8 commits 1c24aba..44e4ffa) plus the
  harness-header commit c6908cd (the round's HEAD; the audit docs
  themselves stay untracked). Growth: 8 rows/88 cells → 28 rows/308
  cells (285 live + 23
  explicit documented skips; every skip names its structural reason — no
  ref/def position inside a lifted-primitive or childless degenerate
  subtree, nil-sample parseable-but-unusable kinds). Families, each
  landed GREEN (no behavioral findings; every red observed was
  neuter-induced or twin-flip-induced) and each neuter-proven red-then-
  restored against the FULL harness (confinement: only the family's
  cells red, all other rows green): · lax names (4 rows: a..b empty-
  component ns RECURSIVE, bare "" at root, trailing-dot "ok." RECURSIVE,
  weird-chars DIAMOND; twins are split-vs-inline fullname spellings of
  the SAME fullname — explicit-empty-ns escape for bare ""; the ""
  reference stays structurally impossible per #60, documented per row;
  neuter = WithLaxNames ignored entirely → 4×11 red; the earlier fn→
  lax(nil) degrade left weird-chars green, which located the row's real
  dependency — the full-ignore neuter is the honest one) · field-level
  logicalType lift (3 rows = the three #33 shapes; time.Time/*big.Rat
  samples make the lift's EFFECTIVENESS the assertion, not tolerance;
  object-form row defines a named fixed inside the lifted subtree and
  splices it decimal-intact from a later cache parse; neuter =
  liftFieldLogicalIntoType disabled → every encode-bearing cell of all
  3 rows red, canonical/String/root cells rightly immune since PCF
  strips logicals) · case-variant reserved keys (2 rows: record/field
  keys incl. enum dEfault + auto-fill-driving field dEfault, and
  container/logical keys iTems/vAlues/sIze/lOgicalType/pRecision/sCale
  with effective-logical samples; neuter = lookupCI exact-only → 2×11
  red) · wrapped + forward references (3 rows: wrapped-backward,
  bare-forward DIAMOND — canonical re-homes the def to first occurrence
  for both spellings, the B7 position-dependence — and wrapped-forward
  RECURSIVE closing through a wrapped self-ref in a null union; cache
  directions wrap cross-parse refs and forward-defined follows; TWO arm
  neuters: bare-fwd signal hard-fail → diamond 11 + wrapped-fwd's 3
  internal-reparse cells red (Root rebuild and splice text legitimately
  emit ref-before-def layouts — those walkers re-derive fwd support),
  wrapped-block skip → both wrapped rows 11 red each) ·
  aliases-any-string (1 row: digit-start/space/comma/non-ASCII/leading-
  dot/bare-dot type AND field aliases vs alias-free twin; parity IS the
  canonical-strip + wire-inertness, weird strings survive into
  String()/Root()/splice emissions whose re-parses must re-accept;
  neuter = grammar check re-added at build, run twice — full → 11 red,
  field-aliases-only → 10 red (cache-def-inside green: its weird
  aliases are type-level) — both alias positions reach independently;
  alias-MATCHING semantics stay outside the twin-parity shape, pinned
  by the dedicated regressions) · degenerate cardinalities (4 rows:
  empty-fields record and size-0 fixed USABLE — flat-vs-nested twins
  compose the flat lift with the degenerate kind, full driver coverage
  incl. cross-parse def of an empty record — plus zero-symbol enum and
  zero-branch union parseable-but-unusable: nil sample skips the
  encode-bearing drivers (wire-parity + soe-roundtrip gained the
  nil-sample skip), pure schema walkers run, compat self-parity holds
  for the empty union; empty union has one spelling so its twin is the
  same text = independent-parse determinism; neuter = all four
  degenerate accepts rejected → 11+11+5+5 red exactly) · duplicate-key
  last-wins (2 rows: structural keys with decoy firsts — empty fields,
  size 999, wrong symbols, wrong items, decoy.Elem-vs-ns.Elem BOTH
  cached so first-wins WRONG-BINDS rather than dangles — and annotation
  keys namespace/default/order/aliases/doc/logicalType with an
  INVALID first default and a different-logical first; the dups survive
  in the TEXT each walker independently re-decodes; vacuity proof =
  TWIN-FLIP (no production arm of ours implements the collapse —
  stdlib map decode): first-wins twins kill every cross-spelling cell
  (7/5 per row), survivors are per-spelling self-containment cells and
  reader-logical-wins resolution cells, definitionally flip-immune) ·
  implicit null defaults (1 row: sample OMITS the field so the
  synthesized default drives auto-fill on both wires + SOE; the
  resolve variant LACKS the field so reader-side fill fires both
  directions; cache directions cross the synthesis with a reference
  branch and an inline-definition branch; neuter = synthesis arm
  disabled → the 6 default-consuming cells red, text walkers rightly
  immune). Harness header updated (families enumerated as seeded, skip
  conventions documented). §Open net gaps: the census line CLOSED —
  the section is now EMPTY. Full suite green (avro 14.7s + ocf 2.6s),
  vet clean, harness -race green (1.8s); fastavro/Java oracles not run
  (test-only round, zero production edits — schema.go/schema_node.go
  neuters all restored byte-identical, verified via git diff empty).
  CONVERGENCE: unchanged (streak-neutral round; the counter still
  needs two consecutive clean full rounds since the 2026-07-12 reset).
  Next round quarantines commits after c6908cd.

- 2026-07-12 · afe3b68 (fourth round today — DEDICATED CLEANLINESS pass
  per CLEAN.md; streak-neutral) · no audit sweep (quarantine untouched;
  c6908cd..afe3b68 are this round's own commits: 1 test-infra repair +
  3 behavior-frozen consolidation steps + 1 comment-staleness follow-up) · 0 behavioral changes shipped;
  1 behavioral finding FILED not fixed — collectTreeDefs gates a
  definition's child namespace scope (and the def visit) on a string
  "name" KEY being present where the parser scopes a named KIND's
  children by its namespace attribute regardless (reachable only via a
  WithLaxNames fn accepting "": {"type":"record","namespace":"x",...}
  with no name key registers fullname "x." and scopes children under
  x); nested defs misfile in SchemaCache.defs under enclosing-scoped
  fullnames; executed blast radius: the parser-scoped fullname finds
  nothing to splice (metadata degrades to the dangling ref), and a
  same-cache reference-then-locally-define parse splices the STALE def
  and rewrites its own definition to a reference, so String()/Root()/
  Canonical() describe a field the wire codec rejects (executed: wire
  encodes {z:string} and rejects {i:int} while metadata describes
  Inner{i:int}). AUDIT_PATTERNS B7 second instance (cross-refs B9);
  fix owed to a fix round with red-then-green lax matrix rows
  (absent-name-key × namespace attr × cross-parse ref × ref-then-define
  order). · Consolidation: the JSON-map schema walkers' child
  enumeration (which keys hold children, the flat-form lift split,
  case-insensitive reads, per-position namespace scope — type@enclosing
  vs containers@child) unified onto walkNodeChildren + nodeChildScope
  (new schema_walk.go, built on the existing shared predicates
  lookupCI/ciKey, flatFieldNeedsLift/flatLiftTypeMap, isNamedKind, no
  new predicate copies); converted one walker per commit — 5859d56
  collectTreeDefs (its two-line name-key scope gate left verbatim as
  the documented divergence above; its dead node-type recursion dropped
  with the always-a-string proof), cce617c inlineTreeDefs
  (inlineNodeContainers DELETED into an enumerator-driven
  inlineNodeChildren; unconditional-nodeNamespace child scope became
  nodeChildScope with the equivalence proof), debe71b nodeFromJSONObject
  (metadataField extracted for the per-field attribute work; scope
  computation untouched — passes its existing nsForChildren). Duplicated
  child-enumeration rule 3→1. Equivalence proofs live at nodeChildScope
  / walkNodeChildren: only named kinds may carry name/namespace ("only
  record, enum, and fixed can have a name"), wrapped refs parse only
  childless, per-kind structural-key exclusivity ("invalid <kind> has
  schema for other types") makes enumeration order unobservable, and
  build rejects nil field types so every parseable field fires exactly
  one callback. Residual: collect's divergent scope gate (above) is the
  one corner NOT unified — preserved, documented at the gate, filed.
  Baseline repair first (f336025, test-only): TestDoSBattery_C9 accept-
  bound cells were red under -race even in isolation (~350ms linear vs
  200ms bound — the cells, added in unpushed 8ee2b1b, never ran under
  -race in CI and lacked the ceiling every other wall-clock cell takes
  via raceRelaxed); wantAcceptUnder now applies the ~3s race ceiling
  through a package-avro raceEnabled build-tag mirror; quadratic
  classes the cells guard stay multi-second under -race and the tight
  bounds stay in force normally. · Gates: full suite + full -race green
  before AND after every step (before-state required the C9 repair);
  feature-walker harness green each step; fastavro differential RAN
  each step (venv restored per testdata/oracle/README: /tmp/
  avro_oracle_venv, fastavro 1.12.2); Java oracle not run (no local
  JRE; covered in CI — CI still unvalidated for everything since
  5b8ce9d, all unpushed). Measurement: non-test LOC 23751→23875 (+124,
  of which +98 comment lines — the shared rule's proof surface and the
  divergence documentation; +26 code — the visitor struct and walk are
  the named concept replacing three inline copies), comment lines
  cache.go 299→309 / schema_node.go 758→764 / schema_parse.go 70→72 /
  schema_walk.go 80 (new), exported identifiers 50→50, hot paths
  untouched (parse/metadata-time only, perf-unconstrained per CLEAN.md
  gate 3). Comment deltas were duplication-consolidation (canonical
  site + pointers), not history-narration deletions. CONVERGENCE:
  unchanged (streak-neutral round; the counter still needs two
  consecutive clean full rounds since the 2026-07-12 reset). Next
  round quarantines commits after afe3b68.

- 2026-07-13 · 18988c2 · FIX round for the 2026-07-12 cleanliness
  round's filed behavioral finding (AUDIT_PATTERNS B7 second instance):
  the keyless carve-out in the SchemaCache splice walkers. Ruled: scope children by
  nodeChildScope regardless of name-key presence AND the def visit
  fires for keyless definitions under the parser's fullname ("x.") —
  walker-parity with the parser admits no keyless carve-out. PINS
  FIRST, executed red pre-fix: TestRegression_CacheKeylessDefStaleSplice
  (canonical/String()/Root() described the stale spliced Inner{w:long}
  and the dupDefRef-rewritten local def while the wire implements
  {z:string} — rabin 7998998244c82d32 vs twin f4950ca4dc6f6fd3) and
  TestRegression_CacheKeylessDefCrossParseRef (String() dangled,
  re-parse "unknown type \"x.Inner\""); wire controls green pre-fix
  (divergence was metadata-only, per the filing). Fix (cache.go):
  collectTreeDefs visits every named KIND (fullname via
  nodeFullnameTree — "x.", or inert-unreferenceable "") and consumes
  nodeChildScope; sibling sweep dropped the SAME name-key gate from
  inlineTreeDefs' map-arm and flat-field local-definition
  registrations (a keyless def as-written inside a SPLICED subtree
  must enter seen, or a later ref to its fullname splices a second
  copy and the duplicate-rejecting rebuild degrades to the dangling
  original); nodeHasStringName DELETED (all three users were the
  carve-out); dupDefRef gained the "" guard (no reference spelling —
  second "" definition stays, coherent degrade). Net:
  TestMatrix_CacheKeylessDefCollection — absent-name-key × {namespace
  attr present, absent} × cross-parse ref to the parser fullname ×
  {reference-then-define, define-then-reference} (B7's both-orders
  rule), plus recursive "x." self-ref, "x."+"x.Inner" diamond,
  nested/flat keyless seen-parity diamonds, same-string re-parse
  dupDefRef structural pin; bare-namespace ref+define orders pin the
  parser's duplicate-name rejection (no twin). Test-infra: the
  per-cell battery + nameOnly lifted from TestMatrix_InternalReparse-
  LaxNames' closure to package-level battery/nameOnlyOpts (reused, not
  duplicated). NEUTER-PROOF: full gate restore → exactly the 2 pins +
  6 keyless cells red, remainder of the whole suite green (bare/* and
  ns/definref are fix-neutral controls); partial neuter (collect
  fixed, seen-gates restored) isolates ns/nested-keyless-diamond
  ("unknown type \"n.X\"" dangle) + samestring-reparse (fallback form
  instead of the dupDefRef rewrite) — nested-keyless-diamond is the
  LOCKSTEP guard (green pre-fix and post-fix, red only half-fixed).
  Post-fix FIX.md sweep run: retrospective gate verdict "documented as
  a FILED BUG owed to this fix round" (gate comment + B7 entry quoted;
  NOT_BUGS #59 posture supports; no doc.go conflict); sibling name-walk
  cleared nsForChildren (keyless-aware), canonical emitter (Name != ""
  || isNamedKind), isNamedKind record/error alias set; item-13
  immunity claims pinned (flat cell asserts Root() keyless shape +
  exact canonical + empty-field-name wire 0x0e). · 1 NEW behavioral
  finding FILED, not fixed (B7 THIRD instance, needs adjudication):
  LEADING-DOT names — parser stores dotted names VERBATIM (".x", ".")
  while nodeFullnameTree/nodeNamespace/defWithExplicitNamespace
  collapse the empty leading component to "x"/"" — executed: ".x"
  cross-parse ref dangles, and the FULL stale-splice divergence
  reproduces (canonical x{w:long} vs wire {z:string}) via a bare-"x"
  ref-then-define after a ".x" define; BLOCKING: the parser itself is
  internally inconsistent for leading-dot parents (child registration
  uses parentName[:dot+1] → ".Inner"; ref resolution uses
  namespaceOf(".x")="" → bare sibling ref "Inner" fails "unknown
  type", executed) — which parser rule to mirror (or whether to reject
  the shape at parse) is an overseer call; probes in scratchpad only,
  no repo test pins the divergent behavior. · Gates: full suite green
  (avro 12.1s + 27.2s runs), full -race green (avro 143s, ocf 53s),
  vet clean (all packages); fastavro/Java oracles not run (no
  canonical/fingerprint emission changed for previously-parseable
  schemas — the fix only makes cache METADATA forms match their
  already-correct twins, asserted per-cell against directly-parsed
  twins). CONVERGENCE: counter stays at ZERO (fix round, not a bare
  full round; the streak rebuild resumes with the next bare full
  round). Next round quarantines commits after 18988c2.

- 2026-07-14 · f47083c · FIX round for the 2026-07-13 round's filed
  behavioral finding (AUDIT_PATTERNS B7 THIRD instance): the
  leading-dot name family. Overseer verified the blocker and ruled NORMALIZE AT PARSE:
  one leading dot is the explicit null-namespace escape (".x" builds
  as name "x", null namespace; "." collapses into the adjudicated
  empty-name family), extending qualifyAliases' alias rule and
  matching Java's Name ctor; strict acceptance UNCHANGED
  (twmb-stricter than Java, documented not widened). ITEM-14 GATE RAN
  FIRST, both anchors executed BEFORE any entry: Java Name ctor
  fetched (Schema.java ~1455, release-1.12.0 — lastDot split;
  `if ("".equals(space)) space = null` — quoted in NOT_BUGS #62);
  fastavro 1.12.2 executed on {".x", ".", ".a.b", refs, sibling} —
  VERBATIM PCF ".x" (rabin c69859279c1a5fbe), "." verbatim
  (b1eae635ed69c128), bare-"x" ref REJECTED (UnknownType: x), children
  null-namespace-scoped, ".a.b" verbatim (013f503d468af517). PREMISE
  CORRECTION reported: the ruling's "old form matched nothing" was
  executed-false for definition-only shapes — pre-fix twmb PCF bytes
  MATCHED fastavro's verbatim form there; the self-inconsistency was
  real everywhere else (bare sibling refs unparseable, cross-parse
  dangles, stale splice, nodeFullname vs nodeFullnameTree disagree),
  and post-fix fingerprints for the lax-only spellings move from
  fastavro's verbatim identity to Java's normalized identity —
  recorded honestly in #62. PINS FIRST, all executed red pre-fix:
  TestRegression_LeadingDotSiblingRefResolves (unknown type "Inner"),
  TestRegression_LeadingDotCrossParseRefSplices (String() dangle,
  unknown type ".x"), TestRegression_LeadingDotStaleSpliceHealed
  (pre-fix the divergent parse SUCCEEDED; post-fix ".x" IS fullname
  "x" so the local re-definition duplicate-rejects). Fix: ONE shared
  helper leadingDotName (schema.go; FIX.md item-12 extraction — the
  rule appeared at 3 sites) consumed by the definition build (o.Name
  normalization after validFullnameErr, so strict is untouched),
  scopedRefKeys (all three name resolvers inherit; the "." reference
  is excluded — nothing registers "." — so the ""/"."-type stays
  unreferenceable in every spelling, preserving #60), and nodeFullname
  (SchemaNode keeps as-written spellings; computed identities resolve
  normalized). The parser-internal inconsistency dissolves: lastDot==0
  parents no longer exist post-normalization, so [:dot+1] and
  namespaceOf agree on every remaining name. Cache walkers untouched —
  their split-rejoin already implemented exactly the Name-ctor rule
  (the parser was the outlier). Net:
  TestMatrix_LeadingDotNameNormalization — crossref × ref spelling
  {"x", ".x"}; refdefine × both spellings + definref (duplicate-name
  rejection pins, no twin); same-parse spelling equivalence (definref,
  refdefine/forward, plain-def × ".x"-ref); "." family join asserted
  NUMERICALLY (canonical {"name":""} bytes + #60's rabin
  3d741707ff4bfa45 + {"name":""} twin parity + unreferenceable
  same-parse AND cross-parse); multidot-verbatim three-way agreement
  control (twmb == Java == fastavro, rabin pinned); Root() agreement
  cell (as-written Name ".x"/Type ".x" preserved, Schema() rebuild
  canonical round-trips). Battery/reader helpers lifted to
  package-level runReparseBattery/reparseAddedReader (reused by the
  keyless matrix — no duplication). NEUTER: reverting the collapse at
  all three sites reddens exactly the 3 pins + 10/11 matrix cells
  (multidot control fix-neutral by design), rest of the suite green;
  restored, residue-grepped. Post-fix FIX.md sweep: retrospective gate
  verdict "filed bug + overseer-ruled" (B7 third instance quoted; #27
  alias-escape precedent supports; #60 family joined not contradicted);
  sibling name-walk cleared collectNamedTypes (keys via nodeFullname,
  wire-parity incl. the inert ""-registration), nodeEffNS (already
  collapsed), toJSONWalk's dotted-name namespace-emission gate
  (correct: ".x" re-parses via the escape — agreement cell pins the
  round-trip), namespaceOf/unqualified (operate on normalized
  fullnames; lastDot==0 inputs no longer reachable), qualifyAliases
  (alias rule #27, deliberately untouched), StructOf/dedupNamedTypes
  (generated names, out of lax reach). · Gates: full suite green, full
  -race green (avro 106s, ocf 38s), vet clean (all packages); fastavro
  oracle EXECUTED for this family (values above; the differential
  suite's schemas are unaffected — no leading-dot cells existed).
  CONVERGENCE: counter stays at ZERO (fix round, not a bare full
  round; the streak rebuild resumes with the next bare full round).
  Next round quarantines commits after f47083c.

- 2026-07-14 · f47083c (second round at this HEAD — bare FULL round,
  first of the post-reset rebuild) · empty (no code commits since
  f47083c) · 0 behavioral · fastavro RAN (venv 1.12.2 at /private/tmp/
  avro_fastavro_venv: full suite green under AVRO_FASTAVRO_PYTHON;
  differential families re-run -v — TestDifferentialFastavro* 64 RUN
  lines 0 skips 0 fails, TestDifferentialAcceptance green); Java oracle
  not run (no local JRE; CI covers — NOTE everything since 5b8ce9d
  remains unpushed, so CI has validated none of the last five rounds'
  commits); -race full suite green (117s+46s); fuzz spot-checks RAN
  after three paced-out rounds (FuzzMatrixCore 45s/717,612 execs clean
  at ~16k/s — no saturation; FuzzDecodeEncodeRoundTrip 45s/211,835
  execs clean; no new corpus crashers). Fronts: inverse-density
  dedicated walks over the never-walked files — skip.go 257-line full
  walk (every src[n:] advance bounded: readLength caps length ≤
  len(src), readBlockHeader validateByteSize=true caps byteSize ≤
  len(src); depth charged at record/blocks/union matching the value
  paths; lazy sync.Once field-skip build bounds recursion by data
  depth; union-index error text identical to deser.go:218; caller
  census: buildSkip → resolve.go:322 only; skipToDeser has ZERO
  production callers — test-only 5-line helper, CLEAN.md candidate,
  not behavioral); reflect.go 706-line first dedicated walk
  (typeFieldMapping per-path visited marking + order-independent
  depth/tag dedup + deferred-ambiguity erroring only on
  schema-referenced names with truncForError-bounded echoes;
  indirect's at-cap base acceptance and indirectAlloc's return-as-is
  both converge to downstream type errors; textOutFor
  value-then-pointer method-set discovery matches stdlib semantics
  incl. the non-addressable pointer-receiver fall-through; valueIsZero
  nil-short-circuit precedes the IsZero assertion and pointer-receiver
  IsZero is reached by boxing so Encode(v)==Encode(&v) — clean, netted
  by the embed-shape/AppendText/pointer-indirection matrices);
  logical.go full walk with boundary re-derivations
  (timeToTimestampScaled EXACT at every edge: the adjustment branch
  admits sec=-maxSec-1 with sub-remainder ≥192 — time.UnixMilli(
  MinInt64) round-trips — and rejects <192 exactly at MinInt64;
  -(maxSec+1)*scale < MinInt64 at all three scales so the plain-branch
  reject is exact; the maxSec sub guard exact; timeToDate's
  midnight-UTC Unix() is always an exact 86400 multiple so the
  truncating division is lossless; timeMicrosToDuration
  truncated-division bounds exact both signs; time-of-day arm parity
  census: Duration→time-millis shares durationToTimeMillis on BOTH
  wires — json_codec.go:399 = ser.go/unsafe.go arms — the time.Time
  arm's raw Milliseconds() is provably <MaxInt32 (wall clock <24h),
  and time-micros raw Microseconds() fits long on both wires — clean);
  schema_walk.go independent re-read (kind-keyed child scope incl. the
  wrapped-ref-childless argument, flat dispatch via the parser's own
  flatFieldNeedsLift, per-kind structural-key exclusivity makes
  enumeration order unobservable — clean); atype constants re-confirm
  vs spec spellings. Pattern-grep refresh P1/P9/Y4abc/B20/P18: the
  production delta since the last verified refresh (9733042) is
  exactly {cache,schema,schema_node,schema_parse,schema_walk}.go; ZERO
  new pattern callsites and ZERO new fmt.Errorf echo sites in the
  delta; unchanged files carry the classified hit sets verbatim (Y4a
  comment-only, Y4b the guarded shapes, Y4c empty, P18 both
  fresh-copy sites, P9 bounded helpers only). B10 error-shape parity
  front: verified NETTED — TestRegression_JSONErrorsAreSemanticWith
  FieldPath pins errors.As + Field=="a.b" on all four paths (binary/
  JSON × encode/decode), TestMatrix_JSONEncodeErrorSemanticParity
  per-fragment encode parity, CompatibilityError/SemanticError render
  bounds pinned; unsafe paths structurally share reflect error shapes
  (type-mismatched targets decline the fast-path compile).
  DoS-battery entry-point coverage re-confirmed (Resolve/
  CheckCompatibility/Root/Canonical/String/Fingerprint/RatFromBytes/
  DurationFromBytes/SingleObjectFingerprint cells enumerated + present).
  Planned invalid-UTF-8 front DROPPED at the NOT_BUGS gate (#6 already
  adjudicates it with CI Java differentials — the filing-time filter
  working as designed). CONVERGENCE: 0 behavioral — FIRST clean full
  round since the 2026-07-12 reset; one more clean full round
  re-converges the walk. Next round quarantines commits after f47083c.

- 2026-07-14 · f47083c (third round at this HEAD — bare FULL round) ·
  empty (no code commits since f47083c; tree verified byte-identical via
  empty `git diff -- '*.go'`) · 1 behavioral FILED, NOT FIXED (read-only
  round): stray `precision`/`scale` schema attributes HARD-REJECT at
  parse (schema.go validateLogical fall-through, "invalid scale or
  precision specified") exactly when NO logicalType — or a valid
  non-decimal logical — accompanies them: `{"type":"int","precision":3}`,
  uuid-on-string+precision, timestamp-on-long+precision, record/fixed/
  array+precision all reject, while the SAME stray keys PARSE when the
  logical placement is invalid (unknown logical, decimal-on-int — the
  soft-drop arms early-return before the check), and the FIELD-level
  twin (stray precision, no logicalType) already lands in
  SchemaField.Props as an inert prop (executed probe) — the references'
  posture, so twmb disagrees with itself across levels. fastavro 1.12.2
  EXECUTED: 9/9 ACCEPT; Java SOURCE-QUOTED: LogicalTypes.fromSchemaImpl
  returns null when the logicalType prop is absent so precision is never
  consulted (LogicalTypes.java:127-130; Schema.java:1979
  fromSchemaIgnoreInvalid), extra attrs are props; spec _index.md:43
  "Attributes not defined in this document are permitted as metadata".
  Verified failing: TestRegression_StrayPrecisionScaleParses (5/5 cells
  red, sandbox). Gate: NOT documented (#41 = quoted-value handling, #55
  = recognized-logical bad params — scoped away; no pins — the error
  string appears only in schema.go; pickaxe → ab1f036 initial commit,
  untouched since). Fix sketch: scope the fall-through reject to decimal
  consumption (#55 untouched), decide metadata routing for the newly
  inert keys (schemaReservedKeys currently swallows type-level
  precision/scale out of Props; field level keeps them), B15 axes 3&4
  with the fix + an attribute-placement × kind × level matrix net. ·
  2 POLICY items for adjudication (not counted): (a) structural-key
  exclusivity rejects (record+items, record+size, array+symbols,
  fixed+items — "invalid X has schema for other types") where Java
  (props) + fastavro (EXECUTED 8/8 ACCEPT) accept — but the reject is
  the cache walkers' stated soundness premise (07-12 census: stray-
  container-key hazard proven unreachable BECAUSE of it), rule-1
  territory: recommend keep-strict + NOT_BUGS record, noting the
  emptiness asymmetry (enum+`"fields":[]` and record+`"symbols":[]`
  PARSE — `len>0` guards for arrays vs nil-pointer guards) and the
  primitive-object capture-drop (`{"type":"int","items":"long"}` parses
  with items dropped from metadata where references keep a prop);
  (b) enum-level non-string/null default rejects are fastavro-
  CORROBORATED (EXECUTED: "Default value for enum must be in symbols
  list"; only Java ignores via getOptionalText→null) — recommend
  folding into #54's record; the `""` echo in the reject message
  (json.Unmarshal error ignored) is cosmetic. · FULL round. Fronts:
  schema.go END-TO-END — the never-walked regions of the largest file
  (4334 lines; the name system, canonical machinery, custom-type
  machinery, finalize, and buildUnion had prior dedicated walks):
  Parse/applySchemaOpts/parse/checkSchemaNestingDepth (4*maxDepth
  bracket ceiling re-derived), isNullableUnion + afield capture + the
  field-logical lift re-read, build/buildPrimitive/unionTypeName,
  buildComplex ALL arms (decimal-on-fixed resurrection nil-precision
  hazard PROVEN closed — decimal on bytes/fixed never soft-drops, bad
  params hard-reject before resurrection can fire; duration/uuid
  ser↔deser size-gate parity; canonObj Items/Values repointing; record
  field name↔alias symmetric collision reject; enum default membership
  posture), validateLogical + maxDecimalDigits + logicalSer/Deser
  tables, and the DEFAULT PIPELINE (schema.go:3470-4334) end-to-end
  (walkDefault idempotent-visit contract, firstUnionBranchAccepting
  Default deep-copy branch probing, validateLeaf per-kind arms,
  numericDefault / floatMantissaLimit / parseFloatAcceptOverflow bounds
  re-derived, defaultObjectShape shared shape rule) — clean except THE
  finding; schema_parse.go END-TO-END post-flat-lift-rework (373 lines:
  single-decode O(n) tree + trailing-content check, schemaTypeMismatch
  stdlib-mirror errors, flat-lift WHEN/WHAT via the shared predicates,
  intPtrFrom/laxInt #41 postures, reserved-key extra routing;
  flatFieldNeedsLift map-iteration determinism proven — only one
  defining key can match a declared type) — clean; acceptance-
  differential EXECUTION front: 25 cells twmb-vs-fastavro (9 precision
  + 8 structural-key + 8 attribute-type; field-order rejects are
  Java-directional, doc-number accepted both sides) — THE finding + the
  policy items; P1/P9/Y4abc/B20/P18 refresh (tree byte-identical to
  f47083c ⇒ hit sets identical to the last verified refresh; P9 hit set
  {deser.go ×3, json_decode.go ×1} and both P18 fresh-copy sites
  (deser.go:2338, resolve.go:548) confirmed verbatim). · fastavro RAN
  (venv 1.12.2: full suite green under AVRO_FASTAVRO_PYTHON; 496
  differential RUN lines, 0 skips, 0 fails; ~40 probe executions this
  round); Java oracle NOT run (no local JRE; CI covers — everything
  since 5b8ce9d REMAINS UNPUSHED, so CI has validated none of the last
  six rounds' commits); -race full suite green (104s+35s); fuzz
  spot-checks not run this round (CPU pacing; ran last round).
  CONVERGENCE: counter RESET by the filed behavioral finding PENDING
  ADJUDICATION — if ruled documented-posture (keep-strict + NOT_BUGS
  record), it reclassifies as a policy record, this round becomes clean
  full round #2, and the walk RE-CONVERGES; if ruled fix, the rebuild
  needs two consecutive clean full rounds once the fix lands. Next
  round quarantines commits after f47083c.

- 2026-07-14 · 5717f32 (fix round at f47083c — overseer-adjudicated
  ACCEPT AS INERT METADATA for the 07-14 filed stray-precision/scale
  finding) · quarantine n/a (fix round) · 1 behavioral FIXED same
  round: stray precision/scale are inert metadata under ONE routing
  rule — reserved (consumed into Precision/Scale, #55-validated)
  exactly on recognized-decimal carriers (decimal on bytes/fixed);
  every other placement surfaces them as plain Props on ALL surfaces
  (Root(), CustomType-callback SchemaNode via o.extra→node.props, wire
  extra routing), matching the field level. validateLogical tail
  reject dropped; nodeFromJSONObject consumption + Props loop and
  aobjectFromMap extra loop share decimalConsumesPrecisionScale/
  schemaReservedKeyForObject (drift structurally impossible); the
  generic-primitive build's unconditional nd.precision/nd.scale copy
  DELETED — unvalidated strays no longer inhabit the validated fields
  (decimal-on-int precision -5 was surfacing in SchemaNode.Precision),
  knock-on: differing STRAY precision is inert under Resolve's decimal
  compat check (pinned, with a consumed-params-still-gate control).
  Pre-fix red EXECUTED: TestRegression_StrayPrecisionScaleParses 6/6
  red, TestRegression_BogusLogicalStrayKeysSurfaceAsProps 5/5 red
  (decimal-on-bytes control green). Class net:
  TestMatrix_StrayPrecisionScalePlacement — placement{no-logical,
  unknown-logical, valid-non-decimal (date/ts-millis/uuid/big-decimal/
  duration per kind), decimal-valid, decimal-invalid} × level{type,
  field} × kind{int,bytes,string,long,fixed,record,array}, 70 cells:
  verdict, routing, String() reparse + Root().Schema() rebuild (B15
  axes 3&4), wire-bytes + Canonical() + Rabin identity vs each
  stray-free twin; PCF-strips-undefined CALIBRATED vs fastavro by
  execution (TestMatrix_StrayPrecisionScaleFastavroPCF; direct oracle
  run: int+precision → PCF "int"). NEUTER-PROOFED both components:
  tail-reject restore reddens the parse pins + exactly the 17
  newly-accepting cells (type no-logical ×7, valid-logical ×5, field
  valid-logical ×5 — record/array soft-drop cells correctly stay green
  as pre-fix-accepted); predicate→true reddens the routing pins (all 5
  bogus subtests, incl. the CustomType surface) + 31 type-level stray
  cells; decimal-valid + #55 controls green under BOTH (0 fails).
  TestValidateLogical's two rejection pins (uuid+scale,
  date+precision) were the bug's own pins — flipped to acceptance.
  FIX.md sweep items 0-14 worked: item-0 retrospective gate re-run
  (zero policy-doc hits for the error string — NOT documented, matches
  the filing-round gate); sibling name-walk: the canonical UNSTRIPPED
  emitter mode (appendCanonObject) is test-only and both #59 internal
  reparses use Parse(writer.full) — same parser, no drift surface;
  wrapped-ref extra→shared-node props probe EXECUTED — per-site
  SchemaNode snapshot (def sees own props, each ref its own; no
  clobber observable; strays behave exactly like any custom prop);
  zero new fmt.Errorf echo sites; goimports clean (gofmt -l flags
  decimal_roundtrip_test.go + schema_for_test.go AT HEAD too —
  pre-existing version-skew drift, untouched). · 2 POLICY items
  RECORDED: NOT_BUGS #63 structural-key exclusivity KEEP-STRICT
  (maintainer-adjudicated; census-proven wrong-bind soundness premise;
  fastavro accepts recorded as the known divergence — 4 reject shapes
  + emptiness asymmetry + primitive capture-drop RE-EXECUTED this
  round, twmb/fastavro postures confirmed; edges observed, NOT
  normalized); #54 EXTENDED with enum-LEVEL non-string/null default
  corroboration (twmb reject `enum default "" is not a member of
  symbols` — `""` echo cosmetic — and fastavro reject "Default value
  for enum must be in symbols list", BOTH EXECUTED this round on
  default:3 and default:null; Java-only ignore via getOptionalText→
  null). · fastavro RAN (venv 1.12.2: full suite green under
  AVRO_FASTAVRO_PYTHON post-fix; PCF calibration + ~16 policy/probe
  executions); Java oracle NOT run (no local JRE; CI covers —
  everything since 5b8ce9d INCLUDING this fix remains UNPUSHED, CI
  unvalidated); -race full suite green (98.5s); fuzz not run (CPU
  pacing, ran two rounds back). Round commit: 5717f32 (code+tests;
  NOT_BUGS #63/#54-ext live in the untracked doc). CONVERGENCE: the
  07-14 pending adjudication resolved as FIX — 1 behavioral fixed same
  round; counter stays at ZERO; the streak rebuild needs two
  consecutive clean full rounds. Next round quarantines commits after
  5717f32.

### AUDIT_CORE framework passages compressed (2026-07-14)

Two driver passages that duplicated one-tier-down material were
compressed to pointers; the originals, verbatim:

From §The executable net runs first — oracle bullet list:

**How to run it:** `go test ./...`, plus — with `AVRO_FASTAVRO_PYTHON` set — the fastavro differential, and under `-tags=cisuite` the Java fingerprint differential. It machine-checks whole classes against *independent* oracles:

- **Encode/decode target-type parity (pattern 12)** — `TestInvariant_EncodeDecodeTargetParity` drives the real paths across a schema × Go-type matrix; any *undocumented* asymmetry fails the build.
- **Wire-format parity vs a foreign impl** — `TestDifferentialFastavro` / `TestDifferentialFastavroBinaryLogical` (fastavro) across primitives + bytes/fixed/decimal/uuid/timestamp.
- **Canonical form + Rabin fingerprint** — `TestApacheSchemaTestsVectors` (vendored Apache vectors) and `TestDifferentialJavaFingerprint` (Java `SchemaNormalization`).
- **Numeric boundaries, reflect-vs-unsafe byte identity, metadata↔wire, resolution promotion, SchemaFor round-trip, decimal round-trip, error-message DoS bounds** — the Tier-2 nets.

From §Re-auditing the patched function — checklist bullets:

The high-level checks the patched function compares against:

- its fast/slow twin (the 2-branch optimization, the unsafe-pointer struct fast-path, the per-primitive container specialization),
- its JSON/binary counterpart (`serFoo` ↔ `appendAvroJSON` case, `deserFoo` ↔ `decodeFoo`/`assignFoo`),
- the helper docstring the fix says it "mirrors / matches" (does the fix cover *every* shape the helper handles, or just the bug report's shape? — pattern 14a),
- the dispatchers that route inputs to the patched per-branch handler (do any `continue` / skip predicates still pre-filter inputs the patched handler would now accept? — pattern 15),
- **the cost of the new code path on a hostile 1 MiB input** (does the precision fix's new helper have a length cap before the O(n²) operation? — pattern 16). Required: time a 1 MiB hostile input through each entry point the fix touched and confirm < 100ms rejection.
- **the schema-parse-time validation arm AND the metadata-API observability arm for the same input** -- pattern 1's four-axis rule, both directions (newly-accepted must parse as a field default and surface typed from `Root()`; newly-rejected must reject at parse too). Full probe recipes: AUDIT_PATTERNS.md, "Axes 3 & 4" blind-spot entry.


### Further AUDIT_CORE passages compressed (2026-07-14, second batch)

From §Scope — public-API entry-point coverage bullet list:

Public-API entry points each get a walk regardless of when last touched:

- **Decoding**: `Schema.Decode`, `Schema.DecodeJSON`, `Schema.DecodeSingleObject`. Safe (reflect) and unsafe (struct fast-path) variants for each Go target type.
- **Encoding**: `Schema.Encode`, `Schema.AppendEncode`, `Schema.EncodeJSON`, `Schema.AppendEncodeJSON`, `Schema.EncodeSingleObject`, `Schema.AppendSingleObject`. Same safe/unsafe split.
- **Schema parsing**: `Parse`, `MustParse`, `SchemaCache`, `SchemaFor`, options (`WithLaxNames`, `WithCustomTypes`, etc.).
- **Schema introspection**: `Schema.Root`, `Schema.Canonical`, `Schema.Fingerprint`, `Schema.JSON`, `Schema.String`.
- **Schema resolution / compatibility**: `Resolve`, `CheckCompatibility`.
- **OCF**: `ocf.NewWriter`, `ocf.NewReader`, codec selection (snappy, deflate, zstd), `WithMaxBlockBytes`, `WithCodec`, `WithReaderSchema`, `WithReaderSchemaFunc`, `WithSchemaOpts`.
- **Single Object Encoding**: magic-byte framing, fingerprint endianness.
- **Custom types**: `CustomType`, `WithCustomTypes`, all combinations of nested positions.
- **Logical types**: `decimal`, `uuid`, `date`, `time-millis`, `time-micros`, `timestamp-millis/micros/nanos`, `local-timestamp-*`, `duration`. Both regular and via custom types.
- **Decoder options**: `TaggedUnions`, `TagLogicalTypes`, `LinkedinFloats`.



From §Finding format — Broken-code / User-visible-breakage explanation:

**The "Broken code" block is required, not optional.** A `file:line` pointer alone forces the maintainer to open the file; show the code. Quote the lines verbatim — do not paraphrase, elide, or summarize "the function does X". If the broken behavior spans multiple non-adjacent sites (e.g. four dispatcher arms diverging on one rule), show each broken excerpt with its own `file:line` header so the maintainer sees the divergence side-by-side. For "missing code" bugs (a case that should exist in a switch, a check that should run before an operation), show the switch/function as it sits AND annotate with `// ← missing: <what>` at the spot the new code belongs. For policy-call findings, show the code anyway — the reader needs to see what they're deciding about.

**Every finding also requires a "User-visible breakage" block** — a runnable Go program (or near-runnable snippet, with `import "github.com/twmb/avro"`) showing what a USER of the package writes that doesn't work. NOT internal `serFoo`/`deserFoo` machinery — what the caller types at their keyboard. Format:


From §Finding format — the post-template contract paragraph:


The contract: a maintainer should read the User-visible breakage block alone and immediately understand "this is something a user would file an issue about." If the only thing you can write is "the internal `deserFoo` arm doesn't accept `time.Duration` as a target," the finding either (a) is actually user-facing — find the public API call that surfaces it (probably `s.Decode(buf, &x)` for some user-typed `x`), or (b) is internal cleanup not a finding. Reflect-only / fast-path-only / unsafe-pointer-only internal divergences that no public API exposes are out of scope.

From §Audit conventions, convention 5 — the "Specifically" enumeration:

Specifically: a comment "matches Java's coerceDefault" is not proof Java's coerceDefault behaves the way our code does; a branch marked "unreachable" is not proof it is; a TestRegression name implying a behavior is locked is not proof the test exercises it; a commit message "mirrors helper X" is not proof the patch covers every shape X handles (verify by listing X's docstring's named input shapes and writing a failing test against each); a doc-string "preserves all metadata" is not proof >2^53 integer extras survive. 

### AUDIT_PATTERNS entries tombstoned (2026-07-14)

Net-guarded categories tombstoned per the feedback loop; the operative tombstones name the guarding nets and re-open conditions. The replaced texts, verbatim:

Blind spot B7 -- non-wire path re-derives a parser resolution:

- **A non-wire path re-derives a resolution the parser already computed — and must reproduce it EXACTLY, including position-dependence.** The parser computes name bindings/branch selections/default-branch choices once (eager, in-scope-first, POSITIONAL). Any other path (SchemaCache splice, canonical emitter, metadata walker) either consumes the binding or re-derives it; re-derivation is correct only if it reproduces the parser's rule in FULL — precedence, namespace scoping, AND position/order. A define-before-reference match silently breaks on reference-before-define, so the matrix must cross BOTH orders. Probe: for any re-deriving path, enumerate EVERY dimension of the parser's rule and reproduce all, or consume the binding directly. Instance: `inlineTreeDefs` cache splice matched precedence (`scopedRefKeys`) but used a position-INDEPENDENT "defined locally anywhere" check → a before-the-definition ref stayed bare where the parser bound it to the cached type, so `String()`/`Canonical()`/`Fingerprint()`/`Root()` described a different schema than the wire codec. Fix: track local names positionally, mirroring `registerNamed`'s timing. Second instance (2026-07-12 cleanliness round; FILED, not fixed — the round was behavior-frozen): `collectTreeDefs` gates a definition's child NAMESPACE SCOPE (and the def visit) on a string `name` KEY being present, while the parser scopes a named kind's children by its namespace attribute regardless of name presence — the build registers fullname `"x."` for `{"type":"record","namespace":"x",...}` with NO name key when a WithLaxNames fn accepts `""`, and its children build under `x`. Nested defs then land in `SchemaCache.defs` under enclosing-scoped fullnames (`Inner` instead of `x.Inner`): a later cross-parse reference to the parser-scoped fullname finds nothing to splice (metadata degrades to the dangling ref — B9's coherent-degrade arm), and a later same-cache parse that references-then-locally-defines the misfiled short name splices the STALE def, rewrites its own local definition to a reference (`dupDefRef`), and ships `String()`/`Root()`/`Canonical()` describing a field the wire codec rejects (executed: wire encodes `{z:string}` values and rejects `{i:int}` while the metadata forms describe `Inner{i:int}`). Reach: WithLaxNames-only (an absent name key never parses strictly), multi-parse, same cache. The shared enumerator (`walkNodeChildren`/`nodeChildScope`, schema_walk.go) implements the parser's scope rule; `collectTreeDefs` carried the divergence as a documented residual at its own gate. FIXED (2026-07-13 fix round; ruled: scope children by `nodeChildScope` regardless of name-key presence AND the def visit fires for keyless definitions under the parser's fullname — walker-parity with the parser admits no keyless carve-out): the same name-key gate was also dropped from its two siblings, `inlineTreeDefs`' map-arm and flat-field local-definition registrations (a keyless def arriving AS-WRITTEN inside a spliced subtree must enter `seen` or a later reference to its fullname splices a SECOND copy and the duplicate-rejecting rebuild degrades the metadata to the dangling original — executed: `{Outer2 [a:"n.X"],[b:"n."]}` re-parse failed `unknown type "n.X"` with collect fixed but the seen-gate restored); `nodeHasStringName` deleted (its only purpose was the carve-out); `dupDefRef` gained the `""` guard (no reference spelling exists — a second `""` definition stays in place, coherent degrade). Net: `TestRegression_CacheKeylessDefStaleSplice` + `TestRegression_CacheKeylessDefCrossParseRef` (both executed red pre-fix: metadata described `Inner{w:long}` where the wire implements `{z:string}`; `unknown type "x.Inner"` dangle) and `TestMatrix_CacheKeylessDefCollection` — absent-name-key × {namespace attr present, absent} × cross-parse reference to the parser fullname × {reference-then-define, define-then-reference}, plus recursive (`"x."` self-ref), diamond (`"x."`+`"x.Inner"`), nested/flat keyless seen-parity diamonds, and a same-string re-parse cell pinning the dupDefRef rewrite structurally; the bare-namespace ref+define orders pin the parser's `duplicate named type "Inner"` rejection (no twin exists). Neuter evidence: full gate restore reddens exactly the two pins + 6 keyless cells with the rest of the suite green (`bare/*` and `ns/definref` are fix-neutral controls); the partial neuter (collect fixed, seen-gates restored) isolates `ns/nested-keyless-diamond` + `samestring-reparse` — the nested-diamond cell is a LOCKSTEP guard, green pre-fix (the rebuild's own parser resolves the dangling ref in-tree) and green post-fix, red only in the half-fixed state. Third instance (2026-07-13 post-fix sweep; FILED, not fixed — needs parser-rule adjudication first): the LEADING-DOT name family. The parser stores a dotted name VERBATIM (`".x"` registers `".x"`, `"."` registers `"."` — lax-only, empty first component), while `nodeFullnameTree`/`nodeNamespace` REBUILD short+namespace and collapse the empty leading component (`"" + "." + "x"` → `"x"`), so the def misfiles under `"x"` — executed: cross-parse ref `".x"` dangles (`unknown type ".x"` on String() re-parse; same for `"."`), and the full stale-splice blast radius reproduces (`{Outer2 [a:"x"],[b:{local x{z:string} def}]}` after a `".x"{w:long}` define: canonical describes `x{w:long}`, wire accepts `{z:string}` and rejects `{w:long}`). `defWithExplicitNamespace` shares the collapse (`unqualified(".x")`=`"x"`, `namespaceOf(".x")`=`""` — even a key-side fix would store a def that re-parses to the WRONG fullname), and `nodeChildScope`→`nodeNamespace` feeds all three walkers including Root(). BLOCKING ADJUDICATION: the parser itself is internally inconsistent for leading-dot parents — child registration prefixes `parentName[:dot+1]` (a child of `".x"` registers `".Inner"`) while reference resolution uses `namespaceOf(".x")`=`""` (a bare sibling ref `"Inner"` inside `".x"` fails `unknown type "Inner"`, executed; only the dotted `".Inner"` spelling resolves) — so "mirror the parser" is ill-defined until the parser's intended rule (or a parse-time rejection of the shape) is ruled. FIXED (2026-07-14 fix round; overseer ruled NORMALIZE AT PARSE): a single leading dot is the explicit null-namespace escape — `".x"` builds as name `"x"`, null namespace, and `"."` collapses into the adjudicated empty-name family — extending `qualifyAliases`' alias rule and matching Java's Name ctor (Schema.java ~1455, fetched and quoted in NOT_BUGS #62: lastDot split; `if ("".equals(space)) space = null`); strict acceptance unchanged (twmb-stricter than Java, documented not widened). The rule lives in ONE helper (`leadingDotName`, schema.go) consumed by the definition build, `scopedRefKeys` (all three resolvers — the `"."` reference is excluded so the empty-name type stays unreferenceable in every spelling), and `nodeFullname` (SchemaNode preserves the as-written spelling; every computed identity resolves normalized — the cache walkers' split-rejoin already implemented exactly this rule, which is what made them "collapse": the parser was the outlier). fastavro 1.12.2 executed 2026-07-14: verbatim `".x"` PCF (rabin c69859279c1a5fbe), bare-`"x"` ref rejected, children null-namespace-scoped — a third posture, documented in #62; `".a.b"` is a three-way byte agreement (rabin 013f503d468af517). Net: TestRegression_LeadingDot{SiblingRefResolves,CrossParseRefSplices,StaleSpliceHealed} (all executed red pre-fix) + TestMatrix_LeadingDotNameNormalization (cross-parse × ref spelling {"x", ".x"} × orders; same-parse spelling equivalence both directions; the "." family join asserted numerically against #60's rabin; multi-dot verbatim boundary control; Root()/walker/parser agreement cell). Neuter: reverting the collapse at all three sites reddens exactly the three pins + every matrix cell except the fix-neutral multidot control, remainder of the suite green.

Blind spot B33 -- two-mechanism recursion-depth accounting:

- **Two-mechanism recursion-depth accounting must charge each schema edge once on BOTH mechanisms.** The encoder threads a `depth` PARAMETER (`fn(..., depth+1)`); the decoder/JSON-decoder bump STATEFUL `sl.depth` on container/record/union node entry. The invariant -- one increment per parent→child schema edge, identical on every path -- breaks at seams where a node has a second entry function: a reflect body dispatching to an unsafe fast body (`serRecordFast`/`deserRecordFast`), a compiled field fn re-entering via `*Via`, the encode-side 2-branch null-union optimizers (must guard at the union node AND charge its edge, mirroring `deserNullUnionAt`), and array-element union fast paths (must not skip the union node; enter the inner at depth+2). Symptom: `errTooDeep` trips at a DIFFERENT depth per path; the effective bound silently halves and min(encode,decode) round-trips break -- invisible to round-trip tests (decode only ever sees the depth encode produced). Probe: the depth-uniformity oracle -- `TestDepthUniformityOracle`/`TestDepthUniformityMutual` (hand-assembled wire, independent of the encoder, every recursive shape × every path asserting ONE trip depth), `TestDepthUniformityNestedStructRecord` (the struct-record seam), `TestDepthBoundCyclicContainers`. When a fix touches any increment site, sweep every `depth+1`/`sl.depth++`/`depth >= maxDepth` across ser.go/deser.go/unsafe.go/json_codec.go/json_decode.go/resolve.go/skip.go and classify each as sole-entry (counts once) vs dispatch-hop (must NOT add its own). Instance: unsafe struct-fast encode double-counted the directly-nested struct-record edge. Full history: BUG_AUDIT.md, Distillation archive (2026-07-01).

Blind spot B34 -- bounded tree walk four cost axes:

- **A bounded recursive walk that serializes a tree has FOUR independent cost axes; bounding one is not bounding the others.** (1) STRUCTURAL depth (`toJSONWalk` items/values/branches/fields). (2) Per-node VALUE payloads (Props / field defaults -- arbitrary user `any` trees) descend a SEPARATE recursion into the same `json.Marshal`; the bound must mirror Marshal's FULL recursion (map/slice/array/struct/pointer/interface via a reflect walk, decrementing on EVERY descent so a cyclic Go type terminates, `[]byte`/`[N]byte` short-circuited), not a type-switch on the shapes the parser happens to emit -- a hand-built typed container bypasses the switch. (3) Tree EXPANSION: depth caps path LENGTH, not fan-out -- a shared-reference DAG (same node via Items AND Values; same sub-value under two keys per level) is tiny in memory but 2^depth when emitted, and `visited` is PATH-scoped so off-path sharing is not a cycle; fix is a single node-count budget shared across the WHOLE walk (structural + every value), checked before descent. (4) NESTED serializations inside the walk that allocate a FRESH budget escape the shared bound (the dedup conflict-comparison re-marshalled bodies via `toJSON()` → budget² work from a tiny output); every nested `json.Marshal`/`toJSON`/re-walk reachable from inside a budgeted walk must thread the SAME budget pointer (`toJSONShared`), and an over-budget comparison reports the expansion error, never a spurious conflict. Probes: after bounding any walk, ask all four axes. Pins: `TestRegression_SchemaNodeSchemaDeepValueBounded`, `TestRegression_SchemaNodeWalkDepthAllChannels`, `TestRegression_SchemaNodeSharedDAGExpansionBounded`, `TestRegression_SchemaNodeDuplicateNamedDefinitionBounded`. Fixes live in schema_node.go (`boundedSerializableValue`/`valueNestsTooDeep`, `maxSchemaJSONNodes` + `valueWalkLimit`, `toJSONShared`). Full history: BUG_AUDIT.md, Distillation archive (2026-07-01).

Index line B7:

- B7. A non-wire path re-derives a parser resolution — must reproduce it EXACTLY, including position-dependence (inlineTreeDefs; collectTreeDefs keyless carve-out FIXED; leading-dot name family FIXED by the normalize-at-parse ruling — NOT_BUGS #62)

Index line B33:

- B33. Two-mechanism recursion-depth accounting — one charge per schema edge on BOTH mechanisms; the seam sweep list

Index line B34:

- B34. A bounded tree walk has FOUR cost axes — structural depth, value-payload depth, DAG expansion, nested fresh-budget serializations


### Round narrative (2026-07-14, dedicated distillation + attribute x placement census round)

Round at HEAD 5717f32, quarantine scope empty (no code commits since the
newest ledger line). Deliverables per the round mandate: (0) distillation,
(1) quarantine, (2) the ATTRIBUTE x PLACEMENT acceptance census with oracle
arms and neuter proofs, (3) ledger + commits (not pushed).

DISTILLATION: the round-ledger narratives (2026-07-01 through 2026-07-14,
~113KB) moved verbatim into this archive above; AUDIT_CORE keeps one
compressed convergence line per round, a restored "Feedback loop" section
carrying the size guard (~55KB driver / ~150KB patterns), and compressed
forms of two tier-duplicated passages (the net-oracle bullet list — the
inventory lives in AUDIT_PATTERNS N1 — and the re-audit checklist, whose
playbook is FIX.md) plus three space-driven compressions (entry-point list
to run-in form, finding-format explanation paragraphs, convention 5's
enumeration), every original archived verbatim above. AUDIT_PATTERNS
tombstoned three net-guarded categories: B7 (non-wire path re-derives a
parser resolution — netted by the FEATURE x WALKER harness + the
keyless/leading-dot matrices), B33 (two-mechanism depth accounting —
depth-uniformity oracle suite), B34 (bounded-walk four cost axes —
schema_node budget pins); originals archived above. Sizes: AUDIT_CORE
163,716 -> ~55,000 bytes; AUDIT_PATTERNS 147,594 -> 138,745; BUG_AUDIT
318,922 -> every removed byte appended, loss-checked by containment
assertions at move time.

THE CENSUS (attribute_placement_census_test.go, permanent): every defined
Avro attribute (precision, scale, items, values, symbols, size, fields,
order, default, aliases, namespace, logicalType; the enum-level default is
the "default" attribute's defined placement on enum) x every kind (null,
boolean, int, long, float, double, string, bytes, fixed, enum, record,
error, array, map; + union at the field level — a union has no type-object
form) x level {type object, field object}, skipping defined placements:
272 cells = 152 type-level + 120 field-level. Field-level
order/default/aliases are defined field attributes (skipped); field-level
logicalType is the #33 lift family (netted by the harness's field-logical
rows; skipped with citation). Verdict/routing classes, all pinned per cell
with wire + Canonical() + Rabin identity against the stray-free twin,
String() reparse and Root().Schema() rebuild canonical-stability:

  - 28 cells (type-level precision/scale, all 14 kinds): Props routing —
    the 5717f32 rule generalized; these are the mandated neuter rows.
  - 24 cells: documented #63 structural-key exclusivity rejects
    ("invalid <kind> has schema for other types") — items x
    {map,record,error,enum,fixed}, values x {array,record,error,enum,
    fixed}, symbols x {array,map,record,error,fixed}, size x
    {array,map,record,error,enum}, fields x {array,map,enum,fixed};
    the fastavro arm asserts fastavro ACCEPTS each (the documented
    divergence, recalibrating loudly if a release flips).
  - 2 cells: THE FINDING (below), fixed same round.
  - 98 cells: captured-reserved accepts — structural keys on primitive
    objects surface AS-WRITTEN on the matching SchemaNode field
    (Items/Values/Symbols/Size/Fields — #63's capture-drop edge corrected
    by execution: only Props drops them, the metadata tree surfaces the
    subtree, the rebuild collapses it); order/default at type level
    surface nowhere; aliases/namespace surface on
    SchemaNode.Aliases/.Namespace; logicalType ("date" off-int) surfaces
    as-written while the wire soft-drops.
  - 120 cells: field-level strays land in SchemaField.Props as written
    (items/values/symbols/size/fields/precision/scale/namespace x 15
    kinds), the field's type node stays clean.

Walker-parser agreement cells (TestMatrix_AttributePlacementDroppedSubtree
Agreement): a named type spelled inside a captured-then-dropped
primitive-object subtree is invisible to the parser (same-parse reference
rejects "unknown type") AND to the SchemaCache walkers (cross-parse
reference rejects identically; the fields-array arm agrees) — the
tombstoned B7 rule holds for the dropped shape; executed pre-fix and
pinned.

THE FINDING (1 behavioral, fixed same round as 70c7c1c): a stray
"namespace" attribute on the unnamed container kinds HARD-REJECTED at
parse (schema.go buildComplex else-branch, "only record, enum, and fixed
can have a name") while the SAME key on primitive type objects always
parsed ({"type":"int","namespace":"x"} accepted, surfaced, dropped from
the rebuild) — twmb disagreed with itself across kinds and with both
references. fastavro 1.12.2 EXECUTED: accepts, and treats the attribute as
fully inert (a named type defined under {"type":"array","namespace":"x"}
inside top.R resolves as top.Inner; x.Inner does NOT resolve). Java
SOURCE-QUOTED: name/namespace are in SCHEMA_RESERVED for EVERY schema
object (Schema.java:175-176), arrays/maps parse via
parsePropertiesAndLogicalType with that set (:1940/:1950) — reserved,
ignored, accepted. Gate: NOT documented (no NOT_BUGS entry, no doc.go
text; pickaxe -> ab1f036 initial commit; one UNMARKED 13a rejection pin
found post-fix — TestBuildComplexErrors/"namespace on non-record" matched
err != nil without quoting the error, invisible to the error-string grep;
flipped like 5717f32 flipped TestValidateLogical's pins. Gate lesson:
in-test-pin searches must grep the SHAPE, not just the error string).
Ruling per the cross-implementation divergence policy: rule 1 satisfied
(inert on every twmb surface — the walkers' scope rules are kind-keyed:
nodeChildScope checks isNamedKind before any attribute read; proven by the
scoping cells + the harness decoy row), rule 2 lean-permissive (both
references accept safely) -> ACCEPT AS INERT, the 5717f32 shape. THE SPLIT:
the o.Name half of the guard KEEPS rejecting on array/map — nsForChildren
(schema_node.go) deliberately scopes children by ANY non-empty
SchemaNode.Name (the hand-built-tree posture kept by the keyless-def fix),
so a PARSED stray name on a kind with child positions would let Root()
scope named descendants differently than the wire parser — the same rule-1
walker-parity territory as #63; primitives keep accepting stray name
(childless). Fix: the guard narrows to o.Name; o.Namespace/canonObj.
Namespace are cleared in the unnamed branch (the canonical writer emits
namespace whenever non-nil — without the clear, PCF kept the stray key and
the Rabin fingerprint diverged from the twin AND from fastavro's PCF;
caught red by the census before the clear landed). nodeChildScope's
premise comment updated. Red-then-green: the 2 census namespace cells +
TestRegression_StrayNamespaceOnUnnamedComplexParses (2 cases, failure
"only record, enum, and fixed can have a name" captured) + 2 scoping
cells; TestRegression_StrayNameOnUnnamedComplexKeepsRejecting +
stray-name-cannot-define pin the keep-strict half (fix-neutral controls,
green throughout). Class nets: the census namespace row + the FEATURE x
WALKER harness row "stray-namespace-on-container" (decoy namespace attrs
on array AND map, named def inside the array items, short reference
resolving in the enclosing scope, cache ref/def directions — 11 drivers).
NOT_BUGS #64 records the split posture with the executed/quoted evidence;
#63's capture-drop edge wording corrected by execution (structural keys
surface on the SchemaNode; Props alone drops them).

ORACLE ARMS: fastavro (TestDifferentialFastavroAttributePlacement) —
every census cell executed through the oracle's parse op, 273/273 runs 0
skips 0 fails (272 cells + the namespace-scoping calibration cell);
included in the differential CI job by name (matches the existing
TestDifferentialFastavro -run filter). Java (cisuite,
TestDifferentialJavaAcceptanceAttributePlacement) — representative subset
(8 kinds x all attrs at type level, 5 kinds at field level, the scoping
composition, the stray-name divergence-direction cell), asserting Java
accepts every placement AND Java's Parsing Canonical Form equals twmb's
Canonical() on every twmb-accepted cell; name matches the java-differential
job's TestDifferentialJavaAcceptance -run filter (the 2609823 round's
vacuous-filter lesson applied at authoring time); compiles under
-tags=cisuite, NOT run locally (no JRE) — everything since 5b8ce9d remains
UNPUSHED, so CI has validated none of it.

NEUTER x3, disjoint red sets, each restored byte-identical (git diff
verified): (a) the old validateLogical fall-through reject re-introduced ->
exactly the 28 type-level precision/scale census cells red (+ the 70-cell
stray matrix's documented set + its regression pins + TestValidateLogical's
flipped pins; census FIELD-level precision/scale cells stayed GREEN — the
field posture never routed through validateLogical, executed proof of the
level split; the reintroduced reject is a superset of the historical one —
the historical soft-drop arms early-returned, so unknown-logical cells red
here that historically parsed — noted for precision). (b) the guard's
namespace half restored -> exactly the 19 acceptance-class subtests red
(2 census cells, 2+1 regression/flipped pins, 2 scoping cells, all 11
harness-row drivers, the schema_test pin). (c) the canon-clear removed
(guard fixed) -> exactly the canonical-sensitive subset red (census
namespace cells on Canonical/Rabin, the pins' canonical asserts, harness
canonical-rabin + cache-ref-into + soe-roundtrip drivers); the scoping
cells stayed green — the clear affects emission only.

GATES: full suite + fastavro differential green x3 (baseline pre-work,
post-fix, final); -race full suite green; go vet (plain + cisuite) clean;
gofmt clean on every touched file. FIX.md sweep: item-0 gate above; item-3
o.Namespace consumer census (named branch validate/consume, canonical
writer schema_canonical.go:151 — the clear feeds it, the new else-branch
clear; no other readers); item-13 immunity claims all executed (primitive
canonical-strip asserted by census cells, wrapped-ref namespace probed
unchanged, scoping proven by cells + neuters); item-14 references executed
or quoted this round (fastavro probes + arm; Java Schema.java read).
CONVERGENCE: 1 behavioral fixed same round; the counter stays at ZERO
(dedicated round, not a bare full round); the rebuild still needs two
consecutive clean bare FULL rounds.

## Distillation archive (2026-07-15)

Round-ledger entry originals moved VERBATIM from AUDIT_CORE.md §Round
ledger on 2026-07-15, when the file stood at 62,336 B against its ~55KB
bound and the 2026-07-12-onward entries had accumulated full narratives in
place of the one-entry-per-round format. The compressed lines now in
AUDIT_CORE.md are operative; these originals are kept so compression loses
nothing. The 3a4d233 (2nd) trust-boundary census and the 79ed5b3 (2nd) and
(3rd) FULL-round narratives had never been archived (their distinctive
phrases — checkedReader, callback_contract, soe/rabin, dedupNamedTypes —
had zero grep hits here before this section); the earlier 2026-07-12…
2026-07-14 entries are archived as they stood at compression time.

### Round-ledger entry originals (2026-07-12 through 2026-07-14 · 79ed5b3 3rd), verbatim

- 2026-07-12 · 8ee2b1b (2nd) · FULL · empty · 1 behavioral FILED→FIXED same
  round as 9733042 (appendAvroString trusted AppendText's returned slice:
  fresh-short PANICS Encode, fresh-long silently replaces sibling bytes;
  minimal shrunk-return guard ruled; return-shape × position matrix + JSON
  immunity, neuter-verified; #61; json/v2 panics identically, EXECUTED) ·
  fastavro RAN · RESET (stands; fixed same round). ser/resolve/custom_type
  first walks otherwise clean.
- 2026-07-12 · 9733042 (2nd) · FULL · 8ee2b1b..9733042 clear · 1 behavioral
  FILED→FIXED same round as b25d878 (SchemaCache splice walkers never
  walked FLAT array/map items/values — cross-parse refs dangled, defs
  uncollected; walkers gated on flatFieldNeedsLift; FEATURE × WALKER
  harness landed: 8 rows × 11 drivers, neuter ×2 incl. the ns-decoy
  wrong-bind trap) · fastavro RAN · RESET (stands; fixed same round).
  json_scan/promote/cache walks otherwise clean.
- 2026-07-12 · b25d878 (3rd; commits → c6908cd) · DEDICATED census
  (neutral) · empty · 0 behavioral · FEATURE × WALKER harness populated to
  28 rows / 308 cells (1c24aba..44e4ffa + c6908cd), every family
  neuter-proven; §Open net gaps EMPTY · oracles not run (test-only).
- 2026-07-12 · afe3b68 (4th) · DEDICATED cleanliness per CLEAN.md (neutral)
  · own commits f336025..afe3b68 (walkNodeChildren/nodeChildScope
  consolidation 3→1; C9 race-ceiling repair) · 0 behavioral shipped; 1
  behavioral FILED (collectTreeDefs keyless-def carve-out — B7 2nd
  instance: defs misfile, stale splice) · fastavro RAN each step.
- 2026-07-13 · 18988c2 · FIX · keyless defs FIXED (collect visits every
  named KIND under the parser's fullname; sibling seen-gates dropped;
  nodeHasStringName deleted; keyless matrix, lockstep-guard neuter) · 1 NEW
  behavioral FILED (leading-dot names stored verbatim, parser internally
  inconsistent — B7 3rd instance, BLOCKING) · oracles not run
  (metadata-only fix, twin-asserted per cell).
- 2026-07-14 · f47083c · FIX · leading-dot FIXED (ruled NORMALIZE AT PARSE:
  one leading dot = the null-namespace escape; Java Name-ctor quoted,
  fastavro EXECUTED, premise correction recorded; shared leadingDotName ×3
  resolvers; normalization matrix, "." family joins #60 numerically; #62) ·
  fastavro EXECUTED for the family.
- 2026-07-14 · f47083c (2nd) · FULL · empty · 0 behavioral · fastavro RAN;
  -race + fuzz clean; Java in CI (all since 5b8ce9d UNPUSHED) ·
  skip/reflect/logical/schema_walk walks clean · clean full #1 since the
  07-12 reset.
- 2026-07-14 · f47083c (3rd) · FULL · empty · 1 behavioral FILED (stray
  precision/scale HARD-REJECTED at parse exactly when no logical or a valid
  non-decimal logical accompanies them — fastavro EXECUTED 9/9 accept, Java
  props source-quoted, field-level twin already Props) + 2 policy items ·
  fastavro EXECUTED · RESET pending adjudication. schema.go +
  schema_parse.go end-to-end walks otherwise clean.
- 2026-07-14 · 5717f32 · FIX · stray precision/scale FIXED (adjudicated
  ACCEPT AS INERT METADATA: recognized-decimal carriers consume, every
  other placement is Props on all surfaces; 70-cell placement × level ×
  kind matrix + fastavro PCF calibration, neuter ×2 both components; #63
  structural-key keep-strict + #54 extended) · fastavro RAN; Java in CI
  (still unpushed) · counter at ZERO; rebuild needs two consecutive clean
  FULL rounds. Next round quarantines commits after 5717f32.
- 2026-07-14 · 5717f32 (2nd) · DEDICATED distillation + ATTRIBUTE ×
  PLACEMENT census (streak-neutral save the finding) · empty · 1 behavioral
  FILED→FIXED same round as 70c7c1c (stray namespace on array/map
  hard-rejected while primitives parsed it — fastavro EXECUTED accept +
  non-scoping, Java SCHEMA_RESERVED quoted; ruled ACCEPT AS INERT; the
  o.Name half KEPT strict per nsForChildren's scope-by-Name walker-parity
  hazard; canon tree clears the attr; one unmarked 13a pin flipped; #64;
  #63 capture-drop edge corrected by execution) · CENSUS: 12 attrs × 14
  kinds (+union field) × 2 levels = 272 cells — 24 documented #63 rejects
  (fastavro-accept asserted per cell), 2 findings (fixed), 246 parity
  accepts, routing pinned (28 props / 98 captured / 120 field Props) +
  dropped-subtree agreement cells + the harness decoy row; fastavro arm EXECUTED 273/273 0 skips; Java
  cisuite arm added (CI-filter-matching, PCF equality per accept cell),
  not run locally (no JRE; all since 5b8ce9d UNPUSHED) · NEUTER ×3
  disjoint (validateLogical tail reject → the 28 precision/scale cells;
  namespace guard → 19 cells incl. all 11 harness drivers; canon-clear →
  the canonical-sensitive subset) · DISTILLATION:
  AUDIT_CORE 163,716→~55,000 B, AUDIT_PATTERNS 147,594→138,745 B
  (B7/B33/B34 tombstoned), BUG_AUDIT holds every removed byte · fastavro
  RAN ×3; -race green · counter stays ZERO; rebuild needs two
  consecutive clean bare FULL rounds. Commits 70c7c1c (fix+census) +
  3a4d233 (docs), NOT pushed. Next round quarantines 5717f32..3a4d233
  (fix UNCONVERGED).
- 2026-07-14 · 3a4d233 (2nd) · DEDICATED trust-boundary census
  (streak-neutral save findings) · 5717f32..3a4d233 cleared
  (decoy-qualified ref dangles, rebuild stable — executed) · 6
  behavioral FILED→FIXED same round: (1) lying io.Reader count (n<0 /
  n>len, nil err) PANICKED through ocf.NewReader/NewAppendWriter
  (bufio arithmetic; encoding/json panics identically EXECUTED) →
  checkedReader named error; (2) lying io.Writer count silently
  truncated the file at both write sites (encoding/json trusts
  identically EXECUTED; io.Copy/bufio discipline chosen) → writeFull
  io.ErrShortWrite + invalid-count; (3) JSON-encode user-value rejects
  lacked the binary twins' *SemanticError identity (enum unknown-symbol
  ×4 input arms + ordinal, fixed size mismatch, missing defaultless
  field) → unified to binary constructions; decode wire-content stays
  plain BOTH wires + already-agreeing families PINNED as the boundary;
  (4) a (0,nil)-stuck reader LIVELOCKED the block-data io.ReadFull
  (bufio bounds only its buffered path — executed 5s spin mid-block vs
  ErrNoProgress mid-header) → checkedReader 100-empty-reads bound;
  (5)+(6) user-originated io.EOF (codec Decompress error; CustomType
  decode callback) matched Decode's clean-end sentinel through the
  decompress/datum %w wraps, contradicting #57's exclusivity →
  noEOF at both wraps · CENSUS: 16 callback-return sites classified
  (site → guarded / signature-immune / trusted-documented / fixed;
  table in the round report): AppendText-inplace GUARDED #61 ·
  materializing text-out ×6 sites guarded-by-construction ·
  TextUnmarshaler signature-immune · CustomType Encode/Decode guarded
  (nil reject / setCustomResult / errors.Is skip) · WithLaxNames +
  IsZero signature-immune · Compress trusted-content (error poisons,
  Close still runs) · Decompress real-len arithmetic (lying bound =
  #45 clause; aliasing safe — owned-values pin incl. reused-buffer
  codec) · io.Reader FIXED ×2 · io.Writer FIXED · ReaderSchemaFunc
  guarded · Codec.Name capped · Seek ignored · map keys never
  consulted (pinned) · Props stdlib-named · permanent matrices ~126
  avro + ~26 ocf cells, neuter ×9 incl. two dodge catches
  (recordFieldError fallback masks top-level arms; helper-message
  neuter masked by call-site wrap) · #65 #66 new + #45 clause; B10
  extended (wrong-CONTENT axis, top-level assertion rule) · fastavro
  EXECUTED; -race green both pkgs; Java oracle validated through
  18988c2 (CI run 29345609516 GREEN) — unvalidated remainder
  f47083c..HEAD incl. this round · counter stays ZERO (fixed same
  round). Commit 79ed5b3, NOT pushed. Next round quarantines
  3a4d233..79ed5b3 (fix UNCONVERGED).
- 2026-07-14 · 79ed5b3 (2nd) · FULL · 3a4d233..79ed5b3 cleared
  (checkedReader wraps BOTH construction sites and every read path incl.
  the io.CopyN limit-reader route; NewAppendWriter seek is absolute-end;
  writeFull at both user-writer sites, flate write is internal
  bytes.Buffer; noEOF→ErrUnexpectedEOF at both new wraps; JSON
  SemanticError arms — 4 enum + ordinal + fall-through + fixed size +
  missing-key via recordFieldError — byte-shape-match the binary twins)
  · 0 behavioral · fastavro RAN (venv rebuilt, 1.12.2); -race green both
  pkgs; fuzz RT+jsonRT 75s×2 sequential clean; Java NOT run (no JRE;
  validated in CI through 18988c2, f47083c..79ed5b3 unpushed) · fronts:
  B25 inverse-density soe/rabin/compat (alias-collision candidate killed
  by #50's parse guard — reachable double-claim variant probe-verified
  consistent *CompatibilityError from both paths; recursive memo,
  promotion order, resolved-SOE fp gating probe-verified) +
  json_decode.go materializer (native gates exclude logical+custom;
  json.Number keys forced to validated reflect path; [N]T count errors
  match binary SemanticError; alias-keyed decode is Java-parity pinned
  conformance_test.go:7547) + Y4/P1/B20/P18 grep refresh (all hits
  guarded) + deser.go spot walk — all clean · NOTE (comment-only):
  resolveRecord's fastavro claim ("second falls through to skip_data")
  holds only for the name+alias sub-case; a two-ALIAS double-claim
  last-wins in fastavro (aliases_field_dict never pruned) — behavior
  unaffected, both twmb paths reject; candidate for next CLEAN pass ·
  clean full #1 since the 07-14 zero; one more consecutive clean bare
  FULL converges. Next round quarantines commits after 79ed5b3 (none
  yet — empty scope unless new code lands).
- 2026-07-14 · 79ed5b3 (3rd) · FULL · empty · 1 behavioral FILED
  (SchemaFor composes CustomType.Schema toJSON subtrees scope-naively:
  dedupNamedTypes keys defined[] and emits references by SHORT name while
  toJSON splits name/namespace — a split-spelling namespaced custom on
  two fields dedups to a bare ref that dangles ("unknown type \"X\"");
  distinct fullnames a.X vs X false-collide ("two different
  definitions"); and a null-namespace custom under WithNamespace is
  silently CAPTURED into the SchemaFor namespace at the internal Parse
  (wire-visible identity change, no "namespace":"" escape emitted) — 3
  probes verified failing in /tmp/avro_audit_verify/round6; the
  dotted-name spelling of the same schemas passes (parser stores dotted
  Name verbatim), so the two spec-equivalent spellings diverge; spec
  Names ¶ quoted (_index.md:250-254), Java Name.full cited
  (Schema.java:719-739), fastavro N/A (no SchemaFor — internal
  consistency, NOT_BUGS #38 framing); inference-only trees immune
  (seen[] refs are fullnames, single uniform namespace)) · fastavro RAN
  (differential executing confirmed via -v); -race green both pkgs; fuzz
  RT+jsonRT 75s×2 sequential clean; Java NOT run (no JRE; CI-validated
  through 18988c2, f47083c..79ed5b3 unpushed) · fronts: schema_for.go +
  atype end-to-end (B21/inverse-density: big.Rat VALUE encode/decode
  cells verified both directions (decimalRatFor tmp-copy /
  setDecimalRat), inferTimeLike × ser time-arm cells parity-checked
  incl. duration-on-date serInt fallback (doc'd meaningless-but-
  allowed), dedup fields-shape matches toJSONWalk's []map[string]any,
  bigRatPtrType switch arm unreachable (pointer arm intercepts — dead
  code, cosmetic only)) + json_codec.go end-to-end (every arm twin-cited
  vs ser.go and verified: tagged-map→nil-first→type-name→try-each with
  2-branch null-skip hook parity, union tag = fullname (Java
  getFullName parity), encodeDefault as union-default branch authority,
  codepoint bytes mapping inverse-paired, jsonCoerce* SemanticError
  wraps, native fast-path gates exclude logical+custom+json.Number
  keys) + narrow-before-check trilogy grep refresh (int(v.Uint()) /
  int64(f) / k*size: only live hit 8*len(b) in bytesToBigInt guarded by
  checkDecimalUnscaledLen at every wire caller incl. json fixed-decimal
  *any at json_decode.go:780; varint shifts loop-counter-bounded) +
  varint.go/errors.go/schema_canonical.go walks (uvarintLens verified
  per Len32 index 0-32, 5th/10th-byte canonical-overflow rejects;
  render-truncation uniform across *Error types; PCF [ORDER]/[STRINGS]
  conform, fastavro-PCF-calibrated) — all otherwise clean · RESET:
  streak to zero (the 79ed5b3-2nd clean full is superseded); fix is
  opt-in; next round quarantines commits after 79ed5b3.
