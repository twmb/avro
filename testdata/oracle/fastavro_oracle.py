#!/usr/bin/env python3
"""fastavro differential oracle for twmb/avro Tier-1 testing.

Long-lived subprocess. Reads one JSON job per line from stdin, writes one
JSON response line per job to stdout. Used by differential_test.go to
cross-check twmb's binary encode/decode against fastavro (an independent
reference impl), comparing everything as Avro bytes so no cross-language
value coercion is needed.

Jobs:
  {"op":"encode","schema":<avro schema>,"value":<json value>,"kind":<kind>}
      -> {"ok":true,"hex":"<schemaless-encoded bytes, hex>"}
  {"op":"decode","schema":<avro schema>,"hex":"<bytes hex>"}
      -> {"ok":true}                       (decodes without error)
  {"op":"ocf","hex":"<whole OCF file bytes, hex>"}
      -> {"ok":true,"values":[...]}        (every record, via fastavro.reader;
                                            the schema comes from the file's own
                                            header. Records must be
                                            JSON-representable — used by the
                                            foreign block-framing matrix, whose
                                            datums are plain strings.)
  {"op":"ocfread","hex":"<whole OCF file bytes, hex>"}
      -> {"ok":true,"records":["<hex>",...]}  (every record via fastavro.reader,
                                            re-encoded schemaless against the
                                            file's own header schema — byte
                                            transport, so records need not be
                                            JSON-representable)
  {"op":"ocfwrite","schema":<avro schema>,"records":["<hex>",...],
   "codec":<name>,"syncInterval":<int>,"meta":{...}}
      -> {"ok":true,"hex":"<whole OCF file fastavro's writer produced>"}
                                           (records are schemaless bytes the
                                            oracle decodes then container-writes
                                            with its own codec framing and block
                                            sizing)
  {"op":"jsonwrite","schema":<avro schema>,"value":<json value>,"kind":<kind>}
      -> {"ok":true,"json":"<one datum's Avro-JSON text via json_writer>"}
  {"op":"jsonread","schema":<avro schema>,"json":"<Avro-JSON text>"}
      -> {"ok":true,"values":[...]}        (one datum via json_reader; values
                                            must be strict-JSON-representable.
                                            An optional "reader" schema applies
                                            writer->reader migration, the JSON
                                            twin of "readresolve".)
  {"op":"readresolve","schema":<writer schema>,"reader":<reader schema>,
   "hex":"<bytes hex>"}
      -> {"ok":true,"values":[<datum>]}    (schemaless_reader with schema
                                            RESOLUTION — exercises fastavro's
                                            skip_* functions for writer fields
                                            the reader drops; the datum must be
                                            strict-JSON-representable)
On any failure: {"ok":false,"err":"<message>"}

"kind" (optional, default "") tells the oracle how to turn the JSON value into
the native Python type fastavro expects for binary- and logical-typed schemas:
  ""                 value used as-is (numbers, strings, bool, null, lists, maps)
  "bytes" / "fixed"  value is a base64 string -> bytes
  "decimal"          value is a decimal string (e.g. "123.45") -> decimal.Decimal
  "timestamp-millis" value is int milliseconds since the unix epoch -> UTC datetime
  "timestamp-micros" value is int microseconds since the unix epoch -> UTC datetime

Install: python3 -m venv venv && venv/bin/pip install fastavro
Point the test at it with AVRO_FASTAVRO_PYTHON=/path/to/venv/bin/python.
"""
import base64
import datetime
import decimal
import io
import json
import sys

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def _coerce(value, kind):
    # Reconstruct the native Python type for a Kind-tagged transport value so
    # fastavro encodes the same logical value twmb does. JSON can carry only
    # numbers/strings/bool/null/lists/maps, so binary and logical types arrive
    # encoded and are rebuilt here.
    if kind in ("bytes", "fixed"):
        return base64.b64decode(value)
    if kind == "nan":
        # NaN cannot travel as strict JSON; the job carries null + this kind.
        return float("nan")
    if kind == "decimal":
        return decimal.Decimal(value)
    if kind == "timestamp-millis":
        return _EPOCH + datetime.timedelta(milliseconds=value)
    if kind == "timestamp-micros":
        return _EPOCH + datetime.timedelta(microseconds=value)
    return value


def _parse(schema):
    # Fresh named-schema table per call so repeated named types across jobs
    # don't trip fastavro's "X already defined" module-level cache.
    try:
        return fastavro.parse_schema(schema, {})
    except TypeError:
        return fastavro.parse_schema(schema)


def handle(job):
    op = job.get("op")
    if op == "ocf":
        # Whole-container read: iterate every record from an OCF file's raw
        # bytes; the writer schema comes from the file's own header. This
        # exercises fastavro's block-framing loop (count / size / payload /
        # sync per block) on hand-framed foreign files, not just files
        # fastavro or twmb wrote themselves.
        buf = io.BytesIO(bytes.fromhex(job["hex"]))
        return {"ok": True, "values": list(fastavro.reader(buf))}
    if op == "ocfread":
        # Whole-container read, byte transport: every record from the file
        # (writer schema from the file's own header), each re-encoded
        # schemaless against that header schema and returned as hex. The
        # caller compares Avro bytes, so records need not be
        # JSON-representable (bytes/fixed/decimal/NaN travel fine).
        buf = io.BytesIO(bytes.fromhex(job["hex"]))
        rdr = fastavro.reader(buf)
        outs = []
        for rec in rdr:
            out = io.BytesIO()
            fastavro.schemaless_writer(out, rdr.writer_schema, rec)
            outs.append(out.getvalue().hex())
        return {"ok": True, "records": outs}
    schema = _parse(job["schema"])
    if op == "encode":
        value = _coerce(job["value"], job.get("kind", ""))
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, schema, value)
        return {"ok": True, "hex": buf.getvalue().hex()}
    if op == "decode":
        buf = io.BytesIO(bytes.fromhex(job["hex"]))
        value = fastavro.schemaless_reader(buf, schema)
        # Values ride back only when strict-JSON-representable; a decode whose
        # value cannot travel still reports ok (the op's original contract).
        try:
            json.dumps([value], allow_nan=False)
            return {"ok": True, "values": [value]}
        except (TypeError, ValueError):
            return {"ok": True}
    if op == "readresolve":
        # Resolved read: the reader schema differs from the writer's, so
        # fastavro routes dropped writer fields through its skip_* functions
        # (_read_py.py) — the reference twin of twmb's skip path.
        reader = _parse(job["reader"])
        buf = io.BytesIO(bytes.fromhex(job["hex"]))
        value = fastavro.schemaless_reader(buf, schema, reader)
        json.dumps([value], allow_nan=False)  # reject non-representable early
        return {"ok": True, "values": [value]}
    if op == "parse":
        # Schema-acceptance probe: _parse already ran above; reaching here
        # means fastavro accepted the schema. (A rejection surfaces as the
        # handler's exception -> {"ok": false}.)
        return {"ok": True}
    if op == "parsedump":
        # Acceptance PLUS what fastavro kept: the parsed schema itself, so a
        # caller can assert an attribute was PRESERVED rather than merely
        # that the schema parsed. fastavro's own bookkeeping keys are
        # dunder-prefixed (__named_schemas, __fastavro_parsed) and are
        # stripped so only the schema's own attributes travel.
        if not isinstance(schema, dict):
            return {"ok": True, "parsed": json.dumps(schema)}
        kept = {k: v for k, v in schema.items() if not k.startswith("__")}
        return {"ok": True, "parsed": json.dumps(kept)}
    if op == "rt":
        # Round-trip THROUGH fastavro: decode twmb's bytes, re-encode the
        # decoded value, return fastavro's bytes. Byte equality means both
        # implementations agree these bytes are THE encoding of the value —
        # no cross-language value comparison needed, and it catches
        # symmetric encode+decode bugs a single-impl round trip cannot.
        buf = io.BytesIO(bytes.fromhex(job["hex"]))
        value = fastavro.schemaless_reader(buf, schema)
        out = io.BytesIO()
        fastavro.schemaless_writer(out, schema, value)
        return {"ok": True, "hex": out.getvalue().hex()}
    if op == "ocfwrite":
        # Foreign-WRITER differential: fastavro WRITES a whole OCF file.
        # Each record arrives as twmb's schemaless bytes (byte transport, no
        # cross-language value coercion); fastavro decodes them and its
        # writer produces the container — its own header rendering of the
        # schema, its own block sizing (sync_interval), its own codec
        # framing (cramjam snappy CRC, zstandard frames, raw-deflate,
        # stdlib bzip2/xz). Returns the whole file's bytes for the caller's
        # reader to consume.
        records = [
            fastavro.schemaless_reader(io.BytesIO(bytes.fromhex(h)), schema)
            for h in job.get("records") or []
        ]
        kwargs = {}
        if job.get("syncInterval"):
            kwargs["sync_interval"] = job["syncInterval"]
        if job.get("meta"):
            kwargs["metadata"] = job["meta"]
        out = io.BytesIO()
        fastavro.writer(out, schema, records, codec=job.get("codec", "null"), **kwargs)
        return {"ok": True, "hex": out.getvalue().hex()}
    if op == "canonical":
        # Parsing Canonical Form per fastavro (Java-validated rules); the
        # caller compares against twmb's Canonical() for fingerprint parity
        # on arbitrary composed schemas, not just the vendored vectors.
        from fastavro.schema import to_parsing_canonical_form

        return {"ok": True, "canonical": to_parsing_canonical_form(json.loads(json.dumps(job["schema"])))}
    if op == "jsonwrite":
        # Avro-JSON encoding of one datum via fastavro.json_writer; the
        # caller compares the text against twmb's EncodeJSON for JSON-wire
        # parity (byte/fixed codepoint strings, tagged-union envelopes,
        # special-float spelling).
        value = _coerce(job["value"], job.get("kind", ""))
        out = io.StringIO()
        fastavro.json_writer(out, schema, [value])
        return {"ok": True, "json": out.getvalue().strip("\n")}
    if op == "jsonread":
        # Avro-JSON decode of one datum via fastavro.json_reader. Values
        # must be strict-JSON-representable to travel back over the line
        # protocol (a non-representable result surfaces as ok:false). An
        # optional reader schema applies writer->reader migration — the
        # JSON-wire twin of "readresolve" (json_reader parses against the
        # WRITER schema, then resolves into reader shape).
        reader = _parse(job["reader"]) if job.get("reader") else None
        vals = list(fastavro.json_reader(io.StringIO(job["json"]), schema, reader))
        json.dumps(vals, allow_nan=False)  # reject non-representable early
        return {"ok": True, "values": vals}
    return {"ok": False, "err": "unknown op %r" % op}


def main():
    global fastavro
    try:
        import fastavro as _fa
        fastavro = _fa
    except Exception as e:  # noqa: BLE001
        sys.stdout.write(json.dumps({"fatal": "fastavro import failed: %s" % e}) + "\n")
        sys.stdout.flush()
        return
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            resp = handle(json.loads(line))
        except Exception as e:  # noqa: BLE001
            resp = {"ok": False, "err": str(e)}
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
