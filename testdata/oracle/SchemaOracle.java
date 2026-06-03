// SchemaOracle is the Apache Avro (Java) reference oracle for twmb/avro
// Tier-1 differential testing. It reads one request (one per line) from stdin
// and writes one response line to stdout. Two request shapes:
//
//   1. Fingerprint (a bare compact-JSON schema line — back-compat default):
//        OK  <tab> <parsingFingerprint64 as signed long> <tab> <base64(toParsingForm)>
//        ERR <tab> <message>            (Java could not parse this schema)
//
//   2. Round-trip a binary-encoded value through Java's datum model, prefixed
//      with the literal command "RT" (tab-delimited):
//        RT <tab> <compact-JSON schema> <tab> <base64(avro binary bytes)>
//      Java parses the schema, binary-decodes the bytes to a generic datum,
//      then re-encodes that datum to JSON (JsonEncoder) AND to binary
//      (BinaryEncoder), answering:
//        OK  <tab> <base64(json bytes)> <tab> <base64(binary re-encode)>
//        ERR <tab> <message>
//      This lets the Go side compare Java's JSON rendering and binary
//      round-trip of a value byte-for-byte — e.g. to learn what Java does
//      with an invalid-UTF-8 Avro string (binary verbatim? JSON U+FFFD?).
//
// Fingerprinting uses org.apache.avro.SchemaNormalization directly — the
// canonical-form and CRC-64-AVRO (Rabin) reference. A fresh Schema.Parser per
// request keeps named-type state from leaking across cases.
//
// Build: javac -cp avro-tools.jar -d testdata/oracle testdata/oracle/SchemaOracle.java
// Run:   java -cp avro-tools.jar:testdata/oracle SchemaOracle
//
// avro-tools is a shaded fat jar that bundles org.apache.avro, so no other
// classpath entries are needed. Driven by java_differential_test.go and
// java_value_differential_test.go (both behind the `cisuite` build tag, so
// they only run in CI).
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

public class SchemaOracle {
    public static void main(String[] args) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        PrintStream out = new PrintStream(System.out, true, "UTF-8");
        String line;
        while ((line = in.readLine()) != null) {
            if (line.trim().isEmpty()) {
                continue;
            }
            try {
                if (line.startsWith("RT\t")) {
                    out.println(roundTrip(line));
                } else {
                    Schema s = new Schema.Parser().parse(line);
                    long fp = SchemaNormalization.parsingFingerprint64(s);
                    String canon = SchemaNormalization.toParsingForm(s);
                    String b64 = Base64.getEncoder().encodeToString(canon.getBytes(StandardCharsets.UTF_8));
                    out.println("OK\t" + fp + "\t" + b64);
                }
            } catch (Throwable e) {
                String m = e.getMessage();
                out.println("ERR\t" + (m == null ? e.toString() : m.replace('\n', ' ').replace('\t', ' ')));
            }
        }
    }

    // roundTrip handles "RT\t<schema>\t<base64 binary>": binary-decode to a
    // datum, then re-encode that datum to JSON and to binary, returning both
    // base64-encoded. A fresh reader/writer per call avoids cross-case state.
    static String roundTrip(String line) throws Exception {
        String[] parts = line.split("\t", 3);
        if (parts.length != 3) {
            return "ERR\tmalformed RT request";
        }
        Schema s = new Schema.Parser().parse(parts[1]);
        byte[] binIn = Base64.getDecoder().decode(parts[2]);

        GenericDatumReader<Object> reader = new GenericDatumReader<>(s);
        Decoder bdec = DecoderFactory.get().binaryDecoder(binIn, null);
        Object datum = reader.read(null, bdec);

        GenericDatumWriter<Object> writer = new GenericDatumWriter<>(s);

        ByteArrayOutputStream jbos = new ByteArrayOutputStream();
        Encoder jenc = EncoderFactory.get().jsonEncoder(s, jbos);
        writer.write(datum, jenc);
        jenc.flush();

        ByteArrayOutputStream bbos = new ByteArrayOutputStream();
        Encoder benc = EncoderFactory.get().binaryEncoder(bbos, null);
        writer.write(datum, benc);
        benc.flush();

        String jsonB64 = Base64.getEncoder().encodeToString(jbos.toByteArray());
        String binB64 = Base64.getEncoder().encodeToString(bbos.toByteArray());
        return "OK\t" + jsonB64 + "\t" + binB64;
    }
}
