// SchemaOracle is the Apache Avro (Java) reference oracle for twmb/avro
// Tier-1 differential testing. It reads one schema (compact JSON, one per
// line) from stdin and writes one response line to stdout:
//
//   OK <tab> <parsingFingerprint64 as signed long> <tab> <base64(toParsingForm)>
//   ERR <tab> <message>            (Java could not parse this schema)
//
// It uses org.apache.avro.SchemaNormalization directly — the canonical-form
// and CRC-64-AVRO (Rabin) fingerprint reference — so there is no CLI
// output-format guessing. A fresh Schema.Parser per line keeps named-type
// state from leaking across cases.
//
// Build: javac -cp avro-tools.jar -d testdata/oracle testdata/oracle/SchemaOracle.java
// Run:   java -cp avro-tools.jar:testdata/oracle SchemaOracle
//
// avro-tools is a shaded fat jar that bundles org.apache.avro, so no other
// classpath entries are needed. Driven by java_differential_test.go (which is
// behind the `cisuite` build tag, so it only runs in CI).
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;

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
                Schema s = new Schema.Parser().parse(line);
                long fp = SchemaNormalization.parsingFingerprint64(s);
                String canon = SchemaNormalization.toParsingForm(s);
                String b64 = Base64.getEncoder().encodeToString(canon.getBytes(StandardCharsets.UTF_8));
                out.println("OK\t" + fp + "\t" + b64);
            } catch (Throwable e) {
                String m = e.getMessage();
                out.println("ERR\t" + (m == null ? e.toString() : m.replace('\n', ' ').replace('\t', ' ')));
            }
        }
    }
}
