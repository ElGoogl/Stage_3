package com.indexer.core;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;

public final class IndexFileWriter {

    private IndexFileWriter() {}

    public static void writePrettyWithBlankLines(Path outFile, Map<String, Object> file) throws Exception {
        StringBuilder sb = new StringBuilder(16_384);

        sb.append("{\n");

        Object termsObj = file.get("terms");

        int written = 0;
        for (Map.Entry<String, Object> e : file.entrySet()) {
            if ("terms".equals(e.getKey())) continue;

            if (written > 0) sb.append(",\n");
            sb.append("  \"").append(escape(e.getKey())).append("\": ");
            sb.append(toJsonValue(e.getValue()));
            written++;
        }

        if (termsObj instanceof Map<?, ?> terms) {
            if (written > 0) sb.append(",\n");
            sb.append("  \"terms\": {\n");

            Iterator<? extends Map.Entry<?, ?>> it = terms.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<?, ?> te = it.next();
                String term = String.valueOf(te.getKey());
                Object count = te.getValue();

                sb.append("    \"").append(escape(term)).append("\": ").append(String.valueOf(count));

                if (it.hasNext()) sb.append(",\n\n");
                else sb.append("\n");
            }

            sb.append("  }\n");
        } else {
            if (written > 0) sb.append(",\n");
            sb.append("  \"terms\": {}\n");
        }

        sb.append("}\n");

        Files.writeString(outFile, sb.toString(), StandardCharsets.UTF_8);
    }

    private static String toJsonValue(Object v) {
        if (v == null) return "null";
        if (v instanceof Number || v instanceof Boolean) return String.valueOf(v);
        return "\"" + escape(String.valueOf(v)) + "\"";
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}