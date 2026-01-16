package com.indexer.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class Tokenizer {

    public List<String> tokenize(String text) {
        if (text == null || text.isBlank()) return List.of();

        String cleaned = text.toLowerCase(Locale.ROOT)
                .replaceAll("[^\\p{L}\\p{Nd}]+", " ")
                .trim();

        if (cleaned.isEmpty()) return List.of();

        String[] parts = cleaned.split("\\s+");
        List<String> out = new ArrayList<>(parts.length);
        for (String p : parts) {
            if (p.length() < 2) continue;
            out.add(p);
        }
        return out;
    }
}