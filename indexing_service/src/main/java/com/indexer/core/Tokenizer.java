package com.indexer.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Tokenizer {

    private static final Pattern WORD = Pattern.compile("\\p{L}+");

    public List<String> tokenize(String text) {
        if (text == null || text.isBlank()) return List.of();

        String lower = text.toLowerCase(Locale.ROOT);

        List<String> out = new ArrayList<>();
        Matcher m = WORD.matcher(lower);
        while (m.find()) {
            String w = m.group();
            if (w.length() < 2) continue;
            out.add(w);
        }
        return out;
    }
}