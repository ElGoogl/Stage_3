package com.example.crawler;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class Parser {
    private static final String START = "*** START OF THE PROJECT GUTENBERG EBOOK";
    private static final String END = "*** END OF THE PROJECT GUTENBERG EBOOK";

    public static Map<String, String> splitBook(String text, int id) {
        if (!text.contains(START) || !text.contains(END)) {
            System.out.printf("Book %d missing expected markers%n", id);
            Map<String, String> result = new HashMap<>();
            result.put("id", String.valueOf(id));
            result.put("header", text.trim());
            result.put("content", "");
            result.put("footer", "");
            return result;
        }

        String[] startSplit = text.split(Pattern.quote(START), 2);
        String header = startSplit[0].trim();
        String[] endSplit = startSplit[1].split(Pattern.quote(END), 2);
        String body = endSplit[0].trim();
        String footer = endSplit.length > 1 ? endSplit[1].trim() : "";

        Map<String, String> data = new HashMap<>();
        data.put("id", String.valueOf(id));
        data.put("header", header);
        data.put("content", body);
        data.put("footer", footer);
        return data;
    }
}
