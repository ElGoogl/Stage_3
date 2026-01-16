package com.indexer.core;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class BookParser {

    private final Gson gson;

    public BookParser(Gson gson) {
        this.gson = gson;
    }

    public ParsedBook parse(Path file) throws IOException {
        try (Reader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            JsonObject obj = gson.fromJson(r, JsonObject.class);
            if (obj == null) throw new IOException("invalid json");

            String id = getString(obj, "id");
            String header = getString(obj, "header");
            String content = getString(obj, "content");
            String footer = getString(obj, "footer");

            return new ParsedBook(id, header, content, footer);
        }
    }

    private static String getString(JsonObject obj, String key) {
        if (!obj.has(key) || obj.get(key).isJsonNull()) return "";
        try {
            return obj.get(key).getAsString();
        } catch (Exception e) {
            return "";
        }
    }

    public record ParsedBook(String id, String header, String content, String footer) {
        public String combinedText() {
            return (header == null ? "" : header) + "\n"
                    + (content == null ? "" : content) + "\n"
                    + (footer == null ? "" : footer);
        }
    }
}