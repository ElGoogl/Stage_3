package com.indexer.core;

import com.google.gson.Gson;
import com.indexer.dto.BookDocument;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class BookLoader {

    private final Gson gson;

    public BookLoader(Gson gson) {
        this.gson = gson;
    }

    public BookDocument load(Path jsonFile) throws IOException {
        String raw = Files.readString(jsonFile, StandardCharsets.UTF_8);

        RawBook rb = gson.fromJson(raw, RawBook.class);
        if (rb == null || rb.id == null || rb.id.isBlank()) {
            throw new IOException("book json missing id");
        }

        int id;
        try {
            id = Integer.parseInt(rb.id.trim());
        } catch (NumberFormatException e) {
            throw new IOException("book id not an int: " + rb.id);
        }

        String content = (rb.content == null) ? "" : rb.content;
        return new BookDocument(id, content);
    }

    private static final class RawBook {
        String id;
        String content;
        String header;
        String footer;
    }
}