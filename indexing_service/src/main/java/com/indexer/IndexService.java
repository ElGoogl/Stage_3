package com.indexer;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import com.google.gson.GsonBuilder;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * IndexService builds and manages hierarchical JSON indexes
 * for books stored in the datalake.
 */
public class IndexService {

    private static final Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .create();

    // --- Directory configuration ---
    private static final String DATA_PATH = "data_repository/datalake_v1/";
    private static final Path INDEX_PATH = Paths.get("data_repository/indexes/");
    private static final Path STATUS_FILE = Paths.get("data_repository/indexed_books.json");


    // --- Track which books are already indexed ---
    private static final Set<Integer> indexedBooks = new HashSet<>();

    // --- Static initializer to load previous indexing state ---
    static {
        try {
            if (Files.exists(STATUS_FILE)) {
                String json = Files.readString(STATUS_FILE);
                List<Integer> list = gson.fromJson(json, new TypeToken<List<Integer>>() {}.getType());
                if (list != null) indexedBooks.addAll(list);
            } else {
                Files.createDirectories(STATUS_FILE.getParent());
                Files.createFile(STATUS_FILE);
                Files.writeString(STATUS_FILE, "[]");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Builds an index for a given book if it has not been indexed before.
     *
     * @param id the book ID to index
     * @return JSON-style response map with status information
     */
    public synchronized Map<String, Object> buildIndex(int id) {
        try {
            if (indexedBooks.contains(id)) {
                System.out.println("[INDEXER] Book " + id + " already indexed. Skipping...");
                return Map.of(
                        "book_id", id,
                        "status", "already_indexed",
                        "message", "Index skipped (duplicate)"
                );
            }

            // --- Load JSON from datalake ---
            Path jsonPath = Paths.get(DATA_PATH + id + ".json");
            if (!Files.exists(jsonPath)) {
                return Map.of(
                        "status", "error",
                        "message", "Book JSON not found for id " + id
                );
            }

            Map<String, Object> jsonData = gson.fromJson(
                    new FileReader(jsonPath.toFile()),
                    new TypeToken<Map<String, Object>>() {}.getType()
            );

            Map<String, String> metadata = MetaDataParser.parseMetadata((String) jsonData.get("header"));
            MetaDataParser.storeMetadata(metadata, id);

            // --- Build hierarchical index (logical JSON structure) ---
            Tokenised tokenizer = new Tokenised();
            Map<String, Map<String, List<Map<String, Object>>>> index =
                    tokenizer.buildHierarchicalIndex(jsonData, id);


            // --- Save index JSON ---
            Files.createDirectories(INDEX_PATH);
            Path indexFile = INDEX_PATH.resolve("index_" + id + ".json");
            Files.writeString(indexFile, gson.toJson(index));

            // --- Update index status ---
            indexedBooks.add(id);
            saveStatus();

            System.out.println("[INDEXER] Indexed book " + id + " successfully.");
            return Map.of(
                    "book_id", id,
                    "status", "indexed",
                    "indexFile", indexFile.toString()
            );

        } catch (Exception e) {
            e.printStackTrace();
            return Map.of("status", "error", "message", e.getMessage());
        }
    }

    /** Checks if a book has already been indexed. */
    public boolean isIndexed(int id) {
        return indexedBooks.contains(id);
    }

    /** Saves the list of indexed books persistently as JSON. */
    private void saveStatus() {
        try {
            Files.createDirectories(STATUS_FILE.getParent());
            Files.writeString(STATUS_FILE, gson.toJson(indexedBooks));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Recursively builds a hierarchical JSON index structure.
     * Converts nested maps into a symbolic tree (for demonstration).
     */
    private Map<String, Object> createHierarchicalIndex(Map<String, Object> json) {
        Map<String, Object> index = new LinkedHashMap<>();
        json.forEach((key, value) -> {
            if (value instanceof Map<?, ?> nested) {
                index.put(key, createHierarchicalIndex((Map<String, Object>) nested));
            } else {
                index.put(key, "leaf");
            }
        });
        return index;
    }
    /**
     * Returns the number of books that have already been indexed.
     * Used by the GET /index/status endpoint in IndexApp.
     */
    public int getIndexedCount() {
        return indexedBooks.size();
    }
}
