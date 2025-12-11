package com.indexer;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class IndexService {

    private static final Gson gson = new Gson();
    private static final String DATA_PATH = "data_repository/datalake_v1/";
    private static final Path STATUS_FILE = Paths.get("data_repository/indexed_books.json");

    // indexed status tracker
    private static final Set<Integer> indexedBooks = new HashSet<>();

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
     * Builds indexer for book if not present already
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

            // get JSON files from datalake
            Path jsonPath = Paths.get(DATA_PATH + id + ".json");
            if (!Files.exists(jsonPath)) {
                return Map.of("status", "error", "message", "Book JSON not found for id " + id);
            }

            Map<String, Object> jsonData = gson.fromJson(
                    new FileReader(jsonPath.toFile()),
                    new TypeToken<Map<String, Object>>() {}.getType()
            );

            Map<String, Object> index = createHierarchicalIndex(jsonData);

            // save index file
            Path indexFile = Paths.get("data_repository/indexes/index_" + id + ".json");
            Files.createDirectories(indexFile.getParent());
            Files.writeString(indexFile, gson.toJson(index));

            // update status
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

    /**
     * Check if book is already indexed
     */
    public boolean isIndexed(int id) {
        return indexedBooks.contains(id);
    }

    /**
     * Save indexed status list
     */
    private void saveStatus() {
        try {
            Files.createDirectories(STATUS_FILE.getParent());
            Files.writeString(STATUS_FILE, gson.toJson(indexedBooks));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Create the hierarchical indexer
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
}
