package com.indexer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/**
 * GlobalIndexer merges all individual per-book indexes (index_XXXX.json)
 * into one unified inverted index (inverted_index.json).
 *
 * Supports token entries with {book_id, count}.
 */
public class GlobalIndexer {

    private static final Path INDEX_DIR = Paths.get("data_repository/indexes/");
    private static final Path GLOBAL_INDEX_FILE = Paths.get("data_repository/inverted_index.json");
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    /**
     * Reads all per-book index files and merges them into one global index.
     * Ensures that required directories and files exist.
     */
    public Map<String, Map<String, List<Map<String, Object>>>> buildGlobalIndex() {
        Map<String, Map<String, List<Map<String, Object>>>> globalIndex = new TreeMap<>();

        try {
            // Ensure that index directory exists
            Files.createDirectories(INDEX_DIR);

            // Ensure that the global index file exists (create empty one if needed)
            if (!Files.exists(GLOBAL_INDEX_FILE)) {
                Files.createFile(GLOBAL_INDEX_FILE);
                Files.writeString(GLOBAL_INDEX_FILE, "{}");
                System.out.println("[GLOBAL] Created new empty global index file.");
            }

            // Read all local index files
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(INDEX_DIR, "index_*.json")) {
                boolean foundFiles = false;

                for (Path file : stream) {
                    foundFiles = true;
                    System.out.println("[GLOBAL] Reading " + file.getFileName());

                    Map<String, Map<String, List<Map<String, Object>>>> localIndex = gson.fromJson(
                            new FileReader(file.toFile()),
                            new TypeToken<Map<String, Map<String, List<Map<String, Object>>>>>() {}.getType()
                    );

                    mergeIndexes(globalIndex, localIndex);
                }

                if (foundFiles) {
                    Files.writeString(GLOBAL_INDEX_FILE, gson.toJson(globalIndex));
                    System.out.println("[GLOBAL] Combined index written to " + GLOBAL_INDEX_FILE);
                } else {
                    System.out.println("[GLOBAL] No local index files found in " + INDEX_DIR);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return globalIndex;
    }

    /**
     * Merges a local index (with {book_id, count}) into the global one.
     */
    private void mergeIndexes(Map<String, Map<String, List<Map<String, Object>>>> global,
                              Map<String, Map<String, List<Map<String, Object>>>> local) {

        for (var letterEntry : local.entrySet()) {
            String letter = letterEntry.getKey();
            Map<String, List<Map<String, Object>>> localGroup = letterEntry.getValue();

            Map<String, List<Map<String, Object>>> globalGroup =
                    global.computeIfAbsent(letter, k -> new TreeMap<>());

            for (var tokenEntry : localGroup.entrySet()) {
                String token = tokenEntry.getKey();
                List<Map<String, Object>> localBookEntries = tokenEntry.getValue();

                List<Map<String, Object>> globalBookEntries =
                        globalGroup.computeIfAbsent(token, k -> new ArrayList<>());

                for (Map<String, Object> localBook : localBookEntries) {
                    int bookId = ((Number) localBook.get("book_id")).intValue();
                    int localCount = ((Number) localBook.get("count")).intValue();

                    // Check if this book already exists in the global entry
                    Optional<Map<String, Object>> existingEntry = globalBookEntries.stream()
                            .filter(e -> ((Number) e.get("book_id")).intValue() == bookId)
                            .findFirst();

                    if (existingEntry.isPresent()) {
                        // Add count if book already exists
                        int existingCount = ((Number) existingEntry.get().get("count")).intValue();
                        existingEntry.get().put("count", existingCount + localCount);
                    } else {
                        // Add new book entry
                        Map<String, Object> newEntry = new LinkedHashMap<>();
                        newEntry.put("book_id", bookId);
                        newEntry.put("count", localCount);
                        globalBookEntries.add(newEntry);
                    }
                }
            }
        }
    }
}