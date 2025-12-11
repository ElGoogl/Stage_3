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
 * Now supports token entries with {book_id, count}.
 */
public class GlobalIndexer {

    private static final Path INDEX_DIR = Paths.get("benchmark_datalake/indexes/");
    private static final Path GLOBAL_INDEX_FILE = Paths.get("benchmark_datalake/inverted_index.json");
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    /**
     * Reads all per-book index files and merges them into one global index.
     * The new structure includes counts per book.
     */
    public Map<String, Map<String, List<Map<String, Object>>>> buildGlobalIndex() {
        Map<String, Map<String, List<Map<String, Object>>>> globalIndex = new TreeMap<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(INDEX_DIR, "index_*.json")) {
            for (Path file : stream) {
                System.out.println("[GLOBAL] Reading " + file.getFileName());

                Map<String, Map<String, List<Map<String, Object>>>> localIndex = gson.fromJson(
                        new FileReader(file.toFile()),
                        new TypeToken<Map<String, Map<String, List<Map<String, Object>>>>>() {}.getType()
                );

                mergeIndexes(globalIndex, localIndex);
            }

            Files.writeString(GLOBAL_INDEX_FILE, gson.toJson(globalIndex));
            System.out.println("[GLOBAL] Combined index written to " + GLOBAL_INDEX_FILE);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return globalIndex;
    }

    /**
     * Merge a local index (with {book_id, count}) into the global one.
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

                    // Prüfen, ob Buch bereits vorhanden ist
                    Optional<Map<String, Object>> existingEntry = globalBookEntries.stream()
                            .filter(e -> ((Number) e.get("book_id")).intValue() == bookId)
                            .findFirst();

                    if (existingEntry.isPresent()) {
                        // Buch existiert → count addieren
                        int existingCount = ((Number) existingEntry.get().get("count")).intValue();
                        existingEntry.get().put("count", existingCount + localCount);
                    } else {
                        // Neues Buch hinzufügen
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
