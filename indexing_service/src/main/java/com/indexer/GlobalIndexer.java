package com.indexer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/**
 * GlobalIndexer merges all per book indexes (index_{ID}.json)
 * into a single inverted index file (inverted_index.json).
 *
 * It also provides a helper to count how many token occurrences
 * a given book contributes either from its local index or by
 * scanning the global inverted index as a fallback.
 */
public class GlobalIndexer {

    private static final Path INDEX_DIR = Paths.get("data_repository/indexes/");
    private static final Path GLOBAL_INDEX_FILE = Paths.get("data_repository/inverted_index.json");
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    /**
     * Reads all per book index files and merges them into one global inverted index.
     * Ensures required directories and files exist.
     */
    public Map<String, Map<String, List<Map<String, Object>>>> buildGlobalIndex() {
        Map<String, Map<String, List<Map<String, Object>>>> globalIndex = new TreeMap<>();

        try {
            // Ensure index directory exists
            Files.createDirectories(INDEX_DIR);

            // Ensure the global index file exists
            if (!Files.exists(GLOBAL_INDEX_FILE)) {
                Files.createDirectories(GLOBAL_INDEX_FILE.getParent());
                Files.createFile(GLOBAL_INDEX_FILE);
                Files.writeString(GLOBAL_INDEX_FILE, "{}");
                System.out.println("[GLOBAL] Created new empty global index file.");
            }

            // Read all local index files index_*.json
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(INDEX_DIR, "index_*.json")) {
                boolean foundFiles = false;

                for (Path file : stream) {
                    foundFiles = true;
                    System.out.println("[GLOBAL] Reading " + file.getFileName());

                    Map<String, Map<String, Number>> localIndex = gson.fromJson(
                            new FileReader(file.toFile()),
                            new TypeToken<Map<String, Map<String, Number>>>() {}.getType()
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
     * Merges a local per book index into the global inverted index structure.
     *
     * local example:
     * {
     *   "a": { "apple": 3, "atlas": 1 },
     *   "b": { "book": 5 }
     * }
     */
    private void mergeIndexes(Map<String, Map<String, List<Map<String, Object>>>> global,
                              Map<String, Map<String, Number>> local) {

        if (local == null) return;

        throw new UnsupportedOperationException(
                "mergeIndexes(global, local) requires bookId. Use mergeIndexes(global, local, bookId).");
    }

    /**
     * Overloaded merge that knows the book id of the local index and can build postings.
     *
     * @param global target global inverted index
     * @param local  local per book index
     * @param bookId id of the book the local index belongs to
     */
    public void mergeIndexes(Map<String, Map<String, List<Map<String, Object>>>> global,
                             Map<String, Map<String, Number>> local,
                             int bookId) {

        if (local == null) return;

        for (var letterEntry : local.entrySet()) {
            String letter = letterEntry.getKey();
            Map<String, Number> tokenCounts = letterEntry.getValue();
            if (tokenCounts == null) continue;

            Map<String, List<Map<String, Object>>> globalGroup =
                    global.computeIfAbsent(letter, k -> new TreeMap<>());

            for (var tokenEntry : tokenCounts.entrySet()) {
                String token = tokenEntry.getKey();
                int count = tokenEntry.getValue() == null ? 0 : tokenEntry.getValue().intValue();
                if (count <= 0) continue;

                List<Map<String, Object>> postings =
                        globalGroup.computeIfAbsent(token, k -> new ArrayList<>());

                // check if this book already exists
                Map<String, Object> existing = null;
                for (Map<String, Object> p : postings) {
                    Number id = (Number) p.get("book_id");
                    if (id != null && id.intValue() == bookId) {
                        existing = p;
                        break;
                    }
                }

                if (existing == null) {
                    Map<String, Object> p = new LinkedHashMap<>();
                    p.put("book_id", bookId);
                    p.put("count", count);
                    postings.add(p);
                } else {
                    int prev = ((Number) existing.get("count")).intValue();
                    existing.put("count", prev + count);
                }
            }
        }
    }

    /**
     * Convenience method that builds the global index by reading each local file,
     * deriving the book id from its file name index_{id}.json and merging it.
     */
    public Map<String, Map<String, List<Map<String, Object>>>> buildGlobalIndexFromLocals() {
        Map<String, Map<String, List<Map<String, Object>>>> globalIndex = new TreeMap<>();
        try {
            Files.createDirectories(INDEX_DIR);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(INDEX_DIR, "index_*.json")) {
                boolean found = false;
                for (Path file : stream) {
                    found = true;
                    String fname = file.getFileName().toString();           // index_1342.json
                    String idStr = fname.substring("index_".length(), fname.length() - ".json".length());
                    int bookId = Integer.parseInt(idStr);

                    Map<String, Map<String, Number>> localIndex = gson.fromJson(
                            new FileReader(file.toFile()),
                            new TypeToken<Map<String, Map<String, Number>>>() {}.getType()
                    );

                    mergeIndexes(globalIndex, localIndex, bookId);
                }

                if (found) {
                    Files.createDirectories(GLOBAL_INDEX_FILE.getParent());
                    Files.writeString(GLOBAL_INDEX_FILE, gson.toJson(globalIndex));
                    System.out.println("[GLOBAL] Combined index written to " + GLOBAL_INDEX_FILE);
                } else {
                    System.out.println("[GLOBAL] No local index files found in " + INDEX_DIR);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return globalIndex;
    }

    /**
     * Returns the total number of token occurrences for a given book.
     * It prefers the local per book index file index_{bookId}.json for speed.
     * If that file is missing, it falls back to scanning the global inverted index.
     */
    public long countEntries(int bookId) throws IOException {
        Path local = INDEX_DIR.resolve("index_" + bookId + ".json");
        if (Files.exists(local)) {
            return countFromLocalIndex(local);
        }
        if (Files.exists(GLOBAL_INDEX_FILE)) {
            return countFromGlobalIndex(bookId);
        }
        return 0L;
    }

    // Reads a local per book index like:
    // { "a": { "apple": 3, "atlas": 1 }, "b": { "book": 5 }, ... }
    // and returns the sum of all counts.
    private long countFromLocalIndex(Path localFile) throws IOException {
        try (FileReader r = new FileReader(localFile.toFile())) {
            JsonObject root = gson.fromJson(r, JsonObject.class);
            if (root == null || root.entrySet().isEmpty()) return 0L;

            long sum = 0L;
            for (var letterEntry : root.entrySet()) {
                JsonObject letterObj = letterEntry.getValue().getAsJsonObject();
                for (var tokenEntry : letterObj.entrySet()) {
                    var val = tokenEntry.getValue();
                    if (val.isJsonPrimitive() && val.getAsJsonPrimitive().isNumber()) {
                        sum += val.getAsLong();
                    }
                }
            }
            return sum;
        }
    }

    // Scans the global inverted index like:
    // { "a": { "apple": [ { "book_id": 1342, "count": 3 }, ... ] }, ... }
    // and returns the sum of counts for the given bookId.
    private long countFromGlobalIndex(int bookId) throws IOException {
        try (FileReader r = new FileReader(GLOBAL_INDEX_FILE.toFile())) {
            JsonObject root = gson.fromJson(r, JsonObject.class);
            if (root == null || root.entrySet().isEmpty()) return 0L;

            long sum = 0L;
            for (var letterEntry : root.entrySet()) {
                JsonObject tokensObj = letterEntry.getValue().getAsJsonObject();
                for (var tokenEntry : tokensObj.entrySet()) {
                    var postings = tokenEntry.getValue();
                    if (postings != null && postings.isJsonArray()) {
                        JsonArray arr = postings.getAsJsonArray();
                        for (var el : arr) {
                            JsonObject obj = el.getAsJsonObject();
                            var id = obj.get("book_id");
                            var cnt = obj.get("count");
                            if (id != null && cnt != null
                                    && id.isJsonPrimitive() && id.getAsInt() == bookId
                                    && cnt.isJsonPrimitive()) {
                                sum += cnt.getAsLong();
                            }
                        }
                    }
                }
            }
            return sum;
        }
    }
}
