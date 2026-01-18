package com.indexer.core;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.indexer.index.IndexedStore;
import com.indexer.index.InvertedIndexStore;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Loads existing index files into Hazelcast on startup.
 * This ensures that if the service restarts, all previously indexed data is available.
 */
public final class IndexLoader {

    private final Path indexRoot;
    private final InvertedIndexStore invertedIndex;
    private final IndexedStore indexedStore;
    private final Gson gson;

    public IndexLoader(
            Path indexRoot,
            InvertedIndexStore invertedIndex,
            IndexedStore indexedStore,
            Gson gson
    ) {
        this.indexRoot = indexRoot;
        this.invertedIndex = invertedIndex;
        this.indexedStore = indexedStore;
        this.gson = gson;
    }

    /**
     * Loads all .index.json files from the index directory into Hazelcast.
     * Returns the number of files successfully loaded.
     */
    public int loadAll() {
        if (!Files.exists(indexRoot) || !Files.isDirectory(indexRoot)) {
            System.out.println("[IndexLoader] Index directory does not exist: " + indexRoot);
            return 0;
        }

        int loaded = 0;
        int skipped = 0;
        int errors = 0;

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(indexRoot, "*.index.json")) {
            for (Path indexFile : stream) {
                try {
                    boolean success = loadIndexFile(indexFile);
                    if (success) {
                        loaded++;
                    } else {
                        skipped++;
                    }
                } catch (Exception e) {
                    errors++;
                    System.err.println("[IndexLoader] Error loading " + indexFile.getFileName() + ": " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("[IndexLoader] Error reading index directory: " + e.getMessage());
            return 0;
        }

        System.out.println("[IndexLoader] Loaded " + loaded + " index files, skipped " + skipped + ", errors " + errors);
        return loaded;
    }

    /**
     * Loads a single index file into Hazelcast.
     * Returns true if loaded, false if already exists or skipped.
     */
    private boolean loadIndexFile(Path indexFile) throws IOException {
        String content = Files.readString(indexFile);
        JsonObject json = gson.fromJson(content, JsonObject.class);

        if (!json.has("bookId") || !json.has("terms") || !json.has("hash")) {
            System.err.println("[IndexLoader] Invalid index file format: " + indexFile.getFileName());
            return false;
        }

        int bookId = json.get("bookId").getAsInt();
        String hash = json.get("hash").getAsString();

        // Check if already loaded in Hazelcast
        String existingHash = indexedStore.getHash(bookId);
        if (hash.equals(existingHash)) {
            // Already loaded with same hash, skip
            return false;
        }

        // Load terms into inverted index
        JsonObject terms = json.getAsJsonObject("terms");
        int termCount = 0;
        for (Map.Entry<String, com.google.gson.JsonElement> entry : terms.entrySet()) {
            String term = entry.getKey();
            invertedIndex.put(term, bookId);
            termCount++;
        }

        // Mark as indexed in Hazelcast
        indexedStore.putHash(bookId, hash);

        System.out.println("[IndexLoader] Loaded book " + bookId + " with " + termCount + " terms from " + indexFile.getFileName());
        return true;
    }
}
