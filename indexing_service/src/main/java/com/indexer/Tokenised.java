package com.indexer;

import java.util.*;

/**
 * Tokenised builds a hierarchical token index grouped alphabetically (A, B, C, ...),
 * removes punctuation and stopwords, trims tokens, and counts occurrences per book.
 *
 * Example output:
 * {
 *   "A": {
 *     "adventure": [ { "book_id": 9999, "count": 2 } ]
 *   }
 * }
 */
public class Tokenised {

    // --- Define English stopwords ---
    private static final Set<String> STOPWORDS = Set.of(
            "a", "an", "and", "are", "as", "at", "be", "but", "by",
            "for", "if", "in", "into", "is", "it",
            "no", "not", "of", "on", "or", "such",
            "that", "the", "their", "then", "there", "these",
            "they", "this", "to", "was", "will", "with"
    );

    /**
     * Builds a hierarchical token-based index with counts per book.
     *
     * @param jsonData the book JSON data
     * @param bookId   the ID of the book being indexed
     * @return hierarchical Map<Letter, Map<Token, List<{book_id, count}>>>
     */
    public Map<String, Map<String, List<Map<String, Object>>>> buildHierarchicalIndex(Map<String, Object> jsonData, int bookId) {

        Map<String, Map<String, List<Map<String, Object>>>> hierarchicalIndex = new TreeMap<>();

        // --- Flexible: support "content" or "body" ---
        Object textObj = jsonData.containsKey("content")
                ? jsonData.get("content")
                : jsonData.get("body");

        if (textObj == null) {
            System.out.println("[TOKENISED] No 'content' or 'body' field found for book " + bookId);
            return hierarchicalIndex;
        }

        String text = textObj.toString().toLowerCase(Locale.ROOT);

        // --- Split text into rough tokens ---
        String[] rawTokens = text.split("\\s+");

        // --- Count cleaned tokens excluding stopwords ---
        Map<String, Integer> tokenCounts = new HashMap<>();
        for (String raw : rawTokens) {
            String token = cleanToken(raw);
            if (token.isBlank()) continue;
            if (STOPWORDS.contains(token)) continue;
            tokenCounts.merge(token, 1, Integer::sum);
        }

        // --- Group alphabetically and build index ---
        for (var entry : tokenCounts.entrySet()) {
            String token = entry.getKey();
            int count = entry.getValue();

            char firstChar = Character.toUpperCase(token.charAt(0));
            String group = Character.isLetter(firstChar) ? String.valueOf(firstChar) : "#";

            Map<String, List<Map<String, Object>>> groupMap =
                    hierarchicalIndex.computeIfAbsent(group, k -> new TreeMap<>());

            Map<String, Object> tokenEntry = new LinkedHashMap<>();
            tokenEntry.put("book_id", bookId);
            tokenEntry.put("count", count);

            groupMap.computeIfAbsent(token, k -> new ArrayList<>()).add(tokenEntry);
        }

        System.out.println("[TOKENISED] Indexed " + hierarchicalIndex.size() + " letter groups for book " + bookId);
        return hierarchicalIndex;
    }

    /**
     * Cleans a token by removing punctuation and trimming whitespace.
     */
    private String cleanToken(String token) {
        return token
                .toLowerCase(Locale.ROOT)
                .replaceAll("^[^a-z0-9]+", "") // remove punctuation at start
                .replaceAll("[^a-z0-9]+$", "") // remove punctuation at end
                .trim();
    }
}
