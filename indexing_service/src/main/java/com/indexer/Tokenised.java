package com.indexer;

import java.util.*;

/**
 * Tokenised builds a hierarchical token index grouped alphabetically (A, B, C, ...),
 * but instead of listing book IDs multiple times, it stores each book once
 * with its count of occurrences.
 *
 * Example output:
 * {
 *   "A": {
 *     "adventure": [ { "book_id": 9999, "count": 2 } ],
 *     "at": [ { "book_id": 9999, "count": 1 } ]
 *   }
 * }
 */
public class Tokenised {

    /**
     * Builds a hierarchical token-based index with counts per book.
     *
     * @param jsonData book JSON data
     * @param bookId   ID of the book being indexed
     * @return Map<Letter, Map<Token, List<{book_id, count}>>>
     */
    public Map<String, Map<String, List<Map<String, Object>>>> buildHierarchicalIndex(Map<String, Object> jsonData, int bookId) {

        Map<String, Map<String, List<Map<String, Object>>>> hierarchicalIndex = new TreeMap<>();

        Object contentObj = jsonData.get("content");
        if (contentObj == null) {
            System.out.println("[TOKENISED] No 'content' field found for book " + bookId);
            return hierarchicalIndex;
        }

        String text = contentObj.toString().toLowerCase(Locale.ROOT);

        String[] tokens = text.split("\\W+");

        // --- Count tokens first ---
        Map<String, Integer> tokenCounts = new HashMap<>();
        for (String token : tokens) {
            if (token.isBlank()) continue;
            tokenCounts.merge(token, 1, Integer::sum);
        }

        // --- Group tokens alphabetically and build index ---
        for (var entry : tokenCounts.entrySet()) {
            String token = entry.getKey();
            int count = entry.getValue();

            char firstChar = Character.toUpperCase(token.charAt(0));
            String group = Character.isLetter(firstChar) ? String.valueOf(firstChar) : "#";

            Map<String, List<Map<String, Object>>> groupMap =
                    hierarchicalIndex.computeIfAbsent(group, k -> new TreeMap<>());

            // each word → [{book_id, count}]
            Map<String, Object> tokenEntry = new LinkedHashMap<>();
            tokenEntry.put("book_id", bookId);
            tokenEntry.put("count", count);

            groupMap.computeIfAbsent(token, k -> new ArrayList<>()).add(tokenEntry);
        }

        System.out.println("[TOKENISED] Indexed " + hierarchicalIndex.size() + " letter groups for book " + bookId);
        return hierarchicalIndex;
    }
}
