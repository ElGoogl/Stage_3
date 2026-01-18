package com.bd.search;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.multimap.MultiMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class SearchService {
    private final MultiMap<String, Integer> invertedIndex;

    SearchService(HazelcastInstance hazelcastClient) {
        this.invertedIndex = hazelcastClient.getMultiMap("inverted-index");
    }

    SearchResult search(String query, int limit) {
        if (query == null || query.trim().isEmpty()) {
            return new SearchResult("", 0, List.of(), 0);
        }

        long startTime = System.currentTimeMillis();
        List<String> rankedResults = searchAndRank(query);
        long searchTime = System.currentTimeMillis() - startTime;

        List<String> limitedResults = rankedResults.stream()
                .limit(limit)
                .toList();

        return new SearchResult(query, rankedResults.size(), limitedResults, searchTime);
    }

    private List<String> searchAndRank(String query) {
        if (invertedIndex == null) {
            return Collections.emptyList();
        }

        List<String> tokens = tokenize(query);
        if (tokens.isEmpty()) {
            return Collections.emptyList();
        }

        Map<Integer, Integer> documentScores = new HashMap<>();
        for (String token : tokens) {
            Collection<Integer> docs = invertedIndex.get(token);
            for (Integer docId : docs) {
                documentScores.put(docId, documentScores.getOrDefault(docId, 0) + 1);
            }
        }

        List<Map.Entry<Integer, Integer>> sortedEntries = new ArrayList<>(documentScores.entrySet());
        sortedEntries.sort((a, b) -> b.getValue().compareTo(a.getValue()));

        return sortedEntries.stream()
                .map(entry -> "doc_" + entry.getKey())
                .toList();
    }

    private List<String> tokenize(String text) {
        if (text == null || text.isBlank()) {
            return List.of();
        }
        return Arrays.stream(text.toLowerCase(Locale.ROOT).split("\\W+"))
                .filter(token -> !token.isBlank())
                .toList();
    }
}
