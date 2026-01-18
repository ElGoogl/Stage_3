package com.bd.search;

import java.util.List;

final class SearchResult {
    final String query;
    final int totalResults;
    final List<String> documents;
    final long searchTimeMs;

    SearchResult(String query, int totalResults, List<String> documents, long searchTimeMs) {
        this.query = query;
        this.totalResults = totalResults;
        this.documents = documents;
        this.searchTimeMs = searchTimeMs;
    }
}
