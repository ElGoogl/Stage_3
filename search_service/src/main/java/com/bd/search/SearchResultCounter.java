package com.bd.search;

import java.util.List;

/**
 * Simple test to check search result counts for benchmark queries
 */
public class SearchResultCounter {
    
    public static void main(String[] args) {
        SearchService searchService = new SearchService();
        
        // Get benchmark configuration
        SearchBenchmarkUtils.BenchmarkConfig config = 
            SearchBenchmarkUtils.setupBenchmarkConfiguration(searchService);
        
        System.out.println("=== Search Result Count Analysis ===");
        System.out.println();
        
        // Test basic search
        System.out.println("1. Basic Search Results:");
        testSearchAndCount("Basic term", searchService, config.searchTerm, null, null, null);
        
        // Test filtered search
        System.out.println("\n2. Filtered Search Results:");
        testSearchAndCount("With author filter", searchService, config.searchTerm, config.authorFilter, null, null);
        
        // Test complex search
        System.out.println("\n3. Complex Search Results:");
        testSearchAndCount("All filters", searchService, config.searchTerm, config.authorFilter, config.languageFilter, config.yearFilter);
        
        // Test multi-word search
        System.out.println("\n4. Multi-word Search Results:");
        testSearchAndCount("Multi-word query", searchService, config.multiWordSearch, null, null, null);
        
        // Test common searches
        System.out.println("\n5. Common Term Searches:");
        testCommonTerms(searchService);
        
        System.out.println("\n=== Analysis Complete ===");
    }
    
    private static void testSearchAndCount(String description, SearchService searchService, 
                                          String term, String author, String language, Integer year) {
        try {
            List<Book> results = searchService.search(term, author, language, year);
            int count = results != null ? results.size() : 0;
            
            System.out.printf("  %-20s: %5d results | Term: '%s'", description, count, term);
            if (author != null) System.out.printf(" | Author: '%s'", author);
            if (language != null) System.out.printf(" | Language: '%s'", language);
            if (year != null) System.out.printf(" | Year: %d", year);
            System.out.println();
            
        } catch (Exception e) {
            System.out.printf("  %-20s: ERROR - %s\n", description, e.getMessage());
        }
    }
    
    private static void testCommonTerms(SearchService searchService) {
        String[] commonTerms = {"the", "and", "of", "to", "a", "in", "for", "is", "on", "that"};
        
        for (String term : commonTerms) {
            try {
                List<Book> results = searchService.search(term, null, null, null);
                int count = results != null ? results.size() : 0;
                System.out.printf("  %-10s: %5d results\n", "'" + term + "'", count);
            } catch (Exception e) {
                System.out.printf("  %-10s: ERROR - %s\n", "'" + term + "'", e.getMessage());
            }
        }
    }
}