package com.bd.search;

import java.util.List;

/**
 * Simple search result count test without benchmark utils
 */
public class SimpleSearchTest {
    
    public static void main(String[] args) {
        SearchService searchService = new SearchService();
        
        System.out.println("=== Simple Search Result Count Test ===");
        
        // Test common terms
        String[] testTerms = {"the", "and", "a", "of", "to", "in", "for", "is", "on", "that"};
        
        for (String term : testTerms) {
            try {
                List<Book> results = searchService.search(term, null, null, null);
                int count = results != null ? results.size() : 0;
                System.out.printf("%-6s: %5d results", "'" + term + "'", count);
                
                if (results != null && results.size() > 0) {
                    // Show first few book IDs
                    System.out.print(" (Books: ");
                    for (int i = 0; i < Math.min(5, results.size()); i++) {
                        if (i > 0) System.out.print(", ");
                        System.out.print(results.get(i).getBook_id());
                    }
                    if (results.size() > 5) {
                        System.out.print("...");
                    }
                    System.out.print(")");
                }
                System.out.println();
                
            } catch (Exception e) {
                System.out.printf("%-6s: ERROR - %s\n", "'" + term + "'", e.getMessage());
            }
        }
        
        // Test empty search to see total books
        try {
            List<Book> allResults = searchService.search("", null, null, null);
            System.out.println("\nEmpty search: " + (allResults != null ? allResults.size() : 0) + " total books");
        } catch (Exception e) {
            System.out.println("\nEmpty search: ERROR - " + e.getMessage());
        }
        
        System.out.println("\n=== Test Complete ===");
    }
}