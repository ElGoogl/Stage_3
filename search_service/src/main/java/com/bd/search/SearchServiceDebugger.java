package com.bd.search;

import java.util.List;

/**
 * Debug search service to understand why only 1 result is returned
 */
public class SearchServiceDebugger {
    
    public static void main(String[] args) {
        SearchService searchService = new SearchService();
        
        System.out.println("=== Search Service Debug Analysis ===");
        
        // Test 1: Check if we can get books at all (using a very common term)
        System.out.println("\n1. Basic Database Connection Test:");
        try {
            // Try to get books with a very common term that should match many books
            List<Book> testBooks = searchService.search("the", null, null, null);
            System.out.println("   Books found with 'the': " + (testBooks != null ? testBooks.size() : 0));
            
            if (testBooks != null && !testBooks.isEmpty()) {
                System.out.println("   First few books:");
                for (int i = 0; i < Math.min(5, testBooks.size()); i++) {
                    Book book = testBooks.get(i);
                    System.out.printf("   - ID: %d, Title: %s, Author: %s\n", 
                        book.getBook_id(), book.getTitle(), book.getAuthor());
                }
            }
        } catch (Exception e) {
            System.out.println("   ERROR: " + e.getMessage());
            e.printStackTrace();
        }
        
        // Test 2: Check inverted index loading
        System.out.println("\n2. Search Without Any Filters:");
        testSearch(searchService, "", null, null, null, "empty search");
        
        // Test 3: Check specific terms
        System.out.println("\n3. Testing Specific Terms:");
        String[] testTerms = {"the", "and", "a", "of", "to", "she", "he", "it"};
        for (String term : testTerms) {
            testSearch(searchService, term, null, null, null, "term: " + term);
        }
        
        // Test 4: Check case sensitivity
        System.out.println("\n4. Case Sensitivity Test:");
        testSearch(searchService, "THE", null, null, null, "uppercase THE");
        testSearch(searchService, "The", null, null, null, "capitalized The");
        testSearch(searchService, "the", null, null, null, "lowercase the");
        
        System.out.println("\n=== Debug Complete ===");
    }
    
    private static void testSearch(SearchService searchService, String term, 
                                  String author, String language, Integer year, String description) {
        try {
            List<Book> results = searchService.search(term, author, language, year);
            int count = results != null ? results.size() : 0;
            
            System.out.printf("   %-25s: %4d results", description, count);
            
            if (results != null && !results.isEmpty() && results.size() <= 3) {
                System.out.print(" (Books: ");
                for (int i = 0; i < results.size(); i++) {
                    if (i > 0) System.out.print(", ");
                    System.out.print(results.get(i).getBook_id());
                }
                System.out.print(")");
            }
            System.out.println();
            
        } catch (Exception e) {
            System.out.printf("   %-25s: ERROR - %s\n", description, e.getMessage());
        }
    }
}