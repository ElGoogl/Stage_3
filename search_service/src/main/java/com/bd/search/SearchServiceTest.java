package com.bd.search;

import java.util.List;

/**
 * Simple test for SearchService functionality
 */
public class SearchServiceTest {
    
    public static void main(String[] args) {
        System.out.println("Testing SearchService functionality...");
        
        SearchService searchService = new SearchService();
        
        // Test 1: Basic search with common term
        System.out.println("\n=== Test 1: Basic search for 'the' ===");
        try {
            List<Book> results = searchService.search("the", null, null, null);
            System.out.println("Results count: " + (results != null ? results.size() : 0));
            if (results != null && !results.isEmpty()) {
                System.out.println("First few results:");
                for (int i = 0; i < Math.min(3, results.size()); i++) {
                    Book book = results.get(i);
                    System.out.println("  - ID: " + book.getBook_id() + ", Title: " + book.getTitle());
                }
            }
        } catch (Exception e) {
            System.err.println("Error in basic search: " + e.getMessage());
            e.printStackTrace();
        }
        
        // Test 2: Search with author filter
        System.out.println("\n=== Test 2: Search with author 'Jane Austen' ===");
        try {
            List<Book> results = searchService.search(null, "Jane Austen", null, null);
            System.out.println("Results count: " + (results != null ? results.size() : 0));
            if (results != null && !results.isEmpty()) {
                for (Book book : results) {
                    System.out.println("  - ID: " + book.getBook_id() + ", Title: " + book.getTitle() + ", Author: " + book.getAuthor());
                }
            }
        } catch (Exception e) {
            System.err.println("Error in author search: " + e.getMessage());
            e.printStackTrace();
        }
        
        // Test 3: Search with year filter
        System.out.println("\n=== Test 3: Search with year 1813 ===");
        try {
            List<Book> results = searchService.search(null, null, null, 1813);
            System.out.println("Results count: " + (results != null ? results.size() : 0));
            if (results != null && !results.isEmpty()) {
                for (Book book : results) {
                    System.out.println("  - ID: " + book.getBook_id() + ", Title: " + book.getTitle() + ", Year: " + book.getYear());
                }
            }
        } catch (Exception e) {
            System.err.println("Error in year search: " + e.getMessage());
            e.printStackTrace();
        }
        
        // Test 4: Combined search
        System.out.println("\n=== Test 4: Combined search for 'pride' by 'Jane Austen' ===");
        try {
            List<Book> results = searchService.search("pride", "Jane Austen", null, null);
            System.out.println("Results count: " + (results != null ? results.size() : 0));
            if (results != null && !results.isEmpty()) {
                for (Book book : results) {
                    System.out.println("  - ID: " + book.getBook_id() + ", Title: " + book.getTitle() + ", Author: " + book.getAuthor());
                }
            }
        } catch (Exception e) {
            System.err.println("Error in combined search: " + e.getMessage());
            e.printStackTrace();
        }
        
        System.out.println("\nSearchService test completed.");
    }
}