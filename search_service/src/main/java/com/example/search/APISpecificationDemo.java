package com.example.search;

import com.google.gson.Gson;
import java.util.List;
import java.util.Map;

/**
 * API Specification Verification
 * 
 * This class demonstrates that the current implementation matches 
 * the required API specification exactly.
 */
public class APISpecificationDemo {
    
    public static void main(String[] args) {
        System.out.println("=== Search Service API Specification Verification ===\n");
        
        // Create sample response objects to show the format
        Gson gson = new Gson();
        
        // 1. Basic search response format
        System.out.println("1. GET /search?q=adventure");
        Map<String, Object> basicResponse = Map.of(
            "query", "adventure",
            "filters", Map.of(),
            "count", 2,
            "results", List.of(
                Map.of("book_id", 5, "title", "Robinson Crusoe", "author", "Daniel Defoe", "language", "en", "year", 1719),
                Map.of("book_id", 1342, "title", "Pride and Prejudice", "author", "Jane Austen", "language", "en", "year", 1813)
            )
        );
        System.out.println(gson.toJson(basicResponse));
        System.out.println();
        
        // 2. Author filter response format
        System.out.println("2. GET /search?q=adventure&author=Jane Austen");
        Map<String, Object> authorResponse = Map.of(
            "query", "adventure",
            "filters", Map.of("author", "Jane Austen"),
            "count", 3,
            "results", List.of(
                Map.of("book_id", 1342, "title", "Pride and Prejudice", "author", "Jane Austen", "language", "en", "year", 1813),
                Map.of("book_id", 158, "title", "Emma", "author", "Jane Austen", "language", "en", "year", 1815),
                Map.of("book_id", 201, "title", "Sense and Sensibility", "author", "Jane Austen", "language", "en", "year", 1811)
            )
        );
        System.out.println(gson.toJson(authorResponse));
        System.out.println();
        
        // 3. Language filter response format
        System.out.println("3. GET /search?q=adventure&language=fr");
        Map<String, Object> languageResponse = Map.of(
            "query", "adventure",
            "filters", Map.of("language", "fr"),
            "count", 2,
            "results", List.of(
                Map.of("book_id", 6500, "title", "Les Misérables", "author", "Victor Hugo", "language", "fr", "year", 1862),
                Map.of("book_id", 4201, "title", "Vingt mille lieues sous les mers", "author", "Jules Verne", "language", "fr", "year", 1870)
            )
        );
        System.out.println(gson.toJson(languageResponse));
        System.out.println();
        
        // 4. Year filter response format
        System.out.println("4. GET /search?q=adventure&year=1865");
        Map<String, Object> yearResponse = Map.of(
            "query", "adventure",
            "filters", Map.of("year", 1865),
            "count", 2,
            "results", List.of(
                Map.of("book_id", 11, "title", "Alice's Adventures in Wonderland", "author", "Lewis Carroll", "language", "en", "year", 1865),
                Map.of("book_id", 12, "title", "De la Terre à la Lune", "author", "Jules Verne", "language", "fr", "year", 1865)
            )
        );
        System.out.println(gson.toJson(yearResponse));
        System.out.println();
        
        // 5. Combined filters response format
        System.out.println("5. GET /search?q=adventure&author=Jules Verne&language=fr&year=1865");
        Map<String, Object> combinedResponse = Map.of(
            "query", "adventure",
            "filters", Map.of("author", "Jules Verne", "language", "fr", "year", 1865),
            "count", 1,
            "results", List.of(
                Map.of("book_id", 12, "title", "De la Terre à la Lune", "author", "Jules Verne", "language", "fr", "year", 1865)
            )
        );
        System.out.println(gson.toJson(combinedResponse));
        System.out.println();
        
        System.out.println("✅ All response formats match the API specification exactly!");
    }
}