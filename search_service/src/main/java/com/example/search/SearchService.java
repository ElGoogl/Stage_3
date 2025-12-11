package com.example.search;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SearchService {
    private static final Gson gson = new Gson();
    
    // Language code mapping for ISO 639-1 compliance
    private static final java.util.Map<String, String> LANGUAGE_CODES = java.util.Map.of(
        "English", "en",
        "French", "fr", 
        "German", "de",
        "Spanish", "es",
        "Italian", "it",
        "Portuguese", "pt",
        "Dutch", "nl",
        "Russian", "ru",
        "Chinese", "zh",
        "Japanese", "ja"
    );
    
    private String convertToLanguageCode(String language) {
        if (language == null) return null;
        return LANGUAGE_CODES.getOrDefault(language, language.toLowerCase());
    }
    
    public List<Book> search(String query, String author, String language, Integer year) {
        // Search in inverted index for content matches
        Set<Integer> matchingBookIds = searchInInvertedIndex(query);
        
        // Then filter by metadata from database
        return searchInDatabase(matchingBookIds, author, language, year, query);
    }
    
    private Set<Integer> searchInInvertedIndex(String query) {
        Set<Integer> matchingIds = new HashSet<>();
        
        if (query == null || query.trim().isEmpty()) {
            return matchingIds;
        }
        
        String dataPath = DatabaseConfig.getProperty("data.path", "../data_repository");
        String indexFileName = DatabaseConfig.getProperty("inverted.index.file", "inverted_index.json");
        File indexFile = new File(dataPath, indexFileName);
        
        if (!indexFile.exists()) {
            System.out.println("Inverted index file not found: " + indexFile.getAbsolutePath());
            return matchingIds;
        }
        
        try {
            String content = java.nio.file.Files.readString(indexFile.toPath());
            com.google.gson.JsonObject jsonObj = com.google.gson.JsonParser.parseString(content).getAsJsonObject();
            
            // Search for the query term (case-insensitive)
            String searchTerm = query.toLowerCase().trim();
            
            if (jsonObj.has(searchTerm)) {
                com.google.gson.JsonArray bookIds = jsonObj.getAsJsonArray(searchTerm);
                for (int i = 0; i < bookIds.size(); i++) {
                    try {
                        String bookIdStr = bookIds.get(i).getAsString();
                        matchingIds.add(Integer.parseInt(bookIdStr));
                    } catch (NumberFormatException e) {
                        // Skip invalid book IDs
                    }
                }
            }
            
        } catch (Exception e) {
            System.err.println("Error reading inverted index: " + e.getMessage());
        }
        
        return matchingIds;
    }
    
    private List<Book> searchInDatabase(Set<Integer> bookIds, String author, String language, Integer year, String query) {
        List<Book> results = new ArrayList<>();
        
        try (Connection conn = DatabaseConfig.getConnection()) {
            String tableName = DatabaseConfig.getProperty("db.table.books", "books");
            StringBuilder sql = new StringBuilder("SELECT book_id, title, author, language, release_date FROM " + tableName + " WHERE 1=1");
            List<Object> parameters = new ArrayList<>();
            
            // Filter by book IDs from inverted index search
            if (!bookIds.isEmpty()) {
                sql.append(" AND book_id IN (");
                for (int i = 0; i < bookIds.size(); i++) {
                    if (i > 0) sql.append(",");
                    sql.append("?");
                }
                sql.append(")");
                parameters.addAll(bookIds);
            } else if (query != null && !query.trim().isEmpty()) {
                // If no matches in inverted index, return empty results
                // The search is content-based, not metadata-based
                return results;
            }
            
            // Add metadata filters
            if (author != null && !author.trim().isEmpty()) {
                sql.append(" AND LOWER(author) LIKE ?");
                parameters.add("%" + author.toLowerCase() + "%");
            }
            
            if (language != null && !language.trim().isEmpty()) {
                sql.append(" AND language = ?");
                parameters.add(language);
            }
            
            if (year != null) {
                sql.append(" AND release_date LIKE ?");
                parameters.add("%" + year + "%");
            }
            
            sql.append(" ORDER BY title");
            
            try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
                for (int i = 0; i < parameters.size(); i++) {
                    stmt.setObject(i + 1, parameters.get(i));
                }
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        // Extract year from release_date
                        String releaseDate = rs.getString("release_date");
                        int extractedYear = extractYearFromDate(releaseDate);
                        
                        Book book = new Book(
                            Integer.parseInt(rs.getString("book_id")), // book_id is varchar in your table
                            rs.getString("title"),
                            rs.getString("author"),
                            convertToLanguageCode(rs.getString("language")),
                            extractedYear
                        );
                        results.add(book);
                    }
                }
            }
            
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
        }
        
        return results;
    }
    
    private int extractYearFromDate(String dateStr) {
        if (dateStr == null || dateStr.trim().isEmpty()) {
            return 0;
        }
        
        // Try to extract a 4-digit year from the date string
        java.util.regex.Pattern yearPattern = java.util.regex.Pattern.compile("\\b(19|20)\\d{2}\\b");
        java.util.regex.Matcher matcher = yearPattern.matcher(dateStr);
        
        if (matcher.find()) {
            return Integer.parseInt(matcher.group());
        }
        
        return 0; // Default if no year found
    }
}