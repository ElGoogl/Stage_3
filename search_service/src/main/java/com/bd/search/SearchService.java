package com.bd.search;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SearchService {
    
    private static final Map<String, String> LANGUAGE_CODES = Map.of(
        "English", "en", "French", "fr", "German", "de", "Spanish", "es",
        "Italian", "it", "Portuguese", "pt", "Dutch", "nl", "Russian", "ru",
        "Chinese", "zh", "Japanese", "ja"
    );
    
    public List<Book> search(String query, String author, String language, Integer year) {
        Set<Integer> matchingBookIds = searchInInvertedIndex(query);
        List<Book> books = searchInDatabase(matchingBookIds, author, language, year, query);
        
        if (query != null && !query.trim().isEmpty() && !books.isEmpty()) {
            List<RankedBook> rankedBooks = RankingService.rankBooks(books, query, matchingBookIds);
            return rankedBooks.stream()
                .map(rb -> new Book(rb.getBook_id(), rb.getTitle(), rb.getAuthor(), rb.getLanguage(), rb.getYear()))
                .collect(Collectors.toList());
        }
        
        return books;
    }
    
    public List<RankedBook> searchWithRanking(String query, String author, String language, Integer year) {
        Set<Integer> matchingBookIds = searchInInvertedIndex(query);
        List<Book> books = searchInDatabase(matchingBookIds, author, language, year, query);
        
        if (query != null && !query.trim().isEmpty() && !books.isEmpty()) {
            return RankingService.rankBooks(books, query, matchingBookIds);
        }
        
        return books.stream().map(RankedBook::new).collect(Collectors.toList());
    }
    
    private Set<Integer> searchInInvertedIndex(String query) {
        Set<Integer> matchingIds = new HashSet<>();
        
        if (query == null || query.trim().isEmpty()) {
            return matchingIds;
        }
        
        String dataPath = DatabaseConfig.getProperty("data.path", "./data_repository");
        String indexFileName = DatabaseConfig.getProperty("inverted.index.file", "inverted_index.json");
        File indexFile = new File(dataPath, indexFileName);
        
        if (!indexFile.exists()) {
            return matchingIds;
        }
        
        try {
            String content = java.nio.file.Files.readString(indexFile.toPath());
            JsonObject jsonObj = JsonParser.parseString(content).getAsJsonObject();
            
            String[] terms = query.toLowerCase().trim().split("\\s+");
            boolean firstTerm = true;
            
            for (String term : terms) {
                if (term.isEmpty()) continue;
                
                Set<Integer> termMatches = getTermMatches(jsonObj, term.trim());
                
                if (firstTerm) {
                    matchingIds.addAll(termMatches);
                    firstTerm = false;
                } else {
                    matchingIds.retainAll(termMatches);
                }
            }
            
        } catch (Exception e) {
            System.err.println("Error reading inverted index: " + e.getMessage());
        }
        
        return matchingIds;
    }
    
    private Set<Integer> getTermMatches(JsonObject jsonObj, String searchTerm) {
        Set<Integer> termMatches = new HashSet<>();
        
        if (searchTerm.length() == 0) return termMatches;
        
        String firstLetter = String.valueOf(searchTerm.charAt(0)).toUpperCase();
        
        if (jsonObj.has(firstLetter)) {
            JsonObject letterSection = jsonObj.getAsJsonObject(firstLetter);
            
            if (letterSection.has(searchTerm)) {
                JsonArray bookEntries = letterSection.getAsJsonArray(searchTerm);
                for (int i = 0; i < bookEntries.size(); i++) {
                    try {
                        JsonObject entry = bookEntries.get(i).getAsJsonObject();
                        termMatches.add(entry.get("book_id").getAsInt());
                    } catch (Exception e) {
                        // Skip invalid entries
                    }
                }
            }
        }
        
        return termMatches;
    }
    
    private List<Book> searchInDatabase(Set<Integer> bookIds, String author, String language, Integer year, String query) {
        List<Book> results = new ArrayList<>();
        
        try (Connection conn = DatabaseConfig.getConnection()) {
            String tableName = DatabaseConfig.getProperty("db.table.books", "book_metadata");
            StringBuilder sql = new StringBuilder("SELECT book_id, title, author, language, year FROM " + tableName + " WHERE 1=1");
            List<Object> parameters = new ArrayList<>();
            
            if (!bookIds.isEmpty()) {
                sql.append(" AND book_id IN (");
                for (int i = 0; i < bookIds.size(); i++) {
                    if (i > 0) sql.append(",");
                    sql.append("?");
                }
                sql.append(")");
                parameters.addAll(bookIds);
            } else if (query != null && !query.trim().isEmpty()) {
                return results;
            }
            
            if (author != null && !author.trim().isEmpty()) {
                sql.append(" AND author = ?");
                parameters.add(author.trim());
            }
            
            if (language != null && !language.trim().isEmpty()) {
                sql.append(" AND language = ?");
                parameters.add(convertToLanguageCode(language.trim()));
            }
            
            if (year != null) {
                sql.append(" AND year = ?");
                parameters.add(year);
            }
            
            sql.append(" ORDER BY title");
            
            try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
                for (int i = 0; i < parameters.size(); i++) {
                    stmt.setObject(i + 1, parameters.get(i));
                }
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        Integer yearValue = rs.getObject("year", Integer.class);
                        results.add(new Book(
                            rs.getInt("book_id"),
                            rs.getString("title"),
                            rs.getString("author"),
                            convertToLanguageCode(rs.getString("language")),
                            yearValue != null ? yearValue : 0
                        ));
                    }
                }
            }
            
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
        }
        
        return results;
    }
    
    private String convertToLanguageCode(String language) {
        if (language == null) return null;
        return LANGUAGE_CODES.getOrDefault(language, language.toLowerCase());
    }
}