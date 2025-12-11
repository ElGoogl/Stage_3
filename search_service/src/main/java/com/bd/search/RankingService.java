package com.bd.search;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.stream.Collectors;

public class RankingService {
    
    public static List<RankedBook> rankBooks(List<Book> books, String query, Set<Integer> bookIds) {
        List<RankedBook> rankedBooks = books.stream()
            .map(RankedBook::new)
            .collect(Collectors.toList());
        
        if (rankedBooks.isEmpty() || query == null) {
            return rankedBooks;
        }
        
        String[] terms = query.toLowerCase().split("\\s+");
        Map<String, Integer> termFreqs = getTermFrequencies(terms);
        
        for (RankedBook book : rankedBooks) {
            double tfidfScore = calculateTfIdfScore(book, terms, termFreqs);
            book.setTfidfScore(tfidfScore);
            book.setFinalScore(tfidfScore);
        }
        
        rankedBooks.sort((a, b) -> {
            int tfidfComparison = Double.compare(b.getTfidfScore(), a.getTfidfScore());
            if (tfidfComparison != 0) {
                return tfidfComparison;
            }
            return Integer.compare(b.getYear(), a.getYear());
        });
        return rankedBooks;
    }
    
    private static double calculateTfIdfScore(RankedBook book, String[] terms, 
                                            Map<String, Integer> termFreqs) {
        double score = 0.0;
        int totalBooks = 100000;
        
        for (String term : terms) {
            int documentFreq = termFreqs.getOrDefault(term, 1);
            
            if (documentFreq > 0) {
                double idf = Math.log((double) totalBooks / documentFreq);
                double tf = 1.0;
                score += tf * idf;
            }
        }
        
        double normalizedScore = terms.length > 0 ? score / terms.length : 0.0;
        return Math.max(0.001, normalizedScore);
    }
    
    private static Map<String, Integer> getTermFrequencies(String[] terms) {
        Map<String, Integer> frequencies = new HashMap<>();
        
        try {
            String dataPath = DatabaseConfig.getProperty("data.path", "./data_repository");
            File indexFile = new File(dataPath + "/inverted_index.json");
            
            if (!indexFile.exists()) {
                for (String term : terms) {
                    frequencies.put(term, 1000);
                }
                return frequencies;
            }
            
            try (FileReader reader = new FileReader(indexFile)) {
                JsonObject jsonObj = JsonParser.parseReader(reader).getAsJsonObject();
                
                for (String term : terms) {
                    String searchTerm = term.toLowerCase().trim();
                    if (searchTerm.length() > 0) {
                        String firstLetter = String.valueOf(searchTerm.charAt(0)).toUpperCase();
                        
                        if (jsonObj.has(firstLetter)) {
                            JsonObject letterSection = jsonObj.getAsJsonObject(firstLetter);
                            
                            if (letterSection.has(searchTerm)) {
                                JsonArray bookIds = letterSection.getAsJsonArray(searchTerm);
                                frequencies.put(term, bookIds.size());
                            } else {
                                frequencies.put(term, 1);
                            }
                        } else {
                            frequencies.put(term, 1);
                        }
                    } else {
                        frequencies.put(term, 1);
                    }
                }
            }
            
        } catch (Exception e) {
            System.err.println("Error reading inverted index: " + e.getMessage());
            for (String term : terms) {
                frequencies.put(term, 1000);
            }
        }
        
        return frequencies;
    }

}