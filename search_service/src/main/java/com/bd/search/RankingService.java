package com.bd.search;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.multimap.MultiMap;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for ranking search results using TF-IDF scoring
 */
public class RankingService {
    
    /**
     * Rank books using TF-IDF scoring
     */
    public static List<RankedBook> rankBooks(List<Book> books, String query, 
                                           HazelcastInstance hazelcastClient) {
        if (books == null || books.isEmpty() || query == null || query.trim().isEmpty()) {
            return books.stream()
                .map(RankedBook::new)
                .collect(Collectors.toList());
        }
        
        List<RankedBook> rankedBooks = books.stream()
            .map(RankedBook::new)
            .collect(Collectors.toList());
        
        String[] terms = tokenize(query);
        Map<String, Integer> termFreqs = getTermFrequencies(terms, hazelcastClient);
        
        // Calculate TF-IDF scores for each book
        for (RankedBook book : rankedBooks) {
            double tfidfScore = calculateTfIdfScore(book, terms, termFreqs);
            book.setTfidfScore(tfidfScore);
            book.setFinalScore(tfidfScore);
        }
        
        // Sort by TF-IDF score (descending), then by year (descending)
        rankedBooks.sort((a, b) -> {
            int tfidfComparison = Double.compare(b.getTfidfScore(), a.getTfidfScore());
            if (tfidfComparison != 0) {
                return tfidfComparison;
            }
            // Sort by year if both have years
            if (a.getYear() != null && b.getYear() != null) {
                return Integer.compare(b.getYear(), a.getYear());
            }
            // Books with year come first
            if (a.getYear() != null) return -1;
            if (b.getYear() != null) return 1;
            return 0;
        });
        
        return rankedBooks;
    }
    
    /**
     * Calculate TF-IDF score for a book
     */
    private static double calculateTfIdfScore(RankedBook book, String[] terms, 
                                            Map<String, Integer> termFreqs) {
        double score = 0.0;
        int totalBooks = 100000; // Approximate total books in collection
        
        for (String term : terms) {
            int documentFreq = termFreqs.getOrDefault(term, 1);
            
            if (documentFreq > 0) {
                // Calculate IDF: log(total_docs / doc_freq)
                double idf = Math.log((double) totalBooks / documentFreq);
                
                // For simplicity, TF is 1.0 (term appears in the document)
                double tf = 1.0;
                
                score += tf * idf;
            }
        }
        
        // Normalize by number of terms
        double normalizedScore = terms.length > 0 ? score / terms.length : 0.0;
        
        // Ensure minimum score to avoid zero
        return Math.max(0.001, normalizedScore);
    }
    
    /**
     * Get term frequencies from inverted index
     */
    private static Map<String, Integer> getTermFrequencies(String[] terms, 
                                                          HazelcastInstance hazelcastClient) {
        Map<String, Integer> frequencies = new HashMap<>();
        
        if (hazelcastClient == null) {
            // Fallback: use default frequency
            for (String term : terms) {
                frequencies.put(term, 1000);
            }
            return frequencies;
        }
        
        try {
            MultiMap<String, Integer> invertedIndex = hazelcastClient.getMultiMap("inverted-index");
            
            for (String term : terms) {
                Collection<Integer> docs = invertedIndex.get(term);
                frequencies.put(term, docs != null ? docs.size() : 1);
            }
            
        } catch (Exception e) {
            System.err.println("Error getting term frequencies: " + e.getMessage());
            // Fallback to default
            for (String term : terms) {
                frequencies.put(term, 1000);
            }
        }
        
        return frequencies;
    }
    
    /**
     * Tokenize query text using same logic as indexer
     */
    private static String[] tokenize(String text) {
        if (text == null || text.isBlank()) {
            return new String[0];
        }
        
        // Normalize: lowercase, replace non-letter/digit with space
        String cleaned = text.toLowerCase(Locale.ROOT)
            .replaceAll("[^\\p{L}\\p{Nd}]+", " ")
            .trim();
        
        if (cleaned.isEmpty()) {
            return new String[0];
        }
        
        // Split and filter by min length
        return Arrays.stream(cleaned.split("\\s+"))
            .filter(token -> token.length() >= 2)
            .toArray(String[]::new);
    }
}
