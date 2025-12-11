package com.bd.search;

import com.bd.search.SearchService;
import com.bd.search.Book;


import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Shared utility class for benchmark setup
 * Provides common functionality for book metadata extraction and validation
 */
public class SearchBenchmarkUtils {
    
    private static final Set<String> STOP_WORDS = Set.of(
        "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
        "has", "he", "in", "is", "it", "its", "of", "on", "that", "the",
        "to", "was", "will", "with", "would", "could", "should", "must",
        "shall", "may", "might", "can", "do", "did", "does", "have", "had",
        "i", "you", "we", "they", "me", "him", "her", "us", "them", "my",
        "your", "his", "our", "their", "this", "these", "those"
    );

    /**
     * Benchmark configuration result containing all necessary parameters
     */
    public static class BenchmarkConfig {
        public final String searchTerm;
        public final String multiWordSearch;
        public final String authorFilter;
        public final String languageFilter;
        public final Integer yearFilter;
        public final List<Book> testBooks;
        public final Set<Integer> bookIds;

        public BenchmarkConfig(String searchTerm, String multiWordSearch, String authorFilter, 
                             String languageFilter, Integer yearFilter, List<Book> testBooks, 
                             Set<Integer> bookIds) {
            this.searchTerm = searchTerm;
            this.multiWordSearch = multiWordSearch;
            this.authorFilter = authorFilter;
            this.languageFilter = languageFilter;
            this.yearFilter = yearFilter;
            this.testBooks = testBooks;
            this.bookIds = bookIds;
        }
    }

    /**
     * Sets up comprehensive benchmark configuration with real book data and extracted terms
     */
    public static BenchmarkConfig setupBenchmarkConfiguration(SearchService searchService) {
        try {
            // Step 1: Find a valid metadata combination that has an index file
            Book validBook = findValidMetadataCombination(searchService);
            
            if (validBook == null) {
                throw new RuntimeException("No valid book with index file found");
            }
            
            // Step 2: Extract common terms from this specific book's index file
            List<String> commonTerms = extractTermsFromBookIndex(validBook.getBook_id());
            
            if (commonTerms.isEmpty()) {
                throw new RuntimeException("No terms could be extracted from index file for book " + validBook.getBook_id());
            }
            
            // Step 3: Get test books for component testing
            List<Book> allBooks = searchService.search("the", null, null, null);
            List<Book> testBooks = allBooks != null && !allBooks.isEmpty() 
                ? allBooks.stream().limit(50).collect(Collectors.toList())
                : new ArrayList<>();
                
            Set<Integer> bookIds = testBooks.stream()
                .map(Book::getBook_id)
                .collect(Collectors.toSet());
            
            // Step 4: Create configuration
            String searchTerm = commonTerms.get(0);
            String multiWordSearch = String.join(" ", commonTerms);
            
            BenchmarkConfig config = new BenchmarkConfig(
                searchTerm, 
                multiWordSearch,
                validBook.getAuthor(),
                validBook.getLanguage(), 
                validBook.getYear(),
                testBooks,
                bookIds
            );
            
            // Step 5: Validate configuration works
            validateBenchmarkConfiguration(searchService, config);
            
            return config;
            
        } catch (Exception e) {
            System.err.println("FATAL ERROR: " + e.getMessage());
            throw new RuntimeException("Benchmark setup failed - cannot proceed without valid configuration");
        }
    }

    /**
     * Finds a book with valid metadata that has a corresponding index file
     */
    private static Book findValidMetadataCombination(SearchService searchService) {
        List<Book> allBooks = searchService.search("the", null, null, null);
        
        if (allBooks != null) {
            // Sort books by ID for deterministic order
            allBooks.sort((a, b) -> Integer.compare(a.getBook_id(), b.getBook_id()));
            
            for (Book book : allBooks) {
                if (hasValidMetadata(book)) {
                    // Check if index file exists for this book - try multiple possible paths
                    String[] possiblePaths = {
                        "../data_repository/indexes/index_" + book.getBook_id() + ".json",
                        "../indexing_service/data_repository/indexes/index_" + book.getBook_id() + ".json"
                    };
                    
                    boolean indexExists = false;
                    for (String indexPath : possiblePaths) {
                        if (Files.exists(Paths.get(indexPath))) {
                            indexExists = true;
                            break;
                        }
                    }
                    
                    if (indexExists) {
                        // Test if this combination returns results
                        List<Book> testResults = searchService.search("the", book.getAuthor(), book.getLanguage(), book.getYear());
                        if (testResults != null && !testResults.isEmpty()) {
                            return book;
                        }
                    }
                }
            }
        }
        
        return null;
    }

    /**
     * Validates that a book has complete and usable metadata
     */
    private static boolean hasValidMetadata(Book book) {
        return book.getAuthor() != null && !book.getAuthor().trim().isEmpty() &&
               book.getLanguage() != null && !book.getLanguage().trim().isEmpty() &&
               book.getYear() > 0;
    }

    /**
     * Extracts common terms from a book's index file for benchmark queries
     */
    private static List<String> extractTermsFromBookIndex(int bookId) {
        try {
            // Try multiple possible paths for the index file
            String[] possiblePaths = {
                "../data_repository/indexes/index_" + bookId + ".json",
                "../indexing_service/data_repository/indexes/index_" + bookId + ".json"
            };
            
            String indexContent = null;
            for (String indexPath : possiblePaths) {
                if (Files.exists(Paths.get(indexPath))) {
                    indexContent = Files.readString(Paths.get(indexPath));
                    break;
                }
            }
            
            if (indexContent == null) {
                throw new RuntimeException("Index file not found for book " + bookId);
            }
            
            // Collect word frequencies using simple text parsing
            Map<String, Integer> wordFrequencies = new TreeMap<>(); // TreeMap for deterministic order
            
            // Parse the index JSON to extract word frequencies
            String[] lines = indexContent.split("\n");
            for (String line : lines) {
                line = line.trim();
                // Look for lines that start with a quoted word followed by array or object
                if (line.matches("^\"[a-zA-Z]{3,}\":\\s*[\\[{].*")) {
                    try {
                        // Extract word from "word": [ or "word": { format
                        int endQuote = line.indexOf("\":");
                        String word = line.substring(1, endQuote).toLowerCase();
                        
                        if (isValidBenchmarkTerm(word)) {
                            // Extract count from the index structure
                            int count = extractCountFromIndex(indexContent, word, bookId);
                            if (count > 0) {
                                wordFrequencies.put(word, count);
                            }
                        }
                    } catch (Exception e) {
                        // Skip malformed lines
                    }
                }
            }
            
            // Sort by frequency (descending) then by word (ascending) for deterministic results
            return wordFrequencies.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed()
                    .thenComparing(Map.Entry.comparingByKey()))
                .limit(4)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
                
        } catch (Exception e) {
            System.err.println("Warning: Could not extract terms from index file for book " + bookId + ": " + e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Helper method to extract count from index structure
     */
    private static int extractCountFromIndex(String indexContent, String word, int bookId) {
        try {
            // Look for both array and object formats
            String arrayPattern = "\"" + word + "\": [";
            String objectPattern = "\"" + word + "\": {";
            
            int wordStart = indexContent.indexOf(arrayPattern);
            if (wordStart == -1) {
                wordStart = indexContent.indexOf(objectPattern);
            }
            if (wordStart == -1) return 0;
            
            // Find the book_id entry for this specific book
            String bookIdPattern = "\"book_id\": " + bookId;
            int bookStart = indexContent.indexOf(bookIdPattern, wordStart);
            if (bookStart == -1) return 0;
            
            // Find the count field after the book_id
            String countPattern = "\"count\": ";
            int countStart = indexContent.indexOf(countPattern, bookStart);
            if (countStart == -1) return 0;
            
            // Extract the numeric value
            int valueStart = countStart + countPattern.length();
            int valueEnd = valueStart;
            while (valueEnd < indexContent.length() && Character.isDigit(indexContent.charAt(valueEnd))) {
                valueEnd++;
            }
            
            if (valueEnd > valueStart) {
                return Integer.parseInt(indexContent.substring(valueStart, valueEnd));
            }
        } catch (Exception e) {
            // Ignore parsing errors
        }
        return 0;
    }

    /**
     * Validates that a word is suitable for benchmark testing
     */
    private static boolean isValidBenchmarkTerm(String word) {
        return word != null && 
               word.length() >= 3 && 
               word.length() <= 15 &&
               word.matches("[a-z]+") && 
               !STOP_WORDS.contains(word);
    }

    /**
     * Validates that the benchmark configuration produces valid results
     */
    private static void validateBenchmarkConfiguration(SearchService searchService, BenchmarkConfig config) {
        List<Book> basicTest = searchService.search(config.searchTerm, null, null, null);
        List<Book> filteredTest = searchService.search(config.searchTerm, config.authorFilter, null, null);
        List<Book> complexTest = searchService.search(config.searchTerm, config.authorFilter, config.languageFilter, config.yearFilter);
        List<Book> multiWordTest = searchService.search(config.multiWordSearch, null, null, null);
        
        System.out.println("Validation results:");
        System.out.println("  - Basic search ('" + config.searchTerm + "'): " + (basicTest != null ? basicTest.size() : 0) + " results");
        System.out.println("  - Filtered search (+ author): " + (filteredTest != null ? filteredTest.size() : 0) + " results");
        System.out.println("  - Complex search (+ language + year): " + (complexTest != null ? complexTest.size() : 0) + " results");
        System.out.println("  - Multi-word search ('" + config.multiWordSearch + "'): " + (multiWordTest != null ? multiWordTest.size() : 0) + " results");
        
        if (basicTest == null || basicTest.isEmpty()) {
            throw new RuntimeException("Basic search validation failed - no results for term '" + config.searchTerm + "'");
        }
    }

    /**
     * Prints benchmark configuration information
     */
    public static void printConfiguration(BenchmarkConfig config, int bookCount) {
        System.out.println("Books available: " + bookCount);
        System.out.println("Search term: '" + config.searchTerm + "'");
        System.out.println("Multi-word: '" + config.multiWordSearch + "'");
        System.out.println("Filters: " + config.authorFilter + ", " + config.languageFilter + ", " + config.yearFilter);
        System.out.println("Test books for components: " + config.testBooks.size());
    }
}