package com.bd.search;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts metadata from book JSON files stored in the datalake
 */
public class MetadataExtractor {
    private static final Gson gson = new Gson();
    private static final Map<Integer, BookMetadata> metadataCache = new HashMap<>();
    
    // Patterns for extracting metadata from Gutenberg headers
    private static final Pattern TITLE_PATTERN = Pattern.compile("Title:\\s*(.+?)(?:\\r?\\n|$)", Pattern.MULTILINE);
    private static final Pattern AUTHOR_PATTERN = Pattern.compile("Author:\\s*(.+?)(?:\\r?\\n|$)", Pattern.MULTILINE);
    private static final Pattern LANGUAGE_PATTERN = Pattern.compile("Language:\\s*(.+?)(?:\\r?\\n|$)", Pattern.MULTILINE);
    private static final Pattern RELEASE_DATE_PATTERN = Pattern.compile("Release date:\\s*(.+?)(?:\\[|\\r?\\n)", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
    private static final Pattern TRANSLATOR_PATTERN = Pattern.compile("Translator:\\s*(.+?)(?:\\r?\\n|$)", Pattern.MULTILINE);
    
    /**
     * Get book metadata from cache or extract from file
     */
    public static BookMetadata getMetadata(int bookId, String dataRepositoryPath) {
        if (metadataCache.containsKey(bookId)) {
            return metadataCache.get(bookId);
        }
        
        BookMetadata metadata = extractMetadata(bookId, dataRepositoryPath);
        if (metadata != null) {
            metadataCache.put(bookId, metadata);
        }
        
        return metadata;
    }
    
    /**
     * Extract metadata from book JSON file
     */
    private static BookMetadata extractMetadata(int bookId, String dataRepositoryPath) {
        try {
            // Search for the book file in the datalake structure
            File bookFile = findBookFile(bookId, dataRepositoryPath);
            
            if (bookFile == null || !bookFile.exists()) {
                return createDefaultMetadata(bookId);
            }
            
            // Read and parse JSON file
            String content = Files.readString(bookFile.toPath());
            JsonObject jsonObj = gson.fromJson(content, JsonObject.class);
            
            BookMetadata metadata = new BookMetadata();
            metadata.setBookId(bookId);
            
            // Extract header information
            if (jsonObj.has("header")) {
                String header = jsonObj.get("header").getAsString();
                extractFromHeader(metadata, header);
            }
            
            // Fallback to default values if extraction failed
            if (metadata.getTitle() == null) {
                metadata.setTitle("Book " + bookId);
            }
            if (metadata.getAuthor() == null) {
                metadata.setAuthor("Unknown");
            }
            if (metadata.getLanguage() == null) {
                metadata.setLanguage("en");
            }
            
            return metadata;
            
        } catch (Exception e) {
            System.err.println("Error extracting metadata for book " + bookId + ": " + e.getMessage());
            return createDefaultMetadata(bookId);
        }
    }
    
    /**
     * Extract metadata from Gutenberg header text
     */
    private static void extractFromHeader(BookMetadata metadata, String header) {
        // Extract title
        Matcher titleMatcher = TITLE_PATTERN.matcher(header);
        if (titleMatcher.find()) {
            metadata.setTitle(titleMatcher.group(1).trim());
        }
        
        // Extract author
        Matcher authorMatcher = AUTHOR_PATTERN.matcher(header);
        if (authorMatcher.find()) {
            metadata.setAuthor(authorMatcher.group(1).trim());
        }
        
        // Extract language
        Matcher languageMatcher = LANGUAGE_PATTERN.matcher(header);
        if (languageMatcher.find()) {
            String language = languageMatcher.group(1).trim();
            metadata.setLanguage(convertToLanguageCode(language));
        }
        
        // Extract release date and year
        Matcher releaseDateMatcher = RELEASE_DATE_PATTERN.matcher(header);
        if (releaseDateMatcher.find()) {
            String releaseDate = releaseDateMatcher.group(1).trim();
            metadata.setReleaseDate(releaseDate);
            
            // Try to extract year from release date
            Integer year = extractYear(releaseDate);
            if (year != null) {
                metadata.setYear(year);
            }
        }
        
        // Extract translator
        Matcher translatorMatcher = TRANSLATOR_PATTERN.matcher(header);
        if (translatorMatcher.find()) {
            metadata.setTranslator(translatorMatcher.group(1).trim());
        }
    }
    
    /**
     * Extract year from date string
     */
    private static Integer extractYear(String dateString) {
        Pattern yearPattern = Pattern.compile("\\b(1[6-9]\\d{2}|20\\d{2})\\b");
        Matcher matcher = yearPattern.matcher(dateString);
        if (matcher.find()) {
            try {
                return Integer.parseInt(matcher.group(1));
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
    
    /**
     * Find book file in datalake structure
     */
    private static File findBookFile(int bookId, String dataRepositoryPath) {
        try {
            // Try common datalake paths
            String[] possiblePaths = {
                dataRepositoryPath + "/datalake_node1",
                dataRepositoryPath + "/datalake_node2",
                "../data_repository/datalake_node1",
                "../data_repository/datalake_node2"
            };
            
            for (String basePath : possiblePaths) {
                File result = searchInDirectory(new File(basePath), bookId + ".json");
                if (result != null) {
                    return result;
                }
            }
        } catch (Exception e) {
            System.err.println("Error finding book file: " + e.getMessage());
        }
        
        return null;
    }
    
    /**
     * Recursively search for a file in directory
     */
    private static File searchInDirectory(File directory, String fileName) {
        if (!directory.exists() || !directory.isDirectory()) {
            return null;
        }
        
        File[] files = directory.listFiles();
        if (files == null) {
            return null;
        }
        
        for (File file : files) {
            if (file.isFile() && file.getName().equals(fileName)) {
                return file;
            } else if (file.isDirectory()) {
                File result = searchInDirectory(file, fileName);
                if (result != null) {
                    return result;
                }
            }
        }
        
        return null;
    }
    
    /**
     * Convert language name to code
     */
    private static String convertToLanguageCode(String language) {
        if (language == null) return "en";
        
        Map<String, String> languageCodes = Map.of(
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
        
        return languageCodes.getOrDefault(language, language.toLowerCase());
    }
    
    /**
     * Create default metadata when extraction fails
     */
    private static BookMetadata createDefaultMetadata(int bookId) {
        BookMetadata metadata = new BookMetadata();
        metadata.setBookId(bookId);
        metadata.setTitle("Book " + bookId);
        metadata.setAuthor("Unknown");
        metadata.setLanguage("en");
        return metadata;
    }
    
    /**
     * Clear metadata cache
     */
    public static void clearCache() {
        metadataCache.clear();
    }
}
