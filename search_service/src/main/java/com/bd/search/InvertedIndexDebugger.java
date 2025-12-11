package com.bd.search;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * Debug the inverted index search logic
 */
public class InvertedIndexDebugger {
    
    public static void main(String[] args) {
        System.out.println("=== Inverted Index Debug Analysis ===");
        
        // Get the data path and index file
        String dataPath = DatabaseConfig.getProperty("data.path", "./data_repository");
        String indexFileName = DatabaseConfig.getProperty("inverted.index.file", "inverted_index.json");
        File indexFile = new File(dataPath, indexFileName);
        
        System.out.println("Data path: " + dataPath);
        System.out.println("Index file: " + indexFile.getAbsolutePath());
        System.out.println("Index file exists: " + indexFile.exists());
        System.out.println("Index file size: " + indexFile.length() + " bytes");
        
        if (!indexFile.exists()) {
            System.out.println("ERROR: Inverted index file not found!");
            return;
        }
        
        try {
            String content = java.nio.file.Files.readString(indexFile.toPath());
            JsonObject jsonObj = JsonParser.parseString(content).getAsJsonObject();
            
            System.out.println("\nIndex structure:");
            System.out.println("Top-level keys: " + jsonObj.keySet());
            
            // Test specific terms
            String[] testTerms = {"the", "and", "a", "of", "to"};
            
            for (String term : testTerms) {
                System.out.println("\n--- Testing term: '" + term + "' ---");
                
                String firstLetter = String.valueOf(term.charAt(0)).toUpperCase();
                System.out.println("Looking for section: " + firstLetter);
                
                if (jsonObj.has(firstLetter)) {
                    JsonObject letterSection = jsonObj.getAsJsonObject(firstLetter);
                    System.out.println("Section '" + firstLetter + "' exists");
                    System.out.println("Keys in section: " + letterSection.keySet().size() + " terms");
                    
                    // Show first few keys
                    int count = 0;
                    for (String key : letterSection.keySet()) {
                        if (count < 5) {
                            System.out.println("  - " + key);
                            count++;
                        }
                    }
                    if (letterSection.keySet().size() > 5) {
                        System.out.println("  ... and " + (letterSection.keySet().size() - 5) + " more");
                    }
                    
                    if (letterSection.has(term)) {
                        JsonArray bookEntries = letterSection.getAsJsonArray(term);
                        System.out.println("Term '" + term + "' found with " + bookEntries.size() + " book entries");
                        
                        // Show first few entries
                        for (int i = 0; i < Math.min(3, bookEntries.size()); i++) {
                            JsonObject entry = bookEntries.get(i).getAsJsonObject();
                            System.out.println("  Book " + entry.get("book_id").getAsInt() + 
                                             " (count: " + entry.get("count").getAsInt() + ")");
                        }
                        if (bookEntries.size() > 3) {
                            System.out.println("  ... and " + (bookEntries.size() - 3) + " more books");
                        }
                    } else {
                        System.out.println("Term '" + term + "' NOT found in section '" + firstLetter + "'");
                    }
                } else {
                    System.out.println("Section '" + firstLetter + "' does NOT exist");
                }
            }
            
        } catch (Exception e) {
            System.err.println("Error reading inverted index: " + e.getMessage());
            e.printStackTrace();
        }
    }
}