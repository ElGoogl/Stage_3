package com.bd.search;

import com.google.gson.Gson;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.multimap.MultiMap;
import io.javalin.Javalin;
import io.javalin.http.Context;

import java.util.*;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Search Service for Stage 3
 * Connects to Hazelcast distributed in-memory inverted index
 * Provides REST API for search queries
 */
public class SearchApp {
    private static final Gson gson = new Gson();
    private static HazelcastInstance hazelcastClient;
    private static MultiMap<String, Integer> invertedIndex;
    
    public static void main(String[] args) {
        // Get configuration from environment variables or use defaults
        String hazelcastHost = System.getenv().getOrDefault("HAZELCAST_HOST", "localhost");
        int hazelcastPort = Integer.parseInt(System.getenv().getOrDefault("HAZELCAST_PORT", "5701"));
        int serverPort = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
        
        // Initialize Hazelcast client
        initHazelcastClient(hazelcastHost, hazelcastPort);
        
        // Create Javalin REST API
        Javalin app = Javalin.create(config -> {
            config.http.defaultContentType = "application/json";
        }).start(serverPort);
        
        System.out.println("[SEARCH-SERVICE] Started on port " + serverPort);
        System.out.println("[SEARCH-SERVICE] Connected to Hazelcast at " + hazelcastHost + ":" + hazelcastPort);
        
        // Health check endpoint
        app.get("/status", ctx -> {
            Map<String, Object> status = new HashMap<>();
            status.put("service", "search-service");
            status.put("status", "running");
            status.put("port", serverPort);
            status.put("hazelcast_connected", hazelcastClient != null && hazelcastClient.getLifecycleService().isRunning());
            ctx.json(status);
        });
        
        // Main search endpoint
        app.get("/search", SearchApp::handleSearch);
        
        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[SEARCH-SERVICE] Shutting down...");
            if (hazelcastClient != null) {
                hazelcastClient.shutdown();
            }
        }));
    }
    
    /**
     * Initialize Hazelcast client and connect to cluster
     */
    private static void initHazelcastClient(String host, int port) {
        try {
            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setClusterName("search-cluster");
            clientConfig.getNetworkConfig().addAddress(host + ":" + port);
            
            // Connection retry settings
            clientConfig.getConnectionStrategyConfig()
                .getConnectionRetryConfig()
                .setClusterConnectTimeoutMillis(10000);
            
            hazelcastClient = HazelcastClient.newHazelcastClient(clientConfig);
            invertedIndex = hazelcastClient.getMultiMap("inverted-index");
            
            System.out.println("[SEARCH-SERVICE] Hazelcast client initialized");
        } catch (Exception e) {
            System.err.println("[SEARCH-SERVICE] Failed to connect to Hazelcast: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Handle search requests
     * Query parameters:
     *   - q: search query (required)
     *   - limit: max results to return (optional, default 100)
     */
    private static void handleSearch(Context ctx) {
        try {
            // Extract query parameter
            String query = ctx.queryParam("q");
            if (query == null || query.trim().isEmpty()) {
                ctx.status(400).json(Map.of("error", "Query parameter 'q' is required"));
                return;
            }
            
            String limitParam = ctx.queryParam("limit");
            int limit = limitParam != null ? Integer.parseInt(limitParam) : 100;
            
            // Perform search and rank results
            long startTime = System.currentTimeMillis();
            List<String> rankedResults = searchAndRank(query);
            long searchTime = System.currentTimeMillis() - startTime;
            
            // Limit results
            List<String> limitedResults = rankedResults.stream()
                .limit(limit)
                .collect(Collectors.toList());
            
            // Build response
            Map<String, Object> response = new HashMap<>();
            response.put("query", query);
            response.put("total_results", rankedResults.size());
            response.put("returned_results", limitedResults.size());
            response.put("search_time_ms", searchTime);
            response.put("documents", limitedResults);
            
            ctx.json(response);
            
        } catch (Exception e) {
            System.err.println("[SEARCH-SERVICE] Error handling search: " + e.getMessage());
            e.printStackTrace();
            ctx.status(500).json(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }
    
    /**
     * Search for documents and rank them by relevance
     * Uses term frequency as the ranking metric
     */
    private static List<String> searchAndRank(String query) {
        if (invertedIndex == null) {
            System.err.println("[SEARCH-SERVICE] Inverted index not available");
            return Collections.emptyList();
        }
        
        // Tokenize query using same logic as indexer
        List<String> tokens = tokenize(query);
        
        if (tokens.isEmpty()) {
            return Collections.emptyList();
        }
        
        // Count term frequency for each document
        Map<Integer, Integer> documentScores = new HashMap<>();
        
        for (String token : tokens) {
            Collection<Integer> docs = invertedIndex.get(token);
            if (docs != null) {
                for (Integer docId : docs) {
                    documentScores.put(docId, documentScores.getOrDefault(docId, 0) + 1);
                }
            }
        }
        
        // For multi-word queries, only keep documents that contain ALL terms (AND semantics)
        if (tokens.size() > 1) {
            final int requiredMatches = tokens.size();
            documentScores.entrySet().removeIf(entry -> entry.getValue() < requiredMatches);
        }
        
        // Sort by score (term frequency) descending, then by document ID
        return documentScores.entrySet().stream()
            .sorted(Map.Entry.<Integer, Integer>comparingByValue().reversed()
                .thenComparing(Map.Entry.comparingByKey()))
            .map(entry -> String.valueOf(entry.getKey()))
            .collect(Collectors.toList());
    }
    
    /**
     * Tokenize text using same logic as the indexing service
     * Matches Tokenizer.java: Unicode letters/digits, min length 2
     */
    private static List<String> tokenize(String text) {
        if (text == null || text.isBlank()) {
            return Collections.emptyList();
        }
        
        // Normalize: lowercase, replace non-letter/digit with space
        String cleaned = text.toLowerCase(Locale.ROOT)
            .replaceAll("[^\\p{L}\\p{Nd}]+", " ")
            .trim();
        
        if (cleaned.isEmpty()) {
            return Collections.emptyList();
        }
        
        // Split and filter by min length
        return Arrays.stream(cleaned.split("\\s+"))
            .filter(token -> token.length() >= 2)
            .collect(Collectors.toList());
    }
    
    /**
     * Search for documents in the distributed inverted index (deprecated - use searchAndRank)
     * Supports single word and multi-word queries
     */
    private static Set<String> searchInIndex(String query) {
        if (invertedIndex == null) {
            System.err.println("[SEARCH-SERVICE] Inverted index not available");
            return Collections.emptySet();
        }
        
        // Tokenize query using same logic as indexer
        List<String> tokens = tokenize(query);
        
        if (tokens.isEmpty()) {
            return Collections.emptySet();
        }
        
        // For single word query
        if (tokens.size() == 1) {
            Collection<Integer> docs = invertedIndex.get(tokens.get(0));
            return docs != null ? docs.stream().map(String::valueOf).collect(Collectors.toSet()) : Collections.emptySet();
        }
        
        // For multi-word query, find intersection (AND semantics)
        Set<Integer> result = null;
        for (String token : tokens) {
            Collection<Integer> docs = invertedIndex.get(token);
            Set<Integer> docSet = docs != null ? new HashSet<>(docs) : Collections.emptySet();
            
            if (result == null) {
                result = docSet;
            } else {
                result.retainAll(docSet); // Intersection
            }
            
            // Early exit if no common documents
            if (result.isEmpty()) {
                break;
            }
        }
        
        return result != null ? result.stream().map(String::valueOf).collect(Collectors.toSet()) : Collections.emptySet();
    }
}
