package com.bd.search;

import com.google.gson.Gson;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Hazelcast;
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
    private static String dataRepositoryPath;
    
    public static void main(String[] args) {
        // Get configuration from environment variables or use defaults
        String hazelcastHost = System.getenv().getOrDefault("HAZELCAST_HOST", "localhost");
        int hazelcastPort = Integer.parseInt(System.getenv().getOrDefault("HAZELCAST_PORT", "5701"));
        int serverPort = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
        String clusterName = System.getenv().getOrDefault("HZ_CLUSTER", "search-cluster");
        dataRepositoryPath = System.getenv().getOrDefault("DATA_REPOSITORY_PATH", "../data_repository");
        
        // Initialize Hazelcast client
        initHazelcastClient(hazelcastHost, hazelcastPort, clusterName);
        
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
        
        // Main search endpoint (with metadata and ranking)
        app.get("/search", SearchApp::handleSearch);
        
        // Legacy simple search endpoint (returns only IDs)
        app.get("/search/simple", SearchApp::handleSimpleSearch);
        
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
     * Falls back to embedded instance if external cluster is unavailable
     */
    private static void initHazelcastClient(String host, int port, String clusterName) {
        // Try to connect to external cluster first
        try {
            System.out.println("[SEARCH-SERVICE] Attempting to connect to Hazelcast at " + host + ":" + port);
            ClientConfig clientConfig = buildClientConfig(host, port, clusterName);
            
            // Shorter timeout for quick failover to embedded mode
            clientConfig.getConnectionStrategyConfig()
                .getConnectionRetryConfig()
                .setClusterConnectTimeoutMillis(5000);
            
            hazelcastClient = HazelcastClient.newHazelcastClient(clientConfig);
            invertedIndex = hazelcastClient.getMultiMap("inverted-index");
            
            System.out.println("[SEARCH-SERVICE] Connected to external Hazelcast cluster");
            return;
        } catch (Exception e) {
            System.out.println("[SEARCH-SERVICE] Failed to connect to external cluster: " + e.getMessage());
            System.out.println("[SEARCH-SERVICE] Falling back to embedded Hazelcast instance");
        }
        
        // Fall back to embedded Hazelcast instance
        try {
            Config config = new Config();
            config.setClusterName("search-cluster-embedded");
            
            // Disable multicast and TCP-IP join for standalone mode
            config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
            
            hazelcastClient = Hazelcast.newHazelcastInstance(config);
            invertedIndex = hazelcastClient.getMultiMap("inverted-index");
            
            System.out.println("[SEARCH-SERVICE] Started embedded Hazelcast instance (standalone mode)");
        } catch (Exception e) {
            System.err.println("[SEARCH-SERVICE] Failed to start embedded Hazelcast: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static ClientConfig buildClientConfig(String host, int port, String clusterName) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterName);
        clientConfig.getNetworkConfig().addAddress(host + ":" + port);

        NearCacheConfig nearCache = new NearCacheConfig("inverted-index");
        nearCache.setInvalidateOnChange(true);
        nearCache.setInMemoryFormat(InMemoryFormat.BINARY);
        nearCache.setCacheLocalEntries(true);
        nearCache.setEvictionConfig(new EvictionConfig()
            .setEvictionPolicy(EvictionPolicy.NONE)
            .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
            .setSize(0));
        clientConfig.addNearCacheConfig(nearCache);

        return clientConfig;
    }
    
    /**
     * Handle search requests with metadata and ranking
     * Query parameters:
     *   - q: search query (required)
     *   - author: filter by author (optional)
     *   - language: filter by language (optional)
     *   - year: filter by year (optional)
     *   - limit: max results to return (optional, default 100)
     */
    private static void handleSearch(Context ctx) {
        try {
            // Extract query parameters
            String query = ctx.queryParam("q");
            if (query == null || query.trim().isEmpty()) {
                ctx.status(400).json(Map.of("error", "Query parameter 'q' is required"));
                return;
            }
            
            String author = ctx.queryParam("author");
            String language = ctx.queryParam("language");
            String yearStr = ctx.queryParam("year");
            String limitParam = ctx.queryParam("limit");
            
            Integer year = null;
            if (yearStr != null && !yearStr.trim().isEmpty()) {
                try {
                    year = Integer.parseInt(yearStr);
                } catch (NumberFormatException e) {
                    ctx.status(400).json(Map.of("error", "Invalid year format"));
                    return;
                }
            }
            
            int limit = limitParam != null ? Integer.parseInt(limitParam) : 100;
            
            // Perform search with metadata and ranking
            long startTime = System.currentTimeMillis();
            List<RankedBook> rankedResults = searchWithMetadata(query, author, language, year);
            long searchTime = System.currentTimeMillis() - startTime;
            
            // Limit results
            List<RankedBook> limitedResults = rankedResults.stream()
                .limit(limit)
                .collect(Collectors.toList());
            
            // Build filters object
            Map<String, Object> filters = new HashMap<>();
            if (author != null && !author.trim().isEmpty()) {
                filters.put("author", author);
            }
            if (language != null && !language.trim().isEmpty()) {
                filters.put("language", language);
            }
            if (year != null) {
                filters.put("year", year);
            }
            
            // Build response
            Map<String, Object> response = new HashMap<>();
            response.put("query", query);
            response.put("filters", filters);
            response.put("total_results", rankedResults.size());
            response.put("returned_results", limitedResults.size());
            response.put("search_time_ms", searchTime);
            response.put("results", limitedResults);
            
            ctx.json(response);
            
        } catch (Exception e) {
            System.err.println("[SEARCH-SERVICE] Error handling search: " + e.getMessage());
            e.printStackTrace();
            ctx.status(500).json(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }
    
    /**
     * Handle simple search requests (legacy - returns only IDs)
     * Query parameters:
     *   - q: search query (required)
     *   - limit: max results to return (optional, default 100)
     */
    private static void handleSimpleSearch(Context ctx) {
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
     * Search with metadata extraction and TF-IDF ranking
     */
    private static List<RankedBook> searchWithMetadata(String query, String author, 
                                                       String language, Integer year) {
        if (invertedIndex == null) {
            System.err.println("[SEARCH-SERVICE] Inverted index not available");
            return Collections.emptyList();
        }
        
        // Step 1: Get matching document IDs from inverted index
        Set<Integer> matchingBookIds = searchInInvertedIndex(query);
        
        // If no text query, we might want all books (for metadata-only search)
        boolean hasTextQuery = query != null && !query.trim().isEmpty();
        
        // Step 2: Extract metadata for matching books
        List<Book> books = new ArrayList<>();
        
        if (hasTextQuery && matchingBookIds.isEmpty()) {
            // No matches from text search
            return Collections.emptyList();
        }
        
        // Get books with metadata
        Set<Integer> idsToFetch = hasTextQuery ? matchingBookIds : getAllBookIds();
        
        for (Integer bookId : idsToFetch) {
            BookMetadata metadata = MetadataExtractor.getMetadata(bookId, dataRepositoryPath);
            
            if (metadata != null) {
                // Apply metadata filters
                if (!matchesFilters(metadata, author, language, year)) {
                    continue;
                }
                
                Book book = new Book(
                    metadata.getBookId(),
                    metadata.getTitle(),
                    metadata.getAuthor(),
                    metadata.getLanguage(),
                    metadata.getYear()
                );
                books.add(book);
            }
        }
        
        // Step 3: Rank books using TF-IDF if we have a text query
        if (hasTextQuery && !books.isEmpty()) {
            return RankingService.rankBooks(books, query, hazelcastClient);
        }
        
        // No ranking needed, just return as RankedBooks
        return books.stream()
            .map(RankedBook::new)
            .collect(Collectors.toList());
    }
    
    /**
     * Search in inverted index and return matching document IDs
     */
    private static Set<Integer> searchInInvertedIndex(String query) {
        Set<Integer> matchingIds = new HashSet<>();
        
        if (query == null || query.trim().isEmpty()) {
            return matchingIds;
        }
        
        List<String> tokens = tokenize(query);
        if (tokens.isEmpty()) {
            return matchingIds;
        }
        
        boolean firstTerm = true;
        
        for (String token : tokens) {
            Collection<Integer> termMatches = invertedIndex.get(token);
            Set<Integer> termSet = termMatches != null ? new HashSet<>(termMatches) : Collections.emptySet();
            
            if (firstTerm) {
                matchingIds.addAll(termSet);
                firstTerm = false;
            } else {
                // AND semantics: intersection
                matchingIds.retainAll(termSet);
            }
            
            // Early exit if no matches
            if (matchingIds.isEmpty()) {
                break;
            }
        }
        
        return matchingIds;
    }
    
    /**
     * Check if metadata matches the given filters
     */
    private static boolean matchesFilters(BookMetadata metadata, String author, 
                                         String language, Integer year) {
        if (author != null && !author.trim().isEmpty()) {
            if (metadata.getAuthor() == null || 
                !metadata.getAuthor().toLowerCase().contains(author.toLowerCase())) {
                return false;
            }
        }
        
        if (language != null && !language.trim().isEmpty()) {
            if (metadata.getLanguage() == null || 
                !metadata.getLanguage().equalsIgnoreCase(language)) {
                return false;
            }
        }
        
        if (year != null) {
            if (metadata.getYear() == null || !metadata.getYear().equals(year)) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Get all book IDs from inverted index (for metadata-only search)
     * Limited to prevent memory issues
     */
    private static Set<Integer> getAllBookIds() {
        Set<Integer> allIds = new HashSet<>();
        
        try {
            // This is a fallback for metadata-only search
            // In practice, this should have a limit or use a different approach
            // For now, return empty to avoid performance issues
            System.out.println("[SEARCH-SERVICE] Metadata-only search not fully supported yet");
        } catch (Exception e) {
            System.err.println("[SEARCH-SERVICE] Error getting all book IDs: " + e.getMessage());
        }
        
        return allIds;
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
