package com.bd.search;

import com.google.gson.Gson;
import com.hazelcast.core.HazelcastInstance;
import io.javalin.Javalin;

/**
 * Search Service for Stage 3
 * Connects to Hazelcast distributed in-memory inverted index
 * Provides REST API for search queries
 */
public class SearchApp {
    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        String hazelcastHost = System.getenv().getOrDefault("HAZELCAST_HOST", "localhost");
        int hazelcastPort = Integer.parseInt(System.getenv().getOrDefault("HAZELCAST_PORT", "5701"));
        int serverPort = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
        String clusterName = System.getenv().getOrDefault("HZ_CLUSTER", "search-cluster");

        HazelcastClientProvider clientProvider = new HazelcastClientProvider();
        HazelcastInstance hazelcastClient = clientProvider.connect(hazelcastHost, hazelcastPort, clusterName);
        SearchService searchService = new SearchService(hazelcastClient);

        Javalin app = Javalin.create(config -> config.http.defaultContentType = "application/json")
                .start(serverPort);

        System.out.println("[SEARCH-SERVICE] Started on port " + serverPort);
        System.out.println("[SEARCH-SERVICE] Connected to Hazelcast at " + hazelcastHost + ":" + hazelcastPort);

        SearchController controller = new SearchController(gson, searchService, hazelcastClient, serverPort);
        controller.registerRoutes(app);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[SEARCH-SERVICE] Shutting down...");
            if (hazelcastClient != null) {
                hazelcastClient.shutdown();
            }
        }));
    }
}
