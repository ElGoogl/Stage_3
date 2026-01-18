package com.bd.search;

import com.google.gson.Gson;
import com.hazelcast.core.HazelcastInstance;
import io.javalin.Javalin;

import java.util.HashMap;
import java.util.Map;

final class SearchController {
    private final Gson gson;
    private final SearchService searchService;
    private final HazelcastInstance hazelcastClient;
    private final int serverPort;

    SearchController(Gson gson, SearchService searchService, HazelcastInstance hazelcastClient, int serverPort) {
        this.gson = gson;
        this.searchService = searchService;
        this.hazelcastClient = hazelcastClient;
        this.serverPort = serverPort;
    }

    void registerRoutes(Javalin app) {
        app.get("/status", ctx -> {
            Map<String, Object> status = new HashMap<>();
            status.put("service", "search-service");
            status.put("status", "running");
            status.put("port", serverPort);
            status.put("hazelcast_connected", hazelcastClient != null && hazelcastClient.getLifecycleService().isRunning());
            ctx.json(status);
        });

        app.get("/search", ctx -> {
            String query = ctx.queryParam("q");
            if (query == null || query.trim().isEmpty()) {
                ctx.status(400).json(Map.of("error", "Query parameter 'q' is required"));
                return;
            }

            String limitParam = ctx.queryParam("limit");
            int limit = limitParam != null ? Integer.parseInt(limitParam) : 100;

            SearchResult result = searchService.search(query, limit);

            Map<String, Object> response = new HashMap<>();
            response.put("query", result.query);
            response.put("total_results", result.totalResults);
            response.put("returned_results", result.documents.size());
            response.put("search_time_ms", result.searchTimeMs);
            response.put("documents", result.documents);

            ctx.result(gson.toJson(response));
        });
    }
}
