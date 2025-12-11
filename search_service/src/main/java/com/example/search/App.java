package com.example.search;

import io.javalin.Javalin;
import io.javalin.http.Context;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class App {
    private static final Gson gson = new Gson();
    private static final SearchService searchService = new SearchService();
    
    public static void main(String[] args) {
        // Get server port from configuration
        int port = Integer.parseInt(DatabaseConfig.getProperty("server.port", "7002"));
        
        Javalin app = Javalin.create(config -> {
            config.http.defaultContentType = "application/json";
        }).start(port);
        
        // Health check endpoint
        app.get("/status", ctx -> {
            Map<String, Object> status = new HashMap<>();
            status.put("service", DatabaseConfig.getProperty("service.name", "search-service"));
            status.put("status", "running");
            status.put("port", port);
            ctx.result(gson.toJson(status));
        });
        
        // Main search endpoint
        app.get("/search", App::handleSearch);
        
        System.out.println("Search Service started on port " + port);
    }
    
    private static void handleSearch(Context ctx) {
        try {
            // Extract query parameters
            String query = ctx.queryParam("q");
            String author = ctx.queryParam("author");
            String language = ctx.queryParam("language");
            String yearStr = ctx.queryParam("year");
            
            // Validate required query parameter
            if (query == null || query.trim().isEmpty()) {
                ctx.status(400);
                Map<String, String> error = Map.of("error", "Query parameter 'q' is required");
                ctx.result(gson.toJson(error));
                return;
            }
            
            // Parse year if provided
            Integer year = null;
            if (yearStr != null && !yearStr.trim().isEmpty()) {
                try {
                    year = Integer.parseInt(yearStr);
                } catch (NumberFormatException e) {
                    ctx.status(400);
                    Map<String, String> error = Map.of("error", "Invalid year format");
                    ctx.result(gson.toJson(error));
                    return;
                }
            }
            
            // Perform search
            List<Book> results = searchService.search(query, author, language, year);
            
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
            Map<String, Object> response = Map.of(
                "query", query,
                "filters", filters,
                "count", results.size(),
                "results", results
            );
            
            ctx.result(gson.toJson(response));
            
        } catch (Exception e) {
            System.err.println("Error handling search request: " + e.getMessage());
            ctx.status(500);
            Map<String, String> error = Map.of("error", "Internal server error");
            ctx.result(gson.toJson(error));
        }
    }
}