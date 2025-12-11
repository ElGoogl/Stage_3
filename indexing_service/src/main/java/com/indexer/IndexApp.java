package com.indexer;

import io.javalin.Javalin;
import io.javalin.json.JavalinJackson;
import java.util.Map;

public class IndexApp {

    public static void main(String[] args) {
        
        Javalin app = Javalin.create(config -> {
            config.jsonMapper(new JavalinJackson());
            config.http.defaultContentType = "application/json";
        }).start(7002);

        System.out.println("[INDEXER] Service started on http://localhost:7002");

        // --- Quick test: build index for one book manually ---
        IndexService indexService = new IndexService();
        Map<String, Object> result8 = indexService.buildIndex(9998);
        Map<String, Object> result9 = indexService.buildIndex(9999);
        System.out.println(result8);
        System.out.println(result9);

        Map<String, Object> result42 = indexService.buildIndex(1342);
        System.out.println(result42);

        GlobalIndexer gi = new GlobalIndexer();
        gi.buildGlobalIndex();

        app.get("/status", ctx -> ctx.json(Map.of(
                "service", "indexing_service",
                "status", "running"
        )));
    }
}
