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

        app.get("/status", ctx -> ctx.json(Map.of(
                "service", "indexing_service",
                "status", "running"
        )));
    }
}
