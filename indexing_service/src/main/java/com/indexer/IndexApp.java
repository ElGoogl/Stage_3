package com.indexer;

import io.javalin.Javalin;
import com.google.gson.Gson;
import java.util.Map;

public class IndexApp {

    private static final Gson gson = new Gson();
    private static final IndexService indexService = new IndexService();

    public static void main(String[] args) {

        Javalin app = Javalin.create(config -> config.http.defaultContentType = "application/json")
                .start(7002);

        System.out.println("[INDEXER] Service started on http://localhost:7002");

        app.get("/status", ctx ->
                ctx.json(Map.of("service", "indexer", "status", "running"))
        );

        app.post("/index/update/{id}", ctx -> {
            int bookId = Integer.parseInt(ctx.pathParam("id"));
            System.out.println("[INDEXER] Building index for book " + bookId);

            Map<String, Object> result = indexService.buildIndex(bookId);
            ctx.json(result);
        });
    }
}

