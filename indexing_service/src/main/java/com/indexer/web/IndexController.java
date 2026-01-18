package com.indexer.web;

import com.google.gson.Gson;
import com.indexer.core.IndexService;
import com.indexer.dto.IndexRequest;
import com.indexer.dto.IndexResponse;
import io.javalin.Javalin;

import java.util.Map;

public final class IndexController {

    private final Gson gson;
    private final IndexService indexService;

    public IndexController(Gson gson, IndexService indexService) {
        this.gson = gson;
        this.indexService = indexService;
    }

    public void registerRoutes(Javalin app) {

        app.get("/health", ctx -> ctx.result(gson.toJson(Map.of("status", "ok"))));

        // validate + index + write index datei
        app.post("/index", ctx -> {
            IndexRequest req;
            try {
                req = gson.fromJson(ctx.body(), IndexRequest.class);
            } catch (Exception e) {
                ctx.status(400).result(gson.toJson(Map.of("error", "invalid json")));
                return;
            }

            if (req == null || req.lakePath() == null) {
                ctx.status(400).result(gson.toJson(Map.of("error", "lakePath missing")));
                return;
            }

            IndexResponse resp = indexService.index(req.lakePath());

            int httpStatus = switch (resp.status()) {
                case "ok", "already_indexed" -> 200;
                case "bad_request" -> 400;
                case "not_found" -> 404;
                case "conflict" -> 409;
                default -> 500;
            };

            ctx.status(httpStatus).result(gson.toJson(resp));
        });
    }
}
