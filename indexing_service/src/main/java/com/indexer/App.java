package com.indexer;

import com.google.gson.Gson;
import com.indexer.core.BookParser;
import com.indexer.core.IndexService;
import com.indexer.core.PathResolver;
import com.indexer.core.Tokenizer;
import com.indexer.hz.HazelcastProvider;
import com.indexer.index.ClaimStore;
import com.indexer.index.InvertedIndexStore;
import com.indexer.web.IndexController;
import io.javalin.Javalin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class App {

    public static Javalin start(int port, Path lakeRoot, Path indexRoot) {
        ensureDirExists(indexRoot);

        String hzMembers = System.getenv().getOrDefault("HZ_MEMBERS", "");
        String hzCluster = System.getenv().getOrDefault("HZ_CLUSTER", "stage3");
        String hzNode = System.getenv().getOrDefault("NODE_ID", "indexer-" + port);

        HazelcastProvider hzProvider = new HazelcastProvider(hzMembers, hzCluster, hzNode);

        InvertedIndexStore invertedIndex = new InvertedIndexStore(hzProvider.instance());
        ClaimStore claimStore = new ClaimStore(hzProvider.instance());

        Gson gson = new Gson();
        BookParser bookParser = new BookParser(gson);
        Tokenizer tokenizer = new Tokenizer();

        PathResolver resolver = new PathResolver(lakeRoot);
        IndexService indexService = new IndexService(resolver, indexRoot, claimStore, invertedIndex, bookParser, tokenizer);
        IndexController indexController = new IndexController(gson, indexService);

        Javalin app = Javalin.create(cfg -> cfg.http.defaultContentType = "application/json");

        // health + /index
        indexController.registerRoutes(app);

        // optional: smoke endpoint bleibt zum schnellen pruefen
        app.post("/hz/smoke", ctx -> {
            String term = java.util.Optional.ofNullable(ctx.queryParam("term")).orElse("hola");
            String idRaw = java.util.Optional.ofNullable(ctx.queryParam("id")).orElse("123");
            int id = Integer.parseInt(idRaw);

            invertedIndex.put(term, id);

            ctx.result(gson.toJson(java.util.Map.of(
                    "status", "ok",
                    "term", term,
                    "count", invertedIndex.valueCount(term),
                    "docs", invertedIndex.get(term)
            )));
        });

        app.events(ev -> ev.serverStopping(hzProvider::shutdown));

        app.start(port);
        return app;
    }

    private static void ensureDirExists(Path dir) {
        if (Files.exists(dir)) return;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create dir: " + dir, e);
        }
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "7002"));

        Path lakeRoot  = Path.of("data_repository", "datalake_node1").normalize();
        Path indexRoot = Path.of("data_repository", "indexes").normalize();

        start(port, lakeRoot, indexRoot);
    }
}