package com.indexer;

import com.indexer.core.IndexService;
import com.indexer.core.PathResolver;
import com.indexer.web.IndexController;
import com.google.gson.Gson;
import io.javalin.Javalin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class App {

    public static Javalin start(int port, Path lakeRoot, Path indexRoot) {
        ensureDirExists(indexRoot);

        Gson gson = new Gson();

        PathResolver resolver = new PathResolver(lakeRoot);
        IndexService indexService = new IndexService(resolver);
        IndexController controller = new IndexController(gson, indexService);

        Javalin app = Javalin.create(cfg -> cfg.http.defaultContentType = "application/json");
        controller.registerRoutes(app);

        app.start(port);
        return app;
    }

    public static void main(String[] args) {
        Path lakeRoot = Paths.get(System.getProperty("lakeRoot", "data_repository/datalake_v1"));
        Path indexRoot = Paths.get(System.getProperty("indexRoot", "data_repository/indexes"));
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "7002"));

        Javalin app = start(port, lakeRoot, indexRoot);

        System.out.println("[INDEX] Started on port " + app.port());
        System.out.println("[INDEX] LAKE_ROOT  : " + lakeRoot.toAbsolutePath());
        System.out.println("[INDEX] INDEX_ROOT : " + indexRoot.toAbsolutePath());
    }

    private static void ensureDirExists(Path dir) {
        if (Files.exists(dir)) return;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create dir: " + dir, e);
        }
    }
}