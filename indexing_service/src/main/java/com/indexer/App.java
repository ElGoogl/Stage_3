package com.indexer;

import com.indexer.core.*;
import com.indexer.web.IndexController;
import com.google.gson.Gson;
import io.javalin.Javalin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class App {

    private static final Path LAKE_ROOT = Paths.get(
            System.getProperty("lakeRoot", "data_repository/datalake_v1")
    );

    private static final Path INDEX_ROOT = Paths.get(
            System.getProperty("indexRoot", "data_repository/indexes")
    );

    public static void main(String[] args) {

        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "7002"));

        ensureDirExists(INDEX_ROOT);

        Gson gson = new Gson();

        PathResolver resolver = new PathResolver(LAKE_ROOT);
        IndexService indexService = new IndexService(resolver);
        IndexController controller = new IndexController(gson, indexService);

        Javalin app = Javalin.create(cfg -> cfg.http.defaultContentType = "application/json")
                .start(port);

        controller.registerRoutes(app);

        System.out.println("[INDEX] Started on port " + port);
        System.out.println("[INDEX] LAKE_ROOT  : " + LAKE_ROOT.toAbsolutePath());
        System.out.println("[INDEX] INDEX_ROOT : " + INDEX_ROOT.toAbsolutePath());
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