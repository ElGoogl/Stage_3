package com.indexer;

import io.javalin.Javalin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class IndexEndpointTest {

    private static Javalin app;
    private static int port;

    @BeforeAll
    static void startServer() {
        Path lakeRoot = Paths.get("data_repository/datalake_v1");
        Path indexRoot = Paths.get("data_repository/indexes");

        app = App.start(0, lakeRoot, indexRoot);
        port = app.port();
    }

    @AfterAll
    static void stopServer() {
        if (app != null) app.stop();
    }

    @Test
    void postIndexShouldReturnOkForExistingFile() throws Exception {
        HttpClient http = HttpClient.newHttpClient();

        String body = """
                { "lakePath": "20260112/23/1346.json" }
                """;

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/index"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, resp.statusCode());
        assertTrue(resp.body().contains("\"status\":\"ok\""));
    }

    @Test
    void postIndexShouldReturnNotFoundForMissingFile() throws Exception {
        HttpClient http = HttpClient.newHttpClient();

        String body = """
                { "lakePath": "20990101/00/999999.json" }
                """;

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/index"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

        assertEquals(404, resp.statusCode());
        assertTrue(resp.body().contains("\"status\":\"not_found\""));
    }
}