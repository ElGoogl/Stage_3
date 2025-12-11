package com.example.crawler;

import java.net.http.*;
import java.net.*;
import java.io.IOException;
import java.nio.file.*;
import java.time.*;
import java.util.*;

public class Downloader {

    private static final String BASE_URL = "https://www.gutenberg.org/cache/epub/%d/pg%d.txt";

    public static boolean downloadAndSave(int bookId, Path baseDir) {
        try {
            String url = String.format(BASE_URL, bookId, bookId);
            HttpClient client = HttpClient.newBuilder()
                    .followRedirects(HttpClient.Redirect.NORMAL)
                    .build();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(15))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                System.out.printf("Failed to download book %d: HTTP %d%n", bookId, response.statusCode());
                return false;
            }

            Map<String, String> parts = Parser.splitBook(response.body(), bookId);
            if (parts == null) return false;

            Files.createDirectories(baseDir);
            Path outFile = baseDir.resolve(bookId + ".json");
            String json = new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(parts);
            Files.writeString(outFile, json);
            System.out.printf("Book %d saved as JSON at %s%n", bookId, outFile);
            return true;

        } catch (IOException | InterruptedException e) {
            System.out.println("Error: " + e.getMessage());
            return false;
        }
    }
}
