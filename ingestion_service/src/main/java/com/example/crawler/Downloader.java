package com.example.crawler;

import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;

public class Downloader {

    private static final String BASE_URL =
            "https://www.gutenberg.org/cache/epub/%d/pg%d.txt";

    private static final HttpClient client = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    public static Optional<Path> downloadAndSave(int bookId, Path baseDir) {
        try {
            // Build URL for this bookId
            String url = String.format(BASE_URL, bookId, bookId);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(30))
                    .GET()
                    .build();

            HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                System.out.printf("Failed to download book %d: HTTP %d (%s)%n",
                        bookId, response.statusCode(), url);
                return Optional.empty();
            }

            Map<String, String> parts = Parser.splitBook(response.body(), bookId);
            if (parts == null) return Optional.empty();

            // Build folder yyyyMMdd/HH
            LocalDateTime now = LocalDateTime.now();
            String date = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            String hour = String.format("%02d", now.getHour());

            Path folder = baseDir.resolve(date).resolve(hour);
            Files.createDirectories(folder);

            Path outFile = folder.resolve(bookId + ".json");

            String json = new GsonBuilder()
                    .setPrettyPrinting()
                    .create()
                    .toJson(parts);

            Files.writeString(outFile, json, StandardCharsets.UTF_8);

            System.out.printf("Book %d saved at %s%n", bookId, outFile);
            return Optional.of(outFile);

        } catch (IOException | InterruptedException e) {
            System.out.println("Download error for book " + bookId + ": " + e.getMessage());
            return Optional.empty();
        }
    }
}
