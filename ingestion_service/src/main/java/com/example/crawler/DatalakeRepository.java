package com.example.crawler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

final class DatalakeRepository {
    private final Path baseDir;

    DatalakeRepository(Path baseDir) {
        this.baseDir = baseDir;
    }

    Path baseDir() {
        return baseDir;
    }

    void ensureBaseDir() throws IOException {
        if (!Files.exists(baseDir)) {
            Files.createDirectories(baseDir);
        }
    }

    Path storeReplica(String date, String hour, int bookId, String jsonBody) throws IOException {
        Path folder = baseDir.resolve(date).resolve(hour);
        Files.createDirectories(folder);
        Path outFile = folder.resolve(bookId + ".json");
        Files.writeString(outFile, jsonBody, StandardCharsets.UTF_8);
        return outFile;
    }

    Optional<Path> findFile(String fileName) {
        try (var stream = Files.walk(baseDir)) {
            return stream
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().equals(fileName))
                    .findFirst();
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    boolean fileExists(String fileName) {
        return findFile(fileName).isPresent();
    }

    List<Integer> listBookIds() throws IOException {
        try (var stream = Files.walk(baseDir)) {
            return stream
                    .filter(Files::isRegularFile)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .filter(n -> n.endsWith(".json"))
                    .map(n -> n.substring(0, n.length() - ".json".length()))
                    .flatMap(s -> {
                        try {
                            return java.util.stream.Stream.of(Integer.parseInt(s));
                        } catch (NumberFormatException e) {
                            return java.util.stream.Stream.empty();
                        }
                    })
                    .sorted()
                    .toList();
        }
    }
}
