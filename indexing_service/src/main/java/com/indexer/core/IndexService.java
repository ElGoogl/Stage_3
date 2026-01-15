package com.indexer.core;

import com.indexer.dto.IndexResponse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class IndexService {

    private final PathResolver resolver;

    public IndexService(PathResolver resolver) {
        this.resolver = resolver;
    }

    public IndexResponse validateAndDescribe(String lakePath) {
        if (lakePath == null || lakePath.isBlank()) {
            return new IndexResponse(
                    "bad_request",
                    null,
                    lakePath,
                    null,
                    null,
                    "lakePath missing"
            );
        }

        Path resolved = resolver.resolve(lakePath);

        if (!Files.exists(resolved) || !Files.isRegularFile(resolved)) {
            return new IndexResponse(
                    "not_found",
                    tryParseBookId(resolved),
                    lakePath,
                    normalize(resolved),
                    null,
                    "file not found"
            );
        }

        long size;
        try {
            size = Files.size(resolved);
        } catch (IOException e) {
            return new IndexResponse(
                    "error",
                    tryParseBookId(resolved),
                    lakePath,
                    normalize(resolved),
                    null,
                    "cannot read file size"
            );
        }

        return new IndexResponse(
                "ok",
                tryParseBookId(resolved),
                lakePath,
                normalize(resolved),
                size,
                null
        );
    }

    private Integer tryParseBookId(Path resolved) {
        if (resolved == null) return null;
        String filename = resolved.getFileName().toString();
        if (filename.endsWith(".json")) filename = filename.substring(0, filename.length() - 5);
        try {
            return Integer.parseInt(filename);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String normalize(Path p) {
        return p.toString().replace("\\", "/");
    }
}