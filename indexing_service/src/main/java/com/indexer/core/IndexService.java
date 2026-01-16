package com.indexer.core;

import com.google.gson.Gson;
import com.indexer.dto.IndexResponse;
import com.indexer.index.ClaimStore;
import com.indexer.index.InvertedIndexStore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class IndexService {

    private final PathResolver resolver;
    private final Path indexRoot;
    private final ClaimStore claims;
    private final InvertedIndexStore invertedIndex;
    private final BookParser bookParser;
    private final Tokenizer tokenizer;

    private final Gson gson = new Gson();

    public IndexService(
            PathResolver resolver,
            Path indexRoot,
            ClaimStore claims,
            InvertedIndexStore invertedIndex,
            BookParser bookParser,
            Tokenizer tokenizer
    ) {
        this.resolver = resolver;
        this.indexRoot = indexRoot;
        this.claims = claims;
        this.invertedIndex = invertedIndex;
        this.bookParser = bookParser;
        this.tokenizer = tokenizer;
    }

    public IndexResponse index(String lakePath) {

        if (lakePath == null || lakePath.isBlank()) {
            return badRequest(lakePath, "lakePath missing");
        }

        Path resolved = resolver.resolve(lakePath);

        if (!Files.exists(resolved) || !Files.isRegularFile(resolved)) {
            return notFound(lakePath, resolved, "file not found");
        }

        Integer bookId = tryParseBookId(resolved);
        if (bookId == null) {
            return error(lakePath, resolved, "cannot parse book id from filename");
        }

        if (claims != null) {
            boolean ok = claims.tryClaim(bookId);
            if (!ok) {
                return conflict(lakePath, resolved, bookId, "book already claimed");
            }
        }

        try {
            BookParser.ParsedBook book = bookParser.parse(resolved);

            String text = book.combinedText();
            if (text == null || text.isBlank()) {
                return error(lakePath, resolved, "no indexable text found in json");
            }

            List<String> tokens = tokenizer.tokenize(text);
            if (tokens.isEmpty()) {
                return error(lakePath, resolved, "no tokens after tokenization");
            }

            Map<String, Integer> counts = toCounts(tokens);
            int tokensTotal = tokens.size();
            int termsUnique = counts.size();

            for (String term : counts.keySet()) {
                invertedIndex.put(term, bookId);
            }

            Files.createDirectories(indexRoot);
            Path out = indexRoot.resolve(bookId + ".index.json");

            Map<String, Object> file = new LinkedHashMap<>();
            file.put("bookId", bookId);
            file.put("sourceBookId", book.id());
            file.put("lakePath", lakePath);
            file.put("resolvedPath", normalize(resolved));
            file.put("tokensTotal", tokensTotal);
            file.put("termsUnique", termsUnique);
            file.put("terms", counts);

            IndexFileWriter.writePrettyWithBlankLines(out, file);

            long size = safeSize(resolved);

            return new IndexResponse(
                    "ok",
                    bookId,
                    lakePath,
                    normalize(resolved),
                    size,
                    normalize(out),
                    tokensTotal,
                    termsUnique,
                    null
            );

        } catch (IOException e) {
            return error(lakePath, resolved, "io error: " + e.getMessage());
        } catch (Exception e) {
            return error(lakePath, resolved, "indexing failed: " + e.getMessage());
        } finally {
            if (claims != null) {
                claims.release(bookId);
            }
        }
    }

    private Map<String, Integer> toCounts(List<String> tokens) {
        Map<String, Integer> counts = new HashMap<>();
        for (String t : tokens) {
            counts.merge(t, 1, Integer::sum);
        }
        return counts;
    }

    private long safeSize(Path p) {
        try {
            return Files.size(p);
        } catch (IOException e) {
            return -1L;
        }
    }

    private IndexResponse badRequest(String lakePath, String msg) {
        return new IndexResponse("bad_request", null, lakePath, null, null, null, null, null, msg);
    }

    private IndexResponse notFound(String lakePath, Path resolved, String msg) {
        return new IndexResponse("not_found", tryParseBookId(resolved), lakePath, normalize(resolved), null, null, null, null, msg);
    }

    private IndexResponse conflict(String lakePath, Path resolved, Integer bookId, String msg) {
        return new IndexResponse("conflict", bookId, lakePath, normalize(resolved), null, null, null, null, msg);
    }

    private IndexResponse error(String lakePath, Path resolved, String msg) {
        return new IndexResponse("error", tryParseBookId(resolved), lakePath, normalize(resolved), null, null, null, null, msg);
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
        return p == null ? null : p.toString().replace("\\", "/");
    }
}