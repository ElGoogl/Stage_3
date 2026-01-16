package com.indexer.core;

import com.indexer.dto.BookDocument;
import com.indexer.dto.IndexResponse;
import com.indexer.index.ClaimStore;
import com.indexer.index.InvertedIndexStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class IndexService {

    private final PathResolver resolver;
    private final BookLoader loader;
    private final Tokenizer tokenizer;
    private final InvertedIndexStore invertedIndex;
    private final ClaimStore claimStore;

    public IndexService(
            PathResolver resolver,
            BookLoader loader,
            Tokenizer tokenizer,
            InvertedIndexStore invertedIndex,
            ClaimStore claimStore
    ) {
        this.resolver = resolver;
        this.loader = loader;
        this.tokenizer = tokenizer;
        this.invertedIndex = invertedIndex;
        this.claimStore = claimStore;
    }

    public IndexResponse validateAndDescribe(String lakePath) {
        if (lakePath == null || lakePath.isBlank()) {
            return new IndexResponse("bad_request", null, lakePath, null, null, "lakePath missing");
        }

        Path resolved = resolver.resolve(lakePath);

        if (!Files.exists(resolved) || !Files.isRegularFile(resolved)) {
            return new IndexResponse("not_found", tryParseBookId(resolved), lakePath, normalize(resolved), null, "file not found");
        }

        long size;
        try {
            size = Files.size(resolved);
        } catch (IOException e) {
            return new IndexResponse("error", tryParseBookId(resolved), lakePath, normalize(resolved), null, "cannot read file size");
        }

        return new IndexResponse("ok", tryParseBookId(resolved), lakePath, normalize(resolved), size, null);
    }

    public IndexResponse index(String lakePath) {
        IndexResponse base = validateAndDescribe(lakePath);
        if (!"ok".equals(base.status())) return base;

        Path resolved = resolver.resolve(lakePath);

        BookDocument doc;
        try {
            doc = loader.load(resolved);
        } catch (IOException e) {
            return new IndexResponse("error", base.bookId(), lakePath, normalize(resolved), base.fileSizeBytes(), "cannot parse book json");
        }

        int bookId = doc.id();

        boolean claimed = claimStore.tryClaim(bookId);
        if (!claimed) {
            return new IndexResponse("already_indexed", bookId, lakePath, normalize(resolved), base.fileSizeBytes(), null);
        }

        List<String> terms = tokenizer.tokenize(doc.content());

        for (String t : terms) {
            invertedIndex.put(t, bookId);
        }

        return new IndexResponse("indexed", bookId, lakePath, normalize(resolved), base.fileSizeBytes(), "terms=" + terms.size());
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