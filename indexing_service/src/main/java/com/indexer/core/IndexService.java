package com.indexer.core;

import com.google.gson.Gson;
import com.indexer.dto.DocumentMetadata;
import com.indexer.dto.IndexResponse;
import com.indexer.index.ClaimStore;
import com.indexer.index.DocumentMetadataStore;
import com.indexer.index.IndexedStore;
import com.indexer.index.InvertedIndexStore;
import com.indexer.index.MetadataLock;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class IndexService {

    private final PathResolver resolver;
    private final Path indexRoot;

    private final ClaimStore claims;
    private final InvertedIndexStore invertedIndex;
    private final IndexedStore indexedStore;
    private final DocumentMetadataStore metadataStore;
    private final String nodeId;

    private final BookParser bookParser;
    private final Tokenizer tokenizer;

    private final Gson gson = new Gson();

    public IndexService(
            PathResolver resolver,
            Path indexRoot,
            ClaimStore claims,
            InvertedIndexStore invertedIndex,
            IndexedStore indexedStore,
            DocumentMetadataStore metadataStore,
            String nodeId,
            BookParser bookParser,
            Tokenizer tokenizer
    ) {
        this.resolver = resolver;
        this.indexRoot = indexRoot;
        this.claims = claims;
        this.invertedIndex = invertedIndex;
        this.indexedStore = indexedStore;
        this.metadataStore = metadataStore;
        this.nodeId = nodeId;
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

        boolean claimed = false;
        if (claims != null) {
            claimed = claims.tryClaim(bookId);
            if (!claimed) {
                return conflict(lakePath, resolved, bookId, "book already claimed");
            }
        }

        String hash = null;
        int tokensTotal = 0;
        int termsUnique = 0;

        try {
            BookParser.ParsedBook book = bookParser.parse(resolved);

            String text = book.combinedText();
            if (text == null || text.isBlank()) {
                return error(lakePath, resolved, "no indexable text found in json");
            }

            hash = sha256Hex(text);

            Files.createDirectories(indexRoot);
            Path out = indexRoot.resolve(bookId + ".index.json");

            MetadataLock lock = metadataStore != null ? metadataStore.lockFor(bookId) : null;
            if (lock != null) {
                lock.lock();
            }

            try {
                boolean indexFileExists = Files.exists(out);
                String existingHash = indexedStore != null ? indexedStore.getHash(bookId) : null;
                DocumentMetadata existingMd = metadataStore != null ? metadataStore.get(bookId) : null;

                boolean sameHash = hash != null && hash.equals(existingHash);
                boolean sameMetadata = existingMd != null
                        && hash != null
                        && hash.equals(existingMd.contentHash())
                        && existingMd.status() == DocumentMetadata.Status.INDEXED;

                if ((sameHash && indexFileExists) || (sameMetadata && indexFileExists)) {
                    if (metadataStore != null && existingMd == null && sameHash) {
                        metadataStore.put(bookId, new DocumentMetadata(
                                bookId,
                                hash,
                                Instant.now().toString(),
                                0,
                                nodeId,
                                DocumentMetadata.Status.INDEXED
                        ));
                    }
                    long size = safeSize(resolved);
                    return new IndexResponse(
                            "already_indexed",
                            bookId,
                            lakePath,
                            normalize(resolved),
                            size,
                            normalize(out),
                            0,
                            0,
                            null
                    );
                }

                List<String> tokens = tokenizer.tokenize(text);
                if (tokens.isEmpty()) {
                    return error(lakePath, resolved, "no tokens after tokenization");
                }

                Map<String, Integer> counts = toCounts(tokens);
                tokensTotal = tokens.size();
                termsUnique = counts.size();

                for (String term : counts.keySet()) {
                    invertedIndex.put(term, bookId);
                }

                Map<String, Object> file = new LinkedHashMap<>();
                file.put("bookId", bookId);
                file.put("sourceBookId", book.id());
                file.put("lakePath", lakePath);
                file.put("resolvedPath", normalize(resolved));
                file.put("tokensTotal", tokensTotal);
                file.put("termsUnique", termsUnique);
                file.put("hash", hash);
                file.put("terms", counts);

                IndexFileWriter.writePrettyWithBlankLines(out, file);

                if (indexedStore != null) {
                    indexedStore.putHash(bookId, hash);
                }
                if (metadataStore != null) {
                    metadataStore.put(bookId, new DocumentMetadata(
                            bookId,
                            hash,
                            Instant.now().toString(),
                            tokensTotal,
                            nodeId,
                            DocumentMetadata.Status.INDEXED
                    ));
                }

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
                if (metadataStore != null && bookId != null) {
                    metadataStore.put(bookId, new DocumentMetadata(
                            bookId,
                            hash,
                            Instant.now().toString(),
                            tokensTotal,
                            nodeId,
                            DocumentMetadata.Status.FAILED
                    ));
                }
                return error(lakePath, resolved, "io error: " + e.getMessage());
            } catch (Exception e) {
                if (metadataStore != null && bookId != null) {
                    metadataStore.put(bookId, new DocumentMetadata(
                            bookId,
                            hash,
                            Instant.now().toString(),
                            tokensTotal,
                            nodeId,
                            DocumentMetadata.Status.FAILED
                    ));
                }
                return error(lakePath, resolved, "indexing failed: " + e.getMessage());
            } finally {
                if (lock != null) {
                    lock.unlock();
                }
            }
        } catch (IOException e) {
            if (metadataStore != null && bookId != null) {
                metadataStore.put(bookId, new DocumentMetadata(
                        bookId,
                        hash,
                        Instant.now().toString(),
                        tokensTotal,
                        nodeId,
                        DocumentMetadata.Status.FAILED
                ));
            }
            return error(lakePath, resolved, "io error: " + e.getMessage());
        } catch (Exception e) {
            if (metadataStore != null && bookId != null) {
                metadataStore.put(bookId, new DocumentMetadata(
                        bookId,
                        hash,
                        Instant.now().toString(),
                        tokensTotal,
                        nodeId,
                        DocumentMetadata.Status.FAILED
                ));
            }
            return error(lakePath, resolved, "indexing failed: " + e.getMessage());
        } finally {
            if (claims != null && claimed) {
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

    private String sha256Hex(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] dig = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(dig.length * 2);
            for (byte b : dig) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException("hash failed", e);
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
