package com.indexer.web;

import com.google.gson.Gson;
import com.indexer.dto.DocumentMetadata;
import com.indexer.index.DocumentMetadataStore;
import io.javalin.Javalin;

import java.util.List;
import java.util.Map;

public final class MetadataController {

    private final Gson gson;
    private final DocumentMetadataStore store;

    public MetadataController(Gson gson, DocumentMetadataStore store) {
        this.gson = gson;
        this.store = store;
    }

    public void registerRoutes(Javalin app) {
        app.get("/metadata/{id}", ctx -> {
            String idRaw = ctx.pathParam("id");
            Integer id = tryParseInt(idRaw);
            if (id == null) {
                ctx.status(400).result(gson.toJson(Map.of("error", "invalid id")));
                return;
            }

            DocumentMetadata md = store.get(id);
            if (md == null) {
                ctx.status(404).result(gson.toJson(Map.of("error", "not_found")));
                return;
            }

            ctx.result(gson.toJson(md));
        });

        app.get("/metadata", ctx -> {
            int offset = parseQueryInt(ctx.queryParam("offset"), 0);
            int limit = parseQueryInt(ctx.queryParam("limit"), 100);

            List<DocumentMetadata> items = store.list(offset, limit);
            ctx.result(gson.toJson(Map.of(
                    "offset", offset,
                    "limit", limit,
                    "count", items.size(),
                    "items", items
            )));
        });
    }

    private Integer tryParseInt(String raw) {
        if (raw == null || raw.isBlank()) return null;
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private int parseQueryInt(String raw, int fallback) {
        if (raw == null || raw.isBlank()) return fallback;
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
