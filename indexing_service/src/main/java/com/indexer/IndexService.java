package com.indexer;

import java.io.FileReader;
import java.nio.file.*;
import java.util.*;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class IndexService {

    private static final Gson gson = new Gson();
    private static final String DATA_PATH = "../ingestion_service/data_repository/datalake_v1/";

    public Map<String, Object> buildIndex(int id) {
        try {
            Path jsonPath = Paths.get(DATA_PATH + id + ".json");
            Map<String, Object> jsonData = gson.fromJson(new FileReader(jsonPath.toFile()),
                    new TypeToken<Map<String, Object>>() {}.getType());

            Map<String, Object> index = createHierarchicalIndex(jsonData);

            Path indexFile = Paths.get("index_" + id + ".json");
            Files.writeString(indexFile, gson.toJson(index));

            return Map.of("status", "success", "indexFile", indexFile.toString());
        } catch (Exception e) {
            e.printStackTrace();
            return Map.of("status", "error", "message", e.getMessage());
        }
    }

    private Map<String, Object> createHierarchicalIndex(Map<String, Object> json) {
        Map<String, Object> index = new LinkedHashMap<>();

        json.forEach((key, value) -> {
            if (value instanceof Map<?, ?> nested) {
                index.put(key, createHierarchicalIndex((Map<String, Object>) nested));
            } else {
                index.put(key, "leaf");
            }
        });

        return index;
    }
}

