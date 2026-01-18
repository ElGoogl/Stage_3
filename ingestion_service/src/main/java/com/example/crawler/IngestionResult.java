package com.example.crawler;

import java.util.Map;

final class IngestionResult {
    final int status;
    final Map<String, Object> payload;

    IngestionResult(int status, Map<String, Object> payload) {
        this.status = status;
        this.payload = payload;
    }
}
