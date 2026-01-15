package com.indexer.dto;

public record IndexResponse(
        String status,
        Integer bookId,
        String lakePath,
        String resolvedPath,
        Long fileSizeBytes,
        String error
) {}