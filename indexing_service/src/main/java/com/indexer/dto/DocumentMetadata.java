package com.indexer.dto;

import java.io.Serializable;

public record DocumentMetadata(
        Integer bookId,
        String contentHash,
        String indexedAt,
        int tokenCount,
        String indexingNodeId,
        Status status
) implements Serializable {

    public enum Status {
        READY,
        INDEXED,
        FAILED
    }
}
