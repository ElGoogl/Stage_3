package com.indexer.index;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.map.IMap;
import com.indexer.dto.DocumentMetadata;

import java.util.ArrayList;
import java.util.List;

public final class DocumentMetadataStore {

    public static final String MAP_NAME = "doc-metadata";

    private final IMap<Integer, DocumentMetadata> map;
    private final CPSubsystem cp;

    public DocumentMetadataStore(HazelcastInstance hz) {
        this.map = hz.getMap(MAP_NAME);
        this.cp = hz.getCPSubsystem();
    }

    public FencedLock lockFor(int bookId) {
        return cp.getLock("doc-metadata-lock-" + bookId);
    }

    public DocumentMetadata get(int bookId) {
        return map.get(bookId);
    }

    public void put(int bookId, DocumentMetadata metadata) {
        map.set(bookId, metadata);
    }

    public List<DocumentMetadata> list(int offset, int limit) {
        int safeOffset = Math.max(0, offset);
        int safeLimit = limit <= 0 ? 100 : limit;

        List<DocumentMetadata> items = new ArrayList<>(safeLimit);
        int idx = 0;
        for (DocumentMetadata md : map.values()) {
            if (idx++ < safeOffset) {
                continue;
            }
            items.add(md);
            if (items.size() >= safeLimit) {
                break;
            }
        }
        return items;
    }
}
