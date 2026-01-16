package com.indexer.index;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.multimap.MultiMap;

import java.util.Collection;

public final class InvertedIndexStore {

    public static final String MAP_NAME = "inverted-index";

    private final MultiMap<String, Integer> mm;

    public InvertedIndexStore(HazelcastInstance hz) {
        this.mm = hz.getMultiMap(MAP_NAME);
    }

    public void put(String term, int bookId) {
        if (term == null || term.isBlank()) return;
        mm.put(term, bookId);
    }

    public Collection<Integer> get(String term) {
        return mm.get(term);
    }

    public long valueCount(String term) {
        return mm.valueCount(term);
    }
}