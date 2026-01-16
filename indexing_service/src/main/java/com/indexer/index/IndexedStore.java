package com.indexer.index;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public final class IndexedStore {

    public static final String MAP_NAME = "indexed-books";

    private final IMap<Integer, String> hashes;

    public IndexedStore(HazelcastInstance hz) {
        this.hashes = hz.getMap(MAP_NAME);
    }

    public String getHash(int bookId) {
        return hashes.get(bookId);
    }

    public void putHash(int bookId, String hash) {
        hashes.put(bookId, hash);
    }
}