package com.indexer.index;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.collection.ISet;

public final class ClaimStore {

    private final ISet<Integer> claimed;

    public ClaimStore(HazelcastInstance hz) {
        this.claimed = hz.getSet("claimed-books");
    }

    public boolean tryClaim(int bookId) {
        return claimed.add(bookId);
    }

    public void release(int bookId) {
        claimed.remove(bookId);
    }
}