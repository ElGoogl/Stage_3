package com.example.crawler;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final class LocalClaimService implements ClaimService {
    private final Set<Integer> claimed = ConcurrentHashMap.newKeySet();

    @Override
    public boolean tryClaim(int bookId) {
        return claimed.add(bookId);
    }

    @Override
    public void release(int bookId) {
        claimed.remove(bookId);
    }
}
