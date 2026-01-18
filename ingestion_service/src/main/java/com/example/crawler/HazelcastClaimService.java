package com.example.crawler;

import com.hazelcast.collection.ISet;

final class HazelcastClaimService implements ClaimService {
    private final ISet<Integer> claimedBooks;
    private final ClaimService fallback;

    HazelcastClaimService(ISet<Integer> claimedBooks, ClaimService fallback) {
        this.claimedBooks = claimedBooks;
        this.fallback = fallback;
    }

    @Override
    public boolean tryClaim(int bookId) {
        if (claimedBooks == null) {
            return fallback.tryClaim(bookId);
        }
        return claimedBooks.add(bookId);
    }

    @Override
    public void release(int bookId) {
        try {
            if (claimedBooks != null) {
                claimedBooks.remove(bookId);
            }
        } catch (Exception ignored) {
        } finally {
            fallback.release(bookId);
        }
    }
}
