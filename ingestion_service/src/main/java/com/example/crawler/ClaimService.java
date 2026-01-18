package com.example.crawler;

interface ClaimService {
    boolean tryClaim(int bookId);

    void release(int bookId);
}
