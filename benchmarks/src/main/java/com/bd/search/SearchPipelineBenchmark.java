package com.bd.search;

import com.bd.search.SearchService;
import com.bd.search.Book;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Full Search Service Benchmarks
 * 
 * PURPOSE: Measures end-to-end search service performance under different data loads
 * SCOPE: Complete search workflows with dynamic scaling (10x, 100x, 1000x iterations)
 * FOCUS: System throughput and scalability analysis
 * METRICS: Milliseconds per operation (suitable for high-volume testing)
 * 
 * Use this for:
 * - Performance regression testing across different data volumes
 * - Scalability analysis as dataset grows
 * - End-to-end system performance validation
 * - Load testing with realistic query patterns
 * 
 * For individual component analysis, use SearchComponentBenchmark instead.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
public class SearchPipelineBenchmark {

    private SearchService searchService;
    private int bookCount;
    private SearchBenchmarkUtils.BenchmarkConfig config;

    @Setup(Level.Trial)
    public void setup() {
        searchService = new SearchService();
        bookCount = getBookCount();
        config = SearchBenchmarkUtils.setupBenchmarkConfiguration(searchService);
        printConfiguration();
    }

    private int getBookCount() {
        try {
            String content = Files.readString(Paths.get("../data_repository/indexed_books.json")).trim();
            if (content.startsWith("[") && content.endsWith("]")) {
                String inner = content.substring(1, content.length() - 1).trim();
                return inner.isEmpty() ? 0 : inner.split(",").length;
            }
        } catch (Exception e) {
            System.err.println("Warning: Could not read indexed_books.json: " + e.getMessage());
        }
        return 0;
    }

    private void printConfiguration() {
        SearchBenchmarkUtils.printConfiguration(config, bookCount);
        
        String message = bookCount >= 1000 ? "All benchmarks (10x, 100x, 1000x) will run" :
                        bookCount >= 100 ? "10x and 100x benchmarks will run" :
                        bookCount >= 10 ? "Only 10x benchmarks will run" :
                        "All scaled benchmarks will skip - need more books";
        System.out.println("✅ " + message);
    }

    // ========== SCALABILITY BENCHMARKS - BASIC SEARCH ==========
    // Tests end-to-end basic search performance at different scales
    
    @Benchmark
    public void basicSearch10x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 10, 10, () -> searchService.search(config.searchTerm, null, null, null));
    }

    @Benchmark
    public void basicSearch100x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 100, 100, () -> searchService.search(config.searchTerm, null, null, null));
    }

    @Benchmark
    public void basicSearch1000x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 1000, 1000, () -> searchService.search(config.searchTerm, null, null, null));
    }

    // ========== SCALABILITY BENCHMARKS - FILTERED SEARCH ==========
    // Tests end-to-end filtered search performance at different scales
    
    @Benchmark
    public void filteredSearch10x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 10, 10, () -> searchService.search(config.searchTerm, config.authorFilter, null, null));
    }

    @Benchmark
    public void filteredSearch100x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 100, 100, () -> searchService.search(config.searchTerm, config.authorFilter, null, null));
    }

    @Benchmark
    public void filteredSearch1000x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 1000, 1000, () -> searchService.search(config.searchTerm, config.authorFilter, null, null));
    }

    // ========== SCALABILITY BENCHMARKS - COMPLEX SEARCH ==========
    // Tests end-to-end complex search (all filters) performance at different scales
    
    @Benchmark
    public void complexSearch10x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 10, 10, () -> searchService.search(config.searchTerm, config.authorFilter, config.languageFilter, config.yearFilter));
    }

    @Benchmark
    public void complexSearch100x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 100, 100, () -> searchService.search(config.searchTerm, config.authorFilter, config.languageFilter, config.yearFilter));
    }

    @Benchmark
    public void complexSearch1000x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 1000, 1000, () -> searchService.search(config.searchTerm, config.authorFilter, config.languageFilter, config.yearFilter));
    }

    // ========== SCALABILITY BENCHMARKS - MULTI-WORD QUERIES ==========
    // Tests end-to-end multi-word query performance at different scales
    
    @Benchmark
    public void longQuery10x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 10, 10, () -> searchService.search(config.multiWordSearch, null, null, null));
    }

    @Benchmark
    public void longQuery100x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 100, 100, () -> searchService.search(config.multiWordSearch, null, null, null));
    }

    @Benchmark
    public void longQuery1000x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 1000, 1000, () -> searchService.search(config.multiWordSearch, null, null, null));
    }

    // Helper method to avoid code duplication
    private void runBenchmarkIfSufficientBooks(Blackhole bh, int requiredBooks, int iterations, SearchOperation operation) {
        if (bookCount >= requiredBooks) {
            for (int i = 0; i < iterations; i++) {
                List<Book> results = operation.execute();
                bh.consume(results);
            }
        } else {
            bh.consume("Skipped - need " + requiredBooks + "+ books, have " + bookCount);
        }
    }

    @FunctionalInterface
    private interface SearchOperation {
        List<Book> execute();
    }
}