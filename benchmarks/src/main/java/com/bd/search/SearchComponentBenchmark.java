package com.bd.search;

import com.bd.search.SearchService;
import com.bd.search.RankingService;
import com.bd.search.Book;
import com.bd.search.RankedBook;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Search Component Scalability Benchmarks
 * 
 * PURPOSE: Measures performance of isolated search service components across different data scales
 * SCOPE: Pure filtering and ranking operations with dynamic scaling (10x, 100x, 1000x iterations)  
 * FOCUS: Component-level scalability analysis and bottleneck identification
 * METRICS: Microseconds per operation (high-precision measurement)
 * 
 * Contains scalable component benchmarks for:
 * - Filtering components: Author, Language, Year, Combined filtering (each at 10x, 100x, 1000x)
 * - Ranking components: Single-term, Multi-term, Complex TF-IDF ranking (each at 10x, 100x, 1000x)
 * 
 * Use this for:
 * - Identifying performance bottlenecks in specific components at scale
 * - Comparing efficiency of different filtering strategies across data sizes
 * - Analyzing TF-IDF ranking performance across query complexities and scales
 * - Fine-tuning individual operations for scalability before system integration
 * 
 * For end-to-end search pipeline benchmarks, use SearchPipelineBenchmark instead.
 * This class focuses exclusively on isolated component scalability measurement.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
public class SearchComponentBenchmark {

    private SearchService searchService;
    private SearchBenchmarkUtils.BenchmarkConfig config;
    private int bookCount;
    
    @Setup(Level.Trial)
    public void setup() {
        System.out.println("Setting up component benchmarks...");
        
        searchService = new SearchService();
        bookCount = getBookCount();
        config = SearchBenchmarkUtils.setupBenchmarkConfiguration(searchService);
        
        printConfiguration();
    }
    
    private int getBookCount() {
        try {
            String content = java.nio.file.Files.readString(java.nio.file.Paths.get("../data_repository/indexed_books.json"));
            return (int) content.chars().filter(ch -> ch == '{').count() - 1; // Subtract 1 for outer object
        } catch (Exception e) {
            System.err.println("Warning: Could not read indexed_books.json: " + e.getMessage());
        }
        return 0;
    }

    private void printConfiguration() {
        SearchBenchmarkUtils.printConfiguration(config, bookCount);
        
        String message = bookCount >= 1000 ? "All component benchmarks (10x, 100x, 1000x) will run" :
                        bookCount >= 100 ? "10x and 100x component benchmarks will run" :
                        bookCount >= 10 ? "Only 10x component benchmarks will run" :
                        "All scaled component benchmarks will skip - need more books";
        System.out.println("✅ " + message);
    }
    
    // ========== SCALABILITY BENCHMARKS - AUTHOR FILTERING ==========
    // Tests author filtering performance at different scales
    
    @Benchmark
    public void filterByAuthor10x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 10, 10, () -> searchService.search(config.searchTerm, config.authorFilter, null, null));
    }

    @Benchmark
    public void filterByAuthor100x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 100, 100, () -> searchService.search(config.searchTerm, config.authorFilter, null, null));
    }

    @Benchmark
    public void filterByAuthor1000x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 1000, 1000, () -> searchService.search(config.searchTerm, config.authorFilter, null, null));
    }

    // ========== SCALABILITY BENCHMARKS - LANGUAGE FILTERING ==========
    // Tests language filtering performance at different scales
    
    @Benchmark
    public void filterByLanguage10x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 10, 10, () -> searchService.search(config.searchTerm, null, config.languageFilter, null));
    }

    @Benchmark
    public void filterByLanguage100x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 100, 100, () -> searchService.search(config.searchTerm, null, config.languageFilter, null));
    }

    @Benchmark
    public void filterByLanguage1000x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 1000, 1000, () -> searchService.search(config.searchTerm, null, config.languageFilter, null));
    }

    // ========== SCALABILITY BENCHMARKS - YEAR FILTERING ==========
    // Tests year filtering performance at different scales
    
    @Benchmark
    public void filterByYear10x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 10, 10, () -> searchService.search(config.searchTerm, null, null, config.yearFilter));
    }

    @Benchmark
    public void filterByYear100x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 100, 100, () -> searchService.search(config.searchTerm, null, null, config.yearFilter));
    }

    @Benchmark
    public void filterByYear1000x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 1000, 1000, () -> searchService.search(config.searchTerm, null, null, config.yearFilter));
    }

    // ========== SCALABILITY BENCHMARKS - COMBINED FILTERING ==========
    // Tests combined filtering (all filters) performance at different scales
    
    @Benchmark
    public void filterCombined10x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 10, 10, () -> searchService.search(config.searchTerm, config.authorFilter, config.languageFilter, config.yearFilter));
    }

    @Benchmark
    public void filterCombined100x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 100, 100, () -> searchService.search(config.searchTerm, config.authorFilter, config.languageFilter, config.yearFilter));
    }

    @Benchmark
    public void filterCombined1000x(Blackhole bh) {
        runBenchmarkIfSufficientBooks(bh, 1000, 1000, () -> searchService.search(config.searchTerm, config.authorFilter, config.languageFilter, config.yearFilter));
    }
    
    // ========== SCALABILITY BENCHMARKS - SINGLE TERM RANKING ==========
    // Tests single term ranking performance at different scales
    
    @Benchmark
    public void rankSingleTerm10x(Blackhole bh) {
        runRankingBenchmarkIfSufficientBooks(bh, 10, 10, () -> RankingService.rankBooks(config.testBooks, config.searchTerm, config.bookIds));
    }

    @Benchmark
    public void rankSingleTerm100x(Blackhole bh) {
        runRankingBenchmarkIfSufficientBooks(bh, 100, 100, () -> RankingService.rankBooks(config.testBooks, config.searchTerm, config.bookIds));
    }

    @Benchmark
    public void rankSingleTerm1000x(Blackhole bh) {
        runRankingBenchmarkIfSufficientBooks(bh, 1000, 1000, () -> RankingService.rankBooks(config.testBooks, config.searchTerm, config.bookIds));
    }

    // ========== SCALABILITY BENCHMARKS - MULTI-TERM RANKING ==========
    // Tests multi-term ranking performance at different scales
    
    @Benchmark
    public void rankMultiTerm10x(Blackhole bh) {
        runRankingBenchmarkIfSufficientBooks(bh, 10, 10, () -> RankingService.rankBooks(config.testBooks, "the and of", config.bookIds));
    }

    @Benchmark
    public void rankMultiTerm100x(Blackhole bh) {
        runRankingBenchmarkIfSufficientBooks(bh, 100, 100, () -> RankingService.rankBooks(config.testBooks, "the and of", config.bookIds));
    }

    @Benchmark
    public void rankMultiTerm1000x(Blackhole bh) {
        runRankingBenchmarkIfSufficientBooks(bh, 1000, 1000, () -> RankingService.rankBooks(config.testBooks, "the and of", config.bookIds));
    }

    // ========== SCALABILITY BENCHMARKS - COMPLEX QUERY RANKING ==========
    // Tests complex query ranking performance at different scales
    
    @Benchmark
    public void rankComplexQuery10x(Blackhole bh) {
        runRankingBenchmarkIfSufficientBooks(bh, 10, 10, () -> RankingService.rankBooks(config.testBooks, config.multiWordSearch, config.bookIds));
    }

    @Benchmark
    public void rankComplexQuery100x(Blackhole bh) {
        runRankingBenchmarkIfSufficientBooks(bh, 100, 100, () -> RankingService.rankBooks(config.testBooks, config.multiWordSearch, config.bookIds));
    }

    @Benchmark
    public void rankComplexQuery1000x(Blackhole bh) {
        runRankingBenchmarkIfSufficientBooks(bh, 1000, 1000, () -> RankingService.rankBooks(config.testBooks, config.multiWordSearch, config.bookIds));
    }

    // ========== UTILITY METHODS ==========
    
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

    private void runRankingBenchmarkIfSufficientBooks(Blackhole bh, int requiredBooks, int iterations, RankingOperation operation) {
        if (bookCount >= requiredBooks) {
            for (int i = 0; i < iterations; i++) {
                List<RankedBook> results = operation.execute();
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

    @FunctionalInterface
    private interface RankingOperation {
        List<RankedBook> execute();
    }
}