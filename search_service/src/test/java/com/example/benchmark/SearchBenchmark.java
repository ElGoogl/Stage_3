package com.example.benchmark;

import com.bd.search.SearchService;
import com.bd.search.Book;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Microbenchmarks for search operations
 * Measures performance of core search functionality
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
public class SearchBenchmark {

    private SearchService searchService;
    
    @Setup(Level.Trial)
    public void setup() {
        searchService = new SearchService();
    }
    
    /**
     * Benchmark basic search queries
     */
    @Benchmark
    public void basicSearch(Blackhole bh) {
        List<Book> results = searchService.search("adventure", null, null, null);
        bh.consume(results);
    }
    
    @Benchmark
    public void filteredSearch(Blackhole bh) {
        List<Book> results = searchService.search("love", "Austen", "en", null);
        bh.consume(results);
    }
    
    @Benchmark
    public void complexSearch(Blackhole bh) {
        List<Book> results = searchService.search("adventure mystery", "Dickens", "en", 1850);
        bh.consume(results);
    }
    
    /**
     * Benchmark different query types
     */
    @Benchmark
    public void shortQuery(Blackhole bh) {
        List<Book> results = searchService.search("war", null, null, null);
        bh.consume(results);
    }
    
    @Benchmark
    public void mediumQuery(Blackhole bh) {
        List<Book> results = searchService.search("love story", null, null, null);
        bh.consume(results);
    }
    
    @Benchmark
    public void longQuery(Blackhole bh) {
        List<Book> results = searchService.search("adventure mystery romance fantasy", null, null, null);
        bh.consume(results);
    }
}