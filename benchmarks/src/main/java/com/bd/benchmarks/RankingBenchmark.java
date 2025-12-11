package com.bd.benchmarks;

import com.bd.search.RankingService;
import com.bd.search.Book;
import com.bd.search.RankedBook;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Microbenchmarks for ranking operations
 * Measures computational efficiency of ranking algorithms
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
public class RankingBenchmark {

    private List<Book> smallDataset;
    private List<Book> mediumDataset;
    private List<Book> largeDataset;
    private Set<Integer> bookIds;
    
    @Setup(Level.Trial)
    public void setup() {
        // Create test datasets of different sizes
        smallDataset = createTestBooks(100);
        mediumDataset = createTestBooks(1000);
        largeDataset = createTestBooks(10000);
        
        bookIds = new HashSet<>();
        for (int i = 1; i <= 10000; i++) {
            bookIds.add(i);
        }
    }
    
    /**
     * Benchmark ranking with small dataset (100 books)
     */
    @Benchmark
    public void rankSmallDataset(Blackhole bh) {
        List<RankedBook> results = RankingService.rankBooks(smallDataset, "adventure story", bookIds);
        bh.consume(results);
    }
    
    /**
     * Benchmark ranking with medium dataset (1000 books)
     */
    @Benchmark
    public void rankMediumDataset(Blackhole bh) {
        List<RankedBook> results = RankingService.rankBooks(mediumDataset, "adventure story", bookIds);
        bh.consume(results);
    }
    
    /**
     * Benchmark ranking with large dataset (10000 books)
     */
    @Benchmark
    public void rankLargeDataset(Blackhole bh) {
        List<RankedBook> results = RankingService.rankBooks(largeDataset, "adventure story", bookIds);
        bh.consume(results);
    }
    
    /**
     * Benchmark different query complexities
     */
    @Benchmark
    public void singleTermQuery(Blackhole bh) {
        List<RankedBook> results = RankingService.rankBooks(mediumDataset, "love", bookIds);
        bh.consume(results);
    }
    
    @Benchmark
    public void multiTermQuery(Blackhole bh) {
        List<RankedBook> results = RankingService.rankBooks(mediumDataset, "love adventure mystery", bookIds);
        bh.consume(results);
    }
    
    @Benchmark
    public void complexQuery(Blackhole bh) {
        List<RankedBook> results = RankingService.rankBooks(mediumDataset, "young love adventure mystery romance", bookIds);
        bh.consume(results);
    }
    
    /**
     * Create test books for benchmarking
     */
    private List<Book> createTestBooks(int count) {
        List<Book> books = new ArrayList<>();
        Random random = new Random(42); // Fixed seed for reproducibility
        
        String[] titles = {
            "The Great Adventure", "Love Story", "Mystery Novel", "Historical Fiction",
            "Science Fantasy", "Romance Drama", "Adventure Quest", "Detective Story",
            "Epic Fantasy", "Modern Romance", "Thriller Novel", "Classic Literature"
        };
        
        String[] authors = {
            "Jane Austen", "Charles Dickens", "William Shakespeare", "Mark Twain",
            "J.K. Rowling", "Stephen King", "Agatha Christie", "Ernest Hemingway"
        };
        
        String[] languages = {"en", "fr", "es", "de"};
        
        for (int i = 1; i <= count; i++) {
            String title = titles[random.nextInt(titles.length)] + " " + i;
            String author = authors[random.nextInt(authors.length)];
            String language = languages[random.nextInt(languages.length)];
            int year = 1800 + random.nextInt(225); // Years 1800-2024
            
            books.add(new Book(i, title, author, language, year));
        }
        
        return books;
    }
}