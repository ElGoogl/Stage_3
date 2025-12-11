package com.bd.benchmarks;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.StringReader;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Microbenchmarks for data processing operations
 * Measures performance of JSON parsing and data structures
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
public class DataProcessingBenchmark {

    private String smallJsonData;
    private String mediumJsonData;
    private String largeJsonData;
    private List<String> searchTerms;
    
    @Setup(Level.Trial)
    public void setup() {
        // Create test JSON data
        smallJsonData = createTestJson(100);
        mediumJsonData = createTestJson(1000);
        largeJsonData = createTestJson(10000);
        
        searchTerms = Arrays.asList("love", "adventure", "mystery", "romance", "fantasy");
    }
    
    /**
     * Benchmark JSON parsing operations
     */
    @Benchmark
    public void parseSmallJson(Blackhole bh) {
        JsonObject obj = JsonParser.parseString(smallJsonData).getAsJsonObject();
        bh.consume(obj);
    }
    
    @Benchmark
    public void parseMediumJson(Blackhole bh) {
        JsonObject obj = JsonParser.parseString(mediumJsonData).getAsJsonObject();
        bh.consume(obj);
    }
    
    @Benchmark
    public void parseLargeJson(Blackhole bh) {
        JsonObject obj = JsonParser.parseString(largeJsonData).getAsJsonObject();
        bh.consume(obj);
    }
    
    /**
     * Benchmark search term lookup in JSON
     */
    @Benchmark
    public void jsonLookup(Blackhole bh) {
        JsonObject obj = JsonParser.parseString(mediumJsonData).getAsJsonObject();
        Set<Integer> bookIds = new HashSet<>();
        
        for (String term : searchTerms) {
            if (obj.has(term)) {
                JsonArray array = obj.getAsJsonArray(term);
                for (int i = 0; i < array.size(); i++) {
                    bookIds.add(array.get(i).getAsInt());
                }
            }
        }
        
        bh.consume(bookIds);
    }
    
    /**
     * Benchmark Set operations (used in search)
     */
    @Benchmark
    public void setOperations(Blackhole bh) {
        Set<Integer> set1 = new HashSet<>();
        Set<Integer> set2 = new HashSet<>();
        
        // Populate sets
        for (int i = 0; i < 1000; i++) {
            set1.add(i);
            set2.add(i + 500);
        }
        
        // Union operation
        Set<Integer> union = new HashSet<>(set1);
        union.addAll(set2);
        
        // Intersection operation
        Set<Integer> intersection = new HashSet<>(set1);
        intersection.retainAll(set2);
        
        bh.consume(union);
        bh.consume(intersection);
    }
    
    /**
     * Create test JSON data for benchmarking
     */
    private String createTestJson(int entryCount) {
        JsonObject jsonObj = new JsonObject();
        Random random = new Random(42);
        
        String[] terms = {"love", "adventure", "mystery", "romance", "fantasy", "war", "peace", "story"};
        
        for (String term : terms) {
            JsonArray bookIds = new JsonArray();
            int numBooks = random.nextInt(entryCount / 4) + 1;
            
            for (int i = 0; i < numBooks; i++) {
                bookIds.add(random.nextInt(entryCount) + 1);
            }
            
            jsonObj.add(term, bookIds);
        }
        
        return jsonObj.toString();
    }
}