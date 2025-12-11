# 🔬 Search Service Benchmarking

Comprehensive performance evaluation of the search service using Java Microbenchmark Harness (JMH).

## 📊 Overview

This benchmarking suite measures:
- **Ranking algorithm performance** under different data sizes
- **Search operation efficiency** with various query complexities  
- **Data processing performance** for JSON parsing and lookup operations

## 🏗️ Architecture

```
search_service/
├── src/test/java/com/example/benchmark/
│   ├── RankingBenchmark.java          # Ranking algorithm performance
│   ├── SearchBenchmark.java           # End-to-end search performance
│   └── DataProcessingBenchmark.java   # JSON/data structure performance
├── run_benchmarks.sh                  # Execution script
└── benchmark_results/                 # Generated results (CSV format)
```

## 🎯 Benchmark Categories

### 1. **Ranking Benchmarks** (`RankingBenchmark.java`)
- **Dataset scaling**: 100, 1K, 10K books
- **Query complexity**: Single term, multi-term, complex queries
- **Measures**: TF-IDF calculation, title matching, sorting performance

### 2. **Search Benchmarks** (`SearchBenchmark.java`) 
- **Search operations**: Basic, filtered, complex searches
- **Query types**: Short, medium, long queries
- **Measures**: End-to-end search time including database access

### 3. **Data Processing Benchmarks** (`DataProcessingBenchmark.java`)
- **JSON parsing**: Small, medium, large JSON datasets
- **Lookup operations**: Term search in inverted index
- **Set operations**: Union, intersection (used in search filtering)

## 🚀 Running Benchmarks

### Quick Start
```bash
# Run all benchmarks with default settings
./run_benchmarks.sh
```

### Manual Execution
```bash
# Build benchmark JAR
mvn clean compile test-compile -Pbenchmark package

# Run specific benchmark class
java -Xmx4G -jar target/search-benchmarks.jar RankingBenchmark

# Run with custom settings
java -Xmx4G -jar target/search-benchmarks.jar \
    -wi 3 -i 5 -f 1 -t 1 \
    -rf csv -rff results.csv
```

## ⚙️ JMH Configuration

### Standard Settings
- **Warm-up**: 5 iterations, 1 second each
- **Measurement**: 10 iterations, 1 second each  
- **JVM**: Single fork, single thread
- **Heap**: Fixed 4GB (`-Xmx4G -Xms4G`)
- **GC**: G1 garbage collector

### Best Practices Applied
✅ **Statistical validity**: Multiple iterations with warm-up  
✅ **Reproducible**: Fixed random seeds, consistent environment  
✅ **Isolated**: Dedicated benchmark profile, separate from production  
✅ **Comprehensive**: Multiple data sizes and query complexities  

## 📈 Expected Metrics

### Response Time Targets
- **Ranking (1K books)**: < 1ms average
- **Basic search**: < 50ms average  
- **Complex search**: < 200ms average
- **JSON parsing**: < 100μs for small datasets

### Scalability Expectations
- **Linear scaling** for ranking with dataset size
- **Logarithmic scaling** for search operations  
- **Constant time** for JSON lookup operations

## 📊 Results Analysis

### CSV Output Format
```csv
Benchmark,Mode,Threads,Samples,Score,Error,Units
RankingBenchmark.rankSmallDataset,avgt,1,10,245.123,±15.234,us/op
SearchBenchmark.basicSearch,avgt,1,10,45.678,±5.123,ms/op
```

### Key Performance Indicators
1. **Average response time** per operation type
2. **95th percentile** response times  
3. **Scaling factors** across dataset sizes
4. **Memory efficiency** (implicit in GC behavior)

## 🔧 Customization

### Adding New Benchmarks
```java
@Benchmark
public void customBenchmark(Blackhole bh) {
    // Your benchmark code
    Object result = performOperation();
    bh.consume(result); // Prevent dead code elimination
}
```

### Modifying Test Data
- Edit `createTestBooks()` in `RankingBenchmark.java`
- Adjust dataset sizes in `@Setup` methods
- Modify query complexity in benchmark methods

## 🎯 Integration with CI/CD

### Performance Regression Detection
```bash
# Compare results with baseline
java -jar target/search-benchmarks.jar \
    -rf json -rff current_results.json

# Performance comparison script (optional)
python compare_benchmarks.py baseline.json current_results.json
```

## 💡 Troubleshooting

### Common Issues
- **Out of Memory**: Increase JVM heap size (`-Xmx8G`)
- **Inconsistent results**: Run on idle system, disable turbo boost
- **Compilation errors**: Ensure JMH dependencies are in test scope

### Validation
```bash
# Quick smoke test (faster, less accurate)
java -jar target/search-benchmarks.jar -wi 1 -i 3 -f 1
```

---

**Note**: Benchmarks are designed for **microbenchmarking** (component-level performance). For system-level benchmarking, use external load testing tools against the running service.