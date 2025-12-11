#!/bin/bash

# Search Service Benchmarking Script
# Runs JMH microbenchmarks with proper JVM settings

set -e

echo "🔬 Search Service Microbenchmarking"
echo "=================================="

# Check if Maven is available
if ! command -v mvn &> /dev/null; then
    echo "❌ Maven is not installed"
    exit 1
fi

# Build benchmark JAR
echo "📦 Building benchmark JAR..."
mvn clean compile test-compile -Pbenchmark package -q

# Check if benchmark JAR was created
BENCHMARK_JAR="target/search-benchmarks.jar"
if [ ! -f "$BENCHMARK_JAR" ]; then
    echo "❌ Benchmark JAR not found: $BENCHMARK_JAR"
    exit 1
fi

# JVM settings for consistent benchmarking
JVM_OPTS="-Xmx4G -Xms4G -XX:+UseG1GC"

# Create results directory
mkdir -p benchmark_results
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

echo "⚡ Running microbenchmarks..."
echo "   JVM Options: $JVM_OPTS"
echo "   Results will be saved to: benchmark_results/"

# Run all benchmarks with CSV output
java $JVM_OPTS -jar "$BENCHMARK_JAR" \
    -rf csv \
    -rff "benchmark_results/results_${TIMESTAMP}.csv" \
    -wi 5 \
    -i 10 \
    -f 1 \
    -t 1

echo ""
echo "✅ Benchmarking complete!"
echo "📊 Results saved to: benchmark_results/results_${TIMESTAMP}.csv"

# Show quick summary
if [ -f "benchmark_results/results_${TIMESTAMP}.csv" ]; then
    echo ""
    echo "📋 Quick Summary (Average Time in μs/ms):"
    echo "----------------------------------------"
    tail -n +2 "benchmark_results/results_${TIMESTAMP}.csv" | \
        awk -F, '{printf "%-40s %10s %s\n", $1, $5, $6}' | \
        head -10
fi

echo ""
echo "💡 Tips:"
echo "   • Run on idle system for accurate results"
echo "   • Multiple runs recommended for statistical validity"
echo "   • Check benchmark_results/ for detailed CSV data"