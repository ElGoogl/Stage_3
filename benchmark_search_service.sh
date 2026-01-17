#!/bin/bash

# Stage 3 Comprehensive Benchmarking Suite
# Tests: Scalability, Latency, Fault Tolerance
# Outputs: CSV results and visualization data

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Configuration
RESULTS_DIR="benchmark_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_FILE="$RESULTS_DIR/benchmark_${TIMESTAMP}.csv"

# Create results directory
mkdir -p "$RESULTS_DIR"

echo "==================================================================="
echo "          Stage 3 Search Service Benchmark Suite"
echo "==================================================================="
echo ""
echo "Timestamp: $TIMESTAMP"
echo "Results will be saved to: $RESULT_FILE"
echo ""

# Initialize CSV file
echo "test_type,num_nodes,concurrent_requests,total_requests,avg_latency_ms,p50_latency_ms,p95_latency_ms,p99_latency_ms,max_latency_ms,requests_per_sec,total_time_sec,cpu_percent,memory_mb,failed_requests" > "$RESULT_FILE"

#==============================================================================
# Function: Run load test with specified parameters
#==============================================================================
run_load_test() {
    local test_name=$1
    local num_nodes=$2
    local concurrent=$3
    local total_requests=$4
    local search_query=$5
    
    echo ""
    echo -e "${YELLOW}Running: $test_name${NC}"
    echo "  Nodes: $num_nodes | Concurrent: $concurrent | Total: $total_requests"
    
    # Run Apache Bench test
    local ab_output=$(ab -n $total_requests -c $concurrent -g /tmp/ab_plot.tsv \
        "http://localhost:8000/search?q==${search_query}&limit=10" 2>&1)
    
    # Extract metrics
    local avg_latency=$(echo "$ab_output" | grep "Time per request" | head -1 | awk '{print $4}')
    local requests_per_sec=$(echo "$ab_output" | grep "Requests per second" | awk '{print $4}')
    local total_time=$(echo "$ab_output" | grep "Time taken for tests" | awk '{print $5}')
    local failed=$(echo "$ab_output" | grep "Failed requests" | awk '{print $3}')
    
    # Calculate percentiles from plot data (tab-separated, header in first row)
    local latencies=$(awk -F'\t' 'NR>1 {print $5}' /tmp/ab_plot.tsv | sort -n)
    local p50=$(echo "$latencies" | awk '{a[NR]=$1} END {if (NR==0) {print ""} else {print a[int((NR-1)*0.50)+1]}}')
    local p95=$(echo "$latencies" | awk '{a[NR]=$1} END {if (NR==0) {print ""} else {print a[int((NR-1)*0.95)+1]}}')
    local p99=$(echo "$latencies" | awk '{a[NR]=$1} END {if (NR==0) {print ""} else {print a[int((NR-1)*0.99)+1]}}')
    local max=$(echo "$latencies" | tail -1)
    
    # Get resource utilization
    local cpu_usage=$(docker stats --no-stream --format "{{.CPUPerc}}" search1 search2 search3 2>/dev/null | \
        sed 's/%//' | awk '{sum+=$1} END {print sum/NR}')
    local mem_usage=$(docker stats --no-stream --format "{{.MemUsage}}" search1 search2 search3 2>/dev/null | \
        awk -F'/' '{print $1}' | sed 's/MiB//' | awk '{sum+=$1} END {print sum}')
    
    # Write to CSV
    echo "$test_name,$num_nodes,$concurrent,$total_requests,$avg_latency,$p50,$p95,$p99,$max,$requests_per_sec,$total_time,$cpu_usage,$mem_usage,$failed" >> "$RESULT_FILE"
    
    # Display summary
    echo -e "  ${GREEN}✓${NC} Avg Latency: ${avg_latency}ms | Throughput: ${requests_per_sec} req/s"
    echo "  P50: ${p50}ms | P95: ${p95}ms | P99: ${p99}ms | Max: ${max}ms"
    echo "  CPU: ${cpu_usage}% | Memory: ${mem_usage}MB | Failed: $failed"
}

#==============================================================================
# 1. BASELINE TEST (Minimal Configuration)
#==============================================================================
echo ""
echo "==================================================================="
echo "  TEST 1: Baseline Performance (3 nodes)"
echo "==================================================================="

run_load_test "baseline_light" 3 5 100 "distributed"
sleep 2
run_load_test "baseline_medium" 3 10 500 "distributed"
sleep 2
run_load_test "baseline_heavy" 3 20 1000 "distributed"

#==============================================================================
# 2. SCALABILITY TEST (Varying Concurrent Load)
#==============================================================================
echo ""
echo "==================================================================="
echo "  TEST 2: Scalability Under Concurrent Load"
echo "==================================================================="

for concurrent in 5 10 20 50 100; do
    run_load_test "scalability_c${concurrent}" 3 $concurrent 1000 "systems"
    sleep 2
done

#==============================================================================
# 3. LATENCY TEST (Different Query Complexities)
#==============================================================================
echo ""
echo "==================================================================="
echo "  TEST 3: Query Latency Analysis"
echo "==================================================================="

run_load_test "latency_single_word" 3 10 500 "distributed"
sleep 2
run_load_test "latency_two_words" 3 10 500 "distributed+systems"
sleep 2
run_load_test "latency_three_words" 3 10 500 "distributed+systems+architecture"

#==============================================================================
# 4. SUSTAINED LOAD TEST
#==============================================================================
echo ""
echo "==================================================================="
echo "  TEST 4: Sustained Load (5000 requests)"
echo "==================================================================="

run_load_test "sustained_load" 3 20 5000 "search"

#==============================================================================
# 5. FAULT TOLERANCE TEST
#==============================================================================
echo ""
echo "==================================================================="
echo "  TEST 5: Fault Tolerance (Node Failure)"
echo "==================================================================="

echo "  Step 1: Baseline with all nodes healthy"
run_load_test "fault_tolerance_before" 3 10 500 "distributed"
sleep 2

echo "  Step 2: Stopping search1..."
docker stop search1 > /dev/null 2>&1
sleep 3

echo "  Step 3: Testing with one node down"
run_load_test "fault_tolerance_during" 2 10 500 "distributed"
sleep 2

echo "  Step 4: Restarting search1..."
docker start search1 > /dev/null 2>&1
echo "  Waiting for service to recover..."
sleep 5

echo "  Step 5: Testing after recovery"
run_load_test "fault_tolerance_after" 3 10 500 "distributed"

#==============================================================================
# GENERATE SUMMARY REPORT
#==============================================================================
echo ""
echo "==================================================================="
echo "  Benchmark Summary"
echo "==================================================================="
echo ""

# Calculate average metrics
echo "Average Latency by Test Type:"
awk -F',' 'NR>1 {sum[$1]+=$5; count[$1]++} END {for(test in sum) printf "  %-30s: %.2f ms\n", test, sum[test]/count[test]}' "$RESULT_FILE" | sort

echo ""
echo "Throughput by Test Type:"
awk -F',' 'NR>1 {sum[$1]+=$10; count[$1]++} END {for(test in sum) printf "  %-30s: %.2f req/s\n", test, sum[test]/count[test]}' "$RESULT_FILE" | sort

echo ""
echo "Resource Utilization:"
awk -F',' 'NR>1 {cpu+=$12; mem+=$13; count++} END {printf "  Average CPU: %.2f%%\n  Average Memory: %.2f MB\n", cpu/count, mem/count}' "$RESULT_FILE"

echo ""
echo "==================================================================="
echo -e "  ${GREEN}Benchmark Complete!${NC}"
echo "==================================================================="
echo ""
echo "Results saved to: $RESULT_FILE"
echo ""
echo "To analyze results:"
echo "  cat $RESULT_FILE"
echo "  python3 visualize_benchmarks.py $RESULT_FILE"
echo ""
