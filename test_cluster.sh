#!/bin/bash

# Test script for Stage 3 distributed search engine cluster
# This script tests basic functionality of the cluster

set -e

echo "=== Stage 3 Cluster Test Script ==="
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test function
test_endpoint() {
    local name=$1
    local url=$2
    local expected_code=${3:-200}
    
    echo -n "Testing $name... "
    response_code=$(curl -s -o /dev/null -w "%{http_code}" "$url")
    
    if [ "$response_code" -eq "$expected_code" ]; then
        echo -e "${GREEN}✓ OK${NC} (HTTP $response_code)"
        return 0
    else
        echo -e "${RED}✗ FAILED${NC} (HTTP $response_code, expected $expected_code)"
        return 1
    fi
}

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 5

echo ""
echo "=== 1. Health Checks ==="

# Test Nginx load balancer
test_endpoint "Nginx Load Balancer" "http://localhost/status"

# Test ActiveMQ web console
test_endpoint "ActiveMQ Web Console" "http://localhost:8161"

echo ""
echo "=== 2. Search Service Tests ==="

# Test search endpoint (may fail if index is empty)
echo -n "Testing search endpoint... "
response=$(curl -s "http://localhost/search?q=test")
if echo "$response" | grep -q '"query"'; then
    echo -e "${GREEN}✓ OK${NC} (returns JSON response)"
    echo "  Response: $response" | head -c 100
    echo "..."
else
    echo -e "${YELLOW}⚠ WARNING${NC} (unexpected response format)"
    echo "  Response: $response"
fi

# Test missing query parameter (should return 400)
echo -n "Testing missing query parameter... "
response_code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost/search")
if [ "$response_code" -eq "400" ]; then
    echo -e "${GREEN}✓ OK${NC} (HTTP 400 as expected)"
else
    echo -e "${RED}✗ FAILED${NC} (HTTP $response_code, expected 400)"
fi

echo ""
echo "=== 3. Load Balancing Test ==="

echo "Sending 10 requests to verify load distribution..."
for i in {1..10}; do
    curl -s "http://localhost/search?q=test&limit=1" > /dev/null
    echo -n "."
done
echo -e " ${GREEN}✓ Completed${NC}"

echo ""
echo "=== 4. Container Status ==="

# Check if all containers are running
containers=("hazelcast1" "hazelcast2" "hazelcast3" "search1" "search2" "search3" "nginx_lb" "activemq")

for container in "${containers[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        echo -e "  $container: ${GREEN}✓ Running${NC}"
    else
        echo -e "  $container: ${RED}✗ Not running${NC}"
    fi
done

echo ""
echo "=== 5. Service Logs (last 3 lines) ==="

echo ""
echo "Search1 logs:"
docker logs search1 2>&1 | tail -n 3

echo ""
echo "Hazelcast1 logs:"
docker logs hazelcast1 2>&1 | tail -n 3

echo ""
echo "=== Test Summary ==="
echo "All basic tests completed!"
echo ""
echo "To view logs:"
echo "  docker logs search1"
echo "  docker logs hazelcast1"
echo "  docker logs nginx_lb"
echo ""
echo "To stop the cluster:"
echo "  docker-compose down"
