#!/usr/bin/env python3
"""
System-Level Benchmark - Integration Testing
Measures end-to-end performance with concurrent requests
"""

import time
import statistics
import threading
import urllib.request
import urllib.parse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "http://localhost:7002"

class SystemBenchmark:
    def __init__(self):
        self.results = []
        self.errors = 0
        
    def single_request(self, query, filters=None):
        """Execute single search request and measure time"""
        params = {"q": query}
        if filters:
            params.update(filters)
            
        url = f"{BASE_URL}/search?" + urllib.parse.urlencode(params)
        start_time = time.time()
        
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                data = json.loads(response.read().decode('utf-8'))
                response_time = (time.time() - start_time) * 1000  # Convert to ms
                
            return {
                'response_time_ms': response_time,
                'result_count': data.get('count', 0),
                'success': True
            }
        except Exception as e:
            self.errors += 1
            return {
                'response_time_ms': (time.time() - start_time) * 1000,
                'error': str(e),
                'success': False
            }
    
    def concurrent_benchmark(self, queries, concurrent_users=10, requests_per_user=50):
        """Run concurrent requests to measure system performance"""
        print(f"🚀 System Benchmark: {concurrent_users} users × {requests_per_user} requests")
        print("-" * 50)
        
        all_results = []
        
        def user_simulation(user_id):
            """Simulate single user making multiple requests"""
            user_results = []
            for i in range(requests_per_user):
                query = queries[i % len(queries)]
                result = self.single_request(query)
                user_results.append(result)
                time.sleep(0.1)  # Small delay between requests
            return user_results
        
        start_time = time.time()
        
        # Execute concurrent users
        with ThreadPoolExecutor(max_workers=concurrent_users) as executor:
            futures = [executor.submit(user_simulation, i) for i in range(concurrent_users)]
            
            for future in as_completed(futures):
                user_results = future.result()
                all_results.extend(user_results)
        
        total_time = time.time() - start_time
        
        # Analyze results
        successful_requests = [r for r in all_results if r['success']]
        response_times = [r['response_time_ms'] for r in successful_requests]
        
        if response_times:
            print(f"✅ Results Summary:")
            print(f"   Total Requests: {len(all_results)}")
            print(f"   Successful: {len(successful_requests)}")
            print(f"   Errors: {self.errors}")
            print(f"   Success Rate: {len(successful_requests)/len(all_results)*100:.1f}%")
            print(f"   Total Time: {total_time:.2f}s")
            print(f"   Throughput: {len(all_results)/total_time:.1f} req/s")
            print()
            print(f"📊 Response Times (ms):")
            print(f"   Average: {statistics.mean(response_times):.2f}")
            print(f"   Median: {statistics.median(response_times):.2f}")
            print(f"   95th percentile: {sorted(response_times)[int(len(response_times)*0.95)]:.2f}")
            print(f"   Min: {min(response_times):.2f}")
            print(f"   Max: {max(response_times):.2f}")
        else:
            print("❌ No successful requests")
            
        return {
            'total_requests': len(all_results),
            'successful_requests': len(successful_requests),
            'errors': self.errors,
            'total_time': total_time,
            'throughput': len(all_results) / total_time,
            'response_times': response_times
        }
    
    def load_test(self):
        """Progressive load testing"""
        test_queries = [
            "love", "adventure", "mystery", "romance", "war", 
            "shakespeare", "jane austen", "adventure story",
            "love romance", "mystery thriller"
        ]
        
        print("🔬 SYSTEM-LEVEL PERFORMANCE TESTING")
        print("=" * 50)
        
        # Check service availability
        try:
            result = self.single_request("test")
            if not result['success']:
                print("❌ Service not available. Start the search service first:")
                print("   java -jar target/search-service-1.0.0.jar")
                return
        except:
            print("❌ Service not available at http://localhost:7002")
            return
        
        print("✅ Service is available")
        
        # Progressive load tests
        test_scenarios = [
            (1, 20),   # 1 user, 20 requests
            (5, 20),   # 5 users, 20 requests  
            (10, 10),  # 10 users, 10 requests
            (20, 5),   # 20 users, 5 requests
        ]
        
        for users, requests in test_scenarios:
            print(f"\n🎯 Test Scenario: {users} concurrent users")
            self.errors = 0  # Reset error counter
            self.concurrent_benchmark(test_queries, users, requests)
            time.sleep(2)  # Cool down between tests

def main():
    benchmark = SystemBenchmark()
    
    print("⚠️  System-Level Benchmark")
    print("   This requires the search service to be running!")
    print("   Start it with: java -jar target/search-service-1.0.0.jar")
    print()
    
    input("Press Enter when service is ready...")
    
    benchmark.load_test()
    
    print("\n🎉 System benchmarking complete!")
    print("\n💡 For microbenchmarking (component-level), run:")
    print("   ./run_benchmarks.sh")

if __name__ == "__main__":
    main()