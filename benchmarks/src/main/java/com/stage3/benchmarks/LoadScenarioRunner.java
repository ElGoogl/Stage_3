package com.stage3.benchmarks;

import java.util.LinkedHashMap;
import java.util.Map;

final class LoadScenarioRunner {
    Map<String, Object> run(BenchmarkContext ctx) throws Exception {
        BenchmarkConfig.LoadScenario scenario = ctx.config.load;
        EndpointPool searchPool = EndpointPool.roundRobin(scenario.searchUrls);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("scenario", "load");

        Map<String, Object> searchResult = ctx.ops.runSearchLoad(searchPool, scenario.queries, scenario.concurrency, scenario.durationSeconds);
        result.put("search", searchResult);
        result.put("system_stats", DockerStatsCollector.collect(ctx.config.dockerStats));
        return result;
    }
}
