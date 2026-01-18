package com.stage3.benchmarks;

import java.util.LinkedHashMap;
import java.util.Map;

final class BaselineScenarioRunner {
    Map<String, Object> run(BenchmarkContext ctx) throws Exception {
        BenchmarkConfig.BaselineScenario scenario = ctx.config.baseline;
        ctx.waitForIngestionReady(ctx.collectBaselineIngestionUrls());

        EndpointPool ingestionPool = EndpointPool.single(scenario.ingestionUrl);
        EndpointPool indexingPool = EndpointPool.single(scenario.indexingUrl);
        EndpointPool searchPool = EndpointPool.single(scenario.searchUrl);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("scenario", "baseline");

        Map<String, Object> ingestionResult = ctx.ops.runIngestion(ingestionPool, scenario.bookIds, scenario.ingestionConcurrency);
        Map<String, Object> indexingResult = ctx.ops.runIndexing(indexingPool, ingestionResult, scenario.indexingConcurrency);
        Map<String, Object> searchResult = ctx.ops.runSearchLoad(searchPool, scenario.queries, scenario.searchConcurrency, scenario.searchDurationSeconds);
        result.put("ingestion", ingestionResult);
        result.put("indexing", indexingResult);
        result.put("search", searchResult);
        result.put("system_stats", DockerStatsCollector.collect(ctx.config.dockerStats));
        return result;
    }
}
