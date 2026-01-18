package com.stage3.benchmarks;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class ScalingScenarioRunner {
    Map<String, Object> run(BenchmarkContext ctx) throws Exception {
        BenchmarkConfig.ScalingScenario scenario = ctx.config.scaling;
        List<Map<String, Object>> scaleResults = new ArrayList<>();

        for (int i = 0; i < scenario.sets.size(); i++) {
            BenchmarkConfig.ScaleSet set = scenario.sets.get(i);
            ctx.waitForIngestionReady(set.ingestionUrls);
            EndpointPool ingestionPool = EndpointPool.roundRobin(set.ingestionUrls);
            EndpointPool indexingPool = EndpointPool.roundRobin(set.indexingUrls);
            EndpointPool searchPool = EndpointPool.roundRobin(set.searchUrls);

            Map<String, Object> setResult = new LinkedHashMap<>();
            setResult.put("name", set.name);

            Map<String, Object> ingestionResult = ctx.ops.runIngestion(ingestionPool, scenario.bookIds, scenario.ingestionConcurrency);
            Map<String, Object> indexingResult = ctx.ops.runIndexing(indexingPool, ingestionResult, scenario.indexingConcurrency);
            Map<String, Object> searchResult = ctx.ops.runSearchLoad(searchPool, scenario.queries, scenario.searchConcurrency, scenario.searchDurationSeconds);

            setResult.put("ingestion", ingestionResult);
            setResult.put("indexing", indexingResult);
            setResult.put("search", searchResult);
            setResult.put("system_stats", DockerStatsCollector.collect(ctx.config.dockerStats));
            scaleResults.add(setResult);

            if (i < scenario.sets.size() - 1) {
                ctx.runResetScript(BenchmarkContext.defaultResetScript());
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("scenario", "scaling");
        result.put("sets", scaleResults);
        return result;
    }
}
