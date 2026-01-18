package com.stage3.benchmarks;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

final class FailureScenarioRunner {
    Map<String, Object> run(BenchmarkContext ctx) throws Exception {
        BenchmarkConfig.FailureScenario scenario = ctx.config.failure;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("scenario", "failure");
        result.put("failure_command", scenario.failureCommand);

        Instant start = Instant.now();
        Process process = new ProcessBuilder("bash", "-lc", scenario.failureCommand)
                .redirectErrorStream(true)
                .start();
        int exitCode = process.waitFor();
        result.put("failure_exit_code", exitCode);

        long recoveryTime = ctx.ops.waitForRecovery(scenario.healthUrls, scenario.recoveryTimeoutSeconds, scenario.pollIntervalSeconds);
        result.put("recovery_time_seconds", recoveryTime >= 0 ? recoveryTime : null);
        result.put("recovered", recoveryTime >= 0);
        result.put("system_stats", DockerStatsCollector.collect(ctx.config.dockerStats));

        Map<String, Object> timings = new LinkedHashMap<>();
        timings.put("total_duration_seconds", Duration.between(start, Instant.now()).toSeconds());
        result.put("timings", timings);
        return result;
    }
}
