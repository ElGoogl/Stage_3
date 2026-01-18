package com.stage3.benchmarks;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public final class BenchmarkRunner {
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final DateTimeFormatter RUN_ID_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        String scenario = args[0].toLowerCase(Locale.ROOT);
        Map<String, String> options = parseOptions(args);
        String configPath = options.get("config");
        String outputDir = options.getOrDefault("output-dir", "benchmark_results");

        BenchmarkConfig config = BenchmarkConfig.load(configPath);
        Path outputPath = Path.of(outputDir);
        Files.createDirectories(outputPath);

        String runId = LocalDateTime.now().format(RUN_ID_FORMAT);
        BenchmarkContext ctx = new BenchmarkContext(config, outputPath, runId, GSON);

        if (options.containsKey("reset")) {
            ctx.runResetScript(options.getOrDefault("reset-script", BenchmarkContext.defaultResetScript()));
        }

        Map<String, Object> result;
        switch (scenario) {
            case "baseline" -> result = new BaselineScenarioRunner().run(ctx);
            case "scaling" -> result = new ScalingScenarioRunner().run(ctx);
            case "load" -> result = new LoadScenarioRunner().run(ctx);
            case "failure" -> result = new FailureScenarioRunner().run(ctx);
            default -> {
                System.err.println("Unknown scenario: " + scenario);
                printUsage();
                return;
            }
        }

        ctx.writeResults(scenario, result);
    }

    private static Map<String, String> parseOptions(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 1; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                String key = args[i].substring(2);
                String value = i + 1 < args.length && !args[i + 1].startsWith("--") ? args[++i] : "true";
                options.put(key, value);
            }
        }
        return options;
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar benchmarks.jar <baseline|scaling|load|failure> [--config path] [--output-dir path] [--reset] [--reset-script path]");
    }
}
