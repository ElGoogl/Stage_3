package com.stage3.benchmarks;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class BenchmarkConfig {
    public BaselineScenario baseline;
    public ScalingScenario scaling;
    public LoadScenario load;
    public FailureScenario failure;
    public DockerStats dockerStats;

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    public static BenchmarkConfig load(String configPath) throws IOException {
        if (configPath != null) {
            Path path = Path.of(configPath);
            if (Files.exists(path)) {
                try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
                    return GSON.fromJson(reader, BenchmarkConfig.class);
                }
            }
        }

        try (var stream = BenchmarkConfig.class.getClassLoader().getResourceAsStream("benchmark-scenarios.json")) {
            if (stream == null) {
                throw new IOException("Default benchmark-scenarios.json resource not found");
            }
            try (Reader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
                return GSON.fromJson(reader, BenchmarkConfig.class);
            }
        }
    }

    public static final class BaselineScenario {
        public String ingestionUrl;
        public String indexingUrl;
        public String searchUrl;
        public List<Integer> bookIds;
        public List<String> queries;
        public int ingestionConcurrency;
        public int indexingConcurrency;
        public int searchConcurrency;
        public int searchDurationSeconds;
    }

    public static final class ScalingScenario {
        public List<ScaleSet> sets;
        public List<Integer> bookIds;
        public List<String> queries;
        public int ingestionConcurrency;
        public int indexingConcurrency;
        public int searchConcurrency;
        public int searchDurationSeconds;
    }

    public static final class ScaleSet {
        public String name;
        public List<String> ingestionUrls;
        public List<String> indexingUrls;
        public List<String> searchUrls;
    }

    public static final class LoadScenario {
        public List<String> searchUrls;
        public List<String> queries;
        public int concurrency;
        public int durationSeconds;
    }

    public static final class FailureScenario {
        public String failureCommand;
        public List<String> healthUrls;
        public int recoveryTimeoutSeconds;
        public int pollIntervalSeconds;
    }

    public static final class DockerStats {
        public boolean enabled;
        public List<String> containers;
    }
}
