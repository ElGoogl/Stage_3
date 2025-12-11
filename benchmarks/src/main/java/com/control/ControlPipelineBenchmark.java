package com.control;

import org.openjdk.jmh.annotations.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Measures end-to-end pipeline duration (Control → Ingestion → Indexing)
 * for different batch sizes (1, 5, 10, 50 books).
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class ControlPipelineBenchmark {

    private static final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    private static final String PIPELINE_URL = "http://localhost:7000/control/pipeline/";

    private int nextBookId = 1342;
    private static final int MAX_ID = 1400;

    private int getNextBookId() {
        if (nextBookId > MAX_ID) nextBookId = 1342;
        return nextBookId++;
    }

    /** Helper for running one pipeline call */
    private void runPipeline(int bookId) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(PIPELINE_URL + bookId))
                .timeout(Duration.ofMinutes(2))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> response = client.send(req, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            System.err.println("Request failed: " + response.statusCode() + " for book " + bookId);
        }
    }

    /** Single-book pipeline benchmark */
    @Benchmark
    public void pipeline1Book() throws Exception {
        runPipeline(getNextBookId());
    }

    /** 5-book pipeline benchmark */
    @Benchmark
    public void pipeline5Books() throws Exception {
        for (int i = 0; i < 5; i++) {
            runPipeline(getNextBookId());
        }
    }

    /** 10-book pipeline benchmark */
    @Benchmark
    public void pipeline10Books() throws Exception {
        for (int i = 0; i < 10; i++) {
            runPipeline(getNextBookId());
        }
    }

    /** 50-book pipeline benchmark */
    @Benchmark
    public void pipeline50Books() throws Exception {
        for (int i = 0; i < 50; i++) {
            runPipeline(getNextBookId());
        }
    }
}
