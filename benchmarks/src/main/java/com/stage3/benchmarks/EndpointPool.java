package com.stage3.benchmarks;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

final class EndpointPool {
    private final List<String> endpoints;
    private final AtomicInteger index = new AtomicInteger(0);

    EndpointPool(List<String> endpoints) {
        this.endpoints = endpoints;
    }

    static EndpointPool single(String endpoint) {
        return new EndpointPool(List.of(endpoint));
    }

    static EndpointPool roundRobin(List<String> endpoints) {
        return new EndpointPool(endpoints);
    }

    String next() {
        int pos = Math.abs(index.getAndIncrement() % endpoints.size());
        return endpoints.get(pos);
    }
}
