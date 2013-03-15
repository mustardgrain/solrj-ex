package com.mustardgrain.solr;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SolrStats {

    private final AtomicInteger successes = new AtomicInteger();

    private final AtomicLong responseTimes = new AtomicLong();

    private final AtomicInteger httpFailures = new AtomicInteger();

    private final AtomicInteger readTimeouts = new AtomicInteger();

    private final AtomicInteger connectTimeouts = new AtomicInteger();

    private final AtomicInteger socketErrors = new AtomicInteger();

    private final AtomicInteger emptyResults = new AtomicInteger();

    public void incrementSuccesses(long responseTime) {
        successes.incrementAndGet();
        responseTimes.addAndGet(responseTime);
    }

    public int getSuccesses() {
        return successes.get();
    }

    public void incrementHttpFailures() {
        httpFailures.incrementAndGet();
    }

    public int getHttpFailures() {
        return httpFailures.get();
    }

    public void incrementReadTimeouts() {
        readTimeouts.incrementAndGet();
    }

    public int getReadTimeouts() {
        return readTimeouts.get();
    }

    public void incrementConnectTimeouts() {
        connectTimeouts.incrementAndGet();
    }

    public int getConnectTimeouts() {
        return connectTimeouts.get();
    }

    public void incrementSocketErrors() {
        socketErrors.incrementAndGet();
    }

    public int getSocketErrors() {
        return socketErrors.get();
    }

    public void incrementEmptyResults() {
        emptyResults.incrementAndGet();
    }

    public int getEmptyResults() {
        return emptyResults.get();
    }

    public Map<String, Number> toStatsMap() {
        long s = successes.get();
        long r = responseTimes.get();
        long avg = s <= 0 ? -1 : r / s;

        Map<String, Number> stats = new HashMap<String, Number>();
        stats.put("successes", (int) s);
        stats.put("averageResponseTime", avg);
        stats.put("httpFailures", httpFailures.get());
        stats.put("readTimeouts", readTimeouts.get());
        stats.put("connectTimeouts", connectTimeouts.get());
        stats.put("socketErrors", socketErrors.get());
        stats.put("emptyResults", emptyResults.get());
        return stats;
    }

    void reset() {
        successes.set(0);
        responseTimes.set(0);
        httpFailures.set(0);
        readTimeouts.set(0);
        connectTimeouts.set(0);
        socketErrors.set(0);
        emptyResults.set(0);
    }

}