package com.mustardgrain.solr;

import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A collection of per-Solr server statistics. These are managed by the
 * {@link SolrClient} class internally and both logged via the
 * {@link SolrClient} as well as exposed via JMX using {@link SolrClientMBean}s.
 * <p/>
 * The values contained are a snapshot in time and are reset by the
 * {@link SolrClient} after every logging. The values returned via the mapping
 * are:
 * <ul>
 * <li>successes: Number of successful queries</li>
 * <li>averageResponseTime: Average response time for successful queries</li>
 * <li>httpFailures: Number of requests generating non-HTTP 200 codes</li>
 * <li>readTimeouts: Number of timeouts while reading query response</li>
 * <li>connectTimeouts: Number of timeouts connecting to Solr servers</li>
 * <li>socketErrors: Number of "other" socket errors</li>
 * <li>emptyResults: Number of requests generating empty results (still
 * considered a "success" though)</li>
 * </ul>
 */

public class SolrStats {

    private final AtomicInteger successes = new AtomicInteger();

    private final AtomicLong responseTimes = new AtomicLong();

    private final AtomicInteger httpFailures = new AtomicInteger();

    private final AtomicInteger readTimeouts = new AtomicInteger();

    private final AtomicInteger connectTimeouts = new AtomicInteger();

    private final AtomicInteger socketErrors = new AtomicInteger();

    private final AtomicInteger emptyResults = new AtomicInteger();

    void incrementSuccesses(long responseTime) {
        successes.incrementAndGet();
        responseTimes.addAndGet(responseTime);
    }

    /**
     * Returns the number of successful queries performed during the interval.
     * 
     * @return Number of successes
     */

    public int getSuccesses() {
        return successes.get();
    }

    void incrementHttpFailures() {
        httpFailures.incrementAndGet();
    }

    /**
     * Returns the number of HTTP-based failures during the interval. HTTP-based
     * failures are designated by HTTP calls that do return but with a
     * non-200-based HTTP code.
     * 
     * @return Number of HTTP-based failures
     */

    public int getHttpFailures() {
        return httpFailures.get();
    }

    void incrementReadTimeouts() {
        readTimeouts.incrementAndGet();
    }

    /**
     * Returns the number of timeouts while reading responses from the Solr
     * server during the interval.
     * 
     * @return Number of read failures
     */

    public int getReadTimeouts() {
        return readTimeouts.get();
    }

    void incrementConnectTimeouts() {
        connectTimeouts.incrementAndGet();
    }

    /**
     * Returns the number of failed HTTP connections during the interval.
     * 
     * @return Number of failed HTTP connections
     */

    public int getConnectTimeouts() {
        return connectTimeouts.get();
    }

    void incrementSocketErrors() {
        socketErrors.incrementAndGet();
    }

    /**
     * Returns the number of {@link SocketException} errors that were received
     * during the interval. These are not {@link SocketTimeoutException} errors,
     * as those are covered by the {@link #getReadTimeouts()} API.
     * 
     * @return Number of non-timeout socket errors
     */

    public int getSocketErrors() {
        return socketErrors.get();
    }

    void incrementEmptyResults() {
        emptyResults.incrementAndGet();
    }

    /**
     * Returns the number of times that queries were successfully returned but
     * resulted in empty results. This is considered a successful operation, but
     * is designated here for interest's sake.
     * 
     * @return Number of queries issued that returned no results
     */

    public int getEmptyResults() {
        return emptyResults.get();
    }

    /**
     * Due to lack of desire/knowledge how to load this class directly in JMX
     * clients, we're simply converting this class instance to a standard map
     * instance.
     * 
     * @return Map-based statistics
     */

    Map<String, Number> toStatsMap() {
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