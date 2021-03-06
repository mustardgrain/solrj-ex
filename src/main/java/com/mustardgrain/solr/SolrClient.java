/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mustardgrain.solr;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.AbstractHttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrjNamedThreadFactory;

/**
 * SolrClient provides some extensions to {@link LBHttpSolrServer}.
 */
public class SolrClient extends SolrServer implements SolrClientMBean {

    // We assume a pool of SolrClient instances may be used, so give each a
    // unique means of identification in JMX
    private final static AtomicInteger objectNameIdCounter = new AtomicInteger();

    // keys to the maps are currently of the form "http://localhost:8983/solr"
    // which should be equivalent to CommonsHttpSolrServer.getBaseURL()
    private final Map<String, ServerWrapper> aliveServers = new LinkedHashMap<String, ServerWrapper>();

    // access to aliveServers should be synchronized on itself

    private final Map<String, ServerWrapper> zombieServers = new ConcurrentHashMap<String, ServerWrapper>();

    private final Map<String, SolrStats> serverStats = new ConcurrentHashMap<String, SolrStats>();

    // changes to aliveServers are reflected in this array, no need to
    // synchronize
    private volatile ServerWrapper[] aliveServerList = new ServerWrapper[0];

    private ScheduledExecutorService aliveCheckExecutor;

    private ScheduledExecutorService statsExecutor;

    private final HttpClient httpClient;

    private final boolean clientIsInternal;

    private final AtomicInteger counter = new AtomicInteger(-1);

    private static final SolrQuery solrQuery = new SolrQuery("*:*");

    private final ResponseParser parser;

    private ObjectName objectName;

    private static final Log LOG = LogFactory.getLog(SolrClient.class);

    static {
        solrQuery.setRows(0);
    }

    private static class ServerWrapper {

        final HttpSolrServer solrServer;

        long lastUsed; // last time used for a real request

        long lastChecked; // last time checked for liveness

        // "standard" servers are used by default. They normally live in the
        // alive list
        // and move to the zombie list when unavailable. When they become
        // available again,
        // they move back to the alive list.
        boolean standard = true;

        int failedPings = 0;

        public ServerWrapper(HttpSolrServer solrServer) {
            this.solrServer = solrServer;
        }

        @Override
        public String toString() {
            return solrServer.getBaseURL();
        }

        public String getKey() {
            return solrServer.getBaseURL();
        }

        @Override
        public int hashCode() {
            return this.getKey().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (!(obj instanceof ServerWrapper))
                return false;
            return this.getKey().equals(((ServerWrapper) obj).getKey());
        }
    }

    public static class Req {

        protected SolrRequest request;

        protected List<String> servers;

        protected int numDeadServersToTry;

        public Req(SolrRequest request, List<String> servers) {
            this.request = request;
            this.servers = servers;
            this.numDeadServersToTry = servers.size();
        }

        public SolrRequest getRequest() {
            return request;
        }

        public List<String> getServers() {
            return servers;
        }

        /**
         * @return the number of dead servers to try if there are no live
         *         servers left
         */
        public int getNumDeadServersToTry() {
            return numDeadServersToTry;
        }

        /**
         * @param numDeadServersToTry The number of dead servers to try if there
         *        are no live servers left. Defaults to the number of servers in
         *        this request.
         */
        public void setNumDeadServersToTry(int numDeadServersToTry) {
            this.numDeadServersToTry = numDeadServersToTry;
        }
    }

    public static class Rsp {

        protected String server;

        protected NamedList<Object> rsp;

        /** The response from the server */
        public NamedList<Object> getResponse() {
            return rsp;
        }

        /** The server that returned the response */
        public String getServer() {
            return server;
        }
    }

    public SolrClient(String... solrServerUrls) throws MalformedURLException {
        this(null, solrServerUrls);
    }

    /** The provided httpClient should use a multi-threaded connection manager */
    public SolrClient(HttpClient httpClient, String... solrServerUrl) throws MalformedURLException {
        this(httpClient, new BinaryResponseParser(), solrServerUrl);
    }

    /** The provided httpClient should use a multi-threaded connection manager */
    public SolrClient(HttpClient httpClient, ResponseParser parser, String... solrServerUrl) throws MalformedURLException {
        clientIsInternal = (httpClient == null);
        this.parser = parser;
        if (httpClient == null) {
            ModifiableSolrParams params = new ModifiableSolrParams();
            params.set(HttpClientUtil.PROP_USE_RETRY, false);
            this.httpClient = HttpClientUtil.createClient(params);
        } else {
            this.httpClient = httpClient;
        }
        for (String s : solrServerUrl) {
            ServerWrapper wrapper = new ServerWrapper(makeServer(s));
            aliveServers.put(wrapper.getKey(), wrapper);
            serverStats.put(wrapper.getKey(), new SolrStats());
        }
        updateAliveList();
        startStatsExecutor();
        registerMBean();
    }

    private void registerMBean() {
        String name = getClass().getPackage().getName() + ":type="
                      + getClass().getSimpleName()
                      + "-"
                      + objectNameIdCounter.incrementAndGet();

        try {
            objectName = new ObjectName(name);
            name = objectName.toString();
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, new ObjectName(name));

            if (LOG.isInfoEnabled())
                LOG.info("Registered " + objectName);
        } catch (Exception e) {
            if (LOG.isWarnEnabled())
                LOG.warn("Couldn't add MBean " + name);
        }
    }

    private void unregisterMBean() {
        if (objectName == null)
            return;

        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.unregisterMBean(objectName);

            if (LOG.isInfoEnabled())
                LOG.info("Unregistered " + objectName);
        } catch (Exception e) {
            if (LOG.isWarnEnabled())
                LOG.warn("Couldn't remove MBean " + objectName.toString());
        }
    }

    public static String normalize(String server) {
        if (server.endsWith("/"))
            server = server.substring(0, server.length() - 1);
        return server;
    }

    protected HttpSolrServer makeServer(String server) throws MalformedURLException {
        return new HttpSolrServer(server, httpClient, parser);
    }

    /**
     * This customization/hack circumvents Solr's hard-coding of the User-Agent
     * header. We actually intercept each HTTP request and force the header to
     * our desired string. This was done because we don't want to have to fork
     * and change HttpSolrServer too.
     * 
     * @param userAgent User agent string
     * @see HttpSolrServer#AGENT
     */
    
    public void setUserAgent(final String userAgent) {
        ((AbstractHttpClient) httpClient).addRequestInterceptor(new HttpRequestInterceptor() {

            @Override
            public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
                request.setHeader("User-Agent", userAgent);
            }
        });
    }

    /**
     * Calling this method creates a hook in the HTTP request pipeline that
     * suppresses the Connection header from being output to "Keep-alive" since
     * we're using HTTP 1.1 in which that is superfluous.
     */
    
    public void disableConnectonHeader() {
        ((AbstractHttpClient) httpClient).addRequestInterceptor(new HttpRequestInterceptor() {

            @Override
            public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
                request.removeHeaders("Connection");
            }
        });
    }

    /**
     * Tries to query a live server from the list provided in Req. Servers in
     * the dead pool are skipped. If a request fails due to an IOException, the
     * server is moved to the dead pool for a certain period of time, or until a
     * test request on that server succeeds. Servers are queried in the exact
     * order given (except servers currently in the dead pool are skipped). If
     * no live servers from the provided list remain to be tried, a number of
     * previously skipped dead servers will be tried.
     * Req.getNumDeadServersToTry() controls how many dead servers will be
     * tried. If no live servers are found a SolrServerException is thrown.
     * 
     * @param req contains both the request as well as the list of servers to
     *        query
     * @return the result of the request
     * @throws IOException If there is a low-level I/O error.
     */
    public Rsp request(Req req) throws SolrServerException, IOException {
        Rsp rsp = new Rsp();
        Exception ex = null;

        List<ServerWrapper> skipped = new ArrayList<ServerWrapper>(req.getNumDeadServersToTry());

        for (String serverStr : req.getServers()) {
            serverStr = normalize(serverStr);
            // if the server is currently a zombie, just skip to the next one
            ServerWrapper wrapper = zombieServers.get(serverStr);
            if (wrapper != null) {
                // System.out.println("ZOMBIE SERVER QUERIED: " + serverStr);
                if (skipped.size() < req.getNumDeadServersToTry())
                    skipped.add(wrapper);
                continue;
            }
            rsp.server = serverStr;
            HttpSolrServer server = makeServer(serverStr);

            try {
                long start = System.currentTimeMillis();
                rsp.rsp = server.request(req.getRequest());
                long end = System.currentTimeMillis();
                updateStatsSuccess(server, end - start);
                return rsp; // SUCCESS
            } catch (SolrException e) {
                updateStatsException(server, e);
                // we retry on 404 or 403 or 503 - you can see this on solr
                // shutdown
                if (e.code() == 404 || e.code() == 403 || e.code() == 503 || e.code() == 500) {
                    ex = addZombie(server, e);
                } else {
                    // Server is alive but the request was likely malformed or
                    // invalid
                    throw e;
                }

                // TODO: consider using below above - currently does cause a
                // problem with distrib updates:
                // seems to match up against a failed forward to leader
                // exception as well...
                // || e.getMessage().contains("java.net.SocketException")
                // || e.getMessage().contains("java.net.ConnectException")
            } catch (SocketException e) {
                updateStatsException(server, e);
                ex = addZombie(server, e);
            } catch (SocketTimeoutException e) {
                updateStatsException(server, e);
                ex = addZombie(server, e);
            } catch (SolrServerException e) {
                Throwable rootCause = e.getRootCause();
                updateStatsException(server, rootCause);
                if (rootCause instanceof IOException) {
                    ex = addZombie(server, e);
                } else {
                    throw e;
                }
            } catch (Exception e) {
                updateStatsException(server, e);
                throw new SolrServerException(e);
            }
        }

        // try the servers we previously skipped
        for (ServerWrapper wrapper : skipped) {
            try {
                long start = System.currentTimeMillis();
                rsp.rsp = wrapper.solrServer.request(req.getRequest());
                long end = System.currentTimeMillis();
                updateStatsSuccess(wrapper.solrServer, end - start);
                zombieServers.remove(wrapper.getKey());
                return rsp; // SUCCESS
            } catch (SolrException e) {
                updateStatsException(wrapper.solrServer, e);
                // we retry on 404 or 403 or 503 - you can see this on solr
                // shutdown
                if (e.code() == 404 || e.code() == 403 || e.code() == 503 || e.code() == 500) {
                    ex = e;
                    // already a zombie, no need to re-add
                } else {
                    // Server is alive but the request was malformed or invalid
                    zombieServers.remove(wrapper.getKey());
                    throw e;
                }

            } catch (SocketException e) {
                updateStatsException(wrapper.solrServer, e);
                ex = e;
            } catch (SocketTimeoutException e) {
                updateStatsException(wrapper.solrServer, e);
                ex = e;
            } catch (SolrServerException e) {
                Throwable rootCause = e.getRootCause();
                updateStatsException(wrapper.solrServer, rootCause);
                if (rootCause instanceof IOException) {
                    ex = e;
                    // already a zombie, no need to re-add
                } else {
                    throw e;
                }
            } catch (Exception e) {
                updateStatsException(wrapper.solrServer, e);
                throw new SolrServerException(e);
            }
        }

        if (ex == null) {
            throw new SolrServerException("No live SolrServers available to handle this request");
        } else {
            throw new SolrServerException("No live SolrServers available to handle this request:" + zombieServers.keySet(),
                                          ex);
        }

    }

    private Exception addZombie(HttpSolrServer server, Exception e) {

        ServerWrapper wrapper;

        wrapper = new ServerWrapper(server);
        wrapper.lastUsed = System.currentTimeMillis();
        wrapper.standard = false;
        zombieServers.put(wrapper.getKey(), wrapper);
        startAliveCheckExecutor();

        if (LOG.isWarnEnabled())
            LOG.warn("Marking server " + wrapper.solrServer.getBaseURL() + " inactive - " + getNestedErrorMessages(e));

        return e;
    }

    private void updateAliveList() {
        synchronized (aliveServers) {
            aliveServerList = aliveServers.values().toArray(new ServerWrapper[aliveServers.size()]);
        }
    }

    private ServerWrapper removeFromAlive(String key) {
        synchronized (aliveServers) {
            ServerWrapper wrapper = aliveServers.remove(key);
            if (wrapper != null)
                updateAliveList();
            return wrapper;
        }
    }

    private void addToAlive(ServerWrapper wrapper) {
        synchronized (aliveServers) {
            ServerWrapper prev = aliveServers.put(wrapper.getKey(), wrapper);
            // TODO: warn if there was a previous entry?
            updateAliveList();
        }
    }

    public void addSolrServer(String server) throws MalformedURLException {
        HttpSolrServer solrServer = makeServer(server);
        addToAlive(new ServerWrapper(solrServer));
    }

    public String removeSolrServer(String server) {
        try {
            server = new URL(server).toExternalForm();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        if (server.endsWith("/")) {
            server = server.substring(0, server.length() - 1);
        }

        // there is a small race condition here - if the server is in the
        // process of being moved between
        // lists, we could fail to remove it.
        removeFromAlive(server);
        zombieServers.remove(server);
        return null;
    }

    public void setConnectionTimeout(int timeout) {
        HttpClientUtil.setConnectionTimeout(httpClient, timeout);
    }

    /**
     * set soTimeout (read timeout) on the underlying HttpConnectionManager.
     * This is desirable for queries, but probably not for indexing.
     */
    public void setSoTimeout(int timeout) {
        HttpClientUtil.setSoTimeout(httpClient, timeout);
    }

    @Override
    public void shutdown() {
        if (aliveCheckExecutor != null) {
            aliveCheckExecutor.shutdownNow();
        }
        if (statsExecutor != null) {
            statsExecutor.shutdownNow();
        }
        if (clientIsInternal) {
            httpClient.getConnectionManager().shutdown();
        }

        unregisterMBean();
    }

    /**
     * Tries to query a live server. A SolrServerException is thrown if all
     * servers are dead. If the request failed due to IOException then the live
     * server is moved to dead pool and the request is retried on another live
     * server. After live servers are exhausted, any servers previously marked
     * as dead will be tried before failing the request.
     * 
     * @param request the SolrRequest.
     * @return response
     * @throws IOException If there is a low-level I/O error.
     */
    @Override
    public NamedList<Object> request(final SolrRequest request) throws SolrServerException, IOException {
        Exception ex = null;
        ServerWrapper[] serverList = aliveServerList;

        int maxTries = serverList.length;
        Map<String, ServerWrapper> justFailed = null;

        for (int attempts = 0; attempts < maxTries; attempts++) {
            int count = counter.incrementAndGet();
            ServerWrapper wrapper = serverList[count % serverList.length];
            wrapper.lastUsed = System.currentTimeMillis();

            try {
                long start = System.currentTimeMillis();
                NamedList<Object> response = wrapper.solrServer.request(request);
                long end = System.currentTimeMillis();
                updateStatsSuccess(wrapper.solrServer, end - start);

                SolrDocumentList results = (SolrDocumentList) response.get("response");

                if (results != null) {
                    if (results.getNumFound() <= 0)
                        updateStatsEmptyResults(wrapper.solrServer);
                }

                return response;
            } catch (SolrException e) {
                // Server is alive but the request was malformed or invalid
                updateStatsException(wrapper.solrServer, e);
                throw e;
            } catch (SolrServerException e) {
                Throwable rootCause = e.getRootCause();
                updateStatsException(wrapper.solrServer, rootCause);

                if (rootCause instanceof IOException) {
                    ex = e;
                    moveAliveToDead(wrapper, e);
                    if (justFailed == null)
                        justFailed = new HashMap<String, ServerWrapper>();
                    justFailed.put(wrapper.getKey(), wrapper);
                } else {
                    throw e;
                }
            } catch (Exception e) {
                updateStatsException(wrapper.solrServer, e);
                throw new SolrServerException(e);
            }
        }

        // try other standard servers that we didn't try just now
        for (ServerWrapper wrapper : zombieServers.values()) {
            if (wrapper.standard == false || justFailed != null && justFailed.containsKey(wrapper.getKey()))
                continue;
            try {
                NamedList<Object> rsp = wrapper.solrServer.request(request);
                // remove from zombie list *before* adding to alive to avoid a
                // race that could lose a server
                zombieServers.remove(wrapper.getKey());
                addToAlive(wrapper);

                if (LOG.isInfoEnabled())
                    LOG.info("Marking server " + wrapper.solrServer.getBaseURL()
                             + " active after exhausting attempts to contact known active servers failed");

                return rsp;
            } catch (SolrException e) {
                // Server is alive but the request was malformed or invalid
                updateStatsException(wrapper.solrServer, e);
                throw e;
            } catch (SolrServerException e) {
                Throwable rootCause = e.getRootCause();
                updateStatsException(wrapper.solrServer, rootCause);
                if (rootCause instanceof IOException) {
                    ex = e;
                    // still dead
                } else {
                    throw e;
                }
            } catch (Exception e) {
                updateStatsException(wrapper.solrServer, e);
                throw new SolrServerException(e);
            }
        }

        if (ex == null) {
            throw new SolrServerException("No live SolrServers available to handle this request");
        } else {
            throw new SolrServerException("No live SolrServers available to handle this request", ex);
        }
    }

    /**
     * Takes up one dead server and check for aliveness. The check is done in a
     * roundrobin. Each server is checked for aliveness once in 'x' millis where
     * x is decided by the setAliveCheckinterval() or it is defaulted to 1
     * minute
     * 
     * @param zombieServer a server in the dead pool
     */
    private void checkAZombieServer(ServerWrapper zombieServer) {
        long currTime = System.currentTimeMillis();
        try {
            zombieServer.lastChecked = currTime;
            long start = System.currentTimeMillis();
            QueryResponse resp = zombieServer.solrServer.query(solrQuery);
            long end = System.currentTimeMillis();
            updateStatsSuccess(zombieServer.solrServer, end - start);
            if (resp.getStatus() == 0) {
                updateStatsSuccess(zombieServer.solrServer, end - start);
                // server has come back up.
                // make sure to remove from zombies before adding to alive to
                // avoid a race condition
                // where another thread could mark it down, move it back to
                // zombie, and then we delete
                // from zombie and lose it forever.
                ServerWrapper wrapper = zombieServers.remove(zombieServer.getKey());
                if (wrapper != null) {
                    wrapper.failedPings = 0;
                    if (wrapper.standard) {
                        addToAlive(wrapper);
                    }
                } else {
                    // something else already moved the server from zombie to
                    // alive
                }

                if (LOG.isInfoEnabled())
                    LOG.info("Marking server " + zombieServer.solrServer.getBaseURL() + " active after check");
            }
        } catch (Exception e) {
            // Expected. The server is still down.
            zombieServer.failedPings++;

            if (LOG.isWarnEnabled())
                LOG.warn("Server " + zombieServer.solrServer.getBaseURL()
                         + " still inactive after "
                         + zombieServer.failedPings
                         + " checks - "
                         + getNestedErrorMessages(e));

            // If the server doesn't belong in the standard set belonging to
            // this load balancer
            // then simply drop it after a certain number of failed pings.
            if (!zombieServer.standard && zombieServer.failedPings >= NONSTANDARD_PING_LIMIT) {
                zombieServers.remove(zombieServer.getKey());
            }
        }
    }

    private void moveAliveToDead(ServerWrapper wrapper, Throwable t) {
        wrapper = removeFromAlive(wrapper.getKey());
        if (wrapper == null)
            return; // another thread already detected the failure and removed
                    // it
        zombieServers.put(wrapper.getKey(), wrapper);
        startAliveCheckExecutor();

        if (LOG.isWarnEnabled())
            LOG.warn("Marking server " + wrapper.solrServer.getBaseURL() + " inactive - " + getNestedErrorMessages(t));
    }

    private int interval = CHECK_INTERVAL;

    /**
     * SolrClient keeps pinging the dead servers at fixed interval to find if it
     * is alive. Use this to set that interval
     * 
     * @param interval time in milliseconds
     */
    public void setAliveCheckInterval(int interval) {
        if (interval <= 0) {
            throw new IllegalArgumentException("Alive check interval must be " + "positive, specified value = "
                                               + interval);
        }
        this.interval = interval;
    }

    private void startAliveCheckExecutor() {
        // double-checked locking, but it's OK because we don't *do* anything
        // with aliveCheckExecutor
        // if it's not null.
        if (aliveCheckExecutor == null) {
            synchronized (this) {
                if (aliveCheckExecutor == null) {
                    aliveCheckExecutor = Executors
                                    .newSingleThreadScheduledExecutor(new SolrjNamedThreadFactory("aliveCheckExecutor"));
                    aliveCheckExecutor.scheduleAtFixedRate(getAliveCheckRunner(new WeakReference<SolrClient>(this)),
                                                           this.interval,
                                                           this.interval,
                                                           TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    private static Runnable getAliveCheckRunner(final WeakReference<SolrClient> lbRef) {
        return new Runnable() {

            @Override
            public void run() {
                SolrClient lb = lbRef.get();
                if (lb != null && lb.zombieServers != null) {
                    for (ServerWrapper zombieServer : lb.zombieServers.values()) {
                        lb.checkAZombieServer(zombieServer);
                    }
                }
            }
        };
    }

    private int statsInterval = STATS_INTERVAL;

    /**
     * SolrClient outputs some statistics to the log on a regular interval. Use
     * this to set that interval
     * 
     * @param interval time in milliseconds
     */
    public void setStatsInterval(int interval) {
        if (interval <= 0) {
            throw new IllegalArgumentException("SolrStats interval must be " + "positive, specified value = "
                                               + interval);
        }
        this.statsInterval = interval;

        if (statsExecutor != null)
            statsExecutor.scheduleAtFixedRate(getStatsRunner(new WeakReference<SolrClient>(this)),
                                              this.statsInterval,
                                              this.statsInterval,
                                              TimeUnit.MILLISECONDS);
    }

    private void startStatsExecutor() {
        statsExecutor = Executors.newSingleThreadScheduledExecutor(new SolrjNamedThreadFactory("statsExecutor"));
        statsExecutor.scheduleAtFixedRate(getStatsRunner(new WeakReference<SolrClient>(this)),
                                          this.statsInterval,
                                          this.statsInterval,
                                          TimeUnit.MILLISECONDS);
    }

    private static Runnable getStatsRunner(final WeakReference<SolrClient> lbRef) {
        return new Runnable() {

            @Override
            public void run() {
                SolrClient lb = lbRef.get();
                if (lb != null && lb.serverStats != null && LOG.isInfoEnabled()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("server responses over past ");
                    sb.append(TimeUnit.MILLISECONDS.toMinutes(lb.statsInterval));
                    sb.append(" mins (good/timeout/zero found) and cache hit ratio: ");
                    boolean appendComma = false;

                    for (Map.Entry<String, SolrStats> entry : lb.serverStats.entrySet()) {
                        if (appendComma)
                            sb.append(", ");

                        String server = entry.getKey();
                        SolrStats stats = entry.getValue();
                        String hitRatio = getHitRatio(server);

                        sb.append(server);
                        sb.append(": ");
                        sb.append(stats.getSuccesses());
                        sb.append("/");
                        sb.append(stats.getReadTimeouts());
                        sb.append("/");
                        sb.append(stats.getEmptyResults());
                        sb.append(" ");
                        sb.append(hitRatio);

                        appendComma = true;

                        stats.reset(); // Reset the counters once printed
                    }

                    LOG.info(sb);
                }
            }

            /**
             * Determines the hit ratio for the query result cache by calling
             * the SolrServer via JMX and reading its MBean containing that
             * information.
             * @param server Server from which to pull information
             * @return String form of hit ratio (either "n/a" or "xx%")
             */
            
            private String getHitRatio(String server) {
                String hitRatio = "n/a";
                JMXConnector jmxc = null;

                try {
                    URL url = new URL(server);
                    String domain = url.getPath();

                    if (domain.startsWith("/"))
                        domain = domain.substring(1);

                    ObjectName name = new ObjectName(domain + ":id=org.apache.solr.search.LRUCache,type=queryResultCache");
                    JMXServiceURL jmxUrl = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + url.getHost()
                                                             + ":7199/jmxrmi");
                    jmxc = JMXConnectorFactory.connect(jmxUrl, null);
                    MBeanServerConnection con = jmxc.getMBeanServerConnection();

                    Object result = con.getAttribute(name, "hitratio");
                    hitRatio = (int) (Float.parseFloat(String.valueOf(result)) * 100.0f) + "%";
                } catch (Exception e) {
                    LOG.error(getNestedErrorMessages(e));
                } finally {
                    if (jmxc != null) {
                        try {
                            jmxc.close();
                        } catch (Exception e) {
                            LOG.error(getNestedErrorMessages(e));
                        }
                    }
                }

                return hitRatio;
            }

        };
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            shutdown();
        } finally {
            super.finalize();
        }
    }

    // defaults
    private static final int CHECK_INTERVAL = 60 * 1000; // 1 minute between
                                                         // checks

    private static final int STATS_INTERVAL = 5 * 60 * 1000; // 5 minute between
                                                             // stats output

    private static final int NONSTANDARD_PING_LIMIT = 5; // number of times
                                                         // we'll ping dead
                                                         // servers not in the
                                                         // server list

    public QueryResponse query(SolrParams params) throws SolrServerException {
        // Force to POST to prevent GET overflows and request logging
        // sensitivity.
        return query(params, METHOD.POST);
    }

    public QueryResponse query(SolrParams params, METHOD method) throws SolrServerException {
        // Force to POST to prevent GET overflows and request logging
        // sensitivity.
        if (method != METHOD.POST) {
            if (LOG.isWarnEnabled())
                LOG.warn("METHOD." + method.name() + " used, forcing use of METHOD.POST for queries");

            method = METHOD.POST;
        }

        return super.query(params, method);
    }

    private void updateStatsException(HttpSolrServer server, Throwable error) {
        SolrStats stats = serverStats.get(server.getBaseURL());

        if (stats == null)
            return;

        if (error instanceof SocketTimeoutException)
            stats.incrementReadTimeouts();
        else if (error instanceof ConnectException)
            stats.incrementConnectTimeouts();
        else if (error instanceof SocketException)
            stats.incrementSocketErrors();
        else if (error instanceof SolrException && ((SolrException) error).code() / 100 != 2)
            stats.incrementHttpFailures(); // Non 2xx HTTP return codes

        if (LOG.isWarnEnabled())
            LOG.warn(getNestedErrorMessages(error));
    }

    private void updateStatsEmptyResults(HttpSolrServer server) {
        SolrStats stats = serverStats.get(server.getBaseURL());

        if (stats == null)
            return;

        stats.incrementEmptyResults();
    }

    private void updateStatsSuccess(HttpSolrServer server, long responseTime) {
        SolrStats stats = serverStats.get(server.getBaseURL());

        if (stats == null)
            return;

        stats.incrementSuccesses(responseTime);
    }

    @Override
    public List<String> getAllServers() {
        return new ArrayList<String>(serverStats.keySet());
    }

    @Override
    public List<String> getAliveServers() {
        return new ArrayList<String>(aliveServers.keySet());
    }

    @Override
    public List<String> getZombieServers() {
        return new ArrayList<String>(zombieServers.keySet());
    }

    public Map<String, Map<String, Number>> getStats() {
        Map<String, Map<String, Number>> ret = new HashMap<String, Map<String, Number>>();

        for (Map.Entry<String, SolrStats> entry : serverStats.entrySet())
            ret.put(entry.getKey(), entry.getValue().toStatsMap());

        return ret;
    }

    private static String getNestedErrorMessages(Throwable t) {
        StringBuilder sb = new StringBuilder();

        while (t != null) {
            if (sb.length() > 0)
                sb.append("; caused by: ");

            sb.append(t.getMessage());
            t = t.getCause();
        }

        return sb.toString();
    }

}
