package com.mustardgrain.solr;

import java.net.InetAddress;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpParams;
import org.apache.solr.client.solrj.impl.HttpClientUtil;

/**
 * SolrClientFactory is the factory for generating SolrClient instance. Used
 * primarily within the Apache Commons pooling logic.
 */

public class SolrClientFactory implements PoolableObjectFactory<SolrClient> {

    private final String version;

    private final String userAgent;

    private final List<String> urls;

    private final int connectTimeout;

    private final int soTimeout;

    private final boolean staleConnectionCheck;

    private final int statsInterval;

    private final boolean disableConnectonHeader;

    /**
     * Creates a new SolrClientFactory.
     * 
     * @param version Version string, or null to use default ("n/a")
     * @param userAgent User agent string, or null to use default
     *        ("<local host name>:<version>")
     * @param urls List of one or more Solr URLs
     * @param connectTimeout Timeout (in milliseconds) for underlying socket
     *        connection, or -1 to use {@link SolrClient} default
     * @param soTimeout Timeout (in milliseconds) for underlying socket reads,
     *        or -1 to use {@link SolrClient} default
     * @param staleConnectionCheck Set to true to cause the Apache Commons HTTP
     *        code to perform stale connection checks, or false to omit the
     *        checks for a (minor) performance increase
     * @param statsInterval Interval (in milliseconds) between statistics
     *        logging, or -1 to use default (5 minutes)
     * @param disableConnectonHeader Set to true to suppress output of the
     *        "Connection" header on HTTP requests as these are unneeded for
     *        HTTP 1.1 communication
     * @see SolrServerPool#SolrServerPool(PoolableObjectFactory)
     */

    public SolrClientFactory(String version,
                             String userAgent,
                             List<String> urls,
                             int connectTimeout,
                             int soTimeout,
                             boolean staleConnectionCheck,
                             int statsInterval,
                             boolean disableConnectonHeader) {
        if (urls == null)
            throw new IllegalArgumentException("List of Solr URLs cannot be null");

        if (urls.isEmpty())
            throw new IllegalArgumentException("List of Solr URLs cannot be empty");

        this.version = version;
        this.userAgent = userAgent;
        this.urls = urls;
        this.connectTimeout = connectTimeout;
        this.soTimeout = soTimeout;
        this.staleConnectionCheck = staleConnectionCheck;
        this.statsInterval = statsInterval;
        this.disableConnectonHeader = disableConnectonHeader;
    }

    @Override
    public SolrClient makeObject() throws Exception {
        SolrClient solrServer = new SolrClient(urls.toArray(new String[0]));

        if (connectTimeout != -1)
            solrServer.setConnectionTimeout(connectTimeout);

        if (soTimeout != -1)
            solrServer.setSoTimeout(soTimeout);

        HttpParams params = solrServer.getHttpClient().getParams();
        params.setBooleanParameter(HttpClientUtil.PROP_USE_RETRY, false);
        params.setBooleanParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, staleConnectionCheck);

        if (statsInterval != -1)
            solrServer.setStatsInterval(statsInterval);

        if (!disableConnectonHeader)
            solrServer.disableConnectonHeader();

        if (!StringUtils.isBlank(userAgent)) {
            solrServer.setUserAgent(userAgent);
        } else {
            solrServer.setUserAgent(InetAddress.getLocalHost().getHostName() + ":"
                                    + (!StringUtils.isBlank(version) ? version : "n/a"));
        }

        return solrServer;
    }

    @Override
    public void destroyObject(SolrClient obj) throws Exception {
        obj.shutdown();
    }

    @Override
    public boolean validateObject(SolrClient obj) {
        return true;
    }

    @Override
    public void activateObject(SolrClient obj) throws Exception {
    }

    @Override
    public void passivateObject(SolrClient obj) throws Exception {
    }

}