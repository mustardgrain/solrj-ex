package com.mustardgrain.solr;

import java.net.InetAddress;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpParams;
import org.apache.solr.client.solrj.impl.HttpClientUtil;

public class SolrClientFactory implements PoolableObjectFactory<SolrClient> {

    private final String version;

    private final String userAgent;

    private final List<String> urls;

    private final int connectTimeout;

    private final int soTimeout;

    private final boolean staleConnectionCheck;

    private final int statsInterval;

    private final boolean disableConnectonHeader;

    public SolrClientFactory(String version,
                             String userAgent,
                             List<String> urls,
                             int connectTimeout,
                             int soTimeout,
                             boolean staleConnectionCheck,
                             int statsInterval,
                             boolean disableConnectonHeader) {
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