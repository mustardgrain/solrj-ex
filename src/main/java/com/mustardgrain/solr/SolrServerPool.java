package com.mustardgrain.solr;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.solr.client.solrj.SolrServer;

public class SolrServerPool<T extends SolrServer> {

    private ObjectPool<T> pool;

    public SolrServerPool(PoolableObjectFactory<T> factory) {
        this(factory, new GenericObjectPool.Config());
    }

    public SolrServerPool(PoolableObjectFactory<T> factory, GenericObjectPool.Config config) {
        pool = new GenericObjectPool<T>(factory, config);
    }

    public T get() throws Exception {
        return pool.borrowObject();
    }

    public void put(T solrServer) throws Exception {
        pool.returnObject(solrServer);
    }

    public void close() throws Exception {
        pool.close();
    }

}
