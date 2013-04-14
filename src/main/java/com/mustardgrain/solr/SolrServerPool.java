package com.mustardgrain.solr;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.solr.client.solrj.SolrServer;

/**
 * A wrapper around the Apache Commons {@link GenericObjectPool}.
 * <p/>
 * Usage follows the standard pattern of {@link #get()}, use, then
 * {@link #put(SolrServer)}. When the application is shutting down (or otherwise
 * no longer using the Solr connections) the method {@link #close()} should be
 * invoked.
 * 
 * @param <T> Type that extends {@link SolrServer}, expected to be
 *        {@link SolrClient}
 * @see SolrClientFactory
 */

public class SolrServerPool<T extends SolrServer> {

    private final ObjectPool<T> pool;

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
