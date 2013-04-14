package com.mustardgrain.solr;

import java.util.List;
import java.util.Map;

/**
 * SolrClientMBean provides introspection methods for the {@link SolrClient}.
 */

public interface SolrClientMBean {

    /**
     * Returns a list of all the servers specified by this client. Essentially
     * this is the union of the calls to {@link #getAliveServers()} and
     * {@link #getZombieServers()}.
     * 
     * @return List of all servers
     */

    public List<String> getAllServers();

    /**
     * Returns a list of all the servers specified by this client
     * <em>that are presently considered alive</em> (i.e. reachable).
     * 
     * @return List of alive servers
     */

    public List<String> getAliveServers();

    /**
     * Returns a list of all the servers specified by this client
     * <em>that are presently considered zombies</em> (i.e. unreachable).
     * 
     * @return List of zombie servers
     */

    public List<String> getZombieServers();

    /**
     * Return a mapping of statistics. The outer map key is the server URL and
     * the inner map's key is the statistic and the associated value.
     * <p/>
     * The nested map is there because I gave up fighting with JMX/RMI class
     * loading the {@link SolrStats} class directly.
     * 
     * @return Mapping of URLs to a map of statistics, as defined in
     *         {@link SolrStats}
     */

    public Map<String, Map<String, Number>> getStats();

}
