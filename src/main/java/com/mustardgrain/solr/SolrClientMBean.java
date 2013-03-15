package com.mustardgrain.solr;

import java.util.List;
import java.util.Map;

public interface SolrClientMBean {

    public List<String> getAllServers();

    public List<String> getAliveServers();

    public List<String> getZombieServers();

    public Map<String, Map<String, Number>> getStats();

}
