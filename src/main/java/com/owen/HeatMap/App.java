package com.owen.HeatMap;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.utils.Utils;

/**
 * This is a demo projects showing how to create a storm project with parallelism
 * hint to storm. Main points here are:
 *   1 we can hint number of executor threads when building the topology
 *   2 we can override the number of tasks each thread should maintain
 *   3 use tick tuple to trigger some action periodically
 */
public class App 
{
    public static void main( String[] args )
    {
        Config config = new Config();
        config.setDebug(true);
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("heat-map-topology", config, HeatMapTopologyBuilder.build());
        
        Utils.sleep(100_000);
        cluster.killTopology("heat-map-topology");
        cluster.shutdown();
    }
}
