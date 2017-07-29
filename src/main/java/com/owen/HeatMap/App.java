package com.owen.HeatMap;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.utils.Utils;

/**
 * Hello world!
 *
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
