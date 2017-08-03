package com.owen.HeatMap;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
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
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.STORM_LOCAL_HOSTNAME, "localhost");
        config.setNumWorkers(2);
        config.setMessageTimeoutSecs(60);
        
        try
		{
			StormSubmitter.submitTopology("heat-map-topology", config, HeatMapTopologyBuilder.build());
		}
		catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e)
		{
			e.printStackTrace();
		}
        
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("heat-map-topology", config, HeatMapTopologyBuilder.build());
//        
//        Utils.sleep(100_000);
//        cluster.killTopology("heat-map-topology");
//        cluster.shutdown();
    }
}
