package com.owen.HeatMap;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class HeatMapTopologyBuilder
{
	public static StormTopology build()
	{
		TopologyBuilder builder = new TopologyBuilder();
		
		// 4 is a parallelism hint, it tells storm to create 4 spouts
		builder.setSpout("checkins", new Checkins(), 4);
		// 8 is the executor thread in JVM and task is the instance of spout or bolt running
		// in the thread, by default, if we set executor thread 8 then spout or bolt instance
		// number would be 8, we can override the task with setNumTasks()
		builder.setBolt("geocode-lookup", new GeocodeLookup(), 8).setNumTasks(64)
			.shuffleGrouping("checkins");
		builder.setBolt("time-interval-extractor", new TimeIntervalExtractor(), 4)
			.shuffleGrouping("geocode-lookup");
		// field grouping means tuples with the same value are emitted to the same instance
		// of bolt, the same task.
		builder.setBolt("heat-map-builder", new HeatMapBuilder(), 4)
			.fieldsGrouping("time-interval-extractor", new Fields("time-interval"));
		builder.setBolt("persister", new Persister(), 1).setNumTasks(4)
			.shuffleGrouping("heat-map-builder");
		return builder.createTopology();
	}
}
