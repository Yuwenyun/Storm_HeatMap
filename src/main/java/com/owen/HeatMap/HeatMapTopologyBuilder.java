package com.owen.HeatMap;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class HeatMapTopologyBuilder
{
	public static StormTopology build()
	{
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("checkins", new Checkins(), 4);
		builder.setBolt("geocode-lookup", new GeocodeLookup(), 8).setNumTasks(64)
			.shuffleGrouping("checkins");
		builder.setBolt("time-interval-extractor", new TimeIntervalExtractor(), 4)
			.shuffleGrouping("geocode-lookup");
		builder.setBolt("heat-map-builder", new HeatMapBuilder(), 4)
			.fieldsGrouping("time-interval-extractor", new Fields("time-interval"));
		builder.setBolt("persister", new Persister(), 1).setNumTasks(4)
			.shuffleGrouping("heat-map-builder");
		return builder.createTopology();
	}
}
