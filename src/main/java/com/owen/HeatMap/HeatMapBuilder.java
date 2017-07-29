package com.owen.HeatMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.code.geocoder.model.LatLng;

public class HeatMapBuilder implements IRichBolt
{
	private Map<Long, List<LatLng>> heatmaps;
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.heatmaps = new HashMap<Long, List<LatLng>>();
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input)
	{
		if(isTickTuple(input))
		{
			Long now = System.currentTimeMillis();
			Long emitUpToTimeInterval = selectTimeInterval(now);
			for(Long timeInterval : this.heatmaps.keySet())
			{
				if(timeInterval <= emitUpToTimeInterval)
				{
					List<LatLng> hotzones = heatmaps.remove(timeInterval);
					this.collector.emit(new Values(timeInterval, hotzones));
				}
			}
		}
		else
		{
			Long timeInterval = input.getLongByField("time-interval");
			LatLng geocode = (LatLng)input.getValueByField("geocode");
			List<LatLng> checkins = getCheckinsForInterval(timeInterval);
			checkins.add(geocode);
		}
	}

	private boolean isTickTuple(Tuple tuple)
	{
		String sourceComponent = tuple.getSourceComponent();
		String sourceStreamId = tuple.getSourceStreamId();
		return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID) &&
				sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
	}

	private Long selectTimeInterval(Long time)
	{
		return time / (15 * 1000);
	}

	private List<LatLng> getCheckinsForInterval(Long timeInterval)
	{
		List<LatLng> hotzones = heatmaps.get(timeInterval);
		if(hotzones == null)
		{
			hotzones = new ArrayList<LatLng>();
			heatmaps.put(timeInterval, hotzones);
		}
		return hotzones;
	}

	@Override
	public void cleanup(){}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("time-interval", "hotzones"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration()
	{
		Config config = new Config();
		config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
		return config;
	}

}
