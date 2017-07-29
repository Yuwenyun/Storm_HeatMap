package com.owen.HeatMap;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.code.geocoder.model.LatLng;

public class TimeIntervalExtractor implements IRichBolt
{
	private OutputCollector collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		System.out.println("time interval: " + Thread.currentThread().getName());
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input)
	{
		Long time = input.getLongByField("time");
		LatLng geocode = (LatLng)input.getValueByField("geocode");
		Long timeInterval = time / (15 * 1000);
		this.collector.emit(new Values(timeInterval, geocode));
	}

	@Override
	public void cleanup(){}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("time-interval", "geocode"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}

}
