package com.owen.HeatMap;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.code.geocoder.model.LatLng;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class Persister implements IRichBolt
{
	private DBCollection hotzones;
	private ObjectMapper mapper;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		try
		{
			MongoClient mongo = new MongoClient("localhost", 27017);
			DB db = mongo.getDB("local");
			hotzones = db.getCollection("hotzones");
		}
		catch (UnknownHostException e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input)
	{
		Long timeInterval = input.getLongByField("time-interval");
		List<LatLng> hzs = (List<LatLng>)input.getValueByField("hotzones");
		List<String> hotzones = asListOfString(hzs);
		try
		{
			String key = "checkins-" + timeInterval;
			String value = mapper.writeValueAsString(hotzones);
			BasicDBObject obj = new BasicDBObject();
			obj.put(key, value);
			this.hotzones.insert(obj);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private List<String> asListOfString(List<LatLng> hzs)
	{
		List<String> hotzones = new ArrayList<String>(hzs.size());
		for(LatLng hz : hzs)
		{
			hotzones.add(hz.toUrlValue());
		}
		return hotzones;
	}

	@Override
	public void cleanup(){}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
	}

	@Override
	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}

}
