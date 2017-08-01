package com.owen.HeatMap;

import java.io.IOException;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.code.geocoder.Geocoder;
import com.google.code.geocoder.GeocoderRequestBuilder;
import com.google.code.geocoder.model.GeocodeResponse;
import com.google.code.geocoder.model.GeocoderRequest;
import com.google.code.geocoder.model.GeocoderResult;
import com.google.code.geocoder.model.GeocoderStatus;
import com.google.code.geocoder.model.LatLng;

public class GeocodeLookup implements IRichBolt
{
	private Geocoder coder;
	private OutputCollector collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		// initialize geocoder object
		this.coder = new Geocoder();
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input)
	{
		String address = input.getStringByField("address");
		Long time = input.getLongByField("time");
		
		GeocoderRequest request = new GeocoderRequestBuilder().setAddress(address)
				.setLanguage("en")
				.getGeocoderRequest();
		try
		{
			// call coder API to get the coordinate
			GeocodeResponse response = coder.geocode(request);
			GeocoderStatus status = response.getStatus();
			if(GeocoderStatus.OK.equals(status))
			{
				GeocoderResult firstResult = response.getResults().get(0);
				LatLng latLng = firstResult.getGeometry().getLocation();
				this.collector.emit(new Values(time, latLng));
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup(){}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("time", "geocode"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}
}
