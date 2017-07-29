package com.owen.HeatMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class Checkins implements IRichSpout
{
	private List<String> checkins;
	private int nextEmitIndex;
	private SpoutOutputCollector collector;
	
	public void ack(Object arg0)
	{
	}

	public void activate()
	{
	}

	public void close()
	{
	}

	public void deactivate()
	{
	}

	public void fail(Object arg0)
	{
	}

	public void nextTuple()
	{
		String checkin = checkins.get(this.nextEmitIndex);
		String[] parts = checkin.split(",");
		Long time = Long.valueOf(parts[0]);
		String address = parts[1];
		this.collector.emit(new Values(time, address));
		this.nextEmitIndex = (this.nextEmitIndex + 1) % this.checkins.size();
	}

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2)
	{
		System.out.println("checkins: " + Thread.currentThread().getName());
		this.collector = arg2;
		this.nextEmitIndex = 0;
		
		try
		{
			BufferedReader stdIn = new BufferedReader(new FileReader(new File("checkins.txt")));
			String line = "";
			while((line = stdIn.readLine()) != null)
				checkins.add(line);
		}
		catch(IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0)
	{
		arg0.declare(new Fields("time", "address"));
	}

	public Map getComponentConfiguration()
	{
		return null;
	}

}
