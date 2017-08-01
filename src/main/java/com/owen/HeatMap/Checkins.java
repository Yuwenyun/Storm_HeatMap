package com.owen.HeatMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * This is an unreliable demo project for storm framework which doesn't keep
 * track of every tuple and guarantees no fault tolerant
 * 
 * @author yuwenyun
 */
public class Checkins implements IRichSpout
{
	private List<String> checkins;
	private int nextEmitIndex;
	private SpoutOutputCollector collector;
	
	public void ack(Object arg0){}
	public void activate(){}
	public void close(){}
	public void deactivate(){}
	public void fail(Object arg0){}

	public void nextTuple()
	{
		String checkin = checkins.get(this.nextEmitIndex);
		String[] parts = checkin.split(",");
		Long time = Long.valueOf(parts[0]);
		String address = parts[1];
		// emit the tuple to bolt
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
			// read data with format of "1382904793783, 287 Hudson St New York NY 10013"
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
		// one tuple containing two fields with two names
		arg0.declare(new Fields("time", "address"));
	}

	public Map getComponentConfiguration()
	{
		return null;
	}
}
