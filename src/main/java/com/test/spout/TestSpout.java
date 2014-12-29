package com.test.spout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TestSpout implements IRichSpout{

	private TopologyContext context;
	private FileReader fileReader;
	private SpoutOutputCollector collector; 

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.context=context;
		try {
			this.fileReader=new FileReader(conf.get("filename").toString());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.collector=collector;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void nextTuple() {

		//start to read from file
		String str;
		BufferedReader bufferedReader=new BufferedReader(fileReader);
		try{
			while((str=bufferedReader.readLine())!=null){
				this.collector.emit(new Values(str),str);
			}
		}
		catch(Exception e){
			throw new RuntimeException("error reading tuple ",e);
		}
		finally{
			return;
		}
	}

	@Override
	public void ack(Object msgId) {
		System.err.println("ack "+msgId.toString());
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
