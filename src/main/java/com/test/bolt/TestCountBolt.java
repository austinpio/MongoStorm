package com.test.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TestCountBolt implements IRichBolt{

	private Integer id;
	private String name;
	private Map<String, Integer>counters;
	private OutputCollector outputCollector;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.outputCollector=outputCollector;
		this.counters=new HashMap<>();
		this.id=context.getThisTaskId();
		this.name=context.getThisComponentId();
	}

	@Override
	public void execute(Tuple input) {
		String word=input.getString(0);
		
		if(counters.containsKey(word)){
			Integer count=counters.get(word);
			counters.put(word, count+1);
		}
		else{
			counters.put(word, 1);
		}
	}

	@Override
	public void cleanup() {
		System.out.println("-- Word Counter ["+name+"-"+id+"] --"); for(Map.Entry<String, Integer> entry : counters.entrySet()){
			System.out.println(entry.getKey()+": "+entry.getValue());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
