package com.test.topology;

import com.test.bolt.TestBolt;
import com.test.bolt.TestCountBolt;
import com.test.spout.TestSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TestTopology {

	public static void main(String[] args){
		try{
			TopologyBuilder topologyBuilder=new TopologyBuilder();
			topologyBuilder.setSpout("test-spout", new TestSpout());
			topologyBuilder.setBolt("test-bolt", new TestBolt()).shuffleGrouping("test-spout");
			topologyBuilder.setBolt("test-count-bolt", new TestCountBolt()).shuffleGrouping("test-bolt");
			
			//configuration
			Config config=new Config();
			config.put("filename", "/Users/austinpio/Downloads/examples-ch02-getting_started-master/src/main/resources/words.txt");
			config.setDebug(true);
			
			//Topology
			config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
			
			LocalCluster localCluster=new LocalCluster();
			localCluster.submitTopology("hello world", config, topologyBuilder.createTopology());
			Thread.sleep(1000);
			
			localCluster.shutdown();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
