package com.test.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.test.spout.MongoSpout;

public class VeeTopology {
	public static void main(String[] args){
		try{
			
			//topology definition
			TopologyBuilder topologyBuilder=new TopologyBuilder();
			topologyBuilder.setSpout("mongo-spout", new MongoSpout());

			

			//Configuration
			Config config=new Config();
		//	config.put("mongo", mongo);
			config.setDebug(true);

			//Topology
			config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
			
			//cluster setup
			LocalCluster localCluster=new LocalCluster();
			localCluster.submitTopology("vee", config, topologyBuilder.createTopology());
			
//			Thread.sleep(10000);
//			
//			localCluster.shutdown();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
