package com.test.spout;

import java.util.Map;

import org.bson.types.BSONTimestamp;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;

public class MongoSpout implements IRichSpout{

	private TopologyContext context;
	private SpoutOutputCollector collector;
	private Mongo mongo;
	private DB mongoDB;

	private DBObject query;
	private DBObject sortByQuery;
	private DBCursor cursor;
	
	private BSONTimestamp lastTimeStamp;

	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void nextTuple() {
		while(this.cursor.hasNext()){
			DBObject dbObject=this.cursor.next();
			if(dbObject!=null ){
				System.err.println(dbObject.toString());
			}
		}
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.context=context;
		try{
			// Mongo client settings
			MongoClientOptions mongoClientOptions=new MongoClientOptions.Builder()
			.connectionsPerHost(100)
			.cursorFinalizerEnabled(true)
			.build();

			//mongo client to be shared across the topology members
			this.mongo=new MongoClient("54.169.120.190",mongoClientOptions);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		//setting up mongo db
		//this.mongo=((Mongo) conf.get("mongoclient"));

		this.mongoDB=this.mongo.getDB("local");
		
		//get last ts
		DBCursor lastCursor=this.mongoDB.getCollection("oplog.rs").find().sort(new BasicDBObject("$natural",-1)).limit(1);
		DBObject lastvalue=lastCursor.next();
		System.err.println(lastvalue.toString());
		lastTimeStamp=(BSONTimestamp) lastvalue.get("ts");
		
		//tailable cursor
		this.query=new BasicDBObject("ts",new BasicDBObject("$gt",lastTimeStamp));
		this.sortByQuery=new BasicDBObject("$natural", 1);
		this.cursor=this.mongoDB.getCollection("oplog.rs")
				.find(this.query)
				.sort(this.sortByQuery)
				//.limit(1)
				.addOption(Bytes.QUERYOPTION_TAILABLE)
				.addOption(Bytes.QUERYOPTION_AWAITDATA)
				.addOption(Bytes.QUERYOPTION_NOTIMEOUT);

		this.collector=collector;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
