package com;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.storm.util.Constants;

public class TopologyProductClushterStarter {

	public static void main(String[] args) {
		
		if(args == null || args.length < 1) {
			throw new RuntimeException("need topology name");
		}
		
		try {
			TopologyBuilder builder = new TopologyBuilder();
			
			int boltCount = 1;
			if(args.length > 1) {
				boltCount = Integer.parseInt(args[1]);
			}
			
			builder.setSpout("read-log", new LogFetchSpout(), 1);
			//builder.setBolt("count-log", new CountBolt(), boltCount).shuffleGrouping("read-log");
			builder.setBolt("count-log", new CountBolt(), boltCount).fieldsGrouping("read-log", new Fields("dbName"));
			
			Config conf = new Config();
			conf.setDebug(true);
			conf.put(Constants.DB_HOST, "42.96.168.163");
			conf.put(Constants.DB_PORT, "3306");
			conf.put(Constants.DB_NAME, "illidan_one");
			conf.put(Constants.DB_USER_NAME, "root");
			conf.put(Constants.DB_PASSWORD, "123456");
			
			
			
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			
			System.out.println("提交任务["+ args[0] + "]结束");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
