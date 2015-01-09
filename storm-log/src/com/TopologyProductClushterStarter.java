package com;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class TopologyProductClushterStarter {

	public static void main(String[] args) {
		
		if(args == null || args.length != 2) {
			throw new RuntimeException("need topology name and file path");
		}
		
		try {
			TopologyBuilder builder = new TopologyBuilder();
			
			builder.setSpout("read-log", new LogFetchSpout(), 1);
			builder.setBolt("out-log", new CountBolt(), 1).shuffleGrouping("read-log");
			
			Config conf = new Config();
			conf.put("message", "hi, this is a test");
			conf.put("file.path", args[1]);
			
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
