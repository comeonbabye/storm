package com;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class TopologyProductClushterStarter {

	public static void main(String[] args) {
		
		if(args == null || args.length != 1) {
			throw new RuntimeException("need topology name");
		}
		
		try {
			TopologyBuilder builder = new TopologyBuilder();
			
			builder.setSpout("read-log", new LogFetchSpout(), 1);
			builder.setBolt("count-log", new CountBolt(), 1).shuffleGrouping("read-log");
			
			Config conf = new Config();
			conf.setDebug(true);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			
			System.out.println("提交任务["+ args[0] + "]结束");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
