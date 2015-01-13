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
			conf.setDebug(true);
			/*conf.setNumWorkers(3);
	        conf.setMaxSpoutPending(5000);*/
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			
			System.out.println("提交任务["+ args[0] + "]结束");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
