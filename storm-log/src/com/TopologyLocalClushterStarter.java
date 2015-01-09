package com;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TopologyLocalClushterStarter {

	/**
	 * 功能:
	 * 作者: tony.he
	 * 创建日期:2015-1-9
	 * 修改者: mender
	 * 修改日期: modifydate
	 * @param args
	 */
	public static void main(String[] args) {
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("read-log", new LogFetchSpout(), 1);
		builder.setBolt("out-log", new CountBolt(), 1).shuffleGrouping("read-log");
		
		
		Config conf = new Config();
		conf.put("message", "hi, this is a test");
		conf.put("file.path", "F:\\data\\ec2\\logs\\dianshang.log");
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		
	}

}
