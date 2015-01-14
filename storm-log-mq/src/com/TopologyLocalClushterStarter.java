package com;

import com.util.Constants;

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
		builder.setBolt("count-log", new CountBolt(), 1).shuffleGrouping("read-log");
		
		
		Config conf = new Config();
		//conf.setDebug(false);
		//conf.setNumWorkers(1);
        //conf.setMaxSpoutPending(5000);
		conf.put(Constants.DB_HOST, "42.96.168.163");
		conf.put(Constants.DB_PORT, "3306");
		conf.put(Constants.DB_NAME, "illidan_one");
		conf.put(Constants.DB_USER_NAME, "root");
		conf.put(Constants.DB_PASSWORD, "123456");
		
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("log", conf, builder.createTopology());
		
	}

}
