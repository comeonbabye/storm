package com;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.storm.util.Constants;

public class TopologyLocalClushterStarter {

	public static void main(String[] args) {
		
		
		long start = System.currentTimeMillis();
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("read-log", new LogFetchSpout(), 4);//通过设置parallelism来指定执行spout/bolt的线程数量
		//builder.setBolt("count-log", new CountBolt(), 4).shuffleGrouping("read-log");//随机平均分配，适合无状态处理
		builder.setBolt("count-log", new CountBolt(), 4).fieldsGrouping("read-log", new Fields("dbName"));//通过字段的值进行分组，这样就不会出现不同的字段发送到不同的任务中
		
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.put("topology.acker.executors", 4);
		//conf.setNumWorkers(1);
        //conf.setMaxSpoutPending(5000);
		conf.put(Constants.DB_HOST, "42.96.168.163");
		conf.put(Constants.DB_PORT, "3306");
		conf.put(Constants.DB_NAME, "illidan_one");
		conf.put(Constants.DB_USER_NAME, "root");
		conf.put(Constants.DB_PASSWORD, "123456");
		conf.setMaxTaskParallelism(20);
		conf.setNumWorkers(4);//指定一个storm集群中执行topolgy的进程数量
		conf.setNumAckers(4);
		conf.put("topology.max.spout.pending", 100000);
		
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("log", conf, builder.createTopology());
		
		long end = System.currentTimeMillis();
		
		System.out.println("elapsed time:" + (end - start));
		
	}

}

