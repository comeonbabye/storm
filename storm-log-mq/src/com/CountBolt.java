package com;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * 
 * 创建日期:2015-1-9
 * Description：对日志进行分析统计
 * @author tony.he
 * @version 1.0
 */
@SuppressWarnings("serial")
public class CountBolt implements IRichBolt {

	//db -> COUNT
	private HashMap<String, Item> counter = new HashMap<String, Item>();
	HashMap<String, Item> pendingToSave = new HashMap<String, Item>();
	
	private OutputCollector collector;
	
	private DBUtil db;
	
	private Date today = new Date();
	private String dateStr = DateUtil.dateToString(today, DateUtil.DATE_FORMAT_2);
	
	private Timer timer;
	
	private void startDownloaderThread() {
		TimerTask t = new TimerTask() {
			@Override
			public void run() {
				
				initDate();
				
				HashMap<String, Item> pendings;
				synchronized (pendingToSave) {
					pendings = pendingToSave;
					pendingToSave = new HashMap<String, Item>();
				}
				
				for (String key : pendings.keySet()) {
					String[] keys = key.split(":");
					String product = keys[0];
					String categ = keys[1];
					Integer count = pendings.get(key);
					jedis.hset(buildRedisKey(product), categ, count.toString());
				}
			}
		};
		timer = new Timer("save fetch count");
		timer.scheduleAtFixedRate(t, 10000L, 10000L);
	}
	
	private synchronized void initDate() {
		today = new Date();
		dateStr = DateUtil.dateToString(today, DateUtil.DATE_FORMAT_2);
	}
	
	private int count(String dbName, String url) {
		int count = getProductCategoryCount(categ, product);
		count ++;
		storeProductCategoryCount(categ, product, count);
		return count;
	}
	
	
	@Override
	public void execute(Tuple input) {

		List<Object> list = input.getValues();
		
		System.out.println("dbName>>>>>>>:" + list.get(0));
		System.out.println("url>>>>>>>:" + list.get(1));
		
		count((String)list.get(0), (String)list.get(1));
		
	}
	

	@SuppressWarnings("unchecked")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		
		this.collector = collector;
		db = new DBUtil((String)stormConf.get(Constants.DB_HOST), 
				(String)stormConf.get(Constants.DB_PORT), 
				(String)stormConf.get(Constants.DB_NAME), 
				(String)stormConf.get(Constants.DB_USER_NAME), 
				(String)stormConf.get(Constants.DB_PASSWORD));
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("id", "info"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
	}

}
