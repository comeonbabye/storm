package com;

import java.sql.ResultSet;
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
	private HashMap<String, Item> pendingToSave = new HashMap<String, Item>();
	
	private OutputCollector collector;
	
	private DBUtil db;
	
	private Date today = new Date();
	private String dateStr = DateUtil.dateToString(today, DateUtil.DATE_FORMAT_5);
	
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

					Item item = pendings.get(key);
					if (item.getId() != null && item.getId() > 0) { // update
						String sql = "update db_count set db_count=" + item.getDbCount() + " where db_key=" + item.getDbKey();
						db.executeUpdate(sql);
					} else {// save
						String sql = "insert into db_count (db_key, db_name, db_date, db_count) values (?,?,?,?)";
						db.executeUpdate2(sql, key, item.getDbName(), item.getDbDate(), item.getDbCount());
					}
				}
			}
		};
		timer = new Timer("save fetch count");
		timer.scheduleAtFixedRate(t, 10000L, 10000L);
	}
	
	private synchronized void initDate() {
		today = new Date();
		dateStr = DateUtil.dateToString(today, DateUtil.DATE_FORMAT_5);
	}
	
	private String buildKey(String dbName) {
		return "dbName_" + dateStr;
	}
	
	private Item getItem(String dbName) {
		String key = buildKey(dbName);
		Item item = counter.get(key);
		if(item == null) {
			ResultSet rs = db.executeQuery("select id, db_key, db_name, db_date, db_count from db_count where db_key=" + key, 1);
			
			item = new Item();
			
			try {
				if(rs.next()) {
					item.setId(rs.getLong("id"));
					item.setDbKey(rs.getString("db_key"));
					item.setDbDate(rs.getDate("db_date"));
					item.setDbName(rs.getString("db_name"));
					item.setDbCount(rs.getInt("db_count"));
				} else {
					item.setId(-1L);
					item.setDbCount(0);
					item.setDbKey(key);
					item.setDbDate(today);
					item.setDbName(dbName);
				}
			} catch (Exception e) {
				e.printStackTrace();
				item.setId(-1L);
				item.setDbCount(0);
				item.setDbKey(key);
				item.setDbDate(today);
				item.setDbName(dbName);
			}
		}
		
		return item;
	}
	
	private void saveItem(String dbName, Item item) {
		String key = buildKey(dbName);
		counter.put(key , item);
		synchronized (pendingToSave) {
			pendingToSave.put(key, item);	
		}
	}
	
	private void count(String dbName, String url) {
		Item item = getItem(dbName);
		item.setDbCount(item.getDbCount() + 1);
		
		saveItem(dbName, item);
	}
	
	
	@Override
	public void execute(Tuple input) {

		List<Object> list = input.getValues();
		
		System.out.println("dbName>>>>>>>:" + list.get(0));
		System.out.println("url>>>>>>>:" + list.get(1));
		
		count((String)list.get(0), (String)list.get(1));
		
		collector.ack(input);
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
		startDownloaderThread();
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
