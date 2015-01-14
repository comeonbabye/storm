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

import com.entitiy.Item;
import com.repository.DateUtil;
import com.repository.ItemRepository;
import com.util.Constants;
import com.util.DBUtil;

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
	
	private ItemRepository itemRepository;
	
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
						String sql = "update db_count set db_count=?, update_time=? where db_key=?";
						itemRepository.saveOrUpdate(sql, item.getDbCount(), new Date(), item.getDbKey());
					} else if(isExsitsItem(item)) {
						String sql = "update db_count set db_count=?, update_time=? where db_key=?";
						itemRepository.saveOrUpdate(sql, item.getDbCount(), new Date(), item.getDbKey());
						Long id = getItemId(item);
						item.setId(id);
					} else {// save
						String sql = "insert into db_count (db_key, db_name, db_date, db_count, update_time) values (?,?,?,?,?)";
						itemRepository.saveOrUpdate(sql, key, item.getDbName(), item.getDbDate(), item.getDbCount(), new Date());
						Long id = getItemId(item);
						item.setId(id);
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
		return dbName + "_" + dateStr;
	}
	
	private Long getItemId(Item item) {
		return getItemId(item.getDbName());
	}
	
	private Long getItemId(String dbName) {
		
		long id = -1;
		
		if(dbName == null) return id;
		
		String key = buildKey(dbName);
		Item item = counter.get(key);
		if(item != null && item.getId() != null && item.getId() > 0) {
			return item.getId();
		} else {
			 return itemRepository.getItemId(key);
		}
	}
	
	private boolean isExsitsItem(Item item) {
		return isExsitsItem(item.getDbName());
	}
	
	private boolean isExsitsItem(String dbName) {
		
		if(dbName == null) return false;
		
		String key = buildKey(dbName);
		Item item = counter.get(key);
		if(item != null && item.getId() != null && item.getId() > 0) {
			return true;
		} else {
			return itemRepository.isExsitsItem(key);
		}
		
	}
	
	private Item getItem(String dbName) {
		String key = buildKey(dbName);
		Item item = counter.get(key);
		if(item == null) {
			item = itemRepository.getItem(dbName, key, today);
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
		DBUtil db = new DBUtil((String)stormConf.get(Constants.DB_HOST), 
				(String)stormConf.get(Constants.DB_PORT), 
				(String)stormConf.get(Constants.DB_NAME), 
				(String)stormConf.get(Constants.DB_USER_NAME), 
				(String)stormConf.get(Constants.DB_PASSWORD));
		
		itemRepository = new ItemRepository(db);
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
