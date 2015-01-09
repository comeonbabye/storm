package com;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 
 * 创建日期:2015-1-9
 * Description：读取日志
 * @author tony.he
 * @version 1.0
 */
@SuppressWarnings("serial")
public class LogFetchSpout extends BaseRichSpout {

	private static final AtomicInteger count = new AtomicInteger(1);
	
	private SpoutOutputCollector collector;
	
	@SuppressWarnings("unchecked")
	private Map conf;
	
	@Override
	public void nextTuple() {
		
		try {
			File file = null;
			
			if(conf.containsKey("file.path") && conf.get("file.path") != null && !"".equalsIgnoreCase(conf.get("file.path").toString().trim())) {
				file = new File((String)conf.get("file.path"));
			} else {
				file = new File("/opt/illidan-realtime/logs/illidan.log");
			}
			
			if(!file.exists()) {
				
				System.out.println("文件不存在！直接返回，不做任何处理");
				return;
			}
			
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = null;
			while((line = br.readLine()) != null) {
				collector.emit(new Values(count.getAndIncrement(), line));
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	
		System.out.println("打印日志信息>>>>>>>>>>>>>\n" + conf);
		this.conf = conf;
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		/**
		 * 检查Spout和Bolt代码中的declareOutputFields方法
		 * declare的Field数量 等于 collector.emit数量
		 */
		declarer.declare(new Fields("id", "info"));

	}

}
