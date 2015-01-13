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
	
	private BufferedReader br;
	
	private boolean complete = false;
	
	@Override
	public void nextTuple() {
		
		try {
			
			if(complete) {
				Thread.sleep(50); //睡眠1毫秒，降低处理器的负载
				return;
			}
			
			if(br != null) {
				String line = null;
				while((line = br.readLine()) != null) {
					collector.emit(new Values(count.getAndIncrement(), line));
				}
			}
			
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			complete = true;
		}
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	
		System.out.println("打印日志信息>>>>>>>>>>>>>\n" + conf);
		this.collector = collector;
		
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
			
			br = new BufferedReader(new FileReader(file));
			 
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("打开文件是吧>>>>>>>>>>>>>\n" + conf);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		/**
		 * 检查Spout和Bolt代码中的declareOutputFields方法
		 * declare的Field数量 等于 collector.emit数量
		 */
		declarer.declare(new Fields("id", "info"));

	}

	@Override
	public void ack(Object msgId) {
		super.ack(msgId);
		System.out.println("成功处理消息:[" + msgId + "]");
	}

	@Override
	public void fail(Object msgId) {
		super.fail(msgId);
		System.out.println("失败处理消息:[" + msgId + "]");
	}

	@Override
	public void close() {
		
		super.close();
		
		if(br != null) {
			try {
				br.close();
				br = null;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}

}
