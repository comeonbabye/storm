package com;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.storm.queue.LogItem;
import com.storm.queue.MessageQueue;
import com.storm.queue.MessageQueueFactory;

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
	
	private MessageQueue<LogItem> messageQueue;
	
	
	@Override
	public void nextTuple() {
		
		try {
			
			LogItem item = messageQueue.take(1000L);
			
			if(item != null) {
				
				System.out.println(item);
				
				collector.emit(new Values(item.getDbName(), item.getUrl()), count.getAndIncrement());
			}
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	
		System.out.println(">>>>>>>>>>>>>\n" + conf);
		this.collector = collector;
		MessageQueueFactory factory = new MessageQueueFactory();
		messageQueue = factory.createConsumerMessageQueue();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		/**
		 * 检查Spout和Bolt代码中的declareOutputFields方法
		 * declare的Field数量 等于 collector.emit数量
		 */
		declarer.declare(new Fields("dbName", "url"));

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
		
		messageQueue.close();
		
	}

}
