package com.storm.test;

import com.storm.queue.LogItem;
import com.storm.queue.MessageQueue;
import com.storm.queue.MessageQueueFactory;

public class SendMessageTest {

	private MessageQueue<LogItem> messageQueue;
	
	private void init() {
		
		MessageQueueFactory factory = new MessageQueueFactory();
		messageQueue = factory.createProduceMessageQueue();
	}
	
	private void send() {
		
		for(int i=0; i<10; i++) {
			LogItem logItem = new LogItem();
			logItem.setDbName("one");
			logItem.setUrl("one_" + i);
			messageQueue.push(logItem);
		}
		messageQueue.close();
	}
	
	
	
	public static void main(String[] args) {
		
		SendMessageTest test = new SendMessageTest();
		test.init();
		test.send();
	}

}
