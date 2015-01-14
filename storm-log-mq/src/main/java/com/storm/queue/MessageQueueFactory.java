package com.storm.queue;

import com.storm.queue.JmsQueue.QueueRole;

public class MessageQueueFactory {

	private MessageQueueConfig config;
	
	public MessageQueueFactory() {
		this.config = new MessageQueueConfig();
	}
	
	public MessageQueueFactory(MessageQueueConfig config) {
		this.config = config;
	}
	
	
	public MessageQueue<LogItem> createProduceMessageQueue() {
		return new LogItemMessageQueue(config, QueueRole.Producer);
	}
	
	public MessageQueue<LogItem> createConsumerMessageQueue() {
		return new LogItemMessageQueue(config, QueueRole.Consumer);
	}
	
}
