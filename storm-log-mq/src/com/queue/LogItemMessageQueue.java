package com.queue;

import com.queue.JmsQueue.QueueRole;


public class LogItemMessageQueue implements MessageQueue<LogItem> {

	private MessageQueueConfig config;
	
    private JmsQueue<LogItem> queue;
    
    private QueueRole role;
    
    public LogItemMessageQueue(MessageQueueConfig config, QueueRole role) {
    	
    	this.role = role;
    	this.config = config;
    	
    	this.init();
    }


    private void init() {

    	queue = new JmsQueue<LogItem>(config.getQueueName(), config.getBrokerUrl(), new LogItemConverter(), role);
             
    	queue.init();
    }


	@Override
	public void push(LogItem e) {
		
		queue.put(e);
		
	}


	@Override
	public LogItem take() throws InterruptedException {

		return queue.take();
	}


	@Override
	public LogItem take(long timeout) throws InterruptedException {
		
		return queue.take(timeout);
	}


	@Override
	public void close() {
		
		queue.close();
		
	}
 
}
