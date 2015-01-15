package com.storm.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.storm.queue.LogItem;
import com.storm.queue.MessageQueue;
import com.storm.queue.MessageQueueFactory;

public class SendMessageTest {

	private MessageQueue<LogItem> messageQueue;
	
	private void init() {
		
		MessageQueueFactory factory = new MessageQueueFactory();
		messageQueue = factory.createProduceMessageQueue();
	}
	
	public void send() {
		for(int i=0; i<100; i++) {
			LogItem logItem = new LogItem();
			logItem.setDbName("one");
			logItem.setUrl("one_" + i);
			messageQueue.push(logItem);
		}
			 
		messageQueue.close();
	}
	
	public void send2() {
		try {
			for(int j=0; j<100; j++) {
				for(int i=0; i<100; i++) {
					LogItem logItem = new LogItem();
					logItem.setDbName("one");
					logItem.setUrl("one_" + i);
					messageQueue.push(logItem);
				}
				Thread.sleep(1000);
			}
			
			messageQueue.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void send3() {
		
		ExecutorService es = Executors.newFixedThreadPool(4);
		
		final String[] dbs = new String[]{"one", "two", "three", "four"};
		
		final AtomicInteger count = new AtomicInteger(0);
		
		for(int i=0; i<dbs.length; i++) {
			
			final int index = i;
			
			es.submit(new Runnable() {

				@Override
				public void run() {
					try {
						for(int j=0; j<10000; j++) {
							LogItem logItem = new LogItem();
							logItem.setDbName(dbs[index]);
							logItem.setUrl(dbs[index] + j);
							messageQueue.push(logItem);
							//Thread.sleep(100 + index);
						}
						
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						count.incrementAndGet();
					}
				}
			});
		}
		
		while(count.get() != dbs.length) {
			try {
				System.out.println("count:" + count);
				Thread.sleep(1000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		messageQueue.close();
		es.shutdownNow();
		
	}
	
	public static void main(String[] args) {
		
		SendMessageTest test = new SendMessageTest();
		test.init();
		test.send3();
	}

}
