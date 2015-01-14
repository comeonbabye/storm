package com.storm.queue;

public class MessageQueueConfig {

	private String brokerUrl = "tcp://192.168.2.14:61616";
    private String queueName = "status_queue";
	public String getBrokerUrl() {
		return brokerUrl;
	}
	public void setBrokerUrl(String brokerUrl) {
		this.brokerUrl = brokerUrl;
	}
	public String getQueueName() {
		return queueName;
	}
	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}
}
