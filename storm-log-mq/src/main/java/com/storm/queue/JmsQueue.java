package com.storm.queue;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsQueue<E> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private String queueName;

    private MessageConverter<E> converter;

    private Destination destination = null;

    private Connection connection = null;

    private Session session = null;

    private MessageProducer producer = null;

    private MessageConsumer consumer = null;

    private JmsListener<E> jmsListener = null;

    private QueueRole role;

    private String brokerUrl;

    public JmsQueue(String queueName, String brokerUrl, MessageConverter<E> converter, QueueRole role) {

        this.queueName = queueName;

        this.brokerUrl = brokerUrl;

        this.converter = converter;

        this.role = role;

    }

    public void init() {

        try {
        	
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_USER, ActiveMQConnection.DEFAULT_PASSWORD, brokerUrl);

            connection = connectionFactory.createConnection();

            connection.start();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            destination = session.createQueue(this.queueName);

            if (this.role.equals(QueueRole.Producer)) {

                producer = session.createProducer(destination);

                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            } else {

                consumer = session.createConsumer(destination);

                this.jmsListener = new JmsListener<E>(this.converter);

                consumer.setMessageListener(jmsListener);

            }

        } catch (JMSException e) {

            logger.error("init jms queue error.", e);

            throw new RuntimeException(e);
        }

    }

    public void put(E item) {

        try {

            Message msg = this.converter.createMsg(session, item);

            if (msg != null) {

                this.producer.send(msg);

            }

        } catch (JMSException e) {

            logger.error("send msg error.msg:{}", item, e);

            e.printStackTrace();
        }
    }

    public E take() throws InterruptedException {

        return this.jmsListener.take();
    }
    
    public E take(long timeout) throws InterruptedException {

        return this.jmsListener.take(timeout);
    }

    public void close() {
    	
    	
    	try {
    		if (consumer != null) {
                consumer.close();
            }
    	} catch (Exception e) {
    		logger.error("close consumer error.", e);
    	}
    	
    	try {
    		if (producer != null) {
    			producer.close();
            }
    	} catch (Exception e) {
    		logger.error("close producer error.", e);
    	}
    	
    	try {
    		if (session != null) {
    			session.close();
            }
    	} catch (Exception e) {
    		logger.error("close session error.", e);
    	}

        try {

            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            logger.error("close connection error.", e);
        }

    }

    /**
     * Enum description
     *
     */
    public static enum QueueRole { Producer, Consumer }

    static class JmsListener<E> implements MessageListener {

        private final Logger logger = LoggerFactory.getLogger(this.getClass());

        private SynchronousQueue<E> queue;

        private MessageConverter<E> converter;

        public JmsListener(MessageConverter<E> converter) {

            this.queue = new SynchronousQueue<E>();
            this.converter = converter;
        }

        @Override
        public void onMessage(Message msg) {

            try {

                E e = converter.parse(msg);

                this.queue.put(e);

            } catch (Exception e) {
                logger.error("onMessage error.", e);
            }

        }

        public E take() throws InterruptedException {
            return queue.take();
        }
        
        public E take(long timeout) throws InterruptedException {
            return queue.poll(timeout, TimeUnit.SECONDS);
        }

    }

}
