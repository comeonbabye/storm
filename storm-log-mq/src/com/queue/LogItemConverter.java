package com.queue;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LogItemConverter implements MessageConverter<LogItem> {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public Message createMsg(Session session, LogItem logItem) {

    	 try {
         	
    		 TextMessage message = session.createTextMessage();
    		 message.setStringProperty("dbName", logItem.getDbName());
    		 message.setStringProperty("url", logItem.getUrl());
             
    		 return message;
    		 
         } catch (JMSException e) {
             e.printStackTrace();
             logger.error("create message error.", e);
         }
         
        return null;
    }

    @Override
    public LogItem parse(Message msg) {

        try {
        	
        	LogItem logItem = new LogItem();
        	
            logItem.setDbName(msg.getStringProperty("dbName"));
            logItem.setUrl(msg.getStringProperty("url"));

            return logItem;
            
        } catch (JMSException e) {
            e.printStackTrace();
            logger.error("parse logItem error.", e);
        }
        
        return null;
    }

}
