package com;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * 
 * 创建日期:2015-1-9
 * Description：对日志进行分析统计
 * @author tony.he
 * @version 1.0
 */
@SuppressWarnings("serial")
public class CountBolt implements IRichBolt {

	private OutputCollector collector;
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
	}

	@Override
	public void execute(Tuple input) {

		List<Object> list = input.getValues();
		
		System.out.println("id>>>>>>>:" + list.get(0));
		System.out.println("content>>>>>>>:" + list.get(1));
		
		collector.emit(list);
		collector.ack(input);
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		
		this.collector = collector;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("id", "info"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	 
}
