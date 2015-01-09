package com;

import java.util.List;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * 
 * 创建日期:2015-1-9
 * Description：对日志进行分析统计
 * @author tony.he
 * @version 1.0
 */
@SuppressWarnings("serial")
public class CountBolt extends BaseBasicBolt {

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		List<Object> list = input.getValues();
		
		System.out.println("id>>>>>>>:" + list.get(0));
		System.out.println("content>>>>>>>:" + list.get(1));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
