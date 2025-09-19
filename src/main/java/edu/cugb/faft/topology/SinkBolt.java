package edu.cugb.faft.topology;

import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * 默认下沉算子（storm默认容错策略使用）
 * 输入字段：word, count
 */
public class SinkBolt extends BaseRichBolt {
    @Override public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {}

    @Override public void execute(Tuple input) {
        String word = input.getStringByField("word");
        int count = input.getIntegerByField("count");
        System.out.printf("[Default Sink] Word=%s | Count=%d%n", word, count);
    }

    @Override public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
